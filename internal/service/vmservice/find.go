/*
Copyright 2023-2026 IONOS Cloud.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package vmservice

import (
	"context"
	"fmt"
	"strings"

	"github.com/luthermonson/go-proxmox"
	"github.com/pkg/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/cluster-api/util"
	"sigs.k8s.io/cluster-api/util/conditions"

	infrav1 "github.com/ionos-cloud/cluster-api-provider-proxmox/api/v1alpha2"
	"github.com/ionos-cloud/cluster-api-provider-proxmox/pkg/scope"
)

var (
	// ErrVMNotCreated VM is not created yet.
	ErrVMNotCreated = errors.New("vm not created")

	// ErrVMNotFound VM is not Found in Proxmox.
	ErrVMNotFound = errors.New("vm not found")

	// ErrVMNotInitialized VM is not Initialized in Proxmox.
	ErrVMNotInitialized = errors.New("vm not initialized")

	// ErrVMIDCollision is returned when spec.virtualMachineID resolves to a VM owned by a
	// different machine, i.e. the chosen id collides with another VM sharing the Proxmox VMID
	// namespace. See handleVMIDCollision for how this is recovered from or surfaced.
	ErrVMIDCollision = errors.New("vmid collision: id belongs to a different vm")
)

// placeholderVMName returns the name Proxmox reports for a VM that has no configured name yet,
// e.g. "VM 136". A freshly-cloned VM carries this placeholder until the name requested at clone
// time propagates; matching it lets the caller wait for initialization instead of mistaking the
// machine's own clone for a foreign VM.
func placeholderVMName(vmID int64) string {
	return fmt.Sprintf("VM %d", vmID)
}

// vmIdentityMatches reports whether vm is the VM this machine owns, and whether it is still
// initializing (so a mismatch means "wait", not "collision"). name is the VM name field the caller
// observed (status name for FindVM, config name for updateVMLocation).
//
// Once a machine has adopted a VM, its identity is the BIOS UUID recorded in spec.providerID, not
// the name: an operator or Proxmox tooling can rename a VM without changing what it is, and a
// rename must not trigger a destructive re-roll. Before adoption there is no UUID yet, so the name
// set at clone time is the only link; an empty or placeholder name means the clone is still
// initializing.
func vmIdentityMatches(vm *proxmox.VirtualMachine, name string, s *scope.MachineScope) (matches, initializing bool) {
	// Use UUID identity only once providerID carries a real UUID; "proxmox://" with an empty UUID
	// (a VM with no readable BIOS UUID) falls back to name matching below.
	if want := strings.TrimPrefix(s.ProxmoxMachine.Spec.ProviderID, "proxmox://"); want != "" {
		got := ""
		if vm.VirtualMachineConfig != nil {
			got = extractUUID(vm.VirtualMachineConfig.SMBios1)
		}
		if got == "" {
			// The adopted VM's UUID is not readable yet; wait rather than declare a collision.
			return false, true
		}
		return got == want, false
	}
	if name == "" || name == placeholderVMName(s.GetVirtualMachineID()) {
		return false, true
	}
	return name == s.ProxmoxMachine.GetName(), false
}

// FindVM returns the Proxmox VM if the vmID is set, otherwise
// returns ErrVMNotCreated or ErrVMNotFound if the VM doesn't exist.
func FindVM(ctx context.Context, scope *scope.MachineScope) (*proxmox.VirtualMachine, error) {
	// find the vm
	vmID := scope.GetVirtualMachineID()
	if vmID > 0 {
		node := scope.LocateProxmoxNode()

		vm, err := scope.InfraCluster.ProxmoxClient.GetVM(ctx, node, vmID)
		if err != nil {
			scope.Error(err, "unable to find vm")
			return nil, ErrVMNotFound
		}
		// Decide identity by UUID once adopted, else by name (placeholder/empty name means the
		// clone is still initializing). A mismatch means the id resolves to a different machine's
		// VM: a VMID collision, handled by the caller via handleVMIDCollision.
		matches, initializing := vmIdentityMatches(vm, vm.Name, scope)
		if initializing {
			scope.Info("vm is not initialized yet")
			return nil, ErrVMNotInitialized
		}
		if !matches {
			return nil, fmt.Errorf("vmid %d resolves to VM %q, expected %q: %w",
				vmID, vm.Name, scope.ProxmoxMachine.GetName(), ErrVMIDCollision)
		}
		return vm, nil
	}

	scope.Info("vmid doesn't exist yet")
	return nil, ErrVMNotCreated
}

func updateVMLocation(ctx context.Context, s *scope.MachineScope) error {
	// if there's an associated task, requeue.
	if s.ProxmoxMachine.Status.TaskRef != nil {
		return errors.New("cannot update with active task")
	}

	// The controller sometimes might to run into the issue, that a VM is orphaned,
	// because it failed to properly update the status.
	// In this case it should try to locate the orphaned VM inside the
	// Proxmox cluster and update the status accordingly.

	vmID := s.GetVirtualMachineID()

	// We are looking for a machine with the ID and check if the name matches.
	// Then we have to update the node in the machine and cluster status.
	rsc, err := s.InfraCluster.ProxmoxClient.FindVMResource(ctx, uint64(vmID))
	if err != nil {
		return err
	}

	// find the VM, to make sure the vm config is up-to-date.
	vm, err := s.InfraCluster.ProxmoxClient.GetVM(ctx, rsc.Node, vmID)
	if err != nil {
		return errors.Wrapf(err, "unable to find vm with id %d", rsc.VMID)
	}

	// Requeue if machine doesn't have a name yet.
	// It seems that the Proxmox source API does not always provide
	// the latest information about the resources in the cluster.
	// It might happen that even when a task is already finished,
	// we still have to wait until we can get the correct
	// information for a particular resource.
	// Decide identity by UUID once adopted, else by config name. If the id resolves cluster-wide
	// to a different VM, report it as a collision; the caller decides whether to recover or fail
	// (see handleVMIDCollision). This keeps collision handling in one place, reachable from every
	// detection path.
	matches, initializing := vmIdentityMatches(vm, vm.VirtualMachineConfig.Name, s)
	if initializing {
		return errors.New("vm exists but does not have a name yet")
	}
	machineName := s.ProxmoxMachine.GetName()
	if !matches {
		return fmt.Errorf("vmid %d resolves to VM %q, expected %q: %w",
			vmID, vm.VirtualMachineConfig.Name, machineName, ErrVMIDCollision)
	}

	// Update the Proxmox node in the status.
	s.ProxmoxMachine.Status.ProxmoxNode = new(vm.Node)

	// Attempt to update the cluster status
	updated := s.InfraCluster.ProxmoxCluster.UpdateNodeLocation(
		machineName,
		vm.Node,
		util.IsControlPlaneMachine(s.Machine),
	)

	if updated {
		return s.InfraCluster.PatchObject()
	}

	return nil
}

// handleVMIDCollision reacts to a detected VMID collision (cause wraps ErrVMIDCollision).
//
// It self-heals only an id the controller itself allocated and has not adopted yet: two
// reconciles sharing one Proxmox VMID namespace selected the same id and the other won the race.
// In that case it releases the id and resets the state machine to Cloning (non-terminal), so the
// next reconcile selects a fresh one. Re-selection provably skips occupied ids (Proxmox nextid
// advances, and the range path CheckID-skips taken ids, failing terminally on exhaustion), so
// this converges rather than looping. Provenance is the VMIDAllocatedByControllerAnnotation set
// at allocation time, an explicit signal that (unlike Status.ProxmoxNode) is never set for an
// operator-pinned id.
//
// For every other case - an operator-pinned id (no annotation) or an already-adopted VM
// (providerID set) whose name no longer matches - it neither releases the id (that would violate
// operator intent / abandon an adopted VM) nor latches a terminal VMProvisionFailed. A terminal
// failure would let MachineHealthCheck remediate by deleting the Machine, and DeleteVM removes
// purely by VMID with no ownership check, so it could destroy a VM this machine does not own.
// Instead it surfaces a visible, non-terminal condition and requeues so an operator can resolve
// the conflict without the controller autonomously deleting anything.
func handleVMIDCollision(s *scope.MachineScope, cause error) error {
	controllerAllocated := s.ProxmoxMachine.Annotations[infrav1.VMIDAllocatedByControllerAnnotation] == "true"
	if s.ProxmoxMachine.Spec.ProviderID != "" || !controllerAllocated {
		s.Logger.Info("VMID collision on a pinned or already-adopted id; requeueing without releasing it",
			"vmID", s.GetVirtualMachineID(), "cause", cause.Error())
		conditions.Set(s.ProxmoxMachine, metav1.Condition{
			Type:    infrav1.ProxmoxMachineVirtualMachineProvisionedCondition,
			Status:  metav1.ConditionFalse,
			Reason:  infrav1.ProxmoxMachineVirtualMachineProvisionedCloningReason,
			Message: cause.Error(),
		})
		return cause
	}

	s.Logger.Info("VMID collision detected, releasing controller-allocated id and requeueing for re-selection",
		"vmID", s.GetVirtualMachineID(), "cause", cause.Error())
	// Persist the cluster-status change first; only mutate the machine once it succeeds. Dropping
	// the stale node location keeps a later AddNodeLocation on re-clone from being a no-op
	// (AddNodeLocation skips machines it already knows). Ordering it before the machine mutations
	// means a failed cluster patch cannot leave the id released while a stale NodeLocation lingers.
	s.InfraCluster.ProxmoxCluster.RemoveNodeLocation(s.ProxmoxMachine.GetName(), util.IsControlPlaneMachine(s.Machine))
	if err := s.InfraCluster.PatchObject(); err != nil {
		return err
	}
	s.ClearVirtualMachineID()
	s.ProxmoxMachine.Status.ProxmoxNode = nil
	delete(s.ProxmoxMachine.Annotations, infrav1.VMIDAllocatedByControllerAnnotation)
	conditions.Set(s.ProxmoxMachine, metav1.Condition{
		Type:    infrav1.ProxmoxMachineVirtualMachineProvisionedCondition,
		Status:  metav1.ConditionFalse,
		Reason:  infrav1.ProxmoxMachineVirtualMachineProvisionedCloningReason,
		Message: cause.Error(),
	})
	return cause
}
