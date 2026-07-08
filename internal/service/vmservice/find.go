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
		// A freshly-cloned VM may not have its name populated yet; wait for it.
		if vm.Name == "" {
			scope.Info("vm is not initialized yet")
			return nil, ErrVMNotInitialized
		}
		// A non-empty name that is not ours means the id resolves to a different machine's VM:
		// a VMID collision (handled by the caller via handleVMIDCollision).
		if vm.Name != scope.ProxmoxMachine.GetName() {
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
	if vm.VirtualMachineConfig.Name == "" {
		return errors.New("vm exists but does not have a name yet")
	}

	// If we locate the VM cluster-wide but its name doesn't match this machine, the chosen
	// VMID belongs to a different VM. Report it as a collision; the caller decides whether to
	// recover or fail (see handleVMIDCollision). This keeps collision handling in one place,
	// reachable from every detection path.
	machineName := s.ProxmoxMachine.GetName()
	if vm.VirtualMachineConfig.Name != machineName {
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
// It self-heals only a controller-selected id that has not been adopted yet: two reconciles
// sharing one Proxmox VMID namespace selected the same id and the other won the race. In that
// case it releases the id and resets the state machine to Cloning (non-terminal), so the next
// reconcile selects a fresh one. Re-selection provably skips occupied ids (Proxmox nextid
// advances, and the range path CheckID-skips taken ids, failing terminally on exhaustion), so
// this converges rather than looping.
//
// It stays terminal (VMProvisionFailed, so MachineHealthCheck can remediate) when either:
//   - providerID is set: we already adopted a VM at this id and its name changed — operator
//     -worthy, and never silently abandon an adopted VM (anti-adoption guard); or
//   - Status.ProxmoxNode is nil: createVM never ran for this machine, so the id was pinned by
//     an operator (or otherwise set externally) rather than allocated by the controller;
//     clobbering it would violate intent.
//
// The returned error is always the (transient) cause, so the controller requeues; the terminal
// path additionally latches HasFailed so the next reconcile short-circuits.
func handleVMIDCollision(s *scope.MachineScope, cause error) error {
	if s.ProxmoxMachine.Spec.ProviderID != "" || s.ProxmoxMachine.Status.ProxmoxNode == nil {
		conditions.Set(s.ProxmoxMachine, metav1.Condition{
			Type:    infrav1.ProxmoxMachineVirtualMachineProvisionedCondition,
			Status:  metav1.ConditionFalse,
			Reason:  infrav1.ProxmoxMachineVirtualMachineProvisionedVMProvisionFailedReason,
			Message: cause.Error(),
		})
		return cause
	}

	s.Logger.Info("VMID collision detected, releasing id and requeueing for re-selection",
		"vmID", s.GetVirtualMachineID(), "cause", cause.Error())
	s.ClearVirtualMachineID()
	s.ProxmoxMachine.Status.ProxmoxNode = nil
	// Drop the stale cluster-status node location so a later AddNodeLocation on re-clone is not
	// a no-op (AddNodeLocation skips machines it already knows).
	s.InfraCluster.ProxmoxCluster.RemoveNodeLocation(s.ProxmoxMachine.GetName(), util.IsControlPlaneMachine(s.Machine))
	conditions.Set(s.ProxmoxMachine, metav1.Condition{
		Type:    infrav1.ProxmoxMachineVirtualMachineProvisionedCondition,
		Status:  metav1.ConditionFalse,
		Reason:  infrav1.ProxmoxMachineVirtualMachineProvisionedCloningReason,
		Message: cause.Error(),
	})
	if err := s.InfraCluster.PatchObject(); err != nil {
		return err
	}
	return cause
}
