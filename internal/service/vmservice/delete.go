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
	"time"

	"github.com/luthermonson/go-proxmox"
	"github.com/pkg/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/cluster-api/util"
	"sigs.k8s.io/cluster-api/util/conditions"
	ctrlutil "sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	infrav1 "github.com/ionos-cloud/cluster-api-provider-proxmox/api/v1alpha2"
	"github.com/ionos-cloud/cluster-api-provider-proxmox/internal/service/taskservice"
	"github.com/ionos-cloud/cluster-api-provider-proxmox/pkg/proxmox/goproxmox"
	"github.com/ionos-cloud/cluster-api-provider-proxmox/pkg/scope"
)

const (
	deletionTaskQMStop     = "qmstop"
	deletionTaskQMDestroy  = "qmdestroy"
	deletionTaskRetryAfter = time.Minute

	// Proxmox allocatable VMIDs start at 100; lower values include the unassigned sentinel
	// and Proxmox-reserved IDs that this provider should not try to delete.
	minimumProxmoxVMID int64 = 100
)

// DeleteVM implements the logic of destroying a VM.
func DeleteVM(ctx context.Context, machineScope *scope.MachineScope) error {
	if inFlight, err := reconcileInFlightDeletionTask(ctx, machineScope); err != nil || inFlight {
		return err
	}

	vmID := machineScope.ProxmoxMachine.GetVirtualMachineID()
	if unassignedOrReservedVMID(vmID) {
		return completeVMDeletion(machineScope)
	}
	node := machineScope.LocateProxmoxNode()

	// Ownership guard: never qmdestroy a VM this machine does not own. If the id resolves at this
	// node to a VM whose identity (UUID once adopted, else name) is not ours - a foreign VM that
	// reused the id, e.g. a pre-upgrade machine that kept a colliding id - our VM is already gone;
	// complete deletion without a destroy that would remove the foreign VM. If the VM cannot be
	// observed here (moved nodes / already gone), fall through: DeleteVM's not-found path and
	// completeIfVMIDFree resolve it cluster-wide.
	if vm, gerr := machineScope.InfraCluster.ProxmoxClient.GetVM(ctx, node, vmID); gerr == nil {
		if matches, initializing := vmIdentityMatches(vm, vm.Name, machineScope); !matches && !initializing {
			return completeVMDeletion(machineScope)
		}
	}

	task, err := machineScope.InfraCluster.ProxmoxClient.DeleteVM(ctx, node, vmID)
	if err != nil {
		if errors.Is(err, goproxmox.ErrVMIDFree) {
			return completeVMDeletion(machineScope)
		}
		if VMNotFound(err) {
			verificationContext := fmt.Sprintf("VM lookup on node %q returned not found: %v", node, err)
			if completed, completionErr := completeIfVMIDFree(ctx, machineScope, verificationContext); completionErr != nil || completed {
				return completionErr
			}
			setDeletionFailedCondition(machineScope, fmt.Sprintf("VMID %d is still in use after VM lookup on node %q returned not found: %v", vmID, node, err))
			// reconcileDelete always returns DefaultReconcilerRequeue after nil, so keep the
			// finalizer and visible failure condition without switching to error backoff.
			return nil
		}
		setDeletionFailedCondition(machineScope, err.Error())
		return err
	}

	if task != nil {
		storeDeletionTask(machineScope, task)
	}

	return nil
}

func reconcileInFlightDeletionTask(ctx context.Context, machineScope *scope.MachineScope) (bool, error) {
	if machineScope.ProxmoxMachine.Status.TaskRef == nil {
		return false, nil
	}

	taskRef := *machineScope.ProxmoxMachine.Status.TaskRef
	task, err := taskservice.GetTask(ctx, machineScope)
	if err != nil {
		verificationContext := fmt.Sprintf("deletion task lookup %s failed: %v", taskRef, err)
		if completed, completionErr := completeIfVMIDFree(ctx, machineScope, verificationContext); completionErr != nil || completed {
			return completed, completionErr
		}
		// The task lookup failed and our VM still exists. Do not depend on the error text to
		// decide whether the task is gone: a task ref naming a removed node ("hostname lookup
		// 'hvXX' failed") or any otherwise-unrecognized failure would loop forever holding a stale
		// ref, while a transient message containing "not found" would falsely look resolved.
		// Completion is already gated on VM ownership above, so here just drop the stale task and
		// let the next reconcile re-derive progress by re-issuing the destroy - idempotent, since
		// a still-running destroy is rejected and retried. This makes deletion robust regardless of
		// how GetTask classified the error.
		clearTaskState(machineScope)
		return false, nil
	}
	if task == nil {
		return true, nil
	}
	if !isDeletionTask(task) {
		// Deletion owns cleanup from here; stale provisioning task refs (for example qmstart)
		// must not block finalizer progress even if the old task is still in flight.
		clearTaskState(machineScope)
		return false, nil
	}

	switch {
	case task.IsRunning:
		storeDeletionTask(machineScope, task)
		return true, nil
	case task.IsSuccessful && task.IsCompleted:
		if task.Type == deletionTaskQMDestroy {
			// qmdestroy success is authoritative for this deletion task. Do not gate
			// finalizer removal on CheckID because the VMID may already have been
			// reused by a replacement VM before this reconcile observes task completion.
			return true, completeVMDeletion(machineScope)
		}
		clearTaskState(machineScope)
		return false, nil
	case task.IsFailed:
		verificationContext := fmt.Sprintf("%s task %s failed: %s", task.Type, task.UPID, task.ExitStatus)
		if completed, err := completeIfVMIDFree(ctx, machineScope, verificationContext); err != nil || completed {
			return completed, err
		}
		if waitForRetryAfter(machineScope, task) {
			return true, nil
		}
		return false, nil
	default:
		return false, taskservice.NewRequeueError(fmt.Sprintf("unknown deletion task state %q for %q", task.ExitStatus, machineScope.ProxmoxMachine.Name), infrav1.DefaultReconcilerRequeue)
	}
}

func storeDeletionTask(machineScope *scope.MachineScope, task *proxmox.Task) {
	taskRef := string(task.UPID)
	machineScope.ProxmoxMachine.Status.TaskRef = &taskRef
	// A fresh/running deletion task supersedes any failed-task retry gate.
	machineScope.ProxmoxMachine.Status.RetryAfter = nil
	setDeletingCondition(machineScope, fmt.Sprintf("waiting for %s task %s", task.Type, task.UPID))
}

func clearTaskState(machineScope *scope.MachineScope) {
	machineScope.ProxmoxMachine.Status.TaskRef = nil
	machineScope.ProxmoxMachine.Status.RetryAfter = nil
}

func waitForRetryAfter(machineScope *scope.MachineScope, task *proxmox.Task) bool {
	if taskservice.RetryAfterExpired(&machineScope.ProxmoxMachine.Status, deletionTaskRetryAfter) {
		clearTaskState(machineScope)
		return false
	}
	setDeletionFailedCondition(machineScope, deletionTaskFailureMessage(task))
	return true
}

func setDeletingCondition(machineScope *scope.MachineScope, message string) {
	conditions.Set(machineScope.ProxmoxMachine, metav1.Condition{
		Type:    infrav1.ProxmoxMachineVirtualMachineProvisionedCondition,
		Status:  metav1.ConditionFalse,
		Reason:  infrav1.ProxmoxMachineVirtualMachineProvisionedDeletingReason,
		Message: message,
	})
}

func completeVMDeletion(machineScope *scope.MachineScope) error {
	clearTaskState(machineScope)
	// remove machine from cluster status
	machineScope.InfraCluster.ProxmoxCluster.RemoveNodeLocation(machineScope.Name(), util.IsControlPlaneMachine(machineScope.Machine))
	// The VM is deleted so remove the finalizer.
	ctrlutil.RemoveFinalizer(machineScope.ProxmoxMachine, infrav1.MachineFinalizer)
	return machineScope.InfraCluster.PatchObject()
}

func completeIfVMIDFree(ctx context.Context, machineScope *scope.MachineScope, verificationContext string) (bool, error) {
	vmID := machineScope.ProxmoxMachine.GetVirtualMachineID()
	if unassignedOrReservedVMID(vmID) {
		return true, completeVMDeletion(machineScope)
	}
	vmIDFree, err := machineScope.InfraCluster.ProxmoxClient.CheckID(ctx, vmID)
	if err != nil {
		setDeletingCondition(machineScope, checkIDErrorMessage(vmID, verificationContext, err))
		return false, checkIDError(vmID, verificationContext, err)
	}
	if vmIDFree {
		return true, completeVMDeletion(machineScope)
	}

	// CheckID reports cluster-wide freeness, not ownership: the id being in use does not mean
	// our VM still exists. It may have been destroyed and the id already reused by a replacement
	// or another cluster's VM sharing the Proxmox VMID namespace - a race that VMID-collision
	// recovery makes more likely by deliberately releasing ids for reuse. Gating the finalizer on
	// freeness alone therefore deadlocks: once the id is reused it never reads free again. Resolve
	// the VM now holding the id cluster-wide and decide by ownership.
	rsc, err := machineScope.InfraCluster.ProxmoxClient.FindVMResource(ctx, uint64(vmID))
	if err != nil {
		setDeletingCondition(machineScope, checkIDErrorMessage(vmID, verificationContext, err))
		return false, checkIDError(vmID, verificationContext, err)
	}
	if vmResourceIsForeign(rsc, machineScope.Name()) {
		// A foreign name means our VM is gone and the id has been reused; this deletion is
		// complete. completeVMDeletion never destroys a VM, so this cannot touch the foreign one.
		return true, completeVMDeletion(machineScope)
	}

	// The VM at our id is still ours (or unnamed/initializing and so not provably foreign - see
	// vmResourceIsForeign). It may have moved off Status.ProxmoxNode since that was recorded (HA
	// failover / live migration), which is why the destroy at the stale node reported not found.
	// Adopt the id's current node so the next reconcile retries the destroy there instead of
	// looping forever on the stale node. Keep the finalizer until the VM is actually gone.
	if rsc.Node != "" && machineScope.LocateProxmoxNode() != rsc.Node {
		machineScope.ProxmoxMachine.Status.ProxmoxNode = new(rsc.Node)
		machineScope.InfraCluster.ProxmoxCluster.UpdateNodeLocation(
			machineScope.Name(), rsc.Node, util.IsControlPlaneMachine(machineScope.Machine))
	}
	return false, nil
}

// vmResourceIsForeign reports whether the cluster resource holding our VMID is definitively a
// different machine's VM. It is foreign only when it carries a real, non-placeholder name that is
// not ours. An empty or placeholder ("VM <vmid>") name belongs to an unnamed/initializing VM that
// cannot be proven foreign, so it is treated as possibly-ours - the safe side, since acting on a
// false "foreign" verdict would orphan our own half-created VM by dropping the finalizer.
func vmResourceIsForeign(rsc *proxmox.ClusterResource, machineName string) bool {
	if rsc.Name == "" || rsc.Name == placeholderVMName(int64(rsc.VMID)) {
		return false
	}
	return rsc.Name != machineName
}

func checkIDErrorMessage(vmID int64, verificationContext string, err error) string {
	if verificationContext == "" {
		return fmt.Sprintf("waiting to verify VMID %d is free: %v", vmID, err)
	}
	return fmt.Sprintf("waiting to verify VMID %d is free after %s: %v", vmID, verificationContext, err)
}

func checkIDError(vmID int64, verificationContext string, err error) error {
	if verificationContext == "" {
		return fmt.Errorf("verify VMID %d is free: %w", vmID, err)
	}
	return fmt.Errorf("verify VMID %d is free after %s: %w", vmID, verificationContext, err)
}

func setDeletionFailedCondition(machineScope *scope.MachineScope, message string) {
	conditions.Set(machineScope.ProxmoxMachine, metav1.Condition{
		Type:    infrav1.ProxmoxMachineVirtualMachineProvisionedCondition,
		Status:  metav1.ConditionFalse,
		Reason:  infrav1.ProxmoxMachineVirtualMachineProvisionedDeletionFailedReason,
		Message: message,
	})
}

func deletionTaskFailureMessage(task *proxmox.Task) string {
	if task.ExitStatus == string(taskservice.TaskInfoStateOK) {
		return fmt.Sprintf("task %s failed but its exit status is OK; this should not happen", task.UPID)
	}
	return fmt.Sprintf("%s: %s", task.Type, task.ExitStatus)
}

func isDeletionTask(task *proxmox.Task) bool {
	return task.Type == deletionTaskQMStop || task.Type == deletionTaskQMDestroy
}

func unassignedOrReservedVMID(vmID int64) bool {
	return vmID < minimumProxmoxVMID
}

// VMNotFound checks if the given err is related to that the VM is not found in Proxmox.
func VMNotFound(err error) bool {
	return strings.Contains(err.Error(), "does not exist")
}
