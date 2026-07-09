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
	"errors"
	"testing"
	"time"

	"github.com/luthermonson/go-proxmox"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/cluster-api/util/conditions"

	infrav1 "github.com/ionos-cloud/cluster-api-provider-proxmox/api/v1alpha2"
	"github.com/ionos-cloud/cluster-api-provider-proxmox/internal/service/taskservice"
	"github.com/ionos-cloud/cluster-api-provider-proxmox/pkg/proxmox/goproxmox"
	"github.com/ionos-cloud/cluster-api-provider-proxmox/pkg/proxmox/proxmoxtest"
	"github.com/ionos-cloud/cluster-api-provider-proxmox/pkg/scope"
)

func TestDeleteVM_SuccessNotFound(t *testing.T) {
	machineScope, proxmoxClient := setupDeleteVMTest(t)

	proxmoxClient.EXPECT().GetVM(context.TODO(), "node1", int64(123)).Return(nil, errors.New("does not exist")).Once()
	proxmoxClient.EXPECT().DeleteVM(context.TODO(), "node1", int64(123)).Return(nil, errors.New("vm does not exist: some reason")).Once()
	proxmoxClient.EXPECT().CheckID(context.TODO(), int64(123)).Return(true, nil).Once()

	require.NoError(t, DeleteVM(context.TODO(), machineScope))
	requireDeleteComplete(t, machineScope)
}

func TestDeleteVM_NotFoundButVMIDStillAllocatedKeepsFinalizer(t *testing.T) {
	machineScope, proxmoxClient := setupDeleteVMTest(t)

	proxmoxClient.EXPECT().GetVM(context.TODO(), "node1", int64(123)).Return(nil, errors.New("does not exist")).Once()
	proxmoxClient.EXPECT().DeleteVM(context.TODO(), "node1", int64(123)).Return(nil, errors.New("vm does not exist: stale node location")).Once()
	proxmoxClient.EXPECT().CheckID(context.TODO(), int64(123)).Return(false, nil).Once()
	proxmoxClient.EXPECT().FindVMResource(context.TODO(), uint64(123)).Return(newVMResource(), nil).Once()

	require.NoError(t, DeleteVM(context.TODO(), machineScope))
	requireDeleteBlockedOnVMID(t, machineScope)

	cond := conditions.Get(machineScope.ProxmoxMachine, infrav1.ProxmoxMachineVirtualMachineProvisionedCondition)
	require.NotNil(t, cond)
	require.Equal(t, metav1.ConditionFalse, cond.Status)
	require.Equal(t, infrav1.ProxmoxMachineVirtualMachineProvisionedDeletionFailedReason, cond.Reason)
	require.Contains(t, cond.Message, "VMID 123 is still in use")
}

// TestDeleteVM_NotFoundAndVMIDReusedByForeignVMCompletesDeletion covers the deadlock that
// gating the finalizer on CheckID alone caused: our VM is gone, but the id now resolves to a
// different VM (a replacement or another cluster's, reused after collision recovery released it),
// so CheckID reports it in use forever. Ownership must be checked - a foreign name means our VM
// is gone and deletion completes, without ever touching the foreign VM.
func TestDeleteVM_NotFoundAndVMIDReusedByForeignVMCompletesDeletion(t *testing.T) {
	machineScope, proxmoxClient := setupDeleteVMTest(t)
	foreign := &proxmox.ClusterResource{Name: "other-cluster-worker", Node: "node2"}

	// The foreign VM is on another node, so the pre-destroy guard cannot observe it here; the
	// cluster-wide lookup below is what recognizes the id was reused.
	proxmoxClient.EXPECT().GetVM(context.TODO(), "node1", int64(123)).Return(nil, errors.New("does not exist")).Once()
	proxmoxClient.EXPECT().DeleteVM(context.TODO(), "node1", int64(123)).Return(nil, errors.New("vm does not exist: some reason")).Once()
	proxmoxClient.EXPECT().CheckID(context.TODO(), int64(123)).Return(false, nil).Once()
	proxmoxClient.EXPECT().FindVMResource(context.TODO(), uint64(123)).Return(foreign, nil).Once()

	require.NoError(t, DeleteVM(context.TODO(), machineScope))
	requireDeleteComplete(t, machineScope)
}

// TestDeleteVM_ForeignVMAtNodeIsNotDestroyed covers the pre-destroy ownership guard: the id
// resolves at this node to a VM that is not ours (a foreign VM that reused the id). Deletion must
// complete without ever issuing a destroy against that VM.
func TestDeleteVM_ForeignVMAtNodeIsNotDestroyed(t *testing.T) {
	machineScope, proxmoxClient := setupDeleteVMTest(t)
	foreign := newRunningVM()
	foreign.Name = "other-cluster-worker"

	proxmoxClient.EXPECT().GetVM(context.TODO(), "node1", int64(123)).Return(foreign, nil).Once()

	require.NoError(t, DeleteVM(context.TODO(), machineScope))
	requireDeleteComplete(t, machineScope)
}

// TestDeleteVM_NotFoundAndVMIDLookupErrorKeepsFinalizer covers a transient failure resolving the
// VM holding the id: with CheckID reporting the id in use and ownership unknown, deletion must not
// complete - keep the finalizer and surface a retryable Deleting condition.
func TestDeleteVM_NotFoundAndVMIDLookupErrorKeepsFinalizer(t *testing.T) {
	machineScope, proxmoxClient := setupDeleteVMTest(t)

	proxmoxClient.EXPECT().GetVM(context.TODO(), "node1", int64(123)).Return(nil, errors.New("does not exist")).Once()
	proxmoxClient.EXPECT().DeleteVM(context.TODO(), "node1", int64(123)).Return(nil, errors.New("vm does not exist: stale node location")).Once()
	proxmoxClient.EXPECT().CheckID(context.TODO(), int64(123)).Return(false, nil).Once()
	proxmoxClient.EXPECT().FindVMResource(context.TODO(), uint64(123)).Return(nil, errors.New("temporary resource lookup failure")).Once()

	err := DeleteVM(context.TODO(), machineScope)
	require.Error(t, err)
	require.ErrorContains(t, err, "temporary resource lookup failure")
	requireDeleteBlockedOnVMID(t, machineScope)

	cond := conditions.Get(machineScope.ProxmoxMachine, infrav1.ProxmoxMachineVirtualMachineProvisionedCondition)
	require.NotNil(t, cond)
	require.Equal(t, metav1.ConditionFalse, cond.Status)
	require.Equal(t, infrav1.ProxmoxMachineVirtualMachineProvisionedDeletingReason, cond.Reason)
	require.Contains(t, cond.Message, "temporary resource lookup failure")
}

// TestDeleteVM_NotFoundAndVMIDPlaceholderNameKeepsFinalizer covers the ownership check meeting
// Proxmox's placeholder name "VM <vmid>" for a still-nameless VM: it is not provably foreign, so
// deletion must not complete (dropping the finalizer would orphan the machine's own half-created
// VM). Keep the finalizer and requeue.
func TestDeleteVM_NotFoundAndVMIDPlaceholderNameKeepsFinalizer(t *testing.T) {
	machineScope, proxmoxClient := setupDeleteVMTest(t)
	placeholder := &proxmox.ClusterResource{Name: placeholderVMName(123), Node: "node1", VMID: 123}

	proxmoxClient.EXPECT().GetVM(context.TODO(), "node1", int64(123)).Return(nil, errors.New("does not exist")).Once()
	proxmoxClient.EXPECT().DeleteVM(context.TODO(), "node1", int64(123)).Return(nil, errors.New("vm does not exist: stale node location")).Once()
	proxmoxClient.EXPECT().CheckID(context.TODO(), int64(123)).Return(false, nil).Once()
	proxmoxClient.EXPECT().FindVMResource(context.TODO(), uint64(123)).Return(placeholder, nil).Once()

	require.NoError(t, DeleteVM(context.TODO(), machineScope))
	requireDeleteBlockedOnVMID(t, machineScope)
}

// TestDeleteVM_NotFoundButVMMigratedRelocatesAndKeepsFinalizer covers a VM that is still ours but
// has moved off the recorded node (HA failover / live migration): the destroy at the stale node
// reports not found while the id resolves cluster-wide to our VM on a different node. Deletion must
// adopt that node so the next reconcile retries the destroy there, rather than looping forever.
func TestDeleteVM_NotFoundButVMMigratedRelocatesAndKeepsFinalizer(t *testing.T) {
	machineScope, proxmoxClient := setupDeleteVMTest(t)
	migrated := &proxmox.ClusterResource{Name: machineScope.Name(), Node: "node2", VMID: 123}

	// The VM has moved to node2, so the guard's lookup at the stale node1 finds nothing.
	proxmoxClient.EXPECT().GetVM(context.TODO(), "node1", int64(123)).Return(nil, errors.New("does not exist")).Once()
	proxmoxClient.EXPECT().DeleteVM(context.TODO(), "node1", int64(123)).Return(nil, errors.New("vm does not exist: stale node location")).Once()
	proxmoxClient.EXPECT().CheckID(context.TODO(), int64(123)).Return(false, nil).Once()
	proxmoxClient.EXPECT().FindVMResource(context.TODO(), uint64(123)).Return(migrated, nil).Once()

	require.NoError(t, DeleteVM(context.TODO(), machineScope))
	require.Contains(t, machineScope.ProxmoxMachine.Finalizers, infrav1.MachineFinalizer)
	require.NotNil(t, machineScope.ProxmoxMachine.Status.ProxmoxNode)
	require.Equal(t, "node2", *machineScope.ProxmoxMachine.Status.ProxmoxNode)
}

// TestDeleteVM_DestroyTaskFailedButVMIDReusedByForeignVMCompletesDeletion covers the same
// ownership check on the in-flight-task path: a failed destroy task whose id has since been reused
// by a foreign VM must complete deletion rather than latch DeletionFailed forever.
func TestDeleteVM_DestroyTaskFailedButVMIDReusedByForeignVMCompletesDeletion(t *testing.T) {
	machineScope, proxmoxClient := setupDeleteVMTest(t)
	machineScope.ProxmoxMachine.Status.TaskRef = ptr.To("UPID:node1:destroy")
	task := &proxmox.Task{
		UPID:        "UPID:node1:destroy",
		IsFailed:    true,
		IsCompleted: true,
		Status:      "stopped",
		ExitStatus:  "VM 103 is running - destroy failed",
		Type:        "qmdestroy",
	}
	foreign := &proxmox.ClusterResource{Name: "other-cluster-worker", Node: "node2"}

	proxmoxClient.EXPECT().GetTask(context.TODO(), "UPID:node1:destroy").Return(task, nil).Once()
	proxmoxClient.EXPECT().CheckID(context.TODO(), int64(123)).Return(false, nil).Once()
	proxmoxClient.EXPECT().FindVMResource(context.TODO(), uint64(123)).Return(foreign, nil).Once()

	require.NoError(t, DeleteVM(context.TODO(), machineScope))
	requireDeleteComplete(t, machineScope)
}

func TestDeleteVM_NotFoundAndCheckIDErrorPreservesDeleteContextAndUsesDeletingReason(t *testing.T) {
	machineScope, proxmoxClient := setupDeleteVMTest(t)

	proxmoxClient.EXPECT().GetVM(context.TODO(), "node1", int64(123)).Return(nil, errors.New("does not exist")).Once()
	proxmoxClient.EXPECT().DeleteVM(context.TODO(), "node1", int64(123)).Return(nil, errors.New("vm does not exist: stale node location")).Once()
	proxmoxClient.EXPECT().CheckID(context.TODO(), int64(123)).Return(false, errors.New("temporary checkid failure")).Once()

	err := DeleteVM(context.TODO(), machineScope)
	require.Error(t, err)
	require.ErrorContains(t, err, "vm does not exist: stale node location")
	require.ErrorContains(t, err, "temporary checkid failure")
	requireDeleteBlockedOnVMID(t, machineScope)

	cond := conditions.Get(machineScope.ProxmoxMachine, infrav1.ProxmoxMachineVirtualMachineProvisionedCondition)
	require.NotNil(t, cond)
	require.Equal(t, metav1.ConditionFalse, cond.Status)
	require.Equal(t, infrav1.ProxmoxMachineVirtualMachineProvisionedDeletingReason, cond.Reason)
	require.Contains(t, cond.Message, "vm does not exist: stale node location")
	require.Contains(t, cond.Message, "temporary checkid failure")
}

func TestDeleteVM_SuccessVMIDFree(t *testing.T) {
	machineScope, proxmoxClient := setupDeleteVMTest(t)

	proxmoxClient.EXPECT().GetVM(context.TODO(), "node1", int64(123)).Return(nil, errors.New("does not exist")).Once()
	proxmoxClient.EXPECT().DeleteVM(context.TODO(), "node1", int64(123)).Return(nil, goproxmox.ErrVMIDFree).Once()

	require.NoError(t, DeleteVM(context.TODO(), machineScope))
	requireDeleteComplete(t, machineScope)
}

func TestDeleteVM_NoVMIDCompletesDeletion(t *testing.T) {
	machineScope, _ := setupDeleteVMTest(t)
	machineScope.ProxmoxMachine.Spec.VirtualMachineID = nil

	require.NoError(t, DeleteVM(context.TODO(), machineScope))
	requireDeleteComplete(t, machineScope)
}

func TestDeleteVM_ReservedVMIDCompletesDeletion(t *testing.T) {
	machineScope, _ := setupDeleteVMTest(t)
	machineScope.ProxmoxMachine.Spec.VirtualMachineID = ptr.To(int64(42))

	require.NoError(t, DeleteVM(context.TODO(), machineScope))
	requireDeleteComplete(t, machineScope)
}

func TestDeleteVM_StoresStopTaskRefAndKeepsFinalizer(t *testing.T) {
	machineScope, proxmoxClient := setupDeleteVMTest(t)
	task := &proxmox.Task{UPID: "UPID:node1:001", Type: "qmstop"}

	proxmoxClient.EXPECT().GetVM(context.TODO(), "node1", int64(123)).Return(newRunningVM(), nil).Once()
	proxmoxClient.EXPECT().DeleteVM(context.TODO(), "node1", int64(123)).Return(task, nil).Once()

	require.NoError(t, DeleteVM(context.TODO(), machineScope))
	requireDeleteInProgress(t, machineScope, string(task.UPID))

	cond := conditions.Get(machineScope.ProxmoxMachine, infrav1.ProxmoxMachineVirtualMachineProvisionedCondition)
	require.NotNil(t, cond)
	require.Equal(t, metav1.ConditionFalse, cond.Status)
	require.Equal(t, infrav1.ProxmoxMachineVirtualMachineProvisionedDeletingReason, cond.Reason)
	require.Contains(t, cond.Message, "waiting for qmstop task")
}

func TestDeleteVM_StoresDestroyTaskRefAndKeepsFinalizer(t *testing.T) {
	machineScope, proxmoxClient := setupDeleteVMTest(t)
	task := &proxmox.Task{UPID: "UPID:node1:002", Type: "qmdestroy"}

	proxmoxClient.EXPECT().GetVM(context.TODO(), "node1", int64(123)).Return(newRunningVM(), nil).Once()
	proxmoxClient.EXPECT().DeleteVM(context.TODO(), "node1", int64(123)).Return(task, nil).Once()

	require.NoError(t, DeleteVM(context.TODO(), machineScope))
	requireDeleteInProgress(t, machineScope, string(task.UPID))

	cond := conditions.Get(machineScope.ProxmoxMachine, infrav1.ProxmoxMachineVirtualMachineProvisionedCondition)
	require.NotNil(t, cond)
	require.Equal(t, metav1.ConditionFalse, cond.Status)
	require.Equal(t, infrav1.ProxmoxMachineVirtualMachineProvisionedDeletingReason, cond.Reason)
	require.Contains(t, cond.Message, "waiting for qmdestroy task")
}

func TestDeleteVM_InFlightDeletionTaskDoesNotStartAnotherDelete(t *testing.T) {
	machineScope, proxmoxClient := setupDeleteVMTest(t)
	machineScope.ProxmoxMachine.Status.TaskRef = ptr.To("UPID:node1:destroy")
	task := &proxmox.Task{UPID: "UPID:node1:destroy", IsRunning: true, Status: "running", Type: "qmdestroy"}

	proxmoxClient.EXPECT().GetTask(context.TODO(), "UPID:node1:destroy").Return(task, nil).Once()

	require.NoError(t, DeleteVM(context.TODO(), machineScope))
	requireDeleteInProgress(t, machineScope, "UPID:node1:destroy")

	cond := conditions.Get(machineScope.ProxmoxMachine, infrav1.ProxmoxMachineVirtualMachineProvisionedCondition)
	require.NotNil(t, cond)
	require.Equal(t, metav1.ConditionFalse, cond.Status)
	require.Equal(t, infrav1.ProxmoxMachineVirtualMachineProvisionedDeletingReason, cond.Reason)
	require.Contains(t, cond.Message, "waiting for qmdestroy task")
}

func TestDeleteVM_UnknownDeletionTaskStateRequestsRequeue(t *testing.T) {
	machineScope, proxmoxClient := setupDeleteVMTest(t)
	machineScope.ProxmoxMachine.Status.TaskRef = ptr.To("UPID:node1:destroy")
	task := &proxmox.Task{UPID: "UPID:node1:destroy", Status: "unknown", ExitStatus: "weird-state", Type: "qmdestroy"}

	proxmoxClient.EXPECT().GetTask(context.TODO(), "UPID:node1:destroy").Return(task, nil).Once()

	err := DeleteVM(context.TODO(), machineScope)
	var requeueErr *taskservice.RequeueError
	require.ErrorAs(t, err, &requeueErr)
	require.Positive(t, requeueErr.RequeueAfter())
	requireDeleteInProgress(t, machineScope, "UPID:node1:destroy")
}

func TestDeleteVM_DestroyTaskFailedSetsDeletionFailed(t *testing.T) {
	machineScope, proxmoxClient := setupDeleteVMTest(t)
	machineScope.ProxmoxMachine.Status.TaskRef = ptr.To("UPID:node1:destroy")
	task := &proxmox.Task{
		UPID:        "UPID:node1:destroy",
		IsFailed:    true,
		IsCompleted: true,
		Status:      "stopped",
		ExitStatus:  "VM 103 is running - destroy failed",
		Type:        "qmdestroy",
	}

	proxmoxClient.EXPECT().GetTask(context.TODO(), "UPID:node1:destroy").Return(task, nil).Once()
	proxmoxClient.EXPECT().CheckID(context.TODO(), int64(123)).Return(false, nil).Once()
	proxmoxClient.EXPECT().FindVMResource(context.TODO(), uint64(123)).Return(newVMResource(), nil).Once()

	require.NoError(t, DeleteVM(context.TODO(), machineScope))
	requireDeleteInProgress(t, machineScope, "UPID:node1:destroy")
	require.NotNil(t, machineScope.ProxmoxMachine.Status.RetryAfter)
	require.True(t, machineScope.ProxmoxMachine.Status.RetryAfter.After(time.Now()))

	cond := conditions.Get(machineScope.ProxmoxMachine, infrav1.ProxmoxMachineVirtualMachineProvisionedCondition)
	require.NotNil(t, cond)
	require.Equal(t, metav1.ConditionFalse, cond.Status)
	require.Equal(t, infrav1.ProxmoxMachineVirtualMachineProvisionedDeletionFailedReason, cond.Reason)
	require.Contains(t, cond.Message, "qmdestroy: VM 103 is running - destroy failed")
}

func TestDeleteVM_StopTaskFailedSetsDeletionFailed(t *testing.T) {
	machineScope, proxmoxClient := setupDeleteVMTest(t)
	machineScope.ProxmoxMachine.Status.TaskRef = ptr.To("UPID:node1:stop")
	task := &proxmox.Task{
		UPID:        "UPID:node1:stop",
		IsFailed:    true,
		IsCompleted: true,
		Status:      "stopped",
		ExitStatus:  "interrupted",
		Type:        "qmstop",
	}

	proxmoxClient.EXPECT().GetTask(context.TODO(), "UPID:node1:stop").Return(task, nil).Once()
	proxmoxClient.EXPECT().CheckID(context.TODO(), int64(123)).Return(false, nil).Once()
	proxmoxClient.EXPECT().FindVMResource(context.TODO(), uint64(123)).Return(newVMResource(), nil).Once()

	require.NoError(t, DeleteVM(context.TODO(), machineScope))
	requireDeleteInProgress(t, machineScope, "UPID:node1:stop")
	require.NotNil(t, machineScope.ProxmoxMachine.Status.RetryAfter)
	require.True(t, machineScope.ProxmoxMachine.Status.RetryAfter.After(time.Now()))

	cond := conditions.Get(machineScope.ProxmoxMachine, infrav1.ProxmoxMachineVirtualMachineProvisionedCondition)
	require.NotNil(t, cond)
	require.Equal(t, metav1.ConditionFalse, cond.Status)
	require.Equal(t, infrav1.ProxmoxMachineVirtualMachineProvisionedDeletionFailedReason, cond.Reason)
	require.Contains(t, cond.Message, "qmstop: interrupted")
}

func TestDeleteVM_DestroyTaskFailedRetryAfterExpiredStartsNewDelete(t *testing.T) {
	machineScope, proxmoxClient := setupDeleteVMTest(t)
	machineScope.ProxmoxMachine.Status.TaskRef = ptr.To("UPID:node1:destroy")
	machineScope.ProxmoxMachine.Status.RetryAfter = &metav1.Time{Time: time.Now().Add(-1 * time.Minute)}
	failedTask := &proxmox.Task{
		UPID:        "UPID:node1:destroy",
		IsFailed:    true,
		IsCompleted: true,
		Status:      "stopped",
		ExitStatus:  "VM 103 is running - destroy failed",
		Type:        "qmdestroy",
	}
	newTask := &proxmox.Task{UPID: "UPID:node1:new-destroy", Type: "qmdestroy"}

	proxmoxClient.EXPECT().GetTask(context.TODO(), "UPID:node1:destroy").Return(failedTask, nil).Once()
	proxmoxClient.EXPECT().CheckID(context.TODO(), int64(123)).Return(false, nil).Once()
	proxmoxClient.EXPECT().FindVMResource(context.TODO(), uint64(123)).Return(newVMResource(), nil).Once()
	proxmoxClient.EXPECT().GetVM(context.TODO(), "node1", int64(123)).Return(newRunningVM(), nil).Once()
	proxmoxClient.EXPECT().DeleteVM(context.TODO(), "node1", int64(123)).Return(newTask, nil).Once()

	require.NoError(t, DeleteVM(context.TODO(), machineScope))
	requireDeleteInProgress(t, machineScope, "UPID:node1:new-destroy")
	require.Nil(t, machineScope.ProxmoxMachine.Status.RetryAfter)
}

func TestDeleteVM_DestroyTaskFailedButVMIDFreeRemovesFinalizer(t *testing.T) {
	machineScope, proxmoxClient := setupDeleteVMTest(t)
	machineScope.ProxmoxMachine.Status.TaskRef = ptr.To("UPID:node1:destroy")
	task := &proxmox.Task{
		UPID:        "UPID:node1:destroy",
		IsFailed:    true,
		IsCompleted: true,
		Status:      "stopped",
		ExitStatus:  "VM 103 is running - destroy failed",
		Type:        "qmdestroy",
	}

	proxmoxClient.EXPECT().GetTask(context.TODO(), "UPID:node1:destroy").Return(task, nil).Once()
	proxmoxClient.EXPECT().CheckID(context.TODO(), int64(123)).Return(true, nil).Once()

	require.NoError(t, DeleteVM(context.TODO(), machineScope))
	requireDeleteComplete(t, machineScope)
}

// TestDeleteVM_UnclassifiableTaskLookupErrorClearsStaleTaskAndReissuesDelete covers a task-lookup
// failure whose text matches none of the "not found" heuristics (e.g. a task ref naming a removed
// node) while the VM is still ours: deletion must not livelock holding the stale ref. It drops the
// stale task and re-issues the destroy, converging regardless of the error classification.
func TestDeleteVM_UnclassifiableTaskLookupErrorClearsStaleTaskAndReissuesDelete(t *testing.T) {
	machineScope, proxmoxClient := setupDeleteVMTest(t)
	machineScope.ProxmoxMachine.Status.TaskRef = ptr.To("UPID:hv99:destroy")
	newTask := &proxmox.Task{UPID: "UPID:node1:new-destroy", Type: "qmdestroy"}

	proxmoxClient.EXPECT().GetTask(context.TODO(), "UPID:hv99:destroy").Return(nil, errors.New("hostname lookup 'hv99' failed")).Once()
	proxmoxClient.EXPECT().CheckID(context.TODO(), int64(123)).Return(false, nil).Once()
	proxmoxClient.EXPECT().FindVMResource(context.TODO(), uint64(123)).Return(newVMResource(), nil).Once()
	proxmoxClient.EXPECT().GetVM(context.TODO(), "node1", int64(123)).Return(newRunningVM(), nil).Once()
	proxmoxClient.EXPECT().DeleteVM(context.TODO(), "node1", int64(123)).Return(newTask, nil).Once()

	require.NoError(t, DeleteVM(context.TODO(), machineScope))
	requireDeleteInProgress(t, machineScope, "UPID:node1:new-destroy")
}

func TestDeleteVM_TaskLookupAndCheckIDErrorsPreserveBothErrorsAndUseDeletingReason(t *testing.T) {
	machineScope, proxmoxClient := setupDeleteVMTest(t)
	machineScope.ProxmoxMachine.Status.TaskRef = ptr.To("UPID:node1:destroy")

	proxmoxClient.EXPECT().GetTask(context.TODO(), "UPID:node1:destroy").Return(nil, errors.New("501 Not Implemented")).Once()
	proxmoxClient.EXPECT().CheckID(context.TODO(), int64(123)).Return(false, errors.New("temporary checkid failure")).Once()

	err := DeleteVM(context.TODO(), machineScope)
	require.Error(t, err)
	require.ErrorContains(t, err, "501 Not Implemented")
	require.ErrorContains(t, err, "temporary checkid failure")
	requireDeleteInProgress(t, machineScope, "UPID:node1:destroy")

	cond := conditions.Get(machineScope.ProxmoxMachine, infrav1.ProxmoxMachineVirtualMachineProvisionedCondition)
	require.NotNil(t, cond)
	require.Equal(t, metav1.ConditionFalse, cond.Status)
	require.Equal(t, infrav1.ProxmoxMachineVirtualMachineProvisionedDeletingReason, cond.Reason)
	require.Contains(t, cond.Message, "deletion task lookup")
	require.Contains(t, cond.Message, "501 Not Implemented")
	require.Contains(t, cond.Message, "temporary checkid failure")
}

func TestDeleteVM_MissingDeletionTaskAndVMIDFreeRemovesFinalizer(t *testing.T) {
	machineScope, proxmoxClient := setupDeleteVMTest(t)
	machineScope.ProxmoxMachine.Status.TaskRef = ptr.To("UPID:node1:destroy")

	proxmoxClient.EXPECT().GetTask(context.TODO(), "UPID:node1:destroy").Return(nil, errors.New("task expired")).Once()
	proxmoxClient.EXPECT().CheckID(context.TODO(), int64(123)).Return(true, nil).Once()

	require.NoError(t, DeleteVM(context.TODO(), machineScope))
	requireDeleteComplete(t, machineScope)
}

func TestDeleteVM_MissingDeletionTaskAndVMIDStillAllocatedStartsNewDelete(t *testing.T) {
	machineScope, proxmoxClient := setupDeleteVMTest(t)
	machineScope.ProxmoxMachine.Status.TaskRef = ptr.To("UPID:node1:destroy")
	destroyTask := &proxmox.Task{UPID: "UPID:node1:new-destroy", Type: "qmdestroy"}

	proxmoxClient.EXPECT().GetTask(context.TODO(), "UPID:node1:destroy").Return(nil, errors.New("task expired")).Once()
	proxmoxClient.EXPECT().CheckID(context.TODO(), int64(123)).Return(false, nil).Once()
	proxmoxClient.EXPECT().FindVMResource(context.TODO(), uint64(123)).Return(newVMResource(), nil).Once()
	proxmoxClient.EXPECT().GetVM(context.TODO(), "node1", int64(123)).Return(newRunningVM(), nil).Once()
	proxmoxClient.EXPECT().DeleteVM(context.TODO(), "node1", int64(123)).Return(destroyTask, nil).Once()

	require.NoError(t, DeleteVM(context.TODO(), machineScope))
	requireDeleteInProgress(t, machineScope, "UPID:node1:new-destroy")

	cond := conditions.Get(machineScope.ProxmoxMachine, infrav1.ProxmoxMachineVirtualMachineProvisionedCondition)
	require.NotNil(t, cond)
	require.Equal(t, metav1.ConditionFalse, cond.Status)
	require.Equal(t, infrav1.ProxmoxMachineVirtualMachineProvisionedDeletingReason, cond.Reason)
	require.Contains(t, cond.Message, "waiting for qmdestroy task")
}

func TestDeleteVM_NonDeletionTaskRefDoesNotBlockDeletion(t *testing.T) {
	machineScope, proxmoxClient := setupDeleteVMTest(t)
	machineScope.ProxmoxMachine.Status.TaskRef = ptr.To("UPID:node1:start")
	startTask := &proxmox.Task{UPID: "UPID:node1:start", IsFailed: true, IsCompleted: true, Status: "stopped", ExitStatus: "ERROR: VM already running", Type: "qmstart"}
	destroyTask := &proxmox.Task{UPID: "UPID:node1:destroy", Type: "qmdestroy"}

	proxmoxClient.EXPECT().GetTask(context.TODO(), "UPID:node1:start").Return(startTask, nil).Once()
	proxmoxClient.EXPECT().GetVM(context.TODO(), "node1", int64(123)).Return(newRunningVM(), nil).Once()
	proxmoxClient.EXPECT().DeleteVM(context.TODO(), "node1", int64(123)).Return(destroyTask, nil).Once()

	require.NoError(t, DeleteVM(context.TODO(), machineScope))
	requireDeleteInProgress(t, machineScope, "UPID:node1:destroy")
}

func TestDeleteVM_StopTaskSucceededStartsDestroyTask(t *testing.T) {
	machineScope, proxmoxClient := setupDeleteVMTest(t)
	machineScope.ProxmoxMachine.Status.TaskRef = ptr.To("UPID:node1:stop")
	stopTask := &proxmox.Task{
		UPID:         "UPID:node1:stop",
		IsCompleted:  true,
		IsSuccessful: true,
		Status:       "stopped",
		ExitStatus:   "OK",
		Type:         "qmstop",
	}
	destroyTask := &proxmox.Task{UPID: "UPID:node1:destroy", Type: "qmdestroy"}

	proxmoxClient.EXPECT().GetTask(context.TODO(), "UPID:node1:stop").Return(stopTask, nil).Once()
	proxmoxClient.EXPECT().GetVM(context.TODO(), "node1", int64(123)).Return(newRunningVM(), nil).Once()
	proxmoxClient.EXPECT().DeleteVM(context.TODO(), "node1", int64(123)).Return(destroyTask, nil).Once()

	require.NoError(t, DeleteVM(context.TODO(), machineScope))
	requireDeleteInProgress(t, machineScope, "UPID:node1:destroy")
}

func TestDeleteVM_DestroyTaskSucceededCompletesWithoutCheckingVMID(t *testing.T) {
	machineScope, proxmoxClient := setupDeleteVMTest(t)
	machineScope.ProxmoxMachine.Status.TaskRef = ptr.To("UPID:node1:destroy")
	task := &proxmox.Task{
		UPID:         "UPID:node1:destroy",
		IsCompleted:  true,
		IsSuccessful: true,
		Status:       "stopped",
		ExitStatus:   "OK",
		Type:         "qmdestroy",
	}

	proxmoxClient.EXPECT().GetTask(context.TODO(), "UPID:node1:destroy").Return(task, nil).Once()

	require.NoError(t, DeleteVM(context.TODO(), machineScope))
	requireDeleteComplete(t, machineScope)
}

func setupDeleteVMTest(t *testing.T) (*scope.MachineScope, *proxmoxtest.MockClient) {
	t.Helper()

	machineScope, proxmoxClient, _ := setupReconcilerTest(t)
	vm := newRunningVM()
	machineScope.ProxmoxMachine.Spec.VirtualMachineID = new(int64(vm.VMID))
	machineScope.InfraCluster.ProxmoxCluster.AddNodeLocation(infrav1.NodeLocation{
		Machine: corev1.LocalObjectReference{Name: machineScope.Name()},
		Node:    "node1",
	}, false)

	return machineScope, proxmoxClient
}

func requireDeleteInProgress(t *testing.T, machineScope *scope.MachineScope, expectedTaskRef string) {
	t.Helper()

	require.Contains(t, machineScope.ProxmoxMachine.Finalizers, infrav1.MachineFinalizer)
	require.Equal(t, "node1", machineScope.InfraCluster.ProxmoxCluster.GetNode(machineScope.Name(), false))
	require.NotNil(t, machineScope.ProxmoxMachine.Status.TaskRef)
	require.Equal(t, expectedTaskRef, *machineScope.ProxmoxMachine.Status.TaskRef)
}

func requireDeleteComplete(t *testing.T, machineScope *scope.MachineScope) {
	t.Helper()

	require.Empty(t, machineScope.ProxmoxMachine.Finalizers)
	require.Empty(t, machineScope.InfraCluster.ProxmoxCluster.GetNode(machineScope.Name(), false))
	require.Nil(t, machineScope.ProxmoxMachine.Status.TaskRef)
	require.Nil(t, machineScope.ProxmoxMachine.Status.RetryAfter)
}

func requireDeleteBlockedOnVMID(t *testing.T, machineScope *scope.MachineScope) {
	t.Helper()

	require.Contains(t, machineScope.ProxmoxMachine.Finalizers, infrav1.MachineFinalizer)
	require.Equal(t, "node1", machineScope.InfraCluster.ProxmoxCluster.GetNode(machineScope.Name(), false))
	require.Nil(t, machineScope.ProxmoxMachine.Status.TaskRef)
	require.Nil(t, machineScope.ProxmoxMachine.Status.RetryAfter)
}
