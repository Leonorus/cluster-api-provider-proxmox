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

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/utils/ptr"

	infrav1 "github.com/ionos-cloud/cluster-api-provider-proxmox/api/v1alpha2"
)

func TestFindVM_FindByNodeAndID(t *testing.T) {
	ctx := context.TODO()
	machineScope, proxmoxClient, _ := setupReconcilerTest(t)
	vm := newRunningVM()
	machineScope.ProxmoxMachine.Spec.VirtualMachineID = new(int64(vm.VMID))

	proxmoxClient.EXPECT().GetVM(ctx, "node1", int64(123)).Return(vm, nil).Once()

	_, err := FindVM(ctx, machineScope)
	require.NoError(t, err)
}

func TestFindVM_FindByNodeLocationsAndID(t *testing.T) {
	ctx := context.TODO()
	machineScope, proxmoxClient, _ := setupReconcilerTest(t)
	vm := newRunningVM()
	machineScope.ProxmoxMachine.Spec.VirtualMachineID = new(int64(vm.VMID))
	machineScope.InfraCluster.ProxmoxCluster.AddNodeLocation(infrav1.NodeLocation{
		Machine: corev1.LocalObjectReference{Name: machineScope.ProxmoxMachine.GetName()},
		Node:    "node3",
	}, false)

	proxmoxClient.EXPECT().GetVM(ctx, "node3", int64(123)).Return(vm, nil).Once()

	_, err := FindVM(ctx, machineScope)
	require.NoError(t, err)
}

func TestFindVM_NotFound(t *testing.T) {
	ctx := context.TODO()
	machineScope, proxmoxClient, _ := setupReconcilerTest(t)
	vm := newRunningVM()
	machineScope.ProxmoxMachine.Spec.VirtualMachineID = new(int64(vm.VMID))
	machineScope.ProxmoxMachine.Status.ProxmoxNode = new("node2")

	proxmoxClient.EXPECT().GetVM(ctx, "node2", int64(123)).Return(nil, errors.New("error")).Once()

	_, err := FindVM(ctx, machineScope)
	require.ErrorIs(t, err, ErrVMNotFound)
}

func TestFindVM_NotCreated(t *testing.T) {
	machineScope, _, _ := setupReconcilerTest(t)

	_, err := FindVM(context.TODO(), machineScope)
	require.ErrorIs(t, err, ErrVMNotCreated)
}

func TestFindVM_NotInitialized(t *testing.T) {
	ctx := context.TODO()
	machineScope, proxmoxClient, _ := setupReconcilerTest(t)
	vm := newRunningVM()
	vm.Name = ""
	machineScope.ProxmoxMachine.Spec.VirtualMachineID = ptr.To(int64(vm.VMID))
	machineScope.ProxmoxMachine.Status.ProxmoxNode = ptr.To("node2")

	proxmoxClient.EXPECT().GetVM(ctx, "node2", int64(123)).Return(vm, nil).Once()

	_, err := FindVM(ctx, machineScope)
	require.ErrorIs(t, err, ErrVMNotInitialized)
}

// TestFindVM_VMIDCollision covers the id resolving to a VM with a different (non-empty) name:
// a VMID collision must be reported as such, not conflated with "not initialized yet".
func TestFindVM_VMIDCollision(t *testing.T) {
	ctx := context.TODO()
	machineScope, proxmoxClient, _ := setupReconcilerTest(t)
	vm := newRunningVM()
	vm.Name = "bar"
	machineScope.ProxmoxMachine.Spec.VirtualMachineID = ptr.To(int64(vm.VMID))
	machineScope.ProxmoxMachine.Status.ProxmoxNode = ptr.To("node2")

	proxmoxClient.EXPECT().GetVM(ctx, "node2", int64(123)).Return(vm, nil).Once()

	_, err := FindVM(ctx, machineScope)
	require.ErrorIs(t, err, ErrVMIDCollision)
}

func TestUpdateVMLocation_MissingName(t *testing.T) {
	ctx := context.TODO()
	machineScope, proxmoxClient, _ := setupReconcilerTest(t)
	vm := newRunningVM()
	vmr := newVMResource()
	vmr.Name = ""
	vm.VirtualMachineConfig.Name = ""
	machineScope.ProxmoxMachine.Spec.VirtualMachineID = new(int64(vm.VMID))

	proxmoxClient.EXPECT().FindVMResource(ctx, uint64(123)).Return(vmr, nil).Once()
	proxmoxClient.EXPECT().GetVM(ctx, "node1", int64(123)).Return(vm, nil).Once()

	require.Error(t, updateVMLocation(ctx, machineScope))
}

// TestUpdateVMLocation_Collision covers updateVMLocation locating the id cluster-wide on a VM
// with a foreign name: it is a pure detector and must report ErrVMIDCollision without mutating
// state (the recover/fail decision lives in handleVMIDCollision).
func TestUpdateVMLocation_Collision(t *testing.T) {
	ctx := context.TODO()
	machineScope, proxmoxClient, _ := setupReconcilerTest(t)
	vm := newRunningVM()
	vmr := newVMResource()
	name := "foo"
	vmr.Name = name
	vm.VirtualMachineConfig.Name = name
	machineScope.ProxmoxMachine.Spec.VirtualMachineID = new(int64(vm.VMID))
	machineScope.ProxmoxMachine.Status.ProxmoxNode = new("node1")

	proxmoxClient.EXPECT().FindVMResource(ctx, uint64(123)).Return(vmr, nil).Once()
	proxmoxClient.EXPECT().GetVM(ctx, "node1", int64(123)).Return(vm, nil).Once()

	err := updateVMLocation(ctx, machineScope)
	require.ErrorIs(t, err, ErrVMIDCollision)
	// Detector only: no state mutation, no terminal failure.
	require.False(t, machineScope.HasFailed())
	require.Equal(t, int64(123), machineScope.GetVirtualMachineID())
}

func TestUpdateVMLocation_UpdateNode(t *testing.T) {
	ctx := context.TODO()
	machineScope, proxmoxClient, _ := setupReconcilerTest(t)
	vm := newRunningVM()
	vmr := newVMResource()
	machineScope.ProxmoxMachine.Spec.VirtualMachineID = new(int64(vm.VMID))
	machineScope.ProxmoxMachine.Status.ProxmoxNode = new("node3")
	machineScope.InfraCluster.ProxmoxCluster.AddNodeLocation(infrav1.NodeLocation{
		Machine: corev1.LocalObjectReference{Name: machineScope.Name()},
		Node:    "node3",
	}, false)

	proxmoxClient.EXPECT().FindVMResource(ctx, uint64(123)).Return(vmr, nil).Once()
	proxmoxClient.EXPECT().GetVM(ctx, "node1", int64(123)).Return(vm, nil).Once()

	require.NoError(t, updateVMLocation(ctx, machineScope))
	require.Equal(t, vmr.Node, *machineScope.ProxmoxMachine.Status.ProxmoxNode)
	require.Equal(t, vmr.Node, machineScope.InfraCluster.ProxmoxCluster.GetNode(machineScope.Name(), false))
}

func TestUpdateVMLocation_WithTask(t *testing.T) {
	machineScope, _, _ := setupReconcilerTest(t)
	vm := newRunningVM()

	machineScope.ProxmoxMachine.Spec.VirtualMachineID = new(int64(vm.VMID))
	machineScope.ProxmoxMachine.Status.TaskRef = new("test-task-uupid")

	require.Error(t, updateVMLocation(context.TODO(), machineScope))
}

// TestHandleVMIDCollision_Recovers covers a controller-selected, not-yet-adopted id (empty
// providerID, ProxmoxNode recorded by createVM): the collision is a benign allocation race and
// must self-heal - release the id and stale node location, reset to Cloning (non-terminal), and
// requeue so a fresh id is selected.
func TestHandleVMIDCollision_Recovers(t *testing.T) {
	machineScope, _, _ := setupReconcilerTest(t)
	machineScope.ProxmoxMachine.Spec.VirtualMachineID = ptr.To(int64(123))
	machineScope.ProxmoxMachine.Status.ProxmoxNode = ptr.To("node1")
	machineScope.InfraCluster.ProxmoxCluster.AddNodeLocation(infrav1.NodeLocation{
		Machine: corev1.LocalObjectReference{Name: machineScope.Name()},
		Node:    "node1",
	}, false)

	err := handleVMIDCollision(machineScope, ErrVMIDCollision)

	// Transient error so the controller requeues, but not a terminal failure.
	require.ErrorIs(t, err, ErrVMIDCollision)
	require.False(t, machineScope.HasFailed())
	requireConditionIsFalse(t, machineScope.ProxmoxMachine, infrav1.ProxmoxMachineVirtualMachineProvisionedCondition)
	// The colliding id and stale node location are released for re-selection.
	require.Equal(t, int64(-1), machineScope.GetVirtualMachineID())
	require.Nil(t, machineScope.ProxmoxMachine.Status.ProxmoxNode)
	require.False(t, machineScope.InfraCluster.ProxmoxCluster.HasMachine(machineScope.Name(), false))
}

// TestHandleVMIDCollision_TerminalWhenAdopted covers a machine that had already adopted a VM
// (providerID set) whose name no longer matches. This is not a benign race, so it stays a
// terminal failure (anti-adoption guard) and the id must NOT be released.
func TestHandleVMIDCollision_TerminalWhenAdopted(t *testing.T) {
	machineScope, _, _ := setupReconcilerTest(t)
	machineScope.ProxmoxMachine.Spec.VirtualMachineID = ptr.To(int64(123))
	machineScope.ProxmoxMachine.Status.ProxmoxNode = ptr.To("node1")
	machineScope.ProxmoxMachine.Spec.ProviderID = "proxmox://some-uuid"

	require.Error(t, handleVMIDCollision(machineScope, ErrVMIDCollision))
	require.True(t, machineScope.HasFailed())
	require.Equal(t, int64(123), machineScope.GetVirtualMachineID())
}

// TestHandleVMIDCollision_TerminalWhenPinned covers an id that the controller never selected
// (no ProxmoxNode recorded, i.e. operator-pinned virtualMachineID). Clobbering it would violate
// operator intent, so it stays a terminal failure and the id is retained.
func TestHandleVMIDCollision_TerminalWhenPinned(t *testing.T) {
	machineScope, _, _ := setupReconcilerTest(t)
	machineScope.ProxmoxMachine.Spec.VirtualMachineID = ptr.To(int64(123))
	machineScope.ProxmoxMachine.Status.ProxmoxNode = nil

	require.Error(t, handleVMIDCollision(machineScope, ErrVMIDCollision))
	require.True(t, machineScope.HasFailed())
	require.Equal(t, int64(123), machineScope.GetVirtualMachineID())
}
