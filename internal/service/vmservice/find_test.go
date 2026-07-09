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

// TestFindVM_NotInitializedPlaceholderName covers the real-hardware behavior that the empty-name
// sentinel missed: Proxmox reports a freshly-cloned, not-yet-renamed VM with the placeholder
// name "VM <vmid>", not "". That is the machine's own in-flight clone, so it must be reported as
// not-initialized (wait) rather than a collision - otherwise the id is released and re-rolled,
// orphaning the clone.
func TestFindVM_NotInitializedPlaceholderName(t *testing.T) {
	ctx := context.TODO()
	machineScope, proxmoxClient, _ := setupReconcilerTest(t)
	vm := newRunningVM()
	vm.Name = placeholderVMName(int64(vm.VMID))
	machineScope.ProxmoxMachine.Spec.VirtualMachineID = ptr.To(int64(vm.VMID))
	machineScope.ProxmoxMachine.Status.ProxmoxNode = ptr.To("node2")

	proxmoxClient.EXPECT().GetVM(ctx, "node2", int64(123)).Return(vm, nil).Once()

	_, err := FindVM(ctx, machineScope)
	require.ErrorIs(t, err, ErrVMNotInitialized)
}

// TestFindVM_AdoptedMatchesByUUIDDespiteRename covers an already-adopted machine (providerID set)
// whose VM was renamed out of band: identity is the BIOS UUID, not the name, so a rename must not
// be mistaken for a collision (which would destructively re-roll and orphan the VM).
func TestFindVM_AdoptedMatchesByUUIDDespiteRename(t *testing.T) {
	ctx := context.TODO()
	machineScope, proxmoxClient, _ := setupReconcilerTest(t)
	const vmUUID = "56603c36-46b9-4608-90ae-c731c15eae64"
	vm := newRunningVM()
	vm.Name = "renamed-out-of-band"
	vm.VirtualMachineConfig.SMBios1 = "uuid=" + vmUUID
	machineScope.ProxmoxMachine.Spec.VirtualMachineID = ptr.To(int64(vm.VMID))
	machineScope.ProxmoxMachine.Status.ProxmoxNode = ptr.To("node2")
	machineScope.ProxmoxMachine.Spec.ProviderID = "proxmox://" + vmUUID

	proxmoxClient.EXPECT().GetVM(ctx, "node2", int64(123)).Return(vm, nil).Once()

	got, err := FindVM(ctx, machineScope)
	require.NoError(t, err)
	require.Equal(t, vm, got)
}

// TestFindVM_AdoptedCollisionWhenUUIDDiffers covers an adopted machine whose id now resolves to a
// VM with a different UUID, even though its name happens to match this machine: UUID identity wins,
// so it is reported as a collision.
func TestFindVM_AdoptedCollisionWhenUUIDDiffers(t *testing.T) {
	ctx := context.TODO()
	machineScope, proxmoxClient, _ := setupReconcilerTest(t)
	vm := newRunningVM() // name "test" matches the machine, but a different UUID must win
	vm.VirtualMachineConfig.SMBios1 = "uuid=00000000-0000-0000-0000-000000000000"
	machineScope.ProxmoxMachine.Spec.VirtualMachineID = ptr.To(int64(vm.VMID))
	machineScope.ProxmoxMachine.Status.ProxmoxNode = ptr.To("node2")
	machineScope.ProxmoxMachine.Spec.ProviderID = "proxmox://56603c36-46b9-4608-90ae-c731c15eae64"

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

// TestUpdateVMLocation_PlaceholderName covers the same placeholder-name case on the cluster-wide
// relocation path: a VM still carrying Proxmox's "VM <vmid>" placeholder is this machine's clone
// initializing, so updateVMLocation must requeue (plain error) rather than report a collision.
func TestUpdateVMLocation_PlaceholderName(t *testing.T) {
	ctx := context.TODO()
	machineScope, proxmoxClient, _ := setupReconcilerTest(t)
	vm := newRunningVM()
	vmr := newVMResource()
	vmr.Name = placeholderVMName(int64(vm.VMID))
	vm.VirtualMachineConfig.Name = placeholderVMName(int64(vm.VMID))
	machineScope.ProxmoxMachine.Spec.VirtualMachineID = ptr.To(int64(vm.VMID))
	machineScope.ProxmoxMachine.Status.ProxmoxNode = ptr.To("node1")

	proxmoxClient.EXPECT().FindVMResource(ctx, uint64(123)).Return(vmr, nil).Once()
	proxmoxClient.EXPECT().GetVM(ctx, "node1", int64(123)).Return(vm, nil).Once()

	err := updateVMLocation(ctx, machineScope)
	require.Error(t, err)
	require.NotErrorIs(t, err, ErrVMIDCollision)
	require.False(t, machineScope.HasFailed())
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

// TestHandleVMIDCollision_Recovers covers a controller-allocated, not-yet-adopted id (empty
// providerID, VMIDAllocatedByControllerAnnotation set at allocation): the collision is a benign
// allocation race and must self-heal - release the id and stale node location, drop the
// annotation, reset to Cloning (non-terminal), and requeue so a fresh id is selected.
func TestHandleVMIDCollision_Recovers(t *testing.T) {
	machineScope, _, _ := setupReconcilerTest(t)
	machineScope.ProxmoxMachine.Spec.VirtualMachineID = ptr.To(int64(123))
	machineScope.ProxmoxMachine.Status.ProxmoxNode = ptr.To("node1")
	machineScope.SetAnnotation(infrav1.VMIDAllocatedByControllerAnnotation, "true")
	machineScope.InfraCluster.ProxmoxCluster.AddNodeLocation(infrav1.NodeLocation{
		Machine: corev1.LocalObjectReference{Name: machineScope.Name()},
		Node:    "node1",
	}, false)

	err := handleVMIDCollision(machineScope, ErrVMIDCollision)

	// Transient error so the controller requeues, but not a terminal failure.
	require.ErrorIs(t, err, ErrVMIDCollision)
	require.False(t, machineScope.HasFailed())
	requireConditionIsFalse(t, machineScope.ProxmoxMachine, infrav1.ProxmoxMachineVirtualMachineProvisionedCondition)
	// The colliding id, its provenance annotation, and the stale node location are released.
	require.Equal(t, int64(-1), machineScope.GetVirtualMachineID())
	require.Nil(t, machineScope.ProxmoxMachine.Status.ProxmoxNode)
	require.NotContains(t, machineScope.ProxmoxMachine.Annotations, infrav1.VMIDAllocatedByControllerAnnotation)
	require.False(t, machineScope.InfraCluster.ProxmoxCluster.HasMachine(machineScope.Name(), false))
}

// TestHandleVMIDCollision_RequeuesWhenAdopted covers a machine that had already adopted a VM
// (providerID set) whose name no longer matches. It must not release the id and must not latch a
// terminal VMProvisionFailed - a terminal failure could let MachineHealthCheck delete the VM by
// id - so it requeues non-terminally with the id retained.
func TestHandleVMIDCollision_RequeuesWhenAdopted(t *testing.T) {
	machineScope, _, _ := setupReconcilerTest(t)
	machineScope.ProxmoxMachine.Spec.VirtualMachineID = ptr.To(int64(123))
	machineScope.ProxmoxMachine.Status.ProxmoxNode = ptr.To("node1")
	machineScope.SetAnnotation(infrav1.VMIDAllocatedByControllerAnnotation, "true")
	machineScope.ProxmoxMachine.Spec.ProviderID = "proxmox://some-uuid"

	err := handleVMIDCollision(machineScope, ErrVMIDCollision)
	require.ErrorIs(t, err, ErrVMIDCollision)
	require.False(t, machineScope.HasFailed())
	require.Equal(t, int64(123), machineScope.GetVirtualMachineID())
}

// TestHandleVMIDCollision_RequeuesWhenPinned covers an operator-pinned id: no allocation
// annotation, even though a node is recorded (updateVMLocation can set Status.ProxmoxNode for a
// located pinned id). Releasing it would violate operator intent and going terminal could let MHC
// delete a VM the machine does not own, so it requeues non-terminally with the id retained.
func TestHandleVMIDCollision_RequeuesWhenPinned(t *testing.T) {
	machineScope, _, _ := setupReconcilerTest(t)
	machineScope.ProxmoxMachine.Spec.VirtualMachineID = ptr.To(int64(123))
	machineScope.ProxmoxMachine.Status.ProxmoxNode = ptr.To("node1")

	err := handleVMIDCollision(machineScope, ErrVMIDCollision)
	require.ErrorIs(t, err, ErrVMIDCollision)
	require.False(t, machineScope.HasFailed())
	require.Equal(t, int64(123), machineScope.GetVirtualMachineID())
}
