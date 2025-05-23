/*
 Licensed to the Apache Software Foundation (ASF) under one
 or more contributor license agreements.  See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership.  The ASF licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
*/

package cache

import (
	"testing"
	"time"

	"gotest.tools/v3/assert"
	v1 "k8s.io/api/core/v1"
	schedulingv1 "k8s.io/api/scheduling/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/apache/yunikorn-k8shim/pkg/client"
	"github.com/apache/yunikorn-k8shim/pkg/common/constants"
	"github.com/apache/yunikorn-k8shim/pkg/common/events"
	"github.com/apache/yunikorn-k8shim/pkg/common/utils"
	"github.com/apache/yunikorn-k8shim/pkg/locking"
	"github.com/apache/yunikorn-scheduler-interface/lib/go/si"
)

func TestTaskStateTransitions(t *testing.T) {
	mockedSchedulerApi := newMockSchedulerAPI()
	mockedContext := initContextForTest()
	resources := make(map[v1.ResourceName]resource.Quantity)
	containers := make([]v1.Container, 0)
	containers = append(containers, v1.Container{
		Name: "container-01",
		Resources: v1.ResourceRequirements{
			Requests: resources,
		},
	})
	pod := &v1.Pod{
		TypeMeta: metav1.TypeMeta{
			Kind:       "Pod",
			APIVersion: "v1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name: "pod-resource-test-00001",
			UID:  "UID-00001",
		},
		Spec: v1.PodSpec{
			Containers: containers,
		},
	}

	app := NewApplication("app01", "root.default",
		"bob", testGroups, map[string]string{}, mockedSchedulerApi)
	task := NewTask("task01", app, mockedContext, pod)
	assert.Equal(t, task.GetTaskState(), TaskStates().New)

	// new task
	event0 := NewSimpleTaskEvent(task.applicationID, task.taskID, InitTask)
	err := task.handle(event0)
	assert.NilError(t, err, "failed to handle InitTask event")
	assert.Equal(t, task.GetTaskState(), TaskStates().Pending)

	// submit task to the scheduler-core
	event1 := NewSubmitTaskEvent(app.applicationID, task.taskID)
	err = task.handle(event1)
	assert.NilError(t, err, "failed to handle SubmitTask event")
	assert.Equal(t, task.GetTaskState(), TaskStates().Scheduling)

	// allocated
	event2 := NewAllocateTaskEvent(app.applicationID, task.taskID, string(pod.UID), "node-1")
	err = task.handle(event2)
	assert.NilError(t, err, "failed to handle AllocateTask event")
	assert.Equal(t, task.GetTaskState(), TaskStates().Allocated)

	// bound
	event3 := NewBindTaskEvent(app.applicationID, task.taskID)
	err = task.handle(event3)
	assert.NilError(t, err, "failed to handle BindTask event")
	assert.Equal(t, task.GetTaskState(), TaskStates().Bound)

	// complete
	event4 := NewSimpleTaskEvent(app.applicationID, task.taskID, CompleteTask)
	err = task.handle(event4)
	assert.NilError(t, err, "failed to handle CompleteTask event")
	assert.Equal(t, task.GetTaskState(), TaskStates().Completed)
}

func TestTaskIllegalEventHandling(t *testing.T) {
	mockedSchedulerApi := newMockSchedulerAPI()
	mockedContext := initContextForTest()
	resources := make(map[v1.ResourceName]resource.Quantity)
	containers := make([]v1.Container, 0)
	containers = append(containers, v1.Container{
		Name: "container-01",
		Resources: v1.ResourceRequirements{
			Requests: resources,
		},
	})
	pod := &v1.Pod{
		TypeMeta: metav1.TypeMeta{
			Kind:       "Pod",
			APIVersion: "v1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name: "pod-resource-test-00001",
			UID:  "UID-00001",
		},
		Spec: v1.PodSpec{
			Containers: containers,
		},
	}

	app := NewApplication("app01", "root.default",
		"bob", testGroups, map[string]string{}, mockedSchedulerApi)
	task := NewTask("task01", app, mockedContext, pod)
	assert.Equal(t, task.GetTaskState(), TaskStates().New)

	// new task
	event0 := NewSimpleTaskEvent(task.applicationID, task.taskID, InitTask)
	err := task.handle(event0)
	assert.NilError(t, err, "failed to handle InitTask event")
	assert.Equal(t, task.GetTaskState(), TaskStates().Pending)

	// verify illegal event handling logic
	event2 := NewAllocateTaskEvent(app.applicationID, task.taskID, string(pod.UID), "node-1")
	err = task.handle(event2)
	if err == nil {
		t.Fatal("expecting an error, event AllocateTask is illegal when task is Pending")
	}

	// task state should not have changed
	assert.Equal(t, task.GetTaskState(), TaskStates().Pending)
}

//nolint:funlen
func TestReleaseTaskAllocation(t *testing.T) {
	mockedSchedulerApi := newMockSchedulerAPI()
	mockedContext := initContextForTest()
	apiProvider := mockedContext.apiProvider
	mockedApiProvider, ok := apiProvider.(*client.MockedAPIProvider)
	assert.Assert(t, ok, "expecting MockedAPIProvider")

	resources := make(map[v1.ResourceName]resource.Quantity)
	containers := make([]v1.Container, 0)
	containers = append(containers, v1.Container{
		Name: "container-01",
		Resources: v1.ResourceRequirements{
			Requests: resources,
		},
	})
	pod := &v1.Pod{
		TypeMeta: metav1.TypeMeta{
			Kind:       "Pod",
			APIVersion: "v1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name: "pod-resource-test-00001",
			UID:  "task01",
		},
		Spec: v1.PodSpec{
			Containers: containers,
		},
	}

	app := NewApplication("app01", "root.default",
		"bob", testGroups, map[string]string{}, mockedSchedulerApi)
	task := NewTask("task01", app, mockedContext, pod)
	assert.Equal(t, task.GetTaskState(), TaskStates().New)

	// new task
	event0 := NewSimpleTaskEvent(task.applicationID, task.taskID, InitTask)
	err := task.handle(event0)
	assert.NilError(t, err, "failed to handle InitTask event")
	assert.Equal(t, task.GetTaskState(), TaskStates().Pending)

	// submit task to the scheduler-core
	event1 := NewSubmitTaskEvent(app.applicationID, task.taskID)
	err = task.handle(event1)
	assert.NilError(t, err, "failed to handle SubmitTask event")
	assert.Equal(t, task.GetTaskState(), TaskStates().Scheduling)

	// allocated
	event2 := NewAllocateTaskEvent(app.applicationID, task.taskID, string(pod.UID), "node-1")
	err = task.handle(event2)
	assert.NilError(t, err, "failed to handle AllocateTask event")
	assert.Equal(t, task.GetTaskState(), TaskStates().Allocated)
	// bind a task is a async process, wait for it to happen
	err = utils.WaitForCondition(
		func() bool {
			return task.GetNodeName() == "node-1"
		},
		100*time.Millisecond,
		3*time.Second,
	)
	assert.NilError(t, err, "failed to wait for allocation allocationKey being set for task")

	// bound
	event3 := NewBindTaskEvent(app.applicationID, task.taskID)
	err = task.handle(event3)
	assert.NilError(t, err, "failed to handle BindTask event")
	assert.Equal(t, task.GetTaskState(), TaskStates().Bound)

	// the mocked update function does nothing than verify the coming messages
	// this is to verify we are sending correct info to the scheduler core
	mockedApiProvider.MockSchedulerAPIUpdateAllocationFn(func(request *si.AllocationRequest) error {
		assert.Assert(t, request.Releases != nil)
		assert.Assert(t, request.Releases.AllocationsToRelease != nil)
		assert.Equal(t, request.Releases.AllocationsToRelease[0].ApplicationID, app.applicationID)
		assert.Equal(t, request.Releases.AllocationsToRelease[0].PartitionName, "default")
		assert.Equal(t, request.Releases.AllocationsToRelease[0].AllocationKey, "task01")
		return nil
	})

	// complete
	event4 := NewSimpleTaskEvent(app.applicationID, task.taskID, CompleteTask)
	err = task.handle(event4)
	assert.NilError(t, err, "failed to handle CompleteTask event")
	assert.Equal(t, task.GetTaskState(), TaskStates().Completed)
	// 2 updates call, 1 for submit, 1 for release
	assert.Equal(t, mockedApiProvider.GetSchedulerAPIUpdateAllocationCount(), int32(2))

	// New to Failed, no AllocationKey is set (only ask is released)
	task = NewTask("task01", app, mockedContext, pod)
	mockedApiProvider.MockSchedulerAPIUpdateAllocationFn(func(request *si.AllocationRequest) error {
		assert.Assert(t, request.Releases != nil)
		assert.Assert(t, request.Releases.AllocationsToRelease != nil)
		assert.Equal(t, request.Releases.AllocationsToRelease[0].ApplicationID, app.applicationID)
		assert.Equal(t, request.Releases.AllocationsToRelease[0].AllocationKey, "task01")
		assert.Equal(t, request.Releases.AllocationsToRelease[0].PartitionName, "default")
		assert.Equal(t, request.Releases.AllocationsToRelease[0].TerminationType, si.TerminationType_STOPPED_BY_RM)
		return nil
	})
	err = task.handle(NewFailTaskEvent(app.applicationID, "task01", "test failure"))
	assert.NilError(t, err, "failed to handle FailTask event")

	// Scheduling to Failed, AllocationKey is set (ask+allocation are both released)
	task = NewTask("task01", app, mockedContext, pod)
	task.setAllocationKey("task01")
	task.sm.SetState(TaskStates().Scheduling)
	mockedApiProvider.MockSchedulerAPIUpdateAllocationFn(func(request *si.AllocationRequest) error {
		assert.Assert(t, request.Releases != nil)
		assert.Assert(t, request.Releases.AllocationsToRelease != nil)
		assert.Equal(t, request.Releases.AllocationsToRelease[0].ApplicationID, app.applicationID)
		assert.Equal(t, request.Releases.AllocationsToRelease[0].PartitionName, "default")
		assert.Equal(t, request.Releases.AllocationsToRelease[0].AllocationKey, "task01")
		assert.Equal(t, request.Releases.AllocationsToRelease[0].TerminationType, si.TerminationType_STOPPED_BY_RM)
		return nil
	})
	err = task.handle(NewFailTaskEvent(app.applicationID, "task01", "test failure"))
	assert.NilError(t, err, "failed to handle FailTask event")
}

func TestReleaseTaskAsk(t *testing.T) {
	mockedSchedulerApi := newMockSchedulerAPI()
	mockedContext := initContextForTest()
	apiProvider := mockedContext.apiProvider
	mockedApiProvider, ok := apiProvider.(*client.MockedAPIProvider)
	if !ok {
		t.Fatal("expecting MockedAPIProvider")
	}

	resources := make(map[v1.ResourceName]resource.Quantity)
	containers := make([]v1.Container, 0)
	containers = append(containers, v1.Container{
		Name: "container-01",
		Resources: v1.ResourceRequirements{
			Requests: resources,
		},
	})
	pod := &v1.Pod{
		TypeMeta: metav1.TypeMeta{
			Kind:       "Pod",
			APIVersion: "v1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name: "pod-resource-test-00001",
			UID:  "UID-00001",
		},
		Spec: v1.PodSpec{
			Containers: containers,
		},
	}

	app := NewApplication("app01", "root.default",
		"bob", testGroups, map[string]string{}, mockedSchedulerApi)
	task := NewTask("task01", app, mockedContext, pod)
	assert.Equal(t, task.GetTaskState(), TaskStates().New)

	// new task
	event0 := NewSimpleTaskEvent(task.applicationID, task.taskID, InitTask)
	err := task.handle(event0)
	assert.NilError(t, err, "failed to handle InitTask event")

	assert.Equal(t, task.GetTaskState(), TaskStates().Pending)

	// submit task to the scheduler-core
	// the task will be at scheduling state from this point on
	event1 := NewSubmitTaskEvent(app.applicationID, task.taskID)
	err = task.handle(event1)
	assert.NilError(t, err, "failed to handle SubmitTask event")
	assert.Equal(t, task.GetTaskState(), TaskStates().Scheduling)

	// the mocked update function does nothing than verify the coming messages
	// this is to verify we are sending correct info to the scheduler core
	mockedApiProvider.MockSchedulerAPIUpdateAllocationFn(func(request *si.AllocationRequest) error {
		assert.Assert(t, request.Releases != nil)
		assert.Assert(t, request.Releases.AllocationsToRelease != nil)
		assert.Equal(t, request.Releases.AllocationsToRelease[0].ApplicationID, app.applicationID)
		assert.Equal(t, request.Releases.AllocationsToRelease[0].PartitionName, "default")
		assert.Equal(t, request.Releases.AllocationsToRelease[0].AllocationKey, task.taskID)
		return nil
	})

	// complete
	event4 := NewSimpleTaskEvent(app.applicationID, task.taskID, CompleteTask)
	err = task.handle(event4)
	assert.NilError(t, err, "failed to handle CompleteTask event")
	assert.Equal(t, task.GetTaskState(), TaskStates().Completed)
	// 2 updates call, 1 for submit, 1 for release
	assert.Equal(t, mockedApiProvider.GetSchedulerAPIUpdateAllocationCount(), int32(2))
}

func TestCreateTask(t *testing.T) {
	time0 := time.Now()
	mockedContext := initContextForTest()
	mockedSchedulerAPI := newMockSchedulerAPI()
	app := NewApplication("app01", "root.default",
		"bob", testGroups, map[string]string{}, mockedSchedulerAPI)

	// pod has timestamp defined
	pod0 := &v1.Pod{
		TypeMeta: metav1.TypeMeta{
			Kind:       "Pod",
			APIVersion: "v1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:              "pod-00",
			UID:               "UID-00",
			CreationTimestamp: metav1.Time{Time: time0},
		},
	}

	// pod has no timestamp defined
	pod1 := &v1.Pod{
		TypeMeta: metav1.TypeMeta{
			Kind:       "Pod",
			APIVersion: "v1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name: "pod-00",
			UID:  "UID-00",
		},
	}

	// make sure the time is passed in to the task
	task0 := NewTask("task00", app, mockedContext, pod0)
	assert.Equal(t, task0.createTime, time0)

	// if pod doesn't have timestamp defined, uses the default value
	task1 := NewTask("task01", app, mockedContext, pod1)
	assert.Equal(t, task1.createTime, time.Time{})
}

func TestSortTasks(t *testing.T) {
	time0 := time.Now()
	time1 := time0.Add(10 * time.Millisecond)
	time2 := time1.Add(10 * time.Millisecond)

	mockedContext := initContextForTest()
	mockedSchedulerAPI := newMockSchedulerAPI()
	app := NewApplication("app01", "root.default",
		"bob", testGroups, map[string]string{}, mockedSchedulerAPI)

	pod0 := &v1.Pod{
		TypeMeta: metav1.TypeMeta{
			Kind:       "Pod",
			APIVersion: "v1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:              "pod-00",
			UID:               "UID-00",
			CreationTimestamp: metav1.Time{Time: time0},
		},
	}

	pod1 := &v1.Pod{
		TypeMeta: metav1.TypeMeta{
			Kind:       "Pod",
			APIVersion: "v1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:              "pod-01",
			UID:               "UID-01",
			CreationTimestamp: metav1.Time{Time: time1},
		},
	}

	pod2 := &v1.Pod{
		TypeMeta: metav1.TypeMeta{
			Kind:       "Pod",
			APIVersion: "v1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:              "pod-02",
			UID:               "UID-02",
			CreationTimestamp: metav1.Time{Time: time2},
		},
	}

	task0 := NewTask("task00", app, mockedContext, pod0)
	task1 := NewTask("task01", app, mockedContext, pod1)
	task2 := NewTask("task02", app, mockedContext, pod2)
	app.addTask(task0)
	app.addTask(task1)
	app.addTask(task2)

	tasks := app.GetNewTasks()
	assert.Equal(t, len(tasks), 3)
	assert.Equal(t, tasks[0], task0)
	assert.Equal(t, tasks[1], task1)
	assert.Equal(t, tasks[2], task2)
}

func TestIsTerminated(t *testing.T) {
	mockedContext := initContextForTest()
	mockedSchedulerAPI := newMockSchedulerAPI()
	app := NewApplication("app01", "root.default",
		"bob", testGroups, map[string]string{}, mockedSchedulerAPI)
	pod := &v1.Pod{
		TypeMeta: metav1.TypeMeta{
			Kind:       "Pod",
			APIVersion: "v1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name: "pod-01",
			UID:  "UID-01",
		},
	}
	task := NewTask("task01", app, mockedContext, pod)
	// set task states to non-terminated
	task.sm.SetState(TaskStates().Pending)
	res := task.isTerminated()
	assert.Equal(t, res, false)

	// set task states to terminated
	task.sm.SetState(TaskStates().Failed)
	res = task.isTerminated()
	assert.Equal(t, res, true)
}

func TestSetTaskGroup(t *testing.T) {
	mockedContext := initContextForTest()
	mockedSchedulerAPI := newMockSchedulerAPI()
	app := NewApplication("app01", "root.default",
		"bob", testGroups, map[string]string{}, mockedSchedulerAPI)
	pod := &v1.Pod{
		TypeMeta: metav1.TypeMeta{
			Kind:       "Pod",
			APIVersion: "v1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name: "pod-01",
			UID:  "UID-01",
		},
	}
	task := NewTask("task01", app, mockedContext, pod)
	task.setTaskGroupName("test-group")
	assert.Equal(t, task.GetTaskGroupName(), "test-group")
}

//nolint:funlen
func TestHandleSubmitTaskEvent(t *testing.T) {
	mockedContext, mockedSchedulerAPI := initContextAndAPIProviderForTest()
	var allocRequest *si.AllocationRequest
	mockedSchedulerAPI.MockSchedulerAPIUpdateAllocationFn(func(request *si.AllocationRequest) error {
		allocRequest = request
		return nil
	})

	preemptNever := v1.PreemptNever
	preemptLowerPriority := v1.PreemptLowerPriority
	priorityClass := &schedulingv1.PriorityClass{
		ObjectMeta: metav1.ObjectMeta{
			Name: "preempt-self-1000",
			Annotations: map[string]string{
				constants.AnnotationAllowPreemption: "true",
			},
		},
		Value:            1000,
		PreemptionPolicy: &preemptNever,
	}
	mockedContext.addPriorityClass(priorityClass)
	priorityClass2 := &schedulingv1.PriorityClass{
		ObjectMeta: metav1.ObjectMeta{
			Name: "preempt-others-1001",
			Annotations: map[string]string{
				constants.AnnotationAllowPreemption: "false",
			},
		},
		Value:            1001,
		PreemptionPolicy: &preemptLowerPriority,
	}
	mockedContext.addPriorityClass(priorityClass2)
	rt := &recorderTime{
		time: int64(0),
		lock: &locking.RWMutex{},
	}
	mr := events.NewMockedRecorder()
	mr.OnEventf = func() {
		rt.lock.Lock()
		defer rt.lock.Unlock()
		rt.time++
	}
	events.SetRecorder(mr)
	defer events.SetRecorder(events.NewMockedRecorder())
	resources := make(map[v1.ResourceName]resource.Quantity)
	containers := make([]v1.Container, 0)
	containers = append(containers, v1.Container{
		Name: "container-01",
		Resources: v1.ResourceRequirements{
			Requests: resources,
		},
	})
	var priority int32 = 1000
	pod1 := &v1.Pod{
		TypeMeta: metav1.TypeMeta{
			Kind:       "Pod",
			APIVersion: "v1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name: "pod-test-00001",
			UID:  "UID-00001",
		},
		Spec: v1.PodSpec{
			Containers:        containers,
			Priority:          &priority,
			PriorityClassName: "preempt-self-1000",
			PreemptionPolicy:  &preemptNever,
		},
	}
	var priority2 int32 = 1001
	pod2 := &v1.Pod{
		TypeMeta: metav1.TypeMeta{
			Kind:       "Pod",
			APIVersion: "v1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name: "pod-test-00002",
			UID:  "UID-00002",
			Annotations: map[string]string{
				constants.AnnotationTaskGroupName: "test-task-group",
			},
		},
		Spec: v1.PodSpec{
			Containers:        containers,
			Priority:          &priority2,
			PriorityClassName: "preempt-others-1001",
			PreemptionPolicy:  &preemptLowerPriority,
		},
	}
	appID := "app-test-001"
	app := NewApplication(appID, "root.abc", "testuser", testGroups, map[string]string{}, mockedSchedulerAPI.GetAPIs().SchedulerAPI)
	task1 := NewTask("task01", app, mockedContext, pod1)
	task2 := NewTask("task02", app, mockedContext, pod2)
	task1.sm.SetState(TaskStates().Pending)
	task2.sm.SetState(TaskStates().Pending)
	// pod without taskGroup name
	event1 := NewSubmitTaskEvent(app.applicationID, task1.taskID)
	err := task1.handle(event1)
	assert.NilError(t, err, "failed to handle SubmitTask event")
	assert.Equal(t, task1.GetTaskState(), TaskStates().Scheduling)
	assert.Equal(t, rt.time, int64(1))
	assert.Assert(t, allocRequest != nil)
	assert.Equal(t, len(allocRequest.Allocations), 1)
	assert.Equal(t, allocRequest.Allocations[0].Priority, int32(1000))
	assert.Assert(t, allocRequest.Allocations[0].PreemptionPolicy != nil)
	assert.Assert(t, allocRequest.Allocations[0].PreemptionPolicy.AllowPreemptSelf)
	assert.Assert(t, !allocRequest.Allocations[0].PreemptionPolicy.AllowPreemptOther)
	allocRequest = nil
	rt.time = 0
	// pod with taskGroup name
	event2 := NewSubmitTaskEvent(app.applicationID, task2.taskID)
	err = task2.handle(event2)
	assert.NilError(t, err, "failed to handle SubmitTask event")
	assert.Equal(t, task2.GetTaskState(), TaskStates().Scheduling)
	assert.Equal(t, rt.time, int64(2))
	assert.Assert(t, allocRequest != nil)
	assert.Equal(t, len(allocRequest.Allocations), 1)
	assert.Equal(t, allocRequest.Allocations[0].Priority, int32(1001))
	assert.Assert(t, allocRequest.Allocations[0].PreemptionPolicy != nil)
	assert.Assert(t, !allocRequest.Allocations[0].PreemptionPolicy.AllowPreemptSelf)
	assert.Assert(t, allocRequest.Allocations[0].PreemptionPolicy.AllowPreemptOther)
}

func TestSimultaneousTaskCompleteAndAllocate(t *testing.T) {
	const (
		podUID    = "UID-00001"
		appID     = "app-test-001"
		queueName = "root.abc"
	)
	mockedContext := initContextForTest()
	mockedAPIProvider, ok := mockedContext.apiProvider.(*client.MockedAPIProvider)
	assert.Equal(t, ok, true)

	resources := make(map[v1.ResourceName]resource.Quantity)
	containers := make([]v1.Container, 0)
	containers = append(containers, v1.Container{
		Name: "container-01",
		Resources: v1.ResourceRequirements{
			Requests: resources,
		},
	})

	pod1 := &v1.Pod{
		TypeMeta: metav1.TypeMeta{
			Kind:       "Pod",
			APIVersion: "v1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name: "pod-test-00001",
			UID:  podUID,
		},
		Spec: v1.PodSpec{
			Containers: containers,
		},
	}

	// simulate app has one task waiting for core's allocation
	app := NewApplication(appID, queueName, "user", testGroups, map[string]string{}, mockedAPIProvider.GetAPIs().SchedulerAPI)
	task1 := NewTask(podUID, app, mockedContext, pod1)
	task1.sm.SetState(TaskStates().Scheduling)

	// notify task complete
	// because the task is in Scheduling state,
	// here we expect to trigger a UpdateRequest that contains a releaseAllocationAsk request
	mockedAPIProvider.MockSchedulerAPIUpdateAllocationFn(func(request *si.AllocationRequest) error {
		assert.Equal(t, len(request.Releases.AllocationsToRelease), 1,
			"allocationsToRelease is not in the expected length")
		askToRelease := request.Releases.AllocationsToRelease[0]
		assert.Equal(t, askToRelease.ApplicationID, appID)
		assert.Equal(t, askToRelease.AllocationKey, podUID)
		return nil
	})
	ev := NewSimpleTaskEvent(appID, task1.GetTaskID(), CompleteTask)
	err := task1.handle(ev)
	assert.NilError(t, err, "failed to handle CompleteTask event")
	assert.Equal(t, task1.GetTaskState(), TaskStates().Completed)

	// simulate the core responses us an allocation
	// the task is already completed, we need to make sure the allocation
	// can be released from the core to avoid resource leak
	alloc := &si.Allocation{
		AllocationKey: string(pod1.UID),
		NodeID:        "fake-node",
		ApplicationID: appID,
		PartitionName: "default",
	}
	mockedAPIProvider.MockSchedulerAPIUpdateAllocationFn(func(request *si.AllocationRequest) error {
		assert.Equal(t, len(request.Releases.AllocationsToRelease), 1,
			"allocationsToRelease is not in the expected length")
		allocToRelease := request.Releases.AllocationsToRelease[0]
		assert.Equal(t, allocToRelease.ApplicationID, alloc.ApplicationID)
		assert.Equal(t, allocToRelease.AllocationKey, alloc.AllocationKey)
		return nil
	})
	ev1 := NewAllocateTaskEvent(app.GetApplicationID(), alloc.AllocationKey, alloc.AllocationKey, alloc.NodeID)
	err = task1.handle(ev1)
	assert.NilError(t, err, "failed to handle AllocateTask event")
	assert.Equal(t, task1.GetTaskState(), TaskStates().Completed)
}

func TestUpdatePodCondition(t *testing.T) {
	condition := v1.PodCondition{
		Type:   v1.ContainersReady,
		Status: v1.ConditionTrue,
		Reason: v1.PodReasonSchedulingGated,
	}

	pod := &v1.Pod{
		TypeMeta: metav1.TypeMeta{
			Kind:       "Pod",
			APIVersion: "v1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name: "pod-test-00001",
		},
		Status: v1.PodStatus{
			Phase: v1.PodPending,
		},
	}
	app := NewApplication(appID, "root.default", "user", testGroups, map[string]string{}, nil)
	task := NewTask("pod-1", app, nil, pod)
	updated, podCopy := task.UpdatePodCondition(&condition)
	assert.Equal(t, true, updated)
	assert.Equal(t, 1, len(podCopy.Status.Conditions))
	assert.Equal(t, v1.ConditionTrue, podCopy.Status.Conditions[0].Status)
	assert.Equal(t, v1.ContainersReady, podCopy.Status.Conditions[0].Type)
	assert.Equal(t, v1.PodReasonSchedulingGated, podCopy.Status.Conditions[0].Reason)
	assert.Equal(t, v1.PodPending, podCopy.Status.Phase)
	assert.Equal(t, 1, len(task.podStatus.Conditions))
	assert.Equal(t, v1.ConditionTrue, task.podStatus.Conditions[0].Status)
	assert.Equal(t, v1.ContainersReady, task.podStatus.Conditions[0].Type)
	assert.Equal(t, v1.PodReasonSchedulingGated, task.podStatus.Conditions[0].Reason)
	assert.Equal(t, v1.PodPending, task.podStatus.Phase)

	podWithCondition := &v1.Pod{
		TypeMeta: metav1.TypeMeta{
			Kind:       "Pod",
			APIVersion: "v1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name: "pod-test-00001",
		},
		Status: v1.PodStatus{
			Phase: v1.PodPending,
			Conditions: []v1.PodCondition{
				condition,
			},
		},
	}
	app = NewApplication(appID, "root.default", "user", testGroups, map[string]string{}, nil)
	task = NewTask("pod-1", app, nil, podWithCondition)

	// no update
	updated, podCopy = task.UpdatePodCondition(&condition)
	assert.Equal(t, false, updated)
	assert.Equal(t, 1, len(task.podStatus.Conditions))
	assert.Equal(t, v1.ConditionTrue, task.podStatus.Conditions[0].Status)
	assert.Equal(t, v1.ContainersReady, task.podStatus.Conditions[0].Type)
	assert.Equal(t, v1.PodPending, task.podStatus.Phase)
	assert.Equal(t, v1.PodReasonSchedulingGated, task.podStatus.Conditions[0].Reason)
	assert.Equal(t, 1, len(podCopy.Status.Conditions))
	assert.Equal(t, v1.ConditionTrue, podCopy.Status.Conditions[0].Status)
	assert.Equal(t, v1.ContainersReady, podCopy.Status.Conditions[0].Type)
	assert.Equal(t, v1.PodPending, podCopy.Status.Phase)
	assert.Equal(t, v1.PodReasonSchedulingGated, podCopy.Status.Conditions[0].Reason)

	// update status & reason
	condition.Status = v1.ConditionFalse
	condition.Reason = v1.PodReasonUnschedulable
	updated, podCopy = task.UpdatePodCondition(&condition)
	assert.Equal(t, true, updated)
	assert.Equal(t, 1, len(task.podStatus.Conditions))
	assert.Equal(t, v1.ConditionFalse, task.podStatus.Conditions[0].Status)
	assert.Equal(t, v1.ContainersReady, task.podStatus.Conditions[0].Type)
	assert.Equal(t, v1.PodPending, task.podStatus.Phase)
	assert.Equal(t, v1.PodReasonUnschedulable, task.podStatus.Conditions[0].Reason)
	assert.Equal(t, 1, len(podCopy.Status.Conditions))
	assert.Equal(t, v1.ConditionFalse, podCopy.Status.Conditions[0].Status)
	assert.Equal(t, v1.ContainersReady, podCopy.Status.Conditions[0].Type)
	assert.Equal(t, v1.PodPending, podCopy.Status.Phase)
	assert.Equal(t, v1.PodReasonUnschedulable, podCopy.Status.Conditions[0].Reason)
}

//nolint:funlen
func TestCheckPodMetadataBeforeScheduling(t *testing.T) {
	app := NewApplication(appID1, "root.default", "user", testGroups, map[string]string{}, nil)

	rt := &recorderTime{
		time: int64(0),
		lock: &locking.RWMutex{},
	}
	mr := events.NewMockedRecorder()
	mr.OnEventf = func() {
		rt.lock.Lock()
		defer rt.lock.Unlock()
		rt.time++
	}
	events.SetRecorder(mr)
	defer events.SetRecorder(events.NewMockedRecorder())

	testCases := []struct {
		name                      string
		pod                       *v1.Pod
		expectedWarningEventCount int64
	}{
		{
			name: "regular",
			pod: &v1.Pod{
				TypeMeta: metav1.TypeMeta{
					Kind:       "Pod",
					APIVersion: "v1",
				},
				ObjectMeta: metav1.ObjectMeta{
					Name: podName1,
					Labels: map[string]string{
						constants.CanonicalLabelApplicationID: appID1,
						constants.CanonicalLabelQueueName:     queueNameA,
					},
				},
				Spec: v1.PodSpec{
					SchedulerName: constants.SchedulerName,
				},
			},
			expectedWarningEventCount: 0,
		},
		{
			name: "inconsistent app id label",
			pod: &v1.Pod{
				TypeMeta: metav1.TypeMeta{
					Kind:       "Pod",
					APIVersion: "v1",
				},
				ObjectMeta: metav1.ObjectMeta{
					Name: podName1,
					Labels: map[string]string{
						constants.CanonicalLabelApplicationID: appID1,
						constants.CanonicalLabelQueueName:     queueNameA,
						constants.LabelApplicationID:          appID2,
					},
				},
				Spec: v1.PodSpec{
					SchedulerName: constants.SchedulerName,
				},
			},
			expectedWarningEventCount: 1,
		},
		{
			name: "inconsistent app id annotation",
			pod: &v1.Pod{
				TypeMeta: metav1.TypeMeta{
					Kind:       "Pod",
					APIVersion: "v1",
				},
				ObjectMeta: metav1.ObjectMeta{
					Name: podName1,
					Labels: map[string]string{
						constants.CanonicalLabelApplicationID: appID1,
						constants.CanonicalLabelQueueName:     queueNameA,
					},
					Annotations: map[string]string{
						constants.AnnotationApplicationID: appID2,
					},
				},
				Spec: v1.PodSpec{
					SchedulerName: constants.SchedulerName,
				},
			},
			expectedWarningEventCount: 1,
		},
		{
			name: "inconsistent queue label",
			pod: &v1.Pod{
				TypeMeta: metav1.TypeMeta{
					Kind:       "Pod",
					APIVersion: "v1",
				},
				ObjectMeta: metav1.ObjectMeta{
					Name: podName1,
					Labels: map[string]string{
						constants.CanonicalLabelApplicationID: appID1,
						constants.CanonicalLabelQueueName:     queueNameA,
						constants.LabelQueueName:              queueNameB,
					},
				},
				Spec: v1.PodSpec{
					SchedulerName: constants.SchedulerName,
				},
			},
			expectedWarningEventCount: 1,
		},
		{
			name: "inconsistent queue annotation",
			pod: &v1.Pod{
				TypeMeta: metav1.TypeMeta{
					Kind:       "Pod",
					APIVersion: "v1",
				},
				ObjectMeta: metav1.ObjectMeta{
					Name: podName1,
					Labels: map[string]string{
						constants.CanonicalLabelApplicationID: appID1,
						constants.CanonicalLabelQueueName:     queueNameA,
					},
					Annotations: map[string]string{
						constants.CanonicalLabelQueueName: queueNameB,
					},
				},
				Spec: v1.PodSpec{
					SchedulerName: constants.SchedulerName,
				},
			},
			expectedWarningEventCount: 1,
		},
		{
			name: "inconsistent app id and queue",
			pod: &v1.Pod{
				TypeMeta: metav1.TypeMeta{
					Kind:       "Pod",
					APIVersion: "v1",
				},
				ObjectMeta: metav1.ObjectMeta{
					Name: podName1,
					Labels: map[string]string{
						constants.CanonicalLabelApplicationID: appID1,
						constants.CanonicalLabelQueueName:     queueNameA,
						constants.LabelApplicationID:          appID2,
						constants.LabelQueueName:              queueNameB,
					},
				},
				Spec: v1.PodSpec{
					SchedulerName: constants.SchedulerName,
				},
			},
			expectedWarningEventCount: 2,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// reset to 0 before every iteration
			rt.time = 0
			task := NewTask("task", app, nil, tc.pod)
			task.checkPodMetadataBeforeScheduling()
			assert.Equal(t, rt.time, tc.expectedWarningEventCount)
		})
	}
}
