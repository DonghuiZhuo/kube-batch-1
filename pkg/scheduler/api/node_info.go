/*
Copyright 2017 The Kubernetes Authors.

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

package api

import (
	"fmt"
	"github.com/golang/glog"

	"k8s.io/api/core/v1"
)

// NodeInfo is node level aggregated information.
type NodeInfo struct {
	Name string
	Node *v1.Node

	// The releasing resource on that node
	Releasing *Resource
	// The idle resource on that node
	Idle *Resource
	// The used resource on that node, including running and terminating
	// pods
	Used *Resource
	// The used resource on the node occupied by backfill tasks
	Backfilled *Resource

	Allocatable *Resource
	Capability  *Resource

	Tasks map[TaskID]*TaskInfo
}

func NewNodeInfo(node *v1.Node) *NodeInfo {
	if node == nil {
		return &NodeInfo{
			Releasing: EmptyResource(),
			Idle:      EmptyResource(),
			Used:      EmptyResource(),
			Backfilled:EmptyResource(),

			Allocatable: EmptyResource(),
			Capability:  EmptyResource(),

			Tasks: make(map[TaskID]*TaskInfo),
		}
	}

	return &NodeInfo{
		Name: node.Name,
		Node: node,

		Releasing: EmptyResource(),
		Idle:      NewResource(node.Status.Allocatable),
		Used:      EmptyResource(),
		Backfilled:EmptyResource(),

		Allocatable: NewResource(node.Status.Allocatable),
		Capability:  NewResource(node.Status.Capacity),

		Tasks: make(map[TaskID]*TaskInfo),
	}
}

func (ni *NodeInfo) Clone() *NodeInfo {
	newNode := NewNodeInfo(ni.Node)
	glog.V(3).Infof("new node <%v>: capability %v,  allocatable %v, idle %v, used %v, backfilled %v, releasing %v", ni.Name, ni.Capability.MilliCPU,
		ni.Allocatable.MilliCPU,
		ni.Idle.MilliCPU,
		ni.Used.MilliCPU,
		ni.Backfilled.MilliCPU,
		ni.Releasing.MilliCPU)

	for _, task := range ni.Tasks {
		glog.V(4).Infof("Adding task <%v/%v> to node <%v> with resource request %v", task.Namespace, task.Name, ni.Name, task.Resreq)
		newNode.AddTask(task)
	}

	return newNode
}

func (ni *NodeInfo) SetNode(node *v1.Node) {
	ni.Name = node.Name
	ni.Node = node

	ni.Allocatable = NewResource(node.Status.Allocatable)
	ni.Capability = NewResource(node.Status.Capacity)
	ni.Idle = NewResource(node.Status.Allocatable)

	for _, task := range ni.Tasks {
		if task.Status == Releasing {
			ni.Releasing.Add(task.Resreq)
		}

		ni.Idle.Sub(task.Resreq)
		ni.Used.Add(task.Resreq)
	}
}

func (ni *NodeInfo) AddTask(task *TaskInfo) error {
	key := PodKey(task.Pod)
	if _, found := ni.Tasks[key]; found {
		return fmt.Errorf("task <%v/%v> already on node <%v>",
			task.Namespace, task.Name, ni.Name)
	}

	// Node will hold a copy of task to make sure the status
	// change will not impact resource in node.
	ti := task.Clone()

	// add backfill flag if not done intially yet during cloning snapshot
	if CheckBackfill(ti.Pod) && !ti.IsBackfill {
		ti.IsBackfill = true
	}

	if ni.Node != nil {
		if task.IsBackfill {
			ni.Backfilled.Add(task.Resreq)
		}

		switch ti.Status {
		case Releasing:
			ni.Releasing.Add(ti.Resreq)
			ni.Idle.Sub(ti.Resreq)
		case Pipelined:
			ni.Releasing.Sub(ti.Resreq)
		default:
			ni.Idle.Sub(ti.Resreq)
		}

		ni.Used.Add(ti.Resreq)
	}

	ni.Tasks[key] = ti

	return nil
}

func (ni *NodeInfo) RemoveTask(ti *TaskInfo) error {
	key := PodKey(ti.Pod)

	task, found := ni.Tasks[key]
	if !found {
		return fmt.Errorf("failed to find task <%v/%v> on host <%v>",
			ti.Namespace, ti.Name, ni.Name)
	}

	if ni.Node != nil {
		if task.IsBackfill {
			ni.Backfilled.Sub(task.Resreq)
		}

		switch task.Status {
		case Releasing:
			ni.Releasing.Sub(task.Resreq)
			ni.Idle.Add(task.Resreq)
		case Pipelined:
			ni.Releasing.Add(task.Resreq)
		default:
			ni.Idle.Add(task.Resreq)
		}

		ni.Used.Sub(task.Resreq)
	}

	delete(ni.Tasks, key)

	return nil
}

func (ni *NodeInfo) UpdateTask(ti *TaskInfo) error {
	if err := ni.RemoveTask(ti); err != nil {
		return err
	}

	return ni.AddTask(ti)
}

func (ni NodeInfo) String() string {
	res := ""

	i := 0
	for _, task := range ni.Tasks {
		res = res + fmt.Sprintf("\n\t %d: %v", i, task)
		i++
	}

	return fmt.Sprintf("Node (%s): idle <%v>, used <%v>, releasing <%v>, taints <%v>%s",
		ni.Name, ni.Idle, ni.Used, ni.Releasing, ni.Node.Spec.Taints, res)

}

func (ni *NodeInfo) Pods() (pods []*v1.Pod) {
	for _, t := range ni.Tasks {
		pods = append(pods, t.Pod)
	}

	return
}

func (ni *NodeInfo) GetAccessibleResource() *Resource {
	idle := ni.Idle.Clone()
	accessible := idle.Add(ni.Backfilled).Clone()
	glog.V(3).Infof("Accessible resources on Node <%v>: %v. Idle: %v. Backfilled: %v", ni.Name, accessible, ni.Idle, ni.Backfilled)
	return accessible
}
