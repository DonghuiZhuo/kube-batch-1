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

package backfill

import (
	"fmt"
	"github.com/kubernetes-sigs/kube-batch/pkg/scheduler/cache"
	"github.com/kubernetes-sigs/kube-batch/pkg/scheduler/conf"
	"github.com/kubernetes-sigs/kube-batch/pkg/scheduler/plugins/gang"
	"reflect"
	"time"

	kbv1 "github.com/kubernetes-sigs/kube-batch/pkg/apis/scheduling/v1alpha1"
	"github.com/kubernetes-sigs/kube-batch/pkg/scheduler/api"
	"github.com/kubernetes-sigs/kube-batch/pkg/scheduler/framework"
	"k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"sync"
	"testing"
)

func buildResourceList(cpu string, memory string) v1.ResourceList {
	return v1.ResourceList{
		v1.ResourceCPU:      resource.MustParse(cpu),
		v1.ResourceMemory:   resource.MustParse(memory),
		api.GPUResourceName: resource.MustParse("0"),
	}
}

func buildNode(name string, alloc v1.ResourceList, labels map[string]string) *v1.Node {
	return &v1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name:   name,
			Labels: labels,
		},
		Status: v1.NodeStatus{
			Capacity:    alloc,
			Allocatable: alloc,
		},
	}
}

func buildPod(ns, n, nn string, p v1.PodPhase, req v1.ResourceList, groupName string, labels map[string]string, selector map[string]string) *v1.Pod {
	return &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			UID:       types.UID(fmt.Sprintf("%v-%v", ns, n)),
			Name:      n,
			Namespace: ns,
			Labels:    labels,
			Annotations: map[string]string{
				kbv1.GroupNameAnnotationKey: groupName,
			},
		},
		Status: v1.PodStatus{
			Phase: p,
		},
		Spec: v1.PodSpec{
			NodeName:     nn,
			NodeSelector: selector,
			Containers: []v1.Container{
				{
					Resources: v1.ResourceRequirements{
						Requests: req,
					},
				},
			},
			Priority: new(int32),
		},
	}
}

type fakeBinder struct {
	sync.Mutex
	binds map[string]string
	c     chan string
}

func (fb *fakeBinder) Bind(p *v1.Pod, hostname string) error {
	fb.Lock()
	defer fb.Unlock()

	key := fmt.Sprintf("%v/%v", p.Namespace, p.Name)
	fb.binds[key] = hostname

	fb.c <- key

	return nil
}

type fakeStatusUpdater struct {
}

func (ftsu *fakeStatusUpdater) UpdatePodCondition(pod *v1.Pod, podCondition *v1.PodCondition) (*v1.Pod, error) {
	// do nothing here
	return nil, nil
}

func (ftsu *fakeStatusUpdater) UpdatePodGroup(pg *kbv1.PodGroup) (*kbv1.PodGroup, error) {
	// do nothing here
	return nil, nil
}

type fakeVolumeBinder struct {
}

func (fvb *fakeVolumeBinder) AllocateVolumes(task *api.TaskInfo, hostname string) error {
	return nil
}
func (fvb *fakeVolumeBinder) BindVolumes(task *api.TaskInfo) error {
	return nil
}

func TestBackFill(t *testing.T) {
	framework.RegisterPluginBuilder("gang", gang.New)
	defer framework.CleanupPluginBuilders()

	tests := []struct {
		name      string
		podGroups []*kbv1.PodGroup
		pods      []*v1.Pod
		nodes     []*v1.Node
		queues    []*kbv1.Queue
		expected  map[string]string
	}{
		{
			name: "two jobs with one node",
			podGroups: []*kbv1.PodGroup{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name:              "pg1",
						Namespace:         "c1",
						CreationTimestamp: metav1.Now(),
					},
					Spec: kbv1.PodGroupSpec{
						Queue:     "c1",
						MinMember: 2,
					},
				},
				{
					ObjectMeta: metav1.ObjectMeta{
						Name:              "pg2",
						Namespace:         "c1",
						CreationTimestamp: metav1.Now(),
					},
					Spec: kbv1.PodGroupSpec{
						Queue:     "c1",
						MinMember: 1,
					},
				},
			},
			pods: []*v1.Pod{
				buildPod("c1", "pg1_1", "", v1.PodPending, buildResourceList("2", "1G"), "pg1", make(map[string]string), make(map[string]string)),
				buildPod("c1", "pg1_2", "", v1.PodPending, buildResourceList("2", "1G"), "pg1", make(map[string]string), make(map[string]string)),
				buildPod("c1", "pg2_1", "", v1.PodPending, buildResourceList("2", "1G"), "pg2", make(map[string]string), make(map[string]string)),
			},
			nodes: []*v1.Node{
				buildNode("n1", buildResourceList("2", "4Gi"), make(map[string]string)),
			},
			queues: []*kbv1.Queue{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: "c1",
					},
					Spec: kbv1.QueueSpec{
						Weight: 1,
					},
				},
			},
			expected: map[string]string{
				"c1/pg2_1": "n1",
			},
		},
	}

	backFill := New()

	for i, test := range tests {
		binder := &fakeBinder{
			binds: map[string]string{},
			c:     make(chan string),
		}
		schedulerCache := &cache.SchedulerCache{
			Nodes:         make(map[string]*api.NodeInfo),
			Jobs:          make(map[api.JobID]*api.JobInfo),
			Queues:        make(map[api.QueueID]*api.QueueInfo),
			Binder:        binder,
			StatusUpdater: &fakeStatusUpdater{},
			VolumeBinder:  &fakeVolumeBinder{},

			Recorder: record.NewFakeRecorder(100),
		}
		for _, node := range test.nodes {
			schedulerCache.AddNode(node)
		}
		for _, pod := range test.pods {
			schedulerCache.AddPod(pod)
		}

		for _, ss := range test.podGroups {
			schedulerCache.AddPodGroup(ss)
		}

		for _, q := range test.queues {
			schedulerCache.AddQueue(q)
		}

		ssn := framework.OpenSession(schedulerCache, []conf.Tier{
			{
				Plugins: []conf.PluginOption{
					{
						Name: "gang",
					},
				},
			},
		})
		defer framework.CloseSession(ssn)

		for _, job := range ssn.Jobs {
			for _, task := range job.Tasks {
				for _, node := range ssn.Nodes {
					if task.Resreq.LessEqual(node.Idle) {
						ssn.Allocate(task, node.Name, false, false)
					}
				}
			}
		}

		ssn.EnableBackfill = true
		ssn.StarvationThreshold = conf.DefaultStarvingThreshold
		backFill.Execute(ssn)

		for i := 0; i < len(test.expected); i++ {
			select {
			case <-binder.c:
			case <-time.After(3 * time.Second):
				t.Errorf("Failed to get binding request.")
			}
		}

		if !reflect.DeepEqual(test.expected, binder.binds) {
			t.Errorf("case %d (%s): expected: %v, got %v ", i, test.name, test.expected, binder.binds)
		}
	}
}
