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

package e2e

import (
	"fmt"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Job E2E Test", func() {
	It("Schedule Job", func() {
		context := initTestContext()
		defer cleanupTestContext(context)
		rep := clusterSize(context, oneCPU)

		_, pg := createJob(context, &jobSpec{
			name: "qj-1",
			tasks: []taskSpec{
				{
					img: "busybox",
					req: oneCPU,
					min: 2,
					rep: rep,
				},
			},
		})

		err := waitPodGroupReady(context, pg)
		checkError(context, err)
	})

	It("Schedule Multiple Jobs", func() {
		context := initTestContext()
		defer cleanupTestContext(context)

		rep := clusterSize(context, oneCPU)

		job := &jobSpec{
			tasks: []taskSpec{
				{
					img: "busybox",
					req: oneCPU,
					min: 2,
					rep: rep,
				},
			},
		}

		job.name = "mqj-1"
		_, pg1 := createJob(context, job)
		job.name = "mqj-2"
		_, pg2 := createJob(context, job)
		job.name = "mqj-3"
		_, pg3 := createJob(context, job)

		err := waitPodGroupReady(context, pg1)
		checkError(context, err)

		err = waitPodGroupReady(context, pg2)
		checkError(context, err)

		err = waitPodGroupReady(context, pg3)
		checkError(context, err)
	})

	It("Gang scheduling", func() {
		context := initTestContext()
		defer cleanupTestContext(context)
		rep := clusterSize(context, oneCPU)/2 + 1

		replicaset := createReplicaSet(context, "rs-1", rep, "nginx", oneCPU)
		err := waitReplicaSetReady(context, replicaset.Name)
		checkError(context, err)

		job := &jobSpec{
			name:      "gang-qj",
			namespace: "test",
			tasks: []taskSpec{
				{
					img: "busybox",
					req: oneCPU,
					min: rep,
					rep: rep,
				},
			},
		}

		_, pg := createJob(context, job)

		err = waitPodGroupPending(context, pg)
		checkError(context, err)

		err = waitPodGroupUnschedulable(context, pg)
		checkError(context, err)

		err = deleteReplicaSet(context, replicaset.Name)
		checkError(context, err)

		err = waitPodGroupReady(context, pg)
		checkError(context, err)
	})

	It("Gang scheduling: Full Occupied", func() {
		context := initTestContext()
		defer cleanupTestContext(context)
		rep := clusterSize(context, oneCPU)

		job := &jobSpec{
			namespace: "test",
			tasks: []taskSpec{
				{
					img: "nginx",
					req: oneCPU,
					min: rep,
					rep: rep,
				},
			},
		}

		job.name = "gang-fq-qj1"
		_, pg1 := createJob(context, job)
		err := waitPodGroupReady(context, pg1)
		checkError(context, err)

		job.name = "gang-fq-qj2"
		_, pg2 := createJob(context, job)
		err = waitPodGroupPending(context, pg2)
		checkError(context, err)

		err = waitPodGroupReady(context, pg1)
		checkError(context, err)
	})

	It("Preemption", func() {
		context := initTestContext()
		defer cleanupTestContext(context)

		slot := oneCPU
		rep := clusterSize(context, slot)

		job := &jobSpec{
			tasks: []taskSpec{
				{
					img: "nginx",
					req: slot,
					min: 1,
					rep: rep,
				},
			},
		}

		job.name = "preemptee-qj"
		_, pg1 := createJob(context, job)
		err := waitTasksReady(context, pg1, int(rep))
		checkError(context, err)

		job.name = "preemptor-qj"
		_, pg2 := createJob(context, job)
		err = waitTasksReady(context, pg1, int(rep)/2)
		checkError(context, err)

		err = waitTasksReady(context, pg2, int(rep)/2)
		checkError(context, err)
	})

	It("Multiple Preemption", func() {
		context := initTestContext()
		defer cleanupTestContext(context)

		slot := oneCPU
		rep := clusterSize(context, slot)

		job := &jobSpec{
			tasks: []taskSpec{
				{
					img: "nginx",
					req: slot,
					min: 1,
					rep: rep,
				},
			},
		}

		job.name = "preemptee-qj"
		_, pg1 := createJob(context, job)
		err := waitTasksReady(context, pg1, int(rep))
		checkError(context, err)

		job.name = "preemptor-qj1"
		_, pg2 := createJob(context, job)
		checkError(context, err)

		job.name = "preemptor-qj2"
		_, pg3 := createJob(context, job)
		checkError(context, err)

		err = waitTasksReady(context, pg1, int(rep)/3)
		checkError(context, err)

		err = waitTasksReady(context, pg2, int(rep)/3)
		checkError(context, err)

		err = waitTasksReady(context, pg3, int(rep)/3)
		checkError(context, err)
	})

	It("Schedule BestEffort Job", func() {
		context := initTestContext()
		defer cleanupTestContext(context)

		slot := oneCPU
		rep := clusterSize(context, slot)

		job := &jobSpec{
			name: "test",
			tasks: []taskSpec{
				{
					img: "nginx",
					req: slot,
					min: 2,
					rep: rep,
				},
				{
					img: "nginx",
					min: 2,
					rep: rep / 2,
				},
			},
		}

		_, pg := createJob(context, job)

		err := waitPodGroupReady(context, pg)
		checkError(context, err)
	})

	It("Statement", func() {
		context := initTestContext()
		defer cleanupTestContext(context)

		slot := oneCPU
		rep := clusterSize(context, slot)

		job := &jobSpec{
			namespace: "test",
			tasks: []taskSpec{
				{
					img: "nginx",
					req: slot,
					min: rep,
					rep: rep,
				},
			},
		}

		job.name = "st-qj-1"
		_, pg1 := createJob(context, job)
		err := waitPodGroupReady(context, pg1)
		checkError(context, err)

		now := time.Now()

		job.name = "st-qj-2"
		_, pg2 := createJob(context, job)
		err = waitPodGroupUnschedulable(context, pg2)
		checkError(context, err)

		// No preemption event
		evicted, err := podGroupEvicted(context, pg1, now)()
		checkError(context, err)
		Expect(evicted).NotTo(BeTrue())
	})

	It("TaskPriority", func() {
		context := initTestContext()
		defer cleanupTestContext(context)

		slot := oneCPU
		rep := clusterSize(context, slot)

		replicaset := createReplicaSet(context, "rs-1", rep/2, "nginx", slot)
		err := waitReplicaSetReady(context, replicaset.Name)
		checkError(context, err)

		_, pg := createJob(context, &jobSpec{
			name: "multi-pod-job",
			tasks: []taskSpec{
				{
					img: "nginx",
					pri: workerPriority,
					min: rep/2 - 1,
					rep: rep,
					req: slot,
				},
				{
					img: "nginx",
					pri: masterPriority,
					min: 1,
					rep: 1,
					req: slot,
				},
			},
		})

		expteced := map[string]int{
			masterPriority: 1,
			workerPriority: int(rep/2) - 1,
		}

		err = waitTasksReadyEx(context, pg, expteced)
		checkError(context, err)
	})

	It("Try to fit unassigned task with different resource requests in one loop", func() {
		context := initTestContext()
		defer cleanupTestContext(context)

		slot := oneCPU
		rep := clusterSize(context, slot)
		minMemberOverride := int32(1)

		replicaset := createReplicaSet(context, "rs-1", rep-1, "nginx", slot)
		err := waitReplicaSetReady(context, replicaset.Name)
		checkError(context, err)

		_, pg := createJob(context, &jobSpec{
			name: "multi-task-diff-resource-job",
			tasks: []taskSpec{
				{
					img: "nginx",
					pri: masterPriority,
					min: 1,
					rep: 1,
					req: twoCPU,
				},
				{
					img: "nginx",
					pri: workerPriority,
					min: 1,
					rep: 1,
					req: halfCPU,
				},
			},
			minMember: &minMemberOverride,
		})

		err = waitPodGroupPending(context, pg)
		checkError(context, err)

		// task_1 has been scheduled
		err = waitTasksReady(context, pg, int(minMemberOverride))
		checkError(context, err)
	})

	It("Job Priority", func() {
		context := initTestContext()
		defer cleanupTestContext(context)

		slot := oneCPU
		rep := clusterSize(context, slot)

		replicaset := createReplicaSet(context, "rs-1", rep, "nginx", slot)
		err := waitReplicaSetReady(context, replicaset.Name)
		checkError(context, err)

		job1 := &jobSpec{
			name: "pri-job-1",
			pri:  workerPriority,
			tasks: []taskSpec{
				{
					img: "nginx",
					req: oneCPU,
					min: rep/2 + 1,
					rep: rep,
				},
			},
		}

		job2 := &jobSpec{
			name: "pri-job-2",
			pri:  masterPriority,
			tasks: []taskSpec{
				{
					img: "nginx",
					req: oneCPU,
					min: rep/2 + 1,
					rep: rep,
				},
			},
		}

		createJob(context, job1)
		_, pg2 := createJob(context, job2)

		// Delete ReplicaSet
		err = deleteReplicaSet(context, replicaset.Name)
		checkError(context, err)

		err = waitPodGroupReady(context, pg2)
		checkError(context, err)
	})

	FIt("Starvation prevention", func() {
		context := initTestContext()
		defer cleanupTestContext(context)
		maxPods := clusterSize(context, oneCPU)

		// create a job to prevent job "big" from running
		smallerJob := &jobSpec{
			name:      "smaller",
			namespace: context.namespace,
			tasks: []taskSpec{
				{
					img: "nginx",
					req: oneCPU,
					min: maxPods-2,
					rep: maxPods-2,
				},
			},
		}
		_, pgs := createJob(context, smallerJob)
		err := waitPodGroupReady(context, pgs)

		// create job "big" --> pending
		bigJob := &jobSpec{
			name:      "big",
			namespace: context.namespace,
			tasks: []taskSpec{
				{
					img: "nginx",
					req: oneCPU,
					min: maxPods,
					rep: maxPods,
				},
			},
		}

		_, pg := createJob(context, bigJob)
		err = waitPodGroupPending(context, pg)
		Expect(err).NotTo(HaveOccurred())
		err = waitPodGroupUnschedulable(context, pg)
		Expect(err).NotTo(HaveOccurred())

		// create backfill job 1 --> running
		bfJob1 := &jobSpec{
			name:      "bf-1",
			namespace: context.namespace,
			tasks: []taskSpec{
				{
					img: "nginx",
					req: oneCPU,
					min: 1,
					rep: 1,
				},
			},
		}
		_, bfPg1 := createJob(context, bfJob1)
		err = waitPodGroupReady(context, bfPg1)
		Expect(err).NotTo(HaveOccurred())

		// delete bfJob
		for i := range bfJob1.tasks {
			err = deleteJob(context, fmt.Sprintf("%s-%d", bfJob1.name, i))
			Expect(err).NotTo(HaveOccurred())
		}

		// exhaust the starvation time
		time.Sleep(1 * time.Minute)

		// create another backfill job. at this point starvation
		// should have kicked in so it will not be backfilled
		bfJob2 := &jobSpec{
			name:      "bf-2",
			namespace: context.namespace,
			tasks: []taskSpec{
				{
					img: "nginx",
					req: oneCPU,
					min: 1,
					rep: 1,
				},
			},
		}

		_, bfPg2 := createJob(context, bfJob2)
		err = waitPodGroupPending(context, bfPg2)
		Expect(err).NotTo(HaveOccurred())
		err = waitPodGroupUnschedulable(context, bfPg2)
		Expect(err).NotTo(HaveOccurred())

		// Delete replica set
		for i := range smallerJob.tasks {
			err = deleteJob(context, fmt.Sprintf("%s-%d", smallerJob.name, i))
			Expect(err).NotTo(HaveOccurred())
		}

		// big job should have enough resource to start
		err = waitPodGroupReady(context, pg)
		Expect(err).NotTo(HaveOccurred())

		// backfill job2 should still be pending
		err = waitPodGroupPending(context, bfPg2)
		Expect(err).NotTo(HaveOccurred())

		// delete the big job
		for i := range bigJob.tasks {
			err = deleteJob(context, fmt.Sprintf("%s-%d", bigJob.name, i))
			Expect(err).NotTo(HaveOccurred())
		}

		// now the backfill job 2 can run
		err = waitPodGroupReady(context, bfPg2)
		Expect(err).NotTo(HaveOccurred())
	})

	FIt("Backfill scheduling", func() {
		context := initTestContext()
		defer cleanupTestContext(context)
		maxCnt := clusterSize(context, oneCPU)

		replicaset := createReplicaSet(context, "rs-1", maxCnt-2, "nginx", oneCPU)
		err := waitReplicaSetReady(context, replicaset.Name)
		Expect(err).NotTo(HaveOccurred())

		job := &jobSpec{
			name:      "gang-qj",
			namespace: context.namespace,
			tasks: []taskSpec{
				{
					img: "busybox",
					req: oneCPU,
					min: maxCnt,
					rep: maxCnt,
				},
			},
		}

		_, pg := createJob(context, job)
		err = waitPodGroupPending(context, pg)
		Expect(err).NotTo(HaveOccurred())

		// Job stuck in pending because no sufficient
		// resources are available.
		err = waitPodGroupUnschedulable(context, pg)
		Expect(err).NotTo(HaveOccurred())

		bfJob := &jobSpec{
			name:      "bf-qj",
			namespace: context.namespace,
			tasks: []taskSpec{
				{
					img: "busybox",
					req: oneCPU,
					min: 1,
					rep: 1,
				},
			},
		}

		// Submit bfJob which requires less resources.
		// bfJob will start running because backfill is enabled.
		_, bfPg := createJob(context, bfJob)
		err = waitPodGroupReady(context, bfPg)
		Expect(err).NotTo(HaveOccurred())

		// Delete bfJob
		for i := range bfJob.tasks {
			err = deleteJob(context, fmt.Sprintf("%s-%d", bfJob.name, i))
			Expect(err).NotTo(HaveOccurred())
		}

		// Delete replica set
		err = deleteReplicaSet(context, replicaset.Name)
		Expect(err).NotTo(HaveOccurred())

		// Original job should have enough resource to start
		err = waitPodGroupReady(context, pg)
		Expect(err).NotTo(HaveOccurred())
	})
})
