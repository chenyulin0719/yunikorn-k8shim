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

package user_group_limit_test

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	v1 "k8s.io/api/core/v1"

	"github.com/apache/yunikorn-core/pkg/common/resources"
	"github.com/apache/yunikorn-core/pkg/webservice/dao"
	amCommon "github.com/apache/yunikorn-k8shim/pkg/admission/common"
	amconf "github.com/apache/yunikorn-k8shim/pkg/admission/conf"
	"github.com/apache/yunikorn-k8shim/pkg/common/constants"
	tests "github.com/apache/yunikorn-k8shim/test/e2e"
	"github.com/apache/yunikorn-k8shim/test/e2e/framework/helpers/common"
	"github.com/apache/yunikorn-k8shim/test/e2e/framework/helpers/ginkgo_writer"
	"github.com/apache/yunikorn-k8shim/test/e2e/framework/helpers/k8s"
	"github.com/apache/yunikorn-k8shim/test/e2e/framework/helpers/yunikorn"
	"github.com/apache/yunikorn-scheduler-interface/lib/go/si"
)

type TestType int

const (
	largeMem    = 100
	mediumMem   = 50
	smallMem    = 30
	sleepPodMem = 99
	user1       = "user1"
	user2       = "user2"
	group1      = "group1"
	group2      = "group2"

	userTestType TestType = iota
	groupTestType
)

var (
	kClient               k8s.KubeCtl
	restClient            yunikorn.RClient
	ns                    *v1.Namespace
	dev                   = "dev" + common.RandSeq(5)
	oldConfigMap          = new(v1.ConfigMap)
	annotation            = "ann-" + common.RandSeq(10)
	admissionCustomConfig = map[string]string{
		"log.core.scheduler.ugm.level":   "debug",
		amconf.AMAccessControlBypassAuth: constants.True,
	}
)

var _ = ginkgo.BeforeSuite(func() {
	suiteName := "user_group_limit"
	ginkgo_writer.SetGinkgoWriterToLocal(suiteName)

	// Initializing kubectl client
	kClient = k8s.KubeCtl{}
	Ω(kClient.SetClient()).To(gomega.BeNil())
	// Initializing rest client
	restClient = yunikorn.RClient{}
	Ω(restClient).NotTo(gomega.BeNil())

	yunikorn.EnsureYuniKornConfigsPresent()

	ginkgo.By("Port-forward the scheduler pod")
	var err = kClient.PortForwardYkSchedulerPod()
	Ω(err).NotTo(gomega.HaveOccurred())

	ginkgo.By("create development namespace")
	ns, err = kClient.CreateNamespace(dev, nil)
	gomega.Ω(err).NotTo(gomega.HaveOccurred())
	gomega.Ω(ns.Status.Phase).To(gomega.Equal(v1.NamespaceActive))
})

var _ = ginkgo.AfterSuite(func() {
	ginkgo.By("Check Yunikorn's health")
	checks, err := yunikorn.GetFailedHealthChecks()
	Ω(err).NotTo(gomega.HaveOccurred())
	Ω(checks).To(gomega.Equal(""), checks)
	ginkgo.By("Tearing down namespace: " + ns.Name)
	err = kClient.TearDownNamespace(ns.Name)
	Ω(err).NotTo(gomega.HaveOccurred())
})

var _ = ginkgo.Describe("UserGroupLimit", func() {
	ginkgo.It("Verify_maxresources_with_a_specific_user_limit", func() {
		ginkgo.By("Do nothing")
	})

	ginkgo.AfterEach(func() {
		testDescription := ginkgo.CurrentSpecReport()
		if testDescription.Failed() {
			ginkgo.By(fmt.Sprintf("Dumping cluster status to artifact directory... (spec:%s)", testDescription.FullText()))
			fmt.Fprintf(ginkgo.GinkgoWriter, "Dump current spec: %s\n", testDescription.FullText())
			tests.LogTestClusterInfoWrapper(testDescription.FailureMessage(), []string{ns.Name})
			tests.LogYunikornContainer(testDescription.FailureMessage())
		}
		// Delete all sleep pods
		ginkgo.By("Delete all sleep pods")
		err := kClient.DeletePods(ns.Name)
		if err != nil {
			fmt.Fprintf(ginkgo.GinkgoWriter, "Failed to delete pods in namespace %s - reason is %s\n", ns.Name, err.Error())
		}

		// reset config
		ginkgo.By("Restoring YuniKorn configuration")
		// yunikorn.RestoreConfigMapWrapper(oldConfigMap, annotation)
	})
})

func deploySleepPod(usergroup *si.UserGroupInformation, queuePath string, expectedRunning bool, reason string) *v1.Pod {
	usergroupJsonBytes, err := json.Marshal(usergroup)
	Ω(err).NotTo(gomega.HaveOccurred())

	sleepPodConfig := k8s.SleepPodConfig{NS: dev, Mem: smallMem, Labels: map[string]string{constants.LabelQueueName: queuePath}}
	sleepPodObj, err := k8s.InitSleepPod(sleepPodConfig)
	Ω(err).NotTo(gomega.HaveOccurred())
	sleepPodObj.Annotations[amCommon.UserInfoAnnotation] = string(usergroupJsonBytes)

	ginkgo.By(fmt.Sprintf("%s deploys the sleep pod %s to queue %s", usergroup, sleepPodObj.Name, queuePath))
	sleepPod, err := kClient.CreatePod(sleepPodObj, dev)
	gomega.Ω(err).NotTo(gomega.HaveOccurred())

	if expectedRunning {
		ginkgo.By(fmt.Sprintf("The sleep pod %s can be scheduled %s", sleepPod.Name, reason))
		err = kClient.WaitForPodRunning(dev, sleepPod.Name, 60*time.Second)
		gomega.Ω(err).NotTo(gomega.HaveOccurred())
	} else {
		ginkgo.By(fmt.Sprintf("The sleep pod %s can't be scheduled %s", sleepPod.Name, reason))
		// Since Pending is the initial state of PodPhase, sleep for 5 seconds, then check whether the pod is still in Pending state.
		time.Sleep(5 * time.Second)
		err = kClient.WaitForPodPending(sleepPod.Namespace, sleepPod.Name, 60*time.Second)
		gomega.Ω(err).NotTo(gomega.HaveOccurred())
	}
	return sleepPod
}

func checkUsage(testType TestType, name string, queuePath string, expectedRunningPods []*v1.Pod) {
	var rootQueueResourceUsageDAO *dao.ResourceUsageDAOInfo
	if testType == userTestType {
		ginkgo.By(fmt.Sprintf("Check user resource usage for %s in queue %s", name, queuePath))
		userUsageDAOInfo, err := restClient.GetUserUsage(constants.DefaultPartition, name)
		Ω(err).NotTo(gomega.HaveOccurred())
		Ω(userUsageDAOInfo).NotTo(gomega.BeNil())

		rootQueueResourceUsageDAO = userUsageDAOInfo.Queues
	} else if testType == groupTestType {
		ginkgo.By(fmt.Sprintf("Check group resource usage for %s in queue %s", name, queuePath))
		groupUsageDAOInfo, err := restClient.GetGroupUsage(constants.DefaultPartition, name)
		Ω(err).NotTo(gomega.HaveOccurred())
		Ω(groupUsageDAOInfo).NotTo(gomega.BeNil())

		rootQueueResourceUsageDAO = groupUsageDAOInfo.Queues
	}
	Ω(rootQueueResourceUsageDAO).NotTo(gomega.BeNil())

	var resourceUsageDAO *dao.ResourceUsageDAOInfo
	for _, queue := range rootQueueResourceUsageDAO.Children {
		if queue.QueuePath == queuePath {
			resourceUsageDAO = queue
			break
		}
	}
	Ω(resourceUsageDAO).NotTo(gomega.BeNil())

	appIDs := make([]interface{}, 0, len(expectedRunningPods))
	for _, pod := range expectedRunningPods {
		appIDs = append(appIDs, pod.Labels[constants.LabelApplicationID])
	}
	Ω(resourceUsageDAO.ResourceUsage).NotTo(gomega.BeNil())
	Ω(resourceUsageDAO.ResourceUsage.Resources["pods"]).To(gomega.Equal(resources.Quantity(len(expectedRunningPods))))
	Ω(resourceUsageDAO.RunningApplications).To(gomega.ConsistOf(appIDs...))
}
