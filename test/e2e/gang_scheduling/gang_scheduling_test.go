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

package gangscheduling_test

import (
	"fmt"
	"strings"
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	batchv1 "k8s.io/api/batch/v1"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	tests "github.com/apache/yunikorn-k8shim/test/e2e"

	"github.com/apache/yunikorn-core/pkg/webservice/dao"
	"github.com/apache/yunikorn-k8shim/pkg/cache"
	"github.com/apache/yunikorn-k8shim/test/e2e/framework/configmanager"
	"github.com/apache/yunikorn-k8shim/test/e2e/framework/helpers/common"
	"github.com/apache/yunikorn-k8shim/test/e2e/framework/helpers/k8s"
	"github.com/apache/yunikorn-k8shim/test/e2e/framework/helpers/yunikorn"
)

const (
	groupA = "groupa"
	groupB = "groupb"
	groupC = "groupc"
)

var (
	restClient                yunikorn.RClient
	ns                        string
	nsQueue                   string
	appID                     string
	minResource               map[string]resource.Quantity
	jobNames                  []string
	unsatisfiableNodeSelector = map[string]string{"kubernetes.io/hostname": "unsatisfiable_node"}
)

var _ = Describe("", func() {
	BeforeEach(func() {
		kClient = k8s.KubeCtl{}
		Ω(kClient.SetClient()).To(BeNil())

		ns = "ns-" + common.RandSeq(10)
		// ns = "gang-test"
		nsQueue = "root." + ns
		By(fmt.Sprintf("Creating namespace: %s for sleep jobs", ns))
		namespace, err := kClient.CreateNamespace(ns, nil)
		Ω(err).NotTo(HaveOccurred())
		Ω(namespace.Status.Phase).To(Equal(v1.NamespaceActive))

		minResource = map[string]resource.Quantity{
			v1.ResourceCPU.String():    resource.MustParse("10m"),
			v1.ResourceMemory.String(): resource.MustParse("20M"),
		}
		jobNames = []string{}
		appID = "appid-" + common.RandSeq(5)
	})

	It("Verify_Hard_GS_Failed_State", func() {
		By("Do nothing")
	})

	AfterEach(func() {
		testDescription := ginkgo.CurrentSpecReport()
		// tests.LogTestClusterInfoWrapper(testDescription.FailureMessage(), []string{ns})
		// tests.LogYunikornContainer(testDescription.FailureMessage())

		if testDescription.Failed() {
			By(fmt.Sprintf("Dumping cluster status to artifact directory... (spec:%s)", testDescription.FullText()))
			fmt.Fprintf(ginkgo.GinkgoWriter, "Dump current spec: %s\n", testDescription.FullText())
			tests.LogTestClusterInfoWrapper(testDescription.FailureMessage(), []string{ns})
			tests.LogYunikornContainer(testDescription.FailureMessage())
		}

		By(fmt.Sprintf("Cleanup jobs: %v", jobNames))
		for _, jobName := range jobNames {
			err := kClient.DeleteJob(jobName, ns)
			Ω(err).NotTo(gomega.HaveOccurred())
		}

		By("Tear down namespace: " + ns)
		err := kClient.TearDownNamespace(ns)

		Ω(err).NotTo(HaveOccurred())
	})

})

func createJob(applicationID string, minResource map[string]resource.Quantity, annotations k8s.PodAnnotation, parallelism int32) (job *batchv1.Job) {
	var (
		err      error
		requests = v1.ResourceList{}
		limits   = v1.ResourceList{}
	)
	for k, v := range minResource {
		key := v1.ResourceName(k)
		requests[key] = v
		if strings.HasPrefix(k, v1.ResourceHugePagesPrefix) {
			limits[key] = v
		}
	}

	podConf := k8s.TestPodConfig{
		Labels: map[string]string{
			"app":           "sleep-" + common.RandSeq(5),
			"applicationId": applicationID,
		},
		Annotations: &annotations,
		Resources: &v1.ResourceRequirements{
			Requests: requests,
			Limits:   limits,
		},
	}
	jobConf := k8s.JobConfig{
		Name:        fmt.Sprintf("gangjob-%s-%s", applicationID, common.RandSeq(5)),
		Namespace:   ns,
		Parallelism: parallelism,
		PodConfig:   podConf,
	}

	job, err = k8s.InitJobConfig(jobConf)
	Ω(err).NotTo(HaveOccurred())

	// By("After InitJobConfig()")

	// time.Sleep(60 * time.Second)
	// By("After sleep InitJobConfig()")

	taskGroupsMap, err := k8s.PodAnnotationToMap(podConf.Annotations)
	Ω(err).NotTo(HaveOccurred())

	By(fmt.Sprintf("[%s] Deploy job %s with task-groups: %+v", applicationID, jobConf.Name, taskGroupsMap[k8s.TaskGroups]))
	job, err = kClient.CreateJob(job, ns)
	Ω(err).NotTo(HaveOccurred())

	// By("Before WaitForJobPodsCreated()")

	// time.Sleep(600 * time.Second)
	// By("After sleep WaitForJobPodsCreated()")

	err = kClient.WaitForJobPodsCreated(ns, job.Name, int(*job.Spec.Parallelism), 30*time.Second)
	Ω(err).NotTo(HaveOccurred())

	// By("After WaitForJobPodsCreated()")
	// time.Sleep(60 * time.Second)
	// By("After sleep #1)")

	jobNames = append(jobNames, job.Name) // for cleanup in afterEach function
	return job
}

func checkAppStatus(applicationID, state string) {
	By(fmt.Sprintf("Verify application %s status is %s", applicationID, state))
	// By(fmt.Sprintf("Verify nsQueue %s", nsQueue))
	// // create a for loop to check the status
	// for i := 0; i < 100; i++ {
	// 	applicationDAOInfo, _ := restClient.GetAppInfo(configmanager.DefaultPartition, nsQueue, applicationID)
	// 	By(fmt.Sprintf("Verify nsQueue %v", applicationDAOInfo.State))
	// 	time.Sleep(1 * time.Second)
	// }

	timeoutErr := restClient.WaitForAppStateTransition(configmanager.DefaultPartition, nsQueue, applicationID, state, 20)
	Ω(timeoutErr).NotTo(HaveOccurred())
}

func checkPlaceholderData(appDaoInfo *dao.ApplicationDAOInfo, tgName string, count, replaced, timeout int) {
	verified := false
	for _, placeholderData := range appDaoInfo.PlaceholderData {
		if tgName == placeholderData.TaskGroupName {
			Ω(int(placeholderData.Count)).To(Equal(count), "Placeholder count is not correct")
			Ω(int(placeholderData.Replaced)).To(Equal(replaced), "Placeholder replaced is not correct")
			Ω(int(placeholderData.TimedOut)).To(Equal(timeout), "Placeholder timeout is not correct")
			verified = true
			break
		}
	}
	Ω(verified).To(Equal(true), fmt.Sprintf("Can't find task group %s in app info", tgName))
}

func verifyOriginatorDeletionCase(withOwnerRef bool) {
	podConf := k8s.TestPodConfig{
		Name: "gang-driver-pod" + common.RandSeq(5),
		Labels: map[string]string{
			"app":           "sleep-" + common.RandSeq(5),
			"applicationId": appID,
		},
		Annotations: &k8s.PodAnnotation{
			TaskGroups: []cache.TaskGroup{
				{
					Name:         groupA,
					MinMember:    int32(3),
					MinResource:  minResource,
					NodeSelector: unsatisfiableNodeSelector,
				},
				{
					Name:        groupB,
					MinMember:   int32(3),
					MinResource: minResource,
				},
			},
		},
		Resources: &v1.ResourceRequirements{
			Requests: v1.ResourceList{
				"cpu":    minResource["cpu"],
				"memory": minResource["memory"],
			},
		},
	}

	podTest, err := k8s.InitTestPod(podConf)
	Ω(err).NotTo(HaveOccurred())

	if withOwnerRef {
		// create a configmap as ownerreference
		testConfigmap := &v1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Name: "test-cm",
				UID:  "test-cm-uid",
			},
			Data: map[string]string{
				"test": "test",
			},
		}
		defer func() {
			err := kClient.DeleteConfigMap(testConfigmap.Name, ns)
			Ω(err).NotTo(HaveOccurred())
		}()

		testConfigmap, err := kClient.CreateConfigMap(testConfigmap, ns)
		Ω(err).NotTo(HaveOccurred())

		podTest.OwnerReferences = []metav1.OwnerReference{
			{
				APIVersion: "v1",
				Kind:       "ConfigMap",
				Name:       testConfigmap.Name,
				UID:        testConfigmap.UID,
			},
		}
	}

	taskGroupsMap, annErr := k8s.PodAnnotationToMap(podConf.Annotations)
	Ω(annErr).NotTo(HaveOccurred())
	By(fmt.Sprintf("Deploy pod %s with task-groups: %+v", podTest.Name, taskGroupsMap[k8s.TaskGroups]))
	originator, err := kClient.CreatePod(podTest, ns)
	Ω(err).NotTo(HaveOccurred())

	checkAppStatus(appID, yunikorn.States().Application.Accepted)

	By("Wait for groupB placeholders running")
	stateRunning := v1.PodRunning
	runErr := kClient.WaitForPlaceholders(ns, "tg-"+appID+"-"+groupB+"-", 3, 30*time.Second, &stateRunning)
	Ω(runErr).NotTo(HaveOccurred())

	By("List placeholders")
	tgPods, listErr := kClient.ListPlaceholders(ns, "tg-"+appID+"-")
	Ω(listErr).NotTo(HaveOccurred())

	By("Delete originator pod")
	deleteErr := kClient.DeletePod(originator.Name, ns)
	Ω(deleteErr).NotTo(HaveOccurred())

	By("Verify placeholders deleted")
	for _, ph := range tgPods {
		deleteErr = kClient.WaitForPodTerminated(ns, ph.Name, 30*time.Second)
		Ω(deleteErr).NotTo(HaveOccurred(), "Placeholder %s still running", ph)
	}

	By("Verify app allocation is empty")
	appInfo, restErr := restClient.GetAppInfo(configmanager.DefaultPartition, nsQueue, appID)
	Ω(restErr).NotTo(HaveOccurred())
	Ω(len(appInfo.Allocations)).To(BeNumerically("==", 0))
}
