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

package bin_packing

import (
	"fmt"

	"github.com/onsi/ginkgo/v2"
	v1 "k8s.io/api/core/v1"

	tests "github.com/apache/yunikorn-k8shim/test/e2e"
	"github.com/apache/yunikorn-k8shim/test/e2e/framework/helpers/common"
)

var _ = Describe("", func() {
	var ns string

	BeforeEach(func() {
		ns = "ns-" + common.RandSeq(10)
		By(fmt.Sprintf("Create namespace: %s", ns))
		var ns1, err1 = kClient.CreateNamespace(ns, nil)
		Ω(err1).NotTo(HaveOccurred())
		Ω(ns1.Status.Phase).To(Equal(v1.NamespaceActive))
	})

	// Verify BinPacking Node Order Memory
	// 1. Sort nodes by increasing available memory: [nodeA, nodeB...]
	// 2. Increase nodeA by 20% of available memory. Add node=nodeA pod label.
	// 3. Increase nodeB by 10% of available memory
	// 4. Submit jobA of 3 pods. Verify all are allocated to nodeA
	// 5. Submit jobB of 3 pods with anti-affinity of node=nodeA. Verify all are allocated to nodeB
	It("Verify_BinPacking_Node_Order_Memory", func() {
		ginkgo.By("Do nothing")
	})

	AfterEach(func() {
		testDescription := ginkgo.CurrentSpecReport()
		if testDescription.Failed() {
			By(fmt.Sprintf("Dumping cluster status to artifact directory... (spec:%s)", testDescription.FullText()))
			fmt.Fprintf(ginkgo.GinkgoWriter, "Dump current spec: %s\n", testDescription.FullText())
			tests.LogTestClusterInfoWrapper(testDescription.FailureMessage(), []string{ns})
			tests.LogYunikornContainer(testDescription.FailureMessage())
		}
		By("Tear down namespace: " + ns)
		err := kClient.DeleteNamespace(ns)
		Ω(err).NotTo(HaveOccurred())
	})
})
