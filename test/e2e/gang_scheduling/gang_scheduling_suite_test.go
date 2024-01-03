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
	"path/filepath"
	"testing"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/ginkgo/v2/reporters"

	"github.com/onsi/gomega"
	v1 "k8s.io/api/core/v1"

	"github.com/apache/yunikorn-k8shim/test/e2e/framework/configmanager"
	"github.com/apache/yunikorn-k8shim/test/e2e/framework/helpers/common"
	"github.com/apache/yunikorn-k8shim/test/e2e/framework/helpers/ginkgo_writer"
	"github.com/apache/yunikorn-k8shim/test/e2e/framework/helpers/k8s"
	"github.com/apache/yunikorn-k8shim/test/e2e/framework/helpers/yunikorn"
)

func init() {
	configmanager.YuniKornTestConfig.ParseFlags()
}

func TestGangScheduling(t *testing.T) {
	ginkgo.ReportAfterSuite("TestGangScheduling", func(report ginkgo.Report) {
		err := common.CreateJUnitReportDir()
		Ω(err).NotTo(gomega.HaveOccurred())
		err = reporters.GenerateJUnitReportWithConfig(
			report,
			filepath.Join(configmanager.YuniKornTestConfig.LogDir, "TEST-gang_scheduling_junit.xml"),
			reporters.JunitReportConfig{OmitSpecLabels: true},
		)
		Ω(err).NotTo(HaveOccurred())
	})
	gomega.RegisterFailHandler(ginkgo.Fail)
	ginkgo.RunSpecs(t, "TestGangScheduling", ginkgo.Label("TestGangScheduling"))
}

var oldConfigMap = new(v1.ConfigMap)
var annotation = "ann-" + common.RandSeq(10)
var kClient = k8s.KubeCtl{} //nolint

// func GetArtifactPath() string {
// 	artifactPath := "ARTIFACT_PATH"
// 	defaultArtifactPath := "/tmp/e2e-test-artifacts"
// 	if value, ok := os.LookupEnv(artifactPath); ok {
// 		return value
// 	}
// 	return defaultArtifactPath
// }

// func ensureArtifactPathExists(artifactPath string) {
// 	if _, err := os.Stat(artifactPath); os.IsNotExist(err) {
// 		err = os.MkdirAll(artifactPath, 0755)
// 		if err != nil {
// 			panic(err)
// 		}
// 		ginkgo.By("Created artifact directory: " + artifactPath)
// 	} else {
// 		ginkgo.By("Artifact directory already exists: " + artifactPath)
// 	}
// }

// func setGinkgoWriterToFile(filePath string) {
// 	file, err := os.Create(filePath)
// 	if err != nil {
// 		ginkgo.Fail(fmt.Sprintf("Failed to create artifact file: %v", err))
// 	}
// 	writer := ginkgo_writer.NewWriter(file)
// 	ginkgo.GinkgoWriter = writer
// }

var _ = BeforeSuite(func() {
	suiteName := "gang_scheduling"
	ginkgo_writer.SetGinkgoWriterToLocal(suiteName)

	// file, err := os.Create(filepath.Join(artifactPath, suite_name+".txt"))
	// if err != nil {
	// 	ginkgo.Fail(fmt.Sprintf("Failed to create artifact file for %s : %v", suite_name, err))
	// }
	// // change from stdout to file
	// writer := ginkgo_writer.NewWriter(file)
	// ginkgo.GinkgoWriter = writer
	// ginkgo.GinkgoLogr = ginkgo_writer.GinkgoLogrFunc(writer)

	annotation = "ann-" + common.RandSeq(10)
	yunikorn.EnsureYuniKornConfigsPresent()
	yunikorn.UpdateConfigMapWrapper(oldConfigMap, "fifo", annotation)
})

var _ = AfterSuite(func() {
	yunikorn.RestoreConfigMapWrapper(oldConfigMap, annotation)
})

// Declarations for Ginkgo DSL
var Describe = ginkgo.Describe

var It = ginkgo.It
var PIt = ginkgo.PIt
var By = ginkgo.By
var BeforeSuite = ginkgo.BeforeSuite
var AfterSuite = ginkgo.AfterSuite
var BeforeEach = ginkgo.BeforeEach
var AfterEach = ginkgo.AfterEach
var DescribeTable = ginkgo.Describe
var Entry = ginkgo.Entry

// Declarations for Gomega Matchers
var Equal = gomega.Equal
var BeNumerically = gomega.BeNumerically
var Ω = gomega.Expect
var BeNil = gomega.BeNil
var HaveOccurred = gomega.HaveOccurred
