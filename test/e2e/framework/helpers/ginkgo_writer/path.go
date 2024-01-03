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

package ginkgo_writer

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/onsi/ginkgo/v2"

	"github.com/apache/yunikorn-k8shim/test/e2e/framework/configmanager"
)

func SetGinkgoWriterToLocal(suiteName string) {
	artifactPath := getArtifactPath()
	ensureArtifactPathExists(artifactPath)
	setGinkgoWriterToFile(filepath.Join(artifactPath, suiteName+".txt"))
}

func getArtifactPath() string {
	defaultArtifactPath := "/tmp/e2e-test-artifacts"
	if value, ok := os.LookupEnv(configmanager.ArtifactPathEnv); ok {
		ginkgo.By("Found artifact path in Env variable: " + value)
		return value
	}
	ginkgo.By("Using default artifact path: " + defaultArtifactPath)
	return configmanager.DefaultArtifactPath
}

func ensureArtifactPathExists(artifactPath string) {
	if _, err := os.Stat(artifactPath); os.IsNotExist(err) {
		err = os.MkdirAll(artifactPath, 0755)
		if err != nil {
			panic(err)
		}
		ginkgo.By("Created artifact directory: " + artifactPath)
	}
}

func setGinkgoWriterToFile(filePath string) {
	file, err := os.Create(filePath)
	if err != nil {
		ginkgo.Fail(fmt.Sprintf("Failed to create artifact file: %v", err))
	}
	writer := NewWriter(file)
	ginkgo.GinkgoWriter = writer
	// Should close previous writer?
}
