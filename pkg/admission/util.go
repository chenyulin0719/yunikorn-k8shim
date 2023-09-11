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

package admission

import (
	"fmt"
	"reflect"

	"github.com/google/uuid"
	"go.uber.org/zap"
	v1 "k8s.io/api/core/v1"

	"github.com/apache/yunikorn-k8shim/pkg/common/constants"
	"github.com/apache/yunikorn-k8shim/pkg/common/utils"
	"github.com/apache/yunikorn-k8shim/pkg/log"
)

func getAnnotationsForApplicationInfoUpdate(pod *v1.Pod, namespace string, generateUniqueAppIds bool, defaultQueueName string) map[string]string {
	annotations := make(map[string]string)
	appID := getApplicationIDFromPod(pod)
	if appID == "" {
		// if app id not exist, generate one
		// for each namespace, we group unnamed pods to one single app - if GenerateUniqueAppId is not set
		// if GenerateUniqueAppId:
		//		application ID convention: ${NAMESPACE}-${GENERATED_UUID}
		// else
		// 		application ID convention: ${AUTO_GEN_PREFIX}-${NAMESPACE}-${AUTO_GEN_SUFFIX}
		appID := generateAppID(namespace, generateUniqueAppIds)
		annotations[constants.AnnotationApplicationID] = appID

		// if we generate an app ID, disable state-aware scheduling for this app
		disableStateAware := "true"
		if value := utils.GetPodLabelValue(pod, constants.LabelDisableStateAware); value != "" {
			disableStateAware = value
		}
		annotations[constants.AnnotationDisableStateAware] = disableStateAware
	}

	// if app id not in pod annotation, add it
	if value := utils.GetPodAnnotationValue(pod, constants.AnnotationIgnoreApplication); value == "" {
		annotations[constants.AnnotationApplicationID] = appID
	}

	queueName := getQueueNameFromPod(pod)
	if queueName == "" {
		// if defaultQueueName is "", skip adding default queue name to the pod labels
		if defaultQueueName != "" {
			// for undefined configuration, am_conf will add 'root.default' to retain existing behavior
			// if a custom name is configured for default queue, it will be used instead of root.default
			queueName = defaultQueueName
		}
	}

	// if queue name not in pod annotation, add it
	if value := utils.GetPodAnnotationValue(pod, constants.AnnotationQueueName); value == "" {
		if queueName != "" {
			annotations[constants.AnnotationQueueName] = queueName
		}
	}

	return annotations
}

func updatePodAnnotation(pod *v1.Pod, key string, value string) map[string]string {
	existingAnnotations := pod.Annotations
	result := make(map[string]string)
	for k, v := range existingAnnotations {
		result[k] = v
	}
	result[key] = value
	return result
}

func convert2Namespace(obj interface{}) *v1.Namespace {
	if nameSpace, ok := obj.(*v1.Namespace); ok {
		return nameSpace
	}
	log.Log(log.AdmissionUtils).Warn("cannot convert to *v1.Namespace", zap.Stringer("type", reflect.TypeOf(obj)))
	return nil
}

// Generate a new uuid. The chance of getting duplicate are very small
func GetNewUUID() string {
	return uuid.NewString()
}

// generate appID based on the namespace value
// if configured to generate unique appID, generate appID as <namespace>-<pod-uid> namespace capped at 26chars
// if not set or configured as false, appID generated as <autogen-prefix>-<namespace>-<autogen-suffix>
func generateAppID(namespace string, generateUniqueAppIds bool) string {
	var generatedID string
	if generateUniqueAppIds {
		uuid := GetNewUUID()
		generatedID = fmt.Sprintf("%.26s-%s", namespace, uuid)
	} else {
		generatedID = fmt.Sprintf("%s-%s-%s", constants.AutoGenAppPrefix, namespace, constants.AutoGenAppSuffix)
	}

	return fmt.Sprintf("%.63s", generatedID)
}

func getApplicationIDFromPod(pod *v1.Pod) string {
	// if existing annotation exist, it takes priority over everything else
	if value := utils.GetPodAnnotationValue(pod, constants.AnnotationApplicationID); value != "" {
		return value
	}
	if value := utils.GetPodLabelValue(pod, constants.LabelApplicationID); value != "" {
		return value
	}
	// application ID can be defined in Spark Operator label
	if value := utils.GetPodLabelValue(pod, constants.SparkLabelAppID); value != "" {
		return value
	}
	return ""
}

func getQueueNameFromPod(pod *v1.Pod) string {
	// if existing annotation exist, it takes priority over everything else
	if value := utils.GetPodAnnotationValue(pod, constants.AnnotationQueueName); value != "" {
		return value
	} else if value := utils.GetPodLabelValue(pod, constants.LabelQueueName); value != "" {
		return value
	}
	return ""
}
