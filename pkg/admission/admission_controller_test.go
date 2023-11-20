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
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"testing"

	"gotest.tools/v3/assert"

	admissionv1 "k8s.io/api/admission/v1"
	appsv1 "k8s.io/api/apps/v1"
	authv1 "k8s.io/api/authentication/v1"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"

	"github.com/apache/yunikorn-k8shim/pkg/admission/common"
	"github.com/apache/yunikorn-k8shim/pkg/admission/conf"
	"github.com/apache/yunikorn-k8shim/pkg/common/constants"
)

type responseMode int

const (
	Success                 = responseMode(0)
	Failure                 = responseMode(1)
	Error                   = responseMode(2)
	validUserInfoAnnotation = "{\"user\":\"test\",\"groups\":[\"devops\",\"system:authenticated\"]}"
)

// nolint: funlen
func TestUpdateApplicationInfo(t *testing.T) {
	// verify when appId/queue are not given,
	// we patch it correctly. (Add appId/queueName/disableStateAware to labels and annotations)
	var patch []common.PatchOperation

	pod := &v1.Pod{
		TypeMeta: metav1.TypeMeta{
			Kind:       "Pod",
			APIVersion: "v1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:            "a-test-pod",
			Namespace:       "default",
			UID:             "7f5fd6c5d5",
			ResourceVersion: "10654",
		},
		Spec:   v1.PodSpec{},
		Status: v1.PodStatus{},
	}

	c := createAdmissionControllerForTest()

	patch = c.updateApplicationInfo("default", pod, patch)

	labelsInPatch := fetchPatchValues(constants.AddPatchOp, constants.LabelPatchPath, patch)
	annotationsInPatch := fetchPatchValues(constants.AddPatchOp, constants.AnnotationPatchPath, patch)

	assert.Equal(t, len(patch), 2)
	assert.Equal(t, len(labelsInPatch), 3)
	assert.Equal(t, labelsInPatch["queue"], "root.default")
	assert.Equal(t, labelsInPatch["disableStateAware"], "true")
	assert.Equal(t, strings.HasPrefix(labelsInPatch["applicationId"], constants.AutoGenAppPrefix), true)
	assert.Equal(t, len(annotationsInPatch), 3)
	assert.Equal(t, annotationsInPatch["yunikorn.apache.org/queue"], "root.default")
	assert.Equal(t, annotationsInPatch["yunikorn.apache.org/disable-state-aware"], "true")
	assert.Equal(t, strings.HasPrefix(annotationsInPatch["yunikorn.apache.org/app-id"], constants.AutoGenAppPrefix), true)

	// verify if applicationId is given in the pod's labels,
	// we won't modify it in admission controller, and will patch the value to annotations
	patch = make([]common.PatchOperation, 0)

	pod = &v1.Pod{
		TypeMeta: metav1.TypeMeta{
			Kind:       "Pod",
			APIVersion: "v1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:            "a-test-pod",
			Namespace:       "default",
			UID:             "7f5fd6c5d5",
			ResourceVersion: "10654",
			Labels: map[string]string{
				"applicationId": "app-0001",
			},
		},
		Spec:   v1.PodSpec{},
		Status: v1.PodStatus{},
	}
	patch = c.updateApplicationInfo("default", pod, patch)

	labelsInPatch = fetchPatchValues(constants.AddPatchOp, constants.LabelPatchPath, patch)
	annotationsInPatch = fetchPatchValues(constants.AddPatchOp, constants.AnnotationPatchPath, patch)

	assert.Equal(t, len(patch), 2)
	assert.Equal(t, len(labelsInPatch), 2)
	assert.Equal(t, labelsInPatch["queue"], "root.default")
	assert.Equal(t, labelsInPatch["applicationId"], "app-0001")
	assert.Equal(t, len(patch), 2)
	assert.Equal(t, len(annotationsInPatch), 2)
	assert.Equal(t, annotationsInPatch["yunikorn.apache.org/queue"], "root.default")
	assert.Equal(t, annotationsInPatch["yunikorn.apache.org/app-id"], "app-0001")

	// verify if queue is given in the pod's labels,
	// we won't modify it in admission controller, and will patch the value to annotations
	patch = make([]common.PatchOperation, 0)

	pod = &v1.Pod{
		TypeMeta: metav1.TypeMeta{
			Kind:       "Pod",
			APIVersion: "v1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:            "a-test-pod",
			Namespace:       "default",
			UID:             "7f5fd6c5d5",
			ResourceVersion: "10654",
			Labels: map[string]string{
				"queue": "root.abc",
			},
		},
		Spec:   v1.PodSpec{},
		Status: v1.PodStatus{},
	}

	patch = c.updateApplicationInfo("default", pod, patch)

	labelsInPatch = fetchPatchValues(constants.AddPatchOp, constants.LabelPatchPath, patch)
	annotationsInPatch = fetchPatchValues(constants.AddPatchOp, constants.AnnotationPatchPath, patch)

	assert.Equal(t, len(patch), 2)
	assert.Equal(t, len(labelsInPatch), 3)
	assert.Equal(t, labelsInPatch["queue"], "root.abc")
	assert.Equal(t, labelsInPatch["disableStateAware"], "true")
	assert.Equal(t, strings.HasPrefix(labelsInPatch["applicationId"], constants.AutoGenAppPrefix), true)
	assert.Equal(t, len(annotationsInPatch), 3)
	assert.Equal(t, annotationsInPatch["yunikorn.apache.org/queue"], "root.abc")
	assert.Equal(t, annotationsInPatch["yunikorn.apache.org/disable-state-aware"], "true")
	assert.Equal(t, strings.HasPrefix(annotationsInPatch["yunikorn.apache.org/app-id"], constants.AutoGenAppPrefix), true)

	// verify if applicationId is given in the pod's annotations,
	// we won't modify it in admission controller, and will patch the value to labels
	patch = make([]common.PatchOperation, 0)

	pod = &v1.Pod{
		TypeMeta: metav1.TypeMeta{
			Kind:       "Pod",
			APIVersion: "v1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:            "a-test-pod",
			Namespace:       "default",
			UID:             "7f5fd6c5d5",
			ResourceVersion: "10654",
			Annotations: map[string]string{
				"yunikorn.apache.org/app-id": "app-0001",
			},
		},
		Spec:   v1.PodSpec{},
		Status: v1.PodStatus{},
	}
	patch = c.updateApplicationInfo("default", pod, patch)

	labelsInPatch = fetchPatchValues(constants.AddPatchOp, constants.LabelPatchPath, patch)
	annotationsInPatch = fetchPatchValues(constants.AddPatchOp, constants.AnnotationPatchPath, patch)

	assert.Equal(t, len(patch), 2)
	assert.Equal(t, len(labelsInPatch), 2)
	assert.Equal(t, labelsInPatch["queue"], "root.default")
	assert.Equal(t, labelsInPatch["applicationId"], "app-0001")
	assert.Equal(t, len(patch), 2)
	assert.Equal(t, len(annotationsInPatch), 2)
	assert.Equal(t, annotationsInPatch["yunikorn.apache.org/queue"], "root.default")
	assert.Equal(t, annotationsInPatch["yunikorn.apache.org/app-id"], "app-0001")

	// if both annotation and labels patch existing
	// we won't create new patch and will add new labels/annotation to the existing patch.
	patch = make([]common.PatchOperation, 0)
	patch = append(patch, common.PatchOperation{
		Op:   constants.AddPatchOp,
		Path: constants.LabelPatchPath,
		Value: map[string]string{
			"existingLabelKey": "existingLabelValue",
		},
	})
	patch = append(patch, common.PatchOperation{
		Op:   constants.AddPatchOp,
		Path: constants.AnnotationPatchPath,
		Value: map[string]string{
			"existingAnnotationKey": "existingAnnotationValue",
		},
	})
	pod = &v1.Pod{}

	patch = c.updateApplicationInfo("default", pod, patch)

	labelsInPatch = fetchPatchValues(constants.AddPatchOp, constants.LabelPatchPath, patch)
	annotationsInPatch = fetchPatchValues(constants.AddPatchOp, constants.AnnotationPatchPath, patch)

	assert.Equal(t, len(labelsInPatch), 4)
	assert.Equal(t, labelsInPatch["existingLabelKey"], "existingLabelValue")
	assert.Equal(t, labelsInPatch["queue"], "root.default")
	assert.Equal(t, labelsInPatch["disableStateAware"], "true")
	assert.Equal(t, strings.HasPrefix(labelsInPatch["applicationId"], constants.AutoGenAppPrefix), true)
	assert.Equal(t, len(annotationsInPatch), 4)
	assert.Equal(t, annotationsInPatch["existingAnnotationKey"], "existingAnnotationValue")
	assert.Equal(t, annotationsInPatch["yunikorn.apache.org/queue"], "root.default")
	assert.Equal(t, annotationsInPatch["yunikorn.apache.org/disable-state-aware"], "true")
	assert.Equal(t, strings.HasPrefix(annotationsInPatch["yunikorn.apache.org/app-id"], constants.AutoGenAppPrefix), true)
}

func TestUpdateLabelPatch(t *testing.T) {
	pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Labels: map[string]string{
				"label_key_in_pod": "label_value_in_pod",
			},
		},
	}

	// Verify if no patch on labels in patch list,
	// create a new patch with pods's existing labels and put new labels into the patch
	patch := make([]common.PatchOperation, 0)
	newLabels := map[string]string{
		"new_key_1": "new_value_1",
		"new_key_2": "new_value_2",
	}
	patch = updateLabelPatch(pod, newLabels, patch)
	result := fetchPatchValues(constants.AddPatchOp, constants.LabelPatchPath, patch)

	assert.Equal(t, len(result), 3)
	assert.Equal(t, result["label_key_in_pod"], "label_value_in_pod")
	assert.Equal(t, result["new_key_1"], "new_value_1")
	assert.Equal(t, result["new_key_2"], "new_value_2")

	// Verify if have patch on labels in patch list,
	// won't crete new patch and will add new labels into the patch
	patch = make([]common.PatchOperation, 0)
	patch = append(patch, common.PatchOperation{
		Op:   constants.AddPatchOp,
		Path: constants.LabelPatchPath,
		Value: map[string]string{
			"label_key_in_patch": "label_value_in_patch",
		},
	})
	newLabels = map[string]string{
		"new_key_1": "new_value_1",
		"new_key_2": "new_value_2",
	}

	patch = updateLabelPatch(pod, newLabels, patch)
	result = fetchPatchValues(constants.AddPatchOp, constants.LabelPatchPath, patch)

	assert.Equal(t, len(result), 3)
	assert.Equal(t, result["label_key_in_patch"], "label_value_in_patch")
	assert.Equal(t, result["new_key_1"], "new_value_1")
	assert.Equal(t, result["new_key_2"], "new_value_2")
}

func TestUpdateAnnotationPatch(t *testing.T) {
	pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Annotations: map[string]string{
				"annotation_key_in_pod": "annotation_value_in_pod",
			},
		},
	}

	// Verify if no patch on annotations in patch list,
	// create a new patch with pods's existing annotations and put new annotation into the patch
	patch := make([]common.PatchOperation, 0)
	newAnnotations := map[string]string{
		"new_key_1": "new_value_1",
		"new_key_2": "new_value_2",
	}
	patch = updateAnnotationPatch(pod, newAnnotations, patch)
	result := fetchPatchValues(constants.AddPatchOp, constants.AnnotationPatchPath, patch)

	assert.Equal(t, len(result), 3)
	assert.Equal(t, result["annotation_key_in_pod"], "annotation_value_in_pod")
	assert.Equal(t, result["new_key_1"], "new_value_1")
	assert.Equal(t, result["new_key_2"], "new_value_2")

	// Verify if have patch on annotation in patch list,
	// won't crete new patch and will add new annotation into the patch
	patch = make([]common.PatchOperation, 0)
	patch = append(patch, common.PatchOperation{
		Op:   constants.AddPatchOp,
		Path: constants.AnnotationPatchPath,
		Value: map[string]string{
			"annotation_key_in_patch": "annotation_value_in_patch",
		},
	})
	newAnnotations = map[string]string{
		"new_key_1": "new_value_1",
		"new_key_2": "new_value_2",
	}

	patch = updateAnnotationPatch(pod, newAnnotations, patch)
	result = fetchPatchValues(constants.AddPatchOp, constants.AnnotationPatchPath, patch)

	assert.Equal(t, len(result), 3)
	assert.Equal(t, result["annotation_key_in_patch"], "annotation_value_in_patch")
	assert.Equal(t, result["new_key_1"], "new_value_1")
	assert.Equal(t, result["new_key_2"], "new_value_2")
}

func fetchPatchValues(op string, path string, patch []common.PatchOperation) map[string]string {
	// fetch an existing patch by op and path
	for _, p := range patch {
		if p.Op == op && p.Path == path {
			return p.Value.(map[string]string)
		}
	}
	return make(map[string]string)
}

func TestUpdateSchedulerName(t *testing.T) {
	var patch []common.PatchOperation
	patch = updateSchedulerName(patch)
	assert.Equal(t, len(patch), 1)
	assert.Equal(t, patch[0].Op, constants.AddPatchOp)
	assert.Equal(t, patch[0].Path, constants.SchedulerNamePatchPath)
	if name, ok := patch[0].Value.(string); ok {
		assert.Equal(t, name, constants.SchedulerName)
	} else {
		t.Fatal("patch info content is not as expected")
	}
}

func TestValidateConfigMapEmpty(t *testing.T) {
	pcCache := createPriorityClassCacheForTest()
	nsCache := createNamespaceClassCacheForTest()
	controller := InitAdmissionController(createConfig(), pcCache, nsCache)
	configmap := &v1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name: constants.ConfigMapName,
		},
		Data: make(map[string]string),
	}
	err := controller.validateConfigMap("yunikorn", configmap)
	assert.NilError(t, err, "unexpected error with missing config")
}

const ConfigData = `
partitions:
  - name: default
    placementrules:
        - name: tag
          value: namespace
          create: true
    queues:
      - name: root
        submitacl: "*"
`

// Test for the case when the POST request is successful, and the config change is allowed.
func TestValidateConfigMapValidConfig(t *testing.T) {
	configmap := prepareConfigMap(ConfigData)
	srv := serverMock(Success)
	defer srv.Close()
	// both server and url pattern contains http://, so we need to delete one
	controller := prepareController(t, strings.Replace(srv.URL, "http://", "", 1), "", "", "", "", false, true)
	err := controller.validateConfigMap("yunikorn", configmap)
	assert.NilError(t, err, "No error expected")
}

// Test for the case when the POST request is successful, but the config change is not allowed.
func TestValidateConfigMapInvalidConfig(t *testing.T) {
	configmap := prepareConfigMap(ConfigData)
	srv := serverMock(Failure)
	defer srv.Close()
	// both server and url pattern contains http://, so we need to delete one
	controller := prepareController(t, strings.Replace(srv.URL, "http://", "", 1), "", "", "", "", false, true)
	err := controller.validateConfigMap("default", configmap)
	assert.Assert(t, err != nil, "error not found")
	assert.Equal(t, "Invalid config", err.Error(),
		"Other error returned than the expected one")
}

// Test for the case when the POST request fails
func TestValidateConfigMapWrongRequest(t *testing.T) {
	configmap := prepareConfigMap(ConfigData)
	srv := serverMock(Failure)
	defer srv.Close()
	// the url is wrong, so the POST request will fail, and success will be assumed
	controller := prepareController(t, srv.URL, "", "", "", "", false, true)
	err := controller.validateConfigMap("yunikorn", configmap)
	assert.NilError(t, err, "No error expected")
}

// Test for the case of server error
func TestValidateConfigMapServerError(t *testing.T) {
	configmap := prepareConfigMap(ConfigData)
	srv := serverMock(Error)
	defer srv.Close()
	// both server and url pattern contains http://, so we need to delete one
	controller := prepareController(t, strings.Replace(srv.URL, "http://", "", 1), "", "", "", "", false, true)
	err := controller.validateConfigMap("yunikorn", configmap)
	assert.NilError(t, err, "No error expected")
}

func prepareConfigMap(data string) *v1.ConfigMap {
	configmap := &v1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name: constants.ConfigMapName,
		},
		Data: map[string]string{"queues.yaml": data},
	}
	return configmap
}

func prepareController(t *testing.T, url string, processNs string, bypassNs string, labelNs string, noLabelNs string, bypassAuth bool, trustControllers bool) *AdmissionController {
	pcCache := createPriorityClassCacheForTest()
	nsCache := createNamespaceClassCacheForTest()
	if bypassNs == "" {
		bypassNs = "^kube-system$"
	}
	config := createConfigWithOverrides(map[string]string{
		conf.AMWebHookSchedulerServiceAddress: url,
		conf.AMFilteringProcessNamespaces:     processNs,
		conf.AMFilteringBypassNamespaces:      bypassNs,
		conf.AMFilteringLabelNamespaces:       labelNs,
		conf.AMFilteringNoLabelNamespaces:     noLabelNs,
		conf.AMAccessControlBypassAuth:        fmt.Sprintf("%t", bypassAuth),
		conf.AMAccessControlTrustControllers:  fmt.Sprintf("%t", trustControllers),
		conf.AMAccessControlSystemUsers:       "^system:serviceaccount:kube-system:job-controller$,^system:serviceaccount:kube-system:deployment-controller$",
		conf.AMAccessControlExternalUsers:     "^testExtUser$",
		conf.AMAccessControlExternalGroups:    "^testExtGroup$",
	})
	return InitAdmissionController(config, pcCache, nsCache)
}

func serverMock(mode responseMode) *httptest.Server {
	handler := http.NewServeMux()
	switch mode {
	case Success:
		handler.HandleFunc("/ws/v1/validate-conf", successResponseMock)
	case Failure:
		handler.HandleFunc("/ws/v1/validate-conf", failedResponseMock)
	case Error:
		handler.HandleFunc("/ws/v1/validate-conf", errorResponseMock)
	}
	srv := httptest.NewServer(handler)
	return srv
}

func successResponseMock(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(200)
	resp := `{
		"allowed": true,
		"reason": ""
		}`
	w.Write([]byte(resp)) //nolint:errcheck
}

func failedResponseMock(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(200)
	resp := `{
		"allowed": false,
		"reason": "Invalid config"
		}`
	w.Write([]byte(resp)) //nolint:errcheck
}

func errorResponseMock(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(500)
	resp := `{}`
	w.Write([]byte(resp)) //nolint:errcheck
}

// nolint: funlen
func TestMutate(t *testing.T) {
	var ac *AdmissionController
	var pod v1.Pod
	var req *admissionv1.AdmissionRequest
	var resp *admissionv1.AdmissionResponse
	var podJSON []byte
	var err error

	ac = prepareController(t, "", "", "^kube-system$,^bypass$", "", "^nolabel$", false, true)

	// nil request
	resp = ac.mutate(nil)
	assert.Check(t, !resp.Allowed, "response allowed with nil request")

	// yunikorn pod
	pod = v1.Pod{ObjectMeta: metav1.ObjectMeta{
		Namespace: "test-ns",
		Labels:    map[string]string{"app": "yunikorn"},
	}}
	req = &admissionv1.AdmissionRequest{
		UID:       "test-uid",
		Namespace: "test-ns",
		Kind:      metav1.GroupVersionKind{Kind: "Pod"},
	}
	podJSON, err = json.Marshal(pod)
	assert.NilError(t, err, "failed to marshal pod")
	req.Object = runtime.RawExtension{Raw: podJSON}
	resp = ac.mutate(req)
	assert.Check(t, resp.Allowed, "response not allowed for yunikorn pod")
	assert.Equal(t, len(resp.Patch), 0, "non-empty patch for yunikorn pod")

	// pod with empty namespace should be scheduled to 'default' namespace
	pod = v1.Pod{ObjectMeta: metav1.ObjectMeta{
		Namespace: "",
	}}
	req = &admissionv1.AdmissionRequest{
		UID:       "test-uid",
		Namespace: "",
		Kind:      metav1.GroupVersionKind{Kind: "Pod"},
	}
	podJSON, err = json.Marshal(pod)
	assert.NilError(t, err, "failed to marshal pod")
	req.Object = runtime.RawExtension{Raw: podJSON}
	resp = ac.mutate(req)
	assert.Check(t, resp.Allowed, "response not allowed for pod")
	assert.Equal(t, schedulerName(t, resp.Patch), "yunikorn", "yunikorn not set as scheduler for pod")
	assert.Equal(t, annotations(t, resp.Patch)["yunikorn.apache.org/app-id"], "yunikorn-default-autogen", "wrong app-id annotation")
	assert.Equal(t, annotations(t, resp.Patch)["yunikorn.apache.org/disable-state-aware"], "true", "missing disable-state-aware annotation")
	assert.Equal(t, annotations(t, resp.Patch)["yunikorn.apache.org/queue"], "root.default", "incorrect queue name")

	// pod without applicationID
	pod = v1.Pod{ObjectMeta: metav1.ObjectMeta{
		Namespace: "test-ns",
	}}
	req = &admissionv1.AdmissionRequest{
		UID:       "test-uid",
		Namespace: "test-ns",
		Kind:      metav1.GroupVersionKind{Kind: "Pod"},
	}
	podJSON, err = json.Marshal(pod)
	assert.NilError(t, err, "failed to marshal pod")
	req.Object = runtime.RawExtension{Raw: podJSON}
	resp = ac.mutate(req)
	assert.Check(t, resp.Allowed, "response not allowed for pod")
	assert.Equal(t, schedulerName(t, resp.Patch), "yunikorn", "yunikorn not set as scheduler for pod")
	assert.Equal(t, annotations(t, resp.Patch)["yunikorn.apache.org/app-id"], "yunikorn-test-ns-autogen", "wrong app-id annotation")
	assert.Equal(t, annotations(t, resp.Patch)["yunikorn.apache.org/disable-state-aware"], "true", "missing disable-state-aware annotation")

	// pod with applicationId
	pod.ObjectMeta.Labels = map[string]string{"applicationId": "test-app"}
	podJSON, err = json.Marshal(pod)
	assert.NilError(t, err, "failed to marshal pod")
	req.Object = runtime.RawExtension{Raw: podJSON}
	resp = ac.mutate(req)
	assert.Check(t, resp.Allowed, "response not allowed for pod")
	assert.Equal(t, schedulerName(t, resp.Patch), "yunikorn", "yunikorn not set as scheduler for pod")
	assert.Equal(t, annotations(t, resp.Patch)["yunikorn.apache.org/app-id"], "test-app", "wrong app-id annotation")

	// pod in bypassed namespace
	pod = v1.Pod{ObjectMeta: metav1.ObjectMeta{
		Namespace: "bypass",
	}}
	req = &admissionv1.AdmissionRequest{
		UID:       "test-uid",
		Namespace: "bypass",
		Kind:      metav1.GroupVersionKind{Kind: "Pod"},
	}
	podJSON, err = json.Marshal(pod)
	assert.NilError(t, err, "failed to marshal pod")
	req.Object = runtime.RawExtension{Raw: podJSON}
	resp = ac.mutate(req)
	assert.Check(t, resp.Allowed, "response not allowed for bypassed pod")
	assert.Equal(t, len(resp.Patch), 0, "non-empty patch for bypassed pod")

	// pod in no-label namespace
	pod = v1.Pod{ObjectMeta: metav1.ObjectMeta{
		Namespace: "nolabel",
	}}
	req = &admissionv1.AdmissionRequest{
		UID:       "test-uid",
		Namespace: "nolabel",
		Kind:      metav1.GroupVersionKind{Kind: "Pod"},
	}
	podJSON, err = json.Marshal(pod)
	assert.NilError(t, err, "failed to marshal pod")
	req.Object = runtime.RawExtension{Raw: podJSON}
	resp = ac.mutate(req)
	assert.Check(t, resp.Allowed, "response not allowed for nolabel pod")
	assert.Equal(t, schedulerName(t, resp.Patch), "yunikorn", "yunikorn not set as scheduler for nolabel pod")
	assert.Equal(t, annotations(t, resp.Patch)["yunikorn.apache.org/app-id"], nil, "non-empty app-id annotation for nolabel pod")
	assert.Equal(t, annotations(t, resp.Patch)["yunikorn.apache.org/disable-state-aware"], nil, "non-empty disable-state-aware annotation for nolabel pod")
	assert.Equal(t, annotations(t, resp.Patch)["yunikorn.apache.org/queue"], nil, "non-empty queue annotation for nolabel pod")

	// unknown object type
	pod = v1.Pod{ObjectMeta: metav1.ObjectMeta{
		Namespace: "test-ns",
	}}
	req = &admissionv1.AdmissionRequest{
		UID:       "test-uid",
		Namespace: "test-ns",
		Kind:      metav1.GroupVersionKind{Kind: "Pod"},
	}
	podJSON, err = json.Marshal(pod)
	assert.NilError(t, err, "failed to marshal pod")
	req.Object = runtime.RawExtension{Raw: podJSON}
	req.Kind = metav1.GroupVersionKind{Kind: "Unknown"}
	resp = ac.mutate(req)
	assert.Check(t, resp.Allowed, "response not allowed for unknown object type")
	assert.Equal(t, len(resp.Patch), 0, "non-empty patch for unknown object type")

	// deployment - annotation is set
	deployment := &appsv1.Deployment{
		Spec: appsv1.DeploymentSpec{
			Template: v1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Annotations: map[string]string{
						common.UserInfoAnnotation: validUserInfoAnnotation,
					},
				},
			},
		},
	}
	req = &admissionv1.AdmissionRequest{
		UID:       "test-uid",
		Namespace: "test-ns",
		Kind:      metav1.GroupVersionKind{Kind: "Deployment"},
		UserInfo: authv1.UserInfo{
			Username: "testExtUser",
		},
	}
	var deploymentJSON []byte
	deploymentJSON, err = json.Marshal(deployment)
	assert.NilError(t, err, "failed to marshal deployment")
	req.Object = runtime.RawExtension{Raw: deploymentJSON}
	resp = ac.mutate(req)
	assert.Check(t, resp.Allowed, "response not allowed for unknown object type")
	assert.Equal(t, len(resp.Patch), 0, "non-empty patch for deployment")

	// deployment - annotation is not set
	delete(deployment.Spec.Template.Annotations, common.UserInfoAnnotation)
	deploymentJSON, err = json.Marshal(deployment)
	assert.NilError(t, err, "failed to marshal deployment")
	req.Object = runtime.RawExtension{Raw: deploymentJSON}
	resp = ac.mutate(req)
	assert.Check(t, resp.Allowed, "response not allowed for unknown object type")
	assert.Check(t, len(resp.Patch) > 0, "empty patch for deployment")
	annotations := annotationsFromDeployment(t, resp.Patch)
	assert.Equal(t, annotations[common.UserInfoAnnotation].(string), "{\"user\":\"testExtUser\"}")

	// deployment - annotation is not set, bypassAuth is enabled
	ac = prepareController(t, "", "", "^kube-system$,^bypass$", "", "^nolabel$", true, true)
	resp = ac.mutate(req)
	assert.Check(t, resp.Allowed, "response not allowed for unknown object type")
	assert.Equal(t, len(resp.Patch), 0, "non-empty patch for deployment")

	// replicaset - no patch for system user
	replicaSet := appsv1.ReplicaSet{
		Spec: appsv1.ReplicaSetSpec{
			Template: v1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "test-ns",
					Annotations: map[string]string{
						common.UserInfoAnnotation: validUserInfoAnnotation,
					},
				},
			},
		},
	}
	ac = prepareController(t, "", "", "^kube-system$,^bypass$", "", "^nolabel$", false, true)
	req = &admissionv1.AdmissionRequest{
		UID:       "test-uid",
		Namespace: "test-ns",
		Kind:      metav1.GroupVersionKind{Kind: "ReplicaSet"},
		UserInfo: authv1.UserInfo{
			Username: "system:serviceaccount:kube-system:deployment-controller",
			Groups:   []string{"system:serviceaccounts", "system:serviceaccounts:kube-system"},
		},
	}
	replicaSetJSON, err2 := json.Marshal(replicaSet)
	assert.NilError(t, err2, "failed to marshal replicaset")
	req.Object = runtime.RawExtension{Raw: replicaSetJSON}
	resp = ac.mutate(req)
	assert.Check(t, resp.Allowed, "response not allowed for replicaset")
	assert.Equal(t, 0, len(resp.Patch), "non-empty patch for replicaset")
}

func TestMutateUpdate(t *testing.T) {
	var ac *AdmissionController
	var pod v1.Pod
	var req *admissionv1.AdmissionRequest
	var resp *admissionv1.AdmissionResponse
	var podJSON []byte
	var err error

	ac = prepareController(t, "", "", "^kube-system$,^bypass$", "", "^nolabel$", true, true)

	// nil request
	resp = ac.mutate(nil)
	assert.Check(t, !resp.Allowed, "response allowed with nil request")

	// yunikorn pod
	pod = v1.Pod{ObjectMeta: metav1.ObjectMeta{
		Namespace: "test-ns",
		Labels:    map[string]string{"app": "yunikorn"},
	}}
	req = &admissionv1.AdmissionRequest{
		UID:       "test-uid",
		Namespace: "test-ns",
		Kind:      metav1.GroupVersionKind{Kind: "Pod"},
		Operation: admissionv1.Update,
	}
	podJSON, err = json.Marshal(pod)
	assert.NilError(t, err, "failed to marshal pod")
	req.Object = runtime.RawExtension{Raw: podJSON}
	req.OldObject = runtime.RawExtension{Raw: podJSON}
	resp = ac.mutate(req)
	assert.Check(t, resp.Allowed, "response not allowed for yunikorn pod")

	// pod in bypassed namespace
	pod = v1.Pod{ObjectMeta: metav1.ObjectMeta{
		Namespace: "bypass",
	}}
	req = &admissionv1.AdmissionRequest{
		UID:       "test-uid",
		Namespace: "bypass",
		Kind:      metav1.GroupVersionKind{Kind: "Pod"},
		Operation: admissionv1.Update,
	}
	podJSON, err = json.Marshal(pod)
	assert.NilError(t, err, "failed to marshal pod")
	req.Object = runtime.RawExtension{Raw: podJSON}
	req.OldObject = runtime.RawExtension{Raw: podJSON}
	resp = ac.mutate(req)
	assert.Check(t, resp.Allowed, "response not allowed for bypassed pod")

	// normal pod, not allowed
	pod = v1.Pod{ObjectMeta: metav1.ObjectMeta{
		Namespace: "test-ns",
	}}
	req = &admissionv1.AdmissionRequest{
		UID:       "test-uid",
		Namespace: "test-ns",
		Kind:      metav1.GroupVersionKind{Kind: "Pod"},
		Operation: admissionv1.Update,
	}
	podJSON, err = json.Marshal(pod)
	assert.NilError(t, err, "failed to marshal pod")
	req.Object = runtime.RawExtension{Raw: podJSON}
	resp = ac.mutate(req)
	assert.Check(t, !resp.Allowed, "response was allowed")

	// normal pod, allowed
	pod = v1.Pod{ObjectMeta: metav1.ObjectMeta{
		Namespace: "test-ns",
		Annotations: map[string]string{
			common.UserInfoAnnotation: validUserInfoAnnotation,
		},
	}}
	oldPod := v1.Pod{ObjectMeta: metav1.ObjectMeta{
		Namespace: "test-ns",
		Labels: map[string]string{
			"test": "yunikorn",
		},
		Annotations: map[string]string{
			common.UserInfoAnnotation: validUserInfoAnnotation,
		},
	}}
	req = &admissionv1.AdmissionRequest{
		UID:       "test-uid",
		Namespace: "test-ns",
		Kind:      metav1.GroupVersionKind{Kind: "Pod"},
		Operation: admissionv1.Update,
	}
	podJSON, err = json.Marshal(pod)
	assert.NilError(t, err, "failed to marshal pod")
	oldPodJSON, err2 := json.Marshal(oldPod)
	assert.NilError(t, err2, "failed to marshal pod")
	req.Object = runtime.RawExtension{Raw: podJSON}
	req.OldObject = runtime.RawExtension{Raw: oldPodJSON}
	resp = ac.mutate(req)
	assert.Check(t, resp.Allowed, "response was not allowed")
}

func TestExternalAuthentication(t *testing.T) {
	ac := prepareController(t, "", "", "^kube-system$,^bypass$", "", "^nolabel$", false, true)

	// validation fails, submitter user is not whitelisted
	pod := v1.Pod{ObjectMeta: metav1.ObjectMeta{
		Namespace: "test-ns",
		Annotations: map[string]string{
			common.UserInfoAnnotation: validUserInfoAnnotation,
		},
	}}
	podJSON, err := json.Marshal(pod)
	assert.NilError(t, err, "failed to marshal pod")
	req := &admissionv1.AdmissionRequest{
		UID:       "test-uid",
		Namespace: "test-ns",
		Kind:      metav1.GroupVersionKind{Kind: "Pod"},
		UserInfo: authv1.UserInfo{
			Username: "test",
			Groups:   []string{"dev"},
		},
	}
	req.Object = runtime.RawExtension{Raw: podJSON}
	req.Kind = metav1.GroupVersionKind{Kind: "Pod"}
	resp := ac.mutate(req)
	assert.Check(t, !resp.Allowed, "response was allowed")
	assert.Check(t, strings.Contains(resp.Result.Message, "not allowed to set user annotation"))

	// should pass as "testExtUser" is allowed to add the userInfo annotation
	req = &admissionv1.AdmissionRequest{
		UID:       "test-uid",
		Namespace: "test-ns",
		Kind:      metav1.GroupVersionKind{Kind: "Pod"},
		UserInfo: authv1.UserInfo{
			Username: "testExtUser",
			Groups:   []string{"dev"},
		},
	}
	req.Object = runtime.RawExtension{Raw: podJSON}
	req.Kind = metav1.GroupVersionKind{Kind: "Pod"}
	resp = ac.mutate(req)
	assert.Check(t, resp.Allowed, "response not allowed")

	// invalid annotation
	pod = v1.Pod{ObjectMeta: metav1.ObjectMeta{
		Namespace: "test-ns",
		Annotations: map[string]string{
			common.UserInfoAnnotation: "xyzxyz",
		},
	}}
	podJSON, err = json.Marshal(pod)
	assert.NilError(t, err, "failed to marshal pod")
	req.Object = runtime.RawExtension{Raw: podJSON}
	req.Kind = metav1.GroupVersionKind{Kind: "Pod"}
	resp = ac.mutate(req)
	assert.Check(t, !resp.Allowed, "response was allowed")
	assert.Check(t, strings.Contains(resp.Result.Message, "invalid character 'x'"))

	// deployment
	deployment := appsv1.Deployment{
		Spec: appsv1.DeploymentSpec{
			Template: v1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "test-ns",
					Annotations: map[string]string{
						common.UserInfoAnnotation: validUserInfoAnnotation,
					},
				},
			},
		},
	}
	deploymentJSON, err2 := json.Marshal(deployment)
	assert.NilError(t, err2, "failed to marshal deployment")
	req.Object = runtime.RawExtension{Raw: deploymentJSON}
	req.Kind = metav1.GroupVersionKind{Kind: "Deployment"}
	resp = ac.mutate(req)
	assert.Check(t, resp.Allowed, "response not allowed")

	// deployment - invalid annotation
	deployment.Spec.Template.Annotations[common.UserInfoAnnotation] = "xyzxyz"
	deploymentJSON, err = json.Marshal(deployment)
	req.Object = runtime.RawExtension{Raw: deploymentJSON}
	req.Kind = metav1.GroupVersionKind{Kind: "Deployment"}
	assert.NilError(t, err, "failed to marshal deployment")
	resp = ac.mutate(req)
	assert.Check(t, !resp.Allowed, "response was allowed")
	assert.Check(t, strings.Contains(resp.Result.Message, "invalid character 'x'"))

	// replicaset submitted by system user
	replicaSet := appsv1.ReplicaSet{
		Spec: appsv1.ReplicaSetSpec{
			Template: v1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "test-ns",
					Annotations: map[string]string{
						common.UserInfoAnnotation: validUserInfoAnnotation,
					},
				},
			},
		},
	}
	replicaSetJSON, err3 := json.Marshal(replicaSet)
	assert.NilError(t, err3, "failed to marshal replicaset")
	req.Object = runtime.RawExtension{Raw: replicaSetJSON}
	req.Kind = metav1.GroupVersionKind{Kind: "ReplicaSet"}
	req.UserInfo = authv1.UserInfo{
		Username: "system:serviceaccount:kube-system:deployment-controller",
		Groups:   []string{"system:serviceaccounts", "system:serviceaccounts:kube-system"},
	}
	resp = ac.mutate(req)
	assert.Check(t, resp.Allowed, "response was not allowed")

	// replicaset submitted by normal user
	req.UserInfo = authv1.UserInfo{
		Username: "testExtUser",
		Groups:   []string{"dev"},
	}
	resp = ac.mutate(req)
	assert.Check(t, resp.Allowed, "response was not allowed")
}

func parsePatch(t *testing.T, patch []byte) []common.PatchOperation {
	res := make([]common.PatchOperation, 0)
	if len(patch) == 0 {
		return res
	}
	err := json.Unmarshal(patch, &res)
	assert.NilError(t, err, "error unmarshalling patch")
	return res
}

func schedulerName(t *testing.T, patch []byte) string {
	ops := parsePatch(t, patch)
	for _, op := range ops {
		if op.Path == constants.SchedulerNamePatchPath {
			return op.Value.(string)
		}
	}
	return ""
}

func labels(t *testing.T, patch []byte) map[string]interface{} {
	ops := parsePatch(t, patch)
	for _, op := range ops {
		if op.Path == constants.LabelPatchPath {
			return op.Value.(map[string]interface{})
		}
	}
	return make(map[string]interface{})
}

func annotations(t *testing.T, patch []byte) map[string]interface{} {
	ops := parsePatch(t, patch)
	for _, op := range ops {
		if op.Path == constants.AnnotationPatchPath {
			return op.Value.(map[string]interface{})
		}
	}
	return make(map[string]interface{})
}

func annotationsFromDeployment(t *testing.T, patch []byte) map[string]interface{} {
	ops := parsePatch(t, patch)
	for _, op := range ops {
		if op.Path == "/spec/template/metadata/annotations" {
			return op.Value.(map[string]interface{})
		}
	}
	return make(map[string]interface{})
}

func TestShouldProcessNamespace(t *testing.T) {
	ac := prepareController(t, "", "", "^kube-system$,^pre-,-post$", "", "", false, true)

	assert.Check(t, ac.shouldProcessNamespace("test"), "test namespace not allowed")
	assert.Check(t, !ac.shouldProcessNamespace("kube-system"), "kube-system namespace allowed")
	assert.Check(t, ac.shouldProcessNamespace("x-kube-system"), "x-kube-system namespace not allowed")
	assert.Check(t, ac.shouldProcessNamespace("kube-system-x"), "kube-system-x namespace not allowed")
	assert.Check(t, !ac.shouldProcessNamespace("pre-ns"), "pre-ns namespace allowed")
	assert.Check(t, !ac.shouldProcessNamespace("ns-post"), "ns-post namespace allowed")

	ac = prepareController(t, "", "^allow-", "^allow-except-", "", "", false, true)
	assert.Check(t, !ac.shouldProcessNamespace("test"), "test namespace allowed when not on process list")
	assert.Check(t, ac.shouldProcessNamespace("allow-this"), "allow-this namespace not allowed when on process list")
	assert.Check(t, !ac.shouldProcessNamespace("allow-except-this"), "allow-except-this namespace allowed when on bypass list")

	ac = prepareController(t, "", "^ns-no-annotation$", "^ns-regexp-deny$", "", "", false, true)
	ac.nsCache.nameSpaces["ns-no-annotation"] = nsFlags{enableYuniKorn: UNSET, generateAppID: UNSET}
	ac.nsCache.nameSpaces["ns-process-true"] = nsFlags{enableYuniKorn: TRUE, generateAppID: UNSET}
	ac.nsCache.nameSpaces["ns-process-false"] = nsFlags{enableYuniKorn: FALSE, generateAppID: UNSET}
	assert.Check(t, ac.shouldProcessNamespace("ns-no-annotation"), "no annotation namespace allowed")
	assert.Check(t, ac.shouldProcessNamespace("ns-process-true"), "namespace process true")
	assert.Check(t, !ac.shouldProcessNamespace("ns-process-false"), "namespace process false")
	assert.Check(t, !ac.shouldProcessNamespace("unknown"), "namespace unknown not allowed")

	// check regexp override
	assert.Check(t, !ac.shouldProcessNamespace("ns-regexp-deny"), "namespace deny regexp not allowed")
	ac.nsCache.nameSpaces["ns-regexp-deny"] = nsFlags{enableYuniKorn: TRUE, generateAppID: UNSET}
	assert.Check(t, ac.shouldProcessNamespace("ns-regexp-deny"), "namespace override via annotation")
}

func TestShouldLabelNamespace(t *testing.T) {
	ac := prepareController(t, "", "", "", "", "^skip$", false, true)
	assert.Check(t, ac.shouldLabelNamespace("test"), "test namespace not allowed")
	assert.Check(t, !ac.shouldLabelNamespace("skip"), "skip namespace allowed when on no-label list")

	ac = prepareController(t, "", "", "", "^allow-", "^allow-except-", false, true)
	assert.Check(t, !ac.shouldLabelNamespace("test"), "test namespace allowed when not on label list")
	assert.Check(t, ac.shouldLabelNamespace("allow-this"), "allow-this namespace not allowed when on label list")
	assert.Check(t, !ac.shouldLabelNamespace("allow-except-this"), "allow-except-this namespace allowed when on no-label list")

	ac = prepareController(t, "", "", "", "^ns-no-annotation$", "^ns-regexp-deny$", false, true)
	ac.nsCache.nameSpaces["ns-no-annotation"] = nsFlags{enableYuniKorn: UNSET, generateAppID: UNSET}
	ac.nsCache.nameSpaces["ns-generate-true"] = nsFlags{enableYuniKorn: UNSET, generateAppID: TRUE}
	ac.nsCache.nameSpaces["ns-generate-false"] = nsFlags{enableYuniKorn: UNSET, generateAppID: FALSE}
	assert.Check(t, ac.shouldLabelNamespace("ns-no-annotation"), "no annotation namespace allowed")
	assert.Check(t, ac.shouldLabelNamespace("ns-generate-true"), "namespace generate true")
	assert.Check(t, !ac.shouldLabelNamespace("ns-generate-false"), "namespace generate false")
	assert.Check(t, !ac.shouldLabelNamespace("unknown"), "namespace unknown not allowed")

	// check regexp override
	assert.Check(t, !ac.shouldLabelNamespace("ns-regexp-deny"), "namespace deny regexp not allowed")
	ac.nsCache.nameSpaces["ns-regexp-deny"] = nsFlags{enableYuniKorn: UNSET, generateAppID: TRUE}
	assert.Check(t, ac.shouldLabelNamespace("ns-regexp-deny"), "namespace override via annotation")
}

func TestParseRegexes(t *testing.T) {
	var regexes []*regexp.Regexp
	var err error

	regexes, err = parseRegexes("")
	assert.NilError(t, err, "failed to parse empty pattern")
	assert.Equal(t, len(regexes), 0, "got results for empty pattern")

	regexes, err = parseRegexes("^test$")
	assert.NilError(t, err, "failed to parse simple pattern")
	assert.Equal(t, len(regexes), 1, "wrong count for simple pattern")
	assert.Check(t, regexes[0].MatchString("test"), "didn't match test")
	assert.Check(t, !regexes[0].MatchString("testx"), "matched testx")
	assert.Check(t, !regexes[0].MatchString("xtest"), "matched xtest")

	regexes, err = parseRegexes(" ^this$, ^that$ ")
	assert.NilError(t, err, "failed to parse compound pattern")
	assert.Equal(t, len(regexes), 2, "wrong count for compound pattern")
	assert.Check(t, regexes[0].MatchString("this"), "didn't match this")
	assert.Check(t, regexes[1].MatchString("that"), "didn't match that")

	regexes, err = parseRegexes("^a\\s+b$")
	assert.NilError(t, err, "failed to parse escaped pattern")
	assert.Equal(t, len(regexes), 1, "wrong count for escaped pattern")
	assert.Check(t, regexes[0].MatchString("a \t b"), "didn't match 'a \t b'")
	assert.Check(t, !regexes[0].MatchString("ab"), "matched 'ab'")

	_, err = parseRegexes("^($")
	assert.ErrorContains(t, err, "error parsing regexp", "bad pattern doesn't fail")
}

func TestInitAdmissionControllerRegexErrorHandling(t *testing.T) {
	pcCache := createPriorityClassCacheForTest()
	nsCache := createNamespaceClassCacheForTest()
	ac := InitAdmissionController(createConfig(), pcCache, nil)
	assert.Equal(t, 1, len(ac.conf.GetBypassNamespaces()))
	assert.Equal(t, conf.DefaultFilteringBypassNamespaces, ac.conf.GetBypassNamespaces()[0].String(), "didn't set default bypassNamespaces")

	ac = InitAdmissionController(createConfigWithOverrides(map[string]string{conf.AMFilteringProcessNamespaces: "("}), pcCache, nsCache)
	assert.Equal(t, 0, len(ac.conf.GetProcessNamespaces()), "didn't fail on bad processNamespaces list")

	ac = InitAdmissionController(createConfigWithOverrides(map[string]string{conf.AMFilteringBypassNamespaces: "("}), pcCache, nsCache)
	assert.Equal(t, 1, len(ac.conf.GetBypassNamespaces()))
	assert.Equal(t, conf.DefaultFilteringBypassNamespaces, ac.conf.GetBypassNamespaces()[0].String(), "didn't fail on bad bypassNamespaces list")

	ac = InitAdmissionController(createConfigWithOverrides(map[string]string{conf.AMFilteringLabelNamespaces: "("}), pcCache, nsCache)
	assert.Equal(t, 0, len(ac.conf.GetLabelNamespaces()), "didn't fail on bad labelNamespaces list")

	ac = InitAdmissionController(createConfigWithOverrides(map[string]string{conf.AMFilteringNoLabelNamespaces: "("}), pcCache, nsCache)
	assert.Equal(t, 0, len(ac.conf.GetNoLabelNamespaces()), "didn't fail on bad noLabelNamespaces list")

	ac = InitAdmissionController(createConfigWithOverrides(map[string]string{conf.AMAccessControlSystemUsers: "("}), pcCache, nsCache)
	assert.Equal(t, 1, len(ac.conf.GetSystemUsers()))
	assert.Equal(t, conf.DefaultAccessControlSystemUsers, ac.conf.GetSystemUsers()[0].String(), "didn't fail on bad systemUsers list")

	ac = InitAdmissionController(createConfigWithOverrides(map[string]string{conf.AMAccessControlExternalUsers: "("}), pcCache, nsCache)
	assert.Equal(t, 0, len(ac.conf.GetExternalUsers()), "didn't fail on bad externalUsers list")

	ac = InitAdmissionController(createConfigWithOverrides(map[string]string{conf.AMAccessControlExternalGroups: "("}), pcCache, nsCache)
	assert.Equal(t, 0, len(ac.conf.GetExternalGroups()), "didn't fail on bad externalGroups list")
}

func createPriorityClassCacheForTest() *PriorityClassCache {
	return &PriorityClassCache{
		priorityClasses: make(map[string]bool),
	}
}

func createNamespaceClassCacheForTest() *NamespaceCache {
	return &NamespaceCache{
		nameSpaces: make(map[string]nsFlags),
	}
}

func createAdmissionControllerForTest() *AdmissionController {
	pcCache := createPriorityClassCacheForTest()
	nsCache := createNamespaceClassCacheForTest()
	return InitAdmissionController(createConfig(), pcCache, nsCache)
}
