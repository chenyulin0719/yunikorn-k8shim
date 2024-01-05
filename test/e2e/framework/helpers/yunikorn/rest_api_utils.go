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

package yunikorn

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/wait"

	"github.com/apache/yunikorn-core/pkg/webservice/dao"
	"github.com/apache/yunikorn-k8shim/test/e2e/framework/configmanager"
)

const DefaultPartition = "default"

type RClient struct {
	BaseURL   *url.URL
	UserAgent string

	httpClient *http.Client
}

func (c *RClient) newRequest(method, path string, body interface{}) (*http.Request, error) {
	rel := &url.URL{Path: path}
	if c.BaseURL == nil {
		c.BaseURL = &url.URL{Host: GetYKHost(),
			Scheme: GetYKScheme()}
	}
	u := c.BaseURL.ResolveReference(rel)
	if c.httpClient == nil {
		c.httpClient = http.DefaultClient
	}
	if len(c.UserAgent) == 0 {
		c.UserAgent = "Golang_Spider_Bot/3.0"
	}
	var buf io.ReadWriter
	if body != nil {
		buf = new(bytes.Buffer)
		err := json.NewEncoder(buf).Encode(body)
		if err != nil {
			return nil, err
		}
	}
	req, err := http.NewRequest(method, u.String(), buf)
	if err != nil {
		return nil, err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", c.UserAgent)
	return req, nil
}
func (c *RClient) do(req *http.Request, v interface{}) (*http.Response, error) {
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	err = json.NewDecoder(resp.Body).Decode(v)
	return resp, err
}

func (c *RClient) GetQueues(partition string) (*dao.PartitionQueueDAOInfo, error) {
	req, err := c.newRequest("GET", fmt.Sprintf(configmanager.QueuesPath, partition), nil)
	if err != nil {
		return nil, err
	}
	var queues *dao.PartitionQueueDAOInfo
	_, err = c.do(req, &queues)
	return queues, err
}

func (c *RClient) GetHealthCheck() (dao.SchedulerHealthDAOInfo, error) {
	req, err := c.newRequest("GET", configmanager.HealthCheckPath, nil)
	if err != nil {
		return dao.SchedulerHealthDAOInfo{}, err
	}
	healthCheck := dao.SchedulerHealthDAOInfo{}
	_, err = c.do(req, &healthCheck)
	return healthCheck, err
}

func (c *RClient) WaitforQueueToAppear(partition string, queueName string, timeout int) error {
	return wait.PollUntilContextTimeout(context.TODO(), 300*time.Microsecond, time.Duration(timeout)*time.Second, false, c.IsQueuePresent(partition, queueName).WithContext())
}

func (c *RClient) IsQueuePresent(partition string, queueName string) wait.ConditionFunc {
	return func() (bool, error) {
		req, err := c.newRequest("GET", fmt.Sprintf(configmanager.QueuesPath, partition), nil)
		if err != nil {
			return false, nil // returning nil here for wait & loop
		}
		var queues *dao.PartitionQueueDAOInfo
		_, err = c.do(req, &queues)
		if err != nil {
			return false, err
		}
		if queueName == configmanager.RootQueue && queues.QueueName == configmanager.RootQueue {
			return true, nil
		}
		for idx := range queues.Children {
			if queues.Children[idx].QueueName == queueName {
				return true, nil
			}
		}
		return false, nil
	}
}

func (c *RClient) GetAllAppInfos() (*dao.ApplicationsDAOInfo, error) {
	appsInfos := new(dao.ApplicationsDAOInfo)

	req, err := c.newRequest("GET", configmanager.AppsPath, nil)
	if err != nil {
		return nil, err
	}

	_, err = c.do(req, &(appsInfos.Applications))
	if err != nil {
		return nil, err
	}
	return appsInfos, nil
}

func (c *RClient) GetApps(partition string, queueName string) ([]*dao.ApplicationDAOInfo, error) {
	req, err := c.newRequest("GET", fmt.Sprintf(configmanager.AppsPath, partition, queueName), nil)
	if err != nil {
		return nil, err
	}
	var apps []*dao.ApplicationDAOInfo
	_, err = c.do(req, &apps)
	return apps, err
}

func (c *RClient) GetAppInfo(partition string, queueName string, appID string) (*dao.ApplicationDAOInfo, error) {
	req, err := c.newRequest("GET", fmt.Sprintf(configmanager.AppPath, partition, queueName, appID), nil)
	if err != nil {
		return nil, err
	}
	var app *dao.ApplicationDAOInfo
	_, err = c.do(req, &app)
	return app, err
}

func (c *RClient) GetCompletedAppInfo(partition string, appID string) (*dao.ApplicationDAOInfo, error) {
	req, err := c.newRequest("GET", fmt.Sprintf(configmanager.CompletedAppPath, partition), nil)
	if err != nil {
		return nil, err
	}
	var apps []*dao.ApplicationDAOInfo
	_, err = c.do(req, &apps)
	if err != nil {
		return nil, err
	}

	for _, app := range apps {
		if app.ApplicationID == appID {
			return app, nil
		}
	}
	return nil, fmt.Errorf("App is not in 'Failed', 'Expired', 'Completed' state.")
}

func (c *RClient) GetAllocationLog(partition string, queueName string, appID string, podName string) ([]*dao.AllocationAskLogDAOInfo, error) {
	reqs, err := c.GetAppInfo(partition, queueName, appID)
	if err != nil {
		return nil, err
	}

	if len(reqs.Requests) > 0 {
		for _, req := range reqs.Requests {
			if len(req.AllocationTags) > 0 && len(req.AllocationLog) > 0 && req.AllocationTags["kubernetes.io/meta/podName"] == podName {
				return req.AllocationLog, nil
			}
		}
	}
	return nil, errors.New("allocation is empty")
}

func (c *RClient) isAllocLogPresent(partition string, queueName string, appID string, podName string) wait.ConditionFunc {
	return func() (bool, error) {
		log, err := c.GetAllocationLog(partition, queueName, appID, podName)
		if err != nil {
			return false, nil
		}
		return log != nil, nil
	}
}

func (c *RClient) WaitForAllocationLog(partition string, queueName string, appID string, podName string, timeout int) error {
	if err := wait.PollUntilContextTimeout(context.TODO(), time.Second, time.Duration(timeout)*time.Second, false, c.isAllocLogPresent(partition, queueName, appID, podName).WithContext()); err != nil {
		return err
	}

	// got at least one entry, wait a few seconds to ensure we get all the entries
	time.Sleep(2 * time.Second)
	return nil
}

func (c *RClient) isAppInDesiredState(partition string, queue string, appID string, state string) wait.ConditionFunc {
	return func() (bool, error) {
		appInfo, err := c.GetAppInfo(partition, queue, appID)
		if err != nil {
			return false, nil // returning nil here for wait & loop
		}
		switch appInfo.State {
		case state:
			return true, nil
		case States().Application.Rejected:
			return false, fmt.Errorf(fmt.Sprintf("App not in desired state: %s", state))
		}
		return false, nil
	}
}

func (c *RClient) isAppInDesiredCompletedState(partition string, appID string, state string) wait.ConditionFunc {
	// Completed state including 'Expired', 'Completed', 'Failed'
	return func() (bool, error) {
		appInfo, err := c.GetCompletedAppInfo(partition, appID)
		if err != nil {
			return false, nil // returning nil here for wait & loop
		}

		if appInfo.State == state {
			return true, nil
		}
		return false, nil
	}
}

func (c *RClient) GetNodes(partition string) (*[]dao.NodeDAOInfo, error) {
	req, err := c.newRequest("GET", fmt.Sprintf(configmanager.NodesPath, partition), nil)
	if err != nil {
		return nil, err
	}
	var nodes []dao.NodeDAOInfo
	_, err = c.do(req, &nodes)
	return &nodes, err
}

func (c *RClient) WaitForAppStateTransition(partition string, queue string, appID string, state string, timeout int) error {
	return wait.PollUntilContextTimeout(context.TODO(), time.Millisecond*300, time.Duration(timeout)*time.Second, false, c.isAppInDesiredState(partition, queue, appID, state).WithContext())
}

func (c *RClient) WaitForCompletedAppStateTransition(partition string, appID string, state string, timeout int) error {
	return wait.PollUntilContextTimeout(context.TODO(), time.Millisecond*300, time.Duration(timeout)*time.Second, false, c.isAppInDesiredCompletedState(partition, appID, state).WithContext())
}

func (c *RClient) AreAllExecPodsAllotted(partition string, queueName string, appID string, execPodCount int) wait.ConditionFunc {
	return func() (bool, error) {
		appInfo, err := c.GetAppInfo(partition, queueName, appID)
		if err != nil {
			return false, err
		}
		if appInfo.Allocations == nil {
			return false, fmt.Errorf(fmt.Sprintf("Allocations are not yet complete for appID: %s", appID))
		}
		if len(appInfo.Allocations) >= execPodCount {
			return true, nil
		}
		return false, nil
	}
}

func (c *RClient) ValidateSchedulerConfig(cm v1.ConfigMap) (*dao.ValidateConfResponse, error) {
	validateConfResponse := new(dao.ValidateConfResponse)
	req, err := c.newRequest("POST", configmanager.ValidateConfPath, &cm)
	if err != nil {
		return validateConfResponse, err
	}
	_, err = c.do(req, validateConfResponse)
	return validateConfResponse, err
}

func isRootSched(policy string) wait.ConditionFunc {
	return func() (bool, error) {
		restClient := RClient{}
		qInfo, err := restClient.GetQueues(DefaultPartition)
		if err != nil {
			return false, err
		}
		if qInfo == nil {
			return false, errors.New("no response from rest client")
		}

		if policy == DefaultPartition {
			return len(qInfo.Properties) == 0, nil
		} else if qInfo.Properties["application.sort.policy"] == policy {
			return true, nil
		}

		return false, nil
	}
}

func WaitForSchedPolicy(policy string, timeout time.Duration) error {
	return wait.PollUntilContextTimeout(context.TODO(), 2*time.Second, timeout, false, isRootSched(policy).WithContext())
}

func GetFailedHealthChecks() (string, error) {
	restClient := RClient{}
	var failCheck string
	healthCheck, err := restClient.GetHealthCheck()
	if err != nil {
		return "", fmt.Errorf("failed to get scheduler health check from API")
	}
	if !healthCheck.Healthy {
		for _, check := range healthCheck.HealthChecks {
			if !check.Succeeded {
				failCheck += fmt.Sprintln("Name:", check.Name, "Message:", check.DiagnosisMessage)
			}
		}
	}
	return failCheck, nil
}

func (c *RClient) GetQueue(partition string, queueName string) (*dao.PartitionQueueDAOInfo, error) {
	queues, err := c.GetQueues(partition)
	if err != nil {
		return nil, err
	}
	if queueName == "root" {
		return queues, nil
	}

	var allSubQueues = queues.Children
	for _, subQ := range allSubQueues {
		if subQ.QueueName == queueName {
			return &subQ, nil
		}
	}
	return nil, fmt.Errorf("QueueInfo not found: %s", queueName)
}

// ConditionFunc returns true if queue timestamp property equals ts
// Expects queuePath to use periods as delimiters. ie "root.queueA.child"
func compareQueueTS(queuePathStr string, ts string) wait.ConditionFunc {
	return func() (bool, error) {
		restClient := RClient{}
		qInfo, err := restClient.GetQueue(DefaultPartition, queuePathStr)
		if err != nil {
			return false, err
		}

		return qInfo.Properties["timestamp"] == ts, nil
	}
}

// Expects queuePath to use periods as delimiters. ie "root.queueA.child"
func WaitForQueueTS(queuePathStr string, ts string, timeout time.Duration) error {
	return wait.PollUntilContextTimeout(context.TODO(), 2*time.Second, timeout, false, compareQueueTS(queuePathStr, ts).WithContext())
}

func AllocLogToStrings(log []*dao.AllocationAskLogDAOInfo) []string {
	result := make([]string, 0)
	for _, entry := range log {
		result = append(result, entry.Message)
	}
	return result
}

func (c *RClient) LogAppsInfo(ns string) error {
	appsInfo, getAppErr := c.GetApps(DefaultPartition, "root."+ns)
	if getAppErr != nil {
		return getAppErr
	}
	appJSON, appJSONErr := json.MarshalIndent(appsInfo, "", "    ")
	if appJSONErr != nil {
		return appJSONErr
	}
	By("Apps REST API response is\n" + string(appJSON))
	return nil
}

func (c *RClient) LogQueuesInfo() error {
	qInfo, getQErr := c.GetPartitions(DefaultPartition)
	if getQErr != nil {
		return getQErr
	}
	qJSON, qJSONErr := json.MarshalIndent(qInfo, "", "    ")
	if qJSONErr != nil {
		return getQErr
	}
	By("Queues REST API response is\n" + string(qJSON))
	return nil
}

func (c *RClient) LogNodesInfo() error {
	nodesInfo, getNodeErr := c.GetNodes(DefaultPartition)
	if getNodeErr != nil {
		return getNodeErr
	}
	nodesJSON, nodeJSONErr := json.MarshalIndent(nodesInfo, "", "    ")
	if nodeJSONErr != nil {
		return nodeJSONErr
	}
	By("Node REST API response is\n" + string(nodesJSON))
	return nil
}

func (c *RClient) GetPartitions(partition string) (*dao.PartitionQueueDAOInfo, error) {
	req, err := c.newRequest("GET", fmt.Sprintf(configmanager.QueuesPath, partition), nil)
	if err != nil {
		return nil, err
	}
	var partitions *dao.PartitionQueueDAOInfo
	_, err = c.do(req, &partitions)
	return partitions, err
}

func (c *RClient) GetUserUsage(partition string, userName string) (*dao.UserResourceUsageDAOInfo, error) {
	req, err := c.newRequest("GET", fmt.Sprintf(configmanager.UserUsagePath, partition, userName), nil)
	if err != nil {
		return nil, err
	}
	var userUsage *dao.UserResourceUsageDAOInfo
	_, err = c.do(req, &userUsage)
	return userUsage, err
}

func (c *RClient) GetGroupUsage(partition string, groupName string) (*dao.GroupResourceUsageDAOInfo, error) {
	req, err := c.newRequest("GET", fmt.Sprintf(configmanager.GroupUsagePath, partition, groupName), nil)
	if err != nil {
		return nil, err
	}
	var groupUsage *dao.GroupResourceUsageDAOInfo
	_, err = c.do(req, &groupUsage)
	return groupUsage, err
}
