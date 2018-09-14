/*
Copyright 2018 Sysdig.

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

package main

import (
"bytes"
"encoding/json"
"fmt"
"io/ioutil"
"log"
"net/http"
"reflect"
"strings"
"sync"
"sort"
"CGSchudeler/kubernetes-scheduler/kubernetes"

)

// Retrieves the metrics information using a name node by calling the Sysdig Api
func GetMetrics(hostname string) (metricValue float64, err error) {
	hostFilter := fmt.Sprintf(`host.hostName = '%s'`, hostname)
	start := -60 // TODO make this configurable by params
	end := 0
	sampling := 60 // TODO make this configurable by params

	metricDataResponse, err := sysdigAPI.GetData(metrics, start, end, sampling, hostFilter, "host")
	if err != nil {
		return
	} else if metricDataResponse.StatusCode != 200 {
		err = fmt.Errorf("metric data response: %s", metricDataResponse.Status)
		return
	}
	defer metricDataResponse.Body.Close()

	all, err := ioutil.ReadAll(metricDataResponse.Body)

	var metricData struct {
		Data []struct {
			D []float64 `json:"d"`
		} `json:"data"`
	}

	err = json.Unmarshal(all, &metricData)
	if err != nil {
		return
	}

	if len(metricData.Data) > 0 && len(metricData.Data[0].D) > 0 {
		metricValue = metricData.Data[0].D[0]
	} else {
		err = noDataFound
	}

	return
}

var bestNodeMutex sync.Mutex

// Calculates the best node based in the metrics provided form a list of node names
func GetBestNodeByMetrics(nodes []string) (bestNodeFound Node, err error) {
	bestNodeMutex.Lock()
	defer bestNodeMutex.Unlock()

	if len(nodes) == 0 {
		err = emptyNodeList
		return
	}

	// If the best node was cached, return it
	if cachedNodes, ok := cachedNodes.Data(); ok {
		if reflect.DeepEqual(cachedNodes, nodes) {
			if bestNode, ok := bestCachedNode.Data(); ok {
				log.Println("Using cache...")
				return bestNode.(Node), nil
			}
		}
	}

	// We will make all the request asynchronous for performance reasons
	wg := sync.WaitGroup{}
	nodeStatsChannel := make(chan Node, len(nodes))
	nodeStatsErrorsChannel := make(chan Node, len(nodes))

	// Launch all requests asynchronously
	// to retrieve the metrics of each node
	for _, node := range nodes {
		wg.Add(1)

		go func(nodeName string) {
			defer wg.Done()

			split := strings.Split(nodeName, ".")
			nodeNameLittle := split[0]

			metricsValue, err := GetMetrics(nodeNameLittle)
			if err == nil { // No error found, we will send the struct
				nodeStatsChannel <- Node{name: nodeName, metric: metricsValue}
			} else {
				nodeStatsErrorsChannel <- Node{name: nodeName, err: err}
			}
		}(node)
	}

	wg.Wait()
	close(nodeStatsChannel)
	close(nodeStatsErrorsChannel)

	// Fill the list with all the succeeded nodes
	nodeList := NodeList{}
	for node := range nodeStatsChannel {
		nodeList = append(nodeList, node)
	}
	if len(nodeList) == 0 {
		err = noNodeFound
	}

	// Calculate the best node
	bestNodeFound, err = BestNodeFromList(nodeList)
	if err != nil {
		return
	}

	// Print any errors found
	errorHappenedString := `Error retrieving node "%s": "%s" \n`
	for node := range nodeStatsErrorsChannel {
		log.Printf(errorHappenedString, node.name, node.err.Error())
	}

	// No errors found? Cache the result
	if err == nil {
		bestCachedNode.SetData(bestNodeFound)
	}

	return
}

// Sorts the list and returns the best node
func BestNodeFromList(list NodeList) (node Node, err error) {
	sort.Sort(list)

	length := len(list)
	if length == 0 {
		return node, emptyNodeList
	}

	if sysdigMetricLower {
		return list[0], nil // Get the first -> Lower
	} else {
		return list[length-1], nil // Get the last -> Higher
	}
}

// Returns a list of all the available nodes found in the Kubernetes cluster
func NodesAvailable() (readyNodes []string) {
	if nodes, ok := cachedNodes.Data(); ok {
		return nodes.([]string)
	}

	nodes, err := kubeAPI.ListNodes()
	if err != nil {
		log.Println(err)
	}
	for _, node := range nodes {
		for _, status := range node.Status.Conditions {
			if status.Status == "True" && status.Type == "Ready" {
				readyNodes = append(readyNodes, node.Metadata.Name)
			}
		}
	}

	 cachedNodes.SetData(readyNodes)
	return readyNodes
}

func FindDeploymentNameFromPod(pod kubernetes.KubePod) (deploymentName string, err error) {
	if pod.Metadata.OwnerReferences[0].Kind == "ReplicaSet" {
		replicaSet, err := kubeAPI.ListNamespacedReplicaset(pod.Metadata.Namespace, pod.Metadata.OwnerReferences[0].Name)
		if err != nil {
			return "", err
		}
		if replicaSet.Metadata.OwnerReferences[0].Kind == "Deployment" {
			return replicaSet.Metadata.OwnerReferences[0].Name, nil
		}
	}
	return "", fmt.Errorf("%s is not supported yet as a OwnerReference", pod.Metadata.OwnerReferences[0].Kind)
}

// Binds a pod with a node in a namespace
func Scheduler(podName, nodeName, namespace string) (response *http.Response, err error) {
	if namespace == "" {
		namespace = "default"
	}

	body := map[string]interface{}{
		"target": map[string]string{
			"kind":       "Node",
			"apiVersion": "v1",
			"name":       nodeName,
			"namespace":  namespace,
		},
		"metadata": map[string]string{
			"name":      podName,
			"namespace": namespace,
		},
	}
	data, err := json.Marshal(body)
	if err != nil {
		return
	}

	return kubeAPI.CreateNamespacedBinding(namespace, bytes.NewReader(data))
}
