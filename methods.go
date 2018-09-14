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
"log"
"net/http"
"CGSchudeler/kubernetes-scheduler/kubernetes"


)

type Node1 struct {
	name   string
	metric float64
	err    error
}

type NodeList1 []Node1



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




func Decision (pod kubernetes.KubePod , ResourceAvailable []string, Resources []string , Information_table [][]interface{} )string   {
	//the image asked by the user
	PendingImage:=pod.Spec.Containers[0].Image
	fmt.Println(PendingImage)
	var ResourceSelected string
	checkImage,repeat_image,ResourceIndex:=CheckInformationTable(PendingImage,Information_table) // call the CheckInformationTable function

	if ( len(ResourceAvailable) > 0 ){
		if (checkImage){
			if (len(ResourceAvailable)==1) { // check if the cluster has single resource available
				ResourceSelected = ResourceAvailable[0] //  assign the only resource available to the resource selected

			}else if (len(ResourceAvailable)==len(Resources) ) { // check if all resource in the cluster are available
				if (checkImage){ // check if the image asked is recorded
					if (repeat_image==1) { // check if the image asked recoded one time
						index:=ResourceIndex[0] // affect the resource index
						ResourceSelected=(Information_table[index][2]).(string) // affect the resource selected

					} else { // if more than image per resource recorded , call the function select best resource
						ResourceSelected = SelectBestResource(ResourceIndex , Information_table  , ResourceAvailable ,true)

					}
				} else  { // if the image is not recorded

					ResourceSelected =MathModel(ResourceAvailable) //call the mathematical model
				}


			} else if( len(ResourceAvailable) < len(Resources)) { // if some resource are available and another not

				check_resource :=CheckResourceFill(ResourceAvailable ,Information_table  , ResourceIndex ) // call the check resource fill to fill the check resource table

				if ((checkImage) && (len(check_resource)>=1) ) { // chkeck if the image is recorded and at least one of its resouce is available


					if ((repeat_image >= 1) && ( (len(check_resource)==1)) ) { // check if one resource is available
						//check_resource  := (Information_table[ResourceIndex[0]][2]).(string) // affect the resource of the image available

						if (CheckResourceAvailable( ResourceAvailable,check_resource[0])){ // check if the resource of the image recorded avaialbe or not
							ResourceSelected=check_resource[0]

						} else { ResourceSelected =MathModel(ResourceAvailable) }


					} else  if ( (repeat_image > 1) && len(check_resource)>1){ // if the image is recorded more than one time and its reources are available


						ResourceSelected = SelectBestResource(ResourceIndex , Information_table  , ResourceAvailable ,true) // select the best resource

					} else { ResourceSelected =MathModel(ResourceAvailable)

					}



				} else { ResourceSelected =MathModel(ResourceAvailable) }


			}

		} else  { // if the image is not recorded

			ResourceSelected =MathModel(ResourceAvailable)
		}

	}




	return ResourceSelected

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
