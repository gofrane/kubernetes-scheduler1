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

	"errors"
)

var Node string

type NodeList1 []string



// Returns a list of all the available nodes found in the Kubernetes cluster
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
	return
}

////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////Decision ///////////////////////////////
////////////////////////////////////////////////////////////////////////////////////



///////////////CheckInformationTable///////////////
//check the image asked is recorded on not

func CheckInformationTable(PindingImage string ,Information_table [][]interface{})(Check bool , repeat_image int ,ResourceIndex []int ){

	for i:=0; i<len(Information_table); i++ {
		if (PindingImage==Information_table[i][1]){ // check

			repeat_image=repeat_image+1
			Check=true
			ResourceIndex = append(ResourceIndex, i)
		}


	}
	return  Check,repeat_image,ResourceIndex

}

/////////////////////////////////////////////////////////////////////////////////////////////////////


/////////////////////////////CheckResourceAvailable//////////////////////

// check the node if available or not


func CheckResourceAvailable(ResourceAvailable []string,check_resource  string )bool {

	check := false
	i:=0
	for ((check ==false) && (i<len(ResourceAvailable))) {

		if (check_resource==ResourceAvailable[i]){
			check=true
		} else { i+=1
		}
	}
	return check
}

////////////////////////////////////////////////////



///////////////////////////////////////////////SelectBestResource///////////////
//select the best node

func SelectBestResource(Resourceindex []int , Information_table [][]interface{} , ResourceAvailable []string,all bool) string{

	selected:=(Information_table[Resourceindex[0]][2]).(string)
	i:=0
	k:=i+1
	for (all && i<(len(Resourceindex)-1)) {


		if ( CheckResourceAvailable(ResourceAvailable, (Information_table[Resourceindex[k]][2]).(string))) {

			val1 := Resourceindex[i]

			val2 := Resourceindex[k]

			time1 := (Information_table[val1][4]).(float64)

			time2 := (Information_table[val2][4]).(float64)
			input1 := (Information_table[val1][5]).(float64)
			input2 := (Information_table[val2][5]).(float64)
			ration1 := time1 / input1
			ration2 := time2 / input2

			if (ration1 > ration2) {
				selected = (Information_table[val2][2]).(string)

			}else { k=k+1

			}
		} else {

			i=i+1
			k=i+1

		}
		i=k
		k=k+1

	}
	return  selected
}
///////////////////////////////////////////////////////////////////////////////////////////////

////////////////////////////CheckResourceFill//////////////////////////////////


// fill the check resource table
func CheckResourceFill(ResourceAvailable []string ,Information_table [][]interface{} , ResourceIndex []int)[]string{
	var checkResource []string

	for i,_:=range(ResourceAvailable) {

		for _,v := range (ResourceIndex) {


			if (ResourceAvailable[i] == (Information_table[v][2]).(string)) {


				checkResource=append( checkResource,ResourceAvailable[i])


			}
		}
	}
	return checkResource
}

////////////////////////////////////////////////////////////////////////

////////////////////////////////////////////make the decseion ///////////////////////////////

func Decision (pod kubernetes.KubePod , ResourceAvailable []string, Lnode int , Information_table [][]interface{} )(ResourceSelected string, err error)    {
	//the image asked by the user
	PendingImage:=pod.Spec.Containers[0].Image
	fmt.Println(PendingImage)

	checkImage,repeat_image,ResourceIndex:=CheckInformationTable(PendingImage,Information_table) // call the CheckInformationTable function

	if  (len(ResourceAvailable) > 0){
		if (checkImage){
			if (len(ResourceAvailable)==1 ){ // check if the cluster has single resource available
				ResourceSelected = ResourceAvailable[0] //  assign the only resource available to the resource selected

			}else if (len(ResourceAvailable)==Lnode ) { // check if all resource in the cluster are available
				if (checkImage){ // check if the image asked is recorded
					if (repeat_image==1) { // check if the image asked recoded one time
						index:=ResourceIndex[0] // affect the resource index
						ResourceSelected=(Information_table[index][2]).(string) // affect the resource selected

					} else { // if more than image per resource recorded , call the function select best resource
						ResourceSelected = SelectBestResource(ResourceIndex , Information_table  , ResourceAvailable ,true)

					}
				} else  { // if the image is not recorded

					err=errors.New("image do not find  ")
				}


			} else if( len(ResourceAvailable) < Lnode) { // if some resource are available and another not

				check_resource :=CheckResourceFill(ResourceAvailable ,Information_table  , ResourceIndex ) // call the check resource fill to fill the check resource table

				if ((checkImage) && (len(check_resource)>=1) ) { // check if the image is recorded and at least one of its resouce is available


					if ((repeat_image >= 1) && ( (len(check_resource)==1)) ) { // check if one resource is available
						//check_resource  := (Information_table[ResourceIndex[0]][2]).(string) // affect the resource of the image available

						if (CheckResourceAvailable( ResourceAvailable,check_resource[0])){ // check if the resource of the image recorded avaialbe or not
							ResourceSelected=check_resource[0]

						} else { err=errors.New("error  ") }


					} else  if ( (repeat_image > 1) && len(check_resource)>1){ // if the image is recorded more than one time and its reources are available


						ResourceSelected = SelectBestResource(ResourceIndex , Information_table  , ResourceAvailable ,true) // select the best resource

					} else { err=errors.New("error  ")

					}



				} else { err=errors.New("error  ") }


			}

		} else  { // if the image is not recorded

			err=errors.New("error  ")
		}

	}else {

		err=errors.New("0 node available  ")

	}




	return ResourceSelected , err

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
