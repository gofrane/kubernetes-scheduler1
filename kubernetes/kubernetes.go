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

// Small Kubernetes api wrapper
package kubernetes

import (
	"bufio"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"time"
	"net"
	"context"
	"bytes"


	"CGSchudeler/kubernetes-scheduler/cache"
	"cgs/yaml-2.2.1"
)

type KubernetesCoreV1Api struct {
	config       KubeConf
	nodeList     cache.Cache
	clientCert   tls.Certificate
	serverCaCert *x509.CertPool
}

func (api KubernetesCoreV1Api) ReplaceDeploymentScheduler(item KubeDeploymentItem, scheduler string) (modified KubeDeploymentItem, err error) {
	url := fmt.Sprintf("apis/apps/v1/namespaces/%s/deployments/%s", item.Metadata.Namespace, item.Metadata.Name)

	patchRequest := []struct {
		Op    string `json:"op"`
		Path  string `json:"path"`
		Value string `json:"value,omitempty"`
	}{{
		Op:    "add",
		Path:  "/spec/template/spec/schedulerName",
		Value: scheduler,
	}}

	data, err := json.Marshal(patchRequest)
	if err != nil {
		return
	}
	body := bytes.NewReader(data)

	response, err := api.Request("PATCH", url, "application/json-patch+json", nil, body)
	if err != nil {
		return
	}
	defer response.Body.Close()

	if response.StatusCode != 200 {
		var responseData struct {
			Message string `json:"message"`
		}
		json.NewDecoder(response.Body).Decode(&responseData)
		err = fmt.Errorf("kubernetes: ReplaceDeploymentScheduler error code %d: %s", response.StatusCode, responseData.Message)
		return
	}

	err = json.NewDecoder(response.Body).Decode(&modified)
	return
}

func (api KubernetesCoreV1Api) ListNamespacedDeployments(namespace, fieldSelector string) (deployments KubeDeployments, err error) {

	values := url.Values{}
	values.Add("fieldSelector", fieldSelector)

	response, err := api.Request("GET", fmt.Sprintf("apis/apps/v1/namespaces/%s/deployments", namespace), "", values, nil)
	if err != nil {
		return
	}
	defer response.Body.Close()

	err = json.NewDecoder(response.Body).Decode(&deployments)
	return
}

func (api KubernetesCoreV1Api) CreateNamespacedBinding(namespace string, body io.Reader) (response *http.Response, err error) {
	return api.Request("POST", fmt.Sprintf("api/v1/namespaces/%s/bindings", namespace), "", nil, body)
}

func (api KubernetesCoreV1Api) Watch(httpMethod, apiMethod string, values url.Values, body io.Reader) (responseChannel chan []byte, err error) {
	if values == nil {
		values = url.Values{}
	}
	values.Add("watch", "true")
	responseChannel = make(chan []byte)
	go func() {
		response, err := api.Request(httpMethod, apiMethod, "", values, body)
		if err != nil {
			close(responseChannel)
			return
		}
		defer response.Body.Close()

		reader := bufio.NewReader(response.Body)
		for {
			line, err := reader.ReadBytes('\n')
			if err != nil {
				log.Println(err)
				continue
			}
			responseChannel <- line
			line = nil
		}
	}()
	return
}

func (api KubernetesCoreV1Api) Request(httpMethod, apiMethod, contentType string, values url.Values, body io.Reader) (response *http.Response, err error) {
	apiUrl := api.currentApiUrlEndpoint()

	certificate, caCertPool := api.currentTLSInfo()
	tlsConfig := &tls.Config{
		Certificates: []tls.Certificate{certificate},
		RootCAs:      caCertPool,
	}

	tlsConfig.BuildNameToCertificate()
	transport := &http.Transport{TLSClientConfig: tlsConfig, DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
		return net.DialTimeout(network, addr, 10*time.Second)
	}}
	client := http.Client{Transport: transport}
	request, err := http.NewRequest(httpMethod, apiUrl+"/"+apiMethod, body)
	if err != nil {
		return
	}
	if values != nil {
		request.URL.RawQuery = values.Encode()
	}

	// Get the info in json
	if contentType == "" {
		contentType = "application/json"
	}
	request.Header.Add("Content-Type", contentType)

	// Make the request
	response, err = client.Do(request)
	return
}

func (api KubernetesCoreV1Api) ListNodes() (nodes []KubeNode, err error) {

	if nodes, ok := api.nodeList.Data(); ok {
		return nodes.([]KubeNode), nil
	}

	response, err := api.Request("GET", "api/v1/nodes", "", nil, nil)
	if err != nil {
		return
	}
	defer response.Body.Close()

	var nodeInfo struct {
		Items []KubeNode `json:"items"`
	}
	err = json.NewDecoder(response.Body).Decode(&nodeInfo)
	if err != nil {
		return
	}
	nodes = nodeInfo.Items

	api.nodeList.SetData(nodes)

	return
}

// Reads the configuration file and loads the config struct
func (api *KubernetesCoreV1Api) LoadKubeConfig() (err error) {
	yamlFile, err := ioutil.ReadFile(getKubeConfigFileDefaultLocation())
	if err != nil {
		panic("Could not load the Kubernetes configuration")
	}

	var kubeConfig KubeConf
	err = yaml.Unmarshal(yamlFile, &kubeConfig)
	if err != nil {
		return err
	}

	// Decode the certificate
	for k, cluster := range kubeConfig.Clusters {
		certBytes, err := base64.StdEncoding.DecodeString(cluster.Data.CertificateAuthorityDataStr)
		if err != nil {
			panic(err)
		}
		cluster.Data.CertificateAuthorityData = certBytes
		kubeConfig.Clusters[k] = cluster
	}

	// Decode the certificate and the key
	for k, user := range kubeConfig.Users {
		cert, err := base64.StdEncoding.DecodeString(user.Data.ClientCertificateDataStr)
		if err != nil {
			panic(err)
		}
		user.Data.ClientCertificateData = cert
		key, err := base64.StdEncoding.DecodeString(user.Data.ClientKeyDataStr)
		if err != nil {
			panic(err)
		}
		user.Data.ClientKeyData = key
		kubeConfig.Users[k] = user
	}

	api.config = kubeConfig
	api.nodeList = cache.Cache{Timeout: 1 * time.Minute}
	api.loadTLSInfo()
	return err
}

func (api KubernetesCoreV1Api) ListNamespacedReplicaset(namespace string, replicaName string) (replicaSet KubeReplicaSet, err error){
	endpoint := fmt.Sprintf("apis/apps/v1/namespaces/%s/replicasets/%s", namespace, replicaName)
	response, err := api.Request("GET", endpoint, "", nil, nil)
	if err != nil {
		return
	}
	defer response.Body.Close()

	err = json.NewDecoder(response.Body).Decode(&replicaSet)
	return
}
