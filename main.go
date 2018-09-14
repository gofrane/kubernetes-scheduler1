
package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"CGSchudeler/kubernetes-scheduler/cache"
	kube "CGSchudeler/kubernetes-scheduler/kubernetes"
	"CGSchudeler/kubernetes-scheduler/sysdig"
	"os/user"
	"time"

)

// Variables that will be used in our scheduler
var (
	schedulerName     string
	kubeAPI           kube.KubernetesCoreV1Api
	sysdigAPI         sysdig.SysdigApiClient
	metrics           []map[string]interface{}
	sysdigMetric      string
	sysdigMetricLower = true // When comparing the metrics, the lowest will be the best one
	bestCachedNode    = cache.Cache{Timeout: 15 * time.Second}
	cachedNodes       = cache.Cache{Timeout: 15 * time.Second}

)


// Errors
var (
	noDataFound   = errors.New("no data found with those parameters")
	emptyNodeList = errors.New("node list must contain at least one element")
	noNodeFound   = errors.New("no node found")
)

// Flags
var (
	sysdigTokenFlag    = flag.String("t", "", "Sysdig Cloud Token")
	kubeConfigFileFlag = flag.String("k", "", "Kubernetes config file")
	sysdigMetricFlag   = flag.String("m", "", "Sysdig metric to monitorize")
	schedulerNameFlag  = flag.String("s", "", "Scheduler name")
)

func init() {

	flag.Usage = usage
	flag.Parse()

	// SCD_TOKEN parameter / env var
	if sysdigTokenEnv, tokenSetByEnv := os.LookupEnv("SDC_TOKEN"); !tokenSetByEnv && *sysdigTokenFlag == "" {
		fmt.Println("Error: Sysdig Cloud token is not set.")
		usage()
	} else {
		if tokenSetByEnv {
			sysdigAPI.SetToken(sysdigTokenEnv)
		}
		if *sysdigTokenFlag != "" { // If the flag is set, overrides the environment
			sysdigAPI.SetToken(*sysdigTokenFlag)
		}
	}

	// KUBECONFIG parameter / env var
	if _, kubeTokenSetByEnv := os.LookupEnv("KUBECONFIG"); !kubeTokenSetByEnv && *kubeConfigFileFlag == "" {
		usr, _ := user.Current()
		os.Setenv("KUBECONFIG", usr.HomeDir+"/.kube/config")
	} else {
		if *kubeConfigFileFlag != "" {
			os.Setenv("KUBECONFIG", *kubeConfigFileFlag)
		}
	}
	kubeAPI.LoadKubeConfig()

	// SCD_METRIC parameter / env var
	if sysdigMetricEnv, sysdigMetricEnvIsSet := os.LookupEnv("SDC_METRIC"); !sysdigMetricEnvIsSet && *sysdigMetricFlag == "" {
		fmt.Println("The Sysdig metric must be defined")
		usage()
	} else {
		if sysdigMetricEnvIsSet {
			sysdigMetric = sysdigMetricEnv
		}
		if *sysdigMetricFlag != "" {
			sysdigMetric = *sysdigMetricFlag
		}
		highOrLowMetric := sysdigMetric[0]
		if highOrLowMetric == '-' {
			sysdigMetric = sysdigMetric[1:]
		} else if highOrLowMetric == '+' {
			sysdigMetric = sysdigMetric[1:]
			sysdigMetricLower = false
		}
	}

	// SDC_SCHEDULER parameter / env var
	if schedulerNameEnv, schedulernameEnvIsSet := os.LookupEnv("SDC_SCHEDULER"); !schedulernameEnvIsSet && *schedulerNameFlag == "" {
		fmt.Println("Scheduler name must be set")
		usage()
	} else {
		if schedulernameEnvIsSet {
			schedulerName = schedulerNameEnv
		}
		if *schedulerNameFlag != "" {
			schedulerName = *schedulerNameFlag
		}
	}

	metrics = append(metrics, map[string]interface{}{
		"id": sysdigMetric,
		"aggregations": map[string]string{
			"time": "timeAvg", "group": "avg",
		},
	})
}

// Usage description
func usage() {
	fmt.Printf("Usage: %s [-s SCHEDULER_NAME] [-m [+|-]SYSDIG_METRIC] [-t SYSDIG_TOKEN] [-k KUBERNETES_CONFIG_FILE]", os.Args[0])
	fmt.Print(`
If the env KUBECONFIG is not set, the -k option must be provided.
If the env SDC_TOKEN is not set, the -t option must be provided.
If the env [+|-]SDC_METRIC is not set, the -m option must be provided. Sort mode: "+" higher, "-" lower. Default sort mode: lower.
If the env SDC_SCHEDULER is not set, the -s option must be provided.
`)
	flag.PrintDefaults()
	os.Exit(2)
}

func main() {

	ch, err := kubeAPI.Watch("GET", "api/v1/pods", nil, nil)
	if err != nil {
		log.Fatalln("fatal: error while connecting with the kubernetes Api:", err)
	}

	for data := range ch {
		go func(data []byte) {
			event := kube.KubePodEvent{}
			err := json.Unmarshal(data, &event)
			if err != nil {
				log.Println("Error:", err)
				return
			}

			// If the pod has been added, is in Pending phase and has the scheduler name, schedule it.
			if event.Object.Status.Phase == "Pending" && event.Object.Spec.SchedulerName == schedulerName && event.Type == "ADDED" {
				log.Println("Scheduling", event.Object.Metadata.Name)

				bestNodeFound, err := GetBestNodeByMetrics(NodesAvailable())
				if err != nil {
					log.Println("error while retrieving the best node:", err.Error())
					// In case a node could not be found, fallback to default scheduler
					log.Println("falling back to the default scheduler...")
					deploymentName, err := FindDeploymentNameFromPod(event.Object)
					if err != nil {
						log.Fatalln(err)
					}
					deployments, err := kubeAPI.ListNamespacedDeployments(event.Object.Metadata.Namespace, "metadata.name="+deploymentName)
					if err != nil {
						log.Fatalln(err)
					}
					for _, item := range deployments.Items {
						_, err := kubeAPI.ReplaceDeploymentScheduler(item, "default-scheduler")
						if err != nil {
							log.Fatalf("could not modify deployment %s: %s\n Fatal: those pods won't be re-scheduled, terminating...", item.Metadata.Name, err.Error())
						}
					}
				} else {
					log.Println("Best node found: ", bestNodeFound.name, bestNodeFound.metric)
					response, err := Scheduler(event.Object.Metadata.Name, bestNodeFound.name, event.Object.Metadata.Namespace)
					if err != nil {
						log.Println("error while scheduling a pod:", err)
					}
					kubeResponse := kube.KubeResponse{}
					err = json.NewDecoder(response.Body).Decode(&kubeResponse)
					if err != nil {
						log.Println("error while decoding kube response: ", err)
					}
					if kubeResponse.Code != 200 && kubeResponse.Code != 201 {
						log.Println("kube response error: ", kubeResponse.Message)
					}

					response.Body.Close()
				}
			}
		}(data)
	}
}
