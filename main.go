package main

import (
	"flag"
	"log"

	SecretsManagementClientset "github.com/naseyro/ssc/pkg/clientset/secrets.management.io/v1"
	"github.com/naseyro/ssc/pkg/controller"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/klog/v2"
)

func main() {
	kubeconfig := flag.String("kubeconfig", "/Volumes/external/Users/onasser/.kube/config", "kubeconfig file")
	restConfig, err := clientcmd.BuildConfigFromFlags("", *kubeconfig)
	if err != nil {
		klog.Errorf("error building the config file: %v", err)
	}

	smClientset, err := SecretsManagementClientset.NewForConfig(restConfig)
	if err != nil {
		klog.Errorf("error creating ManagedSecret clientset: %v", err)
	}

	kcs, err := kubernetes.NewForConfig(restConfig)
	if err != nil {
		klog.Errorf("error building the Kubernetes Secrets clientset: %v", err)
	}
	dynamicClient, err := dynamic.NewForConfig(restConfig)
	if err != nil {
		klog.Errorf("error building the Dynamic Kubernetes clientset: %v", err)
	}

	discoveryClient, err := discovery.NewDiscoveryClientForConfig(restConfig)
	if err != nil {
		log.Fatalf("Error creating discovery client: %v", err)
	}

	msController := controller.NewController(smClientset, kcs, dynamicClient, discoveryClient)
}
