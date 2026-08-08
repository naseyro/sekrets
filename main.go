package main

import (
	"flag"

	SecretRotatorClientSet "github.com/naseyro/ssc/pkg/clientset/secretrotator/v1alpha1"
	"github.com/naseyro/ssc/pkg/controller"
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

	managedSecretClientset, err := SecretRotatorClientSet.NewForConfig(restConfig)
	if err != nil {
		klog.Errorf("error creating ManagedSecret clientset: %v", err)
	}

	kcs, err := kubernetes.NewForConfig(restConfig)
	if err != nil {
		klog.Errorf("error building the Kubernetes Secrets clientset: %v", err)
	}

	msController := controller.NewController(managedSecretClientset, kcs)
}
