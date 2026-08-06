package main

import (
	"context"
	"flag"
	"time"

	SecretRotatorClientSet "github.com/naseyro/ssc/pkg/clientset/secretrotator/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
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

	// Initialize ManagedSecret Controller
	// Initialize Secrets Clientset and Informer
	kcs, err := kubernetes.NewForConfig(restConfig)
	if err != nil {
		klog.Errorf("error building the Kubernetes Secrets clientset: %v", err)
	}
	secretsInformer := cache.NewSharedIndexInformer(&cache.ListWatch{
		ListWithContextFunc: func(ctx context.Context, options metav1.ListOptions) (runtime.Object, error) {
			return kcs.CoreV1().Secrets("").List(ctx, options)
		},
		WatchFuncWithContext: func(ctx context.Context, options metav1.ListOptions) (watch.Interface, error) {
			return kcs.CoreV1().Secrets("").Watch(ctx, options)
		},
	}, &corev1.Secret{}, time.Minute, cache.Indexers{})
}
