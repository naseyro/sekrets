package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"

	SecretsManagementClientset "github.com/naseyro/ssc/pkg/clientset/secrets.management.io/v1"
	"github.com/naseyro/ssc/pkg/clientset/informers"
	secretsmanagerslisters "github.com/naseyro/ssc/pkg/clientset/listers"
	"github.com/naseyro/ssc/pkg/controller"
	"github.com/naseyro/ssc/pkg/webhooks"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/discovery/cached/memory"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/restmapper"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/klog/v2"
)

func main() {
	klog.InitFlags(nil)

	kubeconfig := flag.String("kubeconfig", "", "path to kubeconfig file; empty uses the in-cluster config")
	webhook := flag.Bool("webhook", false, "run the admission webhook server alongside the controller")
	webhookOnly := flag.Bool("webhook-only", false, "run only the admission webhook server")
	tlsCertFile := flag.String("tls-cert-file", "/etc/webhook/certs/tls.crt", "TLS certificate file for the webhook server")
	tlsKeyFile := flag.String("tls-key-file", "/etc/webhook/certs/tls.key", "TLS private key file for the webhook server")
	flag.Parse()

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	var restConfig *rest.Config
	var err error
	if *kubeconfig != "" {
		restConfig, err = clientcmd.BuildConfigFromFlags("", *kubeconfig)
	} else {
		restConfig, err = rest.InClusterConfig()
	}
	if err != nil {
		klog.Fatalf("error building the rest config: %v", err)
	}

	smClientset, err := SecretsManagementClientset.NewForConfig(restConfig)
	if err != nil {
		klog.Fatalf("error creating ManagedSecret clientset: %v", err)
	}

	kcs, err := kubernetes.NewForConfig(restConfig)
	if err != nil {
		klog.Fatalf("error building the Kubernetes Secrets clientset: %v", err)
	}
	dynamicClient, err := dynamic.NewForConfig(restConfig)
	if err != nil {
		klog.Fatalf("error building the Dynamic Kubernetes clientset: %v", err)
	}

	discoveryClient, err := discovery.NewDiscoveryClientForConfig(restConfig)
	if err != nil {
		log.Fatalf("Error creating discovery client: %v", err)
	}

	secretsManagerInformer := informers.NewSharedIndexInformer(smClientset)
	secretsManagerLister := secretsmanagerslisters.NewSecretsManagerLister(secretsManagerInformer.GetIndexer())
	mapper := restmapper.NewDeferredDiscoveryRESTMapper(memory.NewMemCacheClient(discoveryClient))

	go secretsManagerInformer.Run(ctx.Done())
	klog.Info("Waiting for SecretsManager informer cache to sync")
	if ok := cache.WaitForCacheSync(ctx.Done(), secretsManagerInformer.HasSynced); !ok {
		klog.Fatalf("failed to wait for SecretsManager informer cache to sync")
	}

	if *webhookOnly {
		webhookServer := webhooks.NewWebhookServer(secretsManagerInformer, secretsManagerLister, mapper, *tlsCertFile, *tlsKeyFile)
		if err := webhookServer.Serve(ctx); err != nil {
			klog.Fatalf("Webhook server crashed: %v", err)
		}
		return
	}

	if *webhook {
		webhookServer := webhooks.NewWebhookServer(secretsManagerInformer, secretsManagerLister, mapper, *tlsCertFile, *tlsKeyFile)
		go func() {
			if err := webhookServer.Serve(ctx); err != nil {
				klog.Fatalf("Webhook server crashed: %v", err)
			}
		}()
	}

	msController := controller.NewController(smClientset, kcs, dynamicClient, secretsManagerInformer, secretsManagerLister, mapper)

	if err := msController.Run(5, ctx.Done()); err != nil {
		klog.Fatalf("Error running controller: %v", err)
	}
}
