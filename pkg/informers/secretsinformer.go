package informers

import (
	"time"

	kinformers "k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
)

func NewSecretsSharedInformer(kubernetesClientset kubernetes.Interface) cache.SharedIndexInformer {
	informerFactory := kinformers.NewSharedInformerFactory(kubernetesClientset, time.Minute*10)
	secretsInformer := informerFactory.Core().V1().Secrets().Informer()
	return secretsInformer
}
