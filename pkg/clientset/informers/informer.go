package informers

import (
	"context"
	"time"

	apiv1 "github.com/naseyro/ssc/pkg/apis/secrets.management.io/v1"
	v1 "github.com/naseyro/ssc/pkg/clientset/secrets.management.io/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/tools/cache"
)

func NewSharedIndexInformer(clientSet v1.SecretsManagementV1Interface) cache.SharedIndexInformer {
	indexers := cache.Indexers{
		cache.NamespaceIndex: cache.MetaNamespaceIndexFunc,

		"secretName": func(obj interface{}) ([]string, error) {
			ms, ok := obj.(*apiv1.SecretsManager)
			if !ok {
				return []string{}, nil
			}
			secretNameSet := make(map[string]struct{})

			for _, workload := range ms.Spec.TargetWorkloads {
				for _, secretReq := range workload.Secrets {
					secretNameSet[secretReq.Name] = struct{}{}
				}
			}

			var secretNames []string
			for name := range secretNameSet {
				secretNames = append(secretNames, name)
			}

			return secretNames, nil
		},
	}

	sharedInformer := cache.NewSharedIndexInformer(&cache.ListWatch{
		ListWithContextFunc: func(ctx context.Context, options metav1.ListOptions) (runtime.Object, error) {
			return clientSet.SecretsManagers("").List(ctx, options)
		},
		WatchFuncWithContext: func(ctx context.Context, options metav1.ListOptions) (watch.Interface, error) {
			return clientSet.SecretsManagers("").Watch(ctx, options)
		},
	}, &apiv1.SecretsManager{}, 5*time.Minute, indexers)

	return sharedInformer
}
