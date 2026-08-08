package informers

import (
	"context"
	"time"

	api "github.com/naseyro/ssc/pkg/apis/secretrotator/v1alpha1"
	"github.com/naseyro/ssc/pkg/clientset/secretrotator/v1alpha1"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/tools/cache"
)

func NewSharedIndexInformer(clientSet v1alpha1.SecretRotatorV1Alpha1Interface) cache.SharedIndexInformer {
	indexers := cache.Indexers{
		cache.NamespaceIndex: cache.MetaNamespaceIndexFunc,

		"secretName": func(obj interface{}) ([]string, error) {
			ms, ok := obj.(*api.ManagedSecret)
			if !ok {
				return []string{}, nil
			}

			var secretNames []string
			for _, ref := range ms.Spec.SecretRefs {
				secretNames = append(secretNames, ref.Name)
			}
			return secretNames, nil
		},
	}

	sharedInformer := cache.NewSharedIndexInformer(&cache.ListWatch{
		ListWithContextFunc: func(ctx context.Context, options v1.ListOptions) (runtime.Object, error) {
			return clientSet.ManagedSecrets("").List(ctx, options)
		},
		WatchFuncWithContext: func(ctx context.Context, options v1.ListOptions) (watch.Interface, error) {
			return clientSet.ManagedSecrets("").Watch(ctx, options)
		},
	}, &api.ManagedSecret{}, 5*time.Minute, indexers)
	return sharedInformer
}
