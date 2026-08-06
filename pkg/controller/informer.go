package controller

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
	sharedInformer := cache.NewSharedIndexInformer(&cache.ListWatch{
		ListWithContextFunc: func(ctx context.Context, options v1.ListOptions) (runtime.Object, error) {
			return clientSet.ManagedSecrets("").List(ctx, options)
		},
		WatchFuncWithContext: func(ctx context.Context, options v1.ListOptions) (watch.Interface, error) {
			return clientSet.ManagedSecrets("").Watch(ctx, options)
		},
	}, &api.ManagedSecret{}, 5*time.Minute, cache.Indexers{})
	return sharedInformer
}
