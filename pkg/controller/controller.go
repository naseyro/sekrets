package controller

import (
	"fmt"
	"time"

	"github.com/naseyro/ssc/pkg/clientset/secretrotator/v1alpha1"
	"k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"
)

type Controller struct {
	msClientSet v1alpha1.SecretRotatorV1Alpha1Interface
	workqueue   workqueue.TypedRateLimitingInterface[string]
	msInformer  cache.SharedIndexInformer
}

func NewController(c v1alpha1.SecretRotatorV1Alpha1Interface) *Controller {
	workqueue := workqueue.NewTypedRateLimitingQueue(workqueue.DefaultTypedControllerRateLimiter[string]())
	managedSecretsInformer := NewSharedIndexInformer(c)
	ctr := &Controller{
		msClientSet: c,
		workqueue:   workqueue,
		msInformer:  managedSecretsInformer,
	}
	managedSecretsInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    ctr.AddFunc,
		UpdateFunc: ctr.UpdateFunc,
		// Implement DeleteFunc
	})
	return ctr
}

func (c *Controller) AddFunc(obj interface{}) {
	key, err := cache.MetaNamespaceKeyFunc(obj)
	if err != nil {
		runtime.HandleError(err)
		return
	}
	c.workqueue.Add(key)
}

func (c *Controller) UpdateFunc(oldObj interface{}, newObj interface{}) {
	// Read Optimization from Gemini
	key, err := cache.MetaNamespaceKeyFunc(newObj)
	if err != nil {
		runtime.HandleError(err)
		return
	}
	c.workqueue.Add(key)
}

func (c *Controller) Run() error {
	stopCh := make(chan struct{})

	go c.msInformer.Run(stopCh)

	if !cache.WaitForCacheSync(stopCh, c.msInformer.HasSynced) {
		return fmt.Errorf("error syncing the ManagedSecrets Informer Cache")
	}
	wait.Until(c.Worker, time.Second, stopCh)
	<-stopCh

	return nil
}

func (c *Controller) Worker() {
	for c.ProccessNextItem() {

	}
}

func (c *Controller) ProccessNextItem() bool {
	key, shutdown := c.workqueue.Get()
	defer c.workqueue.Done(key)

	if shutdown {
		return false
	}

	if err := Reconcile(key); err != nil {
		runtime.HandleError(err)
		c.workqueue.AddRateLimited(key)
		return true
	}
	c.workqueue.Forget(key)
	return true
}

func Reconcile(key string) error {
	return nil
}
