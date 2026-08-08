package controller

import (
	"fmt"
	"time"

	apiv1alpha1 "github.com/naseyro/ssc/pkg/apis/secretrotator/v1alpha1"
	"github.com/naseyro/ssc/pkg/clientset/secretrotator/v1alpha1"
	"github.com/naseyro/ssc/pkg/informers"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"
)

type Controller struct {
	msClientSet    v1alpha1.SecretRotatorV1Alpha1Interface
	workqueue      workqueue.TypedRateLimitingInterface[string]
	msInformer     cache.SharedIndexInformer
	secretsInfomer cache.SharedIndexInformer
}

func NewController(sc v1alpha1.SecretRotatorV1Alpha1Interface, kc kubernetes.Interface) *Controller {
	workqueue := workqueue.NewTypedRateLimitingQueue(workqueue.DefaultTypedControllerRateLimiter[string]())
	managedSecretsInformer := informers.NewSharedIndexInformer(sc)
	secretsInformer := informers.NewSecretsSharedInformer(kc)
	ctr := &Controller{
		msClientSet:    sc,
		workqueue:      workqueue,
		msInformer:     managedSecretsInformer,
		secretsInfomer: secretsInformer,
	}
	managedSecretsInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    ctr.AddFunc,
		UpdateFunc: ctr.UpdateFunc,
		DeleteFunc: ctr.DeleteFunc,
	})

	secretsInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    ctr.secretsAddFunc,
		UpdateFunc: ctr.secretsUpdateFunc,
		DeleteFunc: ctr.secretsDeleteFunc,
	})
	return ctr
}

func (c *Controller) secretsAddFunc(obj interface{}) {
	secret, ok := obj.(*corev1.Secret)
	if !ok {
		runtime.HandleError(fmt.Errorf("error converting objecto to Secret"))
		return
	}
	c.handleSecret(secret)
}

func (c *Controller) secretsUpdateFunc(oldObj, newObj interface{}) {
	oldMeta, oldOk := oldObj.(metav1.Object)
	newMeta, newOk := newObj.(metav1.Object)
	if oldOk && newOk && oldMeta.GetResourceVersion() == newMeta.GetResourceVersion() {
		return
	}

	secret, ok := newObj.(*corev1.Secret)
	if !ok {
		runtime.HandleError(fmt.Errorf("error converting objecto to Secret"))
		return
	}
	c.handleSecret(secret)
}

func (c *Controller) secretsDeleteFunc(obj interface{}) {
	secret, ok := obj.(*corev1.Secret)
	if !ok {
		tombstone, ok := obj.(cache.DeletedFinalStateUnknown)
		if !ok {
			runtime.HandleError(fmt.Errorf("error decoding tombstone object, invalid type"))
			return
		}
		secret, ok = tombstone.Obj.(*corev1.Secret)
		if !ok {
			runtime.HandleError(fmt.Errorf("error decoding secret object, invalid type"))
			return
		}
	}

	c.handleSecret(secret)
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
	oldMeta, oldOk := oldObj.(metav1.Object)
	newMeta, newOk := newObj.(metav1.Object)

	if oldOk && newOk && oldMeta.GetResourceVersion() == newMeta.GetResourceVersion() {
		return
	}

	key, err := cache.MetaNamespaceKeyFunc(newObj)
	if err != nil {
		runtime.HandleError(err)
		return
	}
	c.workqueue.Add(key)
}

func (c *Controller) DeleteFunc(obj interface{}) {
	ms, ok := obj.(*apiv1alpha1.ManagedSecret)

	if !ok {
		tombstone, ok := obj.(cache.DeletedFinalStateUnknown)
		if !ok {
			runtime.HandleError(fmt.Errorf("couldn't get object from tombstone"))
			return
		}

		ms, ok = tombstone.Obj.(*apiv1alpha1.ManagedSecret)
		if !ok {
			runtime.HandleError(fmt.Errorf("tombstone contained object that is not a ManagedSecret"))
			return
		}
	}

	key, err := cache.MetaNamespaceKeyFunc(ms)
	if err == nil {
		c.workqueue.Add(key)
	}
}

func (c *Controller) Run() error {
	stopCh := make(chan struct{})

	go c.msInformer.Run(stopCh)

	if !cache.WaitForCacheSync(stopCh, c.msInformer.HasSynced) {
		return fmt.Errorf("error syncing the ManagedSecrets Informer Cache")
	}
	wait.Until(c.worker, time.Second, stopCh)
	<-stopCh

	return nil
}

func (c *Controller) worker() {
	for c.proccessNextItem() {

	}
}

func (c *Controller) proccessNextItem() bool {
	key, shutdown := c.workqueue.Get()
	defer c.workqueue.Done(key)

	if shutdown {
		return false
	}

	if err := reconcile(key); err != nil {
		runtime.HandleError(err)
		c.workqueue.AddRateLimited(key)
		return true
	}
	c.workqueue.Forget(key)
	return true
}

func reconcile(key string) error {
	return nil
}

func (c *Controller) handleSecret(secret *corev1.Secret) {
	matchedObjects, err := c.msInformer.GetIndexer().ByIndex("secretName", secret.Name)
	if err != nil {
		runtime.HandleError(err)
		return
	}

	for _, matchedObj := range matchedObjects {
		ms, ok := matchedObj.(*apiv1alpha1.ManagedSecret)
		if !ok {
			continue
		}

		if ms.Namespace == secret.Namespace {
			key, err := cache.MetaNamespaceKeyFunc(ms)
			if err == nil {
				c.workqueue.Add(key)
			}
		}
	}
}
