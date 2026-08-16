package controller

import (
	"context"
	"fmt"
	"strings"
	"time"

	"k8s.io/klog/v2"

	apiv1 "github.com/naseyro/ssc/pkg/apis/secrets.management.io/v1"
	"github.com/naseyro/ssc/pkg/clientset/informers"
	secretsmanagerslisters "github.com/naseyro/ssc/pkg/clientset/listers"
	v1 "github.com/naseyro/ssc/pkg/clientset/secrets.management.io/v1"
	"github.com/naseyro/ssc/pkg/utils"
	corev1 "k8s.io/api/core/v1"
	apimachineryerrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	utilerrors "k8s.io/apimachinery/pkg/util/errors"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	corev1listers "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/restmapper"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"
)

type Controller struct {
	SecretsManagerClient   v1.SecretsManagementV1Interface
	dynamicClient          dynamic.Interface
	kubernetesClient       kubernetes.Interface
	queue                  workqueue.TypedRateLimitingInterface[string]
	SecretsManagerInformer cache.SharedIndexInformer
	SecretsManagerLister   secretsmanagerslisters.SecretsManagerLister
	SecretsInformer        cache.SharedIndexInformer
	SecretsLister          corev1listers.SecretLister
	mapper                 *restmapper.DeferredDiscoveryRESTMapper
}

func NewController(sc v1.SecretsManagementV1Interface, kc kubernetes.Interface, dc dynamic.Interface, secretsManagerInformer cache.SharedIndexInformer, secretsManagerLister secretsmanagerslisters.SecretsManagerLister, mapper *restmapper.DeferredDiscoveryRESTMapper) *Controller {
	queue := workqueue.NewTypedRateLimitingQueue(workqueue.DefaultTypedControllerRateLimiter[string]())
	secretsInformer := informers.NewSecretsSharedInformer(kc)
	secretsLister := corev1listers.NewSecretLister(secretsInformer.GetIndexer())
	ctr := &Controller{
		SecretsManagerClient:   sc,
		dynamicClient:          dc,
		kubernetesClient:       kc,
		queue:                  queue,
		SecretsManagerInformer: secretsManagerInformer,
		SecretsManagerLister:   secretsManagerLister,
		SecretsInformer:        secretsInformer,
		SecretsLister:          secretsLister,
		mapper:                 mapper,
	}
	secretsManagerInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
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
		utilruntime.HandleError(fmt.Errorf("error converting object to Secret"))
		return
	}
	c.addSecretKeyToWorkqueue(secret)
}

func (c *Controller) secretsUpdateFunc(oldObj, newObj interface{}) {
	oldMeta, oldOk := oldObj.(metav1.Object)
	newMeta, newOk := newObj.(metav1.Object)
	if oldOk && newOk && oldMeta.GetResourceVersion() == newMeta.GetResourceVersion() {
		return
	}

	secret, ok := newObj.(*corev1.Secret)
	if !ok {
		utilruntime.HandleError(fmt.Errorf("error converting object to Secret"))
		return
	}
	c.addSecretKeyToWorkqueue(secret)
}

func (c *Controller) secretsDeleteFunc(obj interface{}) {
	secret, ok := obj.(*corev1.Secret)
	if !ok {
		tombstone, ok := obj.(cache.DeletedFinalStateUnknown)
		if !ok {
			utilruntime.HandleError(fmt.Errorf("error decoding tombstone object, invalid type"))
			return
		}
		secret, ok = tombstone.Obj.(*corev1.Secret)
		if !ok {
			utilruntime.HandleError(fmt.Errorf("error decoding secret object, invalid type"))
			return
		}
	}

	c.addSecretKeyToWorkqueue(secret)
}

func (c *Controller) AddFunc(obj interface{}) {
	key, err := cache.MetaNamespaceKeyFunc(obj)
	if err != nil {
		utilruntime.HandleError(err)
		return
	}
	c.queue.Add(key)
}

func (c *Controller) UpdateFunc(oldObj interface{}, newObj interface{}) {
	oldMeta, oldOk := oldObj.(metav1.Object)
	newMeta, newOk := newObj.(metav1.Object)

	if oldOk && newOk && oldMeta.GetResourceVersion() == newMeta.GetResourceVersion() {
		return
	}

	key, err := cache.MetaNamespaceKeyFunc(newObj)
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("error could not retrieve key: %v", err))
		return
	}
	c.queue.Add(key)
}

func (c *Controller) DeleteFunc(obj interface{}) {
	ms, ok := obj.(*apiv1.SecretsManager)

	if !ok {
		tombstone, ok := obj.(cache.DeletedFinalStateUnknown)
		if !ok {
			utilruntime.HandleError(fmt.Errorf("couldn't get object from tombstone"))
			return
		}

		ms, ok = tombstone.Obj.(*apiv1.SecretsManager)
		if !ok {
			utilruntime.HandleError(fmt.Errorf("tombstone contained object that is not a ManagedSecret"))
			return
		}
	}

	key, err := cache.MetaNamespaceKeyFunc(ms)
	if err == nil {
		c.queue.Add(key)
	}
}

func (c *Controller) Run(workers int, stopCh <-chan struct{}) error {
	defer utilruntime.HandleCrash()
	defer c.queue.ShutDown()

	klog.Info("Starting SecretsManager controller")

	go c.SecretsInformer.Run(stopCh)

	klog.Info("Waiting for informer caches to sync")
	if ok := cache.WaitForCacheSync(stopCh, c.SecretsInformer.HasSynced); !ok {
		return fmt.Errorf("failed to wait for caches to sync")
	}

	klog.Info("Starting workers")
	for i := 0; i < workers; i++ {
		go wait.Until(c.worker, time.Second, stopCh)
	}

	klog.Info("Started workers")

	<-stopCh
	klog.Info("Shutting down workers")

	return nil
}

func (c *Controller) worker() {
	for c.proccessNextItem() {
	}
}

func (c *Controller) proccessNextItem() bool {
	key, shutdown := c.queue.Get()
	defer c.queue.Done(key)

	if shutdown {
		return false
	}

	if err := c.reconcile(key); err != nil {
		utilruntime.HandleError(err)
		c.queue.AddRateLimited(key)
		return true
	}
	c.queue.Forget(key)
	return true
}

func (c *Controller) reconcile(key string) error {
	ns, name, err := SeparateKey(key)
	if err != nil {
		return err
	}
	secretsManager, err := c.SecretsManagerClient.SecretsManagers(ns).Get(context.Background(), name, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("error could not retrieve SecretManager %s", name)
	}
	return c.reconcileWorkloads(secretsManager)
}

func (c *Controller) reconcileWorkloads(secretsManager *apiv1.SecretsManager) error {
	workloads := secretsManager.Spec.TargetWorkloads
	var errs []error
	for _, workload := range workloads {
		// We are handling defaults in CRD and Mutating Webhook.
		// TODO: this call and function is redundant now.
		resolveDefaults(&workload, secretsManager)

		resource, err := c.getGVR(&workload)
		if err != nil {
			errs = append(errs, fmt.Errorf("failed to get GVR for workload %s: %w", workload.Name, err))
			continue
		}

		unstructuredWorkload, err := c.dynamicClient.Resource(resource).Namespace(workload.Namespace).Get(context.Background(), workload.Name, metav1.GetOptions{})
		if err != nil {
			if apimachineryerrors.IsNotFound(err) {
				continue
			}
			errs = append(errs, fmt.Errorf("failed to fetch workload %s: %w", workload.Name, err))
			continue
		}

		workloadTemplate, err := getWorkloadTemplate(unstructuredWorkload)
		if err != nil {
			errs = append(errs, fmt.Errorf("failed to extract template for %s: %w", workload.Name, err))
			continue
		}

		annotations := make(map[string]string)
		needsApply := false
		for i := range workload.Secrets {
			secretRef := &workload.Secrets[i]
			secret, err := c.GetSecret(secretRef, workload.Namespace)
			if err != nil {
				continue
			}
			computedHash := utils.ComputeSecretHash(secret)
			annotations[fmt.Sprintf("secrets.management.io/%s", secretRef.Name)] = computedHash
			if !isSecretAnnotated(workloadTemplate, secretRef.Name, computedHash) {
				needsApply = true
			}
		}

		if !needsApply {
			continue
		}

		applyObj, err := buildApplyPayload(unstructuredWorkload, workloadTemplate, annotations, workload.Secrets)
		if err != nil {
			errs = append(errs, fmt.Errorf("failed to build apply payload for %s: %w", workload.Name, err))
			continue
		}

		_, err = c.dynamicClient.Resource(resource).
			Namespace(unstructuredWorkload.GetNamespace()).
			Apply(context.Background(), unstructuredWorkload.GetName(), applyObj, metav1.ApplyOptions{
				FieldManager: utils.FieldManager,
				Force:        true,
			})
		if err != nil {
			errs = append(errs, fmt.Errorf("failed to apply workload %s: %w", workload.Name, err))
			continue
		}
	}
	return utilerrors.NewAggregate(errs)
}

func resolveDefaults(workload *apiv1.Workload, secretsManager *apiv1.SecretsManager) {
	if workload.Namespace == "" {
		workload.Namespace = secretsManager.Namespace
	}
}

func (c *Controller) addSecretKeyToWorkqueue(secret *corev1.Secret) {
	matchedObjects, err := c.SecretsManagerLister.GetBySecretName(secret.Name)
	if err != nil {
		utilruntime.HandleError(err)
		return
	}

	for _, ms := range matchedObjects {
		if ms.Namespace == secret.Namespace {
			key, err := cache.MetaNamespaceKeyFunc(ms)
			if err == nil {
				c.queue.Add(key)
			}
		}
	}
}

func getWorkloadTemplate(unstructuredWorkload *unstructured.Unstructured) (*corev1.PodTemplateSpec, error) {
	templateMap, found, err := unstructured.NestedMap(unstructuredWorkload.Object, "spec", "template")
	if err != nil || !found {
		return nil, fmt.Errorf("spec.template not found in workload")
	}

	var podTemplate corev1.PodTemplateSpec
	err = runtime.DefaultUnstructuredConverter.FromUnstructured(templateMap, &podTemplate)
	if err != nil {
		return nil, fmt.Errorf("failed to convert pod template: %v", err)
	}
	return &podTemplate, nil
}

func buildApplyPayload(workload *unstructured.Unstructured, workloadTemplate *corev1.PodTemplateSpec, annotations map[string]string, secretRefs []apiv1.WorkloadSecret) (*unstructured.Unstructured, error) {
	injected := utils.ComputeInjectedFields(workloadTemplate, secretRefs)

	templateMap := make(map[string]interface{})
	if len(annotations) > 0 {
		templateMap["metadata"] = map[string]interface{}{"annotations": annotations}
	}

	specMap := make(map[string]interface{})
	if len(injected.Volumes) > 0 {
		volumes := make([]interface{}, 0, len(injected.Volumes))
		for i := range injected.Volumes {
			volume, err := runtime.DefaultUnstructuredConverter.ToUnstructured(&injected.Volumes[i])
			if err != nil {
				return nil, fmt.Errorf("failed to convert injected volume: %w", err)
			}
			volumes = append(volumes, volume)
		}
		specMap["volumes"] = volumes
	}
	if len(injected.Containers) > 0 {
		containers := make([]interface{}, 0, len(injected.Containers))
		for i := range injected.Containers {
			container, err := runtime.DefaultUnstructuredConverter.ToUnstructured(&injected.Containers[i])
			if err != nil {
				return nil, fmt.Errorf("failed to convert injected container: %w", err)
			}
			containers = append(containers, container)
		}
		specMap["containers"] = containers
	}
	if len(specMap) > 0 {
		templateMap["spec"] = specMap
	}

	obj := map[string]interface{}{
		"apiVersion": workload.GetAPIVersion(),
		"kind":       workload.GetKind(),
		"metadata": map[string]interface{}{
			"name":      workload.GetName(),
			"namespace": workload.GetNamespace(),
		},
	}
	if len(templateMap) > 0 {
		obj["spec"] = map[string]interface{}{"template": templateMap}
	}
	return &unstructured.Unstructured{Object: obj}, nil
}

func (c *Controller) getGVR(workload *apiv1.Workload) (schema.GroupVersionResource, error) {
	group, version, err := SeparateKey(workload.APIVersion)
	if err != nil {
		return schema.GroupVersionResource{}, fmt.Errorf("error structuring the GroupVersion: %v", err)
	}
	mapping, err := c.mapper.RESTMapping(schema.GroupKind{
		Group: group,
		Kind:  workload.Kind,
	}, version)
	if err != nil {
		return schema.GroupVersionResource{}, fmt.Errorf("error creating the RESTMapping: %v", err)
	}
	return mapping.Resource, nil
}

func isSecretAnnotated(workloadTemplate *corev1.PodTemplateSpec, secretName, computedHash string) bool {
	if workloadTemplate.Annotations == nil {
		return false
	}
	key := fmt.Sprintf("secrets.management.io/%s", secretName)
	if workloadHash, exists := workloadTemplate.Annotations[key]; !exists || (exists && workloadHash != computedHash) {
		return false
	}
	return true
}

func (c *Controller) GetSecret(secretRef *apiv1.WorkloadSecret, namespace string) (*corev1.Secret, error) {
	secret, err := c.SecretsLister.Secrets(namespace).Get(secretRef.Name)
	if err != nil {
		if apimachineryerrors.IsNotFound(err) {
			return nil, fmt.Errorf("referenced secret %s/%s not found in cache", namespace, secretRef.Name)
		}
		return nil, fmt.Errorf("failed to fetch secret %s/%s: %w", namespace, secretRef.Name, err)
	}

	return secret, nil
}

func SeparateKey(key string) (string, string, error) {
	namespaceKey := strings.Split(key, "/")
	if len(namespaceKey) != 2 {
		return "", "", fmt.Errorf("could not retrieve a valid key in the form of: 'namespace/name'")
	}
	return namespaceKey[0], namespaceKey[1], nil
}
