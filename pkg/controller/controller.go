package controller

import (
	"context"
	"fmt"
	"strings"
	"time"

	apiv1 "github.com/naseyro/ssc/pkg/apis/secrets.management.io/v1"
	v1 "github.com/naseyro/ssc/pkg/clientset/secrets.management.io/v1"
	"github.com/naseyro/ssc/pkg/informers"
	"github.com/naseyro/ssc/pkg/utils"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	apimachineryerrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	utilerrors "k8s.io/apimachinery/pkg/util/errors"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/discovery/cached/memory"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	corev1listers "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/restmapper"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog/v2"
)

type Controller struct {
	msClientSet      v1.SecretsManagementV1Interface
	dynamicClient    dynamic.Interface
	discoveryClient  discovery.DiscoveryInterface
	kubernetesClient kubernetes.Interface
	queue            workqueue.TypedRateLimitingInterface[string]
	msInformer       cache.SharedIndexInformer
	secretsInformer  cache.SharedIndexInformer
	secretsLister    corev1listers.SecretLister
	mapper           restmapper.DeferredDiscoveryRESTMapper
}

func NewController(sc v1.SecretsManagementV1Interface, kc kubernetes.Interface, dc dynamic.Interface, discoveryClient discovery.DiscoveryInterface) *Controller {
	queue := workqueue.NewTypedRateLimitingQueue(workqueue.DefaultTypedControllerRateLimiter[string]())
	managedSecretsInformer := informers.NewSharedIndexInformer(sc)
	secretsInformer := informers.NewSecretsSharedInformer(kc)
	secretsLister := corev1listers.NewSecretLister(secretsInformer.GetIndexer())
	mapper := restmapper.NewDeferredDiscoveryRESTMapper(memory.NewMemCacheClient(discoveryClient))
	ctr := &Controller{
		msClientSet:      sc,
		dynamicClient:    dc,
		discoveryClient:  discoveryClient,
		kubernetesClient: kc,
		queue:            queue,
		msInformer:       managedSecretsInformer,
		secretsInformer:  secretsInformer,
		secretsLister:    secretsLister,
		mapper:           *mapper,
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
		utilruntime.HandleError(fmt.Errorf("error converting objecto to Secret"))
		return
	}
	c.addSecretKey(secret)
}

func (c *Controller) secretsUpdateFunc(oldObj, newObj interface{}) {
	oldMeta, oldOk := oldObj.(metav1.Object)
	newMeta, newOk := newObj.(metav1.Object)
	if oldOk && newOk && oldMeta.GetResourceVersion() == newMeta.GetResourceVersion() {
		return
	}

	secret, ok := newObj.(*corev1.Secret)
	if !ok {
		utilruntime.HandleError(fmt.Errorf("error converting objecto to Secret"))
		return
	}
	c.addSecretKey(secret)
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

	c.addSecretKey(secret)
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
	secretsManager, err := c.msClientSet.SecretsManagers(ns).Get(context.Background(), name, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("error could not retrieve SecretManager %s", name)
	}
	return c.reconcileWorkloads(secretsManager)
}

func (c *Controller) reconcileWorkloads(secretsManager *apiv1.SecretsManager) error {
	workloads := secretsManager.Spec.TargetWorkloads
	var errs []error
	for _, workload := range workloads {
		resolveDefaults(&workload, secretsManager)
		resource, err := c.getGVR(&workload)
		if err != nil {
			errs = append(errs, fmt.Errorf("failed to get GVR for workload %s: %w", workload.Name, err))
			continue
		}
		unstructured, err := c.dynamicClient.Resource(resource).Get(context.Background(), workload.Name, metav1.GetOptions{})
		if err != nil {
			if apimachineryerrors.IsNotFound(err) {
				continue
			}
			errs = append(errs, fmt.Errorf("failed to fetch workload %s: %w", workload.Name, err))
			continue
		}
		workloadTemplate, err := getWorkloadTemplate(unstructured)
		if err != nil {
			errs = append(errs, fmt.Errorf("failed to extract template for %s: %w", workload.Name, err))
			continue
		}

		var isModified bool

		for _, s := range workload.Secrets {
			secret, err := c.GetSecret(&s, workload.Namespace)
			if err != nil {
				continue
			}
			computedHash := utils.ComputeSecretHash(secret)
			if isSecretVolumed := isSecretVolumed(workloadTemplate, s.Name); isSecretVolumed {
				if isAnnotated := isSecretAnnotated(workloadTemplate, s.Name, computedHash); !isAnnotated {
					annotate(workloadTemplate, s.Name, computedHash)
					isModified = true
				}
			} else { // TODO: here we should check for the env/envFrom. we will bypass for simplicity now
				err := c.injectSecretAsVolume(workloadTemplate, &s)
				if err != nil {
					klog.Info("Warning: failed to inject secret as a volume")
					continue
				}
				annotate(workloadTemplate, s.Name, computedHash)
				isModified = true
			}
		}
		if isModified {
			err = toPodTemplateSpec(unstructured, workloadTemplate)
			if err != nil {
				errs = append(errs, fmt.Errorf("failed to convert template back for %s: %w", workload.Name, err))
				continue
			}

			_, err = c.dynamicClient.Resource(resource).
				Namespace(unstructured.GetNamespace()).
				Update(context.Background(), unstructured, metav1.UpdateOptions{})

			if err != nil {
				errs = append(errs, fmt.Errorf("failed to update workload %s: %w", workload.Name, err))
				continue
			}
		}
	}
	return utilerrors.NewAggregate(errs)
}

func resolveDefaults(workload *apiv1.Workload, secretsManager *apiv1.SecretsManager) {
	if workload.Namespace == "" {
		workload.Namespace = secretsManager.Namespace
	}
}

func (c *Controller) addSecretKey(secret *corev1.Secret) {
	matchedObjects, err := c.msInformer.GetIndexer().ByIndex("secretName", secret.Name)
	if err != nil {
		utilruntime.HandleError(err)
		return
	}

	for _, matchedObj := range matchedObjects {
		ms, ok := matchedObj.(*apiv1.SecretsManager)
		if !ok {
			continue
		}

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

func toPodTemplateSpec(unstructuredWorkload *unstructured.Unstructured, podTemplate *corev1.PodTemplateSpec) error {
	templateMap, err := runtime.DefaultUnstructuredConverter.ToUnstructured(podTemplate)
	if err != nil {
		return fmt.Errorf("failed to convert pod template back to map: %w", err)
	}

	err = unstructured.SetNestedMap(unstructuredWorkload.Object, templateMap, "spec", "template")
	if err != nil {
		return fmt.Errorf("failed to set updated spec.template into workload: %w", err)
	}
	return nil
}

func (c *Controller) getGVR(workload *apiv1.Workload) (schema.GroupVersionResource, error) {
	group, version, err := SeparateKey(workload.APIVersion)
	if err != nil {
		return schema.GroupVersionResource{}, fmt.Errorf("error structuring the GroupVersion", err)
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

func isSecretVolumed(workloadTemplate *corev1.PodTemplateSpec, secretName string) bool {
	for _, volume := range workloadTemplate.Spec.Volumes {
		if volume.Secret != nil && volume.Secret.SecretName == secretName {
			return true
		}
	}
	return false
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

func annotate(workloadTemplate *corev1.PodTemplateSpec, secretName, computedHash string) {
	if workloadTemplate.Annotations == nil {
		workloadTemplate.Annotations = make(map[string]string)
	}

	key := fmt.Sprintf("secrets.management.io/%s", secretName)
	workloadTemplate.Annotations[key] = computedHash
}

func (c *Controller) injectSecretAsVolume(workloadTemplate *corev1.PodTemplateSpec, secretRef *apiv1.WorkloadSecret) error {
	volumeName := fmt.Sprintf("%s-volume", secretRef.Name)
	if secretRef.MountConfig.MountPath == "" {
		secretRef.MountConfig.MountPath = fmt.Sprintf("/etc/secrets/%s", secretRef.Name)
	}

	workloadTemplate.Spec.Volumes = append(workloadTemplate.Spec.Volumes, corev1.Volume{
		Name: volumeName,
		VolumeSource: corev1.VolumeSource{
			Secret: &corev1.SecretVolumeSource{
				SecretName: secretRef.Name,
			},
		},
	})

	// This is not the optimal way to handle Secrets, we inject secret in all available containers
	// in a workload but this is not necessarily needed. This needs an optimization.
	// Related to https://github.com/naseyro/sekrets/issues/11
	for i := range workloadTemplate.Spec.Containers {
		workloadTemplate.Spec.Containers[i].VolumeMounts = append(
			workloadTemplate.Spec.Containers[i].VolumeMounts,
			corev1.VolumeMount{
				Name:      volumeName,
				MountPath: secretRef.MountConfig.MountPath,
				ReadOnly:  true,
			},
		)
	}
	return nil
}

// We need to check for: Env and EnvFrom
// func isSecretEnved(workloadTemplate *corev1.PodTemplateSpec, secretName string) bool {
// 	containers := workloadTemplate.Spec.Containers
// 	isSecretEnv(containers)
// 	isSecretEnvFrom(containers)
// }

// We need a way to return all available Secrets in the
// func isSecretEnv(containers []corev1.Container) bool {
// 	for
// }

// func isSecretEnvFrom(containers []corev1.Container) bool {

// }

func (c *Controller) GetSecret(secretRef *apiv1.WorkloadSecret, namespace string) (*corev1.Secret, error) {
	secret, err := c.secretsLister.Secrets(namespace).Get(secretRef.Name)
	if err != nil {
		if errors.IsNotFound(err) {
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

// isSecretExistInWorkload does call all of the below
// I want to check if Annotations does exist in the workload metadata
// I want to check if a specific secret is being used in a workload volumes or env
// function to check workload for secret annotations existence
// function to check workload for secret volumes existence
// function to check workload for secret env existence
