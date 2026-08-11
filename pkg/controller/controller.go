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
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
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
)

type Controller struct {
	msClientSet      v1.SecretsManagementV1Interface
	dynamicClient    dynamic.Interface
	discoveryClient  discovery.DiscoveryInterface
	kubernetesClient kubernetes.Interface
	workqueue        workqueue.TypedRateLimitingInterface[string]
	msInformer       cache.SharedIndexInformer
	secretsInfomer   cache.SharedIndexInformer
	secretsLister    corev1listers.SecretLister
	mapper           restmapper.DeferredDiscoveryRESTMapper
}

func NewController(sc v1.SecretsManagementV1Interface, kc kubernetes.Interface, dc dynamic.Interface, discoveryClient discovery.DiscoveryInterface) *Controller {
	workqueue := workqueue.NewTypedRateLimitingQueue(workqueue.DefaultTypedControllerRateLimiter[string]())
	managedSecretsInformer := informers.NewSharedIndexInformer(sc)
	secretsInformer := informers.NewSecretsSharedInformer(kc)
	secretsLister := corev1listers.NewSecretLister(secretsInformer.GetIndexer())
	mapper := restmapper.NewDeferredDiscoveryRESTMapper(memory.NewMemCacheClient(discoveryClient))
	ctr := &Controller{
		msClientSet:      sc,
		dynamicClient:    dc,
		discoveryClient:  discoveryClient,
		kubernetesClient: kc,
		workqueue:        workqueue,
		msInformer:       managedSecretsInformer,
		secretsInfomer:   secretsInformer,
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
		utilruntime.HandleError(fmt.Errorf("error could not retrieve key: %v", err))
		return
	}
	c.workqueue.Add(key)
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

	if err := c.reconcile(key); err != nil {
		utilruntime.HandleError(err)
		c.workqueue.AddRateLimited(key)
		return true
	}
	c.workqueue.Forget(key)
	return true
}

func (c *Controller) reconcile(key string) error {
	ns, name, err := SeparateKey(key)
	if err != nil {
		return err
	}
	secretManager, err := c.msClientSet.SecretsManagers(ns).Get(context.Background(), name, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("error could not retrieve SecretManager %s", name)
	}
	sRefs := secretManager.Spec.SecretRefs

	for i, secret := range sRefs {
		// Check that this secret does exist in the listed workloads
		// if secret does not exist, we need to inject it first in the Workload.
		// if it does exist, check if secret hash and the hash in the workloads are equal == return
		// if hashes are not equal we enforce to update

	}
	return nil
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
				c.workqueue.Add(key)
			}
		}
	}
}

func (c *Controller) isSecretExistinWorkload(secretRef *apiv1.SecretRef) (bool, error) {
	secret, err := c.GetSecret(secretRef)
	if err != nil {
		return false, err
	}
	hash := utils.ComputeSecretHash(secret)
	for _, workload := range secretRef.TargetWorkloads {
		// We need to retrieve Workload Volumes information and Annotations
		// we need to check if the specific secretRef does exist in that workload template spec
		// if does exist in its Volumes or env/envFrom and annotations it's fine.
		// if does not exist in both, we add it based on the mountPath defined.
		// if the annotations are outdated or does not exist, we need to add it.
		// now this workload has the Secret inside of it and the Rollout update is being processed.
		// We need to repeat this for all available workloads for a specific SecretRef.

	}
}

func (c *Controller) getWorkloadSecrets(workload *apiv1.TargetWorkload, secretName, hash string) (bool, error) {
	workloadTemplate, err := c.getWorkloadTemplate(workload)
	if err != nil {
		return false, err
	}
	mapping, gvr, err := c.getGVR(workload)
	if err != nil {
		return false, err
	}
	mountType := workload.MountConfig.MountType
	if mountType == "volume" {
		if !isSecretVolumed(workloadTemplate, secretName) {
			c.injectSecretAsVolume(workloadTemplate, workload, gvr, secretName)
		}
	} else {
		// secret env/envFrom logic
	}
	annotateSecrets(workloadTemplate, secretName, hash)

}

func (c *Controller) getWorkloadTemplate(workload *apiv1.TargetWorkload) (*corev1.PodTemplateSpec, error) {
	group, version, err := SeparateKey(workload.APIVersion)
	if err != nil {
		return nil, fmt.Errorf("error structuring the GroupVersion", err)
	}
	_, resource, err := c.getGVR(group, version, workload.Kind)
	if err != nil {
		return nil, err
	}
	unstructuredWorkload, err := c.dynamicClient.Resource(resource).Namespace(workload.Namespace).Get(context.Background(), workload.Name, metav1.GetOptions{})
	if err != nil {
		return nil, fmt.Errorf("error retrieving workload object: %v", err)
	}
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

func (c *Controller) getGVR(workload *apiv1.TargetWorkload) (*meta.RESTMapping, schema.GroupVersionResource, error) {
	group, version, err := SeparateKey(workload.APIVersion)
	if err != nil {
		return nil, schema.GroupVersionResource{}, fmt.Errorf("error structuring the GroupVersion", err)
	}
	mapping, err := c.mapper.RESTMapping(schema.GroupKind{
		Group: group,
		Kind:  workload.Kind,
	}, version)
	if err != nil {
		return nil, schema.GroupVersionResource{}, fmt.Errorf("error creating the RESTMapping: %v", err)
	}
	return mapping, mapping.Resource, nil
}

func annotateSecrets(workloadTemplate *corev1.PodTemplateSpec, secretName, hash string) bool {
	if workloadTemplate.Annotations == nil {
		workloadTemplate.Annotations = make(map[string]string)
	}

	key := fmt.Sprintf("secrets.management.io/%s", secretName)
	if currentHash, exists := workloadTemplate.Annotations[key]; exists && currentHash == hash {
		return false
	}
	workloadTemplate.Annotations[key] = hash
	return true
}

func isSecretVolumed(workloadTemplate *corev1.PodTemplateSpec, secretName string) bool {
	for _, volume := range workloadTemplate.Spec.Volumes {
		if volume.Secret != nil && volume.Secret.SecretName == secretName {
			return true
		}
	}
	return false
}

func (c *Controller) injectSecretAsVolume(workloadTemplate *corev1.PodTemplateSpec, workload *apiv1.TargetWorkload, gvr schema.GroupVersionResource, secretName string) error {
	volumeName := fmt.Sprintf("%s-volume", secretName)
	if workload.MountConfig.MountPath == "" {
		workload.MountConfig.MountPath = "/etc/secrets"
	}

	workloadTemplate.Spec.Volumes = append(workloadTemplate.Spec.Volumes, corev1.Volume{
		Name: volumeName,
		VolumeSource: corev1.VolumeSource{
			Secret: &corev1.SecretVolumeSource{
				SecretName: secretName,
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
				MountPath: workload.MountConfig.MountPath,
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

func (c *Controller) GetSecret(secretRef *apiv1.SecretRef) (*corev1.Secret, error) {

	secret, err := c.secretsLister.Secrets(secretRef.Namespace).Get(secretRef.Name)
	if err != nil {
		if errors.IsNotFound(err) {
			return nil, fmt.Errorf("referenced secret %s/%s not found in cache", secretRef.Namespace, secretRef.Name)
		}
		return nil, fmt.Errorf("failed to fetch secret %s/%s: %w", secretRef.Namespace, secretRef.Name, err)
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

func toTemplateSpec() {}
