package controller

import (
	"context"
	"fmt"
	"strings"
	"time"

	"k8s.io/klog/v2"

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
	mapper           *restmapper.DeferredDiscoveryRESTMapper
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
		mapper:           mapper,
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
		utilruntime.HandleError(fmt.Errorf("error converting objecto to Secret"))
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

	go c.msInformer.Run(stopCh)
	go c.secretsInformer.Run(stopCh)

	klog.Info("Waiting for informer caches to sync")
	if ok := cache.WaitForCacheSync(stopCh, c.msInformer.HasSynced, c.secretsInformer.HasSynced); !ok {
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
		unstructuredWorkload, err := c.dynamicClient.Resource(resource).Get(context.Background(), workload.Name, metav1.GetOptions{})
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

		var isModified bool

		for _, s := range workload.Secrets {
			secret, err := c.GetSecret(&s, workload.Namespace)
			if err != nil {
				continue
			}
			computedHash := utils.ComputeSecretHash(secret)
			var isMounted bool

			if s.MountConfig == nil || s.MountConfig.MountType == "volume" || s.MountConfig.MountType == "" {
				isMounted = isSecretVolumed(workloadTemplate, s.Name)
			} else if s.MountConfig.MountType == "envFrom" {
				isMounted = isSecretEnvFromMounted(workloadTemplate, s.Name)
			} else if s.MountConfig.MountType == "env" {
				isMounted = isSecretEnvMounted(workloadTemplate, s.Name, s.MountConfig.EnvName)
			}

			if isMounted {
				if !isSecretAnnotated(workloadTemplate, s.Name, computedHash) {
					annotate(workloadTemplate, s.Name, computedHash)
					isModified = true
				}
			} else {
				if s.MountConfig == nil || s.MountConfig.MountType == "volume" || s.MountConfig.MountType == "" {
					injectVolume(workloadTemplate, &s)
				} else if s.MountConfig.MountType == "envFrom" {
					injectEnvFrom(workloadTemplate, s.Name)
				} else if s.MountConfig.MountType == "env" {
					injectEnv(workloadTemplate, s.Name, s.MountConfig.EnvName, s.MountConfig.SecretKey)
				}
				annotate(workloadTemplate, s.Name, computedHash)
				isModified = true
			}
		}
		if isModified {
			err = toPodTemplateSpec(unstructuredWorkload, workloadTemplate)
			if err != nil {
				errs = append(errs, fmt.Errorf("failed to convert template back for %s: %w", workload.Name, err))
				continue
			}

			_, err = c.dynamicClient.Resource(resource).
				Namespace(unstructuredWorkload.GetNamespace()).
				Update(context.Background(), unstructuredWorkload, metav1.UpdateOptions{})

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

func (c *Controller) addSecretKeyToWorkqueue(secret *corev1.Secret) {
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

func isSecretVolumed(workloadTemplate *corev1.PodTemplateSpec, secretName string) bool {
	if workloadTemplate == nil || len(workloadTemplate.Spec.Containers) == 0 {
		return false
	}

	volumeName := fmt.Sprintf("%s-volume", secretName)

	hasVolume := false
	for _, volume := range workloadTemplate.Spec.Volumes {
		if volume.Name == volumeName && volume.Secret != nil && volume.Secret.SecretName == secretName {
			hasVolume = true
			break
		}
	}

	if !hasVolume {
		return false
	}

	for i := range workloadTemplate.Spec.Containers {
		hasMount := false
		for _, mount := range workloadTemplate.Spec.Containers[i].VolumeMounts {
			if mount.Name == volumeName {
				hasMount = true
				break
			}
		}

		if !hasMount {
			return false
		}
	}
	return true
}

func injectVolume(workloadTemplate *corev1.PodTemplateSpec, secretRef *apiv1.WorkloadSecret) {
	volumeName := fmt.Sprintf("%s-volume", secretRef.Name)

	mountPath := secretRef.MountConfig.MountPath
	if mountPath == "" {
		mountPath = fmt.Sprintf("/etc/secrets/%s", secretRef.Name)
	}

	hasVolume := false
	for _, vol := range workloadTemplate.Spec.Volumes {
		if vol.Name == volumeName {
			hasVolume = true
			break
		}
	}
	if !hasVolume {
		workloadTemplate.Spec.Volumes = append(workloadTemplate.Spec.Volumes, corev1.Volume{
			Name: volumeName,
			VolumeSource: corev1.VolumeSource{
				Secret: &corev1.SecretVolumeSource{SecretName: secretRef.Name},
			},
		})
	}

	for i := range workloadTemplate.Spec.Containers {
		hasMount := false
		for _, mount := range workloadTemplate.Spec.Containers[i].VolumeMounts {
			if mount.Name == volumeName {
				hasMount = true
				break
			}
		}

		if !hasMount {
			workloadTemplate.Spec.Containers[i].VolumeMounts = append(
				workloadTemplate.Spec.Containers[i].VolumeMounts,
				corev1.VolumeMount{
					Name:      volumeName,
					MountPath: mountPath,
					ReadOnly:  true,
				},
			)
		}
	}
}

func isSecretEnvFromMounted(workloadTemplate *corev1.PodTemplateSpec, secretName string) bool {
	if workloadTemplate == nil || len(workloadTemplate.Spec.Containers) == 0 {
		return false
	}
	containers := workloadTemplate.Spec.Containers
	for i := range containers {
		var found bool
		for _, envFrom := range containers[i].EnvFrom {
			if envFrom.SecretRef != nil && envFrom.SecretRef.Name == secretName {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	return true
}

func injectEnvFrom(workloadTemplate *corev1.PodTemplateSpec, secretName string) {
	containers := workloadTemplate.Spec.Containers
	for i := range containers {
		hasEnvFrom := false
		for _, ef := range containers[i].EnvFrom {
			if ef.SecretRef != nil && ef.SecretRef.Name == secretName {
				hasEnvFrom = true
				break
			}
		}

		if !hasEnvFrom {
			containers[i].EnvFrom = append(
				containers[i].EnvFrom,
				corev1.EnvFromSource{
					SecretRef: &corev1.SecretEnvSource{
						LocalObjectReference: corev1.LocalObjectReference{Name: secretName},
					},
				},
			)
		}
	}
}

func isSecretEnvMounted(workloadTemplate *corev1.PodTemplateSpec, secretName string, envName string) bool {
	if workloadTemplate == nil || len(workloadTemplate.Spec.Containers) == 0 {
		return false
	}
	containers := workloadTemplate.Spec.Containers
	for i := range containers {
		var found bool

		for _, env := range containers[i].Env {
			if env.Name == envName {
				if env.ValueFrom != nil && env.ValueFrom.SecretKeyRef != nil {
					if env.ValueFrom.SecretKeyRef.Name == secretName {
						found = true
						break
					}
				}
			}
		}
		if !found {
			return false
		}
	}

	return true
}

func injectEnv(workloadTemplate *corev1.PodTemplateSpec, secretName string, envName string, secretKey string) {
	if workloadTemplate == nil {
		return
	}

	for i := range workloadTemplate.Spec.Containers {
		targetEnv := corev1.EnvVar{
			Name: envName,
			ValueFrom: &corev1.EnvVarSource{
				SecretKeyRef: &corev1.SecretKeySelector{
					LocalObjectReference: corev1.LocalObjectReference{Name: secretName},
					Key:                  secretKey,
				},
			},
		}
		envIndex := -1
		for j, env := range workloadTemplate.Spec.Containers[i].Env {
			if env.Name == envName {
				envIndex = j
				break
			}
		}

		if envIndex == -1 {
			workloadTemplate.Spec.Containers[i].Env = append(
				workloadTemplate.Spec.Containers[i].Env,
				targetEnv,
			)
		} else {
			workloadTemplate.Spec.Containers[i].Env[envIndex] = targetEnv
		}
	}
}

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
