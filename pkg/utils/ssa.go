package utils

import (
	apiv1 "github.com/naseyro/ssc/pkg/apis/secrets.management.io/v1"
	corev1 "k8s.io/api/core/v1"
)

const FieldManager = "secrets.management.io"

type ContainerInjection struct {
	Name         string                 `json:"name"`
	Env          []corev1.EnvVar        `json:"env,omitempty"`
	VolumeMounts []corev1.VolumeMount   `json:"volumeMounts,omitempty"`
	EnvFrom      []corev1.EnvFromSource `json:"envFrom,omitempty"`
}

type InjectedFields struct {
	Volumes    []corev1.Volume
	Containers []ContainerInjection
}

type listAccumulator[T any] struct {
	order []string
	items map[string]T
}

func newListAccumulator[T any]() *listAccumulator[T] {
	return &listAccumulator[T]{items: map[string]T{}}
}

func (a *listAccumulator[T]) set(key string, item T) {
	if _, ok := a.items[key]; !ok {
		a.order = append(a.order, key)
	}
	a.items[key] = item
}

func (a *listAccumulator[T]) setIfAbsent(key string, item T) {
	if _, ok := a.items[key]; ok {
		return
	}
	a.items[key] = item
	a.order = append(a.order, key)
}

func (a *listAccumulator[T]) list() []T {
	out := make([]T, 0, len(a.order))
	for _, key := range a.order {
		out = append(out, a.items[key])
	}
	return out
}

func ComputeInjectedFields(workloadTemplate *corev1.PodTemplateSpec, secretRefs []apiv1.WorkloadSecret) *InjectedFields {
	fields := &InjectedFields{}
	if workloadTemplate == nil || len(workloadTemplate.Spec.Containers) == 0 {
		return fields
	}

	volumes := newListAccumulator[corev1.Volume]()
	envs := make([]*listAccumulator[corev1.EnvVar], len(workloadTemplate.Spec.Containers))
	mounts := make([]*listAccumulator[corev1.VolumeMount], len(workloadTemplate.Spec.Containers))
	envFroms := make([]*listAccumulator[corev1.EnvFromSource], len(workloadTemplate.Spec.Containers))
	for i := range workloadTemplate.Spec.Containers {
		envs[i] = newListAccumulator[corev1.EnvVar]()
		mounts[i] = newListAccumulator[corev1.VolumeMount]()
		envFroms[i] = newListAccumulator[corev1.EnvFromSource]()
	}

	for i := range secretRefs {
		secretRef := &secretRefs[i]
		switch secretMountType(secretRef) {
		case "volume":
			volumeName := uniqueVolumeName(secretRef.Name, workloadTemplate.Spec.Volumes)
			volumes.set(volumeName, corev1.Volume{
				Name: volumeName,
				VolumeSource: corev1.VolumeSource{
					Secret: &corev1.SecretVolumeSource{SecretName: secretRef.Name},
				},
			})
			mount := corev1.VolumeMount{
				Name:      volumeName,
				MountPath: desiredVolumeMountPath(secretRef),
				ReadOnly:  true,
			}
			for j := range mounts {
				mounts[j].set(mount.MountPath, mount)
			}
		case "envFrom":
			envFrom := corev1.EnvFromSource{
				SecretRef: &corev1.SecretEnvSource{
					LocalObjectReference: corev1.LocalObjectReference{Name: secretRef.Name},
				},
			}
			for j := range envFroms {
				envFroms[j].setIfAbsent(envFrom.Prefix, envFrom)
			}
		case "env":
			env := corev1.EnvVar{
				Name: secretRef.MountConfig.EnvName,
				ValueFrom: &corev1.EnvVarSource{
					SecretKeyRef: &corev1.SecretKeySelector{
						LocalObjectReference: corev1.LocalObjectReference{Name: secretRef.Name},
						Key:                  secretRef.MountConfig.SecretKey,
					},
				},
			}
			for j := range envs {
				envs[j].set(env.Name, env)
			}
		}
	}

	fields.Volumes = volumes.list()
	for i := range workloadTemplate.Spec.Containers {
		envList := envs[i].list()
		mountList := mounts[i].list()
		envFromList := envFroms[i].list()
		if len(envList) == 0 && len(mountList) == 0 && len(envFromList) == 0 {
			continue
		}
		fields.Containers = append(fields.Containers, ContainerInjection{
			Name:         workloadTemplate.Spec.Containers[i].Name,
			Env:          envList,
			VolumeMounts: mountList,
			EnvFrom:      envFromList,
		})
	}
	return fields
}
