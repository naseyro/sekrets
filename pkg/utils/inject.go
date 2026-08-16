package utils

import (
	"fmt"

	apiv1 "github.com/naseyro/ssc/pkg/apis/secrets.management.io/v1"
	corev1 "k8s.io/api/core/v1"
)

func IsSecretMounted(workloadTemplate *corev1.PodTemplateSpec, secretRef *apiv1.WorkloadSecret) bool {
	switch secretMountType(secretRef) {
	case "volume":
		return IsSecretVolumed(workloadTemplate, secretRef)
	case "envFrom":
		return IsSecretEnvFromMounted(workloadTemplate, secretRef.Name)
	case "env":
		return IsSecretEnvMounted(workloadTemplate, secretRef.Name, secretRef.MountConfig.EnvName)
	default:
		return true
	}
}

func InjectSecret(workloadTemplate *corev1.PodTemplateSpec, secretRef *apiv1.WorkloadSecret) {
	switch secretMountType(secretRef) {
	case "volume":
		InjectVolume(workloadTemplate, secretRef)
	case "envFrom":
		InjectEnvFrom(workloadTemplate, secretRef.Name)
	case "env":
		InjectEnv(workloadTemplate, secretRef.Name, secretRef.MountConfig.EnvName, secretRef.MountConfig.SecretKey)
	}
}

func secretMountType(secretRef *apiv1.WorkloadSecret) string {
	if secretRef.MountConfig == nil || secretRef.MountConfig.MountType == "" {
		return "volume"
	}
	return secretRef.MountConfig.MountType
}

func desiredVolumeMountPath(secretRef *apiv1.WorkloadSecret) string {
	if secretRef.MountConfig != nil && secretRef.MountConfig.MountPath != "" {
		return secretRef.MountConfig.MountPath
	}
	return fmt.Sprintf("/etc/secrets/%s", secretRef.Name)
}

func findSecretVolume(spec *corev1.PodSpec, secretName string) *corev1.Volume {
	for i := range spec.Volumes {
		volume := &spec.Volumes[i]
		if volume.Secret != nil && volume.Secret.SecretName == secretName {
			return volume
		}
	}
	return nil
}

func uniqueVolumeName(secretName string, volumes []corev1.Volume) string {
	base := fmt.Sprintf("%s-volume", secretName)
	used := make(map[string]bool, len(volumes))
	for _, volume := range volumes {
		used[volume.Name] = true
	}
	if !used[base] {
		return base
	}
	for i := 1; ; i++ {
		candidate := fmt.Sprintf("%s-%d", base, i)
		if !used[candidate] {
			return candidate
		}
	}
}

func IsSecretVolumed(workloadTemplate *corev1.PodTemplateSpec, secretRef *apiv1.WorkloadSecret) bool {
	if workloadTemplate == nil || len(workloadTemplate.Spec.Containers) == 0 {
		return false
	}

	volume := findSecretVolume(&workloadTemplate.Spec, secretRef.Name)
	if volume == nil {
		return false
	}

	mountPath := desiredVolumeMountPath(secretRef)
	for i := range workloadTemplate.Spec.Containers {
		hasMount := false
		for _, mount := range workloadTemplate.Spec.Containers[i].VolumeMounts {
			if mount.Name == volume.Name && mount.MountPath == mountPath {
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

func InjectVolume(workloadTemplate *corev1.PodTemplateSpec, secretRef *apiv1.WorkloadSecret) {
	if workloadTemplate == nil || len(workloadTemplate.Spec.Containers) == 0 {
		return
	}

	volume := findSecretVolume(&workloadTemplate.Spec, secretRef.Name)
	if volume == nil {
		volumeName := uniqueVolumeName(secretRef.Name, workloadTemplate.Spec.Volumes)
		workloadTemplate.Spec.Volumes = append(workloadTemplate.Spec.Volumes, corev1.Volume{
			Name: volumeName,
			VolumeSource: corev1.VolumeSource{
				Secret: &corev1.SecretVolumeSource{SecretName: secretRef.Name},
			},
		})
		volume = &workloadTemplate.Spec.Volumes[len(workloadTemplate.Spec.Volumes)-1]
	}

	mountPath := desiredVolumeMountPath(secretRef)
	for i := range workloadTemplate.Spec.Containers {
		found := false
		for j := range workloadTemplate.Spec.Containers[i].VolumeMounts {
			mount := &workloadTemplate.Spec.Containers[i].VolumeMounts[j]
			if mount.Name == volume.Name {
				if mount.MountPath != mountPath {
					mount.MountPath = mountPath
				}
				found = true
				break
			}
		}

		if !found {
			workloadTemplate.Spec.Containers[i].VolumeMounts = append(
				workloadTemplate.Spec.Containers[i].VolumeMounts,
				corev1.VolumeMount{
					Name:      volume.Name,
					MountPath: mountPath,
					ReadOnly:  true,
				},
			)
		}
	}
}

func IsSecretEnvFromMounted(workloadTemplate *corev1.PodTemplateSpec, secretName string) bool {
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

func InjectEnvFrom(workloadTemplate *corev1.PodTemplateSpec, secretName string) {
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

func IsSecretEnvMounted(workloadTemplate *corev1.PodTemplateSpec, secretName string, envName string) bool {
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

func InjectEnv(workloadTemplate *corev1.PodTemplateSpec, secretName string, envName string, secretKey string) {
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
