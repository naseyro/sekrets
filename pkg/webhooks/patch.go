package webhooks

import (
	"fmt"
	"reflect"

	jsonpatch "gomodules.xyz/jsonpatch/v2"
	corev1 "k8s.io/api/core/v1"
)

const templateBasePath = "/spec/template/spec"

func scopedTemplatePatch(before, after *corev1.PodTemplateSpec) ([]jsonpatch.JsonPatchOperation, error) {
	var ops []jsonpatch.JsonPatchOperation

	for _, volume := range after.Spec.Volumes {
		if !hasVolume(before.Spec.Volumes, volume.Name) {
			if before.Spec.Volumes == nil {
				ops = append(ops, jsonpatch.NewOperation("add", templateBasePath+"/volumes", []corev1.Volume{volume}))
			} else {
				ops = append(ops, jsonpatch.NewOperation("add", templateBasePath+"/volumes/-", volume))
			}
		}
	}

	for i := range after.Spec.Containers {
		if i >= len(before.Spec.Containers) {
			break
		}
		beforeContainer := before.Spec.Containers[i]
		afterContainer := after.Spec.Containers[i]
		containerBase := fmt.Sprintf("%s/containers/%d", templateBasePath, i)

		for _, mount := range afterContainer.VolumeMounts {
			if !hasMount(beforeContainer.VolumeMounts, mount.Name) {
				if beforeContainer.VolumeMounts == nil {
					ops = append(ops, jsonpatch.NewOperation("add", containerBase+"/volumeMounts", []corev1.VolumeMount{mount}))
				} else {
					ops = append(ops, jsonpatch.NewOperation("add", containerBase+"/volumeMounts/-", mount))
				}
			}
		}

		for j := range beforeContainer.VolumeMounts {
			if j < len(afterContainer.VolumeMounts) &&
				afterContainer.VolumeMounts[j].Name == beforeContainer.VolumeMounts[j].Name &&
				afterContainer.VolumeMounts[j].MountPath != beforeContainer.VolumeMounts[j].MountPath {
				ops = append(ops, jsonpatch.NewOperation("replace", fmt.Sprintf("%s/volumeMounts/%d/mountPath", containerBase, j), afterContainer.VolumeMounts[j].MountPath))
			}
		}

		for _, envFrom := range afterContainer.EnvFrom {
			if !hasEnvFrom(beforeContainer.EnvFrom, envFrom) {
				if beforeContainer.EnvFrom == nil {
					ops = append(ops, jsonpatch.NewOperation("add", containerBase+"/envFrom", []corev1.EnvFromSource{envFrom}))
				} else {
					ops = append(ops, jsonpatch.NewOperation("add", containerBase+"/envFrom/-", envFrom))
				}
			}
		}

		for j := range beforeContainer.Env {
			if j < len(afterContainer.Env) && afterContainer.Env[j].Name == beforeContainer.Env[j].Name &&
				!reflect.DeepEqual(afterContainer.Env[j], beforeContainer.Env[j]) {
				ops = append(ops, jsonpatch.NewOperation("replace", fmt.Sprintf("%s/env/%d", containerBase, j), afterContainer.Env[j]))
			}
		}

		for _, env := range afterContainer.Env {
			if !hasEnv(beforeContainer.Env, env.Name) {
				if beforeContainer.Env == nil {
					ops = append(ops, jsonpatch.NewOperation("add", containerBase+"/env", []corev1.EnvVar{env}))
				} else {
					ops = append(ops, jsonpatch.NewOperation("add", containerBase+"/env/-", env))
				}
			}
		}
	}

	return ops, nil
}

func hasVolume(volumes []corev1.Volume, name string) bool {
	for _, volume := range volumes {
		if volume.Name == name {
			return true
		}
	}
	return false
}

func hasMount(mounts []corev1.VolumeMount, name string) bool {
	for _, mount := range mounts {
		if mount.Name == name {
			return true
		}
	}
	return false
}

func hasEnvFrom(envFroms []corev1.EnvFromSource, target corev1.EnvFromSource) bool {
	for _, envFrom := range envFroms {
		if envFrom.SecretRef != nil && target.SecretRef != nil &&
			envFrom.SecretRef.Name == target.SecretRef.Name {
			return true
		}
	}
	return false
}

func hasEnv(envs []corev1.EnvVar, name string) bool {
	for _, env := range envs {
		if env.Name == name {
			return true
		}
	}
	return false
}
