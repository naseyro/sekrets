package v1

import v1 "k8s.io/apimachinery/pkg/apis/meta/v1"

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
type SecretsManager struct {
	v1.TypeMeta   `json:",inline"`
	v1.ObjectMeta `json:"metadata,omitempty"`

	Spec SecretsManagerSpec `json:"spec"`

	// Status sub-resource is not supported yet.
}

type SecretsManagerSpec struct {
	SecretRefs []SecretRef `json:"secretRefs"`
}

type SecretRef struct {
	Name            string           `json:"name"`
	Namespace       string           `json:"namespace"`
	TargetWorkloads []TargetWorkload `json:"targetWorkloads"`
}

type TargetWorkload struct {
	Name        string      `json:"name"`
	Kind        string      `json:"kind"`
	APIVersion  string      `json:"apiVersion"`
	Namespace   string      `json:"namespace,omitempty"`
	MountConfig MountConfig `json:"mountConfig"`
}

type MountConfig struct {
	MountType string `json:"mountType"`
	MountPath string `json:"mountPath,omitempty"`
	EnvName   string `json:"envName,omitempty"`
	SecretKey string `json:"secretKey,omitempty"`
	// We need to add SecretKeyRef in case envFrom/env is being used.
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
type SecretsManagerList struct {
	v1.TypeMeta `json:",inline"`
	v1.ListMeta `json:"metadata,omitempty"`

	Items []SecretsManager `json:"items"`
}
