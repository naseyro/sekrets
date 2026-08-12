package v1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
type SecretsManager struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec SecretsManagerSpec `json:"spec,omitempty"`
}

type SecretsManagerSpec struct {
	TargetWorkloads []Workload `json:"workloads"`
}

type Workload struct {
	Name       string           `json:"name"`
	Kind       string           `json:"kind"`
	APIVersion string           `json:"apiVersion"`
	Namespace  string           `json:"namespace,omitempty"`
	Secrets    []WorkloadSecret `json:"secrets"`
}

type WorkloadSecret struct {
	Name        string       `json:"name"`
	MountConfig *MountConfig `json:"mountConfig,omitempty"`
}

type MountConfig struct {
	MountType string `json:"mountType"`
	MountPath string `json:"mountPath,omitempty"`
	EnvName   string `json:"envName,omitempty"`
	SecretKey string `json:"secretKey,omitempty"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
type SecretsManagerList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`

	Items []SecretsManager `json:"items"`
}
