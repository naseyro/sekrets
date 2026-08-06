package v1alpha1

import v1 "k8s.io/apimachinery/pkg/apis/meta/v1"

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
type ManagedSecret struct {
	v1.TypeMeta   `json:",inline"`
	v1.ObjectMeta `json:"metadata,omitempty"`

	Spec ManagedSecretSpec `json:"spec"`

	// Status sub-resource is not supported yet.
}

type ManagedSecretSpec struct {
	SecretRefs []SecretRef `json:"secretRefs"`
}

type SecretRef struct {
	Name            string           `json:"name"`
	Namespace       string           `json:"namespace"`
	TargetWorkloads []TargetWorkload `json:"targetWorkloads"`
}

type TargetWorkload struct {
	Name       string `json:"name"`
	Kind       string `json:"kind"`
	APIVersion string `json:"apiVersion"`
	Namespace  string `json:"namespace,omitempty"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
type ManagedSecretList struct {
	v1.TypeMeta `json:",inline"`
	v1.ListMeta `json:"metadata,omitempty"`

	Items []ManagedSecret `json:"items"`
}
