package v1alpha1

import (
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

var (
	GroupVersion = schema.GroupVersion{
		Group:   "secretsrotator.io",
		Version: "v1alpha1",
	}
	Scheme         = runtime.NewScheme()
	SchemeBuilder  = runtime.NewSchemeBuilder(addKnownTypes)
	ParameterCodec = runtime.NewParameterCodec(Scheme)
	AddKnownTypes  = SchemeBuilder.AddToScheme
)

func addKnownTypes(s *runtime.Scheme) error {
	s.AddKnownTypes(GroupVersion, &ManagedSecret{}, &ManagedSecretList{})
	v1.AddToGroupVersion(s, GroupVersion)
	return nil
}
