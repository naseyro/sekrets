package v1

import (
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

var (
	GroupVersion = schema.GroupVersion{
		Group:   "secrets.management.io",
		Version: "v1",
	}
	Scheme         = runtime.NewScheme()
	SchemeBuilder  = runtime.NewSchemeBuilder(addKnownTypes)
	ParameterCodec = runtime.NewParameterCodec(Scheme)
	AddKnownTypes  = SchemeBuilder.AddToScheme
)

func addKnownTypes(s *runtime.Scheme) error {
	s.AddKnownTypes(GroupVersion, &SecretsManager{}, &SecretsManagerList{})
	v1.AddToGroupVersion(s, GroupVersion)
	return nil
}
