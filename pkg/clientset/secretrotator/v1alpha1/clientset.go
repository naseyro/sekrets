package v1alpha1

import (
	"github.com/naseyro/ssc/pkg/apis/secretrotator/v1alpha1"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
)

type SecretRotatorV1Alpha1Interface interface {
	ManagedSecrets(namespace string) ManagedSecretsInterface
}

type SecretRotatorV1Alpha1 struct {
	rc rest.Interface
}

func (s *SecretRotatorV1Alpha1) ManagedSecrets(namespace string) ManagedSecretsInterface {
	return &ManagedSecrets{
		namespace: namespace,
		rc:        s.rc,
	}
}

func NewForConfig(c *rest.Config) (*SecretRotatorV1Alpha1, error) {
	config := *c
	config.APIPath = "/apis"
	config.NegotiatedSerializer = scheme.Codecs.WithoutConversion()
	config.UserAgent = rest.DefaultKubernetesUserAgent()
	config.ContentConfig.GroupVersion = &v1alpha1.GroupVersion
	client, err := rest.RESTClientFor(&config)
	if err != nil {
		return nil, err
	}
	return &SecretRotatorV1Alpha1{
		rc: client,
	}, nil
}
