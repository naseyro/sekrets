package v1

import (
	v1 "github.com/naseyro/ssc/pkg/apis/secrets.management.io/v1"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
)

// TODO: We can create inner interface to include multiple API Versions
// Instead of SecretRotatorV1Alpha1Interface it should be
// type SecretRotatorInterface interface
// and it has multiple functions about API Versions
/*
type SecretRotatorInterface {
	V1Alpha1()
	V1Beta1()
	V1()
}
And inside every version we should implement the ManagedSecret type based on their types.go
*/

type Interface interface {
	Discovery() discovery.DiscoveryInterface
	SecretsManagementV1() SecretsManagementV1Interface
}

type SecretsManagementV1Interface interface {
	SecretsManagers(namespace string) SecretsManagersInterface
}

type SecretsManagementV1 struct {
	rc rest.Interface
}

func (s *SecretsManagementV1) SecretsManagers(namespace string) SecretsManagersInterface {
	return &SecretsManagers{
		namespace: namespace,
		rc:        s.rc,
	}
}

func NewForConfig(c *rest.Config) (*SecretsManagementV1, error) {
	config := *c
	config.APIPath = "/apis"
	config.NegotiatedSerializer = scheme.Codecs.WithoutConversion()
	config.UserAgent = rest.DefaultKubernetesUserAgent()
	config.ContentConfig.GroupVersion = &v1.GroupVersion
	client, err := rest.RESTClientFor(&config)
	if err != nil {
		return nil, err
	}
	return &SecretsManagementV1{
		rc: client,
	}, nil
}
