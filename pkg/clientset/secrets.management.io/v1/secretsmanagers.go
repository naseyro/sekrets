package v1

import (
	"context"

	v1 "github.com/naseyro/ssc/pkg/apis/secrets.management.io/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/rest"
)

type SecretsManagersInterface interface {
	Get(ctx context.Context, name string, opts metav1.GetOptions) (*v1.SecretsManager, error)
	Watch(ctx context.Context, opts metav1.ListOptions) (watch.Interface, error)
	List(ctx context.Context, opts metav1.ListOptions) (*v1.SecretsManagerList, error)
}

type SecretsManagers struct {
	rc        rest.Interface
	namespace string
}

func (m *SecretsManagers) Get(ctx context.Context, name string, opts metav1.GetOptions) (*v1.SecretsManager, error) {
	result := &v1.SecretsManager{}
	err := m.rc.Get().Namespace(m.namespace).
		Resource("secretsmanagers").
		Name(name).
		VersionedParams(&opts, v1.ParameterCodec).
		Do(ctx).Into(result)
	return result, err
}

func (m *SecretsManagers) Watch(ctx context.Context, opts metav1.ListOptions) (watch.Interface, error) {
	opts.Watch = true
	return m.rc.Get().Namespace(m.namespace).
		Resource("secretsmanagers").
		VersionedParams(&opts, v1.ParameterCodec).
		Watch(ctx)
}

func (m *SecretsManagers) List(ctx context.Context, opts metav1.ListOptions) (*v1.SecretsManagerList, error) {
	result := &v1.SecretsManagerList{}
	err := m.rc.Get().Namespace(m.namespace).
		Resource("secretsmanagers").
		VersionedParams(&opts, v1.ParameterCodec).
		Do(ctx).Into(result)
	return result, err
}
