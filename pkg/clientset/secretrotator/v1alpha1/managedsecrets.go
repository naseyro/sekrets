package v1alpha1

import (
	"context"

	"github.com/naseyro/ssc/pkg/apis/secretrotator/v1alpha1"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/rest"
)

type ManagedSecretsInterface interface {
	Get(ctx context.Context, name string, opts v1.GetOptions) (*v1alpha1.ManagedSecret, error)
	Watch(ctx context.Context, opts v1.ListOptions) (watch.Interface, error)
	List(ctx context.Context, opts v1.ListOptions) (*v1alpha1.ManagedSecretList, error)
}

type ManagedSecrets struct {
	rc        rest.Interface
	namespace string
}

func (m *ManagedSecrets) Get(ctx context.Context, name string, opts v1.GetOptions) (*v1alpha1.ManagedSecret, error) {
	result := &v1alpha1.ManagedSecret{}
	err := m.rc.Get().Namespace(m.namespace).
		Resource("managedsecrets").
		VersionedParams(&opts, v1alpha1.ParameterCodec).
		Do(ctx).Into(result)
	return result, err
}

func (m *ManagedSecrets) Watch(ctx context.Context, opts v1.ListOptions) (watch.Interface, error) {
	opts.Watch = true
	return m.rc.Get().Namespace(m.namespace).
		Resource("managedsecrets").
		VersionedParams(&opts, v1alpha1.ParameterCodec).
		Watch(ctx)
}

func (m *ManagedSecrets) List(ctx context.Context, opts v1.ListOptions) (*v1alpha1.ManagedSecretList, error) {
	result := &v1alpha1.ManagedSecretList{}
	err := m.rc.Get().Namespace(m.namespace).
		Resource("managedsecrets").
		VersionedParams(&opts, v1alpha1.ParameterCodec).
		Do(ctx).Into(result)
	return result, err
}
