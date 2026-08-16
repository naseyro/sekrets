package listers

import (
	apiv1 "github.com/naseyro/ssc/pkg/apis/secrets.management.io/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/tools/cache"
)

type SecretsManagerLister interface {
	List(selector labels.Selector) (ret []*apiv1.SecretsManager, err error)
	SecretsManagers(namespace string) SecretsManagerNamespaceLister
	GetBySecretName(secretName string) ([]*apiv1.SecretsManager, error)
}

type SecretsManagerNamespaceLister interface {
	List(selector labels.Selector) (ret []*apiv1.SecretsManager, err error)
	Get(name string) (ret *apiv1.SecretsManager, err error)
}

type secretsManagerLister struct {
	indexer cache.Indexer
}

func NewSecretsManagerLister(indexer cache.Indexer) SecretsManagerLister {
	return &secretsManagerLister{indexer: indexer}
}

func (s *secretsManagerLister) List(selector labels.Selector) (ret []*apiv1.SecretsManager, err error) {
	err = cache.ListAll(s.indexer, selector, func(m interface{}) {
		ret = append(ret, m.(*apiv1.SecretsManager))
	})
	return ret, err
}

func (s *secretsManagerLister) SecretsManagers(namespace string) SecretsManagerNamespaceLister {
	return &secretsManagerNamespaceLister{indexer: s.indexer, namespace: namespace}
}

func (s *secretsManagerLister) GetBySecretName(secretName string) ([]*apiv1.SecretsManager, error) {
	objs, err := s.indexer.ByIndex("secretName", secretName)
	if err != nil {
		return nil, err
	}

	var ret []*apiv1.SecretsManager
	for _, obj := range objs {
		ret = append(ret, obj.(*apiv1.SecretsManager))
	}

	return ret, nil
}

type secretsManagerNamespaceLister struct {
	indexer   cache.Indexer
	namespace string
}

func (s *secretsManagerNamespaceLister) List(selector labels.Selector) (ret []*apiv1.SecretsManager, err error) {
	err = cache.ListAllByNamespace(s.indexer, s.namespace, selector, func(m interface{}) {
		ret = append(ret, m.(*apiv1.SecretsManager))
	})
	return ret, err
}

func (s *secretsManagerNamespaceLister) Get(name string) (*apiv1.SecretsManager, error) {
	obj, exists, err := s.indexer.GetByKey(s.namespace + "/" + name)
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, errors.NewNotFound(schema.GroupResource{
			Group:    apiv1.GroupVersion.Group,
			Resource: "secretsmanager",
		}, name)
	}
	return obj.(*apiv1.SecretsManager), nil
}
