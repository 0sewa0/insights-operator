package clusterconfig

import (
	"context"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	registryv1 "github.com/openshift/api/imageregistry/v1"

	"github.com/openshift/insights-operator/pkg/record"
)

// GatherClusterImagePruner fetches the image pruner configuration
//
// Location in archive: config/imagepruner/
func GatherClusterImagePruner(i *Gatherer) func() ([]record.Record, []error) {
	return func() ([]record.Record, []error) {
		pruner, err := i.registryClient.ImagePruners().Get("cluster", metav1.GetOptions{})
		if errors.IsNotFound(err) {
			return nil, nil
		}
		if err != nil {
			return nil, []error{err}
		}
		return []record.Record{{Name: "config/imagepruner", Item: ImagePrunerAnonymizer{pruner}}}, nil
	}
}

// ImagePrunerAnonymizer implements serialization with marshalling
type ImagePrunerAnonymizer struct {
	*registryv1.ImagePruner
}

// Marshal serializes ImagePruner with anonymization
func (a ImagePrunerAnonymizer) Marshal(_ context.Context) ([]byte, error) {
	return runtime.Encode(registrySerializer.LegacyCodec(registryv1.SchemeGroupVersion), a.ImagePruner)
}

// GetExtension returns extension for anonymized image pruner objects
func (a ImagePrunerAnonymizer) GetExtension() string {
	return "json"
}