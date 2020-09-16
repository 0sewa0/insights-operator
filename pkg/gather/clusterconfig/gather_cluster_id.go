package clusterconfig

import (
	"github.com/openshift/insights-operator/pkg/record"
)

// GatherClusterID stores ClusterID from ClusterVersion version
// This method uses data already collected by Get ClusterVersion. In particular field .Spec.ClusterID
// The Kubernetes api https://github.com/openshift/client-go/blob/master/config/clientset/versioned/typed/config/v1/clusterversion.go#L50
// Response see https://github.com/openshift/api/blob/master/config/v1/types_cluster_version.go#L38
//
// Location in archive: config/id/
// See: docs/insights-archive-sample/config/id
func GatherClusterID(i *Gatherer) func() ([]record.Record, []error) {
	return func() ([]record.Record, []error) {
		version := i.ClusterVersion()
		if version == nil {
			return nil, nil
		}
		return []record.Record{{Name: "config/id", Item: Raw{string(version.Spec.ClusterID)}}}, nil
	}
}