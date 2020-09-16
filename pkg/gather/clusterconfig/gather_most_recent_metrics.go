package clusterconfig

import (
	"fmt"
	"io"
	"io/ioutil"
	"context"
	"k8s.io/klog"
	"github.com/openshift/insights-operator/pkg/record"
)

// GatherMostRecentMetrics gathers cluster Federated Monitoring metrics.
//
// The GET REST query to URL /federate
// Gathered metrics:
//   etcd_object_counts
//   cluster_installer
//   namespace CPU and memory usage
//   followed by at most 1000 lines of ALERTS metric
//
// Location in archive: config/metrics/
// See: docs/insights-archive-sample/config/metrics

// RawByte is skipping Marshalling from byte slice
type RawByte []byte

// Marshal just returns bytes
func (r RawByte) Marshal(_ context.Context) ([]byte, error) {
	return r, nil
}

// GetExtension returns extension for "id" file - none
func (r RawByte) GetExtension() string {
	return ""
}

func GatherMostRecentMetrics(i *Gatherer) func() ([]record.Record, []error) {
	return func() ([]record.Record, []error) {
		if i.metricsClient == nil {
			return nil, nil
		}
		data, err := i.metricsClient.Get().AbsPath("federate").
			Param("match[]", "etcd_object_counts").
			Param("match[]", "cluster_installer").
			Param("match[]", "namespace:container_cpu_usage_seconds_total:sum_rate").
			Param("match[]", "namespace:container_memory_usage_bytes:sum").
			DoRaw()
		if err != nil {
			// write metrics errors to the file format as a comment
			klog.Errorf("Unable to retrieve most recent metrics: %v", err)
			return []record.Record{{Name: "config/metrics", Item: RawByte(fmt.Sprintf("# error: %v\n", err))}}, nil
		}

		rsp, err := i.metricsClient.Get().AbsPath("federate").
			Param("match[]", "ALERTS").
			Stream()
		if err != nil {
			// write metrics errors to the file format as a comment
			klog.Errorf("Unable to retrieve most recent alerts from metrics: %v", err)
			return []record.Record{{Name: "config/metrics", Item: RawByte(fmt.Sprintf("# error: %v\n", err))}}, nil
		}
		r := NewLineLimitReader(rsp, metricsAlertsLinesLimit)
		alerts, err := ioutil.ReadAll(r)
		if err != nil && err != io.EOF {
			klog.Errorf("Unable to read most recent alerts from metrics: %v", err)
			return nil, []error{err}
		}

		remainingAlertLines, err := countLines(rsp)
		if err != nil && err != io.EOF {
			klog.Errorf("Unable to count truncated lines of alerts metric: %v", err)
			return nil, []error{err}
		}
		totalAlertCount := r.GetTotalLinesRead() + remainingAlertLines

		// # ALERTS <Total Alerts Lines>/<Alerts Line Limit>
		// The total number of alerts will typically be greater than the true number of alerts by 2
		// because the `# TYPE ALERTS untyped` header and the final empty line are counter in.
		data = append(data, []byte(fmt.Sprintf("# ALERTS %d/%d\n", totalAlertCount, metricsAlertsLinesLimit))...)
		data = append(data, alerts...)
		records := []record.Record{
			{Name: "config/metrics", Item: RawByte(data)},
		}

		return records, nil
	}
}
