package clusterconfig

import (
	"fmt"
	"io"
	"io/ioutil"

	"k8s.io/klog"

	"github.com/openshift/insights-operator/pkg/record"
)

const (
	// metricsAlertsLinesLimit is the maximal number of lines read from monitoring Prometheus
	// 500 KiB of alerts is limit, one alert line has typically 450 bytes => 1137 lines.
	// This number has been rounded to 1000 for simplicity.
	// Formerly, the `500 * 1024 / 450` expression was used instead.
	metricsAlertsLinesLimit = 1000
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
			DoRaw(i.ctx)
		if err != nil {
			// write metrics errors to the file format as a comment
			klog.Errorf("Unable to retrieve most recent metrics: %v", err)
			return []record.Record{{Name: "config/metrics", Item: RawByte(fmt.Sprintf("# error: %v\n", err))}}, nil
		}

		rsp, err := i.metricsClient.Get().AbsPath("federate").
			Param("match[]", "ALERTS").
			Stream(i.ctx)
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