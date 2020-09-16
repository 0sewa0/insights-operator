package clusterconfig

import (
	"io"
	"context"
	"bytes"
	"fmt"
	"time"
	"strings"
	"sort"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	configv1 "github.com/openshift/api/config/v1"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/json"
	"k8s.io/klog"

	"github.com/openshift/insights-operator/pkg/record"
	"github.com/openshift/insights-operator/pkg/record/diskrecorder"
)

// CompactedEvent holds one Namespace Event
type CompactedEvent struct {
	Namespace     string    `json:"namespace"`
	LastTimestamp time.Time `json:"lastTimestamp"`
	Reason        string    `json:"reason"`
	Message       string    `json:"message"`
}

// CompactedEventList is collection of events
type CompactedEventList struct {
	Items []CompactedEvent `json:"items"`
}

// EventAnonymizer implements serialization of Events with anonymization
type EventAnonymizer struct{ *CompactedEventList }

// Marshal serializes Events with anonymization
func (a EventAnonymizer) Marshal(_ context.Context) ([]byte, error) {
	return json.Marshal(a.CompactedEventList)
}

// GetExtension returns extension for anonymized event objects
func (a EventAnonymizer) GetExtension() string {
	return "json"
}

// ClusterOperatorAnonymizer implements serialization of ClusterOperator without change
type ClusterOperatorAnonymizer struct{ *configv1.ClusterOperator }

// Marshal serializes ClusterOperator
func (a ClusterOperatorAnonymizer) Marshal(_ context.Context) ([]byte, error) {
	return runtime.Encode(openshiftSerializer, a.ClusterOperator)
}

// GetExtension returns extension for anonymized cluster operator objects
func (a ClusterOperatorAnonymizer) GetExtension() string {
	return "json"
}

// GatherClusterOperators collects all ClusterOperators.
// It finds unhealthy Pods for unhealthy operators
//
// The Kubernetes api https://github.com/openshift/client-go/blob/master/config/clientset/versioned/typed/config/v1/clusteroperator.go#L62
// Response see https://docs.openshift.com/container-platform/4.3/rest_api/index.html#clusteroperatorlist-v1config-openshift-io
//
// Location of operators in archive: config/clusteroperator/
// See: docs/insights-archive-sample/config/clusteroperator
// Location of pods in archive: config/pod/
func GatherClusterOperators(i *Gatherer) func() ([]record.Record, []error) {
	return func() ([]record.Record, []error) {
		config, err := i.client.ClusterOperators().List(metav1.ListOptions{})
		if errors.IsNotFound(err) {
			return nil, nil
		}
		if err != nil {
			return nil, []error{err}
		}
		records := make([]record.Record, 0, len(config.Items))
		for index := range config.Items {
			if config.Items[index].Name == "insights" {
				i.setInsightsVersion(&config.Items[index])
			}
			records = append(records, record.Record{Name: fmt.Sprintf("config/clusteroperator/%s", config.Items[index].Name), Item: ClusterOperatorAnonymizer{&config.Items[index]}})
		}
		namespaceEventsCollected := sets.NewString()
		now := time.Now()
		unhealthyPods := []*corev1.Pod{}
		for _, item := range config.Items {
			if isHealthyOperator(&item) {
				continue
			}
			for _, namespace := range namespacesForOperator(&item) {
				pods, err := i.coreClient.Pods(namespace).List(metav1.ListOptions{})
				if err != nil {
					klog.V(2).Infof("Unable to find pods in namespace %s for failing operator %s", namespace, item.Name)
					continue
				}
				for j := range pods.Items {
					pod := &pods.Items[j]
					if isHealthyPod(pod, now) {
						continue
					}
					records = append(records, record.Record{Name: fmt.Sprintf("config/pod/%s/%s", pod.Namespace, pod.Name), Item: PodAnonymizer{pod}})
					unhealthyPods = append(unhealthyPods, pod)
				}
				if namespaceEventsCollected.Has(namespace) {
					continue
				}
				namespaceRecords, errs := i.gatherNamespaceEvents(namespace)
				if len(errs) > 0 {
					klog.V(2).Infof("Unable to collect events for namespace %q: %#v", namespace, errs)
					continue
				}
				records = append(records, namespaceRecords...)
				namespaceEventsCollected.Insert(namespace)
			}
		}

		// Exit early if no unhealthy pods found
		if len(unhealthyPods) == 0 {
			return records, nil
		}

		// Fetch a list of containers in unhealthy pods and calculate a log size quota
		// Total log size must not exceed maxLogsSize multiplied by logCompressionRatio
		klog.V(2).Infof("Found %d unhealthy pods", len(unhealthyPods))
		totalUnhealthyContainers := 0
		for _, pod := range unhealthyPods {
			totalUnhealthyContainers += len(pod.Spec.InitContainers) + len(pod.Spec.Containers)
		}
		bufferSize := int64(diskrecorder.MaxLogSize * logCompressionRatio / totalUnhealthyContainers / 2)
		klog.V(2).Infof("Maximum buffer size: %v bytes", bufferSize)
		buf := bytes.NewBuffer(make([]byte, 0, bufferSize))

		// Fetch previous and current container logs
		for _, isPrevious := range []bool{true, false} {
			for _, pod := range unhealthyPods {
				allContainers := pod.Spec.InitContainers
				allContainers = append(allContainers, pod.Spec.Containers...)
				for _, c := range allContainers {
					logName := fmt.Sprintf("%s_current.log", c.Name)
					if isPrevious {
						logName = fmt.Sprintf("%s_previous.log", c.Name)
					}
					buf.Reset()
					klog.V(2).Infof("Fetching logs for %s container %s pod in namespace %s (previous: %v): %q", c.Name, pod.Name, pod.Namespace, isPrevious, err)
					// Collect container logs and continue on error
					err = collectContainerLogs(i, pod, buf, c.Name, isPrevious, &bufferSize)
					if err != nil {
						klog.V(2).Infof("Error: %q", err)
						continue
					}
					records = append(records, record.Record{Name: fmt.Sprintf("config/pod/%s/logs/%s/%s", pod.Namespace, pod.Name, logName), Item: Raw{buf.String()}})
				}
			}
		}

		return records, nil
	}
}


func (i *Gatherer) gatherNamespaceEvents(namespace string) ([]record.Record, []error) {
	// do not accidentally collect events for non-openshift namespace
	if !strings.HasPrefix(namespace, "openshift-") {
		return []record.Record{}, nil
	}
	events, err := i.coreClient.Events(namespace).List(metav1.ListOptions{})
	if err != nil {
		return nil, []error{err}
	}
	// filter the event list to only recent events
	oldestEventTime := time.Now().Add(-maxEventTimeInterval)
	var filteredEventIndex []int
	for i := range events.Items {
		if events.Items[i].LastTimestamp.Time.Before(oldestEventTime) {
			continue
		}
		filteredEventIndex = append(filteredEventIndex, i)

	}
	compactedEvents := CompactedEventList{Items: make([]CompactedEvent, len(filteredEventIndex))}
	for i, index := range filteredEventIndex {
		compactedEvents.Items[i] = CompactedEvent{
			Namespace:     events.Items[index].Namespace,
			LastTimestamp: events.Items[index].LastTimestamp.Time,
			Reason:        events.Items[index].Reason,
			Message:       events.Items[index].Message,
		}
	}
	sort.Slice(compactedEvents.Items, func(i, j int) bool {
		return compactedEvents.Items[i].LastTimestamp.Before(compactedEvents.Items[j].LastTimestamp)
	})
	return []record.Record{{Name: fmt.Sprintf("events/%s", namespace), Item: EventAnonymizer{&compactedEvents}}}, nil
}

// collectContainerLogs fetches log lines from the pod
func collectContainerLogs(i *Gatherer, pod *corev1.Pod, buf *bytes.Buffer, containerName string, isPrevious bool, maxBytes *int64) error {
	req := i.coreClient.Pods(pod.Namespace).GetLogs(pod.Name, &corev1.PodLogOptions{Previous: isPrevious, Container: containerName, LimitBytes: maxBytes, TailLines: &logTailLines})
	readCloser, err := req.Stream()
	if err != nil {
		klog.V(2).Infof("Failed to fetch log for %s pod in namespace %s for failing operator %s (previous: %v): %q", pod.Name, pod.Namespace, containerName, isPrevious, err)
		return err
	}

	defer readCloser.Close()

	_, err = io.Copy(buf, readCloser)
	if err != nil && err != io.ErrShortBuffer {
		klog.V(2).Infof("Failed to write log for %s pod in namespace %s for failing operator %s (previous: %v): %q", pod.Name, pod.Namespace, containerName, isPrevious, err)
		return err
	}
	return nil
}

func isHealthyOperator(operator *configv1.ClusterOperator) bool {
	for _, condition := range operator.Status.Conditions {
		switch {
		case condition.Type == configv1.OperatorDegraded && condition.Status == configv1.ConditionTrue,
			condition.Type == configv1.OperatorAvailable && condition.Status == configv1.ConditionFalse:
			return false
		}
	}
	return true
}

func namespacesForOperator(operator *configv1.ClusterOperator) []string {
	var ns []string
	for _, ref := range operator.Status.RelatedObjects {
		if ref.Resource == "namespaces" {
			ns = append(ns, ref.Name)
		}
	}
	return ns
}

func isHealthyPod(pod *corev1.Pod, now time.Time) bool {
	// pending pods may be unable to schedule or start due to failures, and the info they provide in status is important
	// for identifying why scheduling has not happened
	if pod.Status.Phase == corev1.PodPending {
		if now.Sub(pod.CreationTimestamp.Time) > 2*time.Minute {
			return false
		}
	}
	// pods that have containers that have terminated with non-zero exit codes are considered failure
	for _, status := range pod.Status.InitContainerStatuses {
		if status.LastTerminationState.Terminated != nil && status.LastTerminationState.Terminated.ExitCode != 0 {
			return false
		}
		if status.State.Terminated != nil && status.State.Terminated.ExitCode != 0 {
			return false
		}
		if status.RestartCount > 0 {
			return false
		}
	}
	for _, status := range pod.Status.ContainerStatuses {
		if status.LastTerminationState.Terminated != nil && status.LastTerminationState.Terminated.ExitCode != 0 {
			return false
		}
		if status.State.Terminated != nil && status.State.Terminated.ExitCode != 0 {
			return false
		}
		if status.RestartCount > 0 {
			return false
		}
	}
	return true
}