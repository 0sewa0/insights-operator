package clusterconfig

import (
	"fmt"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/openshift/insights-operator/pkg/record"
)

// GatherCertificateSigningRequests collects anonymized CertificateSigningRequests.
// Collects CSRs which werent Verified, or when Now < ValidBefore or Now > ValidAfter
//
// The Kubernetes api https://github.com/kubernetes/client-go/blob/master/kubernetes/typed/certificates/v1beta1/certificatesigningrequest.go#L78
// Response see https://docs.openshift.com/container-platform/4.3/rest_api/index.html#certificatesigningrequestlist-v1beta1certificates
//
// Location in archive: config/certificatesigningrequests/
func GatherCertificateSigningRequests(i *Gatherer) func() ([]record.Record, []error) {
	return func() ([]record.Record, []error) {
		requests, err := i.certClient.CertificateSigningRequests().List(metav1.ListOptions{})
		if errors.IsNotFound(err) {
			return nil, nil
		}
		if err != nil {
			return nil, []error{err}
		}
		csrs, err := FromCSRs(requests).Anonymize().Filter(IncludeCSR).Select()
		if err != nil {
			return nil, []error{err}
		}
		records := make([]record.Record, len(csrs))
		for i, sr := range csrs {
			records[i] = record.Record{Name: fmt.Sprintf("config/certificatesigningrequests/%s", sr.ObjectMeta.Name), Item: sr}
		}
		return records, nil
	}
}