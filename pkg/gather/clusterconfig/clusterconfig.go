package clusterconfig

import (
	"bytes"
	"context"
	"io"
	"regexp"
	"strings"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	kubescheme "k8s.io/client-go/kubernetes/scheme"
	certificatesv1beta1 "k8s.io/client-go/kubernetes/typed/certificates/v1beta1"
	corev1client "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/rest"

	configv1 "github.com/openshift/api/config/v1"
	registryv1 "github.com/openshift/api/imageregistry/v1"
	openshiftscheme "github.com/openshift/client-go/config/clientset/versioned/scheme"
	configv1client "github.com/openshift/client-go/config/clientset/versioned/typed/config/v1"
	imageregistryv1 "github.com/openshift/client-go/imageregistry/clientset/versioned/typed/imageregistry/v1"

	"github.com/openshift/insights-operator/pkg/record"
)

const (
	// Log compression ratio is defining a multiplier for uncompressed logs
	// diskrecorder would refuse to write files larger than MaxLogSize, so GatherClusterOperators
	// has to limit the expected size of the buffer for logs
	logCompressionRatio = 2

	// imageGatherPodLimit is the maximum number of pods that
	// will be listed in a single request to reduce memory usage.
	imageGatherPodLimit = 200

	// metricsAlertsLinesLimit is the maximal number of lines read from monitoring Prometheus
	// 500 KiB of alerts is limit, one alert line has typically 450 bytes => 1137 lines.
	// This number has been rounded to 1000 for simplicity.
	// Formerly, the `500 * 1024 / 450` expression was used instead.
	metricsAlertsLinesLimit = 1000
)

var (
	openshiftSerializer = openshiftscheme.Codecs.LegacyCodec(configv1.SchemeGroupVersion)
	kubeSerializer      = kubescheme.Codecs.LegacyCodec(corev1.SchemeGroupVersion)

	// maxEventTimeInterval represents the "only keep events that are maximum 1h old"
	// TODO: make this dynamic like the reporting window based on configured interval
	maxEventTimeInterval = 1 * time.Hour

	registrySerializer serializer.CodecFactory
	registryScheme     = runtime.NewScheme()

	// logTailLines sets maximum number of lines to fetch from pod logs
	logTailLines = int64(100)

	imageHostRegex = regexp.MustCompile(`(^|\.)(openshift\.org|registry\.redhat\.io|registry\.access\.redhat\.com)$`)

	// lineSep is the line separator used by the alerts metric
	lineSep = []byte{'\n'}
)

func init() {
	utilruntime.Must(registryv1.AddToScheme(registryScheme))
	registrySerializer = serializer.NewCodecFactory(registryScheme)
}

// Gatherer is a driving instance invoking collection of data
type Gatherer struct {
	client         configv1client.ConfigV1Interface
	coreClient     corev1client.CoreV1Interface
	metricsClient  rest.Interface
	certClient     certificatesv1beta1.CertificatesV1beta1Interface
	registryClient imageregistryv1.ImageregistryV1Interface
	lock           sync.Mutex
	lastVersion    *configv1.ClusterVersion
	insightsVersion string
}

// New creates new Gatherer
func New(client configv1client.ConfigV1Interface, coreClient corev1client.CoreV1Interface, certClient certificatesv1beta1.CertificatesV1beta1Interface, metricsClient rest.Interface,
	registryClient imageregistryv1.ImageregistryV1Interface) *Gatherer {
	return &Gatherer{
		client:         client,
		coreClient:     coreClient,
		certClient:     certClient,
		metricsClient:  metricsClient,
		registryClient: registryClient,
	}
}

// Gather is hosting and calling all the recording functions
func (i *Gatherer) Gather(ctx context.Context, recorder record.Interface) error {
	return record.Collect(ctx, recorder,
		GatherMostRecentMetrics(i),
		GatherClusterOperators(i),
		GatherContainerImages(i),
		GatherNodes(i),
		GatherConfigMaps(i),
		GatherClusterVersion(i),
		GatherClusterID(i),
		GatherClusterInfrastructure(i),
		GatherClusterNetwork(i),
		GatherClusterAuthentication(i),
		GatherClusterImageRegistry(i),
		GatherClusterImagePruner(i),
		GatherClusterFeatureGates(i),
		GatherClusterOAuth(i),
		GatherClusterIngress(i),
		GatherClusterProxy(i),
		GatherCertificateSigningRequests(i),
	)
}

// Raw is another simplification of marshalling from string
type Raw struct{ string }

// Marshal returns raw bytes
func (r Raw) Marshal(_ context.Context) ([]byte, error) {
	return []byte(r.string), nil
}

// GetExtension returns extension for raw marshaller
func (r Raw) GetExtension() string {
	return ""
}

// Anonymizer returns serialized runtime.Object without change
type Anonymizer struct{ runtime.Object }

// Marshal serializes with OpenShift client-go openshiftSerializer
func (a Anonymizer) Marshal(_ context.Context) ([]byte, error) {
	return runtime.Encode(openshiftSerializer, a.Object)
}

// GetExtension returns extension for anonymized openshift objects
func (a Anonymizer) GetExtension() string {
	return "json"
}

var reURL = regexp.MustCompile(`[^\.\-/\:]`)

func anonymizeURL(s string) string { return reURL.ReplaceAllString(s, "x") }

func anonymizeString(s string) string {
	return strings.Repeat("x", len(s))
}

// PodAnonymizer implements serialization with anonymization for a Pod
type PodAnonymizer struct{ *corev1.Pod }

// Marshal implements serialization of a Pod with anonymization
func (a PodAnonymizer) Marshal(_ context.Context) ([]byte, error) {
	return runtime.Encode(kubeSerializer, anonymizePod(a.Pod))
}

// GetExtension returns extension for anonymized pod objects
func (a PodAnonymizer) GetExtension() string {
	return "json"
}

func anonymizePod(pod *corev1.Pod) *corev1.Pod {
	// pods gathered from openshift namespaces and cluster operators are expected to be under our control and contain
	// no sensitive information
	return pod
}

func (i *Gatherer) setClusterVersion(version *configv1.ClusterVersion) {
	i.lock.Lock()
	defer i.lock.Unlock()
	if i.lastVersion != nil && i.lastVersion.ResourceVersion == version.ResourceVersion {
		return
	}
	i.lastVersion = version.DeepCopy()
}

func (i *Gatherer) setInsightsVersion(operator *configv1.ClusterOperator) {
	i.lock.Lock()
	defer i.lock.Unlock()
	if i.insightsVersion != "" {
		return
	}
	var version string
	for _, v := range operator.Status.Versions {
		if v.Name == "operator"{
			version = v.Version
			break
		}
	}
	i.insightsVersion = version
}

// ClusterVersion returns Version for this cluster, which is set by running version during Gathering
func (i *Gatherer) ClusterVersion() *configv1.ClusterVersion {
	i.lock.Lock()
	defer i.lock.Unlock()
	return i.lastVersion
}


// InsightsVersion returns the version string for the 'insights' clusteroperator, which is set by running clusteroperator during Gathering
func (i *Gatherer) InsightsVersion() string {
	i.lock.Lock()
	defer i.lock.Unlock()
	return i.insightsVersion
}

// NewLineLimitReader returns a Reader that reads from `r` but stops with EOF after `n` lines.
func NewLineLimitReader(r io.Reader, n int) *LineLimitedReader { return &LineLimitedReader{r, n, 0} }

// A LineLimitedReader reads from R but limits the amount of
// data returned to just N lines. Each call to Read
// updates N to reflect the new amount remaining.
// Read returns EOF when N <= 0 or when the underlying R returns EOF.
type LineLimitedReader struct {
	reader        io.Reader // underlying reader
	maxLinesLimit int       // max lines remaining
	// totalLinesRead is the total number of line separators already read by the underlying reader.
	totalLinesRead int
}

func (l *LineLimitedReader) Read(p []byte) (int, error) {
	if l.maxLinesLimit <= 0 {
		return 0, io.EOF
	}

	rc, err := l.reader.Read(p)
	l.totalLinesRead += bytes.Count(p[:rc], lineSep)

	lc := 0
	for {
		lineSepIdx := bytes.Index(p[lc:rc], lineSep)
		if lineSepIdx == -1 {
			return rc, err
		}
		if l.maxLinesLimit <= 0 {
			return lc, io.EOF
		}
		l.maxLinesLimit--
		lc += lineSepIdx + 1 // skip past the EOF
	}
}

// GetTotalLinesRead return the total number of line separators already read by the underlying reader.
// This includes lines that have been truncated by the `Read` calls after exceeding the line limit.
func (l *LineLimitedReader) GetTotalLinesRead() int { return l.totalLinesRead }

// countLines reads the remainder of the reader and counts the number of lines.
//
// Inspired by: https://stackoverflow.com/a/24563853/
func countLines(r io.Reader) (int, error) {
	buf := make([]byte, 0x8000)
	// Original implementation started from 0, but a file with no line separator
	// still contains a single line, so I would say that was an off-by-1 error.
	lineCount := 1
	for {
		c, err := r.Read(buf)
		lineCount += bytes.Count(buf[:c], lineSep)
		if err != nil {
			return lineCount, err
		}
	}
}
