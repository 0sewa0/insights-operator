package clusterconfig

import (
	"context"
	"testing"

	authv1 "github.com/openshift/api/authorization/v1"
	securityv1 "github.com/openshift/api/security/v1"
	authfake "github.com/openshift/client-go/authorization/clientset/versioned/fake"
	securityfake "github.com/openshift/client-go/security/clientset/versioned/fake"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/runtime/serializer/yaml"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	kubefake "k8s.io/client-go/kubernetes/fake"
)

func TestSAPConfig(t *testing.T) {
	// Initialize the fake dynamic client.
	var datahubYAML = `apiVersion: installers.datahub.sap.com/v1alpha1
kind: DataHub
metadata:
    name: example-datahub
    namespace: example-namespace
`

	datahubsClient := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), map[schema.GroupVersionResource]string{
		datahubGroupVersionResource: "DataHubsList",
	})

	decUnstructured := yaml.NewDecodingSerializer(unstructured.UnstructuredJSONScheme)
	testDatahub := &unstructured.Unstructured{}

	_, _, err := decUnstructured.Decode([]byte(datahubYAML), nil, testDatahub)
	if err != nil {
		t.Fatal("unable to decode datahub YAML", err)
	}

	// Initialize the remaining K8s/OS fake clients.
	coreClient := kubefake.NewSimpleClientset()
	authClient := authfake.NewSimpleClientset()
	securityClient := securityfake.NewSimpleClientset()

	// Security Context Constraints.
	securityClient.SecurityV1().SecurityContextConstraints().Create(
		context.Background(),
		&securityv1.SecurityContextConstraints{ObjectMeta: metav1.ObjectMeta{Name: "anyuid"}},
		metav1.CreateOptions{},
	)
	securityClient.SecurityV1().SecurityContextConstraints().Create(
		context.Background(),
		&securityv1.SecurityContextConstraints{ObjectMeta: metav1.ObjectMeta{Name: "privileged"}},
		metav1.CreateOptions{},
	)
	// This SCC should not be collected.
	securityClient.SecurityV1().SecurityContextConstraints().Create(
		context.Background(),
		&securityv1.SecurityContextConstraints{ObjectMeta: metav1.ObjectMeta{Name: "ignored"}},
		metav1.CreateOptions{},
	)

	// Cluster Role Bindings.
	authClient.AuthorizationV1().ClusterRoleBindings().Create(
		context.Background(),
		&authv1.ClusterRoleBinding{ObjectMeta: metav1.ObjectMeta{Name: "system:openshift:scc:anyuid"}},
		metav1.CreateOptions{},
	)
	authClient.AuthorizationV1().ClusterRoleBindings().Create(
		context.Background(),
		&authv1.ClusterRoleBinding{ObjectMeta: metav1.ObjectMeta{Name: "system:openshift:scc:privileged"}},
		metav1.CreateOptions{},
	)
	// This CRB should not be collected.
	authClient.AuthorizationV1().ClusterRoleBindings().Create(
		context.Background(),
		&authv1.ClusterRoleBinding{ObjectMeta: metav1.ObjectMeta{Name: "system:openshift:scc:ignored"}},
		metav1.CreateOptions{},
	)

	records, errs := gatherSAPConfig(context.Background(), datahubsClient, coreClient.CoreV1(), securityClient.SecurityV1(), authClient.AuthorizationV1())
	if len(errs) > 0 {
		t.Fatalf("unexpected errors: %#v", errs)
	}
	if len(records) != 0 {
		t.Fatalf("unexpected number or records in the first run: %d", len(records))
	}

	// Create the DataHubs resource and now the SCCs and CRBs should be gathered.
	datahubsClient.Resource(datahubGroupVersionResource).Namespace("example-namespace").Create(context.Background(), testDatahub, metav1.CreateOptions{})

	records, errs = gatherSAPConfig(context.Background(), datahubsClient, coreClient.CoreV1(), securityClient.SecurityV1(), authClient.AuthorizationV1())
	if len(errs) > 0 {
		t.Fatalf("unexpected errors: %#v", errs)
	}
	if len(records) != 4 {
		t.Fatalf("unexpected number or records in the second run: %d", len(records))
	}
}
