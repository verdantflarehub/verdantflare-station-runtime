package executor

import (
	"io"
	"net/http"
	"strings"
	"testing"

	"helm.sh/helm/v3/pkg/action"
	"helm.sh/helm/v3/pkg/kube"
	"helm.sh/helm/v3/pkg/release"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/cli-runtime/pkg/resource"
	"k8s.io/client-go/kubernetes/scheme"
	restfake "k8s.io/client-go/rest/fake"
)

type driftCheckKube struct {
	kube.Interface
	resources kube.ResourceList
}

func (k driftCheckKube) Build(io.Reader, bool) (kube.ResourceList, error) { return k.resources, nil }

func driftCheckInfo(expectedImage, liveImage string, externalAnnotation bool) (*action.Configuration, *release.Release) {
	annotations := `"meta.helm.sh/release-name":"app","meta.helm.sh/release-namespace":"vf"`
	if externalAnnotation {
		annotations += `,"external.example/owner":"other"`
	}
	live := `{"apiVersion":"apps/v1","kind":"Deployment","metadata":{"name":"app","namespace":"vf","annotations":{` + annotations + `},"labels":{"app.kubernetes.io/managed-by":"Helm"}},"spec":{"template":{"spec":{"containers":[{"name":"app","image":"` + liveImage + `"}]}}}}`
	client := &restfake.RESTClient{GroupVersion: schema.GroupVersion{Group: "apps", Version: "v1"}, VersionedAPIPath: "/apis", NegotiatedSerializer: scheme.Codecs.WithoutConversion(), Resp: &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": []string{"application/json"}}, Body: io.NopCloser(strings.NewReader(live))}}
	info := &resource.Info{Client: client, Mapping: &meta.RESTMapping{Resource: schema.GroupVersionResource{Group: "apps", Version: "v1", Resource: "deployments"}, GroupVersionKind: schema.GroupVersionKind{Group: "apps", Version: "v1", Kind: "Deployment"}, Scope: meta.RESTScopeNamespace}, Namespace: "vf", Name: "app", Object: &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: "app", Namespace: "vf"}, Spec: appsv1.DeploymentSpec{Template: corev1.PodTemplateSpec{Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "app", Image: expectedImage}}}}}}}
	return &action.Configuration{KubeClient: driftCheckKube{resources: kube.ResourceList{info}}}, &release.Release{Name: "app", Namespace: "vf", Manifest: "apiVersion: apps/v1\nkind: Deployment\nmetadata:\n  name: app\n  namespace: vf\n"}
}

func TestSameChartResourceDriftIsNotReportedAsNoop(t *testing.T) {
	for _, test := range []struct {
		name, expected, live string
		extra                bool
		drift                bool
	}{
		{"unchanged", "example/app:v1", "example/app:v1", false, false},
		{"image drift", "example/app:v1", "example/app:v2", false, true},
		{"external annotation", "example/app:v1", "example/app:v1", true, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			cfg, rel := driftCheckInfo(test.expected, test.live, test.extra)
			present, err := releaseResourcesPresent(cfg, rel)
			if test.drift {
				if err == nil || !strings.Contains(err.Error(), "controlled field drift") || present {
					t.Fatalf("drift should fail closed: present=%v err=%v", present, err)
				}
			} else if err != nil || !present {
				t.Fatalf("unchanged resource should be accepted: present=%v err=%v", present, err)
			}
		})
	}
}

func TestSameChartMissingResourceNeedsRecovery(t *testing.T) {
	cfg, rel := driftCheckInfo("example/app:v1", "example/app:v1", false)
	resources := cfg.KubeClient.(driftCheckKube).resources
	resources[0].Client = &restfake.RESTClient{Err: apierrors.NewNotFound(schema.GroupResource{Group: "apps", Resource: "deployments"}, "app")}
	present, err := releaseResourcesPresent(cfg, rel)
	if err != nil || present {
		t.Fatalf("missing resource must enter recovery: present=%v err=%v", present, err)
	}
}

func TestConfigMapAndPVCDriftBoundaries(t *testing.T) {
	rel := &release.Release{Name: "app", Namespace: "vf"}
	owned := metav1.ObjectMeta{Name: "settings", Namespace: "vf", Annotations: map[string]string{"meta.helm.sh/release-name": "app", "meta.helm.sh/release-namespace": "vf"}, Labels: map[string]string{"app.kubernetes.io/managed-by": "Helm"}}
	want := &corev1.ConfigMap{ObjectMeta: owned, Data: map[string]string{"mode": "original"}}
	live := want.DeepCopy()
	live.Data["mode"] = "changed"
	if same, err := releaseResourceMatches(want, live, rel, "ConfigMap"); err != nil || same {
		t.Fatalf("ConfigMap drift accepted: same=%v err=%v", same, err)
	}
	live.Data["mode"] = "original"
	live.Annotations["external.example/owner"] = "other"
	if same, err := releaseResourceMatches(want, live, rel, "ConfigMap"); err != nil || !same {
		t.Fatalf("external annotation rejected: same=%v err=%v", same, err)
	}
	live.Annotations["meta.helm.sh/release-name"] = "foreign"
	if same, err := releaseResourceMatches(want, live, rel, "ConfigMap"); err == nil || same {
		t.Fatalf("foreign ownership accepted: same=%v err=%v", same, err)
	}
	pvcMeta := owned.DeepCopy()
	pvcMeta.Annotations["helm.sh/resource-policy"] = "keep"
	pvcWant := &corev1.PersistentVolumeClaim{ObjectMeta: *pvcMeta}
	pvcLive := pvcWant.DeepCopy()
	pvcLive.Spec.Resources.Requests = corev1.ResourceList{}
	if same, err := releaseResourceMatches(pvcWant, pvcLive, rel, "PersistentVolumeClaim"); err != nil || !same {
		t.Fatalf("PVC data must be preserved: same=%v err=%v", same, err)
	}
	delete(pvcLive.Annotations, "helm.sh/resource-policy")
	if same, err := releaseResourceMatches(pvcWant, pvcLive, rel, "PersistentVolumeClaim"); err != nil || same {
		t.Fatalf("PVC without keep accepted: same=%v err=%v", same, err)
	}
}

func TestSamePackageDifferentValuesAreNotNoop(t *testing.T) {
	rel := &release.Release{Config: map[string]interface{}{
		"appVersion": "1.0.0",
		"station":    map[string]interface{}{"packageDigest": "sha256:abc", "templateHash": "old"},
	}}
	if !sameReleaseTarget(rel, "sha256:abc", rel.Config) {
		t.Fatal("identical target should be a no-op candidate")
	}
	newValues := map[string]interface{}{
		"appVersion": "1.0.1",
		"station":    map[string]interface{}{"packageDigest": "sha256:abc", "templateHash": "new"},
	}
	if sameReleaseTarget(rel, "sha256:abc", newValues) {
		t.Fatal("same package with different target values was accepted as no-op")
	}
}
