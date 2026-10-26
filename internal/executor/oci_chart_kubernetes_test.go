package executor

import (
	"context"
	"errors"
	"helm.sh/helm/v3/pkg/release"
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

type chartLifecycleStub struct {
	called  int
	failure error
}

func TestHelmRecoveryDoesNotTreatPendingOrFailedAsDeployed(t *testing.T) {
	for _, status := range []release.Status{release.StatusPendingInstall, release.StatusPendingUpgrade, release.StatusFailed} {
		rel := &release.Release{Name: "app-publishing", Namespace: "verdantflare-app", Info: &release.Info{Status: status}}
		if err := requireDeployedRelease(rel, "verdantflare-app"); err == nil {
			t.Fatalf("%s release was treated as deployed", status)
		}
	}
	if err := requireDeployedRelease(&release.Release{Name: "app-publishing", Namespace: "verdantflare-app", Info: &release.Info{Status: release.StatusDeployed}}, "verdantflare-app"); err != nil {
		t.Fatal(err)
	}
	if !freshHelmTransition(time.Now().Add(-time.Minute)) || freshHelmTransition(time.Now().Add(-7*time.Minute)) || freshHelmTransition(time.Time{}) {
		t.Fatal("pending Helm transition is not bounded by its start time")
	}
}

func (s *chartLifecycleStub) Ensure(context.Context, Target, string, string, string, string) (bool, error) {
	s.called++
	return s.failure == nil, s.failure
}

func (s *chartLifecycleStub) Uninstall(context.Context, Target, string, string) (bool, error) {
	s.called++
	return s.failure == nil, s.failure
}

func TestOCIInstallUsesHelmWithoutCreatingPartialWorkload(t *testing.T) {
	ctx := context.Background()
	client := fake.NewSimpleClientset()
	helm := new(chartLifecycleStub)
	k := &Kubernetes{Client: client, Charts: helm}
	target := Target{
		AppID:          "fal",
		Version:        "0.2.14",
		InstallationID: "01a14d95-9a6a-72e0-be03-07ee6d33c97c",
		AppCenter:      true,
		Deployment: &appsv1.Deployment{
			ObjectMeta: metav1.ObjectMeta{Name: "fal-channel", Namespace: "verdantflare-app"},
			Spec:       appsv1.DeploymentSpec{Template: corev1.PodTemplateSpec{Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "gateway", Image: "example/fal:v0.2.14"}}}}},
		},
		OCIChart: &OCIChartMeta{ChartName: "fal", PackageDigest: "sha256:" + strings.Repeat("a", 64)},
	}
	result, err := k.Step(ctx, target, "install", "operation-1", "station-1", "org-1")
	if err != nil || result.Status != "running" || helm.called != 1 {
		t.Fatalf("expected a Helm install attempt, got result=%+v err=%v calls=%d", result, err, helm.called)
	}
	if _, err := client.AppsV1().Deployments("verdantflare-app").Get(ctx, "fal-channel", metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Fatalf("legacy Deployment fallback created a partial workload: %v", err)
	}

	helm.failure = errors.New("Chart PVC has no Helm keep policy")
	result, err = k.Step(ctx, target, "install", "operation-1", "station-1", "org-1")
	if err != nil || result.Status != "failed" || result.Code != "SERVICE_UNAVAILABLE" || !strings.Contains(result.Message, "keep policy") {
		t.Fatalf("Helm failure must use a persistable lifecycle error: %+v %v", result, err)
	}
	result, err = k.stepOCIChartDelete(ctx, target, "station-1", "org-1")
	if err != nil || result.Status != "failed" || result.Code != "SERVICE_UNAVAILABLE" {
		t.Fatalf("Helm uninstall failure must use a persistable lifecycle error: %+v %v", result, err)
	}
	k.Charts = nil
	result, err = k.Step(ctx, target, "install", "operation-1", "station-1", "org-1")
	if err != nil || result.Status != "failed" || result.Code != "SERVICE_UNAVAILABLE" {
		t.Fatalf("expected closed failure without Helm, got %+v err=%v", result, err)
	}
}

func TestChartUninstallRequiresPVCKeepPolicy(t *testing.T) {
	kept := "kind: PersistentVolumeClaim\nmetadata:\n  name: data\n  annotations:\n    helm.sh/resource-policy: keep\n"
	if err := checkPVCRetention(kept); err != nil {
		t.Fatal(err)
	}
	unprotected := "kind: PersistentVolumeClaim\nmetadata:\n  name: data\n"
	if err := checkPVCRetention(unprotected); err == nil {
		t.Fatal("PVC without keep policy must block Chart uninstall")
	}
}
