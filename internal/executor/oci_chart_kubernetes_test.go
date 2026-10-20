package executor

import (
	"context"
	"strings"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

type chartLifecycleStub struct {
	called int
}

func (s *chartLifecycleStub) Ensure(context.Context, Target, string, string, string, string) (bool, error) {
	s.called++
	return true, nil
}

func (s *chartLifecycleStub) Uninstall(context.Context, Target, string, string) (bool, error) {
	s.called++
	return true, nil
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
