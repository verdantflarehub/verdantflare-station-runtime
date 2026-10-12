package executor

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

// Uses only the disposable 5090 PostgreSQL fixture and an in-memory API client.
func TestInstallationDatabaseBindingRecovery(t *testing.T) {
	pool, in := runtimeDB(t)
	target, _ := installationFixture()
	in.InstallationId = uuid.Must(uuid.NewV7()).String()
	client := fake.NewSimpleClientset()
	svc := &Service{Pool: pool, StationID: in.StationId, Targets: map[string]Target{"test-app@1.0.0": target}, Driver: &Kubernetes{Client: client}}
	ctx := context.Background()
	if out, err := svc.Reconcile(ctx, in); err != nil || out.Status != "running" {
		t.Fatal(out, err)
	}
	if len(client.Actions()) != 0 {
		t.Fatal("side effects before durable acceptance")
	}
	original := in.InstallationId
	in.InstallationId = uuid.Must(uuid.NewV7()).String()
	if _, err := svc.Reconcile(ctx, in); status.Code(err) != codes.AlreadyExists {
		t.Fatal("installation change accepted", err)
	}
	if len(client.Actions()) != 0 {
		t.Fatal("conflicting identity reached Kubernetes")
	}
	in.InstallationId = original
	svc = &Service{Pool: pool, StationID: in.StationId, Targets: svc.Targets, Driver: svc.Driver}
	if out, err := svc.Reconcile(ctx, in); err != nil || out.Phase != "installing" {
		t.Fatal(out, err)
	}
	d, err := client.AppsV1().Deployments("test-runtime").Get(ctx, "test-app", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if d.Annotations[installationKey] != original || d.Spec.Template.Spec.Containers[0].Env[0].Value != original {
		t.Fatal("wrong persisted identity")
	}
	d.Status = appsv1.DeploymentStatus{ObservedGeneration: d.Generation, Replicas: 1, UpdatedReplicas: 1, ReadyReplicas: 1, AvailableReplicas: 1}
	if _, err = client.AppsV1().Deployments(d.Namespace).UpdateStatus(ctx, d, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	if out, err := svc.Reconcile(ctx, in); err != nil || out.Status != "succeeded" {
		t.Fatal(out, err)
	}
	before := len(client.Actions())
	delete(svc.Targets, "test-app@1.0.0")
	if out, err := svc.Reconcile(ctx, in); err != nil || out.Status != "succeeded" {
		t.Fatal("original receipt not replayed", out, err)
	}
	if len(client.Actions()) != before {
		t.Fatal("terminal replay changed resources")
	}
}

func TestInstallationDatabaseLegacyReplayIgnoresNewField(t *testing.T) {
	pool, in := runtimeDB(t)
	app := target()
	client := fake.NewSimpleClientset()
	svc := &Service{Pool: pool, StationID: in.StationId, Targets: map[string]Target{"test-app@1.0.0": app}, Driver: &Kubernetes{Client: client}}
	ctx := context.Background()
	if _, err := svc.Reconcile(ctx, in); err != nil {
		t.Fatal(err)
	}
	in.InstallationId = uuid.Must(uuid.NewV7()).String()
	if out, err := svc.Reconcile(ctx, in); err != nil || out.Phase != "installing" {
		t.Fatal("new Core field broke legacy operation", out, err)
	}
	d, err := client.AppsV1().Deployments("test-runtime").Get(ctx, "test-app", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if d.Annotations[installationKey] != "" || len(d.Spec.Template.Spec.Containers[0].Env) != 0 {
		t.Fatal("legacy target changed")
	}
}

func TestInstallationDatabaseMissingIdentityCannotBeAccepted(t *testing.T) {
	pool, in := runtimeDB(t)
	app, _ := installationFixture()
	client := fake.NewSimpleClientset()
	svc := &Service{Pool: pool, StationID: in.StationId, Targets: map[string]Target{"test-app@1.0.0": app}, Driver: &Kubernetes{Client: client}}
	if _, err := svc.Reconcile(context.Background(), in); status.Code(err) != codes.FailedPrecondition {
		t.Fatal(err)
	}
	var count int
	if err := pool.QueryRow(context.Background(), `SELECT count(*) FROM station.runtime_app_executions`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 0 || len(client.Actions()) != 0 {
		t.Fatal("missing identity caused durable or external effects")
	}
}
