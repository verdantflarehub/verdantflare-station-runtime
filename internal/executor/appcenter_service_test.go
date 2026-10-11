package executor

import (
	"context"
	"testing"

	"github.com/google/uuid"
	pb "github.com/verdantflarehub/verdantflare-station-runtime/api/runtimev1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

func TestAppCenterRuntimeDatabaseDurableRecovery(t *testing.T) {
	pool, command := runtimeDB(t)
	r, app := protectedFixture()
	r.Command = command
	command.InstallationId = uuid.Must(uuid.NewV7()).String()
	client := fake.NewSimpleClientset()
	ctx := context.Background()
	newService := func() *Service {
		return &Service{Pool: pool, StationID: command.StationId, Targets: map[string]Target{app.AppID + "@" + app.Version: app}, Driver: &Kubernetes{client}}
	}
	svc := newService()
	out, err := svc.ReconcileBound(ctx, r)
	if err != nil || out.Status != "running" {
		t.Fatal(out, err)
	}
	assertAppCenterReadsOnly(t, client)
	// Reconstruct the Service after durable admission. The registry has no env
	// bindings, but installation identity is still in the saved protocol digest.
	svc = newService()
	changed := proto.Clone(r).(*pb.AppCenterRequest)
	changed.Command.InstallationId = uuid.Must(uuid.NewV7()).String()
	if _, err := svc.ReconcileBound(ctx, changed); status.Code(err) != codes.AlreadyExists {
		t.Fatal("installation omitted from replay", err)
	}
	if _, err := svc.Reconcile(ctx, command); status.Code(err) != codes.AlreadyExists {
		t.Fatal("legacy replay bypassed guard", err)
	}
	r.Command.RequestId = "retry"
	// Discard the first create result, then reconstruct again and retry the same
	// operation. This simulates response loss, not an actual process kill.
	if _, err := svc.ReconcileBound(ctx, r); err != nil {
		t.Fatal(err)
	}
	svc = newService()
	if _, err := svc.ReconcileBound(ctx, r); err != nil {
		t.Fatal(err)
	}
	creates := 0
	for _, a := range client.Actions() {
		if a.GetVerb() == "create" {
			creates++
		}
	}
	if creates != 1 {
		t.Fatal("duplicate workload", creates)
	}
	live, _ := client.AppsV1().Deployments(app.Deployment.Namespace).Get(ctx, app.Deployment.Name, metav1.GetOptions{})
	live.Status = appsv1.DeploymentStatus{ObservedGeneration: live.Generation, Replicas: 1, UpdatedReplicas: 1, ReadyReplicas: 1, AvailableReplicas: 1}
	client.AppsV1().Deployments(live.Namespace).UpdateStatus(ctx, live, metav1.UpdateOptions{})
	out, err = svc.ReconcileBound(ctx, r)
	if err != nil || out.Status != "succeeded" {
		t.Fatal(out, err)
	}
	svc.Targets = nil
	client.ClearActions()
	out, err = svc.ReconcileBound(ctx, r)
	if err != nil || out.Status != "succeeded" || len(client.Actions()) != 0 {
		t.Fatal("terminal replay depends on registry", out, err)
	}
	changed = proto.Clone(r).(*pb.AppCenterRequest)
	changed.Guard.WorkloadUid = uuid.NewString()
	changed.Command.Action = "start"
	if _, err := svc.ReconcileBound(ctx, changed); status.Code(err) != codes.AlreadyExists {
		t.Fatal("terminal payload conflict lost", err)
	}
}

func TestAppCenterRuntimeDatabaseConflictAndTemplate(t *testing.T) {
	for _, variant := range []string{"existing-before-accept", "existing-after-accept", "changed-template"} {
		t.Run(variant, func(t *testing.T) {
			pool, command := runtimeDB(t)
			r, app := protectedFixture()
			r.Command = command
			command.InstallationId = uuid.Must(uuid.NewV7()).String()
			client := fake.NewSimpleClientset()
			svc := &Service{Pool: pool, StationID: command.StationId, Targets: map[string]Target{app.AppID + "@" + app.Version: app}, Driver: &Kubernetes{client}}
			ctx := context.Background()
			if variant == "existing-before-accept" {
				client.Tracker().Add(app.Deployment.DeepCopy())
				if _, err := svc.ReconcileBound(ctx, r); status.Code(err) != codes.FailedPrecondition {
					t.Fatal(err)
				}
				var count int
				if err := pool.QueryRow(ctx, `SELECT count(*) FROM station.runtime_app_executions`).Scan(&count); err != nil || count != 0 {
					t.Fatal("conflict durably accepted", count, err)
				}
			} else {
				if _, err := svc.ReconcileBound(ctx, r); err != nil {
					t.Fatal(err)
				}
				if variant == "changed-template" {
					app.Hash[0]++
					svc.Targets[app.AppID+"@"+app.Version] = app
				} else {
					client.Tracker().Add(app.Deployment.DeepCopy())
				}
				out, err := svc.ReconcileBound(ctx, r)
				if err != nil || out.Status != "failed" || out.ErrorCode != "CONFLICT" {
					t.Fatal("unsafe recovery", out, err)
				}
				out, err = svc.ReconcileBound(ctx, r)
				if err != nil || out.Status != "failed" {
					t.Fatal("conflict did not persist terminal result", out, err)
				}
			}
			assertAppCenterReadsOnly(t, client)
		})
	}
}
