package executor

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	pb "github.com/verdantflarehub/verdantflare-station-runtime/api/runtimev1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"k8s.io/client-go/kubernetes/fake"
)

func runtimeDB(t *testing.T) (*pgxpool.Pool, *pb.AppRequest) {
	t.Helper()
	dsn := os.Getenv("STATION_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("STATION_TEST_DATABASE_URL is required")
	}
	migrationRoot := os.Getenv("STATION_TEST_MIGRATIONS")
	if migrationRoot == "" {
		migrationRoot = "../../../verdantflare-station-core/migrations"
	}
	files, e := filepath.Glob(filepath.Join(migrationRoot, "*.sql"))
	if e != nil || len(files) == 0 {
		t.Fatal("Set STATION_TEST_MIGRATIONS to the central Core migrations directory")
	}
	ctx := context.Background()
	admin, e := pgxpool.New(ctx, dsn)
	if e != nil {
		t.Fatal("invalid test database")
	}
	dbName := "runtime_test_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	if _, e = admin.Exec(ctx, "CREATE DATABASE "+pgx.Identifier{dbName}.Sanitize()); e != nil {
		admin.Close()
		t.Fatal(e)
	}
	cfg := admin.Config()
	cfg.ConnConfig.Database = dbName
	p, e := pgxpool.NewWithConfig(ctx, cfg)
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() {
		p.Close()
		_, e := admin.Exec(ctx, "DROP DATABASE "+pgx.Identifier{dbName}.Sanitize())
		admin.Close()
		if e != nil {
			t.Error(e)
		}
	})
	if _, e = p.Exec(ctx, "CREATE SCHEMA station"); e != nil {
		t.Fatal(e)
	}
	for _, f := range files {
		b, e := os.ReadFile(f)
		if e != nil {
			t.Fatal(e)
		}
		if _, e = p.Exec(ctx, string(b)); e != nil {
			t.Fatal(e)
		}
	}
	in := &pb.AppRequest{OperationId: uuid.NewString(), RequestId: "test-request", UserId: uuid.NewString(), OrganizationId: uuid.NewString(), StationId: uuid.NewString(), AppId: "test-app", AppVersion: "1.0.0", Action: "install"}
	if _, e = p.Exec(ctx, `INSERT INTO station.configuration(singleton,station_id,contracts_major) VALUES(true,$1,1)`, in.StationId); e != nil {
		t.Fatal(e)
	}
	if _, e = p.Exec(ctx, `INSERT INTO station.users(user_id,username,password_hash) VALUES($1,'test','unused')`, in.UserId); e != nil {
		t.Fatal(e)
	}
	if _, e = p.Exec(ctx, `INSERT INTO station.organizations(organization_id,name) VALUES($1,'Test')`, in.OrganizationId); e != nil {
		t.Fatal(e)
	}
	return p, in
}
func TestDurableRecovery(t *testing.T) {
	p, in := runtimeDB(t)
	client := fake.NewSimpleClientset()
	app := target()
	svc := &Service{Pool: p, StationID: in.StationId, Targets: map[string]Target{"test-app@1.0.0": app}, Driver: &Kubernetes{client}}
	ctx := context.Background()
	out, e := svc.Reconcile(ctx, in)
	if e != nil || out.Status != "running" {
		t.Fatal(out, e)
	}
	if len(client.Actions()) != 0 {
		t.Fatal("side effect before durable acceptance")
	}
	out, e = svc.Reconcile(ctx, in)
	if e != nil || out.Phase != "installing" {
		t.Fatal(out, e)
	}
	// Replace the entire service instance; the persisted operation remains resumable.
	svc = &Service{Pool: p, StationID: in.StationId, Targets: svc.Targets, Driver: &Kubernetes{client}}
	in.RequestId = "new-trace-id"
	out, e = svc.Reconcile(ctx, in)
	if e != nil || out.Status != "running" {
		t.Fatal(out, e)
	}
	in.Action = "restart"
	if _, e = svc.Reconcile(ctx, in); status.Code(e) != codes.AlreadyExists {
		t.Fatalf("operation payload conflict: %v", e)
	}
	in.Action = "install"
	if _, e = p.Exec(ctx, `UPDATE station.runtime_app_executions SET created_at=now()-interval '1 hour' WHERE operation_id=$1`, in.OperationId); e != nil {
		t.Fatal(e)
	}
	out, e = svc.Reconcile(ctx, in)
	if e != nil || out.Status != "failed" {
		t.Fatal(out, e)
	}
	n := len(client.Actions())
	out, e = svc.Reconcile(ctx, in)
	if e != nil || out.Status != "failed" || len(client.Actions()) != n {
		t.Fatal("terminal replay performed side effects", out, e)
	}
}
func TestTemplateChangeFailsRecovery(t *testing.T) {
	p, in := runtimeDB(t)
	app := target()
	svc := &Service{Pool: p, StationID: in.StationId, Targets: map[string]Target{"test-app@1.0.0": app}, Driver: &Kubernetes{fake.NewSimpleClientset()}, Timeout: time.Minute}
	if _, e := svc.Reconcile(context.Background(), in); e != nil {
		t.Fatal(e)
	}
	app.Hash[0] = 1
	svc.Targets["test-app@1.0.0"] = app
	out, e := svc.Reconcile(context.Background(), in)
	if e != nil || out.Status != "failed" || out.ErrorCode != "CONFLICT" {
		t.Fatal(out, e)
	}
}
