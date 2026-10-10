package instances

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

func workspaceDB(t *testing.T, in Identity) *pgxpool.Pool {
	t.Helper()
	dsn := os.Getenv("STATION_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("STATION_TEST_DATABASE_URL required for PostgreSQL integration")
	}
	ctx := context.Background()
	admin, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal("invalid test database config")
	}
	name := "runtime_test_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	if _, err = admin.Exec(ctx, "CREATE DATABASE "+pgx.Identifier{name}.Sanitize()); err != nil {
		admin.Close()
		t.Fatal(err)
	}
	cfg := admin.Config()
	cfg.ConnConfig.Database = name
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		pool.Close()
		_, err := admin.Exec(ctx, "DROP DATABASE "+pgx.Identifier{name}.Sanitize())
		admin.Close()
		if err != nil {
			t.Error(err)
		}
	})
	root := os.Getenv("STATION_TEST_MIGRATIONS")
	if root == "" {
		root = "../../../verdantflare-station-core/migrations"
	}
	files, err := filepath.Glob(filepath.Join(root, "*.sql"))
	if err != nil || len(files) == 0 {
		t.Fatal("central Core migrations missing")
	}
	if _, err = pool.Exec(ctx, "CREATE SCHEMA station"); err != nil {
		t.Fatal(err)
	}
	for _, file := range files {
		raw, err := os.ReadFile(file)
		if err != nil {
			t.Fatal(err)
		}
		if _, err = pool.Exec(ctx, string(raw)); err != nil {
			t.Fatal(err)
		}
	}
	if _, err = pool.Exec(ctx, `INSERT INTO station.configuration(singleton,station_id,contracts_major) VALUES(true,$1,1)`, in.StationID); err != nil {
		t.Fatal(err)
	}
	if _, err = pool.Exec(ctx, `INSERT INTO station.organizations(organization_id,name) VALUES($1,'Test')`, in.OrganizationID); err != nil {
		t.Fatal(err)
	}
	return pool
}

func TestDurableReservationConcurrencyAndRetention(t *testing.T) {
	in, p, pvc, pv := fixture()
	pool := workspaceDB(t, in)
	client := fake.NewClientset(pvc, pv)
	store := WorkspaceStore{pool, client}
	ctx := context.Background()
	var group sync.WaitGroup
	results := make(chan error, 8)
	for i := 0; i < 8; i++ {
		group.Add(1)
		go func() { defer group.Done(); _, err := store.Reserve(ctx, in, p); results <- err }()
	}
	group.Wait()
	close(results)
	for err := range results {
		if err != nil {
			t.Fatal(err)
		}
	}
	for _, action := range client.Actions() {
		if action.GetVerb() != "get" && action.GetVerb() != "list" {
			t.Fatal("Kubernetes effect before reservation", action)
		}
	}
	recovered := WorkspaceStore{pool, client}
	b, err := recovered.Lookup(ctx, in, p)
	if err != nil || b.PVCUID != string(pvc.UID) {
		t.Fatal(b, err)
	}
	if err = Claim(ctx, client, in, p, b); err != nil {
		t.Fatal(err)
	}
	other := in
	other.InstanceID = uuid.NewString()
	other.CreateOperationID = uuid.NewString()
	if _, err = recovered.Reserve(ctx, other, p); !errors.Is(err, ErrCapacity) {
		t.Fatal("pool overallocated", err)
	}
	wrong := in
	wrong.ProjectID = uuid.NewString()
	if _, err = recovered.Reserve(ctx, wrong, p); !errors.Is(err, ErrBinding) {
		t.Fatal("project rebound", err)
	}
	changed := p
	changed.Hash[0] = 2
	if _, err = recovered.Reserve(ctx, in, changed); !errors.Is(err, ErrBinding) {
		t.Fatal("profile changed on replay", err)
	}
	pod := volumePod(in, p, pvc.Name)
	if _, err = client.CoreV1().Pods(p.Namespace).Create(ctx, pod, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err = recovered.Retain(ctx, in, p); !errors.Is(err, ErrBusy) {
		t.Fatal("retained live workspace", err)
	}
	if err = client.CoreV1().Pods(p.Namespace).Delete(ctx, pod.Name, metav1.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		retained, err := recovered.Retain(ctx, in, p)
		if err != nil || !retained.Retained {
			t.Fatal(retained, err)
		}
	}
	if _, err = recovered.Reserve(ctx, in, p); !errors.Is(err, ErrRetained) {
		t.Fatal("retained instance resurrected", err)
	}
	if _, err = recovered.Reserve(ctx, other, p); !errors.Is(err, ErrCapacity) {
		t.Fatal("retained slot recycled", err)
	}
	if _, err = client.CoreV1().PersistentVolumeClaims(p.Namespace).Get(ctx, pvc.Name, metav1.GetOptions{}); err != nil {
		t.Fatal("PVC deleted", err)
	}
	if _, err = client.CoreV1().PersistentVolumes().Get(ctx, pv.Name, metav1.GetOptions{}); err != nil {
		t.Fatal("PV deleted", err)
	}
}

func TestDifferentInstancesCompeteForOneDurableSlot(t *testing.T) {
	in, p, pvc, pv := fixture()
	pool := workspaceDB(t, in)
	store := WorkspaceStore{pool, fake.NewClientset(pvc, pv)}
	ctx := context.Background()
	var group sync.WaitGroup
	results := make(chan error, 8)
	for i := 0; i < 8; i++ {
		group.Add(1)
		go func() {
			defer group.Done()
			request := in
			request.InstanceID = uuid.NewString()
			request.CreateOperationID = uuid.NewString()
			_, err := store.Reserve(ctx, request, p)
			results <- err
		}()
	}
	group.Wait()
	close(results)
	success := 0
	for err := range results {
		if err == nil {
			success++
		} else if !errors.Is(err, ErrCapacity) {
			t.Fatal(err)
		}
	}
	if success != 1 {
		t.Fatalf("one slot admitted %d instances", success)
	}
}
