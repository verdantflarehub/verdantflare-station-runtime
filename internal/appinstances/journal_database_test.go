package appinstances

import (
	"context"
	"crypto/sha256"
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	legacy "github.com/verdantflarehub/verdantflare-station-runtime/internal/instances"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"
)

func journalFixture(t *testing.T) (*Journal, Run, Profile, *fake.Clientset) {
	t.Helper()
	dsn := os.Getenv("STATION_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("STATION_TEST_DATABASE_URL required; run database integration on 5090 dev")
	}
	in, p, pvc, pv := fixture()
	ctx := context.Background()
	admin, e := pgxpool.New(ctx, dsn)
	if e != nil {
		t.Fatal("invalid isolated test database configuration")
	}
	name := "appinstance_test_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	if _, e = admin.Exec(ctx, "CREATE DATABASE "+pgx.Identifier{name}.Sanitize()); e != nil {
		admin.Close()
		t.Fatal(e)
	}
	cfg := admin.Config()
	cfg.ConnConfig.Database = name
	pool, e := pgxpool.NewWithConfig(ctx, cfg)
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() {
		pool.Close()
		_, e := admin.Exec(context.Background(), "DROP DATABASE "+pgx.Identifier{name}.Sanitize())
		admin.Close()
		if e != nil {
			t.Error(e)
		}
	})
	root := os.Getenv("STATION_TEST_MIGRATIONS")
	if root == "" {
		root = "../../../verdantflare-station-core/migrations"
	}
	files, e := filepath.Glob(filepath.Join(root, "*.sql"))
	if e != nil || len(files) < 13 {
		t.Fatal("central migrations through instance stops are required")
	}
	if _, e = pool.Exec(ctx, "CREATE SCHEMA station"); e != nil {
		t.Fatal(e)
	}
	for _, file := range files {
		raw, e := os.ReadFile(file)
		if e != nil {
			t.Fatal(e)
		}
		if _, e = pool.Exec(ctx, string(raw)); e != nil {
			t.Fatalf("migration %s: %v", filepath.Base(file), e)
		}
	}
	for _, seed := range []struct {
		sql  string
		args []any
	}{
		{`INSERT INTO station.configuration(singleton,station_id,contracts_major) VALUES(true,$1,1)`, []any{in.StationID}},
		{`INSERT INTO station.users(user_id,username,password_hash) VALUES($1,'test-owner','unused-test-hash')`, []any{in.UserID}},
		{`INSERT INTO station.organizations(organization_id,name) VALUES($1,'Test organization')`, []any{in.OrganizationID}},
		{`INSERT INTO station.app_installations(installation_id,created_by,organization_id,station_id,app_id,app_version,state) VALUES($1,$2,$3,$4,$5,$6,'installed')`, []any{in.InstallationID, in.UserID, in.OrganizationID, in.StationID, p.AppID, p.AppVersion}},
	} {
		if _, e = pool.Exec(ctx, seed.sql, seed.args...); e != nil {
			t.Fatal(e)
		}
	}
	c := client(pvc, pv)
	j := &Journal{Pool: pool, Client: c, StationID: in.StationID, Authorize: func(context.Context, Run, Profile) (int64, error) { return 1, nil }}
	return j, in, p, c
}

func TestJournalAcceptanceReplayAndRestart(t *testing.T) {
	j, in, p, c := journalFixture(t)
	ctx := context.Background()
	first, e := j.Accept(ctx, in, p)
	if e != nil || first.Phase != "accepted" || first.Observation != (Observation{}) {
		t.Fatal("accept failed", e)
	}
	assertNoEffects(t, c)
	restarted := &Journal{Pool: j.Pool, Client: j.Client, StationID: j.StationID, Authorize: j.Authorize}
	again, e := restarted.Accept(ctx, in, p)
	if e != nil || again != first {
		t.Fatal("replay changed accepted record", e)
	}
	changed := in
	changed.OperationID = id()
	if _, e = j.Accept(ctx, changed, p); !errors.Is(e, ErrConflict) {
		t.Fatal("second start accepted", e)
	}
	changed = in
	changed.InstanceID = id()
	if _, e = j.Accept(ctx, changed, p); !errors.Is(e, ErrConflict) {
		t.Fatal("operation rebound", e)
	}
	c.ClearActions()
	j.Authorize = func(context.Context, Run, Profile) (int64, error) { return 0, ErrDenied }
	if _, e = j.Accept(ctx, in, p); !errors.Is(e, ErrDenied) {
		t.Fatal("replay skipped current authorization", e)
	}
	if _, e = j.AdvanceResources(ctx, in, p); !errors.Is(e, ErrDenied) {
		t.Fatal("execution skipped current authorization", e)
	}
	assertNoEffects(t, c)
}

func TestJournalResourcesCommitBeforeLaterFailure(t *testing.T) {
	j, in, p, c := journalFixture(t)
	ctx := context.Background()
	if _, e := j.Accept(ctx, in, p); e != nil {
		t.Fatal(e)
	}
	broken := true
	c.PrependReactor("create", "services", func(ktesting.Action) (bool, runtime.Object, error) {
		if broken {
			return true, nil, errors.New("service API unavailable")
		}
		return false, nil, nil
	})
	partial, e := j.AdvanceResources(ctx, in, p)
	if e == nil || partial.Observation.SecretUID == "" || partial.Observation.PodUID != "" {
		t.Fatal("partial execution not captured", e)
	}
	j = &Journal{Pool: j.Pool, Client: c, StationID: in.StationID, Authorize: j.Authorize}
	reloaded, e := j.Accept(ctx, in, p)
	if e != nil || reloaded.Observation.SecretUID != partial.Observation.SecretUID {
		t.Fatal("partial execution rolled back", e)
	}
	broken = false
	ready, e := j.AdvanceResources(ctx, in, p)
	if e != nil || ready.Observation.SecretUID != partial.Observation.SecretUID || ready.Observation.PodUID == "" || ready.Phase != "launching" {
		t.Fatal("resume failed or reported unproven running", e)
	}
	// The lock has been released and every UID remains durable after reopening.
	reloaded, e = j.Accept(ctx, in, p)
	if e != nil || reloaded.Observation.PodUID != ready.Observation.PodUID {
		t.Fatal(e)
	}
	c.CoreV1().Pods(p.Namespace).Delete(ctx, runName(in), metav1.DeleteOptions{})
	c.ClearActions()
	if _, e = j.AdvanceResources(ctx, in, p); !errors.Is(e, ErrBinding) {
		t.Fatal("lost acknowledged worker recreated", e)
	}
	assertNoEffects(t, c)
}

func TestJournalFailedAcceptanceRollsBackReservation(t *testing.T) {
	j, in, p, c := journalFixture(t)
	ctx := context.Background()
	_, e := j.Pool.Exec(ctx, `CREATE FUNCTION station.reject_test_start() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'test rejection'; END $$; CREATE TRIGGER reject_test_start BEFORE INSERT ON station.app_instance_starts FOR EACH ROW EXECUTE FUNCTION station.reject_test_start()`)
	if e != nil {
		t.Fatal(e)
	}
	if _, e = j.Accept(ctx, in, p); e == nil {
		t.Fatal("failed insertion accepted")
	}
	var count int
	if e = j.Pool.QueryRow(ctx, `SELECT count(*) FROM station.app_instance_workspaces`).Scan(&count); e != nil || count != 0 {
		t.Fatal("reservation survived failed acceptance", e)
	}
	assertNoEffects(t, c)
}

func TestJournalConcurrentOriginalRequestAndCapacity(t *testing.T) {
	j, in, p, c := journalFixture(t)
	ctx := context.Background()
	var group sync.WaitGroup
	results := make(chan error, 8)
	for range 8 {
		group.Add(1)
		go func() {
			defer group.Done()
			_, e := j.Accept(ctx, in, p)
			results <- e
		}()
	}
	group.Wait()
	close(results)
	for e := range results {
		if e != nil && !errors.Is(e, ErrBusy) {
			t.Fatal(e)
		}
	}
	var count int
	if e := j.Pool.QueryRow(ctx, `SELECT count(*) FROM station.app_instance_starts`).Scan(&count); e != nil || count != 1 {
		t.Fatal("duplicate start", e)
	}
	other := in
	other.InstanceID, other.OperationID = id(), id()
	if _, e := j.Accept(ctx, other, p); !errors.Is(e, ErrCapacity) {
		t.Fatal("one slot allocated twice", e)
	}
	assertNoEffects(t, c)
}

func TestJournalInstallationDriftAndStaleCheckpoint(t *testing.T) {
	j, in, p, c := journalFixture(t)
	ctx := context.Background()
	r, e := j.Accept(ctx, in, p)
	if e != nil {
		t.Fatal(e)
	}
	conn, e := j.Pool.Acquire(ctx)
	if e != nil {
		t.Fatal(e)
	}
	stale := r
	if e = j.checkpoint(ctx, conn, &r, Observation{}); e != nil {
		t.Fatal(e)
	}
	if e = j.checkpoint(ctx, conn, &stale, Observation{}); !errors.Is(e, ErrConflict) {
		t.Fatal("stale checkpoint replaced newer revision", e)
	}
	conn.Release()
	if _, e = j.Pool.Exec(ctx, `UPDATE station.app_installations SET state='removed',revision=revision+1 WHERE installation_id=$1`, in.InstallationID); e != nil {
		t.Fatal(e)
	}
	c.ClearActions()
	if _, e = j.AdvanceResources(ctx, in, p); !errors.Is(e, ErrDenied) {
		t.Fatal("removed installation executed", e)
	}
	assertNoEffects(t, c)
}

func TestJournalAndLegacyAllocatorCannotReserveSameVolume(t *testing.T) {
	for _, first := range []string{"app", "legacy"} {
		t.Run(first, func(t *testing.T) {
			j, in, p, c := journalFixture(t)
			ctx := context.Background()
			old := legacy.WorkspaceStore{Pool: j.Pool, Client: c}
			oldIn := legacy.Identity{StationID: in.StationID, OrganizationID: in.OrganizationID, ProjectID: id(), InstanceID: id(), CreateOperationID: id()}
			oldProfile := legacy.StorageProfile{ID: p.ID, Namespace: p.Namespace, Pool: p.Pool, NodeName: p.NodeName, Claims: p.Claims, Bytes: p.StorageBytes, Hash: sha256.Sum256([]byte("test-legacy-profile"))}
			if first == "app" {
				if _, e := j.Accept(ctx, in, p); e != nil {
					t.Fatal(e)
				}
				if _, e := old.Reserve(ctx, oldIn, oldProfile); !errors.Is(e, legacy.ErrCapacity) {
					t.Fatal("legacy stole committed App reservation", e)
				}
			} else {
				if _, e := old.Reserve(ctx, oldIn, oldProfile); e != nil {
					t.Fatal(e)
				}
				if _, e := j.Accept(ctx, in, p); !errors.Is(e, ErrCapacity) {
					t.Fatal("App stole committed legacy reservation", e)
				}
			}
			assertNoEffects(t, c)
		})
	}
}

func TestJournalSingleConnectionProgressAndCanceledCallReleaseLock(t *testing.T) {
	j, in, p, _ := journalFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if _, e := j.Accept(ctx, in, p); e != nil {
		t.Fatal(e)
	}
	cfg := j.Pool.Config()
	cfg.MaxConns = 1
	one, e := pgxpool.NewWithConfig(ctx, cfg)
	if e != nil {
		t.Fatal(e)
	}
	defer one.Close()
	j.Pool = one
	if _, e = j.AdvanceResources(ctx, in, p); e != nil {
		t.Fatal("single connection deadlock or failure", e)
	}
	authorize := j.Authorize
	j.Authorize = func(context.Context, Run, Profile) (int64, error) { return 0, context.Canceled }
	if _, e = j.AdvanceResources(ctx, in, p); !errors.Is(e, context.Canceled) {
		t.Fatal(e)
	}
	j.Authorize = authorize
	if _, e = j.Accept(ctx, in, p); e != nil {
		t.Fatal("canceled call retained instance lock", e)
	}
}

func TestJournalRunningRequiresBoundProbeAcrossRestart(t *testing.T) {
	j, in, p, c := journalFixture(t)
	ctx := context.Background()
	boot, wrong := uuid.NewString(), true
	probes := 0
	observer := HTTPWorkerObserver{Client: &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		probes++
		pod, e := c.CoreV1().Pods(p.Namespace).Get(ctx, runName(in), metav1.GetOptions{})
		if e != nil {
			t.Fatal(e)
		}
		value := proofFor(in, p, Observation{PodUID: string(pod.UID)}, boot)
		if wrong {
			value["start_operation_id"] = id()
		}
		return response(value), nil
	})}}
	j.Observer = observer
	first, e := j.Start(ctx, in, p)
	if e != nil || first.Phase != "launching" || probes != 0 {
		t.Fatal("unready Pod declared running", e)
	}
	pod, _ := c.CoreV1().Pods(p.Namespace).Get(ctx, runName(in), metav1.GetOptions{})
	pod.Spec.NodeName = p.NodeName
	pod.Status.Phase = corev1.PodRunning
	pod.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}
	if _, e = c.CoreV1().Pods(p.Namespace).Update(ctx, pod, metav1.UpdateOptions{}); e != nil {
		t.Fatal(e)
	}
	if _, e = j.Start(ctx, in, p); !errors.Is(e, ErrBinding) {
		t.Fatal("wrong start proof accepted", e)
	}
	stored, e := j.Accept(ctx, in, p)
	if e != nil || stored.Phase != "launching" || stored.Observation.BootID != "" {
		t.Fatal("failed proof committed as success", e)
	}
	wrong = false
	running, e := j.Start(ctx, in, p)
	if e != nil || running.Phase != "running" || running.Observation.BootID != boot {
		t.Fatal("valid proof did not commit", e)
	}
	j = &Journal{Pool: j.Pool, Client: c, StationID: in.StationID, Authorize: j.Authorize, Observer: observer}
	stored, e = j.Accept(ctx, in, p)
	if e != nil || stored.Observation.BootID != boot || stored.Phase != "running" {
		t.Fatal("boot identity lost after coordinator restart", e)
	}
	c.ClearActions()
	if _, e = j.Start(ctx, in, p); e != nil {
		t.Fatal("running replay failed", e)
	}
	assertNoEffects(t, c)
	boot = uuid.NewString()
	if _, e = j.Start(ctx, in, p); !errors.Is(e, ErrBinding) {
		t.Fatal("changed process rebound on replay", e)
	}
	latest, e := j.Accept(ctx, in, p)
	if e != nil || latest.Observation.BootID != stored.Observation.BootID {
		t.Fatal("saved process identity overwritten", e)
	}
}

func TestJournalProbeCannotCertifyReplacedPod(t *testing.T) {
	j, in, p, c := journalFixture(t)
	ctx := context.Background()
	if _, e := j.Accept(ctx, in, p); e != nil {
		t.Fatal(e)
	}
	if _, e := j.AdvanceResources(ctx, in, p); e != nil {
		t.Fatal(e)
	}
	pod, _ := c.CoreV1().Pods(p.Namespace).Get(ctx, runName(in), metav1.GetOptions{})
	pod.Spec.NodeName = p.NodeName
	pod.Status.Phase = corev1.PodRunning
	pod.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}
	c.CoreV1().Pods(p.Namespace).Update(ctx, pod, metav1.UpdateOptions{})
	j.Observer = HTTPWorkerObserver{Client: &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		value := proofFor(in, p, Observation{PodUID: string(pod.UID)}, uuid.NewString())
		changed := pod.DeepCopy()
		changed.UID = types.UID(uuid.NewString())
		if _, e := c.CoreV1().Pods(p.Namespace).Update(ctx, changed, metav1.UpdateOptions{}); e != nil {
			t.Fatal(e)
		}
		return response(value), nil
	})}}
	if _, e := j.Start(ctx, in, p); !errors.Is(e, ErrBinding) {
		t.Fatal("probe certified replaced pod", e)
	}
	stored, e := j.Accept(ctx, in, p)
	if e != nil || stored.Phase == "running" || stored.Observation.BootID != "" {
		t.Fatal("stale probe persisted success", e)
	}
}

func TestJournalAccessRequiresRunningOriginalProcessAndNeverRepairsResources(t *testing.T) {
	j, in, p, c := journalFixture(t)
	ctx := context.Background()
	j.Observer = HTTPWorkerObserver{Client: &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		t.Fatal("unready worker probed")
		return nil, ErrNotReady
	})}}
	if _, e := j.Access(ctx, in, p); !errors.Is(e, pgx.ErrNoRows) {
		t.Fatal("access created a missing operation", e)
	}
	assertNoEffects(t, c)
	if _, e := j.Start(ctx, in, p); e != nil {
		t.Fatal(e)
	}
	c.ClearActions()
	if _, e := j.Access(ctx, in, p); !errors.Is(e, ErrNotReady) {
		t.Fatal("access promoted pending operation", e)
	}
	assertNoEffects(t, c)
	pod, _ := c.CoreV1().Pods(p.Namespace).Get(ctx, runName(in), metav1.GetOptions{})
	pod.Spec.NodeName = p.NodeName
	pod.Status.Phase = corev1.PodRunning
	pod.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}
	if _, e := c.CoreV1().Pods(p.Namespace).Update(ctx, pod, metav1.UpdateOptions{}); e != nil {
		t.Fatal(e)
	}
	boot := uuid.NewString()
	j.Observer = HTTPWorkerObserver{Client: &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		return response(proofFor(in, p, Observation{PodUID: string(pod.UID)}, boot)), nil
	})}}
	if _, e := j.Start(ctx, in, p); e != nil {
		t.Fatal(e)
	}
	c.ClearActions()
	access, e := j.Access(ctx, in, p)
	if e != nil || !access.valid(in, p) || access.BootID != boot {
		t.Fatal("bound private access failed", e)
	}
	assertNoEffects(t, c)
	authorize := j.Authorize
	j.Authorize = func(context.Context, Run, Profile) (int64, error) { return 0, ErrDenied }
	if got, e := j.Access(ctx, in, p); !errors.Is(e, ErrDenied) || got != (WorkerAccess{}) {
		t.Fatal("revoked caller retained private access", e)
	}
	j.Authorize = authorize
	boot = uuid.NewString()
	if got, e := j.Access(ctx, in, p); !errors.Is(e, ErrBinding) || got != (WorkerAccess{}) {
		t.Fatal("new process inherited old access", e)
	}
	boot = access.BootID
	if e := c.CoreV1().Services(p.Namespace).Delete(ctx, runName(in), metav1.DeleteOptions{}); e != nil {
		t.Fatal(e)
	}
	c.ClearActions()
	if got, e := j.Access(ctx, in, p); e == nil || got != (WorkerAccess{}) {
		t.Fatal("missing resource produced private access")
	}
	assertNoEffects(t, c)
}
