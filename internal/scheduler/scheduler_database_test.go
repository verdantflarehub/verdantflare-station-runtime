package scheduler

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
)

type rendezvousBarrier struct {
	point    string
	parties  int
	mu       sync.Mutex
	arrived  int
	released chan struct{}
}

func newRendezvousBarrier(point string, parties int) *rendezvousBarrier {
	return &rendezvousBarrier{
		point:    point,
		parties:  parties,
		released: make(chan struct{}),
	}
}

func (b *rendezvousBarrier) OnStage(ctx context.Context, point string, _ map[string]string) {
	if point != b.point {
		return
	}
	b.mu.Lock()
	b.arrived++
	if b.arrived == b.parties {
		close(b.released)
	}
	ch := b.released
	b.mu.Unlock()
	select {
	case <-ch:
	case <-ctx.Done():
	}
}

type staticTestInstanceDirectory struct {
	byID map[string]RegisteredInstance
}

func (s staticTestInstanceDirectory) LookupInstance(_ context.Context, _, instanceID string) (RegisteredInstance, bool, error) {
	inst, ok := s.byID[instanceID]
	return inst, ok, nil
}

type fakeVerifier struct {
	mu     sync.Mutex
	byKey  map[string]VerifyTerminatedResult
	defRes VerifyTerminatedResult
	defErr error
	calls  []VerifyTerminatedRequest
}

func (f *fakeVerifier) VerifyTerminated(_ context.Context, req VerifyTerminatedRequest) (VerifyTerminatedResult, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, req)
	if f.byKey != nil {
		if r, ok := f.byKey[req.PodUID]; ok {
			if r.ErrorCode == "UNKNOWN_NODE_STATE" {
				return r, ErrNodeStateUnknown
			}
			return r, nil
		}
	}
	return f.defRes, f.defErr
}

func makeMockEpochSealProof(ctx context.Context, pool *pgxpool.Pool, stationID string, cmd EvictVRAMCommand, supPID int, supTicks uint64) map[string]any {
	podUID := "pod-mock"
	cid := "cid-mock"
	bootID := cmd.ExpectedBootID
	if pool != nil && stationID != "" && cmd.InstanceID != "" {
		_ = pool.QueryRow(ctx, `
			SELECT pod_uid, container_id, boot_id
			  FROM station.gpu_instance_residency
			 WHERE station_id = $1 AND instance_id = $2
		`, stationID, cmd.InstanceID).Scan(&podUID, &cid, &bootID)
	}
	if bootID == "" {
		bootID = "boot-mock"
	}
	sealedEpoch := cmd.ExpectedGPUEpoch
	if sealedEpoch <= 0 {
		sealedEpoch = 1
	}
	return map[string]any{
		"reclaimed":                  true,
		"sealed_gpu_epoch":           sealedEpoch,
		"sealed_pod_uid":             podUID,
		"sealed_container_id":        cid,
		"sealed_boot_id":             bootID,
		"supervisor_pid":             supPID,
		"supervisor_starttime_ticks": supTicks,
	}
}

type fakeEvictor struct {
	mu          sync.Mutex
	pool        *pgxpool.Pool
	stationID   string
	evictFn     func(EvictVRAMCommand) (EvictVRAMResult, error)
	revokeCalls []RevokeLeaseCommand
}

func (f *fakeEvictor) EvictVRAM(ctx context.Context, cmd EvictVRAMCommand) (EvictVRAMResult, error) {
	f.mu.Lock()
	fn := f.evictFn
	pool := f.pool
	stationID := f.stationID
	f.mu.Unlock()
	if fn != nil {
		return fn(cmd)
	}
	sealedEpoch := cmd.ExpectedGPUEpoch
	if sealedEpoch <= 0 {
		sealedEpoch = 1
	}
	return EvictVRAMResult{
		Settled:                  true,
		ResidencyState:           "zero_vram_standby",
		ResidencySeq:             cmd.ExpectedResidencySeq + 1,
		GPUEpoch:                 cmd.ExpectedGPUEpoch,
		SealedGPUEpoch:           sealedEpoch,
		BootID:                   cmd.ExpectedBootID,
		SupervisorPID:            9001,
		SupervisorStarttimeTicks: 88001,
		ReclaimProof:             makeMockEpochSealProof(ctx, pool, stationID, cmd, 9001, 88001),
	}, nil
}

func (f *fakeEvictor) RevokeLease(_ context.Context, cmd RevokeLeaseCommand) (RevokeLeaseResult, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.revokeCalls = append(f.revokeCalls, cmd)
	return RevokeLeaseResult{Revoked: true, Settled: true}, nil
}

type schedulerFixture struct {
	svc        *Service
	pool       *pgxpool.Pool
	stationID  string
	nodeUID    string
	tokens     map[string]string
	verifier   *fakeVerifier
	evictor    *fakeEvictor
	currentNow time.Time
	nowMu      sync.Mutex
}

func (f *schedulerFixture) now() time.Time {
	f.nowMu.Lock()
	defer f.nowMu.Unlock()
	return f.currentNow
}

func (f *schedulerFixture) advanceTime(d time.Duration) {
	f.nowMu.Lock()
	f.currentNow = f.currentNow.Add(d)
	f.nowMu.Unlock()
}

func setupSchedulerDB(t *testing.T) *schedulerFixture {
	t.Helper()
	dsn := os.Getenv("STATION_TEST_DATABASE_URL")
	if dsn == "" {
		if os.Getenv("VERDANTFLARE_REQUIRE_TEST_DB") == "1" {
			t.Fatal("STATION_TEST_DATABASE_URL required when VERDANTFLARE_REQUIRE_TEST_DB=1")
		}
		t.Skip("STATION_TEST_DATABASE_URL required")
	}

	ctx := context.Background()
	adminCfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	if strings.EqualFold(strings.TrimSpace(adminCfg.ConnConfig.Database), "station") {
		t.Fatal("REFUSING to connect test admin pool to live 'station' database; point STATION_TEST_DATABASE_URL at 'postgres' maintenance DB")
	}
	admin, err := pgxpool.NewWithConfig(ctx, adminCfg)
	if err != nil {
		t.Fatal(err)
	}
	runID := strings.ReplaceAll(uuid.NewString(), "-", "")[:24]
	dbName := "station_gpu_sched_test_" + runID
	if _, err := admin.Exec(ctx, "CREATE DATABASE "+pgx.Identifier{dbName}.Sanitize()); err != nil {
		admin.Close()
		t.Fatal(err)
	}
	cfg := admin.Config()
	cfg.ConnConfig.Database = dbName
	cfg.MaxConns = 8
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		admin.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		pool.Close()
		_, dropErr := admin.Exec(context.Background(), "DROP DATABASE "+pgx.Identifier{dbName}.Sanitize()+" WITH (FORCE)")
		admin.Close()
		if dropErr != nil {
			t.Error(dropErr)
		}
	})

	migRoot := os.Getenv("STATION_TEST_MIGRATIONS")
	if migRoot == "" {
		migRoot = "../../../verdantflare-station-core/migrations"
	}
	files, err := filepath.Glob(filepath.Join(migRoot, "*.sql"))
	if err != nil || len(files) < 23 {
		t.Fatalf("expected >= 23 migrations, got %d (%v)", len(files), err)
	}
	sort.Strings(files)
	if _, err := pool.Exec(ctx, "CREATE SCHEMA station"); err != nil {
		t.Fatal(err)
	}
	for _, file := range files {
		raw, err := os.ReadFile(file)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := pool.Exec(ctx, string(raw)); err != nil {
			t.Fatalf("migration %s failed: %v", filepath.Base(file), err)
		}
	}

	stationID := uuid.NewString()
	if _, err := pool.Exec(ctx, `INSERT INTO station.configuration(singleton, station_id, contracts_major) VALUES (true, $1, 1)`, stationID); err != nil {
		t.Fatal(err)
	}

	tokens := map[string]string{
		"admin":         strings.Repeat("a", 40),
		"node_agent":    strings.Repeat("n", 40),
		"app-video":     strings.Repeat("v", 40),
		"app-image":     strings.Repeat("i", 40),
		"app-music":     strings.Repeat("m", 40),
		"app-speech":    strings.Repeat("s", 40),
		"app-comfyui":   strings.Repeat("c", 40),
		"app-blender":   strings.Repeat("b", 40),
		"station-core":  strings.Repeat("r", 40),
		"app-resources": strings.Repeat("p", 40),
	}
	regJSON := `[
		{"app_id":"station-admin","kind":"admin","token_env":"TOK_ADMIN"},
		{"app_id":"node-5090-agent","kind":"node_agent","token_env":"TOK_NODE"},
		{"app_id":"app-video","kind":"consumer","token_env":"TOK_VIDEO"},
		{"app_id":"app-image","kind":"consumer","token_env":"TOK_IMAGE"},
		{"app_id":"app-music","kind":"consumer","token_env":"TOK_MUSIC"},
		{"app_id":"app-speech","kind":"consumer","token_env":"TOK_SPEECH"},
		{"app_id":"app-comfyui","kind":"consumer","token_env":"TOK_COMFYUI"},
		{"app_id":"app-blender","kind":"consumer","token_env":"TOK_BLENDER"},
		{"app_id":"station-core","kind":"consumer","token_env":"TOK_CORE"},
		{"app_id":"app-resources","kind":"consumer","token_env":"TOK_RESOURCES"}
	]`
	envMap := map[string]string{
		"TOK_ADMIN":     tokens["admin"],
		"TOK_NODE":      tokens["node_agent"],
		"TOK_VIDEO":     tokens["app-video"],
		"TOK_IMAGE":     tokens["app-image"],
		"TOK_MUSIC":     tokens["app-music"],
		"TOK_SPEECH":    tokens["app-speech"],
		"TOK_COMFYUI":   tokens["app-comfyui"],
		"TOK_BLENDER":   tokens["app-blender"],
		"TOK_CORE":      tokens["station-core"],
		"TOK_RESOURCES": tokens["app-resources"],
	}
	callers, err := LoadCallerRegistry(strings.NewReader(regJSON), func(k string) string { return envMap[k] })
	if err != nil {
		t.Fatalf("LoadCallerRegistry: %v", err)
	}

	verifier := &fakeVerifier{
		defRes: VerifyTerminatedResult{
			Verified:        true,
			ContainerExited: true,
			TombstoneFound:  true,
		},
	}
	evictor := &fakeEvictor{pool: pool, stationID: stationID}
	fix := &schedulerFixture{
		pool:       pool,
		stationID:  stationID,
		nodeUID:    "node-5090-dev-01",
		tokens:     tokens,
		verifier:   verifier,
		evictor:    evictor,
		currentNow: time.Date(2026, 11, 15, 2, 0, 0, 0, time.UTC),
	}
	fix.svc = &Service{
		Pool:        pool,
		StationID:   stationID,
		Callers:     callers,
		AppGPUQuota: DefaultAppGPUQuota,
		Verifier:    verifier,
		Evictor:     evictor,
		Now:         fix.now,
	}

	// Seed dual RTX 5090 cards via ReportNodeTelemetry and switch admission_mode to active.
	nodeCaller := CallerIdentity{AppID: "node-5090-agent", Kind: CallerNodeAgent}
	if err := fix.svc.ReportNodeTelemetry(ctx, nodeCaller, NodeTelemetryReport{
		NodeUID:      fix.nodeUID,
		NodeName:     "k3s-5090-dev",
		SampleID:     1,
		AgentHealthy: true,
		Cards: []CardTelemetrySample{
			{GPUUUID: "GPU-0000", GPUIndex: 0, TotalVRAMMiB: 32607, DriverOverheadMiB: 607, MeasuredFreeVRAMMiB: 32000, MeasuredUsedVRAMMiB: 607, NVMLHealthy: true},
			{GPUUUID: "GPU-0001", GPUIndex: 1, TotalVRAMMiB: 32607, DriverOverheadMiB: 607, MeasuredFreeVRAMMiB: 32000, MeasuredUsedVRAMMiB: 607, NVMLHealthy: true},
		},
	}); err != nil {
		t.Fatalf("seed telemetry: %v", err)
	}
	adminCaller := CallerIdentity{AppID: "station-admin", Kind: CallerAdmin}
	if err := fix.svc.SetAdmissionMode(ctx, adminCaller, AdmissionModeRequest{
		NodeUID:       fix.nodeUID,
		AdmissionMode: AdmissionActive,
	}); err != nil {
		t.Fatalf("seed SetAdmissionMode(active): %v", err)
	}
	return fix
}

func verifyAllCardHashChains(t *testing.T, ctx context.Context, pool *pgxpool.Pool, stationID string) {
	t.Helper()
	cRows, err := pool.Query(ctx, `SELECT node_uid, gpu_uuid, last_event_seq, last_event_hash FROM station.gpu_physical_cards WHERE station_id = $1`, stationID)
	if err != nil {
		t.Fatal(err)
	}
	type cardHead struct {
		nodeUID  string
		gpuUUID  string
		lastSeq  int64
		lastHash string
	}
	var heads []cardHead
	for cRows.Next() {
		var h cardHead
		if err := cRows.Scan(&h.nodeUID, &h.gpuUUID, &h.lastSeq, &h.lastHash); err != nil {
			t.Fatal(err)
		}
		heads = append(heads, h)
	}
	cRows.Close()

	for _, h := range heads {
		eRows, err := pool.Query(ctx, `
			SELECT card_event_seq, prev_hash, event_hash
			  FROM station.gpu_scheduler_events
			 WHERE station_id = $1 AND node_uid = $2 AND gpu_uuid = $3
			 ORDER BY card_event_seq ASC
		`, stationID, h.nodeUID, h.gpuUUID)
		if err != nil {
			t.Fatal(err)
		}
		expectedPrev := "GENESIS"
		var count int64
		for eRows.Next() {
			count++
			var seq int64
			var prev, cur string
			if err := eRows.Scan(&seq, &prev, &cur); err != nil {
				t.Fatal(err)
			}
			if seq != count || prev != expectedPrev || len(cur) != 64 {
				t.Fatalf("card %s hash chain broken at seq=%d (expected %d): prev=%s wantPrev=%s hash=%s", h.gpuUUID, seq, count, prev, expectedPrev, cur)
			}
			expectedPrev = cur
		}
		eRows.Close()
		if count != h.lastSeq || expectedPrev != h.lastHash {
			t.Fatalf("card %s head mismatch: count=%d lastSeq=%d expectedPrev=%s lastHash=%s", h.gpuUUID, count, h.lastSeq, expectedPrev, h.lastHash)
		}
	}
}

func TestCallerRegistryAndHTTPAuthUnit(t *testing.T) {
	tok := strings.Repeat("x", 36)
	reg, err := LoadCallerRegistry(strings.NewReader(`[{"app_id":"app-video","kind":"consumer","token_env":"T"}]`), func(k string) string {
		if k == "T" {
			return tok
		}
		return ""
	})
	if err != nil {
		t.Fatalf("LoadCallerRegistry: %v", err)
	}
	if _, err := reg.Authenticate("Bearer wrong"); !errors.Is(err, ErrUnauthenticated) {
		t.Fatalf("expected ErrUnauthenticated, got %v", err)
	}
	caller, err := reg.Authenticate("Bearer " + tok)
	if err != nil || caller.AppID != "app-video" {
		t.Fatalf("unexpected caller: %+v (%v)", caller, err)
	}

	srv := httptest.NewServer(Handler{Service: &Service{Callers: reg}})
	defer srv.Close()

	// Unauthenticated -> 401.
	r1, err := http.Get(srv.URL + "/internal/v1/gpu-scheduler/snapshot")
	if err != nil {
		t.Fatal(err)
	}
	_ = r1.Body.Close()
	if r1.StatusCode != http.StatusUnauthorized {
		t.Fatalf("got %d, want 401", r1.StatusCode)
	}

	// app-video cannot read global snapshot -> 403.
	req, _ := http.NewRequest(http.MethodGet, srv.URL+"/internal/v1/gpu-scheduler/snapshot", nil)
	req.Header.Set("Authorization", "Bearer "+tok)
	r2, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	_ = r2.Body.Close()
	if r2.StatusCode != http.StatusForbidden {
		t.Fatalf("got %d, want 403", r2.StatusCode)
	}

	sum := sha256.Sum256([]byte("test"))
	if !sha256HexPattern.MatchString(hex.EncodeToString(sum[:])) {
		t.Fatal("expected valid sha256 hex")
	}
}

func TestPA_ConcurrentAdmissionJointLiabilityAndQuota(t *testing.T) {
	fix := setupSchedulerDB(t)
	ctx := context.Background()
	imgCaller := CallerIdentity{AppID: "app-image", Kind: CallerConsumer}

	// Deterministic RendezvousBarrier(2): two 18432 MiB moss-image requests target GPU-0000 simultaneously!
	barrier := newRendezvousBarrier("after_card_read_before_commit", 2)
	// Note: because lockCardsForUpdate serializes at PG row lock level BEFORE after_card_read_before_commit,
	// let's test true pre-tx concurrency across 4 goroutines targeting GPU-0000: at most ONE 18432 MiB lease can be admitted!
	var wg sync.WaitGroup
	startGate := make(chan struct{})
	decisions := make([]LeaseDecision, 4)
	errs := make([]error, 4)
	_ = barrier

	for idx := 0; idx < 4; idx++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-startGate
			decisions[i], errs[i] = fix.svc.Acquire(ctx, imgCaller, AcquireRequest{
				IdempotencyKey:    uuid.NewString(),
				ConsumerKind:      "moss-image",
				AttemptID:         uuid.NewString(),
				CandidateGPUUUIDs: []string{"GPU-0000"},
				RunNonce:          "pa-race",
			})
		}(idx)
	}
	close(startGate)
	wg.Wait()

	leasedCount := 0
	queuedCount := 0
	var activeLease LeaseDecision
	for i := 0; i < 4; i++ {
		if errs[i] != nil {
			t.Fatalf("Acquire[%d] error: %v", i, errs[i])
		}
		switch decisions[i].State {
		case "leased":
			leasedCount++
			activeLease = decisions[i]
		case "queued":
			queuedCount++
		default:
			t.Fatalf("unexpected state %s", decisions[i].State)
		}
	}
	if leasedCount != 1 || queuedCount != 3 {
		t.Fatalf("P-A single-card 18432 MiB race: got leased=%d queued=%d, want leased=1 queued=3", leasedCount, queuedCount)
	}
	if activeLease.FencingToken < 2 {
		t.Fatalf("expected fencing token >= 2, got %d", activeLease.FencingToken)
	}

	// Joint liability (unattributedExternalMiB) on GPU-0001: inject 20000 MiB external CUDA usage via telemetry.
	nodeCaller := CallerIdentity{AppID: "node-5090-agent", Kind: CallerNodeAgent}
	if err := fix.svc.ReportNodeTelemetry(ctx, nodeCaller, NodeTelemetryReport{
		NodeUID:      fix.nodeUID,
		NodeName:     "k3s-5090-dev",
		SampleID:     2,
		AgentHealthy: true,
		Cards: []CardTelemetrySample{
			{GPUUUID: "GPU-0000", GPUIndex: 0, TotalVRAMMiB: 32607, DriverOverheadMiB: 607, MeasuredFreeVRAMMiB: 13500, MeasuredUsedVRAMMiB: 19107, AllocatedK8sSlots: 12, NVMLHealthy: true},
			{GPUUUID: "GPU-0001", GPUIndex: 1, TotalVRAMMiB: 32607, DriverOverheadMiB: 607, MeasuredFreeVRAMMiB: 12000, MeasuredUsedVRAMMiB: 20607, NVMLHealthy: true},
		},
	}); err != nil {
		t.Fatal(err)
	}

	// moss-image (18432 MiB) on GPU-0001 must be queued due to unattributed external VRAM!
	extDec, err := fix.svc.Acquire(ctx, imgCaller, AcquireRequest{
		IdempotencyKey:    uuid.NewString(),
		ConsumerKind:      "moss-image",
		AttemptID:         uuid.NewString(),
		CandidateGPUUUIDs: []string{"GPU-0001"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if extDec.State != "queued" {
		t.Fatalf("expected queued under unattributed external VRAM, got %+v", extDec)
	}

	// D8-1 K8s 12-slot quota saturation: allocated_k8s_slots == 12 blocks new K8s Pod scale-up (`k8s_slots_required: 1`) even for small 4096 MiB RIFE!
	vidCaller := CallerIdentity{AppID: "app-video", Kind: CallerConsumer}
	quotaDec, err := fix.svc.Acquire(ctx, vidCaller, AcquireRequest{
		IdempotencyKey:    uuid.NewString(),
		ConsumerKind:      "rife",
		AttemptID:         uuid.NewString(),
		CandidateGPUUUIDs: []string{"GPU-0001"},
		K8sSlotsRequired:  1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if quotaDec.State != "queued" || quotaDec.BlockingReason != "k8s_gpu_quota_saturated" {
		t.Fatalf("expected k8s_gpu_quota_saturated, got %+v", quotaDec)
	}

	verifyAllCardHashChains(t, ctx, fix.pool, fix.stationID)
}

func TestPB_DeadlockFreeDualCardAndCrossCardHandoff(t *testing.T) {
	fix := setupSchedulerDB(t)
	ctx := context.Background()
	vidCaller := CallerIdentity{AppID: "app-video", Kind: CallerConsumer}
	musCaller := CallerIdentity{AppID: "app-music", Kind: CallerConsumer}

	// Occupy GPU-0000 with an active moss-image (18432 MiB) and reserve GPU-0001 with a queued h3 task.
	imgCaller := CallerIdentity{AppID: "app-image", Kind: CallerConsumer}
	l0, err := fix.svc.Acquire(ctx, imgCaller, AcquireRequest{
		IdempotencyKey:    "img-hold-gpu0",
		ConsumerKind:      "moss-image",
		AttemptID:         "att-img-0",
		CandidateGPUUUIDs: []string{"GPU-0000"},
	})
	if err != nil || l0.State != "leased" {
		t.Fatalf("expected leased on GPU-0000: %+v (%v)", l0, err)
	}

	// Also occupy GPU-0001 with an active speech task (6144 MiB) so h3 (29696 MiB) queues and reserves GPU-0001.
	spCaller := CallerIdentity{AppID: "app-speech", Kind: CallerConsumer}
	l1, err := fix.svc.Acquire(ctx, spCaller, AcquireRequest{
		IdempotencyKey:    "sp-hold-gpu1",
		ConsumerKind:      "indextts2",
		AttemptID:         "att-sp-1",
		CandidateGPUUUIDs: []string{"GPU-0001"},
	})
	if err != nil || l1.State != "leased" {
		t.Fatalf("expected leased on GPU-0001: %+v (%v)", l1, err)
	}

	h3Queued, err := fix.svc.Acquire(ctx, vidCaller, AcquireRequest{
		IdempotencyKey:    "h3-res-gpu1",
		ConsumerKind:      "h3",
		AttemptID:         "att-h3-1",
		CandidateGPUUUIDs: []string{"GPU-0001"},
	})
	if err != nil || h3Queued.State != "queued" || len(h3Queued.ReservedGPUUUIDs) != 1 || h3Queued.ReservedGPUUUIDs[0] != "GPU-0001" {
		t.Fatalf("expected h3 queued reserving GPU-0001, got %+v (%v)", h3Queued, err)
	}

	// Now music3 (CardCount=2, 16384 MiB) arrives when GPU-0000 is unreserved (but busy) and GPU-0001 is reserved by h3:
	// P-B invariant: music3 MUST reserve 0 cards (never hold GPU-0000 partial reservation while waiting for GPU-0001!).
	mDec, err := fix.svc.Acquire(ctx, musCaller, AcquireRequest{
		IdempotencyKey: "music3-dual",
		ConsumerKind:   "music3",
		AttemptID:      "att-music3",
	})
	if err != nil {
		t.Fatal(err)
	}
	if mDec.State != "queued" || len(mDec.ReservedGPUUUIDs) != 0 {
		t.Fatalf("P-B violation: dual-card music3 must reserve 0 cards when only 1 card is unreserved, got %+v", mDec)
	}

	// P-B cross-card hold-and-wait check: holding l0 on GPU-0000 while requesting GPU-0001 with SourceLeaseID=l0.LeaseID must fail 409!
	_, errCross := fix.svc.Acquire(ctx, imgCaller, AcquireRequest{
		IdempotencyKey:    "cross-card-attempt",
		ConsumerKind:      "face-fusion",
		AttemptID:         "att-cross",
		CandidateGPUUUIDs: []string{"GPU-0001"},
		SourceLeaseID:     l0.LeaseID,
	})
	if !errors.Is(errCross, ErrCrossCardHoldAndWait) {
		t.Fatalf("expected ErrCrossCardHoldAndWait, got %v", errCross)
	}

	// Cancel h3Queued and release sp-hold-gpu1 -> re-polling music3 while GPU-0000 is still held by l0 must atomically reserve BOTH {GPU-0000, GPU-0001}!
	if _, err := fix.svc.Release(ctx, vidCaller, h3Queued.LeaseID, ReleaseRequest{IdempotencyKey: "cancel-h3", WorkerSettled: true}); err != nil {
		t.Fatal(err)
	}
	mDec2, err := fix.svc.Acquire(ctx, musCaller, AcquireRequest{
		IdempotencyKey: "music3-dual",
		ConsumerKind:   "music3",
		AttemptID:      "att-music3",
	})
	if err != nil {
		t.Fatal(err)
	}
	if mDec2.State != "queued" || len(mDec2.ReservedGPUUUIDs) != 2 {
		t.Fatalf("expected atomic 2-card reservation {GPU-0000, GPU-0001}, got %+v", mDec2)
	}

	// Release l0 and l1 -> music3 promotes cleanly to 2-card leased!
	if _, err := fix.svc.Release(ctx, imgCaller, l0.LeaseID, ReleaseRequest{IdempotencyKey: "rel-l0", FencingToken: l0.FencingToken, WorkerSettled: true}); err != nil {
		t.Fatal(err)
	}
	if _, err := fix.svc.Release(ctx, spCaller, l1.LeaseID, ReleaseRequest{IdempotencyKey: "rel-l1", FencingToken: l1.FencingToken, WorkerSettled: true}); err != nil {
		t.Fatal(err)
	}
	mPromoted, err := fix.svc.Acquire(ctx, musCaller, AcquireRequest{
		IdempotencyKey: "music3-dual",
		ConsumerKind:   "music3",
		AttemptID:      "att-music3",
	})
	if err != nil || mPromoted.State != "leased" || len(mPromoted.GPUUUIDs) != 2 {
		t.Fatalf("expected music3 promoted to 2-card leased, got %+v (%v)", mPromoted, err)
	}

	verifyAllCardHashChains(t, ctx, fix.pool, fix.stationID)
}

func TestPC_BoundedBypassFairnessAndDowngradeGate(t *testing.T) {
	fix := setupSchedulerDB(t)
	ctx := context.Background()
	vidCaller := CallerIdentity{AppID: "app-video", Kind: CallerConsumer}
	spCaller := CallerIdentity{AppID: "app-speech", Kind: CallerConsumer}

	// Seed 1 active 6144 MiB speech lease on GPU-0000 so h3 (29696 MiB) queues & reserves GPU-0000, while ~23.8 GiB remains free.
	hold, err := fix.svc.Acquire(ctx, spCaller, AcquireRequest{
		IdempotencyKey:    "sp-initial",
		ConsumerKind:      "indextts2",
		AttemptID:         "att-sp-init",
		CandidateGPUUUIDs: []string{"GPU-0000"},
	})
	if err != nil || hold.State != "leased" {
		t.Fatal(err)
	}

	h3, err := fix.svc.Acquire(ctx, vidCaller, AcquireRequest{
		IdempotencyKey:    "h3-head",
		ConsumerKind:      "h3",
		AttemptID:         "att-h3-head",
		CandidateGPUUUIDs: []string{"GPU-0000"},
	})
	if err != nil || h3.State != "queued" {
		t.Fatalf("expected h3 queued: %+v (%v)", h3, err)
	}

	// D5-2: First 2 small shared_slice RIFE tasks (4096 MiB) may bypass h3; 3rd MUST be blocked!
	for i := 1; i <= 2; i++ {
		rDec, err := fix.svc.Acquire(ctx, vidCaller, AcquireRequest{
			IdempotencyKey:    "rife-bypass-" + string(rune('0'+i)),
			ConsumerKind:      "rife",
			AttemptID:         "att-rife-" + string(rune('0'+i)),
			CandidateGPUUUIDs: []string{"GPU-0000"},
		})
		if err != nil || rDec.State != "leased" {
			t.Fatalf("bypass #%d should succeed, got %+v (%v)", i, rDec, err)
		}
		if _, err := fix.svc.Release(ctx, vidCaller, rDec.LeaseID, ReleaseRequest{
			IdempotencyKey: "rel-rife-" + string(rune('0'+i)),
			FencingToken:   rDec.FencingToken,
			WorkerSettled:  true,
		}); err != nil {
			t.Fatal(err)
		}
	}

	// 3rd RIFE task must hit reservation_barrier_locked!
	r3, err := fix.svc.Acquire(ctx, vidCaller, AcquireRequest{
		IdempotencyKey:    "rife-bypass-3",
		ConsumerKind:      "rife",
		AttemptID:         "att-rife-3",
		CandidateGPUUUIDs: []string{"GPU-0000"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if r3.State != "queued" || r3.BlockingReason != "reservation_barrier_locked" {
		t.Fatalf("D5-2 violation: 3rd bypass must be blocked by reservation_barrier_locked, got %+v", r3)
	}

	// Release initial speech hold -> h3 acquires GPU-0000 with ExclusiveCardHeadroomMiB (1024 MiB -> 32607-607-1024 = 30976 >= 29696)!
	if _, err := fix.svc.Release(ctx, spCaller, hold.LeaseID, ReleaseRequest{
		IdempotencyKey: "rel-sp-initial",
		FencingToken:   hold.FencingToken,
		WorkerSettled:  true,
	}); err != nil {
		t.Fatal(err)
	}
	h3Admitted, err := fix.svc.Acquire(ctx, vidCaller, AcquireRequest{
		IdempotencyKey:    "h3-head",
		ConsumerKind:      "h3",
		AttemptID:         "att-h3-head",
		CandidateGPUUUIDs: []string{"GPU-0000"},
	})
	if err != nil || h3Admitted.State != "leased" {
		t.Fatalf("expected h3 admitted after hold release, got %+v (%v)", h3Admitted, err)
	}

	// G6: Downgrade P2 (29696) -> P3 (8192) MUST fail while CutoverCompleted == false!
	downBudget := int64(8192)
	_, errDown := fix.svc.Downgrade(ctx, vidCaller, h3Admitted.LeaseID, HeartbeatRequest{
		IdempotencyKey: "down-before-cutover",
		FencingToken:   h3Admitted.FencingToken,
		DowngradeToMiB: &downBudget,
		NewStageID:     "postprocess_sr",
		WorkerSettled:  true,
	})
	if !errors.Is(errDown, ErrDowngradeDisabled) {
		t.Fatalf("expected ErrDowngradeDisabled before cutover, got %v", errDown)
	}

	// Enable CutoverCompleted -> Downgrade with WorkerSettled=false still fails, then WorkerSettled=true succeeds!
	fix.svc.CutoverCompleted = true
	_, errUnsettled := fix.svc.Downgrade(ctx, vidCaller, h3Admitted.LeaseID, HeartbeatRequest{
		IdempotencyKey: "down-unsettled",
		FencingToken:   h3Admitted.FencingToken,
		DowngradeToMiB: &downBudget,
		WorkerSettled:  false,
	})
	if !errors.Is(errUnsettled, ErrWorkerNotSettled) {
		t.Fatalf("expected ErrWorkerNotSettled, got %v", errUnsettled)
	}

	downOK, err := fix.svc.Downgrade(ctx, vidCaller, h3Admitted.LeaseID, HeartbeatRequest{
		IdempotencyKey: "down-settled",
		FencingToken:   h3Admitted.FencingToken,
		DowngradeToMiB: &downBudget,
		NewStageID:     "postprocess_sr",
		WorkerSettled:  true,
	})
	if err != nil || downOK.PeakBudgetMiB != 8192 || downOK.StageID != "postprocess_sr" || downOK.State != "running" {
		t.Fatalf("expected downgraded running lease at 8192 MiB, got %+v (%v)", downOK, err)
	}

	verifyAllCardHashChains(t, ctx, fix.pool, fix.stationID)
}

func TestPD_HumanS1ProtectionEvictionAndCutoverGate(t *testing.T) {
	fix := setupSchedulerDB(t)
	ctx := context.Background()
	adminCaller := CallerIdentity{AppID: "station-admin", Kind: CallerAdmin}
	blenderCaller := CallerIdentity{AppID: "app-blender", Kind: CallerConsumer}
	comfyCaller := CallerIdentity{AppID: "app-comfyui", Kind: CallerConsumer}
	vidCaller := CallerIdentity{AppID: "app-video", Kind: CallerConsumer}
	nodeCaller := CallerIdentity{AppID: "node-5090-agent", Kind: CallerNodeAgent}

	// Put GPU-0000 back to drain_only and report an un-upgraded human_protected Blender instance -> SetAdmissionMode(active) MUST fail!
	if err := fix.svc.SetAdmissionMode(ctx, adminCaller, AdmissionModeRequest{NodeUID: fix.nodeUID, AdmissionMode: AdmissionDrainOnly}); err != nil {
		t.Fatal(err)
	}
	notUpgraded := false
	alloc6G := int64(6144)
	if err := fix.svc.ReportResidency(ctx, blenderCaller, ResidencyReportRequest{
		InstanceID:         "blender-inst-s1",
		NodeUID:            fix.nodeUID,
		PodUID:             "pod-blender-s1",
		BootID:             "boot-1",
		GPUEpoch:           1,
		ResidencySeq:       1,
		GPUUUID:            "GPU-0000",
		ResidencyState:     "human_protected",
		HumanProtected:     true,
		GUIConnectionAlive: true,
		Dirty:              true,
		BlockingReason:     "human_gui_active",
		PinnedBudgetMiB:    6144,
		VRAMAllocatedMiB:   &alloc6G,
		TemplateUpgraded:   &notUpgraded,
	}); err != nil {
		t.Fatal(err)
	}
	if err := fix.svc.SetAdmissionMode(ctx, adminCaller, AdmissionModeRequest{NodeUID: fix.nodeUID, AdmissionMode: AdmissionActive}); !errors.Is(err, ErrCutoverBlocked) {
		t.Fatalf("expected ErrCutoverBlocked on un-upgraded S1 instance, got %v", err)
	}

	// Stale out-of-order residency_seq on same boot_id must fail ErrVersionConflict!
	upgraded := true
	if err := fix.svc.ReportResidency(ctx, blenderCaller, ResidencyReportRequest{
		InstanceID:         "blender-inst-s1",
		NodeUID:            fix.nodeUID,
		PodUID:             "pod-blender-s1",
		BootID:             "boot-1",
		GPUEpoch:           0,
		ResidencySeq:       1,
		GPUUUID:            "GPU-0000",
		ResidencyState:     "human_protected",
		HumanProtected:     true,
		GUIConnectionAlive: true,
		TemplateUpgraded:   &upgraded,
	}); !errors.Is(err, ErrVersionConflict) {
		t.Fatalf("expected ErrVersionConflict on stale gpu_epoch, got %v", err)
	}

	// Upgrade template on blender-inst-s1 -> SetAdmissionMode(active) succeeds, and S1 human_protected blocks exclusive h3 on GPU-0000!
	if err := fix.svc.ReportResidency(ctx, blenderCaller, ResidencyReportRequest{
		InstanceID:         "blender-inst-s1",
		NodeUID:            fix.nodeUID,
		PodUID:             "pod-blender-s1",
		BootID:             "boot-1",
		GPUEpoch:           1,
		ResidencySeq:       2,
		GPUUUID:            "GPU-0000",
		ResidencyState:     "human_protected",
		HumanProtected:     true,
		GUIConnectionAlive: true,
		BlockingReason:     "human_gui_active",
		PinnedBudgetMiB:    6144,
		VRAMAllocatedMiB:   &alloc6G,
		TemplateUpgraded:   &upgraded,
	}); err != nil {
		t.Fatal(err)
	}
	if err := fix.svc.SetAdmissionMode(ctx, adminCaller, AdmissionModeRequest{NodeUID: fix.nodeUID, AdmissionMode: AdmissionActive}); err != nil {
		t.Fatalf("SetAdmissionMode(active) after upgrade: %v", err)
	}

	h3Blocked, err := fix.svc.Acquire(ctx, vidCaller, AcquireRequest{
		IdempotencyKey:    "h3-vs-human-s1",
		ConsumerKind:      "h3",
		AttemptID:         "att-h3-s1",
		CandidateGPUUUIDs: []string{"GPU-0000"},
	})
	if err != nil || h3Blocked.State != "queued" || h3Blocked.BlockingReason != "human_gui_active" {
		t.Fatalf("P-D violation: h3 must queue behind human_gui_active without evicting S1, got %+v (%v)", h3Blocked, err)
	}

	// On GPU-0001, seed an S2 idle_evictable ComfyUI holding 14336 MiB VRAM (so measured_free = 17664 MiB < 29696 MiB),
	// backed by node-proven AttributedProcesses from station-gpu-agent (P1-H1 / P1-H4).
	alloc14G := int64(14336)
	if err := fix.svc.ReportNodeTelemetry(ctx, nodeCaller, NodeTelemetryReport{
		NodeUID:      fix.nodeUID,
		NodeName:     "k3s-5090-dev",
		AgentBootID:  "agent-boot-1",
		SampleID:     3,
		AgentHealthy: true,
		Cards: []CardTelemetrySample{
			{
				GPUUUID: "GPU-0000", GPUIndex: 0, TotalVRAMMiB: 32607, DriverOverheadMiB: 607,
				MeasuredFreeVRAMMiB: 25856, MeasuredUsedVRAMMiB: 6751, NVMLHealthy: true,
				AttributedProcesses: []AttributedGPUProcess{{PodUID: "pod-blender-s1", PID: 8001, UsedVRAMMiB: 6144}},
			},
			{
				GPUUUID: "GPU-0001", GPUIndex: 1, TotalVRAMMiB: 32607, DriverOverheadMiB: 607,
				MeasuredFreeVRAMMiB: 17664, MeasuredUsedVRAMMiB: 14943, NVMLHealthy: true,
				AttributedProcesses: []AttributedGPUProcess{{PodUID: "pod-comfy-s2", PID: 9001, UsedVRAMMiB: 14336}},
			},
		},
	}); err != nil {
		t.Fatal(err)
	}
	if err := fix.svc.ReportResidency(ctx, comfyCaller, ResidencyReportRequest{
		InstanceID:       "comfy-inst-s2",
		NodeUID:          fix.nodeUID,
		PodUID:           "pod-comfy-s2",
		BootID:           "boot-comfy-1",
		GPUEpoch:         2,
		ResidencySeq:     5,
		GPUUUID:          "GPU-0001",
		ResidencyState:   "idle_evictable",
		BlockingReason:   "none",
		VRAMAllocatedMiB: &alloc14G,
		CallbackEndpoint: "http://comfy-worker.verdantflare-app.svc.cluster.local:8188",
	}); err != nil {
		t.Fatal(err)
	}

	// Case 1: Human takes over during eviction (EvictVRAM returns 409 Conflict) -> h3 reverts cleanly to queued!
	fix.evictor.evictFn = func(_ EvictVRAMCommand) (EvictVRAMResult, error) {
		return EvictVRAMResult{Conflict: true, ReasonCode: "HUMAN_PROTECTED_TAKEOVER"}, nil
	}
	h3Conflict, err := fix.svc.Acquire(ctx, vidCaller, AcquireRequest{
		IdempotencyKey:    "h3-evict-conflict",
		ConsumerKind:      "h3",
		AttemptID:         "att-h3-conflict",
		CandidateGPUUUIDs: []string{"GPU-0001"},
	})
	if err != nil || h3Conflict.State != "queued" || h3Conflict.BlockingReason != "HUMAN_PROTECTED_TAKEOVER" {
		t.Fatalf("expected queued after eviction conflict, got %+v (%v)", h3Conflict, err)
	}
	_, _ = fix.svc.Release(ctx, vidCaller, h3Conflict.LeaseID, ReleaseRequest{IdempotencyKey: "cancel-h3-conflict", WorkerSettled: true})

	// Case 2: EvictVRAM settles cleanly and NodeVerifier confirms 32000 MiB free -> h3 promotes directly to leased!
	fix.evictor.evictFn = func(cmd EvictVRAMCommand) (EvictVRAMResult, error) {
		return EvictVRAMResult{
			Settled:                  true,
			ResidencyState:           "zero_vram_standby",
			ResidencySeq:             cmd.ExpectedResidencySeq + 1,
			GPUEpoch:                 cmd.ExpectedGPUEpoch,
			SealedGPUEpoch:           cmd.ExpectedGPUEpoch,
			BootID:                   cmd.ExpectedBootID,
			SupervisorPID:            9001,
			SupervisorStarttimeTicks: 88001,
			ReclaimProof:             makeMockEpochSealProof(ctx, fix.pool, fix.stationID, cmd, 9001, 88001),
		}, nil
	}
	fix.verifier.byKey = map[string]VerifyTerminatedResult{
		"pod-comfy-s2": {
			Verified:            true,
			TombstoneFound:      true,
			MeasuredFreeVRAMMiB: 32000,
		},
	}
	h3Promoted, err := fix.svc.Acquire(ctx, vidCaller, AcquireRequest{
		IdempotencyKey:    "h3-evict-settled",
		ConsumerKind:      "h3",
		AttemptID:         "att-h3-settled",
		CandidateGPUUUIDs: []string{"GPU-0001"},
	})
	if err != nil || h3Promoted.State != "leased" || h3Promoted.GPUUUID != "GPU-0001" {
		t.Fatalf("expected h3 promoted to leased after S2 eviction settled, got %+v (%v)", h3Promoted, err)
	}

	verifyAllCardHashChains(t, ctx, fix.pool, fix.stationID)
}

func TestPE_PF_ReconcileFailClosedAppendOnlyAndIdempotency(t *testing.T) {
	fix := setupSchedulerDB(t)
	ctx := context.Background()
	spCaller := CallerIdentity{AppID: "app-speech", Kind: CallerConsumer}
	imgCaller := CallerIdentity{AppID: "app-image", Kind: CallerConsumer}

	// P-F: Idempotent Acquire replay vs payload conflict!
	lLive, err := fix.svc.Acquire(ctx, spCaller, AcquireRequest{
		IdempotencyKey:    "sp-live-1",
		ConsumerKind:      "indextts2",
		AttemptID:         "att-live-1",
		PodUID:            "pod-live-1",
		ContainerID:       "cid-live-1",
		CandidateGPUUUIDs: []string{"GPU-0000"},
	})
	if err != nil || lLive.State != "leased" {
		t.Fatal(err)
	}
	lReplay, err := fix.svc.Acquire(ctx, spCaller, AcquireRequest{
		IdempotencyKey:    "sp-live-1",
		ConsumerKind:      "indextts2",
		AttemptID:         "att-live-1",
		PodUID:            "pod-live-1",
		ContainerID:       "cid-live-1",
		CandidateGPUUUIDs: []string{"GPU-0000"},
	})
	if err != nil || lReplay.LeaseID != lLive.LeaseID || lReplay.FencingToken != lLive.FencingToken {
		t.Fatalf("P-F replay mismatch: %+v vs %+v (%v)", lLive, lReplay, err)
	}
	if _, err := fix.svc.Acquire(ctx, spCaller, AcquireRequest{
		IdempotencyKey:    "sp-live-1",
		ConsumerKind:      "moss-image",
		AttemptID:         "att-mutated",
		CandidateGPUUUIDs: []string{"GPU-0000"},
	}); !errors.Is(err, ErrIdempotencyConflict) {
		t.Fatalf("expected ErrIdempotencyConflict, got %v", err)
	}

	// Lease on GPU-0001 that will encounter UNKNOWN_NODE_STATE on Release, then D-state quarantine on ReconcileRestart!
	lDoomed, err := fix.svc.Acquire(ctx, imgCaller, AcquireRequest{
		IdempotencyKey:    "img-doomed",
		ConsumerKind:      "face-fusion",
		AttemptID:         "att-doomed",
		PodUID:            "pod-doomed",
		ContainerID:       "cid-doomed",
		CandidateGPUUUIDs: []string{"GPU-0001"},
	})
	if err != nil || lDoomed.State != "leased" {
		t.Fatal(err)
	}

	// Release with worker_settled=false MUST fail!
	if _, err := fix.svc.Release(ctx, imgCaller, lDoomed.LeaseID, ReleaseRequest{
		IdempotencyKey: "rel-unsettled",
		FencingToken:   lDoomed.FencingToken,
		WorkerSettled:  false,
	}); !errors.Is(err, ErrWorkerNotSettled) {
		t.Fatalf("expected ErrWorkerNotSettled, got %v", err)
	}

	// NodeVerifier returns UNKNOWN_NODE_STATE -> Release fails 503 and puts GPU-0001 into reconciling (fail-closed)!
	fix.verifier.byKey = map[string]VerifyTerminatedResult{
		"pod-doomed": {ErrorCode: "UNKNOWN_NODE_STATE"},
	}
	if _, err := fix.svc.Release(ctx, imgCaller, lDoomed.LeaseID, ReleaseRequest{
		IdempotencyKey: "rel-unknown",
		FencingToken:   lDoomed.FencingToken,
		WorkerSettled:  true,
	}); !errors.Is(err, ErrNodeStateUnknown) {
		t.Fatalf("expected ErrNodeStateUnknown, got %v", err)
	}

	// New Acquire on reconciling GPU-0001 MUST queue instead of double-granting!
	qWhileUnknown, err := fix.svc.Acquire(ctx, spCaller, AcquireRequest{
		IdempotencyKey:    "sp-while-unknown",
		ConsumerKind:      "indextts2",
		AttemptID:         "att-while-unknown",
		CandidateGPUUUIDs: []string{"GPU-0001"},
	})
	if err != nil || qWhileUnknown.State != "queued" {
		t.Fatalf("P-E violation: card in UNKNOWN_NODE_STATE reconciling must not admit new lease, got %+v (%v)", qWhileUnknown, err)
	}

	// Now NodeVerifier comes back and reports pod-doomed is stuck in unkillable D-state, while pod-live-1 is still alive!
	fix.verifier.byKey = map[string]VerifyTerminatedResult{
		"pod-doomed": {
			Verified:         false,
			UnkillableDState: true,
		},
	}
	if err := fix.svc.ReconcileRestart(ctx, RestartObservation{
		NodeUID:     fix.nodeUID,
		LivePodUIDs: map[string]bool{"pod-live-1": true, "pod-doomed": false},
		RunNonce:    "pe-reconcile",
	}); err != nil {
		t.Fatalf("ReconcileRestart: %v", err)
	}

	// Verify: pod-live-1 survived untouched; pod-doomed is quarantined and 6144 MiB is added to GPU-0001 quarantined_mib; GPU-0001 recovered from UNKNOWN_NODE_STATE!
	liveAfter, err := fix.svc.GetLease(ctx, spCaller, lLive.LeaseID)
	if err != nil || liveAfter.State != "leased" {
		t.Fatalf("live lease should survive ReconcileRestart untouched, got %+v (%v)", liveAfter, err)
	}
	doomedAfter, err := fix.svc.GetLease(ctx, imgCaller, lDoomed.LeaseID)
	if err != nil || doomedAfter.State != "quarantined" {
		t.Fatalf("expected doomed lease quarantined, got %+v (%v)", doomedAfter, err)
	}
	// Stale heartbeat from doomed lease must be rejected with ErrFencingRevoked!
	if _, err := fix.svc.Heartbeat(ctx, imgCaller, lDoomed.LeaseID, HeartbeatRequest{
		IdempotencyKey: "hb-after-quarantine",
		FencingToken:   lDoomed.FencingToken,
	}); !errors.Is(err, ErrFencingRevoked) {
		t.Fatalf("expected ErrFencingRevoked on quarantined lease, got %v", err)
	}

	snap, err := fix.svc.Snapshot(ctx)
	if err != nil || len(snap.Cards) != 2 {
		t.Fatalf("Snapshot failed: %v (%+v)", err, snap)
	}
	if snap.Cards[1].QuarantinedMiB != 6144 || snap.Cards[1].State != "ready" {
		t.Fatalf("expected GPU-0001 QuarantinedMiB=6144 and State=ready after node recovery, got %+v", snap.Cards[1])
	}

	// Advance time past TTL and run ReconcileRestart -> pod-live-1 expires cleanly when verified exited!
	fix.advanceTime(120 * time.Second)
	fix.verifier.byKey = map[string]VerifyTerminatedResult{
		"pod-live-1": {Verified: true, ContainerExited: true, TombstoneFound: true},
	}
	if err := fix.svc.ReconcileRestart(ctx, RestartObservation{
		NodeUID:     fix.nodeUID,
		LivePodUIDs: map[string]bool{},
		RunNonce:    "pe-ttl-expire",
	}); err != nil {
		t.Fatal(err)
	}
	expiredLive, err := fix.svc.GetLease(ctx, spCaller, lLive.LeaseID)
	if err != nil || expiredLive.State != "expired" {
		t.Fatalf("expected expired after TTL + container exit, got %+v (%v)", expiredLive, err)
	}

	// P1-H7: Because clock advanced 120s without fresh node telemetry (> 15s MaxTelemetryAge), Acquire MUST queue with telemetry_stale!
	staleDec, err := fix.svc.Acquire(ctx, spCaller, AcquireRequest{
		IdempotencyKey:    "stale-telemetry-check",
		ConsumerKind:      "indextts2",
		AttemptID:         "att-stale-check",
		CandidateGPUUUIDs: []string{"GPU-0000"},
	})
	if err != nil || staleDec.State != "queued" || staleDec.BlockingReason != "telemetry_stale" {
		t.Fatalf("expected queued with telemetry_stale after 120s telemetry silence, got %+v (%v)", staleDec, err)
	}
	_, _ = fix.svc.Release(ctx, spCaller, staleDec.LeaseID, ReleaseRequest{IdempotencyKey: "cancel-stale-check", WorkerSettled: true})

	// Push fresh node telemetry so HTTP E2E Acquire can admit cleanly.
	nodeCaller := CallerIdentity{AppID: "node-5090-agent", Kind: CallerNodeAgent}
	if err := fix.svc.ReportNodeTelemetry(ctx, nodeCaller, NodeTelemetryReport{
		NodeUID:      fix.nodeUID,
		NodeName:     "k3s-5090-dev",
		AgentBootID:  "agent-boot-fresh",
		SampleID:     2,
		AgentHealthy: true,
		Cards: []CardTelemetrySample{
			{GPUUUID: "GPU-0000", GPUIndex: 0, TotalVRAMMiB: 32607, DriverOverheadMiB: 607, MeasuredFreeVRAMMiB: 32000, MeasuredUsedVRAMMiB: 607, NVMLHealthy: true},
			{GPUUUID: "GPU-0001", GPUIndex: 1, TotalVRAMMiB: 32607, DriverOverheadMiB: 607, MeasuredFreeVRAMMiB: 25856, MeasuredUsedVRAMMiB: 6751, NVMLHealthy: true},
		},
	}); err != nil {
		t.Fatal(err)
	}

	// Verify HTTP Handler end-to-end roundtrip (`POST /leases/acquire` -> `POST /heartbeat` -> `POST /release` -> `GET /snapshot`).
	httpSrv := httptest.NewServer(Handler{Service: fix.svc})
	defer httpSrv.Close()

	acqBody, _ := json.Marshal(AcquireRequest{
		IdempotencyKey:    "http-e2e-1",
		ConsumerKind:      "indextts2",
		AttemptID:         "att-http-1",
		CandidateGPUUUIDs: []string{"GPU-0000"},
	})
	httpReq, _ := http.NewRequest(http.MethodPost, httpSrv.URL+"/internal/v1/gpu-scheduler/leases/acquire", bytes.NewReader(acqBody))
	httpReq.Header.Set("Authorization", "Bearer "+fix.tokens["app-speech"])
	httpReq.Header.Set("X-Verdant-Run-Nonce", "http-nonce-1")
	resp, err := http.DefaultClient.Do(httpReq)
	if err != nil {
		t.Fatal(err)
	}
	var httpDec LeaseDecision
	_ = json.NewDecoder(resp.Body).Decode(&httpDec)
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK || httpDec.State != "leased" {
		t.Fatalf("HTTP Acquire failed: status=%d dec=%+v", resp.StatusCode, httpDec)
	}

	// Verify append-only trigger on station.gpu_scheduler_events rejects UPDATE and DELETE!
	if _, err := fix.pool.Exec(ctx, `UPDATE station.gpu_scheduler_events SET reason_code = 'TAMPER'`); err == nil {
		t.Fatal("expected append-only trigger to reject UPDATE on station.gpu_scheduler_events")
	}
	if _, err := fix.pool.Exec(ctx, `DELETE FROM station.gpu_scheduler_events`); err == nil {
		t.Fatal("expected append-only trigger to reject DELETE on station.gpu_scheduler_events")
	}

	// P1-M2: Verify startup DB privilege isolation check passes against 0023 roles!
	if err := fix.svc.VerifyDatabasePrivilegeIsolation(ctx, false); err != nil {
		t.Fatalf("VerifyDatabasePrivilegeIsolation failed: %v", err)
	}

	verifyAllCardHashChains(t, ctx, fix.pool, fix.stationID)
}

func TestP_A_UnmaterializedLeaseDoesNotMaskExternalVRAM(t *testing.T) {
	fix := setupSchedulerDB(t)
	ctx := context.Background()
	imgCaller := CallerIdentity{AppID: "app-image", Kind: CallerConsumer}
	comfyCaller := CallerIdentity{AppID: "app-comfyui", Kind: CallerConsumer}
	nodeCaller := CallerIdentity{AppID: "node-5090-agent", Kind: CallerNodeAgent}

	// 1. Grant an 18432 MiB moss-image lease on GPU-0000 whose CUDA context has NOT allocated any VRAM yet (0 MiB attributed).
	lUnmat, err := fix.svc.Acquire(ctx, imgCaller, AcquireRequest{
		IdempotencyKey:    "unmat-moss-1",
		ConsumerKind:      "moss-image",
		AttemptID:         "att-unmat-1",
		PodUID:            "pod-unmat-moss",
		ContainerID:       "cid-unmat-moss",
		CandidateGPUUUIDs: []string{"GPU-0000"},
	})
	if err != nil || lUnmat.State != "leased" {
		t.Fatalf("expected initial moss-image leased: %+v (%v)", lUnmat, err)
	}

	// 2. Simultaneously, an unattributed external CUDA process consumes 18000 MiB on GPU-0000
	// (MeasuredUsedVRAMMiB = 607 + 18000 = 18607, MeasuredFreeVRAMMiB = 14000), while pod-unmat-moss still has 0 MiB in AttributedProcesses!
	// Also test P1-H4 spoofing defense on GPU-0000: a rogue residency claims 18000 MiB VRAMAllocatedMiB WITHOUT any node-proven AttributedProcesses!
	spoofed18G := int64(18000)
	if err := fix.svc.ReportResidency(ctx, comfyCaller, ResidencyReportRequest{
		InstanceID:       "spoofed-comfy",
		NodeUID:          fix.nodeUID,
		PodUID:           "pod-spoofed-comfy",
		BootID:           "boot-spoof-1",
		GPUEpoch:         1,
		ResidencySeq:     1,
		GPUUUID:          "GPU-0000",
		ResidencyState:   "idle_evictable",
		VRAMAllocatedMiB: &spoofed18G,
		CallbackEndpoint: "http://comfy-spoof.verdantflare-app.svc.cluster.local:8188",
	}); err != nil {
		t.Fatal(err)
	}
	if err := fix.svc.ReportNodeTelemetry(ctx, nodeCaller, NodeTelemetryReport{
		NodeUID:      fix.nodeUID,
		NodeName:     "k3s-5090-dev",
		AgentBootID:  "agent-boot-unmat",
		SampleID:     2,
		AgentHealthy: true,
		Cards: []CardTelemetrySample{
			{
				GPUUUID: "GPU-0000", GPUIndex: 0, TotalVRAMMiB: 32607, DriverOverheadMiB: 607,
				MeasuredFreeVRAMMiB: 14000, MeasuredUsedVRAMMiB: 18607, NVMLHealthy: true,
				AttributedProcesses: []AttributedGPUProcess{}, // Neither pod-unmat-moss nor pod-spoofed-comfy holds real VRAM!
			},
			{
				GPUUUID: "GPU-0001", GPUIndex: 1, TotalVRAMMiB: 32607, DriverOverheadMiB: 607,
				MeasuredFreeVRAMMiB: 32000, MeasuredUsedVRAMMiB: 607, NVMLHealthy: true,
			},
		},
	}); err != nil {
		t.Fatal(err)
	}

	snap, err := fix.svc.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	card0 := snap.Cards[0]
	// P1-H1 & P1-H4 proof:
	// - UnattributedExternalMiB MUST be 18000 (neither masked by unmaterialized 18432 MiB lease nor masked by spoofed 18000 MiB residency!)
	// - EvictableMiB MUST be 0 (spoofed residency has 0 node-proven VRAM)
	// - GrantableStandardMiB = max(0, 14000 - 2048 - 18432) = 0 MiB!
	if card0.UnattributedExternalMiB != 18000 || card0.EvictableMiB != 0 || card0.GrantableStandardMiB != 0 {
		t.Fatalf("P1-H1/P1-H4 accounting failure: got Unattributed=%d Evictable=%d Grantable=%d, want 18000 / 0 / 0",
			card0.UnattributedExternalMiB, card0.EvictableMiB, card0.GrantableStandardMiB)
	}

	// Any new 6144 MiB request on GPU-0000 MUST queue instead of double-committing VRAM!
	spCaller := CallerIdentity{AppID: "app-speech", Kind: CallerConsumer}
	blockedDec, err := fix.svc.Acquire(ctx, spCaller, AcquireRequest{
		IdempotencyKey:    "sp-blocked-by-unmat",
		ConsumerKind:      "indextts2",
		AttemptID:         "att-sp-unmat",
		CandidateGPUUUIDs: []string{"GPU-0000"},
	})
	if err != nil || blockedDec.State != "queued" {
		t.Fatalf("P1-H1 violation: expected queued when unmaterialized lease + external VRAM exceed capacity, got %+v (%v)", blockedDec, err)
	}

	verifyAllCardHashChains(t, ctx, fix.pool, fix.stationID)
}

func TestP1AuditFixes_SecurityAccountingAndLifecycle(t *testing.T) {
	fix := setupSchedulerDB(t)
	ctx := context.Background()
	blenderCaller := CallerIdentity{AppID: "app-blender", Kind: CallerConsumer}
	comfyCaller := CallerIdentity{AppID: "app-comfyui", Kind: CallerConsumer}
	vidCaller := CallerIdentity{AppID: "app-video", Kind: CallerConsumer}
	spCaller := CallerIdentity{AppID: "app-speech", Kind: CallerConsumer}
	nodeCaller := CallerIdentity{AppID: "node-5090-agent", Kind: CallerNodeAgent}

	// 1. P1-H4: SSRF callback validation & cross-app residency hijack rejection!
	for _, badCB := range []string{
		"http://169.254.169.254:80",
		"http://10.43.0.1:443",
		"https://evil.example.com:443",
		"http://comfy-worker.verdantflare-app.svc.cluster.local:9000/admin/steal",
	} {
		if err := fix.svc.ReportResidency(ctx, comfyCaller, ResidencyReportRequest{
			InstanceID:       "ssrf-test",
			NodeUID:          fix.nodeUID,
			PodUID:           "pod-ssrf",
			BootID:           "boot-ssrf",
			GPUEpoch:         1,
			ResidencySeq:     1,
			GPUUUID:          "GPU-0000",
			ResidencyState:   "idle_evictable",
			CallbackEndpoint: badCB,
		}); !errors.Is(err, ErrInvalidRequest) {
			t.Fatalf("expected ErrInvalidRequest for unsafe callback %q, got %v", badCB, err)
		}
	}

	if err := fix.svc.ReportResidency(ctx, blenderCaller, ResidencyReportRequest{
		InstanceID:     "shared-inst-id",
		NodeUID:        fix.nodeUID,
		PodUID:         "pod-blender-owner",
		BootID:         "boot-owner",
		GPUEpoch:       1,
		ResidencySeq:   1,
		GPUUUID:        "GPU-0000",
		ResidencyState: "human_protected",
		HumanProtected: true,
	}); err != nil {
		t.Fatal(err)
	}
	// Cross-app overwrite of blender's instance by app-comfyui MUST fail ErrForbiddenCaller!
	if err := fix.svc.ReportResidency(ctx, comfyCaller, ResidencyReportRequest{
		InstanceID:     "shared-inst-id",
		NodeUID:        fix.nodeUID,
		PodUID:         "pod-comfy-attacker",
		BootID:         "boot-attacker",
		GPUEpoch:       99,
		ResidencySeq:   99,
		GPUUUID:        "GPU-0000",
		ResidencyState: "idle_evictable",
	}); !errors.Is(err, ErrForbiddenCaller) {
		t.Fatalf("P1-H4 violation: expected ErrForbiddenCaller on cross-app residency hijack, got %v", err)
	}

	// 2. P1-M1 & P1-H4: RequestSHA256 verification and write-once lease PodUID binding!
	if _, err := fix.svc.Acquire(ctx, spCaller, AcquireRequest{
		IdempotencyKey:    "bad-sha-req",
		ConsumerKind:      "indextts2",
		AttemptID:         "att-bad-sha",
		RequestSHA256:     strings.Repeat("0", 64),
		CandidateGPUUUIDs: []string{"GPU-0001"},
	}); !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("P1-M1: expected ErrInvalidRequest on mismatched request_sha256, got %v", err)
	}

	lBound, err := fix.svc.Acquire(ctx, vidCaller, AcquireRequest{
		IdempotencyKey:    "vid-write-once",
		ConsumerKind:      "h3",
		AttemptID:         "att-wo-1",
		PodUID:            "pod-vid-1",
		ContainerID:       "cid-vid-1",
		CandidateGPUUUIDs: []string{"GPU-0001"},
	})
	if err != nil || lBound.State != "leased" {
		t.Fatalf("expected h3 leased on GPU-0001: %+v (%v)", lBound, err)
	}
	// Attempting to overwrite bound PodUID on same IdempotencyKey MUST fail ErrIdempotencyConflict!
	if _, err := fix.svc.Acquire(ctx, vidCaller, AcquireRequest{
		IdempotencyKey:    "vid-write-once",
		ConsumerKind:      "h3",
		AttemptID:         "att-wo-1",
		PodUID:            "pod-vid-spoofed",
		ContainerID:       "cid-vid-1",
		CandidateGPUUUIDs: []string{"GPU-0001"},
	}); !errors.Is(err, ErrIdempotencyConflict) {
		t.Fatalf("P1-H4/P1-M1: expected ErrIdempotencyConflict when mutating bound PodUID, got %v", err)
	}

	// 3. P1-H5: Downgrade rejects unapproved stage IDs, and Heartbeat rejects expired leases!
	fix.svc.CutoverCompleted = true
	down8G := int64(8192)
	if _, err := fix.svc.Downgrade(ctx, vidCaller, lBound.LeaseID, HeartbeatRequest{
		IdempotencyKey: "down-bad-stage",
		FencingToken:   lBound.FencingToken,
		DowngradeToMiB: &down8G,
		NewStageID:     "rogue_stage",
		WorkerSettled:  true,
	}); !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("P1-H5: expected ErrInvalidRequest on unapproved downgrade stage, got %v", err)
	}

	fix.advanceTime(100 * time.Second) // Past 90s TTL!
	if _, err := fix.svc.Heartbeat(ctx, vidCaller, lBound.LeaseID, HeartbeatRequest{
		IdempotencyKey: "hb-past-ttl",
		FencingToken:   lBound.FencingToken,
	}); !errors.Is(err, ErrFencingRevoked) && !errors.Is(err, ErrLeaseInactive) {
		t.Fatalf("P1-H5: expected ErrFencingRevoked/ErrLeaseInactive when heartbeating past expires_at, got %v", err)
	}
	_, _ = fix.svc.Release(ctx, vidCaller, lBound.LeaseID, ReleaseRequest{
		IdempotencyKey: "rel-lbound",
		FencingToken:   lBound.FencingToken,
		WorkerSettled:  true,
	})

	// 4. P1-H7 & P1-M4: Duplicate SampleID does NOT refresh telemetry freshness; unscheduled Pending K8s slots + ActiveGPUPods deduplication!
	if err := fix.svc.ReportNodeTelemetry(ctx, nodeCaller, NodeTelemetryReport{
		NodeUID:      fix.nodeUID,
		NodeName:     "k3s-5090-dev",
		AgentBootID:  "boot-m4",
		SampleID:     10,
		AgentHealthy: true,
		Cards: []CardTelemetrySample{
			{GPUUUID: "GPU-0000", GPUIndex: 0, TotalVRAMMiB: 32607, DriverOverheadMiB: 607, MeasuredFreeVRAMMiB: 25000, MeasuredUsedVRAMMiB: 7607, NVMLHealthy: true},
			{
				GPUUUID: "GPU-0001", GPUIndex: 1, TotalVRAMMiB: 32607, DriverOverheadMiB: 607,
				MeasuredFreeVRAMMiB: 32000, MeasuredUsedVRAMMiB: 607,
				AllocatedK8sSlots:          5,
				UnscheduledPendingK8sSlots: 6,
				ActiveGPUPods:              []ActiveGPUPodInfo{{PodUID: "pod-dedup-1", GPUSlots: 1}},
				NVMLHealthy:                true,
			},
		},
	}); err != nil {
		t.Fatal(err)
	}

	// Lease for pod-dedup-1 already counted in ActiveGPUPods -> effective slots = 5 + 6 + 0 = 11 < 12, so pod-dedup-1 is admitted!
	lDedup, err := fix.svc.Acquire(ctx, spCaller, AcquireRequest{
		IdempotencyKey:    "sp-dedup-1",
		ConsumerKind:      "indextts2",
		AttemptID:         "att-dedup-1",
		PodUID:            "pod-dedup-1",
		K8sSlotsRequired:  1,
		CandidateGPUUUIDs: []string{"GPU-0001"},
	})
	if err != nil || lDedup.State != "leased" {
		t.Fatalf("P1-M4: expected pod-dedup-1 admitted via ActiveGPUPods deduplication (11 < 12), got %+v (%v)", lDedup, err)
	}

	// A brand-new pod requesting K8sSlotsRequired=1 pushes 5 + 6 + 1 = 12 <= 12 (admitted), then one more hits 13 > 12 (queued k8s_gpu_quota_saturated)!
	lSlot12, err := fix.svc.Acquire(ctx, spCaller, AcquireRequest{
		IdempotencyKey:    "sp-slot-12",
		ConsumerKind:      "indextts2",
		AttemptID:         "att-slot-12",
		PodUID:            "pod-new-12",
		K8sSlotsRequired:  1,
		CandidateGPUUUIDs: []string{"GPU-0001"},
	})
	if err != nil || lSlot12.State != "leased" {
		t.Fatalf("expected 12th slot admitted, got %+v (%v)", lSlot12, err)
	}
	lSlot13, err := fix.svc.Acquire(ctx, spCaller, AcquireRequest{
		IdempotencyKey:    "sp-slot-13",
		ConsumerKind:      "indextts2",
		AttemptID:         "att-slot-13",
		PodUID:            "pod-new-13",
		K8sSlotsRequired:  1,
		CandidateGPUUUIDs: []string{"GPU-0001"},
	})
	if err != nil || lSlot13.State != "queued" || lSlot13.BlockingReason != "k8s_gpu_quota_saturated" {
		t.Fatalf("P1-M4: expected 13th slot blocked by unscheduled pending K8s quota saturation, got %+v (%v)", lSlot13, err)
	}
	for _, relDec := range []LeaseDecision{lSlot13, lSlot12, lDedup} {
		if _, err := fix.svc.Release(ctx, spCaller, relDec.LeaseID, ReleaseRequest{
			IdempotencyKey: "rel-" + relDec.LeaseID,
			FencingToken:   relDec.FencingToken,
			WorkerSettled:  true,
		}); err != nil {
			t.Fatal(err)
		}
	}

	// Advance 20s and replay duplicate SampleID=10 on same boot-m4 -> telemetry_sampled_at MUST NOT refresh!
	fix.advanceTime(20 * time.Second)
	if err := fix.svc.ReportNodeTelemetry(ctx, nodeCaller, NodeTelemetryReport{
		NodeUID:      fix.nodeUID,
		NodeName:     "k3s-5090-dev",
		AgentBootID:  "boot-m4",
		SampleID:     10,
		AgentHealthy: true,
		Cards: []CardTelemetrySample{
			{GPUUUID: "GPU-0001", GPUIndex: 1, TotalVRAMMiB: 32607, DriverOverheadMiB: 607, MeasuredFreeVRAMMiB: 32000, MeasuredUsedVRAMMiB: 607, NVMLHealthy: true},
		},
	}); err != nil {
		t.Fatal(err)
	}
	staleReplayDec, err := fix.svc.Acquire(ctx, spCaller, AcquireRequest{
		IdempotencyKey:    "sp-after-dup-sample",
		ConsumerKind:      "indextts2",
		AttemptID:         "att-dup-sample",
		CandidateGPUUUIDs: []string{"GPU-0001"},
	})
	if err != nil || staleReplayDec.State != "queued" || staleReplayDec.BlockingReason != "telemetry_stale" {
		t.Fatalf("P1-H7: duplicate SampleID must NOT refresh telemetry freshness, got %+v (%v)", staleReplayDec, err)
	}

	// Zero-card unhealthy report transitions all node cards to reconciling!
	if err := fix.svc.ReportNodeTelemetry(ctx, nodeCaller, NodeTelemetryReport{
		NodeUID:      fix.nodeUID,
		NodeName:     "k3s-5090-dev",
		AgentBootID:  "boot-m4",
		SampleID:     11,
		AgentHealthy: false,
		ErrorCode:    "NVML_UNAVAILABLE",
		Cards:        nil,
	}); err != nil {
		t.Fatal(err)
	}
	snapAfterFail, err := fix.svc.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range snapAfterFail.Cards {
		if c.State != "reconciling" {
			t.Fatalf("P1-H7: expected card %s in reconciling on zero-card unhealthy report, got %+v", c.GPUUUID, c)
		}
	}

	verifyAllCardHashChains(t, ctx, fix.pool, fix.stationID)
}

func TestCodexRound2Fixes_QueueAutoPromotionInstanceDirTokenResolverAndFailClosed(t *testing.T) {
	fix := setupSchedulerDB(t)
	ctx := context.Background()
	vidCaller := CallerIdentity{AppID: "app-video", Kind: CallerConsumer}
	comfyCaller := CallerIdentity{AppID: "app-comfyui", Kind: CallerConsumer}
	blenderCaller := CallerIdentity{AppID: "app-blender", Kind: CallerConsumer}

	// 1. Callback allowlist strictly rejects .verdantflare-station.svc.cluster.local, .internal, unapproved port :9000, and non-root paths!
	for _, disallowed := range []string{
		"http://station-runtime.verdantflare-station.svc.cluster.local:5053",
		"http://metadata.internal:80",
		"http://comfy-worker.verdantflare-app.svc.cluster.local:9000",
		"http://comfy-worker.verdantflare-app.svc.cluster.local:8188/admin/steal",
	} {
		if _, err := validateInternalCallbackEndpoint(disallowed, false); !errors.Is(err, ErrInvalidRequest) {
			t.Fatalf("expected validateInternalCallbackEndpoint(%q) to fail ErrInvalidRequest, got %v", disallowed, err)
		}
	}

	// 2. Queued lease auto-promotes immediately on Release WITHOUT client Acquire replay!
	holderDec, err := fix.svc.Acquire(ctx, vidCaller, AcquireRequest{
		IdempotencyKey:    "r2-holder-1",
		ConsumerKind:      "h3",
		AttemptID:         "att-r2-h1",
		CandidateGPUUUIDs: []string{"GPU-0000"},
	})
	if err != nil || holderDec.State != "leased" {
		t.Fatalf("expected holder leased: %+v (%v)", holderDec, err)
	}
	queuedDec, err := fix.svc.Acquire(ctx, vidCaller, AcquireRequest{
		IdempotencyKey:    "r2-waiter-1",
		ConsumerKind:      "h3",
		AttemptID:         "att-r2-w1",
		CandidateGPUUUIDs: []string{"GPU-0000"},
	})
	if err != nil || queuedDec.State != "queued" {
		t.Fatalf("expected waiter queued: %+v (%v)", queuedDec, err)
	}
	if _, err := fix.svc.Release(ctx, vidCaller, holderDec.LeaseID, ReleaseRequest{
		IdempotencyKey: "rel-r2-holder-1",
		FencingToken:   holderDec.FencingToken,
		WorkerSettled:  true,
	}); err != nil {
		t.Fatalf("Release holder failed: %v", err)
	}
	// Poll ONLY GetLease (zero Acquire replay!) -> waiter MUST already be promoted to 'leased'!
	promotedViaGet, err := fix.svc.GetLease(ctx, vidCaller, queuedDec.LeaseID)
	if err != nil || promotedViaGet.State != "leased" || promotedViaGet.GPUUUID != "GPU-0000" {
		t.Fatalf("expected queued lease auto-promoted to leased without Acquire replay, got %+v (%v)", promotedViaGet, err)
	}
	if _, err := fix.svc.Release(ctx, vidCaller, promotedViaGet.LeaseID, ReleaseRequest{
		IdempotencyKey: "rel-r2-waiter-1",
		FencingToken:   promotedViaGet.FencingToken,
		WorkerSettled:  true,
	}); err != nil {
		t.Fatal(err)
	}

	// 3. PGInstanceDirectory (active vs stopped app_instance_starts & runtime_instance_executions) + KubeInstanceTokenResolver + HTTPResidencyEvictor!
	seedUserID := uuid.NewString()
	seedOrgID := uuid.NewString()
	seedProjectID := uuid.NewString()
	profHex := strings.Repeat("a", 64)
	profBytes := bytes.Repeat([]byte{0xaa}, 32)
	reqHashA := bytes.Repeat([]byte{0xbb}, 32)
	reqHashB := bytes.Repeat([]byte{0xcc}, 32)

	if _, err := fix.pool.Exec(ctx, `
		INSERT INTO station.users(user_id, username, password_hash)
		VALUES ($1, 'gpusched_owner', 'argon2id$test')
	`, seedUserID); err != nil {
		t.Fatalf("seed users: %v", err)
	}
	if _, err := fix.pool.Exec(ctx, `
		INSERT INTO station.organizations(organization_id, name)
		VALUES ($1, 'GPU Scheduler Test Org')
	`, seedOrgID); err != nil {
		t.Fatalf("seed organizations: %v", err)
	}

	comfyInstID := uuid.NewString()
	comfyInstallID := uuid.NewString()
	comfyStartOpID := uuid.NewString()
	comfyPodUID := uuid.NewString()
	comfySecretUID := uuid.NewString()
	comfyWorkerTok := "comfy-per-instance-worker-token-0123456789"

	if _, err := fix.pool.Exec(ctx, `
		INSERT INTO station.app_installations(
			installation_id, created_by, organization_id, station_id, app_id, app_version, state
		) VALUES ($1, $2, $3, $4, 'comfyui', '0.2.0', 'installed')
	`, comfyInstallID, seedUserID, seedOrgID, fix.stationID); err != nil {
		t.Fatalf("seed app_installations: %v", err)
	}
	if _, err := fix.pool.Exec(ctx, `
		INSERT INTO station.app_instance_workspaces(
			instance_id, station_id, organization_id, user_id, installation_id,
			profile_hash, namespace, pvc_name, pvc_uid, pv_name, pv_uid, binding
		) VALUES ($1, $2, $3, $4, $5, $6, 'verdantflare-app', 'pvc-comfy-1', $7, 'pv-comfy-1', $8, '{}'::jsonb)
	`, comfyInstID, fix.stationID, seedOrgID, seedUserID, comfyInstallID, profHex, uuid.NewString(), uuid.NewString()); err != nil {
		t.Fatalf("seed app_instance_workspaces: %v", err)
	}
	comfyObsJSON, _ := json.Marshal(map[string]any{
		"pod_uid":     comfyPodUID,
		"secret_uid":  comfySecretUID,
		"service_uid": uuid.NewString(),
		"pod_ready":   true,
	})
	if _, err := fix.pool.Exec(ctx, `
		INSERT INTO station.app_instance_starts(
			operation_id, instance_id, generation, installation_revision, profile_hash, input, phase, observation
		) VALUES ($1, $2, 1, 1, $3, '{}'::jsonb, 'running', $4::jsonb)
	`, comfyStartOpID, comfyInstID, profHex, string(comfyObsJSON)); err != nil {
		t.Fatalf("seed app_instance_starts: %v", err)
	}

	blenderInstID := uuid.NewString()
	blenderPodUID := uuid.NewString()
	blenderSecretUID := uuid.NewString()
	blenderWorkerTok := "blender-per-instance-worker-token-0123456789"
	blenderCtrlEndpoint := "http://blender-worker.verdantflare-app.svc.cluster.local:48084"
	if _, err := fix.pool.Exec(ctx, `
		INSERT INTO station.runtime_instance_workspaces(
			instance_id, station_id, organization_id, project_id, create_operation_id,
			profile_id, profile_hash, namespace, pool_name, pvc_name, pvc_uid, pv_name, pv_uid, node_name, reserved_bytes
		) VALUES ($1, $2, $3, $4, $5, 'blender-5090-v1', $6, 'verdantflare-app', 'nvme', 'pvc-blender-1', $7, 'pv-blender-1', $8, 'k3s-5090-dev', 1073741824)
	`, blenderInstID, fix.stationID, seedOrgID, seedProjectID, uuid.NewString(), profBytes, uuid.NewString(), uuid.NewString()); err != nil {
		t.Fatalf("seed runtime_instance_workspaces: %v", err)
	}
	blenderStateJSON, _ := json.Marshal(map[string]any{
		"secret_uid": blenderSecretUID,
		"worker": map[string]any{
			"pod_uid":          blenderPodUID,
			"service_uid":      uuid.NewString(),
			"control_endpoint": blenderCtrlEndpoint,
		},
	})
	if _, err := fix.pool.Exec(ctx, `
		INSERT INTO station.runtime_instance_executions(
			operation_id, instance_id, station_id, organization_id, user_id, project_id,
			action, request_hash, profile_hash, input, status, phase, state
		) VALUES ($1, $2, $3, $4, $5, $6, 'start', $7, $8, '{}'::jsonb, 'succeeded', 'running', $9::jsonb)
	`, uuid.NewString(), blenderInstID, fix.stationID, seedOrgID, seedUserID, seedProjectID, reqHashA, profBytes, string(blenderStateJSON)); err != nil {
		t.Fatalf("seed runtime_instance_executions(start): %v", err)
	}

	pgDir := &PGInstanceDirectory{
		Pool: fix.pool,
		AppPorts: map[string]int{
			"comfyui": 8188,
			"blender": 48084,
		},
	}
	kubeFake := fake.NewSimpleClientset(
		&corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: "verdantflare-app",
				Name:      "appi-" + strings.ReplaceAll(comfyInstID, "-", ""),
				UID:       types.UID(comfySecretUID),
			},
			Data: map[string][]byte{"worker-token": []byte(comfyWorkerTok)},
		},
		&corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: "verdantflare-app",
				Name:      "blender-" + strings.ReplaceAll(blenderInstID, "-", ""),
				UID:       types.UID(blenderSecretUID),
			},
			Data: map[string][]byte{"worker-token": []byte(blenderWorkerTok)},
		},
	)
	resolver := KubeInstanceTokenResolver{
		Directory: pgDir,
		StationID: fix.stationID,
		Kube:      kubeFake,
	}
	gotComfyTok, err := resolver.ResolveToken(ctx, comfyInstID)
	if err != nil || gotComfyTok != comfyWorkerTok {
		t.Fatalf("ResolveToken(comfy): got %q (%v), want %q", gotComfyTok, err, comfyWorkerTok)
	}
	gotBlenderTok, err := resolver.ResolveToken(ctx, blenderInstID)
	if err != nil || gotBlenderTok != blenderWorkerTok {
		t.Fatalf("ResolveToken(blender): got %q (%v), want %q", gotBlenderTok, err, blenderWorkerTok)
	}

	// Verify HTTPResidencyEvictor authenticates to worker callback using dynamically resolved per-instance worker-token!
	var seenAuth string
	workerSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seenAuth = r.Header.Get("Authorization")
		if seenAuth != "Bearer "+comfyWorkerTok {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		_ = json.NewEncoder(w).Encode(EvictVRAMResult{
			Settled:                  true,
			ResidencyState:           "zero_vram_standby",
			ResidencySeq:             2,
			GPUEpoch:                 1,
			SupervisorPID:            4200,
			SupervisorStarttimeTicks: 88420,
		})
	}))
	defer workerSrv.Close()

	realEvictor := HTTPResidencyEvictor{
		BearerToken:            "fallback-unused-token",
		TokenResolver:          resolver.ResolveToken,
		AllowLoopbackCallbacks: true,
	}
	evOut, err := realEvictor.EvictVRAM(ctx, EvictVRAMCommand{
		CallbackEndpoint:     workerSrv.URL,
		InstanceID:           comfyInstID,
		ExpectedBootID:       "boot-c1",
		ExpectedResidencySeq: 1,
		ExpectedGPUEpoch:     1,
	})
	if err != nil || !evOut.Settled || seenAuth != "Bearer "+comfyWorkerTok {
		t.Fatalf("HTTPResidencyEvictor dynamic token resolution failed: out=%+v seenAuth=%q err=%v", evOut, seenAuth, err)
	}

	// Wire PGInstanceDirectory onto Service: app-comfyui caller matches canonical 'comfyui', wrong PodUID is rejected,
	// and stopped instances are immediately rejected!
	fix.svc.InstanceDir = pgDir
	defer func() { fix.svc.InstanceDir = nil }()

	if err := fix.svc.ReportResidency(ctx, comfyCaller, ResidencyReportRequest{
		InstanceID:     comfyInstID,
		NodeUID:        fix.nodeUID,
		PodUID:         "wrong-pod-uid",
		BootID:         "boot-c1",
		GPUEpoch:       1,
		ResidencySeq:   1,
		GPUUUID:        "GPU-0000",
		ResidencyState: "idle_evictable",
	}); !errors.Is(err, ErrForbiddenCaller) {
		t.Fatalf("expected ErrForbiddenCaller on mismatched PodUID, got %v", err)
	}
	if err := fix.svc.ReportResidency(ctx, comfyCaller, ResidencyReportRequest{
		InstanceID:     comfyInstID,
		NodeUID:        fix.nodeUID,
		PodUID:         comfyPodUID,
		BootID:         "boot-c1",
		GPUEpoch:       1,
		ResidencySeq:   1,
		GPUUUID:        "GPU-0000",
		ResidencyState: "idle_evictable",
	}); err != nil {
		t.Fatalf("expected ReportResidency(app-comfyui -> comfyui) to succeed with canonical endpoint overwrite: %v", err)
	}
	var storedCB string
	if err := fix.pool.QueryRow(ctx, `SELECT callback_endpoint FROM station.gpu_instance_residency WHERE station_id=$1 AND instance_id=$2`, fix.stationID, comfyInstID).Scan(&storedCB); err != nil {
		t.Fatal(err)
	}
	wantCB := "http://appr-" + strings.ReplaceAll(comfyStartOpID, "-", "") + ".verdantflare-app.svc.cluster.local:8188"
	if storedCB != wantCB {
		t.Fatalf("canonical callback endpoint mismatch: got %q, want %q", storedCB, wantCB)
	}

	// Mark ComfyUI stopped and Blender stopped -> both LookupInstance and ReportResidency MUST fail ErrForbiddenCaller!
	if _, err := fix.pool.Exec(ctx, `UPDATE station.app_instance_starts SET phase = 'stopped' WHERE operation_id = $1`, comfyStartOpID); err != nil {
		t.Fatal(err)
	}
	if _, err := fix.pool.Exec(ctx, `
		INSERT INTO station.runtime_instance_executions(
			operation_id, instance_id, station_id, organization_id, user_id, project_id,
			action, request_hash, profile_hash, input, status, phase, state, created_at
		) VALUES ($1, $2, $3, $4, $5, $6, 'stop', $7, $8, '{}'::jsonb, 'succeeded', 'stopped', '{}'::jsonb, now() + interval '1 second')
	`, uuid.NewString(), blenderInstID, fix.stationID, seedOrgID, seedUserID, seedProjectID, reqHashB, profBytes); err != nil {
		t.Fatal(err)
	}
	if err := fix.svc.ReportResidency(ctx, comfyCaller, ResidencyReportRequest{
		InstanceID:     comfyInstID,
		NodeUID:        fix.nodeUID,
		PodUID:         comfyPodUID,
		BootID:         "boot-c1",
		GPUEpoch:       1,
		ResidencySeq:   2,
		GPUUUID:        "GPU-0000",
		ResidencyState: "idle_evictable",
	}); !errors.Is(err, ErrForbiddenCaller) {
		t.Fatalf("expected stopped ComfyUI instance to be rejected, got %v", err)
	}
	if err := fix.svc.ReportResidency(ctx, blenderCaller, ResidencyReportRequest{
		InstanceID:     blenderInstID,
		NodeUID:        fix.nodeUID,
		PodUID:         blenderPodUID,
		BootID:         "boot-b1",
		GPUEpoch:       1,
		ResidencySeq:   1,
		GPUUUID:        "GPU-0000",
		ResidencyState: "idle_evictable",
	}); !errors.Is(err, ErrForbiddenCaller) {
		t.Fatalf("expected stopped Blender instance to be rejected, got %v", err)
	}
	fix.svc.InstanceDir = nil

	// 4. HTTPNodeVerifier wire contract: bare WorkerSettled=true WITHOUT ReclaimProof fails in epoch_unload mode,
	// and valid ReclaimProof + SupervisorPID forwards Mode="epoch_unload", EpochFenceConfirmed=true, SupervisorPID=5000, GPUEpoch=1
	// over HTTP, succeeding ONLY after node-agent confirms all transitive descendants exited!
	// (Transitive /proc PPID descendant walk + EpochTombstone integrity are verified directly in internal/nodegpu/nodegpu_test.go.)
	cidHex := strings.Repeat("d", 64)
	agentToken := strings.Repeat("g", 40)
	var (
		descendantAliveMu sync.Mutex
		descendantAlive   = true
		lastVerifyReq     struct {
			PodUID                   string `json:"pod_uid"`
			ContainerID              string `json:"container_id"`
			GPUEpoch                 int64  `json:"gpu_epoch"`
			SupervisorPID            int    `json:"supervisor_pid"`
			SupervisorStarttimeTicks uint64 `json:"supervisor_starttime_ticks"`
			Mode                     string `json:"mode"`
			EpochFenceConfirmed      bool   `json:"epoch_fence_confirmed"`
		}
	)
	agentSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer "+agentToken {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		if r.Method != http.MethodPost || r.URL.Path != "/internal/v1/node-gpu/verify-terminated" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		descendantAliveMu.Lock()
		defer descendantAliveMu.Unlock()
		if err := json.NewDecoder(r.Body).Decode(&lastVerifyReq); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		if lastVerifyReq.Mode == "epoch_unload" {
			if !lastVerifyReq.EpochFenceConfirmed || lastVerifyReq.SupervisorPID <= 0 || lastVerifyReq.SupervisorStarttimeTicks == 0 || lastVerifyReq.GPUEpoch <= 0 {
				_ = json.NewEncoder(w).Encode(VerifyTerminatedResult{
					Verified:  false,
					ErrorCode: "EPOCH_FENCE_UNCONFIRMED",
				})
				return
			}
			if descendantAlive {
				_ = json.NewEncoder(w).Encode(VerifyTerminatedResult{
					Verified:      false,
					SurvivingPIDs: []int{5050},
					ErrorCode:     "SUPERVISOR_DESCENDANT_PIDS_ALIVE",
				})
				return
			}
		} else if lastVerifyReq.Mode == "container_exited" {
			// Resident S3 container is STILL running!
			_ = json.NewEncoder(w).Encode(VerifyTerminatedResult{
				Verified:  false,
				ErrorCode: "CONTAINER_STILL_RUNNING",
			})
			return
		}
		_ = json.NewEncoder(w).Encode(VerifyTerminatedResult{Verified: true, MeasuredFreeVRAMMiB: 31800})
	}))
	defer agentSrv.Close()

	origVerifier := fix.svc.Verifier
	fix.svc.Verifier = HTTPNodeVerifier{
		DefaultURL:  agentSrv.URL,
		BearerToken: agentToken,
	}

	lEpoch, err := fix.svc.Acquire(ctx, comfyCaller, AcquireRequest{
		IdempotencyKey:    "comfy-epoch-real-1",
		ConsumerKind:      "comfyui_wake",
		TaskID:            "wake-epoch-real-1",
		AttemptID:         "att-epoch-real-1",
		PodUID:            "pod-epoch-real",
		ContainerID:       cidHex,
		BootID:            "boot-ep-1",
		CandidateGPUUUIDs: []string{"GPU-0001"},
	})
	if err != nil || lEpoch.State != "leased" {
		t.Fatalf("expected lEpoch leased: %+v (%v)", lEpoch, err)
	}

	// 4a) Bare WorkerSettled=true without ReclaimProof in epoch_unload mode MUST be rejected BEFORE calling node-agent!
	if _, err := fix.svc.Release(ctx, comfyCaller, lEpoch.LeaseID, ReleaseRequest{
		IdempotencyKey: "rel-epoch-missing-proof",
		FencingToken:   lEpoch.FencingToken,
		WorkerSettled:  true,
		GPUEpoch:       1,
		ReclaimMode:    "epoch_unload",
	}); !errors.Is(err, ErrReclaimQuarantined) {
		t.Fatalf("expected ErrReclaimQuarantined when ReclaimProof is missing on epoch_unload, got %v", err)
	}

	validProof := map[string]any{
		"sealed_pod_uid":             "pod-epoch-real",
		"sealed_container_id":        cidHex,
		"sealed_boot_id":             "boot-ep-1",
		"sealed_gpu_epoch":           int64(1),
		"supervisor_pid":             5000,
		"supervisor_starttime_ticks": int64(99500),
		"revoked_lease_id":           lEpoch.LeaseID,
		"consumed_fencing_token":     lEpoch.FencingToken,
		"reclaimed":                  true,
	}
	// 4b) Even with valid ReclaimProof, while descendant is still alive under supervisor 5000, HTTPNodeVerifier rejects!
	if _, err := fix.svc.Release(ctx, comfyCaller, lEpoch.LeaseID, ReleaseRequest{
		IdempotencyKey:           "rel-epoch-child-alive",
		FencingToken:             lEpoch.FencingToken,
		WorkerSettled:            true,
		GPUEpoch:                 1,
		SupervisorPID:            5000,
		SupervisorStarttimeTicks: 99500,
		ReclaimMode:              "epoch_unload",
		ReclaimProof:             validProof,
	}); !errors.Is(err, ErrReclaimQuarantined) {
		t.Fatalf("expected ErrReclaimQuarantined while cleared-environ descendant PID 5050 is alive, got %v", err)
	}
	descendantAliveMu.Lock()
	gotSupervisorPID := lastVerifyReq.SupervisorPID
	gotSupervisorTicks := lastVerifyReq.SupervisorStarttimeTicks
	gotFenceConfirmed := lastVerifyReq.EpochFenceConfirmed
	descendantAlive = false
	descendantAliveMu.Unlock()
	if gotSupervisorPID != 5000 || gotSupervisorTicks != 99500 || !gotFenceConfirmed {
		t.Fatalf("expected HTTPNodeVerifier to send SupervisorPID=5000, SupervisorStarttimeTicks=99500 and EpochFenceConfirmed=true, got %+v", lastVerifyReq)
	}

	// 4c) Once descendants exit, Release succeeds!
	relOK, err := fix.svc.Release(ctx, comfyCaller, lEpoch.LeaseID, ReleaseRequest{
		IdempotencyKey:           "rel-epoch-verified",
		FencingToken:             lEpoch.FencingToken,
		WorkerSettled:            true,
		GPUEpoch:                 1,
		SupervisorPID:            5000,
		SupervisorStarttimeTicks: 99500,
		ReclaimMode:              "epoch_unload",
		ReclaimProof:             validProof,
	})
	if err != nil || relOK.State != "released" {
		t.Fatalf("expected epoch_unload Release verified once descendants exit, got %+v (%v)", relOK, err)
	}

	// 4d) Codex Round-3 regression: Resident S3 container (CONTAINER_RUNNING) whose single /release failed or TTL expired:
	//     periodic ReconcileRestart(ctx, RestartObservation{}) with EMPTY RestartObservation MUST:
	//     - extract instance_id (NOT HolderTaskID "wake-...")
	//     - resolve callback URL + dynamic worker token via InstanceDirectory + KubeInstanceTokenResolver
	//     - call POST /internal/v1/gpu-residency/revoke-lease with InstanceID=s3InstID
	//     - verify epoch_unload via HTTPNodeVerifier (NOT container_exited!)
	//     - transition lease to expired & promote queued waiter
	//     - return 200 OK idempotently when worker's pending-release journal subsequently replays POST /release!
	var (
		s3InstID    = uuid.NewString()
		s3PodUID    = uuid.NewString()
		s3WorkerTok = "worker-s3-reconcile-tok-" + strings.Repeat("r", 16)
		seenRevCmd  RevokeLeaseCommand
		seenRevAuth string
	)
	s3WorkerSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seenRevAuth = r.Header.Get("Authorization")
		if seenRevAuth != "Bearer "+s3WorkerTok {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		if r.URL.Path != "/internal/v1/gpu-residency/revoke-lease" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		if err := json.NewDecoder(r.Body).Decode(&seenRevCmd); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		_ = json.NewEncoder(w).Encode(RevokeLeaseResult{
			Revoked:                  true,
			Settled:                  true,
			Reclaimed:                true,
			GPUEpoch:                 2,
			SealedGPUEpoch:           2,
			SupervisorPID:            1,
			SupervisorStarttimeTicks: 77701,
			ReclaimProof: map[string]any{
				"sealed_pod_uid":             s3PodUID,
				"sealed_container_id":        cidHex,
				"sealed_boot_id":             "boot-s3-rec",
				"sealed_gpu_epoch":           int64(2),
				"supervisor_pid":             1,
				"supervisor_starttime_ticks": int64(77701),
				"revoked_lease_id":           seenRevCmd.LeaseID,
				"consumed_fencing_token":     seenRevCmd.FencingToken,
				"reclaimed":                  true,
			},
		})
	}))
	defer s3WorkerSrv.Close()

	staticS3Dir := staticTestInstanceDirectory{
		byID: map[string]RegisteredInstance{
			s3InstID: {
				InstanceID:       s3InstID,
				AppID:            "comfyui",
				PodUID:           s3PodUID,
				CallbackEndpoint: s3WorkerSrv.URL,
			},
		},
	}
	origEvictor := fix.svc.Evictor
	fix.svc.InstanceDir = staticS3Dir
	fix.svc.Evictor = HTTPResidencyEvictor{
		BearerToken: "unused-fallback",
		TokenResolver: func(_ context.Context, instID string) (string, error) {
			if instID == s3InstID {
				return s3WorkerTok, nil
			}
			return "", fmt.Errorf("unexpected instance_id %q", instID)
		},
		AllowLoopbackCallbacks: true,
	}

	lS3Stuck, err := fix.svc.Acquire(ctx, comfyCaller, AcquireRequest{
		IdempotencyKey:    "comfy-s3-stuck-1",
		ConsumerKind:      "comfyui_wake",
		InstanceID:        s3InstID,
		TaskID:            "wake-uuid-not-instance-id",
		AttemptID:         "att-s3-stuck-1",
		PodUID:            s3PodUID,
		ContainerID:       cidHex,
		BootID:            "boot-s3-rec",
		CandidateGPUUUIDs: []string{"GPU-0001"},
	})
	if err != nil || lS3Stuck.State != "leased" {
		t.Fatalf("expected lS3Stuck leased: %+v (%v)", lS3Stuck, err)
	}
	queuedBehindS3, err := fix.svc.Acquire(ctx, vidCaller, AcquireRequest{
		IdempotencyKey:    "h3-behind-s3-stuck",
		ConsumerKind:      "h3",
		AttemptID:         "att-h3-behind-s3",
		CandidateGPUUUIDs: []string{"GPU-0001"},
		TTLSeconds:        600,
	})
	if err != nil || queuedBehindS3.State != "queued" {
		t.Fatalf("expected h3 queued behind lS3Stuck, got %+v (%v)", queuedBehindS3, err)
	}

	// Expire lS3Stuck and run periodic ReconcileRestart with completely EMPTY RestartObservation{}!
	fix.advanceTime(120 * time.Second)
	if err := fix.svc.ReportNodeTelemetry(ctx, CallerIdentity{AppID: "station-gpu-agent", Kind: CallerNodeAgent}, NodeTelemetryReport{
		NodeUID:      fix.nodeUID,
		NodeName:     "k3s-5090-dev",
		SampleID:     99,
		AgentHealthy: true,
		Cards: []CardTelemetrySample{
			{GPUUUID: "GPU-0000", GPUIndex: 0, TotalVRAMMiB: 32607, DriverOverheadMiB: 607, MeasuredFreeVRAMMiB: 32000, MeasuredUsedVRAMMiB: 607, NVMLHealthy: true},
			{GPUUUID: "GPU-0001", GPUIndex: 1, TotalVRAMMiB: 32607, DriverOverheadMiB: 607, MeasuredFreeVRAMMiB: 32000, MeasuredUsedVRAMMiB: 607, NVMLHealthy: true},
		},
	}); err != nil {
		t.Fatalf("refresh telemetry before ReconcileRestart: %v", err)
	}
	if err := fix.svc.ReconcileRestart(ctx, RestartObservation{}); err != nil {
		t.Fatalf("ReconcileRestart(empty observation) failed: %v", err)
	}
	if seenRevCmd.InstanceID != s3InstID {
		t.Fatalf("expected RevokeLeaseCommand.InstanceID=%q, got %q", s3InstID, seenRevCmd.InstanceID)
	}
	descendantAliveMu.Lock()
	gotRecMode := lastVerifyReq.Mode
	gotRecPID := lastVerifyReq.SupervisorPID
	gotRecTicks := lastVerifyReq.SupervisorStarttimeTicks
	descendantAliveMu.Unlock()
	if gotRecMode != "epoch_unload" || gotRecPID != 1 || gotRecTicks != 77701 {
		t.Fatalf("expected ReconcileRestart to verify via epoch_unload(pid=1, ticks=77701), got mode=%q pid=%d ticks=%d", gotRecMode, gotRecPID, gotRecTicks)
	}

	afterS3Rec, err := fix.svc.GetLease(ctx, comfyCaller, lS3Stuck.LeaseID)
	if err != nil || afterS3Rec.State != "expired" {
		t.Fatalf("expected lS3Stuck transitioned to expired, got %+v (%v)", afterS3Rec, err)
	}
	promotedAfterS3, err := fix.svc.GetLease(ctx, vidCaller, queuedBehindS3.LeaseID)
	if err != nil || promotedAfterS3.State != "leased" {
		t.Fatalf("expected queued h3 promoted to leased after S3 reconcile recovery, got %+v (%v)", promotedAfterS3, err)
	}

	// Subsequent replay of POST /release from worker's durable pending_releases journal MUST succeed idempotently (no ErrVersionConflict)!
	replayDec, err := fix.svc.Release(ctx, comfyCaller, lS3Stuck.LeaseID, ReleaseRequest{
		IdempotencyKey:           "comfyui-reap:" + lS3Stuck.LeaseID + ":epoch:2",
		FencingToken:             lS3Stuck.FencingToken,
		WorkerSettled:            true,
		GPUEpoch:                 2,
		SupervisorPID:            1,
		SupervisorStarttimeTicks: 77701,
		ReclaimMode:              "epoch_unload",
	})
	if err != nil || replayDec.State != "expired" {
		t.Fatalf("expected idempotent Release on reconciled-expired lease to return expired without error, got %+v (%v)", replayDec, err)
	}
	fix.svc.Verifier = origVerifier
	fix.svc.InstanceDir = nil
	fix.svc.Evictor = origEvictor
	if relPromoted, err := fix.svc.Release(ctx, vidCaller, promotedAfterS3.LeaseID, ReleaseRequest{
		IdempotencyKey: "rel-promoted-h3",
		FencingToken:   promotedAfterS3.FencingToken,
		WorkerSettled:  true,
	}); err != nil || relPromoted.State != "released" {
		t.Fatalf("expected promotedAfterS3 released cleanly, got %+v (%v)", relPromoted, err)
	}

	// 5. Nil Verifier fail-closed check: Release(wasGranted), Downgrade, and ReconcileRestart MUST transition to reconciling (NEVER expired/released)!
	lNilVer, err := fix.svc.Acquire(ctx, vidCaller, AcquireRequest{
		IdempotencyKey:    "nil-verifier-lease",
		ConsumerKind:      "rife",
		AttemptID:         "att-nil-ver",
		PodUID:            "pod-nil-ver",
		CandidateGPUUUIDs: []string{"GPU-0001"},
	})
	if err != nil || lNilVer.State != "leased" {
		t.Fatal(err)
	}
	fix.svc.Verifier = nil
	if _, err := fix.svc.Release(ctx, vidCaller, lNilVer.LeaseID, ReleaseRequest{
		IdempotencyKey: "rel-while-nil-verifier",
		FencingToken:   lNilVer.FencingToken,
		WorkerSettled:  true,
	}); !errors.Is(err, ErrNodeStateUnknown) {
		t.Fatalf("expected ErrNodeStateUnknown when s.Verifier == nil on Release, got %v", err)
	}
	fix.advanceTime(120 * time.Second)
	if err := fix.svc.ReconcileRestart(ctx, RestartObservation{
		NodeUID:     fix.nodeUID,
		LivePodUIDs: map[string]bool{},
		RunNonce:    "nil-verifier-reconcile",
	}); err != nil {
		t.Fatal(err)
	}
	afterNilRec, err := fix.svc.GetLease(ctx, vidCaller, lNilVer.LeaseID)
	if err != nil || afterNilRec.State != "reconciling" {
		t.Fatalf("expected lease to remain fail-closed in reconciling when s.Verifier == nil, got %+v (%v)", afterNilRec, err)
	}
	fix.svc.Verifier = origVerifier

	// 6. Strict DB role isolation check: superuser fails strict mode, SET LOCAL ROLE vf_gpu_runtime passes strict mode!
	if err := fix.svc.VerifyDatabasePrivilegeIsolation(ctx, true); err == nil {
		t.Fatal("expected superuser connection to fail VerifyDatabasePrivilegeIsolation(ctx, true)")
	}
	txRole, err := fix.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer rollbackTx(txRole)
	if _, err := txRole.Exec(ctx, `SET LOCAL ROLE vf_gpu_runtime`); err != nil {
		t.Fatal(err)
	}
	if err := verifyDBPrivilegeIsolation(ctx, txRole, true); err != nil {
		t.Fatalf("expected SET LOCAL ROLE vf_gpu_runtime to pass verifyDBPrivilegeIsolation(true): %v", err)
	}

	verifyAllCardHashChains(t, ctx, fix.pool, fix.stationID)
}

func TestP1bColdStartBindTargetWarmProofAndAbortUnstarted(t *testing.T) {
	fix := setupSchedulerDB(t)
	ctx := context.Background()

	regJSON := `[
		{"app_id":"image-mcp","kind":"consumer","token_env":"TOK_IMG"},
		{"app_id":"video-mcp","kind":"consumer","token_env":"TOK_VID"}
	]`
	imgTok := "img-scheduler-secret-token-0123456789abcdef"
	vidTok := "vid-scheduler-secret-token-0123456789abcdef"
	reg, err := LoadCallerRegistry(strings.NewReader(regJSON), func(k string) string {
		switch k {
		case "TOK_IMG":
			return imgTok
		case "TOK_VID":
			return vidTok
		default:
			return ""
		}
	})
	if err != nil {
		t.Fatalf("LoadCallerRegistry: %v", err)
	}
	fix.svc.Callers = reg
	srv := httptest.NewServer(Handler{Service: fix.svc})
	defer srv.Close()

	doReq := func(tok, method, path string, body any) (int, map[string]any) {
		t.Helper()
		var rdr *bytes.Reader
		if body != nil {
			raw, mErr := json.Marshal(body)
			if mErr != nil {
				t.Fatal(mErr)
			}
			rdr = bytes.NewReader(raw)
		} else {
			rdr = bytes.NewReader(nil)
		}
		req, rErr := http.NewRequestWithContext(ctx, method, srv.URL+path, rdr)
		if rErr != nil {
			t.Fatal(rErr)
		}
		req.Header.Set("Authorization", "Bearer "+tok)
		req.Header.Set("Content-Type", "application/json")
		resp, dErr := http.DefaultClient.Do(req)
		if dErr != nil {
			t.Fatal(dErr)
		}
		defer resp.Body.Close()
		var out map[string]any
		_ = json.NewDecoder(resp.Body).Decode(&out)
		return resp.StatusCode, out
	}

	// 1. Cold-start Acquire on GPU-0000 + queued waiter on GPU-0000, then Kubelet schedules cold-start pod on GPU-0001 -> BindTarget rebinds & promotes GPU-0000 waiter!
	code, acq1 := doReq(imgTok, http.MethodPost, "/internal/v1/gpu-scheduler/leases/acquire", AcquireRequest{
		IdempotencyKey:    "p1b-cold-moss-1",
		ConsumerKind:      "moss-image",
		TaskID:            "task-moss-1",
		AttemptID:         "att-1",
		StageID:           "generate",
		K8sSlotsRequired:  1,
		CandidateGPUUUIDs: []string{"GPU-0000"},
	})
	if code != http.StatusOK || acq1["state"] != "leased" || acq1["gpu_uuid"] != "GPU-0000" {
		t.Fatalf("expected cold-start moss leased on GPU-0000, got %d %+v", code, acq1)
	}
	mossLeaseID := acq1["lease_id"].(string)
	mossFence := int64(acq1["fencing_token"].(float64))

	code, waiter := doReq(vidTok, http.MethodPost, "/internal/v1/gpu-scheduler/leases/acquire", AcquireRequest{
		IdempotencyKey:    "p1b-waiter-h3-gpu0",
		ConsumerKind:      "h3",
		TaskID:            "task-h3-wait",
		AttemptID:         "att-w1",
		CandidateGPUUUIDs: []string{"GPU-0000"},
	})
	if code != http.StatusAccepted || waiter["state"] != "queued" {
		t.Fatalf("expected h3 queued behind moss on GPU-0000, got %d %+v", code, waiter)
	}
	waiterLeaseID := waiter["lease_id"].(string)

	mossPodUID := uuid.NewString()
	mossCID := strings.Repeat("c", 64)
	mossBootID := "boot-node-5090-1"
	code, bound := doReq(imgTok, http.MethodPost, "/internal/v1/gpu-scheduler/leases/"+mossLeaseID+"/bind-target", BindTargetRequest{
		IdempotencyKey: "bind-moss-1",
		FencingToken:   mossFence,
		PodUID:         mossPodUID,
		ContainerID:    "containerd://" + mossCID,
		BootID:         mossBootID,
		ActualGPUUUIDs: []string{"GPU-0001"},
	})
	if code != http.StatusOK || bound["state"] != "running" || bound["gpu_uuid"] != "GPU-0001" {
		t.Fatalf("expected BindTarget to rebind cold-start moss onto GPU-0001, got %d %+v", code, bound)
	}
	boundFence := int64(bound["fencing_token"].(float64))

	// Waiter on GPU-0000 must now be promoted automatically!
	code, waiterNow := doReq(vidTok, http.MethodGet, "/internal/v1/gpu-scheduler/leases/"+waiterLeaseID, nil)
	if code != http.StatusOK || waiterNow["state"] != "leased" || waiterNow["gpu_uuid"] != "GPU-0000" {
		t.Fatalf("expected GPU-0000 waiter promoted after moss rebound to GPU-0001, got %d %+v", code, waiterNow)
	}

	// 2. Cold-start BindTarget onto now-occupied GPU-0000 (by exclusive h3) MUST fail 422 TOPOLOGY_UNSATISFIABLE!
	code, acq2 := doReq(imgTok, http.MethodPost, "/internal/v1/gpu-scheduler/leases/acquire", AcquireRequest{
		IdempotencyKey:    "p1b-cold-face-1",
		ConsumerKind:      "face-fusion",
		TaskID:            "task-ff-1",
		AttemptID:         "att-ff1",
		CandidateGPUUUIDs: []string{"GPU-0001"},
	})
	if code != http.StatusOK || acq2["state"] != "leased" || acq2["gpu_uuid"] != "GPU-0001" {
		t.Fatalf("expected face-fusion leased alongside moss on GPU-0001, got %d %+v", code, acq2)
	}
	ffLeaseID := acq2["lease_id"].(string)
	ffFence := int64(acq2["fencing_token"].(float64))

	code, badBind := doReq(imgTok, http.MethodPost, "/internal/v1/gpu-scheduler/leases/"+ffLeaseID+"/bind-target", BindTargetRequest{
		IdempotencyKey: "bind-ff-bad-gpu0",
		FencingToken:   ffFence,
		PodUID:         uuid.NewString(),
		ContainerID:    strings.Repeat("d", 64),
		BootID:         mossBootID,
		ActualGPUUUIDs: []string{"GPU-0000"},
	})
	if code != http.StatusUnprocessableEntity || badBind["error_code"] != "TOPOLOGY_UNSATISFIABLE" {
		t.Fatalf("expected BindTarget onto saturated GPU-0000 to fail 422 TOPOLOGY_UNSATISFIABLE, got %d %+v", code, badBind)
	}

	// Unbound ffLeaseID can abort_unstarted cleanly before any container is bound!
	code, aborted := doReq(imgTok, http.MethodPost, "/internal/v1/gpu-scheduler/leases/"+ffLeaseID+"/release", ReleaseRequest{
		IdempotencyKey: "abort-ff-unstarted",
		FencingToken:   ffFence,
		WorkerSettled:  false,
		ReclaimMode:    "abort_unstarted",
	})
	if code != http.StatusOK || aborted["state"] != "released" {
		t.Fatalf("expected unbound cold-start lease to abort_unstarted cleanly, got %d %+v", code, aborted)
	}

	// Bound mossLeaseID MUST reject abort_unstarted (409 WORKER_NOT_SETTLED), unknown 'warm_settled' (400 INVALID_REQUEST), and caller-fabricated proof (503 RECLAIM_FAILED_QUARANTINED)!
	code, badAbort := doReq(imgTok, http.MethodPost, "/internal/v1/gpu-scheduler/leases/"+mossLeaseID+"/release", ReleaseRequest{
		IdempotencyKey: "abort-moss-bound",
		FencingToken:   boundFence,
		WorkerSettled:  false,
		ReclaimMode:    "abort_unstarted",
	})
	if code != http.StatusConflict || badAbort["error_code"] != "WORKER_NOT_SETTLED" {
		t.Fatalf("expected bound lease to reject abort_unstarted with 409 WORKER_NOT_SETTLED, got %d %+v", code, badAbort)
	}

	code, badEnum := doReq(imgTok, http.MethodPost, "/internal/v1/gpu-scheduler/leases/"+mossLeaseID+"/release", ReleaseRequest{
		IdempotencyKey: "rel-moss-bad-enum",
		FencingToken:   boundFence,
		WorkerSettled:  true,
		ReclaimMode:    "warm_settled",
	})
	if code != http.StatusBadRequest || badEnum["error_code"] != "INVALID_REQUEST" {
		t.Fatalf("expected unknown reclaim_mode=warm_settled to fail 400 INVALID_REQUEST, got %d %+v", code, badEnum)
	}

	code, fakeProof := doReq(imgTok, http.MethodPost, "/internal/v1/gpu-scheduler/leases/"+mossLeaseID+"/release", ReleaseRequest{
		IdempotencyKey: "rel-moss-fake-proof",
		FencingToken:   boundFence,
		WorkerSettled:  true,
		ReclaimMode:    "warm_idle_settled",
		ReclaimProof: map[string]any{
			"reclaimed":                  true,
			"active_tasks":               0,
			"supervisor_pid":             1,
			"supervisor_starttime_ticks": 100,
			"sealed_pod_uid":             mossPodUID,
			"sealed_container_id":        mossCID,
			"sealed_boot_id":             mossBootID,
			"revoked_lease_id":           mossLeaseID,
			"consumed_fencing_token":     boundFence,
			// Missing proof_source="receiver_guard" and token_sealed=true!
		},
	})
	if code != http.StatusServiceUnavailable || fakeProof["error_code"] != "RECLAIM_FAILED_QUARANTINED" {
		t.Fatalf("expected caller-fabricated proof without proof_source/token_sealed to fail 503 RECLAIM_FAILED_QUARANTINED, got %d %+v", code, fakeProof)
	}

	// Genuine receiver_guard proof (including attested resident_procs for proxy+upstream topology) releases cleanly!
	code, okRel := doReq(imgTok, http.MethodPost, "/internal/v1/gpu-scheduler/leases/"+mossLeaseID+"/release", ReleaseRequest{
		IdempotencyKey: "rel-moss-genuine-proof",
		FencingToken:   boundFence,
		WorkerSettled:  true,
		ReclaimMode:    "warm_idle_settled",
		ReclaimProof: map[string]any{
			"proof_source":               "receiver_guard",
			"token_sealed":               true,
			"reclaimed":                  true,
			"active_tasks":               0,
			"supervisor_pid":             12,
			"supervisor_starttime_ticks": 100012,
			"resident_procs": []map[string]any{
				{"pid": 1, "starttime_ticks": 100001},
				{"pid": 8, "starttime_ticks": 100008},
			},
			"sealed_pod_uid":         mossPodUID,
			"sealed_container_id":    mossCID,
			"sealed_boot_id":         mossBootID,
			"revoked_lease_id":       mossLeaseID,
			"consumed_fencing_token": boundFence,
		},
	})
	if code != http.StatusOK || okRel["state"] != "released" {
		t.Fatalf("expected genuine receiver_guard proof to release mossLeaseID, got %d %+v", code, okRel)
	}
	fix.verifier.mu.Lock()
	lastCall := fix.verifier.calls[len(fix.verifier.calls)-1]
	fix.verifier.mu.Unlock()
	if len(lastCall.ResidentProcs) != 2 || lastCall.ResidentProcs[1].PID != 8 || lastCall.ResidentProcs[1].StarttimeTicks != 100008 {
		t.Fatalf("expected ResidentProcs forwarded to NodeVerifier, got %+v", lastCall.ResidentProcs)
	}

	waiterFence := int64(waiterNow["fencing_token"].(float64))
	code, relWaiter := doReq(vidTok, http.MethodPost, "/internal/v1/gpu-scheduler/leases/"+waiterLeaseID+"/release", ReleaseRequest{
		IdempotencyKey: "rel-waiter-gpu0",
		FencingToken:   waiterFence,
		WorkerSettled:  true,
	})
	if code != http.StatusOK || relWaiter["state"] != "released" {
		t.Fatalf("expected waiter released cleanly, got %d %+v", code, relWaiter)
	}

	verifyAllCardHashChains(t, ctx, fix.pool, fix.stationID)
}

func TestResidencyEvictForceDrainAndSnapshotP2(t *testing.T) {
	fix := setupSchedulerDB(t)
	ctx := context.Background()
	srv := httptest.NewServer(Handler{Service: fix.svc})
	defer srv.Close()

	doJSON := func(token, method, path string, body any) (int, map[string]any) {
		var rdr *bytes.Reader
		if body != nil {
			b, _ := json.Marshal(body)
			rdr = bytes.NewReader(b)
		} else {
			rdr = bytes.NewReader(nil)
		}
		req, err := http.NewRequest(method, srv.URL+path, rdr)
		if err != nil {
			t.Fatal(err)
		}
		if body != nil {
			req.Header.Set("Content-Type", "application/json")
		}
		req.Header.Set("Authorization", "Bearer "+token)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		var out map[string]any
		_ = json.NewDecoder(resp.Body).Decode(&out)
		return resp.StatusCode, out
	}

	coreTok := fix.tokens["station-core"]
	resTok := fix.tokens["app-resources"]
	comfyTok := fix.tokens["app-comfyui"]
	blenderTok := fix.tokens["app-blender"]
	nodeCaller := CallerIdentity{AppID: "node-5090-agent", Kind: CallerNodeAgent}

	blenderInst := uuid.NewString()
	comfyS2Inst := uuid.NewString()
	comfyS0Inst := uuid.NewString()
	fix.svc.InstanceDir = staticTestInstanceDirectory{
		byID: map[string]RegisteredInstance{
			blenderInst: {
				InstanceID:       blenderInst,
				AppID:            "app-blender",
				PodUID:           "pod-blender-s1",
				CallbackEndpoint: "http://appr-bld01.verdantflare-app.svc.cluster.local:48084",
			},
			comfyS2Inst: {
				InstanceID:       comfyS2Inst,
				AppID:            "app-comfyui",
				PodUID:           "pod-comfy-s2",
				CallbackEndpoint: "http://appr-cfy02.verdantflare-app.svc.cluster.local:8188",
			},
			comfyS0Inst: {
				InstanceID:       comfyS0Inst,
				AppID:            "app-comfyui",
				PodUID:           "pod-comfy-s0",
				CallbackEndpoint: "http://appr-cfy03.verdantflare-app.svc.cluster.local:8188",
			},
		},
	}

	vramS1 := int64(4200)
	vramS2 := int64(11800)
	vramS0 := int64(3600)
	if status, out := doJSON(blenderTok, http.MethodPost, "/internal/v1/gpu-scheduler/residency/report", ResidencyReportRequest{
		InstanceID:         blenderInst,
		NodeUID:            fix.nodeUID,
		PodUID:             "pod-blender-s1",
		ContainerID:        "cid-blender-s1",
		BootID:             "boot-bld-s1",
		GPUEpoch:           3,
		ResidencySeq:       10,
		GPUUUID:            "GPU-0001",
		ResidencyState:     "human_protected",
		HumanProtected:     true,
		GUIConnectionAlive: true,
		Dirty:              true,
		PinnedBudgetMiB:    4200,
		VRAMAllocatedMiB:   &vramS1,
	}); status != http.StatusOK {
		t.Fatalf("report blender S1: %d %+v", status, out)
	}
	if status, out := doJSON(comfyTok, http.MethodPost, "/internal/v1/gpu-scheduler/residency/report", ResidencyReportRequest{
		InstanceID:       comfyS2Inst,
		NodeUID:          fix.nodeUID,
		PodUID:           "pod-comfy-s2",
		ContainerID:      "cid-comfy-s2",
		BootID:           "boot-cfy-s2",
		GPUEpoch:         7,
		ResidencySeq:     20,
		GPUUUID:          "GPU-0001",
		ResidencyState:   "idle_evictable",
		PinnedBudgetMiB:  0,
		VRAMAllocatedMiB: &vramS2,
	}); status != http.StatusOK {
		t.Fatalf("report comfy S2: %d %+v", status, out)
	}
	if status, out := doJSON(comfyTok, http.MethodPost, "/internal/v1/gpu-scheduler/residency/report", ResidencyReportRequest{
		InstanceID:       comfyS0Inst,
		NodeUID:          fix.nodeUID,
		PodUID:           "pod-comfy-s0",
		ContainerID:      "cid-comfy-s0",
		BootID:           "boot-cfy-s0",
		GPUEpoch:         2,
		ResidencySeq:     5,
		GPUUUID:          "GPU-0001",
		ResidencyState:   "active_compute",
		PinnedBudgetMiB:  4096,
		VRAMAllocatedMiB: &vramS0,
	}); status != http.StatusOK {
		t.Fatalf("report comfy S0: %d %+v", status, out)
	}

	if err := fix.svc.ReportNodeTelemetry(ctx, nodeCaller, NodeTelemetryReport{
		NodeUID:      fix.nodeUID,
		NodeName:     "k3s-5090-dev",
		SampleID:     2,
		AgentHealthy: true,
		Cards: []CardTelemetrySample{
			{GPUUUID: "GPU-0000", GPUIndex: 0, TotalVRAMMiB: 32607, DriverOverheadMiB: 607, MeasuredFreeVRAMMiB: 32000, MeasuredUsedVRAMMiB: 607, NVMLHealthy: true},
			{
				GPUUUID:             "GPU-0001",
				GPUIndex:            1,
				TotalVRAMMiB:        32607,
				DriverOverheadMiB:   607,
				MeasuredFreeVRAMMiB: 12400,
				MeasuredUsedVRAMMiB: 20207,
				NVMLHealthy:         true,
				AttributedProcesses: []AttributedGPUProcess{
					{PID: 101, PodUID: "pod-blender-s1", ContainerID: "cid-blender-s1", UsedVRAMMiB: 4200},
					{PID: 102, PodUID: "pod-comfy-s2", ContainerID: "cid-comfy-s2", UsedVRAMMiB: 11800},
					{PID: 103, PodUID: "pod-comfy-s0", ContainerID: "cid-comfy-s0", UsedVRAMMiB: 3600},
				},
			},
		},
	}); err != nil {
		t.Fatalf("telemetry report: %v", err)
	}

	// 1. Verify Snapshot P2 fields (LedgerUncommittedMiB vs GrantableStandardMiB vs UnmaterializedBudgetMiB vs Evictable).
	snap, err := fix.svc.Snapshot(ctx)
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	var c1 CardSnapshot
	for _, c := range snap.Cards {
		if c.GPUIndex == 1 {
			c1 = c
		}
	}
	// GPU 1: Total=32607, Driver=607, Pinned=4200, Leased=4096 (S0 budget 4096 vs actual 3600 -> unmaterialized 496), Headroom=2048
	// LedgerUncommitted = 32607 - 607 - 4200 - 4096 - 2048 = 21656 MiB
	// GrantableStandardMiB (G_now) = min(21656, 12400 - 2048 - 496) = 9856 MiB!
	if c1.LedgerUncommittedMiB != 21656 || c1.UnmaterializedBudgetMiB != 496 || c1.GrantableStandardMiB != 9856 || c1.EvictableMiB != 11800 || !c1.TelemetryFresh || !c1.AdmissionOpen {
		t.Fatalf("unexpected GPU 1 P2 snapshot fields: %+v", c1)
	}

	i64Ptr := func(v int64) *int64 { return &v }

	// 2. S5-N1: Soft-evicting S1 human-protected instance MUST return 409 GUI_EDIT_LEASE_HELD.
	if status, out := doJSON(resTok, http.MethodPost, "/internal/v1/gpu-scheduler/residency/"+blenderInst+"/evict", ResidencyEvictRequest{
		IdempotencyKey:       "evict-s1-blocked",
		ExpectedBootID:       "boot-bld-s1",
		ExpectedGPUEpoch:     i64Ptr(3),
		ExpectedResidencySeq: i64Ptr(10),
	}); status != http.StatusConflict || out["error_code"] != "GUI_EDIT_LEASE_HELD" {
		t.Fatalf("expected 409 GUI_EDIT_LEASE_HELD on S1 evict, got %d %+v", status, out)
	}

	// 3. Soft-evicting S0 active-compute instance MUST return 409 INSTANCE_ACTIVE_COMPUTE.
	if status, out := doJSON(resTok, http.MethodPost, "/internal/v1/gpu-scheduler/residency/"+comfyS0Inst+"/evict", ResidencyEvictRequest{
		IdempotencyKey:       "evict-s0-blocked",
		ExpectedBootID:       "boot-cfy-s0",
		ExpectedGPUEpoch:     i64Ptr(2),
		ExpectedResidencySeq: i64Ptr(5),
	}); status != http.StatusConflict || out["error_code"] != "INSTANCE_ACTIVE_COMPUTE" {
		t.Fatalf("expected 409 INSTANCE_ACTIVE_COMPUTE on S0 evict, got %d %+v", status, out)
	}

	// 4. Soft-evicting S2 idle-evictable instance succeeds reversibly, keeping boot_id unchanged!
	fix.verifier.byKey = map[string]VerifyTerminatedResult{
		"pod-comfy-s2": {
			Verified:            true,
			GPUProcessCount:     0,
			MeasuredFreeVRAMMiB: 24200,
		},
		"pod-blender-s1": {
			Verified:            true,
			GPUProcessCount:     0,
			MeasuredFreeVRAMMiB: 28400,
		},
	}
	status, evOut := doJSON(resTok, http.MethodPost, "/internal/v1/gpu-scheduler/residency/"+comfyS2Inst+"/evict", ResidencyEvictRequest{
		IdempotencyKey:       "evict-s2-ok",
		ExpectedBootID:       "boot-cfy-s2",
		ExpectedGPUEpoch:     i64Ptr(7),
		ExpectedResidencySeq: i64Ptr(20),
		ActorUserID:          "user-member-1",
	})
	if status != http.StatusOK || evOut["residency_state"] != "zero_vram_standby" || evOut["boot_id"] != "boot-cfy-s2" {
		t.Fatalf("expected 200 zero_vram_standby preserving boot_id, got %d %+v", status, evOut)
	}
	// Repeating soft-evict on S3 returns 409 ALREADY_ZERO_VRAM.
	if status, out := doJSON(resTok, http.MethodPost, "/internal/v1/gpu-scheduler/residency/"+comfyS2Inst+"/evict", ResidencyEvictRequest{
		IdempotencyKey:       "evict-s3-repeat",
		ExpectedBootID:       "boot-cfy-s2",
		ExpectedGPUEpoch:     i64Ptr(7),
		ExpectedResidencySeq: i64Ptr(21),
	}); status != http.StatusConflict || out["error_code"] != "ALREADY_ZERO_VRAM" {
		t.Fatalf("expected 409 ALREADY_ZERO_VRAM on S3 evict, got %d %+v", status, out)
	}

	// 5. Force-drain RBAC & S5-N4 two-step confirmation checks:
	// 5a. Non-admin consumer (app-resources directly) is forbidden -> 403 CALLER_IDENTITY_MISMATCH.
	if status, out := doJSON(resTok, http.MethodPost, "/internal/v1/gpu-scheduler/residency/"+blenderInst+"/force-drain", ResidencyForceDrainRequest{
		ConfirmForceDrain:    true,
		ConfirmInstanceID:    blenderInst,
		ExpectedBootID:       "boot-bld-s1",
		ExpectedGPUEpoch:     i64Ptr(3),
		ExpectedResidencySeq: i64Ptr(10),
	}); status != http.StatusForbidden || out["error_code"] != "CALLER_IDENTITY_MISMATCH" {
		t.Fatalf("expected 403 CALLER_IDENTITY_MISMATCH for direct app-resources force-drain, got %d %+v", status, out)
	}
	// 5b. Missing confirm_force_drain or wrong confirm_instance_id -> 400 CONFIRMATION_REQUIRED.
	if status, out := doJSON(coreTok, http.MethodPost, "/internal/v1/gpu-scheduler/residency/"+blenderInst+"/force-drain", ResidencyForceDrainRequest{
		ConfirmForceDrain:    false,
		ConfirmInstanceID:    blenderInst,
		ExpectedBootID:       "boot-bld-s1",
		ExpectedGPUEpoch:     i64Ptr(3),
		ExpectedResidencySeq: i64Ptr(10),
	}); status != http.StatusBadRequest || out["error_code"] != "CONFIRMATION_REQUIRED" {
		t.Fatalf("expected 400 CONFIRMATION_REQUIRED when confirm_force_drain=false, got %d %+v", status, out)
	}
	if status, out := doJSON(coreTok, http.MethodPost, "/internal/v1/gpu-scheduler/residency/"+blenderInst+"/force-drain", ResidencyForceDrainRequest{
		ConfirmForceDrain:    true,
		ConfirmInstanceID:    "wrong-instance",
		ExpectedBootID:       "boot-bld-s1",
		ExpectedGPUEpoch:     i64Ptr(3),
		ExpectedResidencySeq: i64Ptr(10),
	}); status != http.StatusBadRequest || out["error_code"] != "CONFIRMATION_REQUIRED" {
		t.Fatalf("expected 400 CONFIRMATION_REQUIRED when confirm_instance_id mismatches, got %d %+v", status, out)
	}

	// 5c. Confirmed station-core force-drain on S1 human-protected instance succeeds, sends ForceHard=true, keeps boot_id, and logs ADMIN_FORCE_DRAIN!
	var sawForceHard bool
	fix.evictor.evictFn = func(cmd EvictVRAMCommand) (EvictVRAMResult, error) {
		sawForceHard = cmd.ForceHard
		return EvictVRAMResult{
			Settled:                  true,
			ResidencyState:           "zero_vram_standby",
			ResidencySeq:             cmd.ExpectedResidencySeq + 1,
			GPUEpoch:                 cmd.ExpectedGPUEpoch,
			SealedGPUEpoch:           cmd.ExpectedGPUEpoch,
			BootID:                   cmd.ExpectedBootID,
			SupervisorPID:            9100,
			SupervisorStarttimeTicks: 99001,
			ReclaimProof:             makeMockEpochSealProof(ctx, fix.pool, fix.stationID, cmd, 9100, 99001),
		}, nil
	}
	status, fdOut := doJSON(coreTok, http.MethodPost, "/internal/v1/gpu-scheduler/residency/"+blenderInst+"/force-drain", ResidencyForceDrainRequest{
		IdempotencyKey:       "force-drain-s1-ok",
		ConfirmForceDrain:    true,
		ConfirmInstanceID:    blenderInst,
		ExpectedBootID:       "boot-bld-s1",
		ExpectedGPUEpoch:     i64Ptr(3),
		ExpectedResidencySeq: i64Ptr(10),
		ActorUserID:          "admin-mengsk",
		OrganizationID:       "org-vf",
	})
	if status != http.StatusOK || !sawForceHard || fdOut["residency_state"] != "zero_vram_standby" || fdOut["boot_id"] != "boot-bld-s1" {
		t.Fatalf("expected 200 zero_vram_standby with ForceHard=true and preserved boot_id, got %d sawForceHard=%v %+v", status, sawForceHard, fdOut)
	}

	// 6. Verify ADMIN_FORCE_DRAIN is in Snapshot.RecentEvents (and internal *_INTENT rows are excluded) and hash chain is 100% valid; also verify stale telemetry sets TelemetryFresh=false & AdmissionOpen=false & GrantableStandardMiB=0!
	fix.advanceTime(20 * time.Second)
	snapStale, err := fix.svc.Snapshot(ctx)
	if err != nil {
		t.Fatalf("Snapshot stale: %v", err)
	}
	foundForceEvent := false
	for _, ev := range snapStale.RecentEvents {
		if ev.ReasonCode == "EVICT_INTENT" || ev.ReasonCode == "FORCE_DRAIN_INTENT" {
			t.Fatalf("internal intent event leaked into Snapshot.RecentEvents: %+v", ev)
		}
		if ev.ReasonCode == "ADMIN_FORCE_DRAIN" && ev.EntityID == blenderInst {
			foundForceEvent = true
		}
	}
	if !foundForceEvent {
		t.Fatalf("expected ADMIN_FORCE_DRAIN in RecentEvents, got %+v", snapStale.RecentEvents)
	}
	for _, c := range snapStale.Cards {
		if c.TelemetryFresh || c.AdmissionOpen || c.GrantableStandardMiB != 0 {
			t.Fatalf("expected stale telemetry Fail-Closed (TelemetryFresh=false, AdmissionOpen=false, GrantableStandardMiB=0), got %+v", c)
		}
	}

	verifyAllCardHashChains(t, ctx, fix.pool, fix.stationID)
}

type stageHookFunc func(context.Context, string, map[string]string)

func (fn stageHookFunc) OnStage(ctx context.Context, point string, meta map[string]string) {
	fn(ctx, point, meta)
}

func TestP2ForceDrainS0S1RealHTTPIdempotencyAndConcurrencyCAS(t *testing.T) {
	fix := setupSchedulerDB(t)
	ctx := context.Background()

	comfyCaller := CallerIdentity{AppID: "app-comfyui", Kind: CallerConsumer}
	blenderCaller := CallerIdentity{AppID: "app-blender", Kind: CallerConsumer}
	coreTok := fix.tokens["station-core"]
	resTok := fix.tokens["app-resources"]
	upgradedTrue := true

	workerBearer := "worker-http-secret-token"
	var (
		wMu            sync.Mutex
		wState         = "active_compute"
		wBootID        = "boot-s0-real"
		wPodUID        = "pod-s0-real"
		wCID           = "cid-s0-real"
		wEpoch         = int64(2)
		wSeq           = int64(5)
		omitProof      = false
		revokeCount    int
		evictCalls     int
		lastEvictForce bool
	)

	workerSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer "+workerBearer {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		wMu.Lock()
		defer wMu.Unlock()
		switch r.URL.Path {
		case "/internal/v1/gpu-residency/revoke-lease":
			var body map[string]any
			_ = json.NewDecoder(r.Body).Decode(&body)
			revokeCount++
			// Mirror real ComfyUI/Blender worker: RevokeLease increments residency_seq in-epoch when active,
			// and is a settled no-op when already in zero_vram_standby!
			if wState != "zero_vram_standby" {
				wSeq++
				wState = "idle_evictable"
			}
			_ = json.NewEncoder(w).Encode(map[string]any{
				"revoked":         true,
				"settled":         true,
				"boot_id":         wBootID,
				"gpu_epoch":       wEpoch,
				"residency_seq":   wSeq,
				"residency_state": wState,
			})
		case "/internal/v1/gpu-residency/evict-vram":
			var body map[string]any
			_ = json.NewDecoder(r.Body).Decode(&body)
			evictCalls++
			forceHard, _ := body["force_hard"].(bool)
			lastEvictForce = forceHard
			expBoot, _ := body["expected_boot_id"].(string)
			expEpochFloat, _ := body["expected_gpu_epoch"].(float64)
			expSeqFloat, _ := body["expected_residency_seq"].(float64)
			if expBoot != wBootID {
				w.WriteHeader(http.StatusConflict)
				_ = json.NewEncoder(w).Encode(map[string]any{
					"reason_code":    "BOOT_ID_MISMATCH",
					"actual_boot_id": wBootID,
				})
				return
			}
			if int64(expEpochFloat) != wEpoch {
				w.WriteHeader(http.StatusConflict)
				_ = json.NewEncoder(w).Encode(map[string]any{
					"reason_code":      "GPU_EPOCH_CONFLICT",
					"actual_gpu_epoch": wEpoch,
				})
				return
			}
			if int64(expSeqFloat) != wSeq {
				w.WriteHeader(http.StatusConflict)
				_ = json.NewEncoder(w).Encode(map[string]any{
					"reason_code":          "RESIDENCY_SEQ_CONFLICT",
					"actual_boot_id":       wBootID,
					"actual_gpu_epoch":     wEpoch,
					"actual_residency_seq": wSeq,
				})
				return
			}
			if !forceHard && (wState == "active_compute" || wState == "human_protected" || wState == "human_dirty") {
				w.WriteHeader(http.StatusConflict)
				_ = json.NewEncoder(w).Encode(map[string]any{
					"reason_code": "INSTANCE_ACTIVE_COMPUTE",
				})
				return
			}
			// Mirror real ComfyUI/Blender worker: draining keeps gpu_epoch == wEpoch (no increment until next wake),
			// and if already in zero_vram_standby at wSeq, returns ALREADY_STANDBY without advancing wSeq again!
			if wState != "zero_vram_standby" {
				wSeq++
				wState = "zero_vram_standby"
			}
			resp := map[string]any{
				"settled":                    true,
				"residency_state":            "zero_vram_standby",
				"boot_id":                    wBootID,
				"gpu_epoch":                  wEpoch,
				"sealed_gpu_epoch":           wEpoch,
				"residency_seq":              wSeq,
				"supervisor_pid":             7788,
				"supervisor_starttime_ticks": uint64(665544),
			}
			if !omitProof {
				resp["reclaim_proof"] = map[string]any{
					"reclaimed":                  true,
					"sealed_gpu_epoch":           wEpoch,
					"sealed_pod_uid":             wPodUID,
					"sealed_container_id":        wCID,
					"sealed_boot_id":             wBootID,
					"supervisor_pid":             7788,
					"supervisor_starttime_ticks": uint64(665544),
				}
			}
			_ = json.NewEncoder(w).Encode(resp)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer workerSrv.Close()

	fix.svc.AllowLoopbackCallbacks = true
	fix.svc.Evictor = HTTPResidencyEvictor{
		BearerToken:            workerBearer,
		AllowLoopbackCallbacks: true,
	}

	srv := httptest.NewServer(Handler{Service: fix.svc})
	defer srv.Close()

	doJSON := func(token, method, path string, body any) (int, map[string]any) {
		var rdr *bytes.Reader
		if body != nil {
			b, err := json.Marshal(body)
			if err != nil {
				t.Fatalf("marshal: %v", err)
			}
			rdr = bytes.NewReader(b)
		} else {
			rdr = bytes.NewReader(nil)
		}
		req, err := http.NewRequest(method, srv.URL+path, rdr)
		if err != nil {
			t.Fatalf("new req: %v", err)
		}
		req.Header.Set("Authorization", "Bearer "+token)
		if body != nil {
			req.Header.Set("Content-Type", "application/json")
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("do %s %s: %v", method, path, err)
		}
		defer resp.Body.Close()
		var out map[string]any
		_ = json.NewDecoder(resp.Body).Decode(&out)
		return resp.StatusCode, out
	}

	// 1. Setup S0 ComfyUI residency + active running compute lease bound to (pod-s0-real, boot-s0-real).
	const s0Inst = "comfyui-s0-real"
	alloc8g := int64(8192)
	s0Lease, err := fix.svc.Acquire(ctx, comfyCaller, AcquireRequest{
		IdempotencyKey:    "acq-s0-real",
		ConsumerKind:      "comfyui_wake",
		AttemptID:         "att-s0-real",
		TaskID:            "prompt-s0-1",
		InstanceID:        s0Inst,
		PeakBudgetMiB:     8192,
		StartupFreeMinMiB: 8192,
		PodUID:            wPodUID,
		ContainerID:       wCID,
		BootID:            wBootID,
		CandidateGPUUUIDs: []string{"GPU-0001"},
	})
	if err != nil || s0Lease.State != "leased" {
		t.Fatalf("Acquire s0Lease: %+v (%v)", s0Lease, err)
	}
	if err := fix.svc.ReportResidency(ctx, comfyCaller, ResidencyReportRequest{
		InstanceID:       s0Inst,
		NodeUID:          fix.nodeUID,
		PodUID:           wPodUID,
		ContainerID:      wCID,
		BootID:           wBootID,
		GPUEpoch:         wEpoch,
		ResidencySeq:     wSeq,
		GPUUUID:          "GPU-0001",
		ResidencyState:   "active_compute",
		VRAMAllocatedMiB: &alloc8g,
		CallbackEndpoint: workerSrv.URL,
		TemplateUpgraded: &upgradedTrue,
	}); err != nil {
		t.Fatalf("ReportResidency s0Inst: %v", err)
	}
	fix.verifier.byKey = map[string]VerifyTerminatedResult{
		wPodUID: {
			Verified:            true,
			GPUProcessCount:     0,
			MeasuredFreeVRAMMiB: 32000,
		},
	}

	i64Ptr := func(v int64) *int64 { return &v }

	// 1a. Missing idempotency_key or missing expected_* version fields -> 400 INVALID_REQUEST (0 worker calls).
	if st, out := doJSON(coreTok, http.MethodPost, "/internal/v1/gpu-scheduler/residency/"+s0Inst+"/force-drain", ResidencyForceDrainRequest{
		ConfirmForceDrain:    true,
		ConfirmInstanceID:    s0Inst,
		ExpectedBootID:       wBootID,
		ExpectedGPUEpoch:     &wEpoch,
		ExpectedResidencySeq: &wSeq,
	}); st != http.StatusBadRequest || out["error_code"] != "INVALID_REQUEST" {
		t.Fatalf("expected 400 INVALID_REQUEST when idempotency_key missing, got %d %+v", st, out)
	}
	if st, out := doJSON(coreTok, http.MethodPost, "/internal/v1/gpu-scheduler/residency/"+s0Inst+"/force-drain", ResidencyForceDrainRequest{
		IdempotencyKey:    "fd-missing-expected-version",
		ConfirmForceDrain: true,
		ConfirmInstanceID: s0Inst,
	}); st != http.StatusBadRequest || out["error_code"] != "INVALID_REQUEST" {
		t.Fatalf("expected 400 INVALID_REQUEST when expected_* missing on force-drain, got %d %+v", st, out)
	}
	if st, out := doJSON(resTok, http.MethodPost, "/internal/v1/gpu-scheduler/residency/"+s0Inst+"/evict", ResidencyEvictRequest{
		IdempotencyKey: "evict-missing-expected-version",
	}); st != http.StatusBadRequest || out["error_code"] != "INVALID_REQUEST" {
		t.Fatalf("expected 400 INVALID_REQUEST when expected_* missing on evict, got %d %+v", st, out)
	}

	// 1b. Optimistic CAS mismatch -> 409 LEASE_VERSION_CONFLICT before touching worker!
	wrongEpoch := int64(99)
	if st, out := doJSON(coreTok, http.MethodPost, "/internal/v1/gpu-scheduler/residency/"+s0Inst+"/force-drain", ResidencyForceDrainRequest{
		IdempotencyKey:       "fd-wrong-cas",
		ConfirmForceDrain:    true,
		ConfirmInstanceID:    s0Inst,
		ExpectedBootID:       wBootID,
		ExpectedGPUEpoch:     &wrongEpoch,
		ExpectedResidencySeq: &wSeq,
		ActorUserID:          "admin-mengsk",
		OrganizationID:       "org-vf",
	}); st != http.StatusConflict || out["error_code"] != "LEASE_VERSION_CONFLICT" {
		t.Fatalf("expected 409 LEASE_VERSION_CONFLICT on ExpectedGPUEpoch mismatch, got %d %+v", st, out)
	}
	if revokeCount != 0 || evictCalls != 0 {
		t.Fatalf("expected 0 worker calls on pre-check CAS mismatch, got revoke=%d evict=%d", revokeCount, evictCalls)
	}

	// 1c. Real HTTP ForceDrainResidency on S0 succeeds across RevokeLease (seq 5->6) -> EvictVRAM(force_hard=true, seq 6->7, epoch stays 2)!
	curEp := wEpoch
	curSq := wSeq
	st, fdOut := doJSON(coreTok, http.MethodPost, "/internal/v1/gpu-scheduler/residency/"+s0Inst+"/force-drain", ResidencyForceDrainRequest{
		IdempotencyKey:       "fd-s0-real-ok",
		ConfirmForceDrain:    true,
		ConfirmInstanceID:    s0Inst,
		ExpectedBootID:       wBootID,
		ExpectedGPUEpoch:     &curEp,
		ExpectedResidencySeq: &curSq,
		ActorUserID:          "admin-mengsk",
		OrganizationID:       "org-vf",
	})
	if st != http.StatusOK || !lastEvictForce || revokeCount != 1 || evictCalls != 1 {
		t.Fatalf("expected 200 OK real HTTP S0 force-drain (revoke=1, evict=1), got st=%d revoke=%d evict=%d out=%+v", st, revokeCount, evictCalls, fdOut)
	}
	if fdOut["residency_state"] != "zero_vram_standby" || fdOut["boot_id"] != wBootID || int64(fdOut["gpu_epoch"].(float64)) != 2 || int64(fdOut["residency_seq"].(float64)) != 7 {
		t.Fatalf("expected preserved boot_id & gpu_epoch=2 & residency_seq=7, got %+v", fdOut)
	}
	var dbLeaseState string
	if err := fix.pool.QueryRow(ctx, `SELECT state FROM station.gpu_resource_leases WHERE station_id=$1 AND lease_id=$2`, fix.stationID, s0Lease.LeaseID).Scan(&dbLeaseState); err != nil || dbLeaseState != "expired" {
		t.Fatalf("expected bound S0 lease expired after force-drain, got %q (%v)", dbLeaseState, err)
	}

	// 2. Durable idempotency replay AFTER instance wakes into a brand-new epoch (gpu_epoch=3, active_compute):
	//    Replay of "fd-s0-real-ok" MUST return cached epoch=2 result WITHOUT touching the newly woken epoch=3 worker!
	wMu.Lock()
	wEpoch = 3
	wSeq = 8
	wState = "active_compute"
	wMu.Unlock()
	if err := fix.svc.ReportResidency(ctx, comfyCaller, ResidencyReportRequest{
		InstanceID:       s0Inst,
		NodeUID:          fix.nodeUID,
		PodUID:           wPodUID,
		ContainerID:      wCID,
		BootID:           wBootID,
		GPUEpoch:         3,
		ResidencySeq:     8,
		GPUUUID:          "GPU-0001",
		ResidencyState:   "active_compute",
		VRAMAllocatedMiB: &alloc8g,
		CallbackEndpoint: workerSrv.URL,
		TemplateUpgraded: &upgradedTrue,
	}); err != nil {
		t.Fatalf("ReportResidency wake epoch 3: %v", err)
	}
	stReplay, replayOut := doJSON(coreTok, http.MethodPost, "/internal/v1/gpu-scheduler/residency/"+s0Inst+"/force-drain", ResidencyForceDrainRequest{
		IdempotencyKey:       "fd-s0-real-ok",
		ConfirmForceDrain:    true,
		ConfirmInstanceID:    s0Inst,
		ExpectedBootID:       wBootID,
		ExpectedGPUEpoch:     &curEp,
		ExpectedResidencySeq: &curSq,
		ActorUserID:          "admin-mengsk",
		OrganizationID:       "org-vf",
	})
	if stReplay != http.StatusOK || revokeCount != 1 || evictCalls != 1 || int64(replayOut["gpu_epoch"].(float64)) != 2 {
		t.Fatalf("expected durable idempotency replay without touching epoch 3 worker, got st=%d revoke=%d evict=%d out=%+v", stReplay, revokeCount, evictCalls, replayOut)
	}
	var afterReplayState string
	var afterReplayEpoch int64
	if err := fix.pool.QueryRow(ctx, `SELECT residency_state, gpu_epoch FROM station.gpu_instance_residency WHERE station_id=$1 AND instance_id=$2`, fix.stationID, s0Inst).Scan(&afterReplayState, &afterReplayEpoch); err != nil || afterReplayState != "active_compute" || afterReplayEpoch != 3 {
		t.Fatalf("expected woken epoch 3 untouched by replay, got state=%q epoch=%d (%v)", afterReplayState, afterReplayEpoch, err)
	}
	// Reusing "fd-s0-real-ok" with a mutated request field -> 409 IDEMPOTENCY_CONFLICT!
	if stTamper, tamperOut := doJSON(coreTok, http.MethodPost, "/internal/v1/gpu-scheduler/residency/"+s0Inst+"/force-drain", ResidencyForceDrainRequest{
		IdempotencyKey:       "fd-s0-real-ok",
		ConfirmForceDrain:    true,
		ConfirmInstanceID:    s0Inst,
		ExpectedBootID:       wBootID,
		ExpectedGPUEpoch:     &curEp,
		ExpectedResidencySeq: &curSq,
		Reason:               "mutated_reason",
		ActorUserID:          "admin-mengsk",
		OrganizationID:       "org-vf",
	}); stTamper != http.StatusConflict || tamperOut["error_code"] != "IDEMPOTENCY_CONFLICT" {
		t.Fatalf("expected 409 IDEMPOTENCY_CONFLICT on digest mismatch, got %d %+v", stTamper, tamperOut)
	}

	// 3. Missing ReclaimProof on EvictVRAM MUST fail closed with 503 RECLAIM_FAILED_QUARANTINED on BOTH EvictResidency and ForceDrainResidency!
	wMu.Lock()
	wPodUID = "pod-proof-check"
	wCID = "cid-proof-check"
	wBootID = "boot-proof-check"
	wEpoch = 1
	wSeq = 1
	wState = "idle_evictable"
	omitProof = true
	wMu.Unlock()
	fix.verifier.byKey[wPodUID] = VerifyTerminatedResult{Verified: true, GPUProcessCount: 0, MeasuredFreeVRAMMiB: 32000}
	const proofInst = "blender-proof-check"
	if err := fix.svc.ReportResidency(ctx, blenderCaller, ResidencyReportRequest{
		InstanceID:       proofInst,
		NodeUID:          fix.nodeUID,
		PodUID:           wPodUID,
		ContainerID:      wCID,
		BootID:           wBootID,
		GPUEpoch:         1,
		ResidencySeq:     1,
		GPUUUID:          "GPU-0001",
		ResidencyState:   "idle_evictable",
		VRAMAllocatedMiB: &alloc8g,
		CallbackEndpoint: workerSrv.URL,
		TemplateUpgraded: &upgradedTrue,
	}); err != nil {
		t.Fatalf("ReportResidency proofInst: %v", err)
	}
	if stNoProof, outNoProof := doJSON(resTok, http.MethodPost, "/internal/v1/gpu-scheduler/residency/"+proofInst+"/evict", ResidencyEvictRequest{
		IdempotencyKey:       "evict-missing-proof",
		ExpectedBootID:       "boot-proof-check",
		ExpectedGPUEpoch:     i64Ptr(1),
		ExpectedResidencySeq: i64Ptr(1),
	}); stNoProof != http.StatusServiceUnavailable || outNoProof["error_code"] != "RECLAIM_FAILED_QUARANTINED" {
		t.Fatalf("expected 503 RECLAIM_FAILED_QUARANTINED when EvictVRAM omits reclaim_proof, got %d %+v", stNoProof, outNoProof)
	}
	wMu.Lock()
	wState = "human_dirty"
	curProofSeq := wSeq
	wMu.Unlock()
	if err := fix.svc.ReportResidency(ctx, blenderCaller, ResidencyReportRequest{
		InstanceID:       proofInst,
		NodeUID:          fix.nodeUID,
		PodUID:           wPodUID,
		ContainerID:      wCID,
		BootID:           wBootID,
		GPUEpoch:         1,
		ResidencySeq:     curProofSeq,
		GPUUUID:          "GPU-0001",
		ResidencyState:   "human_dirty",
		HumanProtected:   true,
		Dirty:            true,
		PinnedBudgetMiB:  4096,
		VRAMAllocatedMiB: &alloc8g,
		CallbackEndpoint: workerSrv.URL,
		TemplateUpgraded: &upgradedTrue,
	}); err != nil {
		t.Fatalf("ReportResidency human_dirty: %v", err)
	}
	if stFDNoProof, outFDNoProof := doJSON(coreTok, http.MethodPost, "/internal/v1/gpu-scheduler/residency/"+proofInst+"/force-drain", ResidencyForceDrainRequest{
		IdempotencyKey:       "fd-missing-proof",
		ConfirmForceDrain:    true,
		ConfirmInstanceID:    proofInst,
		ExpectedBootID:       "boot-proof-check",
		ExpectedGPUEpoch:     i64Ptr(1),
		ExpectedResidencySeq: &curProofSeq,
		ActorUserID:          "admin-mengsk",
		OrganizationID:       "org-vf",
	}); stFDNoProof != http.StatusServiceUnavailable || outFDNoProof["error_code"] != "RECLAIM_FAILED_QUARANTINED" {
		t.Fatalf("expected 503 RECLAIM_FAILED_QUARANTINED when ForceDrain EvictVRAM omits reclaim_proof, got %d %+v", stFDNoProof, outFDNoProof)
	}

	// 4. Concurrent wake race injected via after_force_drain_verify_before_commit:
	//    While ForceDrain of epoch=1 is about to commit, instance wakes into epoch=2 and acquires a new running compute lease.
	//    ForceDrain MUST abort with 409 LEASE_VERSION_CONFLICT and MUST NOT overwrite epoch=2 or expire the newly acquired lease!
	wMu.Lock()
	omitProof = false
	wPodUID = "pod-race-s1"
	wCID = "cid-race-s1"
	wBootID = "boot-race-s1"
	wEpoch = 1
	wSeq = 2
	wState = "human_protected"
	wMu.Unlock()
	fix.verifier.byKey[wPodUID] = VerifyTerminatedResult{Verified: true, GPUProcessCount: 0, MeasuredFreeVRAMMiB: 32000}
	const raceInst = "blender-race-s1"
	if err := fix.svc.ReportResidency(ctx, blenderCaller, ResidencyReportRequest{
		InstanceID:         raceInst,
		NodeUID:            fix.nodeUID,
		PodUID:             wPodUID,
		ContainerID:        wCID,
		BootID:             wBootID,
		GPUEpoch:           1,
		ResidencySeq:       2,
		GPUUUID:            "GPU-0001",
		ResidencyState:     "human_protected",
		HumanProtected:     true,
		GUIConnectionAlive: true,
		PinnedBudgetMiB:    4096,
		VRAMAllocatedMiB:   &alloc8g,
		CallbackEndpoint:   workerSrv.URL,
		TemplateUpgraded:   &upgradedTrue,
	}); err != nil {
		t.Fatalf("ReportResidency raceInst: %v", err)
	}

	var newEpochLease LeaseDecision
	fix.svc.Hook = stageHookFunc(func(hCtx context.Context, stage string, meta map[string]string) {
		if stage == "after_force_drain_verify_before_commit" && meta["instance_id"] == raceInst {
			var acqErr error
			newEpochLease, acqErr = fix.svc.Acquire(hCtx, blenderCaller, AcquireRequest{
				IdempotencyKey:    "acq-concurrent-wake-epoch2",
				ConsumerKind:      "blender_wake",
				AttemptID:         "att-wake-epoch2",
				TaskID:            "render-epoch2",
				InstanceID:        raceInst,
				PeakBudgetMiB:     6144,
				StartupFreeMinMiB: 6144,
				PodUID:            wPodUID,
				ContainerID:       wCID,
				BootID:            wBootID,
				CandidateGPUUUIDs: []string{"GPU-0000"},
			})
			if acqErr != nil || newEpochLease.State != "leased" {
				t.Errorf("concurrent Acquire during hook failed: %+v (%v)", newEpochLease, acqErr)
			}
			if repErr := fix.svc.ReportResidency(hCtx, blenderCaller, ResidencyReportRequest{
				InstanceID:       raceInst,
				NodeUID:          fix.nodeUID,
				PodUID:           wPodUID,
				ContainerID:      wCID,
				BootID:           wBootID,
				GPUEpoch:         2,
				ResidencySeq:     10,
				GPUUUID:          "GPU-0000",
				ResidencyState:   "active_compute",
				VRAMAllocatedMiB: &alloc8g,
				CallbackEndpoint: workerSrv.URL,
				TemplateUpgraded: &upgradedTrue,
			}); repErr != nil {
				t.Errorf("concurrent ReportResidency epoch 2 failed: %v", repErr)
			}
		}
	})
	defer func() { fix.svc.Hook = nil }()

	stRace, outRace := doJSON(coreTok, http.MethodPost, "/internal/v1/gpu-scheduler/residency/"+raceInst+"/force-drain", ResidencyForceDrainRequest{
		IdempotencyKey:       "fd-race-cas-abort",
		ConfirmForceDrain:    true,
		ConfirmInstanceID:    raceInst,
		ExpectedBootID:       "boot-race-s1",
		ExpectedGPUEpoch:     i64Ptr(1),
		ExpectedResidencySeq: i64Ptr(2),
		ActorUserID:          "admin-mengsk",
		OrganizationID:       "org-vf",
	})
	if stRace != http.StatusConflict || outRace["error_code"] != "LEASE_VERSION_CONFLICT" {
		t.Fatalf("expected 409 LEASE_VERSION_CONFLICT when concurrent wake advances gpu_epoch before commit, got %d %+v", stRace, outRace)
	}
	var finalResState string
	var finalEpoch, finalSeq int64
	if err := fix.pool.QueryRow(ctx, `SELECT residency_state, gpu_epoch, residency_seq FROM station.gpu_instance_residency WHERE station_id=$1 AND instance_id=$2`, fix.stationID, raceInst).Scan(&finalResState, &finalEpoch, &finalSeq); err != nil || finalResState != "active_compute" || finalEpoch != 2 || finalSeq != 10 {
		t.Fatalf("expected concurrent epoch=2 residency preserved untouched, got state=%q epoch=%d seq=%d (%v)", finalResState, finalEpoch, finalSeq, err)
	}
	var newLeaseState string
	if err := fix.pool.QueryRow(ctx, `SELECT state FROM station.gpu_resource_leases WHERE station_id=$1 AND lease_id=$2`, fix.stationID, newEpochLease.LeaseID).Scan(&newLeaseState); err != nil || newLeaseState != "leased" {
		t.Fatalf("expected newly acquired epoch=2 lease NOT expired by stale force-drain, got %q (%v)", newLeaseState, err)
	}

	// 5. Crash AFTER worker drain & BEFORE DB commit (ForceDrain + Evict):
	//    Attempt 1 drains worker (advancing worker residency_seq while DB still has pre-drain seq) and aborts before final DB commit.
	//    Retrying with the SAME IdempotencyKey and original Expected* MUST converge cleanly to 200 OK using durable *_INTENT!
	const crashConvInst = "comfyui-crash-converge"
	wMu.Lock()
	wPodUID = "pod-crash-conv"
	wCID = "cid-crash-conv"
	wBootID = "boot-crash-conv"
	wEpoch = 4
	wSeq = 1
	wState = "active_compute"
	wMu.Unlock()
	fix.verifier.byKey[wPodUID] = VerifyTerminatedResult{Verified: true, GPUProcessCount: 0, MeasuredFreeVRAMMiB: 32000}
	crashConvLease, err := fix.svc.Acquire(ctx, comfyCaller, AcquireRequest{
		IdempotencyKey:    "acq-crash-conv",
		ConsumerKind:      "comfyui_wake",
		AttemptID:         "att-crash-conv",
		TaskID:            "prompt-crash-conv",
		InstanceID:        crashConvInst,
		PeakBudgetMiB:     8192,
		StartupFreeMinMiB: 8192,
		PodUID:            wPodUID,
		ContainerID:       wCID,
		BootID:            wBootID,
		CandidateGPUUUIDs: []string{"GPU-0001"},
	})
	if err != nil || crashConvLease.State != "leased" {
		t.Fatalf("Acquire crashConvLease: %+v (%v)", crashConvLease, err)
	}
	if err := fix.svc.ReportResidency(ctx, comfyCaller, ResidencyReportRequest{
		InstanceID:       crashConvInst,
		NodeUID:          fix.nodeUID,
		PodUID:           wPodUID,
		ContainerID:      wCID,
		BootID:           wBootID,
		GPUEpoch:         4,
		ResidencySeq:     1,
		GPUUUID:          "GPU-0001",
		ResidencyState:   "active_compute",
		VRAMAllocatedMiB: &alloc8g,
		CallbackEndpoint: workerSrv.URL,
		TemplateUpgraded: &upgradedTrue,
	}); err != nil {
		t.Fatalf("ReportResidency crashConvInst: %v", err)
	}
	adminCaller := CallerIdentity{AppID: "station-core", Kind: CallerAdmin}
	consumerCaller := CallerIdentity{AppID: "app-resources", Kind: CallerConsumer}
	attempt1Ctx, cancelAttempt1 := context.WithCancel(ctx)
	var crashedOnce bool
	fix.svc.Hook = stageHookFunc(func(_ context.Context, stage string, meta map[string]string) {
		if stage == "after_force_drain_verify_before_commit" && meta["instance_id"] == crashConvInst && !crashedOnce {
			crashedOnce = true
			cancelAttempt1()
		}
	})
	fdCrashReq := ResidencyForceDrainRequest{
		IdempotencyKey:       "fd-crash-converge-key",
		ConfirmForceDrain:    true,
		ConfirmInstanceID:    crashConvInst,
		ExpectedBootID:       "boot-crash-conv",
		ExpectedGPUEpoch:     i64Ptr(4),
		ExpectedResidencySeq: i64Ptr(1),
		ActorUserID:          "admin-mengsk",
		OrganizationID:       "org-vf",
	}
	if _, crashErr := fix.svc.ForceDrainResidency(attempt1Ctx, adminCaller, crashConvInst, fdCrashReq); crashErr == nil {
		t.Fatalf("expected Attempt 1 ForceDrainResidency to abort before commit when ctx cancelled")
	}
	// Worker is now at zero_vram_standby (seq=3), while DB residency is still active_compute (seq=1) and lease is still leased.
	convRes, convErr := fix.svc.ForceDrainResidency(ctx, adminCaller, crashConvInst, fdCrashReq)
	if convErr != nil {
		t.Fatalf("expected retry with same IdempotencyKey after pre-commit crash to converge to 200 OK, got err=%v", convErr)
	}
	if convRes.ResidencyState != "zero_vram_standby" || convRes.FromState != "active_compute" || convRes.ReclaimedVRAMMiB != 8192 || convRes.GPUEpoch != 4 || convRes.ResidencySeq != 3 {
		t.Fatalf("unexpected converged ForceDrain result: %+v", convRes)
	}
	if err := fix.pool.QueryRow(ctx, `SELECT state FROM station.gpu_resource_leases WHERE station_id=$1 AND lease_id=$2`, fix.stationID, crashConvLease.LeaseID).Scan(&dbLeaseState); err != nil || dbLeaseState != "expired" {
		t.Fatalf("expected crashConvLease expired on retry convergence, got %q (%v)", dbLeaseState, err)
	}

	// Same crash-before-commit retry convergence check for EvictResidency (S2 -> S3):
	const evictConvInst = "comfyui-evict-crash-converge"
	wMu.Lock()
	wPodUID = "pod-evict-conv"
	wCID = "cid-evict-conv"
	wBootID = "boot-evict-conv"
	wEpoch = 6
	wSeq = 11
	wState = "idle_evictable"
	wMu.Unlock()
	fix.verifier.byKey[wPodUID] = VerifyTerminatedResult{Verified: true, GPUProcessCount: 0, MeasuredFreeVRAMMiB: 32000}
	if err := fix.svc.ReportResidency(ctx, comfyCaller, ResidencyReportRequest{
		InstanceID:       evictConvInst,
		NodeUID:          fix.nodeUID,
		PodUID:           wPodUID,
		ContainerID:      wCID,
		BootID:           wBootID,
		GPUEpoch:         6,
		ResidencySeq:     11,
		GPUUUID:          "GPU-0001",
		ResidencyState:   "idle_evictable",
		VRAMAllocatedMiB: &alloc8g,
		CallbackEndpoint: workerSrv.URL,
		TemplateUpgraded: &upgradedTrue,
	}); err != nil {
		t.Fatalf("ReportResidency evictConvInst: %v", err)
	}
	evAttempt1Ctx, cancelEvAttempt1 := context.WithCancel(ctx)
	var evCrashedOnce bool
	fix.svc.Hook = stageHookFunc(func(_ context.Context, stage string, meta map[string]string) {
		if stage == "after_evict_verify_before_commit" && meta["instance_id"] == evictConvInst && !evCrashedOnce {
			evCrashedOnce = true
			cancelEvAttempt1()
		}
	})
	evCrashReq := ResidencyEvictRequest{
		IdempotencyKey:       "ev-crash-converge-key",
		ExpectedBootID:       "boot-evict-conv",
		ExpectedGPUEpoch:     i64Ptr(6),
		ExpectedResidencySeq: i64Ptr(11),
		ActorUserID:          "member-mengsk",
	}
	if _, evCrashErr := fix.svc.EvictResidency(evAttempt1Ctx, consumerCaller, evictConvInst, evCrashReq); evCrashErr == nil {
		t.Fatalf("expected Attempt 1 EvictResidency to abort before commit when ctx cancelled")
	}
	evConvRes, evConvErr := fix.svc.EvictResidency(ctx, consumerCaller, evictConvInst, evCrashReq)
	if evConvErr != nil {
		t.Fatalf("expected EvictResidency retry with same IdempotencyKey to converge to 200 OK, got err=%v", evConvErr)
	}
	if evConvRes.ResidencyState != "zero_vram_standby" || evConvRes.FromState != "idle_evictable" || evConvRes.ReclaimedVRAMMiB != 8192 || evConvRes.GPUEpoch != 6 || evConvRes.ResidencySeq != 12 {
		t.Fatalf("unexpected converged EvictResidency result: %+v", evConvRes)
	}

	// 6. Crash AFTER worker drain + instance wakes into gpu_epoch+1 BEFORE retry:
	//    Retry with the same IdempotencyKey MUST return 409 LEASE_VERSION_CONFLICT with ZERO RevokeLease/EvictVRAM calls on the woken epoch!
	const crashWakeInst = "comfyui-crash-then-wake"
	wMu.Lock()
	wPodUID = "pod-crash-wake"
	wCID = "cid-crash-wake"
	wBootID = "boot-crash-wake"
	wEpoch = 1
	wSeq = 1
	wState = "active_compute"
	wMu.Unlock()
	fix.verifier.byKey[wPodUID] = VerifyTerminatedResult{Verified: true, GPUProcessCount: 0, MeasuredFreeVRAMMiB: 32000}
	if err := fix.svc.ReportResidency(ctx, comfyCaller, ResidencyReportRequest{
		InstanceID:       crashWakeInst,
		NodeUID:          fix.nodeUID,
		PodUID:           wPodUID,
		ContainerID:      wCID,
		BootID:           wBootID,
		GPUEpoch:         1,
		ResidencySeq:     1,
		GPUUUID:          "GPU-0001",
		ResidencyState:   "active_compute",
		VRAMAllocatedMiB: &alloc8g,
		CallbackEndpoint: workerSrv.URL,
		TemplateUpgraded: &upgradedTrue,
	}); err != nil {
		t.Fatalf("ReportResidency crashWakeInst: %v", err)
	}
	wakeAttempt1Ctx, cancelWakeAttempt1 := context.WithCancel(ctx)
	var wakeCrashedOnce bool
	fix.svc.Hook = stageHookFunc(func(_ context.Context, stage string, meta map[string]string) {
		if stage == "after_force_drain_verify_before_commit" && meta["instance_id"] == crashWakeInst && !wakeCrashedOnce {
			wakeCrashedOnce = true
			cancelWakeAttempt1()
		}
	})
	fdCrashWakeReq := ResidencyForceDrainRequest{
		IdempotencyKey:       "fd-crash-then-wake-key",
		ConfirmForceDrain:    true,
		ConfirmInstanceID:    crashWakeInst,
		ExpectedBootID:       "boot-crash-wake",
		ExpectedGPUEpoch:     i64Ptr(1),
		ExpectedResidencySeq: i64Ptr(1),
		ActorUserID:          "admin-mengsk",
		OrganizationID:       "org-vf",
	}
	if _, err := fix.svc.ForceDrainResidency(wakeAttempt1Ctx, adminCaller, crashWakeInst, fdCrashWakeReq); err == nil {
		t.Fatalf("expected Attempt 1 on crashWakeInst to abort before commit")
	}
	fix.svc.Hook = nil
	// Before the client retries, the instance wakes into gpu_epoch=2 and acquires a fresh active compute lease!
	wMu.Lock()
	wEpoch = 2
	wSeq = 5
	wState = "active_compute"
	revokeBeforeRetry := revokeCount
	evictBeforeRetry := evictCalls
	wMu.Unlock()
	wokenEpoch2Lease, err := fix.svc.Acquire(ctx, comfyCaller, AcquireRequest{
		IdempotencyKey:    "acq-woken-epoch2-after-crash",
		ConsumerKind:      "comfyui_wake",
		AttemptID:         "att-woken-epoch2-after-crash",
		TaskID:            "prompt-woken-epoch2",
		InstanceID:        crashWakeInst,
		PeakBudgetMiB:     6144,
		StartupFreeMinMiB: 6144,
		PodUID:            wPodUID,
		ContainerID:       wCID,
		BootID:            wBootID,
		CandidateGPUUUIDs: []string{"GPU-0000"},
	})
	if err != nil || wokenEpoch2Lease.State != "leased" {
		t.Fatalf("Acquire wokenEpoch2Lease: %+v (%v)", wokenEpoch2Lease, err)
	}
	if err := fix.svc.ReportResidency(ctx, comfyCaller, ResidencyReportRequest{
		InstanceID:       crashWakeInst,
		NodeUID:          fix.nodeUID,
		PodUID:           wPodUID,
		ContainerID:      wCID,
		BootID:           wBootID,
		GPUEpoch:         2,
		ResidencySeq:     5,
		GPUUUID:          "GPU-0000",
		ResidencyState:   "active_compute",
		VRAMAllocatedMiB: &alloc8g,
		CallbackEndpoint: workerSrv.URL,
		TemplateUpgraded: &upgradedTrue,
	}); err != nil {
		t.Fatalf("ReportResidency woken epoch 2: %v", err)
	}
	stWakeRetry, outWakeRetry := doJSON(coreTok, http.MethodPost, "/internal/v1/gpu-scheduler/residency/"+crashWakeInst+"/force-drain", fdCrashWakeReq)
	if stWakeRetry != http.StatusConflict || outWakeRetry["error_code"] != "LEASE_VERSION_CONFLICT" {
		t.Fatalf("expected 409 LEASE_VERSION_CONFLICT when retrying stale pre-crash intent after epoch wake, got %d %+v", stWakeRetry, outWakeRetry)
	}
	if revokeCount != revokeBeforeRetry || evictCalls != evictBeforeRetry {
		t.Fatalf("expected ZERO worker calls on woken epoch=2 during stale retry, got revoke delta=%d evict delta=%d", revokeCount-revokeBeforeRetry, evictCalls-evictBeforeRetry)
	}
	if err := fix.pool.QueryRow(ctx, `SELECT state FROM station.gpu_resource_leases WHERE station_id=$1 AND lease_id=$2`, fix.stationID, wokenEpoch2Lease.LeaseID).Scan(&newLeaseState); err != nil || newLeaseState != "leased" {
		t.Fatalf("expected wokenEpoch2Lease preserved as leased, got %q (%v)", newLeaseState, err)
	}

	snapCheck, err := fix.svc.Snapshot(ctx)
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	for _, ev := range snapCheck.RecentEvents {
		if ev.ReasonCode == "EVICT_INTENT" || ev.ReasonCode == "FORCE_DRAIN_INTENT" {
			t.Fatalf("internal intent event leaked into Snapshot.RecentEvents: %+v", ev)
		}
	}

	verifyAllCardHashChains(t, ctx, fix.pool, fix.stationID)
}

func TestUnboundLeaseFiveGateConvergenceAndBindDeadlineDB(t *testing.T) {
	fix := setupSchedulerDB(t)
	ctx := context.Background()

	imgCaller := CallerIdentity{AppID: "app-image", Kind: CallerConsumer}
	nodeCaller := CallerIdentity{AppID: "node-5090-agent", Kind: CallerNodeAgent}

	unboundDec, err := fix.svc.Acquire(ctx, imgCaller, AcquireRequest{
		IdempotencyKey:    "e01-unbound-moss-1",
		ConsumerKind:      "moss-image",
		AttemptID:         "att-unbound-1",
		CandidateGPUUUIDs: []string{"GPU-0001"},
		TTLSeconds:        300,
	})
	if err != nil || unboundDec.State != "leased" {
		t.Fatalf("Acquire unbound lease: %+v (%v)", unboundDec, err)
	}

	fix.advanceTime(61 * time.Second)
	if _, err := fix.svc.BindTarget(ctx, imgCaller, unboundDec.LeaseID, BindTargetRequest{
		IdempotencyKey: "late-bind-1",
		FencingToken:   unboundDec.FencingToken,
		PodUID:         "11111111-2222-3333-4444-555555555555",
		ContainerID:    strings.Repeat("a", 64),
		BootID:         "boot-late-1",
		ActualGPUUUIDs: []string{"GPU-0001"},
	}); !errors.Is(err, ErrFencingRevoked) {
		t.Fatalf("expected late BindTarget past 60s deadline to fail ErrFencingRevoked, got %v", err)
	}

	fix.verifier.defRes = VerifyTerminatedResult{
		Verified:            true,
		GPUProcessCount:     0,
		MeasuredFreeVRAMMiB: 32033,
		MeasuredUsedVRAMMiB: 574,
	}

	if err := fix.svc.ReconcileRestart(ctx, RestartObservation{NodeUID: fix.nodeUID, RunNonce: "e01-r1"}); err != nil {
		t.Fatalf("ReconcileRestart round 1: %v", err)
	}
	afterSeal, err := fix.svc.GetLease(ctx, imgCaller, unboundDec.LeaseID)
	if err != nil || afterSeal.State != "reconciling" {
		t.Fatalf("expected reconciling after seal round, got %+v (%v)", afterSeal, err)
	}
	if _, err := fix.svc.Heartbeat(ctx, imgCaller, unboundDec.LeaseID, HeartbeatRequest{
		IdempotencyKey: "late-hb-after-seal",
		FencingToken:   unboundDec.FencingToken,
	}); !errors.Is(err, ErrFencingRevoked) {
		t.Fatalf("expected Heartbeat after seal to fail ErrFencingRevoked, got %v", err)
	}

	fix.advanceTime(16 * time.Second)
	if err := fix.svc.ReportNodeTelemetry(ctx, nodeCaller, NodeTelemetryReport{
		NodeUID:      fix.nodeUID,
		NodeName:     "k3s-5090-dev",
		AgentBootID:  "boot-e01",
		SampleID:     20,
		AgentHealthy: true,
		Cards: []CardTelemetrySample{
			{GPUUUID: "GPU-0000", GPUIndex: 0, TotalVRAMMiB: 32607, DriverOverheadMiB: 607, MeasuredFreeVRAMMiB: 32000, MeasuredUsedVRAMMiB: 607, NVMLHealthy: true},
			{GPUUUID: "GPU-0001", GPUIndex: 1, TotalVRAMMiB: 32607, DriverOverheadMiB: 607, MeasuredFreeVRAMMiB: 32033, MeasuredUsedVRAMMiB: 574, NVMLHealthy: true},
		},
	}); err != nil {
		t.Fatal(err)
	}
	if err := fix.svc.ReconcileRestart(ctx, RestartObservation{NodeUID: fix.nodeUID, RunNonce: "e01-r2"}); err != nil {
		t.Fatal(err)
	}
	afterS1, err := fix.svc.GetLease(ctx, imgCaller, unboundDec.LeaseID)
	if err != nil || afterS1.State != "reconciling" {
		t.Fatalf("single post-grace sample must NOT expire lease, got %+v (%v)", afterS1, err)
	}

	fix.advanceTime(5 * time.Second)
	if err := fix.svc.ReportNodeTelemetry(ctx, nodeCaller, NodeTelemetryReport{
		NodeUID:      fix.nodeUID,
		NodeName:     "k3s-5090-dev",
		AgentBootID:  "boot-e01",
		SampleID:     21,
		AgentHealthy: true,
		Cards: []CardTelemetrySample{
			{GPUUUID: "GPU-0000", GPUIndex: 0, TotalVRAMMiB: 32607, DriverOverheadMiB: 607, MeasuredFreeVRAMMiB: 32000, MeasuredUsedVRAMMiB: 607, NVMLHealthy: true},
			{GPUUUID: "GPU-0001", GPUIndex: 1, TotalVRAMMiB: 32607, DriverOverheadMiB: 607, MeasuredFreeVRAMMiB: 32033, MeasuredUsedVRAMMiB: 574, NVMLHealthy: true},
		},
	}); err != nil {
		t.Fatal(err)
	}
	if err := fix.svc.ReconcileRestart(ctx, RestartObservation{NodeUID: fix.nodeUID, RunNonce: "e01-r3"}); err != nil {
		t.Fatal(err)
	}
	afterS2, err := fix.svc.GetLease(ctx, imgCaller, unboundDec.LeaseID)
	if err != nil || afterS2.State != "expired" {
		t.Fatalf("expected lease expired after 2 consecutive idle samples, got %+v (%v)", afterS2, err)
	}
	var reasonCode string
	if err := fix.pool.QueryRow(ctx, `
		SELECT reason_code FROM station.gpu_scheduler_events
		 WHERE station_id=$1 AND entity_id=$2 AND to_state='expired'
		 ORDER BY card_event_seq DESC LIMIT 1
	`, fix.stationID, unboundDec.LeaseID).Scan(&reasonCode); err != nil || reasonCode != ReasonReconciledUnboundIdleGPU {
		t.Fatalf("expected audit reason %s, got %q (%v)", ReasonReconciledUnboundIdleGPU, reasonCode, err)
	}
	snapFinal, err := fix.svc.Snapshot(ctx)
	if err != nil || snapFinal.Cards[1].State != "ready" {
		t.Fatalf("expected GPU-0001 recovered to ready, got %+v (%v)", snapFinal, err)
	}
	verifyAllCardHashChains(t, ctx, fix.pool, fix.stationID)
}
