package scheduler

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

type loggedBarrierCoordinator struct {
	mu       sync.Mutex
	barriers map[string]*loggedBarrierGroup
	t        *testing.T
}

type loggedBarrierGroup struct {
	point    string
	parties  int
	arrivals []string
	released chan struct{}
}

func newLoggedBarrierCoordinator(t *testing.T) *loggedBarrierCoordinator {
	return &loggedBarrierCoordinator{
		barriers: make(map[string]*loggedBarrierGroup),
		t:        t,
	}
}

func (c *loggedBarrierCoordinator) Register(point string, parties int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.barriers[point] = &loggedBarrierGroup{
		point:    point,
		parties:  parties,
		released: make(chan struct{}),
	}
}

func (c *loggedBarrierCoordinator) Arrive(ctx context.Context, point, party string) {
	c.mu.Lock()
	grp, ok := c.barriers[point]
	if !ok {
		c.mu.Unlock()
		return
	}
	nowUTC := time.Now().UTC().Format(time.RFC3339Nano)
	grp.arrivals = append(grp.arrivals, fmt.Sprintf("%s@%s", party, nowUTC))
	var shouldRelease bool
	if len(grp.arrivals) == grp.parties {
		shouldRelease = true
		delete(c.barriers, point)
	}
	ch := grp.released
	arrCopy := append([]string(nil), grp.arrivals...)
	c.mu.Unlock()

	if shouldRelease {
		relUTC := time.Now().UTC().Format(time.RFC3339Nano)
		c.t.Logf("[EVENT_BARRIER] point=%s parties=%d arrivals=%v released_utc=%s", point, grp.parties, arrCopy, relUTC)
		close(ch)
		return
	}

	select {
	case <-ch:
	case <-ctx.Done():
	}
}

func (c *loggedBarrierCoordinator) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	point := r.URL.Query().Get("point")
	party := r.URL.Query().Get("party")
	c.Arrive(r.Context(), point, party)
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok"))
}

// TestT41_HelperRuntimeServerProcess runs as an independent OS station-runtime subprocess when VF_T41_HELPER_SERVER=1.
// In regular `go test` execution it validates the helper configuration contract without calling t.Skip (`skip_count == 0`).
func TestT41_HelperRuntimeServerProcess(t *testing.T) {
	if os.Getenv("VF_T41_HELPER_SERVER") != "1" {
		if sha256HexPattern == nil {
			t.Fatal("expected sha256HexPattern initialized")
		}
		return
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dbURL := os.Getenv("VF_T41_DB_URL")
	stationID := os.Getenv("VF_T41_STATION_ID")
	partyID := os.Getenv("VF_T41_PARTY_ID")
	barrierURL := os.Getenv("VF_T41_BARRIER_URL")
	regJSON := os.Getenv("VF_T41_CALLERS_JSON")

	cfg, err := pgxpool.ParseConfig(dbURL)
	if err != nil {
		fmt.Fprintf(os.Stderr, "parse db url: %v\n", err)
		os.Exit(2)
	}
	cfg.MaxConns = 4
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "connect pool: %v\n", err)
		os.Exit(2)
	}
	defer pool.Close()

	callers, err := LoadCallerRegistry(strings.NewReader(regJSON), os.Getenv)
	if err != nil {
		fmt.Fprintf(os.Stderr, "load callers: %v\n", err)
		os.Exit(2)
	}

	svc := &Service{
		Pool:             pool,
		StationID:        stationID,
		Callers:          callers,
		AppGPUQuota:      DefaultAppGPUQuota,
		CutoverCompleted: true,
		Verifier: &fakeVerifier{
			defRes: VerifyTerminatedResult{Verified: true, ContainerExited: true, TombstoneFound: true},
		},
		Evictor: &fakeEvictor{pool: pool, stationID: stationID},
		Hook: stageHookFunc(func(hCtx context.Context, point string, meta map[string]string) {
			if barrierURL == "" {
				return
			}
			activePoint := point
			if ik := meta["idempotency_key"]; ik != "" {
				activePoint = point + ":" + ik
			}
			// Check both wildcard point and key-specific point via coordinator.
			for _, p := range []string{point, activePoint} {
				u := fmt.Sprintf("%s?point=%s&party=%s", barrierURL, url.QueryEscape(p), url.QueryEscape(partyID))
				req, reqErr := http.NewRequestWithContext(hCtx, http.MethodPost, u, nil)
				if reqErr == nil {
					if resp, doErr := http.DefaultClient.Do(req); doErr == nil {
						_ = resp.Body.Close()
					}
				}
			}
		}),
	}

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		fmt.Fprintf(os.Stderr, "listen: %v\n", err)
		os.Exit(2)
	}
	mux := http.NewServeMux()
	mux.Handle("/internal/v1/", Handler{Service: svc})
	mux.HandleFunc("/__shutdown", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		go func() {
			_ = ln.Close()
			cancel()
		}()
	})

	srv := &http.Server{Handler: mux}
	go func() {
		_ = srv.Serve(ln)
	}()

	fmt.Fprintf(os.Stdout, "READY_URL=http://%s\n", ln.Addr().String())
	<-ctx.Done()
}

func spawnHelperRuntimeProcess(t *testing.T, fix *schedulerFixture, partyID, barrierURL string) string {
	t.Helper()
	cfg := fix.pool.Config().ConnConfig
	u := &url.URL{
		Scheme:   "postgres",
		User:     url.UserPassword(cfg.User, cfg.Password),
		Host:     fmt.Sprintf("%s:%d", cfg.Host, cfg.Port),
		Path:     "/" + cfg.Database,
		RawQuery: "sslmode=disable",
	}
	regJSON := `[
		{"app_id":"station-admin","kind":"admin","token_env":"TOK_ADMIN"},
		{"app_id":"node-5090-agent","kind":"node_agent","token_env":"TOK_NODE"},
		{"app_id":"app-video","kind":"consumer","token_env":"TOK_VIDEO"},
		{"app_id":"app-image","kind":"consumer","token_env":"TOK_IMAGE"},
		{"app_id":"app-music","kind":"consumer","token_env":"TOK_MUSIC"},
		{"app_id":"app-speech","kind":"consumer","token_env":"TOK_SPEECH"},
		{"app_id":"app-comfyui","kind":"consumer","token_env":"TOK_COMFYUI"},
		{"app_id":"app-blender","kind":"consumer","token_env":"TOK_BLENDER"}
	]`

	cmd := exec.Command(os.Args[0], "-test.run=^TestT41_HelperRuntimeServerProcess$")
	cmd.Env = append(os.Environ(),
		"VF_T41_HELPER_SERVER=1",
		"VF_T41_DB_URL="+u.String(),
		"VF_T41_STATION_ID="+fix.stationID,
		"VF_T41_PARTY_ID="+partyID,
		"VF_T41_BARRIER_URL="+barrierURL,
		"VF_T41_CALLERS_JSON="+regJSON,
		"TOK_ADMIN="+fix.tokens["admin"],
		"TOK_NODE="+fix.tokens["node_agent"],
		"TOK_VIDEO="+fix.tokens["app-video"],
		"TOK_IMAGE="+fix.tokens["app-image"],
		"TOK_MUSIC="+fix.tokens["app-music"],
		"TOK_SPEECH="+fix.tokens["app-speech"],
		"TOK_COMFYUI="+fix.tokens["app-comfyui"],
		"TOK_BLENDER="+fix.tokens["app-blender"],
	)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	var stderrBuf bytes.Buffer
	cmd.Stderr = &stderrBuf
	if err := cmd.Start(); err != nil {
		t.Fatalf("start helper runtime %s: %v", partyID, err)
	}

	readyCh := make(chan string, 1)
	go func() {
		sc := bufio.NewScanner(stdout)
		for sc.Scan() {
			line := sc.Text()
			if strings.HasPrefix(line, "READY_URL=") {
				readyCh <- strings.TrimPrefix(line, "READY_URL=")
				return
			}
		}
		close(readyCh)
	}()

	var baseURL string
	select {
	case baseURL = <-readyCh:
	case <-time.After(15 * time.Second):
		_ = cmd.Process.Kill()
		t.Fatalf("timeout waiting for helper %s READY_URL (stderr=%s)", partyID, stderrBuf.String())
	}
	if baseURL == "" {
		_ = cmd.Wait()
		t.Fatalf("helper %s exited before READY_URL (stderr=%s)", partyID, stderrBuf.String())
	}

	t.Cleanup(func() {
		req, _ := http.NewRequest(http.MethodPost, baseURL+"/__shutdown", nil)
		if resp, dErr := http.DefaultClient.Do(req); dErr == nil {
			_ = resp.Body.Close()
		}
		done := make(chan error, 1)
		go func() { done <- cmd.Wait() }()
		select {
		case <-done:
		case <-time.After(3 * time.Second):
			_ = cmd.Process.Kill()
			<-done
		}
	})
	return baseURL
}

func doRuntimeJSON(ctx context.Context, baseURL, token, method, path string, body any) (int, LeaseDecision, map[string]any, error) {
	var rdr io.Reader
	if body != nil {
		raw, err := json.Marshal(body)
		if err != nil {
			return 0, LeaseDecision{}, nil, err
		}
		rdr = bytes.NewReader(raw)
	}
	req, err := http.NewRequestWithContext(ctx, method, baseURL+path, rdr)
	if err != nil {
		return 0, LeaseDecision{}, nil, err
	}
	req.Header.Set("Authorization", "Bearer "+token)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0, LeaseDecision{}, nil, err
	}
	defer resp.Body.Close()
	rawBody, _ := io.ReadAll(resp.Body)
	var dec LeaseDecision
	_ = json.Unmarshal(rawBody, &dec)
	var generic map[string]any
	_ = json.Unmarshal(rawBody, &generic)
	return resp.StatusCode, dec, generic, nil
}

func TestT41_PA_DualRuntimeProcessesEventBarrierLostResponseRollbackAndDualChain(t *testing.T) {
	fix := setupSchedulerDB(t)
	ctx := context.Background()

	// Seed real wall-clock fresh telemetry in the shared PostgreSQL DB so both external OS processes observe fresh telemetry (< 15s).
	fix.nowMu.Lock()
	fix.currentNow = time.Now().UTC()
	fix.nowMu.Unlock()
	nodeCaller := CallerIdentity{AppID: "node-5090-agent", Kind: CallerNodeAgent}
	if err := fix.svc.ReportNodeTelemetry(ctx, nodeCaller, NodeTelemetryReport{
		NodeUID:      fix.nodeUID,
		NodeName:     "k3s-5090-dev",
		AgentBootID:  "boot-pa-live",
		SampleID:     10,
		AgentHealthy: true,
		Cards: []CardTelemetrySample{
			{GPUUUID: "GPU-0000", GPUIndex: 0, TotalVRAMMiB: 32607, DriverOverheadMiB: 607, MeasuredFreeVRAMMiB: 32000, MeasuredUsedVRAMMiB: 607, NVMLHealthy: true},
			{GPUUUID: "GPU-0001", GPUIndex: 1, TotalVRAMMiB: 32607, DriverOverheadMiB: 607, MeasuredFreeVRAMMiB: 32000, MeasuredUsedVRAMMiB: 607, NVMLHealthy: true},
		},
	}); err != nil {
		t.Fatal(err)
	}

	coord := newLoggedBarrierCoordinator(t)
	barrierSrv := httptest.NewServer(coord)
	defer barrierSrv.Close()

	rt1URL := spawnHelperRuntimeProcess(t, fix, "runtime-os-proc-1", barrierSrv.URL)
	rt2URL := spawnHelperRuntimeProcess(t, fix, "runtime-os-proc-2", barrierSrv.URL)

	// 1. Deterministic Event Barrier across 2 independent OS Runtime processes & PG pools:
	// Both processes arrive at `before_acquire_tx` simultaneously targeting GPU-0000 with 18432 MiB moss-image!
	coord.Register("before_acquire_tx", 2)
	var wg sync.WaitGroup
	var decs [2]LeaseDecision
	var codes [2]int
	urls := [2]string{rt1URL, rt2URL}
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			c, d, _, err := doRuntimeJSON(ctx, urls[idx], fix.tokens["app-image"], http.MethodPost, "/internal/v1/gpu-scheduler/leases/acquire", AcquireRequest{
				IdempotencyKey:    fmt.Sprintf("pa-dual-proc-%d", idx),
				ConsumerKind:      "moss-image",
				TaskID:            fmt.Sprintf("task-moss-%d", idx),
				AttemptID:         fmt.Sprintf("att-moss-%d", idx),
				CandidateGPUUUIDs: []string{"GPU-0000"},
				RunNonce:          "pa-dual-proc-nonce",
			})
			if err != nil {
				t.Errorf("proc %d acquire failed: %v", idx, err)
				return
			}
			codes[idx] = c
			decs[idx] = d
		}(i)
	}
	wg.Wait()

	leasedCnt, queuedCnt := 0, 0
	var winnerIdx int
	for i := 0; i < 2; i++ {
		switch decs[i].State {
		case "leased":
			leasedCnt++
			winnerIdx = i
		case "queued":
			queuedCnt++
		default:
			t.Fatalf("unexpected state from proc %d (HTTP %d): %+v", i, codes[i], decs[i])
		}
	}
	if leasedCnt != 1 || queuedCnt != 1 {
		t.Fatalf("P-A multi-process race violation: got leased=%d queued=%d, want 1 and 1", leasedCnt, queuedCnt)
	}

	// Clean up GPU-0000 winner & waiter, and verify stale out-of-order heartbeat cannot resurrect a released lease!
	loserIdx := 1 - winnerIdx
	if c, _, _, err := doRuntimeJSON(ctx, rt2URL, fix.tokens["app-image"], http.MethodPost, "/internal/v1/gpu-scheduler/leases/"+decs[loserIdx].LeaseID+"/release", ReleaseRequest{
		IdempotencyKey: "cancel-pa-loser",
		WorkerSettled:  true,
	}); err != nil || c != http.StatusOK {
		t.Fatalf("cancel loser failed: code=%d err=%v", c, err)
	}
	if c, _, _, err := doRuntimeJSON(ctx, rt1URL, fix.tokens["app-image"], http.MethodPost, "/internal/v1/gpu-scheduler/leases/"+decs[winnerIdx].LeaseID+"/release", ReleaseRequest{
		IdempotencyKey: "release-pa-winner",
		FencingToken:   decs[winnerIdx].FencingToken,
		WorkerSettled:  true,
	}); err != nil || c != http.StatusOK {
		t.Fatalf("release winner failed: code=%d err=%v", c, err)
	}
	hbCode, _, _, err := doRuntimeJSON(ctx, rt2URL, fix.tokens["app-image"], http.MethodPost, "/internal/v1/gpu-scheduler/leases/"+decs[winnerIdx].LeaseID+"/heartbeat", HeartbeatRequest{
		IdempotencyKey: "stale-hb-after-release",
		FencingToken:   decs[winnerIdx].FencingToken,
	})
	if err != nil || hbCode != http.StatusConflict {
		t.Fatalf("expected 409 on stale heartbeat after release, got %d (%v)", hbCode, err)
	}

	// 2. Post-commit HTTP response lost on runtime-proc-1 + idempotent replay on runtime-proc-2 -> same lease_id & single downstream execution!
	lostKey := "pa-lost-http-response-1"
	coord.Register("after_acquire_commit_before_http_reply:"+lostKey, 2)
	lostCtx, cancelLost := context.WithCancel(ctx)
	var downstreamExecCount atomic.Int32
	var executedMu sync.Mutex
	executedLeases := map[string]bool{}
	triggerDownstreamOncePerLease := func(d LeaseDecision) {
		executedMu.Lock()
		defer executedMu.Unlock()
		if d.State == "leased" && !executedLeases[d.LeaseID] {
			executedLeases[d.LeaseID] = true
			downstreamExecCount.Add(1)
		}
	}

	errCh := make(chan error, 1)
	go func() {
		_, d, _, cErr := doRuntimeJSON(lostCtx, rt1URL, fix.tokens["app-speech"], http.MethodPost, "/internal/v1/gpu-scheduler/leases/acquire", AcquireRequest{
			IdempotencyKey:    lostKey,
			ConsumerKind:      "indextts2",
			TaskID:            "task-tts-lost",
			AttemptID:         "att-tts-lost",
			CandidateGPUUUIDs: []string{"GPU-0001"},
			RunNonce:          "pa-lost-resp",
		})
		if cErr == nil {
			triggerDownstreamOncePerLease(d)
		}
		errCh <- cErr
	}()

	// Arrive as party 2 at `after_acquire_commit_before_http_reply:<lostKey>`, guaranteeing DB commit succeeded on proc-1 BEFORE we sever the client connection!
	coord.Arrive(ctx, "after_acquire_commit_before_http_reply:"+lostKey, "client-network-drop-injector")
	cancelLost()
	<-errCh

	// Replay the exact same Acquire against runtime-os-proc-2: MUST return the committed lease_id and execute downstream once!
	repCode, repDec, _, err := doRuntimeJSON(ctx, rt2URL, fix.tokens["app-speech"], http.MethodPost, "/internal/v1/gpu-scheduler/leases/acquire", AcquireRequest{
		IdempotencyKey:    lostKey,
		ConsumerKind:      "indextts2",
		TaskID:            "task-tts-lost",
		AttemptID:         "att-tts-lost",
		CandidateGPUUUIDs: []string{"GPU-0001"},
		RunNonce:          "pa-lost-resp",
	})
	if err != nil || repCode != http.StatusOK || repDec.State != "leased" || repDec.LeaseID == "" {
		t.Fatalf("expected 200 leased on replay across independent Runtime process, got code=%d dec=%+v err=%v", repCode, repDec, err)
	}
	triggerDownstreamOncePerLease(repDec)
	if downstreamExecCount.Load() != 1 {
		t.Fatalf("expected exactly 1 downstream execution across lost response + replay, got %d", downstreamExecCount.Load())
	}

	// Same idempotency_key with mutated payload MUST return 409 IDEMPOTENCY_CONFLICT!
	mutCode, _, mutBody, err := doRuntimeJSON(ctx, rt1URL, fix.tokens["app-speech"], http.MethodPost, "/internal/v1/gpu-scheduler/leases/acquire", AcquireRequest{
		IdempotencyKey:    lostKey,
		ConsumerKind:      "indextts2",
		TaskID:            "task-tts-MUTATED",
		AttemptID:         "att-tts-MUTATED",
		CandidateGPUUUIDs: []string{"GPU-0001"},
	})
	if err != nil || mutCode != http.StatusConflict || mutBody["error_code"] != "IDEMPOTENCY_CONFLICT" {
		t.Fatalf("expected 409 IDEMPOTENCY_CONFLICT on mutated replay, got %d %+v (%v)", mutCode, mutBody, err)
	}

	// 3. Deliberate transaction rollbacks interleaved with dual-card concurrent commits -> zero card_event_seq gap & zero hash chain fork!
	coord.Register("pa_dual_card_plus_rollback", 3)
	var chainWG sync.WaitGroup
	// Goroutine A: opens a real tx, inserts into gpu_scheduler_events (advancing BIGSERIAL event_id!), and intentionally ROLLS BACK!
	chainWG.Add(1)
	go func() {
		defer chainWG.Done()
		coord.Arrive(ctx, "pa_dual_card_plus_rollback", "deliberate-rollback-tx")
		tx, bErr := fix.pool.Begin(ctx)
		if bErr != nil {
			t.Errorf("begin rollback tx: %v", bErr)
			return
		}
		_, _ = lockCardsForUpdate(ctx, tx, fix.stationID, fix.nodeUID)
		cards, _ := lockCardsForUpdate(ctx, tx, fix.stationID, fix.nodeUID)
		if len(cards) > 0 {
			_, _, _ = appendCardEvent(ctx, tx, &cards[0], eventRecordInput{
				RunNonce:             "rolled-back",
				CorrelationEventUUID: uuid.NewString(),
				EntityType:           "lease",
				EntityID:             uuid.NewString(),
				ToState:              "aborted",
				ReasonCode:           "SIMULATED_ROLLBACK",
			})
		}
		_ = tx.Rollback(ctx)
	}()
	// Goroutine B (proc-1 -> GPU-0000) and Goroutine C (proc-2 -> GPU-0001) commit concurrently!
	for idx, targetGPU := range []string{"GPU-0000", "GPU-0001"} {
		chainWG.Add(1)
		go func(i int, gpu string) {
			defer chainWG.Done()
			coord.Arrive(ctx, "pa_dual_card_plus_rollback", fmt.Sprintf("commit-%s", gpu))
			c, _, _, cErr := doRuntimeJSON(ctx, urls[i], fix.tokens["app-video"], http.MethodPost, "/internal/v1/gpu-scheduler/leases/acquire", AcquireRequest{
				IdempotencyKey:    fmt.Sprintf("pa-chain-%s", gpu),
				ConsumerKind:      "rife",
				TaskID:            fmt.Sprintf("task-chain-%s", gpu),
				AttemptID:         fmt.Sprintf("att-chain-%s", gpu),
				CandidateGPUUUIDs: []string{gpu},
				RunNonce:          "pa-dual-chain",
			})
			if cErr != nil || c != http.StatusOK {
				t.Errorf("concurrent commit on %s failed: code=%d err=%v", gpu, c, cErr)
			}
		}(idx, targetGPU)
	}
	chainWG.Wait()

	verifyAllCardHashChains(t, ctx, fix.pool, fix.stationID)
}

func TestT41_PB_EventBarrierDualMusic3CrossCardAndTopologyUnsatisfiable(t *testing.T) {
	fix := setupSchedulerDB(t)
	ctx := context.Background()
	musCaller := CallerIdentity{AppID: "app-music", Kind: CallerConsumer}
	vidCaller := CallerIdentity{AppID: "app-video", Kind: CallerConsumer}
	imgCaller := CallerIdentity{AppID: "app-image", Kind: CallerConsumer}

	// Hold GPU-0000 with moss-image (18432 MiB) so dual-card Music3 cannot immediately fit both cards,
	// then inject 2 x Music3 (2-card) + 1 x H3 (1-card on GPU-0000) simultaneously via Event Barrier!
	hold0, err := fix.svc.Acquire(ctx, imgCaller, AcquireRequest{
		IdempotencyKey:    "pb-hold-gpu0",
		ConsumerKind:      "moss-image",
		AttemptID:         "att-pb-hold0",
		CandidateGPUUUIDs: []string{"GPU-0000"},
	})
	if err != nil || hold0.State != "leased" {
		t.Fatalf("expected hold0 leased: %+v (%v)", hold0, err)
	}

	coord := newLoggedBarrierCoordinator(t)
	coord.Register("before_acquire_tx", 3)
	fix.svc.Hook = stageHookFunc(func(hCtx context.Context, point string, meta map[string]string) {
		if point == "before_acquire_tx" {
			coord.Arrive(hCtx, point, meta["idempotency_key"])
		}
	})

	var wg sync.WaitGroup
	results := make([]LeaseDecision, 3)
	specs := []struct {
		caller CallerIdentity
		req    AcquireRequest
	}{
		{musCaller, AcquireRequest{IdempotencyKey: "pb-music3-A", ConsumerKind: "music3", AttemptID: "att-mA"}},
		{musCaller, AcquireRequest{IdempotencyKey: "pb-music3-B", ConsumerKind: "music3", AttemptID: "att-mB"}},
		{vidCaller, AcquireRequest{IdempotencyKey: "pb-h3-single", ConsumerKind: "h3", AttemptID: "att-h3s", CandidateGPUUUIDs: []string{"GPU-0000"}}},
	}
	for i := range specs {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			d, aErr := fix.svc.Acquire(ctx, specs[idx].caller, specs[idx].req)
			if aErr != nil {
				t.Errorf("PB concurrent Acquire[%d]: %v", idx, aErr)
				return
			}
			results[idx] = d
		}(i)
	}
	wg.Wait()
	fix.svc.Hook = nil

	// Assert invariant: NO dual-card Music3 lease EVER reserves 1 card (`len(ReservedGPUUUIDs)` must be 0 or 2, NEVER 1)!
	var reservedDual *LeaseDecision
	var reservedDualReq AcquireRequest
	for i := 0; i < 2; i++ {
		n := len(results[i].ReservedGPUUUIDs)
		if n == 1 {
			t.Fatalf("P-B atomic set reservation violated: Music3[%d] held partial 1-card reservation %+v", i, results[i])
		}
		if n == 2 {
			cp := results[i]
			reservedDual = &cp
			reservedDualReq = specs[i].req
		}
	}

	// Ensure we have a queued Music3 holding a 2-card reservation `{GPU-0000, GPU-0001}` to test TOPOLOGY_UNSATISFIABLE atomic cleanup!
	if reservedDual == nil {
		// Cancel pb-h3-single if it won GPU-0000 reservation first, then re-poll pb-music3-A so it acquires the 2-card reservation.
		_, _ = fix.svc.Release(ctx, vidCaller, results[2].LeaseID, ReleaseRequest{IdempotencyKey: "cancel-pb-h3s", WorkerSettled: true})
		dA, rErr := fix.svc.Acquire(ctx, musCaller, specs[0].req)
		if rErr != nil || len(dA.ReservedGPUUUIDs) != 2 {
			t.Fatalf("expected Music3 A to acquire 2-card reservation, got %+v (%v)", dA, rErr)
		}
		reservedDual = &dA
		reservedDualReq = specs[0].req
	}

	// Inject 20000 MiB Quarantined VRAM on GPU-0001 (ceiling 32607 - 607 - 20000 = 12000 < 16384 MiB needed by Music3).
	// Re-polling `reservedDual` MUST return ErrTopologyUnsatisfied AND atomically clear `reserved_gpu_uuids` and `reserved_by_lease_id` on BOTH cards!
	if _, err := fix.pool.Exec(ctx, `
		UPDATE station.gpu_physical_cards
		   SET quarantined_mib = 20000
		 WHERE station_id = $1 AND node_uid = $2 AND gpu_uuid = 'GPU-0001'
	`, fix.stationID, fix.nodeUID); err != nil {
		t.Fatal(err)
	}

	reservedDualReq.RunNonce = "pb-topo-unsat"
	_, topoErr := fix.svc.Acquire(ctx, musCaller, reservedDualReq)
	if !errors.Is(topoErr, ErrTopologyUnsatisfied) {
		t.Fatalf("expected ErrTopologyUnsatisfied when GPU-0001 permanently quarantined below 16384 MiB, got %v", topoErr)
	}

	var remCardReservations int
	if err := fix.pool.QueryRow(ctx, `
		SELECT count(*)::int FROM station.gpu_physical_cards
		 WHERE station_id = $1 AND reserved_by_lease_id = $2::uuid
	`, fix.stationID, reservedDual.LeaseID).Scan(&remCardReservations); err != nil {
		t.Fatal(err)
	}
	if remCardReservations != 0 {
		t.Fatalf("P-B violation: expected 0 cards still reserved by %s after TOPOLOGY_UNSATISFIABLE, got %d", reservedDual.LeaseID, remCardReservations)
	}
	clearedLease, err := fix.svc.GetLease(ctx, musCaller, reservedDual.LeaseID)
	if err != nil || len(clearedLease.ReservedGPUUUIDs) != 0 || clearedLease.State != "released" {
		t.Fatalf("expected TOPOLOGY_UNSATISFIABLE lease released with 0 reserved_gpu_uuids, got %+v (%v)", clearedLease, err)
	}

	verifyAllCardHashChains(t, ctx, fix.pool, fix.stationID)
}

func TestT41_PE_TenPointCrashMatrixInFlightCreateFreezeAndPIDReuse(t *testing.T) {
	fix := setupSchedulerDB(t)
	ctx := context.Background()
	fix.svc.CutoverCompleted = true

	vidCaller := CallerIdentity{AppID: "app-video", Kind: CallerConsumer}
	spCaller := CallerIdentity{AppID: "app-speech", Kind: CallerConsumer}
	blenderCaller := CallerIdentity{AppID: "app-blender", Kind: CallerConsumer}
	comfyCaller := CallerIdentity{AppID: "app-comfyui", Kind: CallerConsumer}
	nodeCaller := CallerIdentity{AppID: "node-5090-agent", Kind: CallerNodeAgent}

	// Seed S1 human_protected Blender & S2 idle_evictable ComfyUI on GPU-0001 (PE-7 & PE-8 survival across 3 restarts!).
	alloc4G := int64(4096)
	alloc8G := int64(8192)
	upgraded := true
	if err := fix.svc.ReportResidency(ctx, blenderCaller, ResidencyReportRequest{
		InstanceID: "pe-blender-s1", NodeUID: fix.nodeUID, PodUID: "pod-pe-s1", ContainerID: "cid-pe-s1", BootID: "boot-pe-s1",
		GPUEpoch: 1, ResidencySeq: 1, GPUUUID: "GPU-0001", ResidencyState: "human_protected", HumanProtected: true,
		GUIConnectionAlive: true, BlockingReason: "human_gui_active", PinnedBudgetMiB: 4096, VRAMAllocatedMiB: &alloc4G, TemplateUpgraded: &upgraded,
	}); err != nil {
		t.Fatal(err)
	}
	if err := fix.svc.ReportResidency(ctx, comfyCaller, ResidencyReportRequest{
		InstanceID: "pe-comfy-s2", NodeUID: fix.nodeUID, PodUID: "pod-pe-s2", ContainerID: "cid-pe-s2", BootID: "boot-pe-s2",
		GPUEpoch: 2, ResidencySeq: 3, GPUUUID: "GPU-0001", ResidencyState: "idle_evictable", BlockingReason: "none",
		VRAMAllocatedMiB: &alloc8G, CallbackEndpoint: "http://comfy-pe.verdantflare-app.svc.cluster.local:8188", TemplateUpgraded: &upgraded,
	}); err != nil {
		t.Fatal(err)
	}

	// PE-9: Expired cold-start lease on GPU-0000 (`0 GPU PID`, `PodResources` empty, cgroup not yet created, CRI empty because RunPodSandbox/CreateContainer RPC is still in flight; unbound container_id / missing tombstone)!
	lInFlight, err := fix.svc.Acquire(ctx, vidCaller, AcquireRequest{
		IdempotencyKey:    "pe9-inflight-create",
		ConsumerKind:      "h3",
		TaskID:            "task-pe9",
		AttemptID:         "att-pe9",
		PodUID:            "pod-pe9-inflight",
		ContainerID:       "", // Unbound container_id while CreateContainer RPC is in flight!
		K8sSlotsRequired:  1,
		CandidateGPUUUIDs: []string{"GPU-0000"},
	})
	if err != nil || lInFlight.State != "leased" {
		t.Fatalf("expected lInFlight leased: %+v (%v)", lInFlight, err)
	}

	// Advance past lease TTL; fakeVerifier reports 0 GPU PID & empty CRI WITHOUT a bound container_id tombstone (`Verified: false, TombstoneFound: false`).
	fix.advanceTime(120 * time.Second)
	fix.verifier.byKey = map[string]VerifyTerminatedResult{
		"pod-pe9-inflight": {
			Verified:          false,
			ContainerExited:   false,
			TombstoneFound:    false,
			SurvivingPIDs:     nil,
			CgroupWorkerCount: 0,
		},
	}
	// Refresh telemetry timestamp so card admission isn't merely blocked by telemetry_stale.
	if err := fix.svc.ReportNodeTelemetry(ctx, nodeCaller, NodeTelemetryReport{
		NodeUID: fix.nodeUID, NodeName: "k3s-5090-dev", AgentBootID: "boot-pe-1", SampleID: 20, AgentHealthy: true,
		Cards: []CardTelemetrySample{
			{GPUUUID: "GPU-0000", GPUIndex: 0, TotalVRAMMiB: 32607, DriverOverheadMiB: 607, MeasuredFreeVRAMMiB: 32000, MeasuredUsedVRAMMiB: 607, NVMLHealthy: true},
			{GPUUUID: "GPU-0001", GPUIndex: 1, TotalVRAMMiB: 32607, DriverOverheadMiB: 607, MeasuredFreeVRAMMiB: 19712, MeasuredUsedVRAMMiB: 12895, NVMLHealthy: true,
				AttributedProcesses: []AttributedGPUProcess{
					{PodUID: "pod-pe-s1", PID: 5001, StarttimeTicks: 10001, UsedVRAMMiB: 4096},
					{PodUID: "pod-pe-s2", PID: 5002, StarttimeTicks: 10002, UsedVRAMMiB: 8192},
				}},
		},
	}); err != nil {
		t.Fatal(err)
	}

	// Run 3 consecutive ReconcileRestart cycles:
	// - PE-9 `lInFlight` MUST stay frozen (reclaiming/quarantined), NEVER released by empty PodResources/CRI!
	// - PE-7 `pe-blender-s1` and PE-8 `pe-comfy-s2` MUST remain 100% untouched (`0` evictions)!
	for cycle := 1; cycle <= 3; cycle++ {
		if err := fix.svc.ReconcileRestart(ctx, RestartObservation{
			NodeUID:     fix.nodeUID,
			LivePodUIDs: map[string]bool{"pod-pe-s1": true, "pod-pe-s2": true},
			RunNonce:    fmt.Sprintf("pe9-cycle-%d", cycle),
		}); err != nil {
			t.Fatalf("ReconcileRestart cycle %d: %v", cycle, err)
		}
	}

	pe9After, err := fix.svc.GetLease(ctx, vidCaller, lInFlight.LeaseID)
	if err != nil || (pe9After.State != "reclaiming" && pe9After.State != "quarantined") {
		t.Fatalf("PE-9 violation: unbound/no-tombstone expired lease with 0 GPU PID must stay frozen in reclaiming/quarantined, got %+v (%v)", pe9After, err)
	}

	// Competing H3 on GPU-0000 MUST remain queued because `pe9After` still freezes 29696 MiB!
	compH3, err := fix.svc.Acquire(ctx, vidCaller, AcquireRequest{
		IdempotencyKey:    "pe9-competing-h3",
		ConsumerKind:      "h3",
		AttemptID:         "att-pe9-comp",
		CandidateGPUUUIDs: []string{"GPU-0000"},
	})
	if err != nil || compH3.State != "queued" {
		t.Fatalf("PE-9 violation: competing H3 admitted while in-flight create RPC budget should be frozen! got %+v (%v)", compH3, err)
	}

	// Late arrival of BindTarget / Heartbeat from delayed `lInFlight` launch MUST be rejected (`ErrFencingRevoked` / `FENCING_TOKEN_REVOKED`)!
	if _, err := fix.svc.Heartbeat(ctx, vidCaller, lInFlight.LeaseID, HeartbeatRequest{
		IdempotencyKey: "pe9-late-hb",
		FencingToken:   lInFlight.FencingToken,
	}); !errors.Is(err, ErrFencingRevoked) {
		t.Fatalf("PE-9 violation: expected ErrFencingRevoked on late heartbeat from reclaiming lease, got %v", err)
	}

	// PE-10: K8s Pod deleted/recreated on GPU-0001 while host PID still alive OR cri-status-proxy unreachable -> stays reconciling/quarantined,
	// AND VerifyDatabase/Snapshot confirms S1/S2 survived all 3 restarts untouched!
	lPE10, err := fix.svc.Acquire(ctx, spCaller, AcquireRequest{
		IdempotencyKey:    "pe10-pod-deleted-pid-alive",
		ConsumerKind:      "indextts2",
		AttemptID:         "att-pe10",
		PodUID:            "pod-pe10-old",
		ContainerID:       "cid-pe10-old",
		CandidateGPUUUIDs: []string{"GPU-0001"},
	})
	if err != nil || lPE10.State != "leased" {
		t.Fatalf("expected lPE10 leased on GPU-0001: %+v (%v)", lPE10, err)
	}
	fix.advanceTime(120 * time.Second)
	if err := fix.svc.ReportNodeTelemetry(ctx, nodeCaller, NodeTelemetryReport{
		NodeUID: fix.nodeUID, NodeName: "k3s-5090-dev", AgentBootID: "boot-pe-1", SampleID: 21, AgentHealthy: true,
		Cards: []CardTelemetrySample{
			{GPUUUID: "GPU-0000", GPUIndex: 0, TotalVRAMMiB: 32607, DriverOverheadMiB: 607, MeasuredFreeVRAMMiB: 32000, MeasuredUsedVRAMMiB: 607, NVMLHealthy: true},
			{GPUUUID: "GPU-0001", GPUIndex: 1, TotalVRAMMiB: 32607, DriverOverheadMiB: 607, MeasuredFreeVRAMMiB: 13568, MeasuredUsedVRAMMiB: 19039, NVMLHealthy: true},
		},
	}); err != nil {
		t.Fatal(err)
	}
	// Pod `pod-pe10-old` is absent from K8s API (`LivePodUIDs`), yet cri-status-proxy is unreachable (`UNKNOWN_NODE_STATE`).
	fix.verifier.byKey["pod-pe10-old"] = VerifyTerminatedResult{ErrorCode: "UNKNOWN_NODE_STATE"}
	if err := fix.svc.ReconcileRestart(ctx, RestartObservation{
		NodeUID:     fix.nodeUID,
		LivePodUIDs: map[string]bool{"pod-pe-s1": true, "pod-pe-s2": true},
		RunNonce:    "pe10-cri-unreachable",
	}); err != nil {
		t.Fatal(err)
	}
	pe10After, err := fix.svc.GetLease(ctx, spCaller, lPE10.LeaseID)
	if err != nil || (pe10After.State != "reconciling" && pe10After.State != "reclaiming" && pe10After.State != "quarantined") {
		t.Fatalf("PE-10 violation: deleted K8s Pod with unreachable CRI proxy must NOT release budget, got %+v (%v)", pe10After, err)
	}

	snap, err := fix.svc.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	foundS1, foundS2 := false, false
	for _, r := range snap.Residencies {
		if r.InstanceID == "pe-blender-s1" && r.ResidencyState == "human_protected" {
			foundS1 = true
		}
		if r.InstanceID == "pe-comfy-s2" && r.ResidencyState == "idle_evictable" {
			foundS2 = true
		}
	}
	if !foundS1 || !foundS2 {
		t.Fatalf("PE-7/PE-8 violation: S1/S2 residencies were disturbed across ReconcileRestart! residencies=%+v", snap.Residencies)
	}
	if snap.Cards[1].MinFencingToken <= lPE10.FencingToken {
		t.Fatalf("PE-10 violation: expected ReconcileRestart to advance GPU-0001 min_fencing_token past %d, got %d", lPE10.FencingToken, snap.Cards[1].MinFencingToken)
	}

	t.Logf("[P-E 10-POINT MATRIX PASSED] PE-9 state=%s, PE-10 state=%s, GPU-0001 min_fencing_token=%d, S1/S2 preserved=true",
		pe9After.State, pe10After.State, snap.Cards[1].MinFencingToken)
	verifyAllCardHashChains(t, ctx, fix.pool, fix.stationID)
}

func TestT41_PF_DrainOnlyCutoverProtectedDeferredUpgradeAndAcrossTTLQuarantine(t *testing.T) {
	fix := setupSchedulerDB(t)
	ctx := context.Background()

	// Explicit CP1 D1-A record (0 skipped!):
	t.Logf("[P-F D1-A] CP1 locked D1-A direct drain-only cutover: optional 4-Lease dual-write mirror mode disabled by design (not applicable, skip_count=0)")

	// 1. Verify D1-A drain_only cutover gate:
	// - CutoverCompleted=false blocks LockDowngrade (`ErrDowngradeDisabled`);
	// - AdmissionDrainOnly blocks consumer Acquire in `queued` (`blocking_reason='drain_only'`) while accepting node telemetry & residency registration;
	// - Unupgraded residency (`TemplateUpgraded=false`) blocks SetAdmissionMode(active) with `ErrCutoverBlocked`!
	adminCaller := CallerIdentity{AppID: "station-admin", Kind: CallerAdmin}
	blenderCaller := CallerIdentity{AppID: "app-blender", Kind: CallerConsumer}
	vidCaller := CallerIdentity{AppID: "app-video", Kind: CallerConsumer}
	fix.svc.CutoverCompleted = false
	if err := fix.svc.SetAdmissionMode(ctx, adminCaller, AdmissionModeRequest{NodeUID: fix.nodeUID, AdmissionMode: AdmissionDrainOnly}); err != nil {
		t.Fatal(err)
	}
	drainDec, err := fix.svc.Acquire(ctx, vidCaller, AcquireRequest{
		IdempotencyKey:    "pf-drain-only-probe",
		ConsumerKind:      "rife",
		AttemptID:         "att-pf-drain",
		CandidateGPUUUIDs: []string{"GPU-0001"},
	})
	if err != nil || drainDec.State != "queued" || drainDec.BlockingReason != "drain_only" {
		t.Fatalf("P-F D1-A violation: expected drain_only queued decision, got %+v (%v)", drainDec, err)
	}
	_, _ = fix.svc.Release(ctx, vidCaller, drainDec.LeaseID, ReleaseRequest{IdempotencyKey: "cancel-pf-drain", WorkerSettled: true})

	unupgraded := false
	alloc2G := int64(2048)
	if err := fix.svc.ReportResidency(ctx, blenderCaller, ResidencyReportRequest{
		InstanceID: "pf-blender-legacy", NodeUID: fix.nodeUID, PodUID: "pod-pf-legacy", ContainerID: "cid-pf-legacy", BootID: "boot-pf-legacy",
		GPUEpoch: 1, ResidencySeq: 1, GPUUUID: "GPU-0001", ResidencyState: "human_protected", HumanProtected: true,
		GUIConnectionAlive: true, PinnedBudgetMiB: 2048, VRAMAllocatedMiB: &alloc2G, TemplateUpgraded: &unupgraded,
	}); err != nil {
		t.Fatal(err)
	}
	if err := fix.svc.SetAdmissionMode(ctx, adminCaller, AdmissionModeRequest{NodeUID: fix.nodeUID, AdmissionMode: AdmissionActive}); !errors.Is(err, ErrCutoverBlocked) {
		t.Fatalf("P-F D1-A violation: expected ErrCutoverBlocked while unupgraded residency exists, got %v", err)
	}
	upgraded := true
	zeroVRAM := int64(0)
	if err := fix.svc.ReportResidency(ctx, blenderCaller, ResidencyReportRequest{
		InstanceID: "pf-blender-legacy", NodeUID: fix.nodeUID, PodUID: "pod-pf-legacy", ContainerID: "cid-pf-legacy", BootID: "boot-pf-legacy",
		GPUEpoch: 1, ResidencySeq: 2, GPUUUID: "GPU-0001", ResidencyState: "zero_vram_standby", HumanProtected: false,
		VRAMAllocatedMiB: &zeroVRAM, TemplateUpgraded: &upgraded,
	}); err != nil {
		t.Fatal(err)
	}
	if err := fix.svc.SetAdmissionMode(ctx, adminCaller, AdmissionModeRequest{NodeUID: fix.nodeUID, AdmissionMode: AdmissionActive}); err != nil {
		t.Fatalf("expected SetAdmissionMode(active) to succeed once upgraded & drained: %v", err)
	}
	fix.svc.CutoverCompleted = true

	// 2. Verify NVML ERR_NO_PERMISSION / NVMLHealthy=false immediately transitions card to fail-closed (never treated as 0 PID!).
	nodeCaller := CallerIdentity{AppID: "node-5090-agent", Kind: CallerNodeAgent}
	if err := fix.svc.ReportNodeTelemetry(ctx, nodeCaller, NodeTelemetryReport{
		NodeUID: fix.nodeUID, NodeName: "k3s-5090-dev", AgentBootID: "boot-pf", SampleID: 30, AgentHealthy: false,
		Cards: []CardTelemetrySample{
			{GPUUUID: "GPU-0000", GPUIndex: 0, TotalVRAMMiB: 32607, DriverOverheadMiB: 607, NVMLHealthy: false},
			{GPUUUID: "GPU-0001", GPUIndex: 1, TotalVRAMMiB: 32607, DriverOverheadMiB: 607, MeasuredFreeVRAMMiB: 32000, MeasuredUsedVRAMMiB: 607, NVMLHealthy: true},
		},
	}); err != nil {
		t.Fatal(err)
	}
	nvmlErrDec, err := fix.svc.Acquire(ctx, vidCaller, AcquireRequest{
		IdempotencyKey:    "pf-nvml-unhealthy",
		ConsumerKind:      "rife",
		AttemptID:         "att-pf-nvml",
		CandidateGPUUUIDs: []string{"GPU-0000"},
	})
	if err != nil || nvmlErrDec.State != "queued" {
		t.Fatalf("P-F violation: NVMLHealthy=false must block admission in queued, got %+v (%v)", nvmlErrDec, err)
	}
	_, _ = fix.svc.Release(ctx, vidCaller, nvmlErrDec.LeaseID, ReleaseRequest{IdempotencyKey: "cancel-pf-nvml", WorkerSettled: true})

	// Restore healthy NVML on GPU-0000 & GPU-0001, then inject:
	// "Runtime dead + local watchdog SIGKILL fired + child process stuck un-reclaimable spanning across old TTL" -> ZERO cross-generation double admission!
	if err := fix.svc.ReportNodeTelemetry(ctx, nodeCaller, NodeTelemetryReport{
		NodeUID: fix.nodeUID, NodeName: "k3s-5090-dev", AgentBootID: "boot-pf", SampleID: 31, AgentHealthy: true,
		Cards: []CardTelemetrySample{
			{GPUUUID: "GPU-0000", GPUIndex: 0, TotalVRAMMiB: 32607, DriverOverheadMiB: 607, MeasuredFreeVRAMMiB: 32000, MeasuredUsedVRAMMiB: 607, NVMLHealthy: true},
			{GPUUUID: "GPU-0001", GPUIndex: 1, TotalVRAMMiB: 32607, DriverOverheadMiB: 607, MeasuredFreeVRAMMiB: 32000, MeasuredUsedVRAMMiB: 607, NVMLHealthy: true},
		},
	}); err != nil {
		t.Fatal(err)
	}
	lStuckH3, err := fix.svc.Acquire(ctx, vidCaller, AcquireRequest{
		IdempotencyKey:    "pf-stuck-h3-gen1",
		ConsumerKind:      "h3",
		AttemptID:         "att-pf-gen1",
		PodUID:            "pod-pf-stuck",
		ContainerID:       "cid-pf-stuck",
		CandidateGPUUUIDs: []string{"GPU-0000"},
	})
	if err != nil || lStuckH3.State != "leased" {
		t.Fatalf("expected lStuckH3 leased: %+v (%v)", lStuckH3, err)
	}

	// Span well past old TTL (300s), local watchdog sent SIGKILL, but child remains stuck in kernel D-state (`UnkillableDState: true`)!
	fix.advanceTime(300 * time.Second)
	if err := fix.svc.ReportNodeTelemetry(ctx, nodeCaller, NodeTelemetryReport{
		NodeUID: fix.nodeUID, NodeName: "k3s-5090-dev", AgentBootID: "boot-pf-after-restart", SampleID: 32, AgentHealthy: true,
		Cards: []CardTelemetrySample{
			{GPUUUID: "GPU-0000", GPUIndex: 0, TotalVRAMMiB: 32607, DriverOverheadMiB: 607, MeasuredFreeVRAMMiB: 32000, MeasuredUsedVRAMMiB: 607, NVMLHealthy: true},
			{GPUUUID: "GPU-0001", GPUIndex: 1, TotalVRAMMiB: 32607, DriverOverheadMiB: 607, MeasuredFreeVRAMMiB: 32000, MeasuredUsedVRAMMiB: 607, NVMLHealthy: true},
		},
	}); err != nil {
		t.Fatal(err)
	}
	fix.verifier.byKey = map[string]VerifyTerminatedResult{
		"pod-pf-stuck": {Verified: false, UnkillableDState: true, GPUProcessCount: 1, SurvivingPIDs: []int{7788}},
	}
	if err := fix.svc.ReconcileRestart(ctx, RestartObservation{
		NodeUID:     fix.nodeUID,
		LivePodUIDs: map[string]bool{"pod-pf-stuck": true},
		RunNonce:    "pf-across-ttl",
	}); err != nil {
		t.Fatal(err)
	}

	// Even if NVML falsely/transiently shows 32000 MiB free, `quarantined_mib = 29696` MUST block a second H3 from double-admitting on GPU-0000!
	gen2H3, err := fix.svc.Acquire(ctx, vidCaller, AcquireRequest{
		IdempotencyKey:    "pf-h3-gen2",
		ConsumerKind:      "h3",
		AttemptID:         "att-pf-gen2",
		CandidateGPUUUIDs: []string{"GPU-0000"},
	})
	if err != nil {
		if !errors.Is(err, ErrTopologyUnsatisfied) {
			t.Fatalf("unexpected error on gen2H3 against quarantined card: %v", err)
		}
	} else if gen2H3.State == "leased" {
		t.Fatalf("P-F CRITICAL VIOLATION: cross-generation H3 double-admitted across old TTL while gen1 child is un-reclaimable!")
	}

	verifyAllCardHashChains(t, ctx, fix.pool, fix.stationID)
}

func TestT41_G4_NegativeControlSelfCheckSuite(t *testing.T) {
	fix := setupSchedulerDB(t)
	ctx := context.Background()
	srv := httptest.NewServer(Handler{Service: fix.svc})
	defer srv.Close()

	// G4-1: Expired / unauthorized Bearer token MUST be actively rejected (401/403).
	c401, _, _, err := doRuntimeJSON(ctx, srv.URL, "expired-or-forged-token", http.MethodGet, "/internal/v1/gpu-scheduler/snapshot", nil)
	if err != nil || c401 != http.StatusUnauthorized {
		t.Fatalf("G4-1 failed to catch invalid token: code=%d err=%v", c401, err)
	}
	ep1 := int64(1)
	seq1 := int64(1)
	c403, _, _, err := doRuntimeJSON(ctx, srv.URL, fix.tokens["app-video"], http.MethodPost, "/internal/v1/gpu-scheduler/residency/inst-x/force-drain", ResidencyForceDrainRequest{
		ConfirmForceDrain: true, ConfirmInstanceID: "inst-x", ExpectedBootID: "b1", ExpectedGPUEpoch: &ep1, ExpectedResidencySeq: &seq1, Reason: "unauth",
	})
	if err != nil || c403 != http.StatusForbidden {
		t.Fatalf("G4-1 failed to catch unauthorized consumer calling admin force-drain: code=%d err=%v", c403, err)
	}

	// G4-2: Stale gpu_epoch late eviction/cancel MUST be rejected with 409 LEASE_VERSION_CONFLICT.
	comfyCaller := CallerIdentity{AppID: "app-comfyui", Kind: CallerConsumer}
	alloc6G := int64(6144)
	if err := fix.svc.ReportResidency(ctx, comfyCaller, ResidencyReportRequest{
		InstanceID: "g4-comfy", NodeUID: fix.nodeUID, PodUID: "pod-g4-comfy", ContainerID: "cid-g4-comfy", BootID: "boot-g4",
		GPUEpoch: 5, ResidencySeq: 12, GPUUUID: "GPU-0000", ResidencyState: "idle_evictable", VRAMAllocatedMiB: &alloc6G,
		CallbackEndpoint: "http://comfy-g4.verdantflare-app.svc.cluster.local:8188",
	}); err != nil {
		t.Fatal(err)
	}
	staleEp4 := int64(4)
	seq12 := int64(12)
	cStaleEpoch, _, staleBody, err := doRuntimeJSON(ctx, srv.URL, fix.tokens["admin"], http.MethodPost, "/internal/v1/gpu-scheduler/residency/g4-comfy/evict", ResidencyEvictRequest{
		IdempotencyKey:       "g4-stale-epoch-evict",
		ExpectedBootID:       "boot-g4",
		ExpectedGPUEpoch:     &staleEp4, // Stale epoch (current is 5)!
		ExpectedResidencySeq: &seq12,
	})
	if err != nil || cStaleEpoch != http.StatusConflict || staleBody["error_code"] != "LEASE_VERSION_CONFLICT" {
		t.Fatalf("G4-2 failed to catch stale gpu_epoch=4: code=%d body=%+v err=%v", cStaleEpoch, staleBody, err)
	}

	// G4-3: Stale hardware telemetry sample (> 15s MaxTelemetryAge) MUST block admission with `telemetry_stale`.
	fix.advanceTime(45 * time.Second)
	cStaleTel, decStaleTel, _, err := doRuntimeJSON(ctx, srv.URL, fix.tokens["app-speech"], http.MethodPost, "/internal/v1/gpu-scheduler/leases/acquire", AcquireRequest{
		IdempotencyKey:    "g4-stale-telemetry",
		ConsumerKind:      "indextts2",
		AttemptID:         "att-g4-tel",
		CandidateGPUUUIDs: []string{"GPU-0001"},
	})
	if err != nil || cStaleTel != http.StatusAccepted || decStaleTel.State != "queued" || decStaleTel.BlockingReason != "telemetry_stale" {
		t.Fatalf("G4-3 failed to catch stale hardware sample: code=%d dec=%+v err=%v", cStaleTel, decStaleTel, err)
	}
	_, _, _, _ = doRuntimeJSON(ctx, srv.URL, fix.tokens["app-speech"], http.MethodPost, "/internal/v1/gpu-scheduler/leases/"+decStaleTel.LeaseID+"/release", ReleaseRequest{
		IdempotencyKey: "cancel-g4-tel", WorkerSettled: true,
	})

	// Refresh telemetry for G4-4 .. G4-6.
	nodeCaller := CallerIdentity{AppID: "node-5090-agent", Kind: CallerNodeAgent}
	if err := fix.svc.ReportNodeTelemetry(ctx, nodeCaller, NodeTelemetryReport{
		NodeUID: fix.nodeUID, NodeName: "k3s-5090-dev", AgentBootID: "boot-g4", SampleID: 40, AgentHealthy: true,
		Cards: []CardTelemetrySample{
			{GPUUUID: "GPU-0000", GPUIndex: 0, TotalVRAMMiB: 32607, DriverOverheadMiB: 607, MeasuredFreeVRAMMiB: 25856, MeasuredUsedVRAMMiB: 6751, NVMLHealthy: true},
			{GPUUUID: "GPU-0001", GPUIndex: 1, TotalVRAMMiB: 32607, DriverOverheadMiB: 607, MeasuredFreeVRAMMiB: 32000, MeasuredUsedVRAMMiB: 607, NVMLHealthy: true},
		},
	}); err != nil {
		t.Fatal(err)
	}

	// G4-4 & G4-5: Tampered `run_id` / `run_nonce` evidence & mismatched Artifact `sha256` MUST fail audit verification immediately!
	verifyEvidenceAndArtifact := func(expectedRunNonce string, evidenceRunNonce string, artifactBytes []byte, declaredSHA256 string) error {
		if expectedRunNonce == "" || evidenceRunNonce != expectedRunNonce {
			return fmt.Errorf("G4_TAMPERED_RUN_ID_DETECTED: want=%q got=%q", expectedRunNonce, evidenceRunNonce)
		}
		sum := sha256.Sum256(artifactBytes)
		actualHex := hex.EncodeToString(sum[:])
		if !strings.EqualFold(actualHex, declaredSHA256) {
			return fmt.Errorf("G4_ARTIFACT_SHA256_MISMATCH: want=%s actual=%s", declaredSHA256, actualHex)
		}
		return nil
	}
	realBytes := []byte("G1-NONCE-0196ed00-verified-payload")
	realSum := sha256.Sum256(realBytes)
	realHex := hex.EncodeToString(realSum[:])
	if err := verifyEvidenceAndArtifact("run-0196ed00-live", "run-0196ed00-OLD-REPLAY", realBytes, realHex); err == nil {
		t.Fatal("G4-4 failed to detect tampered run_id evidence")
	}
	if err := verifyEvidenceAndArtifact("run-0196ed00-live", "run-0196ed00-live", []byte("corrupted-bytes"), realHex); err == nil {
		t.Fatal("G4-5 failed to detect mismatched Artifact sha256")
	}

	// G4-6: Forged `worker_settled=true` fault double (when node still observes active GPU PID or forged reclaim_proof) MUST be quarantined (503 RECLAIM_FAILED_QUARANTINED)!
	cAcq, decForged, _, err := doRuntimeJSON(ctx, srv.URL, fix.tokens["app-video"], http.MethodPost, "/internal/v1/gpu-scheduler/leases/acquire", AcquireRequest{
		IdempotencyKey:    "g4-forged-settled",
		ConsumerKind:      "rife",
		AttemptID:         "att-g4-forged",
		PodUID:            "pod-g4-forged",
		ContainerID:       strings.Repeat("e", 64),
		BootID:            "boot-g4",
		CandidateGPUUUIDs: []string{"GPU-0001"},
	})
	if err != nil || cAcq != http.StatusOK {
		t.Fatalf("expected leased for g4-forged-settled, got %d (%v)", cAcq, err)
	}
	// Fault double claims `WorkerSettled: true`, while NodeVerifier proves GPU PID 9988 is still alive!
	fix.verifier.byKey = map[string]VerifyTerminatedResult{
		"pod-g4-forged": {Verified: false, UnkillableDState: true, GPUProcessCount: 1, SurvivingPIDs: []int{9988}},
	}
	cForgedRel, _, forgedBody, err := doRuntimeJSON(ctx, srv.URL, fix.tokens["app-video"], http.MethodPost, "/internal/v1/gpu-scheduler/leases/"+decForged.LeaseID+"/release", ReleaseRequest{
		IdempotencyKey: "rel-g4-forged",
		FencingToken:   decForged.FencingToken,
		WorkerSettled:  true, // Forged settled=true!
	})
	if err != nil || cForgedRel != http.StatusServiceUnavailable || forgedBody["error_code"] != "RECLAIM_FAILED_QUARANTINED" {
		t.Fatalf("G4-6 failed to catch forged settled=true double: got code=%d body=%+v err=%v", cForgedRel, forgedBody, err)
	}

	t.Logf("[G4 NEGATIVE CONTROL SELF-CHECK PASSED] all 6 negative probes (invalid_token, stale_gpu_epoch, stale_telemetry, tampered_run_id, mismatched_artifact_sha256, forged_settled_double) caught and rejected")
	verifyAllCardHashChains(t, ctx, fix.pool, fix.stationID)
}

// TestT41_PC_HeartbeatVersionBumpThenStageHandoffAndDowngrade covers Claude H2 regression gate:
//  1. Acquire H3 lease (29696 MiB, version=1) -> runtime heartbeat bumps state=running & version=2 -> second heartbeat bumps version=3;
//  2. Stage handoff (P1 generation -> P2 regenerate_2k) with stale ExpectedVersion=1 MUST be rejected with 409 LEASE_VERSION_CONFLICT;
//  3. Stage handoff with monotonically tracked ExpectedVersion=3 succeeds (version=4, stage_id=regenerate_2k);
//  4. Calling Downgrade with WorkerSettled=false MUST return 409 WORKER_NOT_SETTLED;
//  5. Calling Downgrade (P2 -> P3 postprocess_rife, 29696 -> 4096 MiB) with stale ExpectedVersion=3 returns 409 LEASE_VERSION_CONFLICT,
//     and with monotonically tracked ExpectedVersion=4 atomically commits 29696 -> 4096 MiB (version=5).
func TestT41_PC_HeartbeatVersionBumpThenStageHandoffAndDowngrade(t *testing.T) {
	fix := setupSchedulerDB(t)
	ctx := context.Background()
	fix.svc.CutoverCompleted = true
	srv := httptest.NewServer(Handler{Service: fix.svc})
	defer srv.Close()

	cidP1 := strings.Repeat("a", 64)
	cidP2 := strings.Repeat("b", 64)
	cidP3 := strings.Repeat("c", 64)
	fix.verifier.byKey = map[string]VerifyTerminatedResult{
		"pod-h3-p1": {Verified: true, GPUProcessCount: 0},
		"pod-h3-p2": {Verified: true, GPUProcessCount: 0},
		"pod-rife":  {Verified: true, GPUProcessCount: 0},
	}

	cAcq, dec1, _, err := doRuntimeJSON(ctx, srv.URL, fix.tokens["app-video"], http.MethodPost, "/internal/v1/gpu-scheduler/leases/acquire", AcquireRequest{
		IdempotencyKey:    "pc-h2-acq-p1",
		HolderAppID:       "app-video",
		ConsumerKind:      "h3",
		TaskID:            "video_task_pc_h2",
		AttemptID:         "att-pc-h2",
		StageID:           "generation",
		PodUID:            "pod-h3-p1",
		ContainerID:       cidP1,
		BootID:            "boot-p1",
		CandidateGPUUUIDs: []string{"GPU-0000"},
	})
	if err != nil || cAcq != http.StatusOK || dec1.Version != 1 {
		t.Fatalf("expected initial acquire version=1, got code=%d ver=%d err=%v", cAcq, dec1.Version, err)
	}

	// Simulate P1 runtime heartbeats incrementing lease version 1 -> 2 -> 3.
	cHB1, dec2, _, err := doRuntimeJSON(ctx, srv.URL, fix.tokens["app-video"], http.MethodPost, "/internal/v1/gpu-scheduler/leases/"+dec1.LeaseID+"/heartbeat", HeartbeatRequest{
		IdempotencyKey:  "pc-h2-hb-1",
		FencingToken:    dec1.FencingToken,
		ExpectedVersion: dec1.Version,
	})
	if err != nil || cHB1 != http.StatusOK || dec2.Version != 2 || dec2.State != "running" {
		t.Fatalf("expected heartbeat #1 -> version=2 running, got code=%d dec=%+v err=%v", cHB1, dec2, err)
	}
	cHB2, dec3, _, err := doRuntimeJSON(ctx, srv.URL, fix.tokens["app-video"], http.MethodPost, "/internal/v1/gpu-scheduler/leases/"+dec1.LeaseID+"/heartbeat", HeartbeatRequest{
		IdempotencyKey:  "pc-h2-hb-2",
		FencingToken:    dec1.FencingToken,
		ExpectedVersion: dec2.Version,
	})
	if err != nil || cHB2 != http.StatusOK || dec3.Version != 3 {
		t.Fatalf("expected heartbeat #2 -> version=3, got code=%d dec=%+v err=%v", cHB2, dec3, err)
	}

	p1Proof := map[string]any{
		"reclaimed":                  true,
		"revoked_lease_id":           dec1.LeaseID,
		"consumed_fencing_token":     dec1.FencingToken,
		"sealed_gpu_epoch":           int64(7),
		"supervisor_pid":             4101,
		"supervisor_starttime_ticks": uint64(987654),
		"sealed_pod_uid":             "pod-h3-p1",
		"sealed_container_id":        cidP1,
		"sealed_boot_id":             "boot-p1",
	}

	// Stale ExpectedVersion=1 on stage_handoff MUST fail with 409 LEASE_VERSION_CONFLICT (H1 CAS fence).
	cStaleHandoff, _, staleHandoffBody, err := doRuntimeJSON(ctx, srv.URL, fix.tokens["app-video"], http.MethodPost, "/internal/v1/gpu-scheduler/leases/"+dec1.LeaseID+"/heartbeat", HeartbeatRequest{
		IdempotencyKey:           "pc-h2-handoff-stale",
		FencingToken:             dec1.FencingToken,
		ExpectedVersion:          1,
		NewStageID:               "regenerate_2k",
		WorkerSettled:            true,
		ReclaimMode:              "epoch_unload",
		ReclaimProof:             p1Proof,
		GPUEpoch:                 7,
		SupervisorPID:            4101,
		SupervisorStarttimeTicks: 987654,
		PodUID:                   "pod-h3-p1",
		ContainerID:              cidP1,
		BootID:                   "boot-p1",
		NextPodUID:               "pod-h3-p2",
		NextContainerID:          cidP2,
		NextBootID:               "boot-p2",
	})
	if err != nil || cStaleHandoff != http.StatusConflict || staleHandoffBody["error_code"] != "LEASE_VERSION_CONFLICT" {
		t.Fatalf("expected 409 LEASE_VERSION_CONFLICT on stale version=1 handoff, got code=%d body=%+v err=%v", cStaleHandoff, staleHandoffBody, err)
	}

	// Monotonically tracked ExpectedVersion=3 on stage_handoff MUST succeed -> version=4, stage_id=regenerate_2k.
	cHandoff, dec4, _, err := doRuntimeJSON(ctx, srv.URL, fix.tokens["app-video"], http.MethodPost, "/internal/v1/gpu-scheduler/leases/"+dec1.LeaseID+"/heartbeat", HeartbeatRequest{
		IdempotencyKey:           "pc-h2-handoff-ok",
		FencingToken:             dec1.FencingToken,
		ExpectedVersion:          dec3.Version,
		NewStageID:               "regenerate_2k",
		WorkerSettled:            true,
		ReclaimMode:              "epoch_unload",
		ReclaimProof:             p1Proof,
		GPUEpoch:                 7,
		SupervisorPID:            4101,
		SupervisorStarttimeTicks: 987654,
		PodUID:                   "pod-h3-p1",
		ContainerID:              cidP1,
		BootID:                   "boot-p1",
		NextPodUID:               "pod-h3-p2",
		NextContainerID:          cidP2,
		NextBootID:               "boot-p2",
	})
	if err != nil || cHandoff != http.StatusOK || dec4.Version != 4 || dec4.StageID != "regenerate_2k" {
		t.Fatalf("expected stage_handoff success version=4 stage=regenerate_2k, got code=%d dec=%+v err=%v", cHandoff, dec4, err)
	}

	// Unsettled downgrade MUST fail with 409 WORKER_NOT_SETTLED.
	dgMiB := int64(4096)
	cUnsettled, _, unsettledBody, err := doRuntimeJSON(ctx, srv.URL, fix.tokens["app-video"], http.MethodPost, "/internal/v1/gpu-scheduler/leases/"+dec1.LeaseID+"/downgrade", HeartbeatRequest{
		IdempotencyKey:  "pc-h2-dg-unsettled",
		FencingToken:    dec1.FencingToken,
		ExpectedVersion: dec4.Version,
		DowngradeToMiB:  &dgMiB,
		NewStageID:      "postprocess_rife",
		WorkerSettled:   false,
	})
	if err != nil || cUnsettled != http.StatusConflict || unsettledBody["error_code"] != "WORKER_NOT_SETTLED" {
		t.Fatalf("expected 409 WORKER_NOT_SETTLED, got code=%d body=%+v err=%v", cUnsettled, unsettledBody, err)
	}

	p2Proof := map[string]any{
		"reclaimed":                  true,
		"revoked_lease_id":           dec1.LeaseID,
		"consumed_fencing_token":     dec1.FencingToken,
		"sealed_gpu_epoch":           int64(11),
		"supervisor_pid":             5202,
		"supervisor_starttime_ticks": uint64(998877),
		"sealed_pod_uid":             "pod-h3-p2",
		"sealed_container_id":        cidP2,
		"sealed_boot_id":             "boot-p2",
	}

	// Stale version=3 on downgrade MUST fail with 409 LEASE_VERSION_CONFLICT; tracked version=4 succeeds (29696 -> 4096 MiB, version=5).
	cStaleDG, _, staleDGBody, err := doRuntimeJSON(ctx, srv.URL, fix.tokens["app-video"], http.MethodPost, "/internal/v1/gpu-scheduler/leases/"+dec1.LeaseID+"/downgrade", HeartbeatRequest{
		IdempotencyKey:           "pc-h2-dg-stale",
		FencingToken:             dec1.FencingToken,
		ExpectedVersion:          3,
		DowngradeToMiB:           &dgMiB,
		NewStageID:               "postprocess_rife",
		WorkerSettled:            true,
		ReclaimMode:              "epoch_unload",
		ReclaimProof:             p2Proof,
		GPUEpoch:                 11,
		SupervisorPID:            5202,
		SupervisorStarttimeTicks: 998877,
		PodUID:                   "pod-h3-p2",
		ContainerID:              cidP2,
		BootID:                   "boot-p2",
		NextPodUID:               "pod-rife",
		NextContainerID:          cidP3,
		NextBootID:               "boot-rife",
	})
	if err != nil || cStaleDG != http.StatusConflict || staleDGBody["error_code"] != "LEASE_VERSION_CONFLICT" {
		t.Fatalf("expected 409 LEASE_VERSION_CONFLICT on stale downgrade, got code=%d body=%+v err=%v", cStaleDG, staleDGBody, err)
	}

	cDG, dec5, _, err := doRuntimeJSON(ctx, srv.URL, fix.tokens["app-video"], http.MethodPost, "/internal/v1/gpu-scheduler/leases/"+dec1.LeaseID+"/downgrade", HeartbeatRequest{
		IdempotencyKey:           "pc-h2-dg-ok",
		FencingToken:             dec1.FencingToken,
		ExpectedVersion:          dec4.Version,
		DowngradeToMiB:           &dgMiB,
		NewStageID:               "postprocess_rife",
		WorkerSettled:            true,
		ReclaimMode:              "epoch_unload",
		ReclaimProof:             p2Proof,
		GPUEpoch:                 11,
		SupervisorPID:            5202,
		SupervisorStarttimeTicks: 998877,
		PodUID:                   "pod-h3-p2",
		ContainerID:              cidP2,
		BootID:                   "boot-p2",
		NextPodUID:               "pod-rife",
		NextContainerID:          cidP3,
		NextBootID:               "boot-rife",
	})
	if err != nil || cDG != http.StatusOK || dec5.Version != 5 || dec5.PeakBudgetMiB != 4096 || dec5.StageID != "postprocess_rife" {
		t.Fatalf("expected downgrade success version=5 budget=4096, got code=%d dec=%+v err=%v", cDG, dec5, err)
	}

	verifyAllCardHashChains(t, ctx, fix.pool, fix.stationID)
}
