package appinstances

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/verdantflarehub/verdantflare-station-runtime/internal/scheduler"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type reclamationVerifierFunc func(context.Context, scheduler.VerifyTerminatedRequest) (scheduler.VerifyTerminatedResult, error)

func (f reclamationVerifierFunc) VerifyTerminated(ctx context.Context, req scheduler.VerifyTerminatedRequest) (scheduler.VerifyTerminatedResult, error) {
	return f(ctx, req)
}

func TestRepairRestartStopAndReclamationStatusHandlerContract(t *testing.T) {
	in, p, _, _ := fixture()
	stopOp, callerOp, boot, podUID, token := id(), id(), uuid.NewString(), uuid.NewString(), strings.Repeat("r", 40)
	containerID := "containerd://abcdef0123456789abcdef0123456789"
	caller := Caller{AppID: p.AppID, Authority: &CoreAuthority{}}

	repairCalls := 0
	normalStopCalls := 0
	reclaimCalls := 0
	currentPhase := "stopped"
	var nextProof ReclamationProof

	h := Handler{
		StationID: in.StationID,
		Profiles:  map[string]Profile{p.Key(): p},
		Callers:   AccessRegistry{sha256.Sum256([]byte("Bearer " + token)): caller},
		Stop: func(_ context.Context, _ Caller, _ Run, _ Profile, _, _ string) (StopRecord, error) {
			normalStopCalls++
			return StopRecord{}, nil
		},
		RepairStop: func(_ context.Context, got Caller, run Run, profile Profile, opts RepairStopOptions) (StopRecord, error) {
			repairCalls++
			if got.AppID != caller.AppID || run != in || profile.Hash() != p.Hash() ||
				opts.StopOperationID != stopOp || opts.BootID != boot ||
				opts.CallerOperationID != callerOp || opts.ForceAfterDrainTimeoutSeconds != 15 {
				t.Fatal("repair_restart stop lost options or identity")
			}
			proof := stopProof(in, p, Observation{PodUID: podUID, BootID: boot}, stopOp)
			return StopRecord{
				OperationID: stopOp,
				StartID:     in.OperationID,
				BootID:      boot,
				Revision:    1,
				Progress:    StopProgress{Proof: proof, Phase: currentPhase},
			}, nil
		},
		ReclamationStatus: func(_ context.Context, got Caller, identity Identity, profiles map[string]Profile, gen int64, gotBoot string) (ReclamationProof, error) {
			reclaimCalls++
			if got.AppID != caller.AppID || identity != in.Identity || gen != in.Generation || gotBoot != boot || len(profiles) != 1 {
				t.Fatal("reclamation-status lost caller identity")
			}
			return nextProof, nil
		},
	}

	makeReq := func(path string, payload any, extraHeaders map[string]string) *http.Request {
		raw, _ := json.Marshal(payload)
		r := httptest.NewRequest(http.MethodPost, path, strings.NewReader(string(raw)))
		for k, v := range map[string]string{
			"Authorization":     "Bearer " + token,
			"Content-Type":      "application/json",
			"X-Station-Id":      in.StationID,
			"X-Organization-Id": in.OrganizationID,
			"X-User-Id":         in.UserID,
		} {
			r.Header.Set(k, v)
		}
		for k, v := range extraHeaders {
			r.Header.Set(k, v)
		}
		return r
	}

	// 1. Valid repair_restart stop invokes RepairStop (not Stop) and returns exact 8-field 200 envelope.
	validRepairBody := map[string]any{
		"installation_id":                   in.InstallationID,
		"instance_id":                       in.InstanceID,
		"operation_id":                      in.OperationID,
		"generation":                        in.Generation,
		"app_version":                       p.AppVersion,
		"profile_id":                        p.ID,
		"stop_operation_id":                 stopOp,
		"boot_id":                           boot,
		"reason":                            "repair_restart",
		"caller_operation_id":               callerOp,
		"force_after_drain_timeout_seconds": 15,
	}
	w := httptest.NewRecorder()
	h.ServeHTTP(w, makeReq("/internal/v1/app-instances/stop", validRepairBody, nil))
	var stopOut map[string]any
	if w.Code != 200 || json.Unmarshal(w.Body.Bytes(), &stopOut) != nil || len(stopOut) != 8 ||
		stopOut["status"] != "succeeded" || stopOut["phase"] != "stopped" || repairCalls != 1 || normalStopCalls != 0 {
		t.Fatalf("unexpected repair_restart response: code=%d body=%s", w.Code, w.Body.String())
	}

	// 2. Partial or out-of-range repair_restart fields must fail closed with 400 INVALID_ARGUMENT.
	invalidRepairCases := []struct {
		name    string
		mutate  func(map[string]any)
		headers map[string]string
	}{
		{"missing_caller_op", func(m map[string]any) { delete(m, "caller_operation_id") }, nil},
		{"missing_timeout", func(m map[string]any) { delete(m, "force_after_drain_timeout_seconds") }, nil},
		{"bad_reason", func(m map[string]any) { m["reason"] = "manual" }, nil},
		{"timeout_too_low", func(m map[string]any) { m["force_after_drain_timeout_seconds"] = 4 }, nil},
		{"timeout_too_high", func(m map[string]any) { m["force_after_drain_timeout_seconds"] = 121 }, nil},
		{"template_upgrade_header_conflict", func(map[string]any) {}, map[string]string{"X-Template-Upgrade": "true"}},
	}
	for _, tc := range invalidRepairCases {
		t.Run(tc.name, func(t *testing.T) {
			cp := map[string]any{}
			for k, v := range validRepairBody {
				cp[k] = v
			}
			tc.mutate(cp)
			before := repairCalls
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, makeReq("/internal/v1/app-instances/stop", cp, tc.headers))
			if rec.Code != 400 || repairCalls != before {
				t.Fatalf("expected 400 on %s, got %d (%s)", tc.name, rec.Code, rec.Body.String())
			}
		})
	}

	// 3. Valid reclamation-status (reclaimed=false while running, then reclaimed=true with triple proof).
	recBody := map[string]any{
		"installation_id": in.InstallationID,
		"instance_id":     in.InstanceID,
		"generation":      in.Generation,
		"boot_id":         boot,
	}
	nextProof = ReclamationProof{
		SchemaVersion: 1,
		InstanceID:    in.InstanceID,
		Generation:    in.Generation,
		BootID:        boot,
		Reclaimed:     false,
		PodUID:        podUID,
		ContainerID:   containerID,
		GPUEpoch:      0,
		Reason:        "INSTANCE_NOT_STOPPED",
	}
	w = httptest.NewRecorder()
	h.ServeHTTP(w, makeReq("/internal/v1/app-instances/reclamation-status", recBody, nil))
	if w.Code != 200 || !strings.Contains(w.Body.String(), `"reclaimed":false`) || !strings.Contains(w.Body.String(), `"INSTANCE_NOT_STOPPED"`) {
		t.Fatalf("unexpected running reclamation-status: %d %s", w.Code, w.Body.String())
	}

	tTrue, zeroCount, zeroVRAM := true, 0, int64(0)
	nextProof = ReclamationProof{
		SchemaVersion:       1,
		InstanceID:          in.InstanceID,
		Generation:          in.Generation,
		BootID:              boot,
		Reclaimed:           true,
		PodUID:              podUID,
		ContainerID:         containerID,
		GPUEpoch:            2,
		ContainerTerminated: &tTrue,
		SurvivingPIDs:       &zeroCount,
		VRAMAllocatedMiB:    &zeroVRAM,
	}
	w = httptest.NewRecorder()
	h.ServeHTTP(w, makeReq("/internal/v1/app-instances/reclamation-status", recBody, nil))
	if w.Code != 200 || !strings.Contains(w.Body.String(), `"reclaimed":true`) || !strings.Contains(w.Body.String(), `"gpu_epoch":2`) {
		t.Fatalf("unexpected verified reclamation-status: %d %s", w.Code, w.Body.String())
	}

	// 4. Handler output self-check rejects malformed reclaimed=true proofs (503 INVALID_RECLAMATION_RESULT).
	badSurvivors := 1
	badVRAM := int64(512)
	tFalse := false
	for _, mutate := range []func(*ReclamationProof){
		func(v *ReclamationProof) { v.PodUID = "" },
		func(v *ReclamationProof) { v.ContainerID = "" },
		func(v *ReclamationProof) { v.ContainerTerminated = &tFalse },
		func(v *ReclamationProof) { v.SurvivingPIDs = &badSurvivors },
		func(v *ReclamationProof) { v.VRAMAllocatedMiB = &badVRAM },
	} {
		cand := nextProof
		mutate(&cand)
		nextProof = cand
		w = httptest.NewRecorder()
		h.ServeHTTP(w, makeReq("/internal/v1/app-instances/reclamation-status", recBody, nil))
		if w.Code != 503 || !strings.Contains(w.Body.String(), "INVALID_RECLAMATION_RESULT") {
			t.Fatalf("malformed reclaimed=true proof leaked through handler: %d %s", w.Code, w.Body.String())
		}
	}
}

func TestObserveWorkerExtractsContainerIDAndRejectsContainerDrift(t *testing.T) {
	in, p, pvc, pv := fixture()
	c := client(pvc, pv)
	v := bind(t, c, in, p, pvc.Name)
	ctx := context.Background()
	var checkpoints []Observation
	obs, e := EnsureWorker(ctx, c, in, p, v, Observation{}, func(_ context.Context, next Observation) error {
		checkpoints = append(checkpoints, next)
		return nil
	})
	if e != nil {
		t.Fatal(e)
	}
	pod, e := c.CoreV1().Pods(p.Namespace).Get(ctx, runName(in), metav1.GetOptions{})
	if e != nil {
		t.Fatal(e)
	}
	cid := "containerd://11223344556677889900aabbccddeeff"
	pod.Spec.NodeName = p.NodeName
	pod.Status.Phase = corev1.PodRunning
	pod.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}
	pod.Status.ContainerStatuses = []corev1.ContainerStatus{{Name: "worker", ContainerID: cid, Ready: true}}
	if _, e = c.CoreV1().Pods(p.Namespace).Update(ctx, pod, metav1.UpdateOptions{}); e != nil {
		t.Fatal(e)
	}
	got, e := ObserveWorker(ctx, c, in, p, v, obs)
	if e != nil || got.ContainerID != cid || !got.PodReady {
		t.Fatalf("expected container_id %q and PodReady, got %+v err=%v", cid, got, e)
	}
	drifted := got
	drifted.ContainerID = "containerd://ffeeddccbbaa00998877665544332211"
	if _, e = ObserveWorker(ctx, c, in, p, v, drifted); !errors.Is(e, ErrBinding) {
		t.Fatalf("expected ErrBinding on container_id drift, got %v", e)
	}
}

func TestJournalStopForRepairRestartAndReclamationStatusEndToEnd(t *testing.T) {
	j, in, p, c, boot := runningStopFixture(t)
	ctx := context.Background()
	cid := "containerd://deadbeef0123456789abcdef01234567"

	pod, err := c.CoreV1().Pods(p.Namespace).Get(ctx, runName(in), metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	podUID := string(pod.UID)
	pod.Status.ContainerStatuses = []corev1.ContainerStatus{{Name: "worker", ContainerID: cid, Ready: true}}
	if _, err = c.CoreV1().Pods(p.Namespace).Update(ctx, pod, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}

	profiles := map[string]Profile{p.Key(): p}
	runningProof, err := j.ReclamationStatus(ctx, in.Identity, profiles, in.Generation, boot)
	if err != nil || runningProof.Reclaimed || runningProof.Reason != "INSTANCE_NOT_STOPPED" {
		t.Fatalf("expected INSTANCE_NOT_STOPPED while running, got %+v err=%v", runningProof, err)
	}

	// Simulate hung worker HTTP stop-barrier that fails with ErrUnavailable until drain timeout expires.
	j.Stopper = stopperFunc(func(context.Context, Run, Profile, Observation, string, string) (WorkerStopProof, error) {
		return WorkerStopProof{}, ErrUnavailable
	})
	stopOp, callerOp := id(), id()
	opts := RepairStopOptions{
		StopOperationID:               stopOp,
		BootID:                        boot,
		CallerOperationID:             callerOp,
		ForceAfterDrainTimeoutSeconds: 5,
	}
	rec1, err := j.StopForRepairRestart(ctx, in, p, opts)
	if err != nil || rec1.Progress.Phase != "accepted" {
		t.Fatalf("expected initial accepted phase during open drain window, got %+v err=%v", rec1, err)
	}

	// Backdate stop created_at past ForceAfterDrainTimeoutSeconds to exercise force-after-drain seal.
	if _, err = j.Pool.Exec(ctx, `UPDATE station.app_instance_stops SET created_at = now() - interval '6 seconds' WHERE operation_id = $1`, stopOp); err != nil {
		t.Fatal(err)
	}
	rec2, err := j.StopForRepairRestart(ctx, in, p, opts)
	if err != nil || rec2.Progress.Phase != "pod_delete_requested" {
		t.Fatalf("expected pod_delete_requested after drain expiry, got %+v err=%v", rec2, err)
	}
	rec3, err := j.StopForRepairRestart(ctx, in, p, opts)
	if err != nil || rec3.Progress.Phase != "service_delete_requested" {
		t.Fatalf("expected service_delete_requested, got %+v err=%v", rec3, err)
	}
	rec4, err := j.StopForRepairRestart(ctx, in, p, opts)
	if err != nil || rec4.Progress.Phase != "stopped" {
		t.Fatalf("expected stopped phase, got %+v err=%v", rec4, err)
	}

	// Without ReclamationVerifier, fail closed (`reclaimed=false`).
	unverified, err := j.ReclamationStatus(ctx, in.Identity, profiles, in.Generation, boot)
	if err != nil || unverified.Reclaimed || unverified.Reason != "RECLAMATION_VERIFIER_UNCONFIGURED" {
		t.Fatalf("expected RECLAMATION_VERIFIER_UNCONFIGURED, got %+v err=%v", unverified, err)
	}

	// With NodeVerifier reporting surviving PID -> fail closed (`reclaimed=false`).
	j.ReclamationVerifier = reclamationVerifierFunc(func(_ context.Context, req scheduler.VerifyTerminatedRequest) (scheduler.VerifyTerminatedResult, error) {
		if req.PodUID != podUID || req.ContainerID != cid || req.BootID != boot || req.Mode != "container_exited" {
			t.Fatalf("unexpected VerifyTerminatedRequest: %+v", req)
		}
		return scheduler.VerifyTerminatedResult{
			Verified:        false,
			ContainerExited: true,
			SurvivingPIDs:   []int{4242},
			RawProof:        map[string]any{"sampled_at": time.Now().UTC().Format(time.RFC3339Nano)},
		}, nil
	})
	surviving, err := j.ReclamationStatus(ctx, in.Identity, profiles, in.Generation, boot)
	if err != nil || surviving.Reclaimed || surviving.Reason != "SURVIVING_PROCESSES_PRESENT" || surviving.SurvivingPIDs == nil || *surviving.SurvivingPIDs != 1 {
		t.Fatalf("expected SURVIVING_PROCESSES_PRESENT, got %+v err=%v", surviving, err)
	}

	// With NodeVerifier confirming CRI exit + 0 surviving PIDs + 0 GPU procs -> `reclaimed=true`.
	j.ReclamationVerifier = reclamationVerifierFunc(func(_ context.Context, _ scheduler.VerifyTerminatedRequest) (scheduler.VerifyTerminatedResult, error) {
		return scheduler.VerifyTerminatedResult{
			Verified:        true,
			ContainerExited: true,
			GPUProcessCount: 0,
			SurvivingPIDs:   nil,
			RawProof:        map[string]any{"sampled_at": time.Now().UTC().Format(time.RFC3339Nano)},
		}, nil
	})
	verified, err := j.ReclamationStatus(ctx, in.Identity, profiles, in.Generation, boot)
	if err != nil || !verified.Reclaimed || verified.PodUID != podUID || verified.ContainerID != cid ||
		verified.ContainerTerminated == nil || !*verified.ContainerTerminated ||
		verified.SurvivingPIDs == nil || *verified.SurvivingPIDs != 0 ||
		verified.VRAMAllocatedMiB == nil || *verified.VRAMAllocatedMiB != 0 {
		t.Fatalf("expected reclaimed=true proof, got %+v err=%v", verified, err)
	}
}
