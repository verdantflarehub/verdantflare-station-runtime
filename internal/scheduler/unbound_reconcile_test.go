package scheduler

import (
	"context"
	"testing"
	"time"
)

func TestUnboundLeaseFiveGatesAndBindDeadlineUnit(t *testing.T) {
	ctx := context.Background()
	t0 := time.Date(2026, 11, 18, 0, 0, 0, 0, time.UTC)
	gpu1 := "GPU-73be2803-a95d-7aac-970f-111111111111"
	nodeUID := "node-5090-dev"

	// 1. Gate 4: Bound leases (e.g. GPU 0 speech-mcp or PE-9 pod-pe9-inflight) MUST NOT be handled by unbound reconcile.
	for _, boundCase := range []struct {
		name  string
		lease activeLeaseRow
	}{
		{
			name: "non-empty pod_uid (PE-9 / speech-mcp)",
			lease: activeLeaseRow{
				LeaseID:   "01a1e953-0000-7000-8000-000000000001",
				NodeUID:   nodeUID,
				PodUID:    "pod-pe9-inflight",
				GPUUUIDs:  []string{gpu1},
				State:     "reconciling",
				ExpiresAt: t0.Add(-30 * time.Second),
			},
		},
		{
			name: "non-empty container_id",
			lease: activeLeaseRow{
				LeaseID:     "01a1e953-0000-7000-8000-000000000002",
				NodeUID:     nodeUID,
				ContainerID: "cid-bound-1",
				GPUUUIDs:    []string{gpu1},
				State:       "reconciling",
				ExpiresAt:   t0.Add(-30 * time.Second),
			},
		},
		{
			name: "prior bound target key",
			lease: activeLeaseRow{
				LeaseID:       "01a1e953-0000-7000-8000-000000000003",
				NodeUID:       nodeUID,
				GPUUUIDs:      []string{gpu1},
				State:         "reconciling",
				ResponseCache: map[string]any{"last_bind_key": "bind-1"},
				ExpiresAt:     t0.Add(-30 * time.Second),
			},
		},
		{
			name: "runtime dispatch marker",
			lease: activeLeaseRow{
				LeaseID:       "01a1e953-0000-7000-8000-000000000004",
				NodeUID:       nodeUID,
				GPUUUIDs:      []string{gpu1},
				State:         "leased",
				ResponseCache: map[string]any{"runtime_dispatched": true},
				ExpiresAt:     t0.Add(-30 * time.Second),
			},
		},
	} {
		dec := evaluateUnboundLeaseReconcile(ctx, boundCase.lease, nil, &fakeVerifier{defRes: VerifyTerminatedResult{Verified: true}}, t0, DefaultMaxTelemetryAge)
		if dec.Handled {
			t.Fatalf("%s: expected Handled=false to preserve VerifyTerminated path, got %+v", boundCase.name, dec)
		}
	}

	// 2. 60s UNBOUND_BIND_DEADLINE: unbound lease with 300s TTL stays untouched at t+59s, proactively seals at t+61s,
	//    and rejects late BindTarget/Heartbeat via isRevokedOrPastUnboundDeadline.
	longTTLUnbound := activeLeaseRow{
		LeaseID:       "01a1e953-d7ed-7bac-93e2-2fb2e735fd5c",
		NodeUID:       nodeUID,
		GPUUUIDs:      []string{gpu1},
		FencingToken:  42,
		PeakBudgetMiB: 18432,
		State:         "leased",
		ResponseCache: map[string]any{
			"granted_at": t0.Format(time.RFC3339Nano),
		},
		CreatedAt: t0,
		UpdatedAt: t0,
		ExpiresAt: t0.Add(300 * time.Second),
	}
	at59s := t0.Add(59 * time.Second)
	if isRevokedOrPastUnboundDeadline("leased", "", "", "", longTTLUnbound.ResponseCache, nil, t0, t0, at59s) {
		t.Fatal("expected unbound lease at 59s within 60s bind deadline to remain bindable")
	}
	dec59 := evaluateUnboundLeaseReconcile(ctx, longTTLUnbound, nil, &fakeVerifier{}, at59s, DefaultMaxTelemetryAge)
	if !dec59.Handled || !dec59.Skip {
		t.Fatalf("expected Skip=true at 59s, got %+v", dec59)
	}

	at61s := t0.Add(61 * time.Second)
	if !isRevokedOrPastUnboundDeadline("leased", "", "", "", longTTLUnbound.ResponseCache, nil, t0, t0, at61s) {
		t.Fatal("expected unbound lease past 60s bind deadline to reject late bind/heartbeat")
	}

	// 3. Gate 1: First reconcile past deadline (or on unsealed reconciling lease) ONLY seals; NEVER expires on Round 1 even if GPU is idle!
	tIdleEarly := at61s
	idleCardsEarly := map[string]cardLockState{
		gpu1: {
			NodeUID:             nodeUID,
			GPUUUID:             gpu1,
			State:               "reconciling",
			StateReason:         "UNKNOWN_NODE_STATE",
			MeasuredFreeVRAMMiB: 32033,
			MeasuredUsedVRAMMiB: 574,
			TelemetryBootID:     "boot-agent-1",
			TelemetrySampleID:   100,
			TelemetrySampledAt:  &tIdleEarly,
		},
	}
	idleVerifier := &fakeVerifier{
		defRes: VerifyTerminatedResult{
			Verified:            true,
			GPUProcessCount:     0,
			MeasuredFreeVRAMMiB: 32033,
			MeasuredUsedVRAMMiB: 574,
		},
	}
	sealDec := evaluateUnboundLeaseReconcile(ctx, longTTLUnbound, idleCardsEarly, idleVerifier, at61s, DefaultMaxTelemetryAge)
	if !sealDec.Handled || sealDec.Skip || sealDec.TargetState != "reconciling" || !sealDec.BumpFence || !sealDec.CapExpiresAt || !sealDec.UnboundDeadlineSeal {
		t.Fatalf("Gate 1 violation: expected sealing round to stay reconciling with BumpFence & CapExpiresAt, got %+v", sealDec)
	}
	if _, ok := hasUnboundSeal(sealDec.UpdatedReclaimProof); !ok {
		t.Fatalf("Gate 1 violation: missing unbound_sealed_at in UpdatedReclaimProof: %+v", sealDec.UpdatedReclaimProof)
	}
	if !isRevokedOrPastUnboundDeadline("leased", "", "", "", longTTLUnbound.ResponseCache, sealDec.UpdatedReclaimProof, t0, t0, t0.Add(10*time.Second)) {
		t.Fatal("Gate 1 violation: sealed proof must reject bind/heartbeat regardless of wall clock")
	}

	// Simulate sealed lease row in PostgreSQL (expires_at capped to seal_at = at61s).
	sealedLease := longTTLUnbound
	sealedLease.State = "reconciling"
	sealedLease.ExpiresAt = at61s
	sealedLease.ReclaimProof = sealDec.UpdatedReclaimProof

	// 4. Gate 2: Telemetry at exact seal_at + 15s (not strictly >) must NOT count as Sample 1.
	atGraceExact := at61s.Add(15 * time.Second)
	cardsAtBoundary := map[string]cardLockState{
		gpu1: {
			NodeUID:             nodeUID,
			GPUUUID:             gpu1,
			State:               "reconciling",
			MeasuredFreeVRAMMiB: 32033,
			MeasuredUsedVRAMMiB: 574,
			TelemetryBootID:     "boot-agent-1",
			TelemetrySampleID:   101,
			TelemetrySampledAt:  &atGraceExact,
		},
	}
	decBoundary := evaluateUnboundLeaseReconcile(ctx, sealedLease, cardsAtBoundary, idleVerifier, atGraceExact, DefaultMaxTelemetryAge)
	if decBoundary.TargetState != "reconciling" || len(loadUnboundSamples(decBoundary.UpdatedReclaimProof)) != 0 {
		t.Fatalf("Gate 2 violation: sample at exact +15s boundary must not count, got %+v", decBoundary)
	}

	// 5. Gate 2: Sample 1 at seal_at + 16s (sample_id=102) records sample_1 ONLY — single sample MUST NOT release!
	tSample1 := at61s.Add(16 * time.Second)
	cardsSample1 := map[string]cardLockState{
		gpu1: {
			NodeUID:             nodeUID,
			GPUUUID:             gpu1,
			State:               "reconciling",
			MeasuredFreeVRAMMiB: 32033,
			MeasuredUsedVRAMMiB: 574,
			TelemetryBootID:     "boot-agent-1",
			TelemetrySampleID:   102,
			TelemetrySampledAt:  &tSample1,
		},
	}
	decS1 := evaluateUnboundLeaseReconcile(ctx, sealedLease, cardsSample1, idleVerifier, tSample1.Add(time.Second), DefaultMaxTelemetryAge)
	if decS1.TargetState != "reconciling" {
		t.Fatalf("Gate 2 violation: single sample must NOT expire lease, got %+v", decS1)
	}
	sealedLease.ReclaimProof = decS1.UpdatedReclaimProof

	// Duplicate telemetry_sample_id (102) on next tick must NOT release!
	decDup := evaluateUnboundLeaseReconcile(ctx, sealedLease, cardsSample1, idleVerifier, tSample1.Add(3*time.Second), DefaultMaxTelemetryAge)
	if decDup.TargetState != "reconciling" {
		t.Fatalf("Gate 2 violation: duplicate telemetry_sample_id must NOT expire lease, got %+v", decDup)
	}

	// 6. Gate 3: Non-K8s host compute process (PodUID="", ContainerID="", PID=99112) OR VRAM > 1024 MiB MUST block AND reset Sample 1!
	tHostProc := at61s.Add(21 * time.Second)
	cardsWithHostProc := map[string]cardLockState{
		gpu1: {
			NodeUID:             nodeUID,
			GPUUUID:             gpu1,
			State:               "reconciling",
			MeasuredFreeVRAMMiB: 31800,
			MeasuredUsedVRAMMiB: 807,
			AttributedProcesses: []AttributedGPUProcess{
				{PID: 99112, ProcessType: "C", UsedVRAMMiB: 233}, // Non-K8s host process!
			},
			TelemetryBootID:    "boot-agent-1",
			TelemetrySampleID:  103,
			TelemetrySampledAt: &tHostProc,
		},
	}
	decHostBlock := evaluateUnboundLeaseReconcile(ctx, sealedLease, cardsWithHostProc, idleVerifier, tHostProc, DefaultMaxTelemetryAge)
	if decHostBlock.TargetState != "reconciling" || decHostBlock.Reason != "UNBOUND_CARD_NOT_IDLE" {
		t.Fatalf("Gate 3 violation: non-K8s host GPU process must block convergence, got %+v", decHostBlock)
	}
	if len(loadUnboundSamples(decHostBlock.UpdatedReclaimProof)) != 0 {
		t.Fatalf("Gate 3 violation: non-K8s host GPU process must reset consecutive sample tracker, got %+v", decHostBlock.UpdatedReclaimProof)
	}
	sealedLease.ReclaimProof = decHostBlock.UpdatedReclaimProof

	// Also verify MeasuredUsedVRAMMiB = 1025 (> IdleDriverNoiseCapMiB=1024) blocks even with 0 processes!
	tHighVRAM := at61s.Add(25 * time.Second)
	cardsHighVRAM := map[string]cardLockState{
		gpu1: {
			NodeUID:             nodeUID,
			GPUUUID:             gpu1,
			State:               "reconciling",
			MeasuredFreeVRAMMiB: 31582,
			MeasuredUsedVRAMMiB: IdleDriverNoiseCapMiB + 1,
			TelemetryBootID:     "boot-agent-1",
			TelemetrySampleID:   104,
			TelemetrySampledAt:  &tHighVRAM,
		},
	}
	decVRAMBlock := evaluateUnboundLeaseReconcile(ctx, sealedLease, cardsHighVRAM, idleVerifier, tHighVRAM, DefaultMaxTelemetryAge)
	if decVRAMBlock.TargetState != "reconciling" || decVRAMBlock.Reason != "UNBOUND_CARD_NOT_IDLE" {
		t.Fatalf("Gate 3 violation: used VRAM > 1024 MiB must block convergence, got %+v", decVRAMBlock)
	}

	// 7. Gates 2, 3 & 5: Two consecutive clean samples after reset (sample 105 -> sample 106) converge to expired with RECONCILED_UNBOUND_IDLE_GPU!
	tClean1 := at61s.Add(30 * time.Second)
	cardsClean1 := map[string]cardLockState{
		gpu1: {
			NodeUID:             nodeUID,
			GPUUUID:             gpu1,
			State:               "reconciling",
			StateReason:         "PENDING_LEASE_RECONCILIATION",
			MeasuredFreeVRAMMiB: 32033,
			MeasuredUsedVRAMMiB: 574,
			TelemetryBootID:     "boot-agent-1",
			TelemetrySampleID:   105,
			TelemetrySampledAt:  &tClean1,
		},
	}
	decClean1 := evaluateUnboundLeaseReconcile(ctx, sealedLease, cardsClean1, idleVerifier, tClean1, DefaultMaxTelemetryAge)
	if decClean1.TargetState != "reconciling" {
		t.Fatalf("expected 1st post-reset clean sample to remain reconciling, got %+v", decClean1)
	}
	sealedLease.ReclaimProof = decClean1.UpdatedReclaimProof

	tClean2 := at61s.Add(35 * time.Second)
	cardsClean2 := map[string]cardLockState{
		gpu1: {
			NodeUID:             nodeUID,
			GPUUUID:             gpu1,
			State:               "reconciling",
			StateReason:         "PENDING_LEASE_RECONCILIATION",
			MeasuredFreeVRAMMiB: 32033,
			MeasuredUsedVRAMMiB: 574,
			TelemetryBootID:     "boot-agent-1",
			TelemetrySampleID:   106,
			TelemetrySampledAt:  &tClean2,
		},
	}
	decClean2 := evaluateUnboundLeaseReconcile(ctx, sealedLease, cardsClean2, idleVerifier, tClean2, DefaultMaxTelemetryAge)
	if decClean2.TargetState != "expired" || decClean2.Reason != ReasonReconciledUnboundIdleGPU || !decClean2.BumpFence {
		t.Fatalf("Gate 5 violation: expected expired with %s, got %+v", ReasonReconciledUnboundIdleGPU, decClean2)
	}
	if decClean2.Reason == "RECONCILED_TERMINATED" {
		t.Fatal("Gate 5 violation: must never reuse RECONCILED_TERMINATED")
	}
}
