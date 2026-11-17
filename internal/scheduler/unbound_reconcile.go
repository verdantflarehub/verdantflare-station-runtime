package scheduler

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

type unboundSamplePair struct {
	Sample1BootID string `json:"sample_1_boot_id,omitempty"`
	Sample1ID     int64  `json:"sample_1_id,omitempty"`
	Sample1At     string `json:"sample_1_at,omitempty"`
	Sample2BootID string `json:"sample_2_boot_id,omitempty"`
	Sample2ID     int64  `json:"sample_2_id,omitempty"`
	Sample2At     string `json:"sample_2_at,omitempty"`
}

type unboundEvalDecision struct {
	Handled             bool
	Skip                bool
	TargetState         string
	Reason              string
	BumpFence           bool
	CapExpiresAt        bool
	UnboundDeadlineSeal bool
	CardUnknown         bool
	QuarantineLeak      int64
	VerifiedFreeByID    map[string]int64
	UpdatedReclaimProof map[string]any
}

// isEligibleUnboundLease enforces Gate 4:
// ONLY leases with empty pod_uid/container_id/boot_id, never bound or renewed into running,
// and carrying no runtime dispatch or container/epoch proof markers may enter unbound convergence.
func isEligibleUnboundLease(l activeLeaseRow) bool {
	if len(l.GPUUUIDs) == 0 {
		return false
	}
	if strings.TrimSpace(l.PodUID) != "" || strings.TrimSpace(l.ContainerID) != "" || strings.TrimSpace(l.BootID) != "" {
		return false
	}
	if l.State != "leased" && l.State != "running" && l.State != "reconciling" && l.State != "reclaiming" {
		return false
	}
	if len(l.ResponseCache) > 0 {
		if s, ok := l.ResponseCache["last_bind_key"].(string); ok && strings.TrimSpace(s) != "" {
			return false
		}
		if dispatched, ok := l.ResponseCache["runtime_dispatched"].(bool); ok && dispatched {
			return false
		}
		if rawReq, ok := l.ResponseCache["acquire_req"].(map[string]any); ok {
			for _, key := range []string{"pod_uid", "container_id", "boot_id"} {
				if v, okS := rawReq[key].(string); okS && strings.TrimSpace(v) != "" {
					return false
				}
			}
			if dispatched, okB := rawReq["runtime_dispatched"].(bool); okB && dispatched {
				return false
			}
		}
	}
	if len(l.ReclaimProof) > 0 {
		for _, key := range []string{"sealed_pod_uid", "sealed_container_id", "sealed_boot_id", "sealed_gpu_epoch"} {
			if v, exists := l.ReclaimProof[key]; exists && v != nil && fmt.Sprint(v) != "" && fmt.Sprint(v) != "0" {
				return false
			}
		}
	}
	return true
}

func parseProofTimestamp(raw any) (time.Time, bool) {
	s, ok := raw.(string)
	if !ok {
		return time.Time{}, false
	}
	s = strings.TrimSpace(s)
	if s == "" {
		return time.Time{}, false
	}
	if t, err := time.Parse(time.RFC3339Nano, s); err == nil && !t.IsZero() {
		return t.UTC(), true
	}
	if t, err := time.Parse(time.RFC3339, s); err == nil && !t.IsZero() {
		return t.UTC(), true
	}
	return time.Time{}, false
}

func hasUnboundSeal(reclaimProof map[string]any) (time.Time, bool) {
	if len(reclaimProof) == 0 {
		return time.Time{}, false
	}
	return parseProofTimestamp(reclaimProof["unbound_sealed_at"])
}

func leaseGrantedAtFromParts(responseCache map[string]any, state string, createdAt, updatedAt time.Time) time.Time {
	if len(responseCache) > 0 {
		if t, ok := parseProofTimestamp(responseCache["granted_at"]); ok {
			return t
		}
	}
	if state == "leased" && !updatedAt.IsZero() {
		return updatedAt.UTC()
	}
	if !createdAt.IsZero() {
		return createdAt.UTC()
	}
	return updatedAt.UTC()
}

func leaseGrantedAt(l activeLeaseRow) time.Time {
	return leaseGrantedAtFromParts(l.ResponseCache, l.State, l.CreatedAt, l.UpdatedAt)
}

func isUnboundBindDeadlineExceeded(l activeLeaseRow, now time.Time) bool {
	if !isEligibleUnboundLease(l) || (l.State != "leased" && l.State != "running") {
		return false
	}
	grantedAt := leaseGrantedAt(l)
	if grantedAt.IsZero() {
		return false
	}
	return now.After(grantedAt.Add(UnboundBindDeadline))
}

func isRevokedOrPastUnboundDeadline(
	state, podUID, containerID, bootID string,
	responseCache, reclaimProof map[string]any,
	createdAt, updatedAt, now time.Time,
) bool {
	if state == "reconciling" {
		return true
	}
	if _, sealed := hasUnboundSeal(reclaimProof); sealed {
		return true
	}
	if (state == "leased" || state == "running") && strings.TrimSpace(podUID) == "" && strings.TrimSpace(containerID) == "" && strings.TrimSpace(bootID) == "" {
		if s, ok := responseCache["last_bind_key"].(string); !ok || strings.TrimSpace(s) == "" {
			if dispatched, okD := responseCache["runtime_dispatched"].(bool); !okD || !dispatched {
				grantedAt := leaseGrantedAtFromParts(responseCache, state, createdAt, updatedAt)
				if !grantedAt.IsZero() && now.After(grantedAt.Add(UnboundBindDeadline)) {
					return true
				}
			}
		}
	}
	return false
}

func cloneProofMap(in map[string]any) map[string]any {
	if len(in) == 0 {
		return map[string]any{}
	}
	raw, err := json.Marshal(in)
	if err != nil {
		out := make(map[string]any, len(in))
		for k, v := range in {
			out[k] = v
		}
		return out
	}
	out := map[string]any{}
	_ = json.Unmarshal(raw, &out)
	return out
}

func loadUnboundSamples(proof map[string]any) map[string]unboundSamplePair {
	out := map[string]unboundSamplePair{}
	if len(proof) == 0 {
		return out
	}
	rawMap, ok := proof["unbound_samples"]
	if !ok || rawMap == nil {
		return out
	}
	buf, err := json.Marshal(rawMap)
	if err != nil {
		return out
	}
	_ = json.Unmarshal(buf, &out)
	return out
}

func storeUnboundSamples(proof map[string]any, samples map[string]unboundSamplePair) {
	if len(samples) == 0 {
		proof["unbound_samples"] = map[string]any{}
		return
	}
	buf, err := json.Marshal(samples)
	if err != nil {
		proof["unbound_samples"] = map[string]any{}
		return
	}
	generic := map[string]any{}
	_ = json.Unmarshal(buf, &generic)
	proof["unbound_samples"] = generic
}

func maxTime(a, b time.Time) time.Time {
	if b.After(a) {
		return b
	}
	return a
}

// evaluateUnboundLeaseReconcile executes the 5-gate unbound lease state machine:
//   - Gate 4: strict unbound scope check (handled=false if ineligible, preserving VerifyTerminated path).
//   - 60s UNBOUND_BIND_DEADLINE: proactive sealing without waiting for full long TTL.
//   - Gate 1: seal card min_fencing_token > lease.FencingToken & persist unbound_sealed_at first; never release on seal round.
//   - Gate 2: telemetry fresh AND telemetry_sampled_at > max(expires_at, seal_at) + 15s across 2 consecutive distinct telemetry_sample_ids.
//   - Gate 3: 0 total GPU compute/graphics processes (including non-K8s host processes) AND measured_used_vram_mib <= 1024.
//   - Gate 5: expire with independent ReasonReconciledUnboundIdleGPU.
func evaluateUnboundLeaseReconcile(
	ctx context.Context,
	l activeLeaseRow,
	cardsByUUID map[string]cardLockState,
	verifier NodeVerifier,
	now time.Time,
	maxTelemetryAge time.Duration,
) unboundEvalDecision {
	if !isEligibleUnboundLease(l) {
		return unboundEvalDecision{Handled: false}
	}

	expired := !l.ExpiresAt.After(now)
	deadlineExceeded := isUnboundBindDeadlineExceeded(l, now)
	sealAt, alreadySealed := hasUnboundSeal(l.ReclaimProof)
	inReconcile := l.State == "reconciling" || l.State == "reclaiming"

	if !expired && !deadlineExceeded && !alreadySealed && !inReconcile {
		return unboundEvalDecision{Handled: true, Skip: true}
	}

	proof := cloneProofMap(l.ReclaimProof)

	// Gate 1: First-time seal. Advance min_fencing_token past lease.FencingToken, persist unbound_sealed_at,
	// cap expires_at to seal_at if sealed early due to UNBOUND_BIND_DEADLINE, and wait for post-seal telemetry.
	if !alreadySealed {
		proof["unbound_sealed_at"] = now.UTC().Format(time.RFC3339Nano)
		if deadlineExceeded && !expired {
			proof["unbound_seal_trigger"] = "bind_deadline_exceeded"
		} else {
			proof["unbound_seal_trigger"] = "lease_expired_or_reconciling"
		}
		storeUnboundSamples(proof, nil)
		return unboundEvalDecision{
			Handled:             true,
			TargetState:         "reconciling",
			Reason:              "UNBOUND_LEASE_SEALED",
			BumpFence:           true,
			CapExpiresAt:        l.ExpiresAt.After(now),
			UnboundDeadlineSeal: deadlineExceeded && !expired,
			UpdatedReclaimProof: proof,
		}
	}

	if verifier == nil {
		storeUnboundSamples(proof, nil)
		return unboundEvalDecision{
			Handled:             true,
			TargetState:         "reconciling",
			Reason:              "UNKNOWN_NODE_STATE",
			BumpFence:           true,
			CardUnknown:         true,
			UpdatedReclaimProof: proof,
		}
	}

	graceFloor := maxTime(l.ExpiresAt.UTC(), sealAt.UTC()).Add(UnboundReconcileGrace)
	samples := loadUnboundSamples(proof)
	verifiedFree := make(map[string]int64, len(l.GPUUUIDs))
	allCardsDualSampled := true

	for _, g := range l.GPUUUIDs {
		card, ok := cardsByUUID[g]
		telemetryFresh := ok &&
			card.TelemetrySampledAt != nil &&
			card.State != "quarantined" &&
			now.Sub(*card.TelemetrySampledAt) <= maxTelemetryAge

		vRes, vErr := verifier.VerifyTerminated(ctx, VerifyTerminatedRequest{
			NodeUID: l.NodeUID,
			GPUUUID: g,
			Mode:    "card_idle",
		})
		if vErr != nil || vRes.ErrorCode == "UNKNOWN_NODE_STATE" {
			storeUnboundSamples(proof, nil)
			return unboundEvalDecision{
				Handled:             true,
				TargetState:         "reconciling",
				Reason:              "UNKNOWN_NODE_STATE",
				BumpFence:           true,
				CardUnknown:         true,
				UpdatedReclaimProof: proof,
			}
		}
		if vRes.UnkillableDState {
			storeUnboundSamples(proof, nil)
			return unboundEvalDecision{
				Handled:             true,
				TargetState:         "quarantined",
				Reason:              "UNKILLABLE_D_STATE_QUARANTINED",
				BumpFence:           true,
				QuarantineLeak:      l.PeakBudgetMiB,
				UpdatedReclaimProof: proof,
			}
		}

		// Gate 3: Full-card zero GPU processes (K8s + host) and measured_used_vram_mib <= IdleDriverNoiseCapMiB (1024 MiB).
		// If ANY sample violates Gate 3, reset consecutive sample progress immediately!
		persistedIdle := ok && len(card.AttributedProcesses) == 0 && card.MeasuredUsedVRAMMiB <= IdleDriverNoiseCapMiB
		liveIdle := vRes.Verified && vRes.GPUProcessCount == 0 && vRes.MeasuredUsedVRAMMiB <= IdleDriverNoiseCapMiB
		if !persistedIdle || !liveIdle {
			storeUnboundSamples(proof, nil)
			return unboundEvalDecision{
				Handled:             true,
				TargetState:         "reconciling",
				Reason:              "UNBOUND_CARD_NOT_IDLE",
				UpdatedReclaimProof: proof,
			}
		}

		if !telemetryFresh || card.TelemetrySampleID <= 0 || !card.TelemetrySampledAt.UTC().After(graceFloor) {
			allCardsDualSampled = false
			continue
		}

		pair := samples[g]
		sampleAtStr := card.TelemetrySampledAt.UTC().Format(time.RFC3339Nano)
		if pair.Sample1ID <= 0 {
			pair.Sample1BootID = card.TelemetryBootID
			pair.Sample1ID = card.TelemetrySampleID
			pair.Sample1At = sampleAtStr
			pair.Sample2BootID = ""
			pair.Sample2ID = 0
			pair.Sample2At = ""
			samples[g] = pair
			allCardsDualSampled = false
			continue
		}

		if card.TelemetryBootID == pair.Sample1BootID && card.TelemetrySampleID == pair.Sample1ID {
			// Same telemetry snapshot as Sample 1; wait for next distinct telemetry_sample_id.
			if pair.Sample2ID <= 0 {
				allCardsDualSampled = false
			}
			continue
		}

		distinctNewer := (card.TelemetryBootID == pair.Sample1BootID && card.TelemetrySampleID > pair.Sample1ID) ||
			(card.TelemetryBootID != pair.Sample1BootID && card.TelemetrySampleID > 0)
		if !distinctNewer {
			allCardsDualSampled = false
			continue
		}

		if pair.Sample2ID <= 0 {
			pair.Sample2BootID = card.TelemetryBootID
			pair.Sample2ID = card.TelemetrySampleID
			pair.Sample2At = sampleAtStr
			samples[g] = pair
		}
		if vRes.MeasuredFreeVRAMMiB > 0 {
			verifiedFree[g] = vRes.MeasuredFreeVRAMMiB
		} else if card.MeasuredFreeVRAMMiB > 0 {
			verifiedFree[g] = card.MeasuredFreeVRAMMiB
		}
	}

	storeUnboundSamples(proof, samples)
	if !allCardsDualSampled {
		return unboundEvalDecision{
			Handled:             true,
			TargetState:         "reconciling",
			Reason:              "UNBOUND_AWAITING_DUAL_SAMPLE",
			UpdatedReclaimProof: proof,
		}
	}

	// Gate 5: Both consecutive post-grace samples verified 0 total compute processes & <= 1024 MiB used VRAM.
	return unboundEvalDecision{
		Handled:             true,
		TargetState:         "expired",
		Reason:              ReasonReconciledUnboundIdleGPU,
		BumpFence:           true,
		VerifiedFreeByID:    verifiedFree,
		UpdatedReclaimProof: proof,
	}
}
