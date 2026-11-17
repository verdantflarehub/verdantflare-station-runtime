package scheduler

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

type LedgerSnapshot struct {
	StationID        string                   `json:"station_id"`
	AppGPUQuota      int                      `json:"app_gpu_quota"`
	CutoverCompleted bool                     `json:"cutover_completed"`
	GeneratedAt      string                   `json:"generated_at"`
	Cards            []CardSnapshot           `json:"cards"`
	ActiveLeases     []LeaseDecision          `json:"active_leases"`
	Residencies      []ResidencySnapshot      `json:"residencies"`
	RecentEvents     []SchedulerEventSnapshot `json:"recent_events"`
}

type SchedulerEventSnapshot struct {
	EventID      int64  `json:"event_id"`
	NodeUID      string `json:"node_uid"`
	GPUUUID      string `json:"gpu_uuid"`
	GPUIndex     int    `json:"gpu_index"`
	CardEventSeq int64  `json:"card_event_seq"`
	OccurredAt   string `json:"occurred_at"`
	EntityType   string `json:"entity_type"`
	EntityID     string `json:"entity_id"`
	FromState    string `json:"from_state"`
	ToState      string `json:"to_state"`
	BudgetMiB    int64  `json:"budget_mib"`
	ReasonCode   string `json:"reason_code"`
}

type ResidencyEvictRequest struct {
	IdempotencyKey       string `json:"idempotency_key,omitempty"`
	ExpectedBootID       string `json:"expected_boot_id,omitempty"`
	ExpectedGPUEpoch     *int64 `json:"expected_gpu_epoch,omitempty"`
	ExpectedResidencySeq *int64 `json:"expected_residency_seq,omitempty"`
	ActorUserID          string `json:"actor_user_id,omitempty"`
	OrganizationID       string `json:"organization_id,omitempty"`
	RunNonce             string `json:"run_nonce,omitempty"`
}

type ResidencyForceDrainRequest struct {
	IdempotencyKey       string `json:"idempotency_key,omitempty"`
	ConfirmForceDrain    bool   `json:"confirm_force_drain"`
	ConfirmInstanceID    string `json:"confirm_instance_id"`
	ExpectedBootID       string `json:"expected_boot_id,omitempty"`
	ExpectedGPUEpoch     *int64 `json:"expected_gpu_epoch,omitempty"`
	ExpectedResidencySeq *int64 `json:"expected_residency_seq,omitempty"`
	ActorUserID          string `json:"actor_user_id,omitempty"`
	OrganizationID       string `json:"organization_id,omitempty"`
	Reason               string `json:"reason,omitempty"`
	RunNonce             string `json:"run_nonce,omitempty"`
}

type ResidencyActionResult struct {
	InstanceID       string `json:"instance_id"`
	AppID            string `json:"app_id"`
	FromState        string `json:"from_state"`
	ResidencyState   string `json:"residency_state"`
	BootID           string `json:"boot_id"`
	GPUEpoch         int64  `json:"gpu_epoch"`
	ResidencySeq     int64  `json:"residency_seq"`
	ReclaimedVRAMMiB int64  `json:"reclaimed_vram_mib"`
	LedgerEventSeq   int64  `json:"ledger_event_seq"`
}

type CardSnapshot struct {
	NodeUID                    string  `json:"node_uid"`
	NodeName                   string  `json:"node_name"`
	GPUUUID                    string  `json:"gpu_uuid"`
	GPUIndex                   int     `json:"gpu_index"`
	ModelName                  string  `json:"model_name"`
	TotalVRAMMiB               int64   `json:"total_vram_mib"`
	DriverOverheadMiB          int64   `json:"driver_overhead_mib"`
	HeadroomMiB                int64   `json:"headroom_mib"`
	PinnedMiB                  int64   `json:"pinned_mib"`
	LeasedMiB                  int64   `json:"leased_mib"`
	EvictableMiB               int64   `json:"evictable_mib"`
	QuarantinedMiB             int64   `json:"quarantined_mib"`
	UnattributedExternalMiB    int64   `json:"unattributed_external_mib"`
	UnmaterializedBudgetMiB    int64   `json:"unmaterialized_budget_mib"`
	LedgerUncommittedMiB       int64   `json:"ledger_uncommitted_mib"`
	MeasuredFreeVRAMMiB        int64   `json:"measured_free_vram_mib"`
	MeasuredUsedVRAMMiB        int64   `json:"measured_used_vram_mib"`
	GrantableStandardMiB       int64   `json:"grantable_standard_mib"`
	TelemetryFresh             bool    `json:"telemetry_fresh"`
	AdmissionOpen              bool    `json:"admission_open"`
	TelemetryBootID            string  `json:"telemetry_boot_id,omitempty"`
	TelemetrySampleID          int64   `json:"telemetry_sample_id"`
	TelemetrySampledAt         string  `json:"telemetry_sampled_at,omitempty"`
	FencingEpoch               int64   `json:"fencing_epoch"`
	MinFencingToken            int64   `json:"min_fencing_token"`
	ReservedByLeaseID          *string `json:"reserved_by_lease_id,omitempty"`
	TimeSlicingSlots           int     `json:"time_slicing_slots"`
	AllocatedK8sSlots          int     `json:"allocated_k8s_slots"`
	ScalingUpK8sSlots          int     `json:"scaling_up_k8s_slots"`
	UnscheduledPendingK8sSlots int     `json:"unscheduled_pending_k8s_slots"`
	AdmissionMode              string  `json:"admission_mode"`
	State                      string  `json:"state"`
	StateReason                string  `json:"state_reason"`
	LastEventSeq               int64   `json:"last_event_seq"`
	LastEventHash              string  `json:"last_event_hash"`
}

type ResidencySnapshot struct {
	InstanceID           string `json:"instance_id"`
	AppID                string `json:"app_id"`
	NodeUID              string `json:"node_uid"`
	PodUID               string `json:"pod_uid"`
	BootID               string `json:"boot_id"`
	GPUEpoch             int64  `json:"gpu_epoch"`
	ResidencySeq         int64  `json:"residency_seq"`
	GPUUUID              string `json:"gpu_uuid"`
	ResidencyState       string `json:"residency_state"`
	HumanProtected       bool   `json:"human_protected"`
	GUIConnectionAlive   bool   `json:"gui_connection_alive"`
	Evictable            bool   `json:"evictable"`
	Dirty                bool   `json:"dirty"`
	SnapshotInProgress   bool   `json:"snapshot_in_progress"`
	SnapshotRequired     bool   `json:"snapshot_required"`
	BlockingReason       string `json:"blocking_reason"`
	PinnedBudgetMiB      int64  `json:"pinned_budget_mib"`
	VRAMAllocatedMiB     *int64 `json:"vram_allocated_mib,omitempty"`
	ProvenVRAMMiB        int64  `json:"proven_vram_mib"`
	ActiveComputeLeaseID string `json:"active_compute_lease_id,omitempty"`
	IdleSeconds          int64  `json:"idle_seconds"`
	TemplateUpgraded     bool   `json:"template_upgraded"`
}

type AdmissionModeRequest struct {
	IdempotencyKey   string `json:"idempotency_key,omitempty"`
	NodeUID          string `json:"node_uid,omitempty"`
	GPUUUID          string `json:"gpu_uuid,omitempty"`
	AdmissionMode    string `json:"admission_mode"`
	Reason           string `json:"reason,omitempty"`
	CutoverCompleted *bool  `json:"cutover_completed,omitempty"`
	RunNonce         string `json:"run_nonce,omitempty"`
}

type ResidencyPruneStoppedRequest struct {
	IdempotencyKey string `json:"idempotency_key,omitempty"`
	Reason         string `json:"reason,omitempty"`
	RunNonce       string `json:"run_nonce,omitempty"`
}

type RestartObservation struct {
	NodeUID           string
	LivePodUIDs       map[string]bool
	CallbackByAppTask map[string]string
	RunNonce          string
}

func isAllowedAppCallbackPort(port int) bool {
	switch port {
	case 7860, 8000, 8001, 8080, 8188, 48084:
		return true
	default:
		return false
	}
}

func validateInternalCallbackEndpoint(rawURL string, allowLoopback bool) (string, error) {
	trimmed := strings.TrimSpace(rawURL)
	if trimmed == "" {
		return "", nil
	}
	u, err := url.Parse(trimmed)
	if err != nil {
		return "", ErrInvalidRequest
	}
	if u.Scheme != "http" || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
		return "", ErrInvalidRequest
	}
	if u.Path != "" && u.Path != "/" {
		return "", ErrInvalidRequest
	}
	host := strings.ToLower(strings.TrimSpace(u.Hostname()))
	portStr := strings.TrimSpace(u.Port())
	if host == "" || portStr == "" {
		return "", ErrInvalidRequest
	}
	port, err := strconv.Atoi(portStr)
	if err != nil || port < 1 || port > 65535 {
		return "", ErrInvalidRequest
	}

	if ip := net.ParseIP(host); ip != nil {
		if ip.IsLoopback() && allowLoopback {
			return fmt.Sprintf("http://%s", net.JoinHostPort(host, portStr)), nil
		}
		return "", ErrInvalidRequest
	}

	if host == "localhost" {
		if !allowLoopback {
			return "", ErrInvalidRequest
		}
		return fmt.Sprintf("http://%s:%d", host, port), nil
	}
	if !strings.HasSuffix(host, ".verdantflare-app.svc.cluster.local") &&
		!strings.HasSuffix(host, ".verdantflare-app.pod.cluster.local") &&
		!strings.HasSuffix(host, ".verdantflare-app.svc") {
		return "", ErrInvalidRequest
	}
	if !isAllowedAppCallbackPort(port) {
		return "", ErrInvalidRequest
	}
	return fmt.Sprintf("http://%s:%d", host, port), nil
}

func isApprovedPostprocessStage(stage string) bool {
	switch strings.ToLower(strings.TrimSpace(stage)) {
	case "postprocess_sr", "postprocess_rife", "postprocess_depth", "postprocess", "p3_postprocess":
		return true
	default:
		return false
	}
}

func filterOutHolderProcesses(procs []AttributedGPUProcess, podUID, containerID string) []AttributedGPUProcess {
	pUID := normPodUID(podUID)
	cID := normContainerID(containerID)
	if len(procs) == 0 || (pUID == "" && cID == "") {
		return procs
	}
	out := make([]AttributedGPUProcess, 0, len(procs))
	for _, p := range procs {
		apPod := normPodUID(p.PodUID)
		apCID := normContainerID(p.ContainerID)
		if holderMatchesAttributedProcess(pUID, cID, apPod, apCID) {
			continue
		}
		out = append(out, p)
	}
	return out
}

type rowQueryer interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

func parseProofInt64(v any) (int64, bool) {
	switch val := v.(type) {
	case int:
		return int64(val), true
	case int32:
		return int64(val), true
	case int64:
		return val, true
	case float64:
		if float64(int64(val)) == val {
			return int64(val), true
		}
	case json.Number:
		n, err := val.Int64()
		if err == nil {
			return n, true
		}
	}
	return 0, false
}

func parseProofUint64(v any) (uint64, bool) {
	if n, ok := parseProofInt64(v); ok && n > 0 {
		return uint64(n), true
	}
	if val, ok := v.(uint64); ok && val > 0 {
		return val, true
	}
	return 0, false
}

type verifiedEpochSeal struct {
	GPUEpoch                 int64
	SupervisorPID            int
	SupervisorStarttimeTicks uint64
	ResidentProcs            []ResidentProcIdentity
}

// validateEpochSealProof verifies structured epoch seal metadata (including supervisor starttime ticks
// and irrevocable grant consumption fence when expectedLeaseID != "") before station-runtime
// asserts EpochFenceConfirmed=true on an epoch_unload Evict, Downgrade, Release, or ReconcileRestart.
func validateEpochSealProof(
	proof map[string]any,
	expectedPodUID, expectedCID, expectedBootID string,
	expectedEpoch int64,
	topSupervisorPID int,
	topSupervisorTicks uint64,
	expectedLeaseID string,
	expectedFencingToken int64,
) (verifiedEpochSeal, bool) {
	if expectedEpoch <= 0 || len(proof) == 0 {
		return verifiedEpochSeal{}, false
	}
	reclaimed, ok := proof["reclaimed"].(bool)
	if !ok || !reclaimed {
		return verifiedEpochSeal{}, false
	}
	sealedEpoch, ok := parseProofInt64(proof["sealed_gpu_epoch"])
	if !ok || sealedEpoch != expectedEpoch {
		return verifiedEpochSeal{}, false
	}
	supPID := topSupervisorPID
	if proofPID, ok := parseProofInt64(proof["supervisor_pid"]); ok && proofPID > 0 {
		if supPID > 0 && int(proofPID) != supPID {
			return verifiedEpochSeal{}, false
		}
		supPID = int(proofPID)
	}
	if supPID <= 0 {
		return verifiedEpochSeal{}, false
	}
	supTicks := topSupervisorTicks
	if proofTicks, ok := parseProofUint64(proof["supervisor_starttime_ticks"]); ok && proofTicks > 0 {
		if supTicks > 0 && proofTicks != supTicks {
			return verifiedEpochSeal{}, false
		}
		supTicks = proofTicks
	}
	if supTicks == 0 {
		return verifiedEpochSeal{}, false
	}
	sealedPod, _ := proof["sealed_pod_uid"].(string)
	if normPodUID(sealedPod) == "" || (normPodUID(expectedPodUID) != "" && normPodUID(sealedPod) != normPodUID(expectedPodUID)) {
		return verifiedEpochSeal{}, false
	}
	sealedCID, _ := proof["sealed_container_id"].(string)
	if normContainerID(expectedCID) != "" && normContainerID(sealedCID) != "" && normContainerID(sealedCID) != normContainerID(expectedCID) {
		return verifiedEpochSeal{}, false
	}
	sealedBoot, _ := proof["sealed_boot_id"].(string)
	sealedBoot = strings.TrimSpace(sealedBoot)
	expectedBootID = strings.TrimSpace(expectedBootID)
	if sealedBoot == "" || (expectedBootID != "" && sealedBoot != expectedBootID) {
		return verifiedEpochSeal{}, false
	}
	if strings.TrimSpace(expectedLeaseID) != "" {
		revokedLease, _ := proof["revoked_lease_id"].(string)
		if strings.TrimSpace(revokedLease) != strings.TrimSpace(expectedLeaseID) {
			return verifiedEpochSeal{}, false
		}
		if expectedFencingToken > 0 {
			consumedFence, okC := parseProofInt64(proof["consumed_fencing_token"])
			revokedFence, okR := parseProofInt64(proof["revoked_fencing_token"])
			if (!okC || consumedFence < expectedFencingToken) && (!okR || revokedFence < expectedFencingToken) {
				return verifiedEpochSeal{}, false
			}
		}
	}
	return verifiedEpochSeal{
		GPUEpoch:                 sealedEpoch,
		SupervisorPID:            supPID,
		SupervisorStarttimeTicks: supTicks,
	}, true
}

func isUnknownPlaceholder(s string) bool {
	trimmed := strings.ToLower(strings.TrimSpace(s))
	return trimmed == "" || strings.HasPrefix(trimmed, "unknown")
}

// validateWarmSettledProof verifies structured warm-worker settle metadata for stateless
// single-worker CUDA services (e.g. RIFE, SR, Depth, UVR5, warm IndexTTS2/Music3/Moss)
// whose main process remains CONTAINER_RUNNING after completing inference while any
// resident weights stay tracked via NVML UnattributedExternalMiB / MeasuredFreeVRAMMiB.
// Caller-fabricated proofs are strictly rejected: proof_source="receiver_guard",
// token_sealed=true, and non-empty bound target identity (pod_uid, container_id, boot_id) are mandatory.
func validateWarmSettledProof(
	proof map[string]any,
	expectedPodUID, expectedCID, expectedBootID string,
	topSupervisorPID int,
	topSupervisorTicks uint64,
	expectedLeaseID string,
	expectedFencingToken int64,
) (verifiedEpochSeal, bool) {
	if len(proof) == 0 || strings.TrimSpace(expectedLeaseID) == "" || expectedFencingToken <= 0 {
		return verifiedEpochSeal{}, false
	}
	if src, _ := proof["proof_source"].(string); strings.TrimSpace(src) != "receiver_guard" {
		return verifiedEpochSeal{}, false
	}
	if sealed, ok := proof["token_sealed"].(bool); !ok || !sealed {
		return verifiedEpochSeal{}, false
	}
	reclaimed, ok := proof["reclaimed"].(bool)
	if !ok || !reclaimed {
		return verifiedEpochSeal{}, false
	}
	activeTasks, okActive := parseProofInt64(proof["active_tasks"])
	if !okActive || activeTasks != 0 {
		return verifiedEpochSeal{}, false
	}
	supPID := topSupervisorPID
	if proofPID, ok := parseProofInt64(proof["supervisor_pid"]); ok && proofPID > 0 {
		if supPID > 0 && int(proofPID) != supPID {
			return verifiedEpochSeal{}, false
		}
		supPID = int(proofPID)
	}
	if supPID <= 0 {
		return verifiedEpochSeal{}, false
	}
	supTicks := topSupervisorTicks
	if proofTicks, ok := parseProofUint64(proof["supervisor_starttime_ticks"]); ok && proofTicks > 0 {
		if supTicks > 0 && proofTicks != supTicks {
			return verifiedEpochSeal{}, false
		}
		supTicks = proofTicks
	}
	if supTicks == 0 {
		return verifiedEpochSeal{}, false
	}
	sealedPod, _ := proof["sealed_pod_uid"].(string)
	if isUnknownPlaceholder(sealedPod) || isUnknownPlaceholder(expectedPodUID) ||
		normPodUID(sealedPod) == "" || normPodUID(expectedPodUID) == "" ||
		normPodUID(sealedPod) != normPodUID(expectedPodUID) {
		return verifiedEpochSeal{}, false
	}
	sealedCID, _ := proof["sealed_container_id"].(string)
	if isUnknownPlaceholder(sealedCID) || isUnknownPlaceholder(expectedCID) ||
		normContainerID(sealedCID) == "" || normContainerID(expectedCID) == "" ||
		normContainerID(sealedCID) != normContainerID(expectedCID) {
		return verifiedEpochSeal{}, false
	}
	sealedBoot, _ := proof["sealed_boot_id"].(string)
	sealedBoot = strings.TrimSpace(sealedBoot)
	expectedBootID = strings.TrimSpace(expectedBootID)
	if isUnknownPlaceholder(sealedBoot) || isUnknownPlaceholder(expectedBootID) || sealedBoot != expectedBootID {
		return verifiedEpochSeal{}, false
	}
	revokedLease, _ := proof["revoked_lease_id"].(string)
	if strings.TrimSpace(revokedLease) != strings.TrimSpace(expectedLeaseID) {
		return verifiedEpochSeal{}, false
	}
	consumedFence, okC := parseProofInt64(proof["consumed_fencing_token"])
	revokedFence, okR := parseProofInt64(proof["revoked_fencing_token"])
	if (!okC || consumedFence < expectedFencingToken) && (!okR || revokedFence < expectedFencingToken) {
		return verifiedEpochSeal{}, false
	}
	var residentProcs []ResidentProcIdentity
	if rawProcs, hasProcs := proof["resident_procs"]; hasProcs && rawProcs != nil {
		switch typed := rawProcs.(type) {
		case []ResidentProcIdentity:
			if len(typed) > 128 {
				return verifiedEpochSeal{}, false
			}
			for _, rp := range typed {
				if rp.PID <= 0 || rp.StarttimeTicks == 0 {
					return verifiedEpochSeal{}, false
				}
				residentProcs = append(residentProcs, rp)
			}
		case []any:
			if len(typed) > 128 {
				return verifiedEpochSeal{}, false
			}
			for _, item := range typed {
				m, ok := item.(map[string]any)
				if !ok {
					return verifiedEpochSeal{}, false
				}
				pid, okP := parseProofInt64(m["pid"])
				ticks, okT := parseProofUint64(m["starttime_ticks"])
				if !okP || pid <= 0 || !okT || ticks == 0 {
					return verifiedEpochSeal{}, false
				}
				residentProcs = append(residentProcs, ResidentProcIdentity{
					PID:            int(pid),
					StarttimeTicks: ticks,
				})
			}
		default:
			return verifiedEpochSeal{}, false
		}
	}
	return verifiedEpochSeal{
		SupervisorPID:            supPID,
		SupervisorStarttimeTicks: supTicks,
		ResidentProcs:            residentProcs,
	}, true
}

// VerifyDatabasePrivilegeIsolation enforces P1-M2 at startup/readiness:
// verifies G2 role separation (vf_t4_runner has zero access to events, vf_t4_auditor is read-only)
// and ensures vf_gpu_runtime (and current_user in strict mode) is least-privilege without DELETE/TRUNCATE/UPDATE on events.
func (s *Service) VerifyDatabasePrivilegeIsolation(ctx context.Context, requireNonSuperuserRuntime bool) error {
	if s == nil || s.Pool == nil {
		return errors.New("nil scheduler pool")
	}
	return verifyDBPrivilegeIsolation(ctx, s.Pool, requireNonSuperuserRuntime)
}

func verifyDBPrivilegeIsolation(ctx context.Context, q rowQueryer, requireNonSuperuserRuntime bool) error {
	var curUser string
	var isSuper, bypassRLS, memberOfSuper bool
	var hasRunner, hasAuditor, hasRuntime bool
	if err := q.QueryRow(ctx, `
		SELECT current_user,
		       (SELECT rolsuper FROM pg_roles WHERE rolname = current_user),
		       (SELECT rolbypassrls FROM pg_roles WHERE rolname = current_user),
		       EXISTS(
		           SELECT 1 FROM pg_roles r
		            WHERE r.rolsuper
		              AND pg_has_role(current_user, r.oid, 'MEMBER')
		       ),
		       EXISTS(SELECT 1 FROM pg_roles WHERE rolname = 'vf_t4_runner'),
		       EXISTS(SELECT 1 FROM pg_roles WHERE rolname = 'vf_t4_auditor'),
		       EXISTS(SELECT 1 FROM pg_roles WHERE rolname = 'vf_gpu_runtime')
	`).Scan(&curUser, &isSuper, &bypassRLS, &memberOfSuper, &hasRunner, &hasAuditor, &hasRuntime); err != nil {
		return err
	}
	if !hasRunner || !hasAuditor || !hasRuntime {
		return errors.New("missing G2 database roles (vf_t4_runner, vf_t4_auditor, vf_gpu_runtime)")
	}
	var runnerSelect, auditorSelect, auditorInsert, auditorUpdate, auditorDelete bool
	var runtimeEvSelect, runtimeEvInsert, runtimeEvUpdate, runtimeEvDelete, runtimeEvTrunc bool
	var runtimeAnyDeleteOrTrunc, runtimeBaseReadOK bool
	if err := q.QueryRow(ctx, `
		SELECT has_table_privilege('vf_t4_runner', 'station.gpu_scheduler_events', 'SELECT'),
		       has_table_privilege('vf_t4_auditor', 'station.gpu_scheduler_events', 'SELECT'),
		       has_table_privilege('vf_t4_auditor', 'station.gpu_scheduler_events', 'INSERT'),
		       has_table_privilege('vf_t4_auditor', 'station.gpu_scheduler_events', 'UPDATE'),
		       has_table_privilege('vf_t4_auditor', 'station.gpu_scheduler_events', 'DELETE'),
		       has_table_privilege('vf_gpu_runtime', 'station.gpu_scheduler_events', 'SELECT'),
		       has_table_privilege('vf_gpu_runtime', 'station.gpu_scheduler_events', 'INSERT'),
		       has_table_privilege('vf_gpu_runtime', 'station.gpu_scheduler_events', 'UPDATE'),
		       has_table_privilege('vf_gpu_runtime', 'station.gpu_scheduler_events', 'DELETE'),
		       has_table_privilege('vf_gpu_runtime', 'station.gpu_scheduler_events', 'TRUNCATE'),
		       (
		           has_table_privilege('vf_gpu_runtime', 'station.gpu_physical_cards', 'DELETE') OR
		           has_table_privilege('vf_gpu_runtime', 'station.gpu_physical_cards', 'TRUNCATE') OR
		           has_table_privilege('vf_gpu_runtime', 'station.gpu_resource_leases', 'DELETE') OR
		           has_table_privilege('vf_gpu_runtime', 'station.gpu_resource_leases', 'TRUNCATE') OR
		           has_table_privilege('vf_gpu_runtime', 'station.gpu_instance_residency', 'DELETE') OR
		           has_table_privilege('vf_gpu_runtime', 'station.gpu_instance_residency', 'TRUNCATE')
		       ),
		       (
		           has_table_privilege('vf_gpu_runtime', 'station.configuration', 'SELECT') AND
		           has_table_privilege('vf_gpu_runtime', 'station.app_installations', 'SELECT') AND
		           has_table_privilege('vf_gpu_runtime', 'station.app_instance_workspaces', 'SELECT') AND
		           has_table_privilege('vf_gpu_runtime', 'station.app_instance_starts', 'SELECT') AND
		           has_table_privilege('vf_gpu_runtime', 'station.app_instance_stops', 'SELECT') AND
		           has_table_privilege('vf_gpu_runtime', 'station.runtime_instance_workspaces', 'SELECT') AND
		           has_table_privilege('vf_gpu_runtime', 'station.runtime_instance_executions', 'SELECT')
		       )
	`).Scan(
		&runnerSelect, &auditorSelect, &auditorInsert, &auditorUpdate, &auditorDelete,
		&runtimeEvSelect, &runtimeEvInsert, &runtimeEvUpdate, &runtimeEvDelete, &runtimeEvTrunc,
		&runtimeAnyDeleteOrTrunc, &runtimeBaseReadOK,
	); err != nil {
		return err
	}
	if runnerSelect {
		return errors.New("G2 violation: vf_t4_runner has SELECT on station.gpu_scheduler_events")
	}
	if !auditorSelect || auditorInsert || auditorUpdate || auditorDelete {
		return errors.New("G2 violation: vf_t4_auditor privileges are not strictly read-only")
	}
	if !runtimeEvSelect || !runtimeEvInsert || runtimeEvUpdate || runtimeEvDelete || runtimeEvTrunc {
		return errors.New("G2 violation: vf_gpu_runtime must have SELECT,INSERT only on station.gpu_scheduler_events")
	}
	if runtimeAnyDeleteOrTrunc {
		return errors.New("G2 violation: vf_gpu_runtime must not have DELETE or TRUNCATE on GPU scheduler tables")
	}
	if !runtimeBaseReadOK {
		return errors.New("G2 violation: vf_gpu_runtime missing SELECT on station runtime readiness/instance-directory tables")
	}
	if requireNonSuperuserRuntime || curUser == "vf_gpu_runtime" {
		if requireNonSuperuserRuntime && curUser != "vf_gpu_runtime" {
			return fmt.Errorf("station-runtime must connect as vf_gpu_runtime when strict DB role isolation is enabled (current_user=%s)", curUser)
		}
		if isSuper || bypassRLS || memberOfSuper {
			return errors.New("station-runtime must not connect as superuser, bypassrls, or member of a superuser role")
		}
		var curUpdate, curDelete, curTrunc, curAnyDelete bool
		if err := q.QueryRow(ctx, `
			SELECT has_table_privilege(current_user, 'station.gpu_scheduler_events', 'UPDATE'),
			       has_table_privilege(current_user, 'station.gpu_scheduler_events', 'DELETE'),
			       has_table_privilege(current_user, 'station.gpu_scheduler_events', 'TRUNCATE'),
			       (
			           has_table_privilege(current_user, 'station.gpu_physical_cards', 'DELETE') OR
			           has_table_privilege(current_user, 'station.gpu_resource_leases', 'DELETE') OR
			           has_table_privilege(current_user, 'station.gpu_instance_residency', 'DELETE')
			       )
		`).Scan(&curUpdate, &curDelete, &curTrunc, &curAnyDelete); err != nil {
			return err
		}
		if curUpdate || curDelete || curTrunc || curAnyDelete {
			return fmt.Errorf("current DB role %s has forbidden UPDATE/DELETE/TRUNCATE on GPU scheduler tables", curUser)
		}
	}
	return nil
}

func (s *Service) executeDrainingEviction(
	ctx context.Context,
	caller CallerIdentity,
	req AcquireRequest,
	isAutoPromote bool,
	leaseID string,
	nodeUID string,
	cmds []EvictVRAMCommand,
) (LeaseDecision, error) {
	s.fireHook(ctx, "before_draining_eviction_rpc", map[string]string{
		"lease_id": leaseID,
		"node_uid": nodeUID,
	})

	for _, cmd := range cmds {
		evRes, err := s.Evictor.EvictVRAM(ctx, cmd)
		if err != nil {
			return s.revertDrainingToQueued(ctx, leaseID, nodeUID, "eviction_rpc_error", req.RunNonce, "", 0)
		}
		if evRes.Conflict {
			reason := evRes.ReasonCode
			if reason == "" {
				reason = "eviction_conflict_human_or_compute"
			}
			return s.revertDrainingToQueued(ctx, leaseID, nodeUID, reason, req.RunNonce, "", 0)
		}
		if evRes.Unreclaimed || !evRes.Settled {
			quarantinedInstance := cmd.InstanceID
			var leakMiB int64
			if s.Verifier != nil {
				var podUID, containerID, bootID, gpuUUID string
				_ = s.Pool.QueryRow(ctx, `
					SELECT pod_uid, container_id, boot_id, gpu_uuid
					  FROM station.gpu_instance_residency
					 WHERE station_id = $1 AND instance_id = $2
				`, s.StationID, cmd.InstanceID).Scan(&podUID, &containerID, &bootID, &gpuUUID)
				_, _ = s.Verifier.VerifyTerminated(ctx, VerifyTerminatedRequest{
					NodeUID:     nodeUID,
					GPUUUID:     gpuUUID,
					PodUID:      podUID,
					ContainerID: containerID,
					BootID:      bootID,
					GPUEpoch:    cmd.ExpectedGPUEpoch,
					Mode:        "epoch_unload",
				})
			}
			return s.revertDrainingToQueued(ctx, leaseID, nodeUID, "eviction_peer_quarantined", req.RunNonce, quarantinedInstance, leakMiB)
		}

		// Record settled zero_vram_standby transition and refresh measured free VRAM strictly from NVML.
		if err := s.applyEvictedStandby(ctx, nodeUID, cmd, evRes, req.RunNonce, nil); err != nil {
			if errors.Is(err, ErrVersionConflict) {
				return s.revertDrainingToQueued(ctx, leaseID, nodeUID, "eviction_cas_conflict", req.RunNonce, "", 0)
			}
			if errors.Is(err, ErrReclaimQuarantined) {
				return s.revertDrainingToQueued(ctx, leaseID, nodeUID, "eviction_peer_quarantined", req.RunNonce, cmd.InstanceID, 0)
			}
			return LeaseDecision{}, err
		}
	}

	req.RequestSHA256 = ""
	return s.acquireInternal(ctx, caller, req, isAutoPromote)
}

type boundLeaseRecord struct {
	LeaseID   string `json:"lease_id"`
	Fencing   int64  `json:"fencing_token"`
	AttemptID string `json:"attempt_id,omitempty"`
}

type residencyActionIntent struct {
	ReasonCode         string             `json:"reason_code"`
	InstanceID         string             `json:"instance_id"`
	AppID              string             `json:"app_id"`
	NodeUID            string             `json:"node_uid"`
	GPUUUID            string             `json:"gpu_uuid"`
	PodUID             string             `json:"pod_uid"`
	ContainerID        string             `json:"container_id"`
	TargetBootID       string             `json:"target_boot_id"`
	TargetGPUEpoch     int64              `json:"target_gpu_epoch"`
	TargetResidencySeq int64              `json:"target_residency_seq"`
	FromState          string             `json:"from_state"`
	ReclaimedVRAMMiB   int64              `json:"reclaimed_vram_mib"`
	BoundLeases        []boundLeaseRecord `json:"bound_leases,omitempty"`
}

type residencyActionState struct {
	Completed bool
	Result    ResidencyActionResult
	HasIntent bool
	Intent    residencyActionIntent
}

type residencyIdemEventMeta struct {
	ReasonCode       string
	IdempotencyScope string
	IdempotencyKey   string
	RequestSHA256    string
	ActorUserID      string
	OrganizationID   string
	AppID            string
	ResumeIntent     *residencyActionIntent
	ResultOut        *ResidencyActionResult
}

func validExpectedResidencyTarget(bootID string, epoch, seq *int64) bool {
	trimmed := strings.TrimSpace(bootID)
	if trimmed == "" || len(trimmed) > 128 {
		return false
	}
	if epoch == nil || *epoch < 0 || seq == nil || *seq < 0 {
		return false
	}
	return true
}

func normalizeEvictVRAMResult(evRes EvictVRAMResult) EvictVRAMResult {
	if evRes.BootID == "" && evRes.ActualBootID != "" {
		evRes.BootID = evRes.ActualBootID
	}
	if evRes.GPUEpoch == 0 && evRes.ActualGPUEpoch > 0 {
		evRes.GPUEpoch = evRes.ActualGPUEpoch
	}
	if evRes.ResidencySeq == 0 && evRes.ActualResidencySeq > 0 {
		evRes.ResidencySeq = evRes.ActualResidencySeq
	}
	if len(evRes.Data) > 0 {
		if evRes.BootID == "" {
			if b, ok := evRes.Data["current_boot_id"].(string); ok && b != "" {
				evRes.BootID = b
			}
		}
		if evRes.GPUEpoch == 0 {
			if ep, ok := parseProofInt64(evRes.Data["current_gpu_epoch"]); ok && ep > 0 {
				evRes.GPUEpoch = ep
			}
		}
		if evRes.ResidencySeq == 0 {
			if sq, ok := parseProofInt64(evRes.Data["current_residency_seq"]); ok && sq > 0 {
				evRes.ResidencySeq = sq
			}
		}
	}
	return evRes
}

func (s *Service) lookupResidencyActionState(ctx context.Context, scope, key, reqSHA256 string) (residencyActionState, error) {
	rows, err := s.Pool.Query(ctx, `
		SELECT reason_code,
		       COALESCE(payload->>'request_sha256', ''),
		       payload->'action_result',
		       payload->'action_intent'
		  FROM station.gpu_scheduler_events
		 WHERE station_id = $1
		   AND entity_type = 'residency'
		   AND reason_code IN ('EVICT_SETTLED', 'USER_RESIDENCY_EVICT', 'ADMIN_FORCE_DRAIN', 'EVICT_INTENT', 'FORCE_DRAIN_INTENT')
		   AND payload->>'idempotency_scope' = $2
		   AND payload->>'idempotency_key' = $3
		 ORDER BY event_id DESC
		 LIMIT 4
	`, s.StationID, scope, key)
	if err != nil {
		return residencyActionState{}, err
	}
	defer rows.Close()

	var out residencyActionState
	for rows.Next() {
		var reasonCode, storedSHA string
		var resultRaw, intentRaw []byte
		if err := rows.Scan(&reasonCode, &storedSHA, &resultRaw, &intentRaw); err != nil {
			return residencyActionState{}, err
		}
		if storedSHA != reqSHA256 {
			return residencyActionState{}, ErrIdempotencyConflict
		}
		switch reasonCode {
		case "EVICT_SETTLED", "USER_RESIDENCY_EVICT", "ADMIN_FORCE_DRAIN":
			if !out.Completed {
				var cached ResidencyActionResult
				if len(resultRaw) == 0 || json.Unmarshal(resultRaw, &cached) != nil || cached.InstanceID == "" {
					return residencyActionState{}, errors.New("corrupted cached residency action_result")
				}
				out.Completed = true
				out.Result = cached
			}
		case "EVICT_INTENT", "FORCE_DRAIN_INTENT":
			if !out.HasIntent {
				var intent residencyActionIntent
				if len(intentRaw) == 0 || json.Unmarshal(intentRaw, &intent) != nil || intent.InstanceID == "" || intent.TargetBootID == "" {
					return residencyActionState{}, errors.New("corrupted persisted residency action_intent")
				}
				out.HasIntent = true
				out.Intent = intent
			}
		}
	}
	if err := rows.Err(); err != nil {
		return residencyActionState{}, err
	}
	return out, nil
}

func (s *Service) persistResidencyActionIntent(
	ctx context.Context,
	instanceID, idemScope, idemKey, reqDigest, actorUserID, orgID, runNonce, reasonCode string,
	expBoot string,
	expEpoch, expSeq int64,
	isForceDrain bool,
) (residencyActionIntent, error) {
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return residencyActionIntent{}, err
	}
	defer rollbackTx(tx)

	cards, err := lockCardsForUpdate(ctx, tx, s.StationID, "")
	if err != nil {
		return residencyActionIntent{}, err
	}
	if len(cards) == 0 {
		return residencyActionIntent{}, ErrNodeStateUnknown
	}

	var r residencyRow
	err = tx.QueryRow(ctx, `
		SELECT instance_id, app_id, node_uid, pod_uid, container_name, container_id,
		       boot_id, gpu_epoch, residency_seq, gpu_uuid, residency_state,
		       human_protected, gui_connection_alive, dirty, snapshot_in_progress, snapshot_required,
		       blocking_reason, pinned_budget_mib, vram_allocated_mib, callback_endpoint, template_upgraded
		  FROM station.gpu_instance_residency
		 WHERE station_id = $1 AND instance_id = $2
		   FOR UPDATE
	`, s.StationID, instanceID).Scan(
		&r.InstanceID, &r.AppID, &r.NodeUID, &r.PodUID, &r.ContainerName, &r.ContainerID,
		&r.BootID, &r.GPUEpoch, &r.ResidencySeq, &r.GPUUUID, &r.ResidencyState,
		&r.HumanProtected, &r.GUIConnectionAlive, &r.Dirty, &r.SnapshotInProgress, &r.SnapshotRequired,
		&r.BlockingReason, &r.PinnedBudgetMiB, &r.VRAMAllocatedMiB, &r.CallbackEndpoint, &r.TemplateUpgraded,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return residencyActionIntent{}, ErrResidencyNotFound
	}
	if err != nil {
		return residencyActionIntent{}, err
	}
	if r.BootID != expBoot || r.GPUEpoch != expEpoch || r.ResidencySeq != expSeq {
		return residencyActionIntent{}, ErrVersionConflict
	}

	var boundLeases []boundLeaseRecord
	if isForceDrain {
		lRows, lErr := tx.Query(ctx, `
			SELECT lease_id::text, fencing_token, attempt_id
			  FROM station.gpu_resource_leases
			 WHERE station_id = $1
			   AND state IN ('leased', 'running', 'legacy_acquired', 'releasing', 'reclaiming')
			   AND ((pod_uid <> '' AND lower(pod_uid) = lower($2)) OR (boot_id <> '' AND boot_id = $3))
		`, s.StationID, r.PodUID, r.BootID)
		if lErr != nil {
			return residencyActionIntent{}, lErr
		}
		for lRows.Next() {
			var bl boundLeaseRecord
			if lRows.Scan(&bl.LeaseID, &bl.Fencing, &bl.AttemptID) == nil {
				boundLeases = append(boundLeases, bl)
			}
		}
		lRows.Close()
		if r.ResidencyState == "zero_vram_standby" && !r.HumanProtected && !r.GUIConnectionAlive && len(boundLeases) == 0 {
			return residencyActionIntent{}, ErrAlreadyZeroVRAM
		}
	} else {
		if r.HumanProtected || r.GUIConnectionAlive || r.ResidencyState == "human_protected" || r.ResidencyState == "human_dirty" {
			return residencyActionIntent{}, ErrGUIEditLeaseHeld
		}
		if r.ResidencyState == "zero_vram_standby" {
			return residencyActionIntent{}, ErrAlreadyZeroVRAM
		}
		var hasActiveCompute bool
		_ = tx.QueryRow(ctx, `
			SELECT EXISTS(
				SELECT 1
				  FROM station.gpu_resource_leases
				 WHERE station_id = $1
				   AND state IN ('leased', 'running', 'legacy_acquired', 'releasing', 'reclaiming')
				   AND ((pod_uid <> '' AND lower(pod_uid) = lower($2)) OR (boot_id <> '' AND boot_id = $3))
			)
		`, s.StationID, r.PodUID, r.BootID).Scan(&hasActiveCompute)
		if hasActiveCompute || r.ResidencyState == "active_compute" || r.ResidencyState == "waking" || r.ResidencyState == "sealing" || r.SnapshotInProgress {
			return residencyActionIntent{}, ErrInstanceActiveCompute
		}
		if r.ResidencyState != "idle_evictable" {
			return residencyActionIntent{}, ErrInstanceNotEvictable
		}
	}

	reclaimedMiB := int64(0)
	if r.VRAMAllocatedMiB != nil && *r.VRAMAllocatedMiB > 0 {
		reclaimedMiB = *r.VRAMAllocatedMiB
	} else if r.PinnedBudgetMiB > 0 {
		reclaimedMiB = r.PinnedBudgetMiB
	}

	intent := residencyActionIntent{
		ReasonCode:         reasonCode,
		InstanceID:         instanceID,
		AppID:              r.AppID,
		NodeUID:            r.NodeUID,
		GPUUUID:            r.GPUUUID,
		PodUID:             r.PodUID,
		ContainerID:        r.ContainerID,
		TargetBootID:       r.BootID,
		TargetGPUEpoch:     r.GPUEpoch,
		TargetResidencySeq: r.ResidencySeq,
		FromState:          r.ResidencyState,
		ReclaimedVRAMMiB:   reclaimedMiB,
		BoundLeases:        boundLeases,
	}

	targetCardIdx := -1
	for i := range cards {
		if cards[i].NodeUID == r.NodeUID && cards[i].GPUUUID == r.GPUUUID {
			targetCardIdx = i
			break
		}
	}
	if targetCardIdx < 0 {
		for i := range cards {
			if cards[i].NodeUID == r.NodeUID {
				targetCardIdx = i
				break
			}
		}
	}
	if targetCardIdx < 0 {
		targetCardIdx = len(cards) - 1
	}

	corrUUID := uuid.Must(uuid.NewV7()).String()
	if _, _, err := appendCardEvent(ctx, tx, &cards[targetCardIdx], eventRecordInput{
		RunNonce:             runNonce,
		CorrelationEventUUID: corrUUID,
		EntityType:           "residency",
		EntityID:             instanceID,
		FromState:            r.ResidencyState,
		ToState:              r.ResidencyState,
		ResidencySeq:         r.ResidencySeq,
		GPUEpoch:             r.GPUEpoch,
		BudgetMiB:            0,
		ReasonCode:           reasonCode,
		Payload: map[string]any{
			"idempotency_scope": idemScope,
			"idempotency_key":   idemKey,
			"request_sha256":    reqDigest,
			"actor_user_id":     actorUserID,
			"organization_id":   orgID,
			"action_intent":     intent,
		},
	}); err != nil {
		return residencyActionIntent{}, err
	}

	if err := tx.Commit(ctx); err != nil {
		return residencyActionIntent{}, err
	}
	return intent, nil
}

// applyEvictedStandby enforces P1-H6 & P2-H2:
//   - Unconditionally requires a valid ReclaimProof via validateEpochSealProof (empty ReclaimProof fails closed).
//   - Strict CAS on (pod_uid, container_id, boot_id, gpu_epoch, residency_seq) — never synthesizes curSeq+1 over concurrent mutations.
//   - Never fabricates MeasuredFreeVRAMMiB += reclaimedMiB; updates card VRAM ONLY from NVML-verified observation.
func (s *Service) applyEvictedStandby(
	ctx context.Context,
	nodeUID string,
	cmd EvictVRAMCommand,
	evRes EvictVRAMResult,
	runNonce string,
	idemMeta *residencyIdemEventMeta,
) error {
	var podUID, containerID, bootID, gpuUUID string
	if err := s.Pool.QueryRow(ctx, `
		SELECT pod_uid, container_id, boot_id, gpu_uuid
		  FROM station.gpu_instance_residency
		 WHERE station_id = $1 AND instance_id = $2
	`, s.StationID, cmd.InstanceID).Scan(&podUID, &containerID, &bootID, &gpuUUID); err != nil {
		return err
	}
	if cmd.ExpectedBootID != "" && bootID != cmd.ExpectedBootID {
		return ErrVersionConflict
	}
	if evRes.BootID != "" && evRes.BootID != bootID {
		return ErrVersionConflict
	}

	if s.Verifier == nil {
		return ErrNodeStateUnknown
	}
	expectSealedEpoch := cmd.ExpectedGPUEpoch
	if expectSealedEpoch <= 0 {
		expectSealedEpoch = 1
	}
	if evRes.SealedGPUEpoch > 0 && evRes.SealedGPUEpoch != expectSealedEpoch {
		return ErrVersionConflict
	}
	sealMeta, proofOK := validateEpochSealProof(
		evRes.ReclaimProof,
		podUID,
		containerID,
		bootID,
		expectSealedEpoch,
		evRes.SupervisorPID,
		evRes.SupervisorStarttimeTicks,
		"",
		0,
	)
	if !proofOK {
		return ErrReclaimQuarantined
	}
	supPID := sealMeta.SupervisorPID
	supTicks := sealMeta.SupervisorStarttimeTicks
	if supPID <= 0 || supTicks == 0 {
		return ErrReclaimQuarantined
	}
	var verifiedFreeMiB *int64
	vRes, vErr := s.Verifier.VerifyTerminated(ctx, VerifyTerminatedRequest{
		NodeUID:                  nodeUID,
		GPUUUID:                  gpuUUID,
		PodUID:                   podUID,
		ContainerID:              containerID,
		BootID:                   bootID,
		GPUEpoch:                 expectSealedEpoch,
		SupervisorPID:            supPID,
		SupervisorStarttimeTicks: supTicks,
		EpochFenceConfirmed:      evRes.Settled && supPID > 0 && supTicks > 0,
		Mode:                     "epoch_unload",
	})
	if vErr != nil || vRes.ErrorCode == "UNKNOWN_NODE_STATE" {
		return ErrNodeStateUnknown
	}
	if !vRes.Verified {
		return ErrReclaimQuarantined
	}
	if vRes.MeasuredFreeVRAMMiB > 0 {
		freeVal := vRes.MeasuredFreeVRAMMiB
		verifiedFreeMiB = &freeVal
	}

	s.fireHook(ctx, "after_evict_verify_before_commit", map[string]string{
		"instance_id": cmd.InstanceID,
		"node_uid":    nodeUID,
	})

	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer rollbackTx(tx)

	cards, err := lockCardsForUpdate(ctx, tx, s.StationID, nodeUID)
	if err != nil {
		return err
	}

	var prevState, curBootID, curPodUID, curCID string
	var prevAlloc *int64
	var curEpoch, curSeq int64
	var humanProt, guiAlive bool
	err = tx.QueryRow(ctx, `
		SELECT gpu_uuid, residency_state, vram_allocated_mib, boot_id, pod_uid, container_id, gpu_epoch, residency_seq, human_protected, gui_connection_alive
		  FROM station.gpu_instance_residency
		 WHERE station_id = $1 AND instance_id = $2
		   FOR UPDATE
	`, s.StationID, cmd.InstanceID).Scan(&gpuUUID, &prevState, &prevAlloc, &curBootID, &curPodUID, &curCID, &curEpoch, &curSeq, &humanProt, &guiAlive)
	if err != nil {
		return err
	}
	// Guard against concurrent human takeover, reboot, container swap, or newer epoch/seq that arrived during eviction RPC!
	if humanProt || guiAlive || prevState == "human_protected" || prevState == "human_dirty" {
		return ErrVersionConflict
	}
	resumeIntent := idemMeta != nil && idemMeta.ResumeIntent != nil
	if resumeIntent {
		intent := idemMeta.ResumeIntent
		if normPodUID(curPodUID) != normPodUID(podUID) ||
			normContainerID(curCID) != normContainerID(containerID) ||
			curBootID != intent.TargetBootID ||
			curEpoch != intent.TargetGPUEpoch ||
			curSeq < intent.TargetResidencySeq {
			return ErrVersionConflict
		}
		if evRes.GPUEpoch != curEpoch ||
			curSeq > evRes.ResidencySeq ||
			(curSeq == evRes.ResidencySeq && prevState != "zero_vram_standby") {
			return ErrVersionConflict
		}
	} else {
		if normPodUID(curPodUID) != normPodUID(podUID) ||
			normContainerID(curCID) != normContainerID(containerID) ||
			(cmd.ExpectedBootID != "" && curBootID != cmd.ExpectedBootID) ||
			curEpoch != cmd.ExpectedGPUEpoch ||
			curSeq != cmd.ExpectedResidencySeq {
			return ErrVersionConflict
		}
		if evRes.ResidencySeq <= curSeq || evRes.GPUEpoch != curEpoch {
			return ErrVersionConflict
		}
	}

	newSeq := evRes.ResidencySeq
	newEpoch := evRes.GPUEpoch
	zero := int64(0)
	cmdTag, err := tx.Exec(ctx, `
		UPDATE station.gpu_instance_residency
		   SET residency_state = 'zero_vram_standby',
		       residency_seq = $3,
		       gpu_epoch = $4,
		       vram_allocated_mib = $5,
		       blocking_reason = 'none',
		       updated_at = now()
		 WHERE station_id = $1
		   AND instance_id = $2
		   AND boot_id = $6
		   AND gpu_epoch = $7
		   AND residency_seq = $8
		   AND NOT human_protected
		   AND NOT gui_connection_alive
	`, s.StationID, cmd.InstanceID, newSeq, newEpoch, zero, curBootID, curEpoch, curSeq)
	if err != nil {
		return err
	}
	if cmdTag.RowsAffected() != 1 {
		return ErrVersionConflict
	}

	fromState := prevState
	reclaimedMiB := int64(0)
	if prevAlloc != nil && *prevAlloc > 0 {
		reclaimedMiB = *prevAlloc
	}
	if resumeIntent {
		if idemMeta.ResumeIntent.FromState != "" && prevState == "zero_vram_standby" {
			fromState = idemMeta.ResumeIntent.FromState
		}
		if reclaimedMiB == 0 && idemMeta.ResumeIntent.ReclaimedVRAMMiB > 0 {
			reclaimedMiB = idemMeta.ResumeIntent.ReclaimedVRAMMiB
		}
	}
	corrUUID := uuid.Must(uuid.NewV7()).String()
	eventWritten := false
	for i := range cards {
		if cards[i].GPUUUID == gpuUUID || (!eventWritten && idemMeta != nil && i == len(cards)-1) {
			cleanedAttr := filterOutHolderProcesses(cards[i].AttributedProcesses, curPodUID, curCID)
			attrRaw, _ := json.Marshal(cleanedAttr)
			if cards[i].GPUUUID == gpuUUID && verifiedFreeMiB != nil {
				newFree := *verifiedFreeMiB
				newUsed := cards[i].TotalVRAMMiB - newFree
				if newUsed < cards[i].DriverOverheadMiB {
					newUsed = cards[i].DriverOverheadMiB
				}
				if _, err := tx.Exec(ctx, `
					UPDATE station.gpu_physical_cards
					   SET measured_free_vram_mib = $4,
					       measured_used_vram_mib = $5,
					       attributed_processes = $6,
					       telemetry_sampled_at = $7,
					       updated_at = now()
					 WHERE station_id = $1 AND node_uid = $2 AND gpu_uuid = $3
				`, s.StationID, cards[i].NodeUID, cards[i].GPUUUID, newFree, newUsed, attrRaw, s.now()); err != nil {
					return err
				}
			} else {
				if _, err := tx.Exec(ctx, `
					UPDATE station.gpu_physical_cards
					   SET attributed_processes = $4, updated_at = now()
					 WHERE station_id = $1 AND node_uid = $2 AND gpu_uuid = $3
				`, s.StationID, cards[i].NodeUID, cards[i].GPUUUID, attrRaw); err != nil {
					return err
				}
			}
			reasonCode := "EVICT_SETTLED"
			var evPayload map[string]any
			var actionRes ResidencyActionResult
			if idemMeta != nil {
				if idemMeta.ReasonCode != "" {
					reasonCode = idemMeta.ReasonCode
				}
				actionRes = ResidencyActionResult{
					InstanceID:       cmd.InstanceID,
					AppID:            idemMeta.AppID,
					FromState:        fromState,
					ResidencyState:   "zero_vram_standby",
					BootID:           curBootID,
					GPUEpoch:         newEpoch,
					ResidencySeq:     newSeq,
					ReclaimedVRAMMiB: reclaimedMiB,
					LedgerEventSeq:   cards[i].LastEventSeq + 1,
				}
				evPayload = map[string]any{
					"idempotency_scope": idemMeta.IdempotencyScope,
					"idempotency_key":   idemMeta.IdempotencyKey,
					"request_sha256":    idemMeta.RequestSHA256,
					"actor_user_id":     idemMeta.ActorUserID,
					"organization_id":   idemMeta.OrganizationID,
					"action_result":     actionRes,
				}
			}
			seq, _, err := appendCardEvent(ctx, tx, &cards[i], eventRecordInput{
				RunNonce:             runNonce,
				CorrelationEventUUID: corrUUID,
				EntityType:           "residency",
				EntityID:             cmd.InstanceID,
				FromState:            fromState,
				ToState:              "zero_vram_standby",
				ResidencySeq:         newSeq,
				GPUEpoch:             newEpoch,
				BudgetMiB:            reclaimedMiB,
				ReasonCode:           reasonCode,
				Payload:              evPayload,
			})
			if err != nil {
				return err
			}
			if idemMeta != nil && idemMeta.ResultOut != nil {
				actionRes.LedgerEventSeq = seq
				*idemMeta.ResultOut = actionRes
			}
			eventWritten = true
			if cards[i].GPUUUID == gpuUUID {
				break
			}
		}
	}
	return tx.Commit(ctx)
}

func (s *Service) revertDrainingToQueued(
	ctx context.Context,
	leaseID, nodeUID, reason, runNonce, quarantineInstanceID string,
	fallbackLeakMiB int64,
) (LeaseDecision, error) {
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return LeaseDecision{}, err
	}
	defer rollbackTx(tx)

	cards, err := lockCardsForUpdate(ctx, tx, s.StationID, nodeUID)
	if err != nil {
		return LeaseDecision{}, err
	}
	corrUUID := uuid.Must(uuid.NewV7()).String()

	if quarantineInstanceID != "" {
		var qGPU, qPrev string
		var qAlloc *int64
		var qPinned int64
		qErr := tx.QueryRow(ctx, `
			SELECT gpu_uuid, residency_state, vram_allocated_mib, pinned_budget_mib
			  FROM station.gpu_instance_residency
			 WHERE station_id = $1 AND instance_id = $2
			   FOR UPDATE
		`, s.StationID, quarantineInstanceID).Scan(&qGPU, &qPrev, &qAlloc, &qPinned)
		if qErr == nil {
			leak := fallbackLeakMiB
			if qAlloc != nil && *qAlloc > leak {
				leak = *qAlloc
			}
			if leak == 0 && qPinned > 0 {
				leak = qPinned
			}
			if leak == 0 {
				leak = 4096
			}
			_, _ = tx.Exec(ctx, `
				UPDATE station.gpu_instance_residency
				   SET residency_state = 'quarantined', blocking_reason = 'eviction_unreclaimed', updated_at = now()
				 WHERE station_id = $1 AND instance_id = $2
			`, s.StationID, quarantineInstanceID)
			for i := range cards {
				if cards[i].GPUUUID == qGPU {
					cards[i].QuarantinedMiB += leak
					_, _ = tx.Exec(ctx, `
						UPDATE station.gpu_physical_cards
						   SET quarantined_mib = $4, updated_at = now()
						 WHERE station_id = $1 AND node_uid = $2 AND gpu_uuid = $3
					`, s.StationID, cards[i].NodeUID, cards[i].GPUUUID, cards[i].QuarantinedMiB)
					_, _, _ = appendCardEvent(ctx, tx, &cards[i], eventRecordInput{
						RunNonce:             runNonce,
						CorrelationEventUUID: corrUUID,
						EntityType:           "residency",
						EntityID:             quarantineInstanceID,
						FromState:            qPrev,
						ToState:              "quarantined",
						BudgetMiB:            leak,
						ReasonCode:           "EVICT_UNRECLAIMED_QUARANTINED",
					})
				}
			}
		}
	}

	if err := clearLeaseReservationsTx(ctx, tx, s.StationID, leaseID, cards, runNonce, strings.ToUpper(reason)); err != nil {
		return LeaseDecision{}, err
	}
	if _, err := tx.Exec(ctx, `
		UPDATE station.gpu_resource_leases
		   SET state = 'queued', blocking_reason = $3, version = version + 1, updated_at = now()
		 WHERE station_id = $1 AND lease_id = $2 AND state = 'draining'
	`, s.StationID, leaseID, reason); err != nil {
		return LeaseDecision{}, err
	}
	if len(cards) > 0 {
		if _, _, err := appendCardEvent(ctx, tx, &cards[0], eventRecordInput{
			RunNonce:             runNonce,
			CorrelationEventUUID: corrUUID,
			EntityType:           "lease",
			EntityID:             leaseID,
			FromState:            "draining",
			ToState:              "queued",
			ReasonCode:           strings.ToUpper(reason),
		}); err != nil {
			return LeaseDecision{}, err
		}
	}
	dec, err := s.loadDecisionTx(ctx, tx, leaseID, cards)
	if err != nil {
		return LeaseDecision{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return LeaseDecision{}, err
	}
	return dec, nil
}

func (s *Service) GetLease(ctx context.Context, caller CallerIdentity, leaseID string) (LeaseDecision, error) {
	if _, err := uuid.Parse(leaseID); err != nil {
		return LeaseDecision{}, ErrInvalidRequest
	}
	loadOnce := func() (LeaseDecision, error) {
		tx, err := s.Pool.Begin(ctx)
		if err != nil {
			return LeaseDecision{}, err
		}
		defer rollbackTx(tx)

		cards, err := lockCardsForUpdate(ctx, tx, s.StationID, "")
		if err != nil {
			return LeaseDecision{}, err
		}
		dec, err := s.loadDecisionTx(ctx, tx, leaseID, cards)
		if err != nil {
			return LeaseDecision{}, err
		}
		if caller.Kind != CallerAdmin && dec.HolderAppID != caller.AppID {
			return LeaseDecision{}, ErrForbiddenCaller
		}
		return dec, nil
	}
	dec, err := loadOnce()
	if err != nil {
		return LeaseDecision{}, err
	}
	if dec.State == "queued" || dec.State == "draining" {
		s.PromoteQueuedLeases(ctx, dec.NodeUID)
		return loadOnce()
	}
	return dec, nil
}

func computeHeartbeatHash(leaseID string, req HeartbeatRequest) string {
	if strings.TrimSpace(req.NextPodUID) != "" || strings.TrimSpace(req.NextContainerID) != "" || strings.TrimSpace(req.NextBootID) != "" {
		return computeCanonicalHash(leaseID, req.FencingToken, req.ExpectedVersion, req.DowngradeToMiB, req.NewStageID, req.WorkerSettled, req.NextPodUID, req.NextContainerID, req.NextBootID)
	}
	return computeCanonicalHash(leaseID, req.FencingToken, req.ExpectedVersion, req.DowngradeToMiB, req.NewStageID, req.WorkerSettled)
}

// Downgrade enforces P1-H5:
//   - Requires CutoverCompleted, WorkerSettled=true, non-expired TTL, approved P2->P3 postprocess stage transition,
//     node-side VerifyTerminated proof outside the card lock transaction, and strict CAS upon commit.
func (s *Service) Downgrade(ctx context.Context, caller CallerIdentity, leaseID string, req HeartbeatRequest) (LeaseDecision, error) {
	if caller.Kind != CallerConsumer && caller.Kind != CallerAdmin {
		return LeaseDecision{}, ErrForbiddenCaller
	}
	if _, err := uuid.Parse(leaseID); err != nil || req.DowngradeToMiB == nil {
		return LeaseDecision{}, ErrInvalidRequest
	}
	req.IdempotencyKey = strings.TrimSpace(req.IdempotencyKey)
	if req.IdempotencyKey == "" || len(req.IdempotencyKey) > 256 || req.FencingToken <= 0 {
		return LeaseDecision{}, ErrInvalidRequest
	}
	if req.TTLSeconds <= 0 {
		req.TTLSeconds = DefaultLeaseTTLSeconds
	}
	computedHash := computeHeartbeatHash(leaseID, req)
	if req.RequestSHA256 != "" {
		if !sha256HexPattern.MatchString(req.RequestSHA256) || req.RequestSHA256 != computedHash {
			return LeaseDecision{}, ErrInvalidRequest
		}
	}
	reqHash := computedHash

	var holderApp, snapState, snapStage, nodeUID, podUID, containerID, bootID string
	var gpuUUIDs []string
	var curPeak, curFence, snapVer int64
	var curExpiresAt time.Time
	var cacheRaw []byte
	err := s.Pool.QueryRow(ctx, `
		SELECT holder_app_id, state, stage_id, node_uid, pod_uid, container_id, boot_id,
		       gpu_uuids, peak_budget_mib, fencing_token, version, expires_at, response_cache
		  FROM station.gpu_resource_leases
		 WHERE station_id = $1 AND lease_id = $2
	`, s.StationID, leaseID).Scan(
		&holderApp, &snapState, &snapStage, &nodeUID, &podUID, &containerID, &bootID,
		&gpuUUIDs, &curPeak, &curFence, &snapVer, &curExpiresAt, &cacheRaw,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return LeaseDecision{}, ErrLeaseNotFound
	}
	if err != nil {
		return LeaseDecision{}, err
	}
	if caller.Kind != CallerAdmin && holderApp != caller.AppID {
		return LeaseDecision{}, ErrForbiddenCaller
	}
	if err := verifyWriteOnceIdentity(podUID, containerID, bootID, req.PodUID, req.ContainerID, req.BootID); err != nil {
		return LeaseDecision{}, err
	}

	cacheMap := map[string]any{}
	if len(cacheRaw) > 0 {
		_ = json.Unmarshal(cacheRaw, &cacheMap)
	}
	if prevKey, _ := cacheMap["last_hb_key"].(string); prevKey == req.IdempotencyKey {
		prevSHA, _ := cacheMap["last_hb_sha256"].(string)
		if prevSHA != reqHash {
			return LeaseDecision{}, ErrIdempotencyConflict
		}
		return s.GetLease(ctx, caller, leaseID)
	}

	if snapState == "released" || snapState == "expired" || snapState == "reclaiming" || snapState == "quarantined" {
		return LeaseDecision{}, ErrFencingRevoked
	}
	if snapState != "leased" && snapState != "running" {
		return LeaseDecision{}, ErrLeaseInactive
	}
	if !curExpiresAt.After(s.now()) || req.FencingToken != curFence {
		return LeaseDecision{}, ErrFencingRevoked
	}
	if req.ExpectedVersion > 0 && req.ExpectedVersion != snapVer {
		return LeaseDecision{}, ErrVersionConflict
	}
	if !s.CutoverCompleted {
		return LeaseDecision{}, ErrDowngradeDisabled
	}
	if !req.WorkerSettled {
		return LeaseDecision{}, ErrWorkerNotSettled
	}

	newPeak := *req.DowngradeToMiB
	newStage := strings.TrimSpace(req.NewStageID)
	// Enforce strict P2 -> P3 stage-transition invariant: must strictly lower budget to >= 4096 MiB and transition to an approved postprocess stage.
	if newPeak < 4096 || newPeak >= curPeak || !isApprovedPostprocessStage(newStage) || strings.EqualFold(snapStage, newStage) {
		return LeaseDecision{}, ErrInvalidRequest
	}

	effPodUID := podUID
	if effPodUID == "" {
		effPodUID = req.PodUID
	}
	effCID := containerID
	if effCID == "" {
		effCID = req.ContainerID
	}
	effBoot := bootID
	if effBoot == "" {
		effBoot = req.BootID
	}
	reclaimMode := strings.TrimSpace(req.ReclaimMode)
	if reclaimMode == "" {
		if req.GPUEpoch > 0 {
			reclaimMode = "epoch_unload"
		} else {
			reclaimMode = "container_exited"
		}
	}

	if s.Verifier == nil {
		_ = s.markLeaseNodeUnknownOrQuarantined(ctx, leaseID, nodeUID, gpuUUIDs, snapState, "reconciling", "UNKNOWN_NODE_STATE", 0, req.RunNonce)
		return LeaseDecision{}, ErrNodeStateUnknown
	}
	epochFenceConfirmed := true
	supPID := req.SupervisorPID
	supTicks := req.SupervisorStarttimeTicks
	var residentProcs []ResidentProcIdentity
	if reclaimMode == "epoch_unload" {
		sealMeta, proofOK := validateEpochSealProof(
			req.ReclaimProof,
			effPodUID,
			effCID,
			effBoot,
			req.GPUEpoch,
			req.SupervisorPID,
			req.SupervisorStarttimeTicks,
			"",
			0,
		)
		if !proofOK {
			_ = s.markLeaseNodeUnknownOrQuarantined(ctx, leaseID, nodeUID, gpuUUIDs, snapState, "reclaiming", "EPOCH_SEAL_PROOF_MISSING_OR_INVALID", 0, req.RunNonce)
			return LeaseDecision{}, ErrReclaimQuarantined
		}
		supPID = sealMeta.SupervisorPID
		supTicks = sealMeta.SupervisorStarttimeTicks
		epochFenceConfirmed = true
	} else if reclaimMode == "warm_idle_settled" {
		warmMeta, proofOK := validateWarmSettledProof(
			req.ReclaimProof,
			effPodUID,
			effCID,
			effBoot,
			req.SupervisorPID,
			req.SupervisorStarttimeTicks,
			leaseID,
			curFence,
		)
		if !proofOK {
			_ = s.markLeaseNodeUnknownOrQuarantined(ctx, leaseID, nodeUID, gpuUUIDs, snapState, "reclaiming", "WARM_SETTLE_PROOF_MISSING_OR_INVALID", 0, req.RunNonce)
			return LeaseDecision{}, ErrReclaimQuarantined
		}
		supPID = warmMeta.SupervisorPID
		supTicks = warmMeta.SupervisorStarttimeTicks
		residentProcs = warmMeta.ResidentProcs
		epochFenceConfirmed = true
	}

	verifiedFreeByGPU := make(map[string]int64, len(gpuUUIDs))
	for _, g := range gpuUUIDs {
		vRes, vErr := s.Verifier.VerifyTerminated(ctx, VerifyTerminatedRequest{
			NodeUID:                  nodeUID,
			GPUUUID:                  g,
			PodUID:                   effPodUID,
			ContainerID:              effCID,
			BootID:                   effBoot,
			GPUEpoch:                 req.GPUEpoch,
			SupervisorPID:            supPID,
			SupervisorStarttimeTicks: supTicks,
			ResidentProcs:            residentProcs,
			EpochFenceConfirmed:      epochFenceConfirmed,
			Mode:                     reclaimMode,
		})
		if vErr != nil || vRes.ErrorCode == "UNKNOWN_NODE_STATE" {
			_ = s.markLeaseNodeUnknownOrQuarantined(ctx, leaseID, nodeUID, gpuUUIDs, snapState, "reconciling", "UNKNOWN_NODE_STATE", 0, req.RunNonce)
			return LeaseDecision{}, ErrNodeStateUnknown
		}
		if vRes.UnkillableDState || !vRes.Verified {
			leak := int64(0)
			targetSt := "reclaiming"
			reason := "DOWNGRADE_P2_WORKER_STILL_ACTIVE"
			if vRes.UnkillableDState {
				leak = curPeak
				targetSt = "quarantined"
				reason = "UNKILLABLE_D_STATE_QUARANTINED"
			}
			_ = s.markLeaseNodeUnknownOrQuarantined(ctx, leaseID, nodeUID, gpuUUIDs, snapState, targetSt, reason, leak, req.RunNonce)
			return LeaseDecision{}, ErrReclaimQuarantined
		}
		if vRes.MeasuredFreeVRAMMiB > 0 {
			verifiedFreeByGPU[g] = vRes.MeasuredFreeVRAMMiB
		}
	}

	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return LeaseDecision{}, err
	}
	defer rollbackTx(tx)

	cards, err := lockCardsForUpdate(ctx, tx, s.StationID, nodeUID)
	if err != nil {
		return LeaseDecision{}, err
	}

	var curState, curStage, curPod, curCID, curBoot string
	var curVer int64
	err = tx.QueryRow(ctx, `
		SELECT state, stage_id, pod_uid, container_id, boot_id, fencing_token, version, expires_at, response_cache
		  FROM station.gpu_resource_leases
		 WHERE station_id = $1 AND lease_id = $2
		   FOR UPDATE
	`, s.StationID, leaseID).Scan(&curState, &curStage, &curPod, &curCID, &curBoot, &curFence, &curVer, &curExpiresAt, &cacheRaw)
	if err != nil {
		return LeaseDecision{}, err
	}
	if curState == "released" || curState == "expired" || curState == "reclaiming" || curState == "quarantined" || !curExpiresAt.After(s.now()) || curFence != req.FencingToken {
		return LeaseDecision{}, ErrFencingRevoked
	}
	if curVer != snapVer || curState != snapState || curStage != snapStage {
		return LeaseDecision{}, ErrVersionConflict
	}
	if err := verifyWriteOnceIdentity(curPod, curCID, curBoot, req.PodUID, req.ContainerID, req.BootID); err != nil {
		return LeaseDecision{}, err
	}

	now := s.now()
	expiresAt := now.Add(time.Duration(req.TTLSeconds) * time.Second)
	cacheMap = map[string]any{}
	if len(cacheRaw) > 0 {
		_ = json.Unmarshal(cacheRaw, &cacheMap)
	}
	cacheMap["last_hb_key"] = req.IdempotencyKey
	cacheMap["last_hb_sha256"] = reqHash
	newCacheRaw, _ := json.Marshal(cacheMap)

	nextPod := strings.TrimSpace(req.NextPodUID)
	rebindNextPod := nextPod != ""
	targetPod := curPod
	if targetPod == "" {
		targetPod = req.PodUID
	}
	targetCID := curCID
	if targetCID == "" {
		targetCID = req.ContainerID
	}
	targetBoot := curBoot
	if targetBoot == "" {
		targetBoot = req.BootID
	}
	if rebindNextPod {
		targetPod = nextPod
		targetCID = strings.TrimSpace(req.NextContainerID)
		targetBoot = strings.TrimSpace(req.NextBootID)
	}

	cmdTag, err := tx.Exec(ctx, `
		UPDATE station.gpu_resource_leases
		   SET state = 'running',
		       peak_budget_mib = $3,
		       stage_id = $4,
		       pod_uid = $5,
		       container_id = $6,
		       boot_id = $7,
		       response_cache = $8,
		       expires_at = $9,
		       version = version + 1,
		       updated_at = now()
		 WHERE station_id = $1
		   AND lease_id = $2
		   AND version = $10
		   AND state = $11
		   AND stage_id = $12
		   AND fencing_token = $13
	`, s.StationID, leaseID, newPeak, newStage, targetPod, targetCID, targetBoot, newCacheRaw, expiresAt, snapVer, snapState, snapStage, curFence)
	if err != nil {
		return LeaseDecision{}, err
	}
	if cmdTag.RowsAffected() != 1 {
		return LeaseDecision{}, ErrVersionConflict
	}

	corrUUID := uuid.Must(uuid.NewV7()).String()
	var lastSeq int64
	for i := range cards {
		if cards[i].NodeUID == nodeUID && slices.Contains(gpuUUIDs, cards[i].GPUUUID) {
			if freeMiB, ok := verifiedFreeByGPU[cards[i].GPUUUID]; ok {
				usedMiB := cards[i].TotalVRAMMiB - freeMiB
				if usedMiB < cards[i].DriverOverheadMiB {
					usedMiB = cards[i].DriverOverheadMiB
				}
				if _, err := tx.Exec(ctx, `
					UPDATE station.gpu_physical_cards
					   SET measured_free_vram_mib = $4, measured_used_vram_mib = $5, telemetry_sampled_at = $6, updated_at = now()
					 WHERE station_id = $1 AND node_uid = $2 AND gpu_uuid = $3
				`, s.StationID, cards[i].NodeUID, cards[i].GPUUUID, freeMiB, usedMiB, now); err != nil {
					return LeaseDecision{}, err
				}
			}
			seq, _, err := appendCardEvent(ctx, tx, &cards[i], eventRecordInput{
				RunNonce:             req.RunNonce,
				CorrelationEventUUID: corrUUID,
				EntityType:           "lease",
				EntityID:             leaseID,
				FromState:            curState,
				ToState:              "running",
				FencingToken:         curFence,
				BudgetMiB:            newPeak,
				ReasonCode:           "BUDGET_DOWNGRADED",
				Payload: map[string]any{
					"previous_budget_mib": curPeak,
					"new_budget_mib":      newPeak,
					"stage_id":            newStage,
				},
			})
			if err != nil {
				return LeaseDecision{}, err
			}
			lastSeq = seq
		}
	}

	dec, err := s.loadDecisionTx(ctx, tx, leaseID, cards)
	if err != nil {
		return LeaseDecision{}, err
	}
	if lastSeq > 0 {
		dec.LedgerEventSeq = lastSeq
	}
	if err := tx.Commit(ctx); err != nil {
		return LeaseDecision{}, err
	}
	s.PromoteQueuedLeases(ctx, nodeUID)
	return dec, nil
}

// stageHandoff transitions an active lease from one settled stage to the next same-budget stage
// (e.g. P1 generation -> P2 regenerate_2k across two engine Pods on the same physical card)
// only after node-level VerifyTerminated confirms the outgoing stage container has 0 GPU workers.
func (s *Service) stageHandoff(ctx context.Context, caller CallerIdentity, leaseID string, req HeartbeatRequest) (LeaseDecision, error) {
	if caller.Kind != CallerConsumer && caller.Kind != CallerAdmin {
		return LeaseDecision{}, ErrForbiddenCaller
	}
	if _, err := uuid.Parse(leaseID); err != nil {
		return LeaseDecision{}, ErrInvalidRequest
	}
	req.IdempotencyKey = strings.TrimSpace(req.IdempotencyKey)
	newStage := strings.TrimSpace(req.NewStageID)
	if req.IdempotencyKey == "" || len(req.IdempotencyKey) > 256 || req.FencingToken <= 0 || newStage == "" || len(newStage) > 64 {
		return LeaseDecision{}, ErrInvalidRequest
	}
	if req.TTLSeconds <= 0 {
		req.TTLSeconds = DefaultLeaseTTLSeconds
	}
	computedHash := computeHeartbeatHash(leaseID, req)
	if req.RequestSHA256 != "" {
		if !sha256HexPattern.MatchString(req.RequestSHA256) || req.RequestSHA256 != computedHash {
			return LeaseDecision{}, ErrInvalidRequest
		}
	}
	reqHash := computedHash

	var holderApp, snapState, snapStage, nodeUID, podUID, containerID, bootID string
	var gpuUUIDs []string
	var curPeak, curFence, snapVer int64
	var curExpiresAt time.Time
	var cacheRaw []byte
	err := s.Pool.QueryRow(ctx, `
		SELECT holder_app_id, state, stage_id, node_uid, pod_uid, container_id, boot_id,
		       gpu_uuids, peak_budget_mib, fencing_token, version, expires_at, response_cache
		  FROM station.gpu_resource_leases
		 WHERE station_id = $1 AND lease_id = $2
	`, s.StationID, leaseID).Scan(
		&holderApp, &snapState, &snapStage, &nodeUID, &podUID, &containerID, &bootID,
		&gpuUUIDs, &curPeak, &curFence, &snapVer, &curExpiresAt, &cacheRaw,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return LeaseDecision{}, ErrLeaseNotFound
	}
	if err != nil {
		return LeaseDecision{}, err
	}
	if caller.Kind != CallerAdmin && holderApp != caller.AppID {
		return LeaseDecision{}, ErrForbiddenCaller
	}
	if err := verifyWriteOnceIdentity(podUID, containerID, bootID, req.PodUID, req.ContainerID, req.BootID); err != nil {
		return LeaseDecision{}, err
	}

	cacheMap := map[string]any{}
	if len(cacheRaw) > 0 {
		_ = json.Unmarshal(cacheRaw, &cacheMap)
	}
	if prevKey, _ := cacheMap["last_hb_key"].(string); prevKey == req.IdempotencyKey {
		prevSHA, _ := cacheMap["last_hb_sha256"].(string)
		if prevSHA != reqHash {
			return LeaseDecision{}, ErrIdempotencyConflict
		}
		return s.GetLease(ctx, caller, leaseID)
	}
	if snapState == "released" || snapState == "expired" || snapState == "reclaiming" || snapState == "quarantined" {
		return LeaseDecision{}, ErrFencingRevoked
	}
	if snapState != "leased" && snapState != "running" {
		return LeaseDecision{}, ErrLeaseInactive
	}
	if !curExpiresAt.After(s.now()) || req.FencingToken != curFence {
		return LeaseDecision{}, ErrFencingRevoked
	}
	if req.ExpectedVersion > 0 && req.ExpectedVersion != snapVer {
		return LeaseDecision{}, ErrVersionConflict
	}
	if strings.EqualFold(snapStage, newStage) {
		return LeaseDecision{}, ErrInvalidRequest
	}
	if !req.WorkerSettled {
		return LeaseDecision{}, ErrWorkerNotSettled
	}

	effPodUID := podUID
	if effPodUID == "" {
		effPodUID = req.PodUID
	}
	effCID := containerID
	if effCID == "" {
		effCID = req.ContainerID
	}
	effBoot := bootID
	if effBoot == "" {
		effBoot = req.BootID
	}
	reclaimMode := strings.TrimSpace(req.ReclaimMode)
	if reclaimMode == "" {
		if req.GPUEpoch > 0 {
			reclaimMode = "epoch_unload"
		} else {
			reclaimMode = "container_exited"
		}
	}
	if s.Verifier == nil {
		_ = s.markLeaseNodeUnknownOrQuarantined(ctx, leaseID, nodeUID, gpuUUIDs, snapState, "reconciling", "UNKNOWN_NODE_STATE", 0, req.RunNonce)
		return LeaseDecision{}, ErrNodeStateUnknown
	}
	epochFenceConfirmed := true
	supPID := req.SupervisorPID
	supTicks := req.SupervisorStarttimeTicks
	var residentProcs []ResidentProcIdentity
	if reclaimMode == "epoch_unload" {
		sealMeta, proofOK := validateEpochSealProof(
			req.ReclaimProof,
			effPodUID,
			effCID,
			effBoot,
			req.GPUEpoch,
			req.SupervisorPID,
			req.SupervisorStarttimeTicks,
			"",
			0,
		)
		if !proofOK {
			_ = s.markLeaseNodeUnknownOrQuarantined(ctx, leaseID, nodeUID, gpuUUIDs, snapState, "reclaiming", "EPOCH_SEAL_PROOF_MISSING_OR_INVALID", 0, req.RunNonce)
			return LeaseDecision{}, ErrReclaimQuarantined
		}
		supPID = sealMeta.SupervisorPID
		supTicks = sealMeta.SupervisorStarttimeTicks
		epochFenceConfirmed = true
	} else if reclaimMode == "warm_idle_settled" {
		warmMeta, proofOK := validateWarmSettledProof(
			req.ReclaimProof,
			effPodUID,
			effCID,
			effBoot,
			req.SupervisorPID,
			req.SupervisorStarttimeTicks,
			leaseID,
			curFence,
		)
		if !proofOK {
			_ = s.markLeaseNodeUnknownOrQuarantined(ctx, leaseID, nodeUID, gpuUUIDs, snapState, "reclaiming", "WARM_SETTLE_PROOF_MISSING_OR_INVALID", 0, req.RunNonce)
			return LeaseDecision{}, ErrReclaimQuarantined
		}
		supPID = warmMeta.SupervisorPID
		supTicks = warmMeta.SupervisorStarttimeTicks
		residentProcs = warmMeta.ResidentProcs
		epochFenceConfirmed = true
	}
	for _, g := range gpuUUIDs {
		vRes, vErr := s.Verifier.VerifyTerminated(ctx, VerifyTerminatedRequest{
			NodeUID:                  nodeUID,
			GPUUUID:                  g,
			PodUID:                   effPodUID,
			ContainerID:              effCID,
			BootID:                   effBoot,
			GPUEpoch:                 req.GPUEpoch,
			SupervisorPID:            supPID,
			SupervisorStarttimeTicks: supTicks,
			ResidentProcs:            residentProcs,
			EpochFenceConfirmed:      epochFenceConfirmed,
			Mode:                     reclaimMode,
		})
		if vErr != nil || vRes.ErrorCode == "UNKNOWN_NODE_STATE" {
			_ = s.markLeaseNodeUnknownOrQuarantined(ctx, leaseID, nodeUID, gpuUUIDs, snapState, "reconciling", "UNKNOWN_NODE_STATE", 0, req.RunNonce)
			return LeaseDecision{}, ErrNodeStateUnknown
		}
		if vRes.UnkillableDState || !vRes.Verified {
			leak := int64(0)
			targetSt := "reclaiming"
			reason := "STAGE_HANDOFF_WORKER_STILL_ACTIVE"
			if vRes.UnkillableDState {
				leak = curPeak
				targetSt = "quarantined"
				reason = "UNKILLABLE_D_STATE_QUARANTINED"
			}
			_ = s.markLeaseNodeUnknownOrQuarantined(ctx, leaseID, nodeUID, gpuUUIDs, snapState, targetSt, reason, leak, req.RunNonce)
			return LeaseDecision{}, ErrReclaimQuarantined
		}
	}

	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return LeaseDecision{}, err
	}
	defer rollbackTx(tx)

	cards, err := lockCardsForUpdate(ctx, tx, s.StationID, nodeUID)
	if err != nil {
		return LeaseDecision{}, err
	}
	var curState, curStage, curPod, curCID, curBoot string
	var curVer int64
	err = tx.QueryRow(ctx, `
		SELECT state, stage_id, pod_uid, container_id, boot_id, fencing_token, version, expires_at, response_cache
		  FROM station.gpu_resource_leases
		 WHERE station_id = $1 AND lease_id = $2
		   FOR UPDATE
	`, s.StationID, leaseID).Scan(&curState, &curStage, &curPod, &curCID, &curBoot, &curFence, &curVer, &curExpiresAt, &cacheRaw)
	if err != nil {
		return LeaseDecision{}, err
	}
	if curState == "released" || curState == "expired" || curState == "reclaiming" || curState == "quarantined" || !curExpiresAt.After(s.now()) || curFence != req.FencingToken {
		return LeaseDecision{}, ErrFencingRevoked
	}
	if curVer != snapVer || curState != snapState || curStage != snapStage {
		return LeaseDecision{}, ErrVersionConflict
	}
	if err := verifyWriteOnceIdentity(curPod, curCID, curBoot, req.PodUID, req.ContainerID, req.BootID); err != nil {
		return LeaseDecision{}, err
	}

	now := s.now()
	expiresAt := now.Add(time.Duration(req.TTLSeconds) * time.Second)
	cacheMap = map[string]any{}
	if len(cacheRaw) > 0 {
		_ = json.Unmarshal(cacheRaw, &cacheMap)
	}
	cacheMap["last_hb_key"] = req.IdempotencyKey
	cacheMap["last_hb_sha256"] = reqHash
	newCacheRaw, _ := json.Marshal(cacheMap)

	nextPod := strings.TrimSpace(req.NextPodUID)
	targetPod := curPod
	if targetPod == "" {
		targetPod = req.PodUID
	}
	targetCID := curCID
	if targetCID == "" {
		targetCID = req.ContainerID
	}
	targetBoot := curBoot
	if targetBoot == "" {
		targetBoot = req.BootID
	}
	if nextPod != "" {
		targetPod = nextPod
		targetCID = strings.TrimSpace(req.NextContainerID)
		targetBoot = strings.TrimSpace(req.NextBootID)
	}

	cmdTag, err := tx.Exec(ctx, `
		UPDATE station.gpu_resource_leases
		   SET state = 'running',
		       stage_id = $3,
		       pod_uid = $4,
		       container_id = $5,
		       boot_id = $6,
		       response_cache = $7,
		       expires_at = $8,
		       version = version + 1,
		       updated_at = now()
		 WHERE station_id = $1
		   AND lease_id = $2
		   AND version = $9
		   AND state = $10
		   AND stage_id = $11
		   AND fencing_token = $12
	`, s.StationID, leaseID, newStage, targetPod, targetCID, targetBoot, newCacheRaw, expiresAt, snapVer, snapState, snapStage, curFence)
	if err != nil {
		return LeaseDecision{}, err
	}
	if cmdTag.RowsAffected() != 1 {
		return LeaseDecision{}, ErrVersionConflict
	}

	corrUUID := uuid.Must(uuid.NewV7()).String()
	var lastSeq int64
	for i := range cards {
		if cards[i].NodeUID == nodeUID && slices.Contains(gpuUUIDs, cards[i].GPUUUID) {
			seq, _, err := appendCardEvent(ctx, tx, &cards[i], eventRecordInput{
				RunNonce:             req.RunNonce,
				CorrelationEventUUID: corrUUID,
				EntityType:           "lease",
				EntityID:             leaseID,
				FromState:            curState,
				ToState:              "running",
				FencingToken:         curFence,
				BudgetMiB:            curPeak,
				ReasonCode:           "STAGE_HANDOFF",
				Payload: map[string]any{
					"previous_stage_id": snapStage,
					"stage_id":          newStage,
					"budget_mib":        curPeak,
				},
			})
			if err != nil {
				return LeaseDecision{}, err
			}
			lastSeq = seq
		}
	}
	dec, err := s.loadDecisionTx(ctx, tx, leaseID, cards)
	if err != nil {
		return LeaseDecision{}, err
	}
	if lastSeq > 0 {
		dec.LedgerEventSeq = lastSeq
	}
	if err := tx.Commit(ctx); err != nil {
		return LeaseDecision{}, err
	}
	return dec, nil
}

// BindTarget atomically binds the concrete cold-started or warm target Pod/Container/Boot
// identity to an active lease and reconciles Kubelet-assigned physical GPU UUID(s) against
// the station GPU ledger under canonical card row locks (P1b-H4).
func (s *Service) BindTarget(ctx context.Context, caller CallerIdentity, leaseID string, req BindTargetRequest) (LeaseDecision, error) {
	if caller.Kind != CallerConsumer && caller.Kind != CallerAdmin {
		return LeaseDecision{}, ErrForbiddenCaller
	}
	if _, err := uuid.Parse(leaseID); err != nil {
		return LeaseDecision{}, ErrInvalidRequest
	}
	req.IdempotencyKey = strings.TrimSpace(req.IdempotencyKey)
	if req.IdempotencyKey == "" || len(req.IdempotencyKey) > 256 || req.FencingToken <= 0 {
		return LeaseDecision{}, ErrInvalidRequest
	}
	if isUnknownPlaceholder(req.PodUID) || isUnknownPlaceholder(req.ContainerID) || isUnknownPlaceholder(req.BootID) {
		return LeaseDecision{}, ErrInvalidRequest
	}
	podUID := normPodUID(req.PodUID)
	containerID := normContainerID(req.ContainerID)
	bootID := strings.TrimSpace(req.BootID)
	if podUID == "" || containerID == "" || bootID == "" {
		return LeaseDecision{}, ErrInvalidRequest
	}
	seenGPU := map[string]bool{}
	actualGPUs := make([]string, 0, len(req.ActualGPUUUIDs))
	for _, g := range req.ActualGPUUUIDs {
		clean := strings.TrimSpace(g)
		if clean == "" || isUnknownPlaceholder(clean) {
			return LeaseDecision{}, ErrInvalidRequest
		}
		if !seenGPU[clean] {
			seenGPU[clean] = true
			actualGPUs = append(actualGPUs, clean)
		}
	}
	if len(actualGPUs) == 0 {
		return LeaseDecision{}, ErrInvalidRequest
	}
	sort.Strings(actualGPUs)
	if req.TTLSeconds <= 0 {
		req.TTLSeconds = DefaultLeaseTTLSeconds
	}
	computedHash := computeCanonicalHash("bind_target", leaseID, req.FencingToken, podUID, containerID, bootID, strings.Join(actualGPUs, ","))
	if req.RequestSHA256 != "" {
		if !sha256HexPattern.MatchString(req.RequestSHA256) || req.RequestSHA256 != computedHash {
			return LeaseDecision{}, ErrInvalidRequest
		}
	}
	reqHash := computedHash

	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return LeaseDecision{}, err
	}
	defer rollbackTx(tx)

	cards, err := lockCardsForUpdate(ctx, tx, s.StationID, "")
	if err != nil {
		return LeaseDecision{}, err
	}

	var holderApp, state, stageID, nodeUID, curPod, curCID, curBoot, priority string
	var curGPUs []string
	var cardCount int
	var curPeak, curStartupMin, curFence, curVer int64
	var curHeadroom *int64
	var curExpiresAt time.Time
	var cacheRaw []byte
	err = tx.QueryRow(ctx, `
		SELECT holder_app_id, state, stage_id, node_uid, pod_uid, container_id, boot_id,
		       gpu_uuids, card_count, peak_budget_mib, startup_free_min_mib, headroom_override_mib,
		       priority, fencing_token, version, expires_at, response_cache
		  FROM station.gpu_resource_leases
		 WHERE station_id = $1 AND lease_id = $2
		   FOR UPDATE
	`, s.StationID, leaseID).Scan(
		&holderApp, &state, &stageID, &nodeUID, &curPod, &curCID, &curBoot,
		&curGPUs, &cardCount, &curPeak, &curStartupMin, &curHeadroom,
		&priority, &curFence, &curVer, &curExpiresAt, &cacheRaw,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return LeaseDecision{}, ErrLeaseNotFound
	}
	if err != nil {
		return LeaseDecision{}, err
	}
	if caller.Kind != CallerAdmin && holderApp != caller.AppID {
		return LeaseDecision{}, ErrForbiddenCaller
	}
	if req.NodeUID != "" && strings.TrimSpace(req.NodeUID) != nodeUID {
		return LeaseDecision{}, ErrTopologyUnsatisfied
	}
	if cardCount <= 0 {
		cardCount = len(curGPUs)
	}
	if len(actualGPUs) != cardCount {
		return LeaseDecision{}, ErrTopologyUnsatisfied
	}
	if err := verifyWriteOnceIdentity(curPod, curCID, curBoot, podUID, containerID, bootID); err != nil {
		return LeaseDecision{}, err
	}

	cacheMap := map[string]any{}
	if len(cacheRaw) > 0 {
		_ = json.Unmarshal(cacheRaw, &cacheMap)
	}
	if prevKey, _ := cacheMap["last_bind_key"].(string); prevKey == req.IdempotencyKey {
		prevSHA, _ := cacheMap["last_bind_sha256"].(string)
		if prevSHA != reqHash {
			return LeaseDecision{}, ErrIdempotencyConflict
		}
		return s.loadDecisionTx(ctx, tx, leaseID, cards)
	}

	if state == "released" || state == "expired" || state == "reclaiming" || state == "quarantined" {
		return LeaseDecision{}, ErrFencingRevoked
	}
	if state != "leased" && state != "running" {
		return LeaseDecision{}, ErrLeaseInactive
	}
	now := s.now()
	if !curExpiresAt.After(now) || req.FencingToken != curFence {
		return LeaseDecision{}, ErrFencingRevoked
	}
	if req.ExpectedVersion > 0 && req.ExpectedVersion != curVer {
		return LeaseDecision{}, ErrVersionConflict
	}

	sortedCurGPUs := append([]string(nil), curGPUs...)
	sort.Strings(sortedCurGPUs)
	sameGPUs := slices.Equal(sortedCurGPUs, actualGPUs)

	cardByUUID := make(map[string]*cardLockState, len(cards))
	for i := range cards {
		if cards[i].NodeUID == nodeUID {
			cardByUUID[cards[i].GPUUUID] = &cards[i]
		}
	}
	for _, g := range actualGPUs {
		c, ok := cardByUUID[g]
		if !ok || c.State != "ready" {
			return LeaseDecision{}, ErrTopologyUnsatisfied
		}
	}

	newFence := curFence
	if !sameGPUs {
		// Once identity was already bound on curGPUs, GPU assignment is immutable without stage handoff.
		if curPod != "" || curCID != "" {
			return LeaseDecision{}, ErrVersionConflict
		}
		leases, err := loadActiveLeases(ctx, tx, s.StationID, nodeUID)
		if err != nil {
			return LeaseDecision{}, err
		}
		residencies, err := loadResidencies(ctx, tx, s.StationID, nodeUID)
		if err != nil {
			return LeaseDecision{}, err
		}
		leasesByID := make(map[string]*activeLeaseRow, len(leases))
		for i := range leases {
			leasesByID[leases[i].LeaseID] = &leases[i]
		}
		maxAge := s.maxTelemetryAge()
		for _, g := range actualGPUs {
			if slices.Contains(sortedCurGPUs, g) {
				continue
			}
			c := cardByUUID[g]
			acc := computeCardAccounting(*c, leases, residencies, leaseID)
			fit := evaluateCardFit(*c, acc, leasesByID, leaseID, curPeak, curStartupMin, curHeadroom, priority, now, maxAge, nil)
			if !fit.ImmediateFit {
				return LeaseDecision{}, ErrTopologyUnsatisfied
			}
			if c.MinFencingToken+1 > newFence {
				newFence = c.MinFencingToken + 1
			}
		}
		for _, g := range actualGPUs {
			c := cardByUUID[g]
			if newFence > c.MinFencingToken {
				c.MinFencingToken = newFence
				if _, err := tx.Exec(ctx, `
					UPDATE station.gpu_physical_cards
					   SET min_fencing_token = $4, updated_at = now()
					 WHERE station_id = $1 AND node_uid = $2 AND gpu_uuid = $3
				`, s.StationID, nodeUID, g, newFence); err != nil {
					return LeaseDecision{}, err
				}
			}
		}
	}

	expiresAt := now.Add(time.Duration(req.TTLSeconds) * time.Second)
	cacheMap["last_bind_key"] = req.IdempotencyKey
	cacheMap["last_bind_sha256"] = reqHash
	newCacheRaw, _ := json.Marshal(cacheMap)

	if _, err := tx.Exec(ctx, `
		UPDATE station.gpu_resource_leases
		   SET state = 'running',
		       pod_uid = $3,
		       container_id = $4,
		       boot_id = $5,
		       gpu_uuids = $6,
		       fencing_token = $7,
		       response_cache = $8,
		       expires_at = $9,
		       version = version + 1,
		       updated_at = now()
		 WHERE station_id = $1 AND lease_id = $2
	`, s.StationID, leaseID, podUID, containerID, bootID, actualGPUs, newFence, newCacheRaw, expiresAt); err != nil {
		return LeaseDecision{}, err
	}

	corrUUID := uuid.Must(uuid.NewV7()).String()
	var lastSeq int64
	affectedSet := map[string]bool{}
	for _, g := range sortedCurGPUs {
		affectedSet[g] = true
	}
	for _, g := range actualGPUs {
		affectedSet[g] = true
	}
	for i := range cards {
		if cards[i].NodeUID != nodeUID || !affectedSet[cards[i].GPUUUID] {
			continue
		}
		reasonCode := "LEASE_TARGET_BOUND"
		budgetForEvent := curPeak
		if !sameGPUs {
			if slices.Contains(actualGPUs, cards[i].GPUUUID) && !slices.Contains(sortedCurGPUs, cards[i].GPUUUID) {
				reasonCode = "LEASE_TARGET_REBOUND_IN"
			} else if !slices.Contains(actualGPUs, cards[i].GPUUUID) && slices.Contains(sortedCurGPUs, cards[i].GPUUUID) {
				reasonCode = "LEASE_TARGET_REBOUND_OUT"
				budgetForEvent = 0
			}
		}
		seq, _, err := appendCardEvent(ctx, tx, &cards[i], eventRecordInput{
			RunNonce:             req.RunNonce,
			CorrelationEventUUID: corrUUID,
			EntityType:           "lease",
			EntityID:             leaseID,
			FromState:            state,
			ToState:              "running",
			FencingToken:         newFence,
			BudgetMiB:            budgetForEvent,
			ReasonCode:           reasonCode,
			Payload: map[string]any{
				"pod_uid":          podUID,
				"container_id":     containerID,
				"boot_id":          bootID,
				"previous_gpus":    sortedCurGPUs,
				"actual_gpu_uuids": actualGPUs,
				"stage_id":         stageID,
			},
		})
		if err != nil {
			return LeaseDecision{}, err
		}
		lastSeq = seq
	}

	dec, err := s.loadDecisionTx(ctx, tx, leaseID, cards)
	if err != nil {
		return LeaseDecision{}, err
	}
	if lastSeq > 0 {
		dec.LedgerEventSeq = lastSeq
	}
	if err := tx.Commit(ctx); err != nil {
		return LeaseDecision{}, err
	}
	if !sameGPUs {
		s.PromoteQueuedLeases(ctx, nodeUID)
	}
	return dec, nil
}

func (s *Service) Heartbeat(ctx context.Context, caller CallerIdentity, leaseID string, req HeartbeatRequest) (LeaseDecision, error) {
	if req.DowngradeToMiB != nil {
		return s.Downgrade(ctx, caller, leaseID, req)
	}
	if strings.TrimSpace(req.NewStageID) != "" {
		return s.stageHandoff(ctx, caller, leaseID, req)
	}
	if caller.Kind != CallerConsumer && caller.Kind != CallerAdmin {
		return LeaseDecision{}, ErrForbiddenCaller
	}
	if _, err := uuid.Parse(leaseID); err != nil {
		return LeaseDecision{}, ErrInvalidRequest
	}
	req.IdempotencyKey = strings.TrimSpace(req.IdempotencyKey)
	if req.IdempotencyKey == "" || len(req.IdempotencyKey) > 256 || req.FencingToken <= 0 {
		return LeaseDecision{}, ErrInvalidRequest
	}
	if req.TTLSeconds <= 0 {
		req.TTLSeconds = DefaultLeaseTTLSeconds
	}
	computedHash := computeHeartbeatHash(leaseID, req)
	if req.RequestSHA256 != "" {
		if !sha256HexPattern.MatchString(req.RequestSHA256) || req.RequestSHA256 != computedHash {
			return LeaseDecision{}, ErrInvalidRequest
		}
	}
	reqHash := computedHash

	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return LeaseDecision{}, err
	}
	defer rollbackTx(tx)

	cards, err := lockCardsForUpdate(ctx, tx, s.StationID, "")
	if err != nil {
		return LeaseDecision{}, err
	}

	var holderApp, state, stageID, nodeUID, curPod, curCID, curBoot string
	var gpuUUIDs []string
	var curPeak, curFence, curVer int64
	var curExpiresAt time.Time
	var cacheRaw []byte
	err = tx.QueryRow(ctx, `
		SELECT holder_app_id, state, stage_id, node_uid, pod_uid, container_id, boot_id,
		       gpu_uuids, peak_budget_mib, fencing_token, version, expires_at, response_cache
		  FROM station.gpu_resource_leases
		 WHERE station_id = $1 AND lease_id = $2
		   FOR UPDATE
	`, s.StationID, leaseID).Scan(&holderApp, &state, &stageID, &nodeUID, &curPod, &curCID, &curBoot, &gpuUUIDs, &curPeak, &curFence, &curVer, &curExpiresAt, &cacheRaw)
	if errors.Is(err, pgx.ErrNoRows) {
		return LeaseDecision{}, ErrLeaseNotFound
	}
	if err != nil {
		return LeaseDecision{}, err
	}
	if caller.Kind != CallerAdmin && holderApp != caller.AppID {
		return LeaseDecision{}, ErrForbiddenCaller
	}
	if err := verifyWriteOnceIdentity(curPod, curCID, curBoot, req.PodUID, req.ContainerID, req.BootID); err != nil {
		return LeaseDecision{}, err
	}

	cacheMap := map[string]any{}
	if len(cacheRaw) > 0 {
		_ = json.Unmarshal(cacheRaw, &cacheMap)
	}
	if prevKey, _ := cacheMap["last_hb_key"].(string); prevKey == req.IdempotencyKey {
		prevSHA, _ := cacheMap["last_hb_sha256"].(string)
		if prevSHA != reqHash {
			return LeaseDecision{}, ErrIdempotencyConflict
		}
		return s.loadDecisionTx(ctx, tx, leaseID, cards)
	}

	if state == "released" || state == "expired" || state == "reclaiming" || state == "quarantined" {
		return LeaseDecision{}, ErrFencingRevoked
	}
	if state != "leased" && state != "running" {
		return LeaseDecision{}, ErrLeaseInactive
	}
	// P1-H5: Reject expired leases in Heartbeat even before background ReconcileRestart runs!
	now := s.now()
	if !curExpiresAt.After(now) || req.FencingToken != curFence {
		return LeaseDecision{}, ErrFencingRevoked
	}
	if req.ExpectedVersion > 0 && req.ExpectedVersion != curVer {
		return LeaseDecision{}, ErrVersionConflict
	}

	expiresAt := now.Add(time.Duration(req.TTLSeconds) * time.Second)
	cacheMap["last_hb_key"] = req.IdempotencyKey
	cacheMap["last_hb_sha256"] = reqHash
	newCacheRaw, _ := json.Marshal(cacheMap)

	newState := "running"
	if _, err := tx.Exec(ctx, `
		UPDATE station.gpu_resource_leases
		   SET state = $3,
		       pod_uid = CASE WHEN pod_uid = '' AND $4 <> '' THEN $4 ELSE pod_uid END,
		       container_id = CASE WHEN container_id = '' AND $5 <> '' THEN $5 ELSE container_id END,
		       boot_id = CASE WHEN boot_id = '' AND $6 <> '' THEN $6 ELSE boot_id END,
		       response_cache = $7,
		       expires_at = $8,
		       version = version + 1,
		       updated_at = now()
		 WHERE station_id = $1 AND lease_id = $2
	`, s.StationID, leaseID, newState, req.PodUID, req.ContainerID, req.BootID, newCacheRaw, expiresAt); err != nil {
		return LeaseDecision{}, err
	}

	var lastSeq int64
	if state != newState {
		corrUUID := uuid.Must(uuid.NewV7()).String()
		for i := range cards {
			if cards[i].NodeUID == nodeUID && slices.Contains(gpuUUIDs, cards[i].GPUUUID) {
				seq, _, err := appendCardEvent(ctx, tx, &cards[i], eventRecordInput{
					RunNonce:             req.RunNonce,
					CorrelationEventUUID: corrUUID,
					EntityType:           "lease",
					EntityID:             leaseID,
					FromState:            state,
					ToState:              newState,
					FencingToken:         curFence,
					BudgetMiB:            curPeak,
					ReasonCode:           "HEARTBEAT_RUNNING",
					Payload: map[string]any{
						"previous_budget_mib": curPeak,
						"new_budget_mib":      curPeak,
						"stage_id":            stageID,
					},
				})
				if err != nil {
					return LeaseDecision{}, err
				}
				lastSeq = seq
			}
		}
	}

	dec, err := s.loadDecisionTx(ctx, tx, leaseID, cards)
	if err != nil {
		return LeaseDecision{}, err
	}
	if lastSeq > 0 {
		dec.LedgerEventSeq = lastSeq
	}
	if err := tx.Commit(ctx); err != nil {
		return LeaseDecision{}, err
	}
	return dec, nil
}

func (s *Service) Release(ctx context.Context, caller CallerIdentity, leaseID string, req ReleaseRequest) (LeaseDecision, error) {
	if caller.Kind != CallerConsumer && caller.Kind != CallerAdmin {
		return LeaseDecision{}, ErrForbiddenCaller
	}
	if _, err := uuid.Parse(leaseID); err != nil {
		return LeaseDecision{}, ErrInvalidRequest
	}
	req.IdempotencyKey = strings.TrimSpace(req.IdempotencyKey)
	if req.IdempotencyKey == "" || len(req.IdempotencyKey) > 256 {
		return LeaseDecision{}, ErrInvalidRequest
	}
	computedHash := computeCanonicalHash(leaseID, req.FencingToken, req.WorkerSettled, req.PodUID, req.ContainerID, req.BootID, req.GPUEpoch, req.ReclaimMode)
	if req.RequestSHA256 != "" {
		if !sha256HexPattern.MatchString(req.RequestSHA256) || req.RequestSHA256 != computedHash {
			return LeaseDecision{}, ErrInvalidRequest
		}
	}
	reqHash := computedHash

	var holderApp, state, nodeUID, podUID, containerID, bootID string
	var gpuUUIDs []string
	var curFence, curVer, peakBudget int64
	var cacheRaw []byte
	err := s.Pool.QueryRow(ctx, `
		SELECT holder_app_id, state, node_uid, pod_uid, container_id, boot_id, gpu_uuids, fencing_token, version, peak_budget_mib, response_cache
		  FROM station.gpu_resource_leases
		 WHERE station_id = $1 AND lease_id = $2
	`, s.StationID, leaseID).Scan(&holderApp, &state, &nodeUID, &podUID, &containerID, &bootID, &gpuUUIDs, &curFence, &curVer, &peakBudget, &cacheRaw)
	if errors.Is(err, pgx.ErrNoRows) {
		return LeaseDecision{}, ErrLeaseNotFound
	}
	if err != nil {
		return LeaseDecision{}, err
	}
	if caller.Kind != CallerAdmin && holderApp != caller.AppID {
		return LeaseDecision{}, ErrForbiddenCaller
	}
	if err := verifyWriteOnceIdentity(podUID, containerID, bootID, req.PodUID, req.ContainerID, req.BootID); err != nil {
		return LeaseDecision{}, err
	}

	cacheMap := map[string]any{}
	if len(cacheRaw) > 0 {
		_ = json.Unmarshal(cacheRaw, &cacheMap)
	}
	if prevKey, _ := cacheMap["last_rel_key"].(string); prevKey == req.IdempotencyKey {
		prevSHA, _ := cacheMap["last_rel_sha256"].(string)
		if prevSHA != reqHash {
			return LeaseDecision{}, ErrIdempotencyConflict
		}
		return s.GetLease(ctx, caller, leaseID)
	}
	if state == "released" || state == "expired" {
		return s.GetLease(ctx, caller, leaseID)
	}

	wasGranted := len(gpuUUIDs) > 0 && state != "queued" && state != "draining"
	isUnstartedAbort := strings.TrimSpace(req.ReclaimMode) == "abort_unstarted" &&
		!req.WorkerSettled &&
		podUID == "" && containerID == "" && bootID == "" &&
		strings.TrimSpace(req.PodUID) == "" && strings.TrimSpace(req.ContainerID) == "" && strings.TrimSpace(req.BootID) == ""
	if wasGranted && !req.WorkerSettled && !isUnstartedAbort {
		return LeaseDecision{}, ErrWorkerNotSettled
	}

	effPodUID := podUID
	if effPodUID == "" {
		effPodUID = req.PodUID
	}
	effContainerID := containerID
	if effContainerID == "" {
		effContainerID = req.ContainerID
	}
	effBootID := bootID
	if effBootID == "" {
		effBootID = req.BootID
	}
	reclaimMode := strings.TrimSpace(req.ReclaimMode)
	if reclaimMode == "" {
		if req.GPUEpoch > 0 {
			reclaimMode = "epoch_unload"
		} else {
			reclaimMode = "container_exited"
		}
	}
	if reclaimMode != "container_exited" && reclaimMode != "epoch_unload" && reclaimMode != "warm_idle_settled" && reclaimMode != "abort_unstarted" {
		return LeaseDecision{}, ErrInvalidRequest
	}
	if reclaimMode == "abort_unstarted" && wasGranted && !isUnstartedAbort {
		return LeaseDecision{}, ErrWorkerNotSettled
	}

	proof := map[string]any{}
	for k, v := range req.ReclaimProof {
		proof[k] = v
	}
	var verifiedFreeByGPU map[string]int64
	if wasGranted && !isUnstartedAbort {
		if s.Verifier == nil {
			_ = s.markLeaseNodeUnknownOrQuarantined(ctx, leaseID, nodeUID, gpuUUIDs, state, "reconciling", "UNKNOWN_NODE_STATE", 0, req.RunNonce)
			return LeaseDecision{}, ErrNodeStateUnknown
		}
		epochFenceConfirmed := req.WorkerSettled
		supPID := req.SupervisorPID
		supTicks := req.SupervisorStarttimeTicks
		var residentProcs []ResidentProcIdentity
		if reclaimMode == "epoch_unload" {
			sealMeta, proofOK := validateEpochSealProof(
				req.ReclaimProof,
				effPodUID,
				effContainerID,
				effBootID,
				req.GPUEpoch,
				req.SupervisorPID,
				req.SupervisorStarttimeTicks,
				leaseID,
				curFence,
			)
			if !proofOK {
				_ = s.markLeaseNodeUnknownOrQuarantined(ctx, leaseID, nodeUID, gpuUUIDs, state, "reclaiming", "EPOCH_SEAL_PROOF_MISSING_OR_INVALID", 0, req.RunNonce)
				return LeaseDecision{}, ErrReclaimQuarantined
			}
			supPID = sealMeta.SupervisorPID
			supTicks = sealMeta.SupervisorStarttimeTicks
			epochFenceConfirmed = true
		} else if reclaimMode == "warm_idle_settled" {
			warmMeta, proofOK := validateWarmSettledProof(
				req.ReclaimProof,
				effPodUID,
				effContainerID,
				effBootID,
				req.SupervisorPID,
				req.SupervisorStarttimeTicks,
				leaseID,
				curFence,
			)
			if !proofOK {
				_ = s.markLeaseNodeUnknownOrQuarantined(ctx, leaseID, nodeUID, gpuUUIDs, state, "reclaiming", "WARM_SETTLE_PROOF_MISSING_OR_INVALID", 0, req.RunNonce)
				return LeaseDecision{}, ErrReclaimQuarantined
			}
			supPID = warmMeta.SupervisorPID
			supTicks = warmMeta.SupervisorStarttimeTicks
			residentProcs = warmMeta.ResidentProcs
			epochFenceConfirmed = true
		}
		verifiedFreeByGPU = make(map[string]int64, len(gpuUUIDs))
		for _, g := range gpuUUIDs {
			vRes, vErr := s.Verifier.VerifyTerminated(ctx, VerifyTerminatedRequest{
				NodeUID:                  nodeUID,
				GPUUUID:                  g,
				PodUID:                   effPodUID,
				ContainerID:              effContainerID,
				BootID:                   effBootID,
				GPUEpoch:                 req.GPUEpoch,
				SupervisorPID:            supPID,
				SupervisorStarttimeTicks: supTicks,
				ResidentProcs:            residentProcs,
				EpochFenceConfirmed:      epochFenceConfirmed,
				Mode:                     reclaimMode,
			})
			if vErr != nil || vRes.ErrorCode == "UNKNOWN_NODE_STATE" {
				_ = s.markLeaseNodeUnknownOrQuarantined(ctx, leaseID, nodeUID, gpuUUIDs, state, "reconciling", "UNKNOWN_NODE_STATE", 0, req.RunNonce)
				return LeaseDecision{}, ErrNodeStateUnknown
			}
			if vRes.UnkillableDState || !vRes.Verified {
				leak := int64(0)
				if vRes.UnkillableDState {
					leak = peakBudget
				}
				targetSt := "reclaiming"
				reason := "WORKER_STILL_HOLDING_VRAM"
				if vRes.UnkillableDState {
					targetSt = "quarantined"
					reason = "UNKILLABLE_D_STATE_QUARANTINED"
				}
				_ = s.markLeaseNodeUnknownOrQuarantined(ctx, leaseID, nodeUID, gpuUUIDs, state, targetSt, reason, leak, req.RunNonce)
				return LeaseDecision{}, ErrReclaimQuarantined
			}
			if vRes.MeasuredFreeVRAMMiB > 0 {
				verifiedFreeByGPU[g] = vRes.MeasuredFreeVRAMMiB
			}
			proof["verified_"+g] = vRes
		}
	}

	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return LeaseDecision{}, err
	}
	defer rollbackTx(tx)

	cards, err := lockCardsForUpdate(ctx, tx, s.StationID, nodeUID)
	if err != nil {
		return LeaseDecision{}, err
	}

	var curPod, curCID, curBoot string
	err = tx.QueryRow(ctx, `
		SELECT state, pod_uid, container_id, boot_id, fencing_token, version, response_cache
		  FROM station.gpu_resource_leases
		 WHERE station_id = $1 AND lease_id = $2
		   FOR UPDATE
	`, s.StationID, leaseID).Scan(&state, &curPod, &curCID, &curBoot, &curFence, &curVer, &cacheRaw)
	if err != nil {
		return LeaseDecision{}, err
	}
	if err := verifyWriteOnceIdentity(curPod, curCID, curBoot, req.PodUID, req.ContainerID, req.BootID); err != nil {
		return LeaseDecision{}, err
	}
	if isUnstartedAbort && (curPod != "" || curCID != "" || curBoot != "") {
		return LeaseDecision{}, ErrWorkerNotSettled
	}
	if req.ExpectedVersion > 0 && req.ExpectedVersion != curVer {
		return LeaseDecision{}, ErrVersionConflict
	}
	if req.FencingToken > 0 && req.FencingToken != curFence && state != "queued" && state != "draining" {
		return LeaseDecision{}, ErrFencingRevoked
	}

	if err := clearLeaseReservationsTx(ctx, tx, s.StationID, leaseID, cards, req.RunNonce, "LEASE_RELEASED"); err != nil {
		return LeaseDecision{}, err
	}

	cacheMap = map[string]any{}
	if len(cacheRaw) > 0 {
		_ = json.Unmarshal(cacheRaw, &cacheMap)
	}
	cacheMap["last_rel_key"] = req.IdempotencyKey
	cacheMap["last_rel_sha256"] = reqHash
	newCacheRaw, _ := json.Marshal(cacheMap)
	proofRaw, _ := json.Marshal(proof)

	if _, err := tx.Exec(ctx, `
		UPDATE station.gpu_resource_leases
		   SET state = 'released',
		       blocking_reason = 'none',
		       blocking_holder_id = '',
		       pod_uid = CASE WHEN pod_uid = '' AND $5 <> '' THEN $5 ELSE pod_uid END,
		       container_id = CASE WHEN container_id = '' AND $6 <> '' THEN $6 ELSE container_id END,
		       boot_id = CASE WHEN boot_id = '' AND $7 <> '' THEN $7 ELSE boot_id END,
		       reclaim_proof = $3,
		       response_cache = $4,
		       version = version + 1,
		       updated_at = now()
		 WHERE station_id = $1 AND lease_id = $2
	`, s.StationID, leaseID, proofRaw, newCacheRaw, effPodUID, effContainerID, effBootID); err != nil {
		return LeaseDecision{}, err
	}

	corrUUID := uuid.Must(uuid.NewV7()).String()
	var lastSeq int64
	eventCards := []*cardLockState{}
	for i := range cards {
		if len(gpuUUIDs) > 0 {
			if slices.Contains(gpuUUIDs, cards[i].GPUUUID) {
				eventCards = append(eventCards, &cards[i])
			}
		} else if len(eventCards) == 0 {
			eventCards = append(eventCards, &cards[i])
		}
	}
	now := s.now()
	for _, c := range eventCards {
		cleanedAttr := filterOutHolderProcesses(c.AttributedProcesses, effPodUID, effContainerID)
		attrRaw, _ := json.Marshal(cleanedAttr)
		if freeMiB, ok := verifiedFreeByGPU[c.GPUUUID]; ok {
			usedMiB := c.TotalVRAMMiB - freeMiB
			if usedMiB < c.DriverOverheadMiB {
				usedMiB = c.DriverOverheadMiB
			}
			if _, err := tx.Exec(ctx, `
				UPDATE station.gpu_physical_cards
				   SET measured_free_vram_mib = $4,
				       measured_used_vram_mib = $5,
				       attributed_processes = $6,
				       telemetry_sampled_at = $7,
				       updated_at = now()
				 WHERE station_id = $1 AND node_uid = $2 AND gpu_uuid = $3
			`, s.StationID, c.NodeUID, c.GPUUUID, freeMiB, usedMiB, attrRaw, now); err != nil {
				return LeaseDecision{}, err
			}
		} else if wasGranted {
			if _, err := tx.Exec(ctx, `
				UPDATE station.gpu_physical_cards
				   SET attributed_processes = $4, updated_at = now()
				 WHERE station_id = $1 AND node_uid = $2 AND gpu_uuid = $3
			`, s.StationID, c.NodeUID, c.GPUUUID, attrRaw); err != nil {
				return LeaseDecision{}, err
			}
		}
		seq, _, err := appendCardEvent(ctx, tx, c, eventRecordInput{
			RunNonce:             req.RunNonce,
			CorrelationEventUUID: corrUUID,
			EntityType:           "lease",
			EntityID:             leaseID,
			FromState:            state,
			ToState:              "released",
			FencingToken:         curFence,
			BudgetMiB:            peakBudget,
			ReasonCode:           "RELEASED_VERIFIED",
		})
		if err != nil {
			return LeaseDecision{}, err
		}
		lastSeq = seq
	}

	dec, err := s.loadDecisionTx(ctx, tx, leaseID, cards)
	if err != nil {
		return LeaseDecision{}, err
	}
	dec.LedgerEventSeq = lastSeq
	if err := tx.Commit(ctx); err != nil {
		return LeaseDecision{}, err
	}
	s.PromoteQueuedLeases(ctx, nodeUID)
	return dec, nil
}

func (s *Service) markLeaseNodeUnknownOrQuarantined(
	ctx context.Context,
	leaseID, nodeUID string,
	gpuUUIDs []string,
	fromState, toState, reason string,
	quarantineMiB int64,
	runNonce string,
) error {
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer rollbackTx(tx)

	cards, err := lockCardsForUpdate(ctx, tx, s.StationID, nodeUID)
	if err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `
		UPDATE station.gpu_resource_leases
		   SET state = $3, blocking_reason = $4, version = version + 1, updated_at = now()
		 WHERE station_id = $1 AND lease_id = $2
	`, s.StationID, leaseID, toState, strings.ToLower(reason)); err != nil {
		return err
	}
	corrUUID := uuid.Must(uuid.NewV7()).String()
	for i := range cards {
		if slices.Contains(gpuUUIDs, cards[i].GPUUUID) {
			if toState == "reconciling" {
				cards[i].State = "reconciling"
				cards[i].StateReason = reason
				_, _ = tx.Exec(ctx, `
					UPDATE station.gpu_physical_cards
					   SET state = 'reconciling', state_reason = $4, updated_at = now()
					 WHERE station_id = $1 AND node_uid = $2 AND gpu_uuid = $3
				`, s.StationID, cards[i].NodeUID, cards[i].GPUUUID, reason)
			} else if quarantineMiB > 0 {
				cards[i].QuarantinedMiB += quarantineMiB
				_, _ = tx.Exec(ctx, `
					UPDATE station.gpu_physical_cards
					   SET quarantined_mib = $4, updated_at = now()
					 WHERE station_id = $1 AND node_uid = $2 AND gpu_uuid = $3
				`, s.StationID, cards[i].NodeUID, cards[i].GPUUUID, cards[i].QuarantinedMiB)
			}
			_, _, _ = appendCardEvent(ctx, tx, &cards[i], eventRecordInput{
				RunNonce:             runNonce,
				CorrelationEventUUID: corrUUID,
				EntityType:           "lease",
				EntityID:             leaseID,
				FromState:            fromState,
				ToState:              toState,
				BudgetMiB:            quarantineMiB,
				ReasonCode:           reason,
			})
		}
	}
	return tx.Commit(ctx)
}

func validResidencyState(st string) bool {
	switch st {
	case "active_compute", "human_protected", "human_dirty", "idle_evictable",
		"zero_vram_standby", "waking", "sealing", "draining_hard", "reconciling", "quarantined":
		return true
	default:
		return false
	}
}

func (s *Service) ReportResidency(ctx context.Context, caller CallerIdentity, req ResidencyReportRequest) error {
	if caller.Kind != CallerConsumer && caller.Kind != CallerAdmin {
		return ErrForbiddenCaller
	}
	req.InstanceID = strings.TrimSpace(req.InstanceID)
	req.NodeUID = strings.TrimSpace(req.NodeUID)
	req.PodUID = strings.TrimSpace(req.PodUID)
	req.BootID = strings.TrimSpace(req.BootID)
	req.GPUUUID = strings.TrimSpace(req.GPUUUID)
	if req.NodeUID == "" && req.GPUUUID != "" && s.Pool != nil {
		_ = s.Pool.QueryRow(ctx, `
			SELECT node_uid
			  FROM station.gpu_physical_cards
			 WHERE station_id = $1 AND gpu_uuid = $2
			 ORDER BY updated_at DESC
			 LIMIT 1
		`, s.StationID, req.GPUUUID).Scan(&req.NodeUID)
		req.NodeUID = strings.TrimSpace(req.NodeUID)
	}
	if req.InstanceID == "" || req.NodeUID == "" || req.PodUID == "" || req.BootID == "" || req.GPUUUID == "" {
		return ErrInvalidRequest
	}
	if req.ResidencySeq <= 0 || req.GPUEpoch < 0 || !validResidencyState(req.ResidencyState) {
		return ErrInvalidRequest
	}
	if req.ContainerName == "" {
		req.ContainerName = "worker"
	}
	if req.BlockingReason == "" {
		req.BlockingReason = "none"
	}
	upgraded := true
	if req.TemplateUpgraded != nil {
		upgraded = *req.TemplateUpgraded
	}
	appID := caller.AppID

	// P1-H4: Verify instance ownership & canonical endpoint via InstanceDirectory when configured,
	// and validate callback_endpoint strictly against internal K8s service/pod patterns (blocking SSRF).
	if s.InstanceDir != nil {
		regInst, found, lErr := s.InstanceDir.LookupInstance(ctx, s.StationID, req.InstanceID)
		if lErr != nil {
			return lErr
		}
		if !found {
			return ErrForbiddenCaller
		}
		if caller.Kind != CallerAdmin && regInst.AppID != "" && !sameCanonicalAppID(regInst.AppID, appID) {
			return ErrForbiddenCaller
		}
		if regInst.AppID != "" {
			appID = regInst.AppID
		}
		if normPodUID(regInst.PodUID) == "" || normPodUID(regInst.PodUID) != normPodUID(req.PodUID) {
			return ErrForbiddenCaller
		}
		if strings.TrimSpace(regInst.CallbackEndpoint) == "" {
			return ErrForbiddenCaller
		}
		req.CallbackEndpoint = regInst.CallbackEndpoint
	}
	cleanCallback, cbErr := validateInternalCallbackEndpoint(req.CallbackEndpoint, s.AllowLoopbackCallbacks)
	if cbErr != nil {
		return cbErr
	}
	req.CallbackEndpoint = cleanCallback

	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer rollbackTx(tx)

	cards, err := lockCardsForUpdate(ctx, tx, s.StationID, req.NodeUID)
	if err != nil {
		return err
	}

	var prevAppID, prevBoot, prevState string
	var prevEpoch, prevSeq int64
	err = tx.QueryRow(ctx, `
		SELECT app_id, boot_id, gpu_epoch, residency_seq, residency_state
		  FROM station.gpu_instance_residency
		 WHERE station_id = $1 AND instance_id = $2
		   FOR UPDATE
	`, s.StationID, req.InstanceID).Scan(&prevAppID, &prevBoot, &prevEpoch, &prevSeq, &prevState)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return err
	}
	if err == nil {
		// P1-H4: Forbid cross-app takeover of an existing instance_id!
		if caller.Kind != CallerAdmin && prevAppID != "" && !sameCanonicalAppID(prevAppID, appID) {
			return ErrForbiddenCaller
		}
		if prevAppID != "" {
			appID = prevAppID
		}
		if prevBoot == req.BootID {
			if req.GPUEpoch < prevEpoch || (req.GPUEpoch == prevEpoch && req.ResidencySeq <= prevSeq) {
				return ErrVersionConflict
			}
		}
	}

	cmdTag, err := tx.Exec(ctx, `
		INSERT INTO station.gpu_instance_residency(
			station_id, instance_id, app_id, node_uid, pod_uid, container_name, container_id,
			boot_id, gpu_epoch, residency_seq, gpu_uuid, residency_state,
			human_protected, gui_connection_alive, dirty, snapshot_in_progress, snapshot_required,
			blocking_reason, pinned_budget_mib, vram_allocated_mib, idle_seconds,
			callback_endpoint, template_upgraded, last_active_at, updated_at
		) VALUES (
			$1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,now(),now()
		)
		ON CONFLICT (station_id, instance_id) DO UPDATE SET
			node_uid = EXCLUDED.node_uid,
			pod_uid = EXCLUDED.pod_uid,
			container_name = EXCLUDED.container_name,
			container_id = CASE WHEN EXCLUDED.container_id <> '' THEN EXCLUDED.container_id ELSE station.gpu_instance_residency.container_id END,
			boot_id = EXCLUDED.boot_id,
			gpu_epoch = EXCLUDED.gpu_epoch,
			residency_seq = EXCLUDED.residency_seq,
			gpu_uuid = EXCLUDED.gpu_uuid,
			residency_state = EXCLUDED.residency_state,
			human_protected = EXCLUDED.human_protected,
			gui_connection_alive = EXCLUDED.gui_connection_alive,
			dirty = EXCLUDED.dirty,
			snapshot_in_progress = EXCLUDED.snapshot_in_progress,
			snapshot_required = EXCLUDED.snapshot_required,
			blocking_reason = EXCLUDED.blocking_reason,
			pinned_budget_mib = EXCLUDED.pinned_budget_mib,
			vram_allocated_mib = EXCLUDED.vram_allocated_mib,
			idle_seconds = EXCLUDED.idle_seconds,
			callback_endpoint = CASE WHEN EXCLUDED.callback_endpoint <> '' THEN EXCLUDED.callback_endpoint ELSE station.gpu_instance_residency.callback_endpoint END,
			template_upgraded = EXCLUDED.template_upgraded,
			last_active_at = CASE WHEN EXCLUDED.idle_seconds = 0 THEN now() ELSE station.gpu_instance_residency.last_active_at END,
			updated_at = now()
		WHERE station.gpu_instance_residency.app_id = EXCLUDED.app_id
	`,
		s.StationID, req.InstanceID, appID, req.NodeUID, req.PodUID, req.ContainerName, req.ContainerID,
		req.BootID, req.GPUEpoch, req.ResidencySeq, req.GPUUUID, req.ResidencyState,
		req.HumanProtected, req.GUIConnectionAlive, req.Dirty, req.SnapshotInProgress, req.SnapshotRequired,
		req.BlockingReason, req.PinnedBudgetMiB, req.VRAMAllocatedMiB, req.IdleSeconds,
		req.CallbackEndpoint, upgraded,
	)
	if err != nil {
		return err
	}
	if cmdTag.RowsAffected() != 1 {
		return ErrForbiddenCaller
	}

	if prevState != req.ResidencyState || prevEpoch != req.GPUEpoch {
		corrUUID := uuid.Must(uuid.NewV7()).String()
		var alloc int64
		if req.VRAMAllocatedMiB != nil {
			alloc = *req.VRAMAllocatedMiB
		}
		for i := range cards {
			if cards[i].GPUUUID == req.GPUUUID {
				if _, _, err := appendCardEvent(ctx, tx, &cards[i], eventRecordInput{
					RunNonce:             req.RunNonce,
					CorrelationEventUUID: corrUUID,
					EntityType:           "residency",
					EntityID:             req.InstanceID,
					FromState:            prevState,
					ToState:              req.ResidencyState,
					ResidencySeq:         req.ResidencySeq,
					GPUEpoch:             req.GPUEpoch,
					BudgetMiB:            alloc,
					ReasonCode:           strings.ToUpper(req.BlockingReason),
					Payload: map[string]any{
						"human_protected":      req.HumanProtected,
						"gui_connection_alive": req.GUIConnectionAlive,
						"dirty":                req.Dirty,
					},
				}); err != nil {
					return err
				}
			}
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return err
	}
	if req.ResidencyState == "idle_evictable" || req.ResidencyState == "zero_vram_standby" {
		s.PromoteQueuedLeases(ctx, req.NodeUID)
	}
	return nil
}

func (s *Service) ReportNodeTelemetry(ctx context.Context, caller CallerIdentity, rep NodeTelemetryReport) error {
	if caller.Kind != CallerNodeAgent && caller.Kind != CallerAdmin {
		return ErrForbiddenCaller
	}
	rep.NodeUID = strings.TrimSpace(rep.NodeUID)
	rep.AgentBootID = strings.TrimSpace(rep.AgentBootID)
	if rep.NodeUID == "" || rep.SampleID <= 0 {
		return ErrInvalidRequest
	}
	if len(rep.Cards) == 0 && rep.AgentHealthy {
		return ErrInvalidRequest
	}

	now := s.now()
	sampledAt := now
	if strings.TrimSpace(rep.SampledAt) != "" {
		parsed, pErr := time.Parse(time.RFC3339Nano, strings.TrimSpace(rep.SampledAt))
		if pErr != nil {
			parsed, pErr = time.Parse(time.RFC3339, strings.TrimSpace(rep.SampledAt))
		}
		if pErr != nil {
			return ErrInvalidRequest
		}
		if parsed.After(now.Add(5 * time.Second)) {
			return ErrInvalidRequest
		}
		sampledAt = parsed.UTC()
	}

	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer rollbackTx(tx)

	existingCards, err := lockCardsForUpdate(ctx, tx, s.StationID, rep.NodeUID)
	if err != nil {
		return err
	}

	corrUUID := uuid.Must(uuid.NewV7()).String()

	// P1-H7: Zero-card unhealthy report (e.g. NVML failure before first discovery on new agent boot)
	// immediately freezes all non-quarantined cards on rep.NodeUID to state='reconciling'!
	if len(rep.Cards) == 0 && !rep.AgentHealthy {
		reason := strings.TrimSpace(rep.ErrorCode)
		if reason == "" {
			reason = "UNKNOWN_NODE_STATE"
		}
		for i := range existingCards {
			cur := &existingCards[i]
			if cur.TelemetryBootID == rep.AgentBootID && rep.SampleID <= cur.TelemetrySampleID {
				continue
			}
			prevState := cur.State
			nextState := "reconciling"
			nextReason := reason
			if cur.State == "quarantined" {
				nextState = "quarantined"
				nextReason = cur.StateReason
			}
			if _, err := tx.Exec(ctx, `
				UPDATE station.gpu_physical_cards
				   SET telemetry_boot_id = $4,
				       telemetry_sample_id = $5,
				       state = $6,
				       state_reason = $7,
				       updated_at = now()
				 WHERE station_id = $1 AND node_uid = $2 AND gpu_uuid = $3
			`, s.StationID, cur.NodeUID, cur.GPUUUID, rep.AgentBootID, rep.SampleID, nextState, nextReason); err != nil {
				return err
			}
			cur.State = nextState
			cur.StateReason = nextReason
			if prevState != nextState {
				if _, _, err := appendCardEvent(ctx, tx, cur, eventRecordInput{
					CorrelationEventUUID: corrUUID,
					EntityType:           "card",
					EntityID:             cur.GPUUUID,
					FromState:            prevState,
					ToState:              nextState,
					ReasonCode:           "CARD_STATE_TRANSITION",
				}); err != nil {
					return err
				}
			}
		}
		return tx.Commit(ctx)
	}

	sortedSamples := append([]CardTelemetrySample(nil), rep.Cards...)
	sort.Slice(sortedSamples, func(i, j int) bool { return sortedSamples[i].GPUUUID < sortedSamples[j].GPUUUID })

	existingByUUID := make(map[string]*cardLockState, len(existingCards))
	for i := range existingCards {
		existingByUUID[existingCards[i].GPUUUID] = &existingCards[i]
	}

	var reconcilingLeases int
	_ = tx.QueryRow(ctx, `
		SELECT count(*)::int FROM station.gpu_resource_leases
		 WHERE station_id = $1 AND node_uid = $2 AND state = 'reconciling'
	`, s.StationID, rep.NodeUID).Scan(&reconcilingLeases)

	for _, sample := range sortedSamples {
		sample.GPUUUID = strings.TrimSpace(sample.GPUUUID)
		if sample.GPUUUID == "" || sample.TotalVRAMMiB <= 0 {
			return ErrInvalidRequest
		}
		if sample.ModelName == "" {
			sample.ModelName = "NVIDIA GeForce RTX 5090"
		}
		if sample.DriverOverheadMiB <= 0 {
			sample.DriverOverheadMiB = DefaultDriverOverheadMiB
		}
		if sample.TimeSlicingSlots <= 0 {
			sample.TimeSlicingSlots = DefaultPerCardTimeSlices
		}
		if sample.AttributedProcesses == nil {
			sample.AttributedProcesses = []AttributedGPUProcess{}
		}
		if sample.ActiveGPUPods == nil {
			sample.ActiveGPUPods = []ActiveGPUPodInfo{}
		}
		attrRaw, err := json.Marshal(sample.AttributedProcesses)
		if err != nil {
			return err
		}
		podsRaw, err := json.Marshal(sample.ActiveGPUPods)
		if err != nil {
			return err
		}

		nextState := "ready"
		nextReason := ""
		healthySample := rep.AgentHealthy && sample.NVMLHealthy
		if !healthySample {
			nextState = "reconciling"
			nextReason = sample.ErrorReason
			if nextReason == "" {
				nextReason = rep.ErrorCode
			}
			if nextReason == "" {
				nextReason = "TELEMETRY_UNHEALTHY"
			}
		} else if reconcilingLeases > 0 {
			nextState = "reconciling"
			nextReason = "PENDING_LEASE_RECONCILIATION"
		}

		cur, exists := existingByUUID[sample.GPUUUID]
		if exists {
			// P1-H7: Monotonic sample check scoped by AgentBootID; duplicate/older sample_id on the same boot epoch is ignored without refreshing telemetry_sampled_at!
			if cur.TelemetryBootID == rep.AgentBootID && rep.SampleID <= cur.TelemetrySampleID {
				continue
			}
			if cur.TelemetryBootID != rep.AgentBootID && cur.TelemetrySampledAt != nil && sampledAt.Before(*cur.TelemetrySampledAt) {
				continue
			}
			if cur.State == "quarantined" {
				nextState = "quarantined"
				nextReason = cur.StateReason
			}
		}

		var dbSampledAt *time.Time
		if healthySample {
			tCopy := sampledAt
			dbSampledAt = &tCopy
		}

		if !exists {
			if _, err := tx.Exec(ctx, `
				INSERT INTO station.gpu_physical_cards(
					station_id, node_uid, node_name, gpu_uuid, gpu_index, model_name,
					total_vram_mib, driver_overhead_mib, measured_free_vram_mib, measured_used_vram_mib,
					telemetry_boot_id, telemetry_sample_id, telemetry_sampled_at, attributed_processes,
					time_slicing_slots, allocated_k8s_slots, scaling_up_k8s_slots, unscheduled_pending_k8s_slots,
					active_gpu_pods, admission_mode, state, state_reason
				) VALUES (
					$1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,'drain_only',$20,$21
				)
			`,
				s.StationID, rep.NodeUID, rep.NodeName, sample.GPUUUID, sample.GPUIndex, sample.ModelName,
				sample.TotalVRAMMiB, sample.DriverOverheadMiB, sample.MeasuredFreeVRAMMiB, sample.MeasuredUsedVRAMMiB,
				rep.AgentBootID, rep.SampleID, dbSampledAt, attrRaw,
				sample.TimeSlicingSlots, sample.AllocatedK8sSlots, sample.ScalingUpK8sSlots, sample.UnscheduledPendingK8sSlots,
				podsRaw, nextState, nextReason,
			); err != nil {
				return err
			}
			newCard := cardLockState{
				StationID:     s.StationID,
				NodeUID:       rep.NodeUID,
				GPUUUID:       sample.GPUUUID,
				LastEventSeq:  0,
				LastEventHash: "GENESIS",
			}
			if _, _, err := appendCardEvent(ctx, tx, &newCard, eventRecordInput{
				CorrelationEventUUID: corrUUID,
				EntityType:           "card",
				EntityID:             sample.GPUUUID,
				FromState:            "",
				ToState:              nextState,
				BudgetMiB:            sample.MeasuredFreeVRAMMiB,
				ReasonCode:           "CARD_DISCOVERED",
			}); err != nil {
				return err
			}
			continue
		}

		prevState := cur.State
		cur.State = nextState
		cur.StateReason = nextReason
		if _, err := tx.Exec(ctx, `
			UPDATE station.gpu_physical_cards
			   SET node_name = CASE WHEN $4 <> '' THEN $4 ELSE node_name END,
			       gpu_index = $5,
			       model_name = $6,
			       total_vram_mib = $7,
			       driver_overhead_mib = $8,
			       measured_free_vram_mib = CASE WHEN $20 THEN $9 ELSE measured_free_vram_mib END,
			       measured_used_vram_mib = CASE WHEN $20 THEN $10 ELSE measured_used_vram_mib END,
			       telemetry_boot_id = $11,
			       telemetry_sample_id = $12,
			       telemetry_sampled_at = CASE WHEN $20 THEN $13 ELSE telemetry_sampled_at END,
			       attributed_processes = CASE WHEN $20 THEN $14 ELSE attributed_processes END,
			       time_slicing_slots = $15,
			       allocated_k8s_slots = $16,
			       scaling_up_k8s_slots = $17,
			       unscheduled_pending_k8s_slots = $18,
			       active_gpu_pods = $19,
			       state = $21,
			       state_reason = $22,
			       updated_at = now()
			 WHERE station_id = $1 AND node_uid = $2 AND gpu_uuid = $3
		`,
			s.StationID, rep.NodeUID, sample.GPUUUID, rep.NodeName, sample.GPUIndex, sample.ModelName,
			sample.TotalVRAMMiB, sample.DriverOverheadMiB, sample.MeasuredFreeVRAMMiB, sample.MeasuredUsedVRAMMiB,
			rep.AgentBootID, rep.SampleID, dbSampledAt, attrRaw,
			sample.TimeSlicingSlots, sample.AllocatedK8sSlots, sample.ScalingUpK8sSlots, sample.UnscheduledPendingK8sSlots,
			podsRaw, healthySample, nextState, nextReason,
		); err != nil {
			return err
		}
		if prevState != nextState {
			if _, _, err := appendCardEvent(ctx, tx, cur, eventRecordInput{
				CorrelationEventUUID: corrUUID,
				EntityType:           "card",
				EntityID:             sample.GPUUUID,
				FromState:            prevState,
				ToState:              nextState,
				BudgetMiB:            sample.MeasuredFreeVRAMMiB,
				ReasonCode:           "CARD_STATE_TRANSITION",
			}); err != nil {
				return err
			}
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return err
	}
	if rep.AgentHealthy {
		s.PromoteQueuedLeases(ctx, rep.NodeUID)
	}
	return nil
}

func (s *Service) SetAdmissionMode(ctx context.Context, caller CallerIdentity, req AdmissionModeRequest) error {
	if caller.Kind != CallerAdmin {
		return ErrForbiddenCaller
	}
	if req.AdmissionMode != AdmissionDrainOnly && req.AdmissionMode != AdmissionActive {
		return ErrInvalidRequest
	}
	targetGPU := strings.TrimSpace(req.GPUUUID)
	reason := strings.TrimSpace(req.Reason)

	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer rollbackTx(tx)

	cards, err := lockCardsForUpdate(ctx, tx, s.StationID, req.NodeUID)
	if err != nil {
		return err
	}
	if len(cards) == 0 {
		return ErrTopologyUnsatisfied
	}
	if targetGPU != "" {
		matched := false
		for _, c := range cards {
			if c.GPUUUID == targetGPU {
				matched = true
				break
			}
		}
		if !matched {
			return ErrTopologyUnsatisfied
		}
	}
	leases, err := loadActiveLeases(ctx, tx, s.StationID, req.NodeUID)
	if err != nil {
		return err
	}
	residencies, err := loadResidencies(ctx, tx, s.StationID, req.NodeUID)
	if err != nil {
		return err
	}

	if req.AdmissionMode == AdmissionActive {
		for _, c := range cards {
			if targetGPU != "" && c.GPUUUID != targetGPU {
				continue
			}
			if c.State != "ready" {
				return fmt.Errorf("%w: card %s state=%s", ErrCutoverBlocked, c.GPUUUID, c.State)
			}
		}
		for _, l := range leases {
			if targetGPU != "" && !slices.Contains(l.GPUUUIDs, targetGPU) {
				continue
			}
			if l.State == "legacy_acquired" || l.State == "reconciling" {
				return fmt.Errorf("%w: lease %s state=%s", ErrCutoverBlocked, l.LeaseID, l.State)
			}
		}
		for _, r := range residencies {
			if targetGPU != "" && r.GPUUUID != targetGPU {
				continue
			}
			if r.blocksUnupgradedCutover() {
				return fmt.Errorf("%w: residency %s un-upgraded protected/dirty", ErrCutoverBlocked, r.InstanceID)
			}
		}
	}

	corrUUID := uuid.Must(uuid.NewV7()).String()
	for i := range cards {
		if targetGPU != "" && cards[i].GPUUUID != targetGPU {
			continue
		}
		prevMode := cards[i].AdmissionMode
		cards[i].AdmissionMode = req.AdmissionMode
		if _, err := tx.Exec(ctx, `
			UPDATE station.gpu_physical_cards
			   SET admission_mode = $4, updated_at = now()
			 WHERE station_id = $1 AND node_uid = $2 AND gpu_uuid = $3
		`, s.StationID, cards[i].NodeUID, cards[i].GPUUUID, req.AdmissionMode); err != nil {
			return err
		}
		if prevMode != req.AdmissionMode {
			var payload map[string]any
			if reason != "" {
				payload = map[string]any{"reason": reason}
			}
			if _, _, err := appendCardEvent(ctx, tx, &cards[i], eventRecordInput{
				RunNonce:             req.RunNonce,
				CorrelationEventUUID: corrUUID,
				EntityType:           "admin",
				EntityID:             cards[i].GPUUUID,
				FromState:            prevMode,
				ToState:              req.AdmissionMode,
				ReasonCode:           "ADMISSION_MODE_UPDATED",
				Payload:              payload,
			}); err != nil {
				return err
			}
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return err
	}
	if req.CutoverCompleted != nil {
		s.CutoverCompleted = *req.CutoverCompleted
	}
	if req.AdmissionMode == AdmissionActive {
		s.PromoteQueuedLeases(ctx, req.NodeUID)
	}
	return nil
}

func (s *Service) PruneStoppedResidency(ctx context.Context, caller CallerIdentity, instanceID string, req ResidencyPruneStoppedRequest) (ResidencyActionResult, error) {
	if caller.Kind != CallerAdmin {
		return ResidencyActionResult{}, ErrForbiddenCaller
	}
	instanceID = strings.TrimSpace(instanceID)
	if instanceID == "" || len(instanceID) > 128 {
		return ResidencyActionResult{}, ErrInvalidRequest
	}
	var r residencyRow
	err := s.Pool.QueryRow(ctx, `
		SELECT instance_id, app_id, node_uid, pod_uid, container_name, container_id,
		       boot_id, gpu_epoch, residency_seq, gpu_uuid, residency_state,
		       human_protected, gui_connection_alive, dirty, snapshot_in_progress, snapshot_required,
		       blocking_reason, pinned_budget_mib, vram_allocated_mib, callback_endpoint, template_upgraded
		  FROM station.gpu_instance_residency
		 WHERE station_id = $1 AND instance_id = $2
	`, s.StationID, instanceID).Scan(
		&r.InstanceID, &r.AppID, &r.NodeUID, &r.PodUID, &r.ContainerName, &r.ContainerID,
		&r.BootID, &r.GPUEpoch, &r.ResidencySeq, &r.GPUUUID, &r.ResidencyState,
		&r.HumanProtected, &r.GUIConnectionAlive, &r.Dirty, &r.SnapshotInProgress, &r.SnapshotRequired,
		&r.BlockingReason, &r.PinnedBudgetMiB, &r.VRAMAllocatedMiB, &r.CallbackEndpoint, &r.TemplateUpgraded,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return ResidencyActionResult{}, ErrResidencyNotFound
	}
	if err != nil {
		return ResidencyActionResult{}, err
	}
	if s.InstanceDir != nil {
		_, found, dirErr := s.InstanceDir.LookupInstance(ctx, s.StationID, instanceID)
		if dirErr != nil {
			return ResidencyActionResult{}, dirErr
		}
		if found {
			return ResidencyActionResult{}, ErrInstanceNotEvictable
		}
	}
	if s.Verifier != nil && strings.TrimSpace(r.PodUID) != "" {
		vRes, vErr := s.Verifier.VerifyTerminated(ctx, VerifyTerminatedRequest{
			NodeUID:     r.NodeUID,
			GPUUUID:     r.GPUUUID,
			PodUID:      r.PodUID,
			ContainerID: r.ContainerID,
			BootID:      r.BootID,
			Mode:        "container_exited",
		})
		if vErr != nil || vRes.ErrorCode == "UNKNOWN_NODE_STATE" {
			return ResidencyActionResult{}, ErrNodeStateUnknown
		}
		if !vRes.Verified {
			return ResidencyActionResult{}, ErrInstanceActiveCompute
		}
	}

	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return ResidencyActionResult{}, err
	}
	defer rollbackTx(tx)

	cards, err := lockCardsForUpdate(ctx, tx, s.StationID, r.NodeUID)
	if err != nil {
		return ResidencyActionResult{}, err
	}
	tag, err := tx.Exec(ctx, `
		DELETE FROM station.gpu_instance_residency
		 WHERE station_id = $1 AND instance_id = $2 AND residency_seq = $3
	`, s.StationID, instanceID, r.ResidencySeq)
	if err != nil {
		return ResidencyActionResult{}, err
	}
	if tag.RowsAffected() == 0 {
		return ResidencyActionResult{}, ErrVersionConflict
	}

	var eventSeq int64
	corrUUID := uuid.Must(uuid.NewV7()).String()
	for i := range cards {
		if cards[i].GPUUUID != r.GPUUUID {
			continue
		}
		payload := map[string]any{"pod_uid": r.PodUID, "boot_id": r.BootID}
		if trimmedReason := strings.TrimSpace(req.Reason); trimmedReason != "" {
			payload["reason"] = trimmedReason
		}
		seq, _, evErr := appendCardEvent(ctx, tx, &cards[i], eventRecordInput{
			RunNonce:             req.RunNonce,
			CorrelationEventUUID: corrUUID,
			EntityType:           "residency",
			EntityID:             r.InstanceID,
			FromState:            r.ResidencyState,
			ToState:              "stopped_pruned",
			ResidencySeq:         r.ResidencySeq,
			GPUEpoch:             r.GPUEpoch,
			ReasonCode:           "RESIDENCY_STOPPED_PRUNED",
			Payload:              payload,
		})
		if evErr != nil {
			return ResidencyActionResult{}, evErr
		}
		eventSeq = seq
		break
	}
	if err := tx.Commit(ctx); err != nil {
		return ResidencyActionResult{}, err
	}
	s.PromoteQueuedLeases(ctx, r.NodeUID)
	return ResidencyActionResult{
		InstanceID:     r.InstanceID,
		AppID:          r.AppID,
		FromState:      r.ResidencyState,
		ResidencyState: "stopped_pruned",
		BootID:         r.BootID,
		GPUEpoch:       r.GPUEpoch,
		ResidencySeq:   r.ResidencySeq,
		LedgerEventSeq: eventSeq,
	}, nil
}

// ReconcileRestart executes the P-E three-way reconciliation across PostgreSQL ledger,
// live Kubernetes Pod UIDs, and node-side NVML/cgroup truth, enforcing strict CAS (P1-H6).
func (s *Service) ReconcileRestart(ctx context.Context, obs RestartObservation) error {
	now := s.now()

	// Step 1: Snapshot active leases & residencies outside card lock so any network RPCs (RevokeLease / VerifyTerminated) never hold PG row locks.
	txRead, err := s.Pool.Begin(ctx)
	if err != nil {
		return err
	}
	leases, err := loadActiveLeases(ctx, txRead, s.StationID, obs.NodeUID)
	var residenciesSnap []residencyRow
	if err == nil {
		residenciesSnap, err = loadResidencies(ctx, txRead, s.StationID, obs.NodeUID)
	}
	var cardsSnap []cardLockState
	if err == nil {
		cardsSnap, err = loadCards(ctx, txRead, s.StationID, obs.NodeUID, false)
	}
	rollbackTx(txRead)
	if err != nil {
		return err
	}

	residencyByPod := make(map[string]residencyRow, len(residenciesSnap))
	residencyByInst := make(map[string]residencyRow, len(residenciesSnap))
	for _, r := range residenciesSnap {
		residencyByInst[r.InstanceID] = r
		if p := normPodUID(r.PodUID); p != "" {
			residencyByPod[p] = r
		}
	}

	var terminatedResidencies []residencyRow
	for _, r := range residenciesSnap {
		if r.ResidencyState == "zero_vram_standby" || r.ResidencyState == "quarantined" || r.PodUID == "" || s.Verifier == nil {
			continue
		}
		absent, live := residencyPodAbsent(cardsSnap, r, now, s.maxTelemetryAge())
		if live {
			continue
		}
		if !(absent || (obs.LivePodUIDs != nil && !obs.LivePodUIDs[r.PodUID])) {
			continue
		}
		result, verifyErr := s.Verifier.VerifyTerminated(ctx, VerifyTerminatedRequest{NodeUID: r.NodeUID, GPUUUID: r.GPUUUID, PodUID: r.PodUID, ContainerID: r.ContainerID, BootID: r.BootID, Mode: "container_exited"})
		if verifyErr == nil && result.Verified && !result.UnkillableDState {
			terminatedResidencies = append(terminatedResidencies, r)
		}
	}

	type leaseOutcome struct {
		lease            activeLeaseRow
		targetState      string
		reason           string
		bumpFence        bool
		revokeAcked      bool
		quarantineLeak   int64
		cardUnknown      bool
		verifiedFreeByID map[string]int64
	}
	var outcomes []leaseOutcome

	for _, l := range leases {
		expired := !l.ExpiresAt.After(now)
		podGone := obs.LivePodUIDs != nil && l.PodUID != "" && !obs.LivePodUIDs[l.PodUID]

		if l.State == "queued" || l.State == "draining" {
			if expired || podGone {
				outcomes = append(outcomes, leaseOutcome{
					lease:       l,
					targetState: "expired",
					reason:      "QUEUE_TTL_OR_POD_EXPIRED",
				})
			}
			continue
		}

		if !expired && !podGone && l.State != "reconciling" && l.State != "reclaiming" {
			continue
		}

		// Resolve authoritative resident instance_id & callback URL when periodic reconcile runs with empty RestartObservation{}.
		resolvedInstID := ""
		if rawReq, ok := l.ResponseCache["acquire_req"].(map[string]any); ok {
			if inst, okS := rawReq["instance_id"].(string); okS && strings.TrimSpace(inst) != "" {
				resolvedInstID = strings.TrimSpace(inst)
			}
		}
		var matchedResidency residencyRow
		hasMatchedResidency := false
		if resolvedInstID != "" {
			if r, ok := residencyByInst[resolvedInstID]; ok && sameCanonicalAppID(r.AppID, l.HolderAppID) {
				matchedResidency = r
				hasMatchedResidency = true
			}
		}
		if !hasMatchedResidency && l.PodUID != "" {
			if r, ok := residencyByPod[normPodUID(l.PodUID)]; ok && sameCanonicalAppID(r.AppID, l.HolderAppID) {
				matchedResidency = r
				hasMatchedResidency = true
				if resolvedInstID == "" {
					resolvedInstID = r.InstanceID
				}
			}
		}

		callbackURL := ""
		if obs.CallbackByAppTask != nil {
			if resolvedInstID != "" && obs.CallbackByAppTask[resolvedInstID] != "" {
				callbackURL = obs.CallbackByAppTask[resolvedInstID]
			} else if obs.CallbackByAppTask[l.HolderAppID] != "" {
				callbackURL = obs.CallbackByAppTask[l.HolderAppID]
			}
		}
		if callbackURL == "" && s.InstanceDir != nil && resolvedInstID != "" {
			if reg, found, dirErr := s.InstanceDir.LookupInstance(ctx, s.StationID, resolvedInstID); dirErr == nil && found {
				if sameCanonicalAppID(reg.AppID, l.HolderAppID) &&
					(l.PodUID == "" || reg.PodUID == "" || normPodUID(reg.PodUID) == normPodUID(l.PodUID)) {
					callbackURL = reg.CallbackEndpoint
				}
			}
		}
		if callbackURL == "" && hasMatchedResidency && matchedResidency.CallbackEndpoint != "" {
			callbackURL = matchedResidency.CallbackEndpoint
		}

		revokeInstID := l.HolderTaskID
		if resolvedInstID != "" {
			revokeInstID = resolvedInstID
		}

		revokeAcked := false
		var verifiedSeal verifiedEpochSeal
		hasVerifiedSeal := false
		if s.Evictor != nil && callbackURL != "" && !podGone {
			revRes, revErr := s.Evictor.RevokeLease(ctx, RevokeLeaseCommand{
				CallbackEndpoint: callbackURL,
				LeaseID:          l.LeaseID,
				InstanceID:       revokeInstID,
				FencingToken:     l.FencingToken,
				BootID:           l.BootID,
				PodUID:           l.PodUID,
				ContainerID:      l.ContainerID,
				AttemptID:        l.AttemptID,
				Reason:           "RECONCILE_EXPIRED_OR_ORPHAN",
			})
			if revErr == nil && revRes.Revoked && revRes.Settled {
				revokeAcked = true
				candEpoch := revRes.SealedGPUEpoch
				if candEpoch <= 0 {
					candEpoch = revRes.GPUEpoch
				}
				if candEpoch <= 0 && len(revRes.ReclaimProof) > 0 {
					if ep, okEp := parseProofInt64(revRes.ReclaimProof["sealed_gpu_epoch"]); okEp && ep > 0 {
						candEpoch = ep
					}
				}
				if revRes.Reclaimed && candEpoch > 0 && len(revRes.ReclaimProof) > 0 {
					if sealMeta, okSeal := validateEpochSealProof(
						revRes.ReclaimProof,
						l.PodUID,
						l.ContainerID,
						l.BootID,
						candEpoch,
						revRes.SupervisorPID,
						revRes.SupervisorStarttimeTicks,
						l.LeaseID,
						l.FencingToken,
					); okSeal {
						verifiedSeal = sealMeta
						hasVerifiedSeal = true
					}
				}
			}
		}

		if s.Verifier == nil {
			outcomes = append(outcomes, leaseOutcome{
				lease:       l,
				targetState: "reconciling",
				reason:      "UNKNOWN_NODE_STATE",
				bumpFence:   true,
				revokeAcked: revokeAcked,
				cardUnknown: true,
			})
			continue
		}

		allVerified := true
		anyUnknown := false
		anyDState := false
		verifiedFree := make(map[string]int64, len(l.GPUUUIDs))
		for _, g := range l.GPUUUIDs {
			var vRes VerifyTerminatedResult
			var vErr error
			if hasVerifiedSeal {
				vRes, vErr = s.Verifier.VerifyTerminated(ctx, VerifyTerminatedRequest{
					NodeUID:                  l.NodeUID,
					GPUUUID:                  g,
					PodUID:                   l.PodUID,
					ContainerID:              l.ContainerID,
					BootID:                   l.BootID,
					GPUEpoch:                 verifiedSeal.GPUEpoch,
					SupervisorPID:            verifiedSeal.SupervisorPID,
					SupervisorStarttimeTicks: verifiedSeal.SupervisorStarttimeTicks,
					EpochFenceConfirmed:      true,
					Mode:                     "epoch_unload",
				})
				// If the outer S3 container itself exited after sealing, allow container_exited verification to confirm full teardown.
				if vErr == nil && !vRes.Verified && vRes.ContainerExited {
					vRes, vErr = s.Verifier.VerifyTerminated(ctx, VerifyTerminatedRequest{
						NodeUID:     l.NodeUID,
						GPUUUID:     g,
						PodUID:      l.PodUID,
						ContainerID: l.ContainerID,
						BootID:      l.BootID,
						Mode:        "container_exited",
					})
				}
			} else {
				vRes, vErr = s.Verifier.VerifyTerminated(ctx, VerifyTerminatedRequest{
					NodeUID:     l.NodeUID,
					GPUUUID:     g,
					PodUID:      l.PodUID,
					ContainerID: l.ContainerID,
					BootID:      l.BootID,
					Mode:        "container_exited",
				})
			}
			if vErr != nil || vRes.ErrorCode == "UNKNOWN_NODE_STATE" {
				anyUnknown = true
				allVerified = false
				break
			}
			if vRes.UnkillableDState {
				anyDState = true
				allVerified = false
			} else if !vRes.Verified {
				allVerified = false
			} else if vRes.MeasuredFreeVRAMMiB > 0 {
				verifiedFree[g] = vRes.MeasuredFreeVRAMMiB
			}
		}

		switch {
		case anyUnknown:
			outcomes = append(outcomes, leaseOutcome{
				lease:       l,
				targetState: "reconciling",
				reason:      "UNKNOWN_NODE_STATE",
				bumpFence:   true,
				revokeAcked: revokeAcked,
				cardUnknown: true,
			})
		case anyDState:
			outcomes = append(outcomes, leaseOutcome{
				lease:          l,
				targetState:    "quarantined",
				reason:         "UNKILLABLE_D_STATE_QUARANTINED",
				bumpFence:      true,
				revokeAcked:    revokeAcked,
				quarantineLeak: l.PeakBudgetMiB,
			})
		case allVerified:
			outcomes = append(outcomes, leaseOutcome{
				lease:            l,
				targetState:      "expired",
				reason:           "RECONCILED_TERMINATED",
				bumpFence:        true,
				revokeAcked:      revokeAcked,
				verifiedFreeByID: verifiedFree,
			})
		default:
			outcomes = append(outcomes, leaseOutcome{
				lease:       l,
				targetState: "reclaiming",
				reason:      "RECONCILE_STILL_ACTIVE_ON_GPU",
				bumpFence:   true,
				revokeAcked: revokeAcked,
			})
		}
	}

	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer rollbackTx(tx)

	cards, err := lockCardsForUpdate(ctx, tx, s.StationID, obs.NodeUID)
	if err != nil {
		return err
	}
	corrUUID := uuid.Must(uuid.NewV7()).String()

	for _, out := range outcomes {
		// P1-H6: Lock lease row FOR UPDATE and verify exact CAS against the Step 1 snapshot
		// so a concurrent Heartbeat/Downgrade/Release/Acquire promotion is NEVER clobbered!
		var curState, curPod, curCID, curBoot string
		var curVer, curFence int64
		var curExpires time.Time
		lErr := tx.QueryRow(ctx, `
			SELECT state, version, fencing_token, pod_uid, container_id, boot_id, expires_at
			  FROM station.gpu_resource_leases
			 WHERE station_id = $1 AND lease_id = $2
			   FOR UPDATE
		`, s.StationID, out.lease.LeaseID).Scan(&curState, &curVer, &curFence, &curPod, &curCID, &curBoot, &curExpires)
		if lErr != nil {
			if errors.Is(lErr, pgx.ErrNoRows) {
				continue
			}
			return lErr
		}
		if curVer != out.lease.Version ||
			curState != out.lease.State ||
			curFence != out.lease.FencingToken ||
			curPod != out.lease.PodUID ||
			curCID != out.lease.ContainerID ||
			curBoot != out.lease.BootID {
			continue
		}
		// Double-check TTL if outcome relied on expiry and Pod is not confirmed gone.
		podConfirmedGone := obs.LivePodUIDs != nil && curPod != "" && !obs.LivePodUIDs[curPod]
		if curExpires.After(s.now()) && !podConfirmedGone && curState != "reconciling" && curState != "reclaiming" {
			continue
		}

		if err := clearLeaseReservationsTx(ctx, tx, s.StationID, out.lease.LeaseID, cards, obs.RunNonce, out.reason); err != nil {
			return err
		}
		cmdTag, err := tx.Exec(ctx, `
			UPDATE station.gpu_resource_leases
			   SET state = $3,
			       blocking_reason = $4,
			       revoke_acknowledged = CASE WHEN $5 THEN true ELSE revoke_acknowledged END,
			       version = version + 1,
			       updated_at = now()
			 WHERE station_id = $1
			   AND lease_id = $2
			   AND version = $6
			   AND state = $7
			   AND fencing_token = $8
		`, s.StationID, out.lease.LeaseID, out.targetState, strings.ToLower(out.reason), out.revokeAcked, out.lease.Version, out.lease.State, out.lease.FencingToken)
		if err != nil {
			return err
		}
		if cmdTag.RowsAffected() != 1 {
			continue
		}

		for i := range cards {
			if cards[i].NodeUID == out.lease.NodeUID && (slices.Contains(out.lease.GPUUUIDs, cards[i].GPUUUID) || len(out.lease.GPUUUIDs) == 0) {
				if out.bumpFence {
					cards[i].FencingEpoch++
					cards[i].MinFencingToken++
				}
				if out.cardUnknown {
					cards[i].State = "reconciling"
					cards[i].StateReason = "UNKNOWN_NODE_STATE"
				}
				if out.quarantineLeak > 0 && out.lease.State != "quarantined" {
					cards[i].QuarantinedMiB += out.quarantineLeak
				}
				cleanedAttr := cards[i].AttributedProcesses
				if out.targetState == "expired" {
					cleanedAttr = filterOutHolderProcesses(cards[i].AttributedProcesses, out.lease.PodUID, out.lease.ContainerID)
				}
				attrRaw, _ := json.Marshal(cleanedAttr)
				if out.targetState == "expired" && out.verifiedFreeByID != nil {
					if freeMiB, okFree := out.verifiedFreeByID[cards[i].GPUUUID]; okFree && freeMiB > 0 {
						cards[i].MeasuredFreeVRAMMiB = freeMiB
						usedMiB := cards[i].TotalVRAMMiB - freeMiB
						if usedMiB < cards[i].DriverOverheadMiB {
							usedMiB = cards[i].DriverOverheadMiB
						}
						cards[i].MeasuredUsedVRAMMiB = usedMiB
					}
				}
				if _, err := tx.Exec(ctx, `
					UPDATE station.gpu_physical_cards
					   SET fencing_epoch = $4,
					       min_fencing_token = $5,
					       quarantined_mib = $6,
					       state = $7,
					       state_reason = $8,
					       measured_free_vram_mib = $9,
					       measured_used_vram_mib = $10,
					       attributed_processes = $11,
					       updated_at = now()
					 WHERE station_id = $1 AND node_uid = $2 AND gpu_uuid = $3
				`,
					s.StationID, cards[i].NodeUID, cards[i].GPUUUID,
					cards[i].FencingEpoch, cards[i].MinFencingToken, cards[i].QuarantinedMiB,
					cards[i].State, cards[i].StateReason,
					cards[i].MeasuredFreeVRAMMiB, cards[i].MeasuredUsedVRAMMiB, attrRaw,
				); err != nil {
					return err
				}
				if _, _, err := appendCardEvent(ctx, tx, &cards[i], eventRecordInput{
					RunNonce:             obs.RunNonce,
					CorrelationEventUUID: corrUUID,
					EntityType:           "reconcile",
					EntityID:             out.lease.LeaseID,
					FromState:            out.lease.State,
					ToState:              out.targetState,
					FencingToken:         cards[i].MinFencingToken,
					BudgetMiB:            out.lease.PeakBudgetMiB,
					ReasonCode:           out.reason,
				}); err != nil {
					return err
				}
				if len(out.lease.GPUUUIDs) == 0 {
					break
				}
			}
		}
	}

	for _, r := range terminatedResidencies {
		cardPresent := false
		for _, c := range cards {
			if c.NodeUID == r.NodeUID && c.GPUUUID == r.GPUUUID {
				cardPresent = true
			}
		}
		if !cardPresent {
			continue
		}
		_, live := residencyPodAbsent(cards, r, s.now(), s.maxTelemetryAge())
		if live {
			continue
		}
		var boot, pod, cid, state string
		var epoch, seq int64
		err := tx.QueryRow(ctx, `SELECT boot_id, pod_uid, container_id, gpu_epoch, residency_seq, residency_state FROM station.gpu_instance_residency WHERE station_id=$1 AND instance_id=$2 AND node_uid=$3 AND gpu_uuid=$4 FOR UPDATE`, s.StationID, r.InstanceID, r.NodeUID, r.GPUUUID).Scan(&boot, &pod, &cid, &epoch, &seq, &state)
		if errors.Is(err, pgx.ErrNoRows) {
			continue
		}
		if err != nil {
			return err
		}
		if boot != r.BootID || pod != r.PodUID || cid != r.ContainerID || epoch != r.GPUEpoch || seq != r.ResidencySeq || state != r.ResidencyState {
			continue
		}
		var active bool
		if err = tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM station.gpu_resource_leases WHERE station_id=$1 AND node_uid=$2 AND (replace(lower(pod_uid),'_','-')=replace(lower($3),'_','-') OR boot_id=$4) AND state IN ('leased','running','releasing','reclaiming','reconciling'))`, s.StationID, r.NodeUID, r.PodUID, r.BootID).Scan(&active); err != nil {
			return err
		}
		if active {
			continue
		}
		tag, err := tx.Exec(ctx, `UPDATE station.gpu_instance_residency SET residency_state='zero_vram_standby', human_protected=FALSE, gui_connection_alive=FALSE, dirty=FALSE, snapshot_in_progress=FALSE, snapshot_required=FALSE, pinned_budget_mib=0, vram_allocated_mib=0, blocking_reason='reconciled_terminated', residency_seq=residency_seq+1, updated_at=now() WHERE station_id=$1 AND instance_id=$2 AND boot_id=$3 AND gpu_epoch=$4 AND residency_seq=$5 AND residency_state=$6`, s.StationID, r.InstanceID, r.BootID, r.GPUEpoch, r.ResidencySeq, r.ResidencyState)
		if err != nil {
			return err
		}
		if tag.RowsAffected() != 1 {
			continue
		}
		for i := range cards {
			if cards[i].NodeUID != r.NodeUID || cards[i].GPUUUID != r.GPUUUID {
				continue
			}
			cards[i].AttributedProcesses = filterOutHolderProcesses(cards[i].AttributedProcesses, r.PodUID, r.ContainerID)
			raw, _ := json.Marshal(cards[i].AttributedProcesses)
			if _, err = tx.Exec(ctx, `UPDATE station.gpu_physical_cards SET attributed_processes=$4, updated_at=now() WHERE station_id=$1 AND node_uid=$2 AND gpu_uuid=$3`, s.StationID, r.NodeUID, r.GPUUUID, raw); err != nil {
				return err
			}
			if _, _, err = appendCardEvent(ctx, tx, &cards[i], eventRecordInput{RunNonce: obs.RunNonce, CorrelationEventUUID: corrUUID, EntityType: "residency", EntityID: r.InstanceID, FromState: r.ResidencyState, ToState: "zero_vram_standby", ResidencySeq: r.ResidencySeq + 1, GPUEpoch: r.GPUEpoch, ReasonCode: "RECONCILED_TERMINATED"}); err != nil {
				return err
			}
		}
	}

	// Clear any orphan card reservations whose lease is no longer queued/draining.
	remainingLeases, err := loadActiveLeases(ctx, tx, s.StationID, obs.NodeUID)
	if err != nil {
		return err
	}
	validReservable := map[string]bool{}
	hasReconcilingLeaseByNode := map[string]bool{}
	for _, rl := range remainingLeases {
		if rl.State == "queued" || rl.State == "draining" {
			validReservable[rl.LeaseID] = true
		}
		if rl.State == "reconciling" {
			hasReconcilingLeaseByNode[rl.NodeUID] = true
		}
	}
	for i := range cards {
		if cards[i].ReservedByLeaseID != nil && *cards[i].ReservedByLeaseID != "" && !validReservable[*cards[i].ReservedByLeaseID] {
			orphanID := *cards[i].ReservedByLeaseID
			cards[i].ReservedByLeaseID = nil
			if _, err := tx.Exec(ctx, `
				UPDATE station.gpu_physical_cards
				   SET reserved_by_lease_id = NULL, updated_at = now()
				 WHERE station_id = $1 AND node_uid = $2 AND gpu_uuid = $3
			`, s.StationID, cards[i].NodeUID, cards[i].GPUUUID); err != nil {
				return err
			}
			if _, _, err := appendCardEvent(ctx, tx, &cards[i], eventRecordInput{
				RunNonce:             obs.RunNonce,
				CorrelationEventUUID: corrUUID,
				EntityType:           "reconcile",
				EntityID:             orphanID,
				FromState:            "reserved",
				ToState:              "orphan_reservation_cleared",
				ReasonCode:           "ORPHAN_RESERVATION_CLEARED",
			}); err != nil {
				return err
			}
		}
		if cards[i].State == "reconciling" && cards[i].StateReason == "UNKNOWN_NODE_STATE" && !hasReconcilingLeaseByNode[cards[i].NodeUID] {
			cards[i].State = "ready"
			cards[i].StateReason = ""
			if _, err := tx.Exec(ctx, `
				UPDATE station.gpu_physical_cards
				   SET state = 'ready', state_reason = '', updated_at = now()
				 WHERE station_id = $1 AND node_uid = $2 AND gpu_uuid = $3
			`, s.StationID, cards[i].NodeUID, cards[i].GPUUUID); err != nil {
				return err
			}
			if _, _, err := appendCardEvent(ctx, tx, &cards[i], eventRecordInput{
				RunNonce:             obs.RunNonce,
				CorrelationEventUUID: corrUUID,
				EntityType:           "reconcile",
				EntityID:             cards[i].GPUUUID,
				FromState:            "reconciling",
				ToState:              "ready",
				ReasonCode:           "RECONCILE_NODE_RECOVERED",
			}); err != nil {
				return err
			}
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return err
	}
	s.PromoteQueuedLeases(ctx, obs.NodeUID)
	return nil
}

func (s *Service) Snapshot(ctx context.Context) (LedgerSnapshot, error) {
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return LedgerSnapshot{}, err
	}
	defer rollbackTx(tx)

	cards, err := lockCardsForUpdate(ctx, tx, s.StationID, "")
	if err != nil {
		return LedgerSnapshot{}, err
	}
	leases, err := loadActiveLeases(ctx, tx, s.StationID, "")
	if err != nil {
		return LedgerSnapshot{}, err
	}
	residencies, err := loadResidencies(ctx, tx, s.StationID, "")
	if err != nil {
		return LedgerSnapshot{}, err
	}

	modelByKey := map[string]string{}
	gpuIndexByKey := map[string]int{}
	mRows, err := tx.Query(ctx, `SELECT node_uid, gpu_uuid, model_name FROM station.gpu_physical_cards WHERE station_id = $1`, s.StationID)
	if err == nil {
		for mRows.Next() {
			var n, g, m string
			if mRows.Scan(&n, &g, &m) == nil {
				modelByKey[n+"|"+g] = m
			}
		}
		mRows.Close()
	}

	idleByInst := map[string]int64{}
	iRows, err := tx.Query(ctx, `SELECT instance_id, idle_seconds FROM station.gpu_instance_residency WHERE station_id = $1`, s.StationID)
	if err == nil {
		for iRows.Next() {
			var id string
			var sec int64
			if iRows.Scan(&id, &sec) == nil {
				idleByInst[id] = sec
			}
		}
		iRows.Close()
	}

	now := s.now()
	maxAge := s.maxTelemetryAge()
	cardsByKey := map[string]cardLockState{}
	cardSnaps := make([]CardSnapshot, 0, len(cards))
	for _, c := range cards {
		cardKey := c.NodeUID + "|" + c.GPUUUID
		cardsByKey[cardKey] = c
		gpuIndexByKey[cardKey] = c.GPUIndex

		acc := computeCardAccounting(c, leases, residencies, "")
		unmatQuarantine := int64(0)
		if c.QuarantinedMiB > acc.QuarantinedProvenMiB {
			unmatQuarantine = c.QuarantinedMiB - acc.QuarantinedProvenMiB
		}
		cardHeadroom := DefaultCardHeadroomMiB
		for _, l := range leases {
			if (l.State == "leased" || l.State == "running" || l.State == "legacy_acquired") &&
				slices.Contains(l.GPUUUIDs, c.GPUUUID) &&
				l.Priority == PriorityExclusiveBatch &&
				l.PeakBudgetMiB >= 28672 &&
				acc.ActiveHoldersCount <= 1 {
				cardHeadroom = ExclusiveCardHeadroomMiB
				break
			}
		}
		committed := c.DriverOverheadMiB + acc.PinnedMiB + acc.LeasedMiB + c.QuarantinedMiB + acc.UnattributedExternalMiB
		ledgerUncommitted := c.TotalVRAMMiB - committed - cardHeadroom
		if ledgerUncommitted < 0 {
			ledgerUncommitted = 0
		}
		unmatTotal := acc.UnmaterializedBudgetMiB + unmatQuarantine
		measGrant := c.MeasuredFreeVRAMMiB - cardHeadroom - unmatTotal
		grantable := ledgerUncommitted
		if measGrant < grantable {
			grantable = measGrant
		}
		if grantable < 0 {
			grantable = 0
		}
		telemetryFresh := c.State == "ready" && c.TelemetrySampledAt != nil && now.Sub(*c.TelemetrySampledAt) <= maxAge
		admissionOpen := telemetryFresh && c.AdmissionMode == AdmissionActive
		if !admissionOpen {
			grantable = 0
		}
		sampledAtStr := ""
		if c.TelemetrySampledAt != nil {
			sampledAtStr = c.TelemetrySampledAt.UTC().Format(time.RFC3339Nano)
		}
		cardSnaps = append(cardSnaps, CardSnapshot{
			NodeUID:                    c.NodeUID,
			NodeName:                   c.NodeName,
			GPUUUID:                    c.GPUUUID,
			GPUIndex:                   c.GPUIndex,
			ModelName:                  modelByKey[cardKey],
			TotalVRAMMiB:               c.TotalVRAMMiB,
			DriverOverheadMiB:          c.DriverOverheadMiB,
			HeadroomMiB:                cardHeadroom,
			PinnedMiB:                  acc.PinnedMiB,
			LeasedMiB:                  acc.LeasedMiB,
			EvictableMiB:               acc.EvictableMiB,
			QuarantinedMiB:             c.QuarantinedMiB,
			UnattributedExternalMiB:    acc.UnattributedExternalMiB,
			UnmaterializedBudgetMiB:    unmatTotal,
			LedgerUncommittedMiB:       ledgerUncommitted,
			MeasuredFreeVRAMMiB:        c.MeasuredFreeVRAMMiB,
			MeasuredUsedVRAMMiB:        c.MeasuredUsedVRAMMiB,
			GrantableStandardMiB:       grantable,
			TelemetryFresh:             telemetryFresh,
			AdmissionOpen:              admissionOpen,
			TelemetryBootID:            c.TelemetryBootID,
			TelemetrySampleID:          c.TelemetrySampleID,
			TelemetrySampledAt:         sampledAtStr,
			FencingEpoch:               c.FencingEpoch,
			MinFencingToken:            c.MinFencingToken,
			ReservedByLeaseID:          c.ReservedByLeaseID,
			TimeSlicingSlots:           c.TimeSlicingSlots,
			AllocatedK8sSlots:          c.AllocatedK8sSlots,
			ScalingUpK8sSlots:          c.ScalingUpK8sSlots,
			UnscheduledPendingK8sSlots: c.UnscheduledPendingK8sSlots,
			AdmissionMode:              c.AdmissionMode,
			State:                      c.State,
			StateReason:                c.StateReason,
			LastEventSeq:               c.LastEventSeq,
			LastEventHash:              c.LastEventHash,
		})
	}

	leaseDecisions := make([]LeaseDecision, 0, len(leases))
	queueIdx := 0
	for _, l := range leases {
		qPos := 0
		if l.State == "queued" || l.State == "draining" {
			queueIdx++
			qPos = queueIdx
		}
		leaseDecisions = append(leaseDecisions, decisionFromRow(l, qPos, 0))
	}

	resSnaps := make([]ResidencySnapshot, 0, len(residencies))
	for _, r := range residencies {
		var provenMiB int64
		if c, ok := cardsByKey[r.NodeUID+"|"+r.GPUUUID]; ok {
			pUID := normPodUID(r.PodUID)
			cID := normContainerID(r.ContainerID)
			for _, ap := range c.AttributedProcesses {
				apPod := normPodUID(ap.PodUID)
				apCID := normContainerID(ap.ContainerID)
				if holderMatchesAttributedProcess(pUID, cID, apPod, apCID) {
					if ap.UsedVRAMMiB > 0 {
						provenMiB += ap.UsedVRAMMiB
					}
				}
			}
		}
		activeLeaseID := ""
		for _, l := range leases {
			if l.State == "leased" || l.State == "running" || l.State == "legacy_acquired" || l.State == "releasing" || l.State == "reclaiming" {
				if (normPodUID(r.PodUID) != "" && normPodUID(l.PodUID) == normPodUID(r.PodUID)) ||
					(r.BootID != "" && l.BootID == r.BootID) {
					activeLeaseID = l.LeaseID
					break
				}
			}
		}
		evictable := r.ResidencyState == "idle_evictable" &&
			!r.HumanProtected &&
			!r.GUIConnectionAlive &&
			!r.SnapshotInProgress &&
			activeLeaseID == "" &&
			(strings.TrimSpace(r.CallbackEndpoint) != "" || s.InstanceDir != nil)
		resSnaps = append(resSnaps, ResidencySnapshot{
			InstanceID:           r.InstanceID,
			AppID:                r.AppID,
			NodeUID:              r.NodeUID,
			PodUID:               r.PodUID,
			BootID:               r.BootID,
			GPUEpoch:             r.GPUEpoch,
			ResidencySeq:         r.ResidencySeq,
			GPUUUID:              r.GPUUUID,
			ResidencyState:       r.ResidencyState,
			HumanProtected:       r.HumanProtected,
			GUIConnectionAlive:   r.GUIConnectionAlive,
			Evictable:            evictable,
			Dirty:                r.Dirty,
			SnapshotInProgress:   r.SnapshotInProgress,
			SnapshotRequired:     r.SnapshotRequired,
			BlockingReason:       r.BlockingReason,
			PinnedBudgetMiB:      r.PinnedBudgetMiB,
			VRAMAllocatedMiB:     r.VRAMAllocatedMiB,
			ProvenVRAMMiB:        provenMiB,
			ActiveComputeLeaseID: activeLeaseID,
			IdleSeconds:          idleByInst[r.InstanceID],
			TemplateUpgraded:     r.TemplateUpgraded,
		})
	}

	recentEvents := []SchedulerEventSnapshot{}
	evRows, err := tx.Query(ctx, `
		SELECT event_id, node_uid, gpu_uuid, card_event_seq, occurred_at,
		       entity_type, entity_id, from_state, to_state, budget_mib, reason_code
		  FROM station.gpu_scheduler_events
		 WHERE station_id = $1
		   AND reason_code NOT IN ('EVICT_INTENT', 'FORCE_DRAIN_INTENT')
		 ORDER BY event_id DESC
		 LIMIT 12
	`, s.StationID)
	if err == nil {
		for evRows.Next() {
			var ev SchedulerEventSnapshot
			var occ time.Time
			if evRows.Scan(
				&ev.EventID, &ev.NodeUID, &ev.GPUUUID, &ev.CardEventSeq, &occ,
				&ev.EntityType, &ev.EntityID, &ev.FromState, &ev.ToState, &ev.BudgetMiB, &ev.ReasonCode,
			) == nil {
				ev.OccurredAt = occ.UTC().Format(time.RFC3339Nano)
				ev.GPUIndex = gpuIndexByKey[ev.NodeUID+"|"+ev.GPUUUID]
				recentEvents = append(recentEvents, ev)
			}
		}
		evRows.Close()
	}

	return LedgerSnapshot{
		StationID:        s.StationID,
		AppGPUQuota:      s.appGPUQuota(),
		CutoverCompleted: s.CutoverCompleted,
		GeneratedAt:      now.Format(time.RFC3339Nano),
		Cards:            cardSnaps,
		ActiveLeases:     leaseDecisions,
		Residencies:      resSnaps,
		RecentEvents:     recentEvents,
	}, nil
}

func (s *Service) resolveResidencyCallbackEndpoint(ctx context.Context, instanceID, storedEndpoint string) string {
	cb := strings.TrimSpace(storedEndpoint)
	if cb != "" {
		return cb
	}
	if s.InstanceDir != nil {
		if reg, found, err := s.InstanceDir.LookupInstance(ctx, s.StationID, instanceID); err == nil && found {
			return strings.TrimSpace(reg.CallbackEndpoint)
		}
	}
	return ""
}

func (s *Service) EvictResidency(ctx context.Context, caller CallerIdentity, instanceID string, req ResidencyEvictRequest) (ResidencyActionResult, error) {
	if caller.Kind != CallerAdmin && caller.AppID != "station-core" && caller.AppID != "app-resources" {
		return ResidencyActionResult{}, ErrForbiddenCaller
	}
	instanceID = strings.TrimSpace(instanceID)
	if instanceID == "" || len(instanceID) > 128 {
		return ResidencyActionResult{}, ErrInvalidRequest
	}
	idemKey := strings.TrimSpace(req.IdempotencyKey)
	if idemKey == "" || len(idemKey) > 128 {
		return ResidencyActionResult{}, ErrInvalidRequest
	}
	if !validExpectedResidencyTarget(req.ExpectedBootID, req.ExpectedGPUEpoch, req.ExpectedResidencySeq) {
		return ResidencyActionResult{}, ErrInvalidRequest
	}
	expBoot := strings.TrimSpace(req.ExpectedBootID)
	expEpoch := *req.ExpectedGPUEpoch
	expSeq := *req.ExpectedResidencySeq

	idemScope := fmt.Sprintf(
		"%s:%s:%s:%s",
		caller.Kind,
		caller.AppID,
		strings.TrimSpace(req.OrganizationID),
		strings.TrimSpace(req.ActorUserID),
	)
	reqDigest := computeCanonicalHash(
		"evict",
		instanceID,
		expBoot,
		req.ExpectedGPUEpoch,
		req.ExpectedResidencySeq,
		strings.TrimSpace(req.OrganizationID),
		strings.TrimSpace(req.ActorUserID),
	)
	unlock := s.lockResidencyIdempotency(idemScope, idemKey)
	defer unlock()

	idemState, err := s.lookupResidencyActionState(ctx, idemScope, idemKey, reqDigest)
	if err != nil {
		return ResidencyActionResult{}, err
	}
	if idemState.Completed {
		return idemState.Result, nil
	}

	var r residencyRow
	err = s.Pool.QueryRow(ctx, `
		SELECT instance_id, app_id, node_uid, pod_uid, container_name, container_id,
		       boot_id, gpu_epoch, residency_seq, gpu_uuid, residency_state,
		       human_protected, gui_connection_alive, dirty, snapshot_in_progress, snapshot_required,
		       blocking_reason, pinned_budget_mib, vram_allocated_mib, callback_endpoint, template_upgraded
		  FROM station.gpu_instance_residency
		 WHERE station_id = $1 AND instance_id = $2
	`, s.StationID, instanceID).Scan(
		&r.InstanceID, &r.AppID, &r.NodeUID, &r.PodUID, &r.ContainerName, &r.ContainerID,
		&r.BootID, &r.GPUEpoch, &r.ResidencySeq, &r.GPUUUID, &r.ResidencyState,
		&r.HumanProtected, &r.GUIConnectionAlive, &r.Dirty, &r.SnapshotInProgress, &r.SnapshotRequired,
		&r.BlockingReason, &r.PinnedBudgetMiB, &r.VRAMAllocatedMiB, &r.CallbackEndpoint, &r.TemplateUpgraded,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return ResidencyActionResult{}, ErrResidencyNotFound
	}
	if err != nil {
		return ResidencyActionResult{}, err
	}

	var hasActiveCompute bool
	_ = s.Pool.QueryRow(ctx, `
		SELECT EXISTS(
			SELECT 1
			  FROM station.gpu_resource_leases
			 WHERE station_id = $1
			   AND state IN ('leased', 'running', 'legacy_acquired', 'releasing', 'reclaiming')
			   AND ((pod_uid <> '' AND lower(pod_uid) = lower($2)) OR (boot_id <> '' AND boot_id = $3))
		)
	`, s.StationID, r.PodUID, r.BootID).Scan(&hasActiveCompute)

	var intent residencyActionIntent
	var resumePtr *residencyActionIntent
	if idemState.HasIntent {
		intent = idemState.Intent
		if r.BootID != intent.TargetBootID || r.GPUEpoch != intent.TargetGPUEpoch || r.ResidencySeq < intent.TargetResidencySeq {
			return ResidencyActionResult{}, ErrVersionConflict
		}
		if r.HumanProtected || r.GUIConnectionAlive || r.ResidencyState == "human_protected" || r.ResidencyState == "human_dirty" {
			return ResidencyActionResult{}, ErrGUIEditLeaseHeld
		}
		if hasActiveCompute || r.ResidencyState == "active_compute" || r.ResidencyState == "waking" || r.ResidencyState == "sealing" || r.SnapshotInProgress {
			return ResidencyActionResult{}, ErrInstanceActiveCompute
		}
		if r.ResidencyState != "idle_evictable" && r.ResidencyState != "zero_vram_standby" {
			return ResidencyActionResult{}, ErrInstanceNotEvictable
		}
		resumePtr = &intent
	} else {
		if r.BootID != expBoot || r.GPUEpoch != expEpoch || r.ResidencySeq != expSeq {
			return ResidencyActionResult{}, ErrVersionConflict
		}
		if r.HumanProtected || r.GUIConnectionAlive || r.ResidencyState == "human_protected" || r.ResidencyState == "human_dirty" {
			return ResidencyActionResult{}, ErrGUIEditLeaseHeld
		}
		if r.ResidencyState == "zero_vram_standby" {
			return ResidencyActionResult{}, ErrAlreadyZeroVRAM
		}
		if hasActiveCompute || r.ResidencyState == "active_compute" || r.ResidencyState == "waking" || r.ResidencyState == "sealing" || r.SnapshotInProgress {
			return ResidencyActionResult{}, ErrInstanceActiveCompute
		}
		if r.ResidencyState != "idle_evictable" {
			return ResidencyActionResult{}, ErrInstanceNotEvictable
		}
	}

	cb := s.resolveResidencyCallbackEndpoint(ctx, instanceID, r.CallbackEndpoint)
	if cb == "" || s.Evictor == nil || s.Verifier == nil {
		return ResidencyActionResult{}, ErrNodeStateUnknown
	}

	if !idemState.HasIntent {
		intent, err = s.persistResidencyActionIntent(
			ctx,
			instanceID,
			idemScope,
			idemKey,
			reqDigest,
			strings.TrimSpace(req.ActorUserID),
			strings.TrimSpace(req.OrganizationID),
			req.RunNonce,
			"EVICT_INTENT",
			expBoot,
			expEpoch,
			expSeq,
			false,
		)
		if err != nil {
			return ResidencyActionResult{}, err
		}
		resumePtr = &intent
	}

	cmd := EvictVRAMCommand{
		CallbackEndpoint:     cb,
		InstanceID:           instanceID,
		ExpectedBootID:       intent.TargetBootID,
		ExpectedResidencySeq: r.ResidencySeq,
		ExpectedGPUEpoch:     intent.TargetGPUEpoch,
		SoftEvictDeadlineMs:  DefaultSoftEvictDeadline,
		ForceHard:            false,
		Reason:               "USER_RESIDENCY_EVICT",
	}
	evRes, err := s.Evictor.EvictVRAM(ctx, cmd)
	evRes = normalizeEvictVRAMResult(evRes)
	if err == nil &&
		idemState.HasIntent &&
		evRes.Conflict &&
		strings.EqualFold(strings.TrimSpace(evRes.ReasonCode), "RESIDENCY_SEQ_CONFLICT") &&
		(evRes.BootID == "" || evRes.BootID == intent.TargetBootID) &&
		evRes.GPUEpoch == intent.TargetGPUEpoch &&
		evRes.ResidencySeq > cmd.ExpectedResidencySeq {
		// Converge crash-before-commit retry within the exact same (TargetBootID, TargetGPUEpoch) when worker drained in prior attempt.
		cmd.ExpectedResidencySeq = evRes.ResidencySeq
		evRes, err = s.Evictor.EvictVRAM(ctx, cmd)
		evRes = normalizeEvictVRAMResult(evRes)
	}
	if err != nil {
		return ResidencyActionResult{}, ErrNodeStateUnknown
	}
	if evRes.Conflict {
		rc := strings.ToUpper(strings.TrimSpace(evRes.ReasonCode))
		switch rc {
		case "GUI_EDIT_LEASE_HELD", "HUMAN_PROTECTED":
			return ResidencyActionResult{}, ErrGUIEditLeaseHeld
		case "INSTANCE_ACTIVE_COMPUTE", "ACTIVE_COMPUTE", "WORKER_BUSY":
			return ResidencyActionResult{}, ErrInstanceActiveCompute
		default:
			return ResidencyActionResult{}, ErrVersionConflict
		}
	}
	if evRes.Unreclaimed || !evRes.Settled {
		return ResidencyActionResult{}, ErrReclaimQuarantined
	}
	var actionOut ResidencyActionResult
	if err := s.applyEvictedStandby(ctx, r.NodeUID, cmd, evRes, req.RunNonce, &residencyIdemEventMeta{
		ReasonCode:       "EVICT_SETTLED",
		IdempotencyScope: idemScope,
		IdempotencyKey:   idemKey,
		RequestSHA256:    reqDigest,
		ActorUserID:      strings.TrimSpace(req.ActorUserID),
		OrganizationID:   strings.TrimSpace(req.OrganizationID),
		AppID:            r.AppID,
		ResumeIntent:     resumePtr,
		ResultOut:        &actionOut,
	}); err != nil {
		return ResidencyActionResult{}, err
	}
	s.PromoteQueuedLeases(ctx, r.NodeUID)

	return actionOut, nil
}

func (s *Service) ForceDrainResidency(ctx context.Context, caller CallerIdentity, instanceID string, req ResidencyForceDrainRequest) (ResidencyActionResult, error) {
	if caller.Kind != CallerAdmin && caller.AppID != "station-core" {
		return ResidencyActionResult{}, ErrForbiddenCaller
	}
	instanceID = strings.TrimSpace(instanceID)
	if instanceID == "" || len(instanceID) > 128 {
		return ResidencyActionResult{}, ErrInvalidRequest
	}
	if !req.ConfirmForceDrain || strings.TrimSpace(req.ConfirmInstanceID) != instanceID {
		return ResidencyActionResult{}, ErrConfirmationRequired
	}
	idemKey := strings.TrimSpace(req.IdempotencyKey)
	if idemKey == "" || len(idemKey) > 128 {
		return ResidencyActionResult{}, ErrInvalidRequest
	}
	if !validExpectedResidencyTarget(req.ExpectedBootID, req.ExpectedGPUEpoch, req.ExpectedResidencySeq) {
		return ResidencyActionResult{}, ErrInvalidRequest
	}
	expBoot := strings.TrimSpace(req.ExpectedBootID)
	expEpoch := *req.ExpectedGPUEpoch
	expSeq := *req.ExpectedResidencySeq

	idemScope := fmt.Sprintf(
		"%s:%s:%s:%s",
		caller.Kind,
		caller.AppID,
		strings.TrimSpace(req.OrganizationID),
		strings.TrimSpace(req.ActorUserID),
	)
	reqDigest := computeCanonicalHash(
		"force-drain",
		instanceID,
		req.ConfirmForceDrain,
		strings.TrimSpace(req.ConfirmInstanceID),
		expBoot,
		req.ExpectedGPUEpoch,
		req.ExpectedResidencySeq,
		strings.TrimSpace(req.OrganizationID),
		strings.TrimSpace(req.ActorUserID),
		strings.TrimSpace(req.Reason),
	)
	unlock := s.lockResidencyIdempotency(idemScope, idemKey)
	defer unlock()

	idemState, err := s.lookupResidencyActionState(ctx, idemScope, idemKey, reqDigest)
	if err != nil {
		return ResidencyActionResult{}, err
	}
	if idemState.Completed {
		return idemState.Result, nil
	}

	var r residencyRow
	err = s.Pool.QueryRow(ctx, `
		SELECT instance_id, app_id, node_uid, pod_uid, container_name, container_id,
		       boot_id, gpu_epoch, residency_seq, gpu_uuid, residency_state,
		       human_protected, gui_connection_alive, dirty, snapshot_in_progress, snapshot_required,
		       blocking_reason, pinned_budget_mib, vram_allocated_mib, callback_endpoint, template_upgraded
		  FROM station.gpu_instance_residency
		 WHERE station_id = $1 AND instance_id = $2
	`, s.StationID, instanceID).Scan(
		&r.InstanceID, &r.AppID, &r.NodeUID, &r.PodUID, &r.ContainerName, &r.ContainerID,
		&r.BootID, &r.GPUEpoch, &r.ResidencySeq, &r.GPUUUID, &r.ResidencyState,
		&r.HumanProtected, &r.GUIConnectionAlive, &r.Dirty, &r.SnapshotInProgress, &r.SnapshotRequired,
		&r.BlockingReason, &r.PinnedBudgetMiB, &r.VRAMAllocatedMiB, &r.CallbackEndpoint, &r.TemplateUpgraded,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return ResidencyActionResult{}, ErrResidencyNotFound
	}
	if err != nil {
		return ResidencyActionResult{}, err
	}

	var activeBoundLeases []boundLeaseRecord
	lRows, lErr := s.Pool.Query(ctx, `
		SELECT lease_id::text, fencing_token, attempt_id
		  FROM station.gpu_resource_leases
		 WHERE station_id = $1
		   AND state IN ('leased', 'running', 'legacy_acquired', 'releasing', 'reclaiming')
		   AND ((pod_uid <> '' AND lower(pod_uid) = lower($2)) OR (boot_id <> '' AND boot_id = $3))
	`, s.StationID, r.PodUID, r.BootID)
	if lErr != nil {
		return ResidencyActionResult{}, lErr
	}
	for lRows.Next() {
		var bl boundLeaseRecord
		if lRows.Scan(&bl.LeaseID, &bl.Fencing, &bl.AttemptID) == nil {
			activeBoundLeases = append(activeBoundLeases, bl)
		}
	}
	lRows.Close()

	var intent residencyActionIntent
	if idemState.HasIntent {
		intent = idemState.Intent
		// Strictly pin retry to the exact pre-persisted (TargetBootID, TargetGPUEpoch):
		// NEVER revoke or force-drain if the instance woke into a new gpu_epoch or rebooted after crash!
		if r.BootID != intent.TargetBootID || r.GPUEpoch != intent.TargetGPUEpoch || r.ResidencySeq < intent.TargetResidencySeq {
			return ResidencyActionResult{}, ErrVersionConflict
		}
	} else {
		if r.BootID != expBoot || r.GPUEpoch != expEpoch || r.ResidencySeq != expSeq {
			return ResidencyActionResult{}, ErrVersionConflict
		}
		if r.ResidencyState == "zero_vram_standby" && !r.HumanProtected && !r.GUIConnectionAlive && len(activeBoundLeases) == 0 {
			return ResidencyActionResult{}, ErrAlreadyZeroVRAM
		}
	}

	cb := s.resolveResidencyCallbackEndpoint(ctx, instanceID, r.CallbackEndpoint)
	if cb == "" || s.Evictor == nil || s.Verifier == nil {
		return ResidencyActionResult{}, ErrNodeStateUnknown
	}

	if !idemState.HasIntent {
		intent, err = s.persistResidencyActionIntent(
			ctx,
			instanceID,
			idemScope,
			idemKey,
			reqDigest,
			strings.TrimSpace(req.ActorUserID),
			strings.TrimSpace(req.OrganizationID),
			req.RunNonce,
			"FORCE_DRAIN_INTENT",
			expBoot,
			expEpoch,
			expSeq,
			true,
		)
		if err != nil {
			return ResidencyActionResult{}, err
		}
	}

	seenLeaseID := make(map[string]bool, len(intent.BoundLeases)+len(activeBoundLeases))
	var boundLeases []boundLeaseRecord
	var boundLeaseIDs []string
	for _, bl := range intent.BoundLeases {
		if bl.LeaseID != "" && !seenLeaseID[bl.LeaseID] {
			seenLeaseID[bl.LeaseID] = true
			boundLeases = append(boundLeases, bl)
			boundLeaseIDs = append(boundLeaseIDs, bl.LeaseID)
		}
	}
	for _, bl := range activeBoundLeases {
		if bl.LeaseID != "" && !seenLeaseID[bl.LeaseID] {
			seenLeaseID[bl.LeaseID] = true
			boundLeases = append(boundLeases, bl)
			boundLeaseIDs = append(boundLeaseIDs, bl.LeaseID)
		}
	}

	expectedEpoch := intent.TargetGPUEpoch
	expectedSeq := r.ResidencySeq
	for _, bl := range boundLeases {
		revRes, revErr := s.Evictor.RevokeLease(ctx, RevokeLeaseCommand{
			CallbackEndpoint: cb,
			LeaseID:          bl.LeaseID,
			InstanceID:       instanceID,
			FencingToken:     bl.Fencing,
			BootID:           intent.TargetBootID,
			ExpectedGPUEpoch: expectedEpoch,
			PodUID:           r.PodUID,
			ContainerID:      r.ContainerID,
			AttemptID:        bl.AttemptID,
			Reason:           "ADMIN_FORCE_DRAIN",
		})
		if revErr == nil {
			if revRes.BootID != "" && revRes.BootID != intent.TargetBootID {
				return ResidencyActionResult{}, ErrVersionConflict
			}
			if revRes.GPUEpoch > 0 && revRes.GPUEpoch != expectedEpoch {
				return ResidencyActionResult{}, ErrVersionConflict
			}
			if revRes.ResidencySeq > expectedSeq {
				expectedSeq = revRes.ResidencySeq
			}
		}
	}

	cmd := EvictVRAMCommand{
		CallbackEndpoint:     cb,
		InstanceID:           instanceID,
		ExpectedBootID:       intent.TargetBootID,
		ExpectedResidencySeq: expectedSeq,
		ExpectedGPUEpoch:     expectedEpoch,
		SoftEvictDeadlineMs:  3000,
		ForceHard:            true,
		Reason:               "ADMIN_FORCE_DRAIN",
	}
	evRes, err := s.Evictor.EvictVRAM(ctx, cmd)
	evRes = normalizeEvictVRAMResult(evRes)
	if err == nil &&
		evRes.Conflict &&
		strings.EqualFold(strings.TrimSpace(evRes.ReasonCode), "RESIDENCY_SEQ_CONFLICT") &&
		(evRes.BootID == "" || evRes.BootID == intent.TargetBootID) &&
		evRes.GPUEpoch == expectedEpoch &&
		evRes.ResidencySeq > expectedSeq {
		// In-epoch state sequence advanced on the worker (e.g. RevokeLease, local watchdog, or prior crash-before-commit attempt) before async ReportResidency hit PG.
		// Retry ONCE pinned strictly to the exact same (TargetBootID, ExpectedGPUEpoch) and observed ResidencySeq; never retry across epoch or boot changes.
		cmd.ExpectedResidencySeq = evRes.ResidencySeq
		expectedSeq = evRes.ResidencySeq
		evRes, err = s.Evictor.EvictVRAM(ctx, cmd)
		evRes = normalizeEvictVRAMResult(evRes)
	}
	if err != nil {
		return ResidencyActionResult{}, ErrNodeStateUnknown
	}
	if evRes.Conflict {
		return ResidencyActionResult{}, ErrVersionConflict
	}
	if evRes.Unreclaimed || !evRes.Settled {
		return ResidencyActionResult{}, ErrReclaimQuarantined
	}
	if evRes.BootID != "" && evRes.BootID != intent.TargetBootID {
		return ResidencyActionResult{}, ErrVersionConflict
	}
	if evRes.GPUEpoch != expectedEpoch {
		return ResidencyActionResult{}, ErrVersionConflict
	}

	expectSealedEpoch := expectedEpoch
	if expectSealedEpoch <= 0 {
		expectSealedEpoch = 1
	}
	if evRes.SealedGPUEpoch > 0 && evRes.SealedGPUEpoch != expectSealedEpoch {
		return ResidencyActionResult{}, ErrVersionConflict
	}
	sealMeta, proofOK := validateEpochSealProof(
		evRes.ReclaimProof,
		r.PodUID,
		r.ContainerID,
		intent.TargetBootID,
		expectSealedEpoch,
		evRes.SupervisorPID,
		evRes.SupervisorStarttimeTicks,
		"",
		0,
	)
	if !proofOK {
		return ResidencyActionResult{}, ErrReclaimQuarantined
	}
	supPID := sealMeta.SupervisorPID
	supTicks := sealMeta.SupervisorStarttimeTicks
	if supPID <= 0 || supTicks == 0 {
		return ResidencyActionResult{}, ErrReclaimQuarantined
	}

	vRes, vErr := s.Verifier.VerifyTerminated(ctx, VerifyTerminatedRequest{
		NodeUID:                  r.NodeUID,
		GPUUUID:                  r.GPUUUID,
		PodUID:                   r.PodUID,
		ContainerID:              r.ContainerID,
		BootID:                   intent.TargetBootID,
		GPUEpoch:                 expectSealedEpoch,
		SupervisorPID:            supPID,
		SupervisorStarttimeTicks: supTicks,
		EpochFenceConfirmed:      true,
		Mode:                     "epoch_unload",
	})
	if vErr != nil || vRes.ErrorCode == "UNKNOWN_NODE_STATE" {
		return ResidencyActionResult{}, ErrNodeStateUnknown
	}
	if !vRes.Verified {
		return ResidencyActionResult{}, ErrReclaimQuarantined
	}

	s.fireHook(ctx, "after_force_drain_verify_before_commit", map[string]string{
		"instance_id": instanceID,
		"node_uid":    r.NodeUID,
	})

	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return ResidencyActionResult{}, err
	}
	defer rollbackTx(tx)

	cards, err := lockCardsForUpdate(ctx, tx, s.StationID, r.NodeUID)
	if err != nil {
		return ResidencyActionResult{}, err
	}

	var prevState, curBootID, curPodUID, curCID, curGPUUUID string
	var prevAlloc *int64
	var prevPinned, curEpoch, curSeq int64
	if err := tx.QueryRow(ctx, `
		SELECT gpu_uuid, residency_state, vram_allocated_mib, pinned_budget_mib, boot_id, pod_uid, container_id, gpu_epoch, residency_seq
		  FROM station.gpu_instance_residency
		 WHERE station_id = $1 AND instance_id = $2
		   FOR UPDATE
	`, s.StationID, instanceID).Scan(&curGPUUUID, &prevState, &prevAlloc, &prevPinned, &curBootID, &curPodUID, &curCID, &curEpoch, &curSeq); err != nil {
		return ResidencyActionResult{}, err
	}
	if normPodUID(curPodUID) != normPodUID(r.PodUID) ||
		normContainerID(curCID) != normContainerID(r.ContainerID) ||
		curBootID != intent.TargetBootID ||
		curEpoch != expectedEpoch ||
		curSeq < intent.TargetResidencySeq ||
		curSeq > evRes.ResidencySeq ||
		(curSeq == evRes.ResidencySeq && prevState != "zero_vram_standby") {
		return ResidencyActionResult{}, ErrVersionConflict
	}

	newSeq := evRes.ResidencySeq
	newEpoch := evRes.GPUEpoch
	zero := int64(0)
	cmdTag, err := tx.Exec(ctx, `
		UPDATE station.gpu_instance_residency
		   SET residency_state = 'zero_vram_standby',
		       human_protected = FALSE,
		       gui_connection_alive = FALSE,
		       pinned_budget_mib = 0,
		       vram_allocated_mib = $3,
		       residency_seq = $4,
		       gpu_epoch = $5,
		       blocking_reason = 'none',
		       updated_at = now()
		 WHERE station_id = $1
		   AND instance_id = $2
		   AND boot_id = $6
		   AND gpu_epoch = $7
		   AND residency_seq = $8
	`, s.StationID, instanceID, zero, newSeq, newEpoch, curBootID, curEpoch, curSeq)
	if err != nil {
		return ResidencyActionResult{}, err
	}
	if cmdTag.RowsAffected() != 1 {
		return ResidencyActionResult{}, ErrVersionConflict
	}

	// Expire ONLY the pre-captured bound compute lease IDs that were revoked for this drained epoch (never blanket pod_uid/boot_id)!
	var maxFence int64
	if len(boundLeaseIDs) > 0 {
		expRows, expErr := tx.Query(ctx, `
			UPDATE station.gpu_resource_leases
			   SET state = 'expired',
			       blocking_reason = 'admin_force_drain',
			       revoke_acknowledged = TRUE,
			       version = version + 1,
			       updated_at = now()
			 WHERE station_id = $1
			   AND lease_id = ANY($2::uuid[])
			   AND state IN ('leased', 'running', 'legacy_acquired', 'releasing', 'reclaiming')
			 RETURNING fencing_token
		`, s.StationID, boundLeaseIDs)
		if expErr != nil {
			return ResidencyActionResult{}, expErr
		}
		for expRows.Next() {
			var ft int64
			if expRows.Scan(&ft) == nil && ft > maxFence {
				maxFence = ft
			}
		}
		expRows.Close()
	}

	fromState := prevState
	if intent.FromState != "" && prevState == "zero_vram_standby" {
		fromState = intent.FromState
	}
	reclaimedMiB := int64(0)
	if prevAlloc != nil && *prevAlloc > 0 {
		reclaimedMiB = *prevAlloc
	} else if prevPinned > 0 {
		reclaimedMiB = prevPinned
	}
	if reclaimedMiB == 0 && intent.ReclaimedVRAMMiB > 0 {
		reclaimedMiB = intent.ReclaimedVRAMMiB
	}

	corrUUID := uuid.Must(uuid.NewV7()).String()
	var actionOut ResidencyActionResult
	eventWritten := false
	for i := range cards {
		if cards[i].GPUUUID == curGPUUUID || (!eventWritten && i == len(cards)-1) {
			cleanedAttr := filterOutHolderProcesses(cards[i].AttributedProcesses, curPodUID, curCID)
			attrRaw, _ := json.Marshal(cleanedAttr)
			nextMinFence := cards[i].MinFencingToken
			if maxFence >= nextMinFence {
				nextMinFence = maxFence + 1
			}
			if cards[i].GPUUUID == curGPUUUID && vRes.MeasuredFreeVRAMMiB > 0 {
				newFree := vRes.MeasuredFreeVRAMMiB
				newUsed := cards[i].TotalVRAMMiB - newFree
				if newUsed < cards[i].DriverOverheadMiB {
					newUsed = cards[i].DriverOverheadMiB
				}
				if _, err := tx.Exec(ctx, `
					UPDATE station.gpu_physical_cards
					   SET measured_free_vram_mib = $4,
					       measured_used_vram_mib = $5,
					       attributed_processes = $6,
					       min_fencing_token = $7,
					       telemetry_sampled_at = $8,
					       updated_at = now()
					 WHERE station_id = $1 AND node_uid = $2 AND gpu_uuid = $3
				`, s.StationID, cards[i].NodeUID, cards[i].GPUUUID, newFree, newUsed, attrRaw, nextMinFence, s.now()); err != nil {
					return ResidencyActionResult{}, err
				}
			} else {
				if _, err := tx.Exec(ctx, `
					UPDATE station.gpu_physical_cards
					   SET attributed_processes = $4,
					       min_fencing_token = $5,
					       updated_at = now()
					 WHERE station_id = $1 AND node_uid = $2 AND gpu_uuid = $3
				`, s.StationID, cards[i].NodeUID, cards[i].GPUUUID, attrRaw, nextMinFence); err != nil {
					return ResidencyActionResult{}, err
				}
			}
			expectedEventSeq := cards[i].LastEventSeq + 1
			actionOut = ResidencyActionResult{
				InstanceID:       instanceID,
				AppID:            r.AppID,
				FromState:        fromState,
				ResidencyState:   "zero_vram_standby",
				BootID:           curBootID,
				GPUEpoch:         newEpoch,
				ResidencySeq:     newSeq,
				ReclaimedVRAMMiB: reclaimedMiB,
				LedgerEventSeq:   expectedEventSeq,
			}
			seq, _, err := appendCardEvent(ctx, tx, &cards[i], eventRecordInput{
				RunNonce:             req.RunNonce,
				CorrelationEventUUID: corrUUID,
				EntityType:           "residency",
				EntityID:             instanceID,
				FromState:            fromState,
				ToState:              "zero_vram_standby",
				ResidencySeq:         newSeq,
				GPUEpoch:             newEpoch,
				BudgetMiB:            reclaimedMiB,
				ReasonCode:           "ADMIN_FORCE_DRAIN",
				Payload: map[string]any{
					"idempotency_scope": idemScope,
					"idempotency_key":   idemKey,
					"request_sha256":    reqDigest,
					"actor_user_id":     strings.TrimSpace(req.ActorUserID),
					"organization_id":   strings.TrimSpace(req.OrganizationID),
					"boot_id":           curBootID,
					"force_hard":        true,
					"action_result":     actionOut,
				},
			})
			if err != nil {
				return ResidencyActionResult{}, err
			}
			actionOut.LedgerEventSeq = seq
			eventWritten = true
			if cards[i].GPUUUID == curGPUUUID {
				break
			}
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return ResidencyActionResult{}, err
	}
	s.PromoteQueuedLeases(ctx, r.NodeUID)

	return actionOut, nil
}

func nonRedirectHTTPClient(base *http.Client, defaultTimeout time.Duration) *http.Client {
	noRedirect := func(*http.Request, []*http.Request) error {
		return http.ErrUseLastResponse
	}
	noProxyTransport := func() http.RoundTripper {
		if dt, ok := http.DefaultTransport.(*http.Transport); ok {
			t := dt.Clone()
			t.Proxy = nil
			return t
		}
		return &http.Transport{Proxy: nil}
	}
	if base == nil {
		return &http.Client{
			Timeout:       defaultTimeout,
			CheckRedirect: noRedirect,
			Transport:     noProxyTransport(),
		}
	}
	cloned := *base
	cloned.CheckRedirect = noRedirect
	if cloned.Transport == nil {
		cloned.Transport = noProxyTransport()
	}
	if cloned.Timeout <= 0 {
		cloned.Timeout = defaultTimeout
	}
	return &cloned
}

type HTTPNodeVerifier struct {
	Client         *http.Client
	EndpointByNode map[string]string
	DefaultURL     string
	BearerToken    string
}

func (v HTTPNodeVerifier) VerifyTerminated(ctx context.Context, req VerifyTerminatedRequest) (VerifyTerminatedResult, error) {
	baseURL := v.DefaultURL
	if v.EndpointByNode != nil && v.EndpointByNode[req.NodeUID] != "" {
		baseURL = v.EndpointByNode[req.NodeUID]
	}
	if strings.TrimSpace(baseURL) == "" {
		return VerifyTerminatedResult{ErrorCode: "UNKNOWN_NODE_STATE"}, ErrNodeStateUnknown
	}
	body, err := json.Marshal(req)
	if err != nil {
		return VerifyTerminatedResult{}, err
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(baseURL, "/")+"/internal/v1/node-gpu/verify-terminated", bytes.NewReader(body))
	if err != nil {
		return VerifyTerminatedResult{}, err
	}
	httpReq.Header.Set("Content-Type", "application/json")
	if v.BearerToken != "" {
		httpReq.Header.Set("Authorization", "Bearer "+v.BearerToken)
	}
	client := nonRedirectHTTPClient(v.Client, 8*time.Second)
	resp, err := client.Do(httpReq)
	if err != nil {
		return VerifyTerminatedResult{ErrorCode: "UNKNOWN_NODE_STATE"}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 && resp.StatusCode < 400 {
		return VerifyTerminatedResult{ErrorCode: "UNKNOWN_NODE_STATE"}, ErrNodeStateUnknown
	}
	var out VerifyTerminatedResult
	if err := DecodeJSONObject(resp.Body, 1<<20, &out); err != nil {
		return VerifyTerminatedResult{ErrorCode: "UNKNOWN_NODE_STATE"}, err
	}
	if resp.StatusCode >= 500 {
		if out.ErrorCode == "" {
			out.ErrorCode = "UNKNOWN_NODE_STATE"
		}
		return out, ErrNodeStateUnknown
	}
	return out, nil
}

type HTTPResidencyEvictor struct {
	Client                 *http.Client
	BearerToken            string
	TokenResolver          func(ctx context.Context, instanceID string) (string, error)
	AllowLoopbackCallbacks bool
}

func (e HTTPResidencyEvictor) resolveBearerToken(ctx context.Context, instanceID string) (string, error) {
	inst := strings.TrimSpace(instanceID)
	if e.TokenResolver != nil && inst != "" {
		tok, err := e.TokenResolver(ctx, inst)
		if err != nil {
			return "", err
		}
		if tok != "" {
			return tok, nil
		}
	}
	return e.BearerToken, nil
}

// EvictVRAM enforces P1-H4 (no-redirect + endpoint validation) and P1-H8 (canonical /internal/v1/gpu-residency/evict-vram wire path & DTO).
func (e HTTPResidencyEvictor) EvictVRAM(ctx context.Context, cmd EvictVRAMCommand) (EvictVRAMResult, error) {
	cleanBase, err := validateInternalCallbackEndpoint(cmd.CallbackEndpoint, e.AllowLoopbackCallbacks)
	if err != nil || cleanBase == "" {
		return EvictVRAMResult{}, ErrInvalidRequest
	}
	tok, err := e.resolveBearerToken(ctx, cmd.InstanceID)
	if err != nil {
		return EvictVRAMResult{}, err
	}
	body, err := json.Marshal(cmd)
	if err != nil {
		return EvictVRAMResult{}, err
	}
	targetURL := cleanBase + "/internal/v1/gpu-residency/evict-vram"
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, targetURL, bytes.NewReader(body))
	if err != nil {
		return EvictVRAMResult{}, err
	}
	httpReq.Header.Set("Content-Type", "application/json")
	if tok != "" {
		httpReq.Header.Set("Authorization", "Bearer "+tok)
	}
	client := nonRedirectHTTPClient(e.Client, 15*time.Second)
	resp, err := client.Do(httpReq)
	if err != nil {
		return EvictVRAMResult{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 && resp.StatusCode < 400 {
		return EvictVRAMResult{}, fmt.Errorf("evict-vram redirect forbidden (%d)", resp.StatusCode)
	}
	var out EvictVRAMResult
	decErr := json.NewDecoder(resp.Body).Decode(&out)
	out = normalizeEvictVRAMResult(out)
	if resp.StatusCode == http.StatusConflict {
		out.Conflict = true
		return out, nil
	}
	if resp.StatusCode == http.StatusServiceUnavailable {
		out.Unreclaimed = true
		return out, nil
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return out, fmt.Errorf("evict-vram status %d", resp.StatusCode)
	}
	if decErr != nil {
		return EvictVRAMResult{}, fmt.Errorf("evict-vram malformed response: %w", decErr)
	}
	return out, nil
}

// RevokeLease enforces P1-H4 & P1-H8: posts to /internal/v1/gpu-residency/revoke-lease,
// forbids HTTP redirects, and requires explicit revoked=true && settled=true in body (never treats bare 2xx as Revoked).
func (e HTTPResidencyEvictor) RevokeLease(ctx context.Context, cmd RevokeLeaseCommand) (RevokeLeaseResult, error) {
	cleanBase, err := validateInternalCallbackEndpoint(cmd.CallbackEndpoint, e.AllowLoopbackCallbacks)
	if err != nil || cleanBase == "" {
		return RevokeLeaseResult{}, ErrInvalidRequest
	}
	tok, err := e.resolveBearerToken(ctx, cmd.InstanceID)
	if err != nil {
		return RevokeLeaseResult{}, err
	}
	body, err := json.Marshal(cmd)
	if err != nil {
		return RevokeLeaseResult{}, err
	}
	targetURL := cleanBase + "/internal/v1/gpu-residency/revoke-lease"
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, targetURL, bytes.NewReader(body))
	if err != nil {
		return RevokeLeaseResult{}, err
	}
	httpReq.Header.Set("Content-Type", "application/json")
	if tok != "" {
		httpReq.Header.Set("Authorization", "Bearer "+tok)
	}
	client := nonRedirectHTTPClient(e.Client, 10*time.Second)
	resp, err := client.Do(httpReq)
	if err != nil {
		return RevokeLeaseResult{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 && resp.StatusCode < 400 {
		return RevokeLeaseResult{}, fmt.Errorf("revoke-lease redirect forbidden (%d)", resp.StatusCode)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return RevokeLeaseResult{}, fmt.Errorf("revoke-lease status %d", resp.StatusCode)
	}
	var out RevokeLeaseResult
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return RevokeLeaseResult{}, fmt.Errorf("revoke-lease malformed response: %w", err)
	}
	if !out.Revoked || !out.Settled {
		return out, fmt.Errorf("revoke-lease not settled (revoked=%v settled=%v)", out.Revoked, out.Settled)
	}
	return out, nil
}

// A live Pod on ANY card vetoes reclamation. Telemetry-only absence requires
// every card on the node to have a current sample, not merely an old timestamp.
func residencyPodAbsent(cards []cardLockState, r residencyRow, now time.Time, maxAge time.Duration) (absent, live bool) {
	seen, fresh := false, true
	for _, c := range cards {
		if c.NodeUID != r.NodeUID {
			continue
		}
		seen = true
		if c.TelemetrySampledAt == nil || now.Sub(*c.TelemetrySampledAt) > maxAge {
			fresh = false
		}
		for _, p := range c.ActiveGPUPods {
			if normPodUID(p.PodUID) == normPodUID(r.PodUID) && r.PodUID != "" {
				return false, true
			}
		}
	}
	return seen && fresh && r.PodUID != "", false
}
