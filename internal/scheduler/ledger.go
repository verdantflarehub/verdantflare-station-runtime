package scheduler

import (
	"context"
	"encoding/json"
	"errors"
	"hash/fnv"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

type Service struct {
	Pool                   *pgxpool.Pool
	StationID              string
	Callers                CallerRegistry
	AppGPUQuota            int
	MaxTelemetryAge        time.Duration
	CutoverCompleted       bool
	AllowLoopbackCallbacks bool
	InstanceDir            InstanceDirectory
	Verifier               NodeVerifier
	Evictor                ResidencyEvictor
	Hook                   StageHook
	Now                    func() time.Time
	residencyIdemStripes   [64]sync.Mutex
	drainingInFlight       sync.Map
}

func (s *Service) lockResidencyIdempotency(scope, key string) func() {
	if s == nil {
		return func() {}
	}
	h := fnv.New32a()
	_, _ = h.Write([]byte(scope))
	_, _ = h.Write([]byte{0})
	_, _ = h.Write([]byte(key))
	idx := h.Sum32() % uint32(len(s.residencyIdemStripes))
	s.residencyIdemStripes[idx].Lock()
	return s.residencyIdemStripes[idx].Unlock
}

func (s *Service) now() time.Time {
	if s != nil && s.Now != nil {
		return s.Now().UTC()
	}
	return time.Now().UTC()
}

func (s *Service) appGPUQuota() int {
	if s != nil && s.AppGPUQuota > 0 {
		return s.AppGPUQuota
	}
	return DefaultAppGPUQuota
}

func (s *Service) maxTelemetryAge() time.Duration {
	if s != nil && s.MaxTelemetryAge > 0 {
		return s.MaxTelemetryAge
	}
	return DefaultMaxTelemetryAge
}

func (s *Service) fireHook(ctx context.Context, point string, meta map[string]string) {
	if s != nil && s.Hook != nil {
		s.Hook.OnStage(ctx, point, meta)
	}
}

func rollbackTx(tx pgx.Tx) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = tx.Rollback(ctx)
}

func normPodUID(s string) string {
	return strings.ToLower(strings.ReplaceAll(strings.TrimSpace(s), "_", "-"))
}

func normContainerID(s string) string {
	v := strings.ToLower(strings.TrimSpace(s))
	for _, pfx := range []string{"containerd://", "docker://", "cri-o://"} {
		v = strings.TrimPrefix(v, pfx)
	}
	return strings.TrimSpace(v)
}

func isHex64ContainerID(cid string) bool {
	if len(cid) != 64 {
		return false
	}
	for i := 0; i < 64; i++ {
		ch := cid[i]
		if !((ch >= '0' && ch <= '9') || (ch >= 'a' && ch <= 'f')) {
			return false
		}
	}
	return true
}

func parsePodScopedContainerID(cid string) (embeddedPodUID, containerHint string, ok bool) {
	clean := normContainerID(cid)
	if !strings.HasPrefix(clean, "pod-") || len(clean) < 42 || clean[40] != '-' {
		return "", "", false
	}
	rawUID := clean[4:40]
	parsed, err := uuid.Parse(rawUID)
	if err != nil || parsed == uuid.Nil {
		return "", "", false
	}
	hint := strings.TrimSpace(clean[41:])
	if hint == "" {
		return "", "", false
	}
	return normPodUID(parsed.String()), hint, true
}

// holderMatchesAttributedProcess reconciles unprivileged cgroupns=private container self-identities
// ("pod-<pod_uid>-<cname>") with host-side cgroup 64-hex CRI container IDs while enforcing fail-closed
// anti-spoofing (embedded <pod_uid> must equal both holderPodUID and apPodUID) and preserving strict
// per-container separation whenever both sides carry concrete 64-hex or pod-scoped IDs.
func holderMatchesAttributedProcess(holderPodUID, holderCID, apPodUID, apCID string) bool {
	hPod := normPodUID(holderPodUID)
	hCID := normContainerID(holderCID)
	aPod := normPodUID(apPodUID)
	aCID := normContainerID(apCID)

	if hCID != "" && aCID != "" {
		if hCID == aCID {
			if hPod != "" && aPod != "" && hPod != aPod {
				return false
			}
			if embPod, _, ok := parsePodScopedContainerID(hCID); ok {
				if (hPod != "" && embPod != hPod) || (aPod != "" && embPod != aPod) {
					return false
				}
			}
			return true
		}
		if embPod, _, ok := parsePodScopedContainerID(hCID); ok && isHex64ContainerID(aCID) {
			if aPod == "" || embPod != aPod || (hPod != "" && embPod != hPod) {
				return false
			}
			return true
		}
		if embPod, _, ok := parsePodScopedContainerID(aCID); ok && isHex64ContainerID(hCID) {
			if hPod == "" || embPod != hPod || (aPod != "" && embPod != aPod) {
				return false
			}
			return true
		}
		return false
	}
	if hPod != "" && aPod != "" {
		if embPod, _, ok := parsePodScopedContainerID(hCID); ok && embPod != hPod {
			return false
		}
		if embPod, _, ok := parsePodScopedContainerID(aCID); ok && embPod != aPod {
			return false
		}
		return hPod == aPod
	}
	return false
}

func sameCanonicalAppID(a, b string) bool {
	ca := strings.TrimPrefix(strings.ToLower(strings.TrimSpace(a)), "app-")
	cb := strings.TrimPrefix(strings.ToLower(strings.TrimSpace(b)), "app-")
	return ca != "" && ca == cb
}

type PGInstanceDirectory struct {
	Pool     *pgxpool.Pool
	AppPorts map[string]int
}

func (d *PGInstanceDirectory) LookupInstance(ctx context.Context, stationID, instanceID string) (RegisteredInstance, bool, error) {
	if d == nil || d.Pool == nil {
		return RegisteredInstance{}, false, errors.New("nil PGInstanceDirectory pool")
	}
	cleanUUID, err := uuid.Parse(strings.TrimSpace(instanceID))
	if err != nil {
		return RegisteredInstance{}, false, nil
	}
	canonInstID := cleanUUID.String()
	compactInstID := strings.ReplaceAll(canonInstID, "-", "")

	var appID, namespace, opID, podUID, secretUID string
	err = d.Pool.QueryRow(ctx, `
		SELECT i.app_id,
		       w.namespace,
		       s.operation_id::text,
		       COALESCE(s.observation->>'pod_uid', ''),
		       COALESCE(s.observation->>'secret_uid', '')
		  FROM station.app_instance_workspaces w
		  JOIN station.app_installations i ON i.installation_id = w.installation_id
		  JOIN LATERAL (
		       SELECT st.operation_id, st.phase, st.observation
		         FROM station.app_instance_starts st
		        WHERE st.instance_id = w.instance_id
		        ORDER BY st.generation DESC
		        LIMIT 1
		  ) s ON s.phase <> 'stopped'
		 WHERE w.station_id = $1 AND w.instance_id = $2
	`, stationID, canonInstID).Scan(&appID, &namespace, &opID, &podUID, &secretUID)
	if err == nil {
		if strings.TrimSpace(podUID) == "" || strings.TrimSpace(namespace) == "" || strings.TrimSpace(opID) == "" {
			return RegisteredInstance{}, false, nil
		}
		port := 8188
		if d.AppPorts != nil {
			if p := d.AppPorts[appID]; p > 0 {
				port = p
			}
		}
		compactOp := strings.ReplaceAll(strings.TrimSpace(opID), "-", "")
		cb := "http://appr-" + compactOp + "." + strings.TrimSpace(namespace) + ".svc.cluster.local:" + strconvItoa(port)
		return RegisteredInstance{
			InstanceID:       canonInstID,
			AppID:            appID,
			PodUID:           podUID,
			CallbackEndpoint: cb,
			SecretNamespace:  strings.TrimSpace(namespace),
			SecretName:       "appi-" + compactInstID,
			SecretUID:        strings.TrimSpace(secretUID),
		}, true, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return RegisteredInstance{}, false, err
	}

	var profileID, rtNamespace, rtPodUID, rtCtrlEndpoint, rtSecretUID string
	err = d.Pool.QueryRow(ctx, `
		SELECT w.profile_id,
		       w.namespace,
		       COALESCE(ex.state->'worker'->>'pod_uid', ex.state->>'pod_uid', ''),
		       COALESCE(ex.state->'worker'->>'control_endpoint', ex.state->>'control_endpoint', ''),
		       COALESCE(ex.state->>'secret_uid', '')
		  FROM station.runtime_instance_workspaces w
		  JOIN LATERAL (
		       SELECT e.action, e.status, e.phase, e.state
		         FROM station.runtime_instance_executions e
		        WHERE e.station_id = w.station_id
		          AND e.instance_id = w.instance_id
		          AND e.action <> 'create'
		        ORDER BY e.created_at DESC, e.operation_id DESC
		        LIMIT 1
		  ) ex ON ex.action = 'start'
		      AND ((ex.status = 'running' AND ex.phase IN ('launching', 'loading', 'recovering'))
		        OR (ex.status = 'succeeded' AND ex.phase = 'running'))
		 WHERE w.station_id = $1 AND w.instance_id = $2 AND w.retained_at IS NULL
	`, stationID, canonInstID).Scan(&profileID, &rtNamespace, &rtPodUID, &rtCtrlEndpoint, &rtSecretUID)
	if errors.Is(err, pgx.ErrNoRows) {
		return RegisteredInstance{}, false, nil
	}
	if err != nil {
		return RegisteredInstance{}, false, err
	}
	if strings.TrimSpace(rtPodUID) == "" || strings.TrimSpace(rtCtrlEndpoint) == "" {
		return RegisteredInstance{}, false, nil
	}
	resolvedApp := "blender"
	if strings.Contains(strings.ToLower(profileID), "comfy") {
		resolvedApp = "comfyui"
	}
	return RegisteredInstance{
		InstanceID:       canonInstID,
		AppID:            resolvedApp,
		PodUID:           rtPodUID,
		CallbackEndpoint: strings.TrimSpace(rtCtrlEndpoint),
		SecretNamespace:  strings.TrimSpace(rtNamespace),
		SecretName:       "blender-" + compactInstID,
		SecretUID:        strings.TrimSpace(rtSecretUID),
	}, true, nil
}

type KubeInstanceTokenResolver struct {
	Directory InstanceDirectory
	StationID string
	Kube      kubernetes.Interface
}

func (r KubeInstanceTokenResolver) ResolveToken(ctx context.Context, instanceID string) (string, error) {
	if r.Directory == nil || r.Kube == nil || strings.TrimSpace(instanceID) == "" {
		return "", nil
	}
	inst, found, err := r.Directory.LookupInstance(ctx, r.StationID, instanceID)
	if err != nil {
		return "", err
	}
	if !found || inst.SecretNamespace == "" || inst.SecretName == "" {
		return "", ErrForbiddenCaller
	}
	sec, err := r.Kube.CoreV1().Secrets(inst.SecretNamespace).Get(ctx, inst.SecretName, metav1.GetOptions{})
	if err != nil {
		return "", err
	}
	if inst.SecretUID != "" && string(sec.UID) != inst.SecretUID {
		return "", ErrForbiddenCaller
	}
	tok := strings.TrimSpace(string(sec.Data["worker-token"]))
	if len(tok) < 32 {
		return "", ErrForbiddenCaller
	}
	return tok, nil
}

func strconvItoa(v int) string {
	if v == 0 {
		return "0"
	}
	var buf [20]byte
	i := len(buf)
	for v > 0 {
		i--
		buf[i] = byte('0' + v%10)
		v /= 10
	}
	return string(buf[i:])
}

type residencyRow struct {
	InstanceID         string
	AppID              string
	NodeUID            string
	PodUID             string
	ContainerName      string
	ContainerID        string
	BootID             string
	GPUEpoch           int64
	ResidencySeq       int64
	GPUUUID            string
	ResidencyState     string
	HumanProtected     bool
	GUIConnectionAlive bool
	Dirty              bool
	SnapshotInProgress bool
	SnapshotRequired   bool
	BlockingReason     string
	PinnedBudgetMiB    int64
	VRAMAllocatedMiB   *int64
	CallbackEndpoint   string
	TemplateUpgraded   bool
}

func (r residencyRow) isEvictable() bool {
	return r.ResidencyState == "idle_evictable" &&
		!r.HumanProtected &&
		!r.GUIConnectionAlive &&
		!r.SnapshotInProgress &&
		(r.BlockingReason == "" || r.BlockingReason == "none")
}

func (r residencyRow) isHumanProtected() bool {
	return r.HumanProtected || r.GUIConnectionAlive ||
		r.ResidencyState == "human_protected" || r.ResidencyState == "human_dirty"
}

func (r residencyRow) blocksUnupgradedCutover() bool {
	if r.TemplateUpgraded {
		return false
	}
	return r.isHumanProtected() || r.SnapshotInProgress ||
		r.ResidencyState == "active_compute" || r.ResidencyState == "waking" ||
		r.ResidencyState == "sealing" || r.ResidencyState == "draining_hard" ||
		r.ResidencyState == "reconciling" || r.ResidencyState == "quarantined" ||
		(r.Dirty && (r.SnapshotRequired || r.ResidencyState != "zero_vram_standby"))
}

type activeLeaseRow struct {
	LeaseID           string
	HolderAppID       string
	HolderTaskID      string
	AttemptID         string
	StageID           string
	NodeUID           string
	PodUID            string
	ContainerID       string
	BootID            string
	IdempotencyKey    string
	RequestSHA256     string
	CandidateGPUUUIDs []string
	GPUUUIDs          []string
	ReservedGPUUUIDs  []string
	CardCount         int
	K8sSlotsRequired  int
	PeakBudgetMiB     int64
	StartupFreeMinMiB int64
	HeadroomOverride  *int64
	FencingToken      int64
	Priority          string
	BypassCount       int
	State             string
	BlockingReason    string
	BlockingHolderID  string
	ReclaimProof      map[string]any
	ResponseCache     map[string]any
	ExpiresAt         time.Time
	Version           int64
	CreatedAt         time.Time
	UpdatedAt         time.Time
}

func lockCardsForUpdate(ctx context.Context, tx pgx.Tx, stationID, nodeUID string) ([]cardLockState, error) {
	return loadCards(ctx, tx, stationID, nodeUID, true)
}

func loadCards(ctx context.Context, tx pgx.Tx, stationID, nodeUID string, lock bool) ([]cardLockState, error) {
	suffix := ""
	if lock {
		suffix = " FOR UPDATE"
	}
	rows, err := tx.Query(ctx, `
		SELECT station_id::text, node_uid, node_name, gpu_uuid, gpu_index,
		       total_vram_mib, driver_overhead_mib, measured_free_vram_mib, measured_used_vram_mib,
		       telemetry_boot_id, telemetry_sample_id, telemetry_sampled_at, attributed_processes,
		       fencing_epoch, min_fencing_token, reserved_by_lease_id::text,
		       quarantined_mib, time_slicing_slots, allocated_k8s_slots, scaling_up_k8s_slots,
		       unscheduled_pending_k8s_slots, active_gpu_pods,
		       admission_mode, last_event_seq, last_event_hash, state, state_reason
		  FROM station.gpu_physical_cards
		 WHERE station_id = $1 AND ($2 = '' OR node_uid = $2)
		 ORDER BY node_uid ASC, gpu_uuid ASC
	`+suffix, stationID, nodeUID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var cards []cardLockState
	for rows.Next() {
		var c cardLockState
		var attrRaw, podsRaw []byte
		if err := rows.Scan(
			&c.StationID, &c.NodeUID, &c.NodeName, &c.GPUUUID, &c.GPUIndex,
			&c.TotalVRAMMiB, &c.DriverOverheadMiB, &c.MeasuredFreeVRAMMiB, &c.MeasuredUsedVRAMMiB,
			&c.TelemetryBootID, &c.TelemetrySampleID, &c.TelemetrySampledAt, &attrRaw,
			&c.FencingEpoch, &c.MinFencingToken, &c.ReservedByLeaseID,
			&c.QuarantinedMiB, &c.TimeSlicingSlots, &c.AllocatedK8sSlots, &c.ScalingUpK8sSlots,
			&c.UnscheduledPendingK8sSlots, &podsRaw,
			&c.AdmissionMode, &c.LastEventSeq, &c.LastEventHash, &c.State, &c.StateReason,
		); err != nil {
			return nil, err
		}
		if len(attrRaw) > 0 {
			_ = json.Unmarshal(attrRaw, &c.AttributedProcesses)
		}
		if len(podsRaw) > 0 {
			_ = json.Unmarshal(podsRaw, &c.ActiveGPUPods)
		}
		cards = append(cards, c)
	}
	return cards, rows.Err()
}

func loadActiveLeases(ctx context.Context, tx pgx.Tx, stationID, nodeUID string) ([]activeLeaseRow, error) {
	rows, err := tx.Query(ctx, `
		SELECT lease_id::text, holder_app_id, holder_task_id, attempt_id, stage_id,
		       node_uid, pod_uid, container_id, boot_id, idempotency_key, request_sha256,
		       candidate_gpu_uuids, gpu_uuids, reserved_gpu_uuids, card_count, k8s_slots_required,
		       peak_budget_mib, startup_free_min_mib, headroom_override_mib, fencing_token,
		       priority, bypass_count, state, blocking_reason, blocking_holder_id,
		       reclaim_proof, response_cache, expires_at, version, created_at, updated_at
		  FROM station.gpu_resource_leases
		 WHERE station_id = $1 AND ($2 = '' OR node_uid = $2)
		   AND state NOT IN ('released', 'expired')
		 ORDER BY created_at ASC, lease_id ASC
		   FOR UPDATE
	`, stationID, nodeUID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []activeLeaseRow
	for rows.Next() {
		var r activeLeaseRow
		var proofRaw, cacheRaw []byte
		if err := rows.Scan(
			&r.LeaseID, &r.HolderAppID, &r.HolderTaskID, &r.AttemptID, &r.StageID,
			&r.NodeUID, &r.PodUID, &r.ContainerID, &r.BootID, &r.IdempotencyKey, &r.RequestSHA256,
			&r.CandidateGPUUUIDs, &r.GPUUUIDs, &r.ReservedGPUUUIDs, &r.CardCount, &r.K8sSlotsRequired,
			&r.PeakBudgetMiB, &r.StartupFreeMinMiB, &r.HeadroomOverride, &r.FencingToken,
			&r.Priority, &r.BypassCount, &r.State, &r.BlockingReason, &r.BlockingHolderID,
			&proofRaw, &cacheRaw, &r.ExpiresAt, &r.Version, &r.CreatedAt, &r.UpdatedAt,
		); err != nil {
			return nil, err
		}
		r.ReclaimProof = map[string]any{}
		if len(proofRaw) > 0 {
			_ = json.Unmarshal(proofRaw, &r.ReclaimProof)
		}
		r.ResponseCache = map[string]any{}
		if len(cacheRaw) > 0 {
			_ = json.Unmarshal(cacheRaw, &r.ResponseCache)
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

func loadResidencies(ctx context.Context, tx pgx.Tx, stationID, nodeUID string) ([]residencyRow, error) {
	rows, err := tx.Query(ctx, `
		SELECT instance_id, app_id, node_uid, pod_uid, container_name, container_id,
		       boot_id, gpu_epoch, residency_seq, gpu_uuid, residency_state,
		       human_protected, gui_connection_alive, dirty, snapshot_in_progress, snapshot_required,
		       blocking_reason, pinned_budget_mib, vram_allocated_mib, callback_endpoint, template_upgraded
		  FROM station.gpu_instance_residency
		 WHERE station_id = $1 AND ($2 = '' OR node_uid = $2)
		 ORDER BY instance_id ASC
		   FOR UPDATE
	`, stationID, nodeUID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []residencyRow
	for rows.Next() {
		var r residencyRow
		if err := rows.Scan(
			&r.InstanceID, &r.AppID, &r.NodeUID, &r.PodUID, &r.ContainerName, &r.ContainerID,
			&r.BootID, &r.GPUEpoch, &r.ResidencySeq, &r.GPUUUID, &r.ResidencyState,
			&r.HumanProtected, &r.GUIConnectionAlive, &r.Dirty, &r.SnapshotInProgress, &r.SnapshotRequired,
			&r.BlockingReason, &r.PinnedBudgetMiB, &r.VRAMAllocatedMiB, &r.CallbackEndpoint, &r.TemplateUpgraded,
		); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

type cardAccounting struct {
	PinnedMiB               int64
	LeasedMiB               int64
	EvictableMiB            int64
	ProvenActiveMiB         int64
	QuarantinedProvenMiB    int64
	UnmaterializedBudgetMiB int64
	UnattributedExternalMiB int64
	HumanHolderID           string
	UnupgradedProtectedID   string
	ActiveHoldersCount      int
	EvictTargets            []residencyRow
}

// computeCardAccounting enforces P1-H1 & P1-H4:
//   - Only node-proven process telemetry (card.AttributedProcesses) counts toward holder actual VRAM a_i
//     or evictable VRAM; self-reported residency VRAMAllocatedMiB can NEVER mask external VRAM or inflate EvictableMiB.
//   - Unmaterialized lease headroom sum_i max(0, b_i - a_i) is explicitly reserved in G_now and never masks UnattributedExternalMiB.
func computeCardAccounting(card cardLockState, leases []activeLeaseRow, residencies []residencyRow, excludeLeaseID string) cardAccounting {
	var acc cardAccounting
	claimedProcIdx := make([]bool, len(card.AttributedProcesses))
	claimHolderVRAM := func(podUID, containerID string) int64 {
		pUID := normPodUID(podUID)
		cID := normContainerID(containerID)
		if pUID == "" && cID == "" {
			return 0
		}
		var sum int64
		for idx, ap := range card.AttributedProcesses {
			if claimedProcIdx[idx] {
				continue
			}
			apPod := normPodUID(ap.PodUID)
			apCID := normContainerID(ap.ContainerID)
			matched := holderMatchesAttributedProcess(pUID, cID, apPod, apCID)
			if matched {
				claimedProcIdx[idx] = true
				if ap.UsedVRAMMiB > 0 {
					sum += ap.UsedVRAMMiB
				}
			}
		}
		return sum
	}

	leasedPods := map[string]bool{}
	leasedCIDs := map[string]bool{}
	for _, l := range leases {
		if l.LeaseID == excludeLeaseID {
			continue
		}
		if l.State == "queued" || l.State == "draining" {
			continue
		}
		if slices.Contains(l.GPUUUIDs, card.GPUUUID) {
			actual := claimHolderVRAM(l.PodUID, l.ContainerID)
			budget := l.PeakBudgetMiB
			effective := budget
			if actual > effective {
				effective = actual
			}
			unmat := int64(0)
			if budget > actual {
				unmat = budget - actual
			}
			acc.LeasedMiB += effective
			acc.ProvenActiveMiB += actual
			acc.UnmaterializedBudgetMiB += unmat
			acc.ActiveHoldersCount++
			if pUID := normPodUID(l.PodUID); pUID != "" {
				leasedPods[pUID] = true
			}
			if cID := normContainerID(l.ContainerID); cID != "" {
				leasedCIDs[cID] = true
			}
		}
	}

	for _, r := range residencies {
		if r.NodeUID != card.NodeUID || r.GPUUUID != card.GPUUUID {
			continue
		}
		if r.blocksUnupgradedCutover() && acc.UnupgradedProtectedID == "" {
			acc.UnupgradedProtectedID = r.InstanceID
		}
		alreadyLeased := (r.PodUID != "" && leasedPods[normPodUID(r.PodUID)]) ||
			(r.ContainerID != "" && leasedCIDs[normContainerID(r.ContainerID)])

		if r.isHumanProtected() {
			if acc.HumanHolderID == "" {
				acc.HumanHolderID = r.InstanceID
			}
			if alreadyLeased {
				continue
			}
			actual := claimHolderVRAM(r.PodUID, r.ContainerID)
			budget := r.PinnedBudgetMiB
			if budget <= 0 {
				budget = 4096
			}
			effective := budget
			if actual > effective {
				effective = actual
			}
			unmat := int64(0)
			if budget > actual {
				unmat = budget - actual
			}
			acc.PinnedMiB += effective
			acc.ProvenActiveMiB += actual
			acc.UnmaterializedBudgetMiB += unmat
			acc.ActiveHoldersCount++
			continue
		}

		if r.isEvictable() {
			evictProven := claimHolderVRAM(r.PodUID, r.ContainerID)
			acc.EvictableMiB += evictProven
			acc.EvictTargets = append(acc.EvictTargets, r)
			continue
		}

		if r.ResidencyState == "quarantined" {
			acc.QuarantinedProvenMiB += claimHolderVRAM(r.PodUID, r.ContainerID)
			continue
		}

		if r.ResidencyState == "active_compute" || r.ResidencyState == "waking" ||
			r.ResidencyState == "sealing" || r.ResidencyState == "draining_hard" ||
			r.ResidencyState == "reconciling" {
			if alreadyLeased {
				continue
			}
			actual := claimHolderVRAM(r.PodUID, r.ContainerID)
			budget := r.PinnedBudgetMiB
			if budget <= 0 {
				budget = 6144
			}
			effective := budget
			if actual > effective {
				effective = actual
			}
			unmat := int64(0)
			if budget > actual {
				unmat = budget - actual
			}
			acc.LeasedMiB += effective
			acc.ProvenActiveMiB += actual
			acc.UnmaterializedBudgetMiB += unmat
			acc.ActiveHoldersCount++
		}
	}

	provenUsed := card.DriverOverheadMiB + acc.ProvenActiveMiB + acc.EvictableMiB + acc.QuarantinedProvenMiB
	if card.MeasuredUsedVRAMMiB > provenUsed {
		acc.UnattributedExternalMiB = card.MeasuredUsedVRAMMiB - provenUsed
	}
	return acc
}

func effectiveHeadroom(card cardLockState, acc cardAccounting, reqPeak int64, reqOverride *int64, reqPriority string) int64 {
	if reqOverride != nil && *reqOverride >= 0 && reqPriority == PriorityExclusiveBatch && acc.ActiveHoldersCount == 0 {
		return *reqOverride
	}
	if reqPriority == PriorityExclusiveBatch && reqPeak >= 28672 && acc.ActiveHoldersCount == 0 {
		return ExclusiveCardHeadroomMiB
	}
	return DefaultCardHeadroomMiB
}

type cardFitResult struct {
	ImmediateFit   bool
	DrainingFit    bool
	BlockingReason string
	BlockingHolder string
	EvictTargets   []residencyRow
}

func evaluateCardFit(
	card cardLockState,
	acc cardAccounting,
	leasesByID map[string]*activeLeaseRow,
	selfLeaseID string,
	reqPeak, reqStartupMin int64,
	reqHeadroom *int64,
	reqPriority string,
	now time.Time,
	maxTelemetryAge time.Duration,
	bypassTargetID *string,
) cardFitResult {
	if card.AdmissionMode != AdmissionActive {
		return cardFitResult{BlockingReason: "drain_only"}
	}
	if card.State != "ready" {
		reason := card.State
		if card.StateReason != "" {
			reason = card.StateReason
		}
		return cardFitResult{BlockingReason: reason}
	}
	// P1-H7: Block admission when node telemetry is missing or stale.
	if card.TelemetrySampledAt == nil || now.Sub(*card.TelemetrySampledAt) > maxTelemetryAge {
		return cardFitResult{BlockingReason: "telemetry_stale"}
	}
	if acc.UnupgradedProtectedID != "" {
		return cardFitResult{BlockingReason: "unupgraded_protected_instance", BlockingHolder: acc.UnupgradedProtectedID}
	}
	if reqPriority == PriorityExclusiveBatch && acc.HumanHolderID != "" {
		return cardFitResult{BlockingReason: "human_gui_active", BlockingHolder: acc.HumanHolderID}
	}

	// Check reservation barrier & D5-2 bounded bypass (max_bypass = 2).
	if card.ReservedByLeaseID != nil && *card.ReservedByLeaseID != "" && *card.ReservedByLeaseID != selfLeaseID {
		resHolder, exists := leasesByID[*card.ReservedByLeaseID]
		if exists && (resHolder.State == "queued" || resHolder.State == "draining") {
			if resHolder.State == "draining" {
				return cardFitResult{BlockingReason: "draining_peer_barrier", BlockingHolder: resHolder.LeaseID}
			}
			if priorityRank(reqPriority) <= priorityRank(resHolder.Priority) {
				return cardFitResult{BlockingReason: "fifo_reservation_barrier", BlockingHolder: resHolder.LeaseID}
			}
			if resHolder.BypassCount >= MaxBypassCount {
				return cardFitResult{BlockingReason: "reservation_barrier_locked", BlockingHolder: resHolder.LeaseID}
			}
			if bypassTargetID != nil {
				*bypassTargetID = resHolder.LeaseID
			}
		}
	}

	hdr := effectiveHeadroom(card, acc, reqPeak, reqHeadroom, reqPriority)
	unmatQuarantine := int64(0)
	if card.QuarantinedMiB > acc.QuarantinedProvenMiB {
		unmatQuarantine = card.QuarantinedMiB - acc.QuarantinedProvenMiB
	}
	committedUsed := card.DriverOverheadMiB + acc.PinnedMiB + acc.LeasedMiB + card.QuarantinedMiB + acc.UnattributedExternalMiB
	ledgerGrantable := card.TotalVRAMMiB - committedUsed - hdr
	// G_now = measured_free - headroom - sum_i max(0, b_i - a_i) - unmatQuarantine
	measuredGrantable := card.MeasuredFreeVRAMMiB - hdr - acc.UnmaterializedBudgetMiB - unmatQuarantine
	effectiveStartupFree := card.MeasuredFreeVRAMMiB - acc.UnmaterializedBudgetMiB - unmatQuarantine

	if ledgerGrantable >= reqPeak && measuredGrantable >= reqPeak && effectiveStartupFree >= reqStartupMin {
		return cardFitResult{ImmediateFit: true}
	}

	// Check whether evicting S2 idle_evictable instances on this card suffices (only when not bypassing another holder's reservation).
	if (card.ReservedByLeaseID == nil || *card.ReservedByLeaseID == "" || *card.ReservedByLeaseID == selfLeaseID) && len(acc.EvictTargets) > 0 && acc.EvictableMiB > 0 {
		freeAfterEvict := card.MeasuredFreeVRAMMiB + acc.EvictableMiB
		grantableAfterEvict := freeAfterEvict - hdr - acc.UnmaterializedBudgetMiB - unmatQuarantine
		startupAfterEvict := freeAfterEvict - acc.UnmaterializedBudgetMiB - unmatQuarantine
		if ledgerGrantable >= reqPeak && grantableAfterEvict >= reqPeak && startupAfterEvict >= reqStartupMin {
			return cardFitResult{
				DrainingFit:    true,
				BlockingReason: "draining_evictable_peer",
				EvictTargets:   acc.EvictTargets,
			}
		}
	}

	if acc.HumanHolderID != "" {
		return cardFitResult{BlockingReason: "human_gui_active", BlockingHolder: acc.HumanHolderID}
	}
	return cardFitResult{BlockingReason: "insufficient_vram"}
}

func normalizeAcquireRequest(caller CallerIdentity, req AcquireRequest) (AcquireRequest, string, error) {
	req.IdempotencyKey = strings.TrimSpace(req.IdempotencyKey)
	req.AttemptID = strings.TrimSpace(req.AttemptID)
	if req.IdempotencyKey == "" || len(req.IdempotencyKey) > 256 || req.AttemptID == "" || len(req.AttemptID) > 256 {
		return req, "", ErrInvalidRequest
	}
	if req.HolderAppID != "" && req.HolderAppID != caller.AppID {
		return req, "", ErrForbiddenCaller
	}
	req.HolderAppID = caller.AppID
	if req.TaskID == "" {
		req.TaskID = req.InstanceID
	}
	if req.TaskID == "" {
		req.TaskID = req.AttemptID
	}
	if req.StageID == "" {
		req.StageID = "primary"
	}
	if spec, ok := DefaultConsumerSpec(req.ConsumerKind); ok {
		if req.CardCount == 0 {
			req.CardCount = spec.CardCount
		}
		if req.PeakBudgetMiB == 0 {
			req.PeakBudgetMiB = spec.PeakBudgetMiB
		}
		if req.StartupFreeMinMiB == 0 {
			req.StartupFreeMinMiB = spec.StartupFreeMinMiB
		}
		if req.HeadroomOverrideMiB == nil && spec.HeadroomOverrideMiB != nil {
			v := *spec.HeadroomOverrideMiB
			req.HeadroomOverrideMiB = &v
		}
		if req.PriorityClass == "" {
			req.PriorityClass = spec.Priority
		}
	}
	if req.CardCount == 0 {
		req.CardCount = 1
	}
	if req.CardCount != 1 && req.CardCount != 2 {
		return req, "", ErrInvalidRequest
	}
	if req.PeakBudgetMiB <= 0 || req.StartupFreeMinMiB < 0 || req.K8sSlotsRequired < 0 {
		return req, "", ErrInvalidRequest
	}
	if req.PriorityClass == "" {
		req.PriorityClass = PrioritySharedSlice
	}
	if priorityRank(req.PriorityClass) == 0 {
		return req, "", ErrInvalidRequest
	}
	if req.TTLSeconds <= 0 {
		req.TTLSeconds = DefaultLeaseTTLSeconds
	}
	if req.CandidateGPUUUIDs == nil {
		req.CandidateGPUUUIDs = []string{}
	}
	computedHash := computeCanonicalHash(
		req.HolderAppID, req.ConsumerKind, req.TaskID, req.AttemptID, req.StageID,
		req.NodeUID, req.PodUID, req.ContainerID, req.BootID,
		req.CandidateGPUUUIDs, req.CardCount, req.K8sSlotsRequired,
		req.PeakBudgetMiB, req.StartupFreeMinMiB, req.HeadroomOverrideMiB,
		req.PriorityClass, req.SourceLeaseID,
	)
	// P1-M1: Always verify caller-supplied request_sha256 against canonical request digest!
	if req.RequestSHA256 != "" {
		if !sha256HexPattern.MatchString(req.RequestSHA256) || req.RequestSHA256 != computedHash {
			return req, "", ErrInvalidRequest
		}
	} else {
		req.RequestSHA256 = computedHash
	}
	return req, req.RequestSHA256, nil
}

func decisionFromRow(r activeLeaseRow, queuePos int, ledgerSeq int64) LeaseDecision {
	firstGPU := ""
	if len(r.GPUUUIDs) > 0 {
		firstGPU = r.GPUUUIDs[0]
	}
	return LeaseDecision{
		// Assigned GPUs are written only on grant and retained through expiry.
		// Candidate Pod identity and queue fencing tokens do not prove a grant.
		NeverGranted:      len(r.GPUUUIDs) == 0 && (r.State == "queued" || r.State == "draining" || r.State == "expired"),
		LeaseID:           r.LeaseID,
		State:             r.State,
		HolderAppID:       r.HolderAppID,
		HolderTaskID:      r.HolderTaskID,
		AttemptID:         r.AttemptID,
		StageID:           r.StageID,
		NodeUID:           r.NodeUID,
		PodUID:            r.PodUID,
		ContainerID:       r.ContainerID,
		BootID:            r.BootID,
		GPUUUID:           firstGPU,
		GPUUUIDs:          r.GPUUUIDs,
		ReservedGPUUUIDs:  r.ReservedGPUUUIDs,
		PeakBudgetMiB:     r.PeakBudgetMiB,
		StartupFreeMinMiB: r.StartupFreeMinMiB,
		FencingToken:      r.FencingToken,
		Priority:          r.Priority,
		QueuePosition:     queuePos,
		BlockingReason:    r.BlockingReason,
		BlockingHolderID:  r.BlockingHolderID,
		ExpiresAt:         r.ExpiresAt.UTC().Format(time.RFC3339Nano),
		Version:           r.Version,
		LedgerEventSeq:    ledgerSeq,
	}
}

func verifyWriteOnceIdentity(curPod, curCID, curBoot, reqPod, reqCID, reqBoot string) error {
	if curPod != "" && reqPod != "" && normPodUID(curPod) != normPodUID(reqPod) {
		return ErrForbiddenCaller
	}
	if curCID != "" && reqCID != "" && normContainerID(curCID) != normContainerID(reqCID) {
		return ErrForbiddenCaller
	}
	if curBoot != "" && reqBoot != "" && strings.TrimSpace(curBoot) != strings.TrimSpace(reqBoot) {
		return ErrForbiddenCaller
	}
	return nil
}

func (s *Service) Acquire(ctx context.Context, caller CallerIdentity, rawReq AcquireRequest) (LeaseDecision, error) {
	return s.acquireInternal(ctx, caller, rawReq, false)
}

func (s *Service) PromoteQueuedLeases(ctx context.Context, nodeUID string) {
	if s == nil || s.Pool == nil {
		return
	}
	now := s.now()
	rows, err := s.Pool.Query(ctx, `
		SELECT lease_id::text, holder_app_id, holder_task_id, attempt_id, stage_id,
		       node_uid, pod_uid, container_id, boot_id, idempotency_key,
		       candidate_gpu_uuids, card_count, k8s_slots_required,
		       peak_budget_mib, startup_free_min_mib, headroom_override_mib,
		       priority, response_cache
		  FROM station.gpu_resource_leases
		 WHERE station_id = $1
		   AND ($2 = '' OR node_uid = $2)
		   AND state IN ('queued', 'draining')
		   AND expires_at > $3
		 ORDER BY created_at ASC, lease_id ASC
	`, s.StationID, strings.TrimSpace(nodeUID), now)
	if err != nil {
		return
	}
	type queuedCandidate struct {
		caller CallerIdentity
		req    AcquireRequest
	}
	var candidates []queuedCandidate
	for rows.Next() {
		var leaseID, appID, taskID, attemptID, stageID, nUID, podUID, cid, bootID, idemKey, prio string
		var candGPUs []string
		var cardCount, k8sSlots int
		var peakMiB, startupMiB int64
		var hdrOverride *int64
		var cacheRaw []byte
		if sErr := rows.Scan(
			&leaseID, &appID, &taskID, &attemptID, &stageID,
			&nUID, &podUID, &cid, &bootID, &idemKey,
			&candGPUs, &cardCount, &k8sSlots,
			&peakMiB, &startupMiB, &hdrOverride,
			&prio, &cacheRaw,
		); sErr != nil {
			continue
		}
		if _, inflight := s.drainingInFlight.Load(leaseID); inflight {
			continue
		}
		var req AcquireRequest
		fromCache := false
		if len(cacheRaw) > 0 {
			var cacheMap map[string]json.RawMessage
			if json.Unmarshal(cacheRaw, &cacheMap) == nil {
				if rawReqJSON, ok := cacheMap["acquire_req"]; ok && len(rawReqJSON) > 0 {
					if json.Unmarshal(rawReqJSON, &req) == nil && req.IdempotencyKey != "" {
						req.RequestSHA256 = ""
						fromCache = true
					}
				}
			}
		}
		if !fromCache {
			req = AcquireRequest{
				IdempotencyKey:      idemKey,
				HolderAppID:         appID,
				TaskID:              taskID,
				AttemptID:           attemptID,
				StageID:             stageID,
				NodeUID:             nUID,
				PodUID:              podUID,
				ContainerID:         cid,
				BootID:              bootID,
				CandidateGPUUUIDs:   candGPUs,
				CardCount:           cardCount,
				K8sSlotsRequired:    k8sSlots,
				PeakBudgetMiB:       peakMiB,
				StartupFreeMinMiB:   startupMiB,
				HeadroomOverrideMiB: hdrOverride,
				PriorityClass:       prio,
			}
		}
		candidates = append(candidates, queuedCandidate{
			caller: CallerIdentity{AppID: appID, Kind: CallerConsumer},
			req:    req,
		})
	}
	rows.Close()

	for _, cand := range candidates {
		_, _ = s.acquireInternal(ctx, cand.caller, cand.req, true)
	}
}

func (s *Service) acquireInternal(ctx context.Context, caller CallerIdentity, rawReq AcquireRequest, isAutoPromote bool) (LeaseDecision, error) {
	if caller.Kind != CallerConsumer && caller.Kind != CallerAdmin {
		return LeaseDecision{}, ErrForbiddenCaller
	}
	req, reqHash, err := normalizeAcquireRequest(caller, rawReq)
	if err != nil {
		return LeaseDecision{}, err
	}

	var pendingEvictions []EvictVRAMCommand
	var pendingNodeUID string
	var decision LeaseDecision

	if !isAutoPromote {
		s.fireHook(ctx, "before_acquire_tx", map[string]string{
			"app_id":          req.HolderAppID,
			"idempotency_key": req.IdempotencyKey,
			"task_id":         req.TaskID,
			"consumer_kind":   req.ConsumerKind,
		})
	}

	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return LeaseDecision{}, err
	}
	defer rollbackTx(tx)

	cards, err := lockCardsForUpdate(ctx, tx, s.StationID, req.NodeUID)
	if err != nil {
		return LeaseDecision{}, err
	}
	if len(cards) == 0 {
		return LeaseDecision{}, ErrTopologyUnsatisfied
	}
	targetNodeUID := cards[0].NodeUID

	// Check existing idempotency record first (under card lock).
	var existingLeaseID, existingHash, existingState, existingPod, existingCID, existingBoot string
	var existingBlockReason, existingBlockHolder string
	var existingReservedGPUs []string
	var existingExpiresAt time.Time
	err = tx.QueryRow(ctx, `
		SELECT lease_id::text, request_sha256, state, pod_uid, container_id, boot_id,
		       reserved_gpu_uuids, blocking_reason, blocking_holder_id, expires_at
		  FROM station.gpu_resource_leases
		 WHERE station_id = $1 AND holder_app_id = $2 AND idempotency_key = $3
		   FOR UPDATE
	`, s.StationID, req.HolderAppID, req.IdempotencyKey).Scan(
		&existingLeaseID, &existingHash, &existingState, &existingPod, &existingCID, &existingBoot,
		&existingReservedGPUs, &existingBlockReason, &existingBlockHolder, &existingExpiresAt,
	)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return LeaseDecision{}, err
	}
	if isAutoPromote {
		if errors.Is(err, pgx.ErrNoRows) || (existingState != "queued" && existingState != "draining") || !existingExpiresAt.After(s.now()) {
			return LeaseDecision{}, nil
		}
		reqHash = existingHash
	}
	if err == nil {
		if !isAutoPromote && existingHash != reqHash {
			return LeaseDecision{}, ErrIdempotencyConflict
		}
		if err := verifyWriteOnceIdentity(existingPod, existingCID, existingBoot, req.PodUID, req.ContainerID, req.BootID); err != nil {
			return LeaseDecision{}, err
		}
		if existingState != "queued" && existingState != "draining" {
			return s.loadDecisionTx(ctx, tx, existingLeaseID, cards)
		}
	}

	// Enforce P-B acyclic cross-card handoff: if SourceLeaseID is specified and still un-released on a different card, forbid hold-and-wait.
	if req.SourceLeaseID != "" {
		var srcState string
		var srcGPUs []string
		sErr := tx.QueryRow(ctx, `
			SELECT state, gpu_uuids FROM station.gpu_resource_leases
			 WHERE station_id = $1 AND lease_id = $2
		`, s.StationID, req.SourceLeaseID).Scan(&srcState, &srcGPUs)
		if sErr == nil && srcState != "released" && srcState != "expired" {
			if len(req.CandidateGPUUUIDs) > 0 {
				overlap := false
				for _, g := range req.CandidateGPUUUIDs {
					if slices.Contains(srcGPUs, g) {
						overlap = true
						break
					}
				}
				if !overlap {
					return LeaseDecision{}, ErrCrossCardHoldAndWait
				}
			} else {
				return LeaseDecision{}, ErrCrossCardHoldAndWait
			}
		}
	}

	leases, err := loadActiveLeases(ctx, tx, s.StationID, targetNodeUID)
	if err != nil {
		return LeaseDecision{}, err
	}
	residencies, err := loadResidencies(ctx, tx, s.StationID, targetNodeUID)
	if err != nil {
		return LeaseDecision{}, err
	}

	if !isAutoPromote {
		s.fireHook(ctx, "after_card_read_before_commit", map[string]string{
			"app_id":          req.HolderAppID,
			"idempotency_key": req.IdempotencyKey,
			"task_id":         req.TaskID,
		})
	}

	leaseID := existingLeaseID
	isNew := leaseID == ""
	if isNew {
		leaseID = uuid.Must(uuid.NewV7()).String()
	}

	// Filter candidate cards on targetNodeUID (excluding cards permanently unable to satisfy PeakBudgetMiB due to Quarantine).
	var candidateCards []*cardLockState
	for i := range cards {
		if cards[i].NodeUID != targetNodeUID {
			continue
		}
		if len(req.CandidateGPUUUIDs) > 0 && !slices.Contains(req.CandidateGPUUUIDs, cards[i].GPUUUID) {
			continue
		}
		if cards[i].State == "quarantined" {
			continue
		}
		nonQuarantinedCeiling := cards[i].TotalVRAMMiB - cards[i].DriverOverheadMiB - cards[i].QuarantinedMiB
		if nonQuarantinedCeiling < req.PeakBudgetMiB {
			continue
		}
		candidateCards = append(candidateCards, &cards[i])
	}
	if len(candidateCards) < req.CardCount {
		if !isNew {
			if err := clearLeaseReservationsTx(ctx, tx, s.StationID, leaseID, cards, req.RunNonce, "TOPOLOGY_UNSATISFIABLE"); err != nil {
				return LeaseDecision{}, err
			}
			if _, err := tx.Exec(ctx, `
				UPDATE station.gpu_resource_leases
				   SET state = 'released', reserved_gpu_uuids = '{}', blocking_reason = 'topology_unsatisfiable',
				       version = version + 1, updated_at = now()
				 WHERE station_id = $1 AND lease_id = $2
			`, s.StationID, leaseID); err != nil {
				return LeaseDecision{}, err
			}
			if err := tx.Commit(ctx); err != nil {
				return LeaseDecision{}, err
			}
		}
		return LeaseDecision{}, ErrTopologyUnsatisfied
	}

	// P1-M4: Check K8s 12-slot quota gate across the node (including unscheduled Pending GPU Pods
	// and deduplicating active leases whose Pods have already materialized in Kubernetes).
	totalK8sUsed := 0
	materializedPods := map[string]bool{}
	materializedLeases := map[string]bool{}
	materializedInstances := map[string]bool{}
	for _, c := range cards {
		if c.NodeUID == targetNodeUID {
			totalK8sUsed += c.AllocatedK8sSlots + c.ScalingUpK8sSlots + c.UnscheduledPendingK8sSlots
			for _, p := range c.ActiveGPUPods {
				if p.PodUID != "" {
					materializedPods[normPodUID(p.PodUID)] = true
				}
				if p.LeaseID != "" {
					materializedLeases[p.LeaseID] = true
				}
				if p.InstanceID != "" {
					materializedInstances[p.InstanceID] = true
				}
			}
		}
	}
	for _, l := range leases {
		if l.LeaseID == leaseID || l.K8sSlotsRequired <= 0 {
			continue
		}
		if l.State == "leased" || l.State == "running" || l.State == "preparing" {
			if (l.PodUID != "" && materializedPods[normPodUID(l.PodUID)]) ||
				materializedLeases[l.LeaseID] ||
				(l.HolderTaskID != "" && materializedInstances[l.HolderTaskID]) {
				continue
			}
			totalK8sUsed += l.K8sSlotsRequired
		}
	}
	selfSlotsNeeded := req.K8sSlotsRequired
	if selfSlotsNeeded > 0 && ((req.PodUID != "" && materializedPods[normPodUID(req.PodUID)]) ||
		materializedLeases[leaseID] ||
		(req.InstanceID != "" && materializedInstances[req.InstanceID])) {
		selfSlotsNeeded = 0
	}
	quotaBlocked := (req.K8sSlotsRequired > 0 && totalK8sUsed > s.appGPUQuota()) ||
		(selfSlotsNeeded > 0 && totalK8sUsed+selfSlotsNeeded > s.appGPUQuota())

	leasesByID := make(map[string]*activeLeaseRow, len(leases))
	for i := range leases {
		leasesByID[leases[i].LeaseID] = &leases[i]
	}

	now := s.now()
	maxAge := s.maxTelemetryAge()

	// Evaluate per-card fit.
	type candidateEval struct {
		card     *cardLockState
		acc      cardAccounting
		fit      cardFitResult
		bypassID string
	}
	evals := make([]candidateEval, 0, len(candidateCards))
	for _, c := range candidateCards {
		acc := computeCardAccounting(*c, leases, residencies, leaseID)
		var bypassID string
		fit := evaluateCardFit(*c, acc, leasesByID, leaseID, req.PeakBudgetMiB, req.StartupFreeMinMiB, req.HeadroomOverrideMiB, req.PriorityClass, now, maxAge, &bypassID)
		if quotaBlocked {
			fit.ImmediateFit = false
			fit.DrainingFit = false
			fit.BlockingReason = "k8s_gpu_quota_saturated"
		}
		evals = append(evals, candidateEval{card: c, acc: acc, fit: fit, bypassID: bypassID})
	}

	// Check if there is an earlier queued lease with higher/equal priority on the same candidate set that has not yet been admitted (FIFO + D5-2).
	earlierHeadRow := findEarlierBlockingQueueHead(leases, leaseID, req.PriorityClass, candidateCards)
	if earlierHeadRow != nil && !quotaBlocked {
		if priorityRank(req.PriorityClass) <= priorityRank(earlierHeadRow.Priority) {
			for i := range evals {
				evals[i].fit.ImmediateFit = false
				evals[i].fit.DrainingFit = false
				evals[i].fit.BlockingReason = "fifo_queue_ahead"
				evals[i].fit.BlockingHolder = earlierHeadRow.LeaseID
			}
		} else if earlierHeadRow.BypassCount >= MaxBypassCount {
			for i := range evals {
				evals[i].fit.ImmediateFit = false
				evals[i].fit.DrainingFit = false
				evals[i].fit.BlockingReason = "reservation_barrier_locked"
				evals[i].fit.BlockingHolder = earlierHeadRow.LeaseID
			}
		} else {
			for i := range evals {
				if evals[i].bypassID == "" {
					evals[i].bypassID = earlierHeadRow.LeaseID
				}
			}
		}
	}

	// Select immediate fit cards (prefer card with highest effective grantable free VRAM).
	var immediateChosen []candidateEval
	for _, ev := range evals {
		if ev.fit.ImmediateFit {
			immediateChosen = append(immediateChosen, ev)
		}
	}
	sort.SliceStable(immediateChosen, func(i, j int) bool {
		gi := immediateChosen[i].card.MeasuredFreeVRAMMiB - immediateChosen[i].acc.UnmaterializedBudgetMiB
		gj := immediateChosen[j].card.MeasuredFreeVRAMMiB - immediateChosen[j].acc.UnmaterializedBudgetMiB
		if gi != gj {
			return gi > gj
		}
		return immediateChosen[i].card.GPUUUID < immediateChosen[j].card.GPUUUID
	})

	expiresAt := now.Add(time.Duration(req.TTLSeconds) * time.Second)
	corrUUID := uuid.Must(uuid.NewV7()).String()

	if len(immediateChosen) >= req.CardCount {
		selected := immediateChosen[:req.CardCount]
		sort.Slice(selected, func(i, j int) bool { return selected[i].card.GPUUUID < selected[j].card.GPUUUID })

		var maxFence int64
		grantedGPUs := make([]string, 0, req.CardCount)
		bypassedSet := map[string]bool{}
		for _, sel := range selected {
			nextToken := sel.card.MinFencingToken + 1
			if nextToken > maxFence {
				maxFence = nextToken
			}
		}
		// Character swap persists its replay watermark on the model service's shared
		// volume. Its next grant must advance past prior grants on either 5090.
		// All node cards are locked above. Only selected cards are updated below.
		if req.ConsumerKind == "video.character_swap" {
			for _, card := range cards {
				maxFence = max(maxFence, card.MinFencingToken+1)
			}
		}
		for _, sel := range selected {
			sel.card.MinFencingToken = maxFence
			grantedGPUs = append(grantedGPUs, sel.card.GPUUUID)
			var newReserved *string = sel.card.ReservedByLeaseID
			if newReserved != nil && *newReserved == leaseID {
				newReserved = nil
				sel.card.ReservedByLeaseID = nil
			}
			if _, err := tx.Exec(ctx, `
				UPDATE station.gpu_physical_cards
				   SET min_fencing_token = $4, reserved_by_lease_id = $5::uuid, updated_at = now()
				 WHERE station_id = $1 AND node_uid = $2 AND gpu_uuid = $3
			`, s.StationID, sel.card.NodeUID, sel.card.GPUUUID, maxFence, newReserved); err != nil {
				return LeaseDecision{}, err
			}
			if sel.bypassID != "" {
				bypassedSet[sel.bypassID] = true
			}
		}
		for bID := range bypassedSet {
			if _, err := tx.Exec(ctx, `
				UPDATE station.gpu_resource_leases
				   SET bypass_count = bypass_count + 1, updated_at = now()
				 WHERE station_id = $1 AND lease_id = $2
			`, s.StationID, bID); err != nil {
				return LeaseDecision{}, err
			}
		}

		grantedAtStr := now.UTC().Format(time.RFC3339Nano)
		fromState := ""
		if !isNew {
			fromState = existingState
			if _, err := tx.Exec(ctx, `
				UPDATE station.gpu_resource_leases
				   SET gpu_uuids = $3, reserved_gpu_uuids = '{}', fencing_token = $4,
				       state = 'leased', blocking_reason = 'none', blocking_holder_id = '',
				       pod_uid = CASE WHEN pod_uid = '' AND $5 <> '' THEN $5 ELSE pod_uid END,
				       container_id = CASE WHEN container_id = '' AND $6 <> '' THEN $6 ELSE container_id END,
				       boot_id = CASE WHEN boot_id = '' AND $7 <> '' THEN $7 ELSE boot_id END,
				       response_cache = jsonb_set(COALESCE(response_cache, '{}'::jsonb), '{granted_at}', to_jsonb($9::text), true),
				       expires_at = $8, version = version + 1, updated_at = now()
				 WHERE station_id = $1 AND lease_id = $2
			`, s.StationID, leaseID, grantedGPUs, maxFence, req.PodUID, req.ContainerID, req.BootID, expiresAt, grantedAtStr); err != nil {
				return LeaseDecision{}, err
			}
		} else {
			initCacheRaw, _ := json.Marshal(map[string]any{"acquire_req": req, "granted_at": grantedAtStr})
			if _, err := tx.Exec(ctx, `
				INSERT INTO station.gpu_resource_leases(
					lease_id, station_id, node_uid, holder_app_id, holder_task_id, attempt_id, stage_id,
					pod_uid, container_id, boot_id, idempotency_key, request_sha256,
					candidate_gpu_uuids, gpu_uuids, reserved_gpu_uuids, card_count, k8s_slots_required,
					peak_budget_mib, startup_free_min_mib, headroom_override_mib, fencing_token,
					priority, state, blocking_reason, blocking_holder_id, response_cache, expires_at
				) VALUES (
					$1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,'{}',$15,$16,$17,$18,$19,$20,$21,'leased','none','',$22,$23
				)
			`,
				leaseID, s.StationID, targetNodeUID, req.HolderAppID, req.TaskID, req.AttemptID, req.StageID,
				req.PodUID, req.ContainerID, req.BootID, req.IdempotencyKey, reqHash,
				req.CandidateGPUUUIDs, grantedGPUs, req.CardCount, req.K8sSlotsRequired,
				req.PeakBudgetMiB, req.StartupFreeMinMiB, req.HeadroomOverrideMiB, maxFence,
				req.PriorityClass, initCacheRaw, expiresAt,
			); err != nil {
				return LeaseDecision{}, err
			}
		}

		var lastSeq int64
		for _, sel := range selected {
			seq, _, err := appendCardEvent(ctx, tx, sel.card, eventRecordInput{
				RunNonce:             req.RunNonce,
				CorrelationEventUUID: corrUUID,
				EntityType:           "lease",
				EntityID:             leaseID,
				FromState:            fromState,
				ToState:              "leased",
				FencingToken:         maxFence,
				BudgetMiB:            req.PeakBudgetMiB,
				ReasonCode:           "ADMITTED",
				Payload: map[string]any{
					"holder_app_id":  req.HolderAppID,
					"holder_task_id": req.TaskID,
					"attempt_id":     req.AttemptID,
					"stage_id":       req.StageID,
					"gpu_uuids":      grantedGPUs,
				},
			})
			if err != nil {
				return LeaseDecision{}, err
			}
			lastSeq = seq
		}

		decision, err = s.loadDecisionTx(ctx, tx, leaseID, cards)
		if err != nil {
			return LeaseDecision{}, err
		}
		decision.LedgerEventSeq = lastSeq
		if err := tx.Commit(ctx); err != nil {
			return LeaseDecision{}, err
		}
		if !isAutoPromote {
			s.fireHook(ctx, "after_acquire_commit_before_http_reply", map[string]string{
				"lease_id":        leaseID,
				"idempotency_key": req.IdempotencyKey,
				"state":           decision.State,
			})
		}
		return decision, nil
	}

	// Not immediately admissible: check if DrainingPeer or Atomic Set Reservation applies!
	var drainCandidates []candidateEval
	for _, ev := range evals {
		if ev.fit.ImmediateFit || ev.fit.DrainingFit {
			drainCandidates = append(drainCandidates, ev)
		}
	}

	targetState := "queued"
	blockReason := evals[0].fit.BlockingReason
	blockHolder := evals[0].fit.BlockingHolder
	reservedGPUs := []string{}

	if len(drainCandidates) >= req.CardCount && earlierHeadRow == nil {
		selected := drainCandidates[:req.CardCount]
		sort.Slice(selected, func(i, j int) bool { return selected[i].card.GPUUUID < selected[j].card.GPUUUID })
		allReservable := true
		for _, sel := range selected {
			if sel.card.ReservedByLeaseID != nil && *sel.card.ReservedByLeaseID != "" && *sel.card.ReservedByLeaseID != leaseID {
				allReservable = false
				break
			}
		}
		if allReservable {
			targetState = "draining"
			blockReason = "draining_evictable_peer"
			pendingNodeUID = targetNodeUID
			for _, sel := range selected {
				reservedGPUs = append(reservedGPUs, sel.card.GPUUUID)
				for _, r := range sel.fit.EvictTargets {
					if r.CallbackEndpoint != "" {
						pendingEvictions = append(pendingEvictions, EvictVRAMCommand{
							CallbackEndpoint:     r.CallbackEndpoint,
							InstanceID:           r.InstanceID,
							ExpectedBootID:       r.BootID,
							ExpectedResidencySeq: r.ResidencySeq,
							ExpectedGPUEpoch:     r.GPUEpoch,
							SoftEvictDeadlineMs:  DefaultSoftEvictDeadline,
							Reason:               "PREFLIGHT_S2_EVICT_FOR_QUEUED_PEER",
						})
					}
				}
			}
		}
	} else if earlierHeadRow == nil {
		// Head-of-line queued task: try atomic full-set reservation (P-B: ALL req.CardCount cards must be unreserved, else reserve 0 cards!).
		var freeReservationCards []*cardLockState
		for _, ev := range evals {
			if ev.card.AdmissionMode == AdmissionActive && ev.card.State == "ready" &&
				ev.card.TelemetrySampledAt != nil && now.Sub(*ev.card.TelemetrySampledAt) <= maxAge &&
				(ev.card.ReservedByLeaseID == nil || *ev.card.ReservedByLeaseID == "" || *ev.card.ReservedByLeaseID == leaseID) {
				freeReservationCards = append(freeReservationCards, ev.card)
			}
		}
		if len(freeReservationCards) >= req.CardCount {
			chosenRes := freeReservationCards[:req.CardCount]
			sort.Slice(chosenRes, func(i, j int) bool { return chosenRes[i].GPUUUID < chosenRes[j].GPUUUID })
			for _, c := range chosenRes {
				reservedGPUs = append(reservedGPUs, c.GPUUUID)
			}
		}
	}

	var fence int64 = 1
	if len(candidateCards) > 0 && candidateCards[0].MinFencingToken > fence {
		fence = candidateCards[0].MinFencingToken
	}
	if !isNew {
		if isAutoPromote && (targetState == "queued" || targetState == "draining") &&
			existingState == targetState &&
			slices.Equal(existingReservedGPUs, reservedGPUs) &&
			existingBlockReason == blockReason &&
			existingBlockHolder == blockHolder {
			return s.loadDecisionTx(ctx, tx, leaseID, cards)
		}
		queuedExpires := expiresAt
		if isAutoPromote {
			queuedExpires = existingExpiresAt
		}
		if _, err := tx.Exec(ctx, `
			UPDATE station.gpu_resource_leases
			   SET reserved_gpu_uuids = $3, state = $4, blocking_reason = $5, blocking_holder_id = $6,
			       expires_at = $7, version = version + 1, updated_at = now()
			 WHERE station_id = $1 AND lease_id = $2
		`, s.StationID, leaseID, reservedGPUs, targetState, blockReason, blockHolder, queuedExpires); err != nil {
			return LeaseDecision{}, err
		}
	} else {
		initCacheRaw, _ := json.Marshal(map[string]any{"acquire_req": req})
		if _, err := tx.Exec(ctx, `
			INSERT INTO station.gpu_resource_leases(
				lease_id, station_id, node_uid, holder_app_id, holder_task_id, attempt_id, stage_id,
				pod_uid, container_id, boot_id, idempotency_key, request_sha256,
				candidate_gpu_uuids, gpu_uuids, reserved_gpu_uuids, card_count, k8s_slots_required,
				peak_budget_mib, startup_free_min_mib, headroom_override_mib, fencing_token,
				priority, state, blocking_reason, blocking_holder_id, response_cache, expires_at
			) VALUES (
				$1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,'{}',$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25,$26
			)
		`,
			leaseID, s.StationID, targetNodeUID, req.HolderAppID, req.TaskID, req.AttemptID, req.StageID,
			req.PodUID, req.ContainerID, req.BootID, req.IdempotencyKey, reqHash,
			req.CandidateGPUUUIDs, reservedGPUs, req.CardCount, req.K8sSlotsRequired,
			req.PeakBudgetMiB, req.StartupFreeMinMiB, req.HeadroomOverrideMiB, fence,
			req.PriorityClass, targetState, blockReason, blockHolder, initCacheRaw, expiresAt,
		); err != nil {
			return LeaseDecision{}, err
		}
	}

	var lastSeq int64
	if len(reservedGPUs) > 0 {
		for i := range cards {
			if cards[i].NodeUID == targetNodeUID && slices.Contains(reservedGPUs, cards[i].GPUUUID) {
				already := cards[i].ReservedByLeaseID != nil && *cards[i].ReservedByLeaseID == leaseID
				lID := leaseID
				cards[i].ReservedByLeaseID = &lID
				if _, err := tx.Exec(ctx, `
					UPDATE station.gpu_physical_cards
					   SET reserved_by_lease_id = $4::uuid, updated_at = now()
					 WHERE station_id = $1 AND node_uid = $2 AND gpu_uuid = $3
				`, s.StationID, cards[i].NodeUID, cards[i].GPUUUID, leaseID); err != nil {
					return LeaseDecision{}, err
				}
				if isNew || !already || existingState != targetState {
					seq, _, err := appendCardEvent(ctx, tx, &cards[i], eventRecordInput{
						RunNonce:             req.RunNonce,
						CorrelationEventUUID: corrUUID,
						EntityType:           "lease",
						EntityID:             leaseID,
						FromState:            existingState,
						ToState:              targetState,
						FencingToken:         fence,
						BudgetMiB:            req.PeakBudgetMiB,
						ReasonCode:           strings.ToUpper(blockReason),
						Payload: map[string]any{
							"holder_app_id":      req.HolderAppID,
							"holder_task_id":     req.TaskID,
							"reserved_gpu_uuids": reservedGPUs,
						},
					})
					if err != nil {
						return LeaseDecision{}, err
					}
					lastSeq = seq
				}
			}
		}
	} else if isNew {
		seq, _, err := appendCardEvent(ctx, tx, candidateCards[0], eventRecordInput{
			RunNonce:             req.RunNonce,
			CorrelationEventUUID: corrUUID,
			EntityType:           "lease",
			EntityID:             leaseID,
			FromState:            "",
			ToState:              targetState,
			FencingToken:         fence,
			BudgetMiB:            req.PeakBudgetMiB,
			ReasonCode:           strings.ToUpper(blockReason),
			Payload: map[string]any{
				"holder_app_id":  req.HolderAppID,
				"holder_task_id": req.TaskID,
			},
		})
		if err != nil {
			return LeaseDecision{}, err
		}
		lastSeq = seq
	}

	decision, err = s.loadDecisionTx(ctx, tx, leaseID, cards)
	if err != nil {
		return LeaseDecision{}, err
	}
	if lastSeq > 0 {
		decision.LedgerEventSeq = lastSeq
	}
	if err := tx.Commit(ctx); err != nil {
		return LeaseDecision{}, err
	}

	// Execute Fenced Pre-flight Eviction OUTSIDE the short card-lock transaction (§4.2.1 & §7.2)!
	if targetState == "draining" && len(pendingEvictions) > 0 && s.Evictor != nil {
		if _, loaded := s.drainingInFlight.LoadOrStore(leaseID, true); !loaded {
			defer s.drainingInFlight.Delete(leaseID)
			promoted, pErr := s.executeDrainingEviction(ctx, caller, req, isAutoPromote, leaseID, pendingNodeUID, pendingEvictions)
			if pErr == nil {
				return promoted, nil
			}
		}
	}

	return decision, nil
}

func findEarlierBlockingQueueHead(leases []activeLeaseRow, selfLeaseID, selfPriority string, candidates []*cardLockState) *activeLeaseRow {
	candSet := make(map[string]bool, len(candidates))
	for _, c := range candidates {
		candSet[c.GPUUUID] = true
	}
	for i := range leases {
		l := &leases[i]
		if l.LeaseID == selfLeaseID {
			break
		}
		if l.State != "queued" && l.State != "draining" {
			continue
		}
		if len(l.CandidateGPUUUIDs) > 0 {
			overlaps := false
			for _, g := range l.CandidateGPUUUIDs {
				if candSet[g] {
					overlaps = true
					break
				}
			}
			if !overlaps {
				continue
			}
		}
		return l
	}
	return nil
}

func clearLeaseReservationsTx(ctx context.Context, tx pgx.Tx, stationID, leaseID string, cards []cardLockState, runNonce, reason string) error {
	corrUUID := uuid.Must(uuid.NewV7()).String()
	for i := range cards {
		if cards[i].ReservedByLeaseID != nil && *cards[i].ReservedByLeaseID == leaseID {
			cards[i].ReservedByLeaseID = nil
			if _, err := tx.Exec(ctx, `
				UPDATE station.gpu_physical_cards
				   SET reserved_by_lease_id = NULL, updated_at = now()
				 WHERE station_id = $1 AND node_uid = $2 AND gpu_uuid = $3
			`, stationID, cards[i].NodeUID, cards[i].GPUUUID); err != nil {
				return err
			}
			if _, _, err := appendCardEvent(ctx, tx, &cards[i], eventRecordInput{
				RunNonce:             runNonce,
				CorrelationEventUUID: corrUUID,
				EntityType:           "lease",
				EntityID:             leaseID,
				FromState:            "reserved",
				ToState:              "reservation_cleared",
				ReasonCode:           reason,
			}); err != nil {
				return err
			}
		}
	}
	_, err := tx.Exec(ctx, `
		UPDATE station.gpu_resource_leases
		   SET reserved_gpu_uuids = '{}', updated_at = now()
		 WHERE station_id = $1 AND lease_id = $2
	`, stationID, leaseID)
	return err
}

func (s *Service) loadDecisionTx(ctx context.Context, tx pgx.Tx, leaseID string, cards []cardLockState) (LeaseDecision, error) {
	var r activeLeaseRow
	var cacheRaw []byte
	err := tx.QueryRow(ctx, `
		SELECT lease_id::text, holder_app_id, holder_task_id, attempt_id, stage_id,
		       node_uid, pod_uid, container_id, boot_id, idempotency_key, request_sha256,
		       candidate_gpu_uuids, gpu_uuids, reserved_gpu_uuids, card_count, k8s_slots_required,
		       peak_budget_mib, startup_free_min_mib, headroom_override_mib, fencing_token,
		       priority, bypass_count, state, blocking_reason, blocking_holder_id,
		       response_cache, expires_at, version, created_at
		  FROM station.gpu_resource_leases
		 WHERE station_id = $1 AND lease_id = $2
	`, s.StationID, leaseID).Scan(
		&r.LeaseID, &r.HolderAppID, &r.HolderTaskID, &r.AttemptID, &r.StageID,
		&r.NodeUID, &r.PodUID, &r.ContainerID, &r.BootID, &r.IdempotencyKey, &r.RequestSHA256,
		&r.CandidateGPUUUIDs, &r.GPUUUIDs, &r.ReservedGPUUUIDs, &r.CardCount, &r.K8sSlotsRequired,
		&r.PeakBudgetMiB, &r.StartupFreeMinMiB, &r.HeadroomOverride, &r.FencingToken,
		&r.Priority, &r.BypassCount, &r.State, &r.BlockingReason, &r.BlockingHolderID,
		&cacheRaw, &r.ExpiresAt, &r.Version, &r.CreatedAt,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return LeaseDecision{}, ErrLeaseNotFound
	}
	if err != nil {
		return LeaseDecision{}, err
	}
	queuePos := 0
	if r.State == "queued" || r.State == "draining" {
		_ = tx.QueryRow(ctx, `
			SELECT count(*)::int FROM station.gpu_resource_leases
			 WHERE station_id = $1 AND node_uid = $2
			   AND state IN ('queued', 'draining')
			   AND (created_at < $3 OR (created_at = $3 AND lease_id::text <= $4))
		`, s.StationID, r.NodeUID, r.CreatedAt, r.LeaseID).Scan(&queuePos)
	}
	var maxSeq int64
	for _, c := range cards {
		if c.LastEventSeq > maxSeq {
			maxSeq = c.LastEventSeq
		}
	}
	return decisionFromRow(r, queuePos, maxSeq), nil
}
