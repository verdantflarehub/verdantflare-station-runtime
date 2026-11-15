package scheduler

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/verdantflarehub/verdantflare-station-runtime/internal/strictjson"
)

const (
	// CP1 locked constants (D1-A, D2a-1, D2b-1, D3-2, D4-2, D5-2, D6-1, D7-1, D8-1).
	DefaultCardHeadroomMiB   int64         = 2048
	ExclusiveCardHeadroomMiB int64         = 1024
	DefaultDriverOverheadMiB int64         = 607
	DefaultSoftEvictDeadline int64         = 10000
	MaxBypassCount           int           = 2
	DefaultAppGPUQuota       int           = 12
	DefaultPerCardTimeSlices int           = 6
	DefaultLeaseTTLSeconds   int64         = 90
	DefaultMaxTelemetryAge   time.Duration = 15 * time.Second
)

const (
	PriorityHumanWake      = "human_wake"
	PrioritySharedSlice    = "shared_slice"
	PriorityExclusiveBatch = "exclusive_batch"
)

const (
	AdmissionDrainOnly = "drain_only"
	AdmissionActive    = "active"
)

var (
	ErrUnauthenticated      = errors.New("UNAUTHENTICATED")
	ErrForbiddenCaller      = errors.New("CALLER_IDENTITY_MISMATCH")
	ErrInvalidRequest       = errors.New("INVALID_REQUEST")
	ErrIdempotencyConflict  = errors.New("IDEMPOTENCY_CONFLICT")
	ErrVersionConflict      = errors.New("LEASE_VERSION_CONFLICT")
	ErrLeaseNotFound        = errors.New("LEASE_NOT_FOUND")
	ErrLeaseInactive        = errors.New("LEASE_NOT_ACTIVE")
	ErrFencingRevoked       = errors.New("FENCING_TOKEN_REVOKED")
	ErrWorkerNotSettled     = errors.New("WORKER_NOT_SETTLED")
	ErrDowngradeDisabled    = errors.New("LOCK_DOWNGRADE_DISABLED_BEFORE_CUTOVER")
	ErrCrossCardHoldAndWait = errors.New("CROSS_CARD_HOLD_AND_WAIT_FORBIDDEN")
	ErrTopologyUnsatisfied  = errors.New("TOPOLOGY_UNSATISFIABLE")
	ErrCutoverBlocked       = errors.New("CUTOVER_BLOCKED_UNUPGRADED_OR_UNDRAINED")
	ErrNodeStateUnknown     = errors.New("UNKNOWN_NODE_STATE")
	ErrReclaimQuarantined   = errors.New("RECLAIM_FAILED_QUARANTINED")
	ErrResidencyNotFound    = errors.New("RESIDENCY_NOT_FOUND")
	ErrGUIEditLeaseHeld     = errors.New("GUI_EDIT_LEASE_HELD")
	ErrInstanceActiveCompute = errors.New("INSTANCE_ACTIVE_COMPUTE")
	ErrAlreadyZeroVRAM      = errors.New("ALREADY_ZERO_VRAM")
	ErrInstanceNotEvictable = errors.New("INSTANCE_NOT_EVICTABLE")
	ErrConfirmationRequired = errors.New("CONFIRMATION_REQUIRED")
)

var sha256HexPattern = regexp.MustCompile(`^[0-9a-f]{64}$`)

type CallerKind string

const (
	CallerConsumer  CallerKind = "consumer"
	CallerNodeAgent CallerKind = "node_agent"
	CallerAdmin     CallerKind = "admin"
)

type CallerIdentity struct {
	AppID string     `json:"app_id"`
	Kind  CallerKind `json:"kind"`
}

type CallerRegistry map[[32]byte]CallerIdentity

func LoadCallerRegistry(r io.Reader, getenv func(string) string) (CallerRegistry, error) {
	var entries []struct {
		AppID    string     `json:"app_id"`
		Kind     CallerKind `json:"kind"`
		TokenEnv string     `json:"token_env"`
	}
	if strictjson.Decode(r, 1<<20, &entries) != nil || len(entries) == 0 || len(entries) > 128 {
		return nil, ErrInvalidRequest
	}
	out := make(CallerRegistry, len(entries))
	seenApps := map[string]bool{}
	for _, entry := range entries {
		appID := strings.TrimSpace(entry.AppID)
		if appID == "" || len(appID) > 128 || seenApps[appID+":"+string(entry.Kind)] {
			return nil, ErrInvalidRequest
		}
		switch entry.Kind {
		case CallerConsumer, CallerNodeAgent, CallerAdmin:
		default:
			return nil, ErrInvalidRequest
		}
		tok := strings.TrimSpace(getenv(entry.TokenEnv))
		if len(tok) < 32 || len(tok) > 512 {
			return nil, ErrInvalidRequest
		}
		digest := sha256.Sum256([]byte("Bearer " + tok))
		if _, dup := out[digest]; dup {
			return nil, ErrInvalidRequest
		}
		seenApps[appID+":"+string(entry.Kind)] = true
		out[digest] = CallerIdentity{AppID: appID, Kind: entry.Kind}
	}
	return out, nil
}

func (reg CallerRegistry) Authenticate(authHeader string) (CallerIdentity, error) {
	if len(reg) == 0 || !strings.HasPrefix(authHeader, "Bearer ") {
		return CallerIdentity{}, ErrUnauthenticated
	}
	candidate := sha256.Sum256([]byte(authHeader))
	var matched CallerIdentity
	found := false
	for digest, caller := range reg {
		if hmac.Equal(digest[:], candidate[:]) {
			matched = caller
			found = true
		}
	}
	if !found {
		return CallerIdentity{}, ErrUnauthenticated
	}
	return matched, nil
}

type ConsumerProfileSpec struct {
	CardCount           int
	PeakBudgetMiB       int64
	StartupFreeMinMiB   int64
	HeadroomOverrideMiB *int64
	Priority            string
}

func DefaultConsumerSpec(kind string) (ConsumerProfileSpec, bool) {
	exclHdr := ExclusiveCardHeadroomMiB
	switch strings.ToLower(strings.TrimSpace(kind)) {
	case "h3", "fasth3", "video.generate", "video.regenerate_2k", "vdn", "sol":
		return ConsumerProfileSpec{CardCount: 1, PeakBudgetMiB: 29696, StartupFreeMinMiB: 28672, HeadroomOverrideMiB: &exclHdr, Priority: PriorityExclusiveBatch}, true
	case "rife", "video.interpolate":
		return ConsumerProfileSpec{CardCount: 1, PeakBudgetMiB: 4096, StartupFreeMinMiB: 8192, Priority: PrioritySharedSlice}, true
	case "sr", "super_resolution", "video.sr", "depth", "video.depth":
		return ConsumerProfileSpec{CardCount: 1, PeakBudgetMiB: 8192, StartupFreeMinMiB: 8192, Priority: PrioritySharedSlice}, true
	case "indextts2", "speech.synthesize":
		return ConsumerProfileSpec{CardCount: 1, PeakBudgetMiB: 6144, StartupFreeMinMiB: 6144, Priority: PrioritySharedSlice}, true
	case "music3", "music.generate":
		return ConsumerProfileSpec{CardCount: 2, PeakBudgetMiB: 16384, StartupFreeMinMiB: 18432, Priority: PriorityExclusiveBatch}, true
	case "uvr5", "music.cover":
		return ConsumerProfileSpec{CardCount: 1, PeakBudgetMiB: 6144, StartupFreeMinMiB: 6144, Priority: PrioritySharedSlice}, true
	case "rvc", "voice_training":
		return ConsumerProfileSpec{CardCount: 1, PeakBudgetMiB: 8192, StartupFreeMinMiB: 6144, Priority: PrioritySharedSlice}, true
	case "ace-step", "music.editor", "redraw", "duet_generate":
		return ConsumerProfileSpec{CardCount: 1, PeakBudgetMiB: 12288, StartupFreeMinMiB: 18432, Priority: PrioritySharedSlice}, true
	case "moss-image", "image.create":
		return ConsumerProfileSpec{CardCount: 1, PeakBudgetMiB: 18432, StartupFreeMinMiB: 16384, Priority: PrioritySharedSlice}, true
	case "face-fusion", "image.faceswap":
		return ConsumerProfileSpec{CardCount: 1, PeakBudgetMiB: 6144, StartupFreeMinMiB: 4096, Priority: PrioritySharedSlice}, true
	case "comfyui_wake":
		return ConsumerProfileSpec{CardCount: 1, PeakBudgetMiB: 12288, StartupFreeMinMiB: 10240, Priority: PrioritySharedSlice}, true
	case "blender_wake":
		return ConsumerProfileSpec{CardCount: 1, PeakBudgetMiB: 6144, StartupFreeMinMiB: 6144, Priority: PrioritySharedSlice}, true
	case "blender_human_wake":
		return ConsumerProfileSpec{CardCount: 1, PeakBudgetMiB: 6144, StartupFreeMinMiB: 6144, Priority: PriorityHumanWake}, true
	default:
		return ConsumerProfileSpec{}, false
	}
}

func priorityRank(priority string) int {
	switch priority {
	case PriorityHumanWake:
		return 3
	case PrioritySharedSlice:
		return 2
	case PriorityExclusiveBatch:
		return 1
	default:
		return 0
	}
}

type AcquireRequest struct {
	IdempotencyKey      string   `json:"idempotency_key"`
	RequestSHA256       string   `json:"request_sha256,omitempty"`
	HolderAppID         string   `json:"holder_app_id,omitempty"`
	ConsumerKind        string   `json:"consumer_kind,omitempty"`
	TaskID              string   `json:"task_id,omitempty"`
	InstanceID          string   `json:"instance_id,omitempty"`
	AttemptID           string   `json:"attempt_id"`
	StageID             string   `json:"stage_id,omitempty"`
	NodeUID             string   `json:"node_uid,omitempty"`
	PodUID              string   `json:"pod_uid,omitempty"`
	ContainerID         string   `json:"container_id,omitempty"`
	BootID              string   `json:"boot_id,omitempty"`
	CandidateGPUUUIDs   []string `json:"candidate_gpu_uuids,omitempty"`
	CardCount           int      `json:"card_count,omitempty"`
	K8sSlotsRequired    int      `json:"k8s_slots_required,omitempty"`
	PeakBudgetMiB       int64    `json:"peak_budget_mib,omitempty"`
	StartupFreeMinMiB   int64    `json:"startup_free_min_mib,omitempty"`
	HeadroomOverrideMiB *int64   `json:"headroom_override_mib,omitempty"`
	PriorityClass       string   `json:"priority_class,omitempty"`
	TTLSeconds          int64    `json:"ttl_seconds,omitempty"`
	SourceLeaseID       string   `json:"source_lease_id,omitempty"`
	RunNonce            string   `json:"run_nonce,omitempty"`
}

type LeaseDecision struct {
	LeaseID           string   `json:"lease_id"`
	State             string   `json:"state"`
	HolderAppID       string   `json:"holder_app_id"`
	HolderTaskID      string   `json:"holder_task_id"`
	AttemptID         string   `json:"attempt_id"`
	StageID           string   `json:"stage_id"`
	NodeUID           string   `json:"node_uid"`
	PodUID            string   `json:"pod_uid,omitempty"`
	ContainerID       string   `json:"container_id,omitempty"`
	BootID            string   `json:"boot_id,omitempty"`
	GPUUUID           string   `json:"gpu_uuid,omitempty"`
	GPUUUIDs          []string `json:"gpu_uuids"`
	ReservedGPUUUIDs  []string `json:"reserved_gpu_uuids,omitempty"`
	PeakBudgetMiB     int64    `json:"peak_budget_mib"`
	StartupFreeMinMiB int64    `json:"startup_free_min_mib"`
	FencingToken      int64    `json:"fencing_token"`
	Priority          string   `json:"priority"`
	QueuePosition     int      `json:"queue_position,omitempty"`
	BlockingReason    string   `json:"blocking_reason"`
	BlockingHolderID  string   `json:"blocking_holder_id,omitempty"`
	ExpiresAt         string   `json:"expires_at"`
	Version           int64    `json:"version"`
	LedgerEventSeq    int64    `json:"ledger_event_seq,omitempty"`
}

type HeartbeatRequest struct {
	IdempotencyKey  string         `json:"idempotency_key"`
	RequestSHA256   string         `json:"request_sha256,omitempty"`
	FencingToken    int64          `json:"fencing_token"`
	ExpectedVersion int64          `json:"expected_version,omitempty"`
	TTLSeconds      int64          `json:"ttl_seconds,omitempty"`
	DowngradeToMiB  *int64         `json:"downgrade_to_mib,omitempty"`
	NewStageID      string         `json:"new_stage_id,omitempty"`
	WorkerSettled   bool           `json:"worker_settled,omitempty"`
	GPUEpoch                 int64          `json:"gpu_epoch,omitempty"`
	SupervisorPID            int            `json:"supervisor_pid,omitempty"`
	SupervisorStarttimeTicks uint64         `json:"supervisor_starttime_ticks,omitempty"`
	ReclaimMode              string         `json:"reclaim_mode,omitempty"`
	ReclaimProof             map[string]any `json:"reclaim_proof,omitempty"`
	PodUID                   string         `json:"pod_uid,omitempty"`
	ContainerID              string         `json:"container_id,omitempty"`
	BootID                   string         `json:"boot_id,omitempty"`
	NextPodUID               string         `json:"next_pod_uid,omitempty"`
	NextContainerID          string         `json:"next_container_id,omitempty"`
	NextBootID               string         `json:"next_boot_id,omitempty"`
	RunNonce                 string         `json:"run_nonce,omitempty"`
}

type BindTargetRequest struct {
	IdempotencyKey  string   `json:"idempotency_key"`
	RequestSHA256   string   `json:"request_sha256,omitempty"`
	FencingToken    int64    `json:"fencing_token"`
	ExpectedVersion int64    `json:"expected_version,omitempty"`
	TTLSeconds      int64    `json:"ttl_seconds,omitempty"`
	NodeUID         string   `json:"node_uid,omitempty"`
	PodUID          string   `json:"pod_uid"`
	ContainerID     string   `json:"container_id"`
	BootID          string   `json:"boot_id"`
	ActualGPUUUIDs  []string `json:"actual_gpu_uuids"`
	RunNonce        string   `json:"run_nonce,omitempty"`
}

type ReleaseRequest struct {
	IdempotencyKey           string         `json:"idempotency_key"`
	RequestSHA256            string         `json:"request_sha256,omitempty"`
	FencingToken             int64          `json:"fencing_token"`
	ExpectedVersion          int64          `json:"expected_version,omitempty"`
	WorkerSettled            bool           `json:"worker_settled"`
	PodUID                   string         `json:"pod_uid,omitempty"`
	ContainerID              string         `json:"container_id,omitempty"`
	BootID                   string         `json:"boot_id,omitempty"`
	GPUEpoch                 int64          `json:"gpu_epoch,omitempty"`
	SupervisorPID            int            `json:"supervisor_pid,omitempty"`
	SupervisorStarttimeTicks uint64         `json:"supervisor_starttime_ticks,omitempty"`
	ReclaimMode              string         `json:"reclaim_mode,omitempty"`
	ReclaimProof             map[string]any `json:"reclaim_proof,omitempty"`
	RunNonce                 string         `json:"run_nonce,omitempty"`
}

type ResidencyReportRequest struct {
	InstanceID         string `json:"instance_id"`
	NodeUID            string `json:"node_uid"`
	PodUID             string `json:"pod_uid"`
	ContainerName      string `json:"container_name,omitempty"`
	ContainerID        string `json:"container_id,omitempty"`
	BootID             string `json:"boot_id"`
	GPUEpoch           int64  `json:"gpu_epoch"`
	ResidencySeq       int64  `json:"residency_seq"`
	GPUUUID            string `json:"gpu_uuid"`
	ResidencyState     string `json:"residency_state"`
	HumanProtected     bool   `json:"human_protected"`
	GUIConnectionAlive bool   `json:"gui_connection_alive"`
	Dirty              bool   `json:"dirty"`
	SnapshotInProgress bool   `json:"snapshot_in_progress"`
	SnapshotRequired   bool   `json:"snapshot_required"`
	BlockingReason     string `json:"blocking_reason"`
	PinnedBudgetMiB    int64  `json:"pinned_budget_mib,omitempty"`
	VRAMAllocatedMiB   *int64 `json:"vram_allocated_mib"`
	IdleSeconds        int64  `json:"idle_seconds"`
	CallbackEndpoint   string `json:"callback_endpoint,omitempty"`
	TemplateUpgraded   *bool  `json:"template_upgraded,omitempty"`
	RunNonce           string `json:"run_nonce,omitempty"`
}

type AttributedGPUProcess struct {
	PodUID         string `json:"pod_uid,omitempty"`
	ContainerID    string `json:"container_id,omitempty"`
	PID            int    `json:"pid"`
	StarttimeTicks uint64 `json:"starttime_ticks,omitempty"`
	ProcessType    string `json:"process_type,omitempty"`
	UsedVRAMMiB    int64  `json:"used_vram_mib"`
}

type ActiveGPUPodInfo struct {
	PodUID     string `json:"pod_uid"`
	PodName    string `json:"pod_name,omitempty"`
	NodeName   string `json:"node_name,omitempty"`
	Phase      string `json:"phase,omitempty"`
	GPUSlots   int    `json:"gpu_slots"`
	LeaseID    string `json:"lease_id,omitempty"`
	InstanceID string `json:"instance_id,omitempty"`
	AppID      string `json:"app_id,omitempty"`
}

type CardTelemetrySample struct {
	GPUUUID                    string                 `json:"gpu_uuid"`
	GPUIndex                   int                    `json:"gpu_index"`
	ModelName                  string                 `json:"model_name,omitempty"`
	TotalVRAMMiB               int64                  `json:"total_vram_mib"`
	DriverOverheadMiB          int64                  `json:"driver_overhead_mib,omitempty"`
	MeasuredFreeVRAMMiB        int64                  `json:"measured_free_vram_mib"`
	MeasuredUsedVRAMMiB        int64                  `json:"measured_used_vram_mib"`
	TimeSlicingSlots           int                    `json:"time_slicing_slots,omitempty"`
	AllocatedK8sSlots          int                    `json:"allocated_k8s_slots"`
	ScalingUpK8sSlots          int                    `json:"scaling_up_k8s_slots"`
	UnscheduledPendingK8sSlots int                    `json:"unscheduled_pending_k8s_slots,omitempty"`
	AttributedProcesses        []AttributedGPUProcess `json:"attributed_processes,omitempty"`
	ActiveGPUPods              []ActiveGPUPodInfo     `json:"active_gpu_pods,omitempty"`
	NVMLHealthy                bool                   `json:"nvml_healthy"`
	ErrorReason                string                 `json:"error_reason,omitempty"`
}

type NodeTelemetryReport struct {
	NodeUID      string                `json:"node_uid"`
	NodeName     string                `json:"node_name"`
	AgentBootID  string                `json:"agent_boot_id,omitempty"`
	SampleID     int64                 `json:"telemetry_sample_id"`
	SampledAt    string                `json:"sampled_at,omitempty"`
	AgentHealthy bool                  `json:"agent_healthy"`
	ErrorCode    string                `json:"error_code,omitempty"`
	Cards        []CardTelemetrySample `json:"cards"`
}

type ResidentProcIdentity struct {
	PID            int    `json:"pid"`
	StarttimeTicks uint64 `json:"starttime_ticks"`
}

type VerifyTerminatedRequest struct {
	NodeUID                  string                 `json:"node_uid"`
	GPUUUID                  string                 `json:"gpu_uuid,omitempty"`
	PodUID                   string                 `json:"pod_uid"`
	ContainerID              string                 `json:"container_id,omitempty"`
	BootID                   string                 `json:"boot_id,omitempty"`
	GPUEpoch                 int64                  `json:"gpu_epoch,omitempty"`
	SupervisorPID            int                    `json:"supervisor_pid,omitempty"`
	SupervisorStarttimeTicks uint64                 `json:"supervisor_starttime_ticks,omitempty"`
	ResidentProcs            []ResidentProcIdentity `json:"resident_procs,omitempty"`
	EpochFenceConfirmed      bool                   `json:"epoch_fence_confirmed,omitempty"`
	Mode                     string                 `json:"mode"` // "epoch_unload", "warm_idle_settled", or "container_exited"
}

type VerifyTerminatedResult struct {
	Verified            bool           `json:"verified"`
	ContainerExited     bool           `json:"container_exited"`
	TombstoneFound      bool           `json:"tombstone_found"`
	GPUProcessCount     int            `json:"gpu_process_count"`
	CgroupWorkerCount   int            `json:"cgroup_worker_count"`
	MeasuredFreeVRAMMiB int64          `json:"measured_free_vram_mib"`
	SurvivingPIDs       []int          `json:"surviving_pids,omitempty"`
	UnkillableDState    bool           `json:"unkillable_d_state"`
	ErrorCode           string         `json:"error_code,omitempty"`
	RawProof            map[string]any `json:"raw_proof,omitempty"`
}

type EvictVRAMCommand struct {
	CallbackEndpoint     string `json:"-"`
	InstanceID           string `json:"instance_id"`
	ExpectedBootID       string `json:"expected_boot_id,omitempty"`
	ExpectedResidencySeq int64  `json:"expected_residency_seq"`
	ExpectedGPUEpoch     int64  `json:"expected_gpu_epoch"`
	SoftEvictDeadlineMs  int64  `json:"soft_evict_deadline_ms,omitempty"`
	ForceHard            bool   `json:"force_hard,omitempty"`
	Reason               string `json:"reason,omitempty"`
}

type EvictVRAMResult struct {
	Settled                  bool           `json:"settled"`
	Conflict                 bool           `json:"conflict"`
	Unreclaimed              bool           `json:"unreclaimed"`
	ResidencyState           string         `json:"residency_state"`
	ResidencySeq             int64          `json:"residency_seq"`
	GPUEpoch                 int64          `json:"gpu_epoch"`
	SealedGPUEpoch           int64          `json:"sealed_gpu_epoch,omitempty"`
	BootID                   string         `json:"boot_id"`
	ActualBootID             string         `json:"actual_boot_id,omitempty"`
	ActualGPUEpoch           int64          `json:"actual_gpu_epoch,omitempty"`
	ActualResidencySeq       int64          `json:"actual_residency_seq,omitempty"`
	Data                     map[string]any `json:"data,omitempty"`
	SupervisorPID            int            `json:"supervisor_pid,omitempty"`
	SupervisorStarttimeTicks uint64         `json:"supervisor_starttime_ticks,omitempty"`
	ReclaimProof             map[string]any `json:"reclaim_proof,omitempty"`
	ReasonCode               string         `json:"reason_code"`
	SurvivingPIDs            []int          `json:"surviving_pids,omitempty"`
}

type RevokeLeaseCommand struct {
	CallbackEndpoint string `json:"-"`
	LeaseID          string `json:"lease_id"`
	InstanceID       string `json:"instance_id,omitempty"`
	FencingToken     int64  `json:"fencing_token"`
	BootID           string `json:"boot_id,omitempty"`
	ExpectedGPUEpoch int64  `json:"expected_gpu_epoch,omitempty"`
	PodUID           string `json:"pod_uid,omitempty"`
	ContainerID      string `json:"container_id,omitempty"`
	AttemptID        string `json:"attempt_id,omitempty"`
	Reason           string `json:"reason,omitempty"`
}

type RevokeLeaseResult struct {
	Revoked                  bool           `json:"revoked"`
	Reclaimed                bool           `json:"reclaimed"`
	Settled                  bool           `json:"settled"`
	InstanceID               string         `json:"instance_id,omitempty"`
	LeaseID                  string         `json:"lease_id,omitempty"`
	BootID                   string         `json:"boot_id"`
	FencingToken             int64          `json:"fencing_token,omitempty"`
	MinFenceToken            int64          `json:"min_fencing_token"`
	GPUEpoch                 int64          `json:"gpu_epoch,omitempty"`
	ResidencySeq             int64          `json:"residency_seq,omitempty"`
	ResidencyState           string         `json:"residency_state,omitempty"`
	SealedGPUEpoch           int64          `json:"sealed_gpu_epoch,omitempty"`
	SupervisorPID            int            `json:"supervisor_pid,omitempty"`
	SupervisorStarttimeTicks uint64         `json:"supervisor_starttime_ticks,omitempty"`
	ReclaimProof             map[string]any `json:"reclaim_proof,omitempty"`
	ErrorCode                string         `json:"error_code,omitempty"`
}

type RegisteredInstance struct {
	InstanceID       string
	AppID            string
	NodeUID          string
	PodUID           string
	CallbackEndpoint string
	SecretNamespace  string
	SecretName       string
	SecretUID        string
}

type InstanceDirectory interface {
	LookupInstance(ctx context.Context, stationID, instanceID string) (RegisteredInstance, bool, error)
}

type NodeVerifier interface {
	VerifyTerminated(ctx context.Context, req VerifyTerminatedRequest) (VerifyTerminatedResult, error)
}

type ResidencyEvictor interface {
	EvictVRAM(ctx context.Context, cmd EvictVRAMCommand) (EvictVRAMResult, error)
	RevokeLease(ctx context.Context, cmd RevokeLeaseCommand) (RevokeLeaseResult, error)
}

type StageHook interface {
	OnStage(ctx context.Context, point string, meta map[string]string)
}

func computeCanonicalHash(parts ...any) string {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	_ = enc.Encode(parts)
	raw := bytes.TrimSuffix(buf.Bytes(), []byte{'\n'})
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}

type cardLockState struct {
	StationID                  string
	NodeUID                    string
	NodeName                   string
	GPUUUID                    string
	GPUIndex                   int
	TotalVRAMMiB               int64
	DriverOverheadMiB          int64
	MeasuredFreeVRAMMiB        int64
	MeasuredUsedVRAMMiB        int64
	TelemetryBootID            string
	TelemetrySampleID          int64
	TelemetrySampledAt         *time.Time
	AttributedProcesses        []AttributedGPUProcess
	FencingEpoch               int64
	MinFencingToken            int64
	ReservedByLeaseID          *string
	QuarantinedMiB             int64
	TimeSlicingSlots           int
	AllocatedK8sSlots          int
	ScalingUpK8sSlots          int
	UnscheduledPendingK8sSlots int
	ActiveGPUPods              []ActiveGPUPodInfo
	AdmissionMode              string
	LastEventSeq               int64
	LastEventHash              string
	State                      string
	StateReason                string
}

type eventRecordInput struct {
	RunNonce             string
	CorrelationEventUUID string
	EntityType           string
	EntityID             string
	FromState            string
	ToState              string
	FencingToken         int64
	ResidencySeq         int64
	GPUEpoch             int64
	BudgetMiB            int64
	ReasonCode           string
	Payload              map[string]any
}

func encodeJSONStringNoEscape(s string) string {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	_ = enc.Encode(s)
	out := strings.TrimSuffix(buf.String(), "\n")
	out = strings.ReplaceAll(out, `\u2028`, "\u2028")
	out = strings.ReplaceAll(out, `\u2029`, "\u2029")
	return out
}

func canonicalizeJSONValue(v any) (string, error) {
	switch val := v.(type) {
	case nil:
		return "null", nil
	case bool:
		if val {
			return "true", nil
		}
		return "false", nil
	case json.Number:
		return val.String(), nil
	case string:
		return encodeJSONStringNoEscape(val), nil
	case map[string]any:
		keys := make([]string, 0, len(val))
		for k := range val {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		var b strings.Builder
		b.WriteByte('{')
		for idx, k := range keys {
			if idx > 0 {
				b.WriteByte(',')
			}
			b.WriteString(encodeJSONStringNoEscape(k))
			b.WriteByte(':')
			child, err := canonicalizeJSONValue(val[k])
			if err != nil {
				return "", err
			}
			b.WriteString(child)
		}
		b.WriteByte('}')
		return b.String(), nil
	case []any:
		var b strings.Builder
		b.WriteByte('[')
		for idx, elem := range val {
			if idx > 0 {
				b.WriteByte(',')
			}
			child, err := canonicalizeJSONValue(elem)
			if err != nil {
				return "", err
			}
			b.WriteString(child)
		}
		b.WriteByte(']')
		return b.String(), nil
	default:
		var buf bytes.Buffer
		enc := json.NewEncoder(&buf)
		enc.SetEscapeHTML(false)
		if err := enc.Encode(val); err != nil {
			return "", err
		}
		return strings.TrimSuffix(buf.String(), "\n"), nil
	}
}

// appendCardEvent appends a deterministic hash-chained audit row onto a card
// whose station.gpu_physical_cards row is ALREADY locked FOR UPDATE in tx.
// The computed SHA-256 matches station.canonical_gpu_event_hash(...) in 0023 byte-for-byte.
func appendCardEvent(ctx context.Context, tx pgx.Tx, card *cardLockState, ev eventRecordInput) (int64, string, error) {
	if ev.CorrelationEventUUID == "" {
		ev.CorrelationEventUUID = uuid.Must(uuid.NewV7()).String()
	}
	if parsedCorr, err := uuid.Parse(ev.CorrelationEventUUID); err == nil {
		ev.CorrelationEventUUID = parsedCorr.String()
	}
	stationUUID := card.StationID
	if parsedStation, err := uuid.Parse(stationUUID); err == nil {
		stationUUID = parsedStation.String()
	}
	if ev.Payload == nil {
		ev.Payload = map[string]any{}
	}
	nextSeq := card.LastEventSeq + 1
	prevHash := card.LastEventHash

	var rawPayloadBuf bytes.Buffer
	pEnc := json.NewEncoder(&rawPayloadBuf)
	pEnc.SetEscapeHTML(false)
	if err := pEnc.Encode(ev.Payload); err != nil {
		return 0, "", err
	}
	rawPayload := bytes.TrimSuffix(rawPayloadBuf.Bytes(), []byte{'\n'})

	var decodedPayload map[string]any
	dec := json.NewDecoder(bytes.NewReader(rawPayload))
	dec.UseNumber()
	if err := dec.Decode(&decodedPayload); err != nil {
		return 0, "", err
	}

	payloadKeys := make([]string, 0, len(decodedPayload))
	for k := range decodedPayload {
		payloadKeys = append(payloadKeys, k)
	}
	sort.Strings(payloadKeys)

	var payloadArrayBuilder strings.Builder
	payloadArrayBuilder.WriteByte('[')
	for idx, k := range payloadKeys {
		if idx > 0 {
			payloadArrayBuilder.WriteByte(',')
		}
		valCanon, err := canonicalizeJSONValue(decodedPayload[k])
		if err != nil {
			return 0, "", err
		}
		payloadArrayBuilder.WriteByte('[')
		payloadArrayBuilder.WriteString(encodeJSONStringNoEscape(k))
		payloadArrayBuilder.WriteByte(',')
		payloadArrayBuilder.WriteString(valCanon)
		payloadArrayBuilder.WriteByte(']')
	}
	payloadArrayBuilder.WriteByte(']')

	var canon strings.Builder
	canon.WriteString(prevHash)
	canon.WriteString(`|{"station_id":`)
	canon.WriteString(encodeJSONStringNoEscape(stationUUID))
	canon.WriteString(`,"node_uid":`)
	canon.WriteString(encodeJSONStringNoEscape(card.NodeUID))
	canon.WriteString(`,"gpu_uuid":`)
	canon.WriteString(encodeJSONStringNoEscape(card.GPUUUID))
	canon.WriteString(`,"card_event_seq":`)
	canon.WriteString(strconv.FormatInt(nextSeq, 10))
	canon.WriteString(`,"prev_hash":`)
	canon.WriteString(encodeJSONStringNoEscape(prevHash))
	canon.WriteString(`,"correlation_event_uuid":`)
	canon.WriteString(encodeJSONStringNoEscape(ev.CorrelationEventUUID))
	canon.WriteString(`,"run_nonce":`)
	canon.WriteString(encodeJSONStringNoEscape(ev.RunNonce))
	canon.WriteString(`,"entity_type":`)
	canon.WriteString(encodeJSONStringNoEscape(ev.EntityType))
	canon.WriteString(`,"entity_id":`)
	canon.WriteString(encodeJSONStringNoEscape(ev.EntityID))
	canon.WriteString(`,"from_state":`)
	canon.WriteString(encodeJSONStringNoEscape(ev.FromState))
	canon.WriteString(`,"to_state":`)
	canon.WriteString(encodeJSONStringNoEscape(ev.ToState))
	canon.WriteString(`,"fencing_token":`)
	canon.WriteString(strconv.FormatInt(ev.FencingToken, 10))
	canon.WriteString(`,"residency_seq":`)
	canon.WriteString(strconv.FormatInt(ev.ResidencySeq, 10))
	canon.WriteString(`,"gpu_epoch":`)
	canon.WriteString(strconv.FormatInt(ev.GPUEpoch, 10))
	canon.WriteString(`,"budget_mib":`)
	canon.WriteString(strconv.FormatInt(ev.BudgetMiB, 10))
	canon.WriteString(`,"reason_code":`)
	canon.WriteString(encodeJSONStringNoEscape(ev.ReasonCode))
	canon.WriteString(`,"payload":`)
	canon.WriteString(payloadArrayBuilder.String())
	canon.WriteByte('}')

	sum := sha256.Sum256([]byte(canon.String()))
	eventHash := hex.EncodeToString(sum[:])

	_, err := tx.Exec(ctx, `
		INSERT INTO station.gpu_scheduler_events(
			station_id, node_uid, gpu_uuid, card_event_seq, run_nonce, correlation_event_uuid,
			entity_type, entity_id, from_state, to_state, fencing_token, residency_seq, gpu_epoch,
			budget_mib, reason_code, payload, prev_hash, event_hash
		) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18)
	`,
		stationUUID, card.NodeUID, card.GPUUUID, nextSeq, ev.RunNonce, ev.CorrelationEventUUID,
		ev.EntityType, ev.EntityID, ev.FromState, ev.ToState, ev.FencingToken, ev.ResidencySeq, ev.GPUEpoch,
		ev.BudgetMiB, ev.ReasonCode, rawPayload, prevHash, eventHash,
	)
	if err != nil {
		return 0, "", fmt.Errorf("appendCardEvent(%s/%s seq=%d): %w", card.NodeUID, card.GPUUUID, nextSeq, err)
	}
	card.LastEventSeq = nextSeq
	card.LastEventHash = eventHash
	return nextSeq, eventHash, nil
}
