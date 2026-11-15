package nodegpu

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/verdantflarehub/verdantflare-station-runtime/internal/scheduler"
	"github.com/verdantflarehub/verdantflare-station-runtime/internal/strictjson"
)

type K8sSlotCounter interface {
	CountNodeGPUSlots(ctx context.Context, nodeName string) (allocated int, scalingUp int, err error)
}

type DetailedK8sPodObserver interface {
	ObserveNamespaceGPUPods(ctx context.Context, nodeName string) (NodeGPUSlotObservation, error)
}

type PodContainerIDResolver interface {
	ResolvePodContainerID(ctx context.Context, podUID, containerHint string) (string, error)
}

type Agent struct {
	NodeUID             string
	NodeName            string
	BootID              string
	BearerToken         string
	RuntimeSchedulerURL string
	RuntimeToken        string
	NVML                NVMLProbe
	Proc                ProcInspector
	CRI                 ContainerStatusSource
	SlotCounter         K8sSlotCounter
	TombstoneDir        string
	HTTPClient          *http.Client

	bootOnce      sync.Once
	sampleSeq     atomic.Int64
	lastKnownGPUs []string
}

func (a *Agent) bootID() string {
	a.bootOnce.Do(func() {
		if strings.TrimSpace(a.BootID) == "" {
			a.BootID = uuid.NewString()
		}
	})
	return a.BootID
}

func (a *Agent) sampleNVMLAndProcsAtomically(ctx context.Context) (NVMLSnapshot, []ProcEntry, error) {
	if a.NVML == nil {
		return NVMLSnapshot{}, nil, scheduler.ErrNodeStateUnknown
	}
	for attempt := 0; attempt < 2; attempt++ {
		nvmlSnap, err := a.NVML.Sample(ctx)
		if err != nil {
			return NVMLSnapshot{}, nil, err
		}
		reqPIDs := make([]int, 0, len(nvmlSnap.Processes))
		for _, gp := range nvmlSnap.Processes {
			reqPIDs = append(reqPIDs, gp.PID)
		}
		procs, pErr := a.Proc.ScanProcessesStrict(reqPIDs)
		if pErr == nil {
			return nvmlSnap, procs, nil
		}
		// Retry once if a GPU PID exited mid-scan; fail-closed immediately on permission errors or second mismatch.
		if attempt == 1 || errors.Is(pErr, os.ErrPermission) {
			return NVMLSnapshot{}, nil, pErr
		}
	}
	return NVMLSnapshot{}, nil, ErrProcInspectionIncomplete
}

func (a *Agent) EnsureStorageAndProcAccess() error {
	if dir := strings.TrimSpace(a.TombstoneDir); dir != "" {
		if err := ensurePrivateDir(dir); err != nil {
			return err
		}
		probePath := filepath.Join(dir, ".startup_probe")
		if err := writePrivateAtomic(probePath, []byte("ok\n")); err != nil {
			return err
		}
		_ = os.Remove(probePath)
	}
	return a.Proc.VerifyReadAccess()
}

func (a *Agent) epochTombstoneStore() TombstoneStore {
	return TombstoneStore{
		Dir:     a.TombstoneDir,
		HMACKey: []byte(a.BearerToken),
	}
}

func (a *Agent) loadVerifiedEpochTombstone(podUID, containerID, bootID, gpuUUID string, epoch int64) (EpochTombstone, bool) {
	if strings.TrimSpace(a.TombstoneDir) == "" {
		return EpochTombstone{}, false
	}
	et, ok, err := a.epochTombstoneStore().LoadVerifiedEpochTombstone(podUID, containerID, bootID, gpuUUID, epoch)
	if err != nil || !ok {
		return EpochTombstone{}, false
	}
	return et, true
}

func (a *Agent) resolvePodScopedContainerID(
	ctx context.Context,
	targetPodUID, rawTargetCID string,
	supPID int,
	supTicks uint64,
	procs []ProcEntry,
) (string, bool) {
	embPod, hint, valid := parsePodScopedContainerID(rawTargetCID)
	if !valid || embPod != targetPodUID {
		return "", false
	}

	var candidates []string
	addCandidate := func(cid string) bool {
		clean := strings.ToLower(NormalizeContainerID(cid))
		if !isHex64ContainerID(clean) {
			return true
		}
		for _, existing := range candidates {
			if existing != clean {
				return false
			}
		}
		if len(candidates) == 0 {
			candidates = append(candidates, clean)
		}
		return true
	}

	var supMatches []string
	procCIDSet := map[string]bool{}
	for _, p := range procs {
		if p.PodUID != targetPodUID || !isHex64ContainerID(p.ContainerID) {
			continue
		}
		if p.Comm == "pause" || strings.HasPrefix(p.Cmdline, "/pause") {
			continue
		}
		cid := strings.ToLower(p.ContainerID)
		procCIDSet[cid] = true
		if supPID > 0 && supTicks > 0 && p.InnermostPID() == supPID && p.StarttimeTicks == supTicks {
			supMatches = append(supMatches, cid)
		}
	}
	if len(supMatches) > 1 {
		first := supMatches[0]
		for _, m := range supMatches[1:] {
			if m != first {
				return "", false
			}
		}
	}
	if len(supMatches) >= 1 {
		if !addCandidate(supMatches[0]) {
			return "", false
		}
	} else if len(procCIDSet) == 1 {
		for cid := range procCIDSet {
			if !addCandidate(cid) {
				return "", false
			}
		}
	}

	if a.SlotCounter != nil {
		if resolver, ok := a.SlotCounter.(PodContainerIDResolver); ok {
			k8sCID, err := resolver.ResolvePodContainerID(ctx, targetPodUID, hint)
			if err != nil {
				return "", false
			}
			if k8sCID != "" && !addCandidate(k8sCID) {
				return "", false
			}
		}
	}

	if strings.TrimSpace(a.TombstoneDir) != "" {
		if tombCID, ok := (TombstoneStore{Dir: a.TombstoneDir}).ResolveExitedContainerIDByPod(targetPodUID, hint); ok {
			if !addCandidate(tombCID) {
				return "", false
			}
		}
	}

	if len(candidates) != 1 {
		return "", false
	}
	return candidates[0], true
}

// resolveHostSupervisor maps a container-namespace SupervisorPID + StarttimeTicks
// to the exact host-namespace ProcEntry belonging to targetCID, or re-verifies
// a durable EpochTombstone against live host /proc (never treating container PID as host PID).
func resolveHostSupervisor(
	matchingProcByPID map[int]ProcEntry,
	targetCID string,
	reportedContainerPID int,
	expectedStarttimeTicks uint64,
	epochTomb EpochTombstone,
	hasVerifiedEpochTomb bool,
) (ProcEntry, bool) {
	if reportedContainerPID > 0 {
		if expectedStarttimeTicks == 0 {
			return ProcEntry{}, false
		}
		var matches []ProcEntry
		for _, p := range matchingProcByPID {
			if p.ContainerID != targetCID {
				continue
			}
			if p.InnermostPID() != reportedContainerPID {
				continue
			}
			if p.StarttimeTicks == 0 || p.StarttimeTicks != expectedStarttimeTicks {
				continue
			}
			if hasVerifiedEpochTomb {
				if p.PID != epochTomb.HostSupervisorPID ||
					(epochTomb.PIDNamespace != "" && p.PIDNamespace != epochTomb.PIDNamespace) {
					continue
				}
			}
			matches = append(matches, p)
		}
		if len(matches) != 1 {
			return ProcEntry{}, false
		}
		return matches[0], true
	}
	if hasVerifiedEpochTomb {
		p, exists := matchingProcByPID[epochTomb.HostSupervisorPID]
		if !exists || p.ContainerID != targetCID {
			return ProcEntry{}, false
		}
		if p.InnermostPID() != epochTomb.ContainerSupervisorPID ||
			p.StarttimeTicks == 0 || p.StarttimeTicks != epochTomb.SupervisorStarttimeTicks {
			return ProcEntry{}, false
		}
		if epochTomb.PIDNamespace != "" && p.PIDNamespace != epochTomb.PIDNamespace {
			return ProcEntry{}, false
		}
		return p, true
	}
	return ProcEntry{}, false
}

func (a *Agent) VerifyTerminated(ctx context.Context, req scheduler.VerifyTerminatedRequest) (scheduler.VerifyTerminatedResult, error) {
	mode := strings.TrimSpace(req.Mode)
	if mode == "" {
		mode = "container_exited"
	}
	if mode != "container_exited" && mode != "epoch_unload" && mode != "warm_idle_settled" {
		return scheduler.VerifyTerminatedResult{ErrorCode: "UNKNOWN_NODE_STATE"}, scheduler.ErrNodeStateUnknown
	}

	targetGPU := strings.TrimSpace(req.GPUUUID)
	targetPodUID := normalizePodUID(req.PodUID)
	targetCID := strings.ToLower(NormalizeContainerID(req.ContainerID))
	targetBootID := strings.TrimSpace(req.BootID)

	// Fail-closed identity gates (P1-H2): never verify termination on empty GPUUUID, PodUID, ContainerID, BootID, or nil CRI.
	if targetGPU == "" || targetPodUID == "" || targetCID == "" || a.CRI == nil {
		return scheduler.VerifyTerminatedResult{ErrorCode: "UNKNOWN_NODE_STATE"}, scheduler.ErrNodeStateUnknown
	}
	if mode == "epoch_unload" && (targetBootID == "" || req.GPUEpoch <= 0) {
		return scheduler.VerifyTerminatedResult{ErrorCode: "UNKNOWN_NODE_STATE"}, scheduler.ErrNodeStateUnknown
	}
	if mode == "warm_idle_settled" && targetBootID == "" {
		return scheduler.VerifyTerminatedResult{ErrorCode: "UNKNOWN_NODE_STATE"}, scheduler.ErrNodeStateUnknown
	}

	nvmlSnap, procs, err := a.sampleNVMLAndProcsAtomically(ctx)
	if err != nil {
		return scheduler.VerifyTerminatedResult{ErrorCode: "UNKNOWN_NODE_STATE"}, scheduler.ErrNodeStateUnknown
	}

	var measuredFree int64
	gpuFound := false
	for _, dev := range nvmlSnap.Devices {
		if dev.GPUUUID == targetGPU {
			if !dev.NVMLHealthy {
				return scheduler.VerifyTerminatedResult{ErrorCode: "UNKNOWN_NODE_STATE"}, scheduler.ErrNodeStateUnknown
			}
			measuredFree = dev.MeasuredFreeVRAMMiB
			gpuFound = true
			break
		}
	}
	if !gpuFound {
		return scheduler.VerifyTerminatedResult{ErrorCode: "UNKNOWN_NODE_STATE"}, scheduler.ErrNodeStateUnknown
	}

	if strings.HasPrefix(targetCID, "pod-") {
		resolvedCID, ok := a.resolvePodScopedContainerID(
			ctx,
			targetPodUID,
			targetCID,
			req.SupervisorPID,
			req.SupervisorStarttimeTicks,
			procs,
		)
		if !ok {
			return scheduler.VerifyTerminatedResult{ErrorCode: "UNKNOWN_NODE_STATE"}, scheduler.ErrNodeStateUnknown
		}
		targetCID = resolvedCID
	}

	criStatus, criErr := a.CRI.ContainerStatus(ctx, targetCID)
	if criErr != nil {
		if errors.Is(criErr, ErrContainerNotFound) {
			tombStore := TombstoneStore{Dir: a.TombstoneDir}
			tomb, ok, tErr := tombStore.Load(targetCID)
			if tErr != nil || !ok {
				return scheduler.VerifyTerminatedResult{ErrorCode: "UNKNOWN_NODE_STATE"}, scheduler.ErrNodeStateUnknown
			}
			criStatus = tomb
		} else {
			return scheduler.VerifyTerminatedResult{ErrorCode: "UNKNOWN_NODE_STATE"}, scheduler.ErrNodeStateUnknown
		}
	}
	// Verify CRI status belongs to targetPodUID whenever CRI exposes io.kubernetes.pod.uid!
	if criStatus.PodUID != "" && normalizePodUID(criStatus.PodUID) != targetPodUID {
		return scheduler.VerifyTerminatedResult{ErrorCode: "UNKNOWN_NODE_STATE"}, scheduler.ErrNodeStateUnknown
	}
	if criStatus.Exited && criStatus.FinishedAt > 0 && a.TombstoneDir != "" {
		_ = TombstoneStore{Dir: a.TombstoneDir}.SaveExited(criStatus)
	}

	procByPID := make(map[int]ProcEntry, len(procs))
	matchingProcByPID := make(map[int]ProcEntry)
	childrenByPPID := make(map[int][]int)
	var allMatchingPIDs []int
	var gpuFDPIDs []int
	var epochSeedPIDs []int
	unkillableD := false
	bootIDMismatch := false

	for _, p := range procs {
		procByPID[p.PID] = p
		matchesPod := p.PodUID == targetPodUID
		matchesCID := p.ContainerID == targetCID
		if !matchesPod && !matchesCID {
			continue
		}
		if p.Comm == "pause" || strings.HasPrefix(p.Cmdline, "/pause") {
			continue
		}
		matchingProcByPID[p.PID] = p
		childrenByPPID[p.PPID] = append(childrenByPPID[p.PPID], p.PID)
		allMatchingPIDs = append(allMatchingPIDs, p.PID)
		if p.State == "D" {
			unkillableD = true
		}
		if p.BootIDEnv != "" && p.BootIDEnv != targetBootID {
			bootIDMismatch = true
		}
		// Zero cmdline heuristics (P1-H3): inspect open /dev/nvidia* fds, explicit GPU epoch env vars,
		// and the full transitive descendant tree of SupervisorPID / epoch workers!
		if p.HasNvidiaFD {
			gpuFDPIDs = append(gpuFDPIDs, p.PID)
		}
		if mode == "epoch_unload" && p.GPUEpochEnv != nil && *p.GPUEpochEnv == req.GPUEpoch {
			epochSeedPIDs = append(epochSeedPIDs, p.PID)
		}
	}

	epochTomb, hasVerifiedEpochTomb := a.loadVerifiedEpochTombstone(targetPodUID, targetCID, targetBootID, targetGPU, req.GPUEpoch)
	resolvedSup, supervisorVerified := resolveHostSupervisor(
		matchingProcByPID,
		targetCID,
		req.SupervisorPID,
		req.SupervisorStarttimeTicks,
		epochTomb,
		hasVerifiedEpochTomb,
	)

	var matchingEpochPIDs []int
	if mode == "epoch_unload" {
		epochDescendantSet := map[int]bool{}
		var queue []int
		for _, pid := range epochSeedPIDs {
			if !epochDescendantSet[pid] {
				epochDescendantSet[pid] = true
				queue = append(queue, pid)
			}
		}
		if supervisorVerified && resolvedSup.PID > 0 {
			for _, childPID := range childrenByPPID[resolvedSup.PID] {
				if childPID != resolvedSup.PID && !epochDescendantSet[childPID] {
					epochDescendantSet[childPID] = true
					queue = append(queue, childPID)
				}
			}
		}
		for len(queue) > 0 {
			cur := queue[0]
			queue = queue[1:]
			for _, grandChild := range childrenByPPID[cur] {
				if (!supervisorVerified || grandChild != resolvedSup.PID) && !epochDescendantSet[grandChild] {
					epochDescendantSet[grandChild] = true
					queue = append(queue, grandChild)
				}
			}
		}
		for pid := range epochDescendantSet {
			matchingEpochPIDs = append(matchingEpochPIDs, pid)
		}
		sort.Ints(matchingEpochPIDs)
	}

	gpuProcCount := 0
	survivingSet := map[int]bool{}
	for _, gp := range nvmlSnap.Processes {
		if gp.GPUUUID != targetGPU {
			continue
		}
		p, exists := procByPID[gp.PID]
		if !exists {
			return scheduler.VerifyTerminatedResult{ErrorCode: "UNKNOWN_NODE_STATE"}, scheduler.ErrNodeStateUnknown
		}
		if p.PodUID == targetPodUID || p.ContainerID == targetCID {
			gpuProcCount++
			survivingSet[gp.PID] = true
			if p.State == "D" {
				unkillableD = true
			}
		}
	}
	for _, pid := range gpuFDPIDs {
		if !survivingSet[pid] {
			gpuProcCount++
			survivingSet[pid] = true
		}
	}

	containerExited := criStatus.Exited && criStatus.FinishedAt > 0
	tombstoneFound := criStatus.Tombstone || containerExited || hasVerifiedEpochTomb

	var verified bool
	var cgroupWorkerCount int
	var errCode string
	if mode == "epoch_unload" {
		workerSet := map[int]bool{}
		for _, pid := range matchingEpochPIDs {
			survivingSet[pid] = true
			workerSet[pid] = true
		}
		for _, pid := range gpuFDPIDs {
			workerSet[pid] = true
		}
		cgroupWorkerCount = len(workerSet)
		epochConfirmed := req.EpochFenceConfirmed || hasVerifiedEpochTomb
		if !criStatus.Exited && criStatus.State == "CONTAINER_RUNNING" &&
			!bootIDMismatch &&
			gpuProcCount == 0 && len(matchingEpochPIDs) == 0 && !unkillableD {
			if !supervisorVerified {
				errCode = "SUPERVISOR_PID_UNVERIFIED"
			} else if epochConfirmed {
				tombstoneFound = true
				verified = true
				if a.TombstoneDir != "" {
					_ = a.epochTombstoneStore().SaveEpochTombstone(EpochTombstone{
						PodUID:                   targetPodUID,
						ContainerID:              targetCID,
						BootID:                   targetBootID,
						GPUUUID:                  targetGPU,
						GPUEpoch:                 req.GPUEpoch,
						HostSupervisorPID:        resolvedSup.PID,
						ContainerSupervisorPID:   resolvedSup.InnermostPID(),
						SupervisorStarttimeTicks: resolvedSup.StarttimeTicks,
						PIDNamespace:             resolvedSup.PIDNamespace,
						Reclaimed:                true,
						SealedAtUnixNano:         time.Now().UTC().UnixNano(),
					})
				}
			} else {
				errCode = "EPOCH_FENCE_UNCONFIRMED"
			}
		}
	} else if mode == "warm_idle_settled" {
		// Warm resident service (single-worker RIFE/SR/Depth/UVR5/IndexTTS2, or guard-proxy + attested upstream engine):
		// Verify container is healthy CONTAINER_RUNNING, supervisor (PID, StarttimeTicks) matches host /proc,
		// every attested ResidentProcs entry matches host /proc inside the exact same container cgroup & PID namespace,
		// zero unattested GPU or cgroup processes exist, and zero D-state processes exist.
		// Live MeasuredFreeVRAMMiB is returned so any warm resident weights remain 100% tracked in UnattributedExternalMiB.
		allowedHostPIDs := map[int]bool{}
		residentProcsValid := true
		if supervisorVerified {
			allowedHostPIDs[resolvedSup.PID] = true
			if len(req.ResidentProcs) > 32 {
				residentProcsValid = false
			} else {
				for _, rp := range req.ResidentProcs {
					resolvedRP, ok := resolveHostSupervisor(matchingProcByPID, targetCID, rp.PID, rp.StarttimeTicks, EpochTombstone{}, false)
					if !ok {
						residentProcsValid = false
						break
					}
					if resolvedSup.PIDNamespace != "" && resolvedRP.PIDNamespace != "" && resolvedRP.PIDNamespace != resolvedSup.PIDNamespace {
						residentProcsValid = false
						break
					}
					allowedHostPIDs[resolvedRP.PID] = true
				}
			}
		}
		var nonSupGPUPIDs []int
		for pid := range survivingSet {
			if !supervisorVerified || !residentProcsValid || !allowedHostPIDs[pid] {
				nonSupGPUPIDs = append(nonSupGPUPIDs, pid)
			}
		}
		var childProcs []int
		for _, pid := range allMatchingPIDs {
			if !supervisorVerified || !residentProcsValid || !allowedHostPIDs[pid] {
				childProcs = append(childProcs, pid)
				survivingSet[pid] = true
			}
		}
		cgroupWorkerCount = len(childProcs)
		if !supervisorVerified {
			errCode = "SUPERVISOR_PID_UNVERIFIED"
		} else if !residentProcsValid {
			errCode = "RESIDENT_PROC_UNVERIFIED"
		} else if !criStatus.Exited && criStatus.State == "CONTAINER_RUNNING" &&
			!bootIDMismatch && !unkillableD && len(nonSupGPUPIDs) == 0 && len(childProcs) == 0 {
			if req.EpochFenceConfirmed {
				tombstoneFound = true
				verified = true
				for pid := range allowedHostPIDs {
					delete(survivingSet, pid)
				}
			} else {
				errCode = "EPOCH_FENCE_UNCONFIRMED"
			}
		}
	} else {
		for _, pid := range allMatchingPIDs {
			survivingSet[pid] = true
		}
		cgroupWorkerCount = len(allMatchingPIDs)
		if containerExited && tombstoneFound && !bootIDMismatch &&
			gpuProcCount == 0 && len(allMatchingPIDs) == 0 && !unkillableD {
			verified = true
		}
	}

	survivingPIDs := make([]int, 0, len(survivingSet))
	for pid := range survivingSet {
		survivingPIDs = append(survivingPIDs, pid)
	}
	sort.Ints(survivingPIDs)

	return scheduler.VerifyTerminatedResult{
		Verified:            verified,
		ContainerExited:     containerExited,
		TombstoneFound:      tombstoneFound,
		GPUProcessCount:     gpuProcCount,
		CgroupWorkerCount:   cgroupWorkerCount,
		MeasuredFreeVRAMMiB: measuredFree,
		SurvivingPIDs:       survivingPIDs,
		UnkillableDState:    unkillableD,
		ErrorCode:           errCode,
		RawProof: map[string]any{
			"node_uid":        a.NodeUID,
			"agent_boot_id":   a.bootID(),
			"mode":            mode,
			"cri_state":       criStatus.State,
			"cri_finished_at": criStatus.FinishedAt,
			"cri_tombstone":   criStatus.Tombstone,
			"sampled_at":      nvmlSnap.SampledAt.Format(time.RFC3339Nano),
		},
	}, nil
}

func (a *Agent) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodGet && r.URL.Path == "/healthz" {
		w.WriteHeader(http.StatusOK)
		return
	}
	if !a.authorize(r.Header.Get("Authorization")) {
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		w.WriteHeader(http.StatusUnauthorized)
		_ = json.NewEncoder(w).Encode(map[string]any{"error_code": "UNAUTHENTICATED"})
		return
	}
	if r.Method == http.MethodPost && r.URL.Path == "/internal/v1/node-gpu/verify-terminated" {
		var req scheduler.VerifyTerminatedRequest
		if strictjson.Decode(r.Body, 1<<20, &req) != nil {
			w.Header().Set("Content-Type", "application/json; charset=utf-8")
			w.WriteHeader(http.StatusBadRequest)
			_ = json.NewEncoder(w).Encode(map[string]any{"error_code": "INVALID_REQUEST"})
			return
		}
		res, err := a.VerifyTerminated(r.Context(), req)
		status := http.StatusOK
		if err != nil || res.ErrorCode == "UNKNOWN_NODE_STATE" {
			status = http.StatusServiceUnavailable
			if res.ErrorCode == "" {
				res.ErrorCode = "UNKNOWN_NODE_STATE"
			}
		}
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		w.WriteHeader(status)
		_ = json.NewEncoder(w).Encode(res)
		return
	}
	w.WriteHeader(http.StatusNotFound)
}

func (a *Agent) authorize(header string) bool {
	tok := strings.TrimSpace(a.BearerToken)
	if len(tok) < 32 || !strings.HasPrefix(header, "Bearer ") {
		return false
	}
	expected := sha256.Sum256([]byte("Bearer " + tok))
	got := sha256.Sum256([]byte(header))
	return hmac.Equal(expected[:], got[:])
}

func (a *Agent) BuildTelemetryReport(ctx context.Context) (scheduler.NodeTelemetryReport, error) {
	seq := a.sampleSeq.Add(1)
	rep := scheduler.NodeTelemetryReport{
		NodeUID:      a.NodeUID,
		NodeName:     a.NodeName,
		AgentBootID:  a.bootID(),
		SampleID:     seq,
		SampledAt:    time.Now().UTC().Format(time.RFC3339Nano),
		AgentHealthy: true,
	}

	var allocSlots, scalingSlots, unschedPending int
	var activePods []scheduler.ActiveGPUPodInfo
	if a.SlotCounter != nil {
		if detailed, ok := a.SlotCounter.(DetailedK8sPodObserver); ok {
			obs, sErr := detailed.ObserveNamespaceGPUPods(ctx, a.NodeName)
			if sErr != nil {
				rep.AgentHealthy = false
				rep.ErrorCode = "K8S_SLOT_QUERY_FAILED"
			} else {
				allocSlots = obs.AllocatedOnNode
				scalingSlots = obs.ScalingUpOnNode
				unschedPending = obs.UnscheduledPending
				activePods = obs.ActivePods
			}
		} else {
			var sErr error
			allocSlots, scalingSlots, sErr = a.SlotCounter.CountNodeGPUSlots(ctx, a.NodeName)
			if sErr != nil {
				rep.AgentHealthy = false
				rep.ErrorCode = "K8S_SLOT_QUERY_FAILED"
			}
		}
	}

	snap, procs, err := a.sampleNVMLAndProcsAtomically(ctx)
	if err != nil {
		rep.AgentHealthy = false
		rep.ErrorCode = "UNKNOWN_NODE_STATE"
		for idx, g := range a.lastKnownGPUs {
			rep.Cards = append(rep.Cards, scheduler.CardTelemetrySample{
				GPUUUID:      g,
				GPUIndex:     idx,
				TotalVRAMMiB: 32607,
				NVMLHealthy:  false,
				ErrorReason:  "UNKNOWN_NODE_STATE",
			})
		}
		return rep, err
	}

	if !snap.SampledAt.IsZero() {
		rep.SampledAt = snap.SampledAt.UTC().Format(time.RFC3339Nano)
	}
	procByPID := make(map[int]ProcEntry, len(procs))
	for _, p := range procs {
		procByPID[p.PID] = p
	}

	attrByGPU := make(map[string][]scheduler.AttributedGPUProcess, len(snap.Devices))
	for _, gp := range snap.Processes {
		p, exists := procByPID[gp.PID]
		if !exists {
			rep.AgentHealthy = false
			rep.ErrorCode = "UNATTRIBUTED_MISSING_PROC"
			continue
		}
		if p.PodUID == "" && p.ContainerID == "" {
			continue
		}
		attrByGPU[gp.GPUUUID] = append(attrByGPU[gp.GPUUUID], scheduler.AttributedGPUProcess{
			PodUID:         p.PodUID,
			ContainerID:    p.ContainerID,
			PID:            gp.PID,
			StarttimeTicks: p.StarttimeTicks,
			ProcessType:    gp.ProcessType,
			UsedVRAMMiB:    gp.UsedVRAMMiB,
		})
	}

	known := make([]string, 0, len(snap.Devices))
	for idx, dev := range snap.Devices {
		known = append(known, dev.GPUUUID)
		cardAlloc := 0
		cardScaling := 0
		cardUnsched := 0
		var cardPods []scheduler.ActiveGPUPodInfo
		// Report node-wide K8s GPU slots on index 0 so node sum equals total K8s GPU requests without double counting.
		if idx == 0 {
			cardAlloc = allocSlots
			cardScaling = scalingSlots
			cardUnsched = unschedPending
			cardPods = activePods
		}
		rep.Cards = append(rep.Cards, scheduler.CardTelemetrySample{
			GPUUUID:                    dev.GPUUUID,
			GPUIndex:                   dev.GPUIndex,
			ModelName:                  dev.ModelName,
			TotalVRAMMiB:               dev.TotalVRAMMiB,
			DriverOverheadMiB:          scheduler.DefaultDriverOverheadMiB,
			MeasuredFreeVRAMMiB:        dev.MeasuredFreeVRAMMiB,
			MeasuredUsedVRAMMiB:        dev.MeasuredUsedVRAMMiB,
			TimeSlicingSlots:           scheduler.DefaultPerCardTimeSlices,
			AllocatedK8sSlots:          cardAlloc,
			ScalingUpK8sSlots:          cardScaling,
			UnscheduledPendingK8sSlots: cardUnsched,
			AttributedProcesses:        attrByGPU[dev.GPUUUID],
			ActiveGPUPods:              cardPods,
			NVMLHealthy:                dev.NVMLHealthy,
			ErrorReason:                dev.ErrorReason,
		})
	}
	a.lastKnownGPUs = known
	return rep, nil
}

func (a *Agent) PushTelemetryOnce(ctx context.Context) error {
	if strings.TrimSpace(a.RuntimeSchedulerURL) == "" {
		return nil
	}
	rep, buildErr := a.BuildTelemetryReport(ctx)
	// Always push even when len(rep.Cards) == 0 if !rep.AgentHealthy so Runtime freezes node cards to reconciling!
	if len(rep.Cards) == 0 && rep.AgentHealthy {
		return buildErr
	}
	body, err := json.Marshal(rep)
	if err != nil {
		return err
	}
	target := strings.TrimRight(a.RuntimeSchedulerURL, "/") + "/internal/v1/gpu-scheduler/telemetry/report"
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, target, bytes.NewReader(body))
	if err != nil {
		return err
	}
	httpReq.Header.Set("Content-Type", "application/json")
	if a.RuntimeToken != "" {
		httpReq.Header.Set("Authorization", "Bearer "+a.RuntimeToken)
	}
	client := a.HTTPClient
	if client == nil {
		client = &http.Client{
			Timeout: 6 * time.Second,
			CheckRedirect: func(*http.Request, []*http.Request) error {
				return http.ErrUseLastResponse
			},
		}
	}
	resp, err := client.Do(httpReq)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("telemetry report HTTP %d", resp.StatusCode)
	}
	return buildErr
}
