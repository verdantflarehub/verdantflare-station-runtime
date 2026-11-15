package appinstances

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/verdantflarehub/verdantflare-station-runtime/internal/scheduler"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// ReclamationVerifier queries the node-local station-gpu-agent (CRI + host /proc
// cgroup + live NVML) to prove physical container termination and GPU reclamation.
type ReclamationVerifier interface {
	VerifyTerminated(ctx context.Context, req scheduler.VerifyTerminatedRequest) (scheduler.VerifyTerminatedResult, error)
}

type RepairStopOptions struct {
	StopOperationID               string
	BootID                        string
	CallerOperationID             string
	ForceAfterDrainTimeoutSeconds int
}

type ReclamationStatusInput struct {
	InstallationID string `json:"installation_id"`
	InstanceID     string `json:"instance_id"`
	Generation     int64  `json:"generation"`
	BootID         string `json:"boot_id"`
}

type ReclamationProof struct {
	SchemaVersion       int    `json:"schema_version"`
	InstanceID          string `json:"instance_id"`
	Generation          int64  `json:"generation"`
	BootID              string `json:"boot_id"`
	Reclaimed           bool   `json:"reclaimed"`
	PodUID              string `json:"pod_uid,omitempty"`
	ContainerID         string `json:"container_id,omitempty"`
	GPUEpoch            int64  `json:"gpu_epoch"`
	ContainerTerminated *bool  `json:"container_terminated,omitempty"`
	SurvivingPIDs       *int   `json:"surviving_pids,omitempty"`
	VRAMAllocatedMiB    *int64 `json:"vram_allocated_mib,omitempty"`
	Reason              string `json:"reason,omitempty"`
}

type instanceResidencyMeta struct {
	NodeUID     string
	ContainerID string
	GPUEpoch    int64
	GPUUUIDs    []string
}

func appendUniqueNonEmpty(list []string, values ...string) []string {
	for _, raw := range values {
		v := strings.TrimSpace(raw)
		if v == "" {
			continue
		}
		dup := false
		for _, existing := range list {
			if existing == v {
				dup = true
				break
			}
		}
		if !dup {
			list = append(list, v)
		}
	}
	return list
}

func queryInstanceResidencyMeta(ctx context.Context, q rowQuerier, stationID, instanceID, podUID, bootID string) instanceResidencyMeta {
	meta := instanceResidencyMeta{GPUEpoch: 0}
	if q == nil || stationID == "" || instanceID == "" || podUID == "" || bootID == "" {
		return meta
	}
	var hasResidency, hasLeases bool
	if err := q.QueryRow(ctx, `
		SELECT to_regclass('station.gpu_instance_residency') IS NOT NULL,
		       to_regclass('station.gpu_resource_leases') IS NOT NULL
	`).Scan(&hasResidency, &hasLeases); err != nil {
		return meta
	}
	if hasResidency {
		var nodeUID, gpuUUID, containerID string
		var gpuEpoch int64
		err := q.QueryRow(ctx, `
			SELECT node_uid, gpu_uuid, container_id, gpu_epoch
			  FROM station.gpu_instance_residency
			 WHERE station_id = $1 AND instance_id = $2 AND pod_uid = $3 AND boot_id = $4
		`, stationID, instanceID, podUID, bootID).Scan(&nodeUID, &gpuUUID, &containerID, &gpuEpoch)
		if err == nil {
			meta.NodeUID = strings.TrimSpace(nodeUID)
			if validContainerID(strings.TrimSpace(containerID)) {
				meta.ContainerID = strings.TrimSpace(containerID)
			}
			if gpuEpoch >= 0 {
				meta.GPUEpoch = gpuEpoch
			}
			meta.GPUUUIDs = appendUniqueNonEmpty(meta.GPUUUIDs, gpuUUID)
		}
	}
	if hasLeases {
		var leaseNodeUID, leaseCID string
		var gpuUUIDs []string
		err := q.QueryRow(ctx, `
			SELECT node_uid, container_id, gpu_uuids
			  FROM station.gpu_resource_leases
			 WHERE station_id = $1 AND pod_uid = $2 AND boot_id = $3
			 ORDER BY created_at DESC
			 LIMIT 1
		`, stationID, podUID, bootID).Scan(&leaseNodeUID, &leaseCID, &gpuUUIDs)
		if err == nil {
			if meta.NodeUID == "" {
				meta.NodeUID = strings.TrimSpace(leaseNodeUID)
			}
			if meta.ContainerID == "" && validContainerID(strings.TrimSpace(leaseCID)) {
				meta.ContainerID = strings.TrimSpace(leaseCID)
			}
			meta.GPUUUIDs = appendUniqueNonEmpty(meta.GPUUUIDs, gpuUUIDs...)
		}
	}
	return meta
}

func queryNodeCardGPUUUIDs(ctx context.Context, conn *pgxpool.Conn, stationID, nodeName, nodeUID string) []string {
	var hasCards bool
	if err := conn.QueryRow(ctx, `SELECT to_regclass('station.gpu_physical_cards') IS NOT NULL`).Scan(&hasCards); err != nil || !hasCards {
		return nil
	}
	rows, err := conn.Query(ctx, `
		SELECT gpu_uuid
		  FROM station.gpu_physical_cards
		 WHERE station_id = $1 AND ((node_name <> '' AND node_name = $2) OR (node_uid <> '' AND node_uid = $3))
		 ORDER BY gpu_index ASC, gpu_uuid ASC
	`, stationID, nodeName, nodeUID)
	if err != nil {
		return nil
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var gpuUUID string
		if rows.Scan(&gpuUUID) == nil {
			out = appendUniqueNonEmpty(out, gpuUUID)
		}
	}
	return out
}

func queryAttributedCardVRAMMiB(ctx context.Context, conn *pgxpool.Conn, stationID, podUID, containerID string, minSampledAt time.Time) (int64, error) {
	var hasCards bool
	if err := conn.QueryRow(ctx, `SELECT to_regclass('station.gpu_physical_cards') IS NOT NULL`).Scan(&hasCards); err != nil {
		return 0, err
	}
	if !hasCards {
		return 0, nil
	}
	rows, err := conn.Query(ctx, `
		SELECT telemetry_sampled_at, attributed_processes
		  FROM station.gpu_physical_cards
		 WHERE station_id = $1
	`, stationID)
	if err != nil {
		return 0, err
	}
	defer rows.Close()

	normPod := strings.ToLower(strings.TrimSpace(podUID))
	normCID := strings.ToLower(strings.TrimSpace(containerID))
	if idx := strings.Index(normCID, "://"); idx >= 0 {
		normCID = normCID[idx+3:]
	}

	var totalMiB int64
	for rows.Next() {
		var sampledAt *time.Time
		var rawProcs []byte
		if sErr := rows.Scan(&sampledAt, &rawProcs); sErr != nil {
			return 0, sErr
		}
		if !minSampledAt.IsZero() && (sampledAt == nil || sampledAt.Before(minSampledAt)) {
			continue
		}
		var procs []scheduler.AttributedGPUProcess
		if len(rawProcs) == 0 || json.Unmarshal(rawProcs, &procs) != nil {
			continue
		}
		for _, proc := range procs {
			pPod := strings.ToLower(strings.TrimSpace(proc.PodUID))
			pCID := strings.ToLower(strings.TrimSpace(proc.ContainerID))
			if idx := strings.Index(pCID, "://"); idx >= 0 {
				pCID = pCID[idx+3:]
			}
			if (normPod != "" && pPod == normPod) || (normCID != "" && pCID == normCID) {
				if proc.UsedVRAMMiB > 0 {
					totalMiB += proc.UsedVRAMMiB
				} else {
					totalMiB += 1
				}
			}
		}
	}
	return totalMiB, rows.Err()
}

func backfillLiveContainerID(ctx context.Context, j *Journal, conn *pgxpool.Conn, in Run, p Profile, r *StartRecord, bootID string) error {
	if r.Observation.ContainerID != "" || r.Observation.PodUID == "" {
		return nil
	}
	var cid string
	if pod, err := j.Client.CoreV1().Pods(p.Namespace).Get(ctx, runName(in), metav1.GetOptions{}); err == nil && pod != nil && string(pod.UID) == r.Observation.PodUID {
		for _, cs := range pod.Status.ContainerStatuses {
			if cs.Name == "worker" && validContainerID(cs.ContainerID) {
				cid = cs.ContainerID
				break
			}
		}
	}
	if cid == "" {
		resMeta := queryInstanceResidencyMeta(ctx, conn, j.StationID, in.InstanceID, r.Observation.PodUID, bootID)
		if validContainerID(resMeta.ContainerID) {
			cid = resMeta.ContainerID
		}
	}
	if cid == "" {
		return nil
	}
	next := r.Observation
	next.ContainerID = cid
	if !next.valid() {
		return nil
	}
	raw, _ := json.Marshal(next)
	res, err := conn.Exec(ctx, `
		UPDATE station.app_instance_starts
		   SET observation = $1, revision = revision + 1, updated_at = now()
		 WHERE operation_id = $2 AND revision = $3
	`, raw, r.Run.OperationID, r.Revision)
	if err != nil {
		return err
	}
	if res.RowsAffected() != 1 {
		return ErrConflict
	}
	r.Observation = next
	r.Revision++
	return nil
}

// StopForRepairRestart executes a bounded-drain stop for `repair.restart_worker`.
// It attempts a clean worker stop-barrier seal first, and if the worker HTTP server
// is wedged or busy past ForceAfterDrainTimeoutSeconds (measured from the durable
// `station.app_instance_stops.created_at` timestamp), checkpoints `sealed` directly
// and proceeds through UID/ResourceVersion-fenced Pod and Service deletion.
func (j *Journal) StopForRepairRestart(ctx context.Context, in Run, p Profile, opts RepairStopOptions) (StopRecord, error) {
	if j == nil || j.Stopper == nil ||
		!validID(opts.StopOperationID) || opts.StopOperationID == in.OperationID ||
		!validBoot(opts.BootID) || !validID(opts.CallerOperationID) ||
		opts.ForceAfterDrainTimeoutSeconds < 5 || opts.ForceAfterDrainTimeoutSeconds > 120 {
		return StopRecord{}, ErrInvalid
	}
	var s StopRecord
	_, e := j.withRecord(ctx, in, p, func(conn *pgxpool.Conn, r *StartRecord) error {
		if err := backfillLiveContainerID(ctx, j, conn, in, p, r, opts.BootID); err != nil {
			return err
		}
		var err error
		s, err = acceptStop(ctx, conn, in, p, r, opts.StopOperationID, opts.BootID)
		if err != nil || s.Progress.Phase == "stopped" {
			return err
		}
		if s.Progress.Phase == "accepted" {
			var stopElapsedSec float64
			if qErr := conn.QueryRow(ctx, `SELECT EXTRACT(EPOCH FROM (now() - created_at)) FROM station.app_instance_stops WHERE operation_id = $1`, opts.StopOperationID).Scan(&stopElapsedSec); qErr != nil {
				return qErr
			}
			drainExpired := stopElapsedSec >= float64(opts.ForceAfterDrainTimeoutSeconds)

			desiredPod, _, objErr := WorkerObjects(in, p, r.Volume)
			if objErr != nil {
				return objErr
			}
			pod, podErr := j.Client.CoreV1().Pods(p.Namespace).Get(ctx, desiredPod.Name, metav1.GetOptions{})
			if apierrors.IsNotFound(podErr) {
				proof := stopProof(in, p, r.Observation, opts.StopOperationID)
				if err = checkpointStop(ctx, conn, r, &s, StopProgress{Proof: proof, Phase: "sealed"}); err != nil {
					return err
				}
				if err = checkpointStop(ctx, conn, r, &s, StopProgress{Proof: proof, Phase: "pod_delete_requested"}); err != nil {
					return err
				}
			} else if podErr != nil {
				return podErr
			} else {
				if _, err = ObserveWorker(ctx, j.Client, in, p, r.Volume, r.Observation); err != nil {
					return err
				}
				secret, secErr := j.Client.CoreV1().Secrets(p.Namespace).Get(ctx, instanceName(in.Identity), metav1.GetOptions{})
				if secErr != nil || !secretMatches(secret, in.Identity, p, r.Observation.SecretUID) {
					return ErrBinding
				}
				podNotRunning := pod.Status.Phase != corev1.PodRunning || pod.DeletionTimestamp != nil
				var proof WorkerStopProof
				if podNotRunning {
					proof = stopProof(in, p, r.Observation, opts.StopOperationID)
				} else {
					sealProof, sealErr := j.Stopper.Seal(ctx, in, p, r.Observation, string(secret.Data["worker-token"]), opts.StopOperationID)
					if sealErr == nil {
						if sealProof != stopProof(in, p, r.Observation, opts.StopOperationID) {
							return ErrBinding
						}
						proof = sealProof
					} else if errors.Is(sealErr, ErrBinding) {
						return ErrBinding
					} else if drainExpired && (errors.Is(sealErr, ErrUnavailable) || errors.Is(sealErr, ErrBusy)) {
						proof = stopProof(in, p, r.Observation, opts.StopOperationID)
					} else if errors.Is(sealErr, ErrUnavailable) || errors.Is(sealErr, ErrBusy) {
						// Drain window still open: return `202 running / accepted` so caller polls until seal or drain timeout.
						return nil
					} else {
						return sealErr
					}
				}
				_, obsErr := ObserveWorker(ctx, j.Client, in, p, r.Volume, r.Observation)
				if obsErr != nil && !apierrors.IsNotFound(obsErr) {
					return obsErr
				}
				if err = checkpointStop(ctx, conn, r, &s, StopProgress{Proof: proof, Phase: "sealed"}); err != nil {
					return err
				}
				if apierrors.IsNotFound(obsErr) {
					if cErr := checkpointStop(ctx, conn, r, &s, StopProgress{Proof: proof, Phase: "pod_delete_requested"}); cErr != nil {
						return cErr
					}
				}
			}
		}
		_, err = RemoveWorker(ctx, j.Client, in, p, r.Volume, r.Observation, s.Progress, func(ctx context.Context, next StopProgress) error {
			return checkpointStop(ctx, conn, r, &s, next)
		})
		if errors.Is(err, ErrInvalid) {
			return ErrBinding
		}
		return err
	})
	return s, e
}

// ReclamationStatus evaluates the triple physical reclamation proof for a specific
// `(instance_id, generation, boot_id)` under a shared instance advisory lock and
// fresh Core installation authorization. It never equates Pod API deletion alone
// with physical reclamation (`reclaimed=true`).
func (j *Journal) ReclamationStatus(ctx context.Context, identity Identity, profiles map[string]Profile, generation int64, bootID string) (ReclamationProof, error) {
	if j == nil || j.Pool == nil || j.Client == nil || j.Authorize == nil ||
		identity.StationID != j.StationID || !identity.valid() || generation < 1 || !validBoot(bootID) {
		return ReclamationProof{}, ErrInvalid
	}
	conn, err := j.Pool.Acquire(ctx)
	if err != nil {
		return ReclamationProof{}, err
	}
	defer conn.Release()

	key := "runtime-instance:" + identity.StationID + ":" + identity.InstanceID
	var locked bool
	if err = conn.QueryRow(ctx, `SELECT pg_try_advisory_lock_shared(hashtextextended($1,0))`, key).Scan(&locked); err != nil {
		discardConnection(conn)
		return ReclamationProof{}, err
	}
	if !locked {
		return ReclamationProof{}, ErrBusy
	}
	defer func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		var unlocked bool
		if uErr := conn.QueryRow(cleanup, `SELECT pg_advisory_unlock_shared(hashtextextended($1,0))`, key).Scan(&unlocked); uErr != nil || !unlocked {
			discardConnection(conn)
		}
	}()

	r, err := scanRecord(conn.QueryRow(ctx, `
		SELECT `+recordColumns+`
		  FROM station.app_instance_starts s
		  JOIN station.app_instance_workspaces w USING(instance_id)
		 WHERE s.instance_id = $1 AND s.generation = $2
	`, identity.InstanceID, generation))
	if err != nil {
		return ReclamationProof{}, err
	}
	if r.Run.Identity != identity {
		return ReclamationProof{}, ErrDenied
	}
	if r.Run.Generation != generation {
		return ReclamationProof{}, ErrConflict
	}

	var matchedProfile Profile
	profileFound := false
	for _, cand := range profiles {
		if cand.Valid() && cand.Hash() == r.ProfileHash {
			matchedProfile = cand
			profileFound = true
			break
		}
	}
	if !profileFound {
		return ReclamationProof{}, ErrDenied
	}

	revision, err := j.Authorize(ctx, r.Run, matchedProfile)
	if err != nil {
		return ReclamationProof{}, err
	}
	if err = installationMatches(ctx, conn, r.Run, matchedProfile, revision); err != nil {
		return ReclamationProof{}, err
	}
	if !sameRecord(r, r.Run, matchedProfile, revision) {
		return ReclamationProof{}, ErrConflict
	}
	if r.Observation.BootID == "" || r.Observation.BootID != bootID {
		return ReclamationProof{}, ErrConflict
	}

	resMeta := queryInstanceResidencyMeta(ctx, conn, j.StationID, identity.InstanceID, r.Observation.PodUID, bootID)
	podUID := r.Observation.PodUID
	containerID := r.Observation.ContainerID
	if containerID == "" && validContainerID(resMeta.ContainerID) {
		containerID = resMeta.ContainerID
	}

	proof := ReclamationProof{
		SchemaVersion: 1,
		InstanceID:    identity.InstanceID,
		Generation:    generation,
		BootID:        bootID,
		Reclaimed:     false,
		PodUID:        podUID,
		ContainerID:   containerID,
		GPUEpoch:      resMeta.GPUEpoch,
	}

	if r.Phase != "stopped" {
		proof.Reason = "INSTANCE_NOT_STOPPED"
		return proof, nil
	}
	if !validUID(podUID) {
		proof.Reason = "MISSING_TARGET_POD_UID"
		return proof, nil
	}
	if !validContainerID(containerID) {
		proof.Reason = "MISSING_TARGET_CONTAINER_ID"
		return proof, nil
	}

	_, podErr := j.Client.CoreV1().Pods(matchedProfile.Namespace).Get(ctx, runName(r.Run), metav1.GetOptions{})
	if podErr == nil {
		terminated := false
		proof.ContainerTerminated = &terminated
		proof.Reason = "POD_STILL_PRESENT"
		return proof, nil
	}
	if !apierrors.IsNotFound(podErr) {
		proof.Reason = "KUBE_POD_QUERY_FAILED"
		return proof, nil
	}

	if j.ReclamationVerifier == nil {
		proof.Reason = "RECLAMATION_VERIFIER_UNCONFIGURED"
		return proof, nil
	}

	nodeUID := resMeta.NodeUID
	if nodeUID == "" {
		if node, nErr := j.Client.CoreV1().Nodes().Get(ctx, matchedProfile.NodeName, metav1.GetOptions{}); nErr == nil && node != nil && validUID(string(node.UID)) {
			nodeUID = string(node.UID)
		}
	}
	gpuUUIDs := appendUniqueNonEmpty(nil, resMeta.GPUUUIDs...)
	if len(gpuUUIDs) == 0 {
		gpuUUIDs = queryNodeCardGPUUUIDs(ctx, conn, j.StationID, matchedProfile.NodeName, nodeUID)
	}
	if len(gpuUUIDs) == 0 {
		gpuUUIDs = []string{""}
	}

	allExited := true
	allVerified := true
	unkillableD := false
	totalGPUProcs := 0
	survivingSet := map[int]bool{}
	var earliestSampledAt time.Time

	for _, gpuUUID := range gpuUUIDs {
		vt, vErr := j.ReclamationVerifier.VerifyTerminated(ctx, scheduler.VerifyTerminatedRequest{
			NodeUID:     nodeUID,
			GPUUUID:     gpuUUID,
			PodUID:      podUID,
			ContainerID: containerID,
			BootID:      bootID,
			GPUEpoch:    resMeta.GPUEpoch,
			Mode:        "container_exited",
		})
		if vErr != nil {
			proof.Reason = "NODE_VERIFIER_UNAVAILABLE"
			return proof, nil
		}
		if !vt.ContainerExited {
			allExited = false
		}
		if !vt.Verified {
			allVerified = false
		}
		if vt.UnkillableDState {
			unkillableD = true
		}
		if vt.GPUProcessCount > 0 {
			totalGPUProcs += vt.GPUProcessCount
		}
		for _, pid := range vt.SurvivingPIDs {
			survivingSet[pid] = true
		}
		if vt.RawProof != nil {
			if rawTS, ok := vt.RawProof["sampled_at"].(string); ok && rawTS != "" {
				if parsed, pErr := time.Parse(time.RFC3339Nano, rawTS); pErr == nil {
					if earliestSampledAt.IsZero() || parsed.Before(earliestSampledAt) {
						earliestSampledAt = parsed
					}
				}
			}
		}
	}

	vramMiB, vramErr := queryAttributedCardVRAMMiB(ctx, conn, j.StationID, podUID, containerID, earliestSampledAt)
	if vramErr != nil {
		proof.Reason = "VRAM_LEDGER_QUERY_FAILED"
		return proof, nil
	}
	if totalGPUProcs > 0 && vramMiB == 0 {
		vramMiB = int64(totalGPUProcs)
	}

	survivingCount := len(survivingSet)
	if survivingCount == 0 && totalGPUProcs > 0 {
		survivingCount = totalGPUProcs
	}
	proof.ContainerTerminated = &allExited
	proof.SurvivingPIDs = &survivingCount
	proof.VRAMAllocatedMiB = &vramMiB

	switch {
	case !allExited:
		proof.Reason = "CONTAINER_NOT_EXITED"
	case unkillableD:
		proof.Reason = "UNKILLABLE_D_STATE_PROCESS"
	case survivingCount > 0 || totalGPUProcs > 0:
		proof.Reason = "SURVIVING_PROCESSES_PRESENT"
	case vramMiB > 0:
		proof.Reason = "VRAM_STILL_ATTRIBUTED"
	case !allVerified:
		proof.Reason = "TERMINATION_NOT_VERIFIED"
	default:
		proof.Reclaimed = true
	}
	return proof, nil
}
