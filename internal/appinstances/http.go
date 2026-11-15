package appinstances

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"mime"
	"net/http"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/verdantflarehub/verdantflare-station-runtime/internal/strictjson"
)

type Handler struct {
	StationID         string
	Profiles          map[string]Profile
	Callers           AccessRegistry
	Execute           func(context.Context, Caller, Run, Profile) (StartRecord, error)
	Access            func(context.Context, Caller, Run, Profile) (WorkerAccess, error)
	Stop              func(context.Context, Caller, Run, Profile, string, string) (StopRecord, error)
	UpgradeStop       func(context.Context, Caller, Run, Profile, string, string) (StopRecord, error)
	RepairStop        func(context.Context, Caller, Run, Profile, RepairStopOptions) (StopRecord, error)
	ReclamationStatus func(context.Context, Caller, Identity, map[string]Profile, int64, string) (ReclamationProof, error)
}

type StartInput struct {
	InstallationID string `json:"installation_id"`
	InstanceID     string `json:"instance_id"`
	OperationID    string `json:"operation_id"`
	Generation     int64  `json:"generation"`
	AppVersion     string `json:"app_version"`
	ProfileID      string `json:"profile_id"`
}

type StopInput struct {
	StartInput
	StopOperationID string `json:"stop_operation_id"`
	BootID          string `json:"boot_id"`
}

type stopWireInput struct {
	StopInput
	Reason                        string `json:"reason,omitempty"`
	CallerOperationID             string `json:"caller_operation_id,omitempty"`
	ForceAfterDrainTimeoutSeconds *int   `json:"force_after_drain_timeout_seconds,omitempty"`
}

func validReclamationProof(v ReclamationProof, in ReclamationStatusInput) bool {
	if v.SchemaVersion != 1 || v.InstanceID != in.InstanceID || v.Generation != in.Generation || v.BootID != in.BootID || v.GPUEpoch < 0 {
		return false
	}
	if (v.PodUID != "" && !validUID(v.PodUID)) || (v.ContainerID != "" && !validContainerID(v.ContainerID)) {
		return false
	}
	if v.Reclaimed {
		if !validUID(v.PodUID) || !validContainerID(v.ContainerID) ||
			v.ContainerTerminated == nil || !*v.ContainerTerminated ||
			v.SurvivingPIDs == nil || *v.SurvivingPIDs != 0 ||
			v.VRAMAllocatedMiB == nil || *v.VRAMAllocatedMiB != 0 {
			return false
		}
	}
	return true
}

func jsonReply(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func failure(w http.ResponseWriter, status int, code string) {
	jsonReply(w, status, map[string]string{"code": code})
}

func mapHandlerError(w http.ResponseWriter, e error) {
	switch {
	case errors.Is(e, ErrDenied):
		failure(w, 403, "PERMISSION_DENIED")
	case errors.Is(e, ErrInvalid):
		failure(w, 400, "INVALID_ARGUMENT")
	case errors.Is(e, ErrUpgradeDeferred):
		failure(w, 409, "INSTANCE_UPGRADE_DEFERRED_PROTECTED")
	case errors.Is(e, ErrBusy):
		failure(w, 409, "INSTANCE_BUSY")
	case errors.Is(e, ErrCapacity):
		failure(w, 409, "INSTANCE_CAPACITY_UNAVAILABLE")
	case errors.Is(e, ErrNotReady):
		failure(w, 409, "INSTANCE_NOT_RUNNING")
	case errors.Is(e, ErrConflict), errors.Is(e, ErrBinding):
		failure(w, 409, "INSTANCE_BINDING_CONFLICT")
	case errors.Is(e, pgx.ErrNoRows):
		failure(w, 404, "NOT_FOUND")
	default:
		failure(w, 503, "INSTANCE_SERVICE_UNAVAILABLE")
	}
}

func (h Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	isAccess := r.URL.Path == "/internal/v1/app-instances/access"
	isStop := r.URL.Path == "/internal/v1/app-instances/stop"
	isReclamationStatus := r.URL.Path == "/internal/v1/app-instances/reclamation-status"
	if r.Method != http.MethodPost || (!isAccess && !isStop && !isReclamationStatus && r.URL.Path != "/internal/v1/app-instances/start") {
		failure(w, 404, "NOT_FOUND")
		return
	}
	caller, ok := h.Callers[sha256.Sum256([]byte(r.Header.Get("Authorization")))]
	if !ok || len(r.Header.Values("Authorization")) != 1 {
		failure(w, 401, "UNAUTHENTICATED")
		return
	}
	for _, name := range []string{"X-Station-Id", "X-User-Id", "X-Organization-Id"} {
		if len(r.Header.Values(name)) != 1 || !validID(r.Header.Get(name)) {
			failure(w, 400, "INVALID_ARGUMENT")
			return
		}
	}
	if r.Header.Get("X-Station-Id") != h.StationID {
		failure(w, 403, "PERMISSION_DENIED")
		return
	}
	media, _, e := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if e != nil || media != "application/json" || len(r.Header.Values("Content-Type")) != 1 ||
		r.URL.RawPath != "" || r.URL.RawQuery != "" || r.URL.ForceQuery || r.Header.Get("Content-Encoding") != "" ||
		len(r.TransferEncoding) != 0 || r.ContentLength > 4096 {
		failure(w, 400, "INVALID_ARGUMENT")
		return
	}
	if isReclamationStatus {
		var recInput ReclamationStatusInput
		if strictjson.Decode(r.Body, 4096, &recInput) != nil ||
			!validID(recInput.InstallationID) || !validID(recInput.InstanceID) ||
			recInput.Generation < 1 || !validBoot(recInput.BootID) {
			failure(w, 400, "INVALID_ARGUMENT")
			return
		}
		callerProfiles := map[string]Profile{}
		for k, cand := range h.Profiles {
			if cand.AppID == caller.AppID && cand.Valid() {
				callerProfiles[k] = cand
			}
		}
		if len(callerProfiles) == 0 {
			failure(w, 403, "PROFILE_NOT_ALLOWED")
			return
		}
		if h.ReclamationStatus == nil || caller.Authority == nil {
			failure(w, 503, "SERVICE_UNAVAILABLE")
			return
		}
		identity := Identity{
			StationID:      h.StationID,
			UserID:         r.Header.Get("X-User-Id"),
			OrganizationID: r.Header.Get("X-Organization-Id"),
			InstallationID: recInput.InstallationID,
			InstanceID:     recInput.InstanceID,
		}
		ctx, cancel := context.WithTimeout(r.Context(), 20*time.Second)
		defer cancel()
		proof, rErr := h.ReclamationStatus(ctx, caller, identity, callerProfiles, recInput.Generation, recInput.BootID)
		if rErr != nil {
			mapHandlerError(w, rErr)
			return
		}
		if !validReclamationProof(proof, recInput) {
			failure(w, 503, "INVALID_RECLAMATION_RESULT")
			return
		}
		jsonReply(w, 200, proof)
		return
	}
	var input StartInput
	var stopWire stopWireInput
	isRepairStop := false
	if isStop {
		e = strictjson.Decode(r.Body, 4096, &stopWire)
		input = stopWire.StartInput
		if !validID(stopWire.StopOperationID) || stopWire.StopOperationID == input.OperationID || !validBoot(stopWire.BootID) {
			e = ErrInvalid
		}
		hasRepairFields := stopWire.Reason != "" || stopWire.CallerOperationID != "" || stopWire.ForceAfterDrainTimeoutSeconds != nil
		if hasRepairFields {
			if stopWire.Reason != "repair_restart" || !validID(stopWire.CallerOperationID) ||
				stopWire.ForceAfterDrainTimeoutSeconds == nil ||
				*stopWire.ForceAfterDrainTimeoutSeconds < 5 || *stopWire.ForceAfterDrainTimeoutSeconds > 120 ||
				r.Header.Get("X-Template-Upgrade") != "" {
				e = ErrInvalid
			} else {
				isRepairStop = true
			}
		}
	} else {
		e = strictjson.Decode(r.Body, 4096, &input)
	}
	if e != nil || !validID(input.InstallationID) || !validID(input.InstanceID) ||
		!validID(input.OperationID) || input.Generation < 1 || !version.MatchString(input.AppVersion) || !label(input.ProfileID) {
		failure(w, 400, "INVALID_ARGUMENT")
		return
	}
	p, ok := h.Profiles[caller.AppID+"@"+input.AppVersion+"/"+input.ProfileID]
	if !ok || p.AppID != caller.AppID || !p.Valid() {
		failure(w, 403, "PROFILE_NOT_ALLOWED")
		return
	}
	if (!isAccess && !isStop && h.Execute == nil) || (isAccess && h.Access == nil) ||
		(isStop && !isRepairStop && h.Stop == nil) || (isStop && isRepairStop && h.RepairStop == nil) ||
		caller.Authority == nil {
		failure(w, 503, "SERVICE_UNAVAILABLE")
		return
	}
	in := Run{Identity: Identity{StationID: h.StationID, UserID: r.Header.Get("X-User-Id"), OrganizationID: r.Header.Get("X-Organization-Id"),
		InstallationID: input.InstallationID, InstanceID: input.InstanceID}, OperationID: input.OperationID, Generation: input.Generation}
	ctx, cancel := context.WithTimeout(r.Context(), 20*time.Second)
	defer cancel()
	var result StartRecord
	var access WorkerAccess
	var stopped StopRecord
	if isStop {
		if isRepairStop {
			stopped, e = h.RepairStop(ctx, caller, in, p, RepairStopOptions{
				StopOperationID:               stopWire.StopOperationID,
				BootID:                        stopWire.BootID,
				CallerOperationID:             stopWire.CallerOperationID,
				ForceAfterDrainTimeoutSeconds: *stopWire.ForceAfterDrainTimeoutSeconds,
			})
		} else if r.Header.Get("X-Template-Upgrade") == "true" && h.UpgradeStop != nil {
			stopped, e = h.UpgradeStop(ctx, caller, in, p, stopWire.StopOperationID, stopWire.BootID)
		} else {
			stopped, e = h.Stop(ctx, caller, in, p, stopWire.StopOperationID, stopWire.BootID)
		}
	} else if isAccess {
		access, e = h.Access(ctx, caller, in, p)
	} else {
		result, e = h.Execute(ctx, caller, in, p)
	}
	if e != nil {
		mapHandlerError(w, e)
		return
	}
	if isStop {
		_, known := stopPhases[stopped.Progress.Phase]
		proof := stopped.Progress.Proof
		proofValid := stopped.Progress.Phase == "accepted" || (validUID(proof.Binding.PodUID) &&
			proof == stopProof(in, p, Observation{PodUID: proof.Binding.PodUID, BootID: stopWire.BootID}, stopWire.StopOperationID))
		if stopped.OperationID != stopWire.StopOperationID || stopped.StartID != in.OperationID || stopped.BootID != stopWire.BootID ||
			stopped.Revision < 1 || (!known && stopped.Progress.Phase != "accepted") || !proofValid {
			failure(w, 503, "INVALID_STOP_RESULT")
			return
		}
		status, code := "running", 202
		if stopped.Progress.Phase == "stopped" {
			status, code = "succeeded", 200
		}
		jsonReply(w, code, map[string]any{"schema_version": 1, "operation_id": stopped.OperationID, "start_operation_id": in.OperationID,
			"instance_id": in.InstanceID, "generation": in.Generation, "action": "stop", "status": status, "phase": stopped.Progress.Phase})
		return
	}
	if isAccess {
		if !access.valid(in, p) {
			failure(w, 503, "INVALID_INSTANCE_ACCESS")
			return
		}
		jsonReply(w, 200, access)
		return
	}
	if result.Run != in || result.ProfileHash != p.Hash() || result.InstallationRevision < 1 ||
		(result.Phase != "accepted" && result.Phase != "launching" && result.Phase != "running") ||
		(result.Phase == "running" && (!result.Observation.valid() || !result.Observation.PodReady || !validBoot(result.Observation.BootID))) {
		failure(w, 503, "INVALID_START_RESULT")
		return
	}
	status, code := "running", 202
	if result.Phase == "running" {
		status, code = "succeeded", 200
	}
	jsonReply(w, code, map[string]any{"schema_version": 1, "operation_id": in.OperationID, "instance_id": in.InstanceID,
		"generation": in.Generation, "action": "start", "status": status, "phase": result.Phase})
}
