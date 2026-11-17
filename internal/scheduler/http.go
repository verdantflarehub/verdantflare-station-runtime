package scheduler

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"
	"unicode/utf8"
)

const maxBodyBytes = 1 << 20

// DecodeJSONObject validates a bounded UTF-8 JSON object with no duplicate keys,
// max nesting depth 32, DisallowUnknownFields(), and no trailing tokens, while
// permitting explicit null / zero values on optional or pointer fields.
func DecodeJSONObject(r io.Reader, limit int64, dst any) error {
	if limit <= 0 {
		return ErrInvalidRequest
	}
	b, err := io.ReadAll(io.LimitReader(r, limit+1))
	if err != nil || int64(len(b)) > limit || !utf8.Valid(b) {
		return ErrInvalidRequest
	}
	scan := json.NewDecoder(bytes.NewReader(b))
	scan.UseNumber()
	first, err := scan.Token()
	if err != nil || first != json.Delim('{') {
		return ErrInvalidRequest
	}
	if err := scanJSONObjectRest(scan, 0); err != nil {
		return ErrInvalidRequest
	}
	if _, err := scan.Token(); err != io.EOF {
		return ErrInvalidRequest
	}
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		return ErrInvalidRequest
	}
	if _, err := dec.Token(); err != io.EOF {
		return ErrInvalidRequest
	}
	return nil
}

func scanJSONObjectRest(d *json.Decoder, depth int) error {
	if depth > 32 {
		return ErrInvalidRequest
	}
	seen := make(map[string]bool)
	for d.More() {
		key, err := d.Token()
		if err != nil {
			return ErrInvalidRequest
		}
		name, ok := key.(string)
		if !ok || seen[name] {
			return ErrInvalidRequest
		}
		seen[name] = true
		if err := scanJSONValue(d, depth+1); err != nil {
			return err
		}
	}
	end, err := d.Token()
	if err != nil || end != json.Delim('}') {
		return ErrInvalidRequest
	}
	return nil
}

func scanJSONValue(d *json.Decoder, depth int) error {
	if depth > 32 {
		return ErrInvalidRequest
	}
	t, err := d.Token()
	if err != nil {
		return ErrInvalidRequest
	}
	switch t {
	case json.Delim('{'):
		return scanJSONObjectRest(d, depth)
	case json.Delim('['):
		for d.More() {
			if err := scanJSONValue(d, depth+1); err != nil {
				return err
			}
		}
		end, err := d.Token()
		if err != nil || end != json.Delim(']') {
			return ErrInvalidRequest
		}
	default:
		if num, ok := t.(json.Number); ok {
			if _, err := strconv.ParseFloat(num.String(), 64); err != nil {
				return ErrInvalidRequest
			}
		}
		if _, ok := t.(json.Delim); ok {
			return ErrInvalidRequest
		}
	}
	return nil
}

type Handler struct {
	Service *Service
}

func (h Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if h.Service == nil {
		writeJSONError(w, http.StatusServiceUnavailable, "SCHEDULER_UNAVAILABLE", "GPU scheduler is not initialized")
		return
	}
	caller, err := h.Service.Callers.Authenticate(r.Header.Get("Authorization"))
	if err != nil {
		writeMappedError(w, err)
		return
	}

	path := r.URL.Path
	switch {
	case r.Method == http.MethodPost && path == "/internal/v1/gpu-scheduler/leases/acquire":
		var req AcquireRequest
		if DecodeJSONObject(r.Body, maxBodyBytes, &req) != nil {
			writeMappedError(w, ErrInvalidRequest)
			return
		}
		if runNonce := strings.TrimSpace(r.Header.Get("X-Verdant-Run-Nonce")); runNonce != "" && req.RunNonce == "" {
			req.RunNonce = runNonce
		}
		dec, err := h.Service.Acquire(r.Context(), caller, req)
		if err != nil {
			writeMappedError(w, err)
			return
		}
		status := http.StatusOK
		if dec.State == "queued" || dec.State == "draining" {
			status = http.StatusAccepted
		}
		writeJSON(w, status, dec)

	case r.Method == http.MethodGet && path == "/internal/v1/gpu-scheduler/leases":
		leases, err := h.Service.ListLeases(r.Context(), caller, r.URL.Query().Get("holder_app_id"))
		if err != nil {
			writeMappedError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"leases": leases})

	case strings.HasPrefix(path, "/internal/v1/gpu-scheduler/leases/"):
		tail := strings.TrimPrefix(path, "/internal/v1/gpu-scheduler/leases/")
		parts := strings.Split(tail, "/")
		if len(parts) == 1 && r.Method == http.MethodGet {
			dec, err := h.Service.GetLease(r.Context(), caller, parts[0])
			if err != nil {
				writeMappedError(w, err)
				return
			}
			writeJSON(w, http.StatusOK, dec)
			return
		}
		if len(parts) == 2 && r.Method == http.MethodPost {
			leaseID, action := parts[0], parts[1]
			switch action {
			case "heartbeat", "downgrade":
				var req HeartbeatRequest
				if DecodeJSONObject(r.Body, maxBodyBytes, &req) != nil {
					writeMappedError(w, ErrInvalidRequest)
					return
				}
				if runNonce := strings.TrimSpace(r.Header.Get("X-Verdant-Run-Nonce")); runNonce != "" && req.RunNonce == "" {
					req.RunNonce = runNonce
				}
				var dec LeaseDecision
				if action == "downgrade" {
					dec, err = h.Service.Downgrade(r.Context(), caller, leaseID, req)
				} else {
					dec, err = h.Service.Heartbeat(r.Context(), caller, leaseID, req)
				}
				if err != nil {
					writeMappedError(w, err)
					return
				}
				writeJSON(w, http.StatusOK, dec)
				return

			case "bind-target":
				var req BindTargetRequest
				if DecodeJSONObject(r.Body, maxBodyBytes, &req) != nil {
					writeMappedError(w, ErrInvalidRequest)
					return
				}
				if runNonce := strings.TrimSpace(r.Header.Get("X-Verdant-Run-Nonce")); runNonce != "" && req.RunNonce == "" {
					req.RunNonce = runNonce
				}
				dec, err := h.Service.BindTarget(r.Context(), caller, leaseID, req)
				if err != nil {
					writeMappedError(w, err)
					return
				}
				writeJSON(w, http.StatusOK, dec)
				return

			case "release":
				var req ReleaseRequest
				if DecodeJSONObject(r.Body, maxBodyBytes, &req) != nil {
					writeMappedError(w, ErrInvalidRequest)
					return
				}
				if runNonce := strings.TrimSpace(r.Header.Get("X-Verdant-Run-Nonce")); runNonce != "" && req.RunNonce == "" {
					req.RunNonce = runNonce
				}
				dec, err := h.Service.Release(r.Context(), caller, leaseID, req)
				if err != nil {
					writeMappedError(w, err)
					return
				}
				writeJSON(w, http.StatusOK, dec)
				return
			}
		}
		writeJSONError(w, http.StatusNotFound, "NOT_FOUND", "unknown lease route")

	case r.Method == http.MethodPost && path == "/internal/v1/gpu-scheduler/residency/report":
		var req ResidencyReportRequest
		if DecodeJSONObject(r.Body, maxBodyBytes, &req) != nil {
			writeMappedError(w, ErrInvalidRequest)
			return
		}
		if runNonce := strings.TrimSpace(r.Header.Get("X-Verdant-Run-Nonce")); runNonce != "" && req.RunNonce == "" {
			req.RunNonce = runNonce
		}
		if err := h.Service.ReportResidency(r.Context(), caller, req); err != nil {
			writeMappedError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"ok": true})

	case strings.HasPrefix(path, "/internal/v1/gpu-scheduler/residency/"):
		tail := strings.TrimPrefix(path, "/internal/v1/gpu-scheduler/residency/")
		parts := strings.Split(tail, "/")
		if len(parts) == 2 && r.Method == http.MethodPost && strings.TrimSpace(parts[0]) != "" {
			instanceID, action := strings.TrimSpace(parts[0]), parts[1]
			switch action {
			case "evict":
				var req ResidencyEvictRequest
				if DecodeJSONObject(r.Body, maxBodyBytes, &req) != nil {
					writeMappedError(w, ErrInvalidRequest)
					return
				}
				if runNonce := strings.TrimSpace(r.Header.Get("X-Verdant-Run-Nonce")); runNonce != "" && req.RunNonce == "" {
					req.RunNonce = runNonce
				}
				res, err := h.Service.EvictResidency(r.Context(), caller, instanceID, req)
				if err != nil {
					writeMappedError(w, err)
					return
				}
				writeJSON(w, http.StatusOK, res)
				return

			case "force-drain":
				var req ResidencyForceDrainRequest
				if DecodeJSONObject(r.Body, maxBodyBytes, &req) != nil {
					writeMappedError(w, ErrInvalidRequest)
					return
				}
				if runNonce := strings.TrimSpace(r.Header.Get("X-Verdant-Run-Nonce")); runNonce != "" && req.RunNonce == "" {
					req.RunNonce = runNonce
				}
				res, err := h.Service.ForceDrainResidency(r.Context(), caller, instanceID, req)
				if err != nil {
					writeMappedError(w, err)
					return
				}
				writeJSON(w, http.StatusOK, res)
				return

			case "prune-stopped":
				var req ResidencyPruneStoppedRequest
				if DecodeJSONObject(r.Body, maxBodyBytes, &req) != nil {
					writeMappedError(w, ErrInvalidRequest)
					return
				}
				if runNonce := strings.TrimSpace(r.Header.Get("X-Verdant-Run-Nonce")); runNonce != "" && req.RunNonce == "" {
					req.RunNonce = runNonce
				}
				res, err := h.Service.PruneStoppedResidency(r.Context(), caller, instanceID, req)
				if err != nil {
					writeMappedError(w, err)
					return
				}
				writeJSON(w, http.StatusOK, res)
				return
			}
		}
		writeJSONError(w, http.StatusNotFound, "NOT_FOUND", "unknown residency route")

	case r.Method == http.MethodPost && path == "/internal/v1/gpu-scheduler/telemetry/report":
		var rep NodeTelemetryReport
		if DecodeJSONObject(r.Body, maxBodyBytes, &rep) != nil {
			writeMappedError(w, ErrInvalidRequest)
			return
		}
		if err := h.Service.ReportNodeTelemetry(r.Context(), caller, rep); err != nil {
			writeMappedError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"ok": true})

	case r.Method == http.MethodPost && path == "/internal/v1/gpu-scheduler/admin/admission-mode":
		var req AdmissionModeRequest
		if DecodeJSONObject(r.Body, maxBodyBytes, &req) != nil {
			writeMappedError(w, ErrInvalidRequest)
			return
		}
		if runNonce := strings.TrimSpace(r.Header.Get("X-Verdant-Run-Nonce")); runNonce != "" && req.RunNonce == "" {
			req.RunNonce = runNonce
		}
		if err := h.Service.SetAdmissionMode(r.Context(), caller, req); err != nil {
			writeMappedError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"ok": true, "admission_mode": req.AdmissionMode})

	case r.Method == http.MethodPost && path == "/internal/v1/gpu-scheduler/admin/reconcile":
		if caller.Kind != CallerAdmin {
			writeMappedError(w, ErrForbiddenCaller)
			return
		}
		var req struct {
			NodeUID     string          `json:"node_uid,omitempty"`
			LivePodUIDs map[string]bool `json:"live_pod_uids,omitempty"`
			RunNonce    string          `json:"run_nonce,omitempty"`
		}
		if DecodeJSONObject(r.Body, maxBodyBytes, &req) != nil {
			writeMappedError(w, ErrInvalidRequest)
			return
		}
		if err := h.Service.ReconcileRestart(r.Context(), RestartObservation{
			NodeUID:     req.NodeUID,
			LivePodUIDs: req.LivePodUIDs,
			RunNonce:    req.RunNonce,
		}); err != nil {
			writeMappedError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"ok": true})

	case r.Method == http.MethodGet && path == "/internal/v1/gpu-scheduler/snapshot":
		if caller.Kind != CallerAdmin && caller.AppID != "station-core" && caller.AppID != "app-resources" {
			writeMappedError(w, ErrForbiddenCaller)
			return
		}
		snap, err := h.Service.Snapshot(r.Context())
		if err != nil {
			writeMappedError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, snap)

	default:
		writeJSONError(w, http.StatusNotFound, "NOT_FOUND", "unknown GPU scheduler endpoint")
	}
}

func writeMappedError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, ErrUnauthenticated):
		writeJSONError(w, http.StatusUnauthorized, "UNAUTHENTICATED", "missing or invalid scheduler bearer token")
	case errors.Is(err, ErrForbiddenCaller):
		writeJSONError(w, http.StatusForbidden, "CALLER_IDENTITY_MISMATCH", "caller identity is not authorized for this operation")
	case errors.Is(err, ErrInvalidRequest):
		writeJSONError(w, http.StatusBadRequest, "INVALID_REQUEST", "malformed or invalid scheduler request")
	case errors.Is(err, ErrConfirmationRequired):
		writeJSONError(w, http.StatusBadRequest, "CONFIRMATION_REQUIRED", "confirm_force_drain=true and exact confirm_instance_id match are required")
	case errors.Is(err, ErrLeaseNotFound):
		writeJSONError(w, http.StatusNotFound, "LEASE_NOT_FOUND", "GPU resource lease not found")
	case errors.Is(err, ErrResidencyNotFound):
		writeJSONError(w, http.StatusNotFound, "RESIDENCY_NOT_FOUND", "GPU residency instance not found")
	case errors.Is(err, ErrGUIEditLeaseHeld):
		writeJSONError(w, http.StatusConflict, "GUI_EDIT_LEASE_HELD", "instance is protected by an active human GUI/viewport session")
	case errors.Is(err, ErrInstanceActiveCompute):
		writeJSONError(w, http.StatusConflict, "INSTANCE_ACTIVE_COMPUTE", "instance holds an active compute lease and cannot be soft-evicted")
	case errors.Is(err, ErrAlreadyZeroVRAM):
		writeJSONError(w, http.StatusConflict, "ALREADY_ZERO_VRAM", "instance is already in zero-VRAM standby")
	case errors.Is(err, ErrInstanceNotEvictable):
		writeJSONError(w, http.StatusConflict, "INSTANCE_NOT_EVICTABLE", "instance is not in an idle evictable state")
	case errors.Is(err, ErrIdempotencyConflict):
		writeJSONError(w, http.StatusConflict, "IDEMPOTENCY_CONFLICT", "idempotency_key reused with different request digest")
	case errors.Is(err, ErrVersionConflict):
		writeJSONError(w, http.StatusConflict, "LEASE_VERSION_CONFLICT", "stale version or out-of-order residency sequence")
	case errors.Is(err, ErrFencingRevoked):
		writeJSONError(w, http.StatusConflict, "FENCING_TOKEN_REVOKED", "fencing_token has been revoked or lease expired")
	case errors.Is(err, ErrLeaseInactive):
		writeJSONError(w, http.StatusConflict, "LEASE_NOT_ACTIVE", "lease is not in an active compute state")
	case errors.Is(err, ErrWorkerNotSettled):
		writeJSONError(w, http.StatusConflict, "WORKER_NOT_SETTLED", "worker_settled=true is required before release or downgrade")
	case errors.Is(err, ErrDowngradeDisabled):
		writeJSONError(w, http.StatusConflict, "LOCK_DOWNGRADE_DISABLED_BEFORE_CUTOVER", "P2->P3 lock downgrade is disabled until cutover completes")
	case errors.Is(err, ErrCrossCardHoldAndWait):
		writeJSONError(w, http.StatusConflict, "CROSS_CARD_HOLD_AND_WAIT_FORBIDDEN", "cannot acquire cross-card lease while holding an un-released source lease")
	case errors.Is(err, ErrCutoverBlocked):
		writeJSONError(w, http.StatusConflict, "CUTOVER_BLOCKED", err.Error())
	case errors.Is(err, ErrTopologyUnsatisfied):
		writeJSONError(w, http.StatusUnprocessableEntity, "TOPOLOGY_UNSATISFIABLE", "requested GPU topology cannot be satisfied on node")
	case errors.Is(err, ErrNodeStateUnknown):
		writeJSONError(w, http.StatusServiceUnavailable, "UNKNOWN_NODE_STATE", "node GPU verification unavailable; fail-closed reconciling")
	case errors.Is(err, ErrReclaimQuarantined):
		writeJSONError(w, http.StatusServiceUnavailable, "RECLAIM_FAILED_QUARANTINED", "GPU worker has not released VRAM or entered unkillable D-state")
	default:
		writeJSONError(w, http.StatusInternalServerError, "INTERNAL_ERROR", "scheduler internal error")
	}
}

func writeJSONError(w http.ResponseWriter, status int, code, msg string) {
	writeJSON(w, status, map[string]any{
		"error_code": code,
		"message":    msg,
	})
}

func writeJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(payload)
}
