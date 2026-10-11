package appinstances

import (
	"bytes"
	"context"
	"encoding/json"
	"mime"
	"net/http"

	"github.com/verdantflarehub/verdantflare-station-runtime/internal/strictjson"
)

// WorkerStopProof is bound to the original process and independent stop operation.
// Sealed is an admission barrier, not evidence of Pod termination.
type WorkerStopProof struct {
	SchemaVersion    int             `json:"schema_version"`
	InstanceID       string          `json:"instance_id"`
	StartOperationID string          `json:"start_operation_id"`
	StopOperationID  string          `json:"stop_operation_id"`
	Generation       int64           `json:"generation"`
	BootID           string          `json:"boot_id"`
	Binding          ResourceBinding `json:"resource_binding"`
	State            string          `json:"state"`
}

type WorkerStopper interface {
	Seal(context.Context, Run, Profile, Observation, string, string) (WorkerStopProof, error)
}

type HTTPWorkerStopper struct{ Client *http.Client }

func stopProof(in Run, p Profile, observed Observation, operation string) WorkerStopProof {
	return WorkerStopProof{SchemaVersion: 1, InstanceID: in.InstanceID, StartOperationID: in.OperationID,
		StopOperationID: operation, Generation: in.Generation, BootID: observed.BootID,
		Binding: ResourceBinding{observed.PodUID, runName(in), p.Namespace, "worker"}, State: "sealed"}
}

func (s HTTPWorkerStopper) Seal(ctx context.Context, in Run, p Profile, observed Observation, token, operation string) (WorkerStopProof, error) {
	if !in.Identity.valid() || !validID(in.OperationID) || in.Generation < 1 || !p.Valid() || !observed.valid() ||
		observed.PodUID == "" || !validBoot(observed.BootID) || !credentialValid([]byte(token)) || !validID(operation) || operation == in.OperationID {
		return WorkerStopProof{}, ErrInvalid
	}
	expected := stopProof(in, p, observed, operation)
	raw, _ := json.Marshal(expected)
	// The request has no state assertion. Only the worker can supply sealed.
	var body map[string]json.RawMessage
	_ = json.Unmarshal(raw, &body)
	delete(body, "state")
	raw, _ = json.Marshal(body)
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, workerOrigin(in, p)+"/internal/v1/app-instance/stop-barrier", bytes.NewReader(raw))
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")
	resp, e := controlledRequest(ctx, s.Client, req)
	if e != nil {
		return WorkerStopProof{}, ErrUnavailable
	}
	defer resp.Body.Close()
	media, _, e := mime.ParseMediaType(resp.Header.Get("Content-Type"))
	if e != nil || media != "application/json" || resp.Header.Get("Content-Encoding") != "" || resp.Uncompressed {
		return WorkerStopProof{}, ErrUnavailable
	}
	if resp.StatusCode != http.StatusOK {
		var failure struct {
			Code string `json:"code"`
		}
		if strictjson.Decode(resp.Body, 4096, &failure) != nil {
			return WorkerStopProof{}, ErrUnavailable
		}
		if resp.StatusCode == http.StatusConflict && failure.Code == "INSTANCE_BUSY" {
			return WorkerStopProof{}, ErrBusy
		}
		if resp.StatusCode == http.StatusConflict {
			return WorkerStopProof{}, ErrBinding
		}
		return WorkerStopProof{}, ErrUnavailable
	}
	var proof WorkerStopProof
	if strictjson.Decode(resp.Body, 4096, &proof) != nil || proof != expected {
		return WorkerStopProof{}, ErrBinding
	}
	return proof, nil
}
