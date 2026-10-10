package instances

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"regexp"
)

type StartupProof struct {
	WorkerSource
	StartOperationID  string `json:"start_operation_id"`
	CreateOperationID string `json:"create_operation_id"`
	InstanceID        string `json:"instance_id"`
	ProjectID         string `json:"project_id"`
	PodUID            string `json:"pod_uid"`
}

type RunningProof struct {
	Startup    StartupProof `json:"startup"`
	Generation string       `json:"generation"`
	GPUUUID    string       `json:"gpu_uuid"`
}

type WorkerObserver interface {
	ObserveWorker(context.Context, Identity, string, WorkerSource, WorkerObservation, string) (*RunningProof, error)
}

// ObserveWorker validates authenticated process evidence, never inferring a
// loaded scene from PodReady or from resource requests. Tokens are call-local.
func (o HTTPObserver) ObserveWorker(ctx context.Context, in Identity, startID string, source WorkerSource, worker WorkerObservation, token string) (*RunningProof, error) {
	if !worker.PodReady || worker.PodUID == "" {
		return nil, nil
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, worker.ControlEndpoint+"/internal/status", nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+token)
	client := o.Client
	if client == nil {
		client = InternalHTTPClient()
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusServiceUnavailable {
		return nil, nil
	}
	if resp.StatusCode != http.StatusOK || resp.Header.Get("Content-Encoding") != "" {
		return nil, errors.New("WORKER_STATUS_UNAVAILABLE")
	}
	raw, err := io.ReadAll(io.LimitReader(resp.Body, (32<<10)+1))
	if err != nil || len(raw) > 32<<10 {
		return nil, ErrBinding
	}
	var result struct {
		Status     string        `json:"status"`
		InstanceID string        `json:"instance_id"`
		Generation string        `json:"generation"`
		GUIHeld    *bool         `json:"gui_held"`
		Startup    *StartupProof `json:"startup"`
		Binding    struct {
			PodUID    string `json:"pod_uid"`
			PodName   string `json:"pod_name"`
			Namespace string `json:"namespace"`
			Container string `json:"container"`
			GPUUUID   string `json:"gpu_uuid"`
		} `json:"resource_binding"`
	}
	if json.Unmarshal(raw, &result) != nil || result.Status != "ready" || result.InstanceID != in.InstanceID ||
		!regexp.MustCompile(`^[0-9a-f]{32}$`).MatchString(result.Generation) || result.GUIHeld == nil || *result.GUIHeld || result.Startup == nil {
		return nil, ErrBinding
	}
	var envelope struct {
		Startup json.RawMessage `json:"startup"`
	}
	if json.Unmarshal(raw, &envelope) != nil || decodeRequest(envelope.Startup, result.Startup) != nil {
		return nil, ErrBinding
	}
	b, proof := result.Binding, *result.Startup
	if b.PodUID != worker.PodUID || b.PodName != worker.PodName || b.Namespace != worker.Namespace || b.Container != "blender" ||
		!regexp.MustCompile(`^GPU-[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$`).MatchString(b.GPUUUID) ||
		proof.WorkerSource != source || proof.StartOperationID != startID || proof.CreateOperationID != in.CreateOperationID ||
		proof.InstanceID != in.InstanceID || proof.ProjectID != in.ProjectID || proof.PodUID != worker.PodUID {
		return nil, ErrBinding
	}
	return &RunningProof{Startup: proof, Generation: result.Generation, GPUUUID: b.GPUUUID}, nil
}
