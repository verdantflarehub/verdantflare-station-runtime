package instances

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
)

func (o HTTPObserver) Seal(ctx context.Context, in StopRequest, worker WorkerObservation, token string) error {
	body, _ := json.Marshal(map[string]string{"instance_id": in.InstanceID, "start_operation_id": in.StartOperationID,
		"operation_id": in.OperationID, "generation": in.Generation, "action": "seal"})
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, worker.ControlEndpoint+"/internal/drain", bytes.NewReader(body))
	if err != nil {
		return err
	}
	request.Header.Set("Authorization", "Bearer "+token)
	request.Header.Set("Content-Type", "application/json")
	client := o.Client
	if client == nil {
		client = InternalHTTPClient()
	}
	response, err := client.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode != 200 || response.Header.Get("Content-Encoding") != "" {
		return ErrBinding
	}
	raw, err := io.ReadAll(io.LimitReader(response.Body, (16<<10)+1))
	if err != nil || len(raw) > 16<<10 {
		return ErrBinding
	}
	var envelope struct {
		InstanceID       string          `json:"instance_id"`
		ProjectID        string          `json:"project_id"`
		StartOperationID string          `json:"start_operation_id"`
		OperationID      string          `json:"operation_id"`
		Generation       string          `json:"generation"`
		PodUID           string          `json:"pod_uid"`
		Phase            string          `json:"phase"`
		Frozen           *bool           `json:"frozen"`
		Proof            json.RawMessage `json:"proof"`
	}
	if json.Unmarshal(raw, &envelope) != nil || envelope.InstanceID != in.InstanceID || envelope.ProjectID != in.ProjectID ||
		envelope.StartOperationID != in.StartOperationID || envelope.OperationID != in.OperationID || envelope.Generation != in.Generation ||
		envelope.PodUID != worker.PodUID || envelope.Phase != "sealed" || envelope.Frozen == nil || !*envelope.Frozen {
		return ErrBinding
	}
	var proof FrozenProof
	if decodeRequest(envelope.Proof, &proof) != nil || proof != in.proof() {
		return ErrBinding
	}
	return nil
}
