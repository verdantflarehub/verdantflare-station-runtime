package instances

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/google/uuid"
)

func TestSealRequiresAuthenticatedExactFrozenEvidence(t *testing.T) {
	in := StopRequest{InstanceID: uuid.NewString(), ProjectID: uuid.NewString(), StartOperationID: uuid.NewString(), OperationID: uuid.NewString(), Generation: strings.Repeat("a", 32), AssetID: "checkpoints/checkpoint-4-" + strings.Repeat("a", 32) + ".blend", SHA256: strings.Repeat("b", 64), Size: 123, SceneVersion: 4}
	worker := WorkerObservation{PodUID: uuid.NewString()}
	valid := func() map[string]any {
		return map[string]any{"instance_id": in.InstanceID, "project_id": in.ProjectID, "start_operation_id": in.StartOperationID, "operation_id": in.OperationID, "generation": in.Generation, "pod_uid": worker.PodUID, "phase": "sealed", "frozen": true, "proof": in.proof()}
	}
	result := valid()
	status := 200
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" || r.URL.Path != "/internal/drain" || r.Header.Get("Authorization") != "Bearer private-token" {
			t.Error("invalid seal request")
		}
		var body map[string]string
		if json.NewDecoder(r.Body).Decode(&body) != nil || len(body) != 5 || body["action"] != "seal" || body["operation_id"] != in.OperationID || body["start_operation_id"] != in.StartOperationID || body["generation"] != in.Generation || body["instance_id"] != in.InstanceID {
			t.Error("wrong execution seal")
		}
		w.WriteHeader(status)
		_ = json.NewEncoder(w).Encode(result)
	}))
	defer server.Close()
	worker.ControlEndpoint = server.URL
	o := HTTPObserver{Client: server.Client()}
	if err := o.Seal(context.Background(), in, worker, "private-token"); err != nil {
		t.Fatal(err)
	}
	for _, field := range []string{"instance_id", "project_id", "start_operation_id", "operation_id", "generation", "pod_uid", "phase", "frozen", "proof"} {
		t.Run(field, func(t *testing.T) {
			result = valid()
			delete(result, field)
			if o.Seal(context.Background(), in, worker, "private-token") == nil {
				t.Fatal("missing evidence accepted")
			}
		})
	}
	for _, mutate := range []func(map[string]any){func(v map[string]any) { v["frozen"] = false }, func(v map[string]any) { v["phase"] = "captured" }, func(v map[string]any) { p := in.proof(); p.Size++; v["proof"] = p }, func(v map[string]any) { v["pod_uid"] = uuid.NewString() }, func(v map[string]any) { p := in.proof(); p.SceneVersion++; v["proof"] = p }} {
		result = valid()
		mutate(result)
		if o.Seal(context.Background(), in, worker, "private-token") == nil {
			t.Fatal("mismatched evidence accepted")
		}
	}
	result = valid()
	status = 503
	if o.Seal(context.Background(), in, worker, "private-token") == nil {
		t.Fatal("failed seal accepted")
	}
}
