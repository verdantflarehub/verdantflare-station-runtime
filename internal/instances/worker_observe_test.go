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

func TestWorkerObservationRequiresMatchingLoadProcessAndGPU(t *testing.T) {
	in, _, _, _ := fixture()
	start := uuid.NewString()
	source := WorkerSource{SourceRevisionID: uuid.NewString(), SHA256: strings.Repeat("a", 64), Size: 123}
	worker := WorkerObservation{PodUID: uuid.NewString(), PodName: "blender-worker", Namespace: "blender-test", PodReady: true}
	valid := func() map[string]any {
		return map[string]any{"status": "ready", "instance_id": in.InstanceID, "generation": strings.Repeat("b", 32), "gui_held": false,
			"startup":          map[string]any{"start_operation_id": start, "create_operation_id": in.CreateOperationID, "instance_id": in.InstanceID, "project_id": in.ProjectID, "pod_uid": worker.PodUID, "source_revision_id": source.SourceRevisionID, "sha256": source.SHA256, "size": source.Size, "empty": false},
			"resource_binding": map[string]any{"pod_uid": worker.PodUID, "pod_name": worker.PodName, "namespace": worker.Namespace, "container": "blender", "gpu_uuid": "GPU-12345678-1234-4234-8234-123456789abc"}}
	}
	result := valid()
	status := 200
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if r.Header.Get("Authorization") != "Bearer private-token" || r.URL.Path != "/internal/status" {
			t.Error("invalid worker request")
		}
		w.WriteHeader(status)
		_ = json.NewEncoder(w).Encode(result)
	}))
	defer server.Close()
	worker.ControlEndpoint = server.URL
	observer := HTTPObserver{Client: server.Client()}
	proof, err := observer.ObserveWorker(context.Background(), in, start, source, worker, "private-token")
	if err != nil || proof == nil || proof.Startup.SHA256 != source.SHA256 {
		t.Fatal(proof, err)
	}
	for _, mutate := range []func(map[string]any){
		func(v map[string]any) { v["startup"] = nil }, func(v map[string]any) { v["gui_held"] = true }, func(v map[string]any) { delete(v, "gui_held") }, func(v map[string]any) { v["generation"] = "" },
		func(v map[string]any) { delete(v["startup"].(map[string]any), "empty") },
		func(v map[string]any) { v["startup"].(map[string]any)["start_operation_id"] = uuid.NewString() }, func(v map[string]any) { v["startup"].(map[string]any)["project_id"] = uuid.NewString() },
		func(v map[string]any) { v["startup"].(map[string]any)["sha256"] = strings.Repeat("c", 64) }, func(v map[string]any) { v["resource_binding"].(map[string]any)["pod_uid"] = uuid.NewString() },
		func(v map[string]any) { v["resource_binding"].(map[string]any)["container"] = "files" }, func(v map[string]any) { v["resource_binding"].(map[string]any)["gpu_uuid"] = "0" },
	} {
		result = valid()
		mutate(result)
		if proof, err = observer.ObserveWorker(context.Background(), in, start, source, worker, "private-token"); err == nil || proof != nil {
			t.Fatal("invalid readiness evidence accepted")
		}
	}
	result = valid()
	status = 503
	if proof, err = observer.ObserveWorker(context.Background(), in, start, source, worker, "private-token"); err != nil || proof != nil {
		t.Fatal("pending worker is not running")
	}
	prior := calls
	worker.PodReady = false
	if _, err = observer.ObserveWorker(context.Background(), in, start, source, worker, "private-token"); err != nil || calls != prior {
		t.Fatal("unready worker contacted")
	}
}

func TestStartRequestStrictDecoding(t *testing.T) {
	in := StartRequest{OperationID: uuid.NewString()}
	raw, _ := json.Marshal(in)
	var result StartRequest
	if err := decodeRequest(raw, &result); err != nil {
		t.Fatal(err)
	}
	for _, body := range []string{strings.TrimSuffix(string(raw), "}") + `,"empty":false}`, strings.Replace(string(raw), `"empty":false`, `"empty":null`, 1), strings.TrimSuffix(string(raw), "}") + `,"worker_token":"x"}`, strings.Replace(string(raw), `,"empty":false`, "", 1)} {
		if decodeRequest([]byte(body), &result) == nil {
			t.Fatal("ambiguous or incomplete start decoded")
		}
	}
}
