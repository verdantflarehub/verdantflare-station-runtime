package instances

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestPreparationProofBindsExactPodAndFixedContent(t *testing.T) {
	in := CreateRequest{OperationID: uuid.NewString(), InstanceID: uuid.NewString(), ProjectID: uuid.NewString(), SourceRevisionID: uuid.NewString(), SourceSize: 5, SourceSHA256: strings.Repeat("a", 64)}
	asset := "inbox/abcdefghijklmnop/restore.blend"
	proof := &PreparationProof{PrepareID: in.OperationID, InstanceID: in.InstanceID, ProjectID: in.ProjectID, SourceRevisionID: in.SourceRevisionID, Size: 5, SHA256: &in.SourceSHA256, AssetID: &asset, State: "prepared", PreparedAt: time.Now().UTC().Format(time.RFC3339Nano)}
	body := map[string]any{"instance_id": in.InstanceID, "pod_uid": "pod-a", "preparation": proof}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer test-only" || r.URL.Path != "/internal/workspace/status" {
			t.Error("unscoped probe")
		}
		_ = json.NewEncoder(w).Encode(body)
	}))
	defer server.Close()
	observer := HTTPObserver{Client: InternalHTTPClient()}
	helper := HelperObservation{PodUID: "pod-a", FileEndpoint: server.URL}
	if result, err := observer.Observe(context.Background(), in, helper, "test-only"); err != nil || result == nil {
		t.Fatal(result, err)
	}
	for _, change := range []func(){func() { body["pod_uid"] = "old-pod" }, func() { proof.SourceRevisionID = uuid.NewString() }, func() { proof.Size = 4 }, func() { proof.PreparedAt = time.Now().Add(time.Hour).Format(time.RFC3339Nano) }} {
		original := *proof
		body["pod_uid"] = "pod-a"
		change()
		if _, err := observer.Observe(context.Background(), in, helper, "test-only"); err == nil {
			t.Fatal("mismatched proof accepted")
		}
		*proof = original
	}
	body["pod_uid"] = "pod-a"
	body["preparation"] = nil
	if p, err := observer.Observe(context.Background(), in, helper, "test-only"); err != nil || p != nil {
		t.Fatal("empty workspace claimed prepared", p, err)
	}
}

func TestRuntimeHTTPBoundary(t *testing.T) {
	handler := Handler{Token: "test-service-token"}
	for _, test := range []struct {
		path, body, auth string
		duplicate        bool
		status           int
	}{
		{"/internal/v1/instances/create", "{}", "", false, 401},
		{"/internal/v1/instances/create", "{}", "Bearer test-service-token", true, 401},
		{"/internal/v1/instances/create", "{}", "Bearer test-service-token", false, 400},
		{"/internal/v1/instances/create", "{\"operation_id\":\"x\",\"operation_id\":\"y\"}", "Bearer test-service-token", false, 400},
		{"/mcp", "{}", "Bearer test-service-token", false, 404},
		{"/internal/v1/instances/create?target=other", "{}", "Bearer test-service-token", false, 400},
	} {
		request := httptest.NewRequest("POST", test.path, strings.NewReader(test.body))
		request.Header.Set("Content-Type", "application/json")
		if test.auth != "" {
			request.Header.Set("Authorization", test.auth)
		}
		if test.duplicate {
			request.Header.Add("Authorization", test.auth)
		}
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request)
		if response.Code != test.status {
			t.Fatalf("%s: %d", test.path, response.Code)
		}
	}
}
