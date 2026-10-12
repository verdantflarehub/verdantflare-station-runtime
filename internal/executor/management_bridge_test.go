package executor

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// Requires the Core operations test binary, built and executed only on 5090.
// PostgreSQL and Core-to-Runtime HTTP are real; manager/Kubernetes are fixtures.
func TestCrossServiceTransition(t *testing.T) {
	binary := os.Getenv("STATION_TRANSITION_CORE_TEST_BINARY")
	if binary == "" {
		t.Skip("remote Core test binary is required")
	}
	j, f := managementReleaseDatabase(t)
	token := strings.Repeat("cross-service-test-", 3)
	responseLost := false
	f.response = func(req *http.Request) (*http.Response, error) {
		var proof managementProof
		if json.NewDecoder(req.Body).Decode(&proof) != nil {
			return nil, errors.New("invalid fixture proof")
		}
		state := "sealed"
		if req.URL.Path == "/internal/maintenance/v1/release" {
			state = "released"
			if !responseLost {
				responseLost = true
				return nil, errors.New("response lost after release")
			}
		}
		raw, _ := json.Marshal(managementProofResponse{proof, state})
		return proofResponse(200, string(raw)), nil
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Model a controller making the sole manager Pod match the updated spec.
		deployment, err := f.k.AppsV1().Deployments(f.c.Deployment.Namespace).Get(r.Context(), f.c.Deployment.Name, metav1.GetOptions{})
		if err != nil {
			http.Error(w, "fixture unavailable", 503)
			return
		}
		f.pod.Spec = *deployment.Spec.Template.Spec.DeepCopy()
		if _, err := f.k.CoreV1().Pods(f.pod.Namespace).Update(r.Context(), f.pod, metav1.UpdateOptions{}); err != nil {
			http.Error(w, "fixture unavailable", 503)
			return
		}
		// Reconstruct the Runtime journal for every call to prove DB recovery.
		restarted := &ManagementJournal{Pool: j.Pool, StationID: j.StationID, Targets: j.Targets, Kubernetes: j.Kubernetes, Maintenance: j.Maintenance}
		ManagementHandler{Journal: restarted, Token: token}.ServeHTTP(w, r)
	}))
	defer server.Close()
	path := filepath.Join(t.TempDir(), "bridge.json")
	databaseURL, err := url.Parse(j.Pool.Config().ConnString())
	if err != nil || (databaseURL.Scheme != "postgres" && databaseURL.Scheme != "postgresql") {
		t.Fatal("bridge requires a test database URL")
	}
	databaseURL.Path = "/" + j.Pool.Config().ConnConfig.Database
	raw, _ := json.Marshal(struct{ DatabaseURL, StationID, OperationID, Origin, Token string }{databaseURL.String(), j.StationID, f.b.OperationID, server.URL, token})
	if os.WriteFile(path, raw, 0600) != nil {
		t.Fatal("cannot prepare private bridge configuration")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, binary, "-test.run=^TestCrossServiceTransition$", "-test.v")
	command.Env = append(os.Environ(), "STATION_TRANSITION_BRIDGE_CONFIG="+path)
	// Child errors never expose its private configuration or database parameters.
	output, err := command.CombinedOutput()
	server.Close()
	if err != nil {
		for _, reason := range []string{"bridge database unavailable", "bridge transport configuration invalid", "unexpected bridge reconciliation failure", "lost release response was not exercised", "bridge final state unavailable", "bridge published inconsistent final state", "bridge publication/terminal audit mismatch", "cross-service transition did not finish"} {
			if strings.Contains(string(output), reason) {
				t.Fatal("Core-to-Runtime bridge: " + reason)
			}
		}
		t.Fatal("Core-to-Runtime bridge failed")
	}
	if !strings.Contains(string(output), "--- PASS: TestCrossServiceTransition") {
		t.Fatal("Core bridge did not execute")
	}
	var phase string
	if j.Pool.QueryRow(context.Background(), `SELECT phase FROM station.runtime_management_transitions WHERE operation_id=$1`, f.b.OperationID).Scan(&phase) != nil || phase != "released_target" || !responseLost {
		t.Fatal("Runtime did not durably finish release")
	}
}
