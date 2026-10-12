package executor

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/tools/remotecommand"
)

// The runner provisions an isolated published 0.1.3 Pod with emptyDir data and
// an exact-Pod RBAC identity. Never point this fixture at a user's manager.
func TestManagementLegacyPublishedImageAndSQLiteRecovery(t *testing.T) {
	path := os.Getenv("STATION_LEGACY_TEST_KUBECONFIG")
	if path == "" {
		t.Skip("requires isolated old-image Pod on 5090 dev")
	}
	namespace, pod := os.Getenv("STATION_LEGACY_TEST_NAMESPACE"), os.Getenv("STATION_LEGACY_TEST_POD")
	if namespace != "verdantflare-comfyui" || !strings.HasPrefix(pod, "legacy-maintenance-test-") {
		t.Fatal("invalid isolated test target")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	readme, err := os.ReadFile(os.Getenv("STATION_LEGACY_TEST_PROOF"))
	if err != nil {
		t.Fatal("missing fixture proof")
	}
	var setup struct {
		Proof  managementProof `json:"proof"`
		UserID string          `json:"user_id"`
	}
	if json.Unmarshal(readme, &setup) != nil {
		t.Fatal("invalid fixture proof")
	}
	client, err := NewKubernetes(path, "dev.verdantflarehub.com")
	if err != nil {
		t.Fatal("test client unavailable")
	}
	call := func(action string, proof managementProof) map[string]json.RawMessage {
		t.Helper()
		raw, _ := json.Marshal(map[string]any{"action": action, "proof": proof})
		out, err := client.maintenanceExec(ctx, namespace, pod, "app", raw)
		if err != nil {
			t.Fatal("authenticated legacy transport failed", err)
		}
		var result map[string]json.RawMessage
		if json.Unmarshal(out, &result) != nil {
			t.Fatal("invalid legacy result")
		}
		return result
	}
	code := func(result map[string]json.RawMessage, want string) {
		t.Helper()
		var got string
		if len(result) != 1 || json.Unmarshal(result["code"], &got) != nil || got != want {
			t.Fatal("unexpected maintenance error", string(result["code"]))
		}
	}
	state := func(result map[string]json.RawMessage, proof managementProof, want string) {
		t.Helper()
		var got managementProofResponse
		if len(result) != 1 || json.Unmarshal(result["proof"], &got) != nil || got.managementProof != proof || got.State != want {
			t.Fatal("invalid persisted maintenance proof")
		}
	}
	fixture := func(action string) {
		t.Helper()
		cfg, err := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(&clientcmd.ClientConfigLoadingRules{ExplicitPath: path}, &clientcmd.ConfigOverrides{CurrentContext: "dev.verdantflarehub.com"}).ClientConfig()
		if err != nil {
			t.Fatal("fixture configuration unavailable")
		}
		k, err := kubernetes.NewForConfig(cfg)
		if err != nil {
			t.Fatal("fixture client unavailable")
		}
		program, err := os.ReadFile("testdata/comfyui_013_fixture.py")
		if err != nil {
			t.Fatal(err)
		}
		req := k.CoreV1().RESTClient().Post().Namespace(namespace).Resource("pods").Name(pod).SubResource("exec").VersionedParams(&corev1.PodExecOptions{Container: "app", Command: []string{"python", "-c", string(program), action}, Stdout: true, Stderr: true}, scheme.ParameterCodec)
		executor, err := remotecommand.NewSPDYExecutor(cfg, http.MethodPost, req.URL())
		if err != nil {
			t.Fatal("fixture transport unavailable")
		}
		var out, stderr bytes.Buffer
		if err = executor.StreamWithContext(ctx, remotecommand.StreamOptions{Stdout: &out, Stderr: &stderr}); err != nil || strings.TrimSpace(out.String()) != "FIXTURE_OK" || stderr.Len() != 0 {
			t.Fatal("fixture action failed", action)
		}
	}
	proof := setup.Proof
	code(call("status", proof), "MAINTENANCE_NOT_FOUND")
	for _, field := range []string{"installation", "organization", "digest"} {
		wrong := proof
		want := "LEGACY_MAINTENANCE_UNAVAILABLE"
		switch field {
		case "installation":
			wrong.InstallationID = uuid.Must(uuid.NewV7()).String()
		case "organization":
			wrong.OrganizationID = uuid.Must(uuid.NewV7()).String()
			want = "MAINTENANCE_SCOPE_CONFLICT"
		case "digest":
			wrong.DashboardDigest = "sha256:" + strings.Repeat("0", 64)
		}
		code(call("seal", wrong), want)
	}
	fixture("assert-no-seals")
	fixture("hide-database")
	code(call("seal", proof), "LEGACY_MAINTENANCE_UNAVAILABLE")
	fixture("restore-database")
	fixture("busy")
	code(call("seal", proof), "INSTALLATION_BUSY")
	fixture("assert-no-seals")
	fixture("save-close")
	// Deliberately discard the successful seal reply, then recreate the Runtime
	// client. Recovery must read the original operation in the same old-image DB.
	call("seal", proof)
	client, err = NewKubernetes(path, "dev.verdantflarehub.com")
	if err != nil {
		t.Fatal("restart client unavailable")
	}
	state(call("status", proof), proof, "sealed")
	state(call("seal", proof), proof, "sealed")
	fixture("assert-sealed")
	state(call("release", proof), proof, "released")
	state(call("release", proof), proof, "released")
	next := proof
	next.OperationID = uuid.Must(uuid.NewV7()).String()
	state(call("seal", next), next, "sealed")
	state(call("seal", proof), proof, "released")
	state(call("release", proof), proof, "released")
	state(call("status", next), next, "sealed")
	state(call("release", next), next, "released")
	// The old immutable image has no cancel method. Exercise the production
	// adapter extension against its real SQLite database and busy editor.
	canceled := proof
	canceled.OperationID = uuid.Must(uuid.NewV7()).String()
	fixture("busy")
	code(call("seal", canceled), "INSTALLATION_BUSY")
	wrong := canceled
	wrong.OrganizationID = uuid.Must(uuid.NewV7()).String()
	code(call("cancel", wrong), "MAINTENANCE_SCOPE_CONFLICT")
	call("cancel", canceled) // Discard success as if the response were lost.
	client, err = NewKubernetes(path, "dev.verdantflarehub.com")
	if err != nil {
		t.Fatal("restart client unavailable")
	}
	state(call("cancel", canceled), canceled, "released")
	state(call("seal", canceled), canceled, "released")
	fixture("save-close")
	state(call("seal", canceled), canceled, "released")
	newer := proof
	newer.OperationID = uuid.Must(uuid.NewV7()).String()
	state(call("seal", newer), newer, "sealed")
	state(call("cancel", canceled), canceled, "released")
	state(call("status", newer), newer, "sealed")
	state(call("cancel", newer), newer, "released")
	state(call("seal", newer), newer, "released")
}
