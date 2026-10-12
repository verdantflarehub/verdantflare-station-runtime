package executor

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/google/uuid"
)

func TestManagementTransportRejectsBeforeDatabase(t *testing.T) {
	token := strings.Repeat("coordinator-test-", 3)
	handler := ManagementHandler{Journal: &ManagementJournal{}, Token: token}
	step := ManagementStep{uuid.Must(uuid.NewV7()).String(), uuid.NewString(), "sha256:" + strings.Repeat("a", 64), "prepare"}
	for _, scenario := range []string{"missing token", "duplicate auth", "duplicate field", "unknown field", "wrong mode", "wrong nonce", "query", "content type", "encoded path", "oversized"} {
		t.Run(scenario, func(t *testing.T) {
			input := step
			if scenario == "wrong mode" {
				input.Mode = "restart"
			}
			if scenario == "wrong nonce" {
				input.RequestID = uuid.Must(uuid.NewV7()).String()
			}
			raw, _ := json.Marshal(input)
			if scenario == "duplicate field" {
				raw = []byte(strings.TrimSuffix(string(raw), "}") + `,"mode":"prepare"}`)
			}
			if scenario == "unknown field" {
				raw = []byte(strings.TrimSuffix(string(raw), "}") + `,"target":"user-selected"}`)
			}
			if scenario == "oversized" {
				raw = append(bytes.Repeat([]byte(" "), 4096), raw...)
			}
			req := httptest.NewRequest(http.MethodPost, ManagementReconcilePath, bytes.NewReader(raw))
			req.Header.Set("Authorization", "Bearer "+token)
			req.Header.Set("Content-Type", "application/json")
			want := 400
			switch scenario {
			case "missing token":
				req.Header.Del("Authorization")
				want = 401
			case "duplicate auth":
				req.Header.Add("Authorization", "Bearer "+token)
				want = 401
			case "query":
				req.URL.RawQuery = "app=other"
			case "content type":
				req.Header.Set("Content-Type", "text/plain")
			case "encoded path":
				req.URL.RawPath = "/internal/%69nstallation-transitions/v1/reconcile"
			}
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, req)
			if response.Code != want || strings.Contains(response.Body.String(), token) {
				t.Fatal("invalid request reached executor", response.Code)
			}
		})
	}
}

func TestManagementTransportDatabaseDigestAndReleaseMapping(t *testing.T) {
	j, f := managementReleaseDatabase(t)
	ctx := context.Background()
	token := strings.Repeat("coordinator-test-", 3)
	handler := ManagementHandler{j, token}
	step := ManagementStep{f.b.OperationID, uuid.NewString(), "sha256:" + strings.Repeat("f", 64), "prepare"}
	request := func(want int) ManagementStepResult {
		t.Helper()
		raw, _ := json.Marshal(step)
		req := httptest.NewRequest(http.MethodPost, ManagementReconcilePath, bytes.NewReader(raw))
		req.Header.Set("Authorization", "Bearer "+token)
		req.Header.Set("Content-Type", "application/json")
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, req)
		if response.Code != want {
			t.Fatal("unexpected HTTP status", response.Code)
		}
		var out ManagementStepResult
		if want == 200 {
			if json.Unmarshal(response.Body.Bytes(), &out) != nil || out.ManagementStep != step {
				t.Fatal("response lost exact request binding")
			}
		}
		return out
	}
	request(409)
	var count int
	if err := j.Pool.QueryRow(ctx, `SELECT count(*) FROM station.runtime_management_transitions`).Scan(&count); err != nil || count != 0 || f.calls != 0 || len(f.k.Actions()) != 0 {
		t.Fatal("wrong digest produced side effects", count, err)
	}
	step.InputDigest = "sha256:" + hex.EncodeToString(managementHash(f.b))
	if out := request(200); out.Phase != "checking" {
		t.Fatal(out.Phase)
	}
	if _, err := j.Pool.Exec(ctx, `UPDATE station.app_installation_transitions SET phase='restoring' WHERE operation_id=$1`, step.OperationID); err != nil {
		t.Fatal(err)
	}
	step.RequestID = uuid.NewString()
	step.Mode = "restore"
	if out := request(200); out.Phase != "restored" {
		t.Fatal(out.Phase)
	}
	if _, err := j.Pool.Exec(ctx, `UPDATE station.app_installation_transitions SET phase='releasing' WHERE operation_id=$1`, step.OperationID); err != nil {
		t.Fatal(err)
	}
	step.Mode = "release_source"
	for _, phase := range []string{"releasing", "released"} {
		step.RequestID = uuid.NewString()
		if out := request(200); out.Phase != phase {
			t.Fatal(out.Phase)
		}
	}
}

func TestManagementConfigurationClosesReleaseBinding(t *testing.T) {
	f := newMaintenanceFixture(t)
	s := f.b.Source
	s.ManifestDigest = s.ReleaseDigest
	s.DefaultWorkerVersion = s.AppVersion
	s.CompatibleWorkerVersions = []string{s.AppVersion}
	base := maintenanceRegistration{Snapshot: s, Images: map[string]string{"server": "sha256:" + strings.Repeat("a", 64)}, Service: "manager", Port: 80, TokenEnv: "APP_MAINTENANCE_TEST_TOKEN"}
	control := strings.Repeat("control-test-", 4)
	for _, scenario := range []string{"valid", "missing token", "reused token", "wrong template", "wrong service", "missing image", "extra image", "bad digest", "duplicate version", "inline token", "missing worker"} {
		t.Run(scenario, func(t *testing.T) {
			entry := base
			entry.Images = map[string]string{"server": base.Images["server"]}
			lookup := func(string) string { return strings.Repeat("maintenance-test-", 3) }
			switch scenario {
			case "missing token":
				lookup = func(string) string { return "" }
			case "reused token":
				lookup = func(string) string { return control }
			case "wrong template":
				entry.Snapshot.TemplateDigest = "sha256:" + strings.Repeat("f", 64)
			case "wrong service":
				entry.Service = "unrelated"
			case "missing image":
				delete(entry.Images, "server")
			case "extra image":
				entry.Images["extra"] = entry.Images["server"]
			case "bad digest":
				entry.Images["server"] = "tag:1.0.0"
			case "missing worker":
				entry.Snapshot.CompatibleWorkerVersions = []string{}
			}
			entries := []maintenanceRegistration{entry}
			if scenario == "duplicate version" {
				entries = append(entries, entry)
			}
			raw, _ := json.Marshal(struct {
				SchemaVersion int                       `json:"schema_version"`
				Releases      []maintenanceRegistration `json:"releases"`
			}{1, entries})
			if scenario == "inline token" {
				raw = []byte(strings.Replace(string(raw), `"maintenance_token_env":`, `"token":"not-an-env","maintenance_token_env":`, 1))
			}
			path := filepath.Join(t.TempDir(), "maintenance.json")
			if err := os.WriteFile(path, raw, 0600); err != nil {
				t.Fatal(err)
			}
			client, err := LoadManagementMaintenance(path, control, lookup, f.m.Kubernetes, f.m.Targets)
			if (err == nil) != (scenario == "valid") {
				t.Fatal("invalid release configuration accepted", err)
			}
			if err == nil && client.Releases[s.ReleaseDigest].Snapshot.AppVersion != s.AppVersion {
				t.Fatal("source release binding lost")
			}
		})
	}
}
