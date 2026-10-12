package executor

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"testing"

	"github.com/google/uuid"
)

type cancelMaintenanceFake struct {
	maintenanceFake
	cancelErr error
}

func (m *cancelMaintenanceFake) Cancel(context.Context, ManagementBinding, ManagementCheckpoint) error {
	m.calls = append(m.calls, "cancel")
	return m.cancelErr
}

func TestManagementCancellationBarriersAndUncertainty(t *testing.T) {
	for _, restore := range []bool{false, true} {
		d, k, c := managementFixture(t)
		m := &cancelMaintenanceFake{maintenanceFake: maintenanceFake{sealErr: ErrManagementInstallationBusy}}
		r := managementRecord{ManagementProgress: ManagementProgress{Phase: "sealing"}, Checkpoint: &c}
		next, err := advanceManagement(t.Context(), r, ManagementBinding{}, d, m, restore)
		if err != nil || next.Phase != "canceling_source" {
			t.Fatal(next.Phase, err)
		}
		for _, call := range m.calls {
			if call != "seal" {
				t.Fatal("cancel before intent commit")
			}
		}
		for _, uncertain := range []error{context.DeadlineExceeded, ErrManagementReplacement, ErrManagementMaintenanceUnavailable} {
			m.cancelErr = uncertain
			failed, err := advanceManagement(t.Context(), next, ManagementBinding{}, d, m, restore)
			if !errors.Is(err, uncertain) || failed.Phase != "canceling_source" {
				t.Fatal("uncertainty lost cancellation intent")
			}
		}
		m.cancelErr = nil
		for range 2 { // Successful effect with a lost journal commit, then retry.
			completed, err := advanceManagement(t.Context(), savedManagement(t, next), ManagementBinding{}, d, m, restore)
			if err != nil || completed.Phase != "canceled_source" {
				t.Fatal(completed.Phase, err)
			}
		}
		if len(managementUpdates(t, k)) != 0 {
			t.Fatal("cancellation updated resources")
		}
		// After applying may have begun, recovery must retain the old restore path.
		r.Phase = "applying"
		if out, err := advanceManagement(t.Context(), r, ManagementBinding{}, d, m, true); err != nil || out.Phase != "restoring" {
			t.Fatal(out.Phase, err)
		}
	}
}

func TestManagementCancellationProofAndBusyProcessBinding(t *testing.T) {
	for _, legacy := range []bool{false, true} {
		for _, scenario := range []string{"cancel", "busy", "busy restart", "busy unknown field", "busy duplicate field", "not found", "sealed", "wrong operation", "restart"} {
			t.Run(map[bool]string{false: "http", true: "legacy"}[legacy]+"/"+scenario, func(t *testing.T) {
				f := newMaintenanceFixture(t)
				if legacy {
					f = legacyMaintenanceFixture(t)
				}
				answer := func(action string, p managementProof) ([]byte, int) {
					if scenario == "restart" || scenario == "busy restart" {
						f.pod.Status.ContainerStatuses[0].RestartCount++
						f.savePod(t)
					}
					switch scenario {
					case "busy", "busy restart":
						return []byte(`{"code":"INSTALLATION_BUSY"}`), 409
					case "busy unknown field":
						return []byte(`{"code":"INSTALLATION_BUSY","extra":true}`), 409
					case "busy duplicate field":
						return []byte(`{"code":"INSTALLATION_BUSY","code":"INSTALLATION_BUSY"}`), 409
					case "not found":
						return []byte(`{"code":"MAINTENANCE_NOT_FOUND"}`), 404
					}
					if action != "cancel" {
						t.Fatal("wrong action", action)
					}
					state := "released"
					if scenario == "sealed" {
						state = "sealed"
					}
					if scenario == "wrong operation" {
						p.OperationID = uuid.Must(uuid.NewV7()).String()
					}
					var raw []byte
					if legacy {
						raw, _ = json.Marshal(map[string]any{"proof": managementProofResponse{p, state}})
					} else {
						raw, _ = json.Marshal(managementProofResponse{p, state})
					}
					return raw, 200
				}
				f.response = func(req *http.Request) (*http.Response, error) {
					var p managementProof
					if json.NewDecoder(req.Body).Decode(&p) != nil {
						t.Fatal("invalid request")
					}
					raw, status := answer(req.URL.Path[len("/internal/maintenance/v1/"):], p)
					return proofResponse(status, string(raw)), nil
				}
				f.m.Kubernetes.maintenanceExec = func(_ context.Context, _, _, _ string, raw []byte) ([]byte, error) {
					var in struct {
						Action string          `json:"action"`
						Proof  managementProof `json:"proof"`
					}
					if json.Unmarshal(raw, &in) != nil {
						t.Fatal("invalid request")
					}
					out, _ := answer(in.Action, in.Proof)
					return out, nil
				}
				var err error
				if len(scenario) >= 4 && scenario[:4] == "busy" {
					err = f.m.Seal(t.Context(), f.b, f.c)
				} else {
					err = f.m.Cancel(t.Context(), f.b, f.c)
				}
				if (err == nil) != (scenario == "cancel") || errors.Is(err, ErrManagementInstallationBusy) != (scenario == "busy") {
					t.Fatal("unverified proof accepted", err)
				}
			})
		}
	}
}

func TestManagementCancellationDatabaseRecoveryAndWireMapping(t *testing.T) {
	for _, restore := range []bool{false, true} {
		t.Run(map[bool]string{false: "busy", true: "restore before seal"}[restore], func(t *testing.T) {
			j, f := managementReleaseDatabase(t)
			ctx := t.Context()
			step := ManagementStep{f.b.OperationID, uuid.NewString(), "sha256:" + hex.EncodeToString(managementHash(f.b)), "prepare"}
			advance := func(want string) {
				t.Helper()
				step.RequestID = uuid.NewString()
				out, err := j.ReconcileStep(ctx, step)
				if err != nil || out.Phase != want {
					t.Fatal(want, out.Phase, err)
				}
			}
			for _, phase := range []string{"checking", "sealing"} {
				advance(phase)
			}
			if restore {
				if _, err := j.Pool.Exec(ctx, `UPDATE station.app_installation_transitions SET phase='restoring' WHERE operation_id=$1`, step.OperationID); err != nil {
					t.Fatal(err)
				}
				step.Mode = "restore"
			}
			cancelCalls := 0
			lost := true
			f.response = func(req *http.Request) (*http.Response, error) {
				if req.URL.Path == "/internal/maintenance/v1/seal" {
					return proofResponse(409, `{"code":"INSTALLATION_BUSY"}`), nil
				}
				if req.URL.Path != "/internal/maintenance/v1/cancel" && req.URL.Path != "/internal/maintenance/v1/release" {
					t.Fatal("wrong cancellation action")
				}
				cancelCalls++
				if lost {
					lost = false
					return nil, errors.New("response lost after durable cancellation")
				}
				var p managementProof
				if json.NewDecoder(req.Body).Decode(&p) != nil {
					t.Fatal("bad request")
				}
				raw, _ := json.Marshal(managementProofResponse{p, "released"})
				return proofResponse(200, string(raw)), nil
			}
			pending, completed := "sealing", "rejected"
			if restore {
				pending, completed = "restoring", "restored"
			}
			advance(pending)
			if cancelCalls != 0 {
				t.Fatal("cancel ran before intent commit")
			}
			if _, err := j.ReconcileStep(ctx, step); !errors.Is(err, ErrManagementMaintenanceUnavailable) {
				t.Fatal(err)
			}
			var phase string
			if err := j.Pool.QueryRow(ctx, `SELECT phase FROM station.runtime_management_transitions WHERE operation_id=$1`, step.OperationID).Scan(&phase); err != nil || phase != "canceling_source" {
				t.Fatal(phase, err)
			}
			_, err := j.Pool.Exec(ctx, `CREATE FUNCTION station.fail_cancel_commit() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.phase='canceled_source' THEN RAISE EXCEPTION 'test commit failure'; END IF; RETURN NEW; END $$; CREATE TRIGGER fail_cancel_commit BEFORE UPDATE ON station.runtime_management_transitions FOR EACH ROW EXECUTE FUNCTION station.fail_cancel_commit()`)
			if err != nil {
				t.Fatal(err)
			}
			if _, err = j.ReconcileStep(ctx, step); err == nil {
				t.Fatal("failed journal commit reported completion")
			}
			if _, err = j.Pool.Exec(ctx, `DROP TRIGGER fail_cancel_commit ON station.runtime_management_transitions`); err != nil {
				t.Fatal(err)
			}
			// A fresh coordinator uses only its durable row to resume.
			j = &ManagementJournal{Pool: j.Pool, StationID: j.StationID, Targets: j.Targets, Kubernetes: j.Kubernetes, Maintenance: j.Maintenance}
			advance(completed)
			advance(completed)
			for _, p := range []string{"canceling_source", "canceled_source"} {
				if _, err := j.Pool.Exec(ctx, `UPDATE station.runtime_management_transitions SET phase=$2,checkpoint=NULL WHERE operation_id=$1`, step.OperationID, p); err == nil {
					t.Fatal("cancel without checkpoint accepted")
				}
			}
			var version string
			var revision int64
			if err := j.Pool.QueryRow(ctx, `SELECT app_version,revision FROM station.app_installations WHERE installation_id=$1`, f.b.InstallationID).Scan(&version, &revision); err != nil || version != f.b.Source.AppVersion || revision != f.b.ExpectedRevision {
				t.Fatal("cancellation changed installation", err)
			}
			for _, action := range f.k.Actions() {
				if action.GetVerb() != "get" && action.GetVerb() != "list" {
					t.Fatal("cancellation changed resources", action.GetVerb())
				}
			}
			if restore {
				if _, err := j.Pool.Exec(ctx, `UPDATE station.app_installation_transitions SET phase='releasing' WHERE operation_id=$1`, step.OperationID); err != nil {
					t.Fatal(err)
				}
				step.Mode = "release_source"
				advance("releasing")
				advance("released")
			}
		})
	}
}
