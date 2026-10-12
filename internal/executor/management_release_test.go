package executor

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"testing"
)

func TestManagementMaintenanceReleaseProof(t *testing.T) {
	for _, scenario := range []string{"released", "sealed", "wrong operation", "restart"} {
		t.Run(scenario, func(t *testing.T) {
			f := newMaintenanceFixture(t)
			f.response = func(req *http.Request) (*http.Response, error) {
				if req.URL.Path != "/internal/maintenance/v1/release" {
					t.Fatal("release used another endpoint")
				}
				var body managementProof
				if err := json.NewDecoder(req.Body).Decode(&body); err != nil {
					t.Fatal(err)
				}
				state := "released"
				if scenario == "sealed" {
					state = "sealed"
				}
				if scenario == "wrong operation" {
					body.OperationID = "other"
				}
				if scenario == "restart" {
					f.pod.Status.ContainerStatuses[0].RestartCount++
					f.savePod(t)
				}
				raw, _ := json.Marshal(managementProofResponse{body, state})
				return proofResponse(200, string(raw)), nil
			}
			err := f.m.Release(context.Background(), f.b, f.c, false)
			if (err == nil) != (scenario == "released") {
				t.Fatal("unexpected release proof outcome", err)
			}
		})
	}
}

func managementReleaseDatabase(t *testing.T) (*ManagementJournal, *maintenanceFixture) {
	t.Helper()
	pool, old := runtimeDB(t)
	f := newMaintenanceFixture(t)
	for _, release := range []*ManagementRelease{&f.b.Source, &f.b.Target} {
		release.ManifestDigest = release.ReleaseDigest
		release.DefaultWorkerVersion = release.AppVersion
		release.CompatibleWorkerVersions = []string{f.b.Source.AppVersion, f.b.Target.AppVersion}
		config := f.m.Releases[release.ReleaseDigest]
		config.Snapshot = *release
		f.m.Releases[release.ReleaseDigest] = config
	}
	b := f.b
	ctx := context.Background()
	for _, q := range []struct {
		sql  string
		args []any
	}{
		{`UPDATE station.configuration SET station_id=$1 WHERE station_id=$2`, []any{b.StationID, old.StationId}},
		{`UPDATE station.users SET user_id=$1 WHERE user_id=$2`, []any{b.UserID, old.UserId}},
		{`UPDATE station.organizations SET organization_id=$1 WHERE organization_id=$2`, []any{b.OrganizationID, old.OrganizationId}},
		{`INSERT INTO station.app_installations(installation_id,created_by,organization_id,station_id,app_id,app_version,state,revision) VALUES($1,$2,$3,$4,$5,$6,'installed',$7)`, []any{b.InstallationID, b.UserID, b.OrganizationID, b.StationID, b.Source.AppID, b.Source.AppVersion, b.ExpectedRevision}},
	} {
		if _, err := pool.Exec(ctx, q.sql, q.args...); err != nil {
			t.Fatal(err)
		}
	}
	source, _ := json.Marshal(b.Source)
	target, _ := json.Marshal(b.Target)
	_, err := pool.Exec(ctx, `INSERT INTO station.app_installation_transitions(operation_id,installation_id,user_id,organization_id,station_id,app_id,request_id,idempotency_key,request_hash,action,expected_revision,source_version,target_version,source_snapshot,target_snapshot,status,phase) VALUES($1,$2,$3,$4,$5,$6,'test','management-release-test',decode(repeat('ab',32),'hex'),'upgrade',$7,$8,$9,$10,$11,'running','checking')`, b.OperationID, b.InstallationID, b.UserID, b.OrganizationID, b.StationID, b.Source.AppID, b.ExpectedRevision, b.Source.AppVersion, b.Target.AppVersion, source, target)
	if err != nil {
		t.Fatal(err)
	}
	return &ManagementJournal{Pool: pool, StationID: b.StationID, Targets: f.m.Targets, Kubernetes: f.m.Kubernetes, Maintenance: f.m}, f
}

func TestManagementReleaseDatabasePublicationAndLostCommit(t *testing.T) {
	j, f := managementReleaseDatabase(t)
	ctx := context.Background()
	op := f.b.OperationID
	for _, phase := range []string{"checking", "sealing", "applying", "applying", "applying", "verifying", "prepared"} {
		if phase == "prepared" {
			f.pod.Spec = *f.d.Target.Deployment.Spec.Template.Spec.DeepCopy()
			f.savePod(t)
		}
		out, err := j.Reconcile(ctx, op, false)
		if err != nil || out.Phase != phase {
			t.Fatal(phase, out.Phase, err)
		}
	}
	if _, err := j.Pool.Exec(ctx, `UPDATE station.app_installation_transitions SET phase='publishing' WHERE operation_id=$1`, op); err != nil {
		t.Fatal(err)
	}
	calls := f.calls
	if out, err := j.Reconcile(ctx, op, false); err != nil || out.Phase != "prepared" || f.calls != calls+1 {
		t.Fatal("publishing did not re-prove sealed target", out, err)
	}
	if _, err := j.Release(ctx, op, true); !errors.Is(err, ErrManagementReplacement) {
		t.Fatal("release before Core commit", err)
	}
	// Only this isolated fixture substitutes for Core's tested atomic CAS.
	tx, err := j.Pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	if _, err = tx.Exec(ctx, `UPDATE station.app_installations SET app_version=$2,revision=revision+1 WHERE installation_id=$1`, f.b.InstallationID, f.b.Target.AppVersion); err != nil {
		t.Fatal(err)
	}
	if _, err = tx.Exec(ctx, `UPDATE station.app_installation_transitions SET phase='releasing' WHERE operation_id=$1`, op); err != nil {
		t.Fatal(err)
	}
	if err = tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := j.Release(ctx, op, false); !errors.Is(err, ErrManagementReplacement) {
		t.Fatal("released opposite version", err)
	}
	if out, err := j.Release(ctx, op, true); err != nil || out.Phase != "releasing_target" || f.calls != calls+1 {
		t.Fatal("release crossed durable barrier", out, err)
	}
	f.k.ClearActions()
	responseLost := true
	f.response = func(req *http.Request) (*http.Response, error) {
		if req.URL.Path != "/internal/maintenance/v1/release" {
			t.Fatal("release retry resealed application")
		}
		if responseLost {
			responseLost = false
			return nil, errors.New("response lost after release")
		}
		var proof managementProof
		if err := json.NewDecoder(req.Body).Decode(&proof); err != nil {
			t.Fatal(err)
		}
		raw, _ := json.Marshal(managementProofResponse{proof, "released"})
		return proofResponse(200, string(raw)), nil
	}
	if _, err := j.Release(ctx, op, true); !errors.Is(err, ErrManagementMaintenanceUnavailable) {
		t.Fatal(err)
	}
	var phase string
	if err := j.Pool.QueryRow(ctx, `SELECT phase FROM station.runtime_management_transitions WHERE operation_id=$1`, op).Scan(&phase); err != nil || phase != "releasing_target" {
		t.Fatal(phase, err)
	}
	_, err = j.Pool.Exec(ctx, `CREATE FUNCTION station.fail_release_commit() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.phase='released_target' THEN RAISE EXCEPTION 'test commit failure'; END IF; RETURN NEW; END $$; CREATE TRIGGER fail_release_commit BEFORE UPDATE ON station.runtime_management_transitions FOR EACH ROW EXECUTE FUNCTION station.fail_release_commit()`)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := j.Release(ctx, op, true); err == nil {
		t.Fatal("release journal ignored commit failure")
	}
	if err := j.Pool.QueryRow(ctx, `SELECT phase FROM station.runtime_management_transitions WHERE operation_id=$1`, op).Scan(&phase); err != nil || phase != "releasing_target" {
		t.Fatal(phase, err)
	}
	if _, err := j.Pool.Exec(ctx, `DROP TRIGGER fail_release_commit ON station.runtime_management_transitions`); err != nil {
		t.Fatal(err)
	}
	j = &ManagementJournal{Pool: j.Pool, StationID: j.StationID, Targets: j.Targets, Kubernetes: j.Kubernetes, Maintenance: j.Maintenance}
	for range 2 {
		if out, err := j.Release(ctx, op, true); err != nil || out.Phase != "released_target" {
			t.Fatal("restart/replay did not recover original release", out, err)
		}
	}
	for _, action := range f.k.Actions() {
		if action.GetVerb() != "get" && action.GetVerb() != "list" {
			t.Fatal("release rewrote Kubernetes resource", action.GetVerb())
		}
	}
	// Even a new Core recovery intent cannot undo a possibly released manager.
	if _, err := j.Pool.Exec(ctx, `UPDATE station.app_installation_transitions SET phase='restoring' WHERE operation_id=$1`, op); err != nil {
		t.Fatal(err)
	}
	if _, err := j.Reconcile(ctx, op, true); !errors.Is(err, ErrManagementReplacement) {
		t.Fatal("released target was restored", err)
	}
}

func TestManagementReleaseDatabaseUnsealedRecovery(t *testing.T) {
	j, f := managementReleaseDatabase(t)
	ctx := context.Background()
	op := f.b.OperationID
	if out, err := j.Reconcile(ctx, op, false); err != nil || out.Phase != "checking" {
		t.Fatal(out, err)
	}
	if _, err := j.Pool.Exec(ctx, `UPDATE station.app_installation_transitions SET phase='restoring' WHERE operation_id=$1`, op); err != nil {
		t.Fatal(err)
	}
	if out, err := j.Reconcile(ctx, op, true); err != nil || out.Phase != "restored" {
		t.Fatal(out, err)
	}
	if _, err := j.Pool.Exec(ctx, `UPDATE station.app_installation_transitions SET phase='releasing' WHERE operation_id=$1`, op); err != nil {
		t.Fatal(err)
	}
	for _, phase := range []string{"releasing_source", "released_source", "released_source"} {
		out, err := j.Release(ctx, op, false)
		if err != nil || out.Phase != phase {
			t.Fatal(out, err)
		}
	}
	if f.calls != 0 {
		t.Fatal("unsealed recovery called maintenance")
	}
	if _, err := j.Release(ctx, op, true); !errors.Is(err, ErrManagementReplacement) {
		t.Fatal("source recovery released target", err)
	}
}

func TestManagementReleaseDatabaseRestoredSourceAndRevisionDrift(t *testing.T) {
	j, f := managementReleaseDatabase(t)
	ctx := context.Background()
	b := f.b
	checkpoint, _ := json.Marshal(f.c)
	if _, err := j.Pool.Exec(ctx, `INSERT INTO station.runtime_management_transitions(operation_id,station_id,app_id,input_hash,phase,checkpoint) VALUES($1,$2,$3,$4,'restored',$5)`, b.OperationID, b.StationID, b.Source.AppID, managementHash(b), checkpoint); err != nil {
		t.Fatal(err)
	}
	if _, err := j.Pool.Exec(ctx, `UPDATE station.app_installation_transitions SET phase='releasing' WHERE operation_id=$1`, b.OperationID); err != nil {
		t.Fatal(err)
	}
	if _, err := j.Pool.Exec(ctx, `UPDATE station.app_installations SET revision=revision+3 WHERE installation_id=$1`, b.InstallationID); err != nil {
		t.Fatal(err)
	}
	if _, err := j.Release(ctx, b.OperationID, false); !errors.Is(err, ErrManagementReplacement) || f.calls != 0 {
		t.Fatal("release accepted unexpected installation revision", err)
	}
	// Core's valid post-publication compensation is exactly expected + 2.
	if _, err := j.Pool.Exec(ctx, `UPDATE station.app_installations SET revision=$2 WHERE installation_id=$1`, b.InstallationID, b.ExpectedRevision+2); err != nil {
		t.Fatal(err)
	}
	if out, err := j.Release(ctx, b.OperationID, false); err != nil || out.Phase != "releasing_source" || f.calls != 0 {
		t.Fatal(out, err)
	}
	f.response = func(req *http.Request) (*http.Response, error) {
		var proof managementProof
		if err := json.NewDecoder(req.Body).Decode(&proof); err != nil {
			t.Fatal(err)
		}
		if req.URL.Path != "/internal/maintenance/v1/release" || proof.AppVersion != b.Source.AppVersion {
			t.Fatal("source compensation released another manager")
		}
		raw, _ := json.Marshal(managementProofResponse{proof, "released"})
		return proofResponse(200, string(raw)), nil
	}
	if out, err := j.Release(ctx, b.OperationID, false); err != nil || out.Phase != "released_source" || f.calls != 1 {
		t.Fatal(out, err)
	}
}
