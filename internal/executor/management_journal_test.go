package executor

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"
)

type checkpointRow []byte

func (r checkpointRow) Scan(dest ...any) error {
	*dest[0].(*[]byte) = make([]byte, 32)
	*dest[1].(*string) = "prepared"
	*dest[2].(*int64) = 1
	*dest[3].(*[]byte) = []byte(r)
	return nil
}

func TestManagementJournalCheckpointJSON(t *testing.T) {
	driver, _, checkpoint := managementFixture(t)
	raw, err := json.Marshal(checkpoint)
	if err != nil {
		t.Fatal(err)
	}
	// Both omitted and explicit null creationTimestamp are valid Kubernetes
	// metadata. Serialization differs across client/toolchain versions.
	var object map[string]any
	if err := json.Unmarshal(raw, &object); err != nil {
		t.Fatal(err)
	}
	object["deployment"].(map[string]any)["metadata"].(map[string]any)["creationTimestamp"] = nil
	raw, err = json.Marshal(object)
	if err != nil {
		t.Fatal(err)
	}
	record, err := decodeManagementRecord(checkpointRow(raw))
	if err != nil || record.Checkpoint == nil || !driver.validCheckpoint(*record.Checkpoint) {
		t.Fatal("typed checkpoint did not survive durable round trip", err)
	}
	for _, invalid := range []string{
		strings.TrimSuffix(string(raw), "}") + `,"unexpected":1}`,
		strings.TrimSuffix(string(raw), "}") + `,"deployment":null}`,
		strings.Repeat(" ", 4<<20) + string(raw),
	} {
		if _, err := decodeManagementRecord(checkpointRow(invalid)); !errors.Is(err, ErrManagementReplacement) {
			t.Fatal("invalid checkpoint accepted", err)
		}
	}
}

type maintenanceFake struct {
	calls              []string
	sealErr, verifyErr error
}

func (m *maintenanceFake) Seal(_ context.Context, _ ManagementBinding, _ ManagementCheckpoint) error {
	m.calls = append(m.calls, "seal")
	return m.sealErr
}
func (m *maintenanceFake) Verify(_ context.Context, _ ManagementBinding, _ ManagementCheckpoint, target bool) error {
	if target {
		m.calls = append(m.calls, "verify target")
	} else {
		m.calls = append(m.calls, "verify source")
	}
	return m.verifyErr
}

func savedManagement(t *testing.T, r managementRecord) managementRecord {
	t.Helper()
	raw, err := json.Marshal(r)
	if err != nil {
		t.Fatal(err)
	}
	var copy managementRecord
	if err = json.Unmarshal(raw, &copy); err != nil {
		t.Fatal(err)
	}
	return copy
}

func TestManagementJournalPhaseBarriersAndLostCommit(t *testing.T) {
	ctx := context.Background()
	d, client, _ := managementFixture(t)
	m := &maintenanceFake{}
	r := managementRecord{ManagementProgress: ManagementProgress{Phase: "checking", Revision: 1}}
	step := func(want string) {
		t.Helper()
		next, err := advanceManagement(ctx, savedManagement(t, r), ManagementBinding{}, d, m, false)
		if err != nil || next.Phase != want {
			t.Fatal(next.Phase, err)
		}
		r = savedManagement(t, next)
	}
	step("sealing")
	if len(m.calls) != 0 || len(managementUpdates(t, client)) != 0 {
		t.Fatal("side effects before saved checkpoint")
	}
	// Lost sealing commit: externally sealed, DB still says sealing. Repeating
	// invokes the same admission identity and cannot apply resources yet.
	if next, err := advanceManagement(ctx, savedManagement(t, r), ManagementBinding{}, d, m, false); err != nil || next.Phase != "applying" {
		t.Fatal(next.Phase, err)
	}
	step("applying")
	if !reflect.DeepEqual(m.calls, []string{"seal", "seal"}) || len(managementUpdates(t, client)) != 0 {
		t.Fatal("seal crossed update barrier")
	}
	step("applying") // Service
	// Kubernetes Deployment update succeeds but journal commit is lost.
	if next, err := advanceManagement(ctx, savedManagement(t, r), ManagementBinding{}, d, m, false); err != nil || next.Phase != "applying" {
		t.Fatal(next.Phase, err)
	}
	step("verifying") // New observation sees both effects.
	if got := managementUpdates(t, client); !reflect.DeepEqual(got, []string{"services", "deployments"}) {
		t.Fatal(got)
	}
	step("prepared")
	step("prepared") // Preparing is not releasing or completing the upgrade.
	if !reflect.DeepEqual(m.calls, []string{"seal", "seal", "verify target", "verify target"}) {
		t.Fatal(m.calls)
	}
}

func TestManagementJournalRecoveryRequiresDurableIntent(t *testing.T) {
	ctx := context.Background()
	d, client, c := managementFixture(t)
	m := &maintenanceFake{}
	for i := 0; i < 2; i++ {
		if _, err := d.Apply(ctx, c); err != nil {
			t.Fatal(err)
		}
	}
	client.ClearActions()
	r := managementRecord{ManagementProgress: ManagementProgress{Phase: "verifying", Revision: 4}, Checkpoint: &c}
	r, err := advanceManagement(ctx, r, ManagementBinding{}, d, m, true)
	if err != nil || r.Phase != "restoring" || len(client.Actions()) != 0 || len(m.calls) != 0 {
		t.Fatal("recovery effect before saved intent", err)
	}
	if _, err = advanceManagement(ctx, r, ManagementBinding{}, d, m, false); !errors.Is(err, ErrManagementReplacement) {
		t.Fatal("recovery reversed", err)
	}
	for i := 0; i < 3; i++ {
		r, err = advanceManagement(ctx, savedManagement(t, r), ManagementBinding{}, d, m, true)
		if err != nil {
			t.Fatal(err)
		}
	}
	if r.Phase != "restored" || !reflect.DeepEqual(m.calls, []string{"verify source"}) {
		t.Fatal(r.Phase, m.calls)
	}
	if got := managementUpdates(t, client); !reflect.DeepEqual(got, []string{"deployments", "services"}) {
		t.Fatal(got)
	}
	// No checkpoint means there cannot yet have been a maintenance side effect.
	client.ClearActions()
	m.calls = nil
	r, err = advanceManagement(ctx, managementRecord{ManagementProgress: ManagementProgress{Phase: "checking"}}, ManagementBinding{}, d, m, true)
	if err != nil || r.Phase != "restored" || len(client.Actions()) != 0 || len(m.calls) != 0 {
		t.Fatal("empty recovery mutated state", err)
	}
}

func TestManagementJournalUncertaintyNeverClaimsSuccess(t *testing.T) {
	for _, phase := range []string{"sealing", "verifying", "restoring"} {
		t.Run(phase, func(t *testing.T) {
			d, _, c := managementFixture(t)
			uncertain := errors.New("connection interrupted")
			m := &maintenanceFake{sealErr: uncertain, verifyErr: uncertain}
			r := managementRecord{ManagementProgress: ManagementProgress{Phase: phase}, Checkpoint: &c}
			next, err := advanceManagement(context.Background(), r, ManagementBinding{}, d, m, phase == "restoring")
			if !errors.Is(err, uncertain) || next.Phase != phase {
				t.Fatal(next.Phase, err)
			}
			m.sealErr, m.verifyErr = ErrManagementReplacement, ErrManagementReplacement
			next, err = advanceManagement(context.Background(), r, ManagementBinding{}, d, m, phase == "restoring")
			if phase == "sealing" {
				if !errors.Is(err, ErrManagementReplacement) || next.Phase != "sealing" {
					t.Fatal("lost pre-apply provenance", next.Phase, err)
				}
			} else if err != nil || next.Phase != "recovery_required" {
				t.Fatal(next.Phase, err)
			}
		})
	}
}

func TestManagementJournalInputIdentityIsImmutable(t *testing.T) {
	b := ManagementBinding{OperationID: "operation", InstallationID: "installation", StationID: "station", OrganizationID: "org", UserID: "user", ExpectedRevision: 1,
		Source: ManagementRelease{AppID: "app", AppVersion: "1.0.0", ReleaseDigest: "release-a", TemplateDigest: "template-a"},
		Target: ManagementRelease{AppID: "app", AppVersion: "1.0.1", ReleaseDigest: "release-b", DashboardDigest: "dashboard-b"}}
	for name, change := range map[string]func(*ManagementBinding){
		"organization":         func(v *ManagementBinding) { v.OrganizationID = "other" },
		"installation":         func(v *ManagementBinding) { v.InstallationID = "other" },
		"operation":            func(v *ManagementBinding) { v.OperationID = "other" },
		"revision":             func(v *ManagementBinding) { v.ExpectedRevision++ },
		"release":              func(v *ManagementBinding) { v.Target.ReleaseDigest = "other" },
		"template":             func(v *ManagementBinding) { v.Source.TemplateDigest = "other" },
		"dashboard":            func(v *ManagementBinding) { v.Target.DashboardDigest = "other" },
		"worker compatibility": func(v *ManagementBinding) { v.Target.CompatibleWorkerVersions = []string{"other"} },
	} {
		t.Run(name, func(t *testing.T) {
			v := b
			change(&v)
			if reflect.DeepEqual(managementHash(b), managementHash(v)) {
				t.Fatal("changed identity retained hash")
			}
		})
	}
}
