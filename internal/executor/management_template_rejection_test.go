package executor

import (
	"context"
	"errors"
	"testing"
)

func TestManagementTemplateRejectionNeverMasksMutation(t *testing.T) {
	b := ManagementBinding{OperationID: "original"}
	r := managementRecord{Hash: managementHash(b), ManagementProgress: ManagementProgress{Phase: "checking", Revision: 1}}
	for _, phase := range []string{"checking", "sealing", "applying", "verifying", "publishing", "restoring", "recovery_required", "releasing"} {
		for _, mode := range []string{"prepare", "restore", "release_source", "release_target"} {
			want := phase == "checking" && mode == "prepare"
			if canRejectManagementTemplate(b, r, false, phase, mode) != want || canRejectManagementTemplate(b, managementRecord{}, true, phase, mode) != want {
				t.Fatal("rejection crossed Core phase or release direction", phase, mode)
			}
		}
	}
	for _, phase := range []string{"rejected_preflight", "checking", "sealing", "canceling_source", "canceled_source", "applying", "verifying", "prepared", "restoring", "restored", "recovery_required", "releasing_source", "released_source", "releasing_target", "released_target"} {
		r.Phase = phase
		want := phase == "checking" || phase == "rejected_preflight"
		if canRejectManagementTemplate(b, r, false, "checking", "prepare") != want {
			t.Fatal("mutation history ignored", phase)
		}
		r.Checkpoint = &ManagementCheckpoint{}
		if canRejectManagementTemplate(b, r, false, "checking", "prepare") {
			t.Fatal("existing checkpoint discarded", phase)
		}
		r.Checkpoint = nil
	}
	r.Phase = "checking"
	r.Hash = managementHash(ManagementBinding{OperationID: "another"})
	if canRejectManagementTemplate(b, r, false, "checking", "prepare") {
		t.Fatal("different immutable input accepted")
	}
}

func TestManagementTemplateMismatchDatabaseRejection(t *testing.T) {
	j, f := managementReleaseDatabase(t)
	ctx := context.Background()
	key := f.b.Target.AppID + "@" + f.b.Target.AppVersion
	target := j.Targets[key]
	target.Hash[0] ^= 1
	j.Targets[key] = target
	f.k.ClearActions()
	for range 2 {
		out, err := j.Reconcile(ctx, f.b.OperationID, false)
		if err != nil || out.Phase != "rejected_preflight" {
			t.Fatal("known template mismatch not durably rejected", out, err)
		}
		j = &ManagementJournal{Pool: j.Pool, StationID: j.StationID, Targets: j.Targets, Kubernetes: j.Kubernetes, Maintenance: j.Maintenance}
	}
	if len(f.k.Actions()) != 0 || f.calls != 0 {
		t.Fatal("template rejection touched application")
	}
	if _, err := j.Release(ctx, f.b.OperationID, true); !errors.Is(err, ErrManagementReplacement) {
		t.Fatal("rejected template released target", err)
	}
}
