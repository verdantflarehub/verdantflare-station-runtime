package executor

import (
	"context"
	"errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ktesting "k8s.io/client-go/testing"
	"testing"
)

type stoppedCheckFake struct {
	calls []bool
	fail  error
}

func (f *stoppedCheckFake) Check(_ context.Context, _ ManagementBinding, _ *StoppedReplacement, _ ManagementCheckpoint, target, idle bool) error {
	f.calls = append(f.calls, idle)
	if target {
		return f.fail
	}
	return nil
}

func TestStoppedJournalUpgradeAndReleaseBarrier(t *testing.T) {
	d, k, c := stoppedFixture(t)
	ctx := context.Background()
	checks := &stoppedCheckFake{}
	r := managementRecord{ManagementProgress: ManagementProgress{Phase: "checking", Revision: 1}}
	b := ManagementBinding{}
	step := func(mode, want string) {
		t.Helper()
		var err error
		r, err = advanceStopped(ctx, savedManagement(t, r), b, d, checks, mode)
		if err != nil || r.Phase != want {
			t.Fatalf("%s: got %s %v want %s", mode, r.Phase, err, want)
		}
	}
	step("prepare", "sealing")
	step("prepare", "sealing") // Gate Service.
	step("prepare", "sealing") // Stop source.
	stoppedObservation(t, k, c, false)
	step("prepare", "applying")
	step("prepare", "applying") // Start target with routing closed.
	stoppedObservation(t, k, c, true)
	step("prepare", "verifying")
	step("prepare", "prepared")
	step("prepare", "prepared") // A lost reply must recheck the same target.
	if gated, err := d.routesGated(ctx, c); err != nil || !gated {
		t.Fatal("opened before publication", err)
	}
	step("release_target", "releasing_target")
	if gated, err := d.routesGated(ctx, c); err != nil || !gated {
		t.Fatal("release intent changed routing", err)
	}
	step("release_target", "releasing_target")
	step("release_target", "released_target")
	step("release_target", "released_target")
	if _, err := advanceStopped(ctx, r, b, d, checks, "restore"); !errors.Is(err, ErrManagementReplacement) {
		t.Fatal("restored after reopening", err)
	}
}

func TestStoppedJournalLateTaskRestoresBeforeOpening(t *testing.T) {
	d, k, c := stoppedFixture(t)
	ctx := context.Background()
	checks := &stoppedCheckFake{fail: ErrManagementInstallationBusy}
	_, _ = d.Gate(ctx, c)
	_, _ = d.Stop(ctx, c)
	stoppedObservation(t, k, c, false)
	_, _ = d.Start(ctx, c, true)
	stoppedObservation(t, k, c, true)
	r := managementRecord{ManagementProgress: ManagementProgress{Phase: "verifying"}, Checkpoint: &c}
	step := func(mode, want string) {
		t.Helper()
		var err error
		r, err = advanceStopped(ctx, savedManagement(t, r), ManagementBinding{}, d, checks, mode)
		if err != nil || r.Phase != want {
			t.Fatalf("got %s %v want %s", r.Phase, err, want)
		}
	}
	step("prepare", "recovery_required")
	step("restore", "restoring")
	step("restore", "restoring") // Target stop.
	stoppedObservation(t, k, c, false)
	step("restore", "restoring") // Source start.
	stoppedObservation(t, k, c, true)
	step("restore", "restored")
	if checks.calls[len(checks.calls)-1] {
		t.Fatal("rollback wrongly requires existing work to disappear")
	}
	if gated, err := d.routesGated(ctx, c); err != nil || !gated {
		t.Fatal("rollback opened before Core compensation", err)
	}
	step("release_source", "releasing_source")
	step("release_source", "releasing_source")
	step("release_source", "released_source")
}

func TestStoppedJournalTransientCheckCannotPublish(t *testing.T) {
	d, _, c := stoppedFixture(t)
	checks := &stoppedCheckFake{fail: ErrManagementMaintenanceUnavailable}
	r := managementRecord{ManagementProgress: ManagementProgress{Phase: "prepared"}, Checkpoint: &c}
	got, err := advanceStopped(context.Background(), r, ManagementBinding{}, d, checks, "prepare")
	if !errors.Is(err, ErrManagementMaintenanceUnavailable) || got.Phase != "prepared" {
		t.Fatal("uncertain check claimed success", got.Phase, err)
	}
}

type imageBusyCheck struct{}

func (imageBusyCheck) Check(context.Context, ManagementBinding, *StoppedReplacement, ManagementCheckpoint, bool, bool) error {
	return ErrManagementInstallationBusy
}

func TestMediaStoppedJournalDoesNotStopBusySourceOrRollbackTarget(t *testing.T) {
	for _, app := range []string{"image-mcp", "video-mcp"} {
		for _, target := range []bool{false, true} {
			d, k, c := stoppedFixture(t)
			ctx := context.Background()
			_, _ = d.Gate(ctx, c)
			phase, mode := "sealing", "prepare"
			if target {
				_, _ = d.Stop(ctx, c)
				stoppedObservation(t, k, c, false)
				_, _ = d.Start(ctx, c, true)
				stoppedObservation(t, k, c, true)
				phase, mode = "restoring", "restore"
			}
			r := managementRecord{ManagementProgress: ManagementProgress{Phase: phase}, Checkpoint: &c}
			b := ManagementBinding{Source: ManagementRelease{AppID: app}}
			got, err := advanceStopped(ctx, r, b, d, imageBusyCheck{}, mode)
			if !errors.Is(err, ErrManagementInstallationBusy) || got.Phase != phase {
				t.Fatal("busy task protection lost", got.Phase, err)
			}
			live, isTarget, err := d.liveDeployment(ctx, c)
			if err != nil || *live.Spec.Replicas != 1 || isTarget != target {
				t.Fatal("busy media process was stopped", app, err)
			}
		}
	}
}

func TestStoppedPreflightDriftIsDurablyRejectedWithoutWrites(t *testing.T) {
	d, k, c := stoppedFixture(t)
	ctx := context.Background()
	live, _ := k.AppsV1().Deployments(c.Deployment.Namespace).Get(ctx, c.Deployment.Name, metav1.GetOptions{})
	live.Spec.Template.Spec.Containers[0].Image = "unexpected/newer:version"
	_, _ = k.AppsV1().Deployments(live.Namespace).Update(ctx, live, metav1.UpdateOptions{})
	k.ClearActions()
	r := managementRecord{ManagementProgress: ManagementProgress{Phase: "checking", Revision: 1}}
	checks := &stoppedCheckFake{}
	for i := 0; i < 2; i++ {
		var err error
		r, err = advanceStopped(ctx, savedManagement(t, r), ManagementBinding{}, d, checks, "prepare")
		if err != nil || r.Phase != "rejected_preflight" || r.Checkpoint != nil {
			t.Fatal("drift not terminal", r.Phase, err)
		}
	}
	if len(checks.calls) != 0 {
		t.Fatal("checked a drifted process")
	}
	for _, a := range k.Actions() {
		if a.GetVerb() != "get" {
			t.Fatal("rejection mutated resources", a.GetVerb())
		}
	}
	if _, err := advanceStopped(ctx, r, ManagementBinding{}, d, checks, "restore"); err == nil {
		t.Fatal("rejected drift restored source")
	}
}

func TestStoppedPreflightNetworkFailureRemainsRetryable(t *testing.T) {
	d, k, _ := stoppedFixture(t)
	k.PrependReactor("get", "deployments", func(ktesting.Action) (bool, runtime.Object, error) {
		return true, nil, errors.New("network unavailable")
	})
	r := managementRecord{ManagementProgress: ManagementProgress{Phase: "checking", Revision: 1}}
	got, err := advanceStopped(context.Background(), r, ManagementBinding{}, d, &stoppedCheckFake{}, "prepare")
	if err == nil || got.Phase != "checking" || got.Checkpoint != nil {
		t.Fatal("unknown capture was rejected", got.Phase, err)
	}
}
