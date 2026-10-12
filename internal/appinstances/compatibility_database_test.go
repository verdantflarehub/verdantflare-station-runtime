package appinstances

import (
	"context"
	"errors"
	"testing"
)

func TestJournalCompatibleManagerUpgradePreservesWorkerAndStopRecovery(t *testing.T) {
	j, in, p, c, boot := runningStopFixture(t)
	ctx := context.Background()
	original, err := j.Accept(ctx, in, p)
	if err != nil {
		t.Fatal(err)
	}
	access, err := j.Access(ctx, in, p)
	if err != nil {
		t.Fatal(err)
	}
	// Core's current grant advances with a compatible manager publication. The
	// real release validation is covered in Core; this fixture isolates Runtime's
	// database CAS, durable bindings and Kubernetes side-effect protection.
	revision := int64(2)
	if _, err = j.Pool.Exec(ctx, `UPDATE station.app_installations SET app_version='9.0.0',revision=$2 WHERE installation_id=$1`, in.InstallationID, revision); err != nil {
		t.Fatal(err)
	}
	j.Authorize = func(_ context.Context, run Run, profile Profile) (int64, error) {
		if run != in || profile.Hash() != p.Hash() {
			return 0, ErrDenied
		}
		return revision, nil
	}
	j = &Journal{Pool: j.Pool, Client: c, StationID: j.StationID, Authorize: j.Authorize, Observer: j.Observer, Stopper: j.Stopper}
	for _, manager := range []string{"9.0.0", p.AppVersion, "9.0.1"} {
		if _, err = j.Pool.Exec(ctx, `UPDATE station.app_installations SET app_version=$2,revision=$3 WHERE installation_id=$1`, in.InstallationID, manager, revision); err != nil {
			t.Fatal(err)
		}
		c.ClearActions()
		got, err := j.Access(ctx, in, p)
		if err != nil || got != access {
			t.Fatal("compatible publication lost original worker access", err)
		}
		replayed, err := j.Start(ctx, in, p)
		if err != nil || replayed != original {
			t.Fatal("compatible publication changed start history", err)
		}
		assertNoEffects(t, c)
		revision++
	}
	// Match the last published revision, then verify stop resumes after restart
	// without rewriting the historical installation revision or workspace.
	revision--
	operation := id()
	for _, phase := range []string{"pod_delete_requested", "service_delete_requested", "stopped"} {
		stopped, err := j.Stop(ctx, in, p, operation, boot)
		if err != nil || stopped.Progress.Phase != phase {
			t.Fatal("compatible worker stop failed", phase, err)
		}
		j = &Journal{Pool: j.Pool, Client: c, StationID: j.StationID, Authorize: j.Authorize, Observer: j.Observer, Stopper: j.Stopper}
	}
	stored, err := j.Accept(ctx, in, p)
	if err != nil || stored.InstallationRevision != original.InstallationRevision || stored.Volume != original.Volume || stored.Observation != original.Observation {
		t.Fatal("stop rewrote immutable worker history", err)
	}
	next := in
	next.OperationID, next.Generation = id(), in.Generation+1
	j.Authorize = func(context.Context, Run, Profile) (int64, error) { return revision, nil }
	accepted, err := j.Accept(ctx, next, p)
	if err != nil || accepted.InstallationRevision != revision || accepted.Volume != original.Volume {
		t.Fatal("new generation did not use current grant and retained volume", err)
	}
}

func TestJournalWorkerCompatibilityRejectsRevocationAndRevisionRace(t *testing.T) {
	for _, scenario := range []string{"revoked", "revision race", "removed", "future history", "changed profile"} {
		t.Run(scenario, func(t *testing.T) {
			j, in, p, c, _ := runningStopFixture(t)
			ctx := context.Background()
			if _, err := j.Pool.Exec(ctx, `UPDATE station.app_installations SET app_version='9.0.0',revision=2 WHERE installation_id=$1`, in.InstallationID); err != nil {
				t.Fatal(err)
			}
			j.Authorize = func(context.Context, Run, Profile) (int64, error) { return 2, nil }
			want := ErrDenied
			switch scenario {
			case "revoked":
				j.Authorize = func(context.Context, Run, Profile) (int64, error) { return 0, ErrDenied }
			case "revision race":
				j.Authorize = func(context.Context, Run, Profile) (int64, error) {
					if _, err := j.Pool.Exec(ctx, `UPDATE station.app_installations SET revision=3 WHERE installation_id=$1`, in.InstallationID); err != nil {
						t.Fatal(err)
					}
					return 2, nil
				}
			case "removed":
				if _, err := j.Pool.Exec(ctx, `UPDATE station.app_installations SET state='removed' WHERE installation_id=$1`, in.InstallationID); err != nil {
					t.Fatal(err)
				}
			case "future history":
				if _, err := j.Pool.Exec(ctx, `UPDATE station.app_instance_starts SET installation_revision=3 WHERE operation_id=$1`, in.OperationID); err != nil {
					t.Fatal(err)
				}
				want = ErrConflict
			case "changed profile":
				p.AppVersion = "8.0.0"
				want = ErrConflict
			}
			c.ClearActions()
			if _, err := j.Access(ctx, in, p); !errors.Is(err, want) {
				t.Fatal("invalid current grant or history accepted", err)
			}
			assertNoEffects(t, c)
		})
	}
}
