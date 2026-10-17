package executor

import (
	"context"
	"errors"
	"slices"
)

// The same durable operation and phases drive downtime deployment. The app
// needs no maintenance endpoints or schema changes. Each retry observes the
// resources from the saved checkpoint; it never creates another installation.
func advanceStopped(ctx context.Context, r managementRecord, b ManagementBinding, d *StoppedReplacement, checks stoppedChecker, mode string) (managementRecord, error) {
	if mode == "release_source" || mode == "release_target" {
		target := mode == "release_target"
		prepared, releasing, released := "restored", "releasing_source", "released_source"
		if target {
			prepared, releasing, released = "prepared", "releasing_target", "released_target"
		}
		if r.Phase == prepared {
			r.Phase = releasing
			return r, nil
		}
		if r.Phase != releasing && r.Phase != released {
			return r, ErrManagementReplacement
		}
		if r.Checkpoint == nil {
			if target {
				return r, ErrManagementReplacement
			}
			r.Phase = released
			return r, nil
		}
		done, err := d.Open(ctx, *r.Checkpoint, target)
		if err == nil && done {
			r.Phase = released
		}
		return r, err
	}
	if slices.Contains([]string{"releasing_source", "releasing_target", "released_source", "released_target"}, r.Phase) {
		return r, ErrManagementReplacement
	}
	if mode == "restore" && r.Phase != "restoring" && r.Phase != "restored" {
		if r.Checkpoint == nil {
			r.Phase = "restored"
		} else {
			r.Phase = "restoring"
		}
		return r, nil
	}
	if mode != "restore" && slices.Contains([]string{"restoring", "restored", "recovery_required"}, r.Phase) {
		return r, ErrManagementReplacement
	}
	var err error
	switch r.Phase {
	case "checking":
		var c ManagementCheckpoint
		c, err = d.Capture(ctx)
		if err == nil {
			err = checks.Check(ctx, b, d, c, false, true)
			if err == nil {
				r.Checkpoint = &c
				r.Phase = "sealing"
			} else if errors.Is(err, ErrManagementInstallationBusy) {
				r.Checkpoint = &c
				r.Phase = "canceled_source"
				err = nil
			}
		}
	case "canceled_source":
		// No routing or process change was made for a busy preflight.
	case "sealing":
		var done bool
		done, err = d.Gate(ctx, *r.Checkpoint)
		if err == nil && done {
			if b.Source.AppID == "image-mcp" || b.Source.AppID == "video-mcp" {
				live, _, e := d.liveDeployment(ctx, *r.Checkpoint)
				if e != nil {
					return r, e
				}
				if *live.Spec.Replicas != 0 {
					if e = checks.Check(ctx, b, d, *r.Checkpoint, false, true); e != nil {
						return r, e
					}
				}
			}
			done, err = d.Stop(ctx, *r.Checkpoint)
			if err == nil && done {
				r.Phase = "applying"
			}
		}
	case "applying":
		var done bool
		done, err = d.Start(ctx, *r.Checkpoint, true)
		if err == nil && done {
			r.Phase = "verifying"
		}
	case "verifying", "prepared":
		err = checks.Check(ctx, b, d, *r.Checkpoint, true, true)
		if err == nil {
			r.Phase = "prepared"
		}
	case "restoring":
		live, isTarget, liveErr := d.liveDeployment(ctx, *r.Checkpoint)
		err = liveErr
		if err == nil && isTarget {
			if (b.Source.AppID == "image-mcp" || b.Source.AppID == "video-mcp") && *live.Spec.Replicas != 0 {
				if e := checks.Check(ctx, b, d, *r.Checkpoint, true, true); e != nil {
					return r, e
				}
			}
			var done bool
			done, err = d.Stop(ctx, *r.Checkpoint)
			if err != nil || !done {
				return r, err
			}
		}
		if err == nil {
			// Source may already be running when a preflight/gate was interrupted.
			live, _, e := d.liveDeployment(ctx, *r.Checkpoint)
			err = e
			if err == nil && *live.Spec.Replicas == 0 {
				_, err = d.Start(ctx, *r.Checkpoint, false)
				return r, err
			}
			if err == nil {
				err = checks.Check(ctx, b, d, *r.Checkpoint, false, false)
				if err == nil {
					r.Phase = "restored"
				}
			}
		}
	case "restored":
		if r.Checkpoint != nil {
			err = checks.Check(ctx, b, d, *r.Checkpoint, false, false)
		}
	default:
		return r, ErrManagementReplacement
	}
	if r.Checkpoint != nil && mode != "restore" && (errors.Is(err, ErrManagementReplacement) || errors.Is(err, ErrManagementInstallationBusy)) {
		r.Phase = "recovery_required"
		return r, nil
	}
	return r, err
}
