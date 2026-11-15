package appinstances

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"
)

// ErrUpgradeDeferred indicates that a resident instance cannot be stopped or
// replaced for a template upgrade because it is human-protected, actively
// computing, in a critical snapshot/archive section, or holds unsaved dirty
// edits without a completed standby checkpoint.
var ErrUpgradeDeferred = errors.New("INSTANCE_UPGRADE_DEFERRED_PROTECTED")

type UpgradeGateStatus struct {
	Allowed        bool   `json:"allowed"`
	Deferred       bool   `json:"deferred"`
	ResidencyState string `json:"residency_state,omitempty"`
	BlockingReason string `json:"blocking_reason,omitempty"`
	HumanProtected bool   `json:"human_protected"`
	Dirty          bool   `json:"dirty"`
}

// EvaluateTemplateUpgrade checks whether an existing resident instance may be
// rolled over to a new worker template/profile. Active S0 compute, S1 human GUI
// sessions, transitional/quarantined states, and dirty instances that have not
// yet hibernated into a verified checkpoint defer template rollover instead of
// killing the live Pod.
func EvaluateTemplateUpgrade(ctx context.Context, q rowQuerier, stationID, instanceID string) (UpgradeGateStatus, error) {
	if !validID(stationID) || !validID(instanceID) {
		return UpgradeGateStatus{}, ErrInvalid
	}
	var state, reason string
	var humanProtected, guiAlive, dirty, snapshotInProgress, snapshotRequired bool
	err := q.QueryRow(ctx, `
		SELECT residency_state, blocking_reason, human_protected, gui_connection_alive,
		       dirty, snapshot_in_progress, snapshot_required
		  FROM station.gpu_instance_residency
		 WHERE station_id = $1 AND instance_id = $2
	`, stationID, instanceID).Scan(
		&state, &reason, &humanProtected, &guiAlive, &dirty, &snapshotInProgress, &snapshotRequired,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return UpgradeGateStatus{Allowed: true}, nil
	}
	if err != nil {
		return UpgradeGateStatus{}, err
	}
	protected := humanProtected || guiAlive || snapshotInProgress ||
		state == "human_protected" || state == "human_dirty" || state == "active_compute" ||
		state == "waking" || state == "sealing" || state == "draining_hard" ||
		state == "reconciling" || state == "quarantined" ||
		(dirty && (snapshotRequired || state != "zero_vram_standby")) ||
		(reason != "" && reason != "none")
	if protected {
		if reason == "" || reason == "none" {
			switch {
			case humanProtected || guiAlive:
				reason = "human_gui_active"
			case snapshotInProgress:
				reason = "snapshot_in_progress"
			case dirty:
				reason = "checkpoint_pending"
			default:
				reason = state
			}
		}
		return UpgradeGateStatus{
			Allowed:        false,
			Deferred:       true,
			ResidencyState: state,
			BlockingReason: reason,
			HumanProtected: humanProtected || guiAlive,
			Dirty:          dirty,
		}, ErrUpgradeDeferred
	}
	return UpgradeGateStatus{
		Allowed:        true,
		ResidencyState: state,
		BlockingReason: reason,
		HumanProtected: false,
		Dirty:          dirty,
	}, nil
}

// StopForTemplateUpgrade enforces EvaluateTemplateUpgrade before stopping an
// existing resident worker Pod so active compute, S1 human GUI sessions, and
// uncheckpointed dirty edits are never interrupted by a template upgrade.
func (j *Journal) StopForTemplateUpgrade(ctx context.Context, in Run, p Profile, stopOperationID, bootID string) (StopRecord, error) {
	if j == nil || j.Pool == nil {
		return StopRecord{}, ErrUnavailable
	}
	if _, err := EvaluateTemplateUpgrade(ctx, j.Pool, j.StationID, in.InstanceID); err != nil {
		return StopRecord{}, err
	}
	return j.Stop(ctx, in, p, stopOperationID, bootID)
}
