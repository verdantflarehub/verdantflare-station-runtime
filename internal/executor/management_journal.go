package executor

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"math"
	"slices"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	pb "github.com/verdantflarehub/verdantflare-station-runtime/api/runtimev1"
	"github.com/verdantflarehub/verdantflare-station-runtime/internal/strictjson"
)

var ErrManagementBusy = errors.New("MANAGEMENT_TRANSITION_BUSY")

// Only the authenticated Core coordinator may call this internal component.
// There is deliberately no HTTP/RPC surface until maintenance/image proof and
// Core publication/recovery are wired. Neither prepared nor restored is a
// terminal upgrade result. Release requires Core's durable publication and
// a separate, direction-bound journal barrier.
type ManagementJournal struct {
	Pool          *pgxpool.Pool
	StationID     string
	Targets       map[string]Target
	Kubernetes    *Kubernetes
	Maintenance   ManagementMaintenance
	stoppedChecks stoppedChecker
}

// Implementations must bind the actual management image, installation and
// durable maintenance operation. Kubernetes Ready alone is not a proof.
type ManagementMaintenance interface {
	Seal(context.Context, ManagementBinding, ManagementCheckpoint) error
	Verify(context.Context, ManagementBinding, ManagementCheckpoint, bool) error
}

// Cancellation proves a durable released marker on the original source.
type ManagementMaintenanceCanceler interface {
	Cancel(context.Context, ManagementBinding, ManagementCheckpoint) error
}

type ManagementMaintenanceReleaser interface {
	Release(context.Context, ManagementBinding, ManagementCheckpoint, bool) error
}

type ManagementRelease struct {
	AppID                    string   `json:"app_id"`
	AppVersion               string   `json:"app_version"`
	ReleaseRef               string   `json:"release_ref"`
	ReleaseDigest            string   `json:"release_digest"`
	ManifestDigest           string   `json:"manifest_digest"`
	TemplateDigest           string   `json:"template_digest"`
	DashboardDigest          string   `json:"dashboard_digest"`
	DataContract             string   `json:"data_contract"`
	DefaultWorkerVersion     string   `json:"default_worker_version"`
	CompatibleWorkerVersions []string `json:"compatible_worker_versions"`
	ReleaseKind              string   `json:"release_kind,omitempty"`
	AppImageDigest           string   `json:"app_image_digest,omitempty"`
}

type ManagementBinding struct {
	OperationID      string            `json:"operation_id"`
	InstallationID   string            `json:"installation_id"`
	StationID        string            `json:"station_id"`
	OrganizationID   string            `json:"organization_id"`
	UserID           string            `json:"user_id"`
	ExpectedRevision int64             `json:"expected_revision"`
	Source           ManagementRelease `json:"source"`
	Target           ManagementRelease `json:"target"`
}

type ManagementProgress struct {
	Phase    string
	Revision int64
}

type managementRecord struct {
	ManagementProgress
	Hash       []byte
	Checkpoint *ManagementCheckpoint
}

func managementID(value string) bool {
	id, err := uuid.Parse(value)
	return err == nil && id.Version() == 7 && id.Variant() == uuid.RFC4122 && id.String() == value
}

func managementRollback(tx pgx.Tx) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = tx.Rollback(ctx)
}

func managementHash(b ManagementBinding) []byte {
	raw, _ := json.Marshal(b)
	hash := sha256.Sum256(raw)
	return hash[:]
}

func managementPhase(phase string) bool {
	return slices.Contains([]string{"checking", "sealing", "canceling_source", "canceled_source", "applying", "verifying", "prepared", "restoring", "restored", "recovery_required", "releasing_source", "releasing_target", "released_source", "released_target"}, phase)
}

func decodeManagementRecord(row pgx.Row) (managementRecord, error) {
	var r managementRecord
	var raw []byte
	if err := row.Scan(&r.Hash, &r.Phase, &r.Revision, &raw); err != nil {
		return r, err
	}
	if len(r.Hash) != 32 || r.Revision < 1 || !managementPhase(r.Phase) {
		return r, ErrManagementReplacement
	}
	if raw != nil {
		var c ManagementCheckpoint
		// Kubernetes object JSON allows null optional metadata (including
		// creationTimestamp). This journal contains typed Kubernetes objects, not
		// the null-free management wire protocol. Keep bounds, duplicate-key
		// rejection and unknown-field rejection; validate ownership/specs below.
		checked, err := strictjson.ReadValue(bytes.NewReader(raw), 4<<20)
		if err != nil {
			return r, ErrManagementReplacement
		}
		decoder := json.NewDecoder(bytes.NewReader(checked))
		decoder.DisallowUnknownFields()
		if decoder.Decode(&c) != nil {
			return r, ErrManagementReplacement
		}
		r.Checkpoint = &c
	}
	if r.Checkpoint == nil && !slices.Contains([]string{"checking", "restored", "releasing_source", "released_source"}, r.Phase) {
		return r, ErrManagementReplacement
	}
	return r, nil
}

// Reconcile performs at most one durable phase or one Kubernetes resource
// update. restore is accepted only after Core has durably requested recovery.
func (j *ManagementJournal) Reconcile(ctx context.Context, operation string, restore bool) (ManagementProgress, error) {
	mode := "prepare"
	if restore {
		mode = "restore"
	}
	return j.reconcile(ctx, operation, mode, "")
}

func (j *ManagementJournal) Release(ctx context.Context, operation string, target bool) (ManagementProgress, error) {
	mode := "release_source"
	if target {
		mode = "release_target"
	}
	return j.reconcile(ctx, operation, mode, "")
}

func (j *ManagementJournal) reconcile(ctx context.Context, operation, mode, expectedDigest string) (ManagementProgress, error) {
	var empty ManagementProgress
	if j == nil || j.Pool == nil || j.Kubernetes == nil || !managementID(operation) || !managementID(j.StationID) {
		return empty, ErrManagementReplacement
	}
	tx, err := j.Pool.Begin(ctx)
	if err != nil {
		return empty, err
	}
	defer managementRollback(tx)
	// First read only locates the shared station/App lock. Re-read all authority
	// under the lock; callers cannot choose a different App or installation.
	var app string
	if err = tx.QueryRow(ctx, `SELECT app_id FROM station.app_installation_transitions WHERE operation_id=$1 AND station_id=$2`, operation, j.StationID).Scan(&app); err != nil {
		return empty, err
	}
	var locked bool
	if err = tx.QueryRow(ctx, `SELECT pg_try_advisory_xact_lock(hashtextextended($1,0))`, "runtime-app:"+j.StationID+":"+app).Scan(&locked); err != nil {
		return empty, err
	}
	if !locked {
		return empty, ErrManagementBusy
	}
	b, corePhase, err := readManagementBinding(ctx, tx, operation, j.StationID, app)
	if err != nil {
		return empty, err
	}
	if expectedDigest != "" && expectedDigest != "sha256:"+hex.EncodeToString(managementHash(b)) {
		return empty, ErrManagementReplacement
	}
	releasing := mode == "release_source" || mode == "release_target"
	restore := mode == "restore"
	if releasing != (corePhase == "releasing") || restore != slices.Contains([]string{"restoring", "recovery_required"}, corePhase) {
		return empty, ErrManagementReplacement
	}
	if releasing {
		var version string
		var revision int64
		if err := tx.QueryRow(ctx, `SELECT app_version,revision FROM station.app_installations WHERE installation_id=$1`, b.InstallationID).Scan(&version, &revision); err != nil {
			return empty, err
		}
		target := version == b.Target.AppVersion && revision == b.ExpectedRevision+1
		if target != (mode == "release_target") {
			return empty, ErrManagementReplacement
		}
	}
	source, ok := j.Targets[b.Source.AppID+"@"+b.Source.AppVersion]
	target, targetOK := j.Targets[b.Target.AppID+"@"+b.Target.AppVersion]
	if !ok || !targetOK || b.Source.TemplateDigest != "sha256:"+hex.EncodeToString(source.Hash[:]) || b.Target.TemplateDigest != "sha256:"+hex.EncodeToString(target.Hash[:]) {
		return empty, ErrManagementReplacement
	}
	stopped := b.Source.ReleaseKind == "dashboard-release" && b.Target.ReleaseKind == "dashboard-release"
	if (!stopped && j.Maintenance == nil) || (b.Source.ReleaseKind != b.Target.ReleaseKind) || (b.Source.ReleaseKind != "" && !stopped) ||
		(stopped && (!validManagementRelease(b.Source) || !validManagementRelease(b.Target) || (app != "higgsfield" && app != "project"))) {
		return empty, ErrManagementReplacement
	}
	driver, err := newManagementReplacement(j.Kubernetes, source, target, &pb.AppRequest{OperationId: operation, InstallationId: b.InstallationID, StationId: b.StationID, OrganizationId: b.OrganizationID, UserId: b.UserID, AppId: app, AppVersion: b.Source.AppVersion}, stopped)
	if err != nil {
		return empty, err
	}
	var busy bool
	if err = tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM station.runtime_app_executions WHERE station_id=$1 AND app_id=$2 AND status='running')`, j.StationID, app).Scan(&busy); err != nil {
		return empty, err
	}
	if busy {
		return empty, ErrManagementBusy
	}
	r, err := decodeManagementRecord(tx.QueryRow(ctx, `SELECT input_hash,phase,revision,checkpoint FROM station.runtime_management_transitions WHERE operation_id=$1 FOR UPDATE`, operation))
	if errors.Is(err, pgx.ErrNoRows) {
		if releasing {
			return empty, ErrManagementReplacement
		}
		// Do not call Capture, Seal, or Update until this commit is durable.
		_, err = tx.Exec(ctx, `INSERT INTO station.runtime_management_transitions(operation_id,station_id,app_id,input_hash,phase) VALUES($1,$2,$3,$4,'checking')`, operation, j.StationID, app, managementHash(b))
		if err != nil {
			return empty, err
		}
		return ManagementProgress{"checking", 1}, tx.Commit(ctx)
	}
	if err != nil {
		return empty, err
	}
	if !bytes.Equal(r.Hash, managementHash(b)) {
		return empty, ErrManagementReplacement
	}
	if r.Checkpoint != nil && !driver.validCheckpoint(*r.Checkpoint) {
		return empty, ErrManagementReplacement
	}
	var next managementRecord
	if stopped {
		checks := j.stoppedChecks
		if checks == nil {
			checks = newStoppedChecks()
		}
		next, err = advanceStopped(ctx, r, b, &StoppedReplacement{driver}, checks, mode)
	} else if releasing {
		next, err = advanceManagementRelease(ctx, r, b, j.Maintenance, mode == "release_target")
	} else {
		next, err = advanceManagement(ctx, r, b, driver, j.Maintenance, restore)
	}
	if err != nil {
		return empty, err
	}
	var raw []byte
	if next.Checkpoint != nil {
		raw, err = json.Marshal(next.Checkpoint)
		if err != nil || len(raw) > 4<<20 {
			return empty, ErrManagementReplacement
		}
	}
	// Always persist an observation revision; a lost commit response is resolved
	// by reading the original record, never by resetting it or its checkpoint.
	if err = tx.QueryRow(ctx, `UPDATE station.runtime_management_transitions SET phase=$2,checkpoint=$3,revision=revision+1,updated_at=now() WHERE operation_id=$1 AND revision=$4 RETURNING revision`, operation, next.Phase, raw, r.Revision).Scan(&next.Revision); err != nil {
		return empty, err
	}
	return next.ManagementProgress, tx.Commit(ctx)
}

func readManagementBinding(ctx context.Context, tx pgx.Tx, operation, station, app string) (ManagementBinding, string, error) {
	var b ManagementBinding
	var source, target []byte
	var phase, status, installedVersion string
	var revision int64
	err := tx.QueryRow(ctx, `SELECT t.operation_id::text,t.installation_id::text,t.station_id::text,t.organization_id::text,t.user_id::text,t.expected_revision,t.source_snapshot,t.target_snapshot,t.phase,t.status,i.app_version,i.revision
 FROM station.app_installation_transitions t JOIN station.app_installations i USING(installation_id)
 WHERE t.operation_id=$1 AND t.station_id=$2 AND t.app_id=$3 AND i.station_id=t.station_id AND i.organization_id=t.organization_id AND i.app_id=t.app_id AND i.state='installed' FOR SHARE OF t,i`, operation, station, app).Scan(
		&b.OperationID, &b.InstallationID, &b.StationID, &b.OrganizationID, &b.UserID, &b.ExpectedRevision, &source, &target, &phase, &status, &installedVersion, &revision)
	if err != nil {
		return b, "", err
	}
	if strictjson.Decode(bytes.NewReader(source), 64<<10, &b.Source) != nil || strictjson.Decode(bytes.NewReader(target), 64<<10, &b.Target) != nil ||
		!managementID(b.InstallationID) || !managementID(b.OrganizationID) || !managementID(b.UserID) || b.Source.AppID != app || b.Target.AppID != app || b.Source.AppVersion == b.Target.AppVersion || b.ExpectedRevision < 1 || b.ExpectedRevision > math.MaxInt64-2 {
		return b, "", ErrManagementReplacement
	}
	// Before Core publication only the original revision is valid. During
	// recovery allow the one target publication or one compensating revision.
	recovering := slices.Contains([]string{"restoring", "recovery_required"}, phase)
	if status != "running" && !(status == "recovery_required" && recovering) {
		return b, "", ErrManagementReplacement
	}
	if !slices.Contains([]string{"checking", "sealing", "applying", "verifying", "publishing", "releasing", "restoring", "recovery_required"}, phase) {
		return b, "", ErrManagementReplacement
	}
	original := installedVersion == b.Source.AppVersion && revision == b.ExpectedRevision
	published := installedVersion == b.Target.AppVersion && revision == b.ExpectedRevision+1
	restored := installedVersion == b.Source.AppVersion && revision == b.ExpectedRevision+2
	if !original && !((recovering || phase == "releasing") && (published || restored)) {
		return b, "", ErrManagementReplacement
	}
	return b, phase, nil
}

type managementDriver interface {
	Capture(context.Context) (ManagementCheckpoint, error)
	Apply(context.Context, ManagementCheckpoint) (bool, error)
	Restore(context.Context, ManagementCheckpoint) (bool, error)
}

func advanceManagement(ctx context.Context, r managementRecord, b ManagementBinding, d managementDriver, m ManagementMaintenance, restore bool) (managementRecord, error) {
	if slices.Contains([]string{"releasing_source", "releasing_target", "released_source", "released_target"}, r.Phase) {
		return r, ErrManagementReplacement // Release may already have admitted new work.
	}
	if restore && !slices.Contains([]string{"restoring", "restored", "canceling_source", "canceled_source"}, r.Phase) {
		if r.Checkpoint == nil {
			r.Phase = "restored"
		} else if r.Phase == "sealing" {
			r.Phase = "canceling_source"
		} else {
			r.Phase = "restoring"
		}
		return r, nil // Persist recovery intent BEFORE external effects.
	}
	if !restore && slices.Contains([]string{"restoring", "restored", "recovery_required"}, r.Phase) {
		return r, ErrManagementReplacement
	}
	var err error
	switch r.Phase {
	case "checking":
		var c ManagementCheckpoint
		c, err = d.Capture(ctx)
		if err == nil {
			r.Checkpoint = &c
			r.Phase = "sealing"
		}
	case "sealing":
		err = m.Seal(ctx, b, *r.Checkpoint)
		if err == nil {
			r.Phase = "applying"
		} else if errors.Is(err, ErrManagementInstallationBusy) {
			r.Phase = "canceling_source"
			err = nil // Commit intent; cancellation is a separate external step.
		}
	case "canceling_source", "canceled_source":
		canceler, ok := m.(ManagementMaintenanceCanceler)
		if !ok {
			return r, ErrManagementMaintenanceUnavailable
		}
		err = canceler.Cancel(ctx, b, *r.Checkpoint)
		if err == nil {
			r.Phase = "canceled_source"
		}
	case "applying":
		var done bool
		done, err = d.Apply(ctx, *r.Checkpoint)
		if err == nil && done {
			r.Phase = "verifying"
		}
	case "verifying":
		err = m.Verify(ctx, b, *r.Checkpoint, true)
		if err == nil {
			r.Phase = "prepared"
		}
	case "restoring":
		var done bool
		done, err = d.Restore(ctx, *r.Checkpoint)
		if err == nil && done {
			err = m.Verify(ctx, b, *r.Checkpoint, false)
			if err == nil {
				r.Phase = "restored"
			}
		}
	case "prepared":
		// A caller retry is a new observation, not a cached grant to publish.
		err = m.Verify(ctx, b, *r.Checkpoint, true)
	case "restored":
		if r.Checkpoint != nil {
			err = m.Verify(ctx, b, *r.Checkpoint, false)
		}
	default:
		return r, ErrManagementReplacement
	}
	if errors.Is(err, ErrManagementReplacement) && r.Checkpoint != nil && !slices.Contains([]string{"sealing", "canceling_source", "canceled_source"}, r.Phase) {
		r.Phase = "recovery_required"
		return r, nil
	}
	return r, err
}

func advanceManagementRelease(ctx context.Context, r managementRecord, b ManagementBinding, m ManagementMaintenance, target bool) (managementRecord, error) {
	prepared, releasing, released := "restored", "releasing_source", "released_source"
	if target {
		prepared, releasing, released = "prepared", "releasing_target", "released_target"
	}
	if r.Phase == prepared || (!target && r.Phase == "canceled_source") {
		r.Phase = releasing
		return r, nil // Commit intent before the first external release.
	}
	if r.Phase != releasing && r.Phase != released {
		return r, ErrManagementReplacement
	}
	if r.Checkpoint == nil {
		if target {
			return r, ErrManagementReplacement
		}
		// A restored record without a checkpoint never reached sealing.
		r.Phase = released
		return r, nil
	}
	releaser, ok := m.(ManagementMaintenanceReleaser)
	if !ok {
		return r, ErrManagementReplacement
	}
	// Even after a lost Core terminal response, re-observe this original
	// idempotent release. Never re-seal or compensate an uncertain release.
	if err := releaser.Release(ctx, b, *r.Checkpoint, target); err != nil {
		return r, err
	}
	r.Phase = released
	return r, nil
}
