package executor

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"reflect"
	"slices"

	pb "github.com/verdantflarehub/verdantflare-station-runtime/api/runtimev1"
	"github.com/verdantflarehub/verdantflare-station-runtime/internal/strictjson"
)

type forwardBinding struct {
	RecoveryID        string            `json:"recovery_id"`
	ParentInputDigest string            `json:"parent_input_digest"`
	ExpectedRevision  int64             `json:"expected_revision"`
	InstalledVersion  string            `json:"installed_version"`
	Target            ManagementRelease `json:"target"`
}

func (f forwardBinding) digest() string {
	raw, _ := json.Marshal(f)
	h := sha256.Sum256(raw)
	return "sha256:" + hex.EncodeToString(h[:])
}
func allowedWorldForward(b ManagementBinding, f forwardBinding, phase string) bool {
	return b.Source.AppID == "world" && b.Source.AppVersion == "0.1.4" && b.Target.AppVersion == "0.1.5" && f.Target.AppID == "world" && f.Target.AppVersion == "0.1.6" && b.Source.ReleaseKind == "dashboard-release" && b.Target.ReleaseKind == "dashboard-release" && f.Target.ReleaseKind == "dashboard-release" && validManagementRelease(f.Target) && f.ExpectedRevision == b.ExpectedRevision && f.InstalledVersion == b.Source.AppVersion && slices.Contains([]string{"restoring", "recovery_required"}, phase) && f.ParentInputDigest == "sha256:"+hex.EncodeToString(managementHash(b))
}

// This path reads Kubernetes only. It cannot scale, gate, replace or open routing.
func (j *ManagementJournal) verifyForward(ctx context.Context, step ManagementStep) error {
	if j == nil || j.Pool == nil || j.Kubernetes == nil || !managementID(j.StationID) {
		return ErrManagementReplacement
	}
	tx, err := j.Pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer managementRollback(tx)
	var app, parent string
	if err = tx.QueryRow(ctx, `SELECT t.app_id,t.operation_id::text FROM station.app_installation_forward_recoveries r JOIN station.app_installation_transitions t ON t.operation_id=r.operation_id WHERE r.recovery_id=$1 AND r.station_id=$2 AND t.station_id=r.station_id`, step.OperationID, j.StationID).Scan(&app, &parent); err != nil {
		return err
	}
	var locked bool
	if err = tx.QueryRow(ctx, `SELECT pg_try_advisory_xact_lock(hashtextextended($1,0))`, "runtime-app:"+j.StationID+":"+app).Scan(&locked); err != nil {
		return err
	}
	if !locked {
		return ErrManagementBusy
	}
	b, phase, err := readManagementBinding(ctx, tx, parent, j.StationID, app)
	if err != nil {
		return err
	}
	var f forwardBinding
	var target []byte
	var digest, state string
	err = tx.QueryRow(ctx, `SELECT recovery_id::text,parent_input_digest,expected_revision,installed_version,target_snapshot,input_digest,state FROM station.app_installation_forward_recoveries WHERE recovery_id=$1 AND operation_id=$2 AND station_id=$3 AND organization_id=$4 AND user_id=$5 FOR SHARE`, step.OperationID, parent, b.StationID, b.OrganizationID, b.UserID).Scan(&f.RecoveryID, &f.ParentInputDigest, &f.ExpectedRevision, &f.InstalledVersion, &target, &digest, &state)
	if err != nil {
		return err
	}
	if strictjson.Decode(bytes.NewReader(target), 64<<10, &f.Target) != nil || state != "pending" || f.digest() != digest || digest != step.InputDigest || !allowedWorldForward(b, f, phase) {
		return ErrManagementReplacement
	}
	r, err := decodeManagementRecord(tx.QueryRow(ctx, `SELECT input_hash,phase,revision,checkpoint FROM station.runtime_management_transitions WHERE operation_id=$1 FOR SHARE`, parent))
	if err != nil {
		return err
	}
	if !bytes.Equal(r.Hash, managementHash(b)) || r.Checkpoint == nil || !slices.Contains([]string{"restoring", "recovery_required"}, r.Phase) {
		return ErrManagementReplacement
	}
	var busy bool
	if err = tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM station.runtime_app_executions WHERE station_id=$1 AND app_id=$2 AND status='running')`, j.StationID, app).Scan(&busy); err != nil {
		return err
	}
	if busy {
		return ErrManagementBusy
	}
	source, ok := j.Targets[app+"@"+b.Source.AppVersion]
	targetEntry, targetOK := j.Targets[app+"@"+f.Target.AppVersion]
	if !ok || !targetOK || b.Source.TemplateDigest != "sha256:"+hex.EncodeToString(source.Hash[:]) || f.Target.TemplateDigest != "sha256:"+hex.EncodeToString(targetEntry.Hash[:]) {
		return ErrManagementReplacement
	}
	driver, err := NewStoppedReplacement(j.Kubernetes, source, targetEntry, &pb.AppRequest{OperationId: parent, InstallationId: b.InstallationID, StationId: b.StationID, OrganizationId: b.OrganizationID, UserId: b.UserID, AppId: app, AppVersion: b.Source.AppVersion})
	if err != nil {
		return err
	}
	proof, err := verifyForwardWorkload(ctx, b, f, driver, *r.Checkpoint, j.stoppedChecks)
	if err != nil {
		return err
	}
	raw, err := json.Marshal(proof)
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `INSERT INTO station.runtime_forward_recovery_proofs(recovery_id,input_digest,proof) VALUES($1,$2,$3) ON CONFLICT(recovery_id) DO UPDATE SET proof=EXCLUDED.proof,verified_at=now() WHERE station.runtime_forward_recovery_proofs.input_digest=EXCLUDED.input_digest`, f.RecoveryID, digest, raw)
	if err != nil {
		return err
	}
	return tx.Commit(ctx)
}
func verifyForwardWorkload(ctx context.Context, b ManagementBinding, f forwardBinding, d *StoppedReplacement, c ManagementCheckpoint, checks stoppedChecker) (map[string]string, error) {
	verify := func() error {
		live, isTarget, err := d.liveDeployment(ctx, c)
		if err != nil {
			return err
		}
		if !isTarget || !managementReady(live) || live.Annotations["station.verdantflare.com/external-recovery-operation"] != b.OperationID {
			return ErrManagementReplacement
		}
		for _, s := range c.Services {
			live, gated, err := d.liveService(ctx, s)
			if err != nil {
				return err
			}
			if gated || !d.targetMarked(live.Annotations) || live.Annotations["station.verdantflare.com/external-recovery-operation"] != b.OperationID || !reflect.DeepEqual(live.Spec.Selector, s.Spec.Selector) {
				return ErrManagementReplacement
			}
		}
		return nil
	}
	if err := verify(); err != nil {
		return nil, err
	}
	candidate := b
	candidate.Target = f.Target
	if checks == nil {
		checks = newStoppedChecks()
	}
	if err := checks.Check(ctx, candidate, d, c, true, false); err != nil {
		return nil, err
	}
	if err := verify(); err != nil {
		return nil, err
	}
	live, _, err := d.liveDeployment(ctx, c)
	if err != nil {
		return nil, err
	}
	pod, err := stoppedPod(ctx, d, c, true, f.Target.AppImageDigest)
	if err != nil {
		return nil, err
	}
	return map[string]string{"installation_id": b.InstallationID, "operation_id": b.OperationID, "deployment_uid": string(live.UID), "deployment_resource_version": live.ResourceVersion, "pod_uid": string(pod.UID), "app_version": f.Target.AppVersion, "image_digest": f.Target.AppImageDigest, "dashboard_digest": f.Target.DashboardDigest, "template_digest": f.Target.TemplateDigest}, nil
}
