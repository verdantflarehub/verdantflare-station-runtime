package instances

import (
	"context"
	"errors"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func boundReleaseMatches(in ReleaseWorkspaceRequest, b VolumeBinding) bool {
	return in.InstanceID != "" && in.PVCUID != "" && in.PVUID != "" &&
		b.InstanceID == in.InstanceID && b.StationID == in.StationID && b.OrganizationID == in.OrganizationID &&
		b.Namespace == in.Namespace && b.PVCName == in.PVCName && b.PVCUID == in.PVCUID && b.PVUID == in.PVUID &&
		b.Pool == "blender-local-v2" && b.Retained
}

// releaseBoundWorkspace returns static capacity without deleting the PVC or its
// claimRef. Allocation and instance locks cover all destructive disk operations.
func (s *Service) releaseBoundWorkspace(ctx context.Context, in ReleaseWorkspaceRequest) (WorkspaceVolume, error) {
	id, err := uuid.Parse(in.InstanceID)
	if err != nil || id == uuid.Nil || id.String() != in.InstanceID || in.PVCUID == "" || in.PVUID == "" {
		return WorkspaceVolume{}, ErrInvalid
	}
	if s.WorkspaceCleaner == nil {
		return WorkspaceVolume{}, errors.New("workspace cleaner unavailable")
	}
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return WorkspaceVolume{}, err
	}
	defer tx.Rollback(context.Background())
	return s.releaseBoundWorkspaceTx(ctx, in, tx)
}

func (s *Service) releaseBoundWorkspaceTx(ctx context.Context, in ReleaseWorkspaceRequest, tx pgx.Tx) (WorkspaceVolume, error) {
	var err error
	if _, err = tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1,0))`, "runtime-workspaces:"+in.StationID); err != nil {
		return WorkspaceVolume{}, err
	}
	var locked bool
	if err = tx.QueryRow(ctx, `SELECT pg_try_advisory_xact_lock(hashtextextended($1,0))`, "runtime-instance:"+in.StationID+":"+in.InstanceID).Scan(&locked); err != nil {
		return WorkspaceVolume{}, err
	}
	if !locked {
		return WorkspaceVolume{}, ErrBusy
	}
	b, err := scanBinding(tx.QueryRow(ctx, `SELECT `+bindingColumns+` FROM station.runtime_instance_workspaces WHERE instance_id=$1 FOR UPDATE`, in.InstanceID))
	if err != nil {
		return WorkspaceVolume{}, err
	}
	if !boundReleaseMatches(in, b) {
		return WorkspaceVolume{}, ErrBinding
	}
	kind, instanceID, _, err := s.lockAndValidateRelease(ctx, tx, in.Namespace, in.PVCName)
	if err != nil {
		return WorkspaceVolume{}, err
	}
	if kind != "runtime" || instanceID != in.InstanceID {
		return WorkspaceVolume{}, ErrBinding
	}
	p := StorageProfile{ID: b.ProfileID, Namespace: b.Namespace, Pool: b.Pool, NodeName: b.NodeName, Claims: []string{b.PVCName}, Bytes: b.ReservedBytes}
	copy(p.Hash[:], b.ProfileHash)
	if _, err = Inspect(ctx, s.Client, b.Identity, p, b.PVCName, &b, nil); err != nil {
		return WorkspaceVolume{}, err
	}
	services, err := s.Client.CoreV1().Services(in.Namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return WorkspaceVolume{}, err
	}
	for _, svc := range services.Items {
		if svc.Annotations[instanceKey] == in.InstanceID {
			return WorkspaceVolume{}, ErrBusy
		}
	}
	if err = s.WorkspaceCleaner(ctx, b.Namespace, b.PVCName, b.NodeName); err != nil {
		return WorkspaceVolume{}, err
	}
	if _, err = Inspect(ctx, s.Client, b.Identity, p, b.PVCName, &b, nil); err != nil {
		return WorkspaceVolume{}, err
	}
	pvc, err := s.Client.CoreV1().PersistentVolumeClaims(b.Namespace).Get(ctx, b.PVCName, metav1.GetOptions{})
	if err != nil {
		return WorkspaceVolume{}, err
	}
	if string(pvc.UID) != b.PVCUID || pvc.Spec.VolumeName != b.PVName {
		return WorkspaceVolume{}, ErrBinding
	}
	for key, owner := range volumeOwner(b.Identity) {
		if pvc.Annotations[key] != "" && pvc.Annotations[key] != owner {
			return WorkspaceVolume{}, ErrBinding
		}
		delete(pvc.Annotations, key)
	}
	delete(pvc.Annotations, "station.verdantflare.com/retained-at")
	if _, err = s.Client.CoreV1().PersistentVolumeClaims(b.Namespace).Update(ctx, pvc, metav1.UpdateOptions{}); err != nil {
		return WorkspaceVolume{}, err
	}
	result, err := tx.Exec(ctx, `DELETE FROM station.runtime_instance_workspaces WHERE instance_id=$1 AND station_id=$2 AND pvc_uid=$3 AND pv_uid=$4 AND retained_at IS NOT NULL`, b.InstanceID, b.StationID, b.PVCUID, b.PVUID)
	if err != nil {
		return WorkspaceVolume{}, err
	}
	if result.RowsAffected() != 1 {
		return WorkspaceVolume{}, ErrBinding
	}
	if err = tx.Commit(ctx); err != nil {
		return WorkspaceVolume{}, err
	}
	return WorkspaceVolume{WorkspaceID: b.Namespace + "/" + b.PVCName, PVCName: b.PVCName, PVName: b.PVName, Namespace: b.Namespace, PoolName: b.Pool, AppID: "blender", State: "released", AllowedActions: []string{}}, nil
}
