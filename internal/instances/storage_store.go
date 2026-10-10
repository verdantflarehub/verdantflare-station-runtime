package instances

import (
	"bytes"
	"context"
	"errors"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"k8s.io/client-go/kubernetes"
)

type WorkspaceStore struct {
	Pool   *pgxpool.Pool
	Client kubernetes.Interface
}

const bindingColumns = `instance_id::text,station_id::text,organization_id::text,project_id::text,create_operation_id::text,profile_id,profile_hash,namespace,pool_name,pvc_name,pvc_uid,pv_name,pv_uid,node_name,reserved_bytes,retained_at IS NOT NULL`

func scanBinding(row pgx.Row) (VolumeBinding, error) {
	var b VolumeBinding
	err := row.Scan(&b.InstanceID, &b.StationID, &b.OrganizationID, &b.ProjectID, &b.CreateOperationID, &b.ProfileID, &b.ProfileHash, &b.Namespace, &b.Pool, &b.PVCName, &b.PVCUID, &b.PVName, &b.PVUID, &b.NodeName, &b.ReservedBytes, &b.Retained)
	return b, err
}

func sameReservation(b VolumeBinding, in Identity, p StorageProfile) bool {
	return b.Identity == in && b.ProfileID == p.ID && bytes.Equal(b.ProfileHash, p.Hash[:]) && b.Namespace == p.Namespace && b.Pool == p.Pool && b.NodeName == p.NodeName && b.ReservedBytes == p.Bytes
}

// Reserve commits ownership before any Kubernetes mutation. It never discards a
// reservation on a network error: retry the original instance/operation instead.
func (s *WorkspaceStore) Reserve(ctx context.Context, in Identity, p StorageProfile) (VolumeBinding, error) {
	if !validIdentity(in) || !p.valid() {
		return VolumeBinding{}, ErrInvalid
	}
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return VolumeBinding{}, err
	}
	defer tx.Rollback(context.Background())
	b, err := s.reserve(ctx, tx, in, p)
	if err != nil {
		return VolumeBinding{}, err
	}
	return b, tx.Commit(ctx)
}

func (s *WorkspaceStore) reserve(ctx context.Context, tx pgx.Tx, in Identity, p StorageProfile) (VolumeBinding, error) {
	// All registered pools share this lock: misconfigured overlapping profiles
	// cannot claim the same volume in separate transactions.
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1,0))`, "runtime-workspaces:"+in.StationID); err != nil {
		return VolumeBinding{}, err
	}
	old, err := scanBinding(tx.QueryRow(ctx, `SELECT `+bindingColumns+` FROM station.runtime_instance_workspaces WHERE instance_id=$1`, in.InstanceID))
	if err == nil {
		if !sameReservation(old, in, p) {
			return VolumeBinding{}, ErrBinding
		}
		if old.Retained {
			return VolumeBinding{}, ErrRetained
		}
		return old, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return VolumeBinding{}, err
	}
	for _, claim := range p.Claims {
		var used bool
		if err = tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM station.runtime_instance_workspaces WHERE station_id=$1 AND namespace=$2 AND pvc_name=$3)`, in.StationID, p.Namespace, claim).Scan(&used); err != nil {
			return VolumeBinding{}, err
		}
		if used {
			continue
		}
		b, e := Inspect(ctx, s.Client, in, p, claim, nil, nil)
		if errors.Is(e, ErrCapacity) || errors.Is(e, ErrBusy) || errors.Is(e, ErrBinding) {
			continue
		}
		if e != nil {
			return VolumeBinding{}, e
		}
		if err = tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM station.runtime_instance_workspaces WHERE pvc_uid=$1 OR pv_uid=$2)`, b.PVCUID, b.PVUID).Scan(&used); err != nil {
			return VolumeBinding{}, err
		}
		if used {
			continue
		}
		_, err = tx.Exec(ctx, `INSERT INTO station.runtime_instance_workspaces(instance_id,station_id,organization_id,project_id,create_operation_id,profile_id,profile_hash,namespace,pool_name,pvc_name,pvc_uid,pv_name,pv_uid,node_name,reserved_bytes) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15)`, b.InstanceID, b.StationID, b.OrganizationID, b.ProjectID, b.CreateOperationID, b.ProfileID, b.ProfileHash, b.Namespace, b.Pool, b.PVCName, b.PVCUID, b.PVName, b.PVUID, b.NodeName, b.ReservedBytes)
		if err != nil {
			return VolumeBinding{}, err
		}
		return b, nil
	}
	return VolumeBinding{}, ErrCapacity
}

func (s *WorkspaceStore) Lookup(ctx context.Context, in Identity, p StorageProfile) (VolumeBinding, error) {
	if !validIdentity(in) || !p.valid() {
		return VolumeBinding{}, ErrInvalid
	}
	b, err := scanBinding(s.Pool.QueryRow(ctx, `SELECT `+bindingColumns+` FROM station.runtime_instance_workspaces WHERE instance_id=$1`, in.InstanceID))
	if errors.Is(err, pgx.ErrNoRows) {
		return VolumeBinding{}, ErrBinding
	}
	if err != nil {
		return VolumeBinding{}, err
	}
	if !sameReservation(b, in, p) {
		return VolumeBinding{}, ErrBinding
	}
	return b, nil
}

// Retain is called under the instance execution lock, after all workloads have
// exited. Retention never deletes or frees a PVC/PV or makes its name reusable.
func (s *WorkspaceStore) Retain(ctx context.Context, in Identity, p StorageProfile) (VolumeBinding, error) {
	b, err := s.Lookup(ctx, in, p)
	if err != nil {
		return VolumeBinding{}, err
	}
	if _, err = Inspect(ctx, s.Client, in, p, b.PVCName, &b, nil); err != nil {
		return VolumeBinding{}, err
	}
	result, err := s.Pool.Exec(ctx, `UPDATE station.runtime_instance_workspaces SET retained_at=COALESCE(retained_at,now()) WHERE instance_id=$1 AND station_id=$2 AND organization_id=$3 AND pvc_uid=$4 AND pv_uid=$5`, in.InstanceID, in.StationID, in.OrganizationID, b.PVCUID, b.PVUID)
	if err != nil {
		return VolumeBinding{}, err
	}
	if result.RowsAffected() != 1 {
		return VolumeBinding{}, ErrBinding
	}
	b.Retained = true
	return b, nil
}
