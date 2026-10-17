package executor

import (
	"bytes"
	"context"
	"errors"

	"github.com/jackc/pgx/v5"
)

func canRejectManagementTemplate(b ManagementBinding, r managementRecord, missing bool, corePhase, mode string) bool {
	if mode != "prepare" || corePhase != "checking" || r.Checkpoint != nil {
		return false
	}
	if missing {
		return true
	}
	return bytes.Equal(r.Hash, managementHash(b)) && (r.Phase == "checking" || r.Phase == "rejected_preflight")
}

// Called under the existing station/App transaction lock, after authenticating
// the immutable binding and checking both registered targets and source digest.
// This path never constructs a driver or calls Kubernetes or maintenance APIs.
func rejectManagementTemplate(ctx context.Context, tx pgx.Tx, b ManagementBinding, corePhase, mode string) (ManagementProgress, error) {
	var empty ManagementProgress
	r, err := decodeManagementRecord(tx.QueryRow(ctx, `SELECT input_hash,phase,revision,checkpoint FROM station.runtime_management_transitions WHERE operation_id=$1 FOR UPDATE`, b.OperationID))
	missing := errors.Is(err, pgx.ErrNoRows)
	if err != nil && !missing {
		return empty, err
	}
	if !canRejectManagementTemplate(b, r, missing, corePhase, mode) {
		return empty, ErrManagementReplacement
	}
	if missing {
		err = tx.QueryRow(ctx, `INSERT INTO station.runtime_management_transitions(operation_id,station_id,app_id,input_hash,phase) VALUES($1,$2,$3,$4,'rejected_preflight') RETURNING revision`, b.OperationID, b.StationID, b.Source.AppID, managementHash(b)).Scan(&r.Revision)
	} else {
		err = tx.QueryRow(ctx, `UPDATE station.runtime_management_transitions SET phase='rejected_preflight',revision=revision+1,updated_at=now() WHERE operation_id=$1 AND revision=$2 RETURNING revision`, b.OperationID, r.Revision).Scan(&r.Revision)
	}
	if err != nil {
		return empty, err
	}
	return ManagementProgress{Phase: "rejected_preflight", Revision: r.Revision}, tx.Commit(ctx)
}
