package appinstances

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/verdantflarehub/verdantflare-station-runtime/internal/strictjson"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type StopRecord struct {
	OperationID string
	StartID     string
	BootID      string
	Progress    StopProgress
	Revision    int64
}

func scanStop(row pgx.Row, in Run, p Profile, r StartRecord, operation, boot string) (StopRecord, error) {
	var s StopRecord
	var raw []byte
	if e := row.Scan(&s.OperationID, &s.StartID, &s.BootID, &s.Progress.Phase, &raw, &s.Revision); e != nil {
		return s, e
	}
	if s.OperationID != operation || s.StartID != in.OperationID || s.BootID != boot || boot != r.Observation.BootID || s.Revision < 1 {
		return StopRecord{}, ErrConflict
	}
	if s.Progress.Phase == "accepted" {
		if string(raw) != "{}" || r.Phase != "stopping" {
			return StopRecord{}, ErrBinding
		}
	} else {
		if _, ok := stopPhases[s.Progress.Phase]; !ok || strictjson.Decode(bytes.NewReader(raw), 4096, &s.Progress.Proof) != nil ||
			s.Progress.Proof != stopProof(in, p, r.Observation, operation) ||
			(s.Progress.Phase == "stopped" && r.Phase != "stopped") || (s.Progress.Phase != "stopped" && r.Phase != "stopping") {
			return StopRecord{}, ErrBinding
		}
	}
	return s, nil
}

func acceptStop(ctx context.Context, conn *pgxpool.Conn, in Run, p Profile, r *StartRecord, operation, boot string) (StopRecord, error) {
	s, e := scanStop(conn.QueryRow(ctx, `SELECT operation_id,start_operation_id,boot_id,phase,proof,revision FROM station.app_instance_stops WHERE operation_id=$1`, operation), in, p, *r, operation, boot)
	if e == nil || !errors.Is(e, pgx.ErrNoRows) {
		return s, e
	}
	if r.Phase != "running" || r.Observation.BootID != boot {
		return StopRecord{}, ErrConflict
	}
	tx, e := conn.Begin(ctx)
	if e != nil {
		return StopRecord{}, e
	}
	defer rollback(tx)
	if _, e = tx.Exec(ctx, `INSERT INTO station.app_instance_stops(operation_id,start_operation_id,boot_id,phase) VALUES($1,$2,$3,'accepted')`, operation, in.OperationID, boot); e != nil {
		var conflict *pgconn.PgError
		if errors.As(e, &conflict) && conflict.Code == "23505" {
			return StopRecord{}, ErrConflict
		}
		return StopRecord{}, e
	}
	result, e := tx.Exec(ctx, `UPDATE station.app_instance_starts SET phase='stopping',revision=revision+1,updated_at=now() WHERE operation_id=$1 AND revision=$2 AND phase='running'`, in.OperationID, r.Revision)
	if e != nil {
		return StopRecord{}, e
	}
	if result.RowsAffected() != 1 {
		return StopRecord{}, ErrConflict
	}
	if e = tx.Commit(ctx); e != nil {
		return StopRecord{}, e
	}
	r.Phase, r.Revision = "stopping", r.Revision+1
	return StopRecord{OperationID: operation, StartID: in.OperationID, BootID: boot, Revision: 1, Progress: StopProgress{Phase: "accepted"}}, nil
}

func checkpointStop(ctx context.Context, conn *pgxpool.Conn, r *StartRecord, s *StopRecord, next StopProgress) error {
	valid := s.Progress.Phase == "accepted" && next.Phase == "sealed"
	if s.Progress.Phase != "accepted" {
		before, a := stopPhases[s.Progress.Phase]
		after, b := stopPhases[next.Phase]
		valid = a && b && after == before+1 && next.Proof == s.Progress.Proof
	}
	if !valid || r.Phase != "stopping" || next.Proof.StopOperationID != s.OperationID || next.Proof.BootID != s.BootID {
		return ErrConflict
	}
	raw, _ := json.Marshal(next.Proof)
	tx, e := conn.Begin(ctx)
	if e != nil {
		return e
	}
	defer rollback(tx)
	result, e := tx.Exec(ctx, `UPDATE station.app_instance_stops SET phase=$1,proof=$2,revision=revision+1,updated_at=now() WHERE operation_id=$3 AND revision=$4 AND phase=$5`,
		next.Phase, raw, s.OperationID, s.Revision, s.Progress.Phase)
	if e != nil {
		return e
	}
	if result.RowsAffected() != 1 {
		return ErrConflict
	}
	if next.Phase == "stopped" {
		result, e = tx.Exec(ctx, `UPDATE station.app_instance_starts SET phase='stopped',revision=revision+1,updated_at=now() WHERE operation_id=$1 AND revision=$2 AND phase='stopping'`, r.Run.OperationID, r.Revision)
		if e != nil {
			return e
		}
		if result.RowsAffected() != 1 {
			return ErrConflict
		}
	}
	if e = tx.Commit(ctx); e != nil {
		return e
	}
	if next.Phase == "stopped" {
		r.Phase, r.Revision = "stopped", r.Revision+1
	}
	s.Progress, s.Revision = next, s.Revision+1
	return nil
}

// Stop is called only by the trusted App after its durable admission seal. It
// holds the same lock as start/access across proof and deletion checkpoints.
func (j *Journal) Stop(ctx context.Context, in Run, p Profile, operation, boot string) (StopRecord, error) {
	if j == nil || j.Stopper == nil || !validID(operation) || operation == in.OperationID || !validBoot(boot) {
		return StopRecord{}, ErrInvalid
	}
	var s StopRecord
	_, e := j.withRecord(ctx, in, p, func(conn *pgxpool.Conn, r *StartRecord) error {
		var e error
		s, e = acceptStop(ctx, conn, in, p, r, operation, boot)
		if e != nil || s.Progress.Phase == "stopped" {
			return e
		}
		if s.Progress.Phase == "accepted" {
			if _, e := ObserveWorker(ctx, j.Client, in, p, r.Volume, r.Observation); e != nil {
				return e
			}
			secret, e := j.Client.CoreV1().Secrets(p.Namespace).Get(ctx, instanceName(in.Identity), metav1.GetOptions{})
			if e != nil || !secretMatches(secret, in.Identity, p, r.Observation.SecretUID) {
				return ErrBinding
			}
			proof, e := j.Stopper.Seal(ctx, in, p, r.Observation, string(secret.Data["worker-token"]), operation)
			if e != nil {
				return e
			}
			if proof != stopProof(in, p, r.Observation, operation) {
				return ErrBinding
			}
			if _, e = ObserveWorker(ctx, j.Client, in, p, r.Volume, r.Observation); e != nil {
				return e
			}
			if e = checkpointStop(ctx, conn, r, &s, StopProgress{Proof: proof, Phase: "sealed"}); e != nil {
				return e
			}
		}
		_, e = RemoveWorker(ctx, j.Client, in, p, r.Volume, r.Observation, s.Progress, func(ctx context.Context, next StopProgress) error {
			return checkpointStop(ctx, conn, r, &s, next)
		})
		return e
	})
	return s, e
}
