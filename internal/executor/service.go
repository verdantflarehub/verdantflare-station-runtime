package executor

import (
	"context"
	"crypto/sha256"
	"errors"
	"slices"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	pb "github.com/verdantflarehub/verdantflare-station-runtime/api/runtimev1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type Service struct {
	pb.UnimplementedAppRuntimeServer
	Pool      *pgxpool.Pool
	StationID string
	Targets   map[string]Target
	Driver    Driver
	Timeout   time.Duration
}

func (s *Service) Reconcile(ctx context.Context, in *pb.AppRequest) (*pb.AppProgress, error) {
	for _, v := range []string{in.OperationId, in.UserId, in.OrganizationId, in.StationId} {
		if _, e := uuid.Parse(v); e != nil {
			return nil, status.Error(codes.InvalidArgument, "Invalid identity or operation ID")
		}
	}
	if in.StationId != s.StationID {
		return nil, status.Error(codes.PermissionDenied, "Station mismatch")
	}
	if in.RequestId == "" || len(in.RequestId) > 128 || !validName(in.AppId) || !versionPattern.MatchString(in.AppVersion) || !slices.Contains([]string{"install", "start", "stop", "restart", "delete"}, in.Action) {
		return nil, status.Error(codes.InvalidArgument, "Invalid application command")
	}
	normalized := proto.Clone(in).(*pb.AppRequest)
	normalized.RequestId = ""
	raw, _ := proto.MarshalOptions{Deterministic: true}.Marshal(normalized)
	hash := sha256.Sum256(raw)
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return nil, status.Error(codes.Unavailable, "Runtime database unavailable")
	}
	defer tx.Rollback(context.Background())
	// Workload registrations are station-wide; serialize even across organizations.
	if _, err = tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1,0))`, "runtime-app:"+s.StationID+":"+in.AppId); err != nil {
		return nil, status.Error(codes.Unavailable, "Runtime busy")
	}
	out := &pb.AppProgress{OperationId: in.OperationId, Status: "running", Phase: "checking"}
	var savedHash, targetHash []byte
	var created time.Time
	err = tx.QueryRow(ctx, `SELECT request_hash,target_hash,status,phase,error_code,error_message,created_at FROM station.runtime_app_executions WHERE operation_id=$1`, in.OperationId).Scan(&savedHash, &targetHash, &out.Status, &out.Phase, &out.ErrorCode, &out.ErrorMessage, &created)
	if err == nil {
		if string(savedHash) != string(hash[:]) {
			return nil, status.Error(codes.AlreadyExists, "Operation ID payload conflict")
		}
		if out.Status != "running" {
			return out, nil
		}
	} else if !errors.Is(err, pgx.ErrNoRows) {
		return nil, status.Error(codes.Unavailable, "Runtime state unavailable")
	}
	target, ok := s.Targets[in.AppId+"@"+in.AppVersion]
	if !ok {
		if err == nil {
			return s.finish(ctx, tx, in.OperationId, failed("CONFLICT", "Registered template missing during recovery"))
		}
		return nil, status.Error(codes.FailedPrecondition, "Application version has no registered execution template")
	}
	if errors.Is(err, pgx.ErrNoRows) {
		var busy bool
		if e := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM station.runtime_app_executions WHERE station_id=$1 AND app_id=$2 AND status='running')`, s.StationID, in.AppId).Scan(&busy); e != nil {
			return nil, status.Error(codes.Unavailable, "Runtime state unavailable")
		}
		if busy {
			return nil, status.Error(codes.Aborted, "Application has an active execution")
		}
		_, err = tx.Exec(ctx, `INSERT INTO station.runtime_app_executions(operation_id,station_id,organization_id,user_id,request_hash,app_id,app_version,action,target_hash,status,phase) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,'running','checking')`, in.OperationId, in.StationId, in.OrganizationId, in.UserId, hash[:], in.AppId, in.AppVersion, in.Action, target.Hash[:])
		if err != nil {
			return nil, status.Error(codes.Unavailable, "Cannot record runtime execution")
		}
		// Commit identity and start time before any external side effect.
		if err = tx.Commit(ctx); err != nil {
			return nil, status.Error(codes.Unavailable, "Cannot persist runtime execution")
		}
		return out, nil
	}
	if string(targetHash) != string(target.Hash[:]) {
		return s.finish(ctx, tx, in.OperationId, failed("CONFLICT", "Registered template changed while operation was running"))
	}
	timeout := s.Timeout
	if timeout == 0 {
		timeout = 15 * time.Minute
	}
	if time.Since(created) > timeout {
		return s.finish(ctx, tx, in.OperationId, failed("SERVICE_UNAVAILABLE", "Runtime operation timed out; inspect workload before retrying"))
	}
	result, err := s.Driver.Step(ctx, target, in.Action, in.OperationId, in.StationId, in.OrganizationId)
	if err != nil {
		// Kubernetes/network uncertainty is retryable; preserve the operation and start time.
		return nil, status.Error(codes.Unavailable, "Kubernetes observation unavailable; retry original operation")
	}
	return s.finish(ctx, tx, in.OperationId, result)
}
func (s *Service) finish(ctx context.Context, tx pgx.Tx, id string, r Result) (*pb.AppProgress, error) {
	if _, err := tx.Exec(ctx, `UPDATE station.runtime_app_executions SET status=$2,phase=$3,error_code=$4,error_message=$5,updated_at=now() WHERE operation_id=$1`, id, r.Status, r.Phase, r.Code, r.Message); err != nil {
		return nil, status.Error(codes.Unavailable, "Cannot persist runtime progress")
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, status.Error(codes.Unavailable, "Cannot commit runtime progress")
	}
	return &pb.AppProgress{OperationId: id, Status: r.Status, Phase: r.Phase, ErrorCode: r.Code, ErrorMessage: r.Message}, nil
}
