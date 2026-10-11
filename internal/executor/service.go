package executor

import (
	"context"
	"errors"
	"slices"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	pb "github.com/verdantflarehub/verdantflare-station-runtime/api/runtimev1"
	"github.com/verdantflarehub/verdantflare-station-runtime/internal/releasebinding"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Service struct {
	pb.UnimplementedAppRuntimeServer
	Pool      *pgxpool.Pool
	StationID string
	Targets   map[string]Target
	Driver    Driver
	Timeout   time.Duration
	Releases  *releasebinding.Source
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
	if in.RequestId == "" || len(in.RequestId) > 128 || !validName(in.AppId) || !versionPattern.MatchString(in.AppVersion) || !slices.Contains([]string{"install", "start", "stop", "restart", "delete", "adopt"}, in.Action) {
		return nil, status.Error(codes.InvalidArgument, "Invalid application command")
	}
	if in.Action == "adopt" {
		if _, e := uuid.Parse(in.ExpectedWorkloadUid); e != nil {
			return nil, status.Error(codes.InvalidArgument, "Expected workload UID required")
		}
	} else if in.ExpectedWorkloadUid != "" {
		return nil, status.Error(codes.InvalidArgument, "Unexpected workload UID")
	}
	target, targetExists := s.Targets[in.AppId+"@"+in.AppVersion]
	hash := operationHash(in, len(target.InstallationBindings) > 0)
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
		// A removed registration must still allow an exact terminal replay. Both
		// known wire formats are checked against the saved digest, never rewritten.
		boundHash := operationHash(in, true)
		if string(savedHash) != string(hash[:]) && (targetExists || string(savedHash) != string(boundHash[:])) {
			return nil, status.Error(codes.AlreadyExists, "Operation ID payload conflict")
		}
		if out.Status != "running" {
			return out, nil
		}
	} else if !errors.Is(err, pgx.ErrNoRows) {
		return nil, status.Error(codes.Unavailable, "Runtime state unavailable")
	}
	// Do not accept a new execution while a conversion is active: an accepted
	// ordinary execution would otherwise block the converter's own recovery.
	// Exact terminal replay above is read-only and remains available.
	var transitioning bool
	if e := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM station.app_installation_transitions WHERE station_id=$1 AND app_id=$2 AND status NOT IN ('succeeded','failed'))`, s.StationID, in.AppId).Scan(&transitioning); e != nil {
		return nil, status.Error(codes.Unavailable, "Installation transition state unavailable")
	}
	if transitioning {
		return nil, status.Error(codes.Aborted, "Application has an active installation transition")
	}
	if !targetExists {
		if err == nil {
			return s.finish(ctx, tx, in.OperationId, failed("CONFLICT", "Registered template missing during recovery"))
		}
		return nil, status.Error(codes.FailedPrecondition, "Application version has no registered execution template")
	}
	if s.verifyPreinstalled(in, target) != nil {
		return nil, status.Error(codes.PermissionDenied, "Preinstalled release binding is not trusted")
	}
	target.Preinstalled = in.PreinstalledRelease != nil
	boundTarget, bindingErr := bindInstallation(target, in)
	if bindingErr != nil {
		return nil, status.Error(codes.FailedPrecondition, "Registered application requires a valid installation binding")
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
	boundTarget.ExpectedUID = in.ExpectedWorkloadUid
	result, err := s.Driver.Step(ctx, boundTarget, in.Action, in.OperationId, in.StationId, in.OrganizationId)
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
