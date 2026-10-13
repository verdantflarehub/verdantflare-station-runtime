package executor

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"regexp"

	"github.com/google/uuid"
	pb "github.com/verdantflarehub/verdantflare-station-runtime/api/runtimev1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// CheckAppCenter is read-only. The same guard must also be enforced by Step
// before external writes; an admission read alone is not an execution guard.
type AppCenterDriver interface {
	CheckAppCenter(context.Context, Target, string, string, string, string, bool) error
}

var appCenterDigest = regexp.MustCompile(`^sha256:[a-f0-9]{64}$`)

func (s *Service) ReconcileBound(ctx context.Context, in *pb.AppCenterRequest) (*pb.AppProgress, error) {
	if err := validateAppCenterRequest(in); err != nil {
		return nil, err
	}
	return s.reconcile(ctx, in.Command, in.Guard)
}

func validateAppCenterRequest(in *pb.AppCenterRequest) error {
	invalid := status.Error(codes.InvalidArgument, "Invalid protected execution binding")
	if in == nil || in.Command == nil || in.Guard == nil || proto.Size(in) > 16384 ||
		len(in.ProtoReflect().GetUnknown()) != 0 || len(in.Command.ProtoReflect().GetUnknown()) != 0 || len(in.Guard.ProtoReflect().GetUnknown()) != 0 {
		return invalid
	}
	c, g := in.Command, in.Guard
	id, err := uuid.Parse(c.InstallationId)
	if err != nil || id.Version() != 7 || id.String() != c.InstallationId || g.SchemaVersion != 1 ||
		!appCenterDigest.MatchString(g.ReleaseDigest) || !appCenterDigest.MatchString(g.TemplateDigest) || !appCenterDigest.MatchString(g.PolicyRevision) ||
		c.ExpectedWorkloadUid != "" || c.PreinstalledRelease != nil {
		return invalid
	}
	switch c.Action {
	case "install":
		if g.WorkloadUid != "" {
			return invalid
		}
	case "start", "stop", "restart":
		uid, err := uuid.Parse(g.WorkloadUid)
		if err != nil || uid == uuid.Nil || uid.String() != g.WorkloadUid {
			return invalid
		}
	default:
		return status.Error(codes.FailedPrecondition, "Action has no protected execution support")
	}
	return nil
}

func appCenterOperationHash(in *pb.AppCenterRequest) [32]byte {
	copy := proto.Clone(in).(*pb.AppCenterRequest)
	copy.Command.RequestId = ""
	raw, _ := proto.MarshalOptions{Deterministic: true}.Marshal(copy)
	return sha256.Sum256(append([]byte("station.runtime.appcenter.v1\x00"), raw...))
}

func bindAppCenterTarget(t Target, in *pb.AppRequest, guard *pb.AppCenterGuard) (Target, error) {
	if guard.TemplateDigest != "sha256:"+hex.EncodeToString(t.Hash[:]) {
		return Target{}, status.Error(codes.FailedPrecondition, "Protected execution template changed")
	}
	// Ownership is mandatory even for templates without injected environment.
	t.AppCenter, t.InstallationID, t.ExpectedUID = true, in.InstallationId, guard.WorkloadUid
	return t, nil
}
