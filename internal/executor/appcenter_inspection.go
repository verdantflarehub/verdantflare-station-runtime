package executor

import (
	"context"
	"encoding/hex"

	"github.com/google/uuid"
	pb "github.com/verdantflarehub/verdantflare-station-runtime/api/runtimev1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type AppCenterInspector interface {
	InspectAppCenter(context.Context, Target, string, string, string) (*pb.AppCenterObservation, error)
}

func (s *Service) InspectBound(ctx context.Context, in *pb.AppCenterInspection) (*pb.AppCenterObservation, error) {
	invalid := status.Error(codes.InvalidArgument, "Invalid protected inspection")
	if in == nil || proto.Size(in) > 4096 || len(in.ProtoReflect().GetUnknown()) != 0 || in.SchemaVersion != 1 ||
		!validName(in.AppId) || !versionPattern.MatchString(in.AppVersion) || len(in.AppVersion) > 64 || !appCenterDigest.MatchString(in.TemplateDigest) {
		return nil, invalid
	}
	for _, value := range []string{in.UserId, in.OrganizationId, in.StationId} {
		id, err := uuid.Parse(value)
		if err != nil || id == uuid.Nil || id.String() != value {
			return nil, invalid
		}
	}
	if in.StationId != s.StationID {
		return nil, status.Error(codes.PermissionDenied, "Station mismatch")
	}
	switch in.Action {
	case "install":
		if in.InstallationId != "" {
			return nil, invalid
		}
	case "start", "stop", "restart", "delete":
		id, err := uuid.Parse(in.InstallationId)
		if err != nil || id.Version() != 7 || id.String() != in.InstallationId {
			return nil, invalid
		}
	default:
		return nil, status.Error(codes.FailedPrecondition, "Action has no protected execution support")
	}
	t, exists := s.Targets[in.AppId+"@"+in.AppVersion]
	if !exists || t.Deployment == nil || t.AppID != in.AppId || t.Version != in.AppVersion ||
		in.TemplateDigest != "sha256:"+hex.EncodeToString(t.Hash[:]) || !validInstallationBindings(t) ||
		t.DiskBytes > 0 || len(t.Models) > 0 {
		return nil, status.Error(codes.FailedPrecondition, "Protected execution template or preparation support unavailable")
	}
	if in.Action == "start" || in.Action == "stop" || in.Action == "restart" || in.Action == "delete" {
		var err error
		t, err = bindInstallation(t, &pb.AppRequest{InstallationId: in.InstallationId, StationId: in.StationId, OrganizationId: in.OrganizationId, AppId: in.AppId, AppVersion: in.AppVersion})
		if err != nil {
			return nil, status.Error(codes.FailedPrecondition, "Invalid installation binding")
		}
	}
	t.AppCenter, t.InstallationID = true, in.InstallationId
	inspector, ok := s.Driver.(AppCenterInspector)
	if !ok {
		return nil, status.Error(codes.Unimplemented, "Protected inspection driver unavailable")
	}
	return inspector.InspectAppCenter(ctx, t, in.Action, in.StationId, in.OrganizationId)
}

// No database, creation marker or synthetic installation is needed to inspect
// an absent workload. An existing installation uses a fresh UID and then checks
// it again alongside all resource ownership before returning an observation.
func (k *Kubernetes) InspectAppCenter(ctx context.Context, t Target, action, station, org string) (*pb.AppCenterObservation, error) {
	out := &pb.AppCenterObservation{SchemaVersion: 1, TemplateDigest: "sha256:" + hex.EncodeToString(t.Hash[:]), Namespace: t.Deployment.Namespace, WorkloadName: t.Deployment.Name}
	live, err := k.Client.AppsV1().Deployments(out.Namespace).Get(ctx, out.WorkloadName, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		if action != "install" {
			return nil, status.Error(codes.FailedPrecondition, "Installed workload is missing")
		}
		out.State = "absent"
	} else if err != nil {
		return nil, status.Error(codes.Unavailable, "Workload observation unavailable")
	} else {
		if action == "install" {
			return nil, status.Error(codes.FailedPrecondition, "Install requires an absent workload")
		}
		id, e := uuid.Parse(string(live.UID))
		if e != nil || id == uuid.Nil || id.String() != string(live.UID) {
			return nil, status.Error(codes.FailedPrecondition, "Workload has invalid identity")
		}
		isStopped := live.Spec.Replicas != nil && *live.Spec.Replicas == 0 &&
			live.Status.ObservedGeneration >= live.Generation && live.Status.Replicas == 0
		isRunning := live.Spec.Replicas != nil && *live.Spec.Replicas > 0 &&
			live.Status.ObservedGeneration >= live.Generation && live.Status.AvailableReplicas > 0
		switch action {
		case "start":
			if !isStopped {
				return nil, status.Error(codes.FailedPrecondition, "Start requires an observed stopped workload")
			}
			out.State = "stopped"
		case "stop":
			if !isRunning {
				return nil, status.Error(codes.FailedPrecondition, "Stop requires an observed running workload")
			}
			out.State = "running"
		case "restart", "delete":
			if !isRunning && !isStopped {
				return nil, status.Error(codes.FailedPrecondition, "Action requires an observed running or stopped workload")
			}
			if isRunning {
				out.State = "running"
			} else {
				out.State = "stopped"
			}
		default:
			return nil, status.Error(codes.FailedPrecondition, "Action has no protected execution support")
		}
		t.ExpectedUID, out.WorkloadUid = string(live.UID), string(live.UID)
	}
	if err := k.CheckAppCenter(ctx, t, action, "", station, org, true); err != nil {
		return nil, err
	}
	if action == "install" {
		if err := k.dependencies(ctx, t); err != nil {
			return nil, status.Error(codes.FailedPrecondition, "Required workload dependencies are unavailable")
		}
	}
	return out, nil
}
