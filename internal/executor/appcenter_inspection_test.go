package executor

import (
	"context"
	"testing"

	"github.com/google/uuid"
	pb "github.com/verdantflarehub/verdantflare-station-runtime/api/runtimev1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
)

func TestAppCenterInspectionReadOnlyInstall(t *testing.T) {
	for _, variant := range []string{"absent", "workload-exists", "service-exists", "missing-volume", "gpu", "models", "storage", "template", "wrong-station", "installation-injected", "unsupported-action"} {
		t.Run(variant, func(t *testing.T) {
			request, app := protectedFixture()
			app.InstallationBindings = []InstallationBinding{{Container: "server", Env: "APP_INSTALLATION_ID", Source: "installation_id"}}
			in := &pb.AppCenterInspection{SchemaVersion: 1, UserId: request.Command.UserId, OrganizationId: request.Command.OrganizationId, StationId: request.Command.StationId,
				AppId: app.AppID, AppVersion: app.Version, Action: "install", TemplateDigest: request.Guard.TemplateDigest}
			client := fake.NewSimpleClientset()
			switch variant {
			case "missing-volume":
				app.Deployment.Spec.Template.Spec.Volumes = []corev1.Volume{{Name: "data", VolumeSource: corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: "missing"}}}}
			case "workload-exists":
				client.Tracker().Add(app.Deployment.DeepCopy())
			case "service-exists":
				svc := &corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: "existing", Namespace: app.Deployment.Namespace}}
				app.Services = []*corev1.Service{svc}
				client.Tracker().Add(svc.DeepCopy())
			case "gpu":
				app.GPU = 1
			case "models":
				app.Models = []string{"model"}
			case "storage":
				app.DiskBytes = 1
			case "template":
				app.Hash[0]++
			case "wrong-station":
				in.StationId = uuid.NewString()
			case "installation-injected":
				in.InstallationId = request.Command.InstallationId
			case "unsupported-action":
				in.Action = "adopt"
			}
			svc := &Service{StationID: request.Command.StationId, Targets: map[string]Target{app.AppID + "@" + app.Version: app}, Driver: &Kubernetes{Client: client}}
			out, err := svc.InspectBound(context.Background(), in)
			if variant == "absent" || variant == "gpu" {
				if err != nil || out.State != "absent" || out.WorkloadUid != "" || out.TemplateDigest != in.TemplateDigest {
					t.Fatal(out, err)
				}
			} else if err == nil {
				t.Fatal("unsafe inspection accepted", out)
			}
			assertAppCenterReadsOnly(t, client)
		})
	}
}

func TestAppCenterInspectionStartOwnershipAndState(t *testing.T) {
	for _, variant := range []string{"stopped", "running", "stopping", "unowned", "other-installation", "missing", "invalid-uid"} {
		t.Run(variant, func(t *testing.T) {
			request, app := protectedFixture()
			request.Command.Action = "start"
			in := &pb.AppCenterInspection{SchemaVersion: 1, UserId: request.Command.UserId, OrganizationId: request.Command.OrganizationId, StationId: request.Command.StationId,
				AppId: app.AppID, AppVersion: app.Version, Action: "start", InstallationId: request.Command.InstallationId, TemplateDigest: request.Guard.TemplateDigest}
			live := app.Deployment.DeepCopy()
			bound := app
			bound.InstallationID = in.InstallationId
			mark(&live.ObjectMeta, bound, in.StationId, in.OrganizationId)
			live.UID = types.UID(uuid.NewString())
			zero := int32(0)
			live.Spec.Replicas = &zero
			switch variant {
			case "running":
				one := int32(1)
				live.Spec.Replicas = &one
			case "stopping":
				live.Status.Replicas = 1
			case "unowned":
				live.Annotations = nil
			case "other-installation":
				live.Annotations[installationKey] = uuid.Must(uuid.NewV7()).String()
			case "invalid-uid":
				live.UID = "invalid"
			}
			client := fake.NewSimpleClientset()
			if variant != "missing" {
				client.Tracker().Add(live)
			}
			svc := &Service{StationID: in.StationId, Targets: map[string]Target{app.AppID + "@" + app.Version: app}, Driver: &Kubernetes{Client: client}}
			out, err := svc.InspectBound(context.Background(), in)
			if variant == "stopped" {
				if err != nil || out.State != "stopped" || out.WorkloadUid != string(live.UID) {
					t.Fatal(out, err)
				}
			} else if status.Code(err) != codes.FailedPrecondition {
				t.Fatal("unsafe inspection accepted", out, err)
			}
			assertAppCenterReadsOnly(t, client)
		})
	}
}
