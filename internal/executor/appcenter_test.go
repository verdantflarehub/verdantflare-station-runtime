package executor

import (
	"context"
	"encoding/hex"
	"strings"
	"testing"

	"github.com/google/uuid"
	pb "github.com/verdantflarehub/verdantflare-station-runtime/api/runtimev1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
)

func protectedFixture() (*pb.AppCenterRequest, Target) {
	t := target()
	r := &pb.AppCenterRequest{Command: &pb.AppRequest{OperationId: uuid.NewString(), RequestId: "trace",
		UserId: uuid.NewString(), OrganizationId: uuid.NewString(), StationId: uuid.NewString(),
		AppId: t.AppID, AppVersion: t.Version, Action: "install", InstallationId: uuid.Must(uuid.NewV7()).String()},
		Guard: &pb.AppCenterGuard{SchemaVersion: 1, ReleaseDigest: "sha256:" + strings.Repeat("a", 64),
			TemplateDigest: "sha256:" + hex.EncodeToString(t.Hash[:]), PolicyRevision: "sha256:" + strings.Repeat("b", 64)}}
	return r, t
}

func TestAppCenterGuardValidationAndHash(t *testing.T) {
	r, app := protectedFixture()
	if err := validateAppCenterRequest(r); err != nil {
		t.Fatal(err)
	}
	original := appCenterOperationHash(r)
	changed := proto.Clone(r).(*pb.AppCenterRequest)
	changed.Command.RequestId = "retry-trace"
	if appCenterOperationHash(changed) != original || operationHash(r.Command, true) == original {
		t.Fatal("wrong trace normalization or cross-protocol digest")
	}
	for _, mutate := range []func(*pb.AppCenterRequest){
		func(v *pb.AppCenterRequest) { v.Command.InstallationId = uuid.Must(uuid.NewV7()).String() },
		func(v *pb.AppCenterRequest) { v.Guard.ReleaseDigest = "sha256:" + strings.Repeat("c", 64) },
		func(v *pb.AppCenterRequest) { v.Guard.PolicyRevision = "sha256:" + strings.Repeat("c", 64) },
		func(v *pb.AppCenterRequest) { v.Guard.TemplateDigest = "sha256:" + strings.Repeat("c", 64) },
		func(v *pb.AppCenterRequest) { v.Guard.WorkloadUid = uuid.NewString() },
	} {
		v := proto.Clone(r).(*pb.AppCenterRequest)
		mutate(v)
		if appCenterOperationHash(v) == original {
			t.Fatal("execution guard omitted from digest")
		}
	}
	for _, mutate := range []func(*pb.AppCenterRequest){
		func(v *pb.AppCenterRequest) { v.Guard = nil },
		func(v *pb.AppCenterRequest) { v.Command = nil },
		func(v *pb.AppCenterRequest) { v.Guard.SchemaVersion = 2 },
		func(v *pb.AppCenterRequest) { v.Command.InstallationId = "" },
		func(v *pb.AppCenterRequest) { v.Command.InstallationId = uuid.NewString() },
		func(v *pb.AppCenterRequest) { v.Guard.TemplateDigest = "latest" },
		func(v *pb.AppCenterRequest) { v.Command.PreinstalledRelease = &pb.PreinstalledRelease{} },
		func(v *pb.AppCenterRequest) { v.Command.ExpectedWorkloadUid = uuid.NewString() },
		func(v *pb.AppCenterRequest) { v.Command.Action = "start" },
		func(v *pb.AppCenterRequest) { v.Guard.WorkloadUid = uuid.NewString() },
		func(v *pb.AppCenterRequest) { v.ProtoReflect().SetUnknown([]byte{0x78, 1}) },
		func(v *pb.AppCenterRequest) { v.Guard.ProtoReflect().SetUnknown([]byte{0x78, 1}) },
		func(v *pb.AppCenterRequest) { v.Command.ProtoReflect().SetUnknown([]byte{0x78, 1}) },
	} {
		v := proto.Clone(r).(*pb.AppCenterRequest)
		mutate(v)
		if status.Code(validateAppCenterRequest(v)) != codes.InvalidArgument {
			t.Fatal("invalid guard accepted")
		}
	}
	for _, action := range []string{"adopt", "stop", "restart", "delete", "upgrade", "rollback"} {
		v := proto.Clone(r).(*pb.AppCenterRequest)
		v.Command.Action = action
		if status.Code(validateAppCenterRequest(v)) != codes.FailedPrecondition {
			t.Fatal("unsupported guard action accepted", action)
		}
	}
	bound, err := bindAppCenterTarget(app, r.Command, r.Guard)
	if err != nil || bound.InstallationID != r.Command.InstallationId || !bound.AppCenter || app.InstallationID != "" {
		t.Fatal("unbound template lost installation identity", err)
	}
	app.Hash[0]++
	if _, err := bindAppCenterTarget(app, r.Command, r.Guard); status.Code(err) != codes.FailedPrecondition {
		t.Fatal("changed template accepted")
	}
}

func TestAppCenterKubernetesInstallRecovery(t *testing.T) {
	r, app := protectedFixture()
	app.Services = []*corev1.Service{{ObjectMeta: metav1.ObjectMeta{Name: "app", Namespace: app.Deployment.Namespace}, Spec: corev1.ServiceSpec{Selector: map[string]string{"app": "test"}}}}
	app, _ = bindAppCenterTarget(app, r.Command, r.Guard)
	c := fake.NewSimpleClientset()
	driver := &Kubernetes{Client: c}
	ctx := context.Background()
	check := func(first bool) error {
		return driver.CheckAppCenter(ctx, app, "install", r.Command.OperationId, r.Command.StationId, r.Command.OrganizationId, first)
	}
	step := func() Result {
		t.Helper()
		out, err := driver.Step(ctx, app, "install", r.Command.OperationId, r.Command.StationId, r.Command.OrganizationId)
		if err != nil {
			t.Fatal(err)
		}
		return out
	}
	if err := check(true); err != nil {
		t.Fatal(err)
	}
	if out := step(); out.Status != "running" {
		t.Fatal(out)
	}
	if err := check(true); status.Code(err) != codes.FailedPrecondition {
		t.Fatal("new operation accepted existing workload")
	}
	if err := check(false); err != nil {
		t.Fatal("own create did not recover", err)
	}
	if out := step(); out.Status != "running" {
		t.Fatal(out)
	}
	svc, err := c.CoreV1().Services(app.Deployment.Namespace).Get(ctx, "app", metav1.GetOptions{})
	if err != nil || svc.Annotations[installationKey] != app.InstallationID || svc.Annotations[appCenterOperationKey] != r.Command.OperationId {
		t.Fatal("Service binding missing", err)
	}
	if err := driver.CheckAppCenter(ctx, app, "install", "other-operation", r.Command.StationId, r.Command.OrganizationId, false); status.Code(err) != codes.FailedPrecondition {
		t.Fatal("foreign create adopted")
	}
	deployment, _ := c.AppsV1().Deployments(app.Deployment.Namespace).Get(ctx, app.Deployment.Name, metav1.GetOptions{})
	delete(deployment.Annotations, appCenterOperationKey)
	c.AppsV1().Deployments(app.Deployment.Namespace).Update(ctx, deployment, metav1.UpdateOptions{})
	c.ClearActions()
	if out := step(); out.Code != "CONFLICT" {
		t.Fatal(out)
	}
	assertAppCenterReadsOnly(t, c)
}

func assertAppCenterReadsOnly(t *testing.T, c *fake.Clientset) {
	t.Helper()
	for _, a := range c.Actions() {
		if a.GetVerb() != "get" {
			t.Fatal("unexpected write", a.GetVerb(), a.GetResource().Resource)
		}
	}
}

func TestAppCenterKubernetesStartIdentity(t *testing.T) {
	for _, variant := range []string{"valid", "missing", "replaced-uid", "reinstalled", "unowned", "changed-template", "deleting"} {
		t.Run(variant, func(t *testing.T) {
			r, app := protectedFixture()
			r.Command.Action, r.Guard.WorkloadUid = "start", uuid.NewString()
			app, _ = bindAppCenterTarget(app, r.Command, r.Guard)
			live := app.Deployment.DeepCopy()
			live.UID, live.ResourceVersion = types.UID(r.Guard.WorkloadUid), "7"
			zero := int32(0)
			live.Spec.Replicas = &zero
			mark(&live.ObjectMeta, app, r.Command.StationId, r.Command.OrganizationId)
			switch variant {
			case "replaced-uid":
				live.UID = types.UID(uuid.NewString())
			case "reinstalled":
				live.Annotations[installationKey] = uuid.Must(uuid.NewV7()).String()
			case "unowned":
				live.Annotations = nil
			case "changed-template":
				live.Spec.Template.Spec.Containers[0].Image = "example.invalid/other:2.0.0"
			case "deleting":
				now := metav1.Now()
				live.DeletionTimestamp = &now
			}
			c := fake.NewSimpleClientset()
			if variant != "missing" {
				c.Tracker().Add(live)
			}
			out, err := (&Kubernetes{Client: c}).Step(context.Background(), app, "start", r.Command.OperationId, r.Command.StationId, r.Command.OrganizationId)
			if err != nil {
				t.Fatal(err)
			}
			if variant == "valid" {
				if out.Status != "running" || out.Phase != "starting" {
					t.Fatal(out)
				}
				updated, _ := c.AppsV1().Deployments(live.Namespace).Get(context.Background(), live.Name, metav1.GetOptions{})
				if *updated.Spec.Replicas != 1 || updated.ResourceVersion != "7" || updated.UID != live.UID {
					t.Fatal("lost update precondition")
				}
			} else {
				if out.Code != "CONFLICT" {
					t.Fatal(out)
				}
				assertAppCenterReadsOnly(t, c)
			}
		})
	}
}
