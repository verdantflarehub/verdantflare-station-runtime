package executor

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/google/uuid"
	pb "github.com/verdantflarehub/verdantflare-station-runtime/api/runtimev1"
	"google.golang.org/protobuf/proto"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

func installationFixture() (Target, *pb.AppRequest) {
	t := target()
	t.InstallationBindings = []InstallationBinding{{"server", "APP_INSTALLATION", "installation_id"},
		{"server", "APP_STATION", "station_id"}, {"server", "APP_VERSION", "app_version"}}
	in := &pb.AppRequest{OperationId: uuid.Must(uuid.NewV7()).String(), InstallationId: uuid.Must(uuid.NewV7()).String(),
		UserId: uuid.Must(uuid.NewV7()).String(), StationId: uuid.Must(uuid.NewV7()).String(), OrganizationId: uuid.Must(uuid.NewV7()).String(),
		AppId: t.AppID, AppVersion: t.Version, Action: "install", RequestId: "trace-a"}
	return t, in
}

func TestInstallationBindingIsPrivateAndRequiresIdentity(t *testing.T) {
	template, in := installationFixture()
	bound, err := bindInstallation(template, in)
	if err != nil {
		t.Fatal(err)
	}
	if len(template.Deployment.Spec.Template.Spec.Containers[0].Env) != 0 || template.InstallationID != "" {
		t.Fatal("shared registration mutated")
	}
	if bound.InstallationID != in.InstallationId || bound.Hash != template.Hash {
		t.Fatal("binding identity lost")
	}
	want := []corev1.EnvVar{{Name: "APP_INSTALLATION", Value: in.InstallationId}, {Name: "APP_STATION", Value: in.StationId}, {Name: "APP_VERSION", Value: in.AppVersion}}
	if !reflect.DeepEqual(bound.Deployment.Spec.Template.Spec.Containers[0].Env, want) {
		t.Fatal("wrong injected context")
	}
	for _, id := range []string{"", uuid.NewString(), "019ABC00-0000-7000-8000-000000000001", "bad"} {
		in.InstallationId = id
		if _, err := bindInstallation(template, in); err == nil {
			t.Fatal("invalid installation accepted")
		}
	}
}

func TestInstallationBindingsRejectOverridesAndUndeclaredSources(t *testing.T) {
	for name, change := range map[string]func(*Target){
		"unknown container": func(v *Target) { v.InstallationBindings[0].Container = "other" },
		"unknown source":    func(v *Target) { v.InstallationBindings[0].Source = "worker_token" },
		"invalid env":       func(v *Target) { v.InstallationBindings[0].Env = "A-B" },
		"duplicate env":     func(v *Target) { v.InstallationBindings = append(v.InstallationBindings, v.InstallationBindings[0]) },
		"literal override": func(v *Target) {
			v.Deployment.Spec.Template.Spec.Containers[0].Env = []corev1.EnvVar{{Name: "APP_INSTALLATION", Value: "literal"}}
		},
		"reference override": func(v *Target) {
			v.Deployment.Spec.Template.Spec.Containers[0].Env = []corev1.EnvVar{{Name: "APP_INSTALLATION", ValueFrom: &corev1.EnvVarSource{}}}
		},
		"oversize": func(v *Target) {
			for len(v.InstallationBindings) < 33 {
				v.InstallationBindings = append(v.InstallationBindings, v.InstallationBindings[0])
			}
		},
	} {
		t.Run(name, func(t *testing.T) {
			v, in := installationFixture()
			change(&v)
			if _, err := bindInstallation(v, in); err == nil {
				t.Fatal("unsafe binding accepted")
			}
		})
	}
}

func TestInstallationReplayHashPreservesLegacyAndBindsNewTargets(t *testing.T) {
	_, in := installationFixture()
	legacy := proto.Clone(in).(*pb.AppRequest)
	legacy.InstallationId, legacy.RequestId = "", ""
	raw, err := proto.MarshalOptions{Deterministic: true}.Marshal(legacy)
	if err != nil {
		t.Fatal(err)
	}
	if operationHash(in, false) != sha256.Sum256(raw) {
		t.Fatal("legacy replay digest changed")
	}
	bound := operationHash(in, true)
	in.RequestId = "new-trace"
	if operationHash(in, true) != bound {
		t.Fatal("trace ID changed identity")
	}
	in.InstallationId = uuid.Must(uuid.NewV7()).String()
	if operationHash(in, true) == bound {
		t.Fatal("installation change did not change replay digest")
	}
	if operationHash(in, false) != sha256.Sum256(raw) {
		t.Fatal("legacy target consumed installation field")
	}
}

func TestInstallationRegistryHashCompatibilityAndMapping(t *testing.T) {
	root := t.TempDir()
	app, _ := installationFixture()
	manifestRaw := []byte(`{"app_id":"test-app","version":"1.0.0","required_models":[],"required_resources":{"gpu":0,"disk_bytes":0},"deployment":{"template_ref":"workload.json","workload_name":"test-app","images":[{"component":"server","image":"example.invalid/server:1.0.0"}]}}`)
	app.Deployment.APIVersion, app.Deployment.Kind = "apps/v1", "Deployment"
	deploymentRaw, _ := json.Marshal(app.Deployment)
	for name, raw := range map[string][]byte{"manifest.json": manifestRaw, "workload.json": deploymentRaw} {
		if err := os.WriteFile(filepath.Join(root, name), raw, 0600); err != nil {
			t.Fatal(err)
		}
	}
	read := func(bindings []InstallationBinding) Target {
		t.Helper()
		raw, _ := json.Marshal([]registryEntry{{ManifestFile: "manifest.json", Namespace: "test-runtime", InstallationBindings: bindings}})
		if err := os.WriteFile(filepath.Join(root, "registry.json"), raw, 0600); err != nil {
			t.Fatal(err)
		}
		loaded, err := LoadRegistry(root, filepath.Join(root, "registry.json"))
		if err != nil {
			t.Fatal(err)
		}
		return loaded["test-app@1.0.0"]
	}
	legacy := read(nil)
	var m manifest
	if err := json.Unmarshal(manifestRaw, &m); err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(struct {
		Manifest   manifest
		Deployment *appsv1.Deployment
		Services   []*corev1.Service
	}{m, legacy.Deployment, legacy.Services})
	if legacy.Hash != sha256.Sum256(raw) || read([]InstallationBinding{}).Hash != legacy.Hash {
		t.Fatal("historical template digest changed")
	}
	bound := read(app.InstallationBindings)
	if bound.Hash == legacy.Hash || !reflect.DeepEqual(bound.InstallationBindings, app.InstallationBindings) {
		t.Fatal("mapping not bound to registration")
	}
}

func TestInstallationWorkloadRecoveryAndForeignIdentity(t *testing.T) {
	template, in := installationFixture()
	template.Services = []*corev1.Service{{ObjectMeta: metav1.ObjectMeta{Name: "test-app", Namespace: "test-runtime"},
		Spec: corev1.ServiceSpec{Selector: map[string]string{"app": "test"}, Ports: []corev1.ServicePort{{Name: "http", Port: 8091}}}}}
	bound, err := bindInstallation(template, in)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	client := fake.NewSimpleClientset()
	driver := Kubernetes{client}
	step := func() Result {
		t.Helper()
		r, err := driver.Step(ctx, bound, "install", in.OperationId, in.StationId, in.OrganizationId)
		if err != nil {
			t.Fatal(err)
		}
		return r
	}
	if r := step(); r.Status != "running" {
		t.Fatal(r)
	}
	if r := step(); r.Status != "running" {
		t.Fatal(r)
	}
	d, err := client.AppsV1().Deployments("test-runtime").Get(ctx, "test-app", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if d.Annotations[installationKey] != in.InstallationId {
		t.Fatal("deployment missing installation")
	}
	svc, err := client.CoreV1().Services("test-runtime").Get(ctx, "test-app", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if svc.Annotations[installationKey] != in.InstallationId {
		t.Fatal("service missing installation")
	}
	d.Status = appsv1.DeploymentStatus{ObservedGeneration: d.Generation, Replicas: 1, UpdatedReplicas: 1, ReadyReplicas: 1, AvailableReplicas: 1}
	if _, err = client.AppsV1().Deployments("test-runtime").UpdateStatus(ctx, d, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	if r := step(); r.Status != "succeeded" {
		t.Fatal(r)
	}
	for _, kind := range []string{"missing ownership", "other installation", "changed env", "extra env", "service selector"} {
		t.Run(kind, func(t *testing.T) {
			changed := d.DeepCopy()
			service := svc.DeepCopy()
			switch kind {
			case "missing ownership":
				delete(changed.Annotations, installationKey)
			case "other installation":
				changed.Annotations[installationKey] = uuid.Must(uuid.NewV7()).String()
			case "changed env":
				changed.Spec.Template.Spec.Containers[0].Env[0].Value = uuid.Must(uuid.NewV7()).String()
			case "extra env":
				changed.Spec.Template.Spec.Containers[0].Env = append(changed.Spec.Template.Spec.Containers[0].Env, corev1.EnvVar{Name: "EXTRA", Value: "1"})
			case "service selector":
				service.Spec.Selector["app"] = "other"
			}
			c := fake.NewSimpleClientset(changed, service)
			d := Kubernetes{c}
			r, err := d.Step(ctx, bound, "install", in.OperationId, in.StationId, in.OrganizationId)
			if err != nil || r.Code != "CONFLICT" {
				t.Fatal("foreign identity accepted", r, err)
			}
			for _, a := range c.Actions() {
				if a.GetVerb() != "get" {
					t.Fatal("mutated foreign resource")
				}
			}
		})
	}
}
