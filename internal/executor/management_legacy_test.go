package executor

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
)

func TestManagementLegacyOutputLimitCannotBeBypassedByCopy(t *testing.T) {
	out := &maintenanceOutput{maximum: 16}
	if _, err := io.Copy(out, strings.NewReader(strings.Repeat("x", 32))); err == nil || out.Len() > 16 {
		t.Fatal("stream output exceeded bound")
	}
}

func legacyMaintenanceFixture(t *testing.T) *maintenanceFixture {
	t.Helper()
	f := newMaintenanceFixtureFor(t, func(source, target *Target) {
		source.AppID, target.AppID = "comfyui", "comfyui"
		source.Version, target.Version = "0.1.3", "0.1.4"
		for _, v := range []*Target{source, target} {
			readonly := true
			v.Deployment.Spec.Template.Spec.Containers[0].SecurityContext = &corev1.SecurityContext{ReadOnlyRootFilesystem: &readonly}
			v.Deployment.Spec.Template.Spec.Containers[0].VolumeMounts = []corev1.VolumeMount{{Name: "config", MountPath: "/config", ReadOnly: true}, {Name: "data", MountPath: "/data"}}
		}
	})
	for _, release := range []*ManagementRelease{&f.b.Source, &f.b.Target} {
		release.DataContract = "comfyui-management-v1"
		release.ManifestDigest = release.ReleaseDigest
		release.DefaultWorkerVersion = "0.1.3"
		release.CompatibleWorkerVersions = []string{"0.1.3"}
		config := f.m.Releases[release.ReleaseDigest]
		config.Snapshot = *release
		if release == &f.b.Source {
			config.Transport, config.Container, config.Token = legacyComfyTransport, "server", ""
		}
		f.m.Releases[release.ReleaseDigest] = config
	}
	return f
}

func TestManagementLegacyExplicitSelectionAndOriginalProof(t *testing.T) {
	f := legacyMaintenanceFixture(t)
	calls := 0
	f.m.Kubernetes.maintenanceExec = func(_ context.Context, namespace, pod, container string, raw []byte) ([]byte, error) {
		calls++
		if namespace != f.pod.Namespace || pod != f.pod.Name || container != "server" {
			t.Fatal("unverified exec target")
		}
		var in struct {
			Action string          `json:"action"`
			Proof  managementProof `json:"proof"`
		}
		if json.Unmarshal(raw, &in) != nil || in.Proof.OperationID != f.b.OperationID || in.Proof.AppVersion != "0.1.3" {
			t.Fatal("wrong bootstrap input")
		}
		state := "sealed"
		if in.Action == "release" {
			state = "released"
		}
		return json.Marshal(map[string]any{"proof": managementProofResponse{in.Proof, state}})
	}
	ctx := context.Background()
	for _, action := range []func() error{func() error { return f.m.Seal(ctx, f.b, f.c) }, func() error { return f.m.Verify(ctx, f.b, f.c, false) }, func() error { return f.m.Release(ctx, f.b, f.c, false) }} {
		if err := action(); err != nil {
			t.Fatal(err)
		}
	}
	if calls != 3 || f.calls != 0 {
		t.Fatal("legacy path used HTTP or cached proof")
	}
	config := f.m.Releases[f.b.Source.ReleaseDigest]
	config.Transport, config.Container, config.Token = "http", "", strings.Repeat("test-only-", 5)
	f.m.Releases[f.b.Source.ReleaseDigest] = config
	f.response = func(*http.Request) (*http.Response, error) { return proofResponse(404, `{"code":"NOT_FOUND"}`), nil }
	if err := f.m.Seal(ctx, f.b, f.c); !errors.Is(err, ErrManagementMaintenanceUnavailable) || calls != 3 {
		t.Fatal("404 triggered bootstrap", err)
	}
}

func TestManagementLegacyRefusesUnprovenProcessAndResponses(t *testing.T) {
	for _, scenario := range []string{"image", "mount", "writable root", "wrong app", "wrong version", "token", "unknown transport", "missing executor", "busy", "lost response", "wrong proof", "process replaced"} {
		t.Run(scenario, func(t *testing.T) {
			f := legacyMaintenanceFixture(t)
			calls := 0
			f.m.Kubernetes.maintenanceExec = func(_ context.Context, _, _, _ string, raw []byte) ([]byte, error) {
				calls++
				var in struct {
					Proof managementProof `json:"proof"`
				}
				json.Unmarshal(raw, &in)
				switch scenario {
				case "busy":
					return []byte(`{"code":"INSTALLATION_BUSY"}`), nil
				case "lost response":
					return nil, errors.New("private transport failure")
				case "wrong proof":
					in.Proof.InstallationID = f.b.UserID
				case "process replaced":
					f.pod.Status.ContainerStatuses[0].ContainerID = "replacement"
					f.savePod(t)
				}
				return json.Marshal(map[string]any{"proof": managementProofResponse{in.Proof, "sealed"}})
			}
			config := f.m.Releases[f.b.Source.ReleaseDigest]
			switch scenario {
			case "image":
				f.pod.Status.ContainerStatuses[0].ImageID = "invalid"
			case "mount":
				f.pod.Spec.Containers[0].VolumeMounts = append(f.pod.Spec.Containers[0].VolumeMounts, corev1.VolumeMount{Name: "override", MountPath: "/opt/vf"})
			case "writable root":
				*f.pod.Spec.Containers[0].SecurityContext.ReadOnlyRootFilesystem = false
			case "wrong app":
				config.Snapshot.AppID = "other"
			case "wrong version":
				config.Snapshot.AppVersion = "0.1.4"
			case "token":
				config.Token = strings.Repeat("x", 40)
			case "unknown transport":
				config.Transport = "exec"
			case "missing executor":
				f.m.Kubernetes.maintenanceExec = nil
			}
			f.m.Releases[f.b.Source.ReleaseDigest] = config
			f.savePod(t)
			if err := f.m.Seal(context.Background(), f.b, f.c); err == nil {
				t.Fatal("unproven maintenance accepted")
			}
			if f.calls != 0 {
				t.Fatal("legacy transport fell through to HTTP")
			}
			if !strings.Contains("busy,lost response,wrong proof,process replaced", scenario) && calls != 0 {
				t.Fatal("executed before identity checks")
			}
		})
	}
}

func TestManagementLegacyRegistrationRequiresExplicitSupportedMode(t *testing.T) {
	for _, scenario := range []string{"valid", "token", "missing container", "other version", "other contract", "missing executor", "unknown mode"} {
		t.Run(scenario, func(t *testing.T) {
			f := legacyMaintenanceFixture(t)
			f.m.Kubernetes.maintenanceExec = func(context.Context, string, string, string, []byte) ([]byte, error) {
				t.Fatal("configuration executed program")
				return nil, nil
			}
			c := f.m.Releases[f.b.Source.ReleaseDigest]
			entry := maintenanceRegistration{Snapshot: c.Snapshot, Images: c.Images, Service: c.Service, Port: c.Port, Transport: c.Transport, Container: c.Container}
			switch scenario {
			case "token":
				entry.TokenEnv = "APP_TOKEN"
			case "missing container":
				entry.Container = ""
			case "other version":
				entry.Snapshot.AppVersion = "0.1.4"
			case "other contract":
				entry.Snapshot.DataContract = "other-v1"
			case "missing executor":
				f.m.Kubernetes.maintenanceExec = nil
			case "unknown mode":
				entry.Transport = "exec"
			}
			raw, _ := json.Marshal(map[string]any{"schema_version": 1, "releases": []maintenanceRegistration{entry}})
			path := filepath.Join(t.TempDir(), "registry.json")
			if err := os.WriteFile(path, raw, 0600); err != nil {
				t.Fatal(err)
			}
			_, err := LoadManagementMaintenance(path, strings.Repeat("c", 40), func(string) string { t.Fatal("legacy mode read maintenance token"); return "" }, f.m.Kubernetes, f.m.Targets)
			if (err == nil) != (scenario == "valid") {
				t.Fatal("incorrect registry decision", err)
			}
		})
	}
}
