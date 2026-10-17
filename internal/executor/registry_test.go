package executor

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestRegistryRetainsTwoVersionsWithoutSharingAcrossApps(t *testing.T) {
	root := t.TempDir()
	write := func(name string, value any) {
		t.Helper()
		raw, err := json.Marshal(value)
		if err != nil {
			t.Fatal(err)
		}
		if err = os.WriteFile(filepath.Join(root, name), raw, 0600); err != nil {
			t.Fatal(err)
		}
	}
	writeVersion := func(appID, version, name string) registryEntry {
		t.Helper()
		app := target()
		app.Deployment.APIVersion = "apps/v1"
		app.Deployment.Kind = "Deployment"
		app.Deployment.Spec.Template.Spec.Containers[0].Image = "example.invalid/server:" + version
		write(name+".workload.json", app.Deployment)
		write(name+".manifest.json", map[string]any{"app_id": appID, "version": version, "required_models": []any{}, "required_resources": map[string]any{"gpu": 0, "disk_bytes": 0},
			"deployment": map[string]any{"template_ref": name + ".workload.json", "workload_name": "test-app", "images": []any{map[string]string{"component": "server", "image": "example.invalid/server:" + version}}}})
		return registryEntry{ManifestFile: name + ".manifest.json", Namespace: "test-runtime"}
	}
	source := writeVersion("test-app", "1.0.0", "source")
	target := writeVersion("test-app", "1.1.0", "target")
	write("registry.json", []registryEntry{source, target})
	loaded, err := LoadRegistry(root, filepath.Join(root, "registry.json"))
	if err != nil || len(loaded) != 2 {
		t.Fatal("source and target could not coexist", err)
	}
	if loaded["test-app@1.0.0"].Hash == loaded["test-app@1.1.0"].Hash {
		t.Fatal("version templates not independently bound")
	}
	foreign := writeVersion("another-app", "1.2.0", "foreign")
	write("registry.json", []registryEntry{source, foreign})
	if _, err = LoadRegistry(root, filepath.Join(root, "registry.json")); err == nil {
		t.Fatal("another app shared an existing workload")
	}
	write("registry.json", []registryEntry{source, source})
	if _, err = LoadRegistry(root, filepath.Join(root, "registry.json")); err == nil {
		t.Fatal("duplicate release registration accepted")
	}
}

func TestRegistryRejectsEscapeAndImageMismatch(t *testing.T) {
	root := t.TempDir()
	write := func(name, body string) {
		t.Helper()
		if e := os.WriteFile(filepath.Join(root, name), []byte(body), 0600); e != nil {
			t.Fatal(e)
		}
	}
	write("registry.json", `[{"manifest_file":"manifest.yaml","namespace":"test-runtime"}]`)
	write("manifest.yaml", `app_id: test-app
version: 1.0.0
required_models: []
required_resources: {gpu: 0, disk_bytes: 0}
deployment:
  template_ref: workload.yaml
  workload_name: test-app
  images: [{component: server, image: "example.invalid/server:1.0.0"}]
`)
	template := `apiVersion: apps/v1
kind: Deployment
metadata: {name: test-app, namespace: test-runtime}
spec:
  replicas: 1
  selector: {matchLabels: {app: test}}
  template:
    metadata: {labels: {app: test}}
    spec:
      containers:
        - {name: server, image: "example.invalid/server:1.0.0"}
`
	write("workload.yaml", template)
	targets, e := LoadRegistry(root, filepath.Join(root, "registry.json"))
	if e != nil || len(targets) != 1 {
		t.Fatalf("valid registry: %v", e)
	}
	if _, e = fileInRoot(root, "../outside"); e == nil {
		t.Fatal("path escape accepted")
	}
	outside := filepath.Join(t.TempDir(), "outside")
	if e = os.WriteFile(outside, []byte("data"), 0600); e != nil {
		t.Fatal(e)
	}
	if e = os.Symlink(outside, filepath.Join(root, "escape")); e != nil {
		if errors.Is(e, os.ErrPermission) || strings.Contains(strings.ToLower(e.Error()), "privilege is not held") {
			t.Skip("skipping symlink test on Windows without privilege")
		} else {
			t.Fatal(e)
		}
	}
	if _, e = fileInRoot(root, "escape"); e == nil {
		t.Fatal("symlink escape accepted")
	}
	write("workload.yaml", template+"      initContainers:\n        - {name: unpinned, image: 'example.invalid/unpinned:latest'}\n")
	if _, e = LoadRegistry(root, filepath.Join(root, "registry.json")); e == nil {
		t.Fatal("unregistered init image accepted")
	}
}

func TestVersionedComponentImageTags(t *testing.T) {
	for _, image := range []string{"registry.invalid/app:1.0.0", "registry.invalid/app:v1.0.0", "registry.invalid/app:video-mcp-server-v0.9.2", "registry.invalid/app:video-mcp-server-v0.9.2@sha256:663d2db6f1ecc8c809b5476f157915626b8793f1e956602dd6ed76ff585c44e5"} {
		if !imagePattern.MatchString(image) {
			t.Errorf("valid release rejected: %s", image)
		}
	}
	for _, image := range []string{"registry.invalid/app:latest", "registry.invalid/app:dev", "registry.invalid/app:video-mcp-server-v0.9.2-extra", "registry.invalid/app:video-mcp-server-v0.9.2@sha256:bad"} {
		if imagePattern.MatchString(image) {
			t.Errorf("mutable or malformed tag accepted: %s", image)
		}
	}
}

func TestRegistryContentHealthEndpoints(t *testing.T) {
	for _, app := range []string{"project", "world", "fal", "google", "grok", "chatgpt", "image-mcp", "video-mcp", "unrelated"} {
		for _, endpoint := range []string{"https://test-app.test-runtime.svc.cluster.local:8443/healthz", "http://test-app.test-runtime.svc.cluster.local:8443/healthz", "https://foreign.test-runtime.svc.cluster.local:8443/healthz", "http://test-app.test-runtime.svc.cluster.local:8000/v1/activity", "http://test-app.test-runtime.svc.cluster.local:8001/v1/activity", "http://test-app.test-runtime.svc.cluster.local:8000/api/tasks/stats", "http://test-app.test-runtime.svc.cluster.local:8001/api/tasks/stats", "http://test-app.test-runtime.svc.cluster.local:8000/health", "http://test-app.test-runtime.svc.cluster.local:8001/health", "http://foreign.test-runtime.svc.cluster.local:8000/health", "https://test-app.test-runtime.svc.cluster.local:8000/health"} {
			t.Run(app+"/"+endpoint, func(t *testing.T) {
				root := t.TempDir()
				write := func(name string, value any) {
					t.Helper()
					raw, err := json.Marshal(value)
					if err != nil {
						t.Fatal(err)
					}
					if err = os.WriteFile(filepath.Join(root, name), raw, 0600); err != nil {
						t.Fatal(err)
					}
				}
				deployment := target().Deployment
				deployment.APIVersion, deployment.Kind = "apps/v1", "Deployment"
				deployment.Spec.Template.Spec.Containers[0].Image = "example.invalid/server:1.0.0"
				write("workload.json", deployment)
				write("manifest.json", map[string]any{"app_id": app, "version": "1.0.0", "required_models": []any{}, "required_resources": map[string]any{"gpu": 0, "disk_bytes": 0}, "deployment": map[string]any{"template_ref": "workload.json", "workload_name": "test-app", "images": []any{map[string]string{"component": "server", "image": "example.invalid/server:1.0.0"}}}})
				write("registry.json", []registryEntry{{ManifestFile: "manifest.json", Namespace: "test-runtime", ActivityURL: endpoint, ActivityTokenEnv: "STATION_APP_TEST_TOKEN"}})
				_, err := LoadRegistry(root, filepath.Join(root, "registry.json"))
				want := ((app == "project" || app == "world") && endpoint == "https://test-app.test-runtime.svc.cluster.local:8443/healthz") || ((app == "fal" || app == "google" || app == "grok" || app == "chatgpt") && endpoint == "http://test-app.test-runtime.svc.cluster.local:8000/v1/activity") || (app == "image-mcp" && endpoint == "http://test-app.test-runtime.svc.cluster.local:8000/api/tasks/stats") || (app == "video-mcp" && endpoint == "http://test-app.test-runtime.svc.cluster.local:8000/health")
				if (err == nil) != want {
					t.Fatalf("health registration acceptance=%v, want %v", err == nil, want)
				}
			})
		}
	}
}
