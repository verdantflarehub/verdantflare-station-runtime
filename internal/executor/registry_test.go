package executor

import (
	"os"
	"path/filepath"
	"testing"
)

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
		t.Fatal(e)
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
