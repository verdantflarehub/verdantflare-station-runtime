package releasebuilder

import (
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/verdantflarehub/verdantflare-station-runtime/internal/executor"
	"github.com/verdantflarehub/verdantflare-station-runtime/internal/releasebinding"
)

func builderFixture(t *testing.T) Options {
	t.Helper()
	root := t.TempDir()
	files := map[string][]byte{
		"targets.json":  []byte(`[{"manifest_file":"manifest.yaml","namespace":"test-runtime"}]`),
		"manifest.yaml": []byte("app_id: test-app\nversion: 1.0.0\nrequired_models: []\nrequired_resources: {gpu: 0, disk_bytes: 0}\ndeployment:\n  template_ref: workload.yaml\n  workload_name: test-app\n  images: [{component: server, image: 'example.invalid/server:1.0.0'}]\n"),
		"workload.yaml": []byte("apiVersion: apps/v1\nkind: Deployment\nmetadata: {name: test-app, namespace: test-runtime}\nspec:\n  replicas: 1\n  selector: {matchLabels: {app: test}}\n  template:\n    metadata: {labels: {app: test}}\n    spec:\n      containers:\n        - {name: server, image: 'example.invalid/server:1.0.0', imagePullPolicy: Always}\n"),
		"package.json":  []byte(`{"testPackage":"actual input bytes"}`),
		"chart.tgz":     []byte("actual fixture chart bytes"),
	}
	var err error
	files["delivery.json"], err = json.Marshal(delivery{SchemaVersion: 1, AppID: "test-app", AppVersion: "1.0.0", Image: "example.invalid/server:1.0.0", ImageDigest: digest([]byte("fixture image bytes")), Repository: "verdantflarehub/verdantflare-app-test-app", Revision: strings.Repeat("a", 40)})
	if err != nil {
		t.Fatal(err)
	}
	files["dashboard-declaration.json"] = []byte(`{"fixture":"declaration"}`)
	files["asset-index.json"] = []byte(`{"fixture":"index"}`)
	files["navigation-declaration.json"] = []byte(`{"fixture":"navigation"}`)
	ref := func(name string) materialRef { return materialRef{name, digest(files[name])} }
	files["package.json"], err = json.Marshal(provenance{SchemaVersion: 1, Kind: provenanceKind, AppID: "test-app", AppVersion: "1.0.0", Manifest: ref("manifest.yaml"), Delivery: ref("delivery.json"), Declaration: ref("dashboard-declaration.json"), Index: ref("asset-index.json"), Navigation: ref("navigation-declaration.json")})
	if err != nil {
		t.Fatal(err)
	}
	config := Config{SchemaVersion: releasebinding.SchemaVersion, Releases: []Release{{ReleaseRef: "test-app-v1", AppID: "test-app", AppVersion: "1.0.0", PackageRef: "package.json", ChartRef: "chart.tgz"}}}
	files["prepare.json"], err = json.Marshal(config)
	if err != nil {
		t.Fatal(err)
	}
	for name, raw := range files {
		if err = os.WriteFile(filepath.Join(root, name), raw, 0600); err != nil {
			t.Fatal(err)
		}
	}
	return Options{Root: root, Targets: "targets.json", Config: "prepare.json", Output: filepath.Join(t.TempDir(), "new-release")}
}

func TestReleaseBuilderBindsActualMaterialsAndConsumerTarget(t *testing.T) {
	options := builderFixture(t)
	out, err := Build(options)
	if err != nil || len(out.Releases) != 1 {
		t.Fatal(out, err)
	}
	targets, err := executor.LoadRegistry(options.Root, filepath.Join(options.Root, options.Targets))
	if err != nil {
		t.Fatal(err)
	}
	target := targets["test-app@1.0.0"]
	if out.Releases[0].TemplateDigest != "sha256:"+hex.EncodeToString(target.Hash[:]) {
		t.Fatal("builder invented a different template hash")
	}
	source := &releasebinding.Source{Path: filepath.Join(options.Output, out.RegistryFile)}
	if err = source.Verify(out.Releases[0]); err != nil {
		t.Fatal("consumer cannot verify builder output", err)
	}
	for _, name := range []string{"package.json", "chart.tgz"} {
		original, _ := os.ReadFile(filepath.Join(options.Root, name))
		copied, _ := os.ReadFile(filepath.Join(options.Output, "test-app-v1", name))
		if string(original) != string(copied) {
			t.Fatal("source material changed during copy")
		}
	}
	if _, err = Build(options); !errors.Is(err, ErrOutput) {
		t.Fatal("existing release overwritten", err)
	}
	if err = os.WriteFile(filepath.Join(options.Output, "test-app-v1", "chart.tgz"), []byte("changed"), 0600); err != nil {
		t.Fatal(err)
	}
	if source.Verify(out.Releases[0]) == nil {
		t.Fatal("post-registration material mutation accepted")
	}
}

func TestReleaseBuilderRejectsInvalidInputsBeforeWriting(t *testing.T) {
	for _, kind := range []string{"old-schema", "unknown-field", "duplicate-field", "missing-target", "escape", "wrong-pull-policy", "mutable-image", "public-service"} {
		t.Run(kind, func(t *testing.T) {
			options := builderFixture(t)
			rewrite := func(name, old, next string) {
				raw, err := os.ReadFile(filepath.Join(options.Root, name))
				if err != nil {
					t.Fatal(err)
				}
				if err = os.WriteFile(filepath.Join(options.Root, name), []byte(strings.Replace(string(raw), old, next, 1)), 0600); err != nil {
					t.Fatal(err)
				}
			}
			switch kind {
			case "old-schema":
				rewrite("prepare.json", `"schemaVersion":2`, `"schemaVersion":1`)
			case "unknown-field":
				rewrite("prepare.json", `"schemaVersion":2`, `"schemaVersion":2,"keyId":"test-key"`)
			case "duplicate-field":
				rewrite("prepare.json", `"schemaVersion":2`, `"schemaVersion":2,"schemaVersion":2`)
			case "missing-target":
				rewrite("prepare.json", `"appVersion":"1.0.0"`, `"appVersion":"2.0.0"`)
			case "escape":
				rewrite("prepare.json", `"packageRef":"package.json"`, `"packageRef":"../package.json"`)
			case "wrong-pull-policy":
				rewrite("workload.yaml", "Always", "IfNotPresent")
			case "mutable-image":
				rewrite("manifest.yaml", "server:1.0.0", "server:latest")
				rewrite("workload.yaml", "server:1.0.0", "server:latest")
			case "public-service":
				f, err := os.OpenFile(filepath.Join(options.Root, "workload.yaml"), os.O_APPEND|os.O_WRONLY, 0600)
				if err != nil {
					t.Fatal(err)
				}
				_, err = f.WriteString("---\napiVersion: v1\nkind: Service\nmetadata: {name: test-app, namespace: test-runtime}\nspec:\n  type: NodePort\n  selector: {app: test}\n  ports: [{port: 80, nodePort: 30080}]\n")
				_ = f.Close()
				if err != nil {
					t.Fatal(err)
				}
			}
			if _, err := Build(options); !errors.Is(err, ErrInvalid) {
				t.Fatal("invalid inputs registered", err)
			}
			if _, err := os.Stat(options.Output); !errors.Is(err, os.ErrNotExist) {
				t.Fatal("failed validation wrote release output")
			}
		})
	}
}
