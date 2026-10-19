package releasebuilder

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestImageProvenanceUsesActualHistoricalRepository(t *testing.T) {
	for _, repository := range []string{"verdantflarehub/verdantflare-app-image", "verdantflarehub/verdantflare-app-image-mcp", "other/verdantflare-app-image"} {
		t.Run(repository, func(t *testing.T) {
			options := builderFixture(t)
			for _, name := range []string{"manifest.yaml", "prepare.json", "package.json", "delivery.json"} {
				file := filepath.Join(options.Root, name)
				raw, err := os.ReadFile(file)
				if err != nil {
					t.Fatal(err)
				}
				raw = []byte(strings.ReplaceAll(string(raw), "test-app", "image-mcp"))
				if name == "delivery.json" {
					var d delivery
					if err := json.Unmarshal(raw, &d); err != nil {
						t.Fatal(err)
					}
					d.Repository = repository
					raw, err = json.Marshal(d)
					if err != nil {
						t.Fatal(err)
					}
				}
				if err := os.WriteFile(file, raw, 0600); err != nil {
					t.Fatal(err)
				}
			}
			// The fixture's deployment name follows the manifest workload name.
			workload := filepath.Join(options.Root, "workload.yaml")
			raw, err := os.ReadFile(workload)
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(workload, []byte(strings.ReplaceAll(string(raw), "test-app", "image-mcp")), 0600); err != nil {
				t.Fatal(err)
			}
			packagePath := filepath.Join(options.Root, "package.json")
			raw, err = os.ReadFile(packagePath)
			if err != nil {
				t.Fatal(err)
			}
			var p provenance
			if err := json.Unmarshal(raw, &p); err != nil {
				t.Fatal(err)
			}
			for _, ref := range []*materialRef{&p.Manifest, &p.Delivery} {
				content, err := os.ReadFile(filepath.Join(options.Root, ref.Ref))
				if err != nil {
					t.Fatal(err)
				}
				ref.Digest = digest(content)
			}
			raw, err = json.Marshal(p)
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(packagePath, raw, 0600); err != nil {
				t.Fatal(err)
			}
			_, err = Build(options)
			if (err == nil) != (repository == "verdantflarehub/verdantflare-app-image") {
				t.Fatal("repository admission mismatch", err)
			}
		})
	}
}

func TestReleaseBuilderRejectsUnboundOrMismatchedProvenance(t *testing.T) {
	for _, kind := range []string{"legacy-package", "wrong-app", "wrong-version", "unknown-field", "duplicate-field", "manifest-mapping", "manifest-bytes", "delivery-bytes", "declaration-bytes", "index-bytes", "navigation-bytes", "delivery-image", "delivery-revision"} {
		t.Run(kind, func(t *testing.T) {
			options := builderFixture(t)
			packagePath := filepath.Join(options.Root, "package.json")
			raw, err := os.ReadFile(packagePath)
			if err != nil {
				t.Fatal(err)
			}
			var p provenance
			if err = json.Unmarshal(raw, &p); err != nil {
				t.Fatal(err)
			}
			switch kind {
			case "legacy-package":
				raw = []byte(`{"app_id":"test-app","version":"1.0.0"}`)
			case "wrong-app":
				p.AppID = "other-app"
			case "wrong-version":
				p.AppVersion = "2.0.0"
			case "unknown-field":
				raw = []byte(strings.Replace(string(raw), `"kind":`, `"trusted":true,"kind":`, 1))
			case "duplicate-field":
				raw = []byte(strings.Replace(string(raw), `"schemaVersion":1`, `"schemaVersion":1,"schemaVersion":1`, 1))
			case "manifest-mapping":
				p.Manifest.Ref = "other-manifest.yaml"
				original, _ := os.ReadFile(filepath.Join(options.Root, "manifest.yaml"))
				if err = os.WriteFile(filepath.Join(options.Root, p.Manifest.Ref), original, 0600); err != nil {
					t.Fatal(err)
				}
			case "delivery-image", "delivery-revision":
				original, _ := os.ReadFile(filepath.Join(options.Root, p.Delivery.Ref))
				var d delivery
				if err = json.Unmarshal(original, &d); err != nil {
					t.Fatal(err)
				}
				if kind == "delivery-image" {
					d.Image = "example.invalid/other:1.0.0"
				} else {
					d.Revision = "main"
				}
				original, _ = json.Marshal(d)
				p.Delivery.Digest = digest(original)
				if err = os.WriteFile(filepath.Join(options.Root, p.Delivery.Ref), original, 0600); err != nil {
					t.Fatal(err)
				}
			default:
				refs := map[string]materialRef{"manifest-bytes": p.Manifest, "delivery-bytes": p.Delivery, "declaration-bytes": p.Declaration, "index-bytes": p.Index, "navigation-bytes": p.Navigation}
				file, err := os.OpenFile(filepath.Join(options.Root, refs[kind].Ref), os.O_APPEND|os.O_WRONLY, 0600)
				if err != nil {
					t.Fatal(err)
				}
				_, err = file.WriteString(" ")
				file.Close()
				if err != nil {
					t.Fatal(err)
				}
			}
			if kind != "legacy-package" && kind != "unknown-field" && kind != "duplicate-field" {
				raw, _ = json.Marshal(p)
			}
			if err = os.WriteFile(packagePath, raw, 0600); err != nil {
				t.Fatal(err)
			}
			if _, err = Build(options); err == nil {
				t.Fatal("unbound provenance registered")
			}
			if _, err = os.Stat(options.Output); !os.IsNotExist(err) {
				t.Fatal("invalid provenance created output")
			}
		})
	}
}
