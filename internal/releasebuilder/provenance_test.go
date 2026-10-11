package releasebuilder

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestReleaseBuilderRejectsUnboundOrMismatchedProvenance(t *testing.T) {
	for _, kind := range []string{"legacy-package", "wrong-app", "wrong-version", "unknown-field", "duplicate-field", "manifest-mapping", "manifest-bytes", "delivery-bytes", "declaration-bytes", "index-bytes", "navigation-bytes", "delivery-image", "delivery-revision"} {
		t.Run(kind, func(t *testing.T) {
			options, now, key := builderFixture(t)
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
			if _, err = Build(options, now, key); err == nil {
				t.Fatal("unbound provenance signed")
			}
			if _, err = os.Stat(options.Output); !os.IsNotExist(err) {
				t.Fatal("invalid provenance created output")
			}
		})
	}
}
