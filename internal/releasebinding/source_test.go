package releasebinding

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestPreinstalledReleaseChecksBytesAndBinding(t *testing.T) {
	for _, kind := range []string{"valid", "chart_changed", "package_changed", "binding_changed", "withdrawn", "path_escape", "duplicate_field", "duplicate_ref", "old_schema", "unknown_field"} {
		t.Run(kind, func(t *testing.T) {
			root := t.TempDir()
			manifest, chart := []byte("test package bytes"), []byte("test chart bytes")
			binding := Binding{"resources-v1", "resources", "0.1.0", Sum(manifest), Sum(chart), "sha256:" + strings.Repeat("a", 64)}
			cfg := Registry{SchemaVersion: SchemaVersion, Releases: []Entry{{Binding: binding, PackageRef: "package.json", ChartRef: "chart.tgz"}}}
			switch kind {
			case "chart_changed":
				chart = []byte("changed")
			case "package_changed":
				manifest = []byte("changed")
			case "binding_changed":
				binding.TemplateDigest = "sha256:" + strings.Repeat("b", 64)
			case "withdrawn":
				cfg.Releases = []Entry{}
			case "path_escape":
				cfg.Releases[0].ChartRef = "../outside.tgz"
			case "duplicate_ref":
				cfg.Releases = append(cfg.Releases, cfg.Releases[0])
			case "old_schema":
				cfg.SchemaVersion = 1
			}
			for name, raw := range map[string][]byte{"package.json": manifest, "chart.tgz": chart} {
				if err := os.WriteFile(filepath.Join(root, name), raw, 0600); err != nil {
					t.Fatal(err)
				}
			}
			raw, _ := json.Marshal(cfg)
			switch kind {
			case "duplicate_field":
				raw = append([]byte(`{"schemaVersion":2,`), raw[1:]...)
			case "unknown_field":
				raw = append([]byte(`{"keys":{},`), raw[1:]...)
			}
			path := filepath.Join(root, "registry.json")
			if err := os.WriteFile(path, raw, 0600); err != nil {
				t.Fatal(err)
			}
			source := &Source{Path: path}
			err := source.Verify(binding)
			if (kind == "valid") != (err == nil) {
				t.Fatal(kind, err)
			}
			if kind == "valid" {
				resolved, err := source.Resolve(binding.ReleaseRef)
				if err != nil || resolved != binding {
					t.Fatal("resolved binding changed", err)
				}
			}
		})
	}
}
