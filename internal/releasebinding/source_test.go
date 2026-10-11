package releasebinding

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestSignedPreinstalledReleaseChecksBytesSignatureAndLifetime(t *testing.T) {
	for _, kind := range []string{"valid", "chart_changed", "package_changed", "signature_changed", "unknown_key", "expired", "binding_changed", "withdrawn", "path_escape", "duplicate_field"} {
		t.Run(kind, func(t *testing.T) {
			root := t.TempDir()
			pub, private, err := ed25519.GenerateKey(rand.Reader)
			if err != nil {
				t.Fatal(err)
			}
			manifest, chart := []byte("test package bytes"), []byte("test chart bytes")
			binding := Binding{"resources-v1", "resources", "0.1.0", sum(manifest), sum(chart), "sha256:" + strings.Repeat("a", 64)}
			now := time.Date(2026, 10, 11, 0, 0, 0, 0, time.UTC)
			release := SignedRelease{1, binding, now.Add(-time.Hour).Format(time.RFC3339), now.Add(time.Hour).Format(time.RFC3339)}
			if kind == "expired" {
				release.ExpiresAt = now.Format(time.RFC3339)
			}
			payload, _ := json.Marshal(release)
			signature := ed25519.Sign(private, append([]byte("verdantflare.preinstalled.release.v1\n"), payload...))
			entry := registryRelease{ReleaseRef: binding.ReleaseRef, Payload: base64.StdEncoding.EncodeToString(payload), Signature: base64.StdEncoding.EncodeToString(signature), KeyID: "publisher", PackageRef: "package.json", ChartRef: "chart.tgz"}
			cfg := registry{SchemaVersion: 1, Keys: map[string]string{"publisher": base64.StdEncoding.EncodeToString(pub)}, Releases: []registryRelease{entry}}
			switch kind {
			case "chart_changed":
				chart = []byte("changed")
			case "package_changed":
				manifest = []byte("changed")
			case "signature_changed":
				signature[0] ^= 1
				cfg.Releases[0].Signature = base64.StdEncoding.EncodeToString(signature)
			case "unknown_key":
				cfg.Keys = map[string]string{}
			case "binding_changed":
				binding.TemplateDigest = "sha256:" + strings.Repeat("b", 64)
			case "withdrawn":
				cfg.Releases = []registryRelease{}
			case "path_escape":
				cfg.Releases[0].ChartRef = "../outside.tgz"
			}
			for name, raw := range map[string][]byte{"package.json": manifest, "chart.tgz": chart} {
				if err := os.WriteFile(filepath.Join(root, name), raw, 0600); err != nil {
					t.Fatal(err)
				}
			}
			raw, _ := json.Marshal(cfg)
			if kind == "duplicate_field" {
				raw = append([]byte(`{"schemaVersion":1,`), raw[1:]...)
			}
			path := filepath.Join(root, "registry.json")
			if err := os.WriteFile(path, raw, 0600); err != nil {
				t.Fatal(err)
			}
			source := &Source{Path: path, Now: func() time.Time { return now }}
			err = source.Verify(binding)
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
