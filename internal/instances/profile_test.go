package instances

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func TestProfileRegistryPinsConfigurationAndRejectsUnboundedInputs(t *testing.T) {
	path := filepath.Join(t.TempDir(), "profiles.json")
	original := profileConfig{ID: "blender-standard", Namespace: "blender-test", Pool: "blender", NodeName: "worker-test", Claims: []string{"workspace-1"}, StorageBytes: 50 << 30, Image: "registry.example.test/blender-worker:v0.1.7", MaxFileBytes: 1 << 30}
	load := func(entry profileConfig) (map[string]HelperProfile, error) {
		raw, _ := json.Marshal([]profileConfig{entry})
		if err := os.WriteFile(path, raw, 0600); err != nil {
			t.Fatal(err)
		}
		return LoadProfiles(path)
	}
	profiles, err := load(original)
	if err != nil {
		t.Fatal(err)
	}
	changed := original
	changed.Image = "registry.example.test/blender-worker:v0.1.8"
	next, err := load(changed)
	if err != nil || next[original.ID].Hash == profiles[original.ID].Hash {
		t.Fatal("image change did not change execution hash")
	}
	for _, mutate := range []func(*profileConfig){func(p *profileConfig) { p.Image = "registry.example.test/blender-worker:latest" }, func(p *profileConfig) { p.Namespace = "default" }, func(p *profileConfig) { p.Claims = []string{"../../workspace"} }, func(p *profileConfig) { p.MaxFileBytes = p.StorageBytes + 1 }, func(p *profileConfig) { p.Claims = []string{"workspace-1", "workspace-1"} }} {
		candidate := original
		mutate(&candidate)
		if _, err = load(candidate); err == nil {
			t.Fatal("invalid registry accepted")
		}
	}
	raw, _ := json.Marshal(original)
	if err = os.WriteFile(path, append(raw, []byte(` {"other":true}`)...), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err = LoadProfiles(path); err == nil {
		t.Fatal("trailing JSON accepted")
	}
}
