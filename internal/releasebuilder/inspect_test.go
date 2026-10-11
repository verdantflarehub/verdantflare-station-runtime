package releasebuilder

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestReleaseBuilderInspectorAcceptsWorkflowTagsAndMatchesSigner(t *testing.T) {
	options, now, key := builderFixture(t)
	for _, name := range []string{"manifest.yaml", "workload.yaml"} {
		raw, err := os.ReadFile(filepath.Join(options.Root, name))
		if err != nil {
			t.Fatal(err)
		}
		raw = []byte(strings.ReplaceAll(string(raw), "example.invalid/server:1.0.0", "registry.cn-qingdao.aliyuncs.com/wod/verdantflare-app:project-v0.1.0"))
		if err = os.WriteFile(filepath.Join(options.Root, name), raw, 0600); err != nil {
			t.Fatal(err)
		}
	}
	// Updating a reviewed target also requires updating its original provenance.
	deliveryBytes, err := os.ReadFile(filepath.Join(options.Root, "delivery.json"))
	if err != nil {
		t.Fatal(err)
	}
	deliveryBytes = []byte(strings.ReplaceAll(string(deliveryBytes), "example.invalid/server:1.0.0", "registry.cn-qingdao.aliyuncs.com/wod/verdantflare-app:project-v0.1.0"))
	if err = os.WriteFile(filepath.Join(options.Root, "delivery.json"), deliveryBytes, 0600); err != nil {
		t.Fatal(err)
	}
	packageBytes, _ := os.ReadFile(filepath.Join(options.Root, "package.json"))
	var p provenance
	if err = json.Unmarshal(packageBytes, &p); err != nil {
		t.Fatal(err)
	}
	manifestBytes, _ := os.ReadFile(filepath.Join(options.Root, "manifest.yaml"))
	p.Manifest.Digest, p.Delivery.Digest = digest(manifestBytes), digest(deliveryBytes)
	packageBytes, _ = json.Marshal(p)
	if err = os.WriteFile(filepath.Join(options.Root, "package.json"), packageBytes, 0600); err != nil {
		t.Fatal(err)
	}
	inspection, err := InspectTargets(options.Root, options.Targets)
	if err != nil || len(inspection) != 1 {
		t.Fatal(inspection, err)
	}
	if _, err = os.Stat(options.Output); !os.IsNotExist(err) {
		t.Fatal("inspection wrote output")
	}
	bound, err := Build(options, now, key)
	if err != nil || inspection[0].TemplateDigest != bound.Releases[0].TemplateDigest {
		t.Fatal("inspection and signer target differ", err)
	}
	if _, err = InspectTargets(options.Root, "../targets.json"); err == nil {
		t.Fatal("escaped target registry accepted")
	}
	raw, err := os.ReadFile(filepath.Join(options.Root, "workload.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(filepath.Join(options.Root, "workload.yaml"), []byte(strings.ReplaceAll(string(raw), "Always", "IfNotPresent")), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err = InspectTargets(options.Root, options.Targets); err == nil {
		t.Fatal("inspection skipped signing target policy")
	}
}
