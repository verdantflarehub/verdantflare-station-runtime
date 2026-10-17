package releasebuilder

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestVideoSourceSigningBindsBothContainers(t *testing.T) {
	for _, scenario := range []string{"valid", "valid-target", "valid-rebased-source", "valid-rebased-target", "valid-filter-target", "missing-sidecar", "wrong-sidecar-image", "invalid-sidecar-digest", "wrong-name", "duplicate", "primary-digest", "wrong-repository", "wrong-version", "extra-container"} {
		t.Run(scenario, func(t *testing.T) {
			options, now, key := builderFixture(t)
			for _, name := range []string{"manifest.yaml", "workload.yaml", "prepare.json", "package.json", "delivery.json"} {
				path := filepath.Join(options.Root, name)
				raw, err := os.ReadFile(path)
				if err != nil {
					t.Fatal(err)
				}
				text := strings.ReplaceAll(strings.ReplaceAll(string(raw), "test-app", "video-mcp"), "1.0.0", "0.17.28")
				if name == "manifest.yaml" {
					text = strings.Replace(text, "component: server", "component: api", 1)
					text = strings.Replace(text, "images: [{component: api, image: 'example.invalid/server:0.17.28'}]", "images: [{component: api, image: 'example.invalid/server:0.17.28'}, {component: artifact-authority, image: 'example.invalid/server:0.17.12'}]", 1)
				}
				if name == "workload.yaml" {
					text = strings.Replace(text, "name: server", "name: api", 1)
					text += "        - {name: artifact-authority, image: 'example.invalid/server:0.17.12', imagePullPolicy: Always}\n"
					if scenario == "extra-container" {
						text += "        - {name: extra, image: 'example.invalid/server:0.17.12', imagePullPolicy: Always}\n"
					}
				}
				if scenario == "valid-target" {
					text = strings.ReplaceAll(text, "0.17.28", "0.17.29")
				}
				if scenario == "valid-rebased-source" {
					text = strings.ReplaceAll(text, "0.17.28", "0.17.30")
				}
				if scenario == "valid-filter-target" {
					text = strings.ReplaceAll(text, "0.17.28", "0.17.32")
				}
				if scenario == "valid-rebased-target" {
					text = strings.ReplaceAll(text, "0.17.28", "0.17.31")
				}
				if err := os.WriteFile(path, []byte(text), 0600); err != nil {
					t.Fatal(err)
				}
			}
			deliveryPath := filepath.Join(options.Root, "delivery.json")
			raw, _ := os.ReadFile(deliveryPath)
			var d delivery
			if err := json.Unmarshal(raw, &d); err != nil {
				t.Fatal(err)
			}
			d.Repository = "verdantflarehub/verdantflare-app-video"
			d.Components = []deliveryComponent{{"api", d.Image, d.ImageDigest}, {"artifact-authority", "example.invalid/server:0.17.12", digest([]byte("sidecar"))}}
			switch scenario {
			case "missing-sidecar":
				d.Components = d.Components[:1]
			case "wrong-sidecar-image":
				d.Components[1].Image = d.Image
			case "invalid-sidecar-digest":
				d.Components[1].ImageDigest = "unknown"
			case "wrong-name":
				d.Components[1].Name = "other"
			case "duplicate":
				d.Components[1] = d.Components[0]
			case "primary-digest":
				d.Components[0].ImageDigest = digest([]byte("wrong"))
			case "wrong-repository":
				d.Repository = "verdantflarehub/verdantflare-app-video-mcp"
			case "wrong-version":
				d.AppVersion = "0.17.29"
			}
			raw, _ = json.Marshal(d)
			if err := os.WriteFile(deliveryPath, raw, 0600); err != nil {
				t.Fatal(err)
			}
			packagePath := filepath.Join(options.Root, "package.json")
			raw, _ = os.ReadFile(packagePath)
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
			raw, _ = json.Marshal(p)
			if err := os.WriteFile(packagePath, raw, 0600); err != nil {
				t.Fatal(err)
			}
			_, err := Build(options, now, key)
			valid := strings.HasPrefix(scenario, "valid")
			if (err == nil) != valid {
				t.Fatal("signing admission mismatch", err)
			}
			if !valid {
				if _, err := os.Stat(options.Output); !os.IsNotExist(err) {
					t.Fatal("invalid source created signed output")
				}
			}
		})
	}
}
