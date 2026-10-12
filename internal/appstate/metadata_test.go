package appstate

import (
	"encoding/json"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
)

func TestStateObservationMetadata(t *testing.T) {
	d := deployment(t)
	d.Labels = map[string]string{"apps.verdantflare.com/managed": "true", "app.kubernetes.io/name": "fixture-app", "apps.verdantflare.com/group": "system", "apps.verdantflare.com/brand": "vf", "app.kubernetes.io/version": "1.0.0"}
	d.Annotations["station.verdantflare.com/template-hash"] = strings.Repeat("a", 64)
	d.Annotations["apps.verdantflare.com/display-name"] = "Fixture"
	d.Annotations["apps.verdantflare.com/source"] = "deploys/fixture.yaml"
	d.Annotations["unrelated.example/token"] = "must-not-cross-stream"
	d.Spec.Template.Spec.Containers = []corev1.Container{{Name: "server", Image: "example.invalid/fixture:1.0.0"}}
	o := observation(d, false)
	m := o.Metadata
	if m == nil || !m.OwnershipMarked || !m.ImagesComplete || len(m.Images) != 1 || m.Images[0].Image != d.Spec.Template.Spec.Containers[0].Image || m.TemplateHash != strings.Repeat("a", 64) || !m.Discovery.Enabled || m.Discovery.DisplayName != "Fixture" || m.Discovery.AppID != "fixture-app" {
		t.Fatal("metadata was not preserved")
	}
	raw, err := json.Marshal(o)
	if err != nil || strings.Contains(string(raw), "must-not-cross-stream") || strings.Contains(string(raw), "unrelated.example") {
		t.Fatal("non-allowlisted annotation leaked")
	}
	// The wire value must not retain any Kubernetes object references.
	d.Spec.Template.Spec.Containers[0].Image = "changed"
	d.Annotations["apps.verdantflare.com/display-name"] = "changed"
	if m.Images[0].Image != "example.invalid/fixture:1.0.0" || m.Discovery.DisplayName != "Fixture" {
		t.Fatal("metadata aliases informer input")
	}
}

func TestStateObservationMetadataBoundsAndPartialOwnership(t *testing.T) {
	d := deployment(t)
	d.Annotations = map[string]string{"station.verdantflare.com/unknown-owner-key": ""}
	if m := observationMetadata(d); !m.OwnershipMarked || m.ImagesComplete || len(m.Images) != 0 {
		t.Fatal("partial ownership or missing image evidence was invented")
	}
	d.Annotations = map[string]string{"unrelated.example/key": "value"}
	if observationMetadata(d).OwnershipMarked {
		t.Fatal("unrelated annotation is ownership")
	}
	d.Annotations["station.verdantflare.com/template-hash"] = strings.Repeat("a", 129)
	d.Annotations["apps.verdantflare.com/display-name"] = strings.Repeat("界", 171)
	d.Annotations["apps.verdantflare.com/models"] = strings.Repeat("x", 4097)
	d.Annotations["apps.verdantflare.com/source"] = strings.Repeat("x", 1025)
	d.Labels = map[string]string{"app.kubernetes.io/name": strings.Repeat("x", 129)}
	m := observationMetadata(d)
	if m.TemplateHash != "!invalid" || m.Discovery.DisplayName != "!invalid" || m.Discovery.Models != "!invalid" || m.Discovery.Source != "!invalid" || m.Discovery.AppID != "!invalid" {
		t.Fatal("oversized values were truncated or accepted")
	}
	d.Spec.Template.Spec.Containers = []corev1.Container{{Name: "valid", Image: "example.invalid/image:1"}, {Name: "bad", Image: strings.Repeat("x", 2049)}}
	if m = observationMetadata(d); m.ImagesComplete || len(m.Images) != 0 {
		t.Fatal("partial image list survived overflow")
	}
	d.Spec.Template.Spec.Containers = make([]corev1.Container, 65)
	if m = observationMetadata(d); m.ImagesComplete || len(m.Images) != 0 {
		t.Fatal("image count overflow accepted")
	}
}
