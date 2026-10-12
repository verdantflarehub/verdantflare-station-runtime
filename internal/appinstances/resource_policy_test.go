package appinstances

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/verdantflarehub/verdantflare-station-runtime/internal/computepolicy"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/types"
)

func TestOptionalResourceProfileOmissionAndReplay(t *testing.T) {
	_, p, _, _ := fixture()
	original := p.Hash()
	raw, _ := json.Marshal(p)
	var object map[string]any
	if err := json.Unmarshal(raw, &object); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"cpu_request_millis", "cpu_limit_millis", "memory_request_bytes", "memory_limit_bytes"} {
		delete(object, key)
	}
	raw, _ = json.Marshal([]any{object})
	profiles, err := LoadProfiles(strings.NewReader(string(raw)))
	if err != nil {
		t.Fatal(err)
	}
	next := profiles[p.Key()]
	if next.Hash() == original || !next.Valid() {
		t.Fatal("optional profile must have a distinct identity")
	}
	if got := profileResources(next); !equality.Semantic.DeepEqual(got, corev1.ResourceRequirements{}) {
		t.Fatal("resources were injected")
	}
	// Pod JSON omits empty maps; reload must remain compatible with API output.
	data, _ := json.Marshal(profileResources(next))
	var decoded corev1.ResourceRequirements
	if err = json.Unmarshal(data, &decoded); err != nil || !equality.Semantic.DeepEqual(decoded, profileResources(next)) {
		t.Fatal("API round-trip changed resource expectations")
	}
	legacy := corev1.ResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceCPU: *resource.NewMilliQuantity(p.CPURequestMillis, resource.DecimalSI), corev1.ResourceMemory: *resource.NewQuantity(p.MemoryRequestBytes, resource.BinarySI)}, Limits: corev1.ResourceList{corev1.ResourceCPU: *resource.NewMilliQuantity(p.CPULimitMillis, resource.DecimalSI), corev1.ResourceMemory: *resource.NewQuantity(p.MemoryLimitBytes, resource.BinarySI)}}
	if !equality.Semantic.DeepEqual(profileResources(p), legacy) || p.Hash() != original {
		t.Fatal("legacy resources changed")
	}
}

func TestStationPolicyAllowsLegacyProfileWithoutChangingItsIdentity(t *testing.T) {
	t.Setenv(computepolicy.Environment, computepolicy.Unrestricted)
	in, p, pvc, pv := fixture()
	c := client(pvc, pv)
	b := bind(t, c, in, p, pvc.Name)
	desired, _, err := WorkerObjects(in, p, b)
	if err != nil {
		t.Fatal(err)
	}
	actual := desired.DeepCopy()
	actual.UID = types.UID(uuid.NewString())
	computepolicy.Apply(&actual.Spec)
	if !podMatches(actual, desired, string(actual.UID), p.NodeName) {
		t.Fatal("policy did not preserve legacy profile binding")
	}
	if len(desired.Spec.Containers[0].Resources.Requests) != 2 {
		t.Fatal("original template changed")
	}
	actual.Spec.Containers[0].Resources.Requests = corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("9")}
	if podMatches(actual, desired, string(actual.UID), p.NodeName) {
		t.Fatal("arbitrary resources accepted")
	}
}

func TestOptionalResourcesWorkerSurvivesAPIRoundTrip(t *testing.T) {
	in, p, pvc, pv := fixture()
	p.CPURequestMillis, p.CPULimitMillis, p.MemoryRequestBytes, p.MemoryLimitBytes = 0, 0, 0, 0
	c := client(pvc, pv)
	b := bind(t, c, in, p, pvc.Name)
	pod, _, err := WorkerObjects(in, p, b)
	if err != nil {
		t.Fatal(err)
	}
	data, _ := json.Marshal(pod)
	var actual corev1.Pod
	if err = json.Unmarshal(data, &actual); err != nil {
		t.Fatal(err)
	}
	actual.UID = types.UID(uuid.NewString())
	if !podMatches(&actual, pod, string(actual.UID), p.NodeName) {
		t.Fatal("API round-trip rejected an undeclared resource profile")
	}
	actual.Spec.Containers[0].Resources.Limits = corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("1Gi")}
	if podMatches(&actual, pod, string(actual.UID), p.NodeName) {
		t.Fatal("injected resource limit accepted")
	}
}

func TestOptionalResourceProfileRejectsPartialNegativeAndExcessivePairs(t *testing.T) {
	for _, change := range []func(*Profile){
		func(p *Profile) { p.CPURequestMillis = -1 }, func(p *Profile) { p.CPULimitMillis = 0 }, func(p *Profile) { p.CPURequestMillis = 0 },
		func(p *Profile) { p.MemoryRequestBytes = -1 }, func(p *Profile) { p.MemoryLimitBytes = 0 }, func(p *Profile) { p.MemoryRequestBytes = 0 },
		func(p *Profile) { p.CPULimitMillis = 1048577 }, func(p *Profile) { p.MemoryLimitBytes = 1 << 51 },
	} {
		_, p, _, _ := fixture()
		change(&p)
		if p.Valid() {
			t.Fatal("invalid resource pair accepted")
		}
	}
	_, p, _, _ := fixture()
	p.CPURequestMillis, p.CPULimitMillis = 0, 0
	if !p.Valid() || len(profileResources(p).Requests) != 1 {
		t.Fatal("CPU omission rejected")
	}
	p.MemoryRequestBytes, p.MemoryLimitBytes = 0, 0
	if !p.Valid() {
		t.Fatal("all declarations omitted rejected")
	}
}
