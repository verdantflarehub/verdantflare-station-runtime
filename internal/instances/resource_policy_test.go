package instances

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/google/uuid"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

func TestResourcePolicyPreservesProfileIdentity(t *testing.T) {
	entry := profileConfig{ID: "blender-standard", Namespace: "blender-test", Pool: "blender", NodeName: "worker-test", Claims: []string{"workspace-1"}, StorageBytes: 50 << 30, Image: "registry.example.test/blender-worker:v0.1.7", MaxFileBytes: 1 << 30}
	data, _ := json.Marshal([]profileConfig{entry})
	path := filepath.Join(t.TempDir(), "profiles.json")
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
	profiles, err := LoadProfiles(path)
	if err != nil {
		t.Fatal(err)
	}
	original, _ := json.Marshal(struct {
		Config   profileConfig `json:"config"`
		Template string        `json:"worker_template"`
	}{entry, "blender-desktop-v1"})
	if profiles[entry.ID].Hash != sha256.Sum256(original) {
		t.Fatal("persisted workspace and execution identity changed")
	}
}

func TestResourcePolicyIgnoresCPUAndMemoryReservations(t *testing.T) {
	node := computeNode("worker-test")
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "busy", Namespace: "video"}, Spec: corev1.PodSpec{NodeName: node.Name, Containers: []corev1.Container{{Name: "busy", Resources: corev1.ResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("1000"), corev1.ResourceMemory: resource.MustParse("1Ti")}}}}}}
	if err := AdmitWorker(context.Background(), fake.NewClientset(node, pod), node.Name); err != nil {
		t.Fatal(err)
	}
}

func TestResourcePolicyNewAndLegacyTemplatesRemainStrict(t *testing.T) {
	in, p, b, _, _ := workerFixture(t)
	start := uuid.NewString()
	source := WorkerSource{SourceRevisionID: in.CreateOperationID, Empty: true}
	worker, svc, err := workerObjects(in, p, b, start, source)
	if err != nil {
		t.Fatal(err)
	}
	oldWorker, oldSvc, err := legacyWorkerObjects(in, p, b, start, source)
	if err != nil {
		t.Fatal(err)
	}
	helper, helperSvc, err := helperObjects(in, p, b)
	if err != nil {
		t.Fatal(err)
	}
	oldHelper, oldHelperSvc, err := legacyHelperObjects(in, p, b)
	if err != nil {
		t.Fatal(err)
	}
	if !equality.Semantic.DeepEqual(svc, oldSvc) || !equality.Semantic.DeepEqual(helperSvc, oldHelperSvc) {
		t.Fatal("Service identity changed")
	}
	for _, pod := range []*corev1.Pod{worker, helper} {
		for _, resources := range []corev1.ResourceList{pod.Spec.Containers[0].Resources.Requests, pod.Spec.Containers[0].Resources.Limits} {
			for _, name := range []corev1.ResourceName{corev1.ResourceCPU, corev1.ResourceMemory} {
				if _, ok := resources[name]; ok {
					t.Fatal("CPU/memory declaration retained")
				}
			}
		}
	}
	for _, test := range []struct {
		name            string
		current, legacy *corev1.Pod
		match           func(*corev1.Pod, *corev1.Pod, string) bool
	}{
		{"worker", worker, oldWorker, workerPodMatches}, {"helper", helper, oldHelper, helperPodMatches},
	} {
		t.Run(test.name, func(t *testing.T) {
			for _, original := range []*corev1.Pod{test.current, test.legacy} {
				actual := original.DeepCopy()
				actual.UID = "pinned-uid"
				if !test.match(actual, test.current, "pinned-uid") {
					t.Fatal("approved template rejected")
				}
				if test.match(actual, test.current, "different-uid") {
					t.Fatal("UID drift accepted")
				}
				mutations := []func(*corev1.Pod){
					func(p *corev1.Pod) {
						if p.Spec.Containers[0].Resources.Requests == nil {
							p.Spec.Containers[0].Resources.Requests = corev1.ResourceList{}
						}
						p.Spec.Containers[0].Resources.Requests[corev1.ResourceCPU] = resource.MustParse("9")
					},
					func(p *corev1.Pod) { p.Spec.Containers[0].Image = "evil.example/app:v1.0.0" },
					func(p *corev1.Pod) { p.Spec.Containers[0].Env[0].Value = "other-instance" },
					func(p *corev1.Pod) { p.Spec.Volumes[0].PersistentVolumeClaim.ClaimName = "other-workspace" },
				}
				for _, mutate := range mutations {
					changed := actual.DeepCopy()
					mutate(changed)
					if test.match(changed, test.current, "pinned-uid") {
						t.Fatal("template drift accepted")
					}
				}
			}
		})
	}
}

func TestResourcePolicyResumesLegacyWorkerWithoutRecreation(t *testing.T) {
	in, p, b, client, secret := workerFixture(t)
	ctx := context.Background()
	start := uuid.NewString()
	source := WorkerSource{SourceRevisionID: in.CreateOperationID, Empty: true}
	pod, svc, err := legacyWorkerObjects(in, p, b, start, source)
	if err != nil {
		t.Fatal(err)
	}
	pod, err = client.CoreV1().Pods(p.Namespace).Create(ctx, pod, metav1.CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	svc, err = client.CoreV1().Services(p.Namespace).Create(ctx, svc, metav1.CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	client.ClearActions()
	obs, err := EnsureWorker(ctx, client, in, p, b, start, secret, source, WorkerObservation{PodUID: string(pod.UID), ServiceUID: string(svc.UID)})
	if err != nil || obs.PodUID != string(pod.UID) {
		t.Fatal(obs, err)
	}
	for _, action := range client.Actions() {
		if action.GetVerb() != "get" && action.GetVerb() != "list" {
			t.Fatal("legacy worker mutated", action.GetVerb())
		}
	}
}

func TestResourcePolicyResumesLegacyPreparerWithoutRecreation(t *testing.T) {
	in, p, b, client := helperFixture(t)
	ctx := context.Background()
	pod, svc, err := legacyHelperObjects(in, p, b)
	if err != nil {
		t.Fatal(err)
	}
	pod, err = client.CoreV1().Pods(p.Namespace).Create(ctx, pod, metav1.CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	svc, err = client.CoreV1().Services(p.Namespace).Create(ctx, svc, metav1.CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	client.ClearActions()
	obs, err := EnsureHelper(ctx, client, in, p, b, HelperObservation{PodUID: string(pod.UID), ServiceUID: string(svc.UID)})
	if err != nil || obs.PodUID != string(pod.UID) {
		t.Fatal(obs, err)
	}
	for _, action := range client.Actions() {
		if action.GetVerb() != "get" && action.GetVerb() != "list" {
			t.Fatal("legacy preparer mutated", action.GetVerb())
		}
	}
}
