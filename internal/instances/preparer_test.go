package instances

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/google/uuid"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"
)

func helperFixture(t *testing.T) (Identity, HelperProfile, VolumeBinding, *fake.Clientset) {
	t.Helper()
	in, p, pvc, pv := fixture()
	client := fake.NewClientset(pvc, pv)
	ctx := context.Background()
	b, err := Inspect(ctx, client, in, p, pvc.Name, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err = Claim(ctx, client, in, p, b); err != nil {
		t.Fatal(err)
	}
	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: instanceName(in), Namespace: p.Namespace, Annotations: volumeOwner(in)}, Data: map[string][]byte{"worker-token": []byte(strings.Repeat("t", 32))}}
	secret.Annotations[stationKey] = in.StationID
	if _, err = client.CoreV1().Secrets(p.Namespace).Create(ctx, secret, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	client.PrependReactor("create", "*", func(action ktesting.Action) (bool, runtime.Object, error) {
		object := action.(ktesting.CreateAction).GetObject()
		if meta, ok := object.(metav1.Object); ok {
			meta.SetUID(types.UID(uuid.NewString()))
			meta.SetResourceVersion("1")
		}
		return false, nil, nil
	})
	return in, HelperProfile{StorageProfile: p, Image: "registry.example.test/blender-worker:v0.1.7", MaxFileBytes: 1 << 30}, b, client
}

func TestPreparerHasNoGPUAndRemovalWaitsForObservedExit(t *testing.T) {
	in, p, b, client := helperFixture(t)
	ctx := context.Background()
	observed, err := EnsureHelper(ctx, client, in, p, b, HelperObservation{})
	if err != nil {
		t.Fatal(err)
	}
	if observed.Ready || observed.PodUID == "" || observed.ServiceUID == "" {
		t.Fatal(observed)
	}
	pod, err := client.CoreV1().Pods(p.Namespace).Get(ctx, instanceName(in)+"-prepare", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(pod.Spec.Containers) != 1 || len(pod.Spec.Containers[0].Resources.Requests) != 2 || len(pod.Spec.Containers[0].Resources.Limits) != 2 || pod.Spec.Containers[0].Command[0] != "python3" {
		t.Fatal("preparer requested more than file-agent CPU/memory")
	}
	if pod.Spec.AutomountServiceAccountToken == nil || *pod.Spec.AutomountServiceAccountToken || pod.Spec.HostNetwork {
		t.Fatal("unexpected host or cluster access")
	}
	if pod.Spec.Volumes[0].PersistentVolumeClaim.ClaimName != b.PVCName || pod.Spec.Containers[0].VolumeMounts[0].SubPath != "workspace" {
		t.Fatal("workspace mount changed")
	}
	// Reconcile with no previous response recovers the same owned resource IDs.
	replay, err := EnsureHelper(ctx, client, in, p, b, HelperObservation{})
	if err != nil || replay != observed {
		t.Fatal(replay, err)
	}
	pod.Spec.NodeName = p.NodeName
	pod.Status.Phase = corev1.PodRunning
	pod.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}
	if _, err = client.CoreV1().Pods(p.Namespace).Update(ctx, pod, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	observed, err = EnsureHelper(ctx, client, in, p, b, observed)
	if err != nil || !observed.Ready {
		t.Fatal(observed, err)
	}
	done, err := RemoveHelper(ctx, client, in, p, b, observed)
	if err != nil || done {
		t.Fatal("deletion request counted as observed exit", done, err)
	}
	done, err = RemoveHelper(ctx, client, in, p, b, observed)
	if err != nil || !done {
		t.Fatal(done, err)
	}
	for _, action := range client.Actions() {
		if action.GetVerb() == "delete" {
			opts := action.(ktesting.DeleteAction).GetDeleteOptions()
			if opts.Preconditions == nil || opts.Preconditions.UID == nil || *opts.Preconditions.UID == "" {
				t.Fatal("unfenced deletion", action)
			}
		}
	}
	if _, err = client.CoreV1().PersistentVolumeClaims(p.Namespace).Get(ctx, b.PVCName, metav1.GetOptions{}); err != nil {
		t.Fatal("data was removed", err)
	}
	if _, err = client.CoreV1().Secrets(p.Namespace).Get(ctx, instanceName(in), metav1.GetOptions{}); err != nil {
		t.Fatal("worker credential removed", err)
	}
}

func TestPreparerRefusesReplacedPodsAndSpecificationDrift(t *testing.T) {
	cases := []struct {
		name   string
		change func(*corev1.Pod)
	}{
		{"uid", func(p *corev1.Pod) { p.UID = types.UID(uuid.NewString()) }},
		{"image", func(p *corev1.Pod) { p.Spec.Containers[0].Image = "registry.example.test/other:v1.0.0" }},
		{"host", func(p *corev1.Pod) { p.Spec.HostNetwork = true }},
		{"gpu", func(p *corev1.Pod) { p.Spec.Containers[0].Resources.Limits["nvidia.com/gpu"] = resource.MustParse("1") }},
		{"foreign-org", func(p *corev1.Pod) { p.Annotations[organizationKey] = uuid.NewString() }},
		{"privileged", func(p *corev1.Pod) { yes := true; p.Spec.Containers[0].SecurityContext.Privileged = &yes }},
		{"capabilities", func(p *corev1.Pod) {
			p.Spec.Containers[0].SecurityContext.Capabilities.Add = []corev1.Capability{"SYS_ADMIN"}
		}},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			in, p, b, client := helperFixture(t)
			ctx := context.Background()
			observed, err := EnsureHelper(ctx, client, in, p, b, HelperObservation{})
			if err != nil {
				t.Fatal(err)
			}
			pod, _ := client.CoreV1().Pods(p.Namespace).Get(ctx, instanceName(in)+"-prepare", metav1.GetOptions{})
			test.change(pod)
			if _, err = client.CoreV1().Pods(p.Namespace).Update(ctx, pod, metav1.UpdateOptions{}); err != nil {
				t.Fatal(err)
			}
			if _, err = EnsureHelper(ctx, client, in, p, b, observed); !errors.Is(err, ErrBinding) {
				t.Fatal("adopted modified pod", err)
			}
			before := len(client.Actions())
			if _, err = RemoveHelper(ctx, client, in, p, b, observed); !errors.Is(err, ErrBinding) {
				t.Fatal("deleted modified pod", err)
			}
			for _, action := range client.Actions()[before:] {
				if action.GetVerb() == "delete" {
					t.Fatal("partial deletion despite conflict")
				}
			}
		})
	}
}

func TestMissingObservedHelperIsNotRecreatedAndBusyWorkspaceNotReleased(t *testing.T) {
	in, p, b, client := helperFixture(t)
	ctx := context.Background()
	observed, err := EnsureHelper(ctx, client, in, p, b, HelperObservation{})
	if err != nil {
		t.Fatal(err)
	}
	if err = client.CoreV1().Pods(p.Namespace).Delete(ctx, instanceName(in)+"-prepare", metav1.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err = EnsureHelper(ctx, client, in, p, b, observed); !errors.Is(err, ErrBinding) {
		t.Fatal("missing pod recreated", err)
	}
	if _, err = RemoveHelper(ctx, client, in, p, b, observed); err != nil {
		t.Fatal(err)
	}
	outsider := volumePod(in, p.StorageProfile, b.PVCName)
	outsider.Name = "unexpected-user"
	if _, err = client.CoreV1().Pods(p.Namespace).Create(ctx, outsider, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	if done, err := RemoveHelper(ctx, client, in, p, b, observed); done || !errors.Is(err, ErrBusy) {
		t.Fatal("occupied workspace reported released", done, err)
	}
}

func TestPreparerRequiresClaimAndScopedSecret(t *testing.T) {
	in, p, b, client := helperFixture(t)
	ctx := context.Background()
	claim, _ := client.CoreV1().PersistentVolumeClaims(p.Namespace).Get(ctx, b.PVCName, metav1.GetOptions{})
	delete(claim.Annotations, instanceKey)
	_, _ = client.CoreV1().PersistentVolumeClaims(p.Namespace).Update(ctx, claim, metav1.UpdateOptions{})
	before := len(client.Actions())
	if _, err := EnsureHelper(ctx, client, in, p, b, HelperObservation{}); !errors.Is(err, ErrBinding) {
		t.Fatal("unclaimed workspace mounted", err)
	}
	for _, action := range client.Actions()[before:] {
		if action.GetVerb() == "create" {
			t.Fatal("effect on unclaimed workspace")
		}
	}
	if err := Claim(ctx, client, in, p.StorageProfile, b); err != nil {
		t.Fatal(err)
	}
	secret, _ := client.CoreV1().Secrets(p.Namespace).Get(ctx, instanceName(in), metav1.GetOptions{})
	secret.Annotations[organizationKey] = uuid.NewString()
	_, _ = client.CoreV1().Secrets(p.Namespace).Update(ctx, secret, metav1.UpdateOptions{})
	if _, err := EnsureHelper(ctx, client, in, p, b, HelperObservation{}); !errors.Is(err, ErrBinding) {
		t.Fatal("foreign credential used", err)
	}
}
