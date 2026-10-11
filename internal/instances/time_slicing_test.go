package instances

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

func TestTimeSlicingAdmissionCountsSlots(t *testing.T) {
	ctx := context.Background()
	node := computeNode("worker-test")
	node.Labels["nvidia.com/gpu.sharing-strategy"] = "time-slicing"
	node.Labels["nvidia.com/gpu.replicas"] = "2"
	node.Status.Allocatable[gpuResource] = resource.MustParse("2")
	existing := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "video", Namespace: "video"}, Spec: corev1.PodSpec{NodeName: node.Name, Containers: []corev1.Container{{Name: "video", Resources: workerResources()}}}, Status: corev1.PodStatus{Phase: corev1.PodRunning}}
	client := fake.NewClientset(node, existing)
	if err := AdmitWorker(ctx, client, node.Name); err != nil {
		t.Fatal("free shared slot rejected", err)
	}
	queued := existing.DeepCopy()
	queued.Name, queued.Spec.NodeName, queued.Status.Phase = "pending", "", corev1.PodPending
	if _, err := client.CoreV1().Pods(queued.Namespace).Create(ctx, queued, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	if err := AdmitWorker(ctx, client, node.Name); !errors.Is(err, ErrComputeCapacity) {
		t.Fatal("pending shared slot ignored", err)
	}
	for _, mode := range []string{"mps", "", "unknown"} {
		n := node.DeepCopy()
		n.Labels["nvidia.com/gpu.sharing-strategy"] = mode
		if err := AdmitWorker(ctx, fake.NewClientset(n), n.Name); !errors.Is(err, ErrComputeUnknown) {
			t.Fatal("unverified mode accepted", mode, err)
		}
	}
	for _, replicas := range []string{"", "0", "1", "-1", "invalid"} {
		n := node.DeepCopy()
		n.Labels["nvidia.com/gpu.replicas"] = replicas
		if err := AdmitWorker(ctx, fake.NewClientset(n), n.Name); !errors.Is(err, ErrComputeUnknown) {
			t.Fatal("invalid replica evidence accepted", replicas, err)
		}
	}
	node.Status.Allocatable["nvidia.com/gpu.shared"] = resource.MustParse("2")
	if err := AdmitWorker(ctx, fake.NewClientset(node), node.Name); !errors.Is(err, ErrComputeUnknown) {
		t.Fatal("mixed resource registration accepted", err)
	}
}

func TestTimeSlicingWorkerReplayPreservesBindingAndGraphics(t *testing.T) {
	in, p, b, client, secretUID := workerFixture(t)
	ctx := context.Background()
	node, _ := client.CoreV1().Nodes().Get(ctx, p.NodeName, metav1.GetOptions{})
	node.Labels["nvidia.com/gpu.sharing-strategy"] = "time-slicing"
	node.Labels["nvidia.com/gpu.replicas"] = "2"
	_, _ = client.CoreV1().Nodes().Update(ctx, node, metav1.UpdateOptions{})
	start := uuid.NewString()
	source := WorkerSource{SourceRevisionID: in.CreateOperationID, Empty: true}
	obs, err := EnsureWorker(ctx, client, in, p, b, start, secretUID, source, WorkerObservation{})
	if err != nil {
		t.Fatal(err)
	}
	replay, err := EnsureWorker(ctx, client, in, p, b, start, secretUID, source, obs)
	if err != nil || replay != obs {
		t.Fatal("shared worker binding changed", replay, err)
	}
	pod, _ := client.CoreV1().Pods(p.Namespace).Get(ctx, obs.PodName, metav1.GetOptions{})
	for _, env := range pod.Spec.Containers[0].Env {
		if env.Name == "NVIDIA_DRIVER_CAPABILITIES" && env.Value == "compute,utility,graphics,video,display" {
			return
		}
	}
	t.Fatal("graphics capabilities lost")
}
