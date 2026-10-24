package instances

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/google/uuid"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestWorkerRecoveryRequiresExactFailedBindingAndSource(t *testing.T) {
	in, p, b, client, secret := workerFixture(t)
	ctx := context.Background()
	start := StartRequest{OperationID: uuid.NewString(), CreateOperationID: in.CreateOperationID, StationID: in.StationID, OrganizationID: in.OrganizationID, ProjectID: in.ProjectID, InstanceID: in.InstanceID, ProfileID: p.ID, SourceRevisionID: uuid.NewString(), Empty: true}
	obs, err := EnsureWorker(ctx, client, in, p, b, start.OperationID, secret, start.source(), WorkerObservation{})
	if err != nil {
		t.Fatal(err)
	}
	pod, _ := client.CoreV1().Pods(p.Namespace).Get(ctx, obs.PodName, metav1.GetOptions{})
	pod.Spec.NodeName = p.NodeName
	pod.Status.Phase = corev1.PodRunning
	_, _ = client.CoreV1().Pods(p.Namespace).Update(ctx, pod, metav1.UpdateOptions{})
	next := start
	next.OperationID = uuid.NewString()
	raw, _ := json.Marshal(start)
	state, _ := json.Marshal(startState{Worker: obs, Proof: &RunningProof{}})
	if _, err = prepareWorkerRecovery(ctx, client, next, p, b, raw, state); err == nil {
		t.Fatal("running worker recovered")
	}
	pod.Status.Phase = corev1.PodFailed
	_, _ = client.CoreV1().Pods(p.Namespace).Update(ctx, pod, metav1.UpdateOptions{})
	changed := next
	changed.SourceRevisionID = uuid.NewString()
	if _, err = prepareWorkerRecovery(ctx, client, changed, p, b, raw, state); err == nil {
		t.Fatal("changed source accepted")
	}
	recovery, err := prepareWorkerRecovery(ctx, client, next, p, b, raw, state)
	if err != nil {
		t.Fatal(err)
	}
	wrong := *recovery
	wrong.Worker.PodUID = uuid.NewString()
	if err = verifyFailedWorker(ctx, client, p, wrong, false); err == nil {
		t.Fatal("replaced UID accepted")
	}
	if gone, err := removeWorker(ctx, client, start, p, b, obs); err != nil || gone {
		t.Fatal(gone, err)
	}
	if err = verifyFailedWorker(ctx, client, p, *recovery, true); err != nil {
		t.Fatal("committed cleanup replay failed", err)
	}
	if _, err = prepareWorkerRecovery(ctx, client, next, p, b, raw, state); err == nil {
		t.Fatal("missing pod accepted without persisted recovery")
	}
	if gone, err := removeWorker(ctx, client, start, p, b, obs); err != nil || !gone {
		t.Fatal(gone, err)
	}
	if _, err = client.CoreV1().PersistentVolumeClaims(p.Namespace).Get(ctx, b.PVCName, metav1.GetOptions{}); err != nil {
		t.Fatal("workspace lost", err)
	}
	if _, err = EnsureWorker(ctx, client, in, p, b, next.OperationID, secret, next.source(), WorkerObservation{}); err != nil {
		t.Fatal("new attempt could not start", err)
	}
}

func TestRecoveryAcceptsSucceededButRejectsLiveOrUnknownWorker(t *testing.T) {
	in, p, b, client, secret := workerFixture(t)
	ctx := context.Background()
	start := StartRequest{OperationID: uuid.NewString(), CreateOperationID: in.CreateOperationID, StationID: in.StationID, OrganizationID: in.OrganizationID, ProjectID: in.ProjectID, InstanceID: in.InstanceID, ProfileID: p.ID, SourceRevisionID: uuid.NewString(), Empty: true}
	obs, err := EnsureWorker(ctx, client, in, p, b, start.OperationID, secret, start.source(), WorkerObservation{})
	if err != nil {
		t.Fatal(err)
	}
	next := start
	next.OperationID = uuid.NewString()
	raw, _ := json.Marshal(start)
	state, _ := json.Marshal(startState{Worker: obs, Proof: &RunningProof{}})
	pod, _ := client.CoreV1().Pods(p.Namespace).Get(ctx, obs.PodName, metav1.GetOptions{})
	pod.Spec.NodeName = p.NodeName
	for _, phase := range []corev1.PodPhase{corev1.PodPending, corev1.PodRunning, corev1.PodUnknown, corev1.PodSucceeded} {
		pod.Status.Phase = phase
		_, _ = client.CoreV1().Pods(p.Namespace).Update(ctx, pod, metav1.UpdateOptions{})
		_, err := prepareWorkerRecovery(ctx, client, next, p, b, raw, state)
		if (err == nil) != (phase == corev1.PodSucceeded) {
			t.Fatalf("phase %s: %v", phase, err)
		}
	}
}
