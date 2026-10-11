package appinstances

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"
)

func removalFixture(t *testing.T) (*fake.Clientset, Run, Profile, Volume, Observation, StopProgress) {
	t.Helper()
	in, p, pvc, pv := fixture()
	c := client(pvc, pv)
	volume := bind(t, c, in, p, pvc.Name)
	observed, e := EnsureWorker(context.Background(), c, in, p, volume, Observation{}, func(context.Context, Observation) error { return nil })
	if e != nil {
		t.Fatal(e)
	}
	observed.BootID = uuid.NewString()
	pod, _ := c.CoreV1().Pods(p.Namespace).Get(context.Background(), runName(in), metav1.GetOptions{})
	pod.Spec.NodeName = p.NodeName
	if _, e = c.CoreV1().Pods(p.Namespace).Update(context.Background(), pod, metav1.UpdateOptions{}); e != nil {
		t.Fatal(e)
	}
	if _, e = c.CoreV1().Nodes().Create(context.Background(), &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: p.NodeName},
		Status: corev1.NodeStatus{Conditions: []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionTrue}}}}, metav1.CreateOptions{}); e != nil {
		t.Fatal(e)
	}
	progress := StopProgress{Proof: stopProof(in, p, observed, id()), Phase: "sealed"}
	c.ClearActions()
	return c, in, p, volume, observed, progress
}

func TestRemoveWorkerCommitsEachIntentAndRetainsInstanceData(t *testing.T) {
	c, in, p, volume, observed, progress := removalFixture(t)
	ctx := context.Background()
	var phases []string
	checkpoint := func(_ context.Context, next StopProgress) error {
		phases = append(phases, next.Phase)
		return nil
	}
	for _, phase := range []string{"pod_delete_requested", "service_delete_requested", "stopped"} {
		var e error
		progress, e = RemoveWorker(ctx, c, in, p, volume, observed, progress, checkpoint)
		if e != nil || progress.Phase != phase {
			t.Fatal("incorrect deletion progression", progress.Phase, e)
		}
	}
	if len(phases) != 4 {
		t.Fatal("missing durable stop checkpoints")
	}
	deletes := 0
	for _, a := range c.Actions() {
		if a.GetVerb() == "get" || a.GetVerb() == "list" {
			continue
		}
		if a.GetVerb() != "delete" || (a.GetResource().Resource != "pods" && a.GetResource().Resource != "services") {
			t.Fatal("modified retained resource")
		}
		deletes++
		options := a.(ktesting.DeleteAction).GetDeleteOptions()
		if options.Preconditions == nil || options.Preconditions.UID == nil || options.Preconditions.ResourceVersion == nil {
			t.Fatal("unfenced deletion")
		}
		if a.GetResource().Resource == "pods" && (options.GracePeriodSeconds == nil || *options.GracePeriodSeconds != 30 || string(*options.Preconditions.UID) != observed.PodUID) {
			t.Fatal("forced or wrong Pod deletion")
		}
	}
	if deletes != 2 {
		t.Fatal("unexpected deletion count", deletes)
	}
	if _, e := c.CoreV1().PersistentVolumeClaims(p.Namespace).Get(ctx, volume.PVCName, metav1.GetOptions{}); e != nil {
		t.Fatal("workspace claim lost")
	}
	if _, e := c.CoreV1().PersistentVolumes().Get(ctx, volume.PVName, metav1.GetOptions{}); e != nil {
		t.Fatal("workspace data lost")
	}
	if _, e := c.CoreV1().Secrets(p.Namespace).Get(ctx, instanceName(in.Identity), metav1.GetOptions{}); e != nil {
		t.Fatal("instance credential lost")
	}
	c.ClearActions()
	if _, e := RemoveWorker(ctx, c, in, p, volume, observed, progress, checkpoint); e != nil {
		t.Fatal(e)
	}
	assertNoEffects(t, c)
}

func TestRemoveWorkerCheckpointFailureStopsBeforeNextDelete(t *testing.T) {
	for _, fail := range []string{"pod_delete_requested", "pod_deleted", "service_delete_requested", "stopped"} {
		t.Run(fail, func(t *testing.T) {
			c, in, p, volume, observed, progress := removalFixture(t)
			failed := errors.New("checkpoint failed")
			checkpoint := func(_ context.Context, next StopProgress) error {
				if next.Phase == fail {
					return failed
				}
				return nil
			}
			var e error
			for range 3 {
				progress, e = RemoveWorker(context.Background(), c, in, p, volume, observed, progress, checkpoint)
				if e != nil {
					break
				}
			}
			if !errors.Is(e, failed) {
				t.Fatal("checkpoint failure ignored", e)
			}
			deletes := 0
			for _, a := range c.Actions() {
				if a.GetVerb() == "delete" {
					deletes++
				}
			}
			expected := map[string]int{"pod_delete_requested": 0, "pod_deleted": 1, "service_delete_requested": 1, "stopped": 2}[fail]
			if deletes != expected {
				t.Fatal("progressed beyond uncommitted intent")
			}
		})
	}
}

func TestRemoveWorkerLostDeleteResponseResumesOriginalIntent(t *testing.T) {
	c, in, p, volume, observed, progress := removalFixture(t)
	ctx := context.Background()
	c.PrependReactor("delete", "pods", func(a ktesting.Action) (bool, runtime.Object, error) {
		e := c.Tracker().Delete(corev1.SchemeGroupVersion.WithResource("pods"), p.Namespace, runName(in))
		if e != nil {
			t.Fatal(e)
		}
		return true, nil, errors.New("lost deletion response")
	})
	var persisted StopProgress
	checkpoint := func(_ context.Context, next StopProgress) error { persisted = next; return nil }
	if _, e := RemoveWorker(ctx, c, in, p, volume, observed, progress, checkpoint); e == nil || persisted.Phase != "pod_delete_requested" {
		t.Fatal("missing original deletion intent")
	}
	var e error
	progress, e = RemoveWorker(ctx, c, in, p, volume, observed, persisted, checkpoint)
	if e != nil || progress.Phase != "service_delete_requested" {
		t.Fatal("lost response not recoverable", e)
	}
	progress, e = RemoveWorker(ctx, c, in, p, volume, observed, progress, checkpoint)
	if e != nil || progress.Phase != "stopped" {
		t.Fatal(e)
	}
}

func TestRemoveWorkerRejectsReplacementsMissingIntentAndInvalidProof(t *testing.T) {
	for _, mode := range []string{"pod-replaced", "service-replaced", "missing", "proof", "volume", "secret", "node-unreachable"} {
		t.Run(mode, func(t *testing.T) {
			c, in, p, volume, observed, progress := removalFixture(t)
			ctx := context.Background()
			switch mode {
			case "pod-replaced":
				pod, _ := c.CoreV1().Pods(p.Namespace).Get(ctx, runName(in), metav1.GetOptions{})
				pod.UID = types.UID(uuid.NewString())
				c.CoreV1().Pods(p.Namespace).Update(ctx, pod, metav1.UpdateOptions{})
			case "service-replaced":
				svc, _ := c.CoreV1().Services(p.Namespace).Get(ctx, runName(in), metav1.GetOptions{})
				svc.UID = types.UID(uuid.NewString())
				c.CoreV1().Services(p.Namespace).Update(ctx, svc, metav1.UpdateOptions{})
			case "missing":
				c.CoreV1().Pods(p.Namespace).Delete(ctx, runName(in), metav1.DeleteOptions{})
			case "proof":
				progress.Proof.BootID = uuid.NewString()
			case "volume":
				pvc, _ := c.CoreV1().PersistentVolumeClaims(p.Namespace).Get(ctx, volume.PVCName, metav1.GetOptions{})
				pvc.UID = types.UID(uuid.NewString())
				c.CoreV1().PersistentVolumeClaims(p.Namespace).Update(ctx, pvc, metav1.UpdateOptions{})
			case "secret":
				c.CoreV1().Secrets(p.Namespace).Delete(ctx, instanceName(in.Identity), metav1.DeleteOptions{})
			case "node-unreachable":
				node, _ := c.CoreV1().Nodes().Get(ctx, p.NodeName, metav1.GetOptions{})
				node.Status.Conditions[0].Status = corev1.ConditionUnknown
				c.CoreV1().Nodes().Update(ctx, node, metav1.UpdateOptions{})
			}
			c.ClearActions()
			if _, e := RemoveWorker(ctx, c, in, p, volume, observed, progress, func(context.Context, StopProgress) error { return nil }); e == nil {
				t.Fatal("invalid stop state accepted")
			}
			assertNoEffects(t, c)
		})
	}
}

func TestRemoveWorkerWaitsForNormalPodTermination(t *testing.T) {
	c, in, p, volume, observed, progress := removalFixture(t)
	ctx := context.Background()
	progress.Phase = "pod_delete_requested"
	pod, _ := c.CoreV1().Pods(p.Namespace).Get(ctx, runName(in), metav1.GetOptions{})
	now, grace := metav1.Now(), int64(30)
	pod.DeletionTimestamp, pod.DeletionGracePeriodSeconds = &now, &grace
	c.CoreV1().Pods(p.Namespace).Update(ctx, pod, metav1.UpdateOptions{})
	c.ClearActions()
	next, e := RemoveWorker(ctx, c, in, p, volume, observed, progress, func(context.Context, StopProgress) error { t.Fatal("premature stop checkpoint"); return nil })
	if e != nil || next.Phase != progress.Phase {
		t.Fatal("pending termination treated as stopped", e)
	}
	assertNoEffects(t, c)
	if _, e := c.CoreV1().Services(p.Namespace).Get(ctx, runName(in), metav1.GetOptions{}); apierrors.IsNotFound(e) {
		t.Fatal("Service removed while Pod still present")
	}
}
