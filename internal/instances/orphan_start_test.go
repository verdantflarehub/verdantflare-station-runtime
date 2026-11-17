package instances

import (
	"context"
	"errors"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"
	"testing"
)

func TestOrphanStartAbsenceFailsClosed(t *testing.T) {
	ctx := context.Background()
	client := fake.NewSimpleClientset(&corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "protected", Namespace: "app", Annotations: map[string]string{instanceKey: "other-user"}}})
	if err := orphanStartResourcesAbsent(ctx, client, "app", "orphan"); err != nil {
		t.Fatal(err)
	}
	for _, phase := range []corev1.PodPhase{corev1.PodPending, corev1.PodRunning, corev1.PodSucceeded, corev1.PodFailed} {
		pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "orphan", Namespace: "app", Annotations: map[string]string{instanceKey: "orphan"}}, Status: corev1.PodStatus{Phase: phase}}
		c := fake.NewSimpleClientset(pod)
		if !errors.Is(orphanStartResourcesAbsent(ctx, c, "app", "orphan"), ErrBusy) {
			t.Fatalf("must refuse %s Pod", phase)
		}
	}
	c := fake.NewSimpleClientset(&corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: "old", Namespace: "app", Annotations: map[string]string{instanceKey: "orphan"}}})
	if !errors.Is(orphanStartResourcesAbsent(ctx, c, "app", "orphan"), ErrBusy) {
		t.Fatal("service still exists")
	}
	client.PrependReactor("list", "pods", func(ktesting.Action) (bool, runtime.Object, error) { return true, nil, errors.New("unavailable") })
	if orphanStartResourcesAbsent(ctx, client, "app", "orphan") == nil {
		t.Fatal("API error cannot prove absence")
	}
	for _, action := range client.Actions() {
		if action.GetVerb() != "list" {
			t.Fatalf("unexpected write: %v", action)
		}
	}
}

func TestReapOrphanStartTerminalResources(t *testing.T) {
	for _, shape := range []string{"service_only", "succeeded", "failed", "running", "pending", "terminating", "foreign"} {
		t.Run(shape, func(t *testing.T) {
			b := VolumeBinding{Identity: Identity{InstanceID: "orphan", StationID: "station", OrganizationID: "org"}, Namespace: "app", PVCName: "workspace"}
			meta := metav1.ObjectMeta{Name: "blender-run-old", Namespace: "app", UID: "uid", ResourceVersion: "12", Annotations: map[string]string{instanceKey: b.InstanceID, stationKey: b.StationID, organizationKey: b.OrganizationID}}
			svc := &corev1.Service{ObjectMeta: *meta.DeepCopy()}
			objects := []runtime.Object{svc}
			if shape != "service_only" {
				pod := &corev1.Pod{ObjectMeta: *meta.DeepCopy(), Status: corev1.PodStatus{Phase: corev1.PodSucceeded}, Spec: corev1.PodSpec{Volumes: []corev1.Volume{{Name: "workspace", VolumeSource: corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: b.PVCName}}}}}}
				switch shape {
				case "failed":
					pod.Status.Phase = corev1.PodFailed
				case "running":
					pod.Status.Phase = corev1.PodRunning
				case "pending":
					pod.Status.Phase = corev1.PodPending
				case "terminating":
					now := metav1.Now()
					pod.DeletionTimestamp = &now
				case "foreign":
					pod.Annotations[organizationKey] = "other"
				}
				objects = append(objects, pod)
			}
			client := fake.NewSimpleClientset(objects...)
			err := reapOrphanStartResources(context.Background(), client, b)
			safe := shape == "service_only" || shape == "succeeded" || shape == "failed"
			if safe && err != nil {
				t.Fatal(err)
			}
			if !safe && !errors.Is(err, ErrBusy) {
				t.Fatal("unsafe cleanup", err)
			}
			deletes := 0
			for _, a := range client.Actions() {
				if a.GetVerb() == "delete" {
					deletes++
					opts := a.(ktesting.DeleteAction).GetDeleteOptions()
					if opts.Preconditions == nil || opts.Preconditions.UID == nil || opts.Preconditions.ResourceVersion == nil || string(*opts.Preconditions.UID) != "uid" || *opts.Preconditions.ResourceVersion != "12" {
						t.Fatal("missing exact preconditions")
					}
				}
			}
			if !safe && deletes != 0 {
				t.Fatal("mutated before validation")
			}
			if safe && deletes != len(objects) {
				t.Fatal("leftover resources")
			}
		})
	}
}
