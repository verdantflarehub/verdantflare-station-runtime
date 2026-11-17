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
