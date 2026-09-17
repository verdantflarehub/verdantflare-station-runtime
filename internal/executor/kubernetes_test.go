package executor

import (
	"context"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

func target() Target {
	one := int32(1)
	return Target{AppID: "test-app", Version: "1.0.0", Deployment: &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: "test-app", Namespace: "test-runtime"}, Spec: appsv1.DeploymentSpec{Replicas: &one, Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "test"}}, Template: corev1.PodTemplateSpec{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app": "test"}}, Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "server", Image: "example.invalid/server:1.0.0"}}}}}}}
}
func TestInstallRecoveryAndReadiness(t *testing.T) {
	ctx := context.Background()
	client := fake.NewSimpleClientset()
	driver := Kubernetes{client}
	app := target()
	step := func() Result {
		t.Helper()
		r, e := driver.Step(ctx, app, "install", "operation", "station", "org")
		if e != nil {
			t.Fatal(e)
		}
		return r
	}
	if r := step(); r.Status != "running" || r.Phase != "installing" {
		t.Fatal(r)
	}
	// Repeating after a lost response observes the single existing workload.
	if r := step(); r.Status != "running" {
		t.Fatal(r)
	}
	d, e := client.AppsV1().Deployments(app.Deployment.Namespace).Get(ctx, app.Deployment.Name, metav1.GetOptions{})
	if e != nil {
		t.Fatal(e)
	}
	d.Generation = 2
	d.Status = appsv1.DeploymentStatus{ObservedGeneration: 1, Replicas: 1, UpdatedReplicas: 1, ReadyReplicas: 1, AvailableReplicas: 1}
	if _, e = client.AppsV1().Deployments(d.Namespace).Update(ctx, d, metav1.UpdateOptions{}); e != nil {
		t.Fatal(e)
	}
	if r := step(); r.Status != "running" {
		t.Fatal("stale readiness accepted", r)
	}
	d.Status.ObservedGeneration = 2
	if _, e = client.AppsV1().Deployments(d.Namespace).UpdateStatus(ctx, d, metav1.UpdateOptions{}); e != nil {
		t.Fatal(e)
	}
	if r := step(); r.Status != "succeeded" || r.Phase != "completed" {
		t.Fatal(r)
	}
	creates := 0
	for _, a := range client.Actions() {
		if a.GetVerb() == "create" && a.GetResource().Resource == "deployments" {
			creates++
		}
	}
	if creates != 1 {
		t.Fatalf("duplicate creates: %d", creates)
	}
	r, e := driver.Step(ctx, app, "install", "other", "station", "other-org")
	if e != nil || r.Code != "CONFLICT" {
		t.Fatalf("cross-organization mutation: %+v %v", r, e)
	}
}
func TestUnavailableAdaptersHaveNoSideEffects(t *testing.T) {
	for _, action := range []string{"stop", "restart", "delete"} {
		c := fake.NewSimpleClientset()
		d := Kubernetes{c}
		r, e := d.Step(context.Background(), target(), action, "op", "station", "org")
		if e != nil || r.Status != "failed" || len(c.Actions()) != 0 {
			t.Fatalf("unsafe %s: %+v %v", action, r, e)
		}
	}
	c := fake.NewSimpleClientset()
	d := Kubernetes{c}
	app := target()
	app.Models = []string{"model@1.0.0"}
	r, e := d.Step(context.Background(), app, "install", "op", "station", "org")
	if e != nil || r.Status != "failed" || len(c.Actions()) != 0 {
		t.Fatal("model dependency bypassed")
	}
}
func TestExistingWorkloadIsNotAdoptedImplicitly(t *testing.T) {
	app := target()
	c := fake.NewSimpleClientset(app.Deployment)
	d := Kubernetes{c}
	r, e := d.Step(context.Background(), app, "install", "op", "station", "org")
	if e != nil || r.Code != "CONFLICT" {
		t.Fatal(r, e)
	}
	for _, a := range c.Actions() {
		if a.GetVerb() != "get" {
			t.Fatal("mutated existing workload")
		}
	}
}
