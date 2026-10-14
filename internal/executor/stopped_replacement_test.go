package executor

import (
	"context"
	"errors"
	"reflect"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

func stoppedFixture(t *testing.T) (*StoppedReplacement, *fake.Clientset, ManagementCheckpoint) {
	t.Helper()
	r, k, c := managementFixture(t)
	return &StoppedReplacement{r}, k, c
}

func stoppedObservation(t *testing.T, k *fake.Clientset, c ManagementCheckpoint, ready bool) {
	t.Helper()
	d, err := k.AppsV1().Deployments(c.Deployment.Namespace).Get(context.Background(), c.Deployment.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	d.Status = appsv1.DeploymentStatus{ObservedGeneration: d.Generation, Replicas: *d.Spec.Replicas}
	if ready {
		d.Status.UpdatedReplicas = 1
		d.Status.ReadyReplicas = 1
		d.Status.AvailableReplicas = 1
	}
	if _, err = k.AppsV1().Deployments(d.Namespace).UpdateStatus(context.Background(), d, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
}

func TestStoppedReplacementUpgradeRollbackAndLostResponses(t *testing.T) {
	r, k, c := stoppedFixture(t)
	ctx := context.Background()
	if _, err := r.Stop(ctx, c); !errors.Is(err, ErrManagementReplacement) {
		t.Fatal("stopped before routing gate", err)
	}
	if _, err := r.Start(ctx, c, true); !errors.Is(err, ErrManagementReplacement) {
		t.Fatal("started before routing gate", err)
	}
	for _, target := range []bool{true, false} {
		for n := 0; n < 2; n++ {
			done, err := r.Gate(ctx, c)
			if err != nil || done != (n == 1) {
				t.Fatal("gate", done, err)
			}
		}
		if done, err := r.Stop(ctx, c); err != nil || done {
			t.Fatal("stop skipped drain", err)
		}
		if done, err := r.Stop(ctx, c); err != nil || done {
			t.Fatal("stale status admitted", err)
		}
		stoppedObservation(t, k, c, false)
		if done, err := r.Stop(ctx, c); err != nil || !done {
			t.Fatal("stop", done, err)
		}
		if done, err := r.Start(ctx, c, target); err != nil || done {
			t.Fatal("start", done, err)
		}
		stoppedObservation(t, k, c, true)
		if done, err := r.Start(ctx, c, target); err != nil || !done {
			t.Fatal("replayed start", done, err)
		}
		if gated, err := r.routesGated(ctx, c); err != nil || !gated {
			t.Fatal("verification opened routes", err)
		}
		if _, err := r.Open(ctx, c, !target); !errors.Is(err, ErrManagementReplacement) {
			t.Fatal("opposite publication opened routes", err)
		}
		for n := 0; n < 2; n++ {
			done, err := r.Open(ctx, c, target)
			if err != nil || done != (n == 1) {
				t.Fatal("open replay", done, err)
			}
		}
		d, _, err := r.liveDeployment(ctx, c)
		if err != nil || d.UID != c.Deployment.UID || !reflect.DeepEqual(d.Spec.Template.Spec.Volumes, c.Deployment.Spec.Template.Spec.Volumes) {
			t.Fatal("persistent identity changed", err)
		}
	}
}

func TestStoppedReplacementWaitsForEndpointsAndTerminatingPods(t *testing.T) {
	r, k, c := stoppedFixture(t)
	ctx := context.Background()
	slice := &discoveryv1.EndpointSlice{ObjectMeta: metav1.ObjectMeta{Name: "stale-endpoints", Namespace: c.Deployment.Namespace, Labels: map[string]string{"kubernetes.io/service-name": c.Services[0].Name}}, AddressType: discoveryv1.AddressTypeIPv4, Endpoints: []discoveryv1.Endpoint{{Addresses: []string{"10.0.0.9"}}}}
	if _, err := k.DiscoveryV1().EndpointSlices(slice.Namespace).Create(ctx, slice, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Gate(ctx, c); err != nil {
		t.Fatal(err)
	}
	if done, err := r.Gate(ctx, c); err != nil || done {
		t.Fatal("stale endpoint accepted", err)
	}
	if _, err := r.Stop(ctx, c); !errors.Is(err, ErrManagementReplacement) {
		t.Fatal("routing not reconciled", err)
	}
	if err := k.DiscoveryV1().EndpointSlices(slice.Namespace).Delete(ctx, slice.Name, metav1.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	if done, err := r.Gate(ctx, c); err != nil || !done {
		t.Fatal(err)
	}
	if _, err := r.Stop(ctx, c); err != nil {
		t.Fatal(err)
	}
	stoppedObservation(t, k, c, false)
	time := metav1.Now()
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "draining", Namespace: c.Deployment.Namespace, Labels: c.Deployment.Spec.Selector.MatchLabels, DeletionTimestamp: &time}}
	if _, err := k.CoreV1().Pods(pod.Namespace).Create(ctx, pod, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	if done, err := r.Stop(ctx, c); err != nil || done {
		t.Fatal("terminating writer ignored", err)
	}
	if done, err := r.Start(ctx, c, true); err != nil || done {
		t.Fatal("replacement overlaps writer", err)
	}
}

func TestStoppedReplacementRejectsDriftWithoutMutatingIt(t *testing.T) {
	for _, scenario := range []string{"deployment uid", "foreign operation", "service uid", "selector drift", "image drift"} {
		t.Run(scenario, func(t *testing.T) {
			r, k, c := stoppedFixture(t)
			ctx := context.Background()
			if _, err := r.Gate(ctx, c); err != nil {
				t.Fatal(err)
			}
			d, _, _ := r.liveDeployment(ctx, c)
			s, _, _ := r.liveService(ctx, c.Services[0])
			switch scenario {
			case "deployment uid":
				d.UID = "replacement"
			case "foreign operation":
				s.Annotations[stoppedOperationKey] = "other"
			case "service uid":
				s.UID = "replacement"
			case "selector drift":
				s.Spec.Selector = map[string]string{"app": "other"}
			case "image drift":
				d.Spec.Template.Spec.Containers[0].Image = "unregistered"
			}
			if _, err := k.AppsV1().Deployments(d.Namespace).Update(ctx, d, metav1.UpdateOptions{}); err != nil {
				t.Fatal(err)
			}
			if _, err := k.CoreV1().Services(s.Namespace).Update(ctx, s, metav1.UpdateOptions{}); err != nil {
				t.Fatal(err)
			}
			k.ClearActions()
			if _, err := r.Stop(ctx, c); !errors.Is(err, ErrManagementReplacement) {
				t.Fatal("drift accepted", err)
			}
			for _, action := range k.Actions() {
				if action.GetVerb() != "get" && action.GetVerb() != "list" {
					t.Fatal("drift mutated")
				}
			}
		})
	}
}

func TestStoppedReplacementBindsWorkerlessInstallation(t *testing.T) {
	source, in := installationFixture()
	source.InstallationBindings = nil
	target := source
	target.Version = "1.0.1"
	target.Hash[0] = 1
	target.Deployment = source.Deployment.DeepCopy()
	target.Deployment.Spec.Template.Spec.Containers[0].Image = "example.invalid/server:1.0.1"
	k := &Kubernetes{Client: fake.NewSimpleClientset()}
	if _, err := NewManagementReplacement(k, source, target, in); err == nil {
		t.Fatal("legacy worker path changed")
	}
	r, err := NewStoppedReplacement(k, source, target, in)
	if err != nil || r.Source.InstallationID != in.InstallationId || r.Target.InstallationID != in.InstallationId {
		t.Fatal("workerless identity missing", err)
	}
	if source.InstallationID != "" {
		t.Fatal("shared template mutated")
	}
}

func TestStoppedReplacementFailedRolloutRequiresRecovery(t *testing.T) {
	r, k, c := stoppedFixture(t)
	ctx := context.Background()
	_, _ = r.Gate(ctx, c)
	_, _ = r.Stop(ctx, c)
	stoppedObservation(t, k, c, false)
	_, _ = r.Start(ctx, c, true)
	d, _, err := r.liveDeployment(ctx, c)
	if err != nil {
		t.Fatal(err)
	}
	d.Generation = 3
	d.Status.ObservedGeneration = 2
	d.Status.Conditions = []appsv1.DeploymentCondition{{Type: appsv1.DeploymentProgressing, Status: corev1.ConditionFalse, Reason: "ProgressDeadlineExceeded"}}
	if _, err = k.AppsV1().Deployments(d.Namespace).Update(ctx, d, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	if done, err := r.Start(ctx, c, true); err != nil || done {
		t.Fatal("stale failure used", done, err)
	}
	d.Status.ObservedGeneration = d.Generation
	if _, err = k.AppsV1().Deployments(d.Namespace).UpdateStatus(ctx, d, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	record := managementRecord{ManagementProgress: ManagementProgress{Phase: "applying"}, Checkpoint: &c}
	next, err := advanceStopped(ctx, record, ManagementBinding{}, r, &stoppedCheckFake{}, "prepare")
	if err != nil || next.Phase != "recovery_required" {
		t.Fatal("failed rollout kept waiting", next.Phase, err)
	}
	if gated, err := r.routesGated(ctx, c); err != nil || !gated {
		t.Fatal("failed rollout opened routes", err)
	}
}
