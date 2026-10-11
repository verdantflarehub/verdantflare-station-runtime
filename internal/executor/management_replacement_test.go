package executor

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"
)

func managementFixture(t *testing.T) (*ManagementReplacement, *fake.Clientset, ManagementCheckpoint) {
	t.Helper()
	source, in := installationFixture()
	source.Hash[0] = 1
	source.Services = []*corev1.Service{{ObjectMeta: metav1.ObjectMeta{Name: "manager", Namespace: source.Deployment.Namespace}, Spec: corev1.ServiceSpec{
		Selector: map[string]string{"app": "test"}, Ports: []corev1.ServicePort{{Port: 80}},
	}}}
	target := source
	target.Deployment = source.Deployment.DeepCopy()
	target.Version, target.Hash[0] = "1.0.1", 2
	target.Deployment.Spec.Template.Spec.Containers[0].Image = "example.invalid/server:1.0.1"
	client := fake.NewSimpleClientset()
	r, err := NewManagementReplacement(&Kubernetes{client}, source, target, in)
	if err != nil {
		t.Fatal(err)
	}
	d := r.Source.Deployment.DeepCopy()
	d.UID, d.ResourceVersion, d.Generation = "deployment-uid", "10", 1
	d.Status = appsv1.DeploymentStatus{ObservedGeneration: 1, Replicas: 1, UpdatedReplicas: 1, ReadyReplicas: 1, AvailableReplicas: 1}
	mark(&d.ObjectMeta, r.Source, r.StationID, r.OrganizationID)
	d.Annotations["external.annotation"] = "preserve"
	s := source.Services[0].DeepCopy()
	s.UID, s.ResourceVersion, s.Spec.ClusterIP = "service-uid", "11", "10.0.0.2"
	mark(&s.ObjectMeta, r.Source, r.StationID, r.OrganizationID)
	s.Annotations["external.annotation"] = "preserve"
	if err = client.Tracker().Add(d); err != nil {
		t.Fatal(err)
	}
	if err = client.Tracker().Add(s); err != nil {
		t.Fatal(err)
	}
	c, err := r.Capture(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	client.ClearActions()
	return r, client, c
}

func managementUpdates(t *testing.T, client *fake.Clientset) []string {
	t.Helper()
	var updates []string
	for _, a := range client.Actions() {
		if a.GetVerb() == "get" {
			continue
		}
		if a.GetVerb() != "update" || (a.GetResource().Resource != "services" && a.GetResource().Resource != "deployments") {
			t.Fatalf("unexpected mutation: %s %s", a.GetVerb(), a.GetResource().Resource)
		}
		updates = append(updates, a.GetResource().Resource)
	}
	return updates
}

func TestManagementReplacementReplayAndRestore(t *testing.T) {
	ctx := context.Background()
	r, client, checkpoint := managementFixture(t)
	raw, err := json.Marshal(checkpoint)
	if err != nil {
		t.Fatal(err)
	}
	var saved ManagementCheckpoint
	if err = json.Unmarshal(raw, &saved); err != nil {
		t.Fatal(err)
	}
	before, _ := json.Marshal(r.Source)
	// Simulate API success followed by a lost response on BOTH resources.
	lost := errors.New("response lost")
	client.PrependReactor("update", "*", func(a ktesting.Action) (bool, runtime.Object, error) {
		u := a.(ktesting.UpdateAction)
		if err := client.Tracker().Update(a.GetResource(), u.GetObject(), a.GetNamespace()); err != nil {
			return true, nil, err
		}
		return true, nil, lost
	})
	for i := 0; i < 2; i++ {
		if done, err := r.Apply(ctx, saved); done || !errors.Is(err, lost) {
			t.Fatalf("lost response: %v %v", done, err)
		}
	}
	// A new driver after process restart observes effects, without new writes.
	restarted := *r
	if done, err := restarted.Apply(ctx, saved); !done || err != nil {
		t.Fatalf("replay: %v %v", done, err)
	}
	if got := managementUpdates(t, client); !reflect.DeepEqual(got, []string{"services", "deployments"}) {
		t.Fatal(got)
	}
	// Restoring the same saved JSON also reconciles lost update responses.
	for i := 0; i < 2; i++ {
		if done, err := restarted.Restore(ctx, saved); done || !errors.Is(err, lost) {
			t.Fatalf("restore response: %v %v", done, err)
		}
	}
	if done, err := restarted.Restore(ctx, saved); !done || err != nil {
		t.Fatalf("restored: %v %v", done, err)
	}
	if got := managementUpdates(t, client); !reflect.DeepEqual(got, []string{"services", "deployments", "deployments", "services"}) {
		t.Fatal(got)
	}
	d, _ := client.AppsV1().Deployments(saved.Deployment.Namespace).Get(ctx, saved.Deployment.Name, metav1.GetOptions{})
	s, _ := client.CoreV1().Services(saved.Services[0].Namespace).Get(ctx, saved.Services[0].Name, metav1.GetOptions{})
	if !reflect.DeepEqual(d.Spec, checkpoint.Deployment.Spec) || !reflect.DeepEqual(d.Annotations, checkpoint.Deployment.Annotations) ||
		!reflect.DeepEqual(s.Spec, checkpoint.Services[0].Spec) || !reflect.DeepEqual(s.Annotations, checkpoint.Services[0].Annotations) {
		t.Fatal("original specification not restored")
	}
	after, _ := json.Marshal(r.Source)
	checkAfter, _ := json.Marshal(saved)
	if string(before) != string(after) || string(raw) != string(checkAfter) {
		t.Fatal("template or checkpoint mutated")
	}
}

func TestManagementReplacementPartialFailureAndReadiness(t *testing.T) {
	ctx := context.Background()
	r, client, saved := managementFixture(t)
	if done, err := r.Apply(ctx, saved); done || err != nil {
		t.Fatal(done, err)
	}
	// Service updated, Deployment unavailable. Restore must retain its service
	// marks until the ORIGINAL Deployment has become ready again.
	d := saved.Deployment.DeepCopy()
	d.Status.ReadyReplicas = 0
	gvr := schema.GroupVersionResource{Group: "apps", Version: "v1", Resource: "deployments"}
	if err := client.Tracker().Update(gvr, d, d.Namespace); err != nil {
		t.Fatal(err)
	}
	if done, err := r.Restore(ctx, saved); done || err != nil {
		t.Fatal(done, err)
	}
	if got := managementUpdates(t, client); !reflect.DeepEqual(got, []string{"services"}) {
		t.Fatal(got)
	}
	d.Status.ReadyReplicas = 1
	if err := client.Tracker().Update(gvr, d, d.Namespace); err != nil {
		t.Fatal(err)
	}
	if done, err := r.Restore(ctx, saved); done || err != nil {
		t.Fatal(done, err)
	}
	if done, err := r.Restore(ctx, saved); !done || err != nil {
		t.Fatal(done, err)
	}
	if got := managementUpdates(t, client); !reflect.DeepEqual(got, []string{"services", "services"}) {
		t.Fatal(got)
	}
}

func TestManagementReplacementRejectsDrift(t *testing.T) {
	for name, mutate := range map[string]func(*appsv1.Deployment){
		"uid":              func(d *appsv1.Deployment) { d.UID = "replacement" },
		"resource version": func(d *appsv1.Deployment) { d.ResourceVersion = "" },
		"owner":            func(d *appsv1.Deployment) { d.Annotations[ownerKey] = "foreign" },
		"operation":        func(d *appsv1.Deployment) { d.Annotations[transitionOperationKey] = "foreign" },
		"image":            func(d *appsv1.Deployment) { d.Spec.Template.Spec.Containers[0].Image = "foreign:1" },
		"replicas":         func(d *appsv1.Deployment) { *d.Spec.Replicas = 2 },
		"deleting":         func(d *appsv1.Deployment) { now := metav1.Now(); d.DeletionTimestamp = &now },
	} {
		t.Run(name, func(t *testing.T) {
			r, client, saved := managementFixture(t)
			d := saved.Deployment.DeepCopy()
			mutate(d)
			if err := client.Tracker().Update(schema.GroupVersionResource{Group: "apps", Version: "v1", Resource: "deployments"}, d, d.Namespace); err != nil {
				t.Fatal(err)
			}
			for _, step := range []func(context.Context, ManagementCheckpoint) (bool, error){r.Apply, r.Restore} {
				if done, err := step(context.Background(), saved); done || !errors.Is(err, ErrManagementReplacement) {
					t.Fatal(done, err)
				}
			}
			if len(managementUpdates(t, client)) != 0 {
				t.Fatal("drift overwritten")
			}
		})
	}
	for name, mutate := range map[string]func(*corev1.Service){
		"uid":        func(s *corev1.Service) { s.UID = "replacement" },
		"owner":      func(s *corev1.Service) { s.Annotations[orgKey] = "foreign" },
		"operation":  func(s *corev1.Service) { s.Annotations[transitionOperationKey] = "foreign" },
		"selector":   func(s *corev1.Service) { s.Spec.Selector["app"] = "foreign" },
		"cluster ip": func(s *corev1.Service) { s.Spec.ClusterIP = "10.0.0.3" },
	} {
		t.Run("service "+name, func(t *testing.T) {
			r, client, saved := managementFixture(t)
			s := saved.Services[0].DeepCopy()
			mutate(s)
			if err := client.Tracker().Update(schema.GroupVersionResource{Version: "v1", Resource: "services"}, s, s.Namespace); err != nil {
				t.Fatal(err)
			}
			for _, step := range []func(context.Context, ManagementCheckpoint) (bool, error){r.Apply, r.Restore} {
				if done, err := step(context.Background(), saved); done || !errors.Is(err, ErrManagementReplacement) {
					t.Fatal(done, err)
				}
			}
			if len(managementUpdates(t, client)) != 0 {
				t.Fatal("service drift overwritten")
			}
		})
	}
}

func TestManagementReplacementRejectsIncompatibleTargets(t *testing.T) {
	for name, mutate := range map[string]func(*Target){
		"resources": func(v *Target) {
			v.Deployment.Spec.Template.Spec.Containers[0].Resources.Requests = corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("1Gi")}
		},
		"replica": func(v *Target) { *v.Deployment.Spec.Replicas = 2 },
		"command": func(v *Target) { v.Deployment.Spec.Template.Spec.Containers[0].Command = []string{"other"} },
		"volume": func(v *Target) {
			v.Deployment.Spec.Template.Spec.Volumes = []corev1.Volume{{Name: "other", VolumeSource: corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: "other"}}}}
		},
		"mount": func(v *Target) {
			v.Deployment.Spec.Template.Spec.Containers[0].VolumeMounts = []corev1.VolumeMount{{Name: "other", MountPath: "/data"}}
		},
		"security": func(v *Target) {
			x := true
			v.Deployment.Spec.Template.Spec.Containers[0].SecurityContext = &corev1.SecurityContext{Privileged: &x}
		},
		"service account": func(v *Target) { v.Deployment.Spec.Template.Spec.ServiceAccountName = "other" },
	} {
		t.Run(name, func(t *testing.T) {
			source, in := installationFixture()
			source.Hash[0] = 1
			target := source
			target.Deployment = source.Deployment.DeepCopy()
			target.Version = "1.0.1"
			target.Hash[0] = 2
			mutate(&target)
			client := fake.NewSimpleClientset()
			if _, err := NewManagementReplacement(&Kubernetes{client}, source, target, in); !errors.Is(err, ErrManagementReplacement) {
				t.Fatal(err)
			}
			if len(client.Actions()) != 0 {
				t.Fatal("incompatible target caused API calls")
			}
		})
	}
}

func TestManagementReplacementCaptureRequiresStableReadySource(t *testing.T) {
	for name, mutate := range map[string]func(*appsv1.Deployment){
		"missing uid":              func(d *appsv1.Deployment) { d.UID = "" },
		"missing resource version": func(d *appsv1.Deployment) { d.ResourceVersion = "" },
		"unready":                  func(d *appsv1.Deployment) { d.Status.ReadyReplicas = 0 },
		"old status":               func(d *appsv1.Deployment) { d.Generation++ },
		"extra old replica":        func(d *appsv1.Deployment) { d.Status.Replicas = 2 },
	} {
		t.Run(name, func(t *testing.T) {
			r, client, c := managementFixture(t)
			d := c.Deployment.DeepCopy()
			mutate(d)
			if err := client.Tracker().Update(schema.GroupVersionResource{Group: "apps", Version: "v1", Resource: "deployments"}, d, d.Namespace); err != nil {
				t.Fatal(err)
			}
			if _, err := r.Capture(context.Background()); !errors.Is(err, ErrManagementReplacement) {
				t.Fatal(err)
			}
			if len(managementUpdates(t, client)) != 0 {
				t.Fatal("capture mutated resources")
			}
		})
	}
}
