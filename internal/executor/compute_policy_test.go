package executor

import (
	"testing"

	"context"
	"github.com/verdantflarehub/verdantflare-station-runtime/internal/computepolicy"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

func TestComputePolicyPreservesRegisteredTemplateAndRejectsDrift(t *testing.T) {
	t.Setenv(computepolicy.Environment, computepolicy.Unrestricted)
	want := target().Deployment
	want.Spec.Template.Spec.Containers[0].Resources = corev1.ResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("1"), corev1.ResourceMemory: resource.MustParse("1Gi"), "nvidia.com/gpu": resource.MustParse("1")}}
	live := want.DeepCopy()
	computepolicy.Apply(&live.Spec.Template.Spec)
	if !preinstalledDeploymentCompatible(want, live) {
		t.Fatal("authorized policy rejected")
	}
	if len(want.Spec.Template.Spec.Containers[0].Resources.Requests) != 3 {
		t.Fatal("registered template mutated")
	}
	for _, mutate := range []func(*corev1.Container){func(c *corev1.Container) { c.Image = "other:v1.0.0" }, func(c *corev1.Container) { c.Resources.Requests["nvidia.com/gpu"] = resource.MustParse("2") }, func(c *corev1.Container) { c.Resources.Requests[corev1.ResourceCPU] = resource.MustParse("3") }} {
		bad := live.DeepCopy()
		mutate(&bad.Spec.Template.Spec.Containers[0])
		if preinstalledDeploymentCompatible(want, bad) {
			t.Fatal("policy allowed unrelated drift")
		}
	}
	t.Setenv(computepolicy.Environment, "")
	if preinstalledDeploymentCompatible(want, live) {
		t.Fatal("policy applied outside enabled station")
	}
}

func TestComputePolicyInstallReplaysOriginalHash(t *testing.T) {
	t.Setenv(computepolicy.Environment, computepolicy.Unrestricted)
	app := target()
	app.Deployment.Spec.Template.Spec.Containers[0].Resources = corev1.ResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("1"), corev1.ResourceMemory: resource.MustParse("1Gi")}}
	c := fake.NewClientset()
	k := Kubernetes{Client: c}
	ctx := context.Background()
	if _, err := k.Step(ctx, app, "install", "operation", "station", "org"); err != nil {
		t.Fatal(err)
	}
	live, err := c.AppsV1().Deployments(app.Deployment.Namespace).Get(ctx, app.Deployment.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(live.Spec.Template.Spec.Containers[0].Resources.Requests) != 0 || !owned(live.Annotations, app, "station", "org") {
		t.Fatal("policy lost ownership or retained CPU/memory")
	}
	if !preinstalledDeploymentCompatible(app.Deployment, live) {
		t.Fatal("policy workload cannot be inspected")
	}
}
