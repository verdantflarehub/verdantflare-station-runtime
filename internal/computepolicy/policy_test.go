package computepolicy

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
)

func TestPolicyOnlyRemovesCPUAndMemory(t *testing.T) {
	t.Setenv(Environment, Unrestricted)
	r := corev1.ResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("1"), corev1.ResourceMemory: resource.MustParse("1Gi"), "nvidia.com/gpu": resource.MustParse("1")}, Limits: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("2"), corev1.ResourceMemory: resource.MustParse("2Gi"), "nvidia.com/gpu": resource.MustParse("1")}}
	p := &corev1.PodSpec{Containers: []corev1.Container{{Name: "worker", Resources: r}}, InitContainers: []corev1.Container{{Name: "init", Resources: *r.DeepCopy()}}}
	Apply(p)
	for _, group := range [][]corev1.Container{p.Containers, p.InitContainers} {
		for _, c := range group {
			if len(c.Resources.Requests) != 1 || len(c.Resources.Limits) != 1 {
				t.Fatal("GPU or compute declarations changed incorrectly")
			}
			q := c.Resources.Requests["nvidia.com/gpu"]
			if q.String() != "1" {
				t.Fatal("GPU request changed")
			}
		}
	}
}

func TestPolicyRejectsUnknownConfiguration(t *testing.T) {
	t.Setenv(Environment, "other")
	if Valid() || Enabled() {
		t.Fatal("unknown policy accepted")
	}
}
