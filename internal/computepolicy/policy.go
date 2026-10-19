// Package computepolicy applies a versioned station deployment policy without
// rewriting registered releases, durable request hashes or storage identities.
package computepolicy

import (
	"os"

	corev1 "k8s.io/api/core/v1"
)

const Environment = "STATION_CPU_MEMORY_POLICY"
const Unrestricted = "unrestricted-v1"

func Valid() bool   { return os.Getenv(Environment) == "" || Enabled() }
func Enabled() bool { return os.Getenv(Environment) == Unrestricted }

func Declared(spec *corev1.PodSpec) bool {
	declared := func(r corev1.ResourceRequirements) bool {
		for _, values := range []corev1.ResourceList{r.Requests, r.Limits} {
			for _, name := range []corev1.ResourceName{corev1.ResourceCPU, corev1.ResourceMemory} {
				if _, ok := values[name]; ok {
					return true
				}
			}
		}
		return false
	}
	for _, group := range [][]corev1.Container{spec.Containers, spec.InitContainers} {
		for _, c := range group {
			if declared(c.Resources) {
				return true
			}
		}
	}
	for _, c := range spec.EphemeralContainers {
		if declared(c.Resources) {
			return true
		}
	}
	return spec.Resources != nil && declared(*spec.Resources)
}

func Apply(spec *corev1.PodSpec) {
	if !Enabled() {
		return
	}
	strip := func(r *corev1.ResourceRequirements) {
		delete(r.Requests, corev1.ResourceCPU)
		delete(r.Requests, corev1.ResourceMemory)
		delete(r.Limits, corev1.ResourceCPU)
		delete(r.Limits, corev1.ResourceMemory)
		if len(r.Requests) == 0 {
			r.Requests = nil
		}
		if len(r.Limits) == 0 {
			r.Limits = nil
		}
	}
	for _, group := range [][]corev1.Container{spec.Containers, spec.InitContainers} {
		for i := range group {
			strip(&group[i].Resources)
		}
	}
	for i := range spec.EphemeralContainers {
		strip(&spec.EphemeralContainers[i].Resources)
	}
	if spec.Resources != nil {
		strip(spec.Resources)
		if len(spec.Resources.Requests) == 0 && len(spec.Resources.Limits) == 0 && len(spec.Resources.Claims) == 0 {
			spec.Resources = nil
		}
	}
}
