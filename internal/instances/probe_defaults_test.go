package instances

import (
	"github.com/google/uuid"
	"k8s.io/apimachinery/pkg/types"
	"testing"
)

func TestAPIServerProbeDefaultsPreserveIdentity(t *testing.T) {
	in, p, b, _ := helperFixture(t)
	helper, _, err := helperObjects(in, p, b)
	if err != nil {
		t.Fatal(err)
	}
	worker, _, err := workerObjects(in, p, b, uuid.NewString(), WorkerSource{SourceRevisionID: uuid.NewString(), Empty: true})
	if err != nil {
		t.Fatal(err)
	}
	for _, kind := range []string{"preparer", "worker"} {
		t.Run(kind, func(t *testing.T) {
			desired := helper
			matches := helperPodMatches
			if kind == "worker" {
				desired = worker
				matches = workerPodMatches
			}
			actual := desired.DeepCopy()
			actual.UID = types.UID(uuid.NewString())
			c := &actual.Spec.Containers[0]
			c.ReadinessProbe.SuccessThreshold = 1
			if kind == "preparer" {
				c.ReadinessProbe.FailureThreshold = 3
			} else {
				c.StartupProbe.SuccessThreshold = 1
				c.LivenessProbe.SuccessThreshold = 1
			}
			if !matches(actual, desired, "") {
				t.Fatal("server defaults rejected")
			}
			if desired.Spec.Containers[0].ReadinessProbe.SuccessThreshold != 0 {
				t.Fatal("template mutated")
			}
			c.ReadinessProbe.SuccessThreshold = 2
			if matches(actual, desired, "") {
				t.Fatal("changed readiness accepted")
			}
		})
	}
}
