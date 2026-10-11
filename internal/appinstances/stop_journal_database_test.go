package appinstances

import (
	"context"
	"errors"
	"net/http"
	"testing"

	"github.com/google/uuid"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

type stopperFunc func(context.Context, Run, Profile, Observation, string, string) (WorkerStopProof, error)

func (f stopperFunc) Seal(ctx context.Context, in Run, p Profile, o Observation, token, operation string) (WorkerStopProof, error) {
	return f(ctx, in, p, o, token, operation)
}

func runningStopFixture(t *testing.T) (*Journal, Run, Profile, *fake.Clientset, string) {
	t.Helper()
	j, in, p, c := journalFixture(t)
	ctx, boot := context.Background(), uuid.NewString()
	j.Observer = HTTPWorkerObserver{Client: &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		pod, e := c.CoreV1().Pods(p.Namespace).Get(ctx, runName(in), metav1.GetOptions{})
		if e != nil {
			t.Fatal(e)
		}
		return response(proofFor(in, p, Observation{PodUID: string(pod.UID)}, boot)), nil
	})}}
	if _, e := j.Start(ctx, in, p); e != nil {
		t.Fatal(e)
	}
	pod, _ := c.CoreV1().Pods(p.Namespace).Get(ctx, runName(in), metav1.GetOptions{})
	pod.Spec.NodeName = p.NodeName
	pod.Status.Phase = corev1.PodRunning
	pod.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}
	if _, e := c.CoreV1().Pods(p.Namespace).Update(ctx, pod, metav1.UpdateOptions{}); e != nil {
		t.Fatal(e)
	}
	if _, e := j.Start(ctx, in, p); e != nil {
		t.Fatal(e)
	}
	j.Stopper = stopperFunc(func(_ context.Context, run Run, profile Profile, observed Observation, _, operation string) (WorkerStopProof, error) {
		return stopProof(run, profile, observed, operation), nil
	})
	if _, e := c.CoreV1().Nodes().Create(ctx, &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: p.NodeName},
		Status: corev1.NodeStatus{Conditions: []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionTrue}}}}, metav1.CreateOptions{}); e != nil {
		t.Fatal(e)
	}
	c.ClearActions()
	return j, in, p, c, boot
}

func TestJournalStopPersistsRecoveryAndAllowsOnlyNextGeneration(t *testing.T) {
	j, in, p, c, boot := runningStopFixture(t)
	ctx, operation := context.Background(), id()
	original, e := j.Accept(ctx, in, p)
	if e != nil {
		t.Fatal(e)
	}
	for _, phase := range []string{"pod_delete_requested", "service_delete_requested", "stopped"} {
		s, e := j.Stop(ctx, in, p, operation, boot)
		if e != nil || s.Progress.Phase != phase {
			t.Fatal("stop did not resume", phase, e)
		}
		j = &Journal{Pool: j.Pool, Client: c, StationID: j.StationID, Authorize: j.Authorize, Observer: j.Observer, Stopper: j.Stopper}
	}
	stored, e := j.Accept(ctx, in, p)
	if e != nil || stored.Phase != "stopped" || stored.Volume != original.Volume || stored.Observation != original.Observation {
		t.Fatal("stop lost immutable runtime binding", e)
	}
	c.ClearActions()
	if _, e := j.Stop(ctx, in, p, operation, boot); e != nil {
		t.Fatal("completed stop replay failed", e)
	}
	assertNoEffects(t, c)
	next := in
	next.Generation, next.OperationID = in.Generation+1, id()
	accepted, e := j.Accept(ctx, next, p)
	if e != nil || accepted.Volume != original.Volume || accepted.Observation.SecretUID != original.Observation.SecretUID {
		t.Fatal("next generation lost retained workspace/credential", e)
	}
	if _, e := j.AdvanceResources(ctx, next, p); e != nil {
		t.Fatal("next generation cannot create its own resources", e)
	}
	if _, e := j.Stop(ctx, in, p, operation, boot); e != nil {
		t.Fatal("old completed operation cannot be read after next run", e)
	}
	if _, e := c.CoreV1().Pods(p.Namespace).Get(ctx, runName(next), metav1.GetOptions{}); e != nil {
		t.Fatal("old stop deleted new generation")
	}
}

func TestJournalStopUnknownBarrierRetainsOriginalOperationAndRejectsCompetitor(t *testing.T) {
	j, in, p, c, boot := runningStopFixture(t)
	ctx, operation := context.Background(), id()
	original := j.Stopper
	seen := 0
	j.Stopper = stopperFunc(func(ctx context.Context, r Run, profile Profile, o Observation, token, op string) (WorkerStopProof, error) {
		seen++
		if seen == 1 {
			return WorkerStopProof{}, ErrUnavailable
		}
		return original.Seal(ctx, r, profile, o, token, op)
	})
	if _, e := j.Stop(ctx, in, p, operation, boot); !errors.Is(e, ErrUnavailable) {
		t.Fatal("lost barrier response hidden", e)
	}
	assertNoEffects(t, c)
	if _, e := j.Access(ctx, in, p); !errors.Is(e, ErrNotReady) {
		t.Fatal("stop admission left instance open", e)
	}
	if _, e := j.Stop(ctx, in, p, id(), boot); !errors.Is(e, ErrConflict) {
		t.Fatal("new stop replaced uncertain operation", e)
	}
	if _, e := j.Stop(ctx, in, p, operation, uuid.NewString()); !errors.Is(e, ErrConflict) {
		t.Fatal("stop adopted new worker", e)
	}
	s, e := j.Stop(ctx, in, p, operation, boot)
	if e != nil || s.Progress.Phase != "pod_delete_requested" || seen != 2 {
		t.Fatal("original barrier could not resume", e)
	}
}

func TestJournalStopRevocationAndUncommittedProofPreventDeletion(t *testing.T) {
	j, in, p, c, boot := runningStopFixture(t)
	ctx, operation := context.Background(), id()
	authorize := j.Authorize
	j.Authorize = func(context.Context, Run, Profile) (int64, error) { return 0, ErrDenied }
	if _, e := j.Stop(ctx, in, p, operation, boot); !errors.Is(e, ErrDenied) {
		t.Fatal("revocation bypassed", e)
	}
	assertNoEffects(t, c)
	j.Authorize = authorize
	_, e := j.Pool.Exec(ctx, `CREATE FUNCTION station.reject_stop_checkpoint() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.phase='sealed' THEN RAISE EXCEPTION 'test proof checkpoint failure'; END IF; RETURN NEW; END $$;
		CREATE TRIGGER reject_stop_checkpoint BEFORE UPDATE ON station.app_instance_stops FOR EACH ROW EXECUTE FUNCTION station.reject_stop_checkpoint()`)
	if e != nil {
		t.Fatal(e)
	}
	if _, e := j.Stop(ctx, in, p, operation, boot); e == nil {
		t.Fatal("failed proof commit ignored")
	}
	assertNoEffects(t, c)
	if _, e := j.Pool.Exec(ctx, `DROP TRIGGER reject_stop_checkpoint ON station.app_instance_stops`); e != nil {
		t.Fatal(e)
	}
	if s, e := j.Stop(ctx, in, p, operation, boot); e != nil || s.Progress.Phase != "pod_delete_requested" {
		t.Fatal("proof retry failed", e)
	}
}
