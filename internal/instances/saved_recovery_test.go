package instances

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/google/uuid"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type savedCopyObserver struct {
	source WorkerSource
	calls  int
}

func (o *savedCopyObserver) ObserveWorkingCopy(_ context.Context, c CreateRequest, h HelperObservation, token string, max int64) (*WorkerSource, error) {
	o.calls++
	if !h.Ready || h.PodUID == "" || len(token) < 32 || c.SourceRevisionID != o.source.SourceRevisionID || !o.source.valid(max) {
		return nil, ErrBinding
	}
	result := o.source
	return &result, nil
}

func TestRecoveryReaderCannotWriteWorkspace(t *testing.T) {
	in, p, b, _ := helperFixture(t)
	pod, svc, err := helperObjects(in, p, b, true)
	if err != nil {
		t.Fatal(err)
	}
	c := pod.Spec.Containers[0]
	if !pod.Spec.Volumes[0].PersistentVolumeClaim.ReadOnly || !c.VolumeMounts[0].ReadOnly || c.Command[0] != "python3" || !strings.HasSuffix(pod.Name, "-recover") || svc.Name != pod.Name {
		t.Fatal("reader permits writing or executes desktop")
	}
	if _, ok := c.Resources.Limits["nvidia.com/gpu"]; ok {
		t.Fatal("reader reserves GPU")
	}
	changed := pod.DeepCopy()
	changed.Spec.Containers[0].VolumeMounts[0].ReadOnly = false
	changed.UID = "test-reader"
	if helperPodMatches(changed, pod, string(changed.UID)) {
		t.Fatal("writable reader accepted")
	}
}

func TestSavedWorkingCopyRecoveryKeepsOperationAndOriginalSource(t *testing.T) {
	testSavedWorkingCopyLifecycle(t, false)
}
func TestSavedWorkingCopyStopAndRestart(t *testing.T) { testSavedWorkingCopyLifecycle(t, true) }
func testSavedWorkingCopyLifecycle(t *testing.T, stopAfterRecovery bool) {
	s, access, start, client := accessFixture(t)
	ctx := context.Background()
	p := s.Profiles[start.ProfileID]
	oldName := "blender-run-" + strings.ReplaceAll(start.OperationID, "-", "")
	old, err := client.CoreV1().Pods(p.Namespace).Get(ctx, oldName, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	old.Status.Phase = corev1.PodSucceeded
	client.CoreV1().Pods(p.Namespace).Update(ctx, old, metav1.UpdateOptions{})
	next := start
	next.OperationID = uuid.NewString()
	saved := &savedCopyObserver{source: WorkerSource{SourceRevisionID: start.SourceRevisionID, SHA256: strings.Repeat("c", 64), Size: 128}}
	s.WorkingCopyObserver = saved
	failedUID := ""
	completed := false
	for i := 0; i < 24; i++ {
		out, err := s.Start(ctx, next)
		if err != nil {
			t.Fatalf("step %d: %v", i, err)
		}
		if out.OperationID != next.OperationID {
			t.Fatal("operation changed")
		}
		if out.SourceRecovery != nil {
			if out.SourceRecovery.PreviousWorker.PodUID != failedUID {
				t.Fatal("lost terminal worker evidence")
			}
		}
		if out.Phase == "recovering" {
			hname := instanceName(next.identity()) + "-prepare-recover"
			h, e := client.CoreV1().Pods(p.Namespace).Get(ctx, hname, metav1.GetOptions{})
			if e == nil {
				if !h.Spec.Volumes[0].PersistentVolumeClaim.ReadOnly || !h.Spec.Containers[0].VolumeMounts[0].ReadOnly {
					t.Fatal("writable helper")
				}
				h.Spec.NodeName = p.NodeName
				h.Status.Phase = corev1.PodRunning
				h.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}
				client.CoreV1().Pods(p.Namespace).Update(ctx, h, metav1.UpdateOptions{})
			}
		}
		if out.Worker != nil {
			w, e := client.CoreV1().Pods(p.Namespace).Get(ctx, out.Worker.PodName, metav1.GetOptions{})
			if e != nil {
				t.Fatal(e)
			}
			w.Spec.NodeName = p.NodeName
			if out.SourceRecovery == nil {
				failedUID = out.Worker.PodUID
				w.Status.Phase = corev1.PodSucceeded
			} else {
				if out.Worker.PodUID == failedUID {
					t.Fatal("terminal UID reused")
				}
				w.Status.Phase = corev1.PodRunning
				w.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}
			}
			client.CoreV1().Pods(p.Namespace).Update(ctx, w, metav1.UpdateOptions{})
		}
		if out.Status == "succeeded" {
			if out.Proof == nil || out.Proof.Startup.WorkerSource != saved.source || saved.calls != 1 {
				t.Fatal("saved bytes not proved")
			}
			completed = true
			break
		}
	}
	if !completed {
		t.Fatal("recovery did not finish")
	}
	var raw, stateRaw []byte
	if err = s.Pool.QueryRow(ctx, `SELECT input,state FROM station.runtime_instance_executions WHERE operation_id=$1`, next.OperationID).Scan(&raw, &stateRaw); err != nil {
		t.Fatal(err)
	}
	var persisted StartRequest
	json.Unmarshal(raw, &persisted)
	if persisted != next {
		t.Fatal("immutable start input rewritten")
	}
	var state startState
	json.Unmarshal(stateRaw, &state)
	if state.SavedRecovery == nil || state.SavedRecovery.Stage != "ready" {
		t.Fatal("recovery provenance absent")
	}
	access.StartOperationID = next.OperationID
	if _, err = s.Access(ctx, access); err != nil {
		t.Fatal("recovered access", err)
	}
	wrong := next
	wrong.SHA256 = saved.source.SHA256
	wrong.Size = saved.source.Size
	wrong.Empty = false
	if _, err = s.Start(ctx, wrong); err == nil {
		t.Fatal("caller replaced immutable source")
	}

	if stopAfterRecovery {
		s.DrainObserver = &testDrainObserver{}
		stop := StopRequest{OperationID: uuid.NewString(), StartOperationID: next.OperationID, StationID: next.StationID, OrganizationID: next.OrganizationID, UserID: next.UserID, ProjectID: next.ProjectID, InstanceID: next.InstanceID, Generation: state.Proof.Generation, SavedRevisionID: uuid.NewString(), CommitID: uuid.NewString(), StoreID: uuid.NewString(), ArtifactID: uuid.NewString(), VersionID: uuid.NewString(), AssetID: "checkpoints/checkpoint-4-" + strings.Repeat("b", 32) + ".blend", SHA256: strings.Repeat("d", 64), Size: 164, SceneVersion: 4}
		var stopped StopProgress
		for i := 0; i < 4; i++ {
			stopped, err = s.Stop(ctx, stop)
			if err != nil {
				t.Fatal("stop after source recovery", err)
			}
		}
		if stopped.Status != "succeeded" || stopped.Phase != "stopped" {
			t.Fatal("stop incomplete", stopped)
		}
		restart := next
		restart.OperationID = uuid.NewString()
		restart.SHA256 = stopped.Source.SHA256
		restart.Size = stopped.Source.Size
		restart.Empty = stopped.Source.Empty
		for i := 0; i < 2; i++ {
			if _, err = s.Start(ctx, restart); err != nil {
				t.Fatal("start after recovered stop", err)
			}
		}
		return
	}
	// A later recovery uses the effective source, while retaining the original
	// immutable input and the earlier source observation chain.
	current, err := client.CoreV1().Pods(p.Namespace).Get(ctx, state.Worker.PodName, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	current.Status.Phase = corev1.PodSucceeded
	client.CoreV1().Pods(p.Namespace).Update(ctx, current, metav1.UpdateOptions{})
	newer := wrong
	newer.OperationID = uuid.NewString()
	if _, err = s.Start(ctx, newer); err != nil {
		t.Fatal("subsequent recovery lost source", err)
	}

	for i := 0; i < 8; i++ {
		out, e := s.Start(ctx, newer)
		if e != nil {
			t.Fatal(e)
		}
		if out.Worker != nil {
			worker, e := client.CoreV1().Pods(p.Namespace).Get(ctx, out.Worker.PodName, metav1.GetOptions{})
			if e != nil {
				t.Fatal(e)
			}
			worker.Spec.NodeName = p.NodeName
			worker.Status.Phase = corev1.PodRunning
			worker.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}
			client.CoreV1().Pods(p.Namespace).Update(ctx, worker, metav1.UpdateOptions{})
		}
		if out.Status == "succeeded" {
			break
		}
		if i == 7 {
			t.Fatal("subsequent recovery did not complete")
		}
	}

}
