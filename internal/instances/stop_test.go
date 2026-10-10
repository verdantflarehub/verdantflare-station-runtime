package instances

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/google/uuid"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"
)

type testDrainObserver struct {
	calls int
	err   error
}

func (o *testDrainObserver) Seal(_ context.Context, in StopRequest, worker WorkerObservation, token string) error {
	o.calls++
	if !in.valid() || worker.PodUID == "" || len(token) < 32 {
		return ErrBinding
	}
	return o.err
}

func stopFixture(t *testing.T) (*Service, StopRequest, StartRequest, *fake.Clientset, *testDrainObserver) {
	t.Helper()
	s, _, start, client := accessFixture(t)
	in := StopRequest{OperationID: uuid.NewString(), StartOperationID: start.OperationID, StationID: start.StationID,
		OrganizationID: start.OrganizationID, UserID: start.UserID, ProjectID: start.ProjectID, InstanceID: start.InstanceID,
		Generation: strings.Repeat("a", 32), SavedRevisionID: uuid.NewString(), CommitID: uuid.NewString(),
		StoreID: uuid.NewString(), ArtifactID: uuid.NewString(), VersionID: uuid.NewString(),
		AssetID: "checkpoints/checkpoint-4-" + strings.Repeat("b", 32) + ".blend", SHA256: strings.Repeat("b", 64), Size: 123, SceneVersion: 4}
	observer := &testDrainObserver{}
	s.DrainObserver = observer
	client.ClearActions()
	return s, in, start, client, observer
}

func TestStopPersistsAcceptanceAndSealBeforeDeletionAndSupportsRestart(t *testing.T) {
	s, in, start, client, observer := stopFixture(t)
	ctx := context.Background()
	poll := func(phase string) StopProgress {
		t.Helper()
		s = &Service{Pool: s.Pool, Client: client, StationID: s.StationID, Profiles: s.Profiles, DrainObserver: observer, WorkerObserver: s.WorkerObserver}
		out, err := s.Stop(ctx, in)
		if err != nil || out.Phase != phase {
			t.Fatalf("%s: %+v %v", phase, out, err)
		}
		return out
	}
	poll("accepted")
	if len(client.Actions()) != 0 || observer.calls != 0 {
		t.Fatal("side effect before acceptance")
	}
	observer.err = errors.New("unverified freeze")
	if _, err := s.Stop(ctx, in); err == nil {
		t.Fatal("unverified freeze accepted")
	}
	for _, a := range client.Actions() {
		if a.GetVerb() == "delete" {
			t.Fatal("deleted before seal")
		}
	}
	observer.err = nil
	poll("removing")
	for _, a := range client.Actions() {
		if a.GetVerb() == "delete" {
			t.Fatal("deleted before seal commit")
		}
	}
	client.PrependReactor("delete", "*", func(action ktesting.Action) (bool, runtime.Object, error) {
		var persisted string
		if err := s.Pool.QueryRow(ctx, `SELECT state->>'sealed' FROM station.runtime_instance_executions WHERE operation_id=$1`, in.OperationID).Scan(&persisted); err != nil || persisted != "true" {
			t.Error("delete lacks committed seal", err)
		}
		options := action.(ktesting.DeleteAction).GetDeleteOptions()
		if options.Preconditions == nil || options.Preconditions.UID == nil || *options.Preconditions.UID == "" || options.Preconditions.ResourceVersion == nil {
			t.Error("missing deletion identity preconditions")
		}
		if action.GetResource().Resource != "pods" && action.GetResource().Resource != "services" {
			t.Error("unexpected deletion")
		}
		return false, nil, nil
	})
	poll("removing")
	out := poll("stopped")
	if out.Status != "succeeded" || out.Source.SourceRevisionID != start.SourceRevisionID || out.Source.SHA256 != in.SHA256 || out.Source.Empty || out.SavedRevisionID != in.SavedRevisionID {
		t.Fatal("invalid restart source", out)
	}
	client.ClearActions()
	poll("stopped")
	if len(client.Actions()) != 0 {
		t.Fatal("completed replay caused Kubernetes effects")
	}
	var ledger string
	if err := s.Pool.QueryRow(ctx, `SELECT input::text||state::text FROM station.runtime_instance_executions WHERE operation_id=$1`, in.OperationID).Scan(&ledger); err != nil {
		t.Fatal(err)
	}
	secret, err := client.CoreV1().Secrets(s.Profiles[start.ProfileID].Namespace).Get(ctx, instanceName(start.identity()), metav1.GetOptions{})
	if err != nil {
		t.Fatal("restart credential removed", err)
	}
	for _, v := range secret.Data {
		if strings.Contains(ledger, string(v)) {
			t.Fatal("credential persisted")
		}
	}
	restart := start
	restart.OperationID = uuid.NewString()
	restart.Empty = false
	restart.SHA256 = in.SHA256
	restart.Size = in.Size
	if progress, err := s.Start(ctx, restart); err != nil || progress.Phase != "accepted" {
		t.Fatal("saved workspace cannot restart", progress, err)
	}
}

func TestStopRejectsChangedRequestGenerationAndReplacedResources(t *testing.T) {
	for _, kind := range []string{"generation", "project", "pod", "service", "secret", "claim", "duplicate"} {
		t.Run(kind, func(t *testing.T) {
			s, in, start, client, observer := stopFixture(t)
			ctx := context.Background()
			ns := s.Profiles[start.ProfileID].Namespace
			name := "blender-run-" + strings.ReplaceAll(start.OperationID, "-", "")
			if kind == "generation" {
				in.Generation = strings.Repeat("c", 32)
			}
			if kind == "project" {
				in.ProjectID = uuid.NewString()
			}
			_, err := s.Stop(ctx, in)
			if kind == "generation" || kind == "project" {
				if err == nil {
					t.Fatal("foreign execution accepted")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			switch kind {
			case "pod":
				p, _ := client.CoreV1().Pods(ns).Get(ctx, name, metav1.GetOptions{})
				p.UID = types.UID(uuid.NewString())
				_, _ = client.CoreV1().Pods(ns).Update(ctx, p, metav1.UpdateOptions{})
			case "service":
				p, _ := client.CoreV1().Services(ns).Get(ctx, name, metav1.GetOptions{})
				p.UID = types.UID(uuid.NewString())
				_, _ = client.CoreV1().Services(ns).Update(ctx, p, metav1.UpdateOptions{})
			case "secret":
				p, _ := client.CoreV1().Secrets(ns).Get(ctx, instanceName(start.identity()), metav1.GetOptions{})
				p.UID = types.UID(uuid.NewString())
				_, _ = client.CoreV1().Secrets(ns).Update(ctx, p, metav1.UpdateOptions{})
			case "claim":
				p, _ := client.CoreV1().PersistentVolumeClaims(ns).Get(ctx, s.Profiles[start.ProfileID].Claims[0], metav1.GetOptions{})
				p.Annotations[instanceKey] = uuid.NewString()
				_, _ = client.CoreV1().PersistentVolumeClaims(ns).Update(ctx, p, metav1.UpdateOptions{})
			case "duplicate":
				in.SavedRevisionID = uuid.NewString()
			}
			if _, err = s.Stop(ctx, in); err == nil {
				t.Fatal("changed stop accepted")
			}
			if observer.calls != 0 {
				t.Fatal("contacted replaced worker")
			}
			for _, a := range client.Actions() {
				if a.GetVerb() == "delete" {
					t.Fatal("deleted mismatched resource")
				}
			}
		})
	}
}

func TestStopWaitsForActualExitAndHealthyNodeAfterLostDeleteResponse(t *testing.T) {
	s, in, start, client, _ := stopFixture(t)
	ctx := context.Background()
	ns := s.Profiles[start.ProfileID].Namespace
	for range 2 {
		if _, err := s.Stop(ctx, in); err != nil {
			t.Fatal(err)
		}
	}
	lost := true
	hold := true
	client.PrependReactor("delete", "pods", func(action ktesting.Action) (bool, runtime.Object, error) {
		if hold {
			return true, nil, nil
		} // API accepted deletion, process still terminating.
		if lost {
			lost = false
			_ = client.Tracker().Delete(corev1.SchemeGroupVersion.WithResource("pods"), ns, action.(ktesting.DeleteAction).GetName())
			return true, nil, errors.New("response lost")
		}
		return false, nil, nil
	})
	out, err := s.Stop(ctx, in)
	if err != nil || out.Status != "running" {
		t.Fatal("delete receipt treated as exit", out, err)
	}
	hold = false
	if _, err = s.Stop(ctx, in); err == nil {
		t.Fatal("lost deletion response not surfaced")
	}
	node, _ := client.CoreV1().Nodes().Get(ctx, s.Profiles[start.ProfileID].NodeName, metav1.GetOptions{})
	node.Status.Conditions = []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionUnknown}}
	_, _ = client.CoreV1().Nodes().UpdateStatus(ctx, node, metav1.UpdateOptions{})
	if out, err = s.Stop(ctx, in); !errors.Is(err, ErrComputeUnknown) || out.Status == "succeeded" {
		t.Fatal("unreachable node treated as released", out, err)
	}
	node.Status.Conditions[0].Status = corev1.ConditionTrue
	_, _ = client.CoreV1().Nodes().UpdateStatus(ctx, node, metav1.UpdateOptions{})
	if out, err = s.Stop(ctx, in); err != nil || out.Status != "succeeded" {
		t.Fatal(out, err)
	}
}

func TestStopRequestStrictDecoding(t *testing.T) {
	raw, _ := json.Marshal(StopRequest{})
	var out StopRequest
	if decodeRequest(raw, &out) != nil {
		t.Fatal("valid shape rejected")
	}
	for _, body := range []string{strings.TrimSuffix(string(raw), "}") + `,"size":1}`, strings.Replace(string(raw), `"size":0`, `"size":null`, 1), strings.TrimSuffix(string(raw), "}") + `,"worker_token":"x"}`, strings.Replace(string(raw), `,"size":0`, "", 1)} {
		if decodeRequest([]byte(body), &out) == nil {
			t.Fatal("ambiguous stop decoded")
		}
	}
}
