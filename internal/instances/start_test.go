package instances

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"
)

type testWorkerObserver struct {
	ready bool
	calls int
}

func (o *testWorkerObserver) ObserveWorker(_ context.Context, in Identity, start string, source WorkerSource, worker WorkerObservation, token string) (*RunningProof, error) {
	o.calls++
	if !o.ready {
		return nil, nil
	}
	if len(token) < 32 {
		return nil, ErrBinding
	}
	return &RunningProof{Startup: StartupProof{WorkerSource: source, StartOperationID: start, CreateOperationID: in.CreateOperationID, InstanceID: in.InstanceID, ProjectID: in.ProjectID, PodUID: worker.PodUID}, Generation: strings.Repeat("a", 32), GPUUUID: "GPU-" + uuid.NewString()}, nil
}

func startFixture(t *testing.T) (*Service, StartRequest, *fake.Clientset, *testWorkerObserver) {
	t.Helper()
	s, create, client, observer := createFixture(t)
	ctx := context.Background()
	for range 4 {
		if _, err := s.Create(ctx, create); err != nil {
			t.Fatal(err)
		}
	}
	readyHelper(t, client, create, s.Profiles[create.ProfileID])
	observer.proof = &PreparationProof{PrepareID: create.OperationID, InstanceID: create.InstanceID, ProjectID: create.ProjectID, SourceRevisionID: create.SourceRevisionID, State: "prepared", PreparedAt: time.Now().UTC().Format(time.RFC3339Nano)}
	for range 3 {
		if _, err := s.Create(ctx, create); err != nil {
			t.Fatal(err)
		}
	}
	node := computeNode(s.Profiles[create.ProfileID].NodeName)
	if _, err := client.CoreV1().Nodes().Create(ctx, node, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	o := &testWorkerObserver{}
	s.WorkerObserver = o
	in := StartRequest{OperationID: uuid.NewString(), CreateOperationID: create.OperationID, StationID: create.StationID, OrganizationID: create.OrganizationID, UserID: create.UserID, ProjectID: create.ProjectID, InstanceID: create.InstanceID, ProfileID: create.ProfileID, SourceRevisionID: create.SourceRevisionID, Empty: true}
	return s, in, client, o
}

func TestStartPersistsBeforeEffectsAndNeedsAuthenticatedLoad(t *testing.T) {
	s, in, client, observer := startFixture(t)
	ctx := context.Background()
	before := len(client.Actions())
	out, err := s.Start(ctx, in)
	if err != nil || out.Phase != "accepted" {
		t.Fatal(out, err)
	}
	if len(client.Actions()) != before {
		t.Fatal("Kubernetes effect before start committed")
	}
	// Drop all in-memory coordinator state at every poll.
	call := func(phase string) StartProgress {
		t.Helper()
		s = &Service{Pool: s.Pool, Client: client, StationID: s.StationID, Profiles: s.Profiles, WorkerObserver: observer}
		v, e := s.Start(ctx, in)
		if e != nil || v.Phase != phase {
			t.Fatalf("%s: %+v %v", phase, v, e)
		}
		return v
	}
	out = call("launching")
	if out.Worker == nil || out.Worker.PodUID == "" || observer.calls != 0 {
		t.Fatal(out)
	}
	pod, _ := client.CoreV1().Pods(out.Worker.Namespace).Get(ctx, out.Worker.PodName, metav1.GetOptions{})
	pod.Spec.NodeName = s.Profiles[in.ProfileID].NodeName
	pod.Status.Phase = corev1.PodRunning
	pod.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}
	_, _ = client.CoreV1().Pods(pod.Namespace).Update(ctx, pod, metav1.UpdateOptions{})
	out = call("loading")
	if out.Status != "running" || out.Proof != nil {
		t.Fatal("PodReady treated as loaded", out)
	}
	observer.ready = true
	out = call("running")
	if out.Status != "succeeded" || out.Proof == nil {
		t.Fatal(out)
	}
	before = len(client.Actions())
	call("running")
	if len(client.Actions()) != before {
		t.Fatal("completed replay performed Kube effects")
	}
	var persisted string
	if err = s.Pool.QueryRow(ctx, `SELECT input::text||state::text FROM station.runtime_instance_executions WHERE operation_id=$1`, in.OperationID).Scan(&persisted); err != nil {
		t.Fatal(err)
	}
	secret, _ := client.CoreV1().Secrets(pod.Namespace).Get(ctx, instanceName(in.identity()), metav1.GetOptions{})
	for _, value := range secret.Data {
		if strings.Contains(persisted, string(value)) {
			t.Fatal("credential persisted")
		}
	}
	changed := in
	changed.OperationID = uuid.NewString()
	if _, err = s.Start(ctx, changed); !errors.Is(err, ErrConflict) {
		t.Fatal("second start without completed stop", err)
	}
	changed = in
	changed.UserID = uuid.NewString()
	if _, err = s.Start(ctx, changed); !errors.Is(err, ErrConflict) {
		t.Fatal("operation identity changed", err)
	}
}

func TestStartConcurrentAdmissionAndLostPodResponse(t *testing.T) {
	s, in, client, _ := startFixture(t)
	ctx := context.Background()
	var group sync.WaitGroup
	results := make(chan error, 8)
	for range 8 {
		group.Add(1)
		go func() { defer group.Done(); _, err := s.Start(ctx, in); results <- err }()
	}
	group.Wait()
	close(results)
	for err := range results {
		if err != nil && !errors.Is(err, ErrBusy) {
			t.Fatal(err)
		}
	}
	var count int
	if err := s.Pool.QueryRow(ctx, `SELECT count(*) FROM station.runtime_instance_executions WHERE action='start'`).Scan(&count); err != nil || count != 1 {
		t.Fatal(count, err)
	}
	// Use a fresh fixture so the loss happens on the first Pod create.
	s, in, client, _ = startFixture(t)
	if _, err := s.Start(ctx, in); err != nil {
		t.Fatal(err)
	}
	lose := true
	client.PrependReactor("create", "pods", func(a ktesting.Action) (bool, runtime.Object, error) {
		if !lose {
			return false, nil, nil
		}
		lose = false
		pod := a.(ktesting.CreateAction).GetObject().(*corev1.Pod).DeepCopy()
		pod.UID = "12345678-1234-4234-8234-123456789abc"
		pod.ResourceVersion = "1"
		if err := client.Tracker().Create(corev1.SchemeGroupVersion.WithResource("pods"), pod, pod.Namespace); err != nil {
			t.Fatal(err)
		}
		return true, nil, errors.New("response lost")
	})
	if _, err := s.Start(ctx, in); err == nil {
		t.Fatal("lost response treated as success")
	}
	out, err := s.Start(ctx, in)
	if err != nil || out.Worker == nil || out.Worker.PodUID != "12345678-1234-4234-8234-123456789abc" {
		t.Fatal("did not recover original pod", out, err)
	}
	var phase string
	var state []byte
	if err = s.Pool.QueryRow(ctx, `SELECT phase,state FROM station.runtime_instance_executions WHERE operation_id=$1`, in.OperationID).Scan(&phase, &state); err != nil {
		t.Fatal(err)
	}
	var stored startState
	if json.Unmarshal(state, &stored) != nil || stored.Worker.PodUID != out.Worker.PodUID || phase != "launching" {
		t.Fatal("resource identity not persisted")
	}
}

func TestStartRejectsDifferentSourceAndRetriesSameRequestAfterCapacityReturns(t *testing.T) {
	s, in, client, _ := startFixture(t)
	ctx := context.Background()
	bad := in
	bad.Empty = false
	bad.Size = 1
	bad.SHA256 = strings.Repeat("b", 64)
	if _, err := s.Start(ctx, bad); !errors.Is(err, ErrBinding) {
		t.Fatal("unapproved first startup source", err)
	}
	if _, err := s.Start(ctx, in); err != nil {
		t.Fatal(err)
	}
	video := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "video", Namespace: "video"}, Spec: corev1.PodSpec{NodeName: s.Profiles[in.ProfileID].NodeName, Containers: []corev1.Container{{Name: "video", Resources: workerResources()}}}, Status: corev1.PodStatus{Phase: corev1.PodRunning}}
	_, _ = client.CoreV1().Pods(video.Namespace).Create(ctx, video, metav1.CreateOptions{})
	before := len(client.Actions())
	_, err := s.Start(ctx, in)
	var execution *ExecutionError
	if !errors.As(err, &execution) || execution.Code != "COMPUTE_CAPACITY_UNAVAILABLE" {
		t.Fatal(err)
	}
	for _, a := range client.Actions()[before:] {
		if a.GetVerb() != "get" && a.GetVerb() != "list" {
			t.Fatal("capacity refusal caused effects")
		}
	}
	video.Status.Phase = corev1.PodSucceeded
	_, _ = client.CoreV1().Pods(video.Namespace).Update(ctx, video, metav1.UpdateOptions{})
	out, err := s.Start(ctx, in)
	if err != nil || out.Phase != "launching" {
		t.Fatal(out, err)
	}
}
