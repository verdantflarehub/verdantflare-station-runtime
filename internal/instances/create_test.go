package instances

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"
)

type testObserver struct {
	proof *PreparationProof
	err   error
	calls int
}

func (o *testObserver) Observe(context.Context, CreateRequest, HelperObservation, string) (*PreparationProof, error) {
	o.calls++
	return o.proof, o.err
}

func createFixture(t *testing.T) (*Service, CreateRequest, *fake.Clientset, *testObserver) {
	t.Helper()
	in, profile, pvc, pv := fixture()
	pool := workspaceDB(t, in)
	client := fake.NewClientset(pvc, pv)
	client.PrependReactor("create", "*", func(a ktesting.Action) (bool, runtime.Object, error) {
		obj := a.(ktesting.CreateAction).GetObject()
		if meta, ok := obj.(metav1.Object); ok {
			meta.SetUID(types.UID(uuid.NewString()))
			meta.SetResourceVersion("1")
		}
		return false, nil, nil
	})
	user := uuid.NewString()
	if _, err := pool.Exec(context.Background(), `INSERT INTO station.users(user_id,username,password_hash) VALUES($1,'test-manager','unused')`, user); err != nil {
		t.Fatal(err)
	}
	p := HelperProfile{StorageProfile: profile, Image: "registry.example.test/blender-worker:v0.1.7", MaxFileBytes: 1 << 30}
	observer := &testObserver{}
	service := &Service{Pool: pool, Client: client, StationID: in.StationID, Profiles: map[string]HelperProfile{profile.ID: p}, Observer: observer}
	request := CreateRequest{OperationID: in.CreateOperationID, StationID: in.StationID, OrganizationID: in.OrganizationID, UserID: user, InstanceID: in.InstanceID, ProjectID: in.ProjectID, ProfileID: p.ID, SourceRevisionID: uuid.NewString(), EmptySource: true}
	return service, request, client, observer
}

func readyHelper(t *testing.T, client *fake.Clientset, in CreateRequest, p HelperProfile) {
	t.Helper()
	pod, err := client.CoreV1().Pods(p.Namespace).Get(context.Background(), instanceName(in.identity())+"-prepare", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	pod.Spec.NodeName = p.NodeName
	pod.Status.Phase = corev1.PodRunning
	pod.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}
	if _, err = client.CoreV1().Pods(p.Namespace).Update(context.Background(), pod, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
}

func TestCreateDurableStagesPrivateAccessAndStoppedEvidence(t *testing.T) {
	service, in, client, observer := createFixture(t)
	ctx := context.Background()
	call := func(phase string) CreateProgress {
		t.Helper()
		// Rebuild the service for every stage; no in-memory execution state survives.
		service = &Service{Pool: service.Pool, Client: client, StationID: service.StationID, Profiles: service.Profiles, Observer: observer}
		out, err := service.Create(ctx, in)
		if err != nil || out.Phase != phase {
			t.Fatalf("phase %s: %+v %v", phase, out, err)
		}
		return out
	}
	call("accepted")
	if len(client.Actions()) != 0 {
		t.Fatal("effects before committed acceptance")
	}
	call("reserved")
	call("claimed")
	call("preparing")
	readyHelper(t, client, in, service.Profiles[in.ProfileID])
	access := call("awaiting_content")
	if access.Access == nil || len(access.Access.WorkerToken) < 32 {
		t.Fatal("missing scoped preparation access")
	}
	replay := call("awaiting_content")
	if *replay.Access != *access.Access {
		t.Fatal("credential or endpoint rotated on retry")
	}
	observer.proof = &PreparationProof{PrepareID: in.OperationID, InstanceID: in.InstanceID, ProjectID: in.ProjectID, SourceRevisionID: in.SourceRevisionID, State: "prepared", PreparedAt: time.Now().UTC().Format(time.RFC3339Nano)}
	if call("removing").Access != nil {
		t.Fatal("credential returned after preparation")
	}
	call("removing")
	result := call("stopped")
	if result.Status != "succeeded" || result.Workspace == nil {
		t.Fatal(result)
	}
	prior := len(client.Actions())
	call("stopped")
	if len(client.Actions()) != prior {
		t.Fatal("terminal replay performed Kubernetes effects")
	}
	var raw string
	if err := service.Pool.QueryRow(ctx, `SELECT input::text||state::text FROM station.runtime_instance_executions WHERE operation_id=$1`, in.OperationID).Scan(&raw); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(raw, access.Access.WorkerToken) || strings.Contains(raw, "worker_token") || strings.Contains(raw, "gui-password") {
		t.Fatal("credential persisted in execution JSON")
	}
	secret, err := client.CoreV1().Secrets(service.Profiles[in.ProfileID].Namespace).Get(ctx, instanceName(in.identity()), metav1.GetOptions{})
	if err != nil || secret.Immutable == nil || !*secret.Immutable {
		t.Fatal("immutable credential not retained", err)
	}
	changed := in
	changed.SourceRevisionID = uuid.NewString()
	if _, err = service.Create(ctx, changed); !errors.Is(err, ErrConflict) {
		t.Fatal("input rebound", err)
	}
	changed = in
	changed.OperationID = uuid.NewString()
	if _, err = service.Create(ctx, changed); !errors.Is(err, ErrConflict) {
		t.Fatal("duplicate instance accepted", err)
	}
}

func TestCreateUncertainPodCreationRecoversOriginalResources(t *testing.T) {
	service, in, client, _ := createFixture(t)
	ctx := context.Background()
	for range 3 {
		if _, err := service.Create(ctx, in); err != nil {
			t.Fatal(err)
		}
	}
	lose := true
	client.PrependReactor("create", "pods", func(a ktesting.Action) (bool, runtime.Object, error) {
		if !lose {
			return false, nil, nil
		}
		lose = false
		pod := a.(ktesting.CreateAction).GetObject().(*corev1.Pod)
		pod.UID = types.UID(uuid.NewString())
		pod.ResourceVersion = "1"
		if err := client.Tracker().Create(corev1.SchemeGroupVersion.WithResource("pods"), pod, pod.Namespace); err != nil {
			t.Fatal(err)
		}
		return true, nil, errors.New("response lost after commit")
	})
	if _, err := service.Create(ctx, in); err == nil {
		t.Fatal("uncertainty hidden")
	}
	p := service.Profiles[in.ProfileID]
	pod, _ := client.CoreV1().Pods(p.Namespace).Get(ctx, instanceName(in.identity())+"-prepare", metav1.GetOptions{})
	if _, err := service.Create(ctx, in); err != nil {
		t.Fatal(err)
	}
	var raw []byte
	if err := service.Pool.QueryRow(ctx, `SELECT state FROM station.runtime_instance_executions WHERE operation_id=$1`, in.OperationID).Scan(&raw); err != nil {
		t.Fatal(err)
	}
	var state createState
	if json.Unmarshal(raw, &state) != nil || state.Helper.PodUID != string(pod.UID) {
		t.Fatal("did not recover original UID")
	}
	// A subsequent failed observation must retain the already committed UID.
	if err := client.CoreV1().Pods(p.Namespace).Delete(ctx, pod.Name, metav1.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := service.Create(ctx, in); err == nil {
		t.Fatal("replaced missing observed pod")
	}
	var again []byte
	_ = service.Pool.QueryRow(ctx, `SELECT state FROM station.runtime_instance_executions WHERE operation_id=$1`, in.OperationID).Scan(&again)
	if string(again) != string(raw) {
		t.Fatal("uncertainty erased durable binding")
	}
}

func TestCreateDoesNotDeleteWithoutPreparationProof(t *testing.T) {
	service, in, client, observer := createFixture(t)
	ctx := context.Background()
	for range 4 {
		if _, err := service.Create(ctx, in); err != nil {
			t.Fatal(err)
		}
	}
	readyHelper(t, client, in, service.Profiles[in.ProfileID])
	observer.err = ErrBinding
	if _, err := service.Create(ctx, in); err == nil {
		t.Fatal("invalid preparation accepted")
	}
	for _, action := range client.Actions() {
		if action.GetVerb() == "delete" {
			t.Fatal("resources deleted without proof")
		}
	}
	p := service.Profiles[in.ProfileID]
	p.Hash[0]++
	service.Profiles[in.ProfileID] = p
	if _, err := service.Create(ctx, in); !errors.Is(err, ErrConflict) {
		t.Fatal("template drift ignored", err)
	}
}

func TestCreateHTTPAcceptsExactInternalContract(t *testing.T) {
	service, in, client, _ := createFixture(t)
	handler := Handler{Service: service, Token: "only-core-may-use-this-test-token"}
	raw, _ := json.Marshal(in)
	r := httptest.NewRequest("POST", "/internal/v1/instances/create", bytes.NewReader(raw))
	r.Header.Set("Authorization", "Bearer only-core-may-use-this-test-token")
	r.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, r)
	var result CreateProgress
	if w.Code != 200 || json.Unmarshal(w.Body.Bytes(), &result) != nil || result.Phase != "accepted" || result.Access != nil || len(client.Actions()) != 0 {
		t.Fatal("internal contract not durably accepted", w.Code)
	}
}
