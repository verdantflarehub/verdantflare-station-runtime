package appstate

import (
	"context"
	_ "embed"
	"encoding/json"
	"errors"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"
)

//go:embed testdata/collector.generated.json
var centralVectors []byte

func deployment(t *testing.T) *appsv1.Deployment {
	t.Helper()
	var vectors struct{ Resource ResourceObservation }
	if json.Unmarshal(centralVectors, &vectors) != nil {
		t.Fatal("invalid central vectors")
	}
	o := vectors.Resource
	return &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Namespace: o.Namespace, Name: o.Name, UID: types.UID(o.UID), ResourceVersion: o.ResourceVersion, Generation: o.Generation, Annotations: map[string]string{
		"station.verdantflare.com/station-id": o.StationID, "station.verdantflare.com/organization-id": o.OrganizationID, "station.verdantflare.com/installation-id": o.InstallationID, "station.verdantflare.com/app-id": o.AppID,
	}}, Spec: appsv1.DeploymentSpec{Replicas: &o.Desired}, Status: appsv1.DeploymentStatus{ObservedGeneration: o.ObservedGeneration, Replicas: o.Replicas, ReadyReplicas: o.Ready, UpdatedReplicas: o.Updated, AvailableReplicas: o.Available}}
}

type recordedFrame struct {
	Kind, SyncID, Namespace, Name, UID string
	Objects                            []ResourceObservation
	Object                             ResourceObservation
	TotalCount                         int
}
type recordingCore struct {
	reports      chan recordedFrame
	fail         error
	mu           sync.Mutex
	acquisitions int
}
type recordingStream struct {
	ctx    context.Context
	core   *recordingCore
	events chan StreamEvent
}

func (c *recordingCore) Open(ctx context.Context) (Stream, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.acquisitions++
	if c.fail != nil {
		return nil, c.fail
	}
	return &recordingStream{ctx, c, make(chan StreamEvent, 1)}, nil
}
func (s *recordingStream) Send(frame any) error {
	raw, _ := json.Marshal(frame)
	var v recordedFrame
	if json.Unmarshal(raw, &v) != nil {
		return ErrInvalid
	}
	select {
	case s.core.reports <- v:
		return nil
	case <-s.ctx.Done():
		return s.ctx.Err()
	}
}
func (s *recordingStream) Events() <-chan StreamEvent { return s.events }
func (s *recordingStream) Close()                     {}
func receive(t *testing.T, reports <-chan recordedFrame) recordedFrame {
	t.Helper()
	select {
	case b := <-reports:
		return b
	case <-time.After(3 * time.Second):
		t.Fatal("observer did not report")
		return recordedFrame{}
	}
}

func TestStateObserverListWatchAndCancellation(t *testing.T) {
	d := deployment(t)
	client := fake.NewSimpleClientset()
	stream := watch.NewRaceFreeFake()
	watchStarted := make(chan struct{})
	var once sync.Once
	client.PrependReactor("list", "deployments", func(a ktesting.Action) (bool, kruntime.Object, error) {
		return true, &appsv1.DeploymentList{ListMeta: metav1.ListMeta{ResourceVersion: "list-version"}, Items: []appsv1.Deployment{*d.DeepCopy()}}, nil
	})
	client.PrependWatchReactor("deployments", func(a ktesting.Action) (bool, watch.Interface, error) {
		if a.(ktesting.WatchAction).GetWatchRestrictions().ResourceVersion != "list-version" {
			t.Error("watch did not continue List resourceVersion")
		}
		once.Do(func() { close(watchStarted) })
		return true, stream, nil
	})
	core := &recordingCore{reports: make(chan recordedFrame, 32)}
	o := &Observer{client, core, []string{"apps"}}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- o.Run(ctx, nil) }()
	if b := receive(t, core.reports); b.Kind != "snapshot-begin" {
		t.Fatal("full sync begin missing")
	}
	if b := receive(t, core.reports); b.Kind != "snapshot-objects" || len(b.Objects) != 1 || b.Objects[0].Health != "not_checked" {
		t.Fatal("resource facts missing")
	}
	if b := receive(t, core.reports); b.Kind != "snapshot-end" || b.TotalCount != 1 {
		t.Fatal("complete missing")
	}
	select {
	case <-watchStarted:
	case <-time.After(time.Second):
		t.Fatal("watch not started")
	}
	changed := d.DeepCopy()
	changed.Status.ReadyReplicas = 0
	changed.ResourceVersion = "watch-update"
	stream.Modify(changed)
	if b := receive(t, core.reports); b.Kind != "upsert" || b.Object.Ready != 0 || b.Object.ResourceVersion != "watch-update" {
		t.Fatal("watch update not forwarded")
	}
	stream.Delete(changed)
	if b := receive(t, core.reports); b.Kind != "delete" || b.UID != string(d.UID) {
		t.Fatal("delete lost resource identity")
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("watcher did not stop")
	}
}

func TestStateObserverPreservesInstanceAndInvalidOwnership(t *testing.T) {
	d := deployment(t)
	d.Annotations["station.verdantflare.com/instance-id"] = uuid.NewString()
	d.Annotations["station.verdantflare.com/organization-id"] = strings.Repeat("invalid", 100)
	o := observation(d, false)
	if o.InstanceID != d.Annotations["station.verdantflare.com/instance-id"] || o.OrganizationID != "!invalid" || o.InstallationID == "" {
		t.Fatal("instance identity or malformed ownership lost")
	}
}

func TestStateObserverRejectsPartialListsAndStopsOnProtocolError(t *testing.T) {
	client := fake.NewSimpleClientset()
	d := deployment(t)
	client.PrependReactor("list", "deployments", func(a ktesting.Action) (bool, kruntime.Object, error) {
		if a.GetNamespace() == "apps" {
			return true, &appsv1.DeploymentList{ListMeta: metav1.ListMeta{ResourceVersion: "first"}, Items: []appsv1.Deployment{*d}}, nil
		}
		return true, nil, errors.New("list denied")
	})
	core := &recordingCore{reports: make(chan recordedFrame, 32)}
	o := &Observer{client, core, []string{"apps", "unavailable"}}
	if err := o.Session(t.Context()); !errors.Is(err, ErrUnavailable) {
		t.Fatal("partial list accepted", err)
	}
	if len(core.reports) != 0 {
		t.Fatal("partial List began destructive full synchronization")
	}
	o.Namespaces = []string{"apps"}
	core.fail = ErrInvalid
	if err := o.Run(t.Context(), nil); !errors.Is(err, ErrInvalid) {
		t.Fatal("invalid-protocol process did not stop", err)
	}
	if core.acquisitions != 2 {
		t.Fatal("invalid-protocol process automatically reacquired")
	}
}

func TestStateObserverExpiredWatchRelists(t *testing.T) {
	d := deployment(t)
	client := fake.NewSimpleClientset()
	var mu sync.Mutex
	lists := 0
	watched := make(chan *watch.RaceFreeFakeWatcher, 2)
	client.PrependReactor("list", "deployments", func(a ktesting.Action) (bool, kruntime.Object, error) {
		mu.Lock()
		defer mu.Unlock()
		lists++
		version := "list-" + strconv.Itoa(lists)
		return true, &appsv1.DeploymentList{ListMeta: metav1.ListMeta{ResourceVersion: version}, Items: []appsv1.Deployment{*d.DeepCopy()}}, nil
	})
	client.PrependWatchReactor("deployments", func(a ktesting.Action) (bool, watch.Interface, error) {
		s := watch.NewRaceFreeFake()
		watched <- s
		return true, s, nil
	})
	core := &recordingCore{reports: make(chan recordedFrame, 32)}
	o := &Observer{client, core, []string{"apps"}}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- o.Run(ctx, nil) }()
	first := receive(t, core.reports)
	receive(t, core.reports)
	receive(t, core.reports)
	var s *watch.RaceFreeFakeWatcher
	select {
	case s = <-watched:
	case <-time.After(time.Second):
		t.Fatal("watch missing")
	}
	s.Error(&metav1.Status{Code: 410, Reason: metav1.StatusReasonExpired})
	next := receive(t, core.reports)
	if next.Kind != "snapshot-begin" || next.SyncID == first.SyncID {
		t.Fatal("expired watch reused incomplete snapshot", next.Kind, next.SyncID)
	}
	receive(t, core.reports)
	receive(t, core.reports)
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("relisted watcher leaked")
	}
}

func TestStateSnapshotSplitsEscapedAnnotationsByByteLimit(t *testing.T) {
	core := &recordingCore{reports: make(chan recordedFrame, 16)}
	stream, _ := core.Open(t.Context())
	o := observation(deployment(t), false)
	o.StationID = strings.Repeat("\x01", 128)
	o.OrganizationID = o.StationID
	o.InstallationID = o.StationID
	o.AppID = o.StationID
	o.InstanceID = o.StationID
	resources := make([]ResourceObservation, 500)
	for i := range resources {
		resources[i] = o
		resources[i].Name = "resource-" + strconv.Itoa(i)
	}
	if err := sendSnapshotObjects(stream, uuid.NewString(), resources); err != nil {
		t.Fatal(err)
	}
	if len(core.reports) < 2 {
		t.Fatal("escaped facts not split by size")
	}
	total := 0
	for len(core.reports) > 0 {
		frame := <-core.reports
		raw, _ := json.Marshal(SnapshotObjects{1, frame.Kind, frame.SyncID, frame.Objects})
		if len(raw) > 1<<20 || len(frame.Objects) > 500 || len(frame.Objects) == 0 {
			t.Fatal("unbounded stream frame")
		}
		total += len(frame.Objects)
	}
	if total != 500 {
		t.Fatal("resources lost while splitting")
	}
}
