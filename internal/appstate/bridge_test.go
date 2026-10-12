package appstate

import (
	"bytes"
	"context"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"
)

func TestStateObserverCrossProcessCore(t *testing.T) {
	testObserverCoreStream(t, false, false)
}

func TestStateObserverCalibrationOnSameStream(t *testing.T) {
	testObserverCoreStream(t, true, false)
}

func TestStateObserverHealthCrossProcessCore(t *testing.T) {
	testObserverCoreStream(t, false, true)
}

func testObserverCoreStream(t *testing.T, calibrate, health bool) {
	t.Helper()
	binary := os.Getenv("STATION_APP_STATE_CORE_TEST_BINARY")
	if binary == "" {
		t.Skip("5090 cross-process Core binary required")
	}
	duration := 20 * time.Second
	if calibrate {
		duration = 85 * time.Second
	}
	if health {
		duration = 45 * time.Second
	}
	ctx, cancel := context.WithTimeout(t.Context(), duration)
	defer cancel()
	path := filepath.Join(t.TempDir(), "bridge.json")
	cmd := exec.CommandContext(ctx, binary, "-test.run=^TestCollectorBridge$", "-test.v", "-test.timeout=90s")
	cmd.Env = append(os.Environ(), "STATION_APP_STATE_BRIDGE_CONFIG="+path)
	if health {
		cmd.Env = append(cmd.Env, "STATION_APP_STATE_BRIDGE_HEALTH=1")
	}
	var output bytes.Buffer
	cmd.Stdout = &output
	cmd.Stderr = &output
	input, err := cmd.StdinPipe()
	if err != nil {
		t.Fatal("bridge input unavailable")
	}
	if cmd.Start() != nil {
		t.Fatal("bridge process could not start")
	}
	waited := false
	defer func() {
		input.Close()
		cancel()
		if !waited {
			_ = cmd.Wait()
		}
	}()
	var config struct {
		Origin, Token string
		Resource      ResourceObservation
	}
	for {
		raw, e := os.ReadFile(path)
		if e == nil {
			if json.Unmarshal(raw, &config) != nil {
				t.Fatal("invalid bridge config")
			}
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal("bridge did not initialize")
		case <-time.After(20 * time.Millisecond):
		}
	}
	core, err := NewClient(config.Origin, config.Token)
	if err != nil {
		t.Fatal(err)
	}
	o := config.Resource
	d := &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Namespace: o.Namespace, Name: o.Name, UID: types.UID(o.UID), ResourceVersion: o.ResourceVersion, Generation: o.Generation, Annotations: map[string]string{
		"station.verdantflare.com/station-id": o.StationID, "station.verdantflare.com/organization-id": o.OrganizationID, "station.verdantflare.com/installation-id": o.InstallationID, "station.verdantflare.com/app-id": o.AppID,
	}}, Spec: appsv1.DeploymentSpec{Replicas: &o.Desired}, Status: appsv1.DeploymentStatus{ObservedGeneration: o.ObservedGeneration, Replicas: o.Replicas, ReadyReplicas: o.Ready, UpdatedReplicas: o.Updated, AvailableReplicas: o.Available}}
	kube := fake.NewSimpleClientset()
	started := make(chan *watch.RaceFreeFakeWatcher, 4)
	kube.PrependReactor("list", "deployments", func(ktesting.Action) (bool, kruntime.Object, error) {
		return true, &appsv1.DeploymentList{ListMeta: metav1.ListMeta{ResourceVersion: "bridge:collection"}, Items: []appsv1.Deployment{*d.DeepCopy()}}, nil
	})
	kube.PrependWatchReactor("deployments", func(ktesting.Action) (bool, watch.Interface, error) {
		stream := watch.NewRaceFreeFake()
		started <- stream
		return true, stream, nil
	})
	observer := Observer{Client: kube, Core: core, Namespaces: []string{"apps"}}
	var probeStatus atomic.Int32
	probeStatus.Store(200)
	healthClient := newHealthClient()
	defer healthClient.CloseIdleConnections()
	if health {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path != "/health" || r.Method != "GET" || r.Header.Get("Authorization") != "" {
				t.Error("unexpected health request")
			}
			w.WriteHeader(int(probeStatus.Load()))
		}))
		defer server.Close()
		// Real TCP/HTTP to an isolated 5090 fixture, preserving the configured
		// Service URL and production redirect/proxy/timeout behavior.
		healthClient.Transport.(*http.Transport).DialContext = func(ctx context.Context, network, addr string) (net.Conn, error) {
			if addr != "manager.apps.svc:8091" {
				t.Error("unexpected probe host")
			}
			return (&net.Dialer{}).DialContext(ctx, network, server.Listener.Addr().String())
		}
		observer.HealthTargets = []HealthTarget{{Namespace: o.Namespace, WorkloadName: o.Name, WorkloadUID: o.UID, ServiceName: "manager", Port: 8091, Path: "/health"}}
	}
	runCtx, stop := context.WithCancel(ctx)
	defer stop()
	done := make(chan error, 1)
	go func() {
		if health {
			done <- observer.session(runCtx, healthClient)
		} else {
			done <- observer.Run(runCtx, nil)
		}
	}()
	readState := func(wanted string) {
		t.Helper()
		for {
			req, _ := http.NewRequestWithContext(ctx, "GET", config.Origin+"/test/snapshot", nil)
			req.Header.Set("Authorization", "Bearer "+config.Token)
			res, e := core.http.Do(req)
			if e == nil {
				var v struct {
					Items []struct{ Runtime struct{ State string } }
				}
				err := json.NewDecoder(res.Body).Decode(&v)
				res.Body.Close()
				if res.StatusCode == 200 && err == nil && len(v.Items) == 1 && v.Items[0].Runtime.State == wanted {
					return
				}
			}
			select {
			case <-ctx.Done():
				t.Fatal("cross-process state did not converge")
			case <-time.After(20 * time.Millisecond):
			}
		}
	}
	var stream *watch.RaceFreeFakeWatcher
	select {
	case stream = <-started:
	case <-ctx.Done():
		t.Fatal("observer did not reach Watch")
	}
	readState("ready")
	if health {
		probeStatus.Store(503)
		readState("degraded")
		probeStatus.Store(200)
		readState("ready")
		t.Log("real HTTP health 200 -> 503 -> 200 crossed Runtime stream and independent Core process")
	}
	if calibrate {
		readSession := func() string {
			t.Helper()
			req, _ := http.NewRequestWithContext(ctx, "GET", config.Origin+"/test/session", nil)
			req.Header.Set("Authorization", "Bearer "+config.Token)
			res, err := core.http.Do(req)
			if err != nil {
				t.Fatal("session inspection failed")
			}
			defer res.Body.Close()
			var result struct {
				SessionID string `json:"sessionId"`
			}
			if json.NewDecoder(res.Body).Decode(&result) != nil || res.StatusCode != 200 || result.SessionID == "" {
				t.Fatal("missing live stream")
			}
			return result.SessionID
		}
		first := readSession()
		select {
		case stream = <-started:
		case <-ctx.Done():
			t.Fatal("periodic full calibration missing")
		}
		readState("ready")
		if readSession() != first {
			t.Fatal("sixty-second calibration forced reconnect instead of reusing stream")
		}
	}
	changed := d.DeepCopy()
	changed.Status.ReadyReplicas = 0
	changed.ResourceVersion = "bridge:update"
	stream.Modify(changed)
	readState("starting")
	stop()
	select {
	case <-done:
	case <-ctx.Done():
		t.Fatal("observer failed to stop")
	}
	input.Close()
	err = cmd.Wait()
	waited = true
	if err != nil || !strings.Contains(output.String(), "--- PASS: TestCollectorBridge") {
		t.Fatal("Core bridge verification failed")
	}
}
