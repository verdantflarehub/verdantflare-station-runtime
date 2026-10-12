package appstate

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
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
	binary := os.Getenv("STATION_APP_STATE_CORE_TEST_BINARY")
	if binary == "" {
		t.Skip("5090 cross-process Core binary required")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()
	path := filepath.Join(t.TempDir(), "bridge.json")
	cmd := exec.CommandContext(ctx, binary, "-test.run=^TestCollectorBridge$", "-test.v", "-test.timeout=20s")
	cmd.Env = append(os.Environ(), "STATION_APP_STATE_BRIDGE_CONFIG="+path)
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
	stream := watch.NewRaceFreeFake()
	started := make(chan struct{}, 1)
	kube.PrependReactor("list", "deployments", func(ktesting.Action) (bool, kruntime.Object, error) {
		return true, &appsv1.DeploymentList{ListMeta: metav1.ListMeta{ResourceVersion: "bridge:collection"}, Items: []appsv1.Deployment{*d.DeepCopy()}}, nil
	})
	kube.PrependWatchReactor("deployments", func(ktesting.Action) (bool, watch.Interface, error) { started <- struct{}{}; return true, stream, nil })
	observer := Observer{kube, core, []string{"apps"}}
	runCtx, stop := context.WithCancel(ctx)
	defer stop()
	done := make(chan error, 1)
	go func() { done <- observer.Run(runCtx, nil) }()
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
	select {
	case <-started:
	case <-ctx.Done():
		t.Fatal("observer did not reach Watch")
	}
	readState("ready")
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
