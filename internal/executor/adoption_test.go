package executor

import (
	"context"
	"encoding/json"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"
	"time"
)

func TestAdoptionMetadataOnlyAndReplay(t *testing.T) {
	ctx := context.Background()
	app := target()
	app.ExpectedUID = "00112233-4455-6677-8899-aabbccddeeff"
	live := app.Deployment.DeepCopy()
	live.UID = types.UID(app.ExpectedUID)
	live.ResourceVersion = "7"
	live.Generation = 3
	c := fake.NewSimpleClientset(live)
	d := Kubernetes{c}
	for i := 0; i < 2; i++ {
		r, e := d.Step(ctx, app, "adopt", "op", "station", "org")
		if e != nil || r.Status != "succeeded" {
			t.Fatal(r, e)
		}
	}
	after, _ := c.AppsV1().Deployments(live.Namespace).Get(ctx, live.Name, metav1.GetOptions{})
	if !reflect.DeepEqual(live.Spec, after.Spec) || after.Generation != live.Generation || !owned(after.Annotations, app, "station", "org") {
		t.Fatal("adoption changed workload or failed ownership")
	}
	patches := 0
	for _, a := range c.Actions() {
		if a.GetVerb() == "patch" {
			patches++
		} else if a.GetVerb() != "get" {
			t.Fatal("unexpected mutation", a)
		}
	}
	if patches != 1 {
		t.Fatal("replay patched again", patches)
	}
}
func TestAdoptionPreflightNoMutation(t *testing.T) {
	for _, scenario := range []string{"uid", "foreign", "spec", "service"} {
		t.Run(scenario, func(t *testing.T) {
			app := target()
			app.ExpectedUID = "expected"
			live := app.Deployment.DeepCopy()
			live.UID = "expected"
			switch scenario {
			case "uid":
				live.UID = "changed"
			case "foreign":
				live.Annotations = map[string]string{orgKey: "other"}
			case "spec":
				live.Spec.Template.Spec.Containers[0].Image = "other:1.0.0"
			case "service":
				app.Services = []*corev1.Service{{ObjectMeta: metav1.ObjectMeta{Name: "missing", Namespace: live.Namespace}}}
			}
			c := fake.NewSimpleClientset(live)
			d := Kubernetes{c}
			r, e := d.Step(context.Background(), app, "adopt", "op", "station", "org")
			if e != nil || r.Code != "CONFLICT" {
				t.Fatal(r, e)
			}
			for _, a := range c.Actions() {
				if a.GetVerb() != "get" {
					t.Fatal("mutated during preflight", a)
				}
			}
		})
	}
}
func TestActivityNeverAuthorizesDisruption(t *testing.T) {
	for _, scenario := range []string{"busy", "idle", "stale", "error", "missing"} {
		t.Run(scenario, func(t *testing.T) {
			now := time.Now().UTC()
			counts := map[string]int{"queued": 0, "running": 0, "succeeded": 1, "failed": 0, "cancelled": 0}
			if scenario == "busy" {
				counts["running"] = 2
			}
			if scenario == "stale" {
				now = now.Add(-time.Hour)
			}
			if scenario == "missing" {
				delete(counts, "queued")
			}
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Header.Get("Authorization") != "Bearer test" {
					t.Error("missing auth")
				}
				if scenario == "error" {
					w.WriteHeader(503)
					return
				}
				_ = json.NewEncoder(w).Encode(map[string]any{"counts": counts, "synced_at": now, "sync_errors": 0})
			}))
			defer server.Close()
			t.Setenv("STATION_APP_TEST_TOKEN", "test")
			app := target()
			app.ActivityURL = server.URL
			app.ActivityTokenEnv = "STATION_APP_TEST_TOKEN"
			r := activityGuard(context.Background(), app)
			expected := "SERVICE_UNAVAILABLE"
			if scenario == "busy" {
				expected = "CONFLICT"
			}
			if r.Status != "failed" || r.Code != expected {
				t.Fatal(r)
			}
		})
	}
}
