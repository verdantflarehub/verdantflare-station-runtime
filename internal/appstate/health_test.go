package appstate

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func targetFixture(t *testing.T) (HealthTarget, ResourceObservation) {
	o := observation(deployment(t), false)
	return HealthTarget{Namespace: o.Namespace, WorkloadName: o.Name, WorkloadUID: o.UID, ServiceName: "manager", Port: 8091, Path: "/health"}, o
}

func TestHealthTargetConfiguration(t *testing.T) {
	target, _ := targetFixture(t)
	if !validHealthTargets([]HealthTarget{target}, []string{"apps"}) {
		t.Fatal("valid target rejected")
	}
	for _, change := range []func(*HealthTarget){
		func(v *HealthTarget) { v.Namespace = "outside" },
		func(v *HealthTarget) { v.WorkloadUID = "*" },
		func(v *HealthTarget) { v.ServiceName = "other.namespace.svc" },
		func(v *HealthTarget) { v.Port = 0 },
		func(v *HealthTarget) { v.Path = "//external" },
		func(v *HealthTarget) { v.Path = "/health?url=external" },
		func(v *HealthTarget) { v.Path = "/%2e%2e/secret" },
		func(v *HealthTarget) { v.Path = "/../secret" },
	} {
		bad := target
		change(&bad)
		if validHealthTargets([]HealthTarget{bad}, []string{"apps"}) {
			t.Fatal("bad target accepted")
		}
	}
	if validHealthTargets([]HealthTarget{target, target}, []string{"apps"}) {
		t.Fatal("duplicate target accepted")
	}
}

type noReadBody struct {
	t      *testing.T
	closed bool
}

func (b *noReadBody) Read([]byte) (int, error) {
	b.t.Error("health response body was read")
	return 0, io.EOF
}
func (b *noReadBody) Close() error { b.closed = true; return nil }

func TestHealthHTTPPolicy(t *testing.T) {
	target, _ := targetFixture(t)
	for _, status := range []int{200, 204, 299, 301, 401, 500} {
		t.Run(fmt.Sprint(status), func(t *testing.T) {
			client := newHealthClient()
			transport := client.Transport.(*http.Transport)
			if transport.Proxy != nil || !transport.DisableCompression || client.Timeout != 2*time.Second {
				t.Fatal("unbounded or proxy-enabled probe")
			}
			body := &noReadBody{t: t}
			calls := 0
			client.Transport = roundTrip(func(r *http.Request) (*http.Response, error) {
				calls++
				if r.Method != "GET" || r.URL.String() != "http://manager.apps.svc:8091/health" || len(r.Header) != 0 {
					t.Error("unexpected target or forwarded credentials")
				}
				return &http.Response{StatusCode: status, Header: http.Header{"Location": []string{"http://other.invalid"}}, Body: body}, nil
			})
			want := "failed"
			if status < 300 {
				want = "ready"
			}
			if got := probeHealth(t.Context(), client, target); got != want || calls != 1 || !body.closed {
				t.Fatal(got, calls, body.closed)
			}
		})
	}
	client := newHealthClient()
	client.Transport = roundTrip(func(r *http.Request) (*http.Response, error) { <-r.Context().Done(); return nil, r.Context().Err() })
	start := time.Now()
	if probeHealth(t.Context(), client, target) != "failed" || time.Since(start) > 3*time.Second {
		t.Fatal("timeout not bounded")
	}
}

func TestHealthRoundBoundedAndCanceled(t *testing.T) {
	target, o := targetFixture(t)
	var active, maximum atomic.Int32
	started := make(chan struct{}, 8)
	client := newHealthClient()
	client.Transport = roundTrip(func(r *http.Request) (*http.Response, error) {
		value := active.Add(1)
		defer active.Add(-1)
		for old := maximum.Load(); value > old; old = maximum.Load() {
			if maximum.CompareAndSwap(old, value) {
				break
			}
		}
		started <- struct{}{}
		<-r.Context().Done()
		return nil, r.Context().Err()
	})
	var targets []HealthTarget
	var resources []ResourceObservation
	for i := 0; i < 12; i++ {
		v, resource := target, o
		v.WorkloadName = fmt.Sprintf("app-%d", i)
		resource.Name = v.WorkloadName
		targets = append(targets, v)
		resources = append(resources, resource)
	}
	round := startHealthRound(t.Context(), targets, resources, client)
	defer round.stop()
	for i := 0; i < 4; i++ {
		select {
		case <-started:
		case <-time.After(time.Second):
			t.Fatal("probes missing")
		}
	}
	round.stop()
	if maximum.Load() != 4 || active.Load() != 0 || len(round.results) != 0 {
		t.Fatal("unbounded probe or result survived cancellation", maximum.Load(), active.Load())
	}
}

func TestHealthRoundStaleAndIneligibleResults(t *testing.T) {
	target, o := targetFixture(t)
	for _, change := range []string{"uid", "version", "delete", "instance", "stopped", "age"} {
		t.Run(change, func(t *testing.T) {
			started, finish := make(chan struct{}), make(chan struct{})
			client := newHealthClient()
			client.Transport = roundTrip(func(r *http.Request) (*http.Response, error) {
				close(started)
				select {
				case <-finish:
				case <-r.Context().Done():
					return nil, r.Context().Err()
				}
				return &http.Response{StatusCode: 200, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(""))}, nil
			})
			round := startHealthRound(t.Context(), []HealthTarget{target}, []ResourceObservation{o}, client)
			defer round.stop()
			select {
			case <-started:
			case <-time.After(time.Second):
				t.Fatal("probe missing")
			}
			changed := o
			switch change {
			case "uid":
				changed.UID = "replacement"
			case "version":
				changed.ResourceVersion = "replacement"
			case "delete":
				changed.Deleted = true
			case "instance":
				changed.InstanceID = "worker"
			case "stopped":
				changed.Desired = 0
			}
			round.update(changed)
			close(finish)
			select {
			case result := <-round.results:
				if change == "age" {
					result.at = time.Now().Add(-3 * time.Second)
				}
				if round.current(result) {
					t.Fatal("stale probe accepted")
				}
			case <-time.After(time.Second):
				t.Fatal("probe did not return")
			}
		})
	}
	for _, change := range []func(*ResourceObservation){func(v *ResourceObservation) { v.UID = "other" }, func(v *ResourceObservation) { v.InstanceID = "worker" }, func(v *ResourceObservation) { v.Desired = 0 }, func(v *ResourceObservation) { v.Terminating = true }} {
		bad := o
		change(&bad)
		if healthEligible(bad, target) {
			t.Fatal("ineligible resource accepted")
		}
	}
}

func TestHealthCanceledContext(t *testing.T) {
	target, _ := targetFixture(t)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if probeHealth(ctx, newHealthClient(), target) != "failed" {
		t.Fatal("canceled probe succeeded")
	}
}
