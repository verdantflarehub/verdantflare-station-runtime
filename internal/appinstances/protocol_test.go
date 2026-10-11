package appinstances

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/google/uuid"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }
func response(value any) *http.Response {
	raw, _ := json.Marshal(value)
	return &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": {"application/json"}}, Body: io.NopCloser(strings.NewReader(string(raw)))}
}

func proofFor(in Run, p Profile, observed Observation, boot string) map[string]any {
	return map[string]any{"schema_version": 1, "state": "ready", "instance_id": in.InstanceID, "start_operation_id": in.OperationID,
		"generation": in.Generation, "boot_id": boot, "resource_binding": map[string]any{"pod_uid": observed.PodUID, "pod_name": runName(in), "namespace": p.Namespace, "container": "worker"}}
}

func TestCoreAuthorityUsesExactResponseAndReauthorizesReplay(t *testing.T) {
	in, p, _, _ := fixture()
	calls := 0
	status := 200
	var mutate func(map[string]any)
	a := &CoreAuthority{origin: "http://core.station-test.svc.cluster.local:8080", appID: p.AppID, token: strings.Repeat("c", 40)}
	a.client = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		calls++
		if r.URL.String() != a.origin+"/internal/v1/app-installations/authorize" || r.Method != "POST" || r.Header.Get("Authorization") != "Bearer "+a.token ||
			r.Header.Get("X-User-Id") != in.UserID || r.Header.Get("X-Organization-Id") != in.OrganizationID || r.Header.Get("X-Station-Id") != in.StationID {
			t.Fatal("incorrect authorization request")
		}
		var body map[string]string
		if json.NewDecoder(r.Body).Decode(&body) != nil || len(body) != 2 || body["installation_id"] != in.InstallationID || body["app_version"] != p.AppVersion {
			t.Fatal("unexpected authority payload")
		}
		value := map[string]any{"schema_version": 1, "installation_id": in.InstallationID, "app_id": p.AppID, "app_version": p.AppVersion,
			"station_id": in.StationID, "organization_id": in.OrganizationID, "user_id": in.UserID, "request_id": r.Header.Get("X-Request-Id"), "installation_revision": 7}
		if mutate != nil {
			mutate(value)
		}
		out := response(value)
		out.StatusCode = status
		return out, nil
	})}
	for range 2 {
		if revision, e := a.Check(context.Background(), in, p); e != nil || revision != 7 {
			t.Fatal(e)
		}
	}
	if calls != 2 {
		t.Fatal("authorization cached")
	}
	for _, field := range []string{"installation_id", "station_id", "organization_id", "user_id", "request_id", "app_id", "app_version"} {
		mutate = func(v map[string]any) { v[field] = "wrong" }
		if _, e := a.Check(context.Background(), in, p); !errors.Is(e, ErrDenied) {
			t.Fatal("mismatched authority projection accepted", field, e)
		}
	}
	mutate = func(v map[string]any) { v["installation_revision"] = true }
	if _, e := a.Check(context.Background(), in, p); !errors.Is(e, ErrDenied) {
		t.Fatal("invalid revision accepted", e)
	}
	mutate, status = nil, 403
	if _, e := a.Check(context.Background(), in, p); !errors.Is(e, ErrDenied) {
		t.Fatal("revoked actor used previous authorization", e)
	}
}

func TestAccessRegistryEnforcesAppAndCredentialBoundaries(t *testing.T) {
	const raw = `[{"app_id":"comfyui","token_env":"STATION_APP_INSTANCE_COMFYUI_TOKEN","core_token_env":"STATION_APP_INSTALLATION_COMFYUI_TOKEN"}]`
	lookup := func(name string) string {
		if strings.Contains(name, "INSTALLATION") {
			return strings.Repeat("c", 40)
		}
		return strings.Repeat("r", 40)
	}
	for _, origin := range []string{"https://external.example", "http://127.0.0.1:8080", "http://core.ns.svc.cluster.local/path", "http://user@core.ns.svc.cluster.local", "http://core.ns.svc.cluster.local?x=1"} {
		if _, e := LoadAccessRegistry(strings.NewReader(raw), lookup, origin); e == nil {
			t.Fatal("invalid internal origin accepted")
		}
	}
	origin := "http://core.ns.svc.cluster.local:8080"
	registry, e := LoadAccessRegistry(strings.NewReader(raw), lookup, origin)
	if e != nil || len(registry) != 1 {
		t.Fatal(e)
	}
	if _, ok := registry[sha256.Sum256([]byte("Bearer "+lookup("STATION_APP_INSTANCE_COMFYUI_TOKEN")))]; !ok {
		t.Fatal("caller credential not bound")
	}
	if _, e = LoadAccessRegistry(strings.NewReader(raw), func(string) string { return strings.Repeat("x", 40) }, origin); e == nil {
		t.Fatal("same token allowed for both hops")
	}
	for _, bad := range []string{strings.Replace(raw, `"app_id":"comfyui"`, `"app_id":"comfyui","app_id":"blender"`, 1),
		strings.Replace(raw, `"core_token_env"`, `"token"`, 1), raw[:len(raw)-1] + "," + raw[1:]} {
		if _, e = LoadAccessRegistry(strings.NewReader(bad), lookup, origin); e == nil {
			t.Fatal("ambiguous credential registration accepted")
		}
	}
	transport := InternalHTTPClient().Transport.(*http.Transport)
	if transport.Proxy != nil || !transport.DisableCompression {
		t.Fatal("private transport uses environment proxy or compression")
	}
}

func TestWorkerProbeBindsRunPodAndBootWithoutTrustingEndpoint(t *testing.T) {
	in, p, _, _ := fixture()
	seen := Observation{PodUID: uuid.NewString(), ServiceUID: uuid.NewString(), SecretUID: uuid.NewString(), PodReady: true, Endpoint: "http://external.example"}
	boot, token := uuid.NewString(), strings.Repeat("w", 40)
	var mutate func(map[string]any)
	o := HTTPWorkerObserver{Client: &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		if r.URL.Hostname() != runName(in)+"."+p.Namespace+".svc.cluster.local" || r.URL.Path != "/internal/v1/app-instance/status" ||
			r.Header.Get("Authorization") != "Bearer "+token {
			t.Fatal("worker credential sent to wrong endpoint")
		}
		value := proofFor(in, p, seen, boot)
		if mutate != nil {
			mutate(value)
		}
		return response(value), nil
	})}}
	if actual, e := o.Observe(context.Background(), in, p, seen, token); e != nil || actual != boot {
		t.Fatal(e)
	}
	for _, change := range []func(map[string]any){
		func(v map[string]any) { v["instance_id"] = id() }, func(v map[string]any) { v["start_operation_id"] = id() },
		func(v map[string]any) { v["generation"] = 2 }, func(v map[string]any) { v["boot_id"] = id() },
		func(v map[string]any) { v["extra"] = true }, func(v map[string]any) { v["resource_binding"].(map[string]any)["pod_uid"] = uuid.NewString() },
		func(v map[string]any) { v["resource_binding"].(map[string]any)["container"] = "other" },
	} {
		mutate = change
		if _, e := o.Observe(context.Background(), in, p, seen, token); !errors.Is(e, ErrBinding) {
			t.Fatal("invalid runtime identity accepted", e)
		}
	}
	mutate = nil
	seen.BootID = uuid.NewString()
	if _, e := o.Observe(context.Background(), in, p, seen, token); !errors.Is(e, ErrBinding) {
		t.Fatal("changed process silently adopted", e)
	}
	seen.PodReady = false
	if _, e := o.Observe(context.Background(), in, p, seen, token); !errors.Is(e, ErrNotReady) {
		t.Fatal("non-ready Pod certified", e)
	}
}

func TestPrivateClientsRejectRedirectsAndMalformedResponses(t *testing.T) {
	in, p, _, _ := fixture()
	seen := Observation{PodUID: uuid.NewString(), ServiceUID: uuid.NewString(), SecretUID: uuid.NewString(), PodReady: true}
	for _, mode := range []string{"redirect", "compressed", "oversize", "duplicate"} {
		calls := 0
		client := &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
			calls++
			if calls != 1 {
				t.Fatal("followed private redirect")
			}
			out := response(proofFor(in, p, seen, uuid.NewString()))
			switch mode {
			case "redirect":
				out.StatusCode = 302
				out.Header.Set("Location", "http://external.example/collect")
			case "compressed":
				out.Header.Set("Content-Encoding", "gzip")
			case "oversize":
				out.Body = io.NopCloser(strings.NewReader(strings.Repeat(" ", 20000) + "{}"))
			case "duplicate":
				raw, _ := io.ReadAll(out.Body)
				out.Body = io.NopCloser(strings.NewReader(strings.Replace(string(raw), `"schema_version":1`, `"schema_version":1,"schema_version":1`, 1)))
			}
			return out, nil
		})}
		if _, e := (HTTPWorkerObserver{Client: client}).Observe(context.Background(), in, p, seen, strings.Repeat("w", 40)); e == nil {
			t.Fatal("malformed worker response accepted", mode)
		}
	}
}

func TestStartHandlerScopesAppAndProjectsOnlyPublicResult(t *testing.T) {
	in, p, _, _ := fixture()
	token := strings.Repeat("s", 40)
	caller := Caller{AppID: p.AppID, Authority: &CoreAuthority{}}
	input := StartInput{in.InstallationID, in.InstanceID, in.OperationID, in.Generation, p.AppVersion, p.ID}
	bytes, _ := json.Marshal(input)
	calls := 0
	phase := "launching"
	h := Handler{StationID: in.StationID, Profiles: map[string]Profile{p.Key(): p}, Callers: AccessRegistry{sha256.Sum256([]byte("Bearer " + token)): caller},
		Execute: func(_ context.Context, got Caller, run Run, profile Profile) (StartRecord, error) {
			calls++
			if got.AppID != p.AppID || run != in || profile.Hash() != p.Hash() {
				t.Fatal("request binding lost")
			}
			return StartRecord{Run: run, ProfileHash: profile.Hash(), InstallationRevision: 1, Phase: phase,
				Observation: Observation{PodReady: true, BootID: uuid.NewString(), Endpoint: "http://private.example", SecretUID: uuid.NewString(), PodUID: uuid.NewString(), ServiceUID: uuid.NewString()}}, nil
		}}
	request := func(raw string) *http.Request {
		r := httptest.NewRequest("POST", "/internal/v1/app-instances/start", strings.NewReader(raw))
		for key, value := range map[string]string{"Authorization": "Bearer " + token, "Content-Type": "application/json", "X-Station-Id": in.StationID, "X-Organization-Id": in.OrganizationID, "X-User-Id": in.UserID} {
			r.Header.Set(key, value)
		}
		return r
	}
	for _, step := range []struct {
		phase string
		code  int
	}{{"launching", 202}, {"running", 200}} {
		phase = step.phase
		w := httptest.NewRecorder()
		h.ServeHTTP(w, request(string(bytes)))
		if w.Code != step.code || strings.Contains(w.Body.String(), "private") || strings.Contains(w.Body.String(), "secret") {
			t.Fatal("invalid public result", w.Code)
		}
		var result map[string]any
		json.Unmarshal(w.Body.Bytes(), &result)
		if len(result) != 7 || result["operation_id"] != in.OperationID || result["instance_id"] != in.InstanceID {
			t.Fatal("unexpected public response shape")
		}
	}
	for _, mutate := range []func(*http.Request){
		func(r *http.Request) { r.Header.Set("Authorization", "Bearer invalid") },
		func(r *http.Request) { r.Header.Add("Authorization", "Bearer "+token) },
		func(r *http.Request) { r.Header.Set("X-Station-Id", id()) },
		func(r *http.Request) { r.Header.Add("X-User-Id", in.UserID) },
		func(r *http.Request) { r.URL.RawQuery = "target=external" },
		func(r *http.Request) { r.URL.RawPath = "/internal/v1/app-instances/%73tart" },
	} {
		before := calls
		r := request(string(bytes))
		mutate(r)
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		if w.Code < 400 || calls != before {
			t.Fatal("invalid request executed")
		}
	}
	for _, raw := range []string{strings.Replace(string(bytes), `"generation":1`, `"generation":1,"generation":1`, 1),
		strings.Replace(string(bytes), `"generation":1`, `"generation":1,"app_id":"blender"`, 1),
		strings.Replace(string(bytes), `"profile_id":"base-cpu"`, `"profile_id":"unregistered"`, 1)} {
		before := calls
		w := httptest.NewRecorder()
		h.ServeHTTP(w, request(raw))
		if w.Code < 400 || calls != before {
			t.Fatal("unregistered or ambiguous input executed")
		}
	}
	h.Execute = func(context.Context, Caller, Run, Profile) (StartRecord, error) {
		return StartRecord{}, errors.New("private database diagnostic")
	}
	w := httptest.NewRecorder()
	h.ServeHTTP(w, request(string(bytes)))
	if w.Code != 503 || strings.Contains(w.Body.String(), "private") {
		t.Fatal("internal diagnostic leaked")
	}
}

func TestAccessHandlerIsPrivateAndNeverInvokesStart(t *testing.T) {
	in, p, _, _ := fixture()
	token := strings.Repeat("a", 40)
	caller := Caller{AppID: p.AppID, Authority: &CoreAuthority{}}
	raw, _ := json.Marshal(StartInput{in.InstallationID, in.InstanceID, in.OperationID, in.Generation, p.AppVersion, p.ID})
	access := WorkerAccess{SchemaVersion: 1, InstanceID: in.InstanceID, StartOperationID: in.OperationID, Generation: in.Generation,
		BootID: uuid.NewString(), WorkerOrigin: workerOrigin(in, p), WorkerToken: strings.Repeat("w", 40),
		Binding: ResourceBinding{uuid.NewString(), runName(in), p.Namespace, "worker"}}
	calls := 0
	var resultError error
	h := Handler{StationID: in.StationID, Profiles: map[string]Profile{p.Key(): p},
		Callers: AccessRegistry{sha256.Sum256([]byte("Bearer " + token)): caller},
		Execute: func(context.Context, Caller, Run, Profile) (StartRecord, error) {
			t.Fatal("access invoked startup")
			return StartRecord{}, nil
		},
		Access: func(_ context.Context, c Caller, r Run, profile Profile) (WorkerAccess, error) {
			calls++
			if c.AppID != p.AppID || r != in || profile.Hash() != p.Hash() {
				t.Fatal("access lost binding")
			}
			return access, resultError
		}}
	request := func() *http.Request {
		r := httptest.NewRequest("POST", "/internal/v1/app-instances/access", strings.NewReader(string(raw)))
		for key, value := range map[string]string{"Authorization": "Bearer " + token, "Content-Type": "application/json", "X-Station-Id": in.StationID,
			"X-User-Id": in.UserID, "X-Organization-Id": in.OrganizationID} {
			r.Header.Set(key, value)
		}
		return r
	}
	w := httptest.NewRecorder()
	h.ServeHTTP(w, request())
	var received WorkerAccess
	if w.Code != 200 || w.Header().Get("Cache-Control") != "no-store" || json.Unmarshal(w.Body.Bytes(), &received) != nil || received != access {
		t.Fatal("private access projection lost or cacheable")
	}
	for _, mutate := range []func(*http.Request){
		func(r *http.Request) { r.Header.Del("Authorization") },
		func(r *http.Request) { r.Header.Set("Authorization", "Bearer unknown") },
		func(r *http.Request) { r.Header.Add("Authorization", "Bearer "+token) },
		func(r *http.Request) { r.Header.Set("X-Station-Id", id()) },
		func(r *http.Request) { r.URL.RawQuery = "credential=x" },
	} {
		before := calls
		r := request()
		mutate(r)
		w = httptest.NewRecorder()
		h.ServeHTTP(w, r)
		if w.Code < 400 || calls != before || strings.Contains(w.Body.String(), access.WorkerToken) {
			t.Fatal("invalid caller obtained worker access")
		}
	}
	for _, e := range []error{ErrDenied, ErrNotReady, ErrBinding, errors.New("private diagnostic")} {
		resultError = e
		w = httptest.NewRecorder()
		h.ServeHTTP(w, request())
		if w.Code < 400 || strings.Contains(w.Body.String(), access.WorkerToken) || strings.Contains(w.Body.String(), "private diagnostic") {
			t.Fatal("failed access disclosed private response")
		}
	}
	resultError = nil
	access.WorkerOrigin = "http://unregistered.example"
	w = httptest.NewRecorder()
	h.ServeHTTP(w, request())
	if w.Code != 503 || strings.Contains(w.Body.String(), access.WorkerToken) {
		t.Fatal("unbound access projection released")
	}
}
