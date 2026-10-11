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

func TestStopBarrierBindsOriginalRunProcessAndOperation(t *testing.T) {
	in, p, _, _ := fixture()
	observed := Observation{PodUID: uuid.NewString(), ServiceUID: uuid.NewString(), SecretUID: uuid.NewString(), BootID: uuid.NewString()}
	operation, token := id(), strings.Repeat("t", 40)
	value := stopProof(in, p, observed, operation)
	calls := 0
	stopper := HTTPWorkerStopper{Client: &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		calls++
		if r.Method != "POST" || r.URL.String() != workerOrigin(in, p)+"/internal/v1/app-instance/stop-barrier" || r.Header.Get("Authorization") != "Bearer "+token {
			t.Fatal("stop sent to wrong target")
		}
		var body map[string]any
		if json.NewDecoder(r.Body).Decode(&body) != nil || len(body) != 7 || body["state"] != nil || body["stop_operation_id"] != operation || body["boot_id"] != observed.BootID {
			t.Fatal("invalid stop body")
		}
		return response(value), nil
	})}}
	for range 2 {
		proof, e := stopper.Seal(context.Background(), in, p, observed, token, operation)
		if e != nil || proof != value {
			t.Fatal("original stop replay failed", e)
		}
	}
	if calls != 2 {
		t.Fatal("barrier proof was cached")
	}
	for _, mutate := range []func(*WorkerStopProof){
		func(v *WorkerStopProof) { v.BootID = uuid.NewString() },
		func(v *WorkerStopProof) { v.InstanceID = id() },
		func(v *WorkerStopProof) { v.StopOperationID = id() },
		func(v *WorkerStopProof) { v.StartOperationID = id() },
		func(v *WorkerStopProof) { v.Generation++ },
		func(v *WorkerStopProof) { v.Binding.PodUID = uuid.NewString() },
		func(v *WorkerStopProof) { v.State = "stopped" },
	} {
		value = stopProof(in, p, observed, operation)
		mutate(&value)
		if proof, e := stopper.Seal(context.Background(), in, p, observed, token, operation); !errors.Is(e, ErrBinding) || proof != (WorkerStopProof{}) {
			t.Fatal("wrong stop proof accepted", e)
		}
	}
}

func TestStopBarrierRejectsBusyMalformedAndRedirectedResults(t *testing.T) {
	in, p, _, _ := fixture()
	observed := Observation{PodUID: uuid.NewString(), ServiceUID: uuid.NewString(), SecretUID: uuid.NewString(), BootID: uuid.NewString()}
	operation, token := id(), strings.Repeat("t", 40)
	for _, mode := range []string{"busy", "redirect", "extra", "duplicate", "compression", "boolean", "oversize"} {
		t.Run(mode, func(t *testing.T) {
			calls := 0
			stopper := HTTPWorkerStopper{Client: &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
				calls++
				if calls > 1 {
					t.Fatal("stop followed redirect")
				}
				out := response(stopProof(in, p, observed, operation))
				raw, _ := io.ReadAll(out.Body)
				switch mode {
				case "busy":
					out.StatusCode, raw = 409, []byte(`{"code":"INSTANCE_BUSY"}`)
				case "redirect":
					out.StatusCode = 307
					out.Header.Set("Location", "http://unregistered.example/stop")
				case "extra":
					raw = []byte(strings.Replace(string(raw), `"state":"sealed"`, `"state":"sealed","force":true`, 1))
				case "duplicate":
					raw = []byte(strings.Replace(string(raw), `"state":"sealed"`, `"state":"sealed","state":"sealed"`, 1))
				case "compression":
					out.Header.Set("Content-Encoding", "gzip")
				case "boolean":
					raw = []byte(strings.Replace(string(raw), `"schema_version":1`, `"schema_version":true`, 1))
				case "oversize":
					raw = append([]byte(strings.Repeat(" ", 4097)), raw...)
				}
				out.Body = io.NopCloser(strings.NewReader(string(raw)))
				return out, nil
			})}}
			proof, e := stopper.Seal(context.Background(), in, p, observed, token, operation)
			if e == nil || proof != (WorkerStopProof{}) || (mode == "busy" && !errors.Is(e, ErrBusy)) {
				t.Fatal("invalid stop response accepted", e)
			}
		})
	}
}

func TestStopHandlerBindsOriginalStartAndProjectsOnlyOperation(t *testing.T) {
	in, p, _, _ := fixture()
	operation, boot, token := id(), uuid.NewString(), strings.Repeat("s", 40)
	caller := Caller{AppID: p.AppID, Authority: &CoreAuthority{}}
	input := StopInput{StartInput{in.InstallationID, in.InstanceID, in.OperationID, in.Generation, p.AppVersion, p.ID}, operation, boot}
	raw, _ := json.Marshal(input)
	calls, phase := 0, "accepted"
	h := Handler{StationID: in.StationID, Profiles: map[string]Profile{p.Key(): p}, Callers: AccessRegistry{sha256.Sum256([]byte("Bearer " + token)): caller},
		Stop: func(_ context.Context, got Caller, run Run, profile Profile, op, gotBoot string) (StopRecord, error) {
			calls++
			if got.AppID != caller.AppID || run != in || profile.Hash() != p.Hash() || op != operation || gotBoot != boot {
				t.Fatal("stop request lost identity")
			}
			proof := stopProof(in, p, Observation{PodUID: uuid.NewString(), BootID: boot}, operation)
			return StopRecord{OperationID: op, StartID: in.OperationID, BootID: boot, Revision: 1, Progress: StopProgress{Proof: proof, Phase: phase}}, nil
		}}
	request := func(body string) *http.Request {
		r := httptest.NewRequest("POST", "/internal/v1/app-instances/stop", strings.NewReader(body))
		for key, value := range map[string]string{"Authorization": "Bearer " + token, "Content-Type": "application/json",
			"X-Station-Id": in.StationID, "X-Organization-Id": in.OrganizationID, "X-User-Id": in.UserID} {
			r.Header.Set(key, value)
		}
		return r
	}
	for _, step := range []struct {
		phase  string
		status int
	}{{"accepted", 202}, {"pod_delete_requested", 202}, {"stopped", 200}} {
		phase = step.phase
		w := httptest.NewRecorder()
		h.ServeHTTP(w, request(string(raw)))
		var result map[string]any
		if w.Code != step.status || json.Unmarshal(w.Body.Bytes(), &result) != nil || len(result) != 8 ||
			result["operation_id"] != operation || result["start_operation_id"] != in.OperationID || result["action"] != "stop" ||
			strings.Contains(w.Body.String(), boot) || strings.Contains(w.Body.String(), "proof") {
			t.Fatal("stop result leaked private identity or lost operation", w.Code)
		}
	}
	for _, body := range []string{strings.Replace(string(raw), `"generation":1`, `"generation":1,"force":true`, 1),
		strings.Replace(string(raw), operation, in.OperationID, 1), strings.Replace(string(raw), boot, id(), 1)} {
		before := calls
		w := httptest.NewRecorder()
		h.ServeHTTP(w, request(body))
		if w.Code < 400 || calls != before {
			t.Fatal("invalid stop request executed")
		}
	}
	r := request(string(raw))
	r.Header.Set("Authorization", "Bearer unknown")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Code != 401 {
		t.Fatal("stop without App credential accepted")
	}
}
