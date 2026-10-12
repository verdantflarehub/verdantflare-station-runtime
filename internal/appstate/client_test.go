package appstate

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
)

type roundTrip func(*http.Request) (*http.Response, error)

func (f roundTrip) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestStreamClientRejectsAuthenticationAndHonorsBusy(t *testing.T) {
	for _, tc := range []struct {
		status        int
		body, retry   string
		invalid, busy bool
	}{
		{401, `{"code":"UNAUTHENTICATED"}`, "", true, false},
		{403, `{"code":"FORBIDDEN"}`, "", true, false},
		{409, `{"code":"COLLECTOR_BUSY"}`, "2", false, true},
		{409, `{"code":"COLLECTOR_BUSY"}`, "300", true, false},
		{409, `{"code":"OTHER"}`, "2", true, false},
		{503, `{"code":"SERVICE_UNAVAILABLE"}`, "", false, false},
		{302, `{}`, "", true, false},
	} {
		c, err := NewClient("http://core.invalid", strings.Repeat("t", 40))
		if err != nil {
			t.Fatal(err)
		}
		calls := 0
		c.http.Transport = roundTrip(func(r *http.Request) (*http.Response, error) {
			calls++
			if r.URL.Path != "/internal/app-states/v1/stream" || r.Header.Get("Content-Type") != "application/x-ndjson" {
				t.Error("wrong stream request")
			}
			return &http.Response{StatusCode: tc.status, Header: http.Header{"Retry-After": []string{tc.retry}, "Content-Type": []string{"application/json"}}, Body: io.NopCloser(strings.NewReader(tc.body))}, nil
		})
		_, err = c.Open(t.Context())
		var busy Busy
		if err == nil || errors.Is(err, ErrInvalid) != tc.invalid || errors.As(err, &busy) != tc.busy || calls != 1 {
			t.Fatal("unexpected failure classification or hidden retry", err, calls)
		}
		if tc.busy && busy.Delay != 2*time.Second {
			t.Fatal("Retry-After lost")
		}
	}
}

func TestStreamClientValidatesHelloAndControls(t *testing.T) {
	hello, _ := json.Marshal(Hello{1, "hello", uuid.NewString(), uuid.NewString(), 10, 30})
	for _, body := range []string{`{"schemaVersion":1,"kind":"ping"}`, strings.Replace(string(hello), `"schemaVersion":1`, `"schemaVersion":2`, 1), strings.Replace(string(hello), `"heartbeatSeconds":10`, `"heartbeatSeconds":0`, 1), strings.Replace(string(hello), `"kind":"hello"`, `"kind":"hello","kind":"hello"`, 1)} {
		c, _ := NewClient("http://core.invalid", strings.Repeat("t", 40))
		c.http.Transport = roundTrip(func(*http.Request) (*http.Response, error) {
			return &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": []string{"application/x-ndjson"}}, Body: io.NopCloser(strings.NewReader(body + "\n"))}, nil
		})
		if _, err := c.Open(t.Context()); !errors.Is(err, ErrInvalid) {
			t.Fatal("invalid hello accepted", err)
		}
	}
	c, _ := NewClient("http://core.invalid", strings.Repeat("t", 40))
	c.http.Transport = roundTrip(func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": []string{"application/x-ndjson"}}, Body: io.NopCloser(strings.NewReader(string(hello) + "\n" + `{"schemaVersion":1,"kind":"resync"}` + "\n" + `{"schemaVersion":1,"kind":"closing","reasonCode":"INVALID_ARGUMENT"}` + "\n"))}, nil
	})
	s, err := c.Open(context.Background())
	if err != nil && !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	if s != nil {
		defer s.Close()
		<-s.(*clientStream).done
		select {
		case event := <-s.Events():
			if !errors.Is(event.Err, ErrInvalid) {
				t.Fatal("fatal closing ignored", event)
			}
		case <-time.After(time.Second):
			t.Fatal("closing missing")
		}
		if err := s.Send(Heartbeat{1, "heartbeat"}); !errors.Is(err, ErrInvalid) {
			t.Fatal("fatal protocol failure became retryable", err)
		}
	}
}
