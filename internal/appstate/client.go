package appstate

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"mime"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/verdantflarehub/verdantflare-station-runtime/internal/strictjson"
)

var (
	ErrUnavailable = errors.New("app state stream unavailable")
	ErrInvalid     = errors.New("app state stream protocol invalid")
	ErrResync      = errors.New("app state inventory requires synchronization")
)

type Busy struct{ Delay time.Duration }

func (Busy) Error() string { return "app state collector busy" }

type StreamEvent struct {
	Kind string
	Err  error
}
type Stream interface {
	Send(any) error
	Events() <-chan StreamEvent
	Close()
}
type Reporter interface {
	Open(context.Context) (Stream, error)
}

type Client struct {
	origin, token string
	http          *http.Client
}

func NewClient(origin, token string) (*Client, error) {
	u, err := url.Parse(origin)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Hostname() == "" || u.User != nil || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" || u.RawPath != "" || (u.Path != "" && u.Path != "/") || len(token) < 32 || len(token) > 4096 {
		return nil, ErrInvalid
	}
	for _, ch := range token {
		if ch < 33 || ch > 126 {
			return nil, ErrInvalid
		}
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.Proxy = nil
	transport.ResponseHeaderTimeout = 10 * time.Second
	return &Client{strings.TrimSuffix(origin, "/"), token, &http.Client{Transport: transport, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}}, nil
}

type clientStream struct {
	ctx      context.Context
	cancel   context.CancelFunc
	writer   *io.PipeWriter
	body     io.ReadCloser
	events   chan StreamEvent
	done     chan struct{}
	mu       sync.Mutex
	errMu    sync.Mutex
	terminal error
	hello    chan error
}

func (c *Client) Open(ctx context.Context) (Stream, error) {
	ctx, cancel := context.WithCancel(ctx)
	handshake := time.AfterFunc(30*time.Second, cancel)
	defer handshake.Stop()
	reader, writer := io.Pipe()
	stop := context.AfterFunc(ctx, func() { _ = writer.CloseWithError(ErrUnavailable); _ = reader.CloseWithError(ErrUnavailable) })
	cleanup := func() { cancel(); stop(); writer.Close(); reader.Close() }
	r, err := http.NewRequestWithContext(ctx, "POST", c.origin+"/internal/app-states/v1/stream", reader)
	if err != nil {
		cleanup()
		return nil, ErrInvalid
	}
	r.Header.Set("Authorization", "Bearer "+c.token)
	r.Header.Set("Content-Type", "application/x-ndjson")
	response, err := c.http.Do(r)
	if err != nil {
		cleanup()
		return nil, ErrUnavailable
	}
	media, _, mediaErr := mime.ParseMediaType(response.Header.Get("Content-Type"))
	if response.StatusCode != 200 {
		defer response.Body.Close()
		defer cleanup()
		if response.StatusCode == 401 || response.StatusCode == 403 {
			return nil, ErrInvalid
		}
		if response.StatusCode == 409 {
			var failure struct {
				Code string `json:"code"`
			}
			if strictjson.Decode(response.Body, 4096, &failure) != nil || failure.Code != "COLLECTOR_BUSY" {
				return nil, ErrInvalid
			}
			seconds, err := strconv.Atoi(response.Header.Get("Retry-After"))
			if err != nil || seconds < 1 || seconds > 30 {
				return nil, ErrInvalid
			}
			return nil, Busy{time.Duration(seconds) * time.Second}
		}
		if response.StatusCode >= 500 || response.StatusCode == 429 {
			return nil, ErrUnavailable
		}
		return nil, ErrInvalid
	}
	if mediaErr != nil || media != "application/x-ndjson" {
		response.Body.Close()
		cleanup()
		return nil, ErrInvalid
	}
	s := &clientStream{ctx: ctx, cancel: cancel, writer: writer, body: response.Body, events: make(chan StreamEvent, 1), done: make(chan struct{}), hello: make(chan error, 1)}
	go s.read(cleanup)
	select {
	case err := <-s.hello:
		if err != nil {
			s.Close()
			return nil, err
		}
		return s, nil
	case <-ctx.Done():
		s.Close()
		return nil, s.failure()
	}
}

func (s *clientStream) read(cleanup func()) {
	defer close(s.done)
	defer cleanup()
	defer s.body.Close()
	defer close(s.events)
	deadline := time.AfterFunc(30*time.Second, s.cancel)
	defer deadline.Stop()
	scanner := bufio.NewScanner(s.body)
	scanner.Buffer(make([]byte, 4096), 8193)
	first := true
	fail := func(err error) {
		s.errMu.Lock()
		s.terminal = err
		s.errMu.Unlock()
		if first {
			s.hello <- err
			first = false
		}
		select {
		case s.events <- StreamEvent{Err: err}:
		default:
			// A fatal control frame must replace a coalesced resync hint.
			select {
			case <-s.events:
			default:
			}
			s.events <- StreamEvent{Err: err}
		}
	}
	for scanner.Scan() {
		raw := scanner.Bytes()
		var envelope struct {
			SchemaVersion int    `json:"schemaVersion"`
			Kind          string `json:"kind"`
		}
		if json.Unmarshal(raw, &envelope) != nil || envelope.SchemaVersion != 1 {
			fail(ErrInvalid)
			return
		}
		decode := func(v any) bool { return strictjson.Decode(bytes.NewReader(raw), 8192, v) == nil }
		if first {
			var hello Hello
			if envelope.Kind != "hello" || !decode(&hello) || !validUUID(hello.CoreBootID) || !validUUID(hello.SessionID) || hello.HeartbeatSeconds != 10 || hello.ObservationTTLSeconds != 30 {
				fail(ErrInvalid)
				return
			}
			first = false
			s.hello <- nil
		} else {
			switch envelope.Kind {
			case "ping":
				var f Ping
				if !decode(&f) {
					fail(ErrInvalid)
					return
				}
			case "resync":
				var f Resync
				if !decode(&f) {
					fail(ErrInvalid)
					return
				}
				select {
				case s.events <- StreamEvent{Kind: "resync"}:
				default:
				}
			case "closing":
				var f Closing
				if !decode(&f) {
					fail(ErrInvalid)
					return
				}
				switch f.ReasonCode {
				case "RESYNC_REQUIRED", "STREAM_TIMEOUT", "SERVICE_UNAVAILABLE":
					fail(ErrUnavailable)
				default:
					fail(ErrInvalid)
				}
				return
			default:
				fail(ErrInvalid)
				return
			}
		}
		deadline.Reset(30 * time.Second)
	}
	fail(ErrUnavailable)
}

func validUUID(v string) bool {
	id, err := uuid.Parse(v)
	return err == nil && id != uuid.Nil && id.String() == v
}
func (s *clientStream) Events() <-chan StreamEvent { return s.events }
func (s *clientStream) failure() error {
	s.errMu.Lock()
	defer s.errMu.Unlock()
	if s.terminal != nil {
		return s.terminal
	}
	return ErrUnavailable
}
func (s *clientStream) Send(frame any) error {
	raw, err := json.Marshal(frame)
	if err != nil || len(raw) > 1<<20 {
		return ErrInvalid
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ctx.Err() != nil {
		return s.failure()
	}
	deadline := time.AfterFunc(10*time.Second, s.cancel)
	defer deadline.Stop()
	if _, err = s.writer.Write(append(raw, '\n')); err != nil {
		return s.failure()
	}
	return nil
}
func (s *clientStream) Close() { s.cancel(); s.writer.Close(); s.body.Close(); <-s.done }
