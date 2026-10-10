package instances

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"io"
	"mime"
	"net/http"
	"reflect"
	"sort"
	"time"
)

type Handler struct {
	Service *Service
	Token   string
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func decodeCreate(raw []byte, target *CreateRequest) error {
	return decodeRequest(raw, target)
}

func decodeRequest(raw []byte, target any) error {
	d := json.NewDecoder(bytes.NewReader(raw))
	token, err := d.Token()
	if err != nil || token != json.Delim('{') {
		return ErrInvalid
	}
	fields := map[string]bool{}
	typ := reflect.TypeOf(target).Elem()
	var collect func(reflect.Type)
	collect = func(typ reflect.Type) {
		for i := 0; i < typ.NumField(); i++ {
			field := typ.Field(i)
			if field.Anonymous {
				collect(field.Type)
			} else {
				fields[field.Tag.Get("json")] = false
			}
		}
	}
	collect(typ)
	for d.More() {
		token, err = d.Token()
		key, ok := token.(string)
		seen, known := fields[key]
		if err != nil || !ok || !known || seen {
			return ErrInvalid
		}
		fields[key] = true
		var value json.RawMessage
		if d.Decode(&value) != nil || bytes.Equal(bytes.TrimSpace(value), []byte("null")) {
			return ErrInvalid
		}
	}
	if _, err = d.Token(); err != nil {
		return ErrInvalid
	}
	if _, err = d.Token(); err != io.EOF {
		return ErrInvalid
	}
	for _, seen := range fields {
		if !seen {
			return ErrInvalid
		}
	}
	if json.Unmarshal(raw, target) != nil {
		return ErrInvalid
	}
	return nil
}

func (h Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	isProfiles := r.URL.Path == "/internal/v1/instances/profiles" && r.Method == "GET"
	isStart := r.URL.Path == "/internal/v1/instances/start" && r.Method == "POST"
	isAccess := r.URL.Path == "/internal/v1/instances/access" && r.Method == "POST"
	isStop := r.URL.Path == "/internal/v1/instances/stop" && r.Method == "POST"
	isDestroy := r.URL.Path == "/internal/v1/instances/destroy" && r.Method == "POST"
	if !isProfiles && !isStart && !isAccess && !isStop && !isDestroy && (r.URL.Path != "/internal/v1/instances/create" || r.Method != "POST") {
		writeJSON(w, 404, map[string]string{"code": "NOT_FOUND"})
		return
	}
	expected := sha256.Sum256([]byte("Bearer " + h.Token))
	actual := sha256.Sum256([]byte(r.Header.Get("Authorization")))
	if h.Token == "" || len(r.Header.Values("Authorization")) != 1 || subtle.ConstantTimeCompare(expected[:], actual[:]) != 1 {
		writeJSON(w, 401, map[string]string{"code": "UNAUTHENTICATED"})
		return
	}
	if isProfiles {
		if r.URL.RawQuery != "" || r.URL.RawPath != "" {
			writeJSON(w, 400, map[string]string{"code": "INVALID_ARGUMENT"})
			return
		}
		if h.Service == nil {
			writeJSON(w, 503, map[string]string{"code": "SERVICE_UNAVAILABLE"})
			return
		}
		type profileView struct {
			ID      string `json:"profile_id"`
			Storage int64  `json:"storage_reserved_bytes"`
			MaxFile int64  `json:"max_file_bytes"`
		}
		profiles := []profileView{}
		for _, p := range h.Service.Profiles {
			profiles = append(profiles, profileView{p.ID, p.Bytes, p.MaxFileBytes})
		}
		sort.Slice(profiles, func(i, j int) bool { return profiles[i].ID < profiles[j].ID })
		writeJSON(w, 200, map[string]any{"profiles": profiles})
		return
	}
	media, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil || media != "application/json" || r.URL.RawQuery != "" || r.URL.RawPath != "" || r.Header.Get("Content-Encoding") != "" {
		writeJSON(w, 400, map[string]string{"code": "INVALID_ARGUMENT"})
		return
	}
	raw, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 16<<10))
	var in CreateRequest
	var start StartRequest
	var access AccessRequest
	var stop StopRequest
	var destroy DestroyRequest
	var target any = &in
	if isStart {
		target = &start
	}
	if isAccess {
		target = &access
	}
	if isStop {
		target = &stop
	}
	if isDestroy {
		target = &destroy
	}
	if err != nil || decodeRequest(raw, target) != nil {
		writeJSON(w, 400, map[string]string{"code": "INVALID_ARGUMENT"})
		return
	}
	if h.Service == nil {
		writeJSON(w, 503, map[string]string{"code": "SERVICE_UNAVAILABLE"})
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()
	var out any
	if isDestroy {
		out, err = h.Service.Destroy(ctx, destroy)
	} else if isStop {
		out, err = h.Service.Stop(ctx, stop)
	} else if isAccess {
		out, err = h.Service.Access(ctx, access)
	} else if isStart {
		out, err = h.Service.Start(ctx, start)
	} else {
		out, err = h.Service.Create(ctx, in)
	}
	if err != nil {
		code, status := "SERVICE_UNAVAILABLE", 503
		var execution *ExecutionError
		if errors.Is(err, ErrInvalid) {
			code, status = "INVALID_ARGUMENT", 400
		} else if errors.Is(err, ErrConflict) {
			code, status = "OPERATION_CONFLICT", 409
		} else if errors.Is(err, ErrBusy) {
			code, status = "INSTANCE_BUSY", 409
		} else if errors.Is(err, ErrBinding) {
			code, status = "WORKSPACE_BINDING_CONFLICT", 409
		} else if errors.As(err, &execution) {
			code = execution.Code
		}
		writeJSON(w, status, map[string]string{"code": code})
		return
	}
	writeJSON(w, 200, out)
}
