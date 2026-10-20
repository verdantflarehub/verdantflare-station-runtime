package executor

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"mime"
	"net/http"
	"slices"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/verdantflarehub/verdantflare-station-runtime/internal/strictjson"
)

const ManagementReconcilePath = "/internal/installation-transitions/v1/reconcile"

type ManagementStep struct {
	OperationID string `json:"operation_id"`
	RequestID   string `json:"request_id"`
	InputDigest string `json:"input_digest"`
	Mode        string `json:"mode"`
}

type ManagementStepResult struct {
	ManagementStep
	Phase string `json:"phase"`
}

func (s ManagementStep) valid() bool {
	id, err := uuid.Parse(s.RequestID)
	return managementID(s.OperationID) && err == nil && id.Version() == 4 && id.Variant() == uuid.RFC4122 && id.String() == s.RequestID &&
		managementDigest.MatchString(s.InputDigest) && slices.Contains([]string{"prepare", "restore", "release_source", "release_target", "verify_forward"}, s.Mode)
}

func (j *ManagementJournal) ReconcileStep(ctx context.Context, step ManagementStep) (ManagementStepResult, error) {
	if !step.valid() {
		return ManagementStepResult{}, ErrManagementReplacement
	}
	if step.Mode == "verify_forward" {
		err := j.verifyForward(ctx, step)
		if errors.Is(err, ErrManagementReplacement) {
			return ManagementStepResult{step, "rejected"}, nil
		}
		if err != nil {
			return ManagementStepResult{}, err
		}
		return ManagementStepResult{step, "verified"}, nil
	}
	progress, err := j.reconcile(ctx, step.OperationID, step.Mode, step.InputDigest)
	if err != nil {
		return ManagementStepResult{}, err
	}
	phase := progress.Phase
	switch step.Mode {
	case "release_source", "release_target":
		direction := strings.TrimPrefix(step.Mode, "release_")
		if phase == "releasing_"+direction {
			phase = "releasing"
		} else if phase == "released_"+direction {
			phase = "released"
		} else {
			return ManagementStepResult{}, ErrManagementReplacement
		}
	case "restore":
		if phase == "canceling_source" {
			phase = "restoring"
		} else if phase == "canceled_source" {
			phase = "restored"
		}
		if !slices.Contains([]string{"checking", "restoring", "restored", "recovery_required"}, phase) {
			return ManagementStepResult{}, ErrManagementReplacement
		}
		if phase == "checking" {
			phase = "restoring"
		}
	case "prepare":
		if phase == "canceling_source" {
			phase = "sealing"
		} else if phase == "canceled_source" || phase == "rejected_preflight" {
			phase = "rejected"
		}
		if !slices.Contains([]string{"rejected", "checking", "sealing", "applying", "verifying", "prepared", "recovery_required"}, phase) {
			return ManagementStepResult{}, ErrManagementReplacement
		}
	}
	return ManagementStepResult{step, phase}, nil
}

type ManagementHandler struct {
	Journal *ManagementJournal
	Token   string
}

func validManagementToken(token string) bool {
	if len(token) < 32 || len(token) > 4096 {
		return false
	}
	for _, c := range []byte(token) {
		if c <= 32 || c > 126 {
			return false
		}
	}
	return true
}

func (h ManagementHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	reply := func(status int, value any) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_ = json.NewEncoder(w).Encode(value)
	}
	fail := func(status int, code string) { reply(status, map[string]string{"code": code}) }
	if r.Method != http.MethodPost || r.URL.Path != ManagementReconcilePath {
		fail(404, "NOT_FOUND")
		return
	}
	if !validManagementToken(h.Token) || h.Journal == nil {
		fail(503, "TRANSITION_UNAVAILABLE")
		return
	}
	actual, want := sha256.Sum256([]byte(r.Header.Get("Authorization"))), sha256.Sum256([]byte("Bearer "+h.Token))
	if len(r.Header.Values("Authorization")) != 1 || subtle.ConstantTimeCompare(actual[:], want[:]) != 1 {
		fail(401, "UNAUTHENTICATED")
		return
	}
	media, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil || media != "application/json" || len(r.Header.Values("Content-Type")) != 1 || r.URL.RawPath != "" || r.URL.RawQuery != "" || r.URL.ForceQuery ||
		r.Header.Get("Content-Encoding") != "" || len(r.TransferEncoding) != 0 || r.ContentLength > 4096 {
		fail(400, "INVALID_ARGUMENT")
		return
	}
	var input ManagementStep
	if strictjson.Decode(r.Body, 4096, &input) != nil || !input.valid() {
		fail(400, "INVALID_ARGUMENT")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 20*time.Second)
	defer cancel()
	result, err := h.Journal.ReconcileStep(ctx, input)
	if err != nil {
		switch {
		case errors.Is(err, ErrManagementBusy):
			fail(409, "TRANSITION_BUSY")
		case errors.Is(err, ErrManagementReplacement):
			fail(409, "TRANSITION_CONFLICT")
		case errors.Is(err, pgx.ErrNoRows):
			fail(404, "NOT_FOUND")
		default:
			fail(503, "TRANSITION_UNAVAILABLE")
		}
		return
	}
	reply(200, result)
}
