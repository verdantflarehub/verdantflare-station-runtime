package modelcache

import (
	"crypto/sha256"
	"crypto/subtle"
	"encoding/json"
	"mime"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/verdantflarehub/verdantflare-station-runtime/internal/appinstances"
	"github.com/verdantflarehub/verdantflare-station-runtime/internal/strictjson"
)

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeServiceError(w http.ResponseWriter, err error, rootDir string) {
	if mErr, ok := AsError(err); ok {
		resp := Error{
			Code:         mErr.Code,
			Message:      sanitizePublicMessage(mErr.Message, rootDir),
			ActivePins:   mErr.ActivePins,
			ReservedPins: mErr.ReservedPins,
		}
		writeJSON(w, mErr.Status, resp)
		return
	}
	writeJSON(w, 503, Error{
		Code:    "SERVICE_UNAVAILABLE",
		Message: "model cache service unavailable",
	})
}

func strictJSONContentType(r *http.Request) bool {
	if len(r.Header.Values("Content-Type")) != 1 || r.Header.Get("Content-Encoding") != "" || len(r.TransferEncoding) != 0 {
		return false
	}
	media, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	return err == nil && media == "application/json"
}

// AdminHandler serves Station Core control endpoints on :5054 authenticated by
// STATION_INSTANCE_CONTROL_TOKEN:
// - POST /internal/v1/model-cache/models/{sha256}/evict
// - POST /internal/v1/model-cache/catalog/register
type AdminHandler struct {
	Service *Service
	Token   string
}

func (h AdminHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	expected := sha256.Sum256([]byte("Bearer " + h.Token))
	actual := sha256.Sum256([]byte(r.Header.Get("Authorization")))
	if h.Token == "" || len(r.Header.Values("Authorization")) != 1 || subtle.ConstantTimeCompare(expected[:], actual[:]) != 1 {
		writeJSON(w, 401, Error{Code: "UNAUTHENTICATED", Message: "invalid runtime control token"})
		return
	}
	if h.Service == nil {
		writeJSON(w, 503, Error{Code: "SERVICE_UNAVAILABLE", Message: "model cache provisioner is not configured"})
		return
	}
	if r.URL.RawQuery != "" || r.URL.ForceQuery || r.URL.RawPath != "" || r.Method != http.MethodPost || !strictJSONContentType(r) {
		writeJSON(w, 400, Error{Code: "INVALID_PARAMETERS", Message: "invalid request envelope"})
		return
	}

	if r.URL.Path == "/internal/v1/model-cache/catalog/register" {
		var body struct {
			Models []CatalogEntry `json:"models"`
		}
		if err := strictjson.Decode(r.Body, 256<<10, &body); err != nil || len(body.Models) == 0 || len(body.Models) > 128 {
			writeJSON(w, 400, Error{Code: "INVALID_PARAMETERS", Message: "invalid catalog registration payload"})
			return
		}
		if err := h.Service.RegisterCatalogEntries(r.Context(), body.Models); err != nil {
			writeServiceError(w, err, h.Service.RootDir)
			return
		}
		writeJSON(w, 200, map[string]any{"status": "registered", "count": len(body.Models)})
		return
	}

	const pfx = "/internal/v1/model-cache/models/"
	const sfx = "/evict"
	if strings.HasPrefix(r.URL.Path, pfx) && strings.HasSuffix(r.URL.Path, sfx) {
		sha := strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, pfx), sfx)
		if sha == "" || strings.Contains(sha, "/") || !ValidSHA256(sha) {
			writeJSON(w, 400, Error{Code: "INVALID_PARAMETERS", Message: "invalid model sha256"})
			return
		}
		var in EvictRequest
		if err := strictjson.Decode(r.Body, 4096, &in); err != nil {
			writeJSON(w, 400, Error{Code: "INVALID_PARAMETERS", Message: "invalid model eviction payload"})
			return
		}
		if in.SHA256 != "" && in.SHA256 != sha {
			writeJSON(w, 400, Error{Code: "INVALID_PARAMETERS", Message: "sha256 path/body mismatch"})
			return
		}
		in.SHA256 = sha
		res, err := h.Service.Evict(r.Context(), in)
		if err != nil {
			writeServiceError(w, err, h.Service.RootDir)
			return
		}
		writeJSON(w, 200, res)
		return
	}

	writeJSON(w, 404, Error{Code: "NOT_FOUND", Message: "unknown model-cache admin endpoint"})
}

// AppHandler serves comfyui-manager internal endpoints on :5055 authenticated by
// appCallers (STATION_APP_INSTANCE_COMFYUI_TOKEN) and X-Station-Id:
// - GET  /internal/v1/model-cache/models
// - GET  /internal/v1/model-cache/models/{model_id}
// - POST /internal/v1/model-cache/models/ensure
// - POST /internal/v1/model-cache/pins/reserve
// - POST /internal/v1/model-cache/pins/commit
// - POST /internal/v1/model-cache/pins/release
type AppHandler struct {
	StationID string
	Callers   appinstances.AccessRegistry
	Service   *Service
}

func (h AppHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	caller, ok := h.Callers[sha256.Sum256([]byte(r.Header.Get("Authorization")))]
	if !ok || len(r.Header.Values("Authorization")) != 1 {
		writeJSON(w, 401, Error{Code: "UNAUTHENTICATED", Message: "unauthenticated app caller"})
		return
	}
	if caller.AppID != "comfyui" {
		writeJSON(w, 403, Error{Code: "FORBIDDEN", Message: "app caller is not authorized for ComfyUI model cache"})
		return
	}
	if len(r.Header.Values("X-Station-Id")) != 1 || r.Header.Get("X-Station-Id") != h.StationID {
		writeJSON(w, 403, Error{Code: "FORBIDDEN", Message: "X-Station-Id mismatch"})
		return
	}
	if h.Service == nil {
		writeJSON(w, 503, Error{Code: "SERVICE_UNAVAILABLE", Message: "model cache provisioner is not configured"})
		return
	}
	if r.URL.RawPath != "" || r.Header.Get("Content-Encoding") != "" {
		writeJSON(w, 400, Error{Code: "INVALID_PARAMETERS", Message: "invalid request encoding"})
		return
	}

	if r.Method == http.MethodGet && r.URL.Path == "/internal/v1/model-cache/models" {
		filter, err := parseAppListQuery(r.URL.RawQuery)
		if err != nil {
			writeServiceError(w, err, h.Service.RootDir)
			return
		}
		page, err := h.Service.ListModels(r.Context(), filter)
		if err != nil {
			writeServiceError(w, err, h.Service.RootDir)
			return
		}
		writeJSON(w, 200, page)
		return
	}

	const modelItemPrefix = "/internal/v1/model-cache/models/"
	if r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, modelItemPrefix) {
		if r.URL.RawQuery != "" || r.URL.ForceQuery {
			writeJSON(w, 400, Error{Code: "INVALID_PARAMETERS", Message: "unexpected query string"})
			return
		}
		modelID := strings.TrimPrefix(r.URL.Path, modelItemPrefix)
		if modelID == "" || modelID == "ensure" || strings.Contains(modelID, "/") {
			writeJSON(w, 404, Error{Code: "NOT_FOUND", Message: "not found"})
			return
		}
		view, err := h.Service.GetModelStatus(r.Context(), modelID)
		if err != nil {
			writeServiceError(w, err, h.Service.RootDir)
			return
		}
		writeJSON(w, 200, map[string]any{"model": view})
		return
	}

	if r.Method != http.MethodPost || r.URL.RawQuery != "" || r.URL.ForceQuery || !strictJSONContentType(r) {
		writeJSON(w, 400, Error{Code: "INVALID_PARAMETERS", Message: "invalid POST request"})
		return
	}

	switch r.URL.Path {
	case "/internal/v1/model-cache/models/ensure":
		var in EnsureRequest
		if err := strictjson.Decode(r.Body, 8192, &in); err != nil {
			writeJSON(w, 400, Error{Code: "INVALID_PARAMETERS", Message: "invalid model.ensure JSON body"})
			return
		}
		res, err := h.Service.Ensure(r.Context(), in)
		if err != nil {
			writeServiceError(w, err, h.Service.RootDir)
			return
		}
		status := res.HTTPStatus
		if status == 0 {
			status = 200
		}
		writeJSON(w, status, res)

	case "/internal/v1/model-cache/pins/reserve":
		var in ReservePinsRequest
		if err := strictjson.Decode(r.Body, 16384, &in); err != nil {
			writeJSON(w, 400, Error{Code: "INVALID_PARAMETERS", Message: "invalid pins/reserve JSON body"})
			return
		}
		in.HolderAppID = caller.AppID
		res, err := h.Service.ReservePins(r.Context(), in)
		if err != nil {
			writeServiceError(w, err, h.Service.RootDir)
			return
		}
		writeJSON(w, 200, res)

	case "/internal/v1/model-cache/pins/commit":
		var in CommitPinsRequest
		if err := strictjson.Decode(r.Body, 4096, &in); err != nil {
			writeJSON(w, 400, Error{Code: "INVALID_PARAMETERS", Message: "invalid pins/commit JSON body"})
			return
		}
		in.HolderAppID = caller.AppID
		res, err := h.Service.CommitPins(r.Context(), in)
		if err != nil {
			writeServiceError(w, err, h.Service.RootDir)
			return
		}
		writeJSON(w, 200, res)

	case "/internal/v1/model-cache/pins/release":
		var in ReleasePinsRequest
		if err := strictjson.Decode(r.Body, 4096, &in); err != nil {
			writeJSON(w, 400, Error{Code: "INVALID_PARAMETERS", Message: "invalid pins/release JSON body"})
			return
		}
		in.HolderAppID = caller.AppID
		res, err := h.Service.ReleasePins(r.Context(), in)
		if err != nil {
			writeServiceError(w, err, h.Service.RootDir)
			return
		}
		writeJSON(w, 200, res)

	default:
		writeJSON(w, 404, Error{Code: "NOT_FOUND", Message: "unknown model-cache app endpoint"})
	}
}

func parseAppListQuery(rawQuery string) (ListFilter, error) {
	out := ListFilter{Limit: 25}
	if rawQuery == "" {
		return out, nil
	}
	vals, err := url.ParseQuery(rawQuery)
	if err != nil {
		return ListFilter{}, NewError(400, "INVALID_PARAMETERS", "malformed query parameters")
	}
	for k, v := range vals {
		if len(v) != 1 || v[0] == "" {
			return ListFilter{}, NewError(400, "INVALID_PARAMETERS", "duplicate or empty query parameter")
		}
		switch k {
		case "category":
			out.Category = v[0]
		case "status":
			out.Status = v[0]
		case "query":
			if len(v[0]) > 80 {
				return ListFilter{}, NewError(400, "INVALID_PARAMETERS", "query too long")
			}
			out.Query = v[0]
		case "cursor":
			if len(v[0]) > 128 {
				return ListFilter{}, NewError(400, "INVALID_PARAMETERS", "cursor too long")
			}
			out.Cursor = v[0]
		case "limit":
			n, pErr := strconv.Atoi(v[0])
			if pErr != nil || n < 1 || n > 100 {
				return ListFilter{}, NewError(400, "INVALID_PARAMETERS", "limit must be integer 1..100")
			}
			out.Limit = n
		default:
			return ListFilter{}, NewError(400, "INVALID_PARAMETERS", "unknown query parameter")
		}
	}
	return out, nil
}
