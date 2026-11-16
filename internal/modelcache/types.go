// Package modelcache implements the Station controlled ComfyUI model catalog,
// two-stage SSRF-protected singleflight CAS downloader, .safetensors binary
// frame verifier, RENAME_NOREPLACE view publisher, and cross-DB two-phase
// reference pin coordinator.
package modelcache

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"time"

	"sigs.k8s.io/yaml"
)

const (
	CatalogSchemaVersion = "station-model-catalog-v1"

	DefaultReservedHeadroomBytes = int64(200) * 1024 * 1024 * 1024 // 200 GiB
	DefaultMaxAutoPullModelBytes = int64(25) * 1024 * 1024 * 1024  // 25 GiB
	MaxCatalogModelBytes         = int64(100) * 1024 * 1024 * 1024 // 100 GiB
	MaxSafetensorsHeaderBytes    = uint64(100 * 1024 * 1024)       // 100 MiB

	StatusNotInstalled    = "not_installed"
	StatusQueued          = "queued"
	StatusDownloading     = "downloading"
	StatusVerifyingSHA256 = "verifying_sha256"
	StatusReady           = "ready"
	StatusFailed          = "failed"
	StatusEvicting        = "evicting"

	LicenseTierCommercialFree        = "commercial_free"
	LicenseTierResearchNonCommercial = "research_non_commercial"
	LicenseTierPaidForbidden         = "paid_forbidden"

	OriginPublic = "public"
	OriginLocal  = "local"

	ApprovalApproved     = "approved"
	ApprovalPendingAdmin = "pending_admin"
	ApprovalRejected     = "rejected"

	PinScopeTaskExecution = "task_execution"
	PinScopePublishedAPI  = "published_api"

	PinStateReserved = "reserved"
	PinStateActive   = "active"
	PinStateReleased = "released"

	AdmissionStateCommitted = "committed"
	AdmissionStateRevoked   = "revoked"
	AdmissionStateReleased  = "released"

	IdempotencyStateInProgress = "in_progress"
	IdempotencyStateCompleted  = "completed"
)

var categoryDirs = map[string]string{
	"CKPT": "checkpoints",
	"UNET": "unet",
	"CLIP": "clip",
	"VAE":  "vae",
	"LORA": "loras",
	"CNET": "controlnet",
	"UPSC": "upscale_models",
	"IPAD": "ipadapter",
}

// commercialFreeLicenses matches Design §4.3.1 item 1 exact 5-license allowlist.
var commercialFreeLicenses = map[string]bool{
	"apache-2.0":   true,
	"mit":          true,
	"bsd-3-clause": true,
	"cc-by-4.0":    true,
	"openrail++":   true,
}

var (
	modelIDPattern     = regexp.MustCompile(`^[a-zA-Z0-9_-]{1,80}$`)
	catalogCodePattern = regexp.MustCompile(`^MDL-(CKPT|UNET|CLIP|VAE|LORA|CNET|UPSC|IPAD)-[A-Z0-9]{2,24}-[0-9]{3}$`)
	sha256HexPattern   = regexp.MustCompile(`^[0-9a-f]{64}$`)
	filenamePattern    = regexp.MustCompile(`^[A-Za-z0-9._-]{1,120}\.safetensors$`)
)

// Error carries an HTTP status and stable machine error code for MCP/Core callers.
type Error struct {
	Status       int    `json:"-"`
	Code         string `json:"code"`
	Message      string `json:"message"`
	ActivePins   int    `json:"active_pins,omitempty"`
	ReservedPins int    `json:"reserved_pins,omitempty"`
}

func (e *Error) Error() string {
	if e == nil {
		return ""
	}
	return e.Code + ": " + e.Message
}

func NewError(status int, code, message string) *Error {
	return &Error{Status: status, Code: code, Message: message}
}

func AsError(err error) (*Error, bool) {
	var target *Error
	if errors.As(err, &target) && target != nil {
		return target, true
	}
	return nil, false
}

// StageHook enables deterministic synchronization in concurrency barrier tests (COMFY-P-B).
type StageHook interface {
	OnStage(ctx context.Context, stage string, meta map[string]string)
}

type StageHookFunc func(ctx context.Context, stage string, meta map[string]string)

func (f StageHookFunc) OnStage(ctx context.Context, stage string, meta map[string]string) {
	if f != nil {
		f(ctx, stage, meta)
	}
}

// CatalogEntry represents an authoritative Station model catalog record.
type CatalogEntry struct {
	ModelID          string         `json:"model_id" yaml:"model_id"`
	CatalogCode      string         `json:"catalog_code" yaml:"catalog_code"`
	DisplayName      string         `json:"display_name" yaml:"display_name"`
	Category         string         `json:"category" yaml:"category"`
	Filename         string         `json:"filename" yaml:"filename"`
	SHA256           string         `json:"sha256" yaml:"sha256"`
	SizeBytes        int64          `json:"size_bytes" yaml:"size_bytes"`
	VRAMFootprintMiB int64          `json:"vram_footprint_mib" yaml:"vram_footprint_mib"`
	License          string         `json:"license" yaml:"license"`
	LicenseTier      string         `json:"license_tier,omitempty" yaml:"license_tier,omitempty"`
	PaidAccess       bool           `json:"paid_access" yaml:"paid_access"`
	OriginKind       string         `json:"origin_kind,omitempty" yaml:"origin_kind,omitempty"`
	ApprovalState    string         `json:"approval_state,omitempty" yaml:"approval_state,omitempty"`
	SourceURLs       []string       `json:"source_urls,omitempty" yaml:"source_urls,omitempty"`
	SourcePolicy     map[string]any `json:"source_policy,omitempty" yaml:"source_policy,omitempty"`
}

func CategorySubdir(category string) (string, bool) {
	dir, ok := categoryDirs[strings.TrimSpace(category)]
	return dir, ok
}

func ValidSHA256(v string) bool {
	return sha256HexPattern.MatchString(v)
}

func ValidModelID(v string) bool {
	return modelIDPattern.MatchString(v)
}

func ValidCatalogCode(v string) bool {
	return catalogCodePattern.MatchString(v)
}

func ValidIdempotencyKey(v string) bool {
	return len(v) >= 8 && len(v) <= 128 && !strings.ContainsAny(v, " \t\r\n")
}

// RelativeViewPath builds the immutable digest-prefixed view path under /models/views:
// <category_dir>/<sha256[:12]>__<filename>. Never returns a host path.
func RelativeViewPath(category, sha256Hex, filename string) (string, error) {
	dir, ok := CategorySubdir(category)
	if !ok || !ValidSHA256(sha256Hex) {
		return "", NewError(400, "INVALID_PARAMETERS", "invalid model category or sha256")
	}
	if err := ValidateModelFilename(filename); err != nil {
		return "", err
	}
	return dir + "/" + sha256Hex[:12] + "__" + filename, nil
}

func truthyPolicyFlag(v any) bool {
	switch x := v.(type) {
	case bool:
		return x
	case string:
		s := strings.ToLower(strings.TrimSpace(x))
		return s == "true" || s == "1" || s == "yes" || s == "on"
	case float64:
		return x != 0
	case int:
		return x != 0
	default:
		return false
	}
}

func licenseTierRank(tier string) int {
	switch tier {
	case LicenseTierCommercialFree:
		return 0
	case LicenseTierResearchNonCommercial:
		return 1
	case LicenseTierPaidForbidden:
		return 2
	default:
		return -1
	}
}

// ClassifyLicenseTier maps license strings and paid_access flags to the three-tier policy.
func ClassifyLicenseTier(license string, paidAccess bool, sourcePolicy map[string]any) string {
	if paidAccess {
		return LicenseTierPaidForbidden
	}
	norm := strings.ToLower(strings.TrimSpace(license))
	if norm == "" ||
		strings.Contains(norm, "paid") ||
		strings.Contains(norm, "early-access") ||
		strings.Contains(norm, "early_access") ||
		strings.Contains(norm, "earlyaccess") ||
		strings.Contains(norm, "commercial-subscription") ||
		strings.Contains(norm, "commercial-paid") {
		return LicenseTierPaidForbidden
	}
	if sourcePolicy != nil {
		for _, k := range []string{"paidAccess", "paid_access", "earlyAccess", "early_access", "EarlyAccess", "requires_subscription", "requiresSubscription"} {
			if truthyPolicyFlag(sourcePolicy[k]) {
				return LicenseTierPaidForbidden
			}
		}
		for _, k := range []string{"earlyAccessDeadline", "early_access_deadline", "earlyAccessEndsAt"} {
			if raw, ok := sourcePolicy[k]; ok && raw != nil && strings.TrimSpace(fmt.Sprint(raw)) != "" {
				return LicenseTierPaidForbidden
			}
		}
	}
	if commercialFreeLicenses[norm] {
		return LicenseTierCommercialFree
	}
	return LicenseTierResearchNonCommercial
}

func (e *CatalogEntry) NormalizeAndValidate() error {
	e.ModelID = strings.TrimSpace(e.ModelID)
	e.CatalogCode = strings.TrimSpace(e.CatalogCode)
	e.DisplayName = strings.TrimSpace(e.DisplayName)
	e.Category = strings.TrimSpace(e.Category)
	e.Filename = strings.TrimSpace(e.Filename)
	e.SHA256 = strings.ToLower(strings.TrimSpace(e.SHA256))
	e.License = strings.TrimSpace(e.License)

	if !ValidModelID(e.ModelID) {
		return NewError(400, "INVALID_PARAMETERS", "invalid model_id")
	}
	if !ValidCatalogCode(e.CatalogCode) {
		return NewError(400, "INVALID_PARAMETERS", "invalid catalog_code")
	}
	parts := strings.Split(e.CatalogCode, "-")
	if len(parts) != 4 || parts[1] != e.Category {
		return NewError(400, "INVALID_PARAMETERS", "catalog_code category segment does not match category")
	}
	if len(e.DisplayName) < 2 || len(e.DisplayName) > 160 {
		return NewError(400, "INVALID_PARAMETERS", "invalid display_name length")
	}
	if _, ok := CategorySubdir(e.Category); !ok {
		return NewError(400, "INVALID_PARAMETERS", "unsupported model category")
	}
	if err := ValidateModelFilename(e.Filename); err != nil {
		return err
	}
	if !ValidSHA256(e.SHA256) {
		return NewError(400, "INVALID_PARAMETERS", "invalid sha256 digest")
	}
	if e.SizeBytes < 1 || e.SizeBytes > MaxCatalogModelBytes {
		return NewError(400, "INVALID_PARAMETERS", "size_bytes out of bounds")
	}
	if e.VRAMFootprintMiB < 1 || e.VRAMFootprintMiB > 29696 {
		return NewError(400, "INVALID_PARAMETERS", "vram_footprint_mib out of bounds")
	}
	if len(e.License) < 1 || len(e.License) > 64 {
		return NewError(400, "INVALID_PARAMETERS", "invalid license")
	}
	if e.OriginKind == "" {
		if parts[2] == "LOCAL" {
			e.OriginKind = OriginLocal
		} else {
			e.OriginKind = OriginPublic
		}
	}
	if e.OriginKind != OriginPublic && e.OriginKind != OriginLocal {
		return NewError(400, "INVALID_PARAMETERS", "invalid origin_kind")
	}
	if parts[2] == "LOCAL" && e.OriginKind != OriginLocal {
		return NewError(400, "INVALID_PARAMETERS", "MDL-*-LOCAL-* entries must have origin_kind=local")
	}

	inferredTier := ClassifyLicenseTier(e.License, e.PaidAccess, e.SourcePolicy)
	if e.LicenseTier == "" {
		e.LicenseTier = inferredTier
	}
	if licenseTierRank(e.LicenseTier) < 0 {
		return NewError(400, "INVALID_PARAMETERS", "invalid license_tier")
	}
	// Explicit license_tier may tighten classification, NEVER loosen inferredTier.
	if licenseTierRank(e.LicenseTier) < licenseTierRank(inferredTier) {
		e.LicenseTier = inferredTier
	}
	if e.PaidAccess || inferredTier == LicenseTierPaidForbidden {
		e.LicenseTier = LicenseTierPaidForbidden
	}

	if e.ApprovalState == "" {
		switch {
		case e.PaidAccess || e.LicenseTier == LicenseTierPaidForbidden:
			e.ApprovalState = ApprovalRejected
		case e.OriginKind == OriginLocal || e.LicenseTier == LicenseTierResearchNonCommercial:
			e.ApprovalState = ApprovalPendingAdmin
		default:
			e.ApprovalState = ApprovalApproved
		}
	}
	if (e.PaidAccess || e.LicenseTier == LicenseTierPaidForbidden) && e.ApprovalState == ApprovalApproved {
		return NewError(403, "MODEL_PAID_ACCESS_FORBIDDEN", "paid-access models cannot be marked approved")
	}
	switch e.ApprovalState {
	case ApprovalApproved, ApprovalPendingAdmin, ApprovalRejected:
	default:
		return NewError(400, "INVALID_PARAMETERS", "invalid approval_state")
	}
	if e.SourcePolicy == nil {
		e.SourcePolicy = map[string]any{}
	}
	return nil
}

type rawCatalogEntry struct {
	ModelID          string         `json:"model_id" yaml:"model_id"`
	CatalogCode      string         `json:"catalog_code" yaml:"catalog_code"`
	DisplayName      string         `json:"display_name" yaml:"display_name"`
	Category         string         `json:"category" yaml:"category"`
	Filename         string         `json:"filename" yaml:"filename"`
	SHA256           string         `json:"sha256" yaml:"sha256"`
	SizeBytes        int64          `json:"size_bytes" yaml:"size_bytes"`
	VRAMFootprintMiB int64          `json:"vram_footprint_mib" yaml:"vram_footprint_mib"`
	License          string         `json:"license" yaml:"license"`
	LicenseTier      string         `json:"license_tier,omitempty" yaml:"license_tier,omitempty"`
	PaidAccess       *bool          `json:"paid_access" yaml:"paid_access"`
	OriginKind       string         `json:"origin_kind,omitempty" yaml:"origin_kind,omitempty"`
	ApprovalState    string         `json:"approval_state,omitempty" yaml:"approval_state,omitempty"`
	SourceURLs       []string       `json:"source_urls,omitempty" yaml:"source_urls,omitempty"`
	SourcePolicy     map[string]any `json:"source_policy,omitempty" yaml:"source_policy,omitempty"`
}

type catalogFile struct {
	SchemaVersion string            `json:"schema_version" yaml:"schema_version"`
	Models        []rawCatalogEntry `json:"models" yaml:"models"`
}

// LoadCatalogYAML parses a station-model-catalog-v1 YAML/JSON payload.
func LoadCatalogYAML(raw []byte) ([]CatalogEntry, error) {
	var doc catalogFile
	if err := yaml.UnmarshalStrict(raw, &doc); err != nil {
		return nil, NewError(400, "INVALID_PARAMETERS", fmt.Sprintf("invalid catalog YAML: %v", err))
	}
	if doc.SchemaVersion != CatalogSchemaVersion {
		return nil, NewError(400, "INVALID_PARAMETERS", "unsupported catalog schema_version")
	}
	seenIDs := map[string]bool{}
	seenCodes := map[string]bool{}
	out := make([]CatalogEntry, 0, len(doc.Models))
	for i := range doc.Models {
		r := doc.Models[i]
		if r.PaidAccess == nil {
			return nil, NewError(400, "INVALID_PARAMETERS", "paid_access must be explicitly specified on every catalog entry")
		}
		entry := CatalogEntry{
			ModelID:          r.ModelID,
			CatalogCode:      r.CatalogCode,
			DisplayName:      r.DisplayName,
			Category:         r.Category,
			Filename:         r.Filename,
			SHA256:           r.SHA256,
			SizeBytes:        r.SizeBytes,
			VRAMFootprintMiB: r.VRAMFootprintMiB,
			License:          r.License,
			LicenseTier:      r.LicenseTier,
			PaidAccess:       *r.PaidAccess,
			OriginKind:       r.OriginKind,
			ApprovalState:    r.ApprovalState,
			SourceURLs:       r.SourceURLs,
			SourcePolicy:     r.SourcePolicy,
		}
		if err := entry.NormalizeAndValidate(); err != nil {
			return nil, err
		}
		if seenIDs[entry.ModelID] || seenCodes[entry.CatalogCode] {
			return nil, NewError(400, "INVALID_PARAMETERS", "duplicate model_id or catalog_code in catalog")
		}
		seenIDs[entry.ModelID] = true
		seenCodes[entry.CatalogCode] = true
		out = append(out, entry)
	}
	return out, nil
}

// BlobState mirrors station.model_cache_blobs.
type BlobState struct {
	SHA256           string
	SizeBytes        int64
	Status           string
	DownloadFence    int64
	EvictFence       int64
	ActiveEvictID    string
	BytesDownloaded  int64
	PartBytesOnDisk  int64
	SpeedBps         int64
	ETASeconds       *int
	ETag             string
	PublishedViews   []string
	PendingViews     []string
	CASInode         *uint64
	VerifiedAt       *time.Time
	LastErrorCode    string
	LastErrorMessage string
	UpdatedAt        time.Time
}

// ModelStatusView is the sanitized public/internal view returned to MCP & Studio callers.
type ModelStatusView struct {
	CatalogCode      string  `json:"catalog_code"`
	ModelID          string  `json:"model_id"`
	DisplayName      string  `json:"display_name"`
	Category         string  `json:"category"`
	Filename         string  `json:"filename"`
	SHA256           string  `json:"sha256"`
	SizeBytes        int64   `json:"size_bytes"`
	VRAMFootprintMiB int64   `json:"vram_footprint_mib"`
	License          string  `json:"license"`
	LicenseTier      string  `json:"license_tier"`
	PaidAccess       bool    `json:"paid_access"`
	OriginKind       string  `json:"origin_kind"`
	OriginLabel      string  `json:"origin_label"`
	ApprovalState    string  `json:"approval_state"`
	Status           string  `json:"status"`
	DownloadFence    int64   `json:"download_fence"`
	BytesDownloaded  int64   `json:"bytes_downloaded"`
	SpeedBps         int64   `json:"speed_bps"`
	ETASeconds       *int    `json:"eta_seconds"`
	RelativeViewPath string  `json:"relative_view_path"`
	ViewFilename     string  `json:"view_filename"`
	VerifiedAt       *string `json:"verified_at,omitempty"`
	LastErrorCode    string  `json:"last_error_code,omitempty"`
	LastErrorMessage string  `json:"last_error_message,omitempty"`
	ActivePins       int     `json:"active_pins"`
	ReservedPins     int     `json:"reserved_pins"`
}

// CanonicalModelLockV1 is the 9-field deterministic dependency lock structure (§4.3.1).
type CanonicalModelLockV1 struct {
	CatalogCode      string `json:"catalog_code"`
	ModelID          string `json:"model_id"`
	Category         string `json:"category"`
	Filename         string `json:"filename"`
	SHA256           string `json:"sha256"`
	SizeBytes        int64  `json:"size_bytes"`
	VRAMFootprintMiB int64  `json:"vram_footprint_mib"`
	License          string `json:"license"`
	PaidAccess       bool   `json:"paid_access"`
}

func ToCanonicalModelLock(entry CatalogEntry) CanonicalModelLockV1 {
	return CanonicalModelLockV1{
		CatalogCode:      entry.CatalogCode,
		ModelID:          entry.ModelID,
		Category:         entry.Category,
		Filename:         entry.Filename,
		SHA256:           entry.SHA256,
		SizeBytes:        entry.SizeBytes,
		VRAMFootprintMiB: entry.VRAMFootprintMiB,
		License:          entry.License,
		PaidAccess:       false,
	}
}

func SortCanonicalLocks(locks []CanonicalModelLockV1) []CanonicalModelLockV1 {
	cp := append([]CanonicalModelLockV1(nil), locks...)
	sort.Slice(cp, func(i, j int) bool {
		if cp[i].Category != cp[j].Category {
			return cp[i].Category < cp[j].Category
		}
		if cp[i].ModelID != cp[j].ModelID {
			return cp[i].ModelID < cp[j].ModelID
		}
		return cp[i].SHA256 < cp[j].SHA256
	})
	dedup := make([]CanonicalModelLockV1, 0, len(cp))
	for _, item := range cp {
		if len(dedup) > 0 && dedup[len(dedup)-1] == item {
			continue
		}
		dedup = append(dedup, item)
	}
	return dedup
}

// ComputeModelDependenciesSHA256 returns the RFC 8785 / JCS canonical SHA-256
// digest of canonical_model_lock_v1 (§4.3.1), matching Python's
// compute_model_dependencies_sha256 byte-for-byte.
func ComputeModelDependenciesSHA256(locks []CanonicalModelLockV1) (string, []CanonicalModelLockV1) {
	sorted := SortCanonicalLocks(locks)
	items := make([]map[string]any, 0, len(sorted))
	for _, l := range sorted {
		items = append(items, map[string]any{
			"catalog_code":       l.CatalogCode,
			"category":           l.Category,
			"filename":           l.Filename,
			"license":            l.License,
			"model_id":           l.ModelID,
			"paid_access":        l.PaidAccess,
			"sha256":             l.SHA256,
			"size_bytes":         l.SizeBytes,
			"vram_footprint_mib": l.VRAMFootprintMiB,
		})
	}
	envelope := map[string]any{
		"models":         items,
		"schema_version": "canonical_model_lock_v1",
	}
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	_ = enc.Encode(envelope)
	raw := bytes.TrimSuffix(buf.Bytes(), []byte("\n"))
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:]), sorted
}

type ListFilter struct {
	Category string
	Status   string
	Query    string
	Cursor   string
	Limit    int
}

type ListPage struct {
	Models     []ModelStatusView `json:"models"`
	NextCursor string            `json:"next_cursor,omitempty"`
}

type EnsureRequest struct {
	OrganizationID string `json:"organization_id"`
	ProjectID      string `json:"project_id"`
	PrincipalID    string `json:"principal_id"`
	TargetAlias    string `json:"target_alias,omitempty"`
	IdempotencyKey string `json:"idempotency_key"`
	ModelID        string `json:"model_id"`
	CatalogCode    string `json:"catalog_code,omitempty"`
	ExpectedSHA256 string `json:"expected_sha256,omitempty"`
}

type EnsureResponse struct {
	HTTPStatus       int             `json:"-"`
	Code             string          `json:"code,omitempty"`
	Message          string          `json:"message,omitempty"`
	Deduplicated     bool            `json:"deduplicated"`
	Model            ModelStatusView `json:"model"`
	CanonicalLock    *CanonicalModelLockV1 `json:"canonical_lock,omitempty"`
}

type ReservePinsRequest struct {
	HolderAppID    string   `json:"holder_app_id,omitempty"`
	ReservationID  string   `json:"reservation_id"`
	OperationID    string   `json:"operation_id"`
	ScopeKind      string   `json:"scope_kind"`
	OwnerID        string   `json:"owner_id"`
	OrganizationID string   `json:"organization_id"`
	ProjectID      string   `json:"project_id"`
	RequestSHA256  string   `json:"request_sha256"`
	SHA256List     []string `json:"sha256_list"`
	TTLSeconds     int      `json:"ttl_seconds,omitempty"`
}

type PinRecord struct {
	PinID          string  `json:"pin_id"`
	SHA256         string  `json:"sha256"`
	HolderAppID    string  `json:"holder_app_id"`
	ReservationID  string  `json:"reservation_id"`
	OperationID    string  `json:"operation_id"`
	ScopeKind      string  `json:"scope_kind"`
	OwnerID        string  `json:"owner_id"`
	OrganizationID string  `json:"organization_id"`
	ProjectID      string  `json:"project_id"`
	RequestSHA256  string  `json:"request_sha256"`
	PinState       string  `json:"pin_state"`
	ExpiresAt      *string `json:"expires_at,omitempty"`
}

type ReservePinsResponse struct {
	ReservationID string      `json:"reservation_id"`
	PinState      string      `json:"pin_state"`
	Pins          []PinRecord `json:"pins"`
}

type CommitPinsRequest struct {
	HolderAppID   string `json:"holder_app_id,omitempty"`
	ReservationID string `json:"reservation_id"`
	OwnerID       string `json:"owner_id,omitempty"`
}

type CommitPinsResponse struct {
	ReservationID string `json:"reservation_id"`
	PinState      string `json:"pin_state"`
	UpdatedCount  int    `json:"updated_count"`
}

type ReleasePinsRequest struct {
	HolderAppID   string `json:"holder_app_id,omitempty"`
	ReservationID string `json:"reservation_id"`
	Reason        string `json:"reason,omitempty"`
}

type ReleasePinsResponse struct {
	ReservationID string `json:"reservation_id"`
	PinState      string `json:"pin_state"`
	ReleasedCount int    `json:"released_count"`
}

type EvictRequest struct {
	StationID      string `json:"station_id"`
	OrganizationID string `json:"organization_id"`
	UserID         string `json:"user_id"`
	SHA256         string `json:"sha256"`
	IdempotencyKey string `json:"idempotency_key"`
}

type EvictResponse struct {
	Status string           `json:"status"`
	SHA256 string           `json:"sha256"`
	Model  *ModelStatusView `json:"model,omitempty"`
}

// AppAdmissionVerification is returned by comfyui-manager's cross-DB revocation fence:
// POST /internal/v1/model-admissions/revoke-or-verify.
type AppAdmissionVerification struct {
	ReservationID string `json:"reservation_id"`
	State         string `json:"state"` // "committed" or "revoked"
	EntityID      string `json:"entity_id,omitempty"`
}

// AppAdmissionRevoker executes the cross-DB fence against the App SQLite ledger
// before any expired reserved pin in PostgreSQL is ever released.
type AppAdmissionRevoker interface {
	RevokeOrVerify(ctx context.Context, holderAppID, reservationID string) (AppAdmissionVerification, error)
}

func mustMarshalJSON(v any) []byte {
	raw, _ := json.Marshal(v)
	return raw
}
