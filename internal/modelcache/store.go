package modelcache

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Store abstracts transactional persistence for station.model_* tables so both
// PostgreSQL (production / PGStore SQL suite) and deterministic row-locked
// in-memory tests execute identical protocol invariants.
type Store interface {
	UpsertCatalogEntries(ctx context.Context, entries []CatalogEntry) error
	GetCatalogEntry(ctx context.Context, modelID string) (CatalogEntry, bool, error)
	GetCatalogEntriesBySHA256(ctx context.Context, sha256Hex string) ([]CatalogEntry, error)
	GetModelStatus(ctx context.Context, modelID string) (ModelStatusView, error)
	ListModels(ctx context.Context, filter ListFilter) (ListPage, error)

	BeginEnsureIdempotency(ctx context.Context, req StoredIdempotency, now time.Time) (*StoredIdempotency, string, error)
	CompleteEnsureIdempotency(ctx context.Context, rec StoredIdempotency) error
	AbortEnsureIdempotency(ctx context.Context, orgID, projectID, principalID, targetAlias, key, claimID, requestSHA256 string) error

	AdmitOrJoinDownload(ctx context.Context, req AdmitDownloadInput) (AdmitDownloadOutcome, error)
	TransitionFenceStatus(ctx context.Context, sha256Hex string, fence int64, status string, now time.Time) (bool, error)
	UpdateFenceProgress(ctx context.Context, sha256Hex string, fence int64, bytesDownloaded, partBytes, speedBps int64, etaSeconds *int, etag string, now time.Time) (bool, error)
	FailFenceDownload(ctx context.Context, sha256Hex string, fence int64, code, message string, keepPartBytes int64, etag string, now time.Time) (bool, error)
	PublishUnderFenceLock(ctx context.Context, entry CatalogEntry, fence int64, now time.Time, publishFn func(viewsToPublish []string) (uint64, []string, error)) (ModelStatusView, bool, error)
	PublishViewForReadyBlob(ctx context.Context, entry CatalogEntry, now time.Time, publishFn func() (uint64, string, error)) (ModelStatusView, bool, error)

	EvictUnderLock(ctx context.Context, sha256Hex string, now time.Time, unlinkFn func(relViews []string, expectedCASInode *uint64, downloadFence int64) error) (EvictResponse, error)

	ReservePins(ctx context.Context, req ReservePinsRequest, now time.Time) (ReservePinsResponse, error)
	CommitPins(ctx context.Context, req CommitPinsRequest, now time.Time) (CommitPinsResponse, error)
	ReleasePins(ctx context.Context, req ReleasePinsRequest, now time.Time) (ReleasePinsResponse, error)
	TouchPinGroupReconcileChecked(ctx context.Context, holderAppID, reservationID string, checkedAt time.Time) error
	ListExpiredReservedGroups(ctx context.Context, now time.Time, limit int) ([]ExpiredReservationGroup, error)
	ListStaleActiveTaskGroups(ctx context.Context, olderThan time.Time, limit int) ([]ExpiredReservationGroup, error)
}

type StoredIdempotency struct {
	OrganizationID string
	ProjectID      string
	PrincipalID    string
	TargetAlias    string
	IdempotencyKey string
	ClaimID        string
	State          string
	RequestSHA256  string
	ModelID        string
	SHA256         string
	DownloadFence  int64
	ResponseStatus int
	ResponseJSON   []byte
	ExpiresAt      time.Time
}

type AdmitDownloadInput struct {
	Entry                 CatalogEntry
	DiskStatFn            func() (totalBytes, freeBytes int64, err error)
	FilesystemTotalBytes  int64
	FilesystemFreeBytes   int64
	ReservedHeadroomBytes int64
	MaxAutoPullBytes      int64
	LeaseOwner            string
	LeaseTTL              time.Duration
	Now                   time.Time
	AllowTakeoverActive   bool
}

type AdmitDownloadOutcome struct {
	AlreadyReady      bool
	JoinedActive      bool
	ApprovalRequired  bool
	ApprovalReason    string
	AllowInodeHandoff bool
	PrevFence         int64
	Fence             int64
	ResumeETag        string
	ExistingPartBytes int64
	View              ModelStatusView
}

type ExpiredReservationGroup struct {
	HolderAppID   string
	ReservationID string
}

func normalizeTargetAlias(raw string) string {
	clean := strings.TrimSpace(raw)
	if clean == "" {
		return "default"
	}
	return clean
}

func originLabelFor(kind string) string {
	if kind == OriginLocal {
		return "本站登记"
	}
	return "公共目录"
}

func buildModelStatusView(entry CatalogEntry, blob BlobState, activePins, reservedPins int) ModelStatusView {
	relView, _ := RelativeViewPath(entry.Category, entry.SHA256, entry.Filename)
	viewFilename := entry.SHA256[:12] + "__" + entry.Filename
	status := blob.Status
	bytesDownloaded := blob.BytesDownloaded
	viewPublished := containsString(blob.PublishedViews, relView)
	if status == "" {
		status = StatusNotInstalled
	} else if status == StatusReady && !viewPublished {
		status = StatusNotInstalled
		bytesDownloaded = 0
	}
	var verifiedAt *string
	if blob.VerifiedAt != nil && (blob.Status != StatusReady || viewPublished) {
		ts := blob.VerifiedAt.UTC().Format(time.RFC3339)
		verifiedAt = &ts
	}
	return ModelStatusView{
		CatalogCode:      entry.CatalogCode,
		ModelID:          entry.ModelID,
		DisplayName:      entry.DisplayName,
		Category:         entry.Category,
		Filename:         entry.Filename,
		SHA256:           entry.SHA256,
		SizeBytes:        entry.SizeBytes,
		VRAMFootprintMiB: entry.VRAMFootprintMiB,
		License:          entry.License,
		LicenseTier:      entry.LicenseTier,
		PaidAccess:       entry.PaidAccess,
		OriginKind:       entry.OriginKind,
		OriginLabel:      originLabelFor(entry.OriginKind),
		ApprovalState:    entry.ApprovalState,
		Status:           status,
		DownloadFence:    blob.DownloadFence,
		BytesDownloaded:  bytesDownloaded,
		SpeedBps:         blob.SpeedBps,
		ETASeconds:       blob.ETASeconds,
		RelativeViewPath: relView,
		ViewFilename:     viewFilename,
		VerifiedAt:       verifiedAt,
		LastErrorCode:    blob.LastErrorCode,
		LastErrorMessage: blob.LastErrorMessage,
		ActivePins:       activePins,
		ReservedPins:     reservedPins,
	}
}

func containsString(slice []string, val string) bool {
	for _, item := range slice {
		if item == val {
			return true
		}
	}
	return false
}

func appendUniqueString(slice []string, val string) []string {
	if val == "" {
		return slice
	}
	for _, item := range slice {
		if item == val {
			return slice
		}
	}
	return append(slice, val)
}

func largeModelApproved(policy map[string]any) bool {
	if policy == nil {
		return false
	}
	b, ok := policy["large_model_approved"].(bool)
	return ok && b
}

func sampleStorageCapacity(req AdmitDownloadInput) (int64, int64, error) {
	if req.DiskStatFn != nil {
		total, free, err := req.DiskStatFn()
		if err != nil {
			return 0, 0, NewError(500, "STORAGE_IO_ERROR", "failed reading model cache filesystem capacity")
		}
		return total, free, nil
	}
	return req.FilesystemTotalBytes, req.FilesystemFreeBytes, nil
}

// ============================================================================
// PostgreSQL Store (Production & PGStore Integration Suite)
// ============================================================================

type PGStore struct {
	Pool      *pgxpool.Pool
	StationID string
}

func rollbackPgTx(tx pgx.Tx) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = tx.Rollback(ctx)
}

func (s *PGStore) lockStorageStateRow(ctx context.Context, tx pgx.Tx, now time.Time) (int64, int64, error) {
	_, err := tx.Exec(ctx, `
		INSERT INTO station.model_cache_storage_state (
			station_id, filesystem_total_bytes, filesystem_free_bytes, reserved_headroom_bytes, max_auto_pull_model_bytes, sampled_at, updated_at
		) VALUES ($1, 0, 0, $2, $3, $4, $4)
		ON CONFLICT (station_id) DO NOTHING
	`, s.StationID, DefaultReservedHeadroomBytes, DefaultMaxAutoPullModelBytes, now)
	if err != nil {
		return 0, 0, err
	}
	var dbHeadroom, dbMaxAuto int64
	if err := tx.QueryRow(ctx, `
		SELECT reserved_headroom_bytes, max_auto_pull_model_bytes
		  FROM station.model_cache_storage_state
		 WHERE station_id = $1
		   FOR UPDATE
	`, s.StationID).Scan(&dbHeadroom, &dbMaxAuto); err != nil {
		return 0, 0, err
	}
	return dbHeadroom, dbMaxAuto, nil
}

func (s *PGStore) UpsertCatalogEntries(ctx context.Context, entries []CatalogEntry) error {
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer rollbackPgTx(tx)

	for _, raw := range entries {
		e := raw
		if err := e.NormalizeAndValidate(); err != nil {
			return err
		}
		urlsJSON, _ := json.Marshal(e.SourceURLs)
		if e.SourceURLs == nil {
			urlsJSON = []byte("[]")
		}
		policyJSON, _ := json.Marshal(e.SourcePolicy)
		if e.SourcePolicy == nil {
			policyJSON = []byte("{}")
		}
		_, err := tx.Exec(ctx, `
			INSERT INTO station.model_catalog_entries (
				station_id, model_id, catalog_code, display_name, category, filename,
				sha256, size_bytes, vram_footprint_mib, license, license_tier,
				paid_access, origin_kind, approval_state, source_urls, source_policy, updated_at
			) VALUES (
				$1, $2, $3, $4, $5, $6,
				$7, $8, $9, $10, $11,
				$12, $13, $14, $15::jsonb, $16::jsonb, now()
			)
			ON CONFLICT (station_id, model_id) DO UPDATE SET
				catalog_code = EXCLUDED.catalog_code,
				display_name = EXCLUDED.display_name,
				category = EXCLUDED.category,
				filename = EXCLUDED.filename,
				sha256 = EXCLUDED.sha256,
				size_bytes = EXCLUDED.size_bytes,
				vram_footprint_mib = EXCLUDED.vram_footprint_mib,
				license = EXCLUDED.license,
				license_tier = EXCLUDED.license_tier,
				paid_access = EXCLUDED.paid_access,
				origin_kind = EXCLUDED.origin_kind,
				approval_state = EXCLUDED.approval_state,
				source_urls = EXCLUDED.source_urls,
				source_policy = EXCLUDED.source_policy,
				updated_at = now()
		`, s.StationID, e.ModelID, e.CatalogCode, e.DisplayName, e.Category, e.Filename,
			e.SHA256, e.SizeBytes, e.VRAMFootprintMiB, e.License, e.LicenseTier,
			e.PaidAccess, e.OriginKind, e.ApprovalState, string(urlsJSON), string(policyJSON))
		if err != nil {
			return err
		}
		if !e.PaidAccess && e.LicenseTier != LicenseTierPaidForbidden {
			_, err = tx.Exec(ctx, `
				INSERT INTO station.model_cache_blobs (station_id, sha256, size_bytes, paid_access, status)
				VALUES ($1, $2, $3, false, 'not_installed')
				ON CONFLICT (station_id, sha256) DO NOTHING
			`, s.StationID, e.SHA256, e.SizeBytes)
			if err != nil {
				return err
			}
		}
	}
	return tx.Commit(ctx)
}

func scanPGCatalogRow(row pgx.Row) (CatalogEntry, error) {
	var e CatalogEntry
	var urlsRaw, policyRaw []byte
	err := row.Scan(
		&e.ModelID, &e.CatalogCode, &e.DisplayName, &e.Category, &e.Filename,
		&e.SHA256, &e.SizeBytes, &e.VRAMFootprintMiB, &e.License, &e.LicenseTier,
		&e.PaidAccess, &e.OriginKind, &e.ApprovalState, &urlsRaw, &policyRaw,
	)
	if err != nil {
		return CatalogEntry{}, err
	}
	_ = json.Unmarshal(urlsRaw, &e.SourceURLs)
	_ = json.Unmarshal(policyRaw, &e.SourcePolicy)
	if e.SourcePolicy == nil {
		e.SourcePolicy = map[string]any{}
	}
	return e, nil
}

func (s *PGStore) GetCatalogEntry(ctx context.Context, modelID string) (CatalogEntry, bool, error) {
	row := s.Pool.QueryRow(ctx, `
		SELECT model_id, catalog_code, display_name, category, filename,
		       sha256, size_bytes, vram_footprint_mib, license, license_tier,
		       paid_access, origin_kind, approval_state, source_urls, source_policy
		  FROM station.model_catalog_entries
		 WHERE station_id = $1 AND model_id = $2
	`, s.StationID, modelID)
	e, err := scanPGCatalogRow(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return CatalogEntry{}, false, nil
	}
	if err != nil {
		return CatalogEntry{}, false, err
	}
	return e, true, nil
}

func (s *PGStore) GetCatalogEntriesBySHA256(ctx context.Context, sha256Hex string) ([]CatalogEntry, error) {
	rows, err := s.Pool.Query(ctx, `
		SELECT model_id, catalog_code, display_name, category, filename,
		       sha256, size_bytes, vram_footprint_mib, license, license_tier,
		       paid_access, origin_kind, approval_state, source_urls, source_policy
		  FROM station.model_catalog_entries
		 WHERE station_id = $1 AND sha256 = $2
		 ORDER BY catalog_code ASC
	`, s.StationID, sha256Hex)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []CatalogEntry
	for rows.Next() {
		var e CatalogEntry
		var urlsRaw, policyRaw []byte
		if err := rows.Scan(
			&e.ModelID, &e.CatalogCode, &e.DisplayName, &e.Category, &e.Filename,
			&e.SHA256, &e.SizeBytes, &e.VRAMFootprintMiB, &e.License, &e.LicenseTier,
			&e.PaidAccess, &e.OriginKind, &e.ApprovalState, &urlsRaw, &policyRaw,
		); err != nil {
			return nil, err
		}
		_ = json.Unmarshal(urlsRaw, &e.SourceURLs)
		_ = json.Unmarshal(policyRaw, &e.SourcePolicy)
		out = append(out, e)
	}
	return out, rows.Err()
}

func (s *PGStore) queryBlobAndPins(ctx context.Context, q pgxQuerier, sha256Hex string, fallbackSize int64) (BlobState, int, int, error) {
	blob := BlobState{SHA256: sha256Hex, SizeBytes: fallbackSize, Status: StatusNotInstalled}
	var viewsRaw, pendingRaw []byte
	var casIno *int64
	var activeEvictID *string
	err := q.QueryRow(ctx, `
		SELECT size_bytes, status, download_fence, evict_fence, active_evict_id::text,
		       bytes_downloaded, part_bytes_on_disk, speed_bps, eta_seconds, etag,
		       published_views, pending_views, cas_inode, verified_at,
		       last_error_code, last_error_message, updated_at
		  FROM station.model_cache_blobs
		 WHERE station_id = $1 AND sha256 = $2
	`, s.StationID, sha256Hex).Scan(
		&blob.SizeBytes, &blob.Status, &blob.DownloadFence, &blob.EvictFence, &activeEvictID,
		&blob.BytesDownloaded, &blob.PartBytesOnDisk, &blob.SpeedBps, &blob.ETASeconds, &blob.ETag,
		&viewsRaw, &pendingRaw, &casIno, &blob.VerifiedAt,
		&blob.LastErrorCode, &blob.LastErrorMessage, &blob.UpdatedAt,
	)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return BlobState{}, 0, 0, err
	}
	if activeEvictID != nil {
		blob.ActiveEvictID = *activeEvictID
	}
	if len(viewsRaw) > 0 {
		_ = json.Unmarshal(viewsRaw, &blob.PublishedViews)
	}
	if len(pendingRaw) > 0 {
		_ = json.Unmarshal(pendingRaw, &blob.PendingViews)
	}
	if casIno != nil && *casIno >= 0 {
		u := uint64(*casIno)
		blob.CASInode = &u
	}
	var activePins, reservedPins int
	if pErr := q.QueryRow(ctx, `
		SELECT COUNT(*) FILTER (WHERE pin_state = 'active'),
		       COUNT(*) FILTER (WHERE pin_state = 'reserved')
		  FROM station.model_ref_pins
		 WHERE station_id = $1 AND sha256 = $2 AND pin_state IN ('active', 'reserved')
	`, s.StationID, sha256Hex).Scan(&activePins, &reservedPins); pErr != nil {
		return BlobState{}, 0, 0, pErr
	}
	return blob, activePins, reservedPins, nil
}

type pgxQuerier interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

func (s *PGStore) GetModelStatus(ctx context.Context, modelID string) (ModelStatusView, error) {
	entry, ok, err := s.GetCatalogEntry(ctx, modelID)
	if err != nil {
		return ModelStatusView{}, err
	}
	if !ok {
		return ModelStatusView{}, NewError(404, "MODEL_NOT_FOUND", "model_id not found in Station catalog")
	}
	blob, activePins, reservedPins, err := s.queryBlobAndPins(ctx, s.Pool, entry.SHA256, entry.SizeBytes)
	if err != nil {
		return ModelStatusView{}, err
	}
	return buildModelStatusView(entry, blob, activePins, reservedPins), nil
}

func (s *PGStore) ListModels(ctx context.Context, filter ListFilter) (ListPage, error) {
	limit := filter.Limit
	if limit <= 0 {
		limit = 25
	}
	if limit > 100 {
		limit = 100
	}
	rows, err := s.Pool.Query(ctx, `
		SELECT model_id, catalog_code, display_name, category, filename,
		       sha256, size_bytes, vram_footprint_mib, license, license_tier,
		       paid_access, origin_kind, approval_state, source_urls, source_policy
		  FROM station.model_catalog_entries
		 WHERE station_id = $1
		   AND ($2 = '' OR category = $2)
		   AND ($3 = '' OR catalog_code > $3)
		   AND ($4 = '' OR position(lower($4) IN lower(catalog_code)) > 0
		                OR position(lower($4) IN lower(model_id)) > 0
		                OR position(lower($4) IN lower(display_name)) > 0)
		 ORDER BY catalog_code ASC
	`, s.StationID, filter.Category, filter.Cursor, filter.Query)
	if err != nil {
		return ListPage{}, err
	}
	defer rows.Close()

	var entries []CatalogEntry
	for rows.Next() {
		var e CatalogEntry
		var urlsRaw, policyRaw []byte
		if err := rows.Scan(
			&e.ModelID, &e.CatalogCode, &e.DisplayName, &e.Category, &e.Filename,
			&e.SHA256, &e.SizeBytes, &e.VRAMFootprintMiB, &e.License, &e.LicenseTier,
			&e.PaidAccess, &e.OriginKind, &e.ApprovalState, &urlsRaw, &policyRaw,
		); err != nil {
			return ListPage{}, err
		}
		_ = json.Unmarshal(urlsRaw, &e.SourceURLs)
		_ = json.Unmarshal(policyRaw, &e.SourcePolicy)
		entries = append(entries, e)
	}
	if err := rows.Err(); err != nil {
		return ListPage{}, err
	}

	models := make([]ModelStatusView, 0, limit)
	var nextCursor string
	for _, entry := range entries {
		blob, activePins, reservedPins, err := s.queryBlobAndPins(ctx, s.Pool, entry.SHA256, entry.SizeBytes)
		if err != nil {
			return ListPage{}, err
		}
		view := buildModelStatusView(entry, blob, activePins, reservedPins)
		if filter.Status != "" && view.Status != filter.Status {
			continue
		}
		if len(models) == limit {
			nextCursor = models[len(models)-1].CatalogCode
			break
		}
		models = append(models, view)
	}
	return ListPage{Models: models, NextCursor: nextCursor}, nil
}

// BeginEnsureIdempotency atomically claims or inspects a caller- and alias-scoped
// idempotency slot using row locking and an unforgeable per-claim UUIDv7 token (T32-M03, R2-M01).
func (s *PGStore) BeginEnsureIdempotency(ctx context.Context, req StoredIdempotency, now time.Time) (*StoredIdempotency, string, error) {
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return nil, "", err
	}
	defer rollbackPgTx(tx)

	alias := normalizeTargetAlias(req.TargetAlias)
	inProgExp := now.Add(5 * time.Minute)
	placeholderSHA := req.SHA256
	if !ValidSHA256(placeholderSHA) {
		placeholderSHA = strings.Repeat("0", 64)
	}
	claimUUID, uErr := uuid.NewV7()
	if uErr != nil {
		return nil, "", uErr
	}
	claimID := claimUUID.String()

	for attempt := 0; attempt < 2; attempt++ {
		tag, iErr := tx.Exec(ctx, `
			INSERT INTO station.model_ensure_idempotency (
				station_id, organization_id, project_id, principal_id, target_alias, idempotency_key,
				claim_id, state, request_sha256, model_id, sha256, download_fence, response_status, response_json, created_at, expires_at
			) VALUES ($1, $2, $3, $4, $5, $6, $12::uuid, 'in_progress', $7, $8, $9, 0, 0, '{}'::jsonb, $10, $11)
			ON CONFLICT (station_id, organization_id, project_id, principal_id, target_alias, idempotency_key) DO NOTHING
		`, s.StationID, req.OrganizationID, req.ProjectID, req.PrincipalID, alias, req.IdempotencyKey,
			req.RequestSHA256, req.ModelID, placeholderSHA, now, inProgExp, claimID)
		if iErr != nil {
			return nil, "", iErr
		}
		if tag.RowsAffected() == 1 {
			if err := tx.Commit(ctx); err != nil {
				return nil, "", err
			}
			return nil, claimID, nil
		}

		var existing StoredIdempotency
		sErr := tx.QueryRow(ctx, `
			SELECT organization_id, project_id, principal_id, target_alias, idempotency_key,
			       claim_id::text, state, request_sha256, model_id, sha256, download_fence, response_status, response_json, expires_at
			  FROM station.model_ensure_idempotency
			 WHERE station_id = $1 AND organization_id = $2 AND project_id = $3
			   AND principal_id = $4 AND target_alias = $5 AND idempotency_key = $6
			   FOR UPDATE
		`, s.StationID, req.OrganizationID, req.ProjectID, req.PrincipalID, alias, req.IdempotencyKey).Scan(
			&existing.OrganizationID, &existing.ProjectID, &existing.PrincipalID, &existing.TargetAlias, &existing.IdempotencyKey,
			&existing.ClaimID, &existing.State, &existing.RequestSHA256, &existing.ModelID, &existing.SHA256,
			&existing.DownloadFence, &existing.ResponseStatus, &existing.ResponseJSON, &existing.ExpiresAt,
		)
		if errors.Is(sErr, pgx.ErrNoRows) {
			continue
		}
		if sErr != nil {
			return nil, "", sErr
		}

		if !existing.ExpiresAt.After(now) {
			// Expired record: overwrite ALL columns AND rotate claim_id so stale callers can never overwrite or abort the new claim (R2-M01).
			_, uErr := tx.Exec(ctx, `
				UPDATE station.model_ensure_idempotency
				   SET claim_id = $12::uuid,
				       state = 'in_progress',
				       request_sha256 = $7,
				       model_id = $8,
				       sha256 = $9,
				       download_fence = 0,
				       response_status = 0,
				       response_json = '{}'::jsonb,
				       created_at = $10,
				       expires_at = $11
				 WHERE station_id = $1 AND organization_id = $2 AND project_id = $3
				   AND principal_id = $4 AND target_alias = $5 AND idempotency_key = $6
			`, s.StationID, req.OrganizationID, req.ProjectID, req.PrincipalID, alias, req.IdempotencyKey,
				req.RequestSHA256, req.ModelID, placeholderSHA, now, inProgExp, claimID)
			if uErr != nil {
				return nil, "", uErr
			}
			if err := tx.Commit(ctx); err != nil {
				return nil, "", err
			}
			return nil, claimID, nil
		}

		if existing.RequestSHA256 != req.RequestSHA256 {
			return nil, "", NewError(409, "IDEMPOTENCY_CONFLICT", "idempotency_key reused with different model.ensure parameters")
		}
		if existing.State == IdempotencyStateInProgress {
			return nil, "", NewError(409, "OPERATION_IN_PROGRESS", "concurrent model.ensure with identical idempotency_key is still in progress")
		}
		if err := tx.Commit(ctx); err != nil {
			return nil, "", err
		}
		return &existing, "", nil
	}
	return nil, "", NewError(409, "OPERATION_IN_PROGRESS", "concurrent model.ensure contention; please retry")
}

func (s *PGStore) CompleteEnsureIdempotency(ctx context.Context, rec StoredIdempotency) error {
	if strings.TrimSpace(rec.ClaimID) == "" {
		return NewError(409, "IDEMPOTENCY_CLAIM_SUPERSEDED", "missing idempotency claim_id on completion")
	}
	alias := normalizeTargetAlias(rec.TargetAlias)
	tag, err := s.Pool.Exec(ctx, `
		UPDATE station.model_ensure_idempotency
		   SET state = 'completed',
		       model_id = $8,
		       sha256 = $9,
		       download_fence = $10,
		       response_status = $11,
		       response_json = $12::jsonb,
		       expires_at = $13
		 WHERE station_id = $1 AND organization_id = $2 AND project_id = $3
		   AND principal_id = $4 AND target_alias = $5 AND idempotency_key = $6
		   AND claim_id = $14::uuid
		   AND request_sha256 = $7
		   AND state = 'in_progress'
	`, s.StationID, rec.OrganizationID, rec.ProjectID, rec.PrincipalID, alias, rec.IdempotencyKey,
		rec.RequestSHA256, rec.ModelID, rec.SHA256, rec.DownloadFence, rec.ResponseStatus, string(rec.ResponseJSON), rec.ExpiresAt, rec.ClaimID)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return NewError(409, "IDEMPOTENCY_CLAIM_SUPERSEDED", "idempotency reservation was superseded before completion")
	}
	return nil
}

func (s *PGStore) AbortEnsureIdempotency(ctx context.Context, orgID, projectID, principalID, targetAlias, key, claimID, requestSHA256 string) error {
	if strings.TrimSpace(claimID) == "" {
		return nil
	}
	alias := normalizeTargetAlias(targetAlias)
	_, err := s.Pool.Exec(ctx, `
		DELETE FROM station.model_ensure_idempotency
		 WHERE station_id = $1 AND organization_id = $2 AND project_id = $3
		   AND principal_id = $4 AND target_alias = $5 AND idempotency_key = $6
		   AND claim_id = $7::uuid
		   AND request_sha256 = $8
		   AND state = 'in_progress'
	`, s.StationID, orgID, projectID, principalID, alias, key, claimID, requestSHA256)
	return err
}

func (s *PGStore) AdmitOrJoinDownload(ctx context.Context, req AdmitDownloadInput) (AdmitDownloadOutcome, error) {
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return AdmitDownloadOutcome{}, err
	}
	defer rollbackPgTx(tx)

	// Strictly serialize all capacity & unmaterialized-bytes accounting behind
	// station.model_cache_storage_state FOR UPDATE FIRST (T32-M01).
	dbHeadroom, dbMaxAuto, err := s.lockStorageStateRow(ctx, tx, req.Now)
	if err != nil {
		return AdmitDownloadOutcome{}, err
	}
	if req.ReservedHeadroomBytes > dbHeadroom {
		dbHeadroom = req.ReservedHeadroomBytes
	}
	if dbHeadroom < DefaultReservedHeadroomBytes {
		dbHeadroom = DefaultReservedHeadroomBytes
	}
	if req.MaxAutoPullBytes > 0 && req.MaxAutoPullBytes < dbMaxAuto {
		dbMaxAuto = req.MaxAutoPullBytes
	}
	if dbMaxAuto <= 0 || dbMaxAuto > DefaultMaxAutoPullModelBytes {
		dbMaxAuto = DefaultMaxAutoPullModelBytes
	}

	_, err = tx.Exec(ctx, `
		INSERT INTO station.model_cache_blobs (station_id, sha256, size_bytes, paid_access, status)
		VALUES ($1, $2, $3, false, 'not_installed')
		ON CONFLICT (station_id, sha256) DO NOTHING
	`, s.StationID, req.Entry.SHA256, req.Entry.SizeBytes)
	if err != nil {
		return AdmitDownloadOutcome{}, err
	}

	var status, etag, leaseOwner string
	var fence, bytesDL, partBytes, speedBps int64
	var etaSec *int
	var leaseExp *time.Time
	var pendingRaw []byte
	err = tx.QueryRow(ctx, `
		SELECT status, download_fence, bytes_downloaded, part_bytes_on_disk, speed_bps,
		       eta_seconds, etag, lease_owner, lease_expires_at, pending_views
		  FROM station.model_cache_blobs
		 WHERE station_id = $1 AND sha256 = $2
		   FOR UPDATE
	`, s.StationID, req.Entry.SHA256).Scan(
		&status, &fence, &bytesDL, &partBytes, &speedBps,
		&etaSec, &etag, &leaseOwner, &leaseExp, &pendingRaw,
	)
	if err != nil {
		return AdmitDownloadOutcome{}, err
	}

	if status == StatusEvicting {
		return AdmitDownloadOutcome{}, NewError(409, "MODEL_NOT_READY", "model blob is currently being evicted")
	}
	if status == StatusReady {
		blob, active, reserved, qErr := s.queryBlobAndPins(ctx, tx, req.Entry.SHA256, req.Entry.SizeBytes)
		if qErr != nil {
			return AdmitDownloadOutcome{}, qErr
		}
		if err := tx.Commit(ctx); err != nil {
			return AdmitDownloadOutcome{}, err
		}
		return AdmitDownloadOutcome{
			AlreadyReady: true,
			Fence:        fence,
			View:         buildModelStatusView(req.Entry, blob, active, reserved),
		}, nil
	}

	relView, relErr := RelativeViewPath(req.Entry.Category, req.Entry.SHA256, req.Entry.Filename)
	if relErr != nil {
		return AdmitDownloadOutcome{}, relErr
	}
	var pendingViews []string
	if len(pendingRaw) > 0 {
		_ = json.Unmarshal(pendingRaw, &pendingViews)
	}

	inFlight := status == StatusQueued || status == StatusDownloading || status == StatusVerifyingSHA256
	if inFlight && !req.AllowTakeoverActive && leaseExp != nil && leaseExp.After(req.Now) {
		// Record joining alias's view in pending_views so PublishUnderFenceLock atomically
		// publishes hardlink views for ALL joined catalog aliases on completion (T32-M04).
		pendingViews = appendUniqueString(pendingViews, relView)
		pendingJSON, _ := json.Marshal(pendingViews)
		if _, uErr := tx.Exec(ctx, `
			UPDATE station.model_cache_blobs
			   SET pending_views = $3::jsonb,
			       updated_at = $4
			 WHERE station_id = $1 AND sha256 = $2
		`, s.StationID, req.Entry.SHA256, string(pendingJSON), req.Now); uErr != nil {
			return AdmitDownloadOutcome{}, uErr
		}
		blob, active, reserved, qErr := s.queryBlobAndPins(ctx, tx, req.Entry.SHA256, req.Entry.SizeBytes)
		if qErr != nil {
			return AdmitDownloadOutcome{}, qErr
		}
		if err := tx.Commit(ctx); err != nil {
			return AdmitDownloadOutcome{}, err
		}
		return AdmitDownloadOutcome{
			JoinedActive: true,
			Fence:        fence,
			View:         buildModelStatusView(req.Entry, blob, active, reserved),
		}, nil
	}

	if req.Entry.SizeBytes > dbMaxAuto && !largeModelApproved(req.Entry.SourcePolicy) {
		blob, active, reserved, qErr := s.queryBlobAndPins(ctx, tx, req.Entry.SHA256, req.Entry.SizeBytes)
		if qErr != nil {
			return AdmitDownloadOutcome{}, qErr
		}
		_ = tx.Commit(ctx)
		return AdmitDownloadOutcome{
			ApprovalRequired: true,
			ApprovalReason:   fmt.Sprintf("model size_bytes %d exceeds automatic pull cap %d", req.Entry.SizeBytes, dbMaxAuto),
			Fence:            fence,
			View:             buildModelStatusView(req.Entry, blob, active, reserved),
		}, nil
	}

	// Step A (inside storage_state FOR UPDATE): query unmaterializedOther FIRST (T32-M01).
	var unmaterializedOther int64
	if err := tx.QueryRow(ctx, `
		SELECT COALESCE(SUM(GREATEST(0, size_bytes - part_bytes_on_disk)), 0)
		  FROM station.model_cache_blobs
		 WHERE station_id = $1
		   AND sha256 <> $2
		   AND status IN ('queued', 'downloading', 'verifying_sha256')
	`, s.StationID, req.Entry.SHA256).Scan(&unmaterializedOther); err != nil {
		return AdmitDownloadOutcome{}, err
	}

	// Step B (inside storage_state FOR UPDATE): sample statvfs SECOND so newly flushed
	// disk blocks are never subtracted twice and concurrent ensures cannot race.
	fsTotal, fsFree, statErr := sampleStorageCapacity(req)
	if statErr != nil {
		return AdmitDownloadOutcome{}, statErr
	}
	if _, uErr := tx.Exec(ctx, `
		UPDATE station.model_cache_storage_state
		   SET filesystem_total_bytes = $2,
		       filesystem_free_bytes = $3,
		       reserved_headroom_bytes = $4,
		       max_auto_pull_model_bytes = $5,
		       sampled_at = $6,
		       updated_at = $6
		 WHERE station_id = $1
	`, s.StationID, fsTotal, fsFree, dbHeadroom, dbMaxAuto, req.Now); uErr != nil {
		return AdmitDownloadOutcome{}, uErr
	}

	reusablePart := partBytes
	if etag == "" || reusablePart < 0 || reusablePart >= req.Entry.SizeBytes {
		reusablePart = 0
	}
	// Only credit existing .part disk space toward zero-copy inode handoff when the previous
	// attempt cleanly transitioned to 'failed'; during an expired-lease takeover (inFlight),
	// the old writer may still hold an open writable FD, so we reserve a full independent inode (R2-H01).
	prevWriterStopped := !inFlight && status == StatusFailed
	reusableDiskCredit := reusablePart
	if !prevWriterStopped {
		reusableDiskCredit = 0
	}
	neededNewBytes := req.Entry.SizeBytes - reusableDiskCredit
	remainingAfter := fsFree - unmaterializedOther - neededNewBytes
	if remainingAfter < dbHeadroom {
		blob, active, reserved, qErr := s.queryBlobAndPins(ctx, tx, req.Entry.SHA256, req.Entry.SizeBytes)
		if qErr != nil {
			return AdmitDownloadOutcome{}, qErr
		}
		_ = tx.Commit(ctx)
		return AdmitDownloadOutcome{
			ApprovalRequired: true,
			ApprovalReason:   fmt.Sprintf("insufficient free disk headroom after reservation (%d < %d)", remainingAfter, dbHeadroom),
			Fence:            fence,
			View:             buildModelStatusView(req.Entry, blob, active, reserved),
		}, nil
	}

	prevFence := fence
	nextFence := fence + 1
	exp := req.Now.Add(req.LeaseTTL)
	pendingViews = appendUniqueString(pendingViews, relView)
	pendingJSON, _ := json.Marshal(pendingViews)
	_, err = tx.Exec(ctx, `
		UPDATE station.model_cache_blobs
		   SET status = 'queued',
		       download_fence = $3,
		       bytes_downloaded = $4,
		       part_bytes_on_disk = $5,
		       speed_bps = 0,
		       eta_seconds = NULL,
		       pending_views = $8::jsonb,
		       last_error_code = '',
		       last_error_message = '',
		       lease_owner = $6,
		       lease_expires_at = $7,
		       updated_at = $9
		 WHERE station_id = $1 AND sha256 = $2
	`, s.StationID, req.Entry.SHA256, nextFence, reusablePart, reusableDiskCredit, req.LeaseOwner, exp, string(pendingJSON), req.Now)
	if err != nil {
		return AdmitDownloadOutcome{}, err
	}

	blob, active, reserved, err := s.queryBlobAndPins(ctx, tx, req.Entry.SHA256, req.Entry.SizeBytes)
	if err != nil {
		return AdmitDownloadOutcome{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return AdmitDownloadOutcome{}, err
	}
	return AdmitDownloadOutcome{
		AllowInodeHandoff: prevWriterStopped,
		PrevFence:         prevFence,
		Fence:             nextFence,
		ResumeETag:        etag,
		ExistingPartBytes: reusablePart,
		View:              buildModelStatusView(req.Entry, blob, active, reserved),
	}, nil
}

func (s *PGStore) TransitionFenceStatus(ctx context.Context, sha256Hex string, fence int64, status string, now time.Time) (bool, error) {
	tag, err := s.Pool.Exec(ctx, `
		UPDATE station.model_cache_blobs
		   SET status = $4,
		       lease_expires_at = $5,
		       updated_at = $6
		 WHERE station_id = $1
		   AND sha256 = $2
		   AND download_fence = $3
		   AND status NOT IN ('ready', 'evicting')
	`, s.StationID, sha256Hex, fence, status, now.Add(5*time.Minute), now)
	if err != nil {
		return false, err
	}
	return tag.RowsAffected() == 1, nil
}

func (s *PGStore) UpdateFenceProgress(ctx context.Context, sha256Hex string, fence int64, bytesDownloaded, partBytes, speedBps int64, etaSeconds *int, etag string, now time.Time) (bool, error) {
	tag, err := s.Pool.Exec(ctx, `
		UPDATE station.model_cache_blobs
		   SET status = 'downloading',
		       bytes_downloaded = GREATEST(bytes_downloaded, $4),
		       part_bytes_on_disk = $5,
		       speed_bps = $6,
		       eta_seconds = $7,
		       etag = CASE WHEN $8 <> '' THEN $8 ELSE etag END,
		       lease_expires_at = $9,
		       updated_at = $10
		 WHERE station_id = $1
		   AND sha256 = $2
		   AND download_fence = $3
		   AND status IN ('queued', 'downloading')
	`, s.StationID, sha256Hex, fence, bytesDownloaded, partBytes, speedBps, etaSeconds, etag, now.Add(5*time.Minute), now)
	if err != nil {
		return false, err
	}
	return tag.RowsAffected() == 1, nil
}

func (s *PGStore) FailFenceDownload(ctx context.Context, sha256Hex string, fence int64, code, message string, keepPartBytes int64, etag string, now time.Time) (bool, error) {
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return false, err
	}
	defer rollbackPgTx(tx)

	if _, _, err := s.lockStorageStateRow(ctx, tx, now); err != nil {
		return false, err
	}
	tag, err := tx.Exec(ctx, `
		UPDATE station.model_cache_blobs
		   SET status = 'failed',
		       bytes_downloaded = $4,
		       part_bytes_on_disk = $4,
		       speed_bps = 0,
		       eta_seconds = NULL,
		       etag = $5,
		       last_error_code = $6,
		       last_error_message = $7,
		       lease_owner = '',
		       lease_expires_at = NULL,
		       updated_at = $8
		 WHERE station_id = $1
		   AND sha256 = $2
		   AND download_fence = $3
		   AND status NOT IN ('ready', 'evicting')
	`, s.StationID, sha256Hex, fence, keepPartBytes, etag, code, message, now)
	if err != nil {
		return false, err
	}
	if err := tx.Commit(ctx); err != nil {
		return false, err
	}
	return tag.RowsAffected() == 1, nil
}

func (s *PGStore) PublishUnderFenceLock(ctx context.Context, entry CatalogEntry, fence int64, now time.Time, publishFn func(viewsToPublish []string) (uint64, []string, error)) (ModelStatusView, bool, error) {
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return ModelStatusView{}, false, err
	}
	defer rollbackPgTx(tx)

	// Maintain identical lock ordering (storage_state -> model_cache_blobs) so
	// unmaterializedOther & statvfs never race with RENAME_NOREPLACE publication (T32-M01).
	if _, _, err := s.lockStorageStateRow(ctx, tx, now); err != nil {
		return ModelStatusView{}, false, err
	}

	var curStatus string
	var curFence int64
	var viewsRaw, pendingRaw []byte
	err = tx.QueryRow(ctx, `
		SELECT status, download_fence, published_views, pending_views
		  FROM station.model_cache_blobs
		 WHERE station_id = $1 AND sha256 = $2
		   FOR UPDATE
	`, s.StationID, entry.SHA256).Scan(&curStatus, &curFence, &viewsRaw, &pendingRaw)
	if err != nil {
		return ModelStatusView{}, false, err
	}

	if curFence != fence || curStatus == StatusEvicting || curStatus == StatusReady {
		return ModelStatusView{}, false, nil
	}

	entryView, vErr := RelativeViewPath(entry.Category, entry.SHA256, entry.Filename)
	if vErr != nil {
		return ModelStatusView{}, false, vErr
	}
	var toPublish []string
	if len(pendingRaw) > 0 {
		_ = json.Unmarshal(pendingRaw, &toPublish)
	}
	toPublish = appendUniqueString(toPublish, entryView)

	casIno, publishedNow, pubErr := publishFn(toPublish)
	if pubErr != nil {
		return ModelStatusView{}, false, pubErr
	}

	var views []string
	if len(viewsRaw) > 0 {
		_ = json.Unmarshal(viewsRaw, &views)
	}
	for _, rv := range publishedNow {
		views = appendUniqueString(views, rv)
	}
	viewsJSON, _ := json.Marshal(views)
	casInoSigned := int64(casIno & 0x7fffffffffffffff)

	_, err = tx.Exec(ctx, `
		UPDATE station.model_cache_blobs
		   SET status = 'ready',
		       bytes_downloaded = size_bytes,
		       part_bytes_on_disk = 0,
		       speed_bps = 0,
		       eta_seconds = 0,
		       published_views = $3::jsonb,
		       pending_views = '[]'::jsonb,
		       cas_inode = $4,
		       verified_at = $5,
		       last_error_code = '',
		       last_error_message = '',
		       lease_owner = '',
		       lease_expires_at = NULL,
		       updated_at = $5
		 WHERE station_id = $1 AND sha256 = $2 AND download_fence = $6
	`, s.StationID, entry.SHA256, string(viewsJSON), casInoSigned, now, fence)
	if err != nil {
		return ModelStatusView{}, false, err
	}

	blob, active, reserved, err := s.queryBlobAndPins(ctx, tx, entry.SHA256, entry.SizeBytes)
	if err != nil {
		return ModelStatusView{}, false, err
	}
	if err := tx.Commit(ctx); err != nil {
		return ModelStatusView{}, false, err
	}
	return buildModelStatusView(entry, blob, active, reserved), true, nil
}

func (s *PGStore) PublishViewForReadyBlob(ctx context.Context, entry CatalogEntry, now time.Time, publishFn func() (uint64, string, error)) (ModelStatusView, bool, error) {
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return ModelStatusView{}, false, err
	}
	defer rollbackPgTx(tx)

	var curStatus string
	var viewsRaw []byte
	err = tx.QueryRow(ctx, `
		SELECT status, published_views
		  FROM station.model_cache_blobs
		 WHERE station_id = $1 AND sha256 = $2
		   FOR UPDATE
	`, s.StationID, entry.SHA256).Scan(&curStatus, &viewsRaw)
	if err != nil {
		return ModelStatusView{}, false, err
	}
	if curStatus != StatusReady {
		return ModelStatusView{}, false, nil
	}

	casIno, relView, pubErr := publishFn()
	if pubErr != nil {
		return ModelStatusView{}, false, pubErr
	}
	var views []string
	if len(viewsRaw) > 0 {
		_ = json.Unmarshal(viewsRaw, &views)
	}
	views = appendUniqueString(views, relView)
	viewsJSON, _ := json.Marshal(views)
	casInoSigned := int64(casIno & 0x7fffffffffffffff)

	_, err = tx.Exec(ctx, `
		UPDATE station.model_cache_blobs
		   SET published_views = $3::jsonb,
		       cas_inode = $4,
		       updated_at = $5
		 WHERE station_id = $1 AND sha256 = $2
	`, s.StationID, entry.SHA256, string(viewsJSON), casInoSigned, now)
	if err != nil {
		return ModelStatusView{}, false, err
	}
	blob, active, reserved, err := s.queryBlobAndPins(ctx, tx, entry.SHA256, entry.SizeBytes)
	if err != nil {
		return ModelStatusView{}, false, err
	}
	if err := tx.Commit(ctx); err != nil {
		return ModelStatusView{}, false, err
	}
	return buildModelStatusView(entry, blob, active, reserved), true, nil
}

func (s *PGStore) EvictUnderLock(ctx context.Context, sha256Hex string, now time.Time, unlinkFn func(relViews []string, expectedCASInode *uint64, downloadFence int64) error) (EvictResponse, error) {
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return EvictResponse{}, err
	}
	defer rollbackPgTx(tx)

	var status string
	var downloadFence, evictFence int64
	var casInoSigned *int64
	var viewsRaw []byte
	err = tx.QueryRow(ctx, `
		SELECT status, download_fence, evict_fence, cas_inode, published_views
		  FROM station.model_cache_blobs
		 WHERE station_id = $1 AND sha256 = $2
		   FOR UPDATE
	`, s.StationID, sha256Hex).Scan(&status, &downloadFence, &evictFence, &casInoSigned, &viewsRaw)
	if errors.Is(err, pgx.ErrNoRows) {
		entries, cErr := s.GetCatalogEntriesBySHA256(ctx, sha256Hex)
		if cErr != nil {
			return EvictResponse{}, cErr
		}
		if len(entries) == 0 {
			return EvictResponse{}, NewError(404, "MODEL_NOT_FOUND", "model sha256 not found in catalog")
		}
		view := buildModelStatusView(entries[0], BlobState{SHA256: sha256Hex, SizeBytes: entries[0].SizeBytes, Status: StatusNotInstalled}, 0, 0)
		return EvictResponse{Status: StatusNotInstalled, SHA256: sha256Hex, Model: &view}, nil
	}
	if err != nil {
		return EvictResponse{}, err
	}

	if status == StatusEvicting {
		return EvictResponse{}, NewError(409, "OPERATION_IN_PROGRESS", "model eviction is already in progress")
	}

	var activePins, reservedPins int
	if err := tx.QueryRow(ctx, `
		SELECT COUNT(*) FILTER (WHERE pin_state = 'active'),
		       COUNT(*) FILTER (WHERE pin_state = 'reserved')
		  FROM station.model_ref_pins
		 WHERE station_id = $1 AND sha256 = $2 AND pin_state IN ('active', 'reserved')
	`, s.StationID, sha256Hex).Scan(&activePins, &reservedPins); err != nil {
		return EvictResponse{}, err
	}
	if activePins > 0 || reservedPins > 0 {
		return EvictResponse{}, &Error{
			Status:       409,
			Code:         "MODEL_IN_USE",
			Message:      fmt.Sprintf("model is pinned (active=%d, reserved=%d) and cannot be evicted", activePins, reservedPins),
			ActivePins:   activePins,
			ReservedPins: reservedPins,
		}
	}
	if status == StatusQueued || status == StatusDownloading || status == StatusVerifyingSHA256 {
		return EvictResponse{}, NewError(409, "MODEL_IN_USE", "model download is currently in progress")
	}

	var expectedCASInode *uint64
	if casInoSigned != nil && *casInoSigned >= 0 {
		u := uint64(*casInoSigned)
		expectedCASInode = &u
	}

	var relViews []string
	if len(viewsRaw) > 0 {
		_ = json.Unmarshal(viewsRaw, &relViews)
	}
	entries, err := s.GetCatalogEntriesBySHA256(ctx, sha256Hex)
	if err != nil {
		return EvictResponse{}, err
	}
	for _, e := range entries {
		if rv, rErr := RelativeViewPath(e.Category, e.SHA256, e.Filename); rErr == nil {
			relViews = appendUniqueString(relViews, rv)
		}
	}

	opUUID, uErr := uuid.NewV7()
	if uErr != nil {
		return EvictResponse{}, uErr
	}
	myOpID := opUUID.String()
	myEvictFence := evictFence + 1

	// Phase 1: Commit status = 'evicting' with monotonic evict_fence and active_evict_id (T32-H03).
	if _, err := tx.Exec(ctx, `
		UPDATE station.model_cache_blobs
		   SET status = 'evicting',
		       evict_fence = $3,
		       active_evict_id = $4::uuid,
		       updated_at = $5
		 WHERE station_id = $1 AND sha256 = $2
	`, s.StationID, sha256Hex, myEvictFence, myOpID, now); err != nil {
		return EvictResponse{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return EvictResponse{}, err
	}

	// Phase 2: Unlink ONLY hardlink views, CAS matching expectedCASInode, and residual staging .part files.
	if unlinkFn != nil {
		if uErr := unlinkFn(relViews, expectedCASInode, downloadFence); uErr != nil {
			_, _ = s.Pool.Exec(ctx, `
				UPDATE station.model_cache_blobs
				   SET status = 'failed',
				       active_evict_id = NULL,
				       last_error_code = 'STORAGE_IO_ERROR',
				       last_error_message = 'eviction unlink failed',
				       updated_at = $5
				 WHERE station_id = $1 AND sha256 = $2
				   AND status = 'evicting' AND evict_fence = $3 AND active_evict_id = $4::uuid
			`, s.StationID, sha256Hex, myEvictFence, myOpID, now)
			return EvictResponse{}, uErr
		}
	}

	// Phase 3: Re-lock row FOR UPDATE, verify ownership of active_evict_id & evict_fence and zero live pins.
	tx2, err := s.Pool.Begin(ctx)
	if err != nil {
		return EvictResponse{}, err
	}
	defer rollbackPgTx(tx2)

	var p3Status string
	var p3EvictFence int64
	var p3OpID *string
	if err := tx2.QueryRow(ctx, `
		SELECT status, evict_fence, active_evict_id::text
		  FROM station.model_cache_blobs
		 WHERE station_id = $1 AND sha256 = $2
		   FOR UPDATE
	`, s.StationID, sha256Hex).Scan(&p3Status, &p3EvictFence, &p3OpID); err != nil {
		return EvictResponse{}, err
	}
	if p3Status != StatusEvicting || p3EvictFence != myEvictFence || p3OpID == nil || *p3OpID != myOpID {
		return EvictResponse{}, NewError(409, "OPERATION_SUPERSEDED", "eviction superseded by concurrent state transition")
	}

	var p3Active, p3Reserved int
	if err := tx2.QueryRow(ctx, `
		SELECT COUNT(*) FILTER (WHERE pin_state = 'active'),
		       COUNT(*) FILTER (WHERE pin_state = 'reserved')
		  FROM station.model_ref_pins
		 WHERE station_id = $1 AND sha256 = $2 AND pin_state IN ('active', 'reserved')
	`, s.StationID, sha256Hex).Scan(&p3Active, &p3Reserved); err != nil {
		return EvictResponse{}, err
	}
	if p3Active > 0 || p3Reserved > 0 {
		return EvictResponse{}, &Error{
			Status:       409,
			Code:         "MODEL_IN_USE",
			Message:      "model pins appeared during eviction barrier",
			ActivePins:   p3Active,
			ReservedPins: p3Reserved,
		}
	}

	_, err = tx2.Exec(ctx, `
		UPDATE station.model_cache_blobs
		   SET status = 'not_installed',
		       active_evict_id = NULL,
		       bytes_downloaded = 0,
		       part_bytes_on_disk = 0,
		       speed_bps = 0,
		       eta_seconds = NULL,
		       etag = '',
		       published_views = '[]'::jsonb,
		       pending_views = '[]'::jsonb,
		       cas_inode = NULL,
		       verified_at = NULL,
		       last_error_code = '',
		       last_error_message = '',
		       lease_owner = '',
		       lease_expires_at = NULL,
		       updated_at = $5
		 WHERE station_id = $1 AND sha256 = $2
		   AND status = 'evicting' AND evict_fence = $3 AND active_evict_id = $4::uuid
	`, s.StationID, sha256Hex, myEvictFence, myOpID, now)
	if err != nil {
		return EvictResponse{}, err
	}

	var modelView *ModelStatusView
	if len(entries) > 0 {
		blob, aPins, rPins, qErr := s.queryBlobAndPins(ctx, tx2, sha256Hex, entries[0].SizeBytes)
		if qErr == nil {
			v := buildModelStatusView(entries[0], blob, aPins, rPins)
			modelView = &v
		}
	}
	if err := tx2.Commit(ctx); err != nil {
		return EvictResponse{}, err
	}
	return EvictResponse{Status: StatusNotInstalled, SHA256: sha256Hex, Model: modelView}, nil
}

func (s *PGStore) ReservePins(ctx context.Context, req ReservePinsRequest, now time.Time) (ReservePinsResponse, error) {
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return ReservePinsResponse{}, err
	}
	defer rollbackPgTx(tx)

	sortedSHAs := uniqueSortedSHAs(req.SHA256List)
	for _, sha := range sortedSHAs {
		var status string
		err := tx.QueryRow(ctx, `
			SELECT status
			  FROM station.model_cache_blobs
			 WHERE station_id = $1 AND sha256 = $2
			   FOR UPDATE
		`, s.StationID, sha).Scan(&status)
		if errors.Is(err, pgx.ErrNoRows) || status != StatusReady {
			return ReservePinsResponse{}, NewError(409, "MODEL_NOT_READY", fmt.Sprintf("model %s is not ready (status=%s)", sha, status))
		}
		if err != nil {
			return ReservePinsResponse{}, err
		}
	}

	ttl := time.Duration(req.TTLSeconds) * time.Second
	if ttl <= 0 {
		ttl = 2 * time.Minute
	}
	exp := now.Add(ttl)
	expStr := exp.UTC().Format(time.RFC3339)

	pins := make([]PinRecord, 0, len(sortedSHAs))
	for _, sha := range sortedSHAs {
		var existing PinRecord
		var existingExp *time.Time
		err := tx.QueryRow(ctx, `
			SELECT pin_id::text, request_sha256, pin_state, expires_at
			  FROM station.model_ref_pins
			 WHERE station_id = $1 AND holder_app_id = $2 AND reservation_id = $3 AND sha256 = $4
			   FOR UPDATE
		`, s.StationID, req.HolderAppID, req.ReservationID, sha).Scan(&existing.PinID, &existing.RequestSHA256, &existing.PinState, &existingExp)
		if err == nil {
			if existing.RequestSHA256 != req.RequestSHA256 {
				return ReservePinsResponse{}, NewError(409, "IDEMPOTENCY_CONFLICT", "reservation_id reused with different request_sha256")
			}
			if existing.PinState == PinStateReleased {
				return ReservePinsResponse{}, NewError(409, "ADMISSION_RESERVATION_EXPIRED", "reservation_id was already revoked or released")
			}
			var eStr *string
			if existingExp != nil {
				ts := existingExp.UTC().Format(time.RFC3339)
				eStr = &ts
			}
			pins = append(pins, PinRecord{
				PinID:          existing.PinID,
				SHA256:         sha,
				HolderAppID:    req.HolderAppID,
				ReservationID:  req.ReservationID,
				OperationID:    req.OperationID,
				ScopeKind:      req.ScopeKind,
				OwnerID:        req.OwnerID,
				OrganizationID: req.OrganizationID,
				ProjectID:      req.ProjectID,
				RequestSHA256:  req.RequestSHA256,
				PinState:       existing.PinState,
				ExpiresAt:      eStr,
			})
			continue
		}
		if !errors.Is(err, pgx.ErrNoRows) {
			return ReservePinsResponse{}, err
		}

		pinUUID, uErr := uuid.NewV7()
		if uErr != nil {
			return ReservePinsResponse{}, uErr
		}
		_, err = tx.Exec(ctx, `
			INSERT INTO station.model_ref_pins (
				pin_id, station_id, sha256, holder_app_id, reservation_id, operation_id,
				scope_kind, owner_id, organization_id, project_id, request_sha256,
				pin_state, expires_at, created_at, updated_at
			) VALUES (
				$1, $2, $3, $4, $5, $6,
				$7, $8, $9, $10, $11,
				'reserved', $12, $13, $13
			)
		`, pinUUID.String(), s.StationID, sha, req.HolderAppID, req.ReservationID, req.OperationID,
			req.ScopeKind, req.OwnerID, req.OrganizationID, req.ProjectID, req.RequestSHA256, exp, now)
		if err != nil {
			return ReservePinsResponse{}, err
		}
		pins = append(pins, PinRecord{
			PinID:          pinUUID.String(),
			SHA256:         sha,
			HolderAppID:    req.HolderAppID,
			ReservationID:  req.ReservationID,
			OperationID:    req.OperationID,
			ScopeKind:      req.ScopeKind,
			OwnerID:        req.OwnerID,
			OrganizationID: req.OrganizationID,
			ProjectID:      req.ProjectID,
			RequestSHA256:  req.RequestSHA256,
			PinState:       PinStateReserved,
			ExpiresAt:      &expStr,
		})
	}

	if err := tx.Commit(ctx); err != nil {
		return ReservePinsResponse{}, err
	}
	state := PinStateReserved
	if len(pins) > 0 && pins[0].PinState == PinStateActive {
		state = PinStateActive
	}
	return ReservePinsResponse{
		ReservationID: req.ReservationID,
		PinState:      state,
		Pins:          pins,
	}, nil
}

func (s *PGStore) CommitPins(ctx context.Context, req CommitPinsRequest, now time.Time) (CommitPinsResponse, error) {
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return CommitPinsResponse{}, err
	}
	defer rollbackPgTx(tx)

	rows, err := tx.Query(ctx, `
		SELECT pin_state
		  FROM station.model_ref_pins
		 WHERE station_id = $1 AND holder_app_id = $2 AND reservation_id = $3
		   FOR UPDATE
	`, s.StationID, req.HolderAppID, req.ReservationID)
	if err != nil {
		return CommitPinsResponse{}, err
	}
	var states []string
	for rows.Next() {
		var st string
		if err := rows.Scan(&st); err != nil {
			rows.Close()
			return CommitPinsResponse{}, err
		}
		states = append(states, st)
	}
	rows.Close()
	if len(states) == 0 {
		return CommitPinsResponse{}, NewError(404, "RESERVATION_NOT_FOUND", "reservation_id not found")
	}
	for _, st := range states {
		if st == PinStateReleased {
			return CommitPinsResponse{}, NewError(409, "ADMISSION_RESERVATION_EXPIRED", "reservation_id was already released")
		}
	}

	tag, err := tx.Exec(ctx, `
		UPDATE station.model_ref_pins
		   SET pin_state = 'active',
		       owner_id = CASE WHEN $4 <> '' THEN $4 ELSE owner_id END,
		       expires_at = NULL,
		       committed_at = COALESCE(committed_at, $5),
		       updated_at = $5
		 WHERE station_id = $1 AND holder_app_id = $2 AND reservation_id = $3 AND pin_state IN ('reserved', 'active')
	`, s.StationID, req.HolderAppID, req.ReservationID, req.OwnerID, now)
	if err != nil {
		return CommitPinsResponse{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return CommitPinsResponse{}, err
	}
	return CommitPinsResponse{
		ReservationID: req.ReservationID,
		PinState:      PinStateActive,
		UpdatedCount:  int(tag.RowsAffected()),
	}, nil
}

func (s *PGStore) ReleasePins(ctx context.Context, req ReleasePinsRequest, now time.Time) (ReleasePinsResponse, error) {
	tag, err := s.Pool.Exec(ctx, `
		UPDATE station.model_ref_pins
		   SET pin_state = 'released',
		       release_reason = $4,
		       released_at = COALESCE(released_at, $5),
		       updated_at = $5
		 WHERE station_id = $1 AND holder_app_id = $2 AND reservation_id = $3 AND pin_state IN ('reserved', 'active')
	`, s.StationID, req.HolderAppID, req.ReservationID, req.Reason, now)
	if err != nil {
		return ReleasePinsResponse{}, err
	}
	return ReleasePinsResponse{
		ReservationID: req.ReservationID,
		PinState:      PinStateReleased,
		ReleasedCount: int(tag.RowsAffected()),
	}, nil
}

func (s *PGStore) TouchPinGroupReconcileChecked(ctx context.Context, holderAppID, reservationID string, checkedAt time.Time) error {
	_, err := s.Pool.Exec(ctx, `
		UPDATE station.model_ref_pins
		   SET last_reconcile_checked_at = $4
		 WHERE station_id = $1
		   AND holder_app_id = $2
		   AND reservation_id = $3
		   AND pin_state IN ('reserved', 'active')
	`, s.StationID, holderAppID, reservationID, checkedAt)
	return err
}

func (s *PGStore) ListExpiredReservedGroups(ctx context.Context, now time.Time, limit int) ([]ExpiredReservationGroup, error) {
	if limit <= 0 {
		limit = 50
	}
	rows, err := s.Pool.Query(ctx, `
		SELECT holder_app_id, reservation_id
		  FROM (
		      SELECT holder_app_id,
		             reservation_id,
		             MIN(COALESCE(last_reconcile_checked_at, '1970-01-01T00:00:00Z'::timestamptz)) AS sort_checked_at
		        FROM station.model_ref_pins
		       WHERE station_id = $1
		         AND pin_state = 'reserved'
		         AND expires_at IS NOT NULL
		         AND expires_at <= $2
		       GROUP BY holder_app_id, reservation_id
		  ) AS due_groups
		 ORDER BY sort_checked_at ASC, reservation_id ASC
		 LIMIT $3
	`, s.StationID, now, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []ExpiredReservationGroup
	for rows.Next() {
		var g ExpiredReservationGroup
		if err := rows.Scan(&g.HolderAppID, &g.ReservationID); err != nil {
			return nil, err
		}
		out = append(out, g)
	}
	return out, rows.Err()
}

func (s *PGStore) ListStaleActiveTaskGroups(ctx context.Context, olderThan time.Time, limit int) ([]ExpiredReservationGroup, error) {
	if limit <= 0 {
		limit = 50
	}
	rows, err := s.Pool.Query(ctx, `
		SELECT holder_app_id, reservation_id
		  FROM (
		      SELECT holder_app_id,
		             reservation_id,
		             MIN(COALESCE(last_reconcile_checked_at, committed_at, created_at)) AS sort_checked_at
		        FROM station.model_ref_pins
		       WHERE station_id = $1
		         AND pin_state = 'active'
		         AND scope_kind = 'task_execution'
		       GROUP BY holder_app_id, reservation_id
		  ) AS active_groups
		 WHERE sort_checked_at <= $2
		 ORDER BY sort_checked_at ASC, reservation_id ASC
		 LIMIT $3
	`, s.StationID, olderThan, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []ExpiredReservationGroup
	for rows.Next() {
		var g ExpiredReservationGroup
		if err := rows.Scan(&g.HolderAppID, &g.ReservationID); err != nil {
			return nil, err
		}
		out = append(out, g)
	}
	return out, rows.Err()
}

func uniqueSortedSHAs(in []string) []string {
	cp := make([]string, 0, len(in))
	seen := map[string]bool{}
	for _, s := range in {
		clean := strings.ToLower(strings.TrimSpace(s))
		if clean != "" && !seen[clean] {
			seen[clean] = true
			cp = append(cp, clean)
		}
	}
	sort.Strings(cp)
	return cp
}

// ============================================================================
// Deterministic Row-Locked MemoryStore (Offline COMFY-P-B Unit Tests, Zero Docker)
// ============================================================================

type MemoryStore struct {
	mu               sync.Mutex
	storageMu        sync.Mutex
	blobLocks        map[string]*sync.Mutex
	catalogByID      map[string]CatalogEntry
	blobsBySHA       map[string]*memBlob
	pinsByKey        map[string]*memPin
	idemByKey        map[string]StoredIdempotency
	reservedHeadroom int64
	maxAutoPull      int64
}

type memBlob struct {
	BlobState
	LeaseOwner     string
	LeaseExpiresAt *time.Time
}

type memPin struct {
	PinRecord
	ExpiresAtTime          *time.Time
	UpdatedAt              time.Time
	LastReconcileCheckedAt *time.Time
	ReleaseReason          string
}

func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		blobLocks:        map[string]*sync.Mutex{},
		catalogByID:      map[string]CatalogEntry{},
		blobsBySHA:       map[string]*memBlob{},
		pinsByKey:        map[string]*memPin{},
		idemByKey:        map[string]StoredIdempotency{},
		reservedHeadroom: DefaultReservedHeadroomBytes,
		maxAutoPull:      DefaultMaxAutoPullModelBytes,
	}
}

func (m *MemoryStore) blobLock(sha string) *sync.Mutex {
	m.mu.Lock()
	defer m.mu.Unlock()
	lk, ok := m.blobLocks[sha]
	if !ok {
		lk = &sync.Mutex{}
		m.blobLocks[sha] = lk
	}
	return lk
}

func (m *MemoryStore) UpsertCatalogEntries(_ context.Context, entries []CatalogEntry) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, raw := range entries {
		e := raw
		if err := e.NormalizeAndValidate(); err != nil {
			return err
		}
		for id, existing := range m.catalogByID {
			if id != e.ModelID && existing.CatalogCode == e.CatalogCode {
				return NewError(409, "CATALOG_CODE_CONFLICT", "duplicate catalog_code")
			}
		}
		m.catalogByID[e.ModelID] = e
		if !e.PaidAccess && e.LicenseTier != LicenseTierPaidForbidden {
			if _, ok := m.blobsBySHA[e.SHA256]; !ok {
				m.blobsBySHA[e.SHA256] = &memBlob{
					BlobState: BlobState{
						SHA256:    e.SHA256,
						SizeBytes: e.SizeBytes,
						Status:    StatusNotInstalled,
					},
				}
			}
		}
	}
	return nil
}

func (m *MemoryStore) GetCatalogEntry(_ context.Context, modelID string) (CatalogEntry, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	e, ok := m.catalogByID[modelID]
	return e, ok, nil
}

func (m *MemoryStore) GetCatalogEntriesBySHA256(_ context.Context, sha256Hex string) ([]CatalogEntry, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []CatalogEntry
	for _, e := range m.catalogByID {
		if e.SHA256 == sha256Hex {
			out = append(out, e)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].CatalogCode < out[j].CatalogCode })
	return out, nil
}

func (m *MemoryStore) countPinsLocked(sha string) (int, int) {
	var active, reserved int
	for _, p := range m.pinsByKey {
		if p.SHA256 != sha {
			continue
		}
		switch p.PinState {
		case PinStateActive:
			active++
		case PinStateReserved:
			reserved++
		}
	}
	return active, reserved
}

func (m *MemoryStore) snapshotBlobLocked(sha string, fallbackSize int64) BlobState {
	b, ok := m.blobsBySHA[sha]
	if !ok {
		return BlobState{SHA256: sha, SizeBytes: fallbackSize, Status: StatusNotInstalled}
	}
	cp := b.BlobState
	cp.PublishedViews = append([]string(nil), b.PublishedViews...)
	cp.PendingViews = append([]string(nil), b.PendingViews...)
	return cp
}

func (m *MemoryStore) GetModelStatus(_ context.Context, modelID string) (ModelStatusView, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	e, ok := m.catalogByID[modelID]
	if !ok {
		return ModelStatusView{}, NewError(404, "MODEL_NOT_FOUND", "model_id not found in Station catalog")
	}
	blob := m.snapshotBlobLocked(e.SHA256, e.SizeBytes)
	active, reserved := m.countPinsLocked(e.SHA256)
	return buildModelStatusView(e, blob, active, reserved), nil
}

func (m *MemoryStore) ListModels(_ context.Context, filter ListFilter) (ListPage, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	limit := filter.Limit
	if limit <= 0 {
		limit = 25
	}
	if limit > 100 {
		limit = 100
	}
	entries := make([]CatalogEntry, 0, len(m.catalogByID))
	for _, e := range m.catalogByID {
		if filter.Category != "" && e.Category != filter.Category {
			continue
		}
		if filter.Cursor != "" && e.CatalogCode <= filter.Cursor {
			continue
		}
		if filter.Query != "" {
			q := strings.ToLower(filter.Query)
			if !strings.Contains(strings.ToLower(e.CatalogCode), q) &&
				!strings.Contains(strings.ToLower(e.ModelID), q) &&
				!strings.Contains(strings.ToLower(e.DisplayName), q) {
				continue
			}
		}
		entries = append(entries, e)
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].CatalogCode < entries[j].CatalogCode })

	models := make([]ModelStatusView, 0, limit)
	var nextCursor string
	for _, e := range entries {
		blob := m.snapshotBlobLocked(e.SHA256, e.SizeBytes)
		active, reserved := m.countPinsLocked(e.SHA256)
		view := buildModelStatusView(e, blob, active, reserved)
		if filter.Status != "" && view.Status != filter.Status {
			continue
		}
		if len(models) == limit {
			nextCursor = models[len(models)-1].CatalogCode
			break
		}
		models = append(models, view)
	}
	return ListPage{Models: models, NextCursor: nextCursor}, nil
}

func idemCompositeKey(orgID, projectID, principalID, targetAlias, key string) string {
	return orgID + "|" + projectID + "|" + principalID + "|" + normalizeTargetAlias(targetAlias) + "|" + key
}

func (m *MemoryStore) BeginEnsureIdempotency(_ context.Context, req StoredIdempotency, now time.Time) (*StoredIdempotency, string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	alias := normalizeTargetAlias(req.TargetAlias)
	composite := idemCompositeKey(req.OrganizationID, req.ProjectID, req.PrincipalID, alias, req.IdempotencyKey)
	if existing, ok := m.idemByKey[composite]; ok && existing.ExpiresAt.After(now) {
		if existing.RequestSHA256 != req.RequestSHA256 {
			return nil, "", NewError(409, "IDEMPOTENCY_CONFLICT", "idempotency_key reused with different model.ensure parameters")
		}
		if existing.State == IdempotencyStateInProgress {
			return nil, "", NewError(409, "OPERATION_IN_PROGRESS", "concurrent model.ensure with identical idempotency_key is still in progress")
		}
		cp := existing
		cp.ResponseJSON = append([]byte(nil), existing.ResponseJSON...)
		return &cp, "", nil
	}

	claimUUID, uErr := uuid.NewV7()
	if uErr != nil {
		return nil, "", uErr
	}
	claimID := claimUUID.String()
	m.idemByKey[composite] = StoredIdempotency{
		OrganizationID: req.OrganizationID,
		ProjectID:      req.ProjectID,
		PrincipalID:    req.PrincipalID,
		TargetAlias:    alias,
		IdempotencyKey: req.IdempotencyKey,
		ClaimID:        claimID,
		State:          IdempotencyStateInProgress,
		RequestSHA256:  req.RequestSHA256,
		ModelID:        req.ModelID,
		SHA256:         req.SHA256,
		ExpiresAt:      now.Add(5 * time.Minute),
	}
	return nil, claimID, nil
}

func (m *MemoryStore) CompleteEnsureIdempotency(_ context.Context, rec StoredIdempotency) error {
	if strings.TrimSpace(rec.ClaimID) == "" {
		return NewError(409, "IDEMPOTENCY_CLAIM_SUPERSEDED", "missing idempotency claim_id on completion")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	alias := normalizeTargetAlias(rec.TargetAlias)
	composite := idemCompositeKey(rec.OrganizationID, rec.ProjectID, rec.PrincipalID, alias, rec.IdempotencyKey)
	cur, ok := m.idemByKey[composite]
	if !ok || cur.State != IdempotencyStateInProgress || cur.ClaimID != rec.ClaimID || cur.RequestSHA256 != rec.RequestSHA256 {
		return NewError(409, "IDEMPOTENCY_CLAIM_SUPERSEDED", "idempotency reservation was superseded before completion")
	}
	cp := rec
	cp.TargetAlias = alias
	cp.State = IdempotencyStateCompleted
	cp.ResponseJSON = append([]byte(nil), rec.ResponseJSON...)
	m.idemByKey[composite] = cp
	return nil
}

func (m *MemoryStore) AbortEnsureIdempotency(_ context.Context, orgID, projectID, principalID, targetAlias, key, claimID, requestSHA256 string) error {
	if strings.TrimSpace(claimID) == "" {
		return nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	composite := idemCompositeKey(orgID, projectID, principalID, targetAlias, key)
	if cur, ok := m.idemByKey[composite]; ok && cur.State == IdempotencyStateInProgress && cur.ClaimID == claimID && cur.RequestSHA256 == requestSHA256 {
		delete(m.idemByKey, composite)
	}
	return nil
}

func (m *MemoryStore) AdmitOrJoinDownload(_ context.Context, req AdmitDownloadInput) (AdmitDownloadOutcome, error) {
	m.storageMu.Lock()
	defer m.storageMu.Unlock()

	blk := m.blobLock(req.Entry.SHA256)
	blk.Lock()
	defer blk.Unlock()

	m.mu.Lock()
	defer m.mu.Unlock()

	headroom := req.ReservedHeadroomBytes
	if headroom < DefaultReservedHeadroomBytes {
		headroom = DefaultReservedHeadroomBytes
	}
	maxAuto := req.MaxAutoPullBytes
	if maxAuto <= 0 || maxAuto > DefaultMaxAutoPullModelBytes {
		maxAuto = DefaultMaxAutoPullModelBytes
	}

	b, ok := m.blobsBySHA[req.Entry.SHA256]
	if !ok {
		b = &memBlob{
			BlobState: BlobState{
				SHA256:    req.Entry.SHA256,
				SizeBytes: req.Entry.SizeBytes,
				Status:    StatusNotInstalled,
			},
		}
		m.blobsBySHA[req.Entry.SHA256] = b
	}

	if b.Status == StatusEvicting {
		return AdmitDownloadOutcome{}, NewError(409, "MODEL_NOT_READY", "model blob is currently being evicted")
	}
	activePins, reservedPins := m.countPinsLocked(req.Entry.SHA256)
	if b.Status == StatusReady {
		return AdmitDownloadOutcome{
			AlreadyReady: true,
			Fence:        b.DownloadFence,
			View:         buildModelStatusView(req.Entry, m.snapshotBlobLocked(req.Entry.SHA256, req.Entry.SizeBytes), activePins, reservedPins),
		}, nil
	}

	relView, relErr := RelativeViewPath(req.Entry.Category, req.Entry.SHA256, req.Entry.Filename)
	if relErr != nil {
		return AdmitDownloadOutcome{}, relErr
	}

	inFlight := b.Status == StatusQueued || b.Status == StatusDownloading || b.Status == StatusVerifyingSHA256
	if inFlight && !req.AllowTakeoverActive && b.LeaseExpiresAt != nil && b.LeaseExpiresAt.After(req.Now) {
		b.PendingViews = appendUniqueString(b.PendingViews, relView)
		b.UpdatedAt = req.Now
		return AdmitDownloadOutcome{
			JoinedActive: true,
			Fence:        b.DownloadFence,
			View:         buildModelStatusView(req.Entry, m.snapshotBlobLocked(req.Entry.SHA256, req.Entry.SizeBytes), activePins, reservedPins),
		}, nil
	}

	if req.Entry.SizeBytes > maxAuto && !largeModelApproved(req.Entry.SourcePolicy) {
		return AdmitDownloadOutcome{
			ApprovalRequired: true,
			ApprovalReason:   fmt.Sprintf("model size_bytes %d exceeds automatic pull cap %d", req.Entry.SizeBytes, maxAuto),
			Fence:            b.DownloadFence,
			View:             buildModelStatusView(req.Entry, m.snapshotBlobLocked(req.Entry.SHA256, req.Entry.SizeBytes), activePins, reservedPins),
		}, nil
	}

	// Step A (inside storageMu): compute unmaterializedOther FIRST (T32-M01).
	var unmaterializedOther int64
	for sha, other := range m.blobsBySHA {
		if sha == req.Entry.SHA256 {
			continue
		}
		if other.Status == StatusQueued || other.Status == StatusDownloading || other.Status == StatusVerifyingSHA256 {
			rem := other.SizeBytes - other.PartBytesOnDisk
			if rem > 0 {
				unmaterializedOther += rem
			}
		}
	}

	// Step B (inside storageMu): sample statvfs SECOND.
	_, fsFree, statErr := sampleStorageCapacity(req)
	if statErr != nil {
		return AdmitDownloadOutcome{}, statErr
	}

	reusablePart := b.PartBytesOnDisk
	if b.ETag == "" || reusablePart < 0 || reusablePart >= req.Entry.SizeBytes {
		reusablePart = 0
	}
	prevWriterStopped := !inFlight && b.Status == StatusFailed
	reusableDiskCredit := reusablePart
	if !prevWriterStopped {
		reusableDiskCredit = 0
	}
	neededNew := req.Entry.SizeBytes - reusableDiskCredit
	remainingAfter := fsFree - unmaterializedOther - neededNew
	if remainingAfter < headroom {
		return AdmitDownloadOutcome{
			ApprovalRequired: true,
			ApprovalReason:   fmt.Sprintf("insufficient free disk headroom after reservation (%d < %d)", remainingAfter, headroom),
			Fence:            b.DownloadFence,
			View:             buildModelStatusView(req.Entry, m.snapshotBlobLocked(req.Entry.SHA256, req.Entry.SizeBytes), activePins, reservedPins),
		}, nil
	}

	prevFence := b.DownloadFence
	b.DownloadFence++
	b.Status = StatusQueued
	b.BytesDownloaded = reusablePart
	b.PartBytesOnDisk = reusableDiskCredit
	b.SpeedBps = 0
	b.ETASeconds = nil
	b.PendingViews = appendUniqueString(b.PendingViews, relView)
	b.LastErrorCode = ""
	b.LastErrorMessage = ""
	b.LeaseOwner = req.LeaseOwner
	exp := req.Now.Add(req.LeaseTTL)
	b.LeaseExpiresAt = &exp
	b.UpdatedAt = req.Now

	return AdmitDownloadOutcome{
		AllowInodeHandoff: prevWriterStopped,
		PrevFence:         prevFence,
		Fence:             b.DownloadFence,
		ResumeETag:        b.ETag,
		ExistingPartBytes: reusablePart,
		View:              buildModelStatusView(req.Entry, m.snapshotBlobLocked(req.Entry.SHA256, req.Entry.SizeBytes), activePins, reservedPins),
	}, nil
}

func (m *MemoryStore) TransitionFenceStatus(_ context.Context, sha256Hex string, fence int64, status string, now time.Time) (bool, error) {
	blk := m.blobLock(sha256Hex)
	blk.Lock()
	defer blk.Unlock()

	m.mu.Lock()
	defer m.mu.Unlock()
	b, ok := m.blobsBySHA[sha256Hex]
	if !ok || b.DownloadFence != fence || b.Status == StatusReady || b.Status == StatusEvicting {
		return false, nil
	}
	b.Status = status
	exp := now.Add(5 * time.Minute)
	b.LeaseExpiresAt = &exp
	b.UpdatedAt = now
	return true, nil
}

func (m *MemoryStore) UpdateFenceProgress(_ context.Context, sha256Hex string, fence int64, bytesDownloaded, partBytes, speedBps int64, etaSeconds *int, etag string, now time.Time) (bool, error) {
	blk := m.blobLock(sha256Hex)
	blk.Lock()
	defer blk.Unlock()

	m.mu.Lock()
	defer m.mu.Unlock()
	b, ok := m.blobsBySHA[sha256Hex]
	if !ok || b.DownloadFence != fence || (b.Status != StatusQueued && b.Status != StatusDownloading) {
		return false, nil
	}
	b.Status = StatusDownloading
	if bytesDownloaded > b.BytesDownloaded {
		b.BytesDownloaded = bytesDownloaded
	}
	b.PartBytesOnDisk = partBytes
	b.SpeedBps = speedBps
	b.ETASeconds = etaSeconds
	if etag != "" {
		b.ETag = etag
	}
	exp := now.Add(5 * time.Minute)
	b.LeaseExpiresAt = &exp
	b.UpdatedAt = now
	return true, nil
}

func (m *MemoryStore) FailFenceDownload(_ context.Context, sha256Hex string, fence int64, code, message string, keepPartBytes int64, etag string, now time.Time) (bool, error) {
	m.storageMu.Lock()
	defer m.storageMu.Unlock()

	blk := m.blobLock(sha256Hex)
	blk.Lock()
	defer blk.Unlock()

	m.mu.Lock()
	defer m.mu.Unlock()
	b, ok := m.blobsBySHA[sha256Hex]
	if !ok || b.DownloadFence != fence || b.Status == StatusReady || b.Status == StatusEvicting {
		return false, nil
	}
	b.Status = StatusFailed
	b.BytesDownloaded = keepPartBytes
	b.PartBytesOnDisk = keepPartBytes
	b.SpeedBps = 0
	b.ETASeconds = nil
	b.ETag = etag
	b.LastErrorCode = code
	b.LastErrorMessage = message
	b.LeaseOwner = ""
	b.LeaseExpiresAt = nil
	b.UpdatedAt = now
	return true, nil
}

func (m *MemoryStore) PublishUnderFenceLock(_ context.Context, entry CatalogEntry, fence int64, now time.Time, publishFn func(viewsToPublish []string) (uint64, []string, error)) (ModelStatusView, bool, error) {
	m.storageMu.Lock()
	defer m.storageMu.Unlock()

	blk := m.blobLock(entry.SHA256)
	blk.Lock()
	defer blk.Unlock()

	entryView, vErr := RelativeViewPath(entry.Category, entry.SHA256, entry.Filename)
	if vErr != nil {
		return ModelStatusView{}, false, vErr
	}

	m.mu.Lock()
	b, ok := m.blobsBySHA[entry.SHA256]
	if !ok || b.DownloadFence != fence || b.Status == StatusEvicting || b.Status == StatusReady {
		m.mu.Unlock()
		return ModelStatusView{}, false, nil
	}
	toPublish := append([]string(nil), b.PendingViews...)
	toPublish = appendUniqueString(toPublish, entryView)
	m.mu.Unlock()

	// Execute filesystem RENAME_NOREPLACE while holding storageMu + per-blob row lock!
	casIno, publishedNow, err := publishFn(toPublish)
	if err != nil {
		return ModelStatusView{}, false, err
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	b.Status = StatusReady
	b.BytesDownloaded = b.SizeBytes
	b.PartBytesOnDisk = 0
	b.SpeedBps = 0
	zero := 0
	b.ETASeconds = &zero
	for _, rv := range publishedNow {
		b.PublishedViews = appendUniqueString(b.PublishedViews, rv)
	}
	b.PendingViews = nil
	b.CASInode = &casIno
	ts := now
	b.VerifiedAt = &ts
	b.LastErrorCode = ""
	b.LastErrorMessage = ""
	b.LeaseOwner = ""
	b.LeaseExpiresAt = nil
	b.UpdatedAt = now

	active, reserved := m.countPinsLocked(entry.SHA256)
	return buildModelStatusView(entry, m.snapshotBlobLocked(entry.SHA256, entry.SizeBytes), active, reserved), true, nil
}

func (m *MemoryStore) PublishViewForReadyBlob(_ context.Context, entry CatalogEntry, now time.Time, publishFn func() (uint64, string, error)) (ModelStatusView, bool, error) {
	blk := m.blobLock(entry.SHA256)
	blk.Lock()
	defer blk.Unlock()

	m.mu.Lock()
	b, ok := m.blobsBySHA[entry.SHA256]
	if !ok || b.Status != StatusReady {
		m.mu.Unlock()
		return ModelStatusView{}, false, nil
	}
	m.mu.Unlock()

	casIno, relView, err := publishFn()
	if err != nil {
		return ModelStatusView{}, false, err
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	b.PublishedViews = appendUniqueString(b.PublishedViews, relView)
	b.CASInode = &casIno
	b.UpdatedAt = now
	active, reserved := m.countPinsLocked(entry.SHA256)
	return buildModelStatusView(entry, m.snapshotBlobLocked(entry.SHA256, entry.SizeBytes), active, reserved), true, nil
}

func (m *MemoryStore) EvictUnderLock(ctx context.Context, sha256Hex string, now time.Time, unlinkFn func(relViews []string, expectedCASInode *uint64, downloadFence int64) error) (EvictResponse, error) {
	blk := m.blobLock(sha256Hex)
	blk.Lock()
	var relViews []string
	var entries []CatalogEntry

	m.mu.Lock()
	for _, e := range m.catalogByID {
		if e.SHA256 == sha256Hex {
			entries = append(entries, e)
		}
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].CatalogCode < entries[j].CatalogCode })
	b, ok := m.blobsBySHA[sha256Hex]
	if !ok {
		m.mu.Unlock()
		blk.Unlock()
		if len(entries) == 0 {
			return EvictResponse{}, NewError(404, "MODEL_NOT_FOUND", "model sha256 not found in catalog")
		}
		v := buildModelStatusView(entries[0], BlobState{SHA256: sha256Hex, SizeBytes: entries[0].SizeBytes, Status: StatusNotInstalled}, 0, 0)
		return EvictResponse{Status: StatusNotInstalled, SHA256: sha256Hex, Model: &v}, nil
	}
	if b.Status == StatusEvicting {
		m.mu.Unlock()
		blk.Unlock()
		return EvictResponse{}, NewError(409, "OPERATION_IN_PROGRESS", "model eviction is already in progress")
	}
	activePins, reservedPins := m.countPinsLocked(sha256Hex)
	if activePins > 0 || reservedPins > 0 {
		m.mu.Unlock()
		blk.Unlock()
		return EvictResponse{}, &Error{
			Status:       409,
			Code:         "MODEL_IN_USE",
			Message:      fmt.Sprintf("model is pinned (active=%d, reserved=%d) and cannot be evicted", activePins, reservedPins),
			ActivePins:   activePins,
			ReservedPins: reservedPins,
		}
	}
	if b.Status == StatusQueued || b.Status == StatusDownloading || b.Status == StatusVerifyingSHA256 {
		m.mu.Unlock()
		blk.Unlock()
		return EvictResponse{}, NewError(409, "MODEL_IN_USE", "model download is currently in progress")
	}

	var expectedCASInode *uint64
	if b.CASInode != nil {
		cpIno := *b.CASInode
		expectedCASInode = &cpIno
	}

	relViews = append(relViews, b.PublishedViews...)
	for _, e := range entries {
		if rv, rErr := RelativeViewPath(e.Category, e.SHA256, e.Filename); rErr == nil {
			relViews = appendUniqueString(relViews, rv)
		}
	}
	opUUID, _ := uuid.NewV7()
	myOpID := opUUID.String()
	myDownloadFence := b.DownloadFence
	b.EvictFence++
	myEvictFence := b.EvictFence
	b.ActiveEvictID = myOpID
	b.Status = StatusEvicting
	b.UpdatedAt = now
	m.mu.Unlock()
	blk.Unlock()

	_ = ctx
	if unlinkFn != nil {
		if uErr := unlinkFn(relViews, expectedCASInode, myDownloadFence); uErr != nil {
			blk.Lock()
			m.mu.Lock()
			if b.Status == StatusEvicting && b.EvictFence == myEvictFence && b.ActiveEvictID == myOpID {
				b.Status = StatusFailed
				b.ActiveEvictID = ""
				b.LastErrorCode = "STORAGE_IO_ERROR"
				b.LastErrorMessage = "eviction unlink failed"
				b.UpdatedAt = now
			}
			m.mu.Unlock()
			blk.Unlock()
			return EvictResponse{}, uErr
		}
	}

	blk.Lock()
	defer blk.Unlock()
	m.mu.Lock()
	defer m.mu.Unlock()

	if b.Status != StatusEvicting || b.EvictFence != myEvictFence || b.ActiveEvictID != myOpID {
		return EvictResponse{}, NewError(409, "OPERATION_SUPERSEDED", "eviction superseded by concurrent state transition")
	}
	aPins, rPins := m.countPinsLocked(sha256Hex)
	if aPins > 0 || rPins > 0 {
		return EvictResponse{}, &Error{
			Status:       409,
			Code:         "MODEL_IN_USE",
			Message:      "model pins appeared during eviction barrier",
			ActivePins:   aPins,
			ReservedPins: rPins,
		}
	}

	// Monotonic download_fence and evict_fence are preserved untouched!
	b.Status = StatusNotInstalled
	b.ActiveEvictID = ""
	b.BytesDownloaded = 0
	b.PartBytesOnDisk = 0
	b.SpeedBps = 0
	b.ETASeconds = nil
	b.ETag = ""
	b.PublishedViews = nil
	b.PendingViews = nil
	b.CASInode = nil
	b.VerifiedAt = nil
	b.LastErrorCode = ""
	b.LastErrorMessage = ""
	b.LeaseOwner = ""
	b.LeaseExpiresAt = nil
	b.UpdatedAt = now

	var modelView *ModelStatusView
	if len(entries) > 0 {
		v := buildModelStatusView(entries[0], m.snapshotBlobLocked(sha256Hex, entries[0].SizeBytes), 0, 0)
		modelView = &v
	}
	return EvictResponse{Status: StatusNotInstalled, SHA256: sha256Hex, Model: modelView}, nil
}

func (m *MemoryStore) ReservePins(_ context.Context, req ReservePinsRequest, now time.Time) (ReservePinsResponse, error) {
	sortedSHAs := uniqueSortedSHAs(req.SHA256List)
	locks := make([]*sync.Mutex, 0, len(sortedSHAs))
	for _, sha := range sortedSHAs {
		lk := m.blobLock(sha)
		lk.Lock()
		locks = append(locks, lk)
	}
	defer func() {
		for i := len(locks) - 1; i >= 0; i-- {
			locks[i].Unlock()
		}
	}()

	m.mu.Lock()
	defer m.mu.Unlock()

	for _, sha := range sortedSHAs {
		b, ok := m.blobsBySHA[sha]
		st := StatusNotInstalled
		if ok {
			st = b.Status
		}
		if !ok || st != StatusReady {
			return ReservePinsResponse{}, NewError(409, "MODEL_NOT_READY", fmt.Sprintf("model %s is not ready (status=%s)", sha, st))
		}
	}

	ttl := time.Duration(req.TTLSeconds) * time.Second
	if ttl <= 0 {
		ttl = 2 * time.Minute
	}
	exp := now.Add(ttl)
	expStr := exp.UTC().Format(time.RFC3339)

	pins := make([]PinRecord, 0, len(sortedSHAs))
	for _, sha := range sortedSHAs {
		pk := req.HolderAppID + "|" + req.ReservationID + "|" + sha
		if existing, ok := m.pinsByKey[pk]; ok {
			if existing.RequestSHA256 != req.RequestSHA256 {
				return ReservePinsResponse{}, NewError(409, "IDEMPOTENCY_CONFLICT", "reservation_id reused with different request_sha256")
			}
			if existing.PinState == PinStateReleased {
				return ReservePinsResponse{}, NewError(409, "ADMISSION_RESERVATION_EXPIRED", "reservation_id was already revoked or released")
			}
			pins = append(pins, existing.PinRecord)
			continue
		}
		pinUUID, _ := uuid.NewV7()
		rec := PinRecord{
			PinID:          pinUUID.String(),
			SHA256:         sha,
			HolderAppID:    req.HolderAppID,
			ReservationID:  req.ReservationID,
			OperationID:    req.OperationID,
			ScopeKind:      req.ScopeKind,
			OwnerID:        req.OwnerID,
			OrganizationID: req.OrganizationID,
			ProjectID:      req.ProjectID,
			RequestSHA256:  req.RequestSHA256,
			PinState:       PinStateReserved,
			ExpiresAt:      &expStr,
		}
		expCopy := exp
		m.pinsByKey[pk] = &memPin{PinRecord: rec, ExpiresAtTime: &expCopy, UpdatedAt: now}
		pins = append(pins, rec)
	}

	state := PinStateReserved
	if len(pins) > 0 && pins[0].PinState == PinStateActive {
		state = PinStateActive
	}
	return ReservePinsResponse{
		ReservationID: req.ReservationID,
		PinState:      state,
		Pins:          pins,
	}, nil
}

func (m *MemoryStore) CommitPins(_ context.Context, req CommitPinsRequest, now time.Time) (CommitPinsResponse, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	var matched []*memPin
	for _, p := range m.pinsByKey {
		if p.HolderAppID == req.HolderAppID && p.ReservationID == req.ReservationID {
			matched = append(matched, p)
		}
	}
	if len(matched) == 0 {
		return CommitPinsResponse{}, NewError(404, "RESERVATION_NOT_FOUND", "reservation_id not found")
	}
	for _, p := range matched {
		if p.PinState == PinStateReleased {
			return CommitPinsResponse{}, NewError(409, "ADMISSION_RESERVATION_EXPIRED", "reservation_id was already released")
		}
	}
	for _, p := range matched {
		p.PinState = PinStateActive
		if req.OwnerID != "" {
			p.OwnerID = req.OwnerID
		}
		p.ExpiresAt = nil
		p.ExpiresAtTime = nil
		p.UpdatedAt = now
	}
	return CommitPinsResponse{
		ReservationID: req.ReservationID,
		PinState:      PinStateActive,
		UpdatedCount:  len(matched),
	}, nil
}

func (m *MemoryStore) ReleasePins(_ context.Context, req ReleasePinsRequest, now time.Time) (ReleasePinsResponse, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	released := 0
	for _, p := range m.pinsByKey {
		if p.HolderAppID == req.HolderAppID && p.ReservationID == req.ReservationID && (p.PinState == PinStateReserved || p.PinState == PinStateActive) {
			p.PinState = PinStateReleased
			p.ReleaseReason = req.Reason
			p.UpdatedAt = now
			released++
		}
	}
	return ReleasePinsResponse{
		ReservationID: req.ReservationID,
		PinState:      PinStateReleased,
		ReleasedCount: released,
	}, nil
}

func (m *MemoryStore) TouchPinGroupReconcileChecked(_ context.Context, holderAppID, reservationID string, checkedAt time.Time) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	cp := checkedAt
	for _, p := range m.pinsByKey {
		if p.HolderAppID == holderAppID && p.ReservationID == reservationID && (p.PinState == PinStateReserved || p.PinState == PinStateActive) {
			p.LastReconcileCheckedAt = &cp
		}
	}
	return nil
}

type memSortableGroup struct {
	group     ExpiredReservationGroup
	sortCheck time.Time
}

func (m *MemoryStore) ListExpiredReservedGroups(_ context.Context, now time.Time, limit int) ([]ExpiredReservationGroup, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if limit <= 0 {
		limit = 50
	}
	epochZero := time.Unix(0, 0).UTC()
	byKey := map[string]*memSortableGroup{}
	for _, p := range m.pinsByKey {
		if p.PinState == PinStateReserved && p.ExpiresAtTime != nil && !p.ExpiresAtTime.After(now) {
			chk := epochZero
			if p.LastReconcileCheckedAt != nil {
				chk = *p.LastReconcileCheckedAt
			}
			k := p.HolderAppID + "|" + p.ReservationID
			if cur, ok := byKey[k]; !ok || chk.Before(cur.sortCheck) {
				byKey[k] = &memSortableGroup{
					group: ExpiredReservationGroup{
						HolderAppID:   p.HolderAppID,
						ReservationID: p.ReservationID,
					},
					sortCheck: chk,
				}
			}
		}
	}
	items := make([]memSortableGroup, 0, len(byKey))
	for _, v := range byKey {
		items = append(items, *v)
	}
	sort.Slice(items, func(i, j int) bool {
		if !items[i].sortCheck.Equal(items[j].sortCheck) {
			return items[i].sortCheck.Before(items[j].sortCheck)
		}
		return items[i].group.ReservationID < items[j].group.ReservationID
	})
	if len(items) > limit {
		items = items[:limit]
	}
	out := make([]ExpiredReservationGroup, 0, len(items))
	for _, item := range items {
		out = append(out, item.group)
	}
	return out, nil
}

func (m *MemoryStore) ListStaleActiveTaskGroups(_ context.Context, olderThan time.Time, limit int) ([]ExpiredReservationGroup, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if limit <= 0 {
		limit = 50
	}
	byKey := map[string]*memSortableGroup{}
	for _, p := range m.pinsByKey {
		if p.PinState == PinStateActive && p.ScopeKind == PinScopeTaskExecution {
			chk := p.UpdatedAt
			if p.LastReconcileCheckedAt != nil {
				chk = *p.LastReconcileCheckedAt
			}
			k := p.HolderAppID + "|" + p.ReservationID
			if cur, ok := byKey[k]; !ok || chk.Before(cur.sortCheck) {
				byKey[k] = &memSortableGroup{
					group: ExpiredReservationGroup{
						HolderAppID:   p.HolderAppID,
						ReservationID: p.ReservationID,
					},
					sortCheck: chk,
				}
			}
		}
	}
	items := make([]memSortableGroup, 0, len(byKey))
	for _, v := range byKey {
		if !v.sortCheck.After(olderThan) {
			items = append(items, *v)
		}
	}
	sort.Slice(items, func(i, j int) bool {
		if !items[i].sortCheck.Equal(items[j].sortCheck) {
			return items[i].sortCheck.Before(items[j].sortCheck)
		}
		return items[i].group.ReservationID < items[j].group.ReservationID
	})
	if len(items) > limit {
		items = items[:limit]
	}
	out := make([]ExpiredReservationGroup, 0, len(items))
	for _, item := range items {
		out = append(out, item.group)
	}
	return out, nil
}
