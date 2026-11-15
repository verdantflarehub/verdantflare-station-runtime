package modelcache

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

var sensitiveQueryOrTokenPattern = regexp.MustCompile(`(?i)(bearer\s+\S+|token=[^&\s]+|key=[^&\s]+|password=[^&\s]+)`)

func sanitizePublicMessage(msg, rootDir string) string {
	out := strings.TrimSpace(msg)
	if rootDir != "" {
		out = strings.ReplaceAll(out, rootDir, "[MODEL_CACHE_ROOT]")
	}
	out = strings.ReplaceAll(out, "/data/models/comfyui", "[MODEL_CACHE_ROOT]")
	out = sensitiveQueryOrTokenPattern.ReplaceAllString(out, "[REDACTED]")
	if len(out) > 240 {
		out = out[:240]
	}
	return out
}

// Service coordinates model catalog gates, singleflight CAS downloads,
// RENAME_NOREPLACE view publication, and cross-DB two-phase reference pins.
type Service struct {
	Store                  Store
	StationID              string
	RootDir                string
	Downloader             *SafeDownloader
	DiskStat               DiskStatFunc
	Revoker                AppAdmissionRevoker
	UpstreamBearerToken    string
	ReservedHeadroomBytes  int64
	MaxAutoPullModelBytes  int64
	DownloadLeaseTTL        time.Duration
	ActiveTaskReconcileAge  time.Duration
	ReconcilePerRPCTimeout  time.Duration
	ReconcileStoreOpTimeout time.Duration
	Hook                    StageHook
	Now                     func() time.Time
	// SynchronousDownload waits for background downloads inline when true (useful in deterministic tests).
	SynchronousDownload bool

	reconcileRound uint64
	wg             sync.WaitGroup
}

func (s *Service) now() time.Time {
	if s != nil && s.Now != nil {
		return s.Now().UTC()
	}
	return time.Now().UTC()
}

func (s *Service) fireHook(ctx context.Context, stage string, meta map[string]string) {
	if s != nil && s.Hook != nil {
		s.Hook.OnStage(ctx, stage, meta)
	}
}

func (s *Service) diskStat() (int64, int64, error) {
	if s.DiskStat != nil {
		return s.DiskStat(s.RootDir)
	}
	return StatDiskSpace(s.RootDir)
}

func (s *Service) WaitBackgroundDownloads() {
	s.wg.Wait()
}

func computeEnsureRequestSHA256(req EnsureRequest) string {
	payload := struct {
		TargetAlias    string `json:"target_alias"`
		ModelID        string `json:"model_id"`
		CatalogCode    string `json:"catalog_code"`
		ExpectedSHA256 string `json:"expected_sha256"`
	}{
		TargetAlias:    normalizeTargetAlias(req.TargetAlias),
		ModelID:        strings.TrimSpace(req.ModelID),
		CatalogCode:    strings.TrimSpace(req.CatalogCode),
		ExpectedSHA256: strings.ToLower(strings.TrimSpace(req.ExpectedSHA256)),
	}
	raw, _ := json.Marshal(payload)
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}

func validIdentitySegment(v string) bool {
	return len(v) >= 1 && len(v) <= 128 && !strings.ContainsAny(v, "\r\n\t")
}

func (s *Service) RegisterCatalogEntries(ctx context.Context, entries []CatalogEntry) error {
	if s == nil || s.Store == nil {
		return NewError(503, "SERVICE_UNAVAILABLE", "model cache store not configured")
	}
	return s.Store.UpsertCatalogEntries(ctx, entries)
}

func (s *Service) ListModels(ctx context.Context, filter ListFilter) (ListPage, error) {
	if s == nil || s.Store == nil {
		return ListPage{}, NewError(503, "SERVICE_UNAVAILABLE", "model cache store not configured")
	}
	if filter.Category != "" {
		if _, ok := CategorySubdir(filter.Category); !ok {
			return ListPage{}, NewError(400, "INVALID_PARAMETERS", "invalid category filter")
		}
	}
	if filter.Status != "" {
		switch filter.Status {
		case StatusNotInstalled, StatusQueued, StatusDownloading, StatusVerifyingSHA256, StatusReady, StatusFailed, StatusEvicting:
		default:
			return ListPage{}, NewError(400, "INVALID_PARAMETERS", "invalid status filter")
		}
	}
	return s.Store.ListModels(ctx, filter)
}

func (s *Service) GetModelStatus(ctx context.Context, modelID string) (ModelStatusView, error) {
	if s == nil || s.Store == nil {
		return ModelStatusView{}, NewError(503, "SERVICE_UNAVAILABLE", "model cache store not configured")
	}
	if !ValidModelID(strings.TrimSpace(modelID)) {
		return ModelStatusView{}, NewError(400, "INVALID_PARAMETERS", "invalid model_id")
	}
	view, err := s.Store.GetModelStatus(ctx, strings.TrimSpace(modelID))
	if err != nil {
		return ModelStatusView{}, err
	}
	view.LastErrorMessage = sanitizePublicMessage(view.LastErrorMessage, s.RootDir)
	return view, nil
}

// Ensure validates caller- and alias-scoped atomic idempotency, license/paid_access redlines,
// synchronous expected_sha256 pre-verification, Stage-1 SSRF checks, and
// serialized non-double-counted disk headroom before queuing or joining a
// Station-wide singleflight CAS download.
func (s *Service) Ensure(ctx context.Context, req EnsureRequest) (EnsureResponse, error) {
	if s == nil || s.Store == nil || s.Downloader == nil || strings.TrimSpace(s.RootDir) == "" {
		return EnsureResponse{}, NewError(503, "SERVICE_UNAVAILABLE", "model cache provisioner is not configured")
	}
	req.OrganizationID = strings.TrimSpace(req.OrganizationID)
	req.ProjectID = strings.TrimSpace(req.ProjectID)
	req.PrincipalID = strings.TrimSpace(req.PrincipalID)
	req.TargetAlias = normalizeTargetAlias(req.TargetAlias)
	req.IdempotencyKey = strings.TrimSpace(req.IdempotencyKey)
	req.ModelID = strings.TrimSpace(req.ModelID)
	req.CatalogCode = strings.TrimSpace(req.CatalogCode)
	req.ExpectedSHA256 = strings.ToLower(strings.TrimSpace(req.ExpectedSHA256))

	if !validIdentitySegment(req.OrganizationID) ||
		!validIdentitySegment(req.ProjectID) ||
		!validIdentitySegment(req.PrincipalID) ||
		!validIdentitySegment(req.TargetAlias) ||
		!ValidIdempotencyKey(req.IdempotencyKey) ||
		!ValidModelID(req.ModelID) {
		return EnsureResponse{}, NewError(400, "INVALID_PARAMETERS", "missing or invalid model.ensure identity or parameters")
	}
	if req.CatalogCode != "" && !ValidCatalogCode(req.CatalogCode) {
		return EnsureResponse{}, NewError(400, "INVALID_PARAMETERS", "invalid catalog_code format")
	}
	if req.ExpectedSHA256 != "" && !ValidSHA256(req.ExpectedSHA256) {
		return EnsureResponse{}, NewError(400, "INVALID_PARAMETERS", "invalid expected_sha256 format")
	}

	now := s.now()
	reqSHA := computeEnsureRequestSHA256(req)

	// Step 0: Atomic caller + target_alias scoped idempotency claim with unforgeable claim_id (T32-M03, R2-M01).
	prev, claimID, err := s.Store.BeginEnsureIdempotency(ctx, StoredIdempotency{
		OrganizationID: req.OrganizationID,
		ProjectID:      req.ProjectID,
		PrincipalID:    req.PrincipalID,
		TargetAlias:    req.TargetAlias,
		IdempotencyKey: req.IdempotencyKey,
		RequestSHA256:  reqSHA,
		ModelID:        req.ModelID,
	}, now)
	if err != nil {
		return EnsureResponse{}, err
	}
	if prev != nil {
		var saved EnsureResponse
		_ = json.Unmarshal(prev.ResponseJSON, &saved)
		saved.HTTPStatus = prev.ResponseStatus
		saved.Deduplicated = true
		if entry, found, cErr := s.Store.GetCatalogEntry(ctx, req.ModelID); cErr == nil && found && !entry.PaidAccess && entry.LicenseTier != LicenseTierPaidForbidden {
			if liveView, vErr := s.GetModelStatus(ctx, req.ModelID); vErr == nil {
				if liveView.Status == StatusNotInstalled && saved.Code == "" {
					// Underlying CAS blob may have transitioned to ready via another alias; publish view if ready.
					if pubView, ok, pubErr := s.Store.PublishViewForReadyBlob(ctx, entry, now, func() (uint64, string, error) {
						return PublishCASAndView(s.RootDir, entry.SHA256, entry.Category, entry.Filename, entry.SizeBytes, liveView.DownloadFence, true)
					}); pubErr == nil && ok {
						liveView = pubView
					}
				}
				saved.Model = liveView
				if liveView.Status == StatusReady && saved.Code == "" {
					saved.HTTPStatus = 200
				}
			}
		}
		return saved, nil
	}

	idempotencyCompleted := false
	defer func() {
		if !idempotencyCompleted && claimID != "" {
			_ = s.Store.AbortEnsureIdempotency(context.Background(), req.OrganizationID, req.ProjectID, req.PrincipalID, req.TargetAlias, req.IdempotencyKey, claimID, reqSHA)
		}
	}()

	entry, found, err := s.Store.GetCatalogEntry(ctx, req.ModelID)
	if err != nil {
		return EnsureResponse{}, err
	}
	if !found {
		return EnsureResponse{}, NewError(404, "MODEL_NOT_FOUND", "model_id is not registered in Station catalog")
	}
	if req.CatalogCode != "" && req.CatalogCode != entry.CatalogCode {
		return EnsureResponse{}, NewError(400, "INVALID_PARAMETERS", "catalog_code does not match model_id catalog entry")
	}

	// Gate 1: Strict paid-access redline (S6 item 1) — synchronous 403, zero queuing.
	if entry.PaidAccess || entry.LicenseTier == LicenseTierPaidForbidden || ClassifyLicenseTier(entry.License, entry.PaidAccess, entry.SourcePolicy) == LicenseTierPaidForbidden {
		return EnsureResponse{}, NewError(403, "MODEL_PAID_ACCESS_FORBIDDEN", "paid-access or Early Access models are strictly forbidden")
	}

	// Gate 2: Synchronous expected_sha256 pre-verification (S6 item 3a) — never pollutes shared blob state.
	if req.ExpectedSHA256 != "" && req.ExpectedSHA256 != entry.SHA256 {
		return EnsureResponse{}, NewError(409, "MODEL_CATALOG_SHA256_MISMATCH", "expected_sha256 does not match authoritative catalog sha256")
	}

	// Gate 3: Controlled .safetensors filename & format check (S6 item 4).
	if err := ValidateModelFilename(entry.Filename); err != nil {
		return EnsureResponse{}, err
	}

	// Gate 4: License tier & local model approval gate (§4.3.1 item 1).
	if entry.ApprovalState == ApprovalRejected {
		return EnsureResponse{}, NewError(403, "MODEL_APPROVAL_REJECTED", "model catalog entry was rejected by administrator")
	}
	if (entry.OriginKind == OriginLocal || entry.LicenseTier == LicenseTierResearchNonCommercial) && entry.ApprovalState != ApprovalApproved {
		liveView, _ := s.GetModelStatus(ctx, entry.ModelID)
		lock := ToCanonicalModelLock(entry)
		// Do NOT cache MODEL_APPROVAL_REQUIRED in idempotency table so post-approval retry succeeds immediately (T32-M03).
		return EnsureResponse{
			HTTPStatus:    202,
			Code:          "MODEL_APPROVAL_REQUIRED",
			Message:       "local or non-commercial model requires Station administrator approval before provisioning",
			Deduplicated:  false,
			Model:         liveView,
			CanonicalLock: &lock,
		}, nil
	}

	if err := EnsureLayout(s.RootDir); err != nil {
		return EnsureResponse{}, NewError(500, "STORAGE_IO_ERROR", "failed initializing model cache layout")
	}

	// Fast check before network DNS if THIS alias's view is ALREADY ready on disk.
	if curView, vErr := s.GetModelStatus(ctx, entry.ModelID); vErr == nil && curView.Status == StatusReady {
		pubView, ok, pubErr := s.Store.PublishViewForReadyBlob(ctx, entry, now, func() (uint64, string, error) {
			return PublishCASAndView(s.RootDir, entry.SHA256, entry.Category, entry.Filename, entry.SizeBytes, curView.DownloadFence, true)
		})
		if pubErr != nil {
			return EnsureResponse{}, pubErr
		}
		if ok {
			curView = pubView
		}
		lock := ToCanonicalModelLock(entry)
		resp := EnsureResponse{
			HTTPStatus:    200,
			Deduplicated:  true,
			Model:         curView,
			CanonicalLock: &lock,
		}
		if err := s.completeEnsureIdempotency(ctx, req, claimID, reqSHA, entry, curView.DownloadFence, resp, now); err != nil {
			if _, ok := AsError(err); ok {
				return EnsureResponse{}, err
			}
			return EnsureResponse{}, NewError(503, "SERVICE_UNAVAILABLE", "failed persisting model.ensure idempotency record")
		}
		idempotencyCompleted = true
		return resp, nil
	}

	// Gate 5: Stage-1 synchronous SSRF check on source URLs BEFORE queuing or linking (S6 item 2a).
	if len(entry.SourceURLs) == 0 {
		return EnsureResponse{}, NewError(403, "MODEL_SOURCE_SSRF_BLOCKED", "catalog entry has no verified HTTPS source URLs")
	}
	primarySource := entry.SourceURLs[0]
	if _, err := s.Downloader.ValidateSourceURL(ctx, primarySource); err != nil {
		return EnsureResponse{}, err
	}

	// Gate 6: Serialized non-double-counted quota check (sampling statvfs INSIDE storage_state lock) + monotonic download_fence.
	s.fireHook(ctx, "before_admit_download", map[string]string{
		"model_id": entry.ModelID,
		"sha256":   entry.SHA256,
	})

	leaseTTL := s.DownloadLeaseTTL
	if leaseTTL <= 0 {
		leaseTTL = 5 * time.Minute
	}
	headroom := s.ReservedHeadroomBytes
	if headroom < DefaultReservedHeadroomBytes {
		headroom = DefaultReservedHeadroomBytes
	}
	maxAuto := s.MaxAutoPullModelBytes
	if maxAuto <= 0 || maxAuto > DefaultMaxAutoPullModelBytes {
		maxAuto = DefaultMaxAutoPullModelBytes
	}

	outcome, err := s.Store.AdmitOrJoinDownload(ctx, AdmitDownloadInput{
		Entry:                 entry,
		DiskStatFn:            s.diskStat,
		ReservedHeadroomBytes: headroom,
		MaxAutoPullBytes:      maxAuto,
		LeaseOwner:            "station-runtime",
		LeaseTTL:              leaseTTL,
		Now:                   now,
	})
	if err != nil {
		return EnsureResponse{}, err
	}

	lock := ToCanonicalModelLock(entry)
	if outcome.AlreadyReady {
		pubView, ok, pubErr := s.Store.PublishViewForReadyBlob(ctx, entry, now, func() (uint64, string, error) {
			return PublishCASAndView(s.RootDir, entry.SHA256, entry.Category, entry.Filename, entry.SizeBytes, outcome.Fence, true)
		})
		if pubErr != nil {
			return EnsureResponse{}, pubErr
		}
		if ok {
			outcome.View = pubView
		}
		resp := EnsureResponse{
			HTTPStatus:    200,
			Deduplicated:  true,
			Model:         outcome.View,
			CanonicalLock: &lock,
		}
		if err := s.completeEnsureIdempotency(ctx, req, claimID, reqSHA, entry, outcome.Fence, resp, now); err != nil {
			if _, ok := AsError(err); ok {
				return EnsureResponse{}, err
			}
			return EnsureResponse{}, NewError(503, "SERVICE_UNAVAILABLE", "failed persisting model.ensure idempotency record")
		}
		idempotencyCompleted = true
		return resp, nil
	}

	if outcome.ApprovalRequired {
		// Do NOT persist MODEL_APPROVAL_REQUIRED as a 24h completed idempotency record (T32-M03).
		return EnsureResponse{
			HTTPStatus:    202,
			Code:          "MODEL_APPROVAL_REQUIRED",
			Message:       sanitizePublicMessage(outcome.ApprovalReason, s.RootDir),
			Deduplicated:  false,
			Model:         outcome.View,
			CanonicalLock: &lock,
		}, nil
	}

	if outcome.JoinedActive {
		resp := EnsureResponse{
			HTTPStatus:    202,
			Deduplicated:  true,
			Model:         outcome.View,
			CanonicalLock: &lock,
		}
		if err := s.completeEnsureIdempotency(ctx, req, claimID, reqSHA, entry, outcome.Fence, resp, now); err != nil {
			if _, ok := AsError(err); ok {
				return EnsureResponse{}, err
			}
			return EnsureResponse{}, NewError(503, "SERVICE_UNAVAILABLE", "failed persisting model.ensure idempotency record")
		}
		idempotencyCompleted = true
		return resp, nil
	}

	resp := EnsureResponse{
		HTTPStatus:    202,
		Deduplicated:  false,
		Model:         outcome.View,
		CanonicalLock: &lock,
	}

	if s.SynchronousDownload {
		s.runDownloadFenceWithPrev(ctx, entry, outcome.PrevFence, outcome.Fence, primarySource, outcome.ResumeETag, outcome.AllowInodeHandoff)
		if updated, gErr := s.GetModelStatus(ctx, entry.ModelID); gErr == nil {
			resp.Model = updated
			if updated.Status == StatusReady {
				resp.HTTPStatus = 200
			}
		}
		if err := s.completeEnsureIdempotency(ctx, req, claimID, reqSHA, entry, outcome.Fence, resp, s.now()); err != nil {
			if _, ok := AsError(err); ok {
				return EnsureResponse{}, err
			}
			return EnsureResponse{}, NewError(503, "SERVICE_UNAVAILABLE", "failed persisting model.ensure idempotency record")
		}
		idempotencyCompleted = true
		return resp, nil
	}

	if err := s.completeEnsureIdempotency(ctx, req, claimID, reqSHA, entry, outcome.Fence, resp, now); err != nil {
		if _, ok := AsError(err); ok {
			return EnsureResponse{}, err
		}
		return EnsureResponse{}, NewError(503, "SERVICE_UNAVAILABLE", "failed persisting model.ensure idempotency record")
	}
	idempotencyCompleted = true

	s.wg.Add(1)
	go func(ent CatalogEntry, prevFence, fence int64, src, resumeETag string, allowInodeHandoff bool) {
		defer s.wg.Done()
		bgCtx, cancel := context.WithTimeout(context.Background(), 2*time.Hour)
		defer cancel()
		s.runDownloadFenceWithPrev(bgCtx, ent, prevFence, fence, src, resumeETag, allowInodeHandoff)
	}(entry, outcome.PrevFence, outcome.Fence, primarySource, outcome.ResumeETag, outcome.AllowInodeHandoff)
	return resp, nil
}

func (s *Service) completeEnsureIdempotency(ctx context.Context, req EnsureRequest, claimID, reqSHA string, entry CatalogEntry, fence int64, resp EnsureResponse, now time.Time) error {
	return s.Store.CompleteEnsureIdempotency(ctx, StoredIdempotency{
		OrganizationID: req.OrganizationID,
		ProjectID:      req.ProjectID,
		PrincipalID:    req.PrincipalID,
		TargetAlias:    req.TargetAlias,
		IdempotencyKey: req.IdempotencyKey,
		ClaimID:        claimID,
		State:          IdempotencyStateCompleted,
		RequestSHA256:  reqSHA,
		ModelID:        entry.ModelID,
		SHA256:         entry.SHA256,
		DownloadFence:  fence,
		ResponseStatus: resp.HTTPStatus,
		ResponseJSON:   mustMarshalJSON(resp),
		ExpiresAt:      now.Add(24 * time.Hour),
	})
}

// RunDownloadFence executes a single fenced CAS provisioning attempt.
func (s *Service) RunDownloadFence(ctx context.Context, entry CatalogEntry, fence int64, sourceURL, resumeETag string) {
	s.runDownloadFenceWithPrev(ctx, entry, fence-1, fence, sourceURL, resumeETag, false)
}

func (s *Service) runDownloadFenceWithPrev(ctx context.Context, entry CatalogEntry, prevFence, fence int64, sourceURL, resumeETag string, allowInodeHandoff bool) {
	partPath := PartFilePath(s.RootDir, entry.SHA256, fence)
	relPart := PartRelPath(entry.SHA256, fence)
	meta := map[string]string{
		"model_id": entry.ModelID,
		"sha256":   entry.SHA256,
		"fence":    strconv.FormatInt(fence, 10),
	}
	var known FenceArtifacts

	ok, err := s.Store.TransitionFenceStatus(ctx, entry.SHA256, fence, StatusDownloading, s.now())
	if err != nil || !ok {
		_, _ = CleanupFenceArtifactsWithIdentity(s.RootDir, entry.SHA256, fence, known)
		return
	}
	if resumeETag != "" && prevFence > 0 && fence > prevFence {
		headroom := s.ReservedHeadroomBytes
		if headroom < DefaultReservedHeadroomBytes {
			headroom = DefaultReservedHeadroomBytes
		}
		// Safe resume handoff (R2-H01, R3-H01): zero-copy rename ONLY when allowInodeHandoff == true
		// AND OS-level exclusive writer probe returns WriterProbeProvenSafe; otherwise clone prefix
		// bytes into an independent inode under verified quota!
		_, _, preparedID := prepareResumePartWithIdentity(
			s.RootDir,
			entry.SHA256,
			prevFence,
			fence,
			entry.SizeBytes,
			allowInodeHandoff,
			func(_ int64) bool {
				if !allowInodeHandoff {
					// AdmitOrJoinDownload already reserved the full entry.SizeBytes quota.
					return true
				}
				_, freeBytes, stErr := s.diskStat()
				return stErr == nil && freeBytes-headroom >= entry.SizeBytes
			},
		)
		if preparedID != nil {
			cp := *preparedID
			known.PartID = &cp
		}
	}

	s.fireHook(ctx, "before_download_stream", meta)
	dlRes, dlErr := s.Downloader.DownloadToPart(ctx, DownloadOptions{
		SourceURL:    sourceURL,
		RootDir:      s.RootDir,
		RelPartPath:  relPart,
		PartPath:     partPath,
		ExpectedSize: entry.SizeBytes,
		ResumeETag:   resumeETag,
		BearerToken:  s.UpstreamBearerToken,
		OnProgress: func(bytesDownloaded, partBytesOnDisk, speedBps int64, etaSeconds *int, etag string) error {
			stillOwner, pErr := s.Store.UpdateFenceProgress(ctx, entry.SHA256, fence, bytesDownloaded, partBytesOnDisk, speedBps, etaSeconds, etag, s.now())
			if pErr != nil {
				return pErr
			}
			if !stillOwner {
				return NewError(409, "DOWNLOAD_FENCE_SUPERSEDED", "download fence lease was superseded")
			}
			return nil
		},
	})
	if dlRes.PartIdentity != nil {
		cp := *dlRes.PartIdentity
		known.PartID = &cp
	}
	if dlErr != nil {
		code := "MODEL_DOWNLOAD_FAILED"
		msg := dlErr.Error()
		if mErr, ok := AsError(dlErr); ok {
			code = mErr.Code
			msg = mErr.Message
		}
		if code == "DOWNLOAD_FENCE_SUPERSEDED" {
			_, _ = CleanupFenceArtifactsWithIdentity(s.RootDir, entry.SHA256, fence, known)
			return
		}
		var keepBytes int64
		var keepETag string
		if code == "MODEL_DOWNLOAD_INTERRUPTED" {
			effectiveETag := dlRes.ETag
			if effectiveETag == "" {
				effectiveETag = resumeETag
			}
			resumable := InspectResumeCandidate(s.RootDir, relPart, entry.SizeBytes, effectiveETag)
			if resumable > 0 && effectiveETag != "" && known.PartID != nil {
				if curID, stErr := statBeneathNoFollow(s.RootDir, relPart); stErr == nil && curID.Dev == known.PartID.Dev && curID.Ino == known.PartID.Ino {
					keepBytes = resumable
					keepETag = effectiveETag
				} else {
					if _, cErr := CleanupFenceArtifactsWithIdentity(s.RootDir, entry.SHA256, fence, known); cErr != nil {
						code = "STORAGE_IO_ERROR"
						msg = cErr.Error()
					}
				}
			} else {
				if _, cErr := CleanupFenceArtifactsWithIdentity(s.RootDir, entry.SHA256, fence, known); cErr != nil {
					code = "STORAGE_IO_ERROR"
					msg = cErr.Error()
				}
			}
		} else {
			if _, cErr := CleanupFenceArtifactsWithIdentity(s.RootDir, entry.SHA256, fence, known); cErr != nil && code != "STORAGE_IO_ERROR" {
				msg = msg + "; cleanup anomaly: " + cErr.Error()
			}
		}
		_, _ = s.Store.FailFenceDownload(ctx, entry.SHA256, fence, code, sanitizePublicMessage(msg, s.RootDir), keepBytes, keepETag, s.now())
		return
	}

	ok, err = s.Store.TransitionFenceStatus(ctx, entry.SHA256, fence, StatusVerifyingSHA256, s.now())
	if err != nil || !ok {
		_, _ = CleanupFenceArtifactsWithIdentity(s.RootDir, entry.SHA256, fence, known)
		return
	}

	s.fireHook(ctx, "before_verify_part", meta)
	verifiedPart, vErr := OpenAndVerifyStagingPartWithExpected(s.RootDir, entry.SHA256, fence, entry.SizeBytes, known.PartID)
	if vErr != nil {
		tampered, cErr := CleanupFenceArtifactsWithIdentity(s.RootDir, entry.SHA256, fence, known)
		code := "MODEL_SHA256_MISMATCH"
		msg := vErr.Error()
		if mErr, ok := AsError(vErr); ok {
			code = mErr.Code
			msg = mErr.Message
		}
		if (tampered || cErr != nil) && code != "STORAGE_IO_ERROR" {
			code = "STORAGE_IO_ERROR"
			if cErr != nil {
				msg = msg + "; " + cErr.Error()
			}
		}
		_, _ = s.Store.FailFenceDownload(ctx, entry.SHA256, fence, code, sanitizePublicMessage(msg, s.RootDir), 0, "", s.now())
		return
	}
	defer verifiedPart.Close()
	verifiedIDCopy := verifiedPart.Identity
	known.PartID = &verifiedIDCopy
	known.TempViewID = &verifiedIDCopy

	// Barrier point for COMFY-P-B item 3 & item 4 (stale downloader paused right before publish lock).
	s.fireHook(ctx, "before_publish_lock", meta)

	_, published, pubErr := s.Store.PublishUnderFenceLock(ctx, entry, fence, s.now(), func(viewsToPublish []string) (uint64, []string, error) {
		s.fireHook(ctx, "inside_publish_lock_before_rename", meta)
		casIno, views, pErr := PublishCASAndViews(s.RootDir, entry.SHA256, viewsToPublish, entry.SizeBytes, fence, verifiedPart, false)
		if casIno != 0 {
			known.TempViewID = &FileIdentity{Dev: verifiedIDCopy.Dev, Ino: casIno}
		}
		return casIno, views, pErr
	})
	verifiedPart.Close()
	if pubErr != nil {
		// Identity-bound cleanup (R3-H02): if a replacement entry was swapped onto .staging/<sha>.<fence>.part
		// or views/.tmp.*, CleanupFenceArtifactsWithIdentity preserves the replacement entry intact!
		_, cErr := CleanupFenceArtifactsWithIdentity(s.RootDir, entry.SHA256, fence, known)
		code := "STORAGE_IO_ERROR"
		msg := pubErr.Error()
		if mErr, ok := AsError(pubErr); ok {
			code = mErr.Code
			msg = mErr.Message
		}
		if cErr != nil && !strings.Contains(msg, cErr.Error()) {
			msg = msg + "; " + cErr.Error()
		}
		_, _ = s.Store.FailFenceDownload(ctx, entry.SHA256, fence, code, sanitizePublicMessage(msg, s.RootDir), 0, "", s.now())
		return
	}
	if !published {
		_, _ = CleanupFenceArtifactsWithIdentity(s.RootDir, entry.SHA256, fence, known)
		return
	}
	_, _ = CleanupFenceArtifactsWithIdentity(s.RootDir, entry.SHA256, fence, known)
}

func (s *Service) Evict(ctx context.Context, req EvictRequest) (EvictResponse, error) {
	if s == nil || s.Store == nil || strings.TrimSpace(s.RootDir) == "" {
		return EvictResponse{}, NewError(503, "SERVICE_UNAVAILABLE", "model cache provisioner is not configured")
	}
	sha := strings.ToLower(strings.TrimSpace(req.SHA256))
	if !ValidSHA256(sha) {
		return EvictResponse{}, NewError(400, "INVALID_PARAMETERS", "invalid sha256")
	}
	if req.StationID != "" && s.StationID != "" && req.StationID != s.StationID {
		return EvictResponse{}, NewError(403, "PERMISSION_DENIED", "station_id mismatch")
	}
	return s.Store.EvictUnderLock(ctx, sha, s.now(), func(relViews []string, expectedCASInode *uint64, downloadFence int64) error {
		s.fireHook(ctx, "during_evicting_unlink", map[string]string{"sha256": sha})
		return EvictCASViewsAndStaging(s.RootDir, sha, relViews, expectedCASInode, downloadFence)
	})
}

func (s *Service) ReservePins(ctx context.Context, req ReservePinsRequest) (ReservePinsResponse, error) {
	if s == nil || s.Store == nil {
		return ReservePinsResponse{}, NewError(503, "SERVICE_UNAVAILABLE", "model cache store not configured")
	}
	req.HolderAppID = strings.TrimSpace(req.HolderAppID)
	req.ReservationID = strings.TrimSpace(req.ReservationID)
	req.OperationID = strings.TrimSpace(req.OperationID)
	req.ScopeKind = strings.TrimSpace(req.ScopeKind)
	req.OwnerID = strings.TrimSpace(req.OwnerID)
	req.OrganizationID = strings.TrimSpace(req.OrganizationID)
	req.ProjectID = strings.TrimSpace(req.ProjectID)
	req.RequestSHA256 = strings.ToLower(strings.TrimSpace(req.RequestSHA256))

	if req.HolderAppID == "" || req.ReservationID == "" || req.OperationID == "" || req.OwnerID == "" || !ValidSHA256(req.RequestSHA256) || len(req.SHA256List) == 0 || len(req.SHA256List) > 32 {
		return ReservePinsResponse{}, NewError(400, "INVALID_PARAMETERS", "invalid pin reservation parameters")
	}
	if req.ScopeKind != PinScopeTaskExecution && req.ScopeKind != PinScopePublishedAPI {
		return ReservePinsResponse{}, NewError(400, "INVALID_PARAMETERS", "invalid scope_kind")
	}
	for _, sha := range req.SHA256List {
		if !ValidSHA256(strings.ToLower(strings.TrimSpace(sha))) {
			return ReservePinsResponse{}, NewError(400, "INVALID_PARAMETERS", "invalid model sha256 in sha256_list")
		}
	}
	return s.Store.ReservePins(ctx, req, s.now())
}

func (s *Service) CommitPins(ctx context.Context, req CommitPinsRequest) (CommitPinsResponse, error) {
	if s == nil || s.Store == nil {
		return CommitPinsResponse{}, NewError(503, "SERVICE_UNAVAILABLE", "model cache store not configured")
	}
	req.HolderAppID = strings.TrimSpace(req.HolderAppID)
	req.ReservationID = strings.TrimSpace(req.ReservationID)
	req.OwnerID = strings.TrimSpace(req.OwnerID)
	if req.HolderAppID == "" || req.ReservationID == "" {
		return CommitPinsResponse{}, NewError(400, "INVALID_PARAMETERS", "missing holder_app_id or reservation_id")
	}
	return s.Store.CommitPins(ctx, req, s.now())
}

func (s *Service) ReleasePins(ctx context.Context, req ReleasePinsRequest) (ReleasePinsResponse, error) {
	if s == nil || s.Store == nil {
		return ReleasePinsResponse{}, NewError(503, "SERVICE_UNAVAILABLE", "model cache store not configured")
	}
	req.HolderAppID = strings.TrimSpace(req.HolderAppID)
	req.ReservationID = strings.TrimSpace(req.ReservationID)
	req.Reason = strings.TrimSpace(req.Reason)
	if req.HolderAppID == "" || req.ReservationID == "" {
		return ReleasePinsResponse{}, NewError(400, "INVALID_PARAMETERS", "missing holder_app_id or reservation_id")
	}
	if req.Reason == "" {
		req.Reason = "app_released"
	}
	return s.Store.ReleasePins(ctx, req, s.now())
}

func (s *Service) reconcilePerRPCTimeout(phaseBudget time.Duration, hasPhaseDeadline bool) time.Duration {
	t := s.ReconcilePerRPCTimeout
	if t <= 0 {
		t = 1500 * time.Millisecond
	}
	if hasPhaseDeadline && phaseBudget > 0 {
		half := phaseBudget / 2
		if half < 5*time.Millisecond {
			half = phaseBudget
		}
		if t > half {
			t = half
		}
	}
	return t
}

func (s *Service) reconcileStoreContext(parent context.Context) (context.Context, context.CancelFunc) {
	if errors.Is(parent.Err(), context.Canceled) {
		return parent, func() {}
	}
	opTimeout := s.ReconcileStoreOpTimeout
	if opTimeout <= 0 {
		opTimeout = 1500 * time.Millisecond
	}
	// Preserve a dedicated short persistence window even if parent's round deadline just elapsed
	// during a slow RPC so last_reconcile_checked_at / CommitPins / ReleasePins always land in DB (R3-M01).
	return context.WithTimeout(context.WithoutCancel(parent), opTimeout)
}

// ReconcileExpiredPins enforces §4.3.3 Step 4 AND starvation-free active/reserved reconciliation (T32-M02, R2-M03, R3-M01):
//  1. Splits any round deadline into two independent phase budgets (reserved vs active, alternating
//     first-mover order across rounds) while reserving DB persistence headroom.
//  2. Bounds EVERY individual RevokeOrVerify RPC with a short sub-context timeout so slow/hanging
//     upstream RPCs cannot exhaust the other phase's budget.
//  3. Persists TouchPinGroupReconcileChecked / CommitPins / ReleasePins via a dedicated short DB
//     context so timed-out groups ALWAYS advance last_reconcile_checked_at and rotate behind healthy items.
func (s *Service) ReconcileExpiredPins(ctx context.Context) (promoted int, released int, err error) {
	if s == nil || s.Store == nil || s.Revoker == nil {
		return 0, 0, nil
	}
	if err := ctx.Err(); err != nil {
		return 0, 0, err
	}
	const (
		batchSize  = 50
		maxBatches = 8
	)
	now := s.now()

	var hasRoundDeadline bool
	var phaseBudget time.Duration
	if roundDeadline, ok := ctx.Deadline(); ok {
		total := time.Until(roundDeadline)
		if total <= 0 {
			return 0, 0, ctx.Err()
		}
		hasRoundDeadline = true
		dbReserve := total / 10
		if dbReserve < 10*time.Millisecond {
			dbReserve = total / 5
		}
		if dbReserve > time.Second {
			dbReserve = time.Second
		}
		usable := total - dbReserve
		if usable <= 0 {
			usable = total
		}
		phaseBudget = usable / 2
		if phaseBudget <= 0 {
			phaseBudget = total / 2
		}
	}
	perRPC := s.reconcilePerRPCTimeout(phaseBudget, hasRoundDeadline)

	runReservedPhase := func() error {
		var phaseDeadline time.Time
		if hasRoundDeadline {
			phaseDeadline = time.Now().Add(phaseBudget)
		}
		seenReserved := make(map[string]bool)
		for batch := 0; batch < maxBatches; batch++ {
			if errors.Is(ctx.Err(), context.Canceled) {
				return ctx.Err()
			}
			if hasRoundDeadline && !time.Now().Before(phaseDeadline) {
				break
			}
			listCtx, listCancel := s.reconcileStoreContext(ctx)
			groups, lErr := s.Store.ListExpiredReservedGroups(listCtx, now, batchSize)
			listCancel()
			if lErr != nil {
				return lErr
			}
			progressed := 0
			for _, g := range groups {
				if errors.Is(ctx.Err(), context.Canceled) {
					return ctx.Err()
				}
				if hasRoundDeadline && !time.Now().Before(phaseDeadline) {
					return nil
				}
				key := g.HolderAppID + "\x00" + g.ReservationID
				if seenReserved[key] {
					continue
				}
				seenReserved[key] = true
				progressed++

				callTimeout := perRPC
				if hasRoundDeadline {
					if rem := time.Until(phaseDeadline); rem <= 0 {
						return nil
					} else if callTimeout > rem {
						callTimeout = rem
					}
				}
				rpcCtx, rpcCancel := context.WithTimeout(context.WithoutCancel(ctx), callTimeout)
				stopPropagate := context.AfterFunc(ctx, func() {
					if errors.Is(ctx.Err(), context.Canceled) {
						rpcCancel()
					}
				})
				ver, rErr := s.Revoker.RevokeOrVerify(rpcCtx, g.HolderAppID, g.ReservationID)
				stopPropagate()
				rpcCancel()

				dbCtx, dbCancel := s.reconcileStoreContext(ctx)
				if rErr != nil {
					// Iron rule: query timeout / RPC error NEVER releases a reserved pin,
					// and ALWAYS advances last_reconcile_checked_at via dedicated DB budget!
					_ = s.Store.TouchPinGroupReconcileChecked(dbCtx, g.HolderAppID, g.ReservationID, now)
					dbCancel()
					continue
				}
				switch ver.State {
				case AdmissionStateCommitted:
					if _, cErr := s.Store.CommitPins(dbCtx, CommitPinsRequest{
						HolderAppID:   g.HolderAppID,
						ReservationID: g.ReservationID,
						OwnerID:       ver.EntityID,
					}, now); cErr == nil {
						promoted++
					} else {
						_ = s.Store.TouchPinGroupReconcileChecked(dbCtx, g.HolderAppID, g.ReservationID, now)
					}
				case AdmissionStateRevoked, AdmissionStateReleased:
					if _, relErr := s.Store.ReleasePins(dbCtx, ReleasePinsRequest{
						HolderAppID:   g.HolderAppID,
						ReservationID: g.ReservationID,
						Reason:        "reconciler_revoked_tombstone",
					}, now); relErr == nil {
						released++
					} else {
						_ = s.Store.TouchPinGroupReconcileChecked(dbCtx, g.HolderAppID, g.ReservationID, now)
					}
				default:
					_ = s.Store.TouchPinGroupReconcileChecked(dbCtx, g.HolderAppID, g.ReservationID, now)
				}
				dbCancel()
			}
			if progressed == 0 || len(groups) < batchSize {
				break
			}
		}
		return nil
	}

	runActivePhase := func() error {
		var phaseDeadline time.Time
		if hasRoundDeadline {
			phaseDeadline = time.Now().Add(phaseBudget)
		}
		activeAge := s.ActiveTaskReconcileAge
		if activeAge <= 0 {
			activeAge = 1 * time.Minute
		}
		cutoff := now.Add(-activeAge)
		seenActive := make(map[string]bool)
		for batch := 0; batch < maxBatches; batch++ {
			if errors.Is(ctx.Err(), context.Canceled) {
				return ctx.Err()
			}
			if hasRoundDeadline && !time.Now().Before(phaseDeadline) {
				break
			}
			listCtx, listCancel := s.reconcileStoreContext(ctx)
			staleActive, aErr := s.Store.ListStaleActiveTaskGroups(listCtx, cutoff, batchSize)
			listCancel()
			if aErr != nil {
				return aErr
			}
			progressed := 0
			for _, g := range staleActive {
				if errors.Is(ctx.Err(), context.Canceled) {
					return ctx.Err()
				}
				if hasRoundDeadline && !time.Now().Before(phaseDeadline) {
					return nil
				}
				key := g.HolderAppID + "\x00" + g.ReservationID
				if seenActive[key] {
					continue
				}
				seenActive[key] = true
				progressed++

				callTimeout := perRPC
				if hasRoundDeadline {
					if rem := time.Until(phaseDeadline); rem <= 0 {
						return nil
					} else if callTimeout > rem {
						callTimeout = rem
					}
				}
				rpcCtx, rpcCancel := context.WithTimeout(context.WithoutCancel(ctx), callTimeout)
				stopPropagate := context.AfterFunc(ctx, func() {
					if errors.Is(ctx.Err(), context.Canceled) {
						rpcCancel()
					}
				})
				ver, rErr := s.Revoker.RevokeOrVerify(rpcCtx, g.HolderAppID, g.ReservationID)
				stopPropagate()
				rpcCancel()

				dbCtx, dbCancel := s.reconcileStoreContext(ctx)
				if rErr != nil {
					_ = s.Store.TouchPinGroupReconcileChecked(dbCtx, g.HolderAppID, g.ReservationID, now)
					dbCancel()
					continue
				}
				if ver.State == AdmissionStateReleased || ver.State == AdmissionStateRevoked {
					if _, relErr := s.Store.ReleasePins(dbCtx, ReleasePinsRequest{
						HolderAppID:   g.HolderAppID,
						ReservationID: g.ReservationID,
						Reason:        "reconciler_terminal_task_released",
					}, now); relErr == nil {
						released++
					} else {
						_ = s.Store.TouchPinGroupReconcileChecked(dbCtx, g.HolderAppID, g.ReservationID, now)
					}
				} else {
					_ = s.Store.TouchPinGroupReconcileChecked(dbCtx, g.HolderAppID, g.ReservationID, now)
				}
				dbCancel()
			}
			if progressed == 0 || len(staleActive) < batchSize {
				break
			}
		}
		return nil
	}

	round := atomic.AddUint64(&s.reconcileRound, 1)
	if round%2 == 1 {
		if err := runReservedPhase(); err != nil {
			return promoted, released, err
		}
		if err := runActivePhase(); err != nil {
			return promoted, released, err
		}
	} else {
		if err := runActivePhase(); err != nil {
			return promoted, released, err
		}
		if err := runReservedPhase(); err != nil {
			return promoted, released, err
		}
	}
	return promoted, released, nil
}

// HTTPAppAdmissionRevoker calls comfyui-manager's internal revocation fence endpoint.
type HTTPAppAdmissionRevoker struct {
	EndpointsByApp map[string]string
	BearerToken    string
	Client         *http.Client
}

func (r HTTPAppAdmissionRevoker) RevokeOrVerify(ctx context.Context, holderAppID, reservationID string) (AppAdmissionVerification, error) {
	endpoint := strings.TrimSpace(r.EndpointsByApp[holderAppID])
	if endpoint == "" || strings.TrimSpace(r.BearerToken) == "" {
		return AppAdmissionVerification{}, errors.New("app admission revoker endpoint not configured")
	}
	client := r.Client
	if client == nil {
		client = &http.Client{
			Timeout: 5 * time.Second,
			CheckRedirect: func(*http.Request, []*http.Request) error {
				return errors.New("redirect denied")
			},
		}
	}
	payload, _ := json.Marshal(map[string]string{"reservation_id": reservationID})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(payload))
	if err != nil {
		return AppAdmissionVerification{}, err
	}
	req.Header.Set("Authorization", "Bearer "+r.BearerToken)
	req.Header.Set("Content-Type", "application/json")

	resp, err := client.Do(req)
	if err != nil {
		return AppAdmissionVerification{}, err
	}
	defer resp.Body.Close()

	raw, err := io.ReadAll(io.LimitReader(resp.Body, 16384))
	if err != nil || resp.StatusCode != http.StatusOK {
		return AppAdmissionVerification{}, errors.New("app admission revoker returned non-200")
	}
	var out AppAdmissionVerification
	if err := json.Unmarshal(raw, &out); err != nil || out.ReservationID != reservationID {
		return AppAdmissionVerification{}, errors.New("invalid app admission verification payload")
	}
	if out.State != AdmissionStateCommitted && out.State != AdmissionStateRevoked && out.State != AdmissionStateReleased {
		return AppAdmissionVerification{}, errors.New("unexpected app admission state")
	}
	return out, nil
}
