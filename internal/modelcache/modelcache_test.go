package modelcache

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/verdantflarehub/verdantflare-station-runtime/internal/appinstances"
)

type staticResolver map[string][]net.IPAddr

func (r staticResolver) LookupIPAddr(_ context.Context, host string) ([]net.IPAddr, error) {
	addrs, ok := r[strings.ToLower(host)]
	if !ok {
		return nil, errors.New("nxdomain")
	}
	return addrs, nil
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

// buildValidSafetensors constructs a valid .safetensors binary with optional header padding
// (e.g. minHeaderLen=128 -> little-endian byte 0 == 0x80!).
func buildValidSafetensors(t *testing.T, minHeaderLen int, extraPayload []byte) ([]byte, string) {
	t.Helper()
	tensorBytes := []byte{1, 2, 3, 4, 5, 6, 7, 8} // F16 shape [2,2] => 4 * 2 = 8 bytes
	headerObj := `{"__metadata__":{"format":"pt"},"weight":{"dtype":"F16","shape":[2,2],"data_offsets":[0,8]}}`
	headerBytes := []byte(headerObj)
	if len(headerBytes) < minHeaderLen {
		headerBytes = append(headerBytes, bytes.Repeat([]byte(" "), minHeaderLen-len(headerBytes))...)
	}
	var lenBuf [8]byte
	binary.LittleEndian.PutUint64(lenBuf[:], uint64(len(headerBytes)))

	var out bytes.Buffer
	out.Write(lenBuf[:])
	out.Write(headerBytes)
	out.Write(tensorBytes)
	out.Write(extraPayload)

	raw := out.Bytes()
	sum := sha256.Sum256(raw)
	return raw, hex.EncodeToString(sum[:])
}

func TestClassifyIPAndStage1SSRF(t *testing.T) {
	publicCases := []string{
		"93.184.216.34",
		"151.101.1.195",
		"2606:2800:220:1:248:1893:25c8:1946",
	}
	for _, raw := range publicCases {
		addr := netip.MustParseAddr(raw)
		if ok, reason := ClassifyIP(addr); !ok {
			t.Fatalf("expected public IP %s allowed, got blocked (%s)", raw, reason)
		}
	}

	blockedCases := []string{
		"0.0.0.0",
		"127.0.0.1",
		"10.0.0.1",
		"100.64.0.1",
		"100.100.100.200",
		"169.254.169.254",
		"172.16.0.5",
		"192.168.1.10",
		"198.18.0.1",
		"224.0.0.1",
		"255.255.255.255",
		"::",
		"::1",
		"::127.0.0.1",
		"::ffff:127.0.0.1",
		"::ffff:10.0.0.1",
		"::ffff:169.254.169.254",
		"64:ff9b::a9fe:a9fe",
		"2001:db8::1",
		"2001:0000:4136:e378:8000:63bf:3fff:fdd2",
		"2002:7f00:0001::",
		"fd00:ec2::254",
		"fe80::1",
		"ff02::1",
	}
	for _, raw := range blockedCases {
		addr := netip.MustParseAddr(raw)
		if ok, _ := ClassifyIP(addr); ok {
			t.Fatalf("expected IP %s to be blocked by ClassifyIP", raw)
		}
	}

	dl, err := NewSafeDownloader([]string{"models-test.verdantflare.internal"}, "")
	if err != nil {
		t.Fatalf("NewSafeDownloader: %v", err)
	}
	dl.Resolver = staticResolver{
		"huggingface.co": {{IP: net.ParseIP("93.184.216.34")}},
		"models-test.verdantflare.internal": {
			{IP: net.ParseIP("93.184.216.34")},
			{IP: net.ParseIP("169.254.169.254")}, // mixed public + metadata A record MUST block
		},
	}

	badURLs := []string{
		"http://huggingface.co/model.safetensors",
		"https://huggingface.co:8443/model.safetensors",
		"https://user:secret@huggingface.co/model.safetensors",
		"https://huggingface.co/model.safetensors#frag",
		"https://93.184.216.34/model.safetensors",
		"https://evil.example.com/model.safetensors",
		"https://models-test.verdantflare.internal/model.safetensors",
	}
	for _, u := range badURLs {
		_, vErr := dl.ValidateSourceURL(context.Background(), u)
		mErr, ok := AsError(vErr)
		if !ok || mErr.Status != 403 || mErr.Code != "MODEL_SOURCE_SSRF_BLOCKED" {
			t.Fatalf("URL %q expected 403 MODEL_SOURCE_SSRF_BLOCKED, got %v", u, vErr)
		}
	}
}

func TestStage2RedirectSSRFAndCredentialStripping(t *testing.T) {
	rootDir := t.TempDir()
	validBytes, validSHA := buildValidSafetensors(t, 128, nil)

	var seenCrossOriginAuth string
	dl, err := NewSafeDownloader(nil, "")
	if err != nil {
		t.Fatalf("NewSafeDownloader: %v", err)
	}
	dl.Resolver = staticResolver{
		"huggingface.co":         {{IP: net.ParseIP("93.184.216.34")}},
		"cdn-lfs.huggingface.co": {{IP: net.ParseIP("151.101.1.195")}},
		"civitai.com":            {{IP: net.ParseIP("169.254.169.254")}}, // Stage-2 private redirect target
	}
	dl.TransportForTarget = func(target VerifiedTarget) (http.RoundTripper, func(), error) {
		rt := roundTripFunc(func(req *http.Request) (*http.Response, error) {
			switch req.URL.Path {
			case "/redirect-ok":
				if req.Header.Get("Authorization") != "Bearer hf_token_123" {
					t.Errorf("expected Authorization on first hop, got %q", req.Header.Get("Authorization"))
				}
				h := make(http.Header)
				h.Set("Location", "https://cdn-lfs.huggingface.co/lfs-blob")
				return &http.Response{StatusCode: 302, Header: h, Body: io.NopCloser(strings.NewReader(""))}, nil
			case "/lfs-blob":
				seenCrossOriginAuth = req.Header.Get("Authorization")
				return &http.Response{
					StatusCode:    200,
					ContentLength: int64(len(validBytes)),
					Header:        make(http.Header),
					Body:          io.NopCloser(bytes.NewReader(validBytes)),
				}, nil
			case "/redirect-ssrf":
				h := make(http.Header)
				h.Set("Location", "https://civitai.com/internal-metadata")
				return &http.Response{StatusCode: 302, Header: h, Body: io.NopCloser(strings.NewReader("redirecting"))}, nil
			default:
				return &http.Response{StatusCode: 404, Body: io.NopCloser(strings.NewReader(""))}, nil
			}
		})
		return rt, func() {}, nil
	}

	// 1. Cross-origin redirect to valid public CDN strips Authorization header.
	partOK := PartFilePath(rootDir, validSHA, 1)
	if err := EnsureLayout(rootDir); err != nil {
		t.Fatalf("EnsureLayout: %v", err)
	}
	_, err = dl.DownloadToPart(context.Background(), DownloadOptions{
		SourceURL:    "https://huggingface.co/redirect-ok",
		PartPath:     partOK,
		ExpectedSize: int64(len(validBytes)),
		BearerToken:  "hf_token_123",
	})
	if err != nil {
		t.Fatalf("expected valid cross-origin redirect to succeed, got %v", err)
	}
	if seenCrossOriginAuth != "" {
		t.Fatalf("expected cross-origin redirect to strip Authorization, got %q", seenCrossOriginAuth)
	}
	_ = os.Remove(partOK)

	// 2. Redirect to private/metadata IP aborts BEFORE opening .part -> zero .part files on disk!
	partBlocked := PartFilePath(rootDir, validSHA, 2)
	_, err = dl.DownloadToPart(context.Background(), DownloadOptions{
		SourceURL:    "https://huggingface.co/redirect-ssrf",
		PartPath:     partBlocked,
		ExpectedSize: int64(len(validBytes)),
	})
	mErr, ok := AsError(err)
	if !ok || mErr.Code != "MODEL_SOURCE_SSRF_BLOCKED" {
		t.Fatalf("expected MODEL_SOURCE_SSRF_BLOCKED on hop 2, got %v", err)
	}
	if _, statErr := os.Stat(partBlocked); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("expected zero .part file created on redirect SSRF block, statErr=%v", statErr)
	}
}

func TestSafetensorsBinaryFrameMatrix(t *testing.T) {
	dir := t.TempDir()

	// Positive fixture: header_len = 128 -> byte 0 is 0x80! Must NOT be misclassified as Pickle!
	valid128, _ := buildValidSafetensors(t, 128, nil)
	if valid128[0] != 0x80 {
		t.Fatalf("expected header_len=128 little-endian byte 0 == 0x80, got 0x%x", valid128[0])
	}
	validPath := dir + "/valid128.safetensors"
	if err := os.WriteFile(validPath, valid128, 0o600); err != nil {
		t.Fatal(err)
	}
	vf, err := os.Open(validPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := ValidateSafetensorsFile(vf, int64(len(valid128))); err != nil {
		_ = vf.Close()
		t.Fatalf("valid header_len=128 safetensors rejected: %v", err)
	}
	_ = vf.Close()

	makeHeader := func(jsonStr string, dataLen int) []byte {
		var lenBuf [8]byte
		binary.LittleEndian.PutUint64(lenBuf[:], uint64(len(jsonStr)))
		var b bytes.Buffer
		b.Write(lenBuf[:])
		b.WriteString(jsonStr)
		b.Write(make([]byte, dataLen))
		return b.Bytes()
	}

	withTrailing, _ := buildValidSafetensors(t, 128, []byte("EVIL"))
	badSamples := map[string][]byte{
		"disguised_zip":       append([]byte("PK\x03\x04"), bytes.Repeat([]byte{0}, 64)...),
		"disguised_pickle":    append([]byte("\x80\x04\x95\x10\x00\x00\x00\x00\x00\x00\x00"), bytes.Repeat([]byte{'a'}, 32)...),
		"duplicate_root_key":  makeHeader(`{"w":{"dtype":"F16","shape":[2],"data_offsets":[0,4]},"w":{"dtype":"F16","shape":[2],"data_offsets":[4,8]}}`, 8),
		"bad_metadata_type":   makeHeader(`{"__metadata__":{"bad":123},"w":{"dtype":"F16","shape":[2],"data_offsets":[0,4]}}`, 4),
		"shape_span_mismatch": makeHeader(`{"w":{"dtype":"F16","shape":[2,2],"data_offsets":[0,4]}}`, 4),
		"offset_gap":          makeHeader(`{"a":{"dtype":"U8","shape":[4],"data_offsets":[0,4]},"b":{"dtype":"U8","shape":[4],"data_offsets":[8,12]}}`, 12),
		"offset_overlap":      makeHeader(`{"a":{"dtype":"U8","shape":[8],"data_offsets":[0,8]},"b":{"dtype":"U8","shape":[8],"data_offsets":[4,12]}}`, 12),
		"trailing_bytes":      withTrailing,
		"float_shape_literal": makeHeader(`{"w":{"dtype":"U8","shape":[4.0],"data_offsets":[0,4]}}`, 4),
	}

	for name, payload := range badSamples {
		p := fmt.Sprintf("%s/%s.safetensors", dir, name)
		if err := os.WriteFile(p, payload, 0o600); err != nil {
			t.Fatal(err)
		}
		f, err := os.Open(p)
		if err != nil {
			t.Fatal(err)
		}
		vErr := ValidateSafetensorsFile(f, int64(len(payload)))
		_ = f.Close()
		mErr, ok := AsError(vErr)
		if !ok || mErr.Status != 400 || mErr.Code != "UNSAFE_MODEL_FORMAT" {
			t.Fatalf("sample %q expected 400 UNSAFE_MODEL_FORMAT, got %v", name, vErr)
		}
	}

	if mErr, ok := AsError(ValidateModelFilename("model.ckpt")); !ok || mErr.Code != "UNSAFE_MODEL_FORMAT" {
		t.Fatalf("expected .ckpt to fail UNSAFE_MODEL_FORMAT, got %v", mErr)
	}
	if mErr, ok := AsError(ValidateModelFilename("../escape.safetensors")); !ok || mErr.Code != "INVALID_PARAMETERS" {
		t.Fatalf("expected ../ path traversal to fail INVALID_PARAMETERS, got %v", mErr)
	}
}

func newTestService(t *testing.T, freeBytes int64, blobs map[string][]byte) (*Service, *MemoryStore, *int32) {
	t.Helper()
	rootDir := t.TempDir()
	store := NewMemoryStore()
	var upstreamHits int32

	dl, err := NewSafeDownloader(nil, "")
	if err != nil {
		t.Fatal(err)
	}
	dl.Resolver = staticResolver{
		"huggingface.co": {{IP: net.ParseIP("93.184.216.34")}},
	}
	dl.TransportForTarget = func(target VerifiedTarget) (http.RoundTripper, func(), error) {
		rt := roundTripFunc(func(req *http.Request) (*http.Response, error) {
			atomic.AddInt32(&upstreamHits, 1)
			data, ok := blobs[req.URL.Path]
			if !ok {
				return &http.Response{StatusCode: 404, Body: io.NopCloser(strings.NewReader(""))}, nil
			}
			h := make(http.Header)
			h.Set("ETag", `"etag-v1"`)
			if rng := req.Header.Get("Range"); strings.HasPrefix(rng, "bytes=") {
				var start int
				_, _ = fmt.Sscanf(rng, "bytes=%d-", &start)
				if start > 0 && start < len(data) {
					h.Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, len(data)-1, len(data)))
					return &http.Response{
						StatusCode:    206,
						ContentLength: int64(len(data) - start),
						Header:        h,
						Body:          io.NopCloser(bytes.NewReader(data[start:])),
					}, nil
				}
			}
			return &http.Response{
				StatusCode:    200,
				ContentLength: int64(len(data)),
				Header:        h,
				Body:          io.NopCloser(bytes.NewReader(data)),
			}, nil
		})
		return rt, func() {}, nil
	}

	svc := &Service{
		Store:     store,
		StationID: "019a0000-0000-7000-8000-000000000001",
		RootDir:   rootDir,
		Downloader: dl,
		DiskStat: func(string) (int64, int64, error) {
			return 2000 * 1024 * 1024 * 1024, freeBytes, nil
		},
		SynchronousDownload: true,
	}
	return svc, store, &upstreamHits
}

func TestPolicyGatesAndQuotaNonDoubleCounting(t *testing.T) {
	validBytes, validSHA := buildValidSafetensors(t, 128, nil)
	const giB = int64(1024 * 1024 * 1024)

	// Start with 218 GiB free on disk.
	svc, store, hits := newTestService(t, 218*giB, map[string][]byte{
		"/sdxl.safetensors": validBytes,
	})
	ctx := context.Background()

	err := svc.RegisterCatalogEntries(ctx, []CatalogEntry{
		{
			ModelID:          "paid-civitai-lora",
			CatalogCode:      "MDL-LORA-PAID-001",
			DisplayName:      "Paid Early Access LoRA",
			Category:         "LORA",
			Filename:         "paid_lora.safetensors",
			SHA256:           strings.Repeat("a", 64),
			SizeBytes:        1024,
			VRAMFootprintMiB: 256,
			License:          "early-access",
			PaidAccess:       true,
			SourceURLs:       []string{"https://huggingface.co/paid.safetensors"},
		},
		{
			ModelID:          "local-unapproved-lora",
			CatalogCode:      "MDL-LORA-LOCAL-001",
			DisplayName:      "Local Brand LoRA",
			Category:         "LORA",
			Filename:         "local_brand.safetensors",
			SHA256:           strings.Repeat("b", 64),
			SizeBytes:        1024,
			VRAMFootprintMiB: 256,
			License:          "apache-2.0",
			PaidAccess:       false,
			OriginKind:       OriginLocal,
			ApprovalState:    ApprovalPendingAdmin,
			SourceURLs:       []string{"https://huggingface.co/local.safetensors"},
		},
		{
			ModelID:          "sdxl-base-1_0",
			CatalogCode:      "MDL-CKPT-SDXL-001",
			DisplayName:      "SDXL Base 1.0",
			Category:         "CKPT",
			Filename:         "sd_xl_base_1.0.safetensors",
			SHA256:           validSHA,
			SizeBytes:        int64(len(validBytes)),
			VRAMFootprintMiB: 6912,
			License:          "openrail++",
			PaidAccess:       false,
			SourceURLs:       []string{"https://huggingface.co/sdxl.safetensors"},
		},
	})
	if err != nil {
		t.Fatalf("RegisterCatalogEntries: %v", err)
	}

	// 1. Paid access -> 403 MODEL_PAID_ACCESS_FORBIDDEN synchronously.
	_, err = svc.Ensure(ctx, EnsureRequest{
		OrganizationID: "org-1",
		ProjectID:      "proj-1",
		PrincipalID:    "user-1",
		IdempotencyKey: "idem-paid-0001",
		ModelID:        "paid-civitai-lora",
	})
	if mErr, ok := AsError(err); !ok || mErr.Status != 403 || mErr.Code != "MODEL_PAID_ACCESS_FORBIDDEN" {
		t.Fatalf("expected 403 MODEL_PAID_ACCESS_FORBIDDEN, got %v", err)
	}

	// 2. expected_sha256 mismatch -> 409 MODEL_CATALOG_SHA256_MISMATCH synchronously, zero upstream hits, blob stays not_installed!
	_, err = svc.Ensure(ctx, EnsureRequest{
		OrganizationID: "org-1",
		ProjectID:      "proj-1",
		PrincipalID:    "user-1",
		IdempotencyKey: "idem-mismatch-0001",
		ModelID:        "sdxl-base-1_0",
		ExpectedSHA256: strings.Repeat("c", 64),
	})
	if mErr, ok := AsError(err); !ok || mErr.Status != 409 || mErr.Code != "MODEL_CATALOG_SHA256_MISMATCH" {
		t.Fatalf("expected 409 MODEL_CATALOG_SHA256_MISMATCH, got %v", err)
	}
	stView, _ := svc.GetModelStatus(ctx, "sdxl-base-1_0")
	if stView.Status != StatusNotInstalled || stView.DownloadFence != 0 || atomic.LoadInt32(hits) != 0 {
		t.Fatalf("expected zero state pollution after expected_sha256 mismatch, got %+v hits=%d", stView, atomic.LoadInt32(hits))
	}

	// 3. Unapproved local model -> 202 MODEL_APPROVAL_REQUIRED without starting download.
	localRes, err := svc.Ensure(ctx, EnsureRequest{
		OrganizationID: "org-1",
		ProjectID:      "proj-1",
		PrincipalID:    "user-1",
		IdempotencyKey: "idem-local-0001",
		ModelID:        "local-unapproved-lora",
	})
	if err != nil || localRes.HTTPStatus != 202 || localRes.Code != "MODEL_APPROVAL_REQUIRED" {
		t.Fatalf("expected 202 MODEL_APPROVAL_REQUIRED for unapproved local model, got res=%+v err=%v", localRes, err)
	}

	// 4. Non-double-counted quota formula (COMFY-P-B barrier 2):
	// Simulate in-flight Model A (size=10 GiB, part_bytes_on_disk=8 GiB) while disk free is 218 GiB.
	// Remaining unmaterialized = 2 GiB. Model B (15 GiB) needs 218 - 2 - 15 = 201 GiB >= 200 GiB -> MUST pass!
	inFlightSHA := strings.Repeat("d", 64)
	store.mu.Lock()
	exp := time.Now().UTC().Add(10 * time.Minute)
	store.blobsBySHA[inFlightSHA] = &memBlob{
		BlobState: BlobState{
			SHA256:          inFlightSHA,
			SizeBytes:       10 * giB,
			PartBytesOnDisk: 8 * giB,
			BytesDownloaded: 8 * giB,
			Status:          StatusDownloading,
			DownloadFence:   1,
		},
		LeaseExpiresAt: &exp,
	}
	store.mu.Unlock()

	modelBEntry := CatalogEntry{
		ModelID:          "flux-vae-15g",
		CatalogCode:      "MDL-VAE-FLUX1-001",
		DisplayName:      "Flux Large Test",
		Category:         "VAE",
		Filename:         "flux_vae.safetensors",
		SHA256:           strings.Repeat("e", 64),
		SizeBytes:        15 * giB,
		VRAMFootprintMiB: 2048,
		License:          "apache-2.0",
	}
	_ = modelBEntry.NormalizeAndValidate()
	admitB, err := store.AdmitOrJoinDownload(ctx, AdmitDownloadInput{
		Entry:                 modelBEntry,
		FilesystemTotalBytes:  2000 * giB,
		FilesystemFreeBytes:   218 * giB,
		ReservedHeadroomBytes: DefaultReservedHeadroomBytes,
		MaxAutoPullBytes:      DefaultMaxAutoPullModelBytes,
		LeaseOwner:            "runtime",
		LeaseTTL:              5 * time.Minute,
		Now:                   time.Now().UTC(),
	})
	if err != nil || admitB.ApprovalRequired {
		t.Fatalf("expected Model B admitted without double-counting .part bytes, got outcome=%+v err=%v", admitB, err)
	}

	// Now unmaterialized is 2 + 15 = 17 GiB; Model C (5 GiB) leaves 218 - 17 - 5 = 196 GiB < 200 GiB -> MUST require approval!
	modelCEntry := CatalogEntry{
		ModelID:          "extra-5g",
		CatalogCode:      "MDL-CLIP-FLUX1-001",
		DisplayName:      "Extra 5G CLIP",
		Category:         "CLIP",
		Filename:         "clip_l.safetensors",
		SHA256:           strings.Repeat("f", 64),
		SizeBytes:        5 * giB,
		VRAMFootprintMiB: 1024,
		License:          "mit",
	}
	_ = modelCEntry.NormalizeAndValidate()
	admitC, err := store.AdmitOrJoinDownload(ctx, AdmitDownloadInput{
		Entry:                 modelCEntry,
		FilesystemTotalBytes:  2000 * giB,
		FilesystemFreeBytes:   218 * giB,
		ReservedHeadroomBytes: DefaultReservedHeadroomBytes,
		MaxAutoPullBytes:      DefaultMaxAutoPullModelBytes,
		LeaseOwner:            "runtime",
		LeaseTTL:              5 * time.Minute,
		Now:                   time.Now().UTC(),
	})
	if err != nil || !admitC.ApprovalRequired {
		t.Fatalf("expected Model C blocked by 200 GiB headroom floor, got outcome=%+v err=%v", admitC, err)
	}
}

func TestStaleFenceIsolationRENAME_NOREPLACEAndSameFilenameViews(t *testing.T) {
	bytesA, shaA := buildValidSafetensors(t, 128, nil)
	// Build a second distinct valid safetensors file (minHeaderLen=160) with the SAME filename!
	bytesB, shaB := buildValidSafetensors(t, 160, nil)

	const giB = int64(1024 * 1024 * 1024)
	svc, store, _ := newTestService(t, 600*giB, map[string][]byte{
		"/lora-a.safetensors": bytesA,
		"/lora-b.safetensors": bytesB,
	})
	svc.SynchronousDownload = false
	ctx := context.Background()

	entryA := CatalogEntry{
		ModelID:          "style-lora-v1",
		CatalogCode:      "MDL-LORA-STYLE-001",
		DisplayName:      "Style LoRA v1",
		Category:         "LORA",
		Filename:         "shared_name.safetensors",
		SHA256:           shaA,
		SizeBytes:        int64(len(bytesA)),
		VRAMFootprintMiB: 512,
		License:          "apache-2.0",
		SourceURLs:       []string{"https://huggingface.co/lora-a.safetensors"},
	}
	entryB := CatalogEntry{
		ModelID:          "style-lora-v2",
		CatalogCode:      "MDL-LORA-STYLE-002",
		DisplayName:      "Style LoRA v2",
		Category:         "LORA",
		Filename:         "shared_name.safetensors", // identical filename, distinct sha256!
		SHA256:           shaB,
		SizeBytes:        int64(len(bytesB)),
		VRAMFootprintMiB: 512,
		License:          "apache-2.0",
		SourceURLs:       []string{"https://huggingface.co/lora-b.safetensors"},
	}
	if err := svc.RegisterCatalogEntries(ctx, []CatalogEntry{entryA, entryB}); err != nil {
		t.Fatal(err)
	}
	if err := EnsureLayout(svc.RootDir); err != nil {
		t.Fatal(err)
	}

	// Deterministic event barrier for COMFY-P-B item 4:
	// Pause Fence 1 right at `before_publish_lock` after writing .staging/<shaA>.1.part.
	fence1Paused := make(chan struct{})
	releaseFence1 := make(chan struct{})
	var pausedFence1 atomic.Bool

	svc.Hook = StageHookFunc(func(_ context.Context, stage string, meta map[string]string) {
		if stage == "before_publish_lock" && meta["sha256"] == shaA && meta["fence"] == "1" {
			if pausedFence1.CompareAndSwap(false, true) {
				close(fence1Paused)
				<-releaseFence1
			}
		}
	})

	// Launch Fence 1 via Ensure.
	res1, err := svc.Ensure(ctx, EnsureRequest{
		OrganizationID: "org-1",
		ProjectID:      "proj-a",
		PrincipalID:    "user-1",
		IdempotencyKey: "idem-style-a-1",
		ModelID:        "style-lora-v1",
	})
	if err != nil || res1.Model.DownloadFence != 1 {
		t.Fatalf("Fence 1 Ensure failed: res=%+v err=%v", res1, err)
	}

	// Second concurrent Ensure from another project joins active Fence 1 without bumping fence!
	resJoin, err := svc.Ensure(ctx, EnsureRequest{
		OrganizationID: "org-1",
		ProjectID:      "proj-b",
		PrincipalID:    "user-2",
		IdempotencyKey: "idem-style-a-1", // same key in different project must NOT collide
		ModelID:        "style-lora-v1",
	})
	if err != nil || !resJoin.Deduplicated || resJoin.Model.DownloadFence != 1 {
		t.Fatalf("expected second caller to join active Fence 1, got %+v err=%v", resJoin, err)
	}

	// Wait for Fence 1 to reach `before_publish_lock`.
	<-fence1Paused

	// Simulate Fence 1 lease expiry and Fence 2 takeover + completion + active task pin BEFORE Fence 1 resumes!
	admit2, err := store.AdmitOrJoinDownload(ctx, AdmitDownloadInput{
		Entry:                 entryA,
		FilesystemTotalBytes:  2000 * giB,
		FilesystemFreeBytes:   600 * giB,
		ReservedHeadroomBytes: DefaultReservedHeadroomBytes,
		MaxAutoPullBytes:      DefaultMaxAutoPullModelBytes,
		LeaseOwner:            "runtime-takeover",
		LeaseTTL:              5 * time.Minute,
		Now:                   time.Now().UTC(),
		AllowTakeoverActive:   true,
	})
	if err != nil || admit2.Fence != 2 {
		t.Fatalf("expected takeover Fence 2, got %+v err=%v", admit2, err)
	}
	svc.RunDownloadFence(ctx, entryA, 2, entryA.SourceURLs[0], "")

	casPathA := CASFilePath(svc.RootDir, shaA)
	relViewA, _ := RelativeViewPath("LORA", shaA, entryA.Filename)
	viewPathA, _ := ViewFilePath(svc.RootDir, relViewA)

	inoBefore, err := inspectFileInode(casPathA)
	if err != nil {
		t.Fatalf("inspectFileInode before stale fence release: %v", err)
	}
	viewInoBefore, err := inspectFileInode(viewPathA)
	if err != nil || viewInoBefore != inoBefore {
		t.Fatalf("expected CAS and view hardlink to share inode (%d vs %d, err=%v)", inoBefore, viewInoBefore, err)
	}

	// Pin Fence 2's ready model with an active task pin.
	_, err = svc.ReservePins(ctx, ReservePinsRequest{
		HolderAppID:    "comfyui",
		ReservationID:  "resv-task-active-1",
		OperationID:    "op-task-1",
		ScopeKind:      PinScopeTaskExecution,
		OwnerID:        "task-001",
		OrganizationID: "org-1",
		ProjectID:      "proj-a",
		RequestSHA256:  strings.Repeat("1", 64),
		SHA256List:     []string{shaA},
	})
	if err != nil {
		t.Fatalf("ReservePins: %v", err)
	}
	if _, err := svc.CommitPins(ctx, CommitPinsRequest{HolderAppID: "comfyui", ReservationID: "resv-task-active-1"}); err != nil {
		t.Fatalf("CommitPins: %v", err)
	}

	// Now release stale Fence 1 and wait for background goroutines!
	close(releaseFence1)
	svc.WaitBackgroundDownloads()

	// Assert: cas/<shaA> inode, view inode, bytes, ready status, fence==2, and active_pins==1 are 100% untouched,
	// while .staging/<shaA>.1.part was cleanly deleted!
	inoAfter, err := inspectFileInode(casPathA)
	if err != nil || inoAfter != inoBefore {
		t.Fatalf("CAS inode mutated by stale fence! before=%d after=%d err=%v", inoBefore, inoAfter, err)
	}
	viewInoAfter, err := inspectFileInode(viewPathA)
	if err != nil || viewInoAfter != inoBefore {
		t.Fatalf("View inode mutated by stale fence! before=%d after=%d err=%v", inoBefore, viewInoAfter, err)
	}
	if _, statErr := os.Stat(PartFilePath(svc.RootDir, shaA, 1)); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("stale fence .part file was not cleaned up: %v", statErr)
	}
	afterStatus, err := svc.GetModelStatus(ctx, "style-lora-v1")
	if err != nil || afterStatus.Status != StatusReady || afterStatus.DownloadFence != 2 || afterStatus.ActivePins != 1 {
		t.Fatalf("unexpected model status after stale fence exit: %+v err=%v", afterStatus, err)
	}

	// COMFY-P-B item 9: Provision entryB (identical filename "shared_name.safetensors", different shaB).
	svc.SynchronousDownload = true
	resB, err := svc.Ensure(ctx, EnsureRequest{
		OrganizationID: "org-1",
		ProjectID:      "proj-a",
		PrincipalID:    "user-1",
		IdempotencyKey: "idem-style-b-1",
		ModelID:        "style-lora-v2",
	})
	if err != nil || resB.Model.Status != StatusReady {
		t.Fatalf("Ensure entryB failed: %+v err=%v", resB, err)
	}
	if resB.Model.RelativeViewPath == afterStatus.RelativeViewPath {
		t.Fatalf("same-filename distinct-sha256 views collided: %s", resB.Model.RelativeViewPath)
	}
}

type fakeRevoker struct {
	mu      sync.Mutex
	err     error
	results map[string]AppAdmissionVerification
}

func (f *fakeRevoker) RevokeOrVerify(_ context.Context, _, reservationID string) (AppAdmissionVerification, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return AppAdmissionVerification{}, f.err
	}
	res, ok := f.results[reservationID]
	if !ok {
		return AppAdmissionVerification{}, errors.New("not configured")
	}
	return res, nil
}

func TestCrossDBTwoPhasePinsReconcilerAndEvictBarriers(t *testing.T) {
	validBytes, validSHA := buildValidSafetensors(t, 128, nil)
	const giB = int64(1024 * 1024 * 1024)
	svc, _, _ := newTestService(t, 600*giB, map[string][]byte{
		"/sdxl.safetensors": validBytes,
	})
	now := time.Date(2026, 11, 15, 6, 0, 0, 0, time.UTC)
	svc.Now = func() time.Time { return now }
	revoker := &fakeRevoker{results: map[string]AppAdmissionVerification{}}
	svc.Revoker = revoker

	ctx := context.Background()
	_ = svc.RegisterCatalogEntries(ctx, []CatalogEntry{{
		ModelID:          "sdxl-base-1_0",
		CatalogCode:      "MDL-CKPT-SDXL-001",
		DisplayName:      "SDXL Base 1.0",
		Category:         "CKPT",
		Filename:         "sd_xl_base_1.0.safetensors",
		SHA256:           validSHA,
		SizeBytes:        int64(len(validBytes)),
		VRAMFootprintMiB: 6912,
		License:          "openrail++",
		SourceURLs:       []string{"https://huggingface.co/sdxl.safetensors"},
	}})
	if _, err := svc.Ensure(ctx, EnsureRequest{
		OrganizationID: "org-1",
		ProjectID:      "proj-1",
		PrincipalID:    "user-1",
		IdempotencyKey: "idem-sdxl-ready-1",
		ModelID:        "sdxl-base-1_0",
	}); err != nil {
		t.Fatal(err)
	}

	// Reserve two pins: resv-orphan (never committed in App SQLite) and resv-lost-ack (committed in App SQLite).
	for _, rID := range []string{"resv-orphan", "resv-lost-ack"} {
		_, err := svc.ReservePins(ctx, ReservePinsRequest{
			HolderAppID:    "comfyui",
			ReservationID:  rID,
			OperationID:    "op-" + rID,
			ScopeKind:      PinScopeTaskExecution,
			OwnerID:        "pending",
			OrganizationID: "org-1",
			ProjectID:      "proj-1",
			RequestSHA256:  strings.Repeat("2", 64),
			SHA256List:     []string{validSHA},
			TTLSeconds:     30,
		})
		if err != nil {
			t.Fatalf("ReservePins(%s): %v", rID, err)
		}
	}

	// Advance clock past TTL.
	now = now.Add(2 * time.Minute)

	// COMFY-P-B barrier 5a: Reconciler encounters RPC timeout -> MUST NOT release reserved pins!
	revoker.err = errors.New("rpc timeout")
	promoted, released, err := svc.ReconcileExpiredPins(ctx)
	if err != nil || promoted != 0 || released != 0 {
		t.Fatalf("expected 0 released on RPC timeout, got promoted=%d released=%d err=%v", promoted, released, err)
	}
	if _, evErr := svc.Evict(ctx, EvictRequest{SHA256: validSHA}); evErr == nil {
		t.Fatalf("expected Evict blocked by reserved pins when RPC timed out")
	}

	// COMFY-P-B barriers 5b & 6: App SQLite writes 'revoked' tombstone for resv-orphan, and confirms 'committed' for resv-lost-ack!
	revoker.err = nil
	revoker.results["resv-orphan"] = AppAdmissionVerification{
		ReservationID: "resv-orphan",
		State:         AdmissionStateRevoked,
	}
	revoker.results["resv-lost-ack"] = AppAdmissionVerification{
		ReservationID: "resv-lost-ack",
		State:         AdmissionStateCommitted,
		EntityID:      "task-committed-99",
	}
	promoted, released, err = svc.ReconcileExpiredPins(ctx)
	if err != nil || promoted != 1 || released != 1 {
		t.Fatalf("expected promoted=1 released=1, got promoted=%d released=%d err=%v", promoted, released, err)
	}

	// Late CommitPins for revoked resv-orphan MUST fail with 409 ADMISSION_RESERVATION_EXPIRED!
	_, commitErr := svc.CommitPins(ctx, CommitPinsRequest{HolderAppID: "comfyui", ReservationID: "resv-orphan"})
	if mErr, ok := AsError(commitErr); !ok || mErr.Status != 409 || mErr.Code != "ADMISSION_RESERVATION_EXPIRED" {
		t.Fatalf("expected 409 ADMISSION_RESERVATION_EXPIRED on late commit, got %v", commitErr)
	}

	// Evict while resv-lost-ack is active -> 409 MODEL_IN_USE (active_pins=1, reserved_pins=0)!
	_, evErr := svc.Evict(ctx, EvictRequest{SHA256: validSHA})
	if mErr, ok := AsError(evErr); !ok || mErr.Status != 409 || mErr.Code != "MODEL_IN_USE" || mErr.ActivePins != 1 || mErr.ReservedPins != 0 {
		t.Fatalf("expected 409 MODEL_IN_USE active=1, got %v", evErr)
	}

	// Release active pin, then pause inside `evicting` state to verify concurrent ReservePins -> 409 MODEL_NOT_READY!
	if _, err := svc.ReleasePins(ctx, ReleasePinsRequest{HolderAppID: "comfyui", ReservationID: "resv-lost-ack", Reason: "task_completed"}); err != nil {
		t.Fatal(err)
	}
	inEvicting := make(chan struct{})
	resumeEvict := make(chan struct{})
	svc.Hook = StageHookFunc(func(_ context.Context, stage string, _ map[string]string) {
		if stage == "during_evicting_unlink" {
			close(inEvicting)
			<-resumeEvict
		}
	})

	evictDone := make(chan EvictResponse, 1)
	go func() {
		res, e := svc.Evict(ctx, EvictRequest{SHA256: validSHA})
		if e != nil {
			t.Errorf("Evict failed: %v", e)
		}
		evictDone <- res
	}()

	<-inEvicting
	_, resvDuringEvictErr := svc.ReservePins(ctx, ReservePinsRequest{
		HolderAppID:    "comfyui",
		ReservationID:  "resv-during-evict",
		OperationID:    "op-during-evict",
		ScopeKind:      PinScopeTaskExecution,
		OwnerID:        "task-x",
		OrganizationID: "org-1",
		ProjectID:      "proj-1",
		RequestSHA256:  strings.Repeat("3", 64),
		SHA256List:     []string{validSHA},
	})
	if mErr, ok := AsError(resvDuringEvictErr); !ok || mErr.Status != 409 || mErr.Code != "MODEL_NOT_READY" {
		t.Fatalf("expected 409 MODEL_NOT_READY during evicting state, got %v", resvDuringEvictErr)
	}
	close(resumeEvict)
	evRes := <-evictDone
	if evRes.Status != StatusNotInstalled || evRes.Model == nil || evRes.Model.DownloadFence != 1 {
		t.Fatalf("expected status=not_installed with preserved download_fence=1, got %+v", evRes)
	}
}

func TestHTTPAdminAndAppHandlers(t *testing.T) {
	validBytes, validSHA := buildValidSafetensors(t, 128, nil)
	const giB = int64(1024 * 1024 * 1024)
	svc, _, _ := newTestService(t, 600*giB, map[string][]byte{
		"/sdxl.safetensors": validBytes,
	})

	adminToken := strings.Repeat("k", 40)
	comfyToken := "Bearer " + strings.Repeat("c", 40)
	blenderToken := "Bearer " + strings.Repeat("b", 40)
	callers := appinstances.AccessRegistry{
		sha256.Sum256([]byte(comfyToken)):   {AppID: "comfyui"},
		sha256.Sum256([]byte(blenderToken)): {AppID: "blender"},
	}

	adminSrv := httptest.NewServer(AdminHandler{Service: svc, Token: adminToken})
	defer adminSrv.Close()
	appSrv := httptest.NewServer(AppHandler{StationID: svc.StationID, Callers: callers, Service: svc})
	defer appSrv.Close()

	// 1. Admin register catalog entry.
	regBody, _ := json.Marshal(map[string]any{
		"models": []CatalogEntry{{
			ModelID:          "sdxl-base-1_0",
			CatalogCode:      "MDL-CKPT-SDXL-001",
			DisplayName:      "SDXL Base 1.0",
			Category:         "CKPT",
			Filename:         "sd_xl_base_1.0.safetensors",
			SHA256:           validSHA,
			SizeBytes:        int64(len(validBytes)),
			VRAMFootprintMiB: 6912,
			License:          "openrail++",
			SourceURLs:       []string{"https://huggingface.co/sdxl.safetensors"},
		}},
	})
	regReq, _ := http.NewRequest(http.MethodPost, adminSrv.URL+"/internal/v1/model-cache/catalog/register", bytes.NewReader(regBody))
	regReq.Header.Set("Authorization", "Bearer "+adminToken)
	regReq.Header.Set("Content-Type", "application/json")
	regResp, err := http.DefaultClient.Do(regReq)
	if err != nil || regResp.StatusCode != 200 {
		t.Fatalf("catalog/register failed: resp=%v err=%v", regResp, err)
	}
	_ = regResp.Body.Close()

	// 2. Non-comfyui caller (blender) is denied 403 on AppHandler.
	bReq, _ := http.NewRequest(http.MethodGet, appSrv.URL+"/internal/v1/model-cache/models", nil)
	bReq.Header.Set("Authorization", blenderToken)
	bReq.Header.Set("X-Station-Id", svc.StationID)
	bResp, err := http.DefaultClient.Do(bReq)
	if err != nil || bResp.StatusCode != 403 {
		t.Fatalf("expected 403 for non-comfyui caller, got %v err=%v", bResp, err)
	}
	_ = bResp.Body.Close()

	// 3. ComfyUI caller triggers model.ensure -> 200 ready (synchronous test mode), zero host path leakage!
	ensBody, _ := json.Marshal(EnsureRequest{
		OrganizationID: "org-1",
		ProjectID:      "proj-1",
		PrincipalID:    "user-1",
		IdempotencyKey: "idem-http-ensure-1",
		ModelID:        "sdxl-base-1_0",
	})
	ensReq, _ := http.NewRequest(http.MethodPost, appSrv.URL+"/internal/v1/model-cache/models/ensure", bytes.NewReader(ensBody))
	ensReq.Header.Set("Authorization", comfyToken)
	ensReq.Header.Set("X-Station-Id", svc.StationID)
	ensReq.Header.Set("Content-Type", "application/json")
	ensResp, err := http.DefaultClient.Do(ensReq)
	if err != nil || ensResp.StatusCode != 200 {
		t.Fatalf("models/ensure failed: resp=%v err=%v", ensResp, err)
	}
	rawEns, _ := io.ReadAll(ensResp.Body)
	_ = ensResp.Body.Close()
	if strings.Contains(string(rawEns), svc.RootDir) || strings.Contains(string(rawEns), "/data/models") {
		t.Fatalf("host path leaked in model.ensure response: %s", string(rawEns))
	}
}
