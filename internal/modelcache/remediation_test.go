package modelcache

import (
	"bufio"
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func generateSelfSignedHostCert(t *testing.T, dnsName string) (tls.Certificate, *x509.CertPool) {
	t.Helper()
	priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: dnsName},
		NotBefore:    time.Now().Add(-1 * time.Hour),
		NotAfter:     time.Now().Add(24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		DNSNames:     []string{dnsName},
		IsCA:         true,
		BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &priv.PublicKey, priv)
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	pool := x509.NewCertPool()
	pool.AddCert(parsed)
	return tls.Certificate{
		Certificate: [][]byte{der},
		PrivateKey:  priv,
		Leaf:        parsed,
	}, pool
}

func TestEgressProxyConnectUsesVerifiedIPLiteralOnly(t *testing.T) {
	payload, _ := buildValidSafetensors(t, 128, nil)

	// Origin TLS server presenting a certificate valid for "huggingface.co".
	originSrv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("ETag", `"proxy-etag-1"`)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(payload)
	}))
	originSrv.TLS = &tls.Config{MinVersion: tls.VersionTLS12}
	originSrv.StartTLS()
	defer originSrv.Close()

	// Add "huggingface.co" to a custom server leaf cert using originSrv's listener?
	// Even simpler: configure InsecureSkipVerify=false with VerifyPeerCertificate or custom cert!
	// Wait: httptest.NewTLSServer cert is for example.com / 127.0.0.1; let's inspect VerifyConnection or generate a self-signed cert for huggingface.co!
	certPool := x509.NewCertPool()
	// Let's check ServerName received at origin via GetConfigForClient!
	originSrv.Close()

	cert, rootPool := generateSelfSignedHostCert(t, "huggingface.co")
	var observedSNIMu sync.Mutex
	var observedSNI string
	originLn, err := tls.Listen("tcp", "127.0.0.1:0", &tls.Config{
		MinVersion:   tls.VersionTLS12,
		Certificates: []tls.Certificate{cert},
		GetConfigForClient: func(hello *tls.ClientHelloInfo) (*tls.Config, error) {
			observedSNIMu.Lock()
			observedSNI = hello.ServerName
			observedSNIMu.Unlock()
			return nil, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer originLn.Close()
	_ = certPool

	httpOrigin := &http.Server{
		Handler: http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("ETag", `"proxy-etag-1"`)
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write(payload)
		}),
	}
	go func() { _ = httpOrigin.Serve(originLn) }()
	defer httpOrigin.Close()

	// Controlled HTTP CONNECT proxy that records exact CONNECT authority & Host header,
	// then splices bytes to our local originTLS listener while asserting the client asked for 93.184.216.34:443!
	proxyLn, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer proxyLn.Close()

	var proxyMu sync.Mutex
	var connectTargets []string
	var connectHosts []string

	go func() {
		for {
			c, aErr := proxyLn.Accept()
			if aErr != nil {
				return
			}
			go func(clientConn net.Conn) {
				defer clientConn.Close()
				br := bufio.NewReader(clientConn)
				req, rErr := http.ReadRequest(br)
				if rErr != nil || req.Method != http.MethodConnect {
					return
				}
				proxyMu.Lock()
				connectTargets = append(connectTargets, req.RequestURI)
				connectHosts = append(connectHosts, req.Host)
				proxyMu.Unlock()

				if strings.Contains(strings.ToLower(req.RequestURI), "huggingface.co") || strings.Contains(strings.ToLower(req.Host), "huggingface.co") {
					_, _ = io.WriteString(clientConn, "HTTP/1.1 403 Forbidden\r\n\r\n")
					return
				}
				upConn, dErr := net.Dial("tcp", originLn.Addr().String())
				if dErr != nil {
					_, _ = io.WriteString(clientConn, "HTTP/1.1 502 Bad Gateway\r\n\r\n")
					return
				}
				defer upConn.Close()
				_, _ = io.WriteString(clientConn, "HTTP/1.1 200 Connection Established\r\n\r\n")

				done := make(chan struct{}, 2)
				go func() { _, _ = io.Copy(upConn, br); done <- struct{}{} }()
				go func() { _, _ = io.Copy(clientConn, upConn); done <- struct{}{} }()
				<-done
			}(c)
		}
	}()

	proxyURL := "http://" + proxyLn.Addr().String()
	dl, err := NewSafeDownloader(nil, proxyURL)
	if err != nil {
		t.Fatalf("NewSafeDownloader: %v", err)
	}
	dl.Resolver = staticResolver{
		"huggingface.co": {{IP: net.ParseIP("93.184.216.34")}},
	}
	dl.TLSConfig = &tls.Config{
		MinVersion: tls.VersionTLS12,
		RootCAs:    rootPool,
	}

	rootDir := t.TempDir()
	if err := EnsureLayout(rootDir); err != nil {
		t.Fatal(err)
	}
	relPart := PartRelPath(strings.Repeat("a", 64), 1)
	res, err := dl.DownloadToPart(context.Background(), DownloadOptions{
		SourceURL:    "https://huggingface.co/model.safetensors",
		RootDir:      rootDir,
		RelPartPath:  relPart,
		PartPath:     PartFilePath(rootDir, strings.Repeat("a", 64), 1),
		ExpectedSize: int64(len(payload)),
	})
	if err != nil {
		t.Fatalf("DownloadToPart via pinned CONNECT proxy failed: %v", err)
	}
	if res.BytesWritten != int64(len(payload)) {
		t.Fatalf("unexpected bytes written: %d", res.BytesWritten)
	}

	proxyMu.Lock()
	defer proxyMu.Unlock()
	if len(connectTargets) != 1 || connectTargets[0] != "93.184.216.34:443" || connectHosts[0] != "93.184.216.34:443" {
		t.Fatalf("expected CONNECT 93.184.216.34:443 only (no hostname), got targets=%v hosts=%v", connectTargets, connectHosts)
	}
	observedSNIMu.Lock()
	defer observedSNIMu.Unlock()
	if observedSNI != "huggingface.co" {
		t.Fatalf("expected end-to-end TLS SNI huggingface.co, got %q", observedSNI)
	}
}

func TestStagingHardlinkSymlinkAndPostVerifySwapRejected(t *testing.T) {
	validBytes, validSHA := buildValidSafetensors(t, 128, nil)
	const giB = int64(1024 * 1024 * 1024)
	svc, _, _ := newTestService(t, 600*giB, map[string][]byte{
		"/lora.safetensors": validBytes,
	})
	ctx := context.Background()
	entry := CatalogEntry{
		ModelID:          "hardlink-guard-lora",
		CatalogCode:      "MDL-LORA-HLNK-001",
		DisplayName:      "Hardlink Guard LoRA",
		Category:         "LORA",
		Filename:         "guard_lora.safetensors",
		SHA256:           validSHA,
		SizeBytes:        int64(len(validBytes)),
		VRAMFootprintMiB: 256,
		License:          "apache-2.0",
		SourceURLs:       []string{"https://huggingface.co/lora.safetensors"},
	}
	if err := svc.RegisterCatalogEntries(ctx, []CatalogEntry{entry}); err != nil {
		t.Fatal(err)
	}

	// 1. Pre-plant a hardlinked victim file at .staging/<sha>.1.part (nlink == 2).
	// OpenStagingPartForWrite MUST unlink & create a brand-new inode (nlink == 1), leaving victim untouched!
	victimPath := filepath.Join(t.TempDir(), "victim.txt")
	// Put victim inside same filesystem (under rootDir parent) so os.Link succeeds.
	victimSameFS := filepath.Join(filepath.Dir(svc.RootDir), "victim_same_fs.bin")
	sentinel := []byte("DO_NOT_OVERWRITE_VICTIM")
	if err := os.WriteFile(victimSameFS, sentinel, 0o600); err != nil {
		t.Fatal(err)
	}
	defer os.Remove(victimSameFS)
	_ = victimPath

	if err := EnsureLayout(svc.RootDir); err != nil {
		t.Fatal(err)
	}
	part1 := PartFilePath(svc.RootDir, validSHA, 1)
	if err := os.Link(victimSameFS, part1); err != nil {
		t.Fatalf("os.Link pre-plant failed: %v", err)
	}

	// 2. Also inject a post-verify file replacement right at `before_publish_lock` on Fence 1!
	evilBytes := append([]byte(nil), validBytes...)
	evilBytes[len(evilBytes)-1] ^= 0xff
	svc.Hook = StageHookFunc(func(_ context.Context, stage string, meta map[string]string) {
		if stage == "before_publish_lock" && meta["fence"] == "1" {
			// Attacker replaces .staging/<sha>.1.part with a different inode after OpenAndVerifyStagingPart!
			movedOld := part1 + ".displaced"
			_ = os.Rename(part1, movedOld)
			defer os.Remove(movedOld)
			_ = os.WriteFile(part1, evilBytes, 0o444)
		}
	})

	res1, err := svc.Ensure(ctx, EnsureRequest{
		OrganizationID: "org-1",
		ProjectID:      "proj-1",
		PrincipalID:    "user-1",
		IdempotencyKey: "idem-post-verify-swap-1",
		ModelID:        entry.ModelID,
	})
	if err != nil {
		t.Fatalf("Ensure returned unexpected sync err: %v", err)
	}
	if res1.Model.Status != StatusFailed {
		t.Fatalf("expected post-verify inode swap to fail publish, got status=%s", res1.Model.Status)
	}
	if _, statErr := os.Lstat(CASFilePath(svc.RootDir, validSHA)); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("evil swapped .part must NEVER be published to CAS! statErr=%v", statErr)
	}
	gotVictim, err := os.ReadFile(victimSameFS)
	if err != nil || string(gotVictim) != string(sentinel) {
		t.Fatalf("pre-planted hardlink victim was modified! got=%q err=%v", string(gotVictim), err)
	}
}

func TestConcurrentEvictAndInodeGuardedUnlink(t *testing.T) {
	validBytes, validSHA := buildValidSafetensors(t, 128, nil)
	const giB = int64(1024 * 1024 * 1024)
	svc, _, _ := newTestService(t, 600*giB, map[string][]byte{
		"/ckpt.safetensors": validBytes,
	})
	ctx := context.Background()
	entry := CatalogEntry{
		ModelID:          "evict-race-ckpt",
		CatalogCode:      "MDL-CKPT-EVCT-001",
		DisplayName:      "Evict Race Checkpoint",
		Category:         "CKPT",
		Filename:         "evict_race.safetensors",
		SHA256:           validSHA,
		SizeBytes:        int64(len(validBytes)),
		VRAMFootprintMiB: 2048,
		License:          "mit",
		SourceURLs:       []string{"https://huggingface.co/ckpt.safetensors"},
	}
	_ = svc.RegisterCatalogEntries(ctx, []CatalogEntry{entry})
	if _, err := svc.Ensure(ctx, EnsureRequest{
		OrganizationID: "org-1",
		ProjectID:      "proj-1",
		PrincipalID:    "user-1",
		IdempotencyKey: "idem-evict-race-init",
		ModelID:        entry.ModelID,
	}); err != nil {
		t.Fatal(err)
	}

	inUnlink := make(chan struct{})
	releaseUnlink := make(chan struct{})
	svc.Hook = StageHookFunc(func(_ context.Context, stage string, _ map[string]string) {
		if stage == "during_evicting_unlink" {
			close(inUnlink)
			<-releaseUnlink
		}
	})

	e1ErrCh := make(chan error, 1)
	go func() {
		_, e := svc.Evict(ctx, EvictRequest{SHA256: validSHA})
		e1ErrCh <- e
	}()

	<-inUnlink

	// Concurrent Evict E2 MUST be rejected with 409 OPERATION_IN_PROGRESS!
	_, e2Err := svc.Evict(ctx, EvictRequest{SHA256: validSHA})
	if mErr, ok := AsError(e2Err); !ok || mErr.Status != 409 || mErr.Code != "OPERATION_IN_PROGRESS" {
		t.Fatalf("expected E2 409 OPERATION_IN_PROGRESS during E1 evicting, got %v", e2Err)
	}

	// Concurrent Ensure during evicting MUST be rejected with 409 MODEL_NOT_READY!
	_, ensErr := svc.Ensure(ctx, EnsureRequest{
		OrganizationID: "org-1",
		ProjectID:      "proj-1",
		PrincipalID:    "user-1",
		IdempotencyKey: "idem-ensure-during-evict",
		ModelID:        entry.ModelID,
	})
	if mErr, ok := AsError(ensErr); !ok || mErr.Status != 409 || mErr.Code != "MODEL_NOT_READY" {
		t.Fatalf("expected Ensure 409 MODEL_NOT_READY during evicting, got %v", ensErr)
	}

	close(releaseUnlink)
	if e1Err := <-e1ErrCh; e1Err != nil {
		t.Fatalf("E1 Evict failed: %v", e1Err)
	}

	// Re-ensure to ready, then verify EvictCASAndViews refuses to delete when expectedCASInode mismatches!
	svc.Hook = nil
	if _, err := svc.Ensure(ctx, EnsureRequest{
		OrganizationID: "org-1",
		ProjectID:      "proj-1",
		PrincipalID:    "user-1",
		IdempotencyKey: "idem-evict-race-redownload",
		ModelID:        entry.ModelID,
	}); err != nil {
		t.Fatal(err)
	}
	wrongIno := uint64(999999999)
	relView, _ := RelativeViewPath(entry.Category, entry.SHA256, entry.Filename)
	if err := EvictCASAndViews(svc.RootDir, validSHA, []string{relView}, &wrongIno); err != nil {
		t.Fatalf("EvictCASAndViews with mismatched inode returned err: %v", err)
	}
	if _, err := os.Lstat(CASFilePath(svc.RootDir, validSHA)); err != nil {
		t.Fatalf("EvictCASAndViews deleted newer CAS inode despite expectedCASInode mismatch: %v", err)
	}
}

func TestMultiAliasSingleflightIdempotencyAndApprovalRetry(t *testing.T) {
	validBytes, validSHA := buildValidSafetensors(t, 128, nil)
	const giB = int64(1024 * 1024 * 1024)
	svc, _, _ := newTestService(t, 600*giB, map[string][]byte{
		"/shared.safetensors": validBytes,
	})
	now := time.Date(2026, 11, 15, 7, 0, 0, 0, time.UTC)
	svc.Now = func() time.Time { return now }
	ctx := context.Background()

	aliasA := CatalogEntry{
		ModelID:          "shared-lora-a",
		CatalogCode:      "MDL-LORA-SHRA-001",
		DisplayName:      "Shared LoRA Alias A",
		Category:         "LORA",
		Filename:         "shared_alias_a.safetensors",
		SHA256:           validSHA,
		SizeBytes:        int64(len(validBytes)),
		VRAMFootprintMiB: 256,
		License:          "apache-2.0",
		SourceURLs:       []string{"https://huggingface.co/shared.safetensors"},
	}
	aliasB := CatalogEntry{
		ModelID:          "shared-lora-b",
		CatalogCode:      "MDL-LORA-SHRB-002",
		DisplayName:      "Shared LoRA Alias B",
		Category:         "LORA",
		Filename:         "shared_alias_b.safetensors",
		SHA256:           validSHA,
		SizeBytes:        int64(len(validBytes)),
		VRAMFootprintMiB: 256,
		License:          "apache-2.0",
		SourceURLs:       []string{"https://huggingface.co/shared.safetensors"},
	}
	pendingLocal := CatalogEntry{
		ModelID:          "local-pending-vae",
		CatalogCode:      "MDL-VAE-LOCAL-001",
		DisplayName:      "Local Pending VAE",
		Category:         "VAE",
		Filename:         "local_pending_vae.safetensors",
		SHA256:           strings.Repeat("b", 64),
		SizeBytes:        int64(len(validBytes)),
		VRAMFootprintMiB: 512,
		License:          "cc-by-sa-4.0",
		OriginKind:       OriginLocal,
		ApprovalState:    ApprovalPendingAdmin,
		SourceURLs:       []string{"https://huggingface.co/shared.safetensors"},
	}
	pendingLocal.SHA256 = validSHA // Reuse valid bytes once approved
	pendingLocal.Filename = "local_approved_vae.safetensors"

	if err := svc.RegisterCatalogEntries(ctx, []CatalogEntry{aliasA, aliasB, pendingLocal}); err != nil {
		t.Fatal(err)
	}

	// 1. Multi-alias singleflight: pause aliasA at before_download_stream, join with aliasB, then resume!
	svc.SynchronousDownload = false
	dlPaused := make(chan struct{})
	dlResume := make(chan struct{})
	svc.Hook = StageHookFunc(func(_ context.Context, stage string, _ map[string]string) {
		if stage == "before_download_stream" {
			close(dlPaused)
			<-dlResume
		}
	})

	rA, err := svc.Ensure(ctx, EnsureRequest{
		OrganizationID: "org-1",
		ProjectID:      "proj-1",
		PrincipalID:    "user-1",
		TargetAlias:    "lora_slot_1",
		IdempotencyKey: "idem-multi-alias-key",
		ModelID:        aliasA.ModelID,
	})
	if err != nil || rA.HTTPStatus != 202 {
		t.Fatalf("Ensure aliasA failed: %+v err=%v", rA, err)
	}
	<-dlPaused

	// Same idempotency_key with DIFFERENT target_alias ("lora_slot_2") succeeds cleanly and joins singleflight!
	rB, err := svc.Ensure(ctx, EnsureRequest{
		OrganizationID: "org-1",
		ProjectID:      "proj-1",
		PrincipalID:    "user-1",
		TargetAlias:    "lora_slot_2",
		IdempotencyKey: "idem-multi-alias-key",
		ModelID:        aliasB.ModelID,
	})
	if err != nil || !rB.Deduplicated || rB.HTTPStatus != 202 {
		t.Fatalf("Ensure aliasB join failed: %+v err=%v", rB, err)
	}

	// Same idempotency_key + SAME target_alias ("lora_slot_1") with different model_id -> 409 IDEMPOTENCY_CONFLICT!
	_, conflictErr := svc.Ensure(ctx, EnsureRequest{
		OrganizationID: "org-1",
		ProjectID:      "proj-1",
		PrincipalID:    "user-1",
		TargetAlias:    "lora_slot_1",
		IdempotencyKey: "idem-multi-alias-key",
		ModelID:        aliasB.ModelID,
	})
	if mErr, ok := AsError(conflictErr); !ok || mErr.Status != 409 || mErr.Code != "IDEMPOTENCY_CONFLICT" {
		t.Fatalf("expected 409 IDEMPOTENCY_CONFLICT on same target_alias, got %v", conflictErr)
	}

	close(dlResume)
	svc.WaitBackgroundDownloads()
	svc.Hook = nil
	svc.SynchronousDownload = true

	// BOTH aliasA and aliasB MUST now be StatusReady with published views on disk!
	stA, err := svc.GetModelStatus(ctx, aliasA.ModelID)
	if err != nil || stA.Status != StatusReady {
		t.Fatalf("expected aliasA StatusReady after singleflight, got %+v err=%v", stA, err)
	}
	stB, err := svc.GetModelStatus(ctx, aliasB.ModelID)
	if err != nil || stB.Status != StatusReady {
		t.Fatalf("expected aliasB StatusReady after singleflight (pending_views published), got %+v err=%v", stB, err)
	}

	// 2. Expired idempotency key reuse with new parameters after 25 hours MUST succeed!
	now = now.Add(25 * time.Hour)
	reusedExpired, err := svc.Ensure(ctx, EnsureRequest{
		OrganizationID: "org-1",
		ProjectID:      "proj-1",
		PrincipalID:    "user-1",
		TargetAlias:    "lora_slot_1",
		IdempotencyKey: "idem-multi-alias-key",
		ModelID:        aliasB.ModelID,
	})
	if err != nil || reusedExpired.Model.Status != StatusReady {
		t.Fatalf("expected expired idempotency key reuse to succeed, got %+v err=%v", reusedExpired, err)
	}

	// 3. MODEL_APPROVAL_REQUIRED is NOT cached as a 24h blocker: post-approval retry with SAME key succeeds!
	appReq := EnsureRequest{
		OrganizationID: "org-1",
		ProjectID:      "proj-1",
		PrincipalID:    "user-1",
		TargetAlias:    "vae_main",
		IdempotencyKey: "idem-approval-retry-1",
		ModelID:        pendingLocal.ModelID,
	}
	appRes1, err := svc.Ensure(ctx, appReq)
	if err != nil || appRes1.Code != "MODEL_APPROVAL_REQUIRED" {
		t.Fatalf("expected 202 MODEL_APPROVAL_REQUIRED before approval, got %+v err=%v", appRes1, err)
	}
	pendingLocal.ApprovalState = ApprovalApproved
	if err := svc.RegisterCatalogEntries(ctx, []CatalogEntry{pendingLocal}); err != nil {
		t.Fatal(err)
	}
	appRes2, err := svc.Ensure(ctx, appReq)
	if err != nil || appRes2.Code != "" || appRes2.Model.Status != StatusReady {
		t.Fatalf("expected post-approval retry with same idempotency_key to reach StatusReady, got %+v err=%v", appRes2, err)
	}
}

func TestStaleActiveTaskPinReconcilerAndFaultInjection(t *testing.T) {
	validBytes, validSHA := buildValidSafetensors(t, 128, nil)
	const giB = int64(1024 * 1024 * 1024)
	svc, _, _ := newTestService(t, 600*giB, map[string][]byte{
		"/unet.safetensors": validBytes,
	})
	now := time.Date(2026, 11, 15, 8, 0, 0, 0, time.UTC)
	svc.Now = func() time.Time { return now }
	svc.ActiveTaskReconcileAge = 30 * time.Second
	revoker := &fakeRevoker{results: map[string]AppAdmissionVerification{}}
	svc.Revoker = revoker
	ctx := context.Background()

	entry := CatalogEntry{
		ModelID:          "flux-schnell-unet",
		CatalogCode:      "MDL-UNET-FLUX-001",
		DisplayName:      "Flux Schnell UNET",
		Category:         "UNET",
		Filename:         "flux_schnell.safetensors",
		SHA256:           validSHA,
		SizeBytes:        int64(len(validBytes)),
		VRAMFootprintMiB: 12288,
		License:          "apache-2.0",
		SourceURLs:       []string{"https://huggingface.co/unet.safetensors"},
	}
	_ = svc.RegisterCatalogEntries(ctx, []CatalogEntry{entry})

	// 1. Fault-inject chmod 0444 failure -> must fail closed (StatusFailed, zero CAS/view published)!
	testHookSetReadOnly = func(_ string) error {
		return errors.New("simulated EIO on chmod 0444")
	}
	failRes, err := svc.Ensure(ctx, EnsureRequest{
		OrganizationID: "org-1",
		ProjectID:      "proj-1",
		PrincipalID:    "user-1",
		IdempotencyKey: "idem-chmod-fault-1",
		ModelID:        entry.ModelID,
	})
	testHookSetReadOnly = nil
	if err != nil || failRes.Model.Status != StatusFailed || failRes.Model.LastErrorCode != "STORAGE_IO_ERROR" {
		t.Fatalf("expected chmod fault to fail closed with STORAGE_IO_ERROR, got %+v err=%v", failRes, err)
	}
	if _, sErr := os.Lstat(CASFilePath(svc.RootDir, validSHA)); !errors.Is(sErr, os.ErrNotExist) {
		t.Fatalf("CAS file must not exist when chmod 0444 failed")
	}

	// 2. Fault-inject fsyncDir("cas") failure -> must fail closed!
	testHookFsyncDir = func(relDir string) error {
		if relDir == "cas" {
			return errors.New("simulated EIO on cas dir fsync")
		}
		return nil
	}
	failRes2, err := svc.Ensure(ctx, EnsureRequest{
		OrganizationID: "org-1",
		ProjectID:      "proj-1",
		PrincipalID:    "user-1",
		IdempotencyKey: "idem-fsync-fault-2",
		ModelID:        entry.ModelID,
	})
	testHookFsyncDir = nil
	if err != nil || failRes2.Model.Status != StatusFailed {
		t.Fatalf("expected fsyncDir fault to fail closed, got %+v err=%v", failRes2, err)
	}
	faultCASIno, inoErr := inspectFileInode(CASFilePath(svc.RootDir, validSHA))
	if inoErr != nil {
		t.Fatalf("expected fault-injected CAS inode before test cleanup, got %v", inoErr)
	}
	if err := EvictCASAndViews(svc.RootDir, validSHA, []string{failRes2.Model.RelativeViewPath}, &faultCASIno); err != nil {
		t.Fatalf("EvictCASAndViews cleanup after fsync fault failed: %v", err)
	}

	// 3. Normal Ensure -> ReservePins -> CommitPins (active task_execution) -> task completes in App SQLite (State: released) -> Reconciler frees pin!
	okRes, err := svc.Ensure(ctx, EnsureRequest{
		OrganizationID: "org-1",
		ProjectID:      "proj-1",
		PrincipalID:    "user-1",
		IdempotencyKey: "idem-normal-ok-3",
		ModelID:        entry.ModelID,
	})
	if err != nil || okRes.Model.Status != StatusReady {
		t.Fatalf("expected StatusReady, got %+v err=%v", okRes, err)
	}
	_, _ = svc.ReservePins(ctx, ReservePinsRequest{
		HolderAppID:    "comfyui",
		ReservationID:  "resv-stale-active-task",
		OperationID:    "op-stale-active-task",
		ScopeKind:      PinScopeTaskExecution,
		OwnerID:        "task-finished-77",
		OrganizationID: "org-1",
		ProjectID:      "proj-1",
		RequestSHA256:  strings.Repeat("7", 64),
		SHA256List:     []string{validSHA},
	})
	_, _ = svc.CommitPins(ctx, CommitPinsRequest{HolderAppID: "comfyui", ReservationID: "resv-stale-active-task"})

	now = now.Add(2 * time.Minute)
	revoker.results["resv-stale-active-task"] = AppAdmissionVerification{
		ReservationID: "resv-stale-active-task",
		State:         AdmissionStateReleased,
		EntityID:      "task-finished-77",
	}
	promoted, released, err := svc.ReconcileExpiredPins(ctx)
	if err != nil || promoted != 0 || released != 1 {
		t.Fatalf("expected stale active task pin released=1, got promoted=%d released=%d err=%v", promoted, released, err)
	}
	if _, evErr := svc.Evict(ctx, EvictRequest{SHA256: validSHA}); evErr != nil {
		t.Fatalf("expected Evict to succeed after stale active task pin reconciliation: %v", evErr)
	}
}

func TestLicenseAllowlistAndCanonicalModelLockGoldenSHA256(t *testing.T) {
	// 1. Exact 5 commercial-free licenses; cc-by-sa-4.0 is research_non_commercial; EarlyAccess string "true" is paid_forbidden.
	if tier := ClassifyLicenseTier("cc-by-sa-4.0", false, nil); tier != LicenseTierResearchNonCommercial {
		t.Fatalf("expected cc-by-sa-4.0 -> research_non_commercial, got %s", tier)
	}
	if tier := ClassifyLicenseTier("apache-2.0", false, map[string]any{"EarlyAccess": "true"}); tier != LicenseTierPaidForbidden {
		t.Fatalf("expected EarlyAccess='true' -> paid_forbidden, got %s", tier)
	}

	// 2. Explicit LicenseTier: commercial_free MUST NOT loosen a non-commercial license!
	ncEntry := CatalogEntry{
		ModelID:          "nc-lora",
		CatalogCode:      "MDL-LORA-NCOM-001",
		DisplayName:      "Non Commercial LoRA",
		Category:         "LORA",
		Filename:         "nc_lora.safetensors",
		SHA256:           strings.Repeat("c", 64),
		SizeBytes:        1024,
		VRAMFootprintMiB: 128,
		License:          "cc-by-nc-4.0",
		LicenseTier:      LicenseTierCommercialFree,
		ApprovalState:    ApprovalPendingAdmin,
	}
	if err := ncEntry.NormalizeAndValidate(); err != nil {
		t.Fatalf("NormalizeAndValidate: %v", err)
	}
	if ncEntry.LicenseTier != LicenseTierResearchNonCommercial {
		t.Fatalf("expected LicenseTier kept at research_non_commercial, got %s", ncEntry.LicenseTier)
	}

	// 3. LoadCatalogYAML MUST reject entries missing explicit paid_access boolean!
	missingPaidYAML := []byte(`
models:
  - model_id: sdxl-base-1_0
    catalog_code: MDL-CKPT-SDXL-001
    display_name: SDXL Base 1.0
    category: CKPT
    filename: sd_xl_base_1.0.safetensors
    sha256: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    size_bytes: 1048576
    vram_footprint_mib: 6912
    license: openrail++
`)
	if _, err := LoadCatalogYAML(missingPaidYAML); err == nil {
		t.Fatalf("expected LoadCatalogYAML to reject entry missing explicit paid_access")
	}

	// 4. Cross-language golden canonical_model_lock_v1 SHA-256 (must match Python compute_model_dependencies_sha256).
	digest, _ := ComputeModelDependenciesSHA256([]CanonicalModelLockV1{
		{
			CatalogCode:      "MDL-LORA-DETL-002",
			ModelID:          "sdxl-detail-lora",
			Category:         "LORA",
			Filename:         "detail_tweaker_xl.safetensors",
			SHA256:           "2222222222222222222222222222222222222222222222222222222222222222",
			SizeBytes:        228589568,
			VRAMFootprintMiB: 384,
			License:          "apache-2.0",
			PaidAccess:       false,
		},
		{
			CatalogCode:      "MDL-CKPT-SDXL-001",
			ModelID:          "sdxl-base-1_0",
			Category:         "CKPT",
			Filename:         "sd_xl_base_1.0.safetensors",
			SHA256:           "1111111111111111111111111111111111111111111111111111111111111111",
			SizeBytes:        6938078334,
			VRAMFootprintMiB: 6912,
			License:          "openrail++",
			PaidAccess:       false,
		},
	})
	const expectedGoldenDigest = "5c805448667c510d5eb171a8297f61471f2df75e15f49914a8d901209167dbb0"
	if digest != expectedGoldenDigest {
		t.Fatalf("golden canonical_model_lock_v1 digest mismatch: got=%s want=%s", digest, expectedGoldenDigest)
	}
}

func setupModelCachePGStore(t *testing.T) (*PGStore, string) {
	t.Helper()
	dsn := os.Getenv("STATION_TEST_DATABASE_URL")
	if dsn == "" {
		if os.Getenv("VERDANTFLARE_REQUIRE_TEST_DB") == "1" {
			t.Fatal("STATION_TEST_DATABASE_URL required when VERDANTFLARE_REQUIRE_TEST_DB=1")
		}
		t.Skip("STATION_TEST_DATABASE_URL required for PGStore integration")
	}

	ctx := context.Background()
	adminCfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	if strings.EqualFold(strings.TrimSpace(adminCfg.ConnConfig.Database), "station") {
		t.Fatal("refusing to connect modelcache test admin pool to live 'station' database")
	}
	admin, err := pgxpool.NewWithConfig(ctx, adminCfg)
	if err != nil {
		t.Fatal(err)
	}
	dbName := "modelcache_test_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	if _, err := admin.Exec(ctx, "CREATE DATABASE "+pgx.Identifier{dbName}.Sanitize()); err != nil {
		admin.Close()
		t.Fatal(err)
	}
	cfg := admin.Config()
	cfg.ConnConfig.Database = dbName
	cfg.MaxConns = 8
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		admin.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		pool.Close()
		_, dropErr := admin.Exec(context.Background(), "DROP DATABASE "+pgx.Identifier{dbName}.Sanitize()+" WITH (FORCE)")
		admin.Close()
		if dropErr != nil {
			t.Error(dropErr)
		}
	})

	migRoot := os.Getenv("STATION_TEST_MIGRATIONS")
	if migRoot == "" {
		migRoot = "../../../verdantflare-station-core/migrations"
	}
	files, err := filepath.Glob(filepath.Join(migRoot, "*.sql"))
	if err != nil || len(files) < 24 {
		t.Fatalf("expected >= 24 migrations, got %d (%v)", len(files), err)
	}
	sort.Strings(files)
	if _, err := pool.Exec(ctx, "CREATE SCHEMA station"); err != nil {
		t.Fatal(err)
	}
	for _, file := range files {
		raw, err := os.ReadFile(file)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := pool.Exec(ctx, string(raw)); err != nil {
			t.Fatalf("migration %s failed: %v", filepath.Base(file), err)
		}
	}

	stationID := uuid.NewString()
	if _, err := pool.Exec(ctx, `INSERT INTO station.configuration(singleton, station_id, contracts_major) VALUES (true, $1, 1)`, stationID); err != nil {
		t.Fatal(err)
	}
	return &PGStore{Pool: pool, StationID: stationID}, stationID
}

func TestPGStoreFullLifecycleAndConstraints(t *testing.T) {
	pgStore, stationID := setupModelCachePGStore(t)
	validBytes, validSHA := buildValidSafetensors(t, 128, nil)
	const giB = int64(1024 * 1024 * 1024)

	mux := http.NewServeMux()
	mux.HandleFunc("/sdxl.safetensors", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("ETag", `"pg-etag-1"`)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(validBytes)
	})
	cert, rootPool := generateSelfSignedHostCert(t, "huggingface.co")
	srv := httptest.NewUnstartedServer(mux)
	srv.TLS = &tls.Config{
		MinVersion:   tls.VersionTLS12,
		Certificates: []tls.Certificate{cert},
	}
	srv.StartTLS()
	defer srv.Close()

	dl, err := NewSafeDownloader(nil, "")
	if err != nil {
		t.Fatal(err)
	}
	dl.Resolver = staticResolver{"huggingface.co": {{IP: net.ParseIP("93.184.216.34")}}}
	dl.TransportForTarget = func(target VerifiedTarget) (http.RoundTripper, func(), error) {
		srvURL, _ := url.Parse(srv.URL)
		base := &http.Transport{
			Proxy:             nil,
			DisableKeepAlives: true,
			TLSClientConfig: &tls.Config{
				MinVersion: tls.VersionTLS12,
				ServerName: target.Hostname,
				RootCAs:    rootPool,
			},
			DialContext: func(ctx context.Context, network, _ string) (net.Conn, error) {
				var d net.Dialer
				return d.DialContext(ctx, network, srvURL.Host)
			},
		}
		return base, base.CloseIdleConnections, nil
	}

	rootDir := t.TempDir()
	if err := EnsureLayout(rootDir); err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 11, 15, 9, 0, 0, 0, time.UTC)
	revoker := &fakeRevoker{results: map[string]AppAdmissionVerification{}}
	svc := &Service{
		Store:                  pgStore,
		StationID:              stationID,
		RootDir:                rootDir,
		Downloader:             dl,
		DiskStat:               func(string) (int64, int64, error) { return 2000 * giB, 600 * giB, nil },
		Revoker:                revoker,
		ActiveTaskReconcileAge: 30 * time.Second,
		Now:                    func() time.Time { return now },
		SynchronousDownload:    true,
	}

	ctx := context.Background()
	entryA := CatalogEntry{
		ModelID:          "pg-sdxl-a",
		CatalogCode:      "MDL-CKPT-PGA1-001",
		DisplayName:      "PG SDXL Alias A",
		Category:         "CKPT",
		Filename:         "pg_sdxl_a.safetensors",
		SHA256:           validSHA,
		SizeBytes:        int64(len(validBytes)),
		VRAMFootprintMiB: 6912,
		License:          "openrail++",
		SourceURLs:       []string{"https://huggingface.co/sdxl.safetensors"},
	}
	entryPending := CatalogEntry{
		ModelID:          "pg-local-pending",
		CatalogCode:      "MDL-LORA-LOCAL-001",
		DisplayName:      "PG Local Pending",
		Category:         "LORA",
		Filename:         "pg_local.safetensors",
		SHA256:           validSHA,
		SizeBytes:        int64(len(validBytes)),
		VRAMFootprintMiB: 256,
		License:          "cc-by-sa-4.0",
		OriginKind:       OriginLocal,
		ApprovalState:    ApprovalPendingAdmin,
		SourceURLs:       []string{"https://huggingface.co/sdxl.safetensors"},
	}
	if err := svc.RegisterCatalogEntries(ctx, []CatalogEntry{entryA, entryPending}); err != nil {
		t.Fatalf("PGStore UpsertCatalogEntries: %v", err)
	}

	// 1. Approval gate does NOT leave stale idempotency record in PG; post-approval retry succeeds!
	pReq := EnsureRequest{
		OrganizationID: "org-pg",
		ProjectID:      "proj-pg",
		PrincipalID:    "user-pg",
		TargetAlias:    "lora_1",
		IdempotencyKey: "pg-idem-approval-1",
		ModelID:        entryPending.ModelID,
	}
	pRes1, err := svc.Ensure(ctx, pReq)
	if err != nil || pRes1.Code != "MODEL_APPROVAL_REQUIRED" {
		t.Fatalf("expected MODEL_APPROVAL_REQUIRED on PGStore, got %+v err=%v", pRes1, err)
	}
	entryPending.ApprovalState = ApprovalApproved
	if err := svc.RegisterCatalogEntries(ctx, []CatalogEntry{entryPending}); err != nil {
		t.Fatal(err)
	}
	pRes2, err := svc.Ensure(ctx, pReq)
	if err != nil || pRes2.Model.Status != StatusReady {
		t.Fatalf("expected StatusReady on PGStore post-approval retry, got %+v err=%v", pRes2, err)
	}

	// 2. Ensure second alias entryA on already-ready CAS blob -> publishes view & returns 200 ready!
	aRes, err := svc.Ensure(ctx, EnsureRequest{
		OrganizationID: "org-pg",
		ProjectID:      "proj-pg",
		PrincipalID:    "user-pg",
		TargetAlias:    "ckpt_main",
		IdempotencyKey: "pg-idem-approval-1",
		ModelID:        entryA.ModelID,
	})
	if err != nil || aRes.HTTPStatus != 200 || aRes.Model.Status != StatusReady {
		t.Fatalf("expected second alias 200 ready on PGStore, got %+v err=%v", aRes, err)
	}

	// 3. ReservePins -> CommitPins -> Evict blocked (409 MODEL_IN_USE) -> Reconciler releases terminal task -> Evict succeeds!
	if _, err := svc.ReservePins(ctx, ReservePinsRequest{
		HolderAppID:    "comfyui",
		ReservationID:  "pg-resv-1",
		OperationID:    "pg-op-1",
		ScopeKind:      PinScopeTaskExecution,
		OwnerID:        "pg-task-1",
		OrganizationID: "org-pg",
		ProjectID:      "proj-pg",
		RequestSHA256:  strings.Repeat("9", 64),
		SHA256List:     []string{validSHA},
	}); err != nil {
		t.Fatalf("PGStore ReservePins: %v", err)
	}
	if _, err := svc.CommitPins(ctx, CommitPinsRequest{HolderAppID: "comfyui", ReservationID: "pg-resv-1"}); err != nil {
		t.Fatalf("PGStore CommitPins: %v", err)
	}
	if _, evErr := svc.Evict(ctx, EvictRequest{SHA256: validSHA}); evErr == nil {
		t.Fatalf("expected PGStore Evict blocked by active pin")
	}

	now = now.Add(2 * time.Minute)
	revoker.results["pg-resv-1"] = AppAdmissionVerification{
		ReservationID: "pg-resv-1",
		State:         AdmissionStateReleased,
		EntityID:      "pg-task-1",
	}
	_, relCount, err := svc.ReconcileExpiredPins(ctx)
	if err != nil || relCount != 1 {
		t.Fatalf("expected PGStore ReconcileExpiredPins relCount=1, got %d err=%v", relCount, err)
	}

	evRes, err := svc.Evict(ctx, EvictRequest{SHA256: validSHA})
	if err != nil || evRes.Status != StatusNotInstalled || evRes.Model == nil || evRes.Model.DownloadFence != 1 {
		t.Fatalf("expected PGStore Evict not_installed with download_fence=1 preserved, got %+v err=%v", evRes, err)
	}

	// 4. PGStore R2-M01: Expired idempotency claim_id rotation blocks delayed Complete/Abort from stale caller!
	reqHash := strings.Repeat("4", 64)
	baseIdem := StoredIdempotency{
		OrganizationID: "org-pg",
		ProjectID:      "proj-pg",
		PrincipalID:    "user-pg",
		TargetAlias:    "slot_claim_cas",
		IdempotencyKey: "pg-claim-cas-key",
		RequestSHA256:  reqHash,
		ModelID:        entryA.ModelID,
	}
	prevClaim1, claimA, err := pgStore.BeginEnsureIdempotency(ctx, baseIdem, now)
	if err != nil || prevClaim1 != nil || claimA == "" {
		t.Fatalf("expected fresh PG claimA, got prev=%+v claimA=%q err=%v", prevClaim1, claimA, err)
	}
	now = now.Add(6 * time.Minute) // expire claimA's 5m in_progress lease
	prevClaim2, claimB, err := pgStore.BeginEnsureIdempotency(ctx, baseIdem, now)
	if err != nil || prevClaim2 != nil || claimB == "" || claimB == claimA {
		t.Fatalf("expected rotated PG claimB != claimA, got prev=%+v claimB=%q err=%v", prevClaim2, claimB, err)
	}
	// Delayed abort from stale claimA MUST NOT delete claimB's active reservation!
	if err := pgStore.AbortEnsureIdempotency(ctx, baseIdem.OrganizationID, baseIdem.ProjectID, baseIdem.PrincipalID, baseIdem.TargetAlias, baseIdem.IdempotencyKey, claimA, reqHash); err != nil {
		t.Fatalf("stale PG AbortEnsureIdempotency unexpected err: %v", err)
	}
	if _, _, cErr := pgStore.BeginEnsureIdempotency(ctx, baseIdem, now.Add(1*time.Second)); cErr == nil {
		t.Fatalf("expected claimB still in_progress after stale claimA abort")
	}
	if err := pgStore.CompleteEnsureIdempotency(ctx, StoredIdempotency{
		OrganizationID: baseIdem.OrganizationID,
		ProjectID:      baseIdem.ProjectID,
		PrincipalID:    baseIdem.PrincipalID,
		TargetAlias:    baseIdem.TargetAlias,
		IdempotencyKey: baseIdem.IdempotencyKey,
		ClaimID:        claimB,
		RequestSHA256:  reqHash,
		ModelID:        entryA.ModelID,
		SHA256:         validSHA,
		DownloadFence:  2,
		ResponseStatus: 200,
		ResponseJSON:   []byte(`{"deduplicated":false}`),
		ExpiresAt:      now.Add(24 * time.Hour),
	}); err != nil {
		t.Fatalf("claimB CompleteEnsureIdempotency failed: %v", err)
	}
	// Delayed complete from stale claimA MUST fail with 409 IDEMPOTENCY_CLAIM_SUPERSEDED!
	staleCompErr := pgStore.CompleteEnsureIdempotency(ctx, StoredIdempotency{
		OrganizationID: baseIdem.OrganizationID,
		ProjectID:      baseIdem.ProjectID,
		PrincipalID:    baseIdem.PrincipalID,
		TargetAlias:    baseIdem.TargetAlias,
		IdempotencyKey: baseIdem.IdempotencyKey,
		ClaimID:        claimA,
		RequestSHA256:  reqHash,
		ModelID:        entryA.ModelID,
		SHA256:         validSHA,
		DownloadFence:  1,
		ResponseStatus: 500,
		ResponseJSON:   []byte(`{"code":"STALE"}`),
		ExpiresAt:      now.Add(24 * time.Hour),
	})
	if mErr, ok := AsError(staleCompErr); !ok || mErr.Status != 409 || mErr.Code != "IDEMPOTENCY_CLAIM_SUPERSEDED" {
		t.Fatalf("expected stale PG CompleteEnsureIdempotency 409 IDEMPOTENCY_CLAIM_SUPERSEDED, got %v", staleCompErr)
	}

	// 5. PGStore R2-M03: >50 running active pins on PostgreSQL never starve terminal pins behind them!
	if _, err := svc.Ensure(ctx, EnsureRequest{
		OrganizationID: "org-pg",
		ProjectID:      "proj-pg",
		PrincipalID:    "user-pg",
		TargetAlias:    "ckpt_reseed",
		IdempotencyKey: "pg-idem-reseed-m03",
		ModelID:        entryA.ModelID,
	}); err != nil {
		t.Fatal(err)
	}
	for i := 1; i <= 58; i++ {
		rID := fmt.Sprintf("pg-fair-%03d", i)
		if _, err := svc.ReservePins(ctx, ReservePinsRequest{
			HolderAppID:    "comfyui",
			ReservationID:  rID,
			OperationID:    "op-" + rID,
			ScopeKind:      PinScopeTaskExecution,
			OwnerID:        fmt.Sprintf("pg-task-%03d", i),
			OrganizationID: "org-pg",
			ProjectID:      "proj-pg",
			RequestSHA256:  strings.Repeat("5", 64),
			SHA256List:     []string{validSHA},
		}); err != nil {
			t.Fatalf("ReservePins(%s): %v", rID, err)
		}
		if _, err := svc.CommitPins(ctx, CommitPinsRequest{HolderAppID: "comfyui", ReservationID: rID}); err != nil {
			t.Fatalf("CommitPins(%s): %v", rID, err)
		}
		state := AdmissionStateCommitted
		if i > 52 {
			state = AdmissionStateReleased
		}
		revoker.results[rID] = AppAdmissionVerification{
			ReservationID: rID,
			State:         state,
			EntityID:      fmt.Sprintf("pg-task-%03d", i),
		}
	}
	now = now.Add(2 * time.Minute)
	_, fairRel, err := svc.ReconcileExpiredPins(ctx)
	if err != nil || fairRel != 6 {
		t.Fatalf("expected PGStore fair ReconcileExpiredPins to release all 6 terminal pins past LIMIT 50, got %d err=%v", fairRel, err)
	}
	stFair, err := svc.GetModelStatus(ctx, entryA.ModelID)
	if err != nil || stFair.ActivePins != 52 {
		t.Fatalf("expected 52 active running pins remaining on PGStore, got %+v err=%v", stFair, err)
	}
}

// TestHelperHoldStagingPartOpen is invoked as a real child OS process by
// TestSubprocessPausedWriterTakeoverPreservesPublishedCAS (R2-H01).
func TestHelperHoldStagingPartOpen(t *testing.T) {
	partPath := os.Getenv("VF_TEST_HOLD_PART_PATH")
	if partPath == "" {
		return
	}
	f, err := os.OpenFile(partPath, os.O_RDWR, 0)
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "open failed: %v\n", err)
		os.Exit(2)
	}
	_, _ = fmt.Fprintln(os.Stdout, "READY")
	_ = os.Stdout.Sync()

	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "WRITE_JUNK" {
			junk := bytes.Repeat([]byte{0xde, 0xad, 0xbe, 0xef}, 256)
			if _, sErr := f.Seek(0, io.SeekStart); sErr != nil {
				_, _ = fmt.Fprintf(os.Stderr, "seek failed: %v\n", sErr)
				os.Exit(3)
			}
			if _, wErr := f.Write(junk); wErr != nil {
				_, _ = fmt.Fprintf(os.Stderr, "write failed: %v\n", wErr)
				os.Exit(4)
			}
			_ = f.Sync()
			_ = f.Close()
			_, _ = fmt.Fprintln(os.Stdout, "WROTE")
			return
		}
	}
	_ = f.Close()
}

func TestSubprocessPausedWriterTakeoverPreservesPublishedCAS(t *testing.T) {
	validBytes, validSHA := buildValidSafetensors(t, 512, nil)
	half := len(validBytes) / 2
	const giB = int64(1024 * 1024 * 1024)

	svc, store, _ := newTestService(t, 600*giB, map[string][]byte{
		"/takeover.safetensors": validBytes,
	})
	now := time.Date(2026, 11, 15, 10, 0, 0, 0, time.UTC)
	svc.Now = func() time.Time { return now }
	ctx := context.Background()

	entry := CatalogEntry{
		ModelID:          "takeover-writer-ckpt",
		CatalogCode:      "MDL-CKPT-TKOV-001",
		DisplayName:      "Takeover Writer Checkpoint",
		Category:         "CKPT",
		Filename:         "takeover_writer.safetensors",
		SHA256:           validSHA,
		SizeBytes:        int64(len(validBytes)),
		VRAMFootprintMiB: 2048,
		License:          "apache-2.0",
		SourceURLs:       []string{"https://huggingface.co/takeover.safetensors"},
	}
	if err := svc.RegisterCatalogEntries(ctx, []CatalogEntry{entry}); err != nil {
		t.Fatal(err)
	}
	if err := EnsureLayout(svc.RootDir); err != nil {
		t.Fatal(err)
	}

	// 1. Materialize Fence 1 partial .part file and keep an actual child OS process holding an O_RDWR FD open on it!
	part1Path := PartFilePath(svc.RootDir, validSHA, 1)
	if err := os.WriteFile(part1Path, validBytes[:half], 0o600); err != nil {
		t.Fatal(err)
	}
	part1Ino, err := inspectFileInode(part1Path)
	if err != nil {
		t.Fatalf("inspectFileInode(part1): %v", err)
	}

	cmd := exec.Command(os.Args[0], "-test.run=^TestHelperHoldStagingPartOpen$")
	cmd.Env = append(os.Environ(), "VF_TEST_HOLD_PART_PATH="+part1Path)
	childIn, err := cmd.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	childOut, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	var childErr bytes.Buffer
	cmd.Stderr = &childErr
	if err := cmd.Start(); err != nil {
		t.Fatalf("failed starting helper process: %v", err)
	}
	t.Cleanup(func() {
		_ = childIn.Close()
		if cmd.Process != nil {
			_ = cmd.Process.Kill()
			_ = cmd.Wait()
		}
	})

	outReader := bufio.NewReader(childOut)
	readyLine, err := outReader.ReadString('\n')
	if err != nil || strings.TrimSpace(readyLine) != "READY" {
		t.Fatalf("helper failed to report READY: line=%q err=%v stderr=%s", readyLine, err, childErr.String())
	}

	// Verify OS-level TryExclusiveHandoffPart refuses zero-copy rename even if allowInodeHandoff=true while child holds O_RDWR!
	resumedNoCopy, _ := PrepareResumePart(svc.RootDir, validSHA, 1, 99, int64(len(validBytes)), true, func(int64) bool { return false })
	if resumedNoCopy != 0 {
		t.Fatalf("expected PrepareResumePart with copyQuota=false to refuse zero-copy handoff while writer FD is open, got %d", resumedNoCopy)
	}

	// 2. Seed Fence 1 as an expired in-flight download lease in Store, then run Fence 2 takeover via svc.Ensure!
	expiredAt := now.Add(-1 * time.Minute)
	store.mu.Lock()
	store.blobsBySHA[validSHA] = &memBlob{
		BlobState: BlobState{
			SHA256:          validSHA,
			SizeBytes:       int64(len(validBytes)),
			PartBytesOnDisk: int64(half),
			BytesDownloaded: int64(half),
			ETag:            `"etag-v1"`,
			Status:          StatusDownloading,
			DownloadFence:   1,
		},
		LeaseOwner:     "stale-worker-1",
		LeaseExpiresAt: &expiredAt,
	}
	store.mu.Unlock()

	ensRes, err := svc.Ensure(ctx, EnsureRequest{
		OrganizationID: "org-1",
		ProjectID:      "proj-1",
		PrincipalID:    "user-1",
		IdempotencyKey: "idem-takeover-open-fd-1",
		ModelID:        entry.ModelID,
	})
	if err != nil || ensRes.Model.Status != StatusReady || ensRes.Model.DownloadFence != 2 {
		t.Fatalf("expected Fence 2 takeover StatusReady, got %+v err=%v", ensRes, err)
	}

	casPath := CASFilePath(svc.RootDir, validSHA)
	relView, _ := RelativeViewPath(entry.Category, validSHA, entry.Filename)
	viewPath, _ := ViewFilePath(svc.RootDir, relView)
	casInoBefore, err := inspectFileInode(casPath)
	if err != nil {
		t.Fatalf("inspectFileInode(casPath): %v", err)
	}
	if casInoBefore == part1Ino {
		t.Fatalf("published CAS inode (%d) MUST NOT equal stale writer's open .part inode (%d)!", casInoBefore, part1Ino)
	}

	// 3. Resume the paused Fence 1 child process so it writes 0xdeadbeef garbage + fsync() to its still-open writable FD!
	if _, err := io.WriteString(childIn, "WRITE_JUNK\n"); err != nil {
		t.Fatalf("failed signaling child to write junk: %v", err)
	}
	_ = childIn.Close()
	wroteLine, err := outReader.ReadString('\n')
	if err != nil || strings.TrimSpace(wroteLine) != "WROTE" {
		t.Fatalf("expected child WROTE confirmation, got line=%q err=%v stderr=%s", wroteLine, err, childErr.String())
	}
	if err := cmd.Wait(); err != nil {
		t.Fatalf("helper wait failed: %v stderr=%s", err, childErr.String())
	}

	// 4. Published CAS and hardlink view MUST remain 100% intact and match authoritative SHA-256!
	gotSHA, gotSize, err := ComputeFileSHA256(casPath)
	if err != nil || gotSHA != validSHA || gotSize != int64(len(validBytes)) {
		t.Fatalf("published CAS corrupted by late stale writer FD! gotSHA=%s want=%s size=%d err=%v", gotSHA, validSHA, gotSize, err)
	}
	viewBytes, err := os.ReadFile(viewPath)
	if err != nil || !bytes.Equal(viewBytes, validBytes) {
		t.Fatalf("published view bytes corrupted by late stale writer FD! err=%v", err)
	}
	casInoAfter, err := inspectFileInode(casPath)
	if err != nil || casInoAfter != casInoBefore {
		t.Fatalf("published CAS inode changed after late write: before=%d after=%d err=%v", casInoBefore, casInoAfter, err)
	}

	// 5. Verify OnProgress fence check aborts superseded stream immediately and cleans up its .part file!
	otherBytes, otherSHA := buildValidSafetensors(t, 256, bytes.Repeat([]byte{0x77}, 96*1024))
	entryAbort := CatalogEntry{
		ModelID:          "superseded-stream-lora",
		CatalogCode:      "MDL-LORA-SUPR-001",
		DisplayName:      "Superseded Stream LoRA",
		Category:         "LORA",
		Filename:         "superseded_stream.safetensors",
		SHA256:           otherSHA,
		SizeBytes:        int64(len(otherBytes)),
		VRAMFootprintMiB: 256,
		License:          "mit",
		SourceURLs:       []string{"https://huggingface.co/superseded.safetensors"},
	}
	svcAbort, storeAbort, _ := newTestService(t, 600*giB, map[string][]byte{
		"/superseded.safetensors": otherBytes,
	})
	_ = svcAbort.RegisterCatalogEntries(ctx, []CatalogEntry{entryAbort})
	admit1, err := storeAbort.AdmitOrJoinDownload(ctx, AdmitDownloadInput{
		Entry:                 entryAbort,
		FilesystemTotalBytes:  2000 * giB,
		FilesystemFreeBytes:   600 * giB,
		ReservedHeadroomBytes: DefaultReservedHeadroomBytes,
		MaxAutoPullBytes:      DefaultMaxAutoPullModelBytes,
		LeaseOwner:            "worker-1",
		LeaseTTL:              5 * time.Minute,
		Now:                   now,
	})
	if err != nil || admit1.Fence != 1 {
		t.Fatalf("AdmitOrJoinDownload fence 1 failed: %+v err=%v", admit1, err)
	}
	svcAbort.Hook = StageHookFunc(func(_ context.Context, stage string, meta map[string]string) {
		if stage == "before_download_stream" && meta["fence"] == "1" {
			// Supersede Fence 1 -> Fence 2 right before stream progresses!
			_, _ = storeAbort.AdmitOrJoinDownload(ctx, AdmitDownloadInput{
				Entry:                 entryAbort,
				FilesystemTotalBytes:  2000 * giB,
				FilesystemFreeBytes:   600 * giB,
				ReservedHeadroomBytes: DefaultReservedHeadroomBytes,
				MaxAutoPullBytes:      DefaultMaxAutoPullModelBytes,
				LeaseOwner:            "worker-2",
				LeaseTTL:              5 * time.Minute,
				Now:                   now.Add(1 * time.Second),
				AllowTakeoverActive:   true,
			})
		}
	})
	svcAbort.RunDownloadFence(ctx, entryAbort, 1, entryAbort.SourceURLs[0], "")
	if _, stErr := os.Lstat(PartFilePath(svcAbort.RootDir, otherSHA, 1)); !errors.Is(stErr, os.ErrNotExist) {
		t.Fatalf("expected superseded Fence 1 .part file cleaned up immediately on OnProgress abort, got %v", stErr)
	}
	stAbort, _ := svcAbort.GetModelStatus(ctx, entryAbort.ModelID)
	if stAbort.DownloadFence != 2 || stAbort.Status == StatusFailed {
		t.Fatalf("superseded Fence 1 must NOT mark Fence 2 failed, got %+v", stAbort)
	}
}

func TestPostFinalStatPreSyscallSwapProtectedByQuarantineAndFDLink(t *testing.T) {
	validBytes, validSHA := buildValidSafetensors(t, 128, nil)
	const giB = int64(1024 * 1024 * 1024)
	svc, _, _ := newTestService(t, 600*giB, map[string][]byte{
		"/quarantine.safetensors": validBytes,
	})
	ctx := context.Background()
	entry := CatalogEntry{
		ModelID:          "quarantine-guard-ckpt",
		CatalogCode:      "MDL-CKPT-QUAR-001",
		DisplayName:      "Quarantine Guard Checkpoint",
		Category:         "CKPT",
		Filename:         "quarantine_guard.safetensors",
		SHA256:           validSHA,
		SizeBytes:        int64(len(validBytes)),
		VRAMFootprintMiB: 2048,
		License:          "apache-2.0",
		SourceURLs:       []string{"https://huggingface.co/quarantine.safetensors"},
	}
	if err := svc.RegisterCatalogEntries(ctx, []CatalogEntry{entry}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { testHookAfterFinalStatBeforeSyscall = nil })

	// 1. Provision valid model and record CAS inode.
	res, err := svc.Ensure(ctx, EnsureRequest{
		OrganizationID: "org-1",
		ProjectID:      "proj-1",
		PrincipalID:    "user-1",
		IdempotencyKey: "idem-quarantine-seed-1",
		ModelID:        entry.ModelID,
	})
	if err != nil || res.Model.Status != StatusReady {
		t.Fatalf("seed Ensure failed: %+v err=%v", res, err)
	}
	casPath := CASFilePath(svc.RootDir, validSHA)
	relView, _ := RelativeViewPath(entry.Category, validSHA, entry.Filename)
	origIno, err := inspectFileInode(casPath)
	if err != nil {
		t.Fatal(err)
	}

	// Inject post-final-stat-pre-syscall swap during EvictCASAndViews:
	// Right AFTER StatInDir("cas", validSHA) saw origIno, swap cas/<validSHA> with a new file!
	replacementPayload := []byte("REPLACEMENT_CAS_OBJECT_DO_NOT_DELETE")
	var swappedIno uint64
	testHookAfterFinalStatBeforeSyscall = func(op, relPath string) {
		if op == "quarantine_unlink" && relPath == "cas/"+validSHA {
			_ = os.Chmod(casPath, 0o600)
			_ = os.Remove(casPath)
			if wErr := os.WriteFile(casPath, replacementPayload, 0o444); wErr != nil {
				t.Errorf("swap write failed: %v", wErr)
				return
			}
			swappedIno, _ = inspectFileInode(casPath)
		}
	}
	if err := EvictCASAndViews(svc.RootDir, validSHA, []string{relView}, &origIno); err != nil {
		t.Fatalf("EvictCASAndViews returned unexpected error on post-stat swap: %v", err)
	}
	testHookAfterFinalStatBeforeSyscall = nil

	// Replacement file MUST have been atomically restored from .quarantine without being deleted!
	restoredBytes, err := os.ReadFile(casPath)
	if err != nil || !bytes.Equal(restoredBytes, replacementPayload) {
		t.Fatalf("replacement CAS file was deleted or altered despite inode mismatch! got=%q err=%v", string(restoredBytes), err)
	}
	restoredIno, err := inspectFileInode(casPath)
	if err != nil || swappedIno == 0 || restoredIno != swappedIno {
		t.Fatalf("expected restored inode %d, got %d (err=%v)", swappedIno, restoredIno, err)
	}
	qEntries, _ := os.ReadDir(QuarantineDir(svc.RootDir))
	if len(qEntries) != 0 {
		t.Fatalf("expected .quarantine directory empty after restore, got %d entries", len(qEntries))
	}

	// Clean up replacement file so we can test publish-time staging swap protection!
	_ = os.Chmod(casPath, 0o600)
	_ = os.Remove(casPath)

	// 2. Inject post-final-stat-pre-syscall swap on .staging/<sha>.2.part during PublishCASAndViews cleanup:
	// LinkOpenFileToDir links from the verified open FD (never the swapped path), and quarantineAndConfirmUnlink
	// detects the staging swap, restores the bystander file intact, removes the newly linked CAS, and fails closed!
	bytes2, sha2 := buildValidSafetensors(t, 160, nil)
	root2 := t.TempDir()
	if err := EnsureLayout(root2); err != nil {
		t.Fatal(err)
	}
	part2 := PartFilePath(root2, sha2, 1)
	if err := os.WriteFile(part2, bytes2, 0o600); err != nil {
		t.Fatal(err)
	}
	verifiedPart, err := OpenAndVerifyStagingPart(root2, sha2, 1, int64(len(bytes2)))
	if err != nil {
		t.Fatalf("OpenAndVerifyStagingPart: %v", err)
	}
	bystanderBytes := []byte("BYSTANDER_IN_STAGING_DO_NOT_UNLINK")
	testHookAfterFinalStatBeforeSyscall = func(op, relPath string) {
		if op == "quarantine_unlink" && relPath == PartRelPath(sha2, 1) {
			_ = os.Chmod(part2, 0o600)
			_ = os.Remove(part2)
			_ = os.WriteFile(part2, bystanderBytes, 0o600)
		}
	}
	relView2, _ := RelativeViewPath("CKPT", sha2, "guard2.safetensors")
	_, _, pubErr := PublishCASAndViews(root2, sha2, []string{relView2}, int64(len(bytes2)), 1, verifiedPart, false)
	testHookAfterFinalStatBeforeSyscall = nil
	if pubErr == nil {
		t.Fatalf("expected PublishCASAndViews to fail closed when staging entry was swapped concurrently")
	}
	if _, cErr := os.Lstat(CASFilePath(root2, sha2)); !errors.Is(cErr, os.ErrNotExist) {
		t.Fatalf("expected CAS rolled back cleanly on staging tamper, got %v", cErr)
	}
	gotBystander, bErr := os.ReadFile(part2)
	if bErr != nil || !bytes.Equal(gotBystander, bystanderBytes) {
		t.Fatalf("expected swapped bystander file restored intact in .staging, got=%q err=%v", string(gotBystander), bErr)
	}
}

func TestExpiredIdempotencyClaimTokenCASBlocksStaleCompleteAndAbort(t *testing.T) {
	store := NewMemoryStore()
	ctx := context.Background()
	t0 := time.Date(2026, 11, 15, 11, 0, 0, 0, time.UTC)
	reqSHA := strings.Repeat("a", 64)
	modelSHA := strings.Repeat("b", 64)

	req := StoredIdempotency{
		OrganizationID: "org-1",
		ProjectID:      "proj-1",
		PrincipalID:    "user-1",
		TargetAlias:    "default",
		IdempotencyKey: "claim-cas-key-1",
		RequestSHA256:  reqSHA,
		ModelID:        "sdxl-base-1_0",
	}

	// 1. Caller A claims idempotency slot at t0.
	prevA, claimA, err := store.BeginEnsureIdempotency(ctx, req, t0)
	if err != nil || prevA != nil || claimA == "" {
		t.Fatalf("Caller A claim failed: prev=%+v claimA=%q err=%v", prevA, claimA, err)
	}

	// 2. Advance past 5-minute in_progress lease; Caller B reclaims slot with a brand-new claimB!
	t1 := t0.Add(6 * time.Minute)
	prevB, claimB, err := store.BeginEnsureIdempotency(ctx, req, t1)
	if err != nil || prevB != nil || claimB == "" || claimB == claimA {
		t.Fatalf("Caller B reclaim failed: prev=%+v claimB=%q claimA=%q err=%v", prevB, claimB, claimA, err)
	}

	// 3. Delayed Abort from stale Caller A MUST NOT delete Caller B's active reservation!
	if err := store.AbortEnsureIdempotency(ctx, req.OrganizationID, req.ProjectID, req.PrincipalID, req.TargetAlias, req.IdempotencyKey, claimA, reqSHA); err != nil {
		t.Fatalf("stale Caller A abort returned err: %v", err)
	}
	_, _, cErr := store.BeginEnsureIdempotency(ctx, req, t1.Add(1*time.Second))
	if mErr, ok := AsError(cErr); !ok || mErr.Status != 409 || mErr.Code != "OPERATION_IN_PROGRESS" {
		t.Fatalf("expected Caller B reservation still OPERATION_IN_PROGRESS after stale abort, got %v", cErr)
	}

	// 4. Stale Caller A attempting CompleteEnsureIdempotency while Caller B is in_progress MUST fail 409!
	staleComplete1 := store.CompleteEnsureIdempotency(ctx, StoredIdempotency{
		OrganizationID: req.OrganizationID,
		ProjectID:      req.ProjectID,
		PrincipalID:    req.PrincipalID,
		TargetAlias:    req.TargetAlias,
		IdempotencyKey: req.IdempotencyKey,
		ClaimID:        claimA,
		RequestSHA256:  reqSHA,
		ModelID:        req.ModelID,
		SHA256:         modelSHA,
		DownloadFence:  1,
		ResponseStatus: 500,
		ResponseJSON:   []byte(`{"code":"STALE_A"}`),
		ExpiresAt:      t1.Add(24 * time.Hour),
	})
	if mErr, ok := AsError(staleComplete1); !ok || mErr.Status != 409 || mErr.Code != "IDEMPOTENCY_CLAIM_SUPERSEDED" {
		t.Fatalf("expected stale Caller A complete rejected 409 IDEMPOTENCY_CLAIM_SUPERSEDED, got %v", staleComplete1)
	}

	// 5. Caller B completes with valid claimB -> succeeds; subsequent stale Caller A complete ALSO rejected!
	if err := store.CompleteEnsureIdempotency(ctx, StoredIdempotency{
		OrganizationID: req.OrganizationID,
		ProjectID:      req.ProjectID,
		PrincipalID:    req.PrincipalID,
		TargetAlias:    req.TargetAlias,
		IdempotencyKey: req.IdempotencyKey,
		ClaimID:        claimB,
		RequestSHA256:  reqSHA,
		ModelID:        req.ModelID,
		SHA256:         modelSHA,
		DownloadFence:  2,
		ResponseStatus: 200,
		ResponseJSON:   []byte(`{"code":"FRESH_B"}`),
		ExpiresAt:      t1.Add(24 * time.Hour),
	}); err != nil {
		t.Fatalf("Caller B complete failed: %v", err)
	}
	staleComplete2 := store.CompleteEnsureIdempotency(ctx, StoredIdempotency{
		OrganizationID: req.OrganizationID,
		ProjectID:      req.ProjectID,
		PrincipalID:    req.PrincipalID,
		TargetAlias:    req.TargetAlias,
		IdempotencyKey: req.IdempotencyKey,
		ClaimID:        claimA,
		RequestSHA256:  reqSHA,
		ModelID:        req.ModelID,
		SHA256:         modelSHA,
		DownloadFence:  1,
		ResponseStatus: 500,
		ResponseJSON:   []byte(`{"code":"STALE_A"}`),
		ExpiresAt:      t1.Add(24 * time.Hour),
	})
	if mErr, ok := AsError(staleComplete2); !ok || mErr.Status != 409 || mErr.Code != "IDEMPOTENCY_CLAIM_SUPERSEDED" {
		t.Fatalf("expected post-completion stale Caller A complete rejected 409, got %v", staleComplete2)
	}
	saved, _, err := store.BeginEnsureIdempotency(ctx, req, t1.Add(2*time.Second))
	if err != nil || saved == nil || saved.ResponseStatus != 200 || !strings.Contains(string(saved.ResponseJSON), "FRESH_B") {
		t.Fatalf("expected Caller B response preserved intact, got %+v err=%v", saved, err)
	}
}

func TestSameCategoryMultiViewFsyncOrderAndRetryAfterFsyncFailure(t *testing.T) {
	validBytes, validSHA := buildValidSafetensors(t, 128, nil)
	rootDir := t.TempDir()
	if err := EnsureLayout(rootDir); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { testHookFsyncDir = nil })

	relView1, _ := RelativeViewPath("CKPT", validSHA, "dual_ckpt_one.safetensors")
	relView2, _ := RelativeViewPath("CKPT", validSHA, "dual_ckpt_two.safetensors")
	viewPath1, _ := ViewFilePath(rootDir, relView1)
	viewPath2, _ := ViewFilePath(rootDir, relView2)

	part1 := PartFilePath(rootDir, validSHA, 1)
	if err := os.WriteFile(part1, validBytes, 0o600); err != nil {
		t.Fatal(err)
	}
	verified1, err := OpenAndVerifyStagingPart(rootDir, validSHA, 1, int64(len(validBytes)))
	if err != nil {
		t.Fatal(err)
	}

	// Attempt 1: Both views are in views/checkpoints. Verify that when fsync("views/checkpoints") runs,
	// BOTH view files ALREADY exist on disk, AND inject a transient fsync failure on views/checkpoints!
	var attempt1Synced []string
	bothPresentAtSubdirSync := false
	testHookFsyncDir = func(relDir string) error {
		attempt1Synced = append(attempt1Synced, relDir)
		if relDir == "views/checkpoints" {
			_, e1 := os.Lstat(viewPath1)
			_, e2 := os.Lstat(viewPath2)
			bothPresentAtSubdirSync = (e1 == nil && e2 == nil)
			return errors.New("simulated transient fsync failure on views/checkpoints")
		}
		return nil
	}
	_, _, err1 := PublishCASAndViews(rootDir, validSHA, []string{relView1, relView2}, int64(len(validBytes)), 1, verified1, false)
	if err1 == nil {
		t.Fatalf("expected Attempt 1 to fail closed when views/checkpoints fsync failed")
	}
	if !bothPresentAtSubdirSync {
		t.Fatalf("expected BOTH views in views/checkpoints renamed BEFORE directory fsync ran! synced=%v", attempt1Synced)
	}

	// Attempt 2 (retry when cas/<sha> and views/checkpoints/* ALREADY exist -> EEXIST path):
	// MUST still execute fsync("cas") AND fsync("views/checkpoints") before returning success!
	var attempt2Synced []string
	testHookFsyncDir = func(relDir string) error {
		attempt2Synced = append(attempt2Synced, relDir)
		return nil
	}
	casIno, publishedViews, err2 := PublishCASAndViews(rootDir, validSHA, []string{relView1, relView2}, int64(len(validBytes)), 2, nil, false)
	testHookFsyncDir = nil
	if err2 != nil || casIno == 0 || len(publishedViews) != 2 {
		t.Fatalf("expected Attempt 2 EEXIST recovery to succeed, got ino=%d views=%v err=%v", casIno, publishedViews, err2)
	}
	if !containsString(attempt2Synced, "cas") || !containsString(attempt2Synced, "views/checkpoints") {
		t.Fatalf("expected Attempt 2 EEXIST retry to fsync both cas and views/checkpoints, got %v", attempt2Synced)
	}
}

func TestFairPinReconciliationOver50RunningAndTimeoutGroups(t *testing.T) {
	validBytes, validSHA := buildValidSafetensors(t, 128, nil)
	const giB = int64(1024 * 1024 * 1024)
	svc, _, _ := newTestService(t, 600*giB, map[string][]byte{
		"/fair.safetensors": validBytes,
	})
	now := time.Date(2026, 11, 15, 12, 0, 0, 0, time.UTC)
	svc.Now = func() time.Time { return now }
	svc.ActiveTaskReconcileAge = 30 * time.Second
	revoker := &fakeRevoker{results: map[string]AppAdmissionVerification{}}
	svc.Revoker = revoker
	ctx := context.Background()

	entry := CatalogEntry{
		ModelID:          "fair-reconcile-ckpt",
		CatalogCode:      "MDL-CKPT-FAIR-001",
		DisplayName:      "Fair Reconcile Checkpoint",
		Category:         "CKPT",
		Filename:         "fair_reconcile.safetensors",
		SHA256:           validSHA,
		SizeBytes:        int64(len(validBytes)),
		VRAMFootprintMiB: 2048,
		License:          "apache-2.0",
		SourceURLs:       []string{"https://huggingface.co/fair.safetensors"},
	}
	_ = svc.RegisterCatalogEntries(ctx, []CatalogEntry{entry})
	if _, err := svc.Ensure(ctx, EnsureRequest{
		OrganizationID: "org-1",
		ProjectID:      "proj-1",
		PrincipalID:    "user-1",
		IdempotencyKey: "idem-fair-seed-1",
		ModelID:        entry.ModelID,
	}); err != nil {
		t.Fatal(err)
	}

	// Create 65 active task_execution pins:
	//   001..030 -> committed (still running)
	//   031..055 -> missing in fakeRevoker.results (simulates RPC error!)
	//   056..065 -> released (terminal tasks sitting past LIMIT 50!)
	for i := 1; i <= 65; i++ {
		rID := fmt.Sprintf("resv-active-%03d", i)
		if _, err := svc.ReservePins(ctx, ReservePinsRequest{
			HolderAppID:    "comfyui",
			ReservationID:  rID,
			OperationID:    "op-" + rID,
			ScopeKind:      PinScopeTaskExecution,
			OwnerID:        fmt.Sprintf("task-%03d", i),
			OrganizationID: "org-1",
			ProjectID:      "proj-1",
			RequestSHA256:  strings.Repeat("6", 64),
			SHA256List:     []string{validSHA},
		}); err != nil {
			t.Fatal(err)
		}
		if _, err := svc.CommitPins(ctx, CommitPinsRequest{HolderAppID: "comfyui", ReservationID: rID}); err != nil {
			t.Fatal(err)
		}
		if i <= 30 {
			revoker.results[rID] = AppAdmissionVerification{
				ReservationID: rID,
				State:         AdmissionStateCommitted,
				EntityID:      fmt.Sprintf("task-%03d", i),
			}
		} else if i >= 56 {
			revoker.results[rID] = AppAdmissionVerification{
				ReservationID: rID,
				State:         AdmissionStateReleased,
				EntityID:      fmt.Sprintf("task-%03d", i),
			}
		}
	}

	// Create 60 expired reserved pins:
	//   001..052 -> missing in fakeRevoker.results (RPC timeout -> MUST NOT release!)
	//   053..060 -> revoked tombstone sitting past LIMIT 50!
	for i := 1; i <= 60; i++ {
		rID := fmt.Sprintf("resv-expired-%03d", i)
		if _, err := svc.ReservePins(ctx, ReservePinsRequest{
			HolderAppID:    "comfyui",
			ReservationID:  rID,
			OperationID:    "op-" + rID,
			ScopeKind:      PinScopeTaskExecution,
			OwnerID:        "pending",
			OrganizationID: "org-1",
			ProjectID:      "proj-1",
			RequestSHA256:  strings.Repeat("8", 64),
			SHA256List:     []string{validSHA},
			TTLSeconds:     30,
		}); err != nil {
			t.Fatal(err)
		}
		if i >= 53 {
			revoker.results[rID] = AppAdmissionVerification{
				ReservationID: rID,
				State:         AdmissionStateRevoked,
			}
		}
	}

	now = now.Add(2 * time.Minute)
	promoted, released, err := svc.ReconcileExpiredPins(ctx)
	if err != nil || promoted != 0 || released != 18 {
		t.Fatalf("expected promoted=0 released=18 (10 active terminal + 8 expired revoked past LIMIT 50), got promoted=%d released=%d err=%v", promoted, released, err)
	}
	st, err := svc.GetModelStatus(ctx, entry.ModelID)
	if err != nil || st.ActivePins != 55 || st.ReservedPins != 52 {
		t.Fatalf("expected ActivePins=55 ReservedPins=52 preserved without starvation or unsafe drop, got %+v err=%v", st, err)
	}
}

// TestNonCooperativeWriteFDAndLeaseProbeFailureRefusesSharedInodeAndPublish covers Round-3 R3-H01:
//  1. classifyLinuxWriterProbe strictly separates ProvenSafe (nil/nil) from ActiveWriter
//     (flock error or EAGAIN/EACCES/EBUSY) and Unprovable (EPERM/EINVAL/ENOSYS) — advisory
//     flock success NEVER substitutes for a failed F_SETLEASE probe.
//  2. When a child OS process holds a non-cooperative O_RDWR FD (without flock) AND lease
//     probing returns WriterProbeUnprovable (simulating EPERM/EINVAL/ENOSYS), HandOffResumePart
//     refuses shared-inode handoff, OpenAndVerifyStagingPart / PublishCASAndViews refuse publish
//     fail-closed, and svc.Ensure clones the resumable prefix into an independent inode so a
//     late write on the child FD cannot corrupt published CAS bytes.
func TestNonCooperativeWriteFDAndLeaseProbeFailureRefusesSharedInodeAndPublish(t *testing.T) {
	if got := classifyLinuxWriterProbe(nil, nil); got != WriterProbeProvenSafe {
		t.Fatalf("expected WriterProbeProvenSafe on (nil, nil), got %v", got)
	}
	for _, errCase := range []struct {
		flockErr error
		leaseErr error
		want     WriterProbeState
	}{
		{syscall.EWOULDBLOCK, nil, WriterProbeActiveWriter},
		{nil, syscall.EAGAIN, WriterProbeActiveWriter},
		{nil, syscall.EACCES, WriterProbeActiveWriter},
		{nil, syscall.EBUSY, WriterProbeActiveWriter},
		{nil, syscall.EPERM, WriterProbeUnprovable},
		{nil, syscall.EINVAL, WriterProbeUnprovable},
		{nil, syscall.ENOSYS, WriterProbeUnprovable},
	} {
		if got := classifyLinuxWriterProbe(errCase.flockErr, errCase.leaseErr); got != errCase.want {
			t.Fatalf("classifyLinuxWriterProbe(%v, %v) = %v, want %v", errCase.flockErr, errCase.leaseErr, got, errCase.want)
		}
	}

	validBytes, validSHA := buildValidSafetensors(t, 512, nil)
	half := len(validBytes) / 2
	const giB = int64(1024 * 1024 * 1024)

	svc, store, _ := newTestService(t, 600*giB, map[string][]byte{
		"/unprovable.safetensors": validBytes,
	})
	now := time.Date(2026, 11, 15, 13, 0, 0, 0, time.UTC)
	svc.Now = func() time.Time { return now }
	ctx := context.Background()

	entry := CatalogEntry{
		ModelID:          "r3-h01-unprovable-ckpt",
		CatalogCode:      "MDL-CKPT-R3H1-001",
		DisplayName:      "R3-H01 Lease Probe Unprovable Checkpoint",
		Category:         "CKPT",
		Filename:         "r3_h01_unprovable.safetensors",
		SHA256:           validSHA,
		SizeBytes:        int64(len(validBytes)),
		VRAMFootprintMiB: 2048,
		License:          "apache-2.0",
		SourceURLs:       []string{"https://huggingface.co/unprovable.safetensors"},
	}
	if err := svc.RegisterCatalogEntries(ctx, []CatalogEntry{entry}); err != nil {
		t.Fatal(err)
	}
	if err := EnsureLayout(svc.RootDir); err != nil {
		t.Fatal(err)
	}

	// Materialize Fence 1 partial .part and open a non-cooperative O_RDWR FD in a real child OS process.
	part1Path := PartFilePath(svc.RootDir, validSHA, 1)
	if err := os.WriteFile(part1Path, validBytes[:half], 0o600); err != nil {
		t.Fatal(err)
	}
	part1Ino, err := inspectFileInode(part1Path)
	if err != nil {
		t.Fatalf("inspectFileInode(part1): %v", err)
	}

	cmd := exec.Command(os.Args[0], "-test.run=^TestHelperHoldStagingPartOpen$")
	cmd.Env = append(os.Environ(), "VF_TEST_HOLD_PART_PATH="+part1Path)
	stdinPipe, err := cmd.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	stdoutPipe, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	var stderrBuf bytes.Buffer
	cmd.Stderr = &stderrBuf
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = stdinPipe.Close()
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
	}()

	outReader := bufio.NewReader(stdoutPipe)
	readyLine, err := outReader.ReadString('\n')
	if err != nil || strings.TrimSpace(readyLine) != "READY" {
		t.Fatalf("child writer helper did not report READY: line=%q err=%v stderr=%s", readyLine, err, stderrBuf.String())
	}

	// Force WriterProbeUnprovable on Fence 1's staging part AND on Fence 99's verify/publish probe.
	unprovable := WriterProbeUnprovable
	activeState := WriterProbeActiveWriter
	defer func() { testHookWriterProbeOverride = nil }()
	testHookWriterProbeOverride = func(op, relPath string) *WriterProbeState {
		if relPath == PartRelPath(validSHA, 1) && op == "handoff_probe" {
			return &unprovable
		}
		if relPath == PartRelPath(validSHA, 99) {
			return &unprovable
		}
		if relPath == PartRelPath(validSHA, 98) {
			return &activeState
		}
		return nil
	}

	// 1. HandOffResumePart MUST refuse shared-inode handoff when probe is WriterProbeUnprovable!
	if n, handedOff := HandOffResumePart(svc.RootDir, validSHA, 1, 2, int64(len(validBytes))); handedOff || n != 0 {
		t.Fatalf("expected HandOffResumePart refused under WriterProbeUnprovable, got n=%d handedOff=%v", n, handedOff)
	}
	if _, stErr := os.Lstat(PartFilePath(svc.RootDir, validSHA, 2)); !errors.Is(stErr, os.ErrNotExist) {
		t.Fatalf("expected Fence 2 part NOT created on refused shared-inode handoff, got err=%v", stErr)
	}

	// 2. OpenAndVerifyStagingPart AND PublishCASAndViews(..., verified=nil, ...) MUST refuse publish fail-closed
	//    when writer probe returns WriterProbeUnprovable (Fence 99) or WriterProbeActiveWriter (Fence 98)!
	for _, fCase := range []struct {
		fence    int64
		wantCode string
	}{
		{99, "STORAGE_IO_ERROR"},
		{98, "OPERATION_IN_PROGRESS"},
	} {
		fPath := PartFilePath(svc.RootDir, validSHA, fCase.fence)
		if err := os.WriteFile(fPath, validBytes, 0o600); err != nil {
			t.Fatal(err)
		}
		if vp, vErr := OpenAndVerifyStagingPart(svc.RootDir, validSHA, fCase.fence, int64(len(validBytes))); vErr == nil {
			vp.Close()
			t.Fatalf("expected OpenAndVerifyStagingPart on fence %d to fail closed", fCase.fence)
		} else if mErr, ok := AsError(vErr); !ok || mErr.Code != fCase.wantCode {
			t.Fatalf("expected %s on fence %d verify, got %v", fCase.wantCode, fCase.fence, vErr)
		}
		relView, _ := RelativeViewPath(entry.Category, validSHA, entry.Filename)
		if _, _, pErr := PublishCASAndViews(svc.RootDir, validSHA, []string{relView}, int64(len(validBytes)), fCase.fence, nil, false); pErr == nil {
			t.Fatalf("expected PublishCASAndViews(verified=nil) on fence %d to fail closed", fCase.fence)
		} else if mErr, ok := AsError(pErr); !ok || mErr.Code != fCase.wantCode {
			t.Fatalf("expected %s on fence %d publish, got %v", fCase.wantCode, fCase.fence, pErr)
		}
		fID, err := statBeneathNoFollow(svc.RootDir, PartRelPath(validSHA, fCase.fence))
		if err != nil {
			t.Fatalf("expected rejected staging .part preserved on fence %d: %v", fCase.fence, err)
		}
		if tampered, cErr := CleanupFenceArtifactsWithIdentity(svc.RootDir, validSHA, fCase.fence, FenceArtifacts{PartID: &fID}); tampered || cErr != nil {
			t.Fatalf("cleanup fence %d failed: tampered=%v err=%v", fCase.fence, tampered, cErr)
		}
	}

	// 3. Seed Fence 1 in FAILED state (where allowInodeHandoff == true on Fence 2!) with resumable ETag:
	//    Ensure on Fence 2 hits WriterProbeUnprovable on Fence 1 -> MUST clone prefix to a brand-new inode!
	store.mu.Lock()
	store.blobsBySHA[validSHA] = &memBlob{
		BlobState: BlobState{
			SHA256:          validSHA,
			SizeBytes:       int64(len(validBytes)),
			PartBytesOnDisk: int64(half),
			BytesDownloaded: int64(half),
			ETag:            `"test-etag"`,
			Status:          StatusFailed,
			DownloadFence:   1,
			UpdatedAt:       now,
		},
	}
	store.mu.Unlock()

	now = now.Add(5 * time.Second)
	res2, err := svc.Ensure(ctx, EnsureRequest{
		OrganizationID: "org-1",
		ProjectID:      "proj-1",
		PrincipalID:    "user-1",
		IdempotencyKey: "idem-r3-h01-ensure",
		ModelID:        entry.ModelID,
	})
	if err != nil || res2.Model.Status != StatusReady {
		t.Fatalf("expected Fence 2 Ensure READY via independent-inode fallback, got %+v err=%v", res2, err)
	}

	// Have the non-cooperative child O_RDWR FD overwrite Fence 1's inode with 0xdeadbeef junk NOW!
	if _, err := io.WriteString(stdinPipe, "WRITE_JUNK\n"); err != nil {
		t.Fatalf("failed signaling child writer: %v", err)
	}
	wroteLine, err := outReader.ReadString('\n')
	if err != nil || strings.TrimSpace(wroteLine) != "WROTE" {
		t.Fatalf("child writer did not confirm WROTE: line=%q err=%v stderr=%s", wroteLine, err, stderrBuf.String())
	}
	_ = stdinPipe.Close()
	if err := cmd.Wait(); err != nil {
		t.Fatalf("child writer exited with error: %v stderr=%s", err, stderrBuf.String())
	}

	casPath := CASFilePath(svc.RootDir, validSHA)
	casIno, err := inspectFileInode(casPath)
	if err != nil {
		t.Fatalf("inspectFileInode(cas): %v", err)
	}
	if casIno == part1Ino {
		t.Fatalf("CRITICAL R3-H01 VIOLATION: published CAS shared inode %d with unprovable non-cooperative writer FD!", casIno)
	}
	gotSHA, gotSize, err := ComputeFileSHA256(casPath)
	if err != nil || gotSHA != validSHA || gotSize != int64(len(validBytes)) {
		t.Fatalf("published CAS corrupted after late write on old FD: sha=%s want=%s size=%d err=%v", gotSHA, validSHA, gotSize, err)
	}
}

// TestFullDownloadFenceFailureCleanupPreservesSwappedEntriesAndRejectsUnknownIdentity covers Round-3 R3-H02:
//  1. End-to-end svc.Ensure / runDownloadFenceWithPrev failure cleanup preserves concurrently swapped
//     entries on BOTH .staging/<sha>.<fence>.part and views/.tmp.<sha12>.<fence> during publish failure.
//  2. End-to-end svc.Ensure / runDownloadFenceWithPrev verification failure cleanup preserves swapped
//     .staging/<sha>.<fence>.part and escalates error_code to STORAGE_IO_ERROR.
//  3. CleanupFenceArtifacts / CleanupFenceArtifactsWithIdentity with unknown identity (nil) and
//     EvictCASAndViews(..., nil) refuse blind deletion and preserve existing files intact.
func TestFullDownloadFenceFailureCleanupPreservesSwappedEntriesAndRejectsUnknownIdentity(t *testing.T) {
	validBytes, validSHA := buildValidSafetensors(t, 256, nil)
	badBytes := append([]byte(nil), validBytes...)
	badBytes[len(badBytes)-1] ^= 0xff

	const giB = int64(1024 * 1024 * 1024)
	serveCorrupt := false
	svc, _, _ := newTestService(t, 600*giB, map[string][]byte{
		"/r3h02.safetensors": validBytes,
	})
	svc.Downloader.TransportForTarget = func(_ VerifiedTarget) (http.RoundTripper, func(), error) {
		rt := roundTripFunc(func(_ *http.Request) (*http.Response, error) {
			payload := validBytes
			if serveCorrupt {
				payload = badBytes
			}
			h := make(http.Header)
			h.Set("ETag", `"r3h02-etag"`)
			return &http.Response{
				StatusCode:    http.StatusOK,
				ContentLength: int64(len(payload)),
				Header:        h,
				Body:          io.NopCloser(bytes.NewReader(payload)),
			}, nil
		})
		return rt, func() {}, nil
	}
	now := time.Date(2026, 11, 15, 14, 0, 0, 0, time.UTC)
	svc.Now = func() time.Time { return now }
	ctx := context.Background()

	entry := CatalogEntry{
		ModelID:          "r3-h02-cleanup-ckpt",
		CatalogCode:      "MDL-CKPT-R3H2-001",
		DisplayName:      "R3-H02 Fence Cleanup Checkpoint",
		Category:         "CKPT",
		Filename:         "r3_h02_cleanup.safetensors",
		SHA256:           validSHA,
		SizeBytes:        int64(len(validBytes)),
		VRAMFootprintMiB: 2048,
		License:          "apache-2.0",
		SourceURLs:       []string{"https://huggingface.co/r3h02.safetensors"},
	}
	if err := svc.RegisterCatalogEntries(ctx, []CatalogEntry{entry}); err != nil {
		t.Fatal(err)
	}

	// Scenario A: Full svc.Ensure -> runDownloadFenceWithPrev publish swap + outer CleanupFenceArtifactsWithIdentity!
	bystanderPublishPart := []byte("BYSTANDER_PUBLISH_PART_DO_NOT_DELETE")
	bystanderPublishTmpView := []byte("BYSTANDER_TMP_VIEW_DO_NOT_DELETE")
	defer func() {
		testHookAfterFinalStatBeforeSyscall = nil
		svc.Hook = nil
	}()
	testHookAfterFinalStatBeforeSyscall = func(op, relPath string) {
		if op == "publish_cas" && relPath == PartRelPath(validSHA, 1) {
			p1 := PartFilePath(svc.RootDir, validSHA, 1)
			_ = os.Chmod(p1, 0o600)
			_ = os.Rename(p1, p1+".orig")
			_ = os.WriteFile(p1, bystanderPublishPart, 0o600)
			_ = os.WriteFile(TempViewLinkPath(svc.RootDir, validSHA, 1), bystanderPublishTmpView, 0o600)
		}
	}

	res1, err1 := svc.Ensure(ctx, EnsureRequest{
		OrganizationID: "org-1",
		ProjectID:      "proj-1",
		PrincipalID:    "user-1",
		IdempotencyKey: "idem-r3-h02-pub-swap",
		ModelID:        entry.ModelID,
	})
	testHookAfterFinalStatBeforeSyscall = nil
	if err1 != nil {
		t.Fatalf("Ensure call itself returned unexpected transport error: %v", err1)
	}
	if res1.Model.Status != StatusFailed || res1.Model.LastErrorCode != "STORAGE_IO_ERROR" {
		t.Fatalf("expected StatusFailed + STORAGE_IO_ERROR after publish swap, got %+v", res1.Model)
	}
	gotPart1, p1Err := os.ReadFile(PartFilePath(svc.RootDir, validSHA, 1))
	if p1Err != nil || !bytes.Equal(gotPart1, bystanderPublishPart) {
		t.Fatalf("CRITICAL R3-H02 VIOLATION: outer fence cleanup deleted or altered swapped .staging part! data=%q err=%v", gotPart1, p1Err)
	}
	gotTmp1, tv1Err := os.ReadFile(TempViewLinkPath(svc.RootDir, validSHA, 1))
	if tv1Err != nil || !bytes.Equal(gotTmp1, bystanderPublishTmpView) {
		t.Fatalf("CRITICAL R3-H02 VIOLATION: outer fence cleanup deleted or altered swapped views/.tmp entry! data=%q err=%v", gotTmp1, tv1Err)
	}
	_ = os.Remove(PartFilePath(svc.RootDir, validSHA, 1))
	_ = os.Remove(PartFilePath(svc.RootDir, validSHA, 1) + ".orig")
	_ = os.Remove(TempViewLinkPath(svc.RootDir, validSHA, 1))

	// Scenario B: Full svc.Ensure -> runDownloadFenceWithPrev verification swap + outer CleanupFenceArtifactsWithIdentity!
	serveCorrupt = true
	bystanderVerifyPart := []byte("BYSTANDER_VERIFY_PART_DO_NOT_DELETE")
	svc.Hook = StageHookFunc(func(_ context.Context, stage string, _ map[string]string) {
		if stage == "before_verify_part" {
			p2 := PartFilePath(svc.RootDir, validSHA, 2)
			_ = os.Rename(p2, p2+".orig")
			_ = os.WriteFile(p2, bystanderVerifyPart, 0o600)
		}
	})
	now = now.Add(5 * time.Second)
	res2, err2 := svc.Ensure(ctx, EnsureRequest{
		OrganizationID: "org-1",
		ProjectID:      "proj-1",
		PrincipalID:    "user-1",
		IdempotencyKey: "idem-r3-h02-verify-swap",
		ModelID:        entry.ModelID,
	})
	svc.Hook = nil
	serveCorrupt = false
	if err2 != nil {
		t.Fatalf("Ensure fence 2 returned unexpected error: %v", err2)
	}
	if res2.Model.Status != StatusFailed || res2.Model.LastErrorCode != "STORAGE_IO_ERROR" {
		t.Fatalf("expected StatusFailed + STORAGE_IO_ERROR on pre-verify swap, got %+v", res2.Model)
	}
	gotPart2, p2Err := os.ReadFile(PartFilePath(svc.RootDir, validSHA, 2))
	if p2Err != nil || !bytes.Equal(gotPart2, bystanderVerifyPart) {
		t.Fatalf("CRITICAL R3-H02 VIOLATION: pre-verify or outer cleanup deleted swapped .staging part! data=%q err=%v", gotPart2, p2Err)
	}
	_ = os.Remove(PartFilePath(svc.RootDir, validSHA, 2))
	_ = os.Remove(PartFilePath(svc.RootDir, validSHA, 2) + ".orig")

	// Scenario C: Unknown identity CleanupFenceArtifacts / CleanupFenceArtifactsWithIdentity(..., empty)
	//             AND EvictCASAndViews(..., nil) MUST preserve existing files intact and report anomaly!
	unknownPartPath := PartFilePath(svc.RootDir, validSHA, 99)
	unknownData := []byte("UNKNOWN_IDENTITY_ENTRY_PRESERVE")
	if err := os.WriteFile(unknownPartPath, unknownData, 0o600); err != nil {
		t.Fatal(err)
	}
	CleanupFenceArtifacts(svc.RootDir, validSHA, 99)
	if got, err := os.ReadFile(unknownPartPath); err != nil || !bytes.Equal(got, unknownData) {
		t.Fatalf("CleanupFenceArtifacts blindly unlinked entry with unknown identity! got=%q err=%v", got, err)
	}
	tampered, unkErr := CleanupFenceArtifactsWithIdentity(svc.RootDir, validSHA, 99, FenceArtifacts{})
	if !tampered || unkErr == nil {
		t.Fatalf("expected tampered=true and non-nil error for unknown identity cleanup, got tampered=%v err=%v", tampered, unkErr)
	}
	if got, err := os.ReadFile(unknownPartPath); err != nil || !bytes.Equal(got, unknownData) {
		t.Fatalf("CleanupFenceArtifactsWithIdentity({}, nil) altered entry! got=%q err=%v", got, err)
	}
	_ = os.Remove(unknownPartPath)

	// Publish Fence 3 cleanly, then verify EvictCASAndViews(..., nil) refuses unverified deletion!
	now = now.Add(5 * time.Second)
	res3, err3 := svc.Ensure(ctx, EnsureRequest{
		OrganizationID: "org-1",
		ProjectID:      "proj-1",
		PrincipalID:    "user-1",
		IdempotencyKey: "idem-r3-h02-clean-pub",
		ModelID:        entry.ModelID,
	})
	if err3 != nil || res3.Model.Status != StatusReady {
		t.Fatalf("expected clean Fence 3 READY, got %+v err=%v", res3, err3)
	}
	relView, _ := RelativeViewPath(entry.Category, validSHA, entry.Filename)
	if evNilErr := EvictCASAndViews(svc.RootDir, validSHA, []string{relView}, nil); evNilErr == nil {
		t.Fatalf("expected EvictCASAndViews(..., nil) to refuse deletion when CAS/view files exist")
	}
	if _, err := os.Lstat(CASFilePath(svc.RootDir, validSHA)); err != nil {
		t.Fatalf("expected CAS preserved when expectedCASInode == nil: %v", err)
	}
	viewFull, _ := ViewFilePath(svc.RootDir, relView)
	if _, err := os.Lstat(viewFull); err != nil {
		t.Fatalf("expected view preserved when expectedCASInode == nil: %v", err)
	}
}

type blockingReservedRevoker struct {
	mu          sync.Mutex
	fastResults map[string]AppAdmissionVerification
	slowCalls   int
}

func (b *blockingReservedRevoker) RevokeOrVerify(ctx context.Context, _ string, reservationID string) (AppAdmissionVerification, error) {
	b.mu.Lock()
	res, ok := b.fastResults[reservationID]
	if !ok {
		b.slowCalls++
	}
	b.mu.Unlock()
	if ok {
		return res, nil
	}
	// Simulate an unresponsive upstream endpoint that blocks until the caller's RPC deadline expires!
	<-ctx.Done()
	return AppAdmissionVerification{}, ctx.Err()
}

// TestSlowTimeoutReservedGroupsNeverStarveActiveOrphansUnderBoundedContext covers Round-3 R3-M01
// across BOTH MemoryStore and PostgreSQL PGStore:
//  1. 20 expired reserved groups hang until per-RPC context deadline (<-ctx.Done()), exhausting
//     the reserved phase budget inside a tight round context (220ms).
//  2. Zero hanging reserved pins are falsely released, last_reconcile_checked_at still persists
//     via dedicated DB headroom, and all 10 downstream active terminal orphans are released in Round 1!
//  3. Fast revoked expired reserved groups sitting behind the 20 slow groups are reached and released
//     as slow groups rotate behind via last_reconcile_checked_at across bounded rounds.
func TestSlowTimeoutReservedGroupsNeverStarveActiveOrphansUnderBoundedContext(t *testing.T) {
	runCase := func(t *testing.T, store Store, stationID string) {
		validBytes, validSHA := buildValidSafetensors(t, 128, nil)
		const giB = int64(1024 * 1024 * 1024)

		mux := http.NewServeMux()
		mux.HandleFunc("/r3m01.safetensors", func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("ETag", `"r3m01-etag"`)
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write(validBytes)
		})
		cert, rootPool := generateSelfSignedHostCert(t, "huggingface.co")
		srv := httptest.NewUnstartedServer(mux)
		srv.TLS = &tls.Config{
			MinVersion:   tls.VersionTLS12,
			Certificates: []tls.Certificate{cert},
		}
		srv.StartTLS()
		defer srv.Close()

		dl, err := NewSafeDownloader(nil, "")
		if err != nil {
			t.Fatal(err)
		}
		dl.Resolver = staticResolver{"huggingface.co": {{IP: net.ParseIP("93.184.216.34")}}}
		dl.TransportForTarget = func(target VerifiedTarget) (http.RoundTripper, func(), error) {
			srvURL, _ := url.Parse(srv.URL)
			base := &http.Transport{
				Proxy:             nil,
				DisableKeepAlives: true,
				TLSClientConfig: &tls.Config{
					MinVersion: tls.VersionTLS12,
					ServerName: target.Hostname,
					RootCAs:    rootPool,
				},
				DialContext: func(ctx context.Context, network, _ string) (net.Conn, error) {
					var d net.Dialer
					return d.DialContext(ctx, network, srvURL.Host)
				},
			}
			return base, base.CloseIdleConnections, nil
		}

		rootDir := t.TempDir()
		if err := EnsureLayout(rootDir); err != nil {
			t.Fatal(err)
		}
		now := time.Date(2026, 11, 15, 15, 0, 0, 0, time.UTC)
		revoker := &blockingReservedRevoker{fastResults: map[string]AppAdmissionVerification{}}
		svc := &Service{
			Store:                   store,
			StationID:               stationID,
			RootDir:                 rootDir,
			Downloader:              dl,
			DiskStat:                func(string) (int64, int64, error) { return 2000 * giB, 600 * giB, nil },
			Revoker:                 revoker,
			ActiveTaskReconcileAge:  30 * time.Second,
			ReconcilePerRPCTimeout:  15 * time.Millisecond,
			ReconcileStoreOpTimeout: 500 * time.Millisecond,
			Now:                     func() time.Time { return now },
			SynchronousDownload:     true,
		}

		ctx := context.Background()
		entry := CatalogEntry{
			ModelID:          "r3-m01-budget-ckpt",
			CatalogCode:      "MDL-CKPT-R3M1-001",
			DisplayName:      "R3-M01 Bounded Reconcile Checkpoint",
			Category:         "CKPT",
			Filename:         "r3_m01_budget.safetensors",
			SHA256:           validSHA,
			SizeBytes:        int64(len(validBytes)),
			VRAMFootprintMiB: 2048,
			License:          "apache-2.0",
			SourceURLs:       []string{"https://huggingface.co/r3m01.safetensors"},
		}
		if err := svc.RegisterCatalogEntries(ctx, []CatalogEntry{entry}); err != nil {
			t.Fatal(err)
		}
		if _, err := svc.Ensure(ctx, EnsureRequest{
			OrganizationID: "org-1",
			ProjectID:      "proj-1",
			PrincipalID:    "user-1",
			IdempotencyKey: "idem-r3-m01-seed",
			ModelID:        entry.ModelID,
		}); err != nil {
			t.Fatal(err)
		}

		// 1. Create 20 slow hanging expired reserved groups (created earliest -> selected FIRST!)
		for i := 1; i <= 20; i++ {
			rID := fmt.Sprintf("resv-slow-%03d", i)
			if _, err := svc.ReservePins(ctx, ReservePinsRequest{
				HolderAppID:    "comfyui",
				ReservationID:  rID,
				OperationID:    "op-" + rID,
				ScopeKind:      PinScopeTaskExecution,
				OwnerID:        "pending",
				OrganizationID: "org-1",
				ProjectID:      "proj-1",
				RequestSHA256:  strings.Repeat("a", 64),
				SHA256List:     []string{validSHA},
				TTLSeconds:     30,
			}); err != nil {
				t.Fatal(err)
			}
		}

		// 2. Create 3 fast revoked expired reserved groups AFTER the 20 slow groups!
		now = now.Add(time.Second)
		for i := 1; i <= 3; i++ {
			rID := fmt.Sprintf("resv-fast-revoked-%03d", i)
			if _, err := svc.ReservePins(ctx, ReservePinsRequest{
				HolderAppID:    "comfyui",
				ReservationID:  rID,
				OperationID:    "op-" + rID,
				ScopeKind:      PinScopeTaskExecution,
				OwnerID:        "pending",
				OrganizationID: "org-1",
				ProjectID:      "proj-1",
				RequestSHA256:  strings.Repeat("b", 64),
				SHA256List:     []string{validSHA},
				TTLSeconds:     30,
			}); err != nil {
				t.Fatal(err)
			}
			revoker.fastResults[rID] = AppAdmissionVerification{
				ReservationID: rID,
				State:         AdmissionStateRevoked,
			}
		}

		// 3. Create 10 active terminal task_execution orphans (and measure 10 real store commits
		// so tight phase budgets remain valid both for sub-ms MemoryStore and high-RTT remote PG)!
		commitSampleStart := time.Now()
		for i := 1; i <= 10; i++ {
			rID := fmt.Sprintf("act-orphan-%03d", i)
			if _, err := svc.ReservePins(ctx, ReservePinsRequest{
				HolderAppID:    "comfyui",
				ReservationID:  rID,
				OperationID:    "op-" + rID,
				ScopeKind:      PinScopeTaskExecution,
				OwnerID:        fmt.Sprintf("task-orphan-%03d", i),
				OrganizationID: "org-1",
				ProjectID:      "proj-1",
				RequestSHA256:  strings.Repeat("c", 64),
				SHA256List:     []string{validSHA},
			}); err != nil {
				t.Fatal(err)
			}
			if _, err := svc.CommitPins(ctx, CommitPinsRequest{HolderAppID: "comfyui", ReservationID: rID}); err != nil {
				t.Fatal(err)
			}
			revoker.fastResults[rID] = AppAdmissionVerification{
				ReservationID: rID,
				State:         AdmissionStateReleased,
				EntityID:      fmt.Sprintf("task-orphan-%03d", i),
			}
		}
		tenTxDur := time.Since(commitSampleStart)
		roundBudget := 220 * time.Millisecond
		if tenTxDur > 30*time.Millisecond {
			// Over remote/port-forwarded PostgreSQL, calibrate budgets from measured 10-tx latency so:
			// (a) Phase 1 still genuinely exhausts after ~3-4 slow RPCs (well short of the 20 slow groups), and
			// (b) Phase 2 has >= 2.5x the measured 10-tx DB RTT needed to release all 10 active orphans in Round 1.
			phaseTarget := tenTxDur * 3
			svc.ReconcilePerRPCTimeout = phaseTarget / 4
			svc.ReconcileStoreOpTimeout = tenTxDur
			roundBudget = phaseTarget * 9 / 4
		}

		// Advance clock past both TTL (30s) and ActiveTaskReconcileAge (30s).
		now = now.Add(2 * time.Minute)

		// Round 1 under a tight bounded deadline (~3-6 slow RPCs time out, genuinely exhausting the
		// reserved phase budget BEFORE reaching all 20 slow groups!):
		// Downstream active phase MUST still run in Round 1 and release all 10 active orphans!
		r1Ctx, r1Cancel := context.WithTimeout(ctx, roundBudget)
		_, rel1, err1 := svc.ReconcileExpiredPins(r1Ctx)
		r1Cancel()
		if err1 != nil {
			t.Fatalf("Round 1 ReconcileExpiredPins failed under tight deadline: %v", err1)
		}
		stAfterR1, err := svc.GetModelStatus(ctx, entry.ModelID)
		if err != nil {
			t.Fatal(err)
		}
		if stAfterR1.ActivePins != 0 || rel1 < 10 {
			t.Fatalf("CRITICAL R3-M01 VIOLATION: slow reserved timeouts starved active orphans in Round 1! ActivePins=%d rel1=%d", stAfterR1.ActivePins, rel1)
		}
		if stAfterR1.ReservedPins < 20 {
			t.Fatalf("CRITICAL SAFETY VIOLATION: hanging reserved pins were falsely released on timeout! ReservedPins=%d", stAfterR1.ReservedPins)
		}

		// Run bounded subsequent rounds (advancing logical `now` by 1s per round so last_reconcile_checked_at
		// rotates timed-out slow groups behind untouched groups) until the 3 fast revoked groups past the
		// 20 slow groups are reached and released!
		totalReleased := rel1
		for round := 2; round <= 12; round++ {
			now = now.Add(time.Second)
			rCtx, rCancel := context.WithTimeout(ctx, roundBudget)
			_, relN, rErr := svc.ReconcileExpiredPins(rCtx)
			rCancel()
			if rErr != nil {
				t.Fatalf("Round %d ReconcileExpiredPins failed: %v", round, rErr)
			}
			totalReleased += relN
			stCur, sErr := svc.GetModelStatus(ctx, entry.ModelID)
			if sErr != nil {
				t.Fatal(sErr)
			}
			if stCur.ReservedPins == 20 && totalReleased == 13 {
				break
			}
		}

		stFinal, err := svc.GetModelStatus(ctx, entry.ModelID)
		if err != nil {
			t.Fatal(err)
		}
		if stFinal.ActivePins != 0 || stFinal.ReservedPins != 20 || totalReleased != 13 {
			t.Fatalf("expected ActivePins=0 ReservedPins=20 (all 20 slow preserved, 3 fast revoked + 10 active released = 13), got ActivePins=%d ReservedPins=%d totalReleased=%d",
				stFinal.ActivePins, stFinal.ReservedPins, totalReleased)
		}
	}

	t.Run("MemoryStore", func(t *testing.T) {
		runCase(t, NewMemoryStore(), "station-mem-r3m01")
	})
	t.Run("PGStore", func(t *testing.T) {
		pgStore, stationID := setupModelCachePGStore(t)
		runCase(t, pgStore, stationID)
	})
}
