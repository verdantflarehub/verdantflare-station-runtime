package nodegpu

import (
	"bufio"
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/md5"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

var collectorDigestRe = regexp.MustCompile(`^sha256:[0-9a-f]{64}$`)

type CollectorSample struct {
	CollectorSeq int64        `json:"collector_seq"`
	NodeUID      string       `json:"node_uid"`
	NodeName     string       `json:"node_name"`
	SampledAt    string       `json:"sampled_at"`
	NVML         NVMLSnapshot `json:"nvml"`
	ProcCount    int          `json:"proc_count"`
	GPUProcs     []ProcEntry  `json:"gpu_procs"`
	PrevHash     string       `json:"prev_hash"`
	SampleHash   string       `json:"sample_hash"`
}

type S3PutOnlyConfig struct {
	EndpointURL    string
	Bucket         string
	Region         string
	KeyPrefix      string
	AccessKey      string
	SecretKey      string
	RetentionHours int
}

type collectorCheckpoint struct {
	NodeUID         string `json:"node_uid"`
	LastSeq         int64  `json:"last_seq"`
	LastHash        string `json:"last_hash"`
	LastUploadedSeq int64  `json:"last_uploaded_seq"`
	UpdatedAt       string `json:"updated_at"`
}

type Collector struct {
	NodeUID             string
	NodeName            string
	SpoolDir            string
	StrictAudit         bool
	ImageRef            string
	ExpectedImageDigest string
	NVML                NVMLProbe
	Proc                ProcInspector
	S3                  *S3PutOnlyConfig
	HTTPClient          *http.Client
	Now                 func() time.Time

	mu              sync.Mutex
	recovered       bool
	lastUploadedSeq int64
	lastErr         atomic.Value
	seq             atomic.Int64
	prevHash        string
}

func (c *Collector) now() time.Time {
	if c != nil && c.Now != nil {
		return c.Now().UTC()
	}
	return time.Now().UTC()
}

func (c *Collector) s3Enabled() bool {
	return c != nil && c.S3 != nil &&
		strings.TrimSpace(c.S3.EndpointURL) != "" &&
		strings.TrimSpace(c.S3.Bucket) != ""
}

func (c *Collector) ValidateConfig() error {
	if strings.TrimSpace(c.NodeUID) == "" {
		return errors.New("collector missing authoritative NodeUID")
	}
	if !c.StrictAudit {
		return nil
	}
	if strings.TrimSpace(c.SpoolDir) == "" {
		return errors.New("strict audit collector requires durable COLLECTOR_SPOOL_DIR")
	}
	if !c.s3Enabled() || strings.TrimSpace(c.S3.AccessKey) == "" || strings.TrimSpace(c.S3.SecretKey) == "" {
		return errors.New("strict audit collector requires complete S3 PutObject configuration")
	}
	imgRef := strings.TrimSpace(c.ImageRef)
	expDigest := strings.ToLower(strings.TrimSpace(c.ExpectedImageDigest))
	if expDigest == "" || !collectorDigestRe.MatchString(expDigest) {
		return errors.New("strict audit collector requires immutable sha256 COLLECTOR_EXPECTED_IMAGE_DIGEST")
	}
	if imgRef == "" || !strings.HasSuffix(strings.ToLower(imgRef), "@"+expDigest) {
		return errors.New("strict audit collector requires COLLECTOR_IMAGE_REF pinned to @sha256 digest matching COLLECTOR_EXPECTED_IMAGE_DIGEST")
	}
	return nil
}

func computeCollectorSampleHash(s CollectorSample) (string, error) {
	clone := s
	clone.SampleHash = ""
	raw, err := json.Marshal(clone)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:]), nil
}

func (c *Collector) checkpointPath() string {
	return filepath.Join(c.SpoolDir, "checkpoint.json")
}

func (c *Collector) spoolFilePath() string {
	return filepath.Join(c.SpoolDir, "node_nvml_samples.jsonl")
}

func (c *Collector) saveCheckpointLocked(now time.Time) error {
	if strings.TrimSpace(c.SpoolDir) == "" {
		return nil
	}
	if err := ensurePrivateDir(c.SpoolDir); err != nil {
		return err
	}
	cp := collectorCheckpoint{
		NodeUID:         c.NodeUID,
		LastSeq:         c.seq.Load(),
		LastHash:        c.prevHash,
		LastUploadedSeq: c.lastUploadedSeq,
		UpdatedAt:       now.Format(time.RFC3339Nano),
	}
	raw, err := json.Marshal(cp)
	if err != nil {
		return err
	}
	return writePrivateAtomic(c.checkpointPath(), raw)
}

// RecoverAndReplay restores monotonic (collector_seq, prev_hash) state from the durable spool
// and replays any un-uploaded JSONL samples to S3 in strict ascending collector_seq order
// BEFORE any new sample is captured after a crash or container restart.
func (c *Collector) RecoverAndReplay(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.recoverAndReplayLocked(ctx)
}

func (c *Collector) recoverAndReplayLocked(ctx context.Context) error {
	if err := c.ValidateConfig(); err != nil {
		return err
	}
	if err := c.Proc.VerifyReadAccess(); err != nil {
		return err
	}
	if strings.TrimSpace(c.SpoolDir) == "" {
		if c.prevHash == "" {
			c.prevHash = "GENESIS"
		}
		c.recovered = true
		return nil
	}
	if err := ensurePrivateDir(c.SpoolDir); err != nil {
		return err
	}

	var cp collectorCheckpoint
	if rawCP, err := os.ReadFile(c.checkpointPath()); err == nil && len(rawCP) > 0 {
		if err := json.Unmarshal(rawCP, &cp); err != nil {
			return fmt.Errorf("corrupt collector checkpoint: %w", err)
		}
	}

	var samples []CollectorSample
	var rawLines [][]byte
	if f, err := os.Open(c.spoolFilePath()); err == nil {
		scanner := bufio.NewScanner(f)
		scanner.Buffer(make([]byte, 64*1024), 16*1024*1024)
		expectedPrev := "GENESIS"
		var expectedSeq int64 = 1
		for scanner.Scan() {
			line := bytes.TrimSpace(scanner.Bytes())
			if len(line) == 0 {
				continue
			}
			var s CollectorSample
			if err := json.Unmarshal(line, &s); err != nil {
				_ = f.Close()
				return fmt.Errorf("corrupt spool sample at seq %d: %w", expectedSeq, err)
			}
			wantHash, err := computeCollectorSampleHash(s)
			if err != nil || s.CollectorSeq != expectedSeq || s.PrevHash != expectedPrev || s.SampleHash != wantHash {
				_ = f.Close()
				return fmt.Errorf("collector spool hash chain broken at seq %d", s.CollectorSeq)
			}
			expectedPrev = s.SampleHash
			expectedSeq++
			samples = append(samples, s)
			lineCopy := append([]byte(nil), line...)
			lineCopy = append(lineCopy, '\n')
			rawLines = append(rawLines, lineCopy)
		}
		scanErr := scanner.Err()
		_ = f.Close()
		if scanErr != nil {
			return scanErr
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}

	if len(samples) > 0 {
		last := samples[len(samples)-1]
		c.seq.Store(last.CollectorSeq)
		c.prevHash = last.SampleHash
		if cp.LastUploadedSeq > 0 && cp.LastUploadedSeq <= last.CollectorSeq {
			c.lastUploadedSeq = cp.LastUploadedSeq
		}
	} else if cp.LastSeq > 0 && cp.LastHash != "" {
		c.seq.Store(cp.LastSeq)
		c.prevHash = cp.LastHash
		c.lastUploadedSeq = cp.LastUploadedSeq
	} else if c.prevHash == "" {
		c.prevHash = "GENESIS"
	}

	if c.s3Enabled() {
		for idx, s := range samples {
			if s.CollectorSeq <= c.lastUploadedSeq {
				continue
			}
			sampleTime, pErr := time.Parse(time.RFC3339Nano, s.SampledAt)
			if pErr != nil {
				sampleTime = c.now()
			}
			objKey := c.objectKeyFor(s, sampleTime)
			if err := c.putObjectImmutable(ctx, objKey, rawLines[idx], c.now()); err != nil {
				c.lastErr.Store(err.Error())
				return err
			}
			c.lastUploadedSeq = s.CollectorSeq
			if err := c.saveCheckpointLocked(c.now()); err != nil {
				return err
			}
		}
	}

	c.recovered = true
	return c.saveCheckpointLocked(c.now())
}

func (c *Collector) objectKeyFor(s CollectorSample, sampleTime time.Time) string {
	objKey := fmt.Sprintf(
		"%s/%s/%s/sample_%08d_%s.json",
		strings.Trim(c.S3.KeyPrefix, "/"),
		c.NodeUID,
		sampleTime.UTC().Format("20060102"),
		s.CollectorSeq,
		s.SampleHash[:12],
	)
	return strings.TrimLeft(objKey, "/")
}

func (c *Collector) CaptureOnce(ctx context.Context) (CollectorSample, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if !c.recovered {
		if err := c.recoverAndReplayLocked(ctx); err != nil {
			c.lastErr.Store(err.Error())
			return CollectorSample{}, err
		}
	} else if c.s3Enabled() && strings.TrimSpace(c.SpoolDir) != "" && c.lastUploadedSeq < c.seq.Load() {
		// Replay any previously failed S3 uploads in strict collector_seq order before appending new samples!
		if err := c.recoverAndReplayLocked(ctx); err != nil {
			c.lastErr.Store(err.Error())
			return CollectorSample{}, err
		}
	}

	if c.prevHash == "" {
		c.prevHash = "GENESIS"
	}
	nvmlSnap, err := c.NVML.Sample(ctx)
	if err != nil {
		c.lastErr.Store(err.Error())
		return CollectorSample{}, err
	}
	reqPIDs := make([]int, 0, len(nvmlSnap.Processes))
	for _, gp := range nvmlSnap.Processes {
		reqPIDs = append(reqPIDs, gp.PID)
	}
	procs, err := c.Proc.ScanProcessesStrict(reqPIDs)
	if err != nil {
		// Retry once on transient PID exit; fail closed if still incomplete.
		nvmlSnap2, err2 := c.NVML.Sample(ctx)
		if err2 != nil {
			c.lastErr.Store(err2.Error())
			return CollectorSample{}, err2
		}
		reqPIDs2 := make([]int, 0, len(nvmlSnap2.Processes))
		for _, gp := range nvmlSnap2.Processes {
			reqPIDs2 = append(reqPIDs2, gp.PID)
		}
		procs, err = c.Proc.ScanProcessesStrict(reqPIDs2)
		if err != nil {
			c.lastErr.Store(err.Error())
			return CollectorSample{}, err
		}
		nvmlSnap = nvmlSnap2
	}

	gpuPIDSet := make(map[int]bool, len(nvmlSnap.Processes))
	for _, gp := range nvmlSnap.Processes {
		gpuPIDSet[gp.PID] = true
	}
	var gpuProcs []ProcEntry
	for _, p := range procs {
		if gpuPIDSet[p.PID] || p.HasNvidiaFD || p.PodUID != "" {
			gpuProcs = append(gpuProcs, p)
		}
	}

	nextSeq := c.seq.Load() + 1
	now := c.now()
	sample := CollectorSample{
		CollectorSeq: nextSeq,
		NodeUID:      c.NodeUID,
		NodeName:     c.NodeName,
		SampledAt:    now.Format(time.RFC3339Nano),
		NVML:         nvmlSnap,
		ProcCount:    len(procs),
		GPUProcs:     gpuProcs,
		PrevHash:     c.prevHash,
	}
	sampleHash, err := computeCollectorSampleHash(sample)
	if err != nil {
		return CollectorSample{}, err
	}
	sample.SampleHash = sampleHash

	lineBytes, err := json.Marshal(sample)
	if err != nil {
		return CollectorSample{}, err
	}
	lineBytes = append(lineBytes, '\n')

	if strings.TrimSpace(c.SpoolDir) != "" {
		if err := ensurePrivateDir(c.SpoolDir); err != nil {
			c.lastErr.Store(err.Error())
			return CollectorSample{}, err
		}
		f, err := os.OpenFile(c.spoolFilePath(), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
		if err != nil {
			c.lastErr.Store(err.Error())
			return CollectorSample{}, err
		}
		_ = f.Chmod(0o600)
		_, wErr := f.Write(lineBytes)
		sErr := f.Sync()
		cErr := f.Close()
		if wErr != nil {
			c.lastErr.Store(wErr.Error())
			return CollectorSample{}, wErr
		}
		if sErr != nil {
			c.lastErr.Store(sErr.Error())
			return CollectorSample{}, sErr
		}
		if cErr != nil {
			c.lastErr.Store(cErr.Error())
			return CollectorSample{}, cErr
		}
	}

	// Only advance in-memory chain head once durable spool write succeeded!
	c.seq.Store(nextSeq)
	c.prevHash = sample.SampleHash
	if err := c.saveCheckpointLocked(now); err != nil {
		c.lastErr.Store(err.Error())
		return sample, err
	}

	if c.s3Enabled() {
		objKey := c.objectKeyFor(sample, now)
		if err := c.putObjectImmutable(ctx, objKey, lineBytes, now); err != nil {
			c.lastErr.Store(err.Error())
			return sample, err
		}
		c.lastUploadedSeq = nextSeq
		if err := c.saveCheckpointLocked(now); err != nil {
			c.lastErr.Store(err.Error())
			return sample, err
		}
	} else {
		c.lastUploadedSeq = nextSeq
	}

	c.lastErr.Store("")
	return sample, nil
}

func (c *Collector) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet || r.URL.Path != "/healthz" {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	if err := c.ValidateConfig(); err != nil {
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte(err.Error()))
		return
	}
	if last, _ := c.lastErr.Load().(string); last != "" {
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte(last))
		return
	}
	if c.seq.Load() <= 0 {
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte("waiting for first collector sample"))
		return
	}
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok"))
}

func (c *Collector) putObjectImmutable(ctx context.Context, objectKey string, body []byte, now time.Time) error {
	base, err := url.Parse(strings.TrimRight(c.S3.EndpointURL, "/"))
	if err != nil {
		return err
	}
	base.Path = strings.TrimRight(base.Path, "/") + "/" + strings.Trim(c.S3.Bucket, "/") + "/" + objectKey
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, base.String(), bytes.NewReader(body))
	if err != nil {
		return err
	}

	payloadSum := sha256.Sum256(body)
	payloadHex := hex.EncodeToString(payloadSum[:])
	md5Sum := md5.Sum(body)
	req.Header.Set("Content-Type", "application/x-ndjson")
	req.Header.Set("Content-MD5", base64.StdEncoding.EncodeToString(md5Sum[:]))
	req.Header.Set("X-Amz-Content-Sha256", payloadHex)
	req.Header.Set("X-Amz-Date", now.Format("20060102T150405Z"))
	if c.S3.RetentionHours > 0 {
		req.Header.Set("X-Amz-Object-Lock-Mode", "GOVERNANCE")
		req.Header.Set("X-Amz-Object-Lock-Retain-Until-Date", now.Add(time.Duration(c.S3.RetentionHours)*time.Hour).Format(time.RFC3339))
	}
	req.Host = base.Host

	region := strings.TrimSpace(c.S3.Region)
	if region == "" {
		region = "us-east-1"
	}
	signS3PutRequest(req, c.S3.AccessKey, c.S3.SecretKey, region, payloadHex, now)

	client := c.HTTPClient
	if client == nil {
		client = &http.Client{
			Timeout: 8 * time.Second,
			CheckRedirect: func(*http.Request, []*http.Request) error {
				return http.ErrUseLastResponse
			},
		}
	}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("s3 PutObject status %d", resp.StatusCode)
	}
	return nil
}

func signS3PutRequest(req *http.Request, accessKey, secretKey, region, payloadHex string, now time.Time) {
	if accessKey == "" || secretKey == "" {
		return
	}
	amzDate := now.Format("20060102T150405Z")
	dateStamp := now.Format("20060102")

	headersToSign := map[string]string{
		"host":                 req.Host,
		"content-md5":          req.Header.Get("Content-MD5"),
		"content-type":         req.Header.Get("Content-Type"),
		"x-amz-content-sha256": payloadHex,
		"x-amz-date":           amzDate,
	}
	if mode := req.Header.Get("X-Amz-Object-Lock-Mode"); mode != "" {
		headersToSign["x-amz-object-lock-mode"] = mode
		headersToSign["x-amz-object-lock-retain-until-date"] = req.Header.Get("X-Amz-Object-Lock-Retain-Until-Date")
	}

	keys := make([]string, 0, len(headersToSign))
	for k := range headersToSign {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	var canonicalHeaders strings.Builder
	for _, k := range keys {
		canonicalHeaders.WriteString(k)
		canonicalHeaders.WriteByte(':')
		canonicalHeaders.WriteString(strings.TrimSpace(headersToSign[k]))
		canonicalHeaders.WriteByte('\n')
	}
	signedHeaders := strings.Join(keys, ";")

	canonicalReq := strings.Join([]string{
		req.Method,
		req.URL.EscapedPath(),
		req.URL.RawQuery,
		canonicalHeaders.String(),
		signedHeaders,
		payloadHex,
	}, "\n")
	canonHash := sha256.Sum256([]byte(canonicalReq))
	scope := fmt.Sprintf("%s/%s/s3/aws4_request", dateStamp, region)
	stringToSign := strings.Join([]string{
		"AWS4-HMAC-SHA256",
		amzDate,
		scope,
		hex.EncodeToString(canonHash[:]),
	}, "\n")

	hmacSHA256 := func(key []byte, data string) []byte {
		h := hmac.New(sha256.New, key)
		_, _ = h.Write([]byte(data))
		return h.Sum(nil)
	}
	kDate := hmacSHA256([]byte("AWS4"+secretKey), dateStamp)
	kRegion := hmacSHA256(kDate, region)
	kService := hmacSHA256(kRegion, "s3")
	kSigning := hmacSHA256(kService, "aws4_request")
	signature := hex.EncodeToString(hmacSHA256(kSigning, stringToSign))

	req.Header.Set("Authorization", fmt.Sprintf(
		"AWS4-HMAC-SHA256 Credential=%s/%s, SignedHeaders=%s, Signature=%s",
		accessKey, scope, signedHeaders, signature,
	))
}
