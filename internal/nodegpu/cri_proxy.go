package nodegpu

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/verdantflarehub/verdantflare-station-runtime/internal/strictjson"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protowire"
)

const criContainerStatusMethod = "/runtime.v1.RuntimeService/ContainerStatus"

var ErrContainerNotFound = errors.New("CRI_CONTAINER_NOT_FOUND")

type CRIContainerStatus struct {
	ContainerID   string `json:"container_id"`
	ContainerName string `json:"container_name,omitempty"`
	PodUID        string `json:"pod_uid,omitempty"`
	State         string `json:"state"`
	Exited        bool   `json:"exited"`
	ExitCode      int32  `json:"exit_code"`
	StartedAt     int64  `json:"started_at,omitempty"`
	FinishedAt    int64  `json:"finished_at"`
	Tombstone     bool   `json:"tombstone,omitempty"`
}

type ContainerStatusSource interface {
	ContainerStatus(ctx context.Context, containerID string) (CRIContainerStatus, error)
}

type TombstoneStore struct {
	Dir     string
	HMACKey []byte
}

type EpochTombstone struct {
	PodUID                   string `json:"pod_uid"`
	ContainerID              string `json:"container_id"`
	BootID                   string `json:"boot_id"`
	GPUUUID                  string `json:"gpu_uuid"`
	GPUEpoch                 int64  `json:"gpu_epoch"`
	HostSupervisorPID        int    `json:"host_supervisor_pid"`
	ContainerSupervisorPID   int    `json:"container_supervisor_pid"`
	SupervisorStarttimeTicks uint64 `json:"supervisor_starttime_ticks"`
	PIDNamespace             string `json:"pid_namespace,omitempty"`
	Reclaimed                bool   `json:"reclaimed"`
	SealedAtUnixNano         int64  `json:"sealed_at_unix_nano"`
	HMACSHA256               string `json:"hmac_sha256,omitempty"`
}

func computeEpochTombstoneHMAC(key []byte, et EpochTombstone) string {
	mac := hmac.New(sha256.New, key)
	_, _ = fmt.Fprintf(
		mac,
		"%s|%s|%s|%s|%d|%d|%d|%d|%s|%t|%d",
		et.PodUID,
		et.ContainerID,
		et.BootID,
		et.GPUUUID,
		et.GPUEpoch,
		et.HostSupervisorPID,
		et.ContainerSupervisorPID,
		et.SupervisorStarttimeTicks,
		et.PIDNamespace,
		et.Reclaimed,
		et.SealedAtUnixNano,
	)
	return hex.EncodeToString(mac.Sum(nil))
}

func (ts TombstoneStore) pathFor(cleanID string) string {
	return filepath.Join(ts.Dir, cleanID+".json")
}

func (ts TombstoneStore) epochPathFor(podUID string, epoch int64) string {
	return filepath.Join(ts.Dir, fmt.Sprintf("%s_epoch_%d.json", normalizePodUID(podUID), epoch))
}

func ensurePrivateDir(dir string) error {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	_ = os.Chmod(dir, 0o700)
	return nil
}

func writePrivateAtomic(targetPath string, raw []byte) error {
	tmpPath := targetPath + ".tmp"
	f, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		return err
	}
	_ = f.Chmod(0o600)
	if _, err := f.Write(raw); err != nil {
		_ = f.Close()
		_ = os.Remove(tmpPath)
		return err
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		_ = os.Remove(tmpPath)
		return err
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(tmpPath)
		return err
	}
	return os.Rename(tmpPath, targetPath)
}

func (ts TombstoneStore) SaveExited(st CRIContainerStatus) error {
	if strings.TrimSpace(ts.Dir) == "" {
		return nil
	}
	cleanID := NormalizeContainerID(st.ContainerID)
	if cleanID == "" || strings.Contains(cleanID, "/") || !st.Exited || st.FinishedAt <= 0 {
		return nil
	}
	if err := ensurePrivateDir(ts.Dir); err != nil {
		return err
	}
	st.ContainerID = cleanID
	st.Tombstone = true
	raw, err := json.Marshal(st)
	if err != nil {
		return err
	}
	return writePrivateAtomic(ts.pathFor(cleanID), raw)
}

func (ts TombstoneStore) Load(containerID string) (CRIContainerStatus, bool, error) {
	if strings.TrimSpace(ts.Dir) == "" {
		return CRIContainerStatus{}, false, nil
	}
	cleanID := NormalizeContainerID(containerID)
	if cleanID == "" || strings.Contains(cleanID, "/") {
		return CRIContainerStatus{}, false, nil
	}
	raw, err := os.ReadFile(ts.pathFor(cleanID))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return CRIContainerStatus{}, false, nil
		}
		return CRIContainerStatus{}, false, err
	}
	var out CRIContainerStatus
	if err := strictjson.Decode(bytes.NewReader(raw), 64<<10, &out); err != nil {
		return CRIContainerStatus{}, false, err
	}
	if !out.Exited || out.FinishedAt <= 0 || NormalizeContainerID(out.ContainerID) != cleanID {
		return CRIContainerStatus{}, false, nil
	}
	out.Tombstone = true
	return out, true, nil
}

func (ts TombstoneStore) SaveEpochTombstone(et EpochTombstone) error {
	if strings.TrimSpace(ts.Dir) == "" {
		return nil
	}
	et.PodUID = normalizePodUID(et.PodUID)
	et.ContainerID = strings.ToLower(NormalizeContainerID(et.ContainerID))
	et.BootID = strings.TrimSpace(et.BootID)
	et.GPUUUID = strings.TrimSpace(et.GPUUUID)
	et.PIDNamespace = strings.TrimSpace(et.PIDNamespace)
	if et.PodUID == "" || strings.Contains(et.PodUID, "/") ||
		et.ContainerID == "" || strings.Contains(et.ContainerID, "/") ||
		et.BootID == "" || et.GPUUUID == "" || et.GPUEpoch <= 0 ||
		et.HostSupervisorPID <= 0 || et.ContainerSupervisorPID <= 0 || et.SupervisorStarttimeTicks == 0 ||
		!et.Reclaimed || et.SealedAtUnixNano <= 0 {
		return errors.New("incomplete epoch tombstone")
	}
	if len(ts.HMACKey) > 0 {
		et.HMACSHA256 = computeEpochTombstoneHMAC(ts.HMACKey, et)
	}
	if err := ensurePrivateDir(ts.Dir); err != nil {
		return err
	}
	raw, err := json.Marshal(et)
	if err != nil {
		return err
	}
	return writePrivateAtomic(ts.epochPathFor(et.PodUID, et.GPUEpoch), raw)
}

func (ts TombstoneStore) LoadVerifiedEpochTombstone(podUID, containerID, bootID, gpuUUID string, epoch int64) (EpochTombstone, bool, error) {
	if strings.TrimSpace(ts.Dir) == "" {
		return EpochTombstone{}, false, nil
	}
	cleanPod := normalizePodUID(podUID)
	cleanCID := strings.ToLower(NormalizeContainerID(containerID))
	cleanBoot := strings.TrimSpace(bootID)
	cleanGPU := strings.TrimSpace(gpuUUID)
	if cleanPod == "" || strings.Contains(cleanPod, "/") || cleanCID == "" || cleanBoot == "" || epoch <= 0 {
		return EpochTombstone{}, false, nil
	}
	raw, err := os.ReadFile(ts.epochPathFor(cleanPod, epoch))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return EpochTombstone{}, false, nil
		}
		return EpochTombstone{}, false, err
	}
	var out EpochTombstone
	if err := strictjson.Decode(bytes.NewReader(raw), 64<<10, &out); err != nil {
		return EpochTombstone{}, false, err
	}
	if !out.Reclaimed || out.SealedAtUnixNano <= 0 || out.HostSupervisorPID <= 0 ||
		out.ContainerSupervisorPID <= 0 || out.SupervisorStarttimeTicks == 0 || out.GPUEpoch != epoch {
		return EpochTombstone{}, false, nil
	}
	if normalizePodUID(out.PodUID) != cleanPod ||
		strings.ToLower(NormalizeContainerID(out.ContainerID)) != cleanCID ||
		strings.TrimSpace(out.BootID) != cleanBoot {
		return EpochTombstone{}, false, nil
	}
	if cleanGPU != "" && strings.TrimSpace(out.GPUUUID) != cleanGPU {
		return EpochTombstone{}, false, nil
	}
	if len(ts.HMACKey) > 0 {
		expectedMAC := computeEpochTombstoneHMAC(ts.HMACKey, out)
		if out.HMACSHA256 == "" || !hmac.Equal([]byte(out.HMACSHA256), []byte(expectedMAC)) {
			return EpochTombstone{}, false, nil
		}
	}
	return out, true, nil
}

type rawBytesCodec struct{}

func (rawBytesCodec) Marshal(v any) ([]byte, error) {
	b, ok := v.([]byte)
	if !ok {
		return nil, errors.New("rawBytesCodec requires []byte")
	}
	return b, nil
}

func (rawBytesCodec) Unmarshal(data []byte, v any) error {
	out, ok := v.(*[]byte)
	if !ok {
		return errors.New("rawBytesCodec requires *[]byte")
	}
	*out = append((*out)[:0], data...)
	return nil
}

func (rawBytesCodec) Name() string { return "proto" }

// ContainerdCRISource queries containerd CRI over a read-only Unix socket and
// physically implements ONLY /runtime.v1.RuntimeService/ContainerStatus.
type ContainerdCRISource struct {
	SocketPath string
}

func encodeContainerStatusRequest(containerID string) []byte {
	var b []byte
	b = protowire.AppendTag(b, 1, protowire.BytesType)
	b = protowire.AppendString(b, containerID)
	return b
}

func decodeProtoStringMapEntry(raw []byte) (key, val string, ok bool) {
	for len(raw) > 0 {
		num, typ, n := protowire.ConsumeTag(raw)
		if n < 0 {
			return "", "", false
		}
		raw = raw[n:]
		switch {
		case num == 1 && typ == protowire.BytesType:
			s, m := protowire.ConsumeString(raw)
			if m < 0 {
				return "", "", false
			}
			key = s
			raw = raw[m:]
		case num == 2 && typ == protowire.BytesType:
			s, m := protowire.ConsumeString(raw)
			if m < 0 {
				return "", "", false
			}
			val = s
			raw = raw[m:]
		default:
			m := protowire.ConsumeFieldValue(num, typ, raw)
			if m < 0 {
				return "", "", false
			}
			raw = raw[m:]
		}
	}
	return key, val, key != ""
}

func decodeContainerMetadataName(raw []byte) string {
	for len(raw) > 0 {
		num, typ, n := protowire.ConsumeTag(raw)
		if n < 0 {
			return ""
		}
		raw = raw[n:]
		if num == 1 && typ == protowire.BytesType {
			s, m := protowire.ConsumeString(raw)
			if m < 0 {
				return ""
			}
			return s
		}
		m := protowire.ConsumeFieldValue(num, typ, raw)
		if m < 0 {
			return ""
		}
		raw = raw[m:]
	}
	return ""
}

func decodeContainerStatusResponse(raw []byte) (CRIContainerStatus, error) {
	var statusBytes []byte
	for len(raw) > 0 {
		num, typ, n := protowire.ConsumeTag(raw)
		if n < 0 {
			return CRIContainerStatus{}, protowire.ParseError(n)
		}
		raw = raw[n:]
		if num == 1 && typ == protowire.BytesType {
			val, m := protowire.ConsumeBytes(raw)
			if m < 0 {
				return CRIContainerStatus{}, protowire.ParseError(m)
			}
			statusBytes = val
			raw = raw[m:]
			continue
		}
		m := protowire.ConsumeFieldValue(num, typ, raw)
		if m < 0 {
			return CRIContainerStatus{}, protowire.ParseError(m)
		}
		raw = raw[m:]
	}
	if len(statusBytes) == 0 {
		return CRIContainerStatus{}, errors.New("empty CRI ContainerStatus response")
	}

	var out CRIContainerStatus
	var stateEnum uint64
	for len(statusBytes) > 0 {
		num, typ, n := protowire.ConsumeTag(statusBytes)
		if n < 0 {
			return CRIContainerStatus{}, protowire.ParseError(n)
		}
		statusBytes = statusBytes[n:]
		switch {
		case num == 1 && typ == protowire.BytesType:
			s, m := protowire.ConsumeString(statusBytes)
			if m < 0 {
				return CRIContainerStatus{}, protowire.ParseError(m)
			}
			out.ContainerID = s
			statusBytes = statusBytes[m:]
		case num == 2 && typ == protowire.BytesType:
			metaBytes, m := protowire.ConsumeBytes(statusBytes)
			if m < 0 {
				return CRIContainerStatus{}, protowire.ParseError(m)
			}
			if name := decodeContainerMetadataName(metaBytes); name != "" {
				out.ContainerName = name
			}
			statusBytes = statusBytes[m:]
		case num == 3 && typ == protowire.VarintType:
			v, m := protowire.ConsumeVarint(statusBytes)
			if m < 0 {
				return CRIContainerStatus{}, protowire.ParseError(m)
			}
			stateEnum = v
			statusBytes = statusBytes[m:]
		case num == 5 && typ == protowire.VarintType:
			v, m := protowire.ConsumeVarint(statusBytes)
			if m < 0 {
				return CRIContainerStatus{}, protowire.ParseError(m)
			}
			out.StartedAt = int64(v)
			statusBytes = statusBytes[m:]
		case num == 6 && typ == protowire.VarintType:
			v, m := protowire.ConsumeVarint(statusBytes)
			if m < 0 {
				return CRIContainerStatus{}, protowire.ParseError(m)
			}
			out.FinishedAt = int64(v)
			statusBytes = statusBytes[m:]
		case num == 7 && typ == protowire.VarintType:
			v, m := protowire.ConsumeVarint(statusBytes)
			if m < 0 {
				return CRIContainerStatus{}, protowire.ParseError(m)
			}
			out.ExitCode = int32(v)
			statusBytes = statusBytes[m:]
		case (num == 11 || num == 12) && typ == protowire.BytesType:
			entryBytes, m := protowire.ConsumeBytes(statusBytes)
			if m < 0 {
				return CRIContainerStatus{}, protowire.ParseError(m)
			}
			if k, v, ok := decodeProtoStringMapEntry(entryBytes); ok {
				switch k {
				case "io.kubernetes.pod.uid", "io.kubernetes.cri.sandbox-uid":
					if out.PodUID == "" && v != "" {
						out.PodUID = normalizePodUID(v)
					}
				case "io.kubernetes.container.name":
					if out.ContainerName == "" && v != "" {
						out.ContainerName = v
					}
				}
			}
			statusBytes = statusBytes[m:]
		default:
			m := protowire.ConsumeFieldValue(num, typ, statusBytes)
			if m < 0 {
				return CRIContainerStatus{}, protowire.ParseError(m)
			}
			statusBytes = statusBytes[m:]
		}
	}

	switch stateEnum {
	case 0:
		out.State = "CONTAINER_CREATED"
	case 1:
		out.State = "CONTAINER_RUNNING"
	case 2:
		out.State = "CONTAINER_EXITED"
		out.Exited = true
	default:
		out.State = "CONTAINER_UNKNOWN"
	}
	return out, nil
}

func isCRINotFoundError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, ErrContainerNotFound) {
		return true
	}
	if st, ok := status.FromError(err); ok && st.Code() == codes.NotFound {
		return true
	}
	lower := strings.ToLower(err.Error())
	return strings.Contains(lower, "not found") || strings.Contains(lower, "notfound")
}

func (s ContainerdCRISource) ContainerStatus(ctx context.Context, containerID string) (CRIContainerStatus, error) {
	cleanID := NormalizeContainerID(containerID)
	if cleanID == "" {
		return CRIContainerStatus{}, errors.New("empty container_id")
	}
	sock := strings.TrimSpace(s.SocketPath)
	if sock == "" {
		sock = "/run/k3s/containerd/containerd.sock"
	}
	conn, err := grpc.NewClient(
		"passthrough:///unix",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(dialCtx context.Context, _ string) (net.Conn, error) {
			var d net.Dialer
			return d.DialContext(dialCtx, "unix", sock)
		}),
	)
	if err != nil {
		return CRIContainerStatus{}, err
	}
	defer conn.Close()

	reqBytes := encodeContainerStatusRequest(cleanID)
	var respBytes []byte
	err = conn.Invoke(ctx, criContainerStatusMethod, reqBytes, &respBytes, grpc.ForceCodec(rawBytesCodec{}))
	if err != nil {
		if isCRINotFoundError(err) {
			return CRIContainerStatus{}, fmt.Errorf("%w: %v", ErrContainerNotFound, err)
		}
		return CRIContainerStatus{}, err
	}
	return decodeContainerStatusResponse(respBytes)
}

func NormalizeContainerID(raw string) string {
	s := strings.TrimSpace(raw)
	for _, prefix := range []string{"containerd://", "docker://", "cri-o://"} {
		s = strings.TrimPrefix(s, prefix)
	}
	return strings.TrimSpace(s)
}

func isHex64ContainerID(raw string) bool {
	s := strings.ToLower(NormalizeContainerID(raw))
	if len(s) != 64 {
		return false
	}
	for i := 0; i < 64; i++ {
		ch := s[i]
		if !((ch >= '0' && ch <= '9') || (ch >= 'a' && ch <= 'f')) {
			return false
		}
	}
	return true
}

func parsePodScopedContainerID(raw string) (embeddedPodUID, containerHint string, ok bool) {
	clean := strings.ToLower(NormalizeContainerID(raw))
	if !strings.HasPrefix(clean, "pod-") || len(clean) < 42 || clean[40] != '-' {
		return "", "", false
	}
	parsed, err := uuid.Parse(clean[4:40])
	if err != nil || parsed == uuid.Nil {
		return "", "", false
	}
	hint := strings.TrimSpace(clean[41:])
	if hint == "" || strings.Contains(hint, "/") {
		return "", "", false
	}
	return normalizePodUID(parsed.String()), hint, true
}

func (ts TombstoneStore) ResolveExitedContainerIDByPod(podUID, containerHint string) (string, bool) {
	if strings.TrimSpace(ts.Dir) == "" {
		return "", false
	}
	cleanPod := normalizePodUID(podUID)
	if cleanPod == "" {
		return "", false
	}
	entries, err := os.ReadDir(ts.Dir)
	if err != nil {
		return "", false
	}
	var exactMatch string
	var podMatches []string
	seen := map[string]bool{}
	for _, d := range entries {
		if d.IsDir() || !strings.HasSuffix(d.Name(), ".json") {
			continue
		}
		stem := strings.TrimSuffix(d.Name(), ".json")
		if !isHex64ContainerID(stem) {
			continue
		}
		st, ok, lErr := ts.Load(stem)
		if lErr != nil || !ok || normalizePodUID(st.PodUID) != cleanPod {
			continue
		}
		cid := strings.ToLower(NormalizeContainerID(st.ContainerID))
		if !isHex64ContainerID(cid) {
			continue
		}
		if containerHint != "" && strings.EqualFold(strings.TrimSpace(st.ContainerName), containerHint) {
			if exactMatch != "" && exactMatch != cid {
				return "", false
			}
			exactMatch = cid
		}
		if !seen[cid] {
			seen[cid] = true
			podMatches = append(podMatches, cid)
		}
	}
	if exactMatch != "" {
		return exactMatch, true
	}
	if len(podMatches) == 1 {
		return podMatches[0], true
	}
	return "", false
}

// CRIProxyHandler enforces the CP1 read-only CRI whitelist: ONLY
// GET /v1/containers/{id}/status is permitted; all other methods/routes fail 403.
// When Tombstones.Dir is set, exited container statuses (FinishedAt > 0) are durably
// fsynced so container GC after exit still returns cryptographic exit proof instead of silent loss.
type CRIProxyHandler struct {
	Source     ContainerStatusSource
	Tombstones TombstoneStore
}

func (h CRIProxyHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet || !strings.HasPrefix(r.URL.Path, "/v1/containers/") || !strings.HasSuffix(r.URL.Path, "/status") {
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		w.WriteHeader(http.StatusForbidden)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"error_code": "METHOD_NOT_ALLOWED_READ_ONLY_CRI_PROXY",
			"message":    "station-cri-status-proxy permits only GET /v1/containers/{id}/status",
		})
		return
	}
	trimmed := strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/v1/containers/"), "/status")
	containerID := NormalizeContainerID(trimmed)
	if containerID == "" || strings.Contains(containerID, "/") {
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		w.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(w).Encode(map[string]any{"error_code": "INVALID_CONTAINER_ID"})
		return
	}
	if h.Source == nil {
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		w.WriteHeader(http.StatusServiceUnavailable)
		_ = json.NewEncoder(w).Encode(map[string]any{"error_code": "UNKNOWN_NODE_STATE"})
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 4*time.Second)
	defer cancel()
	st, err := h.Source.ContainerStatus(ctx, containerID)
	if err != nil {
		if isCRINotFoundError(err) {
			tomb, ok, tErr := h.Tombstones.Load(containerID)
			if tErr != nil {
				w.Header().Set("Content-Type", "application/json; charset=utf-8")
				w.WriteHeader(http.StatusServiceUnavailable)
				_ = json.NewEncoder(w).Encode(map[string]any{
					"error_code": "TOMBSTONE_READ_FAILED",
					"message":    tErr.Error(),
				})
				return
			}
			if ok {
				w.Header().Set("Content-Type", "application/json; charset=utf-8")
				w.WriteHeader(http.StatusOK)
				_ = json.NewEncoder(w).Encode(tomb)
				return
			}
			w.Header().Set("Content-Type", "application/json; charset=utf-8")
			w.WriteHeader(http.StatusNotFound)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"error_code": "CRI_CONTAINER_NOT_FOUND",
				"message":    "container not found in CRI or durable tombstone store",
			})
			return
		}
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		w.WriteHeader(http.StatusServiceUnavailable)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"error_code": "CRI_STATUS_UNAVAILABLE",
			"message":    err.Error(),
		})
		return
	}
	if st.ContainerID == "" {
		st.ContainerID = containerID
	}
	if st.Exited && st.FinishedAt > 0 {
		_ = h.Tombstones.SaveExited(st)
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(st)
}

type HTTPCRIStatusClient struct {
	SocketPath string
	BaseURL    string
	Client     *http.Client
}

func (c HTTPCRIStatusClient) ContainerStatus(ctx context.Context, containerID string) (CRIContainerStatus, error) {
	cleanID := NormalizeContainerID(containerID)
	if cleanID == "" {
		return CRIContainerStatus{}, errors.New("empty container_id")
	}
	client := c.Client
	baseURL := strings.TrimRight(c.BaseURL, "/")
	if client == nil {
		if c.SocketPath != "" {
			sock := c.SocketPath
			client = &http.Client{
				Timeout: 4 * time.Second,
				CheckRedirect: func(*http.Request, []*http.Request) error {
					return http.ErrUseLastResponse
				},
				Transport: &http.Transport{
					DialContext: func(dialCtx context.Context, _, _ string) (net.Conn, error) {
						var d net.Dialer
						return d.DialContext(dialCtx, "unix", sock)
					},
				},
			}
			if baseURL == "" {
				baseURL = "http://unix"
			}
		} else {
			client = &http.Client{
				Timeout: 4 * time.Second,
				CheckRedirect: func(*http.Request, []*http.Request) error {
					return http.ErrUseLastResponse
				},
			}
		}
	}
	if baseURL == "" {
		return CRIContainerStatus{}, errors.New("missing CRI proxy endpoint")
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, fmt.Sprintf("%s/v1/containers/%s/status", baseURL, cleanID), nil)
	if err != nil {
		return CRIContainerStatus{}, err
	}
	resp, err := client.Do(req)
	if err != nil {
		return CRIContainerStatus{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		return CRIContainerStatus{}, ErrContainerNotFound
	}
	if resp.StatusCode != http.StatusOK {
		return CRIContainerStatus{}, fmt.Errorf("cri proxy status %d", resp.StatusCode)
	}
	var out CRIContainerStatus
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return CRIContainerStatus{}, err
	}
	return out, nil
}
