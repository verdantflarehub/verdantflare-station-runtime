package nodegpu

import (
	"bytes"
	"context"
	"encoding/csv"
	"encoding/xml"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"time"
)

var ErrProcInspectionIncomplete = errors.New("proc inspection incomplete or permission denied")

type GPUDeviceObservation struct {
	GPUUUID             string `json:"gpu_uuid"`
	GPUIndex            int    `json:"gpu_index"`
	ModelName           string `json:"model_name"`
	TotalVRAMMiB        int64  `json:"total_vram_mib"`
	MeasuredFreeVRAMMiB int64  `json:"measured_free_vram_mib"`
	MeasuredUsedVRAMMiB int64  `json:"measured_used_vram_mib"`
	NVMLHealthy         bool   `json:"nvml_healthy"`
	ErrorReason         string `json:"error_reason,omitempty"`
}

type GPUProcessObservation struct {
	GPUUUID     string `json:"gpu_uuid"`
	PID         int    `json:"pid"`
	ProcessType string `json:"process_type,omitempty"` // "C", "G", or "C+G"
	UsedVRAMMiB int64  `json:"used_vram_mib"`
}

type NVMLSnapshot struct {
	SampledAt time.Time               `json:"sampled_at"`
	Devices   []GPUDeviceObservation  `json:"devices"`
	Processes []GPUProcessObservation `json:"processes"`
}

type NVMLProbe interface {
	Sample(ctx context.Context) (NVMLSnapshot, error)
}

type NvidiaSMINVMLProbe struct {
	Binary  string
	Timeout time.Duration
}

func (p NvidiaSMINVMLProbe) Sample(ctx context.Context) (NVMLSnapshot, error) {
	bin := strings.TrimSpace(p.Binary)
	if bin == "" {
		bin = "nvidia-smi"
	}
	timeout := p.Timeout
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	cctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	// Single atomic XML query captures per-GPU fb_memory_usage AND both Compute ("C")
	// and Graphics ("G" / "C+G") processes (e.g. Blender EEVEE/OpenGL/Vulkan).
	xmlOut, err := exec.CommandContext(cctx, bin, "-q", "-x").Output()
	if err != nil {
		return NVMLSnapshot{}, err
	}
	snap, err := parseNvidiaSMIXML(xmlOut)
	if err != nil {
		return NVMLSnapshot{}, err
	}
	snap.SampledAt = time.Now().UTC()
	return snap, nil
}

type nvidiaSMIXMLLog struct {
	XMLName xml.Name          `xml:"nvidia_smi_log"`
	GPUs    []nvidiaSMIXMLGPU `xml:"gpu"`
}

type nvidiaSMIXMLGPU struct {
	ID          string `xml:"id,attr"`
	ProductName string `xml:"product_name"`
	UUID        string `xml:"uuid"`
	MinorNumber string `xml:"minor_number"`
	FBMemory    struct {
		Total string `xml:"total"`
		Used  string `xml:"used"`
		Free  string `xml:"free"`
	} `xml:"fb_memory_usage"`
	Processes struct {
		Infos []struct {
			PID        string `xml:"pid"`
			Type       string `xml:"type"`
			UsedMemory string `xml:"used_memory"`
		} `xml:"process_info"`
	} `xml:"processes"`
}

func parseMiBValue(raw string) (int64, error) {
	s := strings.TrimSpace(raw)
	if s == "" || strings.EqualFold(s, "N/A") || strings.EqualFold(s, "[N/A]") {
		return 0, errors.New("unavailable MiB value")
	}
	fields := strings.Fields(s)
	if len(fields) == 0 {
		return 0, errors.New("empty MiB value")
	}
	val, err := strconv.ParseInt(fields[0], 10, 64)
	if err != nil {
		return 0, err
	}
	if len(fields) >= 2 {
		switch strings.ToLower(fields[1]) {
		case "gib", "gb":
			val *= 1024
		case "kib", "kb":
			val /= 1024
		}
	}
	return val, nil
}

func parseNvidiaSMIXML(raw []byte) (NVMLSnapshot, error) {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 {
		return NVMLSnapshot{}, errors.New("empty nvidia-smi XML output")
	}
	var doc nvidiaSMIXMLLog
	if err := xml.Unmarshal(trimmed, &doc); err != nil {
		return NVMLSnapshot{}, fmt.Errorf("parse nvidia-smi XML: %w", err)
	}
	if len(doc.GPUs) == 0 {
		return NVMLSnapshot{}, errors.New("nvidia-smi XML reported zero GPUs")
	}

	devices := make([]GPUDeviceObservation, 0, len(doc.GPUs))
	type procKey struct {
		gpuUUID string
		pid     int
	}
	procMap := map[procKey]GPUProcessObservation{}

	for idx, g := range doc.GPUs {
		gpuUUID := strings.TrimSpace(g.UUID)
		if gpuUUID == "" {
			return NVMLSnapshot{}, errors.New("nvidia-smi XML GPU missing uuid")
		}
		gpuIdx := idx
		if m, err := strconv.Atoi(strings.TrimSpace(g.MinorNumber)); err == nil && m >= 0 {
			gpuIdx = m
		}
		tot, err1 := parseMiBValue(g.FBMemory.Total)
		free, err2 := parseMiBValue(g.FBMemory.Free)
		used, err3 := parseMiBValue(g.FBMemory.Used)
		if err1 != nil || err2 != nil || err3 != nil || tot <= 0 {
			return NVMLSnapshot{}, errors.New("nvidia-smi XML malformed fb_memory_usage")
		}
		model := strings.TrimSpace(g.ProductName)
		if model == "" {
			model = "NVIDIA GeForce RTX 5090"
		}
		devices = append(devices, GPUDeviceObservation{
			GPUUUID:             gpuUUID,
			GPUIndex:            gpuIdx,
			ModelName:           model,
			TotalVRAMMiB:        tot,
			MeasuredFreeVRAMMiB: free,
			MeasuredUsedVRAMMiB: used,
			NVMLHealthy:         true,
		})

		for _, pi := range g.Processes.Infos {
			pid, err := strconv.Atoi(strings.TrimSpace(pi.PID))
			if err != nil || pid <= 0 {
				return NVMLSnapshot{}, errors.New("nvidia-smi XML malformed process pid")
			}
			var usedProc int64
			if v, uErr := parseMiBValue(pi.UsedMemory); uErr == nil && v > 0 {
				usedProc = v
			}
			pType := strings.ToUpper(strings.TrimSpace(pi.Type))
			if pType == "" {
				pType = "C"
			}
			key := procKey{gpuUUID: gpuUUID, pid: pid}
			if prev, exists := procMap[key]; exists {
				if usedProc > prev.UsedVRAMMiB {
					prev.UsedVRAMMiB = usedProc
				}
				if prev.ProcessType != pType && !strings.Contains(prev.ProcessType, pType) {
					prev.ProcessType = "C+G"
				}
				procMap[key] = prev
			} else {
				procMap[key] = GPUProcessObservation{
					GPUUUID:     gpuUUID,
					PID:         pid,
					ProcessType: pType,
					UsedVRAMMiB: usedProc,
				}
			}
		}
	}

	sort.Slice(devices, func(i, j int) bool { return devices[i].GPUUUID < devices[j].GPUUUID })
	procs := make([]GPUProcessObservation, 0, len(procMap))
	for _, p := range procMap {
		procs = append(procs, p)
	}
	sort.Slice(procs, func(i, j int) bool {
		if procs[i].GPUUUID != procs[j].GPUUUID {
			return procs[i].GPUUUID < procs[j].GPUUUID
		}
		return procs[i].PID < procs[j].PID
	})
	return NVMLSnapshot{
		Devices:   devices,
		Processes: procs,
	}, nil
}

func parseNvidiaSMIGPUCSV(raw []byte) ([]GPUDeviceObservation, error) {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 {
		return nil, errors.New("empty nvidia-smi GPU output")
	}
	r := csv.NewReader(bytes.NewReader(trimmed))
	r.FieldsPerRecord = -1
	rows, err := r.ReadAll()
	if err != nil {
		return nil, err
	}
	var out []GPUDeviceObservation
	for _, row := range rows {
		if len(row) < 6 {
			return nil, errors.New("malformed nvidia-smi GPU row")
		}
		idx, err1 := strconv.Atoi(strings.TrimSpace(row[1]))
		tot, err2 := strconv.ParseInt(strings.TrimSpace(row[3]), 10, 64)
		free, err3 := strconv.ParseInt(strings.TrimSpace(row[4]), 10, 64)
		used, err4 := strconv.ParseInt(strings.TrimSpace(row[5]), 10, 64)
		if err1 != nil || err2 != nil || err3 != nil || err4 != nil {
			return nil, errors.New("non-numeric nvidia-smi GPU memory field")
		}
		out = append(out, GPUDeviceObservation{
			GPUUUID:             strings.TrimSpace(row[0]),
			GPUIndex:            idx,
			ModelName:           strings.TrimSpace(row[2]),
			TotalVRAMMiB:        tot,
			MeasuredFreeVRAMMiB: free,
			MeasuredUsedVRAMMiB: used,
			NVMLHealthy:         true,
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].GPUUUID < out[j].GPUUUID })
	return out, nil
}

func parseNvidiaSMIProcsCSV(raw []byte) ([]GPUProcessObservation, error) {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 {
		return []GPUProcessObservation{}, nil
	}
	r := csv.NewReader(bytes.NewReader(trimmed))
	r.FieldsPerRecord = -1
	rows, err := r.ReadAll()
	if err != nil {
		return nil, err
	}
	var out []GPUProcessObservation
	for _, row := range rows {
		if len(row) == 0 || strings.TrimSpace(row[0]) == "" {
			continue
		}
		if len(row) < 3 {
			return nil, errors.New("malformed nvidia-smi compute-apps row")
		}
		pid, err := strconv.Atoi(strings.TrimSpace(row[1]))
		if err != nil {
			return nil, err
		}
		usedStr := strings.TrimSpace(row[2])
		var used int64
		if usedStr != "" && !strings.EqualFold(usedStr, "[N/A]") && !strings.EqualFold(usedStr, "N/A") {
			used, _ = strconv.ParseInt(usedStr, 10, 64)
		}
		out = append(out, GPUProcessObservation{
			GPUUUID:     strings.TrimSpace(row[0]),
			PID:         pid,
			ProcessType: "C",
			UsedVRAMMiB: used,
		})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].GPUUUID != out[j].GPUUUID {
			return out[i].GPUUUID < out[j].GPUUUID
		}
		return out[i].PID < out[j].PID
	})
	return out, nil
}

var (
	podUIDCgroupRe      = regexp.MustCompile(`pod([0-9a-fA-F]{8}[-_][0-9a-fA-F]{4}[-_][0-9a-fA-F]{4}[-_][0-9a-fA-F]{4}[-_][0-9a-fA-F]{12})`)
	containerIDCgroupRe = regexp.MustCompile(`(?:cri-containerd-|docker-|crio-|/)([0-9a-fA-F]{64})(?:\.scope|$|/)`)
	gpuDeviceUUIDRe     = regexp.MustCompile(`^GPU-[0-9a-fA-F-]{36}$`)
)

type ProcEntry struct {
	PID             int      `json:"pid"`
	PPID            int      `json:"ppid"`
	StarttimeTicks  uint64   `json:"starttime_ticks"`
	State           string   `json:"state"`
	Comm            string   `json:"comm"`
	Cmdline         string   `json:"cmdline"`
	PodUID          string   `json:"pod_uid,omitempty"`
	ContainerID     string   `json:"container_id,omitempty"`
	NSPIDs          []int    `json:"ns_pids,omitempty"`
	PIDNamespace    string   `json:"pid_namespace,omitempty"`
	HasNvidiaFD     bool     `json:"has_nvidia_fd,omitempty"`
	GPUEpochEnv     *int64   `json:"gpu_epoch_env,omitempty"`
	BootIDEnv       string   `json:"boot_id_env,omitempty"`
	VisibleGPUUUIDs []string `json:"visible_gpu_uuids,omitempty"`
}

func (p ProcEntry) InnermostPID() int {
	if len(p.NSPIDs) == 0 {
		return 0
	}
	return p.NSPIDs[len(p.NSPIDs)-1]
}

type ProcInspector struct {
	ProcRoot string
}

func normalizePodUID(uid string) string {
	return strings.ToLower(strings.ReplaceAll(strings.TrimSpace(uid), "_", "-"))
}

func parseProcStat(raw string) (comm, state string, ppid int, starttimeTicks uint64, ok bool) {
	openIdx := strings.IndexByte(raw, '(')
	closeIdx := strings.LastIndexByte(raw, ')')
	if openIdx < 0 || closeIdx <= openIdx || closeIdx+2 >= len(raw) {
		return "", "", 0, 0, false
	}
	comm = raw[openIdx+1 : closeIdx]
	rest := strings.Fields(raw[closeIdx+2:])
	if len(rest) < 2 {
		return "", "", 0, 0, false
	}
	state = rest[0]
	p, err := strconv.Atoi(rest[1])
	if err != nil {
		return "", "", 0, 0, false
	}
	// Field 22 in /proc/<pid>/stat is starttime (ticks since boot), located at rest[19].
	if len(rest) >= 20 {
		if st, sErr := strconv.ParseUint(rest[19], 10, 64); sErr == nil {
			starttimeTicks = st
		}
	}
	return comm, state, p, starttimeTicks, true
}

func extractCgroupIdentity(cgroupContent string) (podUID, containerID string) {
	if m := podUIDCgroupRe.FindStringSubmatch(cgroupContent); len(m) == 2 {
		podUID = normalizePodUID(m[1])
	}
	if m := containerIDCgroupRe.FindStringSubmatch(cgroupContent); len(m) == 2 {
		containerID = strings.ToLower(m[1])
	}
	return podUID, containerID
}

func parseProcEnviron(raw []byte) (gpuEpoch *int64, bootID string, visibleGPUs []string) {
	if len(raw) == 0 {
		return nil, "", nil
	}
	seenGPU := map[string]bool{}
	addVisibleGPU := func(val string) {
		for _, tok := range strings.Split(val, ",") {
			cand := strings.TrimSpace(tok)
			if sep := strings.Index(cand, "::"); sep > 0 {
				cand = strings.TrimSpace(cand[:sep])
			}
			if gpuDeviceUUIDRe.MatchString(cand) && !seenGPU[cand] {
				seenGPU[cand] = true
				visibleGPUs = append(visibleGPUs, cand)
			}
		}
	}
	for _, entry := range bytes.Split(raw, []byte{0}) {
		kv := string(entry)
		eq := strings.IndexByte(kv, '=')
		if eq <= 0 {
			continue
		}
		k := strings.TrimSpace(kv[:eq])
		v := strings.TrimSpace(kv[eq+1:])
		switch k {
		case "VF_GPU_EPOCH", "BLENDER_GPU_EPOCH", "COMFYUI_GPU_EPOCH":
			if parsed, err := strconv.ParseInt(v, 10, 64); err == nil && parsed >= 0 {
				ep := parsed
				gpuEpoch = &ep
			}
		case "VF_BOOT_ID", "COMFYUI_SUPERVISOR_BOOT_ID", "BLENDER_INSTANCE_BOOT_ID":
			if v != "" && bootID == "" {
				bootID = v
			}
		case "NVIDIA_VISIBLE_DEVICES", "VF_VISIBLE_GPU_UUIDS", "COMFYUI_GPU_UUID", "BLENDER_GPU_UUID":
			if v != "" && !strings.EqualFold(v, "all") && !strings.EqualFold(v, "none") && !strings.EqualFold(v, "void") {
				addVisibleGPU(v)
			}
		}
	}
	return gpuEpoch, bootID, visibleGPUs
}

func parseProcStatusNSPIDs(raw string) []int {
	for _, line := range strings.Split(raw, "\n") {
		trimmed := strings.TrimSpace(line)
		if !strings.HasPrefix(trimmed, "NSpid:") {
			continue
		}
		fields := strings.Fields(strings.TrimPrefix(trimmed, "NSpid:"))
		out := make([]int, 0, len(fields))
		for _, f := range fields {
			v, err := strconv.Atoi(f)
			if err != nil || v <= 0 {
				return nil
			}
			out = append(out, v)
		}
		return out
	}
	return nil
}

func readPIDNamespace(pidDir string, strictPerm bool) (string, error) {
	nsPath := filepath.Join(pidDir, "ns", "pid")
	target, err := os.Readlink(nsPath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return "", nil
		}
		if strictPerm && errors.Is(err, os.ErrPermission) {
			return "", fmt.Errorf("%w: ns/pid: %v", ErrProcInspectionIncomplete, err)
		}
		if runtime.GOOS == "windows" {
			if raw, rErr := os.ReadFile(nsPath); rErr == nil {
				return strings.TrimSpace(string(raw)), nil
			}
		}
		return "", nil
	}
	return strings.TrimSpace(target), nil
}

func inspectNvidiaOpenFDs(pidDir string, strictPerm bool) (bool, error) {
	fdDir := filepath.Join(pidDir, "fd")
	entries, err := os.ReadDir(fdDir)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return false, nil
		}
		if strictPerm && errors.Is(err, os.ErrPermission) {
			return false, fmt.Errorf("%w: %v", ErrProcInspectionIncomplete, err)
		}
		return false, nil
	}
	for _, d := range entries {
		entryPath := filepath.Join(fdDir, d.Name())
		target, lErr := os.Readlink(entryPath)
		if lErr != nil {
			if strictPerm && errors.Is(lErr, os.ErrPermission) {
				return false, fmt.Errorf("%w: %v", ErrProcInspectionIncomplete, lErr)
			}
			if runtime.GOOS == "windows" && d.Type().IsRegular() {
				if raw, rErr := os.ReadFile(entryPath); rErr == nil {
					target = string(raw)
				} else {
					continue
				}
			} else {
				continue
			}
		}
		cleanTarget := filepath.ToSlash(strings.TrimSpace(target))
		if strings.HasPrefix(cleanTarget, "/dev/nvidia") {
			return true, nil
		}
	}
	return false, nil
}

func (p ProcInspector) ScanProcesses() ([]ProcEntry, error) {
	return p.scan(nil, false)
}

// VerifyReadAccess checks at startup and on demand that /proc/<pid>/{environ,fd}
// are readable across host/container PIDs (requiring CAP_SYS_PTRACE + CAP_DAC_READ_SEARCH
// under non-privileged hostPID securityContext), failing closed on os.ErrPermission.
func (p ProcInspector) VerifyReadAccess() error {
	root := strings.TrimSpace(p.ProcRoot)
	if root == "" {
		root = "/proc"
	}
	entries, err := os.ReadDir(root)
	if err != nil {
		return fmt.Errorf("%w: read %s: %v", ErrProcInspectionIncomplete, root, err)
	}
	pid1Dir := filepath.Join(root, "1")
	if st, sErr := os.Stat(filepath.Join(pid1Dir, "stat")); sErr == nil && !st.IsDir() {
		if _, envErr := os.ReadFile(filepath.Join(pid1Dir, "environ")); envErr != nil && errors.Is(envErr, os.ErrPermission) {
			return fmt.Errorf("%w: host pid 1 environ read denied: %v", ErrProcInspectionIncomplete, envErr)
		}
		if _, fdErr := inspectNvidiaOpenFDs(pid1Dir, true); fdErr != nil {
			return fdErr
		}
	}
	for _, d := range entries {
		if !d.IsDir() {
			continue
		}
		pid, pErr := strconv.Atoi(d.Name())
		if pErr != nil || pid <= 0 {
			continue
		}
		pidDir := filepath.Join(root, d.Name())
		cgBytes, cgErr := os.ReadFile(filepath.Join(pidDir, "cgroup"))
		if cgErr != nil {
			if errors.Is(cgErr, os.ErrPermission) {
				return fmt.Errorf("%w: cgroup pid %d: %v", ErrProcInspectionIncomplete, pid, cgErr)
			}
			continue
		}
		podUID, containerID := extractCgroupIdentity(string(cgBytes))
		if podUID == "" && containerID == "" {
			continue
		}
		if _, envErr := os.ReadFile(filepath.Join(pidDir, "environ")); envErr != nil && errors.Is(envErr, os.ErrPermission) {
			return fmt.Errorf("%w: environ pid %d: %v", ErrProcInspectionIncomplete, pid, envErr)
		}
		if _, stErr := os.ReadFile(filepath.Join(pidDir, "status")); stErr != nil && errors.Is(stErr, os.ErrPermission) {
			return fmt.Errorf("%w: status pid %d: %v", ErrProcInspectionIncomplete, pid, stErr)
		}
		if _, nsErr := readPIDNamespace(pidDir, true); nsErr != nil {
			return nsErr
		}
		if _, fdErr := inspectNvidiaOpenFDs(pidDir, true); fdErr != nil {
			return fdErr
		}
	}
	return nil
}

// ScanProcessesStrict enforces fail-closed semantics: any permission error reading
// /proc/<pid>/{stat,cgroup,environ,fd} or any missing required GPU PID returns ErrProcInspectionIncomplete.
func (p ProcInspector) ScanProcessesStrict(requiredGPUPIDs []int) ([]ProcEntry, error) {
	return p.scan(requiredGPUPIDs, true)
}

func (p ProcInspector) scan(requiredGPUPIDs []int, strict bool) ([]ProcEntry, error) {
	root := strings.TrimSpace(p.ProcRoot)
	if root == "" {
		root = "/proc"
	}
	entries, err := os.ReadDir(root)
	if err != nil {
		return nil, err
	}
	requiredSet := make(map[int]bool, len(requiredGPUPIDs))
	for _, pid := range requiredGPUPIDs {
		if pid > 0 {
			requiredSet[pid] = true
		}
	}

	var out []ProcEntry
	seenPIDs := make(map[int]bool, len(entries))
	for _, d := range entries {
		if !d.IsDir() {
			continue
		}
		pid, err := strconv.Atoi(d.Name())
		if err != nil || pid <= 0 {
			continue
		}
		pidDir := filepath.Join(root, d.Name())
		statBytes, err := os.ReadFile(filepath.Join(pidDir, "stat"))
		if err != nil {
			if strict && errors.Is(err, os.ErrPermission) {
				return nil, fmt.Errorf("%w: stat pid %d: %v", ErrProcInspectionIncomplete, pid, err)
			}
			continue
		}
		comm, state, ppid, startTicks, ok := parseProcStat(string(statBytes))
		if !ok {
			if strict && requiredSet[pid] {
				return nil, fmt.Errorf("%w: malformed stat pid %d", ErrProcInspectionIncomplete, pid)
			}
			continue
		}
		cgBytes, cgErr := os.ReadFile(filepath.Join(pidDir, "cgroup"))
		if cgErr != nil {
			if strict && errors.Is(cgErr, os.ErrPermission) {
				return nil, fmt.Errorf("%w: cgroup pid %d: %v", ErrProcInspectionIncomplete, pid, cgErr)
			}
			if strict && requiredSet[pid] && errors.Is(cgErr, os.ErrNotExist) {
				return nil, fmt.Errorf("%w: gpu pid %d vanished mid-scan", ErrProcInspectionIncomplete, pid)
			}
		}
		podUID, containerID := extractCgroupIdentity(string(cgBytes))

		cmdBytes, cmdErr := os.ReadFile(filepath.Join(pidDir, "cmdline"))
		if cmdErr != nil && strict && errors.Is(cmdErr, os.ErrPermission) {
			return nil, fmt.Errorf("%w: cmdline pid %d: %v", ErrProcInspectionIncomplete, pid, cmdErr)
		}
		cmdline := strings.TrimSpace(strings.ReplaceAll(string(cmdBytes), "\x00", " "))

		isTargetProc := podUID != "" || containerID != "" || requiredSet[pid]

		var nsPIDs []int
		statusBytes, statusErr := os.ReadFile(filepath.Join(pidDir, "status"))
		if statusErr != nil {
			if strict && errors.Is(statusErr, os.ErrPermission) && isTargetProc {
				return nil, fmt.Errorf("%w: status pid %d: %v", ErrProcInspectionIncomplete, pid, statusErr)
			}
		} else {
			nsPIDs = parseProcStatusNSPIDs(string(statusBytes))
		}

		pidNamespace, nsErr := readPIDNamespace(pidDir, strict && isTargetProc)
		if nsErr != nil {
			return nil, nsErr
		}

		var gpuEpoch *int64
		var bootID string
		var visibleGPUs []string
		envBytes, envErr := os.ReadFile(filepath.Join(pidDir, "environ"))
		if envErr != nil {
			if strict && errors.Is(envErr, os.ErrPermission) && isTargetProc {
				return nil, fmt.Errorf("%w: environ pid %d: %v", ErrProcInspectionIncomplete, pid, envErr)
			}
		} else {
			gpuEpoch, bootID, visibleGPUs = parseProcEnviron(envBytes)
		}

		hasNvidiaFD, fdErr := inspectNvidiaOpenFDs(pidDir, strict && isTargetProc)
		if fdErr != nil {
			return nil, fdErr
		}

		seenPIDs[pid] = true
		out = append(out, ProcEntry{
			PID:             pid,
			PPID:            ppid,
			StarttimeTicks:  startTicks,
			State:           state,
			Comm:            comm,
			Cmdline:         cmdline,
			PodUID:          podUID,
			ContainerID:     containerID,
			NSPIDs:          nsPIDs,
			PIDNamespace:    pidNamespace,
			HasNvidiaFD:     hasNvidiaFD,
			GPUEpochEnv:     gpuEpoch,
			BootIDEnv:       bootID,
			VisibleGPUUUIDs: visibleGPUs,
		})
	}
	if strict {
		for reqPID := range requiredSet {
			if !seenPIDs[reqPID] {
				return nil, fmt.Errorf("%w: nvml pid %d missing from /proc scan", ErrProcInspectionIncomplete, reqPID)
			}
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].PID < out[j].PID })
	return out, nil
}
