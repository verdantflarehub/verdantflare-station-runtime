package main

import (
	"context"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/verdantflarehub/verdantflare-station-runtime/internal/nodegpu"
)

func run() int {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	nodeUID := strings.TrimSpace(os.Getenv("STATION_NODE_UID"))
	nodeName := strings.TrimSpace(os.Getenv("STATION_NODE_NAME"))
	bearer := strings.TrimSpace(os.Getenv("STATION_GPU_AGENT_TOKEN"))
	if len(bearer) < 32 {
		logger.Error("invalid_station_gpu_agent_token")
		return 1
	}
	addr := strings.TrimSpace(os.Getenv("STATION_GPU_AGENT_ADDR"))
	if addr == "" {
		addr = "0.0.0.0:5057"
	}

	tombstoneDir := strings.TrimSpace(os.Getenv("STATION_GPU_TOMBSTONE_DIR"))
	kubeClient, kubeErr := nodegpu.NewKubeClient(os.Getenv("STATION_KUBECONFIG"))
	var slotCounter nodegpu.K8sSlotCounter
	if kubeErr == nil {
		slotCounter = nodegpu.KubeNodeSlotCounter{
			Client:    kubeClient,
			Namespace: os.Getenv("STATION_APP_GPU_NAMESPACE"),
			Cache:     nodegpu.NewPodContainerCache(tombstoneDir),
		}
	}
	// P1-H9: Resolve authoritative Node.metadata.uid from Kubernetes API whenever STATION_NODE_UID
	// is unset or equals spec.nodeName; fail closed instead of falling back to nodeName.
	if nodeUID == "" || nodeUID == nodeName {
		if kubeErr != nil || nodeName == "" {
			logger.Error("missing_authoritative_station_node_uid")
			return 1
		}
		resolveCtx, resolveCancel := context.WithTimeout(context.Background(), 5*time.Second)
		resolvedUID, rErr := nodegpu.ResolveNodeUID(resolveCtx, kubeClient, nodeName)
		resolveCancel()
		if rErr != nil || resolvedUID == "" {
			logger.Error("resolve_node_uid_failed")
			return 1
		}
		nodeUID = resolvedUID
	}

	var criSource nodegpu.ContainerStatusSource
	if proxySock := strings.TrimSpace(os.Getenv("STATION_CRI_PROXY_SOCKET")); proxySock != "" {
		criSource = nodegpu.HTTPCRIStatusClient{SocketPath: proxySock}
	} else if proxyURL := strings.TrimSpace(os.Getenv("STATION_CRI_PROXY_URL")); proxyURL != "" {
		criSource = nodegpu.HTTPCRIStatusClient{BaseURL: proxyURL}
	}

	agent := &nodegpu.Agent{
		NodeUID:             nodeUID,
		NodeName:            nodeName,
		BearerToken:         bearer,
		RuntimeSchedulerURL: strings.TrimSpace(os.Getenv("STATION_RUNTIME_SCHEDULER_URL")),
		RuntimeToken:        strings.TrimSpace(os.Getenv("STATION_RUNTIME_SCHEDULER_TOKEN")),
		NVML:                nodegpu.NvidiaSMINVMLProbe{Binary: os.Getenv("NVIDIA_SMI_PATH")},
		Proc:                nodegpu.ProcInspector{ProcRoot: os.Getenv("HOST_PROC_ROOT")},
		CRI:                 criSource,
		SlotCounter:         slotCounter,
		TombstoneDir:        tombstoneDir,
	}
	if criSource == nil {
		logger.Error("missing_cri_status_source")
		return 1
	}
	if err := agent.EnsureStorageAndProcAccess(); err != nil {
		logger.Error("gpu_agent_storage_or_proc_access_failed")
		return 1
	}

	lis, err := net.Listen("tcp", addr)
	if err != nil {
		logger.Error("gpu_agent_listen_failed")
		return 1
	}
	defer lis.Close()

	intervalSec := 5
	if v, pErr := strconv.Atoi(strings.TrimSpace(os.Getenv("STATION_GPU_TELEMETRY_INTERVAL_SECONDS"))); pErr == nil && v >= 1 && v <= 60 {
		intervalSec = v
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	srv := &http.Server{
		Handler:           agent,
		ReadHeaderTimeout: 3 * time.Second,
		ReadTimeout:       8 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       30 * time.Second,
		MaxHeaderBytes:    16 << 10,
	}
	done := make(chan error, 1)
	go func() { done <- srv.Serve(lis) }()

	go func() {
		_ = agent.PushTelemetryOnce(ctx)
		ticker := time.NewTicker(time.Duration(intervalSec) * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				_ = agent.PushTelemetryOnce(ctx)
			}
		}
	}()

	logger.Info("station_gpu_agent_started", "node_uid", nodeUID)
	select {
	case <-ctx.Done():
	case <-done:
		logger.Error("station_gpu_agent_stopped")
		return 1
	}
	shutdownCtx, finish := context.WithTimeout(context.Background(), 4*time.Second)
	defer finish()
	_ = srv.Shutdown(shutdownCtx)
	return 0
}

func main() { os.Exit(run()) }
