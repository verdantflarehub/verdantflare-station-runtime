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
	// P1-H9: Resolve authoritative Node.metadata.uid from Kubernetes API whenever STATION_NODE_UID
	// is unset or equals spec.nodeName; fail closed instead of substituting nodeName.
	if nodeUID == "" || nodeUID == nodeName {
		kubeClient, kErr := nodegpu.NewKubeClient(os.Getenv("STATION_KUBECONFIG"))
		if kErr != nil || nodeName == "" {
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

	retentionHours := 168
	if v, err := strconv.Atoi(strings.TrimSpace(os.Getenv("COLLECTOR_S3_RETENTION_HOURS"))); err == nil && v >= 0 {
		retentionHours = v
	}

	var s3Cfg *nodegpu.S3PutOnlyConfig
	if ep := strings.TrimSpace(os.Getenv("COLLECTOR_S3_ENDPOINT")); ep != "" {
		s3Cfg = &nodegpu.S3PutOnlyConfig{
			EndpointURL:    ep,
			Bucket:         strings.TrimSpace(os.Getenv("COLLECTOR_S3_BUCKET")),
			Region:         strings.TrimSpace(os.Getenv("COLLECTOR_S3_REGION")),
			KeyPrefix:      strings.TrimSpace(os.Getenv("COLLECTOR_S3_PREFIX")),
			AccessKey:      strings.TrimSpace(os.Getenv("COLLECTOR_S3_ACCESS_KEY")),
			SecretKey:      strings.TrimSpace(os.Getenv("COLLECTOR_S3_SECRET_KEY")),
			RetentionHours: retentionHours,
		}
	}

	strictAudit := strings.EqualFold(strings.TrimSpace(os.Getenv("COLLECTOR_AUDIT_STRICT")), "true")
	collector := &nodegpu.Collector{
		NodeUID:             nodeUID,
		NodeName:            nodeName,
		SpoolDir:            strings.TrimSpace(os.Getenv("COLLECTOR_SPOOL_DIR")),
		StrictAudit:         strictAudit,
		ImageRef:            strings.TrimSpace(os.Getenv("COLLECTOR_IMAGE_REF")),
		ExpectedImageDigest: strings.TrimSpace(os.Getenv("COLLECTOR_EXPECTED_IMAGE_DIGEST")),
		NVML:                nodegpu.NvidiaSMINVMLProbe{Binary: os.Getenv("NVIDIA_SMI_PATH")},
		Proc:                nodegpu.ProcInspector{ProcRoot: os.Getenv("HOST_PROC_ROOT")},
		S3:                  s3Cfg,
	}
	if err := collector.ValidateConfig(); err != nil {
		logger.Error("invalid_collector_config")
		return 1
	}

	intervalSec := 5
	if v, err := strconv.Atoi(strings.TrimSpace(os.Getenv("COLLECTOR_INTERVAL_SECONDS"))); err == nil && v >= 1 && v <= 60 {
		intervalSec = v
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	if err := collector.RecoverAndReplay(ctx); err != nil {
		logger.Error("collector_recover_replay_failed")
		if strictAudit {
			return 1
		}
	}

	var probeSrv *http.Server
	probeDone := make(chan error, 1)
	if probeAddr := strings.TrimSpace(os.Getenv("COLLECTOR_PROBE_ADDR")); probeAddr != "" {
		lis, err := net.Listen("tcp", probeAddr)
		if err != nil {
			logger.Error("collector_probe_listen_failed")
			return 1
		}
		defer lis.Close()
		probeSrv = &http.Server{
			Handler:           collector,
			ReadHeaderTimeout: 3 * time.Second,
			ReadTimeout:       5 * time.Second,
			WriteTimeout:      5 * time.Second,
			IdleTimeout:       15 * time.Second,
			MaxHeaderBytes:    8 << 10,
		}
		go func() { probeDone <- probeSrv.Serve(lis) }()
	}

	logger.Info("station_gpu_collector_started", "node_uid", nodeUID)
	if _, err := collector.CaptureOnce(ctx); err != nil {
		logger.Error("collector_initial_sample_failed")
	}
	ticker := time.NewTicker(time.Duration(intervalSec) * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			if probeSrv != nil {
				sCtx, finish := context.WithTimeout(context.Background(), 3*time.Second)
				_ = probeSrv.Shutdown(sCtx)
				finish()
			}
			return 0
		case <-probeDone:
			logger.Error("collector_probe_server_stopped")
			return 1
		case <-ticker.C:
			if _, err := collector.CaptureOnce(ctx); err != nil {
				logger.Error("collector_sample_failed")
			}
		}
	}
}

func main() { os.Exit(run()) }
