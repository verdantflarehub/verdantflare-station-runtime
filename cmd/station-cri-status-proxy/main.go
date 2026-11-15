package main

import (
	"context"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/verdantflarehub/verdantflare-station-runtime/internal/nodegpu"
)

func run() int {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	criSocket := strings.TrimSpace(os.Getenv("STATION_CONTAINERD_SOCKET"))
	if criSocket == "" {
		criSocket = "/run/k3s/containerd/containerd.sock"
	}
	listenUDS := strings.TrimSpace(os.Getenv("STATION_CRI_PROXY_SOCKET"))
	listenTCP := strings.TrimSpace(os.Getenv("STATION_CRI_PROXY_ADDR"))
	if listenUDS == "" && listenTCP == "" {
		listenUDS = "/run/vf-cri-proxy/status.sock"
	}

	var lis net.Listener
	var err error
	if listenUDS != "" {
		if e := os.MkdirAll(filepath.Dir(listenUDS), 0o700); e != nil {
			logger.Error("cri_proxy_mkdir_failed")
			return 1
		}
		_ = os.Chmod(filepath.Dir(listenUDS), 0o700)
		_ = os.Remove(listenUDS)
		lis, err = net.Listen("unix", listenUDS)
		if err == nil {
			_ = os.Chmod(listenUDS, 0o600)
		}
	} else {
		lis, err = net.Listen("tcp", listenTCP)
	}
	if err != nil {
		logger.Error("cri_proxy_listen_failed")
		return 1
	}
	defer lis.Close()

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	srv := &http.Server{
		Handler: nodegpu.CRIProxyHandler{
			Source:     nodegpu.ContainerdCRISource{SocketPath: criSocket},
			Tombstones: nodegpu.TombstoneStore{Dir: strings.TrimSpace(os.Getenv("STATION_GPU_TOMBSTONE_DIR"))},
		},
		ReadHeaderTimeout: 3 * time.Second,
		ReadTimeout:       5 * time.Second,
		WriteTimeout:      5 * time.Second,
		IdleTimeout:       15 * time.Second,
		MaxHeaderBytes:    8 << 10,
	}
	done := make(chan error, 1)
	go func() { done <- srv.Serve(lis) }()

	logger.Info("cri_status_proxy_started")
	select {
	case <-ctx.Done():
	case <-done:
		logger.Error("cri_status_proxy_stopped")
		return 1
	}
	shutdownCtx, finish := context.WithTimeout(context.Background(), 3*time.Second)
	defer finish()
	_ = srv.Shutdown(shutdownCtx)
	return 0
}

func main() { os.Exit(run()) }
