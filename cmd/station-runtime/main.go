package main

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	pb "github.com/verdantflarehub/verdantflare-station-runtime/api/runtimev1"
	"github.com/verdantflarehub/verdantflare-station-runtime/internal/executor"
	"google.golang.org/grpc"
)

func run() int {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	station := os.Getenv("STATION_ID")
	if _, e := uuid.Parse(station); e != nil {
		logger.Error("invalid_station_id")
		return 1
	}
	targets, err := executor.LoadRegistry(os.Getenv("STATION_TEMPLATE_ROOT"), os.Getenv("STATION_RUNTIME_REGISTRY"))
	if err != nil {
		logger.Error("invalid_runtime_registry")
		return 1
	}
	driver, err := executor.NewKubernetes(os.Getenv("STATION_KUBECONFIG"), os.Getenv("STATION_KUBE_CONTEXT"))
	if err != nil {
		logger.Error("invalid_kubernetes_config")
		return 1
	}
	cfg, err := pgxpool.ParseConfig(os.Getenv("STATION_DATABASE_URL"))
	if err != nil {
		logger.Error("invalid_database_config")
		return 1
	}
	cfg.MaxConns = 4
	cfg.ConnConfig.ConnectTimeout = 3 * time.Second
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		logger.Error("database_unavailable")
		return 1
	}
	defer pool.Close()
	ready := func(c context.Context) error {
		var id string
		var table bool
		e := pool.QueryRow(c, `SELECT station_id::text,to_regclass('station.runtime_app_executions') IS NOT NULL FROM station.configuration WHERE singleton`).Scan(&id, &table)
		if e != nil || id != station || !table {
			return errors.New("database not ready")
		}
		return nil
	}
	check, stop := context.WithTimeout(ctx, 5*time.Second)
	err = ready(check)
	stop()
	if err != nil {
		logger.Error("migration_or_station_not_ready")
		return 1
	}
	addr := os.Getenv("STATION_RUNTIME_ADDR")
	if addr == "" {
		addr = "127.0.0.1:5052"
	}
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		logger.Error("listen_failed")
		return 1
	}
	server := grpc.NewServer(grpc.MaxRecvMsgSize(16384))
	pb.RegisterAppRuntimeServer(server, &executor.Service{Pool: pool, StationID: station, Targets: targets, Driver: driver})
	probeAddr := os.Getenv("STATION_RUNTIME_PROBE_ADDR")
	if probeAddr == "" {
		probeAddr = "127.0.0.1:5053"
	}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(200) })
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, r *http.Request) {
		c, done := context.WithTimeout(r.Context(), 3*time.Second)
		defer done()
		if ready(c) != nil {
			http.Error(w, "not ready", 503)
			return
		}
		w.WriteHeader(200)
	})
	probe := &http.Server{Addr: probeAddr, Handler: mux, ReadHeaderTimeout: 3 * time.Second}
	done := make(chan error, 2)
	go func() { done <- server.Serve(lis) }()
	go func() { done <- probe.ListenAndServe() }()
	logger.Info("runtime_started", "station_id", station, "registered_apps", len(targets))
	result := 0
	select {
	case <-ctx.Done():
	case <-done:
		logger.Error("runtime_server_stopped")
		result = 1
	}
	server.Stop()
	shutdown, finish := context.WithTimeout(context.Background(), 5*time.Second)
	defer finish()
	_ = probe.Shutdown(shutdown)
	return result
}
func main() { os.Exit(run()) }
