package main

import (
	"context"
	"crypto/sha256"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"slices"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	pb "github.com/verdantflarehub/verdantflare-station-runtime/api/runtimev1"
	"github.com/verdantflarehub/verdantflare-station-runtime/internal/appinstances"
	"github.com/verdantflarehub/verdantflare-station-runtime/internal/appstate"
	"github.com/verdantflarehub/verdantflare-station-runtime/internal/computepolicy"
	"github.com/verdantflarehub/verdantflare-station-runtime/internal/executor"
	"github.com/verdantflarehub/verdantflare-station-runtime/internal/instances"
	"github.com/verdantflarehub/verdantflare-station-runtime/internal/modelcache"
	"github.com/verdantflarehub/verdantflare-station-runtime/internal/releasebinding"
	"github.com/verdantflarehub/verdantflare-station-runtime/internal/scheduler"
	"google.golang.org/grpc"
)

// Dashboard-only transitions have no HTTP maintenance registry.
// Their control credential must still remain separate from instance callers.
func transitionCredentialsDistinct(control, instance string, maintenance *executor.HTTPManagementMaintenance, callers appinstances.AccessRegistry) bool {
	credentials := []string{}
	if control != "" {
		credentials = append(credentials, control)
	}
	if maintenance != nil {
		for _, release := range maintenance.Releases {
			if release.Token != "" {
				credentials = append(credentials, release.Token)
			}
		}
	}
	for _, credential := range credentials {
		if _, reused := callers[sha256.Sum256([]byte("Bearer "+credential))]; reused || credential == instance {
			return false
		}
	}
	return true
}

func selectCleanerTarget(profiles map[string]instances.HelperProfile, appProfiles map[string]appinstances.Profile, namespace, pvcName string) (string, int64, bool) {
	for _, p := range profiles {
		if p.Namespace == namespace && slices.Contains(p.Claims, pvcName) {
			return p.Image, 1000, true
		}
	}
	for _, p := range appProfiles {
		if p.Namespace == namespace && slices.Contains(p.Claims, pvcName) {
			return p.Image, p.RunAsUser, true
		}
	}
	pvcLower := strings.ToLower(pvcName)
	if strings.HasPrefix(pvcLower, "comfyui") {
		for _, p := range appProfiles {
			if p.Namespace == namespace {
				return p.Image, p.RunAsUser, true
			}
		}
	}
	if strings.HasPrefix(pvcLower, "blender") {
		for _, p := range profiles {
			if p.Namespace == namespace {
				return p.Image, 1000, true
			}
		}
	}
	for _, p := range profiles {
		if p.Namespace == namespace {
			return p.Image, 1000, true
		}
	}
	for _, p := range appProfiles {
		if p.Namespace == namespace {
			return p.Image, p.RunAsUser, true
		}
	}
	return "", 0, false
}

func mergeStateNamespaces(existing []string, rawEnv string) []string {
	seen := map[string]bool{}
	var out []string
	add := func(ns string) {
		ns = strings.TrimSpace(ns)
		if ns == "" || seen[ns] {
			return
		}
		seen[ns] = true
		out = append(out, ns)
	}
	for _, ns := range existing {
		add(ns)
	}
	for _, part := range strings.Split(rawEnv, ",") {
		add(part)
	}
	slices.Sort(out)
	return out
}

func multiplexModelCacheHandler(primary http.Handler, modelHandler http.Handler) http.Handler {
	if modelHandler == nil {
		return primary
	}
	if primary == nil {
		return modelHandler
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, "/internal/v1/model-cache/") || r.URL.Path == "/internal/v1/model-cache" {
			modelHandler.ServeHTTP(w, r)
			return
		}
		primary.ServeHTTP(w, r)
	})
}

func run() int {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	if !computepolicy.Valid() {
		logger.Error("invalid_cpu_memory_policy")
		return 1
	}
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
	stateConfig, err := appstate.LoadConfig(os.Getenv("STATION_APP_STATE_OBSERVER_CONFIG"), os.Getenv, []string{os.Getenv("STATION_APP_TRANSITION_CONTROL_TOKEN"), os.Getenv("STATION_INSTANCE_CONTROL_TOKEN")})
	if err != nil {
		logger.Error("invalid_app_state_observer_configuration")
		return 1
	}
	var stateObserver *appstate.Observer
	if stateConfig != nil {
		if extraNS := os.Getenv("STATION_APP_STATE_NAMESPACES"); extraNS != "" {
			stateConfig.Namespaces = mergeStateNamespaces(stateConfig.Namespaces, extraNS)
		}
		watchClient, e := appstate.NewKubernetes(os.Getenv("STATION_KUBECONFIG"), os.Getenv("STATION_KUBE_CONTEXT"))
		if e != nil {
			logger.Error("invalid_app_state_watch_configuration")
			return 1
		}
		stateObserver = &appstate.Observer{Client: watchClient, Core: stateConfig.Core, Namespaces: stateConfig.Namespaces, HealthTargets: stateConfig.HealthTargets}
	}
	transitionToken := os.Getenv("STATION_APP_TRANSITION_CONTROL_TOKEN")
	maintenance, err := executor.LoadManagementMaintenance(os.Getenv("STATION_APP_TRANSITION_MAINTENANCE_REGISTRY"), transitionToken, os.Getenv, driver, targets)
	if err != nil || (transitionToken != "" && transitionToken == os.Getenv("STATION_INSTANCE_CONTROL_TOKEN")) {
		logger.Error("invalid_transition_maintenance_configuration")
		return 1
	}
	var profiles map[string]instances.HelperProfile
	if path := os.Getenv("STATION_INSTANCE_REGISTRY"); path != "" {
		profiles, err = instances.LoadProfiles(path)
		if err != nil || len(os.Getenv("STATION_INSTANCE_CONTROL_TOKEN")) < 32 {
			logger.Error("invalid_instance_configuration")
			return 1
		}
	}
	var profileHistory map[string]instances.HelperProfile
	if path := os.Getenv("STATION_INSTANCE_HISTORY_REGISTRY"); path != "" {
		profileHistory, err = instances.LoadProfileHistory(path)
		if err != nil {
			logger.Error("invalid_instance_history_configuration")
			return 1
		}
	}
	var capacityClaims instances.CapacityClaims
	if path := os.Getenv("STATION_INSTANCE_CAPACITY_REGISTRY"); path != "" {
		capacityClaims, err = instances.LoadCapacityClaims(path, profiles, profileHistory)
		if err != nil {
			logger.Error("invalid_instance_capacity_configuration")
			return 1
		}
	}
	var appProfiles map[string]appinstances.Profile
	var appCallers appinstances.AccessRegistry
	appProfilePath, appAccessPath := os.Getenv("STATION_APP_INSTANCE_PROFILES"), os.Getenv("STATION_APP_INSTANCE_ACCESS_REGISTRY")
	if appProfilePath != "" || appAccessPath != "" || os.Getenv("STATION_APP_INSTANCE_CORE_ORIGIN") != "" || os.Getenv("STATION_APP_INSTANCE_ADDR") != "" {
		load := func() error {
			f, e := os.Open(appProfilePath)
			if e != nil {
				return e
			}
			defer f.Close()
			appProfiles, e = appinstances.LoadProfiles(f)
			if e != nil {
				return e
			}
			a, e := os.Open(appAccessPath)
			if e != nil {
				return e
			}
			defer a.Close()
			appCallers, e = appinstances.LoadAccessRegistry(a, os.Getenv, os.Getenv("STATION_APP_INSTANCE_CORE_ORIGIN"))
			if e != nil {
				return e
			}
			apps := map[string]bool{}
			for _, caller := range appCallers {
				apps[caller.AppID] = false
			}
			for _, p := range appProfiles {
				if _, ok := apps[p.AppID]; !ok {
					return errors.New("profile has no registered App caller")
				}
				apps[p.AppID] = true
			}
			for _, exists := range apps {
				if !exists {
					return errors.New("App caller has no registered profile")
				}
			}
			return nil
		}
		if err = load(); err != nil {
			logger.Error("invalid_app_instance_configuration")
			return 1
		}
	}
	if !transitionCredentialsDistinct(transitionToken, os.Getenv("STATION_INSTANCE_CONTROL_TOKEN"), maintenance, appCallers) {
		logger.Error("transition_credential_reused")
		return 1
	}
	var gpuCallers scheduler.CallerRegistry
	var gpuSvc *scheduler.Service
	var gpuPool *pgxpool.Pool
	gpuCallersPath := strings.TrimSpace(os.Getenv("STATION_GPU_SCHEDULER_CALLERS"))
	gpuAddrEnv := strings.TrimSpace(os.Getenv("STATION_GPU_SCHEDULER_ADDR"))
	if gpuCallersPath != "" || gpuAddrEnv != "" {
		f, e := os.Open(gpuCallersPath)
		if e != nil {
			logger.Error("invalid_gpu_scheduler_callers")
			return 1
		}
		gpuCallers, e = scheduler.LoadCallerRegistry(f, os.Getenv)
		_ = f.Close()
		if e != nil {
			logger.Error("invalid_gpu_scheduler_callers")
			return 1
		}
		schedPool := pool
		if gpuDSN := strings.TrimSpace(os.Getenv("STATION_GPU_DATABASE_URL")); gpuDSN != "" {
			gCfg, pErr := pgxpool.ParseConfig(gpuDSN)
			if pErr != nil {
				logger.Error("invalid_gpu_database_config")
				return 1
			}
			gCfg.MaxConns = 4
			gCfg.ConnConfig.ConnectTimeout = 3 * time.Second
			gpuPool, pErr = pgxpool.NewWithConfig(ctx, gCfg)
			if pErr != nil {
				logger.Error("gpu_database_unavailable")
				return 1
			}
			defer gpuPool.Close()
			schedPool = gpuPool
		}
		quota := scheduler.DefaultAppGPUQuota
		if q, pErr := strconv.Atoi(strings.TrimSpace(os.Getenv("STATION_APP_GPU_QUOTA"))); pErr == nil && q > 0 {
			quota = q
		}
		agentURL := strings.TrimSpace(os.Getenv("STATION_GPU_AGENT_DEFAULT_URL"))
		agentToken := strings.TrimSpace(os.Getenv("STATION_GPU_AGENT_TOKEN"))
		if agentURL == "" || len(agentToken) < 32 {
			logger.Error("invalid_gpu_agent_verifier_configuration")
			return 1
		}
		verifier := scheduler.HTTPNodeVerifier{
			DefaultURL:  agentURL,
			BearerToken: agentToken,
		}
		var instanceDir scheduler.InstanceDirectory
		var tokenResolver func(context.Context, string) (string, error)
		if !strings.EqualFold(strings.TrimSpace(os.Getenv("STATION_GPU_VERIFY_INSTANCE_DIRECTORY")), "false") {
			appPortMap := map[string]int{
				"comfyui":     8188,
				"app-comfyui": 8188,
				"blender":     48084,
				"app-blender": 48084,
			}
			for _, p := range appProfiles {
				if p.AppID != "" && p.Port > 0 {
					appPortMap[p.AppID] = int(p.Port)
				}
			}
			instanceDir = &scheduler.PGInstanceDirectory{
				Pool:     schedPool,
				AppPorts: appPortMap,
			}
			if driver.Client != nil {
				tokenResolver = (scheduler.KubeInstanceTokenResolver{
					Directory: instanceDir,
					StationID: station,
					Kube:      driver.Client,
				}).ResolveToken
			}
		}
		gpuSvc = &scheduler.Service{
			Pool:             schedPool,
			StationID:        station,
			Callers:          gpuCallers,
			AppGPUQuota:      quota,
			CutoverCompleted: strings.EqualFold(strings.TrimSpace(os.Getenv("STATION_GPU_CUTOVER_COMPLETED")), "true"),
			InstanceDir:      instanceDir,
			Verifier:         verifier,
			Evictor: scheduler.HTTPResidencyEvictor{
				BearerToken:   strings.TrimSpace(os.Getenv("STATION_GPU_EVICTOR_TOKEN")),
				TokenResolver: tokenResolver,
			},
		}
	}
	var modelCacheSvc *modelcache.Service
	var initialCatalogEntries []modelcache.CatalogEntry
	modelCacheRoot := strings.TrimSpace(os.Getenv("STATION_MODEL_CACHE_ROOT"))
	if modelCacheRoot != "" {
		var allowedHosts []string
		if rawHosts := strings.TrimSpace(os.Getenv("STATION_MODEL_ALLOWED_HOSTS")); rawHosts != "" {
			for _, h := range strings.Split(rawHosts, ",") {
				if trimmed := strings.TrimSpace(h); trimmed != "" {
					allowedHosts = append(allowedHosts, trimmed)
				}
			}
		}
		safeDL, dlErr := modelcache.NewSafeDownloader(allowedHosts, os.Getenv("STATION_MODEL_EGRESS_PROXY_URL"))
		if dlErr != nil {
			logger.Error("invalid_model_cache_downloader_configuration")
			return 1
		}
		if err = modelcache.EnsureLayout(modelCacheRoot); err != nil {
			logger.Error("invalid_model_cache_root_layout")
			return 1
		}
		if catFile := strings.TrimSpace(os.Getenv("STATION_MODEL_CATALOG_FILE")); catFile != "" {
			rawCat, rErr := os.ReadFile(catFile)
			if rErr != nil {
				logger.Error("invalid_model_catalog_file")
				return 1
			}
			initialCatalogEntries, rErr = modelcache.LoadCatalogYAML(rawCat)
			if rErr != nil {
				logger.Error("invalid_model_catalog_yaml")
				return 1
			}
		}
		var revoker modelcache.AppAdmissionRevoker
		reconcileToken := strings.TrimSpace(os.Getenv("STATION_MODEL_RECONCILE_TOKEN"))
		if reconcileToken == "" {
			reconcileToken = strings.TrimSpace(os.Getenv("STATION_APP_INSTANCE_COMFYUI_TOKEN"))
		}
		if reconcileToken == "" {
			reconcileToken = strings.TrimSpace(os.Getenv("STATION_GPU_EVICTOR_TOKEN"))
		}
		appAdmissionOrigin := strings.TrimRight(strings.TrimSpace(os.Getenv("STATION_MODEL_APP_ADMISSION_URL")), "/")
		if appAdmissionOrigin != "" && reconcileToken != "" {
			revoker = modelcache.HTTPAppAdmissionRevoker{
				EndpointsByApp: map[string]string{
					"comfyui":     appAdmissionOrigin + "/internal/v1/model-admissions/revoke-or-verify",
					"app-comfyui": appAdmissionOrigin + "/internal/v1/model-admissions/revoke-or-verify",
				},
				BearerToken: reconcileToken,
			}
		}
		modelCacheSvc = &modelcache.Service{
			Store:               &modelcache.PGStore{Pool: pool, StationID: station},
			StationID:           station,
			RootDir:             modelCacheRoot,
			Downloader:          safeDL,
			UpstreamBearerToken: strings.TrimSpace(os.Getenv("STATION_MODEL_UPSTREAM_BEARER_TOKEN")),
			Revoker:             revoker,
		}
	}
	enforceStrictDBRoles := strings.EqualFold(strings.TrimSpace(os.Getenv("STATION_ENFORCE_DB_ROLE_ISOLATION")), "true")
	ready := func(c context.Context) error {
		var id string
		var table bool
		e := pool.QueryRow(c, `SELECT station_id::text,to_regclass('station.runtime_app_executions') IS NOT NULL FROM station.configuration WHERE singleton`).Scan(&id, &table)
		if e != nil || id != station || !table {
			return errors.New("database not ready")
		}
		if profiles != nil {
			if e = pool.QueryRow(c, `SELECT to_regclass('station.runtime_instance_executions') IS NOT NULL AND to_regclass('station.runtime_instance_workspaces') IS NOT NULL AND to_regclass('station.app_instance_workspaces') IS NOT NULL`).Scan(&table); e != nil || !table {
				return errors.New("instance migrations not ready")
			}
		}
		if appProfiles != nil {
			if e = pool.QueryRow(c, `SELECT to_regclass('station.app_instance_workspaces') IS NOT NULL AND to_regclass('station.app_instance_starts') IS NOT NULL AND to_regclass('station.app_instance_stops') IS NOT NULL AND to_regclass('station.app_installations') IS NOT NULL AND to_regclass('station.runtime_instance_workspaces') IS NOT NULL`).Scan(&table); e != nil || !table {
				return errors.New("App instance migrations not ready")
			}
		}
		if maintenance != nil || transitionToken != "" {
			if e = pool.QueryRow(c, `SELECT EXISTS(SELECT 1 FROM station.schema_migrations WHERE version=20 AND name='0020_runtime_preflight_rejection.sql')`).Scan(&table); e != nil || !table {
				return errors.New("transition migrations not ready")
			}
		}
		if gpuSvc != nil {
			if gpuSvc.Verifier == nil {
				return errors.New("GPU node verifier not configured")
			}
			if e = gpuSvc.Pool.QueryRow(c, `SELECT to_regclass('station.gpu_physical_cards') IS NOT NULL AND to_regclass('station.gpu_resource_leases') IS NOT NULL AND to_regclass('station.gpu_instance_residency') IS NOT NULL AND to_regclass('station.gpu_scheduler_events') IS NOT NULL`).Scan(&table); e != nil || !table {
				return errors.New("GPU scheduler migrations not ready")
			}
			if e = gpuSvc.VerifyDatabasePrivilegeIsolation(c, enforceStrictDBRoles); e != nil {
				return e
			}
		}
		if modelCacheSvc != nil {
			if e = pool.QueryRow(c, `SELECT to_regclass('station.model_catalog_entries') IS NOT NULL AND to_regclass('station.model_cache_blobs') IS NOT NULL AND to_regclass('station.model_ref_pins') IS NOT NULL AND to_regclass('station.model_ensure_idempotency') IS NOT NULL`).Scan(&table); e != nil || !table {
				return errors.New("model cache migrations not ready")
			}
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
	if modelCacheSvc != nil && len(initialCatalogEntries) > 0 {
		seedCtx, seedStop := context.WithTimeout(ctx, 10*time.Second)
		seedErr := modelCacheSvc.RegisterCatalogEntries(seedCtx, initialCatalogEntries)
		seedStop()
		if seedErr != nil {
			logger.Error("model_catalog_seed_failed")
			return 1
		}
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
	appService := &executor.Service{Pool: pool, StationID: station, Targets: targets, Charts: executor.NewOCIChartSourceFromEnv(), Driver: driver, Releases: &releasebinding.Source{Path: os.Getenv("STATION_PREINSTALLED_RELEASE_REGISTRY")}}
	pb.RegisterAppRuntimeServer(server, appService)
	pb.RegisterAppCenterRuntimeServer(server, appService)
	probeAddr := os.Getenv("STATION_RUNTIME_PROBE_ADDR")
	if probeAddr == "" {
		probeAddr = "127.0.0.1:5053"
	}
	mux := http.NewServeMux()
	if maintenance != nil || transitionToken != "" {
		journal := &executor.ManagementJournal{Pool: pool, StationID: station, Targets: targets, Kubernetes: driver, Maintenance: maintenance}
		mux.Handle(executor.ManagementReconcilePath, executor.ManagementHandler{Journal: journal, Token: transitionToken})
	}
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
	probe := &http.Server{Addr: probeAddr, Handler: mux, ReadHeaderTimeout: 3 * time.Second, ReadTimeout: 10 * time.Second, WriteTimeout: 25 * time.Second, IdleTimeout: 30 * time.Second, MaxHeaderBytes: 16 << 10}
	done := make(chan error, 5)
	var appInstanceServer *http.Server
	if appProfiles != nil {
		appAddr := os.Getenv("STATION_APP_INSTANCE_ADDR")
		if appAddr == "" {
			appAddr = "127.0.0.1:5055"
		}
		var recVerifier appinstances.ReclamationVerifier
		if gpuSvc != nil && gpuSvc.Verifier != nil {
			recVerifier = gpuSvc.Verifier
		} else if agentURL := strings.TrimSpace(os.Getenv("STATION_GPU_AGENT_DEFAULT_URL")); agentURL != "" {
			if agentToken := strings.TrimSpace(os.Getenv("STATION_GPU_AGENT_TOKEN")); len(agentToken) >= 32 {
				recVerifier = scheduler.HTTPNodeVerifier{DefaultURL: agentURL, BearerToken: agentToken}
			}
		}
		observer := appinstances.HTTPWorkerObserver{Client: appinstances.InternalHTTPClient()}
		handler := appinstances.Handler{StationID: station, Profiles: appProfiles, Callers: appCallers,
			Execute: func(ctx context.Context, caller appinstances.Caller, in appinstances.Run, p appinstances.Profile) (appinstances.StartRecord, error) {
				journal := &appinstances.Journal{Pool: pool, Client: driver.Client, StationID: station, Authorize: caller.Authority.Check, Observer: observer, ReclamationVerifier: recVerifier}
				return journal.Start(ctx, in, p)
			},
			Access: func(ctx context.Context, caller appinstances.Caller, in appinstances.Run, p appinstances.Profile) (appinstances.WorkerAccess, error) {
				journal := &appinstances.Journal{Pool: pool, Client: driver.Client, StationID: station, Authorize: caller.Authority.Check, Observer: observer, ReclamationVerifier: recVerifier}
				return journal.Access(ctx, in, p)
			},
			Stop: func(ctx context.Context, caller appinstances.Caller, in appinstances.Run, p appinstances.Profile, operation, boot string) (appinstances.StopRecord, error) {
				journal := &appinstances.Journal{Pool: pool, Client: driver.Client, StationID: station, Authorize: caller.Authority.Check,
					Stopper: appinstances.HTTPWorkerStopper{Client: appinstances.InternalHTTPClient()}, ReclamationVerifier: recVerifier}
				return journal.Stop(ctx, in, p, operation, boot)
			},
			UpgradeStop: func(ctx context.Context, caller appinstances.Caller, in appinstances.Run, p appinstances.Profile, operation, boot string) (appinstances.StopRecord, error) {
				journal := &appinstances.Journal{Pool: pool, Client: driver.Client, StationID: station, Authorize: caller.Authority.Check,
					Stopper: appinstances.HTTPWorkerStopper{Client: appinstances.InternalHTTPClient()}, ReclamationVerifier: recVerifier}
				return journal.StopForTemplateUpgrade(ctx, in, p, operation, boot)
			},
			RepairStop: func(ctx context.Context, caller appinstances.Caller, in appinstances.Run, p appinstances.Profile, opts appinstances.RepairStopOptions) (appinstances.StopRecord, error) {
				journal := &appinstances.Journal{Pool: pool, Client: driver.Client, StationID: station, Authorize: caller.Authority.Check,
					Stopper: appinstances.HTTPWorkerStopper{Client: appinstances.InternalHTTPClient()}, ReclamationVerifier: recVerifier}
				return journal.StopForRepairRestart(ctx, in, p, opts)
			},
			ReclamationStatus: func(ctx context.Context, caller appinstances.Caller, identity appinstances.Identity, callerProfiles map[string]appinstances.Profile, generation int64, boot string) (appinstances.ReclamationProof, error) {
				journal := &appinstances.Journal{Pool: pool, Client: driver.Client, StationID: station, Authorize: caller.Authority.Check, ReclamationVerifier: recVerifier}
				return journal.ReclamationStatus(ctx, identity, callerProfiles, generation, boot)
			}}
		var appRootHandler http.Handler = handler
		if modelCacheSvc != nil {
			appRootHandler = multiplexModelCacheHandler(handler, modelcache.AppHandler{
				StationID: station,
				Callers:   appCallers,
				Service:   modelCacheSvc,
			})
		}
		appInstanceServer = &http.Server{Addr: appAddr, Handler: appRootHandler, ReadHeaderTimeout: 3 * time.Second,
			ReadTimeout: 10 * time.Second, WriteTimeout: 25 * time.Second, IdleTimeout: 30 * time.Second, MaxHeaderBytes: 16 << 10}
		go func() { done <- appInstanceServer.ListenAndServe() }()
	} else if modelCacheSvc != nil && appCallers != nil {
		appAddr := os.Getenv("STATION_APP_INSTANCE_ADDR")
		if appAddr == "" {
			appAddr = "127.0.0.1:5055"
		}
		appInstanceServer = &http.Server{
			Addr:              appAddr,
			Handler:           modelcache.AppHandler{StationID: station, Callers: appCallers, Service: modelCacheSvc},
			ReadHeaderTimeout: 3 * time.Second,
			ReadTimeout:       10 * time.Second,
			WriteTimeout:      25 * time.Second,
			IdleTimeout:       30 * time.Second,
			MaxHeaderBytes:    16 << 10,
		}
		go func() { done <- appInstanceServer.ListenAndServe() }()
	}
	var gpuSchedulerServer *http.Server
	if gpuSvc != nil {
		gpuAddr := gpuAddrEnv
		if gpuAddr == "" {
			gpuAddr = "0.0.0.0:5056"
		}
		recCtx, recCancel := context.WithTimeout(ctx, 10*time.Second)
		_ = gpuSvc.ReconcileRestart(recCtx, scheduler.RestartObservation{})
		recCancel()

		gpuSchedulerServer = &http.Server{
			Addr:              gpuAddr,
			Handler:           scheduler.Handler{Service: gpuSvc},
			ReadHeaderTimeout: 3 * time.Second,
			ReadTimeout:       15 * time.Second,
			WriteTimeout:      25 * time.Second,
			IdleTimeout:       30 * time.Second,
			MaxHeaderBytes:    16 << 10,
		}
		go func() { done <- gpuSchedulerServer.ListenAndServe() }()
		go func() {
			ticker := time.NewTicker(15 * time.Second)
			defer ticker.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					sCtx, sCancel := context.WithTimeout(ctx, 10*time.Second)
					_ = gpuSvc.ReconcileRestart(sCtx, scheduler.RestartObservation{})
					sCancel()
				}
			}
		}()
	}
	var instanceServer *http.Server
	if profiles != nil {
		instanceAddr := os.Getenv("STATION_INSTANCE_ADDR")
		if instanceAddr == "" {
			instanceAddr = "127.0.0.1:5054"
		}
		cleaner := func(ctx context.Context, namespace, pvcName, nodeName string) error {
			if image, runAsUser, ok := selectCleanerTarget(profiles, appProfiles, namespace, pvcName); ok {
				return instances.CleanWorkspacePod(ctx, driver.Client, namespace, pvcName, nodeName, image, runAsUser)
			}
			return instances.ErrInvalid
		}
		var workspaceNamespaces []string
		if stateConfig != nil {
			workspaceNamespaces = append(workspaceNamespaces, stateConfig.Namespaces...)
		}
		for _, p := range profiles {
			workspaceNamespaces = append(workspaceNamespaces, p.Namespace)
		}
		for _, p := range appProfiles {
			workspaceNamespaces = append(workspaceNamespaces, p.Namespace)
		}
		workspaceNamespaces = append(workspaceNamespaces, "verdantflare-app")
		instanceService := &instances.Service{Pool: pool, Client: driver.Client, StationID: station, Namespaces: workspaceNamespaces, Profiles: profiles, ProfileHistory: profileHistory, CapacityClaims: capacityClaims, Observer: instances.HTTPObserver{Client: instances.InternalHTTPClient()}, WorkingCopyObserver: instances.HTTPObserver{Client: instances.InternalHTTPClient()}, WorkerObserver: instances.HTTPObserver{Client: instances.InternalHTTPClient()}, DrainObserver: instances.HTTPObserver{Client: instances.InternalHTTPClient()}, WorkspaceCleaner: cleaner}
		var adminRootHandler http.Handler = instances.Handler{Service: instanceService, Token: os.Getenv("STATION_INSTANCE_CONTROL_TOKEN")}
		if modelCacheSvc != nil {
			adminModelToken := strings.TrimSpace(os.Getenv("STATION_MODEL_CACHE_CONTROL_TOKEN"))
			if adminModelToken == "" {
				adminModelToken = os.Getenv("STATION_INSTANCE_CONTROL_TOKEN")
			}
			adminRootHandler = multiplexModelCacheHandler(adminRootHandler, modelcache.AdminHandler{Service: modelCacheSvc, Token: adminModelToken})
		}
		instanceServer = &http.Server{Addr: instanceAddr, Handler: adminRootHandler, ReadHeaderTimeout: 3 * time.Second, ReadTimeout: 10 * time.Second, WriteTimeout: 25 * time.Second, IdleTimeout: 30 * time.Second, MaxHeaderBytes: 16 << 10}
		go func() { done <- instanceServer.ListenAndServe() }()
	} else if modelCacheSvc != nil {
		adminModelToken := strings.TrimSpace(os.Getenv("STATION_MODEL_CACHE_CONTROL_TOKEN"))
		if adminModelToken == "" {
			adminModelToken = os.Getenv("STATION_INSTANCE_CONTROL_TOKEN")
		}
		if len(adminModelToken) >= 32 {
			instanceAddr := os.Getenv("STATION_INSTANCE_ADDR")
			if instanceAddr == "" {
				instanceAddr = "127.0.0.1:5054"
			}
			instanceServer = &http.Server{Addr: instanceAddr, Handler: modelcache.AdminHandler{Service: modelCacheSvc, Token: adminModelToken}, ReadHeaderTimeout: 3 * time.Second, ReadTimeout: 10 * time.Second, WriteTimeout: 25 * time.Second, IdleTimeout: 30 * time.Second, MaxHeaderBytes: 16 << 10}
			go func() { done <- instanceServer.ListenAndServe() }()
		}
	}
	if modelCacheSvc != nil {
		go func() {
			ticker := time.NewTicker(15 * time.Second)
			defer ticker.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					// ReconcileExpiredPins partitions this round deadline into independent
					// reserved/active phase budgets + per-RPC timeouts + DB touch headroom.
					rCtx, rCancel := context.WithTimeout(ctx, 12*time.Second)
					_, _, _ = modelCacheSvc.ReconcileExpiredPins(rCtx)
					rCancel()
				}
			}
		}()
	}
	if stateObserver != nil {
		observeCtx, stopObserver := context.WithCancel(ctx)
		observerDone := make(chan struct{})
		go func() {
			defer close(observerDone)
			if stateObserver.Run(observeCtx, logger) != nil && observeCtx.Err() == nil {
				logger.Error("app_state_observer_stopped")
			}
		}()
		defer func() { stopObserver(); <-observerDone }()
	}
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
	if instanceServer != nil {
		_ = instanceServer.Shutdown(shutdown)
	}
	if appInstanceServer != nil {
		_ = appInstanceServer.Shutdown(shutdown)
	}
	if gpuSchedulerServer != nil {
		_ = gpuSchedulerServer.Shutdown(shutdown)
	}
	if modelCacheSvc != nil {
		modelCacheSvc.WaitBackgroundDownloads()
	}
	return result
}
func main() { os.Exit(run()) }
