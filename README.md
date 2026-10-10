# verdantflare-station-runtime

Station Runtime Go service. Product and internal API contracts are maintained in the central verdantflare-design repository.

## Build and test

```sh
go build ./cmd/station-runtime
go test ./...
```

PostgreSQL integration tests use `STATION_TEST_DATABASE_URL` (must permit disposable database creation). `STATION_TEST_MIGRATIONS` points at the central Core migrations directory; in the design workspace the sibling path is discovered by default. Tests create and remove only their own databases.

## Configuration

- `STATION_ID`, `STATION_DATABASE_URL`: Station identity and database; apply Core migration 4 before starting.
- `STATION_TEMPLATE_ROOT`: read-only mount of the central design repository/template root.
- `STATION_RUNTIME_REGISTRY`: JSON file containing `manifest_file` and `namespace` registrations; manifest and template paths resolve beneath the root.
- `STATION_RUNTIME_ADDR`: internal gRPC listener, default `127.0.0.1:5052`.
- `STATION_RUNTIME_PROBE_ADDR`: `/healthz`, `/readyz`, default `127.0.0.1:5053`.
- `STATION_KUBECONFIG`, `STATION_KUBE_CONTEXT`: optional explicit local cluster configuration; otherwise use the in-cluster service account.

Core dispatch is enabled with `STATION_RUNTIME_TARGET=station-runtime:5052`. Run only on the trusted internal Station network. Central deployment manifests own namespace/RBAC/template mounts; this repository does not provide an alternate deployment source.

## GitHub Actions release

`.github/workflows/station-runtime.yml` runs only on pushes to `release`, compiles
and packages the Dockerfile, then publishes the linux/amd64 image. Tests run locally;
CI does not add check/test jobs. Release runs are serialized and never auto-cancelled.

Required repository or organization Actions secrets:

- `REGISTRY_ENDPOINT_ALIYUN`: registry host without a URL scheme.
- `REGISTRY_USER_ALIYUN`: registry username.
- `REGISTRY_PASSWORD_ALIYUN`: registry password.

The configured release image is
`<registry>/wod/verdantflare-station:station-runtime-v0.2.2`.
Increment `IMAGE_VERSION` for subsequent releases. The preflight refuses existing
tags and stops on registry/authentication errors; it does not publish moving SHA,
branch or latest tags.

Integrate and verify on `dev`, then fast-forward to `release` and push to trigger
Actions. The workflow does not migrate the database or deploy to Kubernetes.
Registry secret visibility and branch protections must be configured in GitHub.
Central deployment manifests remain in the design repository.

## Deployment assets and promotion path

Reusable deployment assets are developed and tested in this repository under
the single `installs/` directory:

- `installs/helm/station-runtime/` is the portable Runtime chart. It owns the
  Runtime Deployment, Service, ServiceAccount, least-privilege RoleBindings, health
  probes, and read-only ConfigMap mounts. It does not install PostgreSQL, GPU
  plugins, StorageClass/PV, or application workloads.
- `installs/docker/coturn/` provides the independent host-network Docker media relay,
  systemd units and scoped host firewall. Environment values and verification
  records remain in the central design repository.
- `installs/scripts/validate-chart.sh` runs `helm lint` and a template render.
- `installs/scripts/render-dev.sh` renders a reviewable manifest bundle; it never
  applies to a cluster.
- `installs/Workflow.md` and `installs/scripts/server-init-workflow.sh` define the server initialization
  sequence from node facts through GPU/model probes and Runtime readiness. This is
  an operator workflow, not a GitHub workflow.

The promotion path is deliberately two-step:

1. Develop the chart, scripts, probe contracts, and server initialization workflow
   under `installs/`; validate the rendered output and Runtime behavior against the dev cluster
   using an explicit context and a reviewable image version.
2. After dev evidence is accepted, copy only the environment-specific values,
   ConfigMaps, probe Jobs, and pinned image/chart references into
   `verdantflare-design/deploys/k8s.dev.verdantflarehub.com/`. The design repository
   remains the deployment fact source; this repository remains the reusable package
   and release source.

Do not commit registry credentials, Station database URLs, model weights, audio/video
assets, or rendered secrets to either repository.
