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

The configured initial image is
`<registry>/wod/verdantflare-station:station-runtime-v0.1.0`.
Increment `IMAGE_VERSION` for subsequent releases. The preflight refuses existing
tags and stops on registry/authentication errors; it does not publish moving SHA,
branch or latest tags.

Integrate and verify on `dev`, then fast-forward to `release` and push to trigger
Actions. The workflow does not migrate the database or deploy to Kubernetes.
Registry secret visibility and branch protections must be configured in GitHub.
Central deployment manifests remain in the design repository.
