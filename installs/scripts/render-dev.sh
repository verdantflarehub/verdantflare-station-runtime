#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
OUT="${1:-$ROOT/.tmp/rendered-dev}"
mkdir -p "$OUT"
command -v helm >/dev/null || { echo "helm is required" >&2; exit 1; }
helm template station-runtime "$ROOT/installs/helm/station-runtime" \
  --namespace verdantflare-station \
  --values "$ROOT/installs/helm/station-runtime/values.yaml" > "$OUT/station-runtime.yaml"
echo "rendered $OUT/station-runtime.yaml"
