#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
CHART="$ROOT/installs/helm/station-runtime"
command -v helm >/dev/null || { echo "helm is required" >&2; exit 1; }
helm lint "$CHART"
helm template station-runtime "$CHART" --namespace verdantflare-station >/dev/null
echo "station-runtime chart validation passed"
