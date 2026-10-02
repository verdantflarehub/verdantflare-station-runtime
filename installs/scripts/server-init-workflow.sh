#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat >&2 <<'EOF'
Usage: server-init-workflow.sh --context <kubectl-context> --node <node-name> [--plan]

The script is intentionally a gate runner. It performs read-only checks by
default; mutating host or cluster steps must be implemented and reviewed in the
environment-specific deployment directory before applying them.
EOF
}

CONTEXT=""
NODE=""
MODE="plan"
while (($#)); do
  case "$1" in
    --context) CONTEXT="${2:?missing context}"; shift 2 ;;
    --node) NODE="${2:?missing node}"; shift 2 ;;
    --apply) MODE="apply"; shift ;;
    --plan) MODE="plan"; shift ;;
    -h|--help) usage; exit 0 ;;
    *) echo "unknown argument: $1" >&2; usage; exit 2 ;;
  esac
done

[[ -n "$CONTEXT" && -n "$NODE" ]] || { usage; exit 2; }
command -v kubectl >/dev/null || { echo "kubectl is required" >&2; exit 1; }

K=(kubectl --context "$CONTEXT")
echo "[S0] context=$CONTEXT node=$NODE mode=$MODE"
"${K[@]}" get node "$NODE" -o jsonpath='{.metadata.uid}{"\n"}{.status.conditions[?(@.type=="Ready")].status}{"\n"}'
"${K[@]}" get node "$NODE" -o jsonpath='{.status.allocatable.nvidia\.com/gpu}{"\n"}'
echo "[S1] host NVIDIA/containerd check: run the approved node bootstrap on $NODE"
echo "[S2] device plugin: verify exactly one approved provider"
echo "[S3] storage: verify /data, StorageClass and Retain PVs"
echo "[S4] namespaces/secrets: apply environment-owned manifests only"
echo "[S5] probes: run gpu-probe and model-probe for this Node UID and manifest"
echo "[S6] runtime: render/lint chart, then verify 5052 and 5053"
echo "[S7] reconcile: one operation_id, one Attempt, one controlled workload"
echo "[S8] acceptance: real input and human quality gate before local default routing"

if [[ "$MODE" == plan ]]; then
  echo "plan complete; no mutation performed"
  exit 0
fi

echo "--apply is reserved for the reviewed environment implementation" >&2
echo "Use deploys/k8s.dev.verdantflarehub.com/ with the recorded evidence contract." >&2
exit 3
