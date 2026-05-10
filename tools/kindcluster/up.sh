#!/usr/bin/env bash
set -euo pipefail
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$DIR/../.." && pwd)"
export KUBECONFIG="$ROOT/kubeconfig.local"

if ! command -v kind > /dev/null; then
  echo "Install kind: brew install kind"; exit 1
fi
if ! command -v kubectl > /dev/null; then
  echo "Install kubectl: brew install kubectl"; exit 1
fi

mkdir -p "$ROOT/deploy/policies/seccomp"
[ -f "$ROOT/deploy/policies/seccomp/ironbook-sandbox.json" ] || \
  echo '{"defaultAction":"SCMP_ACT_ALLOW"}' > "$ROOT/deploy/policies/seccomp/ironbook-sandbox.json"

if kind get clusters | grep -q '^ironbook-control$'; then
  echo "[kindcluster] already running"
else
  kind create cluster --config "$DIR/kind-config.yaml" --kubeconfig "$KUBECONFIG"
fi
echo "[kindcluster] KUBECONFIG=$KUBECONFIG"
kubectl --kubeconfig "$KUBECONFIG" cluster-info
