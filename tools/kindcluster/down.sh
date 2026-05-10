#!/usr/bin/env bash
set -euo pipefail
kind delete cluster --name ironbook-control || true
rm -f kubeconfig.local
