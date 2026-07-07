#!/usr/bin/env bash
# Live GPU-scheduler proof harness (KWOK-backed) for the ksolver shadow dashboard.
#
# Seeds a real Kubernetes cluster with fake GPU nodes and a fragmentation +
# VRAM-blocked workload so the shadow dashboard (Live trace / Diagnostics) shows
# ksolver-vs-kube placement on genuinely un-schedulable jobs. KWOK only manages
# nodes carrying the `kwok.x-k8s.io/node: fake` annotation, so real nodes are
# never touched. Everything is scoped to the `ksolver-demo` namespace + nodes
# labelled `ksolver-demo=true` for clean teardown.
#
# Usage:
#   KUBECONFIG=~/.kube/wsl scripts/kwok-live-demo/demo.sh up
#   KUBECONFIG=~/.kube/wsl scripts/kwok-live-demo/demo.sh down
#
# Then run the shadow server against the same cluster:
#   KUBECONFIG=~/.kube/wsl KSOLVER_SHADOW_ADDR=127.0.0.1:8090 \
#     cargo run --features rust-cp-sat -- shadow
# and open http://127.0.0.1:8090
set -euo pipefail

KWOK_VER="${KWOK_VER:-v0.8.0}"
NS=ksolver-demo
here="$(cd "$(dirname "$0")" && pwd)"

up() {
  echo "==> installing KWOK ${KWOK_VER} into namespace ${NS} (fake-node-only)"
  kubectl create namespace "$NS" --dry-run=client -o yaml | kubectl apply -f -
  kubectl label namespace "$NS" ksolver-demo=true --overwrite >/dev/null
  # retarget the release manifest's kube-system resources into our namespace
  curl -sSL "https://github.com/kubernetes-sigs/kwok/releases/download/${KWOK_VER}/kwok.yaml" \
    | sed "s/namespace: kube-system/namespace: ${NS}/g" | kubectl apply -f -
  kubectl wait --for=condition=Established crd/stages.kwok.x-k8s.io --timeout=60s
  kubectl apply -f "https://github.com/kubernetes-sigs/kwok/releases/download/${KWOK_VER}/stage-fast.yaml"
  kubectl -n "$NS" wait --for=condition=Ready pod -l app=kwok-controller --timeout=120s

  echo "==> creating fake GPU nodes + workload"
  kubectl apply -f "$here/10-gpu-nodes.yaml"
  sleep 3
  kubectl apply -f "$here/20-frag-running.yaml"
  kubectl apply -f "$here/21-bigjob-4gpu.yaml"
  kubectl apply -f "$here/22-vram-blocked.yaml"
  kubectl apply -f "$here/23-split-gang.yaml"
  echo "==> seeded. Expect in the shadow dashboard Live trace:"
  echo "    - bigjob-4gpu  : unplaced by kube AND ksolver; ksolver emits a dry-run repair (migrate frag-a to free a 4-GPU island)"
  echo "    - vram-huge-40g: kube would place it (OOM RISK liability); ksolver blocks it (VRAM > device memory), no repair"
  echo "    - teamb-train  : kube SPLITS the co-located gang 2+2 across nodes (split-gang liability); ksolver keeps it whole or declines"
  echo "    => the live-trace callout should read 'ksolver is safer' and list kube's liabilities"
}

down() {
  echo "==> tearing down demo (leaving the cluster as found)"
  kubectl delete -f "$here/23-split-gang.yaml" --ignore-not-found --wait=false || true
  kubectl delete -f "$here/22-vram-blocked.yaml" --ignore-not-found --wait=false || true
  kubectl delete -f "$here/21-bigjob-4gpu.yaml" --ignore-not-found --wait=false || true
  kubectl delete -f "$here/20-frag-running.yaml" --ignore-not-found --wait=false || true
  kubectl delete nodes -l ksolver-demo=true --ignore-not-found || true
  kubectl delete -f "https://github.com/kubernetes-sigs/kwok/releases/download/${KWOK_VER}/stage-fast.yaml" --ignore-not-found || true
  curl -sSL "https://github.com/kubernetes-sigs/kwok/releases/download/${KWOK_VER}/kwok.yaml" \
    | sed "s/namespace: kube-system/namespace: ${NS}/g" | kubectl delete -f - --ignore-not-found || true
  kubectl delete namespace "$NS" --ignore-not-found || true
  echo "==> done. Remaining nodes:"; kubectl get nodes
}

case "${1:-}" in
  up)   up ;;
  down) down ;;
  *) echo "usage: $0 {up|down}   (needs KUBECONFIG set)"; exit 2 ;;
esac
