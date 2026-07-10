#!/usr/bin/env bash
# Capture a real gang-aware (Volcano) baseline datapoint for one gang config, on fake GPU nodes
# (kind + KWOK + Volcano). Outputs the placement metrics ksolver's win-classification needs
# (useful GPU under all-or-nothing gang semantics), so a `beats-gang-aware` comparison can be made.
# This is the "capture" half of the gang-aware baseline harness
# (docs/superpowers/specs/2026-07-10-volcano-gang-aware-baseline.md), as a standalone, verified tool.
#
# Usage: volcano-baseline-capture.sh [--node-gpus N] [--nodes M] [--pods P] [--gpus-per-pod G] [--min-available A]
#   defaults: node-gpus=4 nodes=1 pods=3 gpus-per-pod=2 min-available=<pods>
# Emits one JSON line: {"volcano_useful_gpu":..,"placed":..,"replicas":..,"min_available":..,"gang_complete":..}
set -euo pipefail

NODE_GPUS=4; NODES=1; PODS=3; GPP=2; MINAVAIL=""
while [ $# -gt 0 ]; do case "$1" in
  --node-gpus) NODE_GPUS="$2"; shift 2;; --nodes) NODES="$2"; shift 2;;
  --pods) PODS="$2"; shift 2;; --gpus-per-pod) GPP="$2"; shift 2;;
  --min-available) MINAVAIL="$2"; shift 2;; *) echo "unknown arg $1" >&2; exit 2;; esac; done
MINAVAIL="${MINAVAIL:-$PODS}"

NAME="vc-capture-$$"; KC="$(mktemp -t vc-kubeconfig.XXXXXX)"
KWOK_VER="${KWOK_VER:-v0.8.0}"
VOLCANO_MANIFEST="${VOLCANO_MANIFEST:-https://raw.githubusercontent.com/volcano-sh/volcano/master/installer/volcano-development.yaml}"
cleanup() { kind delete cluster --name "$NAME" >/dev/null 2>&1 || true; rm -f "$KC" "$KC".kind; }
trap cleanup EXIT

printf 'kind: Cluster\napiVersion: kind.x-k8s.io/v1alpha4\nnodes: [{role: control-plane}]\n' > "$KC.kind"
echo "==> kind + KWOK + Volcano (node-gpus=$NODE_GPUS nodes=$NODES; gang pods=$PODS gpp=$GPP minAvailable=$MINAVAIL)" >&2
kind create cluster --name "$NAME" --config "$KC.kind" --kubeconfig "$KC" >/dev/null
export KUBECONFIG="$KC"

kubectl apply -f "https://github.com/kubernetes-sigs/kwok/releases/download/${KWOK_VER}/kwok.yaml" >/dev/null
kubectl apply -f "https://github.com/kubernetes-sigs/kwok/releases/download/${KWOK_VER}/stage-fast.yaml" >/dev/null
kubectl -n kube-system rollout status deploy/kwok-controller --timeout=120s >&2
for i in $(seq 1 "$NODES"); do
  kubectl apply -f - >/dev/null <<EOF
apiVersion: v1
kind: Node
metadata: {annotations: {kwok.x-k8s.io/node: fake, node.alpha.kubernetes.io/ttl: "0"}, labels: {type: kwok, kubernetes.io/hostname: kwok-$i}, name: kwok-$i}
spec: {taints: [{key: kwok.x-k8s.io/node, value: fake, effect: NoSchedule}]}
status:
  allocatable: {cpu: "32", memory: 256Gi, pods: "110", nvidia.com/gpu: "$NODE_GPUS"}
  capacity: {cpu: "32", memory: 256Gi, pods: "110", nvidia.com/gpu: "$NODE_GPUS"}
EOF
done

kubectl apply -f "$VOLCANO_MANIFEST" >/dev/null
kubectl -n volcano-system rollout status deploy/volcano-scheduler --timeout=180s >&2
kubectl -n volcano-system rollout status deploy/volcano-admission --timeout=180s >&2
for _ in $(seq 1 40); do kubectl -n volcano-system get endpoints volcano-admission-service -o jsonpath='{.subsets[*].addresses[*].ip}' 2>/dev/null | grep -q . && break; sleep 3; done

gang="apiVersion: batch.volcano.sh/v1alpha1
kind: Job
metadata: {name: gang, namespace: default}
spec:
  minAvailable: $MINAVAIL
  schedulerName: volcano
  tasks:
    - replicas: $PODS
      name: w
      template:
        spec:
          schedulerName: volcano
          nodeSelector: {type: kwok}
          tolerations: [{key: kwok.x-k8s.io/node, operator: Exists, effect: NoSchedule}]
          containers: [{name: c, image: registry.k8s.io/pause:3.10, resources: {limits: {nvidia.com/gpu: \"$GPP\"}}}]"
for a in $(seq 1 20); do printf '%s' "$gang" | kubectl apply -f - >/dev/null 2>&1 && break; sleep 3; done

# Poll to steady state. Count pods PLACED (assigned to a node), not "Running": KWOK's stage-fast
# fast-forwards scheduled pods to Succeeded, so phase is unreliable — nodeName is the placement
# signal (a gang-blocked/pending pod has no nodeName). Steady = unchanged for 3 consecutive checks.
placed_count() {
  kubectl get pods -l volcano.sh/job-name=gang \
    -o jsonpath='{range .items[*]}{.spec.nodeName}{"\n"}{end}' 2>/dev/null | grep -c . || true
}
echo "==> polling Volcano to steady state" >&2
prev=-1; stable=0; placed=0
for _ in $(seq 1 40); do
  placed=$(placed_count)
  if [ "$placed" -eq "$prev" ]; then stable=$((stable+1)); else stable=0; fi
  [ "$stable" -ge 3 ] && break
  prev="$placed"; sleep 3
done

complete=false; useful=0
if [ "$placed" -ge "$MINAVAIL" ] && [ "$placed" -gt 0 ]; then complete=true; useful=$((placed * GPP)); fi
printf '{"volcano_useful_gpu":%d,"placed":%d,"replicas":%d,"min_available":%d,"gang_complete":%s}\n' \
  "$useful" "$placed" "$PODS" "$MINAVAIL" "$complete"
