#!/usr/bin/env bash
# Feasibility/regression demo for a real gang-aware baseline (Volcano) — see
# docs/superpowers/specs/2026-07-10-volcano-gang-aware-baseline.md. Stands up an ephemeral kind
# cluster, installs Volcano, and shows the gang-aware contrast on a gang that cannot fully fit:
#   - Volcano (gang):  PodGroup stays Pending, ZERO members placed  (all-or-nothing).
#   - default kube:    a plain ReplicaSet PARTIAL-places (some Running) — a broken gang.
# This is the credible, non-strawman baseline the differentiator claim (beats-gang-aware) needs.
# Tears the cluster down on exit. Verified 2026-07-10.
set -euo pipefail

NAME="volcano-gang-demo-$$"
KC="$(mktemp -t volcano-kubeconfig.XXXXXX)"
VOLCANO_MANIFEST="${VOLCANO_MANIFEST:-https://raw.githubusercontent.com/volcano-sh/volcano/master/installer/volcano-development.yaml}"

cleanup() { kind delete cluster --name "$NAME" >/dev/null 2>&1 || true; rm -f "$KC" "$KC".kind; }
trap cleanup EXIT

cat > "$KC.kind" <<'EOF'
kind: Cluster
apiVersion: kind.x-k8s.io/v1alpha4
nodes: [{role: control-plane}]
EOF

echo "==> creating kind cluster $NAME"
kind create cluster --name "$NAME" --config "$KC.kind" --kubeconfig "$KC" >/dev/null
export KUBECONFIG="$KC"

echo "==> installing Volcano"
kubectl apply -f "$VOLCANO_MANIFEST" >/dev/null
kubectl -n volcano-system rollout status deploy/volcano-scheduler --timeout=180s
kubectl -n volcano-system rollout status deploy/volcano-admission --timeout=180s
# The mutating webhook (needed to create vcjobs) comes up a bit after the deployment; wait for its
# service to have ready endpoints so `kubectl apply` of the Job isn't refused.
echo "==> waiting for Volcano admission webhook endpoints"
for _ in $(seq 1 40); do
  if kubectl -n volcano-system get endpoints volcano-admission-service \
       -o jsonpath='{.subsets[*].addresses[*].ip}' 2>/dev/null | grep -q .; then
    break
  fi
  sleep 3
done

ALLOC="$(kubectl get node -o jsonpath='{.items[0].status.allocatable.cpu}')"
# Per-pod CPU = ~70% of allocatable: one pod fits, two do not -> a 2-member gang cannot fully fit.
REQ=$(( ALLOC * 7 / 10 ))
[ "$REQ" -lt 1 ] && REQ=1
echo "==> node allocatable cpu=$ALLOC; per-pod request=$REQ (2x$REQ > $ALLOC, so a 2-gang cannot fit)"

# Volcano gang: minAvailable=2, all-or-nothing. Retry the apply — the mutating webhook can still be
# warming up right after its endpoints appear.
gang_manifest="apiVersion: batch.volcano.sh/v1alpha1
kind: Job
metadata: {name: gang, namespace: default}
spec:
  minAvailable: 2
  schedulerName: volcano
  tasks:
    - replicas: 2
      name: w
      template:
        spec:
          schedulerName: volcano
          containers: [{name: c, image: registry.k8s.io/pause:3.10, resources: {requests: {cpu: \"$REQ\"}}}]"
for attempt in $(seq 1 20); do
  if printf '%s' "$gang_manifest" | kubectl apply -f - >/dev/null 2>&1; then
    break
  fi
  [ "$attempt" -eq 20 ] && { echo "FAIL: could not create Volcano Job (admission webhook not ready)"; exit 1; }
  sleep 3
done

# Default scheduler, no gang: partial placement.
cat <<EOF | kubectl apply -f - >/dev/null
apiVersion: apps/v1
kind: ReplicaSet
metadata: {name: nogang, namespace: default}
spec:
  replicas: 2
  selector: {matchLabels: {app: nogang}}
  template:
    metadata: {labels: {app: nogang}}
    spec:
      containers: [{name: c, image: registry.k8s.io/pause:3.10, resources: {requests: {cpu: "$REQ"}}}]
EOF

echo "==> waiting ~25s for scheduling to settle"
sleep 25

gang_running=$(kubectl get pods -l volcano.sh/job-name=gang --no-headers 2>/dev/null | grep -c Running || true)
nogang_running=$(kubectl get pods -l app=nogang --no-headers 2>/dev/null | grep -c Running || true)
echo "    gang (Volcano) members Running:   $gang_running / 2"
echo "    nogang (default) members Running: $nogang_running / 2"

# Gang-aware: Volcano places 0 (all-or-nothing); default places >=1 (partial/broken gang).
if [ "$gang_running" -eq 0 ] && [ "$nogang_running" -ge 1 ]; then
  echo "PASS: Volcano gang is all-or-nothing (0 placed) while default kube partial-places ($nogang_running) — a credible gang-aware baseline."
else
  echo "FAIL: expected gang=0 and nogang>=1, got gang=$gang_running nogang=$nogang_running"
  exit 1
fi
