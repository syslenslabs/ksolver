#!/usr/bin/env bash
# Live smoke test for version-adaptive DRA (k8s 1.31–1.35). Creates an ephemeral kind cluster with
# Dynamic Resource Allocation enabled, applies a DeviceClass + node-scoped ResourceSlice at whatever
# resource.k8s.io version the cluster serves, runs `ksolver analyze` against it, and asserts the
# synthetic `dra.ksolver/<class>` capacity was read (proving discovery + dynamic list + shape-tolerant
# parse work at that version). Tears the cluster down on exit.
#
# Usage:
#   scripts/dra-version-smoke.sh                        # kind default image (recent GA / v1)
#   scripts/dra-version-smoke.sh kindest/node:v1.31.6   # oldest end of the range (v1alpha3)
#
# Verified 2026-07-10 on both endpoints: k8s 1.34 (resource.k8s.io/v1) and k8s 1.31 (v1alpha3),
# same binary, both read `dra.ksolver/gpu.example = 2`.
set -euo pipefail

IMAGE="${1:-}"
NAME="ksolver-dra-smoke-$$"
KUBECONFIG_FILE="$(mktemp -t ksolver-dra-kubeconfig.XXXXXX)"
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CLASS="gpu.example"
DRIVER="gpu.example.com"
EXPECT=2

cleanup() { kind delete cluster --name "$NAME" >/dev/null 2>&1 || true; rm -f "$KUBECONFIG_FILE"; }
trap cleanup EXIT

# Enable DRA: the feature gate PLUS `api/all=true` so the resource.k8s.io group is served whatever
# version this k8s ships (GA v1 is on by default, but alpha/beta versions need to be turned on).
cat > "${KUBECONFIG_FILE}.kind.yaml" <<'EOF'
kind: Cluster
apiVersion: kind.x-k8s.io/v1alpha4
featureGates:
  DynamicResourceAllocation: true
runtimeConfig:
  "api/all": "true"
nodes:
  - role: control-plane
EOF

echo "==> creating kind cluster ${NAME} ${IMAGE:+(image $IMAGE)}"
if [[ -n "$IMAGE" ]]; then
  kind create cluster --name "$NAME" --image "$IMAGE" --config "${KUBECONFIG_FILE}.kind.yaml" --kubeconfig "$KUBECONFIG_FILE"
else
  kind create cluster --name "$NAME" --config "${KUBECONFIG_FILE}.kind.yaml" --kubeconfig "$KUBECONFIG_FILE"
fi
export KUBECONFIG="$KUBECONFIG_FILE"

# (|| true so `grep` finding nothing doesn't trip `set -o pipefail` before the friendly check.)
VERSION="$(kubectl api-versions | { grep '^resource.k8s.io/' || true; } | sort | tail -1 | cut -d/ -f2)"
if [[ -z "$VERSION" ]]; then
  echo "FAIL: resource.k8s.io not served (DRA not enabled on this image)"; exit 1
fi
echo "==> cluster serves resource.k8s.io/${VERSION}"
NODE="$(kubectl get nodes -o jsonpath='{.items[0].metadata.name}')"

# v1alpha3/v1beta1 devices carry a `basic:` wrapper; v1beta2/v1 flatten it. The class selects on the
# slice driver, so devices need no attributes here.
DEVICE_BODY="    - name: gpu0\n    - name: gpu1"
case "$VERSION" in
  v1alpha3|v1beta1) DEVICE_BODY="    - name: gpu0\n      basic: {}\n    - name: gpu1\n      basic: {}" ;;
esac

cat > "${KUBECONFIG_FILE}.dra.yaml" <<EOF
apiVersion: resource.k8s.io/${VERSION}
kind: DeviceClass
metadata:
  name: ${CLASS}
spec:
  selectors:
    - cel:
        expression: device.driver == "${DRIVER}"
---
apiVersion: resource.k8s.io/${VERSION}
kind: ResourceSlice
metadata:
  name: slice-0
spec:
  driver: ${DRIVER}
  nodeName: ${NODE}
  pool:
    name: pool-a
    generation: 1
    resourceSliceCount: 1
  devices:
$(printf "%b" "$DEVICE_BODY")
EOF
kubectl apply -f "${KUBECONFIG_FILE}.dra.yaml"

echo "==> running ksolver analyze against the live cluster"
REPORT="$(mktemp -t ksolver-dra-report.XXXXXX)"
( cd "$REPO_ROOT" && RUST_LOG=warn cargo run --features rust-cp-sat --quiet -- analyze --kubeconfig "$KUBECONFIG_FILE" >"$REPORT" 2>/dev/null )

GOT="$(grep -o "\"dra.ksolver/${CLASS}\": *[0-9]*" "$REPORT" | grep -o '[0-9]*$' | sort -u | head -1)"
rm -f "$REPORT"
if [[ "$GOT" == "$EXPECT" ]]; then
  echo "PASS: resource.k8s.io/${VERSION} -> dra.ksolver/${CLASS} = ${GOT} (expected ${EXPECT})"
else
  echo "FAIL: resource.k8s.io/${VERSION} -> dra.ksolver/${CLASS} = '${GOT:-<absent>}' (expected ${EXPECT})"; exit 1
fi
