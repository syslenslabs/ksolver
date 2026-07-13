#!/usr/bin/env bash
# End-to-end gang-aware (Volcano) baseline for ONE scenario, tying together the components:
#   ksolver dump-scenarios (topology+jobs) -> KWOK fake GPU nodes + Volcano Jobs -> read placements
#   -> ksolver score-gang-baseline (VRAM-SAFE useful GPU, counted the same way ksolver counts it).
# Emits Volcano's VRAM-safe useful GPU for the scenario — the datapoint classify_win's `gang_aware`
# arg needs. See docs/superpowers/specs/2026-07-10-volcano-gang-aware-baseline.md. Tears down on exit.
#
# Usage: volcano-baseline-run.sh <scenario-name>   (e.g. colocated-gang-vs-large)
set -euo pipefail

SCENARIO="${1:?usage: volcano-baseline-run.sh <scenario-name>}"
REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
NAME="vc-base-$$"; KC="$(mktemp -t vc-kubeconfig.XXXXXX)"
KWOK_VER="${KWOK_VER:-v0.8.0}"
VOLCANO_MANIFEST="${VOLCANO_MANIFEST:-https://raw.githubusercontent.com/volcano-sh/volcano/master/installer/volcano-development.yaml}"
PY="${PY:-python3}"
BIN="$REPO/target/debug/ksolver"
cleanup() { kind delete cluster --name "$NAME" >/dev/null 2>&1 || true; rm -f "$KC" "$KC".kind "$KC".scn "$KC".apply "$KC".score "$KC".yaml "$KC".pods; }
trap cleanup EXIT

# Use the built binary directly (not `cargo run`) — cargo's registry auto-gc is flaky in some
# sandboxes and can make `cargo run` emit nothing.
[ -x "$BIN" ] || ( cd "$REPO" && cargo build -q -p ksolver )

echo "==> exporting scenario '$SCENARIO' topology + jobs" >&2
# NOTE: `python - <args>` reads the PROGRAM from stdin (the heredoc), so data is passed by FILE PATH
# in argv — never via `< file` (that would collide with the heredoc and execute the JSON as code).
"$BIN" dump-scenarios > "$KC.scn"
"$PY" - "$SCENARIO" "$KC.scn" > "$KC.apply" <<'PY'
import json,sys
scn=[s for s in json.load(open(sys.argv[2])) if s["name"]==sys.argv[1]]
if not scn: sys.exit(f"scenario {sys.argv[1]} not found")
s=scn[0]; out={"nodes":s["nodes"],"jobs":s["jobs"]}
print(json.dumps(out))
PY
[ -s "$KC.apply" ] || { echo "no scenario data" >&2; exit 1; }

echo "==> kind + KWOK + Volcano" >&2
printf 'kind: Cluster\napiVersion: kind.x-k8s.io/v1alpha4\nnodes: [{role: control-plane}]\n' > "$KC.kind"
kind create cluster --name "$NAME" --config "$KC.kind" --kubeconfig "$KC" >/dev/null
export KUBECONFIG="$KC"
kubectl apply -f "https://github.com/kubernetes-sigs/kwok/releases/download/${KWOK_VER}/kwok.yaml" >/dev/null
kubectl apply -f "https://github.com/kubernetes-sigs/kwok/releases/download/${KWOK_VER}/stage-fast.yaml" >/dev/null
# Keep pods RUNNING: stage-fast's pod-complete stage fast-forwards pods to Succeeded, freeing their
# GPU reservation so Volcano keeps piling more onto a node (cumulative over-count, e.g. 20 on 19 GPU).
kubectl delete stage pod-complete --ignore-not-found >/dev/null 2>&1
kubectl -n kube-system rollout status deploy/kwok-controller --timeout=120s >&2
kubectl apply -f "$VOLCANO_MANIFEST" >/dev/null
kubectl -n volcano-system rollout status deploy/volcano-scheduler --timeout=180s >&2
kubectl -n volcano-system rollout status deploy/volcano-admission --timeout=180s >&2
for _ in $(seq 1 60); do kubectl -n volcano-system get endpoints volcano-admission-service -o jsonpath='{.subsets[*].addresses[*].ip}' 2>/dev/null | grep -q . && break; sleep 3; done

echo "==> creating KWOK GPU nodes + Volcano Jobs for the scenario" >&2
# Emit node + vcjob manifests from the scenario, then apply.
"$PY" - "$KC.apply" > "$KC.yaml" <<'PY'
import json,sys
s=json.load(open(sys.argv[1])); docs=[]
for n in s["nodes"]:
    docs.append({"apiVersion":"v1","kind":"Node",
      "metadata":{"name":"kwok-"+n["name"],"annotations":{"kwok.x-k8s.io/node":"fake","node.alpha.kubernetes.io/ttl":"0"},"labels":{"type":"kwok","kubernetes.io/hostname":"kwok-"+n["name"]}},
      "spec":{"taints":[{"key":"kwok.x-k8s.io/node","value":"fake","effect":"NoSchedule"}]},
      "status":{"allocatable":{"cpu":"32","memory":"256Gi","pods":"110","nvidia.com/gpu":str(n["gpus"])},
                "capacity":{"cpu":"32","memory":"256Gi","pods":"110","nvidia.com/gpu":str(n["gpus"])}}})
for j in s["jobs"]:
    reqs={"nvidia.com/gpu":str(j["gpus_per_pod"])} if j["gpus_per_pod"]>0 else {}
    colo = j["colocate"] and j["pods"]>1
    tspec={"schedulerName":"volcano","nodeSelector":{"type":"kwok"},
      "tolerations":[{"key":"kwok.x-k8s.io/node","operator":"Exists","effect":"NoSchedule"}],
      "containers":[{"name":"c","image":"registry.k8s.io/pause:3.10","resources":{"limits":reqs}}]}
    # Colocated gang => all pods on ONE node (self pod-affinity on hostname). minAvailable gives
    # all-or-nothing but NOT single-node; without this, colocated gangs scatter and fragment nodes,
    # crippling Volcano's placement and dishonestly understating its useful GPU.
    if colo:
        tspec["affinity"]={"podAffinity":{"requiredDuringSchedulingIgnoredDuringExecution":[
          {"labelSelector":{"matchLabels":{"gang":j["name"]}},"topologyKey":"kubernetes.io/hostname"}]}}
    docs.append({"apiVersion":"batch.volcano.sh/v1alpha1","kind":"Job",
      "metadata":{"name":j["name"],"namespace":"default"},
      "spec":{"minAvailable": j["pods"] if colo else 1,"schedulerName":"volcano","queue":"default",
        "tasks":[{"replicas":j["pods"],"name":"w","template":{"metadata":{"labels":{"gang":j["name"]}},"spec":tspec}}]}})
print("\n---\n".join(json.dumps(d) for d in docs))
PY
for a in $(seq 1 20); do kubectl apply -f "$KC.yaml" >/dev/null 2>&1 && break; sleep 3; done

echo "==> polling Volcano to steady state" >&2
prev=-1; stable=0
for _ in $(seq 1 40); do
  cur=$(kubectl get pods -n default -o jsonpath='{range .items[*]}{.spec.nodeName}{"\n"}{end}' 2>/dev/null | grep -c . || true)
  if [ "$cur" -eq "$prev" ]; then stable=$((stable+1)); else stable=0; fi
  [ "$stable" -ge 6 ] && break; prev="$cur"; sleep 3
done

echo "==> reading placements + scoring VRAM-safe useful GPU" >&2
# Build score-gang-baseline input from the scenario (node VRAM, per-gang predicted VRAM) + live placements.
kubectl get pods -n default -o json > "$KC.pods"
"$PY" - "$KC.apply" "$KC.pods" > "$KC.score" <<'PY'
import json,sys
GIB=1024**3
s=json.load(open(sys.argv[1])); pods=json.load(open(sys.argv[2]))
node_vram={"kwok-"+n["name"]: n["vram_gib_per_gpu"]*GIB for n in s["nodes"]}
# map job -> placements (pods with a nodeName). Volcano labels pods volcano.sh/job-name=<job>.
placed={}
for p in pods["items"]:
    node=p.get("spec",{}).get("nodeName")
    if not node: continue
    jn=p.get("metadata",{}).get("labels",{}).get("volcano.sh/job-name")
    placed.setdefault(jn,[]).append({"node":node})
gangs=[]
for j in s["jobs"]:
    gangs.append({"name":j["name"],"gpus_per_pod":j["gpus_per_pod"],
      "predicted_peak_vram_bytes": j.get("predicted_peak_vram_gib",0)*GIB,
      "min_available": j["pods"] if (j["colocate"] and j["pods"]>1) else 1,
      "placements": placed.get(j["name"],[])})
print(json.dumps({"nodes":node_vram,"gangs":gangs}))
PY
echo "==> scenario: $SCENARIO" >&2
"$BIN" score-gang-baseline < "$KC.score"
