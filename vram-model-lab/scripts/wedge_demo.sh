#!/usr/bin/env bash
# One-command demo of the VRAM->DRA wedge: starts the predictor service (with the tier-4
# observation store) and drives three AdmissionReviews through /admit, showing how each tier
# resolves and what mutation the webhook injects. Fully local; no cluster required.
#
# Usage: vram-model-lab/scripts/wedge_demo.sh
set -euo pipefail

here="$(cd "$(dirname "$0")" && pwd)"
export PYTHONPATH="$here${PYTHONPATH:+:$PYTHONPATH}"
PY="${PY:-/tmp/vram-venv/bin/python}"
PORT="${PORT:-8091}"
STORE="$here/../data/observations.jsonl"
# Serve from a temp COPY so /observe writes never mutate the committed store.
DEMO_STORE="/tmp/wedge-obs-$$.jsonl"
cp "$STORE" "$DEMO_STORE" 2>/dev/null || : > "$DEMO_STORE"

"$PY" -c "import numpy, yaml" 2>/dev/null || { echo "need numpy+pyyaml (set PY=<venv python>)"; exit 1; }

# a pod matching a recurring fingerprint (tier 4), reconstructed from real runs
"$PY" - "$STORE" <<'PY'
import json, sys
import vram_resolver as vr, build_observation_store as b
store = vr.load_observations(sys.argv[1])
recurring = {k for k, v in store.items() if len(v) >= vr.FINGERPRINT_MIN_SAMPLES}
for line in (vr.ROOT / "data" / "results.jsonl").read_text().splitlines():
    row = json.loads(line)
    if not row.get("ok") or not row.get("nvidia_smi_peak_used_mib"):
        continue
    pod = b.pod_from_row(row)
    if vr.fingerprint_key(vr.pod_fingerprint(pod)) in recurring:
        json.dump(pod, open("/tmp/wedge-historical-pod.json", "w"))
        break
PY

lsof -ti "tcp:$PORT" 2>/dev/null | xargs kill -9 2>/dev/null || true
# Promoted mode so model-predicted tiers hard-constrain (default is advisory until calibration
# is operator-promoted). Tier 1 (explicit) and tier 4 (measured) hard-constrain regardless.
( cd "$here" && KSOLVER_VRAM_HARD_ADMIT=true "$PY" vram_admission_service.py --port "$PORT" --observations "$DEMO_STORE" >/tmp/wedge-svc.log 2>&1 ) &
SVC=$!
trap 'kill "$SVC" 2>/dev/null || true; rm -f "$DEMO_STORE"' EXIT
sleep 2

admit() { # $1=label  $2=pod-json
  local review
  review=$(printf '{"request":{"uid":"u","operation":"CREATE","object":%s}}' "$2")
  echo "== $1 =="
  curl -s -X POST "http://127.0.0.1:$PORT/admit" -d "$review" | "$PY" -c "
import json,sys,base64
r=json.load(sys.stdin)['response']
ops=json.loads(base64.b64decode(r['patch'])) if r.get('patch') else []
src=next((o['value'] for o in ops if o['path'].endswith('predicted-peak-vram-source')),'-')
gib=next((o['value'] for o in ops if o['path'].endswith('predicted-peak-vram-gib')),'-')
aff='yes' if any(o['path']=='/spec/affinity' for o in ops) else 'no'
print(f'  source={src}  vram_gib={gib}  hard_node_affinity={aff}  ops={len(ops)}')
"
}

GPU='"resources":{"requests":{"nvidia.com/gpu":"1"},"limits":{"nvidia.com/gpu":"1"}}'
admit "tier 1 (explicit annotation)" \
  "{\"metadata\":{\"annotations\":{\"ksolver.dev/predicted-peak-vram-gib\":\"22\"}},\"spec\":{\"containers\":[{\"name\":\"t\",$GPU}]}}"
admit "tier 2 (static sniff -> model)" \
  "{\"metadata\":{\"annotations\":{\"ksolver.ai/vram-family\":\"transformer\",\"ksolver.ai/vram-hidden-size\":\"2048\",\"ksolver.ai/vram-layers\":\"24\"}},\"spec\":{\"containers\":[{\"name\":\"t\",\"args\":[\"--batch-size\",\"8\",\"--seq-len\",\"1024\"],$GPU}]}}"
admit "tier 4 (historical fingerprint)" "$(cat /tmp/wedge-historical-pod.json)"
admit "unknown (advisory only)" \
  "{\"metadata\":{},\"spec\":{\"containers\":[{\"name\":\"t\",\"args\":[\"--epochs\",\"3\"],$GPU}]}}"

# Learning loop: a brand-new workload is unknown, then becomes historical after 3 observations.
echo "== learning loop (/observe): same workload before vs after 3 measured runs =="
LP="{\"metadata\":{},\"spec\":{\"containers\":[{\"name\":\"t\",\"image\":\"acme/new:${RANDOM}${RANDOM}\",\"command\":[\"python\",\"run.py\"],\"args\":[\"--custom\",\"1\"],$GPU}]}}"
src() { curl -s -X POST "http://127.0.0.1:$PORT/predict" -d "$1" | "$PY" -c "import json,sys;d=json.load(sys.stdin);print(f\"  {sys.argv[1]}: source={d['source']} vram_gib={d['vram_gib']}\")" "$2"; }
src "$LP" "before"
for p in 6000 6100 6200; do
  curl -s -X POST "http://127.0.0.1:$PORT/observe" -d "{\"pod\":$LP,\"peak_mib\":$p}" >/dev/null
done
src "$LP" "after "

echo "done."
