#!/usr/bin/env bash
# Build the gang-aware (Volcano) baseline CACHE: run every gang scenario through the Volcano harness
# (scripts/volcano-baseline-run.sh) and assemble a JSON map {scenario_name: volcano_safe_useful_gpu}.
# Feed the result to `ksolver gpu-scenarios --volcano-baseline <cache.json>` so wins can be classified
# beats-gang-aware. The harness is slow (one kind+KWOK+Volcano cluster per scenario), so this is run
# OFFLINE and cached — same pattern as the KSS simulator cache.
#
# Usage: volcano-baseline-cache.sh [out.json]   (default: volcano-baseline-cache.json)
set -euo pipefail

OUT="${1:-volcano-baseline-cache.json}"
REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BIN="$REPO/target/debug/ksolver"
PY="${PY:-python3}"
[ -x "$BIN" ] || ( cd "$REPO" && cargo build -q -p ksolver )

# Gang scenarios only (co-located gang with >1 pod) — non-gang scenarios don't exercise gang-awareness.
GANGS="$("$BIN" dump-scenarios | "$PY" -c 'import json,sys; print("\n".join(s["name"] for s in json.load(sys.stdin) if any(j["colocate"] and j["pods"]>1 for j in s["jobs"])))')"

echo "==> capturing Volcano baseline for gang scenarios:" >&2
printf '%s\n' "$GANGS" | sed 's/^/   - /' >&2

tmp="$(mktemp -d)"; trap 'rm -rf "$tmp"' EXIT
i=0
for scn in $GANGS; do
  [ -n "$scn" ] || continue
  echo "==> [$scn]" >&2
  if bash "$REPO/scripts/volcano-baseline-run.sh" "$scn" > "$tmp/$scn.json" 2>"$tmp/$scn.err"; then
    val="$("$PY" -c 'import json,sys; print(json.load(open(sys.argv[1]))["volcano_safe_useful_gpu"])' "$tmp/$scn.json")"
    echo "$scn=$val" >> "$tmp/pairs"
    echo "    volcano_safe_useful_gpu=$val" >&2
  else
    echo "    FAILED (see stderr); skipping — this scenario stays gang_aware=None (honest)" >&2
    tail -2 "$tmp/$scn.err" >&2 || true
  fi
  i=$((i+1))
done

# Assemble the cache map.
"$PY" - "${tmp}/pairs" > "$OUT" <<'PY'
import json,sys,os
m={}
p=sys.argv[1]
if os.path.exists(p):
    for line in open(p):
        line=line.strip()
        if not line: continue
        k,v=line.rsplit("=",1); m[k]=int(v)
print(json.dumps(m, indent=2, sort_keys=True))
PY
echo "==> wrote $(wc -l < "$OUT" 2>/dev/null || echo '?') lines to $OUT ($i scenarios attempted)" >&2
cat "$OUT"
