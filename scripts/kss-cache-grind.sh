#!/usr/bin/env bash
# Build the full kube-scheduler-simulator (KSS) baseline CACHE for the GPU scenario library
# (33 scenarios x {spread, binpack} = 66 baselines), tolerating a fragile simulator.
#
# WHY A GRIND: the self-built arm64 kube-scheduler-simulator's `PUT /api/v1/reset` is broken — it
# returns 202 but NEVER drains imported objects (verified by direct probe), so each container can
# serve ~ONE baseline before it is poisoned. `gpu-scenarios --refresh-simulator-cache-only` writes
# each baseline to the cache DIR incrementally as it succeeds, and each pool worker's FIRST baseline
# (on a fresh, empty container) succeeds. So: run a pool, let each worker cache its first baseline,
# restart the pool fresh, repeat — the on-disk cache accumulates until all 66 are present (~11 rounds
# with an 8-container pool). On a WORKING simulator (amd64 official images) `/reset` drains and this
# completes in a single round; the grind is simply harmless there.
#
# Usage: kss-cache-grind.sh [cache_dir] [pool_size] [base_port] [max_rounds]
#   defaults: /tmp/ksolver-kss-cache 8 12140 16
# Prereq: the simulator images exist (see scripts/kss-pool.sh preflight) and the ksolver binary is
# built with --features rust-cp-sat. Feed the result to:
#   ksolver gpu-scenarios --simulator-cache-dir <cache_dir> --volcano-baseline volcano-baseline-cache.json
set -uo pipefail

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CACHE="${1:-/tmp/ksolver-kss-cache}"
N="${2:-8}"
BP="${3:-12140}"
MAX_ROUNDS="${4:-16}"
BIN="$REPO/target/debug/ksolver"
[ -x "$BIN" ] || { echo "build the binary first: cargo build -p ksolver --features rust-cp-sat" >&2; exit 1; }

# Expected baseline count = (# scenarios) x 2 variants.
SCN_COUNT="$("$BIN" dump-scenarios | python3 -c 'import json,sys; print(len(json.load(sys.stdin)))')"
TARGET=$(( SCN_COUNT * 2 ))
POOL=""; for i in $(seq 0 $((N-1))); do POOL="$POOL,http://127.0.0.1:$((BP+i))"; done; POOL="${POOL#,}"
count() { find "$CACHE" -type f 2>/dev/null | wc -l | tr -d ' '; }

echo "==> target $TARGET baselines ($SCN_COUNT scenarios x 2 variants) into $CACHE" >&2
stale=0
for round in $(seq 1 "$MAX_ROUNDS"); do
  before=$(count)
  echo "[round $round] cached $before / $TARGET" >&2
  [ "$before" -ge "$TARGET" ] && { echo "COMPLETE" >&2; break; }
  bash "$REPO/scripts/kss-pool.sh" stop "$N" "$BP" "$CACHE" >/dev/null 2>&1
  bash "$REPO/scripts/kss-pool.sh" start "$N" "$BP" "$CACHE" 150 >/dev/null 2>&1 || { echo "  pool start failed; retrying" >&2; continue; }
  "$BIN" gpu-scenarios \
    --simulator-pool "$POOL" \
    --simulator-cache-dir "$CACHE" \
    --refresh-simulator-cache-only \
    --simulator-max-live-baselines all \
    --simulator-timeout-ms 60000 \
    --json >/dev/null 2>&1 || true   # per-round failures are expected (broken reset); cache persists
  after=$(count)
  echo "  round $round added $((after-before)) (total $after)" >&2
  if [ "$after" -le "$before" ]; then stale=$((stale+1)); else stale=0; fi
  [ "$stale" -ge 3 ] && { echo "STALLED 3 rounds at $after/$TARGET (some scenarios' imports may be failing)" >&2; break; }
done
bash "$REPO/scripts/kss-pool.sh" stop "$N" "$BP" "$CACHE" >/dev/null 2>&1
final=$(count)
echo "==> done: $final/$TARGET baselines cached in $CACHE" >&2
[ "$final" -ge "$TARGET" ]
