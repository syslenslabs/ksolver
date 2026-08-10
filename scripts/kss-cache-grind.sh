#!/usr/bin/env bash
# Build the full kube-scheduler-simulator (KSS) baseline CACHE for the GPU scenario library
# (33 scenarios x {spread, binpack} = 66 baselines), tolerating a fragile simulator.
#
# NOTE (2026-07-13): `/api/v1/reset` now DRAINS correctly — the two KWOK apiserver bugs behind the
# old "reset never drains" behavior were fixed in scripts/kss-pool.sh (ServiceAccount admission
# rejecting pod imports -> --kube-admission=false; and an etcd-prefix mismatch -> --extra-args
# kube-apiserver=etcd-prefix=/kube-scheduler-simulator). A single fresh pool now serves ALL 66
# baselines in ONE round (verified: `gpu-scenarios --refresh-simulator-cache` -> 66/66 live, 0 errors).
#
# The multi-round loop below is therefore usually unnecessary — it detects completion and exits after
# round 1 — but is retained as a harmless, robust fallback: if a container ever wedges mid-run, the
# next round restarts the pool fresh and the on-disk cache (written incrementally by
# `--refresh-simulator-cache-only`) still accumulates to all 66. On a fully healthy pool this is a
# no-op after the first round.
#
# HISTORICAL: before the kss-pool.sh fix, the self-built arm64 simulator's reset returned 202 but
# never drained, so each container served ~ONE baseline before being poisoned and the grind needed
# ~11 rounds. That premise no longer holds.
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
