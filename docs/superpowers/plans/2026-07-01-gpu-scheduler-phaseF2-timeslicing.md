# GPU Scheduler — Phase F2: Time-Sliced GPU Disclosure — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: superpowers:subagent-driven-development or superpowers:executing-plans. Steps use checkbox (`- [ ]`) syntax.

**Goal:** When the shadow scheduler places a GPU pod on a **time-sliced** (oversubscribed) node, disclose it with a caveat — "time-sliced GPU: shared, no isolation" — so operators know that "fits" ≠ "isolated performance".

**Why:** Per `docs/superpowers/specs/2026-07-01-fractional-gpu-design.md` (F2), NVIDIA time-slicing advertises one physical GPU as N integer replicas with NO memory/fault isolation and no proportional-compute guarantee. Placement already works (integer replicas ride the existing whole-GPU path); the missing piece is **disclosure** — otherwise we'd imply isolated capacity where there is none. Pure observability; no placement/feasibility change.

**Architecture:** Detect a time-sliced node from its labels (the NVIDIA GPU operator sets `nvidia.com/gpu.replicas` > 1 and/or `nvidia.com/gpu.sharing-strategy=time-slicing`). Shadow computes the set of time-sliced node names from the normalized cluster and passes it to `build_decision_trace`, which appends the caveat to any **placed** pod whose assigned node is in that set. `NormalizedNode` already carries `labels`. No node/solver/builder change; the caveat is a per-decision string like the existing "pod anti-affinity" caveat.

**Tech Stack:** Rust; `scheduler/decision.rs`, `scheduler/shadow.rs`, a small pure detector (in `decision.rs` or `config.rs`).

## Global Constraints

- **Detection is label-based and conservative (codex):** if `nvidia.com/gpu.sharing-strategy` is PRESENT, time-sliced iff it equals `time-slicing` (values `none`/`mps` are NOT time-slicing — MPS also uses replicas, so don't treat replicas as authoritative when the strategy label exists). Only when the sharing-strategy label is ABSENT, fall back to `nvidia.com/gpu.replicas > 1` (legacy time-slicing). Unparseable/absent ⇒ not time-sliced.
- **Placement unchanged:** only a caveat is added to already-`Placed` decisions; unplaced/other logic untouched.
- **Caveat is additive:** appended to the decision's `caveats` vec (which already carries pod-level caveats); dedupe not required (node caveat is distinct text).
- `cargo fmt` + clean clippy; pure detector unit-tested + a decision-trace test; binds nothing.

## File Structure

- `ksolver/src/scheduler/decision.rs` — `build_decision_trace` gains `time_sliced_nodes: &HashSet<String>`; append the caveat to placed pods whose node is in the set. A pure `fn is_time_sliced_node(labels: &BTreeMap<String,String>) -> bool`.
- `ksolver/src/scheduler/shadow.rs` — compute the time-sliced node-name set from `normalized.nodes` and pass it in.

## Tasks

### Task 1: Pure detector
- [ ] In `decision.rs` add:
```rust
pub(crate) fn is_time_sliced_node(labels: &std::collections::BTreeMap<String, String>) -> bool {
    // If the sharing-strategy label is present it is authoritative: only "time-slicing"
    // counts (MPS also uses replicas, so replicas is not authoritative here). Fall back to
    // replicas>1 ONLY when the strategy label is absent (legacy time-slicing).
    match labels.get("nvidia.com/gpu.sharing-strategy") {
        Some(s) => s == "time-slicing",
        None => labels
            .get("nvidia.com/gpu.replicas")
            .and_then(|v| v.parse::<i64>().ok())
            .map(|n| n > 1)
            .unwrap_or(false),
    }
}
```
- [ ] Unit tests: `sharing-strategy=time-slicing` ⇒ true; `sharing-strategy=mps` (even with `replicas=4`) ⇒ FALSE (codex — MPS is not time-slicing); `sharing-strategy=none` ⇒ false; no strategy + `replicas=4` ⇒ true (fallback); no strategy + `replicas=1` ⇒ false; `replicas=x` ⇒ false; no labels ⇒ false. Run → commit.

### Task 2: Trace caveat + wiring
- [ ] `build_decision_trace(..., time_sliced_nodes: &HashSet<String>)`: after building each `PodDecision`, if its placement is `Placed { node }` and `time_sliced_nodes.contains(node)`, push `"time-sliced GPU: shared, no isolation"` to `caveats`. (Do it where the decision is assembled, using the resolved node.)
- [ ] Update ALL `build_decision_trace` call sites (decision.rs tests pass `&HashSet::new()`; shadow passes the real set). Add a decision test: a pod placed on a node in the time-sliced set gets the caveat; a pod on a normal node does not.
- [ ] Run → commit.

### Task 3: Wire shadow + docs
- [ ] `shadow::run_one_solve`: build `time_sliced: HashSet<String>` = `normalized.nodes.iter().filter(|n| is_time_sliced_node(&n.labels)).map(|n| n.name.clone())`; pass `&time_sliced` to `build_decision_trace`.
- [ ] Full `cargo test --features rust-cp-sat` + clippy. README: note time-sliced placements are disclosed. Update memory.

## Self-Review Notes
- Detection label-based + conservative; placement/feasibility unchanged.
- Caveat appended only to placed pods on time-sliced nodes; mirrors existing caveat mechanism.
- All call sites updated; pure detector + trace test cover it.
