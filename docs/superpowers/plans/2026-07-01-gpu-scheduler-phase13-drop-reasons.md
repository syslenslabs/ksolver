# GPU Scheduler — Phase 13: Per-Pod Unschedulability Reasons in Shadow Trace — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: superpowers:subagent-driven-development or superpowers:executing-plans. Steps use checkbox (`- [ ]`) syntax.

**Goal:** When a pending pod is dropped during input build (never submitted to the solver), report WHY in the decision trace — instead of the generic "not submitted to solver (filtered as unschedulable during input build)".

**Why:** North-star: the shadow scheduler exists so operators can see *what's happening*. Today a dropped pod's reason is opaque. Distinguishing "no node has enough free GPU / all feasible nodes excluded by anti-affinity" from "gang members are heterogeneous" or "co-location can't fit" makes the tool actionable. Pure observability — placement/feasibility logic is unchanged, so zero risk to the verified matching code.

**Architecture:** `build_pending_input` currently returns `OptimizationInput` and `continue`s (silently dropping) at each rejection point in the gang loop. Rename the body to `build_pending_input_diagnosed(cluster, pending, quotas) -> (OptimizationInput, Vec<DropInfo>)` where `DropInfo { pod_scopes: Vec<String>, reason: String }` records, at every drop point, the affected pending pods and a specific reason. Keep `build_pending_input(...) -> OptimizationInput` as a thin wrapper returning `.0`, so ALL existing callers/tests are unchanged (zero ripple). Shadow's `run_one_solve` switches to the diagnosed variant and threads a `pod-scope -> reason` map into `build_decision_trace`, which uses it for pods that were never submitted. No matching, residual, or solver behavior changes.

**Tech Stack:** Rust; `scheduler/pending_input.rs`, `scheduler/decision.rs`, `scheduler/shadow.rs`.

## Global Constraints

- **Zero behavior change to placement/feasibility:** only reason REPORTING is added. `build_pending_input` keeps its exact current signature + output via the wrapper; all its existing tests pass untouched.
- **Reasons are specific but honest:** the "no feasible node" reason must say "insufficient residual capacity OR excluded by anti-affinity/topology" (we don't separate those two sub-causes here — the feasible-node filter combines them).
- Only the shadow path consumes diagnostics; the offline planner path is untouched.
- `cargo fmt` + clean clippy; new unit tests for the diagnosed reasons; existing tests unchanged. Binds nothing.

## File Structure

- `ksolver/src/scheduler/pending_input.rs` — `DropInfo` type; `build_pending_input_diagnosed`; `build_pending_input` becomes a wrapper; record a reason at each `continue`.
- `ksolver/src/scheduler/decision.rs` — `build_decision_trace` gains a `drop_reasons: &BTreeMap<String,String>` param (pod scope `ns/name` -> reason); used for the "not submitted" branch.
- `ksolver/src/scheduler/shadow.rs` — call the diagnosed builder; build the `drop_reasons` map (flatten `DropInfo`); pass to `build_decision_trace`.

## Tasks

### Task 1: Diagnosed builder
- [ ] In `pending_input.rs` add:
```rust
#[derive(Debug, Clone)]
pub struct DropInfo {
    pub pod_scopes: Vec<String>, // "ns/name" of each affected pending pod
    pub reason: String,
}
```
- [ ] Rename the current `pub fn build_pending_input` body to `pub fn build_pending_input_diagnosed(cluster, pending, quotas) -> (OptimizationInput, Vec<DropInfo>)`. Add a `let mut dropped: Vec<DropInfo> = Vec::new();`. At EACH `continue` in the gang loop, before continuing, push a `DropInfo` with `members.iter().map(|m| format!("{}/{}", m.namespace, m.name)).collect()` and a specific reason:
  - member workload missing ⇒ "gang member missing from cluster snapshot"
  - heterogeneous signature ⇒ "gang members have heterogeneous requests or feasible sets"
  - colocate disagreement ⇒ "gang members disagree on co-location"
  - host-selector disagreement ⇒ "gang members disagree on anti-affinity selectors"
  - topology-selector disagreement ⇒ "gang members disagree on topology anti-affinity selectors"
  - colocate && self_anti ⇒ "co-location conflicts with self-spread anti-affinity"
  - empty feasible_nodes ⇒ "no feasible node (insufficient residual capacity or excluded by anti-affinity)"
- [ ] Return `(OptimizationInput { .. }, dropped)`. Add `pub fn build_pending_input(cluster, pending, quotas) -> OptimizationInput { build_pending_input_diagnosed(cluster, pending, quotas).0 }`.
- [ ] Unit tests (call `build_pending_input_diagnosed` directly): a heterogeneous gang ⇒ one DropInfo with both member scopes + the heterogeneity reason; a 1-GPU singleton on a fully-consumed node ⇒ DropInfo with the "no feasible node" reason. Existing `build_pending_input` tests still pass (wrapper). Run → commit.

### Task 2: Decision trace consumes reasons
- [ ] `build_decision_trace(... , drop_reasons: &BTreeMap<String,String>, ...)`: in the final loop over `pending`, when a pod has no entry in `placement_for` (never submitted), set the Unplaced reason to `drop_reasons.get("ns/name")` if present, else the current generic string. Keep everything else identical.
- [ ] Update `decision.rs` tests to pass `&BTreeMap::new()` (generic-reason path unchanged) plus one test asserting a supplied drop reason is surfaced. Run → commit.

### Task 3: Wire shadow
- [ ] `shadow::run_one_solve`: call `build_pending_input_diagnosed(&normalized, pending, &cfg.namespace_gpu_quotas)`; build `drop_reasons: BTreeMap<String,String>` by flattening each `DropInfo` (each scope -> reason); pass `&input` and `&drop_reasons` to `build_decision_trace`. Build (feature) + full tests + clippy.
- [ ] Optional cluster smoke: a pending GPU pod that fits nowhere (all nodes full) shows the "no feasible node" reason in its trace; binds nothing. Update README (trace now explains why a pod was not scheduled) + memory.

## Self-Review Notes
- `build_pending_input` wrapper preserves the exact signature/output ⇒ all existing tests/callers untouched (zero ripple).
- Only reporting added; placement/feasibility/solver unchanged.
- "No feasible node" reason honestly combines capacity + anti-affinity exclusion (the filter merges them).
- New tests cover a gang-drop reason and a capacity-drop reason.
