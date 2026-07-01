# GPU Scheduler — Phase 11: Per-Namespace GPU Quota — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: superpowers:subagent-driven-development or superpowers:executing-plans. Steps use checkbox (`- [ ]`) syntax.

**Goal:** Enforce a hard **per-namespace GPU quota** in the shadow scheduler so one team can't monopolize the fleet — the core of the "$$$ savings" story (idle team-owned GPUs are the #1 waste). Under `partial_admission`, a namespace over its cap simply gets fewer pods admitted (up to the cap), rather than starving others. Quota = remaining cap after the namespace's already-running GPU usage.

**Why:** The north-star use case is multi-tenant fleets with per-team quotas. This is a bounded, backward-compatible solver constraint (offline planner emits no quota groups → unchanged) directly tied to cost governance.

**Architecture:** `OptimizationInput` gains `quota_groups: Vec<QuotaGroup { workload_ids, resource, limit }>` (serde default empty). In `cpsat_rust::solve`, for each group add **`Σ total_resource_w · placed[w] ≤ limit`** over the group's workloads — using each workload's stored TOTAL resource request times its admission bool `placed[w]` (exact, integer, no `per_replica` division — codex #1). This only applies when `partial_admission` is on (that's what creates `placed_vars`); a workload without a `placed` var is skipped, so strict/planner paths that never set `partial_admission` can't be made infeasible by a stray quota group (codex #2 — safe by construction). Shadow's `ShadowConfig` gains `namespace_gpu_quotas` (env `KSOLVER_SHADOW_QUOTAS="ns=cap,..."`, pure-parsed). `build_pending_input` computes `remaining = cap - running_gpu_in_ns` (clamped ≥0, running GPU accumulated in the SAME residual pass — codex #3) and emits a `QuotaGroup` over that namespace's pending workload ids. Broaden the not-admitted trace reason to "insufficient capacity or quota" (still contains "gang not admitted").

**Tech Stack:** Rust; existing `model`, `cpsat_rust`, `scheduler::{config, pending_input, decision}`.

## Global Constraints

- **Backward compatible:** `quota_groups` defaults empty; the offline planner never sets it → solver behavior unchanged there (guarded by `if !input.quota_groups.is_empty()` / per-group iteration).
- Constraint coefficient = the workload's **stored TOTAL** resource request (`extended_resource_requests[resource]`) × its admission bool `placed[w]`. Exact integer, no division. Only workloads that have a `placed` var (i.e. `partial_admission`) contribute — a stray quota group on a strict/planner solve is a silent no-op, never infeasible.
- MVP scope: resource is `nvidia.com/gpu`; tenant = **namespace** (a configurable tenant label can come later). Quotas from env `KSOLVER_SHADOW_QUOTAS="team-a=200,team-b=300"` (pure `parse_quotas(Option<String>) -> BTreeMap<String,i64>`, tested).
- **Remaining quota** = configured cap − sum of GPUs used by that namespace's **running** pods (`current_node != ""`), clamped ≥ 0. So quota accounts for existing usage (not just new). Namespaces without a configured quota are unconstrained.
- Quota groups reference emitted workload ids only (skip ids not in `input.workloads`); a group with no members or a huge limit is a no-op.
- Decision reason: a pod unadmitted due to quota currently reads "gang not admitted (insufficient capacity for all replicas)" — broaden to "gang not admitted (insufficient capacity or quota)". Keeps the "gang not admitted" substring (existing tests). (Exact capacity-vs-quota attribution is a future refinement.)
- `cargo fmt` + clean clippy; unit tests without the feature (config/builder), feature-gated solver test. Binds nothing.

## File Structure

- Modify `ksolver/src/model.rs` — `QuotaGroup` struct + `OptimizationInput.quota_groups`.
- Modify `ksolver/src/cpsat_rust.rs` — quota constraint + feature-gated test.
- Modify `ksolver/src/scheduler/config.rs` — `namespace_gpu_quotas` + `parse_quotas` + tests.
- Modify `ksolver/src/scheduler/pending_input.rs` — build quota_groups from config + running usage.
- Modify `ksolver/src/scheduler/decision.rs` — broaden the not-admitted reason.
- Modify `ksolver/src/scheduler/shadow.rs` — pass namespace quotas into the builder.
- Modify `README.md` — document `KSOLVER_SHADOW_QUOTAS`.

## Tasks

### Task 1: Model
- [ ] Add:
```rust
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct QuotaGroup {
    #[serde(default)] pub workload_ids: Vec<String>,
    #[serde(default)] pub resource: String,
    #[serde(default)] pub limit: i64,
}
```
and `#[serde(default)] pub quota_groups: Vec<QuotaGroup>,` on `OptimizationInput`. **Update ALL full `OptimizationInput { .. }` literals** (codex #4: `optimizer_input.rs`, `pending_input.rs`, `cpsat_rust.rs` tests, `bench.rs`, `decision.rs` tests, planner/verifier tests) — prefer appending `..Default::default()` so future fields don't repeat this. `rg "OptimizationInput \{" ksolver/src` to find them all. Build → commit.

### Task 2: Solver constraint (placed-based, exact)
- [ ] In `cpsat_rust::solve`, AFTER the placed/anti-affinity blocks (so `placed_vars` is populated), add:
```rust
        if !input.quota_groups.is_empty() {
            let by_id: HashMap<&str, &OptimizationWorkload> =
                input.workloads.iter().map(|w| (w.id.as_str(), w)).collect();
            for group in &input.quota_groups {
                if group.limit < 0 || group.resource.is_empty() { continue; }
                let mut expr = LinearExpr::default();
                for wid in &group.workload_ids {
                    // Quota binds admitted workloads; requires partial_admission (placed var).
                    let (Some(placed), Some(w)) = (placed_vars.get(wid), by_id.get(wid.as_str()))
                    else { continue };
                    let total = w.extended_resource_requests.get(&group.resource).copied().unwrap_or(0);
                    if total > 0 { expr += (total, *placed); }
                }
                model.add_le(expr, group.limit);
            }
        }
```
Exact integer coefficient = the workload's TOTAL resource request × its `placed` bool; no `per_replica` division (codex #1). Skips workloads lacking a `placed` var → strict/planner paths unaffected even if given a quota group (codex #2).
- [ ] Feature-gated tests: (a) two singleton 1-GPU workloads on a 4-GPU node, `partial_admission=true`, `QuotaGroup{[both], "nvidia.com/gpu", 1}` → exactly one admitted; limit 2 → both. (b) a `group_size=2` gang (total 2 GPU) with a quota of 1 → the gang is NOT admitted (whole-gang counts as 2 > 1) — proves gang quota accounting. (c) backward-compat: same inputs with `partial_admission=false` and a quota group → quota ignored (no placed vars), solve behaves as before. Run → commit.

### Task 3: Config
- [ ] Add `pub namespace_gpu_quotas: BTreeMap<String, i64>` to `ShadowConfig`; pure helper:
```rust
fn parse_quotas(v: Option<String>) -> BTreeMap<String, i64> {
    let mut m = BTreeMap::new();
    if let Some(s) = v {
        for part in s.split(',') {
            let p = part.trim();
            if let Some((k, val)) = p.split_once('=') {
                if let Ok(n) = val.trim().parse::<i64>() {
                    if n >= 0 && !k.trim().is_empty() { m.insert(k.trim().to_string(), n); }
                }
            }
        }
    }
    m
}
```
`from_env` uses `parse_quotas(std::env::var("KSOLVER_SHADOW_QUOTAS").ok())`. Tests: empty→{}, "a=2,b=3"→{a:2,b:3}, malformed entries skipped. Update all `ShadowConfig { .. }` test literals to add `namespace_gpu_quotas: Default::default(),`. Run → commit.

### Task 4: Builder + reason
- [ ] `build_pending_input` signature gains `quotas: &BTreeMap<String,i64>`. **In the SAME pass that already walks `cluster.workloads` to subtract running pods from node residuals** (codex #3 — don't add a second loop that could drift), also accumulate `running_gpu_by_ns: BTreeMap<String,i64>` for pods with `current_node != ""`, summing `extended_resource_requests["nvidia.com/gpu"]`. If that residual pass doesn't currently key by namespace, add the accumulation inline there. After emitting workloads: for each `(ns, cap)` in `quotas`, `remaining = (cap - running_gpu_by_ns.get(ns)).max(0)`; collect emitted workload ids whose namespace == that ns; push `QuotaGroup { workload_ids, resource: "nvidia.com/gpu", limit: remaining }` (only if there are member workloads). Set `input.quota_groups`. Update `decision.rs` unresolved reason string to "...insufficient capacity or quota". Update the shadow call + all builder test call sites (pass `&Default::default()` where quotas irrelevant). Tests: (a) namespace quota of 1 with two 1-GPU pending singletons and zero running → group present with the two ids and limit 1; (b) same but with a running 1-GPU pod in that ns → limit clamped to 0. Run → commit.

### Task 5: Wire shadow + verify
- [ ] `shadow.rs run_one_solve`: `build_pending_input(&normalized, pending, &cfg.namespace_gpu_quotas)`. README env doc. Build (feature) + full tests + clippy.
- [ ] Cluster: namespace `qteam` with `KSOLVER_SHADOW_QUOTAS=qteam=1`, GPU node with ≥2 GPUs, two pending `qteam` ksolver pods → exactly ONE placed (quota), the other unplaced with "capacity or quota" reason; a pod in a non-quota'd namespace still places. Nothing bound. Clean up.

## Self-Review Notes
- Backward compatible (empty quota_groups → planner unchanged; strict paths have no `placed` var → quota is a no-op, never infeasible — codex #2).
- Coefficient = stored TOTAL × `placed[w]`: exact integer, no `per_replica` division/undercount (codex #1); a whole gang counts as its full total.
- Running GPU per namespace accumulated in the existing residual pass (codex #3); remaining clamped ≥0; unconfigured namespaces unconstrained.
- All full `OptimizationInput`/`ShadowConfig` literals updated (codex #4, #5); prefer `..Default::default()`.
- Reason broadened to "capacity or quota" (exact attribution deferred).
- Namespace = tenant for MVP (configurable label later); resource = GPU.
- Binds nothing; unit + feature-gated tests; codex review before implementing.
