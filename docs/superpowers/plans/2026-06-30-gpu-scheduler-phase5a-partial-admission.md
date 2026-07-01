# GPU Scheduler — Phase 5a: Partial Admission (solver latch) — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development or superpowers:executing-plans. Steps use checkbox (`- [ ]`) syntax.

**Goal:** Make the CP-SAT solver able to **place what fits and leave the rest unplaced** instead of failing the whole solve when pending pods compete for scarce capacity. Add an all-or-nothing admission latch (`sum = group_size * placed[w]`) plus an objective that maximizes admitted workloads, gated by a new `partial_admission` scenario flag so the offline planner/simulator behavior is unchanged. Enable it in shadow mode.

**Why now:** Today `cpsat_rust::solve` uses a hard `sum = group_size`, so if two pending pods can't both fit, the model is INFEASIBLE and *both* are reported unplaced. This is wrong for a scheduler and is also the prerequisite for gang scheduling (group_size > 1 all-or-nothing without failing everything). This phase is the solver foundation; gang *grouping* by label is a separate follow-up (Phase 5b).

**Architecture:** One narrowly-scoped change to `cpsat_rust::solve` behind `scenario.partial_admission`. When set: introduce `placed[w]` bool per workload, replace the hard equality with `sum_expr = group_size * placed[w]`, and add a strongly-weighted admission reward to the objective so the solver always prefers admitting a workload over saving cost. Shadow sets the flag; the decision builder already treats "no assignment" as unplaced, so it needs no change.

**Tech Stack:** Rust; `cp_sat` crate (OR-Tools) behind the `rust-cp-sat` feature; existing `ScenarioConfig`, `cpsat_rust`.

## Global Constraints

- Verified facts (from `ksolver/src/cpsat_rust.rs`):
  - Today: `model.add_eq(sum_expr, group_size)` (hard) at the workload loop (~line 120), and `model.add_le(x, (group_size, y))` per feasible node.
  - `solve` **bails** if `workload.feasible_nodes.is_empty()` (~line 40) — unchanged; shadow already excludes those.
  - Objective is built with `model.minimize(objective)` where `objective: LinearExpr` accumulates cost/slack/churn terms (~line 295–340).
  - Solver tests are gated `#[cfg(all(test, feature = "rust-cp-sat"))]` — a solver behavior test for this must be feature-gated.
  - `ScenarioConfig` has a manual `impl Default` (~line 1093 of `model.rs`); add the new field there too.
  - cp_sat API: `model.add_eq(lhs, rhs)` accepts anything convertible to `LinearExpr`; a term `(coeff: i64, var)` is a valid `LinearExpr` summand; bool vars via `model.new_bool_var_with_name(...)`.
- The new fields are `#[serde(default)]` and default to off → **flag-off solver behavior is unchanged** (same hard-equality path) and old serialized requests/reports still deserialize (backward compatible). Do NOT claim "byte-for-byte" serialized output — new fields appear in freshly-serialized JSON; that is fine and backward-compatible. Verify flag-off by running the existing feature-gated solver tests unchanged.
- **Admission weight must dominate the entire rest of the objective** so the solver maximizes admitted count first, then minimizes cost among equal-admission solutions. Do NOT use a blind constant (per codex review). Instead **auto-compute a dominating weight** inside `solve` when `partial_admission` and `admission_weight == 0`:
  - Accumulate an `i128` conservative upper bound `rest_bound` of the max magnitude of all non-admission objective terms: for each node `|cost_coeff| + active_node_weight`; plus each slack weight × its node capacity (mem/cpu/scalar caps); plus churn contributions. Use `i128` for the accumulation.
  - Set `W = rest_bound + 1`. Guard overflow: require `W.checked_mul(num_workloads as i128).and_then(|v| v.checked_add(rest_bound)) <= i64::MAX as i128`; if it would overflow, `bail!` with a clear message (do not silently truncate). Cast `W` to `i64` once bounded.
  - `admission_weight != 0` acts as an explicit override (still overflow-checked).
  - This is safe for pending-only shadow (few workloads); it is NOT enabled in the planner.
- **Do NOT enable `partial_admission` in `service::Analyzer`/offline planner.** Dropping already-running workloads as "unadmitted" would corrupt planner recommendations/resource summaries. Shadow is the only caller that sets it.
- **Guard incompatible combo:** if `partial_admission && enable_joint_rightsizing`, `bail!` — level selection (`selected_levels`/`rightsized_workloads`) is populated regardless of admission, so an unplaced workload could be reported rightsized. Shadow never enables rightsizing, so a hard guard is fine.
- Guard `group_size > 0` before creating the `placed` latch (skip/º treat as unplaced otherwise).
- Log `partial_admission` and the effective admission weight in the solver status/stats string so shadow traces show which mode ran.
- `cargo fmt` + clean `cargo clippy` before each commit. Unit tests that don't need the solver must pass without the feature.

## File Structure

- Modify `ksolver/src/model.rs` — add `partial_admission: bool` and `admission_weight: i64` to `ScenarioConfig` + its `Default`.
- Modify `ksolver/src/cpsat_rust.rs` — latch + admission objective under the flag; feature-gated test.
- Modify `ksolver/src/scheduler/shadow.rs` — set `partial_admission: true` in the shadow scenario.

---

## Task 1: Add scenario fields

**Files:** Modify `ksolver/src/model.rs`.

- [ ] **Step 1: Add fields to `ScenarioConfig`.** In the struct, near the other weights, add (`admission_weight == 0` means "auto-compute a dominating weight"):
```rust
    #[serde(default)]
    pub partial_admission: bool,
    #[serde(default)]
    pub admission_weight: i64,
```
- [ ] **Step 2: Update `impl Default for ScenarioConfig`** to set:
```rust
            partial_admission: false,
            admission_weight: 0,
```
- [ ] **Step 4: Build (no feature).** `cargo build -p ksolver` → compiles.
- [ ] **Step 5: Commit.**
```bash
cargo fmt
git add ksolver/src/model.rs
git commit -m "feat(solver): add partial_admission + admission_weight scenario fields"
```

---

## Task 2: Admission latch + objective in the solver

**Files:** Modify `ksolver/src/cpsat_rust.rs`.

**Interfaces:** No signature change; behavior switches on `scenario.partial_admission`.

- [ ] **Step 0: Guard incompatible config.** Near the top of `solve` (after the existing validation), add:
```rust
        if scenario.partial_admission && scenario.enable_joint_rightsizing {
            bail!("partial_admission is incompatible with enable_joint_rightsizing");
        }
```

- [ ] **Step 0b: Defensively clamp x-var bounds (fix 5).** In the earlier variable-creation loop where `let upper = i64::from(workload.group_size);` sets the x-var domain `[(0, upper)]`, change it to `let upper = i64::from(workload.group_size).max(0);` so a stray negative `group_size` can never create invalid variable bounds. This is a no-op for all real inputs (builders emit `group_size >= 1`; `Default` is 0) and does not change valid behavior.

- [ ] **Step 1: Introduce `placed[w]` and switch the equality.** Locate the workload constraint loop (currently):
```rust
        for workload in &input.workloads {
            let group_size = i64::from(workload.group_size);
            let sum_expr: LinearExpr = workload
                .feasible_nodes
                .iter()
                .map(|node_name| x_vars[&(workload.id.clone(), node_name.clone())])
                .collect();
            model.add_eq(sum_expr, group_size);

            for node_name in &workload.feasible_nodes {
                let x = x_vars[&(workload.id.clone(), node_name.clone())];
                let y = y_vars[node_name];
                model.add_le(x, (group_size, y));
            }
        }
```
Replace with a version that, under the flag, ties the sum to a `placed` bool and records placed vars:
```rust
        let mut placed_vars: HashMap<String, BoolVar> = HashMap::new();
        for workload in &input.workloads {
            let group_size = i64::from(workload.group_size);
            let sum_expr: LinearExpr = workload
                .feasible_nodes
                .iter()
                .map(|node_name| x_vars[&(workload.id.clone(), node_name.clone())])
                .collect();
            if scenario.partial_admission && group_size > 0 {
                let placed = model
                    .new_bool_var_with_name(format!("placed_{}", sanitize(&workload.id)));
                // sum of replicas == group_size * placed  (all-or-nothing admission)
                model.add_eq(sum_expr, (group_size, placed));
                placed_vars.insert(workload.id.clone(), placed);
            } else {
                model.add_eq(sum_expr, group_size);
            }

            for node_name in &workload.feasible_nodes {
                let x = x_vars[&(workload.id.clone(), node_name.clone())];
                let y = y_vars[node_name];
                model.add_le(x, (group_size, y));
            }
        }
```
(If `BoolVar` / `HashMap` aren't already imported in the `enabled` module, add them — `BoolVar` is already used for level vars, `HashMap` is already used.)

- [ ] **Step 2: Compute the dominating admission weight and add the reward.** Just before `model.minimize(objective);`, add. This computes a conservative `i128` upper bound of the rest of the objective, sets `W = rest_bound + 1` (or the explicit override), overflow-checks, then rewards each `placed`:
```rust
        let effective_admission_weight = if scenario.partial_admission && !placed_vars.is_empty() {
            // Conservative upper bound (i128) of the max magnitude of ALL non-admission
            // objective terms. Node terms use int vars y in [0, node.count], so every
            // per-node term scales by node.count. Slack <= capacity * count. Use the
            // absolute value of each weight (weights are not validated nonnegative).
            let mut rest_bound: i128 = 0;
            for node in &input.nodes {
                let count = i128::from(node.count.max(0));
                let cost_coeff =
                    ((node.price.monthly * scenario.cost_weight as f64).round() as i128).abs();
                rest_bound = rest_bound.saturating_add(cost_coeff.saturating_mul(count));
                rest_bound = rest_bound.saturating_add(
                    (scenario.active_node_weight as i128).abs().saturating_mul(count),
                );
                rest_bound = rest_bound.saturating_add(
                    (scenario.memory_slack_weight as i128)
                        .abs()
                        .saturating_mul((node.effective_capacity.memory_bytes as i128).max(0))
                        .saturating_mul(count),
                );
                rest_bound = rest_bound.saturating_add(
                    (scenario.cpu_slack_weight as i128)
                        .abs()
                        .saturating_mul((node.effective_capacity.milli_cpu as i128).max(0))
                        .saturating_mul(count),
                );
                for cap in node.extended_resources.values() {
                    rest_bound = rest_bound.saturating_add(
                        (scenario.memory_slack_weight as i128)
                            .abs()
                            .saturating_mul((*cap).max(0) as i128)
                            .saturating_mul(count),
                    );
                }
            }
            // Churn reward: -churn_weight * x on edges with current_count>0; x <= group_size,
            // so per workload the magnitude is bounded by churn_weight * group_size.
            for workload in &input.workloads {
                let gs = i128::from(workload.group_size.max(0));
                rest_bound = rest_bound
                    .saturating_add((scenario.churn_weight as i128).abs().saturating_mul(gs));
            }
            let w: i128 = if scenario.admission_weight > 0 {
                let explicit = scenario.admission_weight as i128;
                if explicit <= rest_bound {
                    bail!(
                        "admission_weight {explicit} does not dominate objective bound {rest_bound}; use 0 for auto or a larger value"
                    );
                }
                explicit
            } else {
                // rightsizing objective terms cannot coexist (Step 0 bails on
                // partial_admission && enable_joint_rightsizing), so rest_bound covers
                // the full objective. saturating_add guards the i128::MAX saturated case.
                rest_bound.saturating_add(1)
            };
            let n = placed_vars.len() as i128;
            let total = w
                .checked_mul(n)
                .and_then(|v| v.checked_add(rest_bound))
                .unwrap_or(i128::MAX);
            if w > i64::MAX as i128 || total > i64::MAX as i128 {
                bail!("partial_admission weight would overflow i64 objective (workloads={n}); reduce scope or set a smaller admission_weight");
            }
            w as i64
        } else {
            0
        };
        for placed in placed_vars.values() {
            // Reward admitting a workload; weight dominates the rest of the objective
            // so the solver maximizes admitted count first, then minimizes cost.
            objective -= (effective_admission_weight, *placed);
        }
```

- [ ] **Step 2b: Surface the mode in stats.** Where the solver builds its status/stats string (near `let stats = cp_solver_response_stats(...)`), include `partial_admission` and `effective_admission_weight` so shadow traces show which mode ran (append to the string that becomes `SolverInfo.status`, matching the existing `status=...; workers=...;` prefix style).

- [ ] **Step 3: Write a feature-gated solver test.** In the `#[cfg(all(test, feature = "rust-cp-sat"))] mod tests` block, add a test where two unit workloads each need the only node but the node fits only one; assert the solve succeeds (not infeasible) and admits exactly one:
```rust
    #[test]
    fn partial_admission_places_what_fits() {
        use crate::model::{
            OptimizationInput, OptimizationNode, OptimizationWorkload, ResourceList, ScenarioConfig,
        };
        // One node with room for exactly one 1-GPU pod.
        let mut node_ext = std::collections::BTreeMap::new();
        node_ext.insert("nvidia.com/gpu".to_string(), 1);
        let node = OptimizationNode {
            name: "n1".to_string(),
            count: 1,
            members: vec!["n1".to_string()],
            effective_capacity: ResourceList { milli_cpu: 8000, memory_bytes: 32 << 30, ephemeral_storage: 0, pods: 110 },
            extended_resources: node_ext,
            ..Default::default()
        };
        let mk = |name: &str| {
            let mut ext = std::collections::BTreeMap::new();
            ext.insert("nvidia.com/gpu".to_string(), 1);
            OptimizationWorkload {
                id: format!("t/{name}"),
                namespace: "t".to_string(),
                name: name.to_string(),
                group_size: 1,
                requests: ResourceList { milli_cpu: 1000, memory_bytes: 1 << 30, ephemeral_storage: 0, pods: 0 },
                extended_resource_requests: ext,
                feasible_nodes: vec!["n1".to_string()],
                ..Default::default()
            }
        };
        let input = OptimizationInput { nodes: vec![node], workloads: vec![mk("a"), mk("b")], anti_affinity_pairs: vec![] };
        let scenario = ScenarioConfig { solver: "cp-sat-rust".to_string(), partial_admission: true, ..Default::default() };
        let (solution, info) = super::enabled::solve(&input, &scenario).expect("solve should succeed, not be infeasible");
        // assignment_counts is authoritative (per codex review): count workloads with
        // any positive placement. Exactly one of the two competing pods is admitted.
        let admitted = solution
            .assignment_counts
            .values()
            .filter(|counts| counts.values().any(|c| *c > 0))
            .count();
        assert_eq!(admitted, 1, "expected exactly one admitted; status={}", info.status);
    }
```

- [ ] **Step 3b: Regression test — flag OFF stays infeasible/hard.** Add a sibling test with the same two-competing-pods input but `partial_admission: false`, asserting the solve returns an error (the current hard-equality behavior — infeasible model bails). This locks in that the default path is unchanged.

- [ ] **Step 4: Run the gated test.** `cargo test -p ksolver --features rust-cp-sat partial_admission_places_what_fits` → PASS.
- [ ] **Step 5: Regression — existing solver tests unchanged.** `cargo test -p ksolver --features rust-cp-sat` → all prior solver tests still PASS (proves flag-off behavior unchanged).
- [ ] **Step 6: Commit.**
```bash
cargo fmt
git add ksolver/src/cpsat_rust.rs
git commit -m "feat(solver): partial-admission latch and admission objective behind flag"
```

---

## Task 3: Enable partial admission in shadow

**Files:** Modify `ksolver/src/scheduler/shadow.rs`.

- [ ] **Step 1: Set the flag** in `run_one_solve`'s scenario:
```rust
    let scenario = ScenarioConfig {
        solver: "cp-sat-rust".to_string(),
        partial_admission: true,
        ..Default::default()
    };
```
- [ ] **Step 2: Feature build + full tests.** `cargo build -p ksolver --features rust-cp-sat` and `cargo test -p ksolver` → green.
- [ ] **Step 3: Clippy.** `cargo clippy -p ksolver --features rust-cp-sat --all-targets` → clean.
- [ ] **Step 4: Commit.**
```bash
cargo fmt
git add ksolver/src/scheduler/shadow.rs
git commit -m "feat(scheduler): shadow enables partial admission (place what fits)"
```

---

## Task 4: Verify against a cluster

- [ ] **Step 1:** On `kind-solver-lab`, add a fake GPU node with only 1 GPU and two pending 1-GPU ksolver pods. Before this change both would be unplaced (infeasible model); now expect exactly one `placed` and one `unplaced` in the trace.
- [ ] **Step 2:** Confirm `.spec.nodeName` empty for both (still binds nothing). Clean up (delete pods + node).

---

## Self-Review Notes (incl. codex review fixes)

- Fixes "one competing pod fails the whole solve" (real limitation found reading the solver) → Tasks 1–3.
- Planner/simulator unchanged when `partial_admission=false` (default) → regression tests (Task 2 Steps 3b & 5). New serde fields are `#[serde(default)]` (backward compatible); "byte-for-byte serialized output" is explicitly NOT claimed.
- Decision builder needs no change: unadmitted workloads have no assignment → already reported "no feasible placement found".
- **Admission weight** is auto-computed as a dominating bound with `i128` accumulation + `checked_mul`/`checked_add` overflow guard that `bail!`s rather than truncating (codex fix #2).
- **`partial_admission` + `enable_joint_rightsizing` guarded** with a `bail!` (codex fix #5 — unplaced workloads could otherwise be reported rightsized).
- **`group_size > 0` guarded** before creating the latch.
- Test asserts on **`assignment_counts`** (authoritative), not `assignments` (codex fix #3).
- **Shadow-only:** `service::Analyzer`/planner never set the flag (codex fix #6 — dropping running workloads would corrupt planner output). Only `shadow::run_one_solve` sets it.
- Mode + effective weight surfaced in solver stats (codex fix — observability).
- Deferred to Phase 5b: gang *grouping* of pending pods by a configurable gang label into `group_size > 1` workloads, and per-member trace mapping. This phase is the solver foundation that makes that safe.
```
