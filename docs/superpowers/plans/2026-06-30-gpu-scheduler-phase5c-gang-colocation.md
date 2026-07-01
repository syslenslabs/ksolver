# GPU Scheduler — Phase 5c: Single-Node Gang Co-location — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: superpowers:subagent-driven-development or superpowers:executing-plans. Steps use checkbox (`- [ ]`) syntax.

**Goal:** Let a gang opt into **single-node co-location** — all its replicas land on one node (the common requirement for multi-GPU training that shares NVLink). Modeled as a solver constraint that keeps `group_size = N` (so pod/resource counting stays correct), gated by a per-workload `colocate` flag; opt-in per gang via a label. Non-co-located gangs keep today's spread behavior.

**Architecture:** Add `colocate: bool` to `OptimizationWorkload` (serde default false → planner/simulator unaffected). In `cpsat_rust::solve`, a co-located workload gets `node_used[w,n]` bools with `x[w,n] <= group_size * node_used[w,n]` and `sum_n node_used[w,n] <= 1`, so combined with the admission latch (`sum x = group_size * placed`) an admitted co-located gang places all `group_size` replicas on exactly one node. Shadow marks a gang co-located when its members carry a configurable co-location label, and (for co-located gangs) filters feasible nodes by whole-gang residual. The decision builder is unchanged (a co-located gang yields `assignment_counts = {node: N}`, which the existing per-member distribution maps 1:1 onto that node).

**Tech Stack:** Rust; `cp_sat` (OR-Tools) behind `rust-cp-sat`; existing `model`, `scheduler::{config, pod_filter, pending_input, decision, shadow}`.

## Global Constraints

- Verified facts:
  - Solver counts pod slots as `sum(x)` per node (`pods += x`, `pods <= effective_capacity.pods * y`), so keeping `group_size = N` counts a co-located gang's N pods correctly (a `group_size=1` block would under-count to 1). This is why co-location is a constraint, not a re-模eling.
  - Admission latch (Phase 5a): `sum_n x[w,n] == group_size * placed[w]`; `x[w,n] <= group_size * y[n]`.
  - `OptimizationWorkload` derives `Default`; new `#[serde(default)] pub colocate: bool` is backward compatible.
  - Gang builder (Phase 5b) already stores `requests = total (N×per-replica)`; the solver divides by `group_size` to recover per-replica. Co-location does NOT change this — it only restricts replicas to one node.
  - `cp_sat`: `model.new_bool_var_with_name`, `model.add_le(x, (coeff, boolvar))`, and summing bools into a `LinearExpr` for `add_le(sum, 1)` are all supported (used elsewhere in `solve`).
- Feasibility for a co-located gang: only nodes whose **residual fits the whole gang** (N replicas). The builder pre-filters to those; the solver's per-node capacity also enforces it.
- Per-workload, not global: `colocate` is set per gang; other gangs/singletons are unaffected. Planner never sets it.
- Unit tests pass without the `rust-cp-sat` feature; the co-location solver behavior test is feature-gated. `cargo fmt` + clean clippy. Still binds nothing (guard test intact).

## File Structure

- Modify `ksolver/src/model.rs` — add `colocate: bool` to `OptimizationWorkload`.
- Modify `ksolver/src/cpsat_rust.rs` — single-node constraint for co-located workloads + feature-gated test.
- Modify `ksolver/src/scheduler/config.rs` — add `gang_colocate_label: String`.
- Modify `ksolver/src/scheduler/pod_filter.rs` — add `colocate: bool` to `PendingGpuPod`; `classify` reads the label.
- Modify `ksolver/src/scheduler/pending_input.rs` — set `colocate`, require member agreement, filter feasible by whole-gang residual for co-located gangs.

---

## Task 1: `colocate` field on OptimizationWorkload

**Files:** Modify `model.rs`.

- [ ] **Step 1:** In `OptimizationWorkload`, add:
```rust
    /// Require all replicas of this gang on a single node (co-location).
    #[serde(default)]
    pub colocate: bool,
```
- [ ] **Step 2: Build (no feature).** `cargo build -p ksolver` → compiles (Default derive covers the new field; existing struct literals that use `..Default::default()` are fine; any full literal in code/tests must add `colocate: false` — the compiler will list them).
- [ ] **Step 3: Commit.**
```bash
cargo fmt
git add ksolver/src/model.rs
git commit -m "feat(solver): add colocate flag to OptimizationWorkload"
```

---

## Task 2: Single-node co-location constraint in the solver

**Files:** Modify `cpsat_rust.rs`.

- [ ] **Step 1: Add the constraint.** In the workload constraint loop (where the latch and `x <= group_size * y` are added), after the per-node `x <= group_size * y` loop, add for co-located workloads:
```rust
            if workload.colocate && group_size > 0 {
                let mut used_sum = LinearExpr::default();
                for node_name in &workload.feasible_nodes {
                    let x = x_vars[&(workload.id.clone(), node_name.clone())];
                    let used = model.new_bool_var_with_name(format!(
                        "used_{}__{}",
                        sanitize(&workload.id),
                        sanitize(node_name)
                    ));
                    // x > 0  =>  used = 1
                    model.add_le(x, (group_size, used));
                    used_sum += used;
                }
                // At most one node may hold this gang's replicas.
                model.add_le(used_sum, 1_i64);
            }
```
(`LinearExpr` and `sanitize` are already in scope in this module.)

- [ ] **Step 2: Feature-gated test.** In the `#[cfg(all(test, feature = "rust-cp-sat"))]` tests, add: two 2-GPU nodes, one gang `group_size=4`, `colocate=true`, total gpu=4 (1/replica), feasible on both nodes; `partial_admission=true`. Assert the gang is **not** admitted (no `assignment_counts` entry — 4 replicas can't fit on any single 2-GPU node). Then a sibling with `colocate=false` asserts it **is** admitted (spread 2+2 across the two nodes, `sum == 4`). This proves co-location forces single-node.
```rust
    fn colocate_gang(colocate: bool) -> OptimizationInput {
        // reuse gpu_node(); two 2-GPU nodes; one group_size=4 gang, total 4 GPU.
        let mut w = gang_workload(4, 4, &["n1", "n2"]);
        w.colocate = colocate;
        OptimizationInput { nodes: vec![gpu_node("n1", 2), gpu_node("n2", 2)], workloads: vec![w], anti_affinity_pairs: vec![] }
    }

    #[test]
    fn colocated_gang_needs_single_node() {
        use crate::model::ScenarioConfig;
        let scenario = ScenarioConfig { solver: "cp-sat-rust".to_string(), partial_admission: true, ..Default::default() };
        let (sol, info) = super::enabled::solve(&colocate_gang(true), &scenario).expect("solve");
        assert!(!sol.assignment_counts.contains_key("gang:t/job"), "colocated 4-gang must not fit on 2-GPU nodes; status={}", info.status);
    }

    #[test]
    fn non_colocated_gang_spreads() {
        use crate::model::ScenarioConfig;
        let scenario = ScenarioConfig { solver: "cp-sat-rust".to_string(), partial_admission: true, ..Default::default() };
        let (sol, _info) = super::enabled::solve(&colocate_gang(false), &scenario).expect("solve");
        let total: i64 = sol.assignment_counts.get("gang:t/job").map(|c| c.values().map(|v| i64::from(*v)).sum()).unwrap_or(0);
        assert_eq!(total, 4, "non-colocated 4-gang should spread 2+2 across the two nodes");
    }
```
(Confirm `gang_workload`/`gpu_node` helpers from Phase 5b exist in the test module; they do.)

- [ ] **Step 3: Run.** `cargo test -p ksolver --features rust-cp-sat cpsat_rust` → PASS (incl. the two new tests + all Phase-5a/5b tests unchanged).
- [ ] **Step 4: Commit.**
```bash
cargo fmt
git add ksolver/src/cpsat_rust.rs
git commit -m "feat(solver): single-node co-location constraint for colocate gangs"
```

---

## Task 3: Co-location label → PendingGpuPod

**Files:** Modify `config.rs`, `pod_filter.rs`; extend tests.

- [ ] **Step 1: Config.** Add `pub gang_colocate_label: String` to `ShadowConfig`; in `from_env`:
```rust
            gang_colocate_label: std::env::var("KSOLVER_SHADOW_COLOCATE_LABEL")
                .unwrap_or_else(|_| "scheduling.x-k8s.io/gang-colocate".to_string()),
```
Update all `ShadowConfig` test literals (config.rs, pod_filter.rs, watch_state.rs) to add `gang_colocate_label: "scheduling.x-k8s.io/gang-colocate".to_string(),`.

- [ ] **Step 2: PendingGpuPod.** Add `pub colocate: bool`. In `classify`, set:
```rust
    let colocate = !cfg.gang_colocate_label.is_empty()
        && pod.metadata.labels.as_ref()
            .and_then(|l| l.get(&cfg.gang_colocate_label))
            .map(|v| v == "true")
            .unwrap_or(false);
```
and include `colocate` in the returned struct. Update the `PendingGpuPod` literals in `pending_input.rs`/`decision.rs` test helpers to add `colocate: false` (compiler lists them).

- [ ] **Step 3: Test.** Add a `pod_filter` test: a pod with the co-location label `= "true"` → `classify(..).colocate == true`; absent → false.

- [ ] **Step 4: Run.** `cargo test -p ksolver scheduler::` → PASS.
- [ ] **Step 5: Commit.**
```bash
cargo fmt
git add ksolver/src/scheduler/config.rs ksolver/src/scheduler/pod_filter.rs ksolver/src/scheduler/watch_state.rs
git commit -m "feat(scheduler): co-location label -> PendingGpuPod.colocate"
```

---

## Task 4: Builder sets colocate + whole-gang feasibility

**Files:** Modify `pending_input.rs`; extend tests.

- [ ] **Step 1: Member agreement.** A gang is co-located iff **all** members have `colocate == true`; if members disagree, exclude the gang (extend the homogeneity check — add each pod's `colocate` to the per-gang agreement test). Determine `colocate` from the (agreed) members.

- [ ] **Step 2: Feasibility by whole gang.** For a co-located gang, filter `feasible_nodes` by `residual.fits(total_requests, total_ext)` (the node must hold all N replicas), where `total = scale by N` of the representative per-replica requests. For non-co-located gangs keep the per-replica filter (Phase 5b). Set `colocate` on the emitted `OptimizationWorkload`.

- [ ] **Step 3: Tests.**
  - co-located gang of 2 (1 GPU each) on a node with residual 1 GPU → excluded (can't fit whole gang); the same gang non-co-located → included (spread) — proves the whole-gang feasibility filter.
  - members disagree on colocate → excluded.
  - co-located gang emits `workloads[0].colocate == true`.

- [ ] **Step 4: Run.** `cargo test -p ksolver scheduler::pending_input` → PASS.
- [ ] **Step 5: Commit.**
```bash
cargo fmt
git add ksolver/src/scheduler/pending_input.rs
git commit -m "feat(scheduler): mark co-located gangs and filter by whole-gang residual"
```

---

## Task 5: Full gate + cluster verify

- [ ] **Step 1: Gate.** `cargo test -p ksolver` and `cargo test -p ksolver --features rust-cp-sat` and `cargo clippy -p ksolver --features rust-cp-sat --all-targets` → all green.
- [ ] **Step 2: Cluster.** On `kind-solver-lab`, two 2-GPU nodes. A co-located 4-pod gang (labels: gang + `gang-colocate=true`) → **all unplaced** (no single node fits 4). A co-located 2-pod gang → **all placed on one node**. A non-co-located 4-pod gang → **all placed** (spread 2+2). Confirm `.spec.nodeName` empty throughout.
- [ ] **Step 3:** Clean up (delete pods + nodes).

---

## Self-Review Notes

- Co-location kept as `group_size = N` + single-node constraint (not a `group_size=1` block) so pod/resource counting stays correct (solver counts `sum(x)` pods).
- Per-workload `colocate` (serde default false) → planner/simulator/singletons unaffected.
- Whole-gang residual feasibility for co-located gangs; per-replica for spread gangs.
- Member agreement on co-location folded into gang homogeneity (disagreement → exclude).
- Decision builder unchanged: a co-located gang yields `assignment_counts = {node: N}`, which the existing per-member distribution maps onto that node.
- Feature-gated tests prove co-location forces single-node (4-gang rejected on 2-GPU nodes) while spread still admits.
- Deferred: multi-node topology (NVLink/rack-aware placement, cross-node RDMA locality) — a larger L2 topology phase; this phase covers the single-node case only.
```
