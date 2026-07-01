# GPU Scheduler — Phase 7: Fix Single-Worker Cap for Pending Solves — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: superpowers:subagent-driven-development or superpowers:executing-plans. Steps use checkbox (`- [ ]`) syntax.

**Goal:** Let CP-SAT use multiple search workers for the shadow/pending path, which the current heuristic pins to **1 worker whenever `node_count ≥ 96`** — even for small-workload models. Model hardness is driven by variable/constraint count (`workload_count` × `assignment_edges`), not raw node count. Fix the heuristic to key off model size, then re-benchmark to quantify.

**Why:** Phase-6 load test showed large single-pod models (500–900 workloads on 100 nodes) run single-threaded and only reach Feasible in 60s, while structured gang models (few workloads) solve to Optimal fast. The pending path always has 100+ nodes → always 1 worker. Raw node count is a poor proxy for hardness: per-node vars/constraints are linear and small; the heavy dimensions are workloads and assignment edges.

**Architecture:** In `cpsat_rust::recommended_worker_count`, remove the standalone `node_count >= 96 -> 1` and `node_count >= 48 -> 2` gates; keep the `workload_count`/`assignment_edges` tiers (which capture true model size). This raises worker counts for the pending path (small workloads, many nodes) while still returning 1 for genuinely huge models (≥8000 workloads or ≥200k edges — the offline-planner case). Re-run the Phase-6 matrix to measure improvement.

**Tech Stack:** Rust; `cpsat_rust` (behind `rust-cp-sat`); existing `scheduler::bench`.

## Global Constraints

- Current heuristic (in `cpsat_rust::enabled::recommended_worker_count`):
  ```
  if workload>=8000 || edges>=200000 || nodes>=96 { 1 }
  else if workload>=3000 || edges>=75000 || nodes>=48 { 2 }
  else if workload>=1000 || edges>=25000 { 4 }
  else { 8 }
  ```
- Change: drop the `nodes>=96` and `nodes>=48` terms only. Keep workload/edge thresholds and the max of 8. This is a shared function used by BOTH the offline planner and the shadow path.
- **Planner safety:** the offline planner's hardness is captured by `workload_count`/`assignment_edges` (its large models have thousands of workloads and/or ≥200k edges → still 1 worker). Per-node variables/constraints scale linearly and are small (hundreds at 100 nodes), so many nodes alone does not justify single-worker. Removing the node gate does not raise workers for genuinely large models (they trip the workload/edge gates first). Verify no existing solver test asserts a node-count-driven worker number (the one test `large_models_use_single_worker` uses 8000 workloads, not node count).
- Do NOT change the max (8) or add CPU-count coupling in this phase (out of scope; keep the change minimal and reviewable).
- Re-benchmark with the Phase-6 harness (`ksolver bench`, release) to quantify; report before/after.
- `cargo fmt` + clean clippy.

## File Structure

- Modify `ksolver/src/cpsat_rust.rs` — `recommended_worker_count` (drop node-count gates); update/extend the feature-gated test.

---

## Task 1: Relax the heuristic

**Files:** `cpsat_rust.rs`.

- [ ] **Step 1:** Change `recommended_worker_count` (the `enabled` mod, feature build) to:
```rust
    pub fn recommended_worker_count(input: &OptimizationInput) -> i32 {
        let workload_count = input.workloads.len();
        let assignment_edges: usize =
            input.workloads.iter().map(|w| w.feasible_nodes.len()).sum();
        // Hardness tracks variables/constraints (workloads x edges), not raw node count:
        // per-node vars/constraints are small and linear, so many nodes alone does not
        // require single-worker. Huge models (offline planner) still trip these gates.
        if workload_count >= 8_000 || assignment_edges >= 200_000 {
            return 1;
        }
        if workload_count >= 3_000 || assignment_edges >= 75_000 {
            return 2;
        }
        if workload_count >= 1_000 || assignment_edges >= 25_000 {
            return 4;
        }
        8
    }
```
- [ ] **Step 2: Update tests** in the feature-gated `mod tests`:
  - keep `large_models_use_single_worker` (8000 workloads → 1) — unchanged, proves huge models stay single-worker.
  - add `many_nodes_few_workloads_use_multiple_workers`: an input with 100 nodes but only ~50 workloads each feasible on all nodes (edges 5000) → `recommended_worker_count == 8` (previously would have been 1). This is the pending-path case.
  - add `medium_model_uses_some_workers`: 500 workloads × 100 nodes = 50k edges → `== 4`.
```rust
    #[test]
    fn many_nodes_few_workloads_use_multiple_workers() {
        let input = OptimizationInput {
            nodes: (0..100).map(|i| OptimizationNode { name: format!("n-{i}"), count: 1, ..Default::default() }).collect(),
            workloads: (0..50).map(|i| OptimizationWorkload {
                id: format!("w-{i}"),
                feasible_nodes: (0..100).map(|k| format!("n-{k}")).collect(),
                ..Default::default()
            }).collect(),
            anti_affinity_pairs: Vec::new(),
        };
        assert_eq!(recommended_worker_count(&input), 8);
    }

    #[test]
    fn medium_model_uses_four_workers() {
        let input = OptimizationInput {
            nodes: (0..100).map(|i| OptimizationNode { name: format!("n-{i}"), count: 1, ..Default::default() }).collect(),
            workloads: (0..500).map(|i| OptimizationWorkload {
                id: format!("w-{i}"),
                feasible_nodes: (0..100).map(|k| format!("n-{k}")).collect(),
                ..Default::default()
            }).collect(),
            anti_affinity_pairs: Vec::new(),
        };
        assert_eq!(recommended_worker_count(&input), 4); // 50k edges -> tier 4
    }
```
- [ ] **Step 3: Run.** `cargo test -p ksolver --features rust-cp-sat cpsat_rust` → PASS.
- [ ] **Step 4: Full gate.** `cargo test -p ksolver`; `cargo test -p ksolver --features rust-cp-sat`; `cargo clippy -p ksolver --features rust-cp-sat --all-targets` → green.
- [ ] **Step 5: Commit.**
```bash
cargo fmt
git add ksolver/src/cpsat_rust.rs
git commit -m "perf(solver): drive worker count by model size, not raw node count"
```

---

## Task 2: Re-benchmark and report

- [ ] **Step 1:** `cargo build --release -p ksolver --features rust-cp-sat` then `RUST_LOG=off ./ksolver/target/release/ksolver bench 2>/dev/null` (or `./target/release/...`). Capture the table. Expect the `wrk` column to now show 2/4/8 instead of 1, and solve_ms to drop for the large singleton models (feasible/optimal sooner).
- [ ] **Step 2:** Compare before/after (Phase-6 numbers are in memory). Report per-scenario deltas: did baseline-500j / scarce-900j reach Optimal or a better incumbent within 60s? Did any newly hit Optimal?
- [ ] **Step 3:** Note residual findings (e.g. if the heavy admission objective still dominates → that's lever #2 for a later phase). Don't fix here.

---

## Self-Review Notes

- Minimal, targeted change: only the node-count gates are removed; workload/edge tiers and max-8 unchanged.
- Planner-safe: genuinely large models still return 1 (workload/edge gates); many-nodes-few-workloads (pending path) now parallelizes.
- Verified against the existing `large_models_use_single_worker` test; two new tests lock in the pending-path behavior.
- Quantified by re-running the Phase-6 harness; findings reported, lever #2 (objective weight) deferred.
- No behavior change to placement correctness — only solver parallelism.
```
