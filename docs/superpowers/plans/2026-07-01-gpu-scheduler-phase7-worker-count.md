# GPU Scheduler — Phase 7: Fix Single-Worker Cap for Pending Solves — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: superpowers:subagent-driven-development or superpowers:executing-plans. Steps use checkbox (`- [ ]`) syntax.

**Goal:** Let CP-SAT use multiple search workers for the shadow/pending path, which the current heuristic pins to **1 worker whenever `node_count ≥ 96`** — even for small-workload models. Model hardness is driven by variable/constraint count (`workload_count` × `assignment_edges`), not raw node count. Fix the heuristic to key off model size, then re-benchmark to quantify.

**Why:** Phase-6 load test showed large single-pod models (500–900 workloads on 100 nodes) run single-threaded and only reach Feasible in 60s, while structured gang models (few workloads) solve to Optimal fast. The pending path always has 100+ nodes → always 1 worker. Raw node count is a poor proxy for hardness: per-node vars/constraints are linear and small; the heavy dimensions are workloads and assignment edges.

**Architecture (revised per codex):** Split into a **pure** `model_worker_count(input)` (deterministic, testable) and a runtime `recommended_worker_count(input)` that applies a CPU/env cap. `model_worker_count` keys the primary tier off `workload_count`/`assignment_edges` (true model size) AND keeps node count as a *secondary* cap (very large node counts still limit parallelism because per-node vars/slack/constraints and per-worker model copies cost memory) — but node count no longer forces 1 worker for small models. `recommended_worker_count` then caps by `available_parallelism()-1` (or `KSOLVER_SOLVER_MAX_WORKERS`) so we never oversubscribe small machines/containers. Re-run the Phase-6 matrix (+ higher-node variants) to measure improvement.

**Tech Stack:** Rust; `cpsat_rust` (behind `rust-cp-sat`); existing `scheduler::bench`.

## Global Constraints

- Current heuristic (in `cpsat_rust::enabled::recommended_worker_count`):
  ```
  if workload>=8000 || edges>=200000 || nodes>=96 { 1 }
  else if workload>=3000 || edges>=75000 || nodes>=48 { 2 }
  else if workload>=1000 || edges>=25000 { 4 }
  else { 8 }
  ```
- **Node count is a *secondary* proxy, not useless** (codex #1): per-node `y`/slack vars + capacity/objective terms scale with node count, and each parallel worker copies the model — so extreme node counts must still cap parallelism (memory). Keep a node cap tier, just not `->1` for small models.
- **Split for testability + safety:** `model_worker_count` (pure, no env/CPU) is unit-tested deterministically; `recommended_worker_count` = `model_worker_count.min(cpu/env cap)`. This avoids machine-dependent unit tests and prevents oversubscription (codex #3).
- **Offline strict-retry safety (codex #2):** a strict retry with few workloads over thousands of nodes now gets capped by the node tier (e.g. ≥5000 nodes → ≤2) rather than jumping to 8. Small strict models (≤~100s of nodes) parallelizing is fine.
- **Correctness wording (codex #4):** worker count changes only CP-SAT search — no *constraint-correctness* impact; but feasible incumbents under a timeout may differ (placements/cost can vary). State it that way, not "no behavior change."
- Re-benchmark with the Phase-6 harness (`ksolver bench`, release) to quantify; report before/after.
- `cargo fmt` + clean clippy.

## File Structure

- Modify `ksolver/src/cpsat_rust.rs` — `recommended_worker_count` (drop node-count gates); update/extend the feature-gated test.

---

## Task 1: Relax the heuristic

**Files:** `cpsat_rust.rs`.

- [ ] **Step 1:** Replace `recommended_worker_count` (the `enabled` mod, feature build) with a pure model heuristic + a capped public wrapper:
```rust
    /// Pure model-size worker heuristic (deterministic; no CPU/env coupling).
    /// Primary tier = workloads x assignment edges (true var/constraint count);
    /// node count is a secondary cap (per-node vars + per-worker model copies cost memory).
    pub fn model_worker_count(input: &OptimizationInput) -> i32 {
        let workload_count = input.workloads.len();
        let assignment_edges: usize =
            input.workloads.iter().map(|w| w.feasible_nodes.len()).sum();
        let nodes = input.nodes.len();
        let by_model = if workload_count >= 8_000 || assignment_edges >= 200_000 {
            1
        } else if workload_count >= 3_000 || assignment_edges >= 75_000 {
            2
        } else if workload_count >= 1_000 || assignment_edges >= 25_000 {
            4
        } else {
            8
        };
        // Extreme node counts still limit parallelism (memory), but do not force 1.
        let by_nodes = if nodes >= 5_000 {
            2
        } else if nodes >= 2_000 {
            4
        } else {
            8
        };
        by_model.min(by_nodes)
    }

    fn max_worker_cap() -> i32 {
        if let Ok(v) = std::env::var("KSOLVER_SOLVER_MAX_WORKERS") {
            if let Ok(n) = v.parse::<i32>() {
                if n >= 1 {
                    return n;
                }
            }
        }
        // Leave one core of headroom; never below 1.
        std::thread::available_parallelism()
            .map(|n| (n.get().saturating_sub(1)).max(1) as i32)
            .unwrap_or(1)
    }

    pub fn recommended_worker_count(input: &OptimizationInput) -> i32 {
        model_worker_count(input).min(max_worker_cap()).max(1)
    }
```
Also add a matching `pub fn model_worker_count(_input) -> i32 { 1 }` (and keep `recommended_worker_count`) in the `#[cfg(not(feature))]` stub mod, and extend the re-export: `pub use enabled::{model_worker_count, recommended_worker_count, solve, solver_info};`.
- [ ] **Step 2: Update tests** — target the pure `model_worker_count` (deterministic; not machine-dependent). Change the existing `large_models_use_single_worker` to call `model_worker_count` (8000 workloads → 1). Add:
  - `many_nodes_few_workloads`: 100 nodes, 50 workloads (edges 5000) → `model_worker_count == 8` (was 1 before the fix — the pending-path case).
  - `medium_model_four_workers`: 500 workloads × 100 nodes (50k edges) → `== 4`.
  - `extreme_nodes_capped`: 5000 nodes, 2 workloads feasible on all → `== 2` (node cap prevents blindly returning 8 — codex #1/#2 strict-retry safety).
  - `recommended_never_below_one`: `recommended_worker_count(&tiny) >= 1` (CPU-capped path).
```rust
    fn nodes(n: usize) -> Vec<OptimizationNode> {
        (0..n).map(|i| OptimizationNode { name: format!("n-{i}"), count: 1, ..Default::default() }).collect()
    }
    fn workloads(w: usize, feas: usize) -> Vec<OptimizationWorkload> {
        (0..w).map(|i| OptimizationWorkload {
            id: format!("w-{i}"),
            feasible_nodes: (0..feas).map(|k| format!("n-{k}")).collect(),
            ..Default::default()
        }).collect()
    }
    #[test]
    fn many_nodes_few_workloads() {
        let input = OptimizationInput { nodes: nodes(100), workloads: workloads(50, 100), anti_affinity_pairs: vec![] };
        assert_eq!(model_worker_count(&input), 8);
    }
    #[test]
    fn medium_model_four_workers() {
        let input = OptimizationInput { nodes: nodes(100), workloads: workloads(500, 100), anti_affinity_pairs: vec![] };
        assert_eq!(model_worker_count(&input), 4);
    }
    #[test]
    fn extreme_nodes_capped() {
        let input = OptimizationInput { nodes: nodes(5000), workloads: workloads(2, 5000), anti_affinity_pairs: vec![] };
        assert_eq!(model_worker_count(&input), 2);
    }
    #[test]
    fn recommended_never_below_one() {
        let input = OptimizationInput { nodes: nodes(4), workloads: workloads(2, 4), anti_affinity_pairs: vec![] };
        assert!(recommended_worker_count(&input) >= 1);
    }
```
Import both `model_worker_count` and `recommended_worker_count` in the test module.
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

## Self-Review Notes (incl. codex fixes)

- Node count kept as a *secondary* cap (≥5000 → 2, ≥2000 → 4), not `->1` for small models (codex #1); extreme-node strict-retry stays bounded (codex #2).
- Pure `model_worker_count` (deterministic, unit-tested) split from CPU/env-capped `recommended_worker_count` (codex #3) — no oversubscription, no machine-dependent tests; `KSOLVER_SOLVER_MAX_WORKERS` override.
- Correctness wording fixed (codex #4): only CP-SAT search changes; feasible incumbents under a timeout may differ (placements/cost can vary), but constraint-correctness is unchanged.
- Tests cover pending-path (many nodes/few workloads), medium model, extreme-node cap, and the ≥1 floor (codex #5).
- Quantified by re-running the Phase-6 harness (+ a 1000-node variant); lever #2 (objective weight) deferred.
```
