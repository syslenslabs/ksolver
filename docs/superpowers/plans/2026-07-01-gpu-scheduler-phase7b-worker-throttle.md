# GPU Scheduler — Phase 7b: Fix Worker-Count Throttle for the Pending/Shadow Path — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: superpowers:subagent-driven-development or superpowers:executing-plans. Steps use checkbox (`- [ ]`) syntax.

**Goal:** Stop throttling CP-SAT workers *down* for large-but-lightweight pending/shadow models so the hard singleton cases use the machine's cores instead of 2–4 of them.

**Why:** Measured on an M4 Max (16 cores), at the 10s shadow cap: scarce-900j (900 jobs for 800 GPU slots) admits only **336/800 at 2 workers** (the current heuristic) vs **800/800 proven-OPTIMAL at 12 workers** in 8.8s; fragmented-500j 124→200 (Optimal); baseline-500j 496→500. The dense-oversubscription weakness is self-inflicted throttling, not a hardware limit. `model_worker_count` (cpsat_rust.rs) throttles workers *down* as assignment-edges grow (≥75k→2, ≥25k→4), a rule tuned for the offline planner's huge memory footprint — but the pending path's models are small (≤~1000 vars × ~100 nodes) and not memory-bound. Fixing this is free (no new hardware) and is the single biggest latency/quality win available.

**Architecture:** The primary risk the down-throttle guards against is per-worker model-copy memory, which scales with *model size* (vars ≈ assignment edges), not merely node count. Keep a down-throttle for genuinely huge models (the offline planner), but make it far less aggressive so 25k–90k-edge models (100-node pending solves) get 8 workers instead of 2–4. The final count is still capped by `max_worker_cap()` (available cores − 1, or `KSOLVER_SOLVER_MAX_WORKERS`), so we never oversubscribe the box. No behavior change for tiny models (already 8) or the truly enormous ones (still throttled).

**Tech Stack:** Rust; `ksolver/src/cpsat_rust.rs` (`model_worker_count`, `recommended_worker_count`), pure functions with existing unit tests.

## Global Constraints

- **Deterministic + pure:** `model_worker_count` must remain a pure function of the input (no env, no CPU) — it already is; keep it that way. Only `recommended_worker_count`/`max_worker_cap` read env/CPU.
- **Never oversubscribe:** the returned worker count is always `≤ max_worker_cap()` (cores−1 or `KSOLVER_SOLVER_MAX_WORKERS`). This plan raises the *model tier*, not the cap.
- **Don't regress the offline planner:** the planner builds one big grouped model; genuinely enormous models (≫100k edges / thousands of workloads) must still be throttled to avoid memory blowups. Keep the top tier(s); only relax the middle tiers that the pending path hits.
- `cargo fmt` + clean clippy; update the existing worker-count unit tests to the new thresholds; add a test asserting a 100-node / ~90k-edge pending-style model yields ≥ 8 (pre-cap).

## File Structure

- Modify `ksolver/src/cpsat_rust.rs` — `model_worker_count` tier thresholds + tests only. No other file changes (callers already use `recommended_worker_count`).

## Tasks

### Task 1: Raise the middle tiers so 100-node pending models use 8 workers
- [ ] In `model_worker_count`, change the `by_model` tiers so only genuinely huge models throttle. Current:
```rust
        let by_model = if workload_count >= 8_000 || assignment_edges >= 200_000 {
            1
        } else if workload_count >= 3_000 || assignment_edges >= 75_000 {
            2
        } else if workload_count >= 1_000 || assignment_edges >= 25_000 {
            4
        } else {
            8
        };
```
New (relaxed middle; keep a real ceiling for the planner):
```rust
        // Down-throttle only genuinely huge models (offline planner) where per-worker
        // model copies threaten memory. The pending/shadow path (≤~1000 workloads ×
        // ~100 nodes ⇒ ≤~100k edges) is small and benefits from full parallelism, so it
        // now lands in the 8-worker tier. Final count is still capped by available cores.
        let by_model = if workload_count >= 20_000 || assignment_edges >= 1_000_000 {
            2
        } else if workload_count >= 8_000 || assignment_edges >= 400_000 {
            4
        } else {
            8
        };
```
- [ ] Keep the `by_nodes` secondary cap unchanged (it only bites at ≥2000 nodes, which the pending path never hits) and keep `by_model.min(by_nodes)`.

### Task 2: Update + add tests
- [ ] Update existing `model_worker_count` tests to the new thresholds (search `model_worker_count` / `recommended_worker_count` in the `#[cfg(test)]` block; the fixtures use `nodes(N)` / `wls(count, edges)` helpers). Any test asserting the old 2/4-worker tiers for mid-size models must assert the new value.
- [ ] Add a test proving the fix: a pending-style model (100 nodes, ~900 workloads each feasible on all 100 nodes ⇒ ~90k edges) yields `model_worker_count == 8` (the pure, pre-cap value). Assert on `model_worker_count` (not `recommended_worker_count`) so the result is CPU-independent.
- [ ] Add a test that a genuinely huge model (e.g. 25_000 workloads) still throttles to `2` (planner protection preserved).
- [ ] Run: `cargo test --features rust-cp-sat --lib worker` → all pass. `cargo clippy` clean. Commit.

### Task 3: Re-bench to confirm the win
- [ ] `cargo build --release --features rust-cp-sat`.
- [ ] `KSOLVER_BENCH_SOLVE_SECS=10 ./target/release/ksolver bench` and capture the `wrk` / `adm` / `status` columns for baseline-500j, scarce-900j, fragmented-500j. Expected: scarce-900j `wrk` now 8 (was 2) and admitted near 800 (was 336); fragmented reaches Optimal; baseline-500j admits 500. (Exact numbers vary with the box; the point is `wrk` rises and admitted/optimality improve.)
- [ ] Record the before/after in the memory status file. No cluster step needed (pure solver-core change; shadow already calls `recommended_worker_count`).

## Self-Review Notes
- Pure/deterministic `model_worker_count` preserved; env/CPU only in `recommended_worker_count`.
- Never oversubscribes: still `.min(max_worker_cap())` downstream.
- Planner protected: top tiers retained (2 workers for ≥20k workloads / ≥1M edges).
- Pending/shadow 100-node models now land in the 8-worker tier — the measured 2.4× admission win.
- Tests assert the pure pre-cap value so they pass on any CI core count.
