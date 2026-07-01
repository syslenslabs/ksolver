# GPU Scheduler — Phase 8: Bounded Shadow Solve Latency — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: superpowers:subagent-driven-development or superpowers:executing-plans. Steps use checkbox (`- [ ]`) syntax.

**Goal:** Bound shadow-scheduler solve latency. Today `shadow::run_one_solve` builds a `ScenarioConfig` without `solve_time_limit_secs`, so it defaults to **600s** — meaning a production shadow solve can spend up to 10 minutes *proving* cost-optimality even though a valid full-admission placement is found in milliseconds (Phase-6/7 finding: baseline cases hit the cap at `Feasible`, correct answer instant). A scheduler wants a good incumbent fast. Add a configurable shadow solve time limit (default short) and accept the best incumbent within it.

**Why:** Phase-7 sped structured cases dramatically, but `baseline-50j/500j` still burn 60s proving optimality; in real shadow mode the cap is 600s. The scheduling *decision* (which pods placed where, constraints satisfied) is done in ms; the rest is optimality proof the scheduler doesn't need. Bounding the solve makes shadow responsive and matches the batch-window cadence.

**Architecture:** `ShadowConfig` gains `solve_time_limit_secs` (env `KSOLVER_SHADOW_SOLVE_SECS`, default 10). `run_one_solve` passes it into `ScenarioConfig.solve_time_limit_secs` (Phase-6 knob), so CP-SAT returns the best incumbent (usually `Feasible` full-admission) within the limit. The decision trace already surfaces `solver_status` (Feasible vs Optimal) so operators can see when the limit truncated the search. No solver-behavior change beyond honoring the existing time-limit field; offline planner untouched (still 0 → 600s → proves optimal).

**Tech Stack:** Rust; existing `scheduler::{config, shadow}`, `model::ScenarioConfig` (Phase-6 field).

## Global Constraints

- `ScenarioConfig.solve_time_limit_secs` already exists (Phase 6): 0 → 600s default; >0 → that many seconds. Reuse it.
- `run_one_solve` currently: `ScenarioConfig { solver: "cp-sat-rust", partial_admission: true, ..Default::default() }` → `solve_time_limit_secs = 0` → 600s. Change to pass the shadow config's limit.
- CP-SAT returns `Feasible` incumbents within the limit; `solve` only errors on no-incumbent (`UNKNOWN`)/infeasible. Under a short limit a very hard batch could return no incumbent → current code already catches the error and records `status="error"` with all pods unplaced (graceful — no crash). Keep that; optionally clarify the recorded status string. Do NOT change the solver's bail behavior (shared with planner).
- Accepting `Feasible` (not `Optimal`) is correct for a scheduler: placement validity + full admission is what matters; cost/packing optimality is best-effort within the budget. The trace's `solver_status` discloses which was achieved.
- Default 10s is ≤ the default batch window (10s) so a solve fits the cadence; fully configurable.
- Unit tests without the `rust-cp-sat` feature (config parsing + wiring). `cargo fmt` + clean clippy. Still binds nothing.

## File Structure

- Modify `ksolver/src/scheduler/config.rs` — add `solve_time_limit_secs: i64` (env `KSOLVER_SHADOW_SOLVE_SECS`, default 10).
- Modify `ksolver/src/scheduler/shadow.rs` — pass it into the scenario in `run_one_solve`.
- Modify `README.md` — document the env var.

## Tasks

### Task 1: ShadowConfig field
- [ ] **Step 1:** Add `pub solve_time_limit_secs: i64,` to `ShadowConfig`. In `from_env`:
```rust
            solve_time_limit_secs: std::env::var("KSOLVER_SHADOW_SOLVE_SECS")
                .ok()
                .and_then(|v| v.parse::<i64>().ok())
                .filter(|v| *v > 0)
                .unwrap_or(10),
```
- [ ] **Step 2: Failing test** in `config.rs`: a `ShadowConfig` literal test already exists — add a small test asserting `from_env` default is 10 when the env is unset (guard against other env by constructing via `from_env` in a cleared-env context is fragile; instead just assert the base test literal compiles and add a unit test on a helper if needed). Simpler: add `solve_time_limit_secs` to the existing test literals and assert a constructed value round-trips. Update ALL `ShadowConfig { .. }` literals (config.rs, pod_filter.rs, watch_state.rs tests) to include `solve_time_limit_secs: 10,`.
- [ ] **Step 3: Build + tests.** `cargo test -p ksolver scheduler::` → green.
- [ ] **Step 4: Commit.**
```bash
cargo fmt
git add ksolver/src/scheduler/config.rs ksolver/src/scheduler/pod_filter.rs ksolver/src/scheduler/watch_state.rs
git commit -m "feat(scheduler): configurable shadow solve time limit (default 10s)"
```

### Task 2: Wire into run_one_solve
- [ ] **Step 1:** In `shadow.rs run_one_solve`, change the scenario to:
```rust
    let scenario = ScenarioConfig {
        solver: "cp-sat-rust".to_string(),
        partial_admission: true,
        solve_time_limit_secs: cfg.solve_time_limit_secs,
        ..Default::default()
    };
```
- [ ] **Step 2: Build (feature) + full tests + clippy** → green.
- [ ] **Step 3: README** — add `KSOLVER_SHADOW_SOLVE_SECS` (default 10) to the shadow env var list, noting shadow accepts the best incumbent within this budget (status Feasible vs Optimal shown in traces).
- [ ] **Step 4: Commit.**
```bash
cargo fmt
git add ksolver/src/scheduler/shadow.rs README.md
git commit -m "feat(scheduler): shadow bounds solve to configurable limit, accepts best incumbent"
```

### Task 3: Verify (cluster)
- [ ] **Step 1:** On `kind-solver-lab`, run `KSOLVER_SHADOW_SOLVE_SECS=5 ... shadow` with a handful of pending GPU pods; confirm each trace's `solve_millis` is bounded near the limit (or less) and decisions are produced (placed/unplaced), nothing bound.
- [ ] **Step 2:** Confirm default (unset) is ~10s via a trace `solve_millis` upper bound on a non-trivial pending set.

## Self-Review Notes
- Reuses the Phase-6 `solve_time_limit_secs` knob; no solver change; planner untouched (its path leaves it 0 → 600s).
- Scheduler-correct: accepts Feasible full-admission within budget; `solver_status` in the trace discloses Feasible-vs-Optimal.
- Graceful on no-incumbent (existing catch → error status → pods unplaced); solver bail behavior unchanged.
- Default 10s matches the default batch window; fully configurable via env.
- Still binds nothing; no-mutation guard unaffected.
