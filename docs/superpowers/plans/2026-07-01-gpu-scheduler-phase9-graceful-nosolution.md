# GPU Scheduler — Phase 9: Distinguish Solver No-Solution from Unschedulable — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: superpowers:subagent-driven-development or superpowers:executing-plans. Steps use checkbox (`- [ ]`) syntax.

**Goal:** Make shadow traces honest when the solver returns **no solution** (timeout with no incumbent, or infeasible/error): today those pods are reported as `Unplaced { "no feasible placement found" }` / `"gang not admitted"`, which implies the scheduler *decided* they don't fit. In truth the solver never produced a result. Report a distinct reason ("solver produced no solution within the time budget") so operators can tell "couldn't solve in time" from "genuinely unschedulable."

**Why:** Phase-8 bounds the solve; under a tight budget a hard batch can return no incumbent (`cpsat_rust::solve` bails on `UNKNOWN`/infeasible → shadow catches it and records `status="error"` with an empty solution). With an empty solution, `build_decision_trace` currently labels every *submitted* pod "no feasible placement found" — misleading. This is the graceful-handling follow-up codex flagged in Phases 6/8.

**Architecture:** `run_one_solve` already knows whether `cpsat_rust::solve` returned `Ok` or `Err`. Thread a `solve_ok: bool` into `build_decision_trace`. When `solve_ok == false`, pods that were **submitted** to the solver (in `input.workloads`) but have no assignment get the "no solution within budget" reason instead of "no feasible placement" / "gang not admitted". Pods that were never submitted (filtered during input build) keep "not submitted (filtered as unschedulable)". When `solve_ok == true`, behavior is unchanged (a real solution distinguishes admitted vs genuinely-unadmitted).

**Tech Stack:** Rust; existing `scheduler::{decision, shadow}`.

## Global Constraints

- `cpsat_rust::solve` returns `Ok((solution, info))` for `Optimal`/`Feasible` (incl. time-limited incumbents), and `Err` for `UNKNOWN` (no incumbent)/`INFEASIBLE`/validation/feature-off. `run_one_solve` maps `Err` to `(Default::default(), "error".to_string())` today.
- Signal is binary: `solve_ok = solve() returned Ok`. Do not try to parse the status string (fragile).
- Only the *submitted-but-unresolved* branch changes when `solve_ok == false`. "Not submitted" (filtered during build) is a build-time fact independent of the solve, so it is unchanged. Placed pods only exist when `solve_ok == true`.
- Reason string: `"solver produced no solution within the time budget (timeout or infeasible model)"`.
- No behavior/placement change; trace-reason only. Binds nothing. Offline planner untouched.
- Unit tests without the `rust-cp-sat` feature. `cargo fmt` + clean clippy.

## File Structure

- Modify `ksolver/src/scheduler/decision.rs` — add `solve_ok: bool` to `build_decision_trace`; branch the unresolved-reason on it.
- Modify `ksolver/src/scheduler/shadow.rs` — compute `solve_ok` and pass it.

## Tasks

### Task 1: `solve_ok` in the decision builder
- [ ] **Step 1:** Add `solve_ok: bool` param to `build_decision_trace` (place it next to `solver_status`). Introduce:
```rust
    let no_solution_reason = "solver produced no solution within the time budget (timeout or infeasible model)";
```
Then:
  - Gang admitted branch: only reachable meaningfully when `solve_ok` (assignment_counts populated); unchanged.
  - Gang NOT-admitted branch (`else`): reason = if `solve_ok` { "gang not admitted (insufficient capacity for all replicas)" } else { `no_solution_reason` }.
  - Final per-pod fallback for pods absent from `placement_for` (not submitted): unchanged "not submitted ...". (These are genuinely build-filtered regardless of solve.)
  Note: single (non-gang) submitted workloads are `group_size==1`; if `solve_ok` but unadmitted they already fall through the not-admitted branch → keep that behavior; when `!solve_ok` they get `no_solution_reason`.
- [ ] **Step 2: Failing tests** in `decision.rs`:
  - `solve_ok=false`, a submitted workload with empty solution → its members' placement reason contains "no solution within the time budget" (NOT "gang not admitted" / "no feasible").
  - `solve_ok=true`, submitted workload with empty solution → reason contains "gang not admitted" (unchanged).
  - a pod NOT in `input.workloads` → "not submitted" regardless of `solve_ok`.
  Update all existing `build_decision_trace(...)` test calls to pass `solve_ok` (true for the existing placed/spread cases).
- [ ] **Step 3: Run → fail; implement; Run → pass.** `cargo test -p ksolver scheduler::decision`.
- [ ] **Step 4: Commit.**
```bash
cargo fmt
git add ksolver/src/scheduler/decision.rs
git commit -m "feat(scheduler): distinguish solver no-solution from unschedulable in traces"
```

### Task 2: Wire `solve_ok` from run_one_solve
- [ ] **Step 1:** In `shadow.rs run_one_solve`, track success:
```rust
    let (solution, status, solve_ok) = match cpsat_rust::solve(&input, &scenario) {
        Ok((sol, info)) => (sol, info.status, true),
        Err(e) => {
            warn!(error = %e, "solver produced no solution");
            (Default::default(), format!("no-solution: {e}"), false)
        }
    };
```
Pass `solve_ok` into `build_decision_trace`.
- [ ] **Step 2: Build (feature) + full tests + clippy** → green.
- [ ] **Step 3: Commit.**
```bash
cargo fmt
git add ksolver/src/scheduler/shadow.rs
git commit -m "feat(scheduler): shadow reports solver no-solution distinctly"
```

### Task 3: Verify (unit is primary)
- [ ] **Step 1:** The unit tests cover the reason branching. Optionally on-cluster: run shadow with `KSOLVER_SHADOW_SOLVE_SECS=1` against a large pending batch to try to force `UNKNOWN`; if it triggers, confirm the trace reason says "no solution within the time budget" (not "no feasible placement"). If the batch solves within 1s, note that and rely on the unit tests. Confirm nothing bound.

## Self-Review Notes
- Binary `solve_ok` signal (Ok vs Err); no status-string parsing.
- Only the submitted-but-unresolved reason changes when the solve produced nothing; "not submitted" and placed cases unchanged.
- Honest observability: separates "solver timed out / infeasible model" from "this pod is unschedulable."
- No placement/behavior change; binds nothing; planner untouched.
