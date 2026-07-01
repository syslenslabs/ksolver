# GPU Scheduler — Phase 6: Scale Benchmark Harness — Implementation Plan (v2, post-codex)

> **For agentic workers:** REQUIRED SUB-SKILL: superpowers:subagent-driven-development or superpowers:executing-plans. Steps use checkbox (`- [ ]`) syntax.

**Goal:** Measure solver-core latency (`build_pending_input` + `cpsat_rust::solve`) at scale across well-characterised scenarios (incl. the user's 50/500 jobs on 100 nodes), with a `ksolver bench` subcommand that generates deterministic synthetic clusters, validates they actually reach the solver, bounds runtime via a configurable solve time limit, and prints a timing table. Then run it and report.

**Why:** Spec release gate + explicit user request to know solve latency at realistic sizes and constraint mixes. Must produce *meaningful* numbers — not timings of empty/trivial inputs.

**Architecture:** New `scheduler::bench` module with **pure deterministic** generators producing full `NormalizedWorkload`s (requests/ext/labels/feasible) + matching `PendingGpuPod`s. A runner times build+solve, records validity (input workloads > 0), worker count, status, admitted, and asserts admitted is in an expected band per scenario. A new `ScenarioConfig.solve_time_limit_secs` (default 600 → unchanged for the planner) lets the bench cap solves so one pathological case can't pin the run for 10 minutes. Scenarios are the codex-recommended named set that separates trivial-fit, scarce, fragmented, gang-spread, gang-colocated, self-spread, and all-pairs anti-affinity stress.

**Tech Stack:** Rust; existing `model`, `scheduler::{pending_input,pod_filter}`, `cpsat_rust`; `std::time::Instant`.

## Global Constraints

- **No new deps.** Deterministic generation via index arithmetic (reproducible; no RNG).
- Generators MUST set on each `NormalizedWorkload`: `namespace,name,labels,requests(cpu/mem),extended_resource_requests(gpu),feasible_node_names,current_node`. The solver uses these (not `PendingGpuPod.gpu_request`) after `build_pending_input` scales them (codex #6). Gang members must be homogeneous (requests/ext/feasible identical) and agree on colocate + anti-affinity selectors, else the gang is silently dropped (codex #1).
- **Anti-affinity modes (codex #2), explicit:**
  - `SelfSpread`: each gang gets a UNIQUE label `job=job{i}` and selector `{job:job{i}}` → self-spread only (no cross-workload graph).
  - `GlobalStress`: every pending pod shares `{app:trainer}` and selector `{app:trainer}` → all-pairs cross anti-affinity (O(W²) pairs) — labeled a STRESS case, not generic.
  - `None`.
- **Never** combine `colocate=true` with anti-affinity on the same gang (dropped before solve — codex #3); the matrix keeps them separate.
- **Configurable solve time limit (codex #4):** add `ScenarioConfig.solve_time_limit_secs: i64` (0 → default 600, preserving planner behavior). Solver reads it. Bench sets a modest cap (e.g. 60s) so a scenario that can't prove optimality reports `Feasible` with solve_ms≈cap — itself a finding, not a hang.
- **Report worker count (codex #5):** surface `recommended_worker_count(&input)` in the table (it returns 1 for ≥96 nodes).
- **Validity:** print `pending_pods`, `input.workloads`, `anti_affinity_pairs`; flag a scenario invalid if `input.workloads == 0` when non-empty was expected.
- Runner labels output "solver-core only (build + solve; excludes collect/normalize)" (codex measurement-scope note).
- Run with `--release`. `cargo fmt` + clean clippy. Measurement only; binds nothing; no scheduler-behavior change beyond the time-limit knob (guarded by default).

## File Structure

- Modify `ksolver/src/model.rs` — add `solve_time_limit_secs: i64` to `ScenarioConfig` (+ Default).
- Modify `ksolver/src/cpsat_rust.rs` — use it (`if > 0 { that } else { 600.0 }`).
- Create `ksolver/src/scheduler/bench.rs` — generators + runner + matrix.
- Modify `ksolver/src/scheduler/mod.rs` — `pub mod bench;`.
- Modify `ksolver/src/main.rs` — `bench` subcommand.

---

## Task 1: Configurable solve time limit

**Files:** `model.rs`, `cpsat_rust.rs`.

- [ ] **Step 1:** Add `#[serde(default)] pub solve_time_limit_secs: i64,` to `ScenarioConfig`; set `solve_time_limit_secs: 0,` in its `Default`.
- [ ] **Step 2:** In `cpsat_rust::solve`, where `SatParameters { max_time_in_seconds: Some(600.0), .. }` is built, use:
```rust
        let time_limit = if scenario.solve_time_limit_secs > 0 {
            scenario.solve_time_limit_secs as f64
        } else {
            600.0
        };
        // ... max_time_in_seconds: Some(time_limit),
```
- [ ] **Step 3: Build + tests.** `cargo build -p ksolver` and `cargo test -p ksolver --features rust-cp-sat cpsat_rust` → green (default path unchanged; existing tests still pass).
- [ ] **Step 4: Commit.**
```bash
cargo fmt
git add ksolver/src/model.rs ksolver/src/cpsat_rust.rs
git commit -m "feat(solver): configurable solve_time_limit_secs (default 600)"
```

---

## Task 2: Deterministic generators

**Files:** Create `ksolver/src/scheduler/bench.rs`; inline tests.

**Interfaces:**
```rust
pub enum AntiAffinity { None, SelfSpread, GlobalStress }
pub struct BenchScenario {
    pub name: &'static str,
    pub nodes: usize,
    pub gpus_per_node: i64,
    pub jobs: usize,           // number of workloads (gangs or singletons)
    pub gang_size: usize,      // 1 = singleton
    pub colocate: bool,
    pub anti: AntiAffinity,
    pub running_fill: i64,      // GPUs pre-consumed per node by running pods (residual pressure)
    pub expect_admitted: (usize, usize), // inclusive band for validation
}
pub fn generate(s: &BenchScenario) -> (NormalizedCluster, Vec<PendingGpuPod>);
```
Rules: nodes `n{k}` cpu 64000/mem 256Gi/pods 110/gpu `gpus_per_node`; `running_fill` running pods per node (`current_node=nk`, 1 GPU each, label `app=running`). Pending: `jobs` workloads; gang `g{i}` with members `g{i}-m{j}` (unique names). Requests cpu 1000/mem 4Gi/gpu 1 per pod. `feasible_node_names` = all nodes. Labels + selectors per `anti`. `colocate` sets the flag on members. Add each pending pod's `NormalizedWorkload` (current_node "") to the cluster and a `PendingGpuPod` to the vec.

- [ ] **Step 1: Failing tests.** tiny scenario → correct pending count, matching workloads, non-empty feasible; `SelfSpread` gang → members share `{job:job0}` label+selector; `GlobalStress` → all share `{app:trainer}`.
- [ ] **Step 2: Run → fail; implement; Step 3: Run → pass.**
- [ ] **Step 4: Sanity via build.** Add a (no-feature) test: for a representative scenario, `build_pending_input(&cluster, &pending).workloads.len()` matches expectation (jobs, since gangs collapse to 1 workload each) — proves inputs survive the builder.
- [ ] **Step 5: Commit.**
```bash
cargo fmt
git add ksolver/src/scheduler/bench.rs ksolver/src/scheduler/mod.rs
git commit -m "feat(bench): deterministic generators with explicit anti-affinity modes"
```

---

## Task 3: Runner + matrix + subcommand

**Files:** `bench.rs`, `main.rs`.

**Interfaces:**
```rust
pub struct BenchResult {
    pub name: &'static str, pub nodes: usize, pub pending_pods: usize,
    pub workloads: usize, pub anti_pairs: usize, pub workers: i32,
    pub build_ms: u128, pub solve_ms: u128, pub status: String,
    pub admitted: usize, pub valid: bool,
}
pub fn run_scenario(s: &BenchScenario) -> BenchResult; // solve_time_limit_secs = 60
pub fn default_matrix() -> Vec<BenchScenario>;
pub fn run_matrix(&[BenchScenario]) -> Vec<BenchResult>;
pub fn print_table(&[BenchResult]);
```
`run_scenario`: `generate` → time `build_pending_input` → `recommended_worker_count` (report) → time `cpsat_rust::solve` with `ScenarioConfig { partial_admission:true, solve_time_limit_secs:60, ..default }` → `admitted` = workloads with any positive `assignment_counts`; `valid = workloads>0`; check `admitted` within `expect_admitted`.

**Matrix (codex-recommended, + user's sizes; gpus_per_node=8):**
- `baseline-50j-100n`: jobs 50, gang 1, expect (50,50)
- `baseline-500j-100n`: jobs 500, gang 1, expect (500,500)
- `scarce-900j-100n`: jobs 900, gang 1, expect (800,800)
- `fragmented-500j-100n`: jobs 500, gang 1, running_fill 6 (2 GPU left/node → 200 total) expect (200,200)
- `gang8-spread-125j-100n`: jobs 125, gang 8, anti None, expect (100,100)
- `gang8-colocated-125j-100n`: jobs 125, gang 8, colocate true, expect (100,100)
- `selfspread-gang8-125j-100n`: jobs 125, gang 8, anti SelfSpread, expect (100,100)
- `global-aa-stress-200j-100n`: jobs 200, gang 1, anti GlobalStress, expect (100,100)  // STRESS
- `global-aa-stress-500j-100n`: jobs 500, gang 1, anti GlobalStress, expect (100,100)  // STRESS (O(W²) pairs)

- [ ] **Step 1: Implement runner + matrix + table** (cols: name, nodes, pods, workloads, anti_pairs, workers, build_ms, solve_ms, status, admitted, valid, in-band?). Print a header noting "solver-core only; solve capped at 60s".
- [ ] **Step 2: `bench` subcommand** in `main.rs`.
- [ ] **Step 3: Build (feature) + smoke test** (feature-gated) running one tiny scenario asserts timings recorded and `valid`.
- [ ] **Step 4: Commit.**
```bash
cargo fmt
git add ksolver/src/scheduler/bench.rs ksolver/src/main.rs
git commit -m "feat(bench): scenario runner, validated matrix, bench subcommand"
```

---

## Task 4: Run and report

- [ ] **Step 1:** `cargo run --release --features rust-cp-sat -- bench 2>/dev/null` → capture the table.
- [ ] **Step 2:** Report results; flag any scenario hitting the 60s cap (status Feasible not Optimal), any `valid=false`, any `admitted` out of band, and worker counts. Summarise the latency picture per size/constraint.
- [ ] **Step 3:** Note findings (e.g. all-pairs anti-affinity stress latency, fragmented packing cost) as candidates for later work (per-pool decomposition, tighter limits) — don't "fix" here.

---

## Self-Review Notes (incl. codex fixes)

- Configurable solve time limit (codex #4) caps the run; default 600 preserves planner behavior.
- Generators set full normalized resources (codex #6); gang homogeneity + colocate/anti agreement respected so inputs aren't silently dropped (codex #1).
- Anti-affinity split into SelfSpread vs GlobalStress (codex #2); colocate never combined with anti (codex #3).
- Validity + expected-admission bands guard against timing empty/trivial inputs; worker count surfaced (codex #5).
- Output labeled "solver-core only" (measurement scope).
- Deterministic, reproducible; measurement only; binds nothing.
```
