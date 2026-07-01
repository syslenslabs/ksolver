# GPU Scheduler — Phase 6: Scale Benchmark Harness — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: superpowers:subagent-driven-development or superpowers:executing-plans. Steps use checkbox (`- [ ]`) syntax.

**Goal:** Measure how long the shadow scheduler's core path (`build_pending_input` + `cpsat_rust::solve`) takes at scale, across scenarios the user named — 50 jobs / 100 nodes, 500 jobs / 100 nodes — and constraint mixes (plain, gangs, co-located gangs, anti-affinity, mixed). Deliver a reusable `ksolver bench` subcommand that generates synthetic clusters deterministically and prints a timing table, then run the matrix and report.

**Why:** The spec calls scale benchmarking a release gate; the user explicitly wants to know solve latency at realistic sizes and under different constraints. This validates the ~3-min budget assumption and surfaces where CP-SAT slows down.

**Architecture:** A new `scheduler::bench` module with **pure, deterministic** generators (no RNG — index-derived variety, so runs are reproducible): `gen_cluster(cfg) -> NormalizedCluster` (N GPU nodes, optional running pods for residual pressure) and `gen_pending(cfg) -> Vec<PendingGpuPod>` with matching `NormalizedWorkload`s in the cluster (feasible sets, labels, gang keys, co-location, anti-affinity selectors). A runner times `build_pending_input` then `cpsat_rust::solve` (with `partial_admission`), reporting nodes / pending pods / solver workloads / status / admitted / build_ms / solve_ms. Exposed via a `bench` subcommand (real timings require the `rust-cp-sat` feature; without it, solve is a stub and the harness prints that).

**Tech Stack:** Rust; existing `model`, `scheduler::{pending_input, pod_filter}`, `cpsat_rust`; `std::time::Instant`.

## Global Constraints

- **No new dependencies.** Deterministic generation via index arithmetic (e.g. spread pods across nodes by modulo), not `rand` (also keeps runs reproducible for comparison).
- Generators must produce inputs `build_pending_input` accepts: every pending `PendingGpuPod` needs a matching `NormalizedWorkload` in `cluster.workloads` with `current_node == ""`, non-empty `feasible_node_names`, `requests`, `labels`. Running pods (residual pressure) get `current_node` set.
- The real solve requires `--features rust-cp-sat` + OR-Tools. The `bench` subcommand runs regardless; without the feature `cpsat_rust::solve` returns an error → report `status=unavailable`.
- The solver has a hardcoded `max_time_in_seconds: 600`. A scenario that can't prove optimality may run up to ~600s; the runner prints wall time so slow scenarios are visible. Keep the default matrix modest; allow scaling via args.
- Timing is wall-clock via `Instant`; report milliseconds. Run benches with `--release` for representative numbers (note this in the run step).
- `cargo fmt` + clean clippy. This is a measurement tool — it does not touch scheduler behavior or bind anything.

## File Structure

- Create `ksolver/src/scheduler/bench.rs` — generators + runner + scenario matrix.
- Modify `ksolver/src/scheduler/mod.rs` — `pub mod bench;`.
- Modify `ksolver/src/main.rs` — `bench` subcommand.

---

## Task 1: Synthetic generators

**Files:** Create `ksolver/src/scheduler/bench.rs`; inline tests.

**Interfaces:**
- `pub struct BenchScenario { pub name: String, pub nodes: usize, pub gpus_per_node: i64, pub jobs: usize, pub gang_size: usize, pub colocate: bool, pub anti_affinity: bool, pub running_fill_frac: f64 }`
- `pub fn generate(s: &BenchScenario) -> (NormalizedCluster, Vec<PendingGpuPod>)` — deterministic.

Generation rules:
- Nodes `n{0..N}` with `effective_capacity` (cpu 64000m, mem 256Gi, pods 110) and `nvidia.com/gpu = gpus_per_node`.
- Optional running pods to consume `running_fill_frac` of each node's GPUs (as `NormalizedWorkload` with `current_node` set, labelled `app=running`), to exercise residual capacity.
- Pending: `jobs` workloads. If `gang_size > 1`, each job is a gang of `gang_size` pods (gang key `ns/job{i}`), each requesting 1 GPU; else singletons requesting 1 GPU. `feasible_node_names` = all nodes (worst-case edge count) unless noted. Labels `app=trainer`; if `anti_affinity`, give each pending pod a hostname self-anti-affinity selector `{app:trainer}` (drives spread). `colocate` sets the co-location flag.
- Each pending pod gets a `NormalizedWorkload` (current_node "", requests cpu 1000m/mem 4Gi/1 GPU, labels, feasible_node_names) added to the cluster, plus a `PendingGpuPod` (uid/ns/name/gpu 1/gang_key/colocate/aa selectors/empty unmodeled).

- [ ] **Step 1: Failing test.** A tiny scenario (nodes 4, jobs 6, gang_size 1) → `generate` yields 6 pending pods and a cluster whose `workloads` contains 6 matching entries with non-empty feasible sets; with `gang_size=3, jobs=2` → 6 pending pods across 2 gang keys.
- [ ] **Step 2: Run → fail.** `cargo test -p ksolver scheduler::bench`.
- [ ] **Step 3: Implement `BenchScenario` + `generate`.** Deterministic; document the index math.
- [ ] **Step 4: Run → pass.**
- [ ] **Step 5: Commit.**
```bash
cargo fmt
git add ksolver/src/scheduler/bench.rs ksolver/src/scheduler/mod.rs
git commit -m "feat(bench): deterministic synthetic cluster/pending generators"
```

---

## Task 2: Runner + `bench` subcommand

**Files:** `bench.rs`, `main.rs`.

**Interfaces:**
- `pub struct BenchResult { pub scenario: String, pub nodes: usize, pub pending_pods: usize, pub solver_workloads: usize, pub build_ms: u128, pub solve_ms: u128, pub status: String, pub admitted: usize }`
- `pub fn run_scenario(s: &BenchScenario) -> BenchResult` — times `build_pending_input` then `cpsat_rust::solve` (scenario `partial_admission: true`); computes `admitted` = workloads with any positive `assignment_counts`.
- `pub fn default_matrix() -> Vec<BenchScenario>` — the user's scenarios + constraint variants:
  - `50j/100n plain` (jobs 50, gang 1), `500j/100n plain` (jobs 500, gang 1)
  - `50j/100n gang8`, `500j/100n gang8` (gang_size 8)
  - `50j/100n colocated-gang8`, `500j/100n colocated-gang8`
  - `50j/100n anti-affinity`, `500j/100n anti-affinity`
  - a "mixed + residual" variant (running_fill_frac 0.5)
  (gpus_per_node 8 throughout unless a variant needs otherwise.)
- `pub fn run_matrix(scenarios: &[BenchScenario]) -> Vec<BenchResult>` and a `print_table(results)`.

- [ ] **Step 1: Implement runner + matrix + table printer** (columns: scenario, nodes, pods, workloads, build_ms, solve_ms, status, admitted).
- [ ] **Step 2: `bench` subcommand.** In `main.rs`, `Some("bench") => { … run default_matrix(), print table … }`. Accept optional args later; default matrix for now.
- [ ] **Step 3: Build (feature).** `cargo build -p ksolver --features rust-cp-sat` → compiles.
- [ ] **Step 4: Smoke test.** A `#[cfg(all(test, feature = "rust-cp-sat"))]` test running one tiny scenario asserts `build_ms`/`solve_ms` are recorded and status is a solved state.
- [ ] **Step 5: Commit.**
```bash
cargo fmt
git add ksolver/src/scheduler/bench.rs ksolver/src/main.rs
git commit -m "feat(bench): scenario runner and bench subcommand"
```

---

## Task 3: Run the matrix and report

- [ ] **Step 1:** `cargo run --release --features rust-cp-sat -- bench` and capture the table.
- [ ] **Step 2:** Record results (build_ms/solve_ms per scenario) in the commit message / a short results note; call out any scenario approaching the 600s cap or with surprising latency.
- [ ] **Step 3:** If a scenario is pathologically slow, note it as a finding (candidate for per-pool decomposition / tighter time limit) rather than "fixing" here.

---

## Self-Review Notes

- Pure, deterministic generators (no RNG) → reproducible comparisons.
- Measures the real shadow path (`build_pending_input` + `cpsat_rust::solve`) at the user's requested sizes and constraint mixes.
- Measurement only — no change to scheduler behavior; binds nothing.
- Release-mode note included so numbers are representative.
- Findings (slow scenarios) are reported, not silently hidden; solver's 600s cap acknowledged.
```
