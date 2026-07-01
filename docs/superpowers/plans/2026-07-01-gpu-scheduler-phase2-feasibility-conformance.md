# GPU Scheduler — Phase 2: Feasibility Conformance vs kube-scheduler — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: superpowers:subagent-driven-development or superpowers:executing-plans. Steps use checkbox (`- [ ]`) syntax.

**Goal:** Prove our node-feasibility logic (`feasible_on_node`) agrees with real kube-scheduler Filter decisions by comparing, per (pod, node) pair, our verdict against the kube-scheduler-simulator's — and reporting every disagreement.

**Why:** The north-star claim is that ksolver "accounts for everything." That only holds if our feasibility predicate matches the real scheduler's Filter phase. A conformance harness turns "we think we model it right" into a measured agreement rate + an explicit list of the predicates where we diverge (which become the disclosed caveats / next phases). Read-only; binds nothing on the real cluster (the simulator is a sandbox).

**Architecture:** A new `conformance` module + `conform` subcommand. For each in-scope pod and each candidate node, we get TWO verdicts: (a) ours = `feasible_on_node(pod, node, …).is_empty()`; (b) the scheduler's = present the simulator a snapshot containing exactly that one node (empty of other pods) plus the pod, run scheduling, and read whether the pod bound to that node (feasible) or came back unschedulable with a `filter-result` (infeasible). One node isolates Filter (Score is moot with a single candidate). We classify each pair as Agree / FalsePositive (we say feasible, scheduler rejects — the dangerous case) / FalseNegative (we say infeasible, scheduler accepts — we're over-conservative), aggregate a confusion matrix, and print mismatches with both reason strings. The pure classification/aggregation/report logic is unit-tested; the simulator round-trip reuses `verifier.rs`'s existing reset/import/export client and is exercised the same way verifier's payload tests are. When no simulator URL is configured, the harness reports "simulator not configured" and exits cleanly (no crash), mirroring `verifier.rs`.

**Tech Stack:** Rust; reuses `normalizer::feasible_on_node`, `verifier.rs` simulator client, `model` types, `k8s-openapi`.

## Global Constraints

- **Read-only / sandbox:** the harness only talks to the simulator (a sandbox) and reads the live cluster snapshot; it binds nothing on the real cluster. No `create`/`bind` against real kube.
- **Apples-to-apples capacity (codex #1 — critical):** the normalizer sets `NormalizedNode.effective_capacity = allocatable − DaemonSet reserve`, and `feasible_on_node` compares against `effective_capacity`. But the simulator sees an EMPTY node with raw allocatable. To avoid bogus false-negatives, the conformance builder must make BOTH sides test the same capacity: construct the `NormalizedNode` passed to `node_feasibility_reasons` with `effective_capacity = allocatable` and `reserved = 0` (clone the collected node, then overwrite those two fields), and present the simulator that same empty node with raw allocatable. DaemonSet-reserve and overcommit/headroom are separate ksolver modeling layers, NOT kube-scheduler Filter predicates, so they are intentionally excluded from Filter conformance (documented). Use `Options::default()` (no overcommit/headroom) as well.
- **Isolate Filter:** exactly one node per simulator scheduling attempt, so a successful bind ⇒ that node passed Filter; unschedulable ⇒ it failed Filter. Score/prioritization cannot affect a single-node decision.
- **Known-unmodeled predicates are expected divergences, not bugs:** pod affinity/anti-affinity, topology spread, priority/preemption are known-unmodeled in `feasible_on_node`; the report must BUCKET expected-divergence pods separately (pods carrying those constructs) from unexpected mismatches, so the signal isn't drowned out. Pods with none of those constructs must match exactly (that's the hard assertion).
- **Graceful when simulator absent:** if `KSOLVER_SCHEDULER_SIMULATOR_URL`/`SCHEDULER_SIMULATOR_URL` is unset, print a clear "conformance skipped: simulator URL not configured" and exit 0. Never panic.
- `cargo fmt` + clean clippy; pure logic unit-tested; no network in unit tests.

## File Structure

- Modify `ksolver/src/normalizer.rs` — expose feasibility: add `pub(crate) fn node_feasibility_reasons(pod, node, volumes_by_claim, options) -> Vec<String>` delegating to the existing `feasible_on_node` (keep the private fn; just a visible wrapper).
- Modify `ksolver/src/verifier.rs` — extract a reusable `pub(crate) async fn schedule_one(simulator_url, payload) -> Result<SimulatorExportPayload>` (or expose the existing reset/import/export sequence) so the conformance module can drive a single scheduling attempt without duplicating the HTTP client. No behavior change to existing verify paths.
- Create `ksolver/src/conformance.rs` — pure classification + aggregation + report types, the single-node snapshot builder, and the orchestration `run_conformance(...)`.
- Modify `ksolver/src/main.rs` — add a `conform` subcommand.
- Modify `ksolver/src/lib.rs` (or wherever modules are declared) — declare `mod conformance;`.
- Modify `README.md` — document the `conform` subcommand + simulator URL env.

## Tasks

### Task 1: Expose feasibility + classification core (pure, TDD)
- [ ] In `normalizer.rs`, add `pub(crate) fn node_feasibility_reasons(pod: &Pod, node: &NormalizedNode, volumes_by_claim: &BTreeMap<String, VolumeAttachment>, options: &Options) -> Vec<String>` that just calls `feasible_on_node(...)`. (Thin wrapper so the private fn stays put.)
- [ ] Create `conformance.rs` with pure types:
```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Verdict { Agree, FalsePositive, FalseNegative } // FP = we feasible, sched rejects

pub fn classify(ours_feasible: bool, scheduler_feasible: bool) -> Verdict {
    match (ours_feasible, scheduler_feasible) {
        (true, true) | (false, false) => Verdict::Agree,
        (true, false) => Verdict::FalsePositive,
        (false, true) => Verdict::FalseNegative,
    }
}

#[derive(Debug, Default, Clone)]
pub struct ConfusionMatrix { pub agree: usize, pub false_positive: usize, pub false_negative: usize }
impl ConfusionMatrix {
    pub fn record(&mut self, v: Verdict) { /* increment */ }
    pub fn total(&self) -> usize { self.agree + self.false_positive + self.false_negative }
    pub fn agreement_rate(&self) -> f64 { if self.total()==0 {1.0} else {self.agree as f64 / self.total() as f64} }
}
```
- [ ] Unit tests: `classify` covers all four combinations; `ConfusionMatrix::record`/`agreement_rate` (incl. empty → 1.0). Run → commit.

### Task 2: Single-node snapshot builder (pure, TDD)
- [ ] In `conformance.rs`, add a builder that, given the collected cluster snapshot, a pod, and one node, produces the simulator import payload containing ONLY that node and that pod (as an unscheduled verification clone — reuse `verifier::clone_as_unscheduled_verification_pod` semantics; if private, expose `pub(crate)`), plus the PVs/PVCs/StorageClasses/PriorityClasses/Namespaces the pod references. Include the node's labels/taints/allocatable verbatim; the simulator node carries RAW allocatable and NO other pods.
- [ ] Add `fn conformance_node(collected: &NormalizedNode) -> NormalizedNode`: clones the node but sets `effective_capacity = allocatable` and `reserved = ResourceList::default()` (codex #1), so `node_feasibility_reasons` tests the same raw allocatable the empty simulator node exposes. Unit-test that the returned node's `effective_capacity == allocatable` and `reserved` is zeroed.
- [ ] A `ExpectedDivergence` helper: `fn pod_has_unmodeled_constructs(pod) -> bool` (required pod affinity/anti-affinity or DoNotSchedule topology spread present) — reuse the shadow `pod_filter::unmodeled_constraints` logic or the raw affinity checks. Used to bucket expected divergences.
- [ ] Unit tests: builder emits exactly one node + one unscheduled pod with node_name cleared; `pod_has_unmodeled_constructs` true for a pod with required podAntiAffinity, false for a plain pod. Run → commit.

### Task 3: Simulator client reuse
- [ ] In `verifier.rs`, factor the reset→import→poll-export sequence into `pub(crate) async fn schedule_snapshot(simulator_url: &str, payload: SimulatorImportPayload) -> anyhow::Result<SimulatorExportPayload>` and have the existing verify path call it (no behavior change). Expose `SimulatorImportPayload`/`SimulatorExportPayload` as `pub(crate)` if not already.
- [ ] In `conformance.rs`, add `scheduler_feasible(export, node_name) -> (bool, Option<String>)`: feasible iff the pod's `selected-node` annotation == node_name **OR** `spec.nodeName == node_name` (codex #2 — match verifier's bind fallback, else a bind reported only via `spec.nodeName` yields a bogus false-negative); else infeasible with the `filter-result` string (if present) as the reason. Unit-test this parser against synthetic `SimulatorExportPayload`s: (a) feasible via selected-node annotation; (b) feasible via spec.nodeName only; (c) infeasible with filter-result set — no network. Run → commit.

### Task 4: Orchestration + subcommand
- [ ] In `conformance.rs`, `pub async fn run_conformance(kubeconfig, cluster_name, simulator_url, sample) -> ConformanceReport`: collect snapshot (reuse `collector`), normalize once (for `feasible_node_names` context + volumes map), select pods (default: all pending pods, capped by `sample` for cost), and for each (pod, node) pair: compute ours via `node_feasibility_reasons(pod, &conformance_node(node), …)` (raw-allocatable node per codex #1), scheduler via `schedule_snapshot` + `scheduler_feasible`, `classify`, and record into the matrix (bucketed expected-divergence vs strict). Collect mismatch details (pod, node, ours_reasons, scheduler_reason).
- [ ] `ConformanceReport` prints: totals, agreement rate (strict bucket), and up to N mismatches with both reasons. FalsePositives (we feasible, scheduler rejects) listed first — those are the dangerous ones.
- [ ] `main.rs`: `conform [--simulator <url>] [--sample <n>] [--cluster <name>] [--kubeconfig <path>]`. If no simulator URL (arg or env), print "conformance skipped: simulator URL not configured" and exit 0.
- [ ] Guard cost: default `--sample` to a small number (e.g. 20 pods) and `log()` how many (pod,node) attempts will run; document that each attempt is one simulator round-trip.
- [ ] Build + clippy + fmt. Commit.

### Task 5: Docs + verify
- [ ] README: add a "Feasibility conformance" subsection — what `conform` does, the simulator URL env, the one-node-isolates-Filter method, and that expected-divergence pods (affinity/spread/priority) are bucketed separately.
- [ ] Verify: unit tests all pass; `cargo run -- conform` with NO simulator prints the graceful skip and exits 0 (the assertable path without external infra). If a kube-scheduler-simulator is available, run against `kind-solver-lab` and record the agreement rate + any FalsePositives in the memory status file; otherwise note in memory that live conformance needs a simulator deployment and the harness + graceful-skip are verified.

## Self-Review Notes
- Read-only/sandbox; binds nothing on the real cluster.
- One node per attempt isolates Filter from Score.
- `conformance_node` (effective_capacity=allocatable, reserved=0) + `Options::default()` + empty simulator node ⇒ both sides test the same raw allocatable (codex #1 fix); DaemonSet-reserve/overcommit/headroom excluded as non-Filter layers.
- Known-unmodeled predicates bucketed as expected divergence; plain pods must match exactly.
- Graceful skip when no simulator; never panics.
- `scheduler_feasible` accepts a bind via selected-node annotation OR spec.nodeName (codex #2), matching verifier's fallback.
- Pure logic unit-tested (classify, matrix, export parser, single-node builder); network path reused from verifier and exercised like its existing tests.
