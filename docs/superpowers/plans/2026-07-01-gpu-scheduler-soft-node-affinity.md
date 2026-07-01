# GPU Scheduler — Soft (Preferred) Node Affinity, Phase 1 — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: superpowers:subagent-driven-development or superpowers:executing-plans. Steps use checkbox (`- [ ]`) syntax.

**Goal:** Honor `preferredDuringSchedulingIgnoredDuringExecution` **node** affinity as a tie-breaker
in the shadow scheduler — among cost-equal placements, prefer nodes that satisfy more preferred
weight — WITHOUT ever changing which pods are admitted or the cost-optimal objective value. Per
`docs/superpowers/specs/2026-07-01-soft-affinity-scoring-design.md` (the first, cleanest slice:
node affinity only — its score is a pure function of node labels, no running-pod dependency).

**Why:** Preferred terms are ignored today; kube-scheduler's Score honors them. Modeling preferred
node affinity tightens conformance without risking cost/admission. Node affinity is the safe first
slice (no co-placement/running-pod interaction).

**Architecture — two-phase lexicographic, shadow-gated, proven-optimal-only:**
- `OptimizationWorkload` gains `soft_scores: BTreeMap<String, i64>` (node name → summed preferred
  weight if a replica lands there). `ScenarioConfig` gains `enable_soft_affinity: bool` (shadow sets
  it; planner leaves it false ⇒ zero change to the verified offline path).
- Preferred node-affinity terms (weight 1–100 + a `NodeAffinityGroup`-style selector) are parsed by
  the collector/pod_filter and carried on `PendingGpuPod`; the **builder** computes `soft_scores[n]`
  = Σ weights of preferred terms whose selector matches node `n`'s labels (reuse
  `node_affinity_expr_matches`), for each feasible node.
- `cpsat_rust::solve`: Phase 1 = today's solve. If `enable_soft_affinity` AND Phase 1 status ==
  Optimal AND some workload has non-empty `soft_scores`, run **Phase 2 on a freshly rebuilt model**
  (factor the model construction into a helper so both phases build identically): add
  `objective_expr ≤ phase1_objective_value` (integer; from `response.objective_value().round()`), fix
  each `placed` var to its Phase-1 value (`add_eq(placed, v)`), and set the objective to
  `minimize(−Σ soft_scores[n]·x[w,n])`. Extract the Phase-2 solution. Otherwise return Phase 1
  unchanged. This provably cannot change admission (placed fixed) or the cost objective (constrained
  ≤ its optimum, and Phase 2 only improves soft among cost-equal solutions).

**Tech Stack:** Rust; `model.rs`, `collector.rs`, `pod_filter.rs`, `pending_input.rs`, `cpsat_rust.rs`,
`shadow.rs`. The core-solver two-phase is the delicate part — guarded by an invariant test.

## Global Constraints

- **Invariant (the safety net):** with soft on vs off, the set of admitted workloads AND the Phase-1
  objective value are identical; only node choice among cost-equal options may differ. A unit test
  asserts this on a fixture with two cost-equal nodes differing only in preferred-affinity match.
- **Shadow-only:** gated by `enable_soft_affinity`; the offline planner never sets it (unchanged).
- **Proven-optimal-only:** skip Phase 2 unless Phase 1 is Optimal (shadow's bounded solve may return
  Feasible); then the Phase-1 placement stands.
- **Node affinity slice only:** preferred pod (anti-)affinity deferred (needs running-pod/domain eval).
- `cargo fmt` + clean clippy; new tests; binds nothing.

## Tasks

### Task 1: Model + config
- [ ] `ScenarioConfig`: add `#[serde(default)] pub enable_soft_affinity: bool`. `OptimizationWorkload`:
  add `#[serde(default)] pub soft_scores: BTreeMap<String, i64>`. `PendingGpuPod`: add
  `preferred_node_affinity: Vec<PreferredNodeTerm>` where `PreferredNodeTerm { weight: i64, exprs: Vec<NodeAffinityTerm> }` (matchExpressions only for the slice; matchFields deferred). Build; fix literals. Commit.

### Task 2: Extraction (preferred node affinity)
- [ ] In `pod_filter` (and collector if the offline path wants it — optional), parse
  `spec.affinity.node_affinity.preferred_during_scheduling_ignored_during_execution`: each entry has
  `weight` + `preference` (a NodeSelectorTerm). Map matchExpressions → `NodeAffinityTerm`s (reuse the
  existing lowering); skip a term with matchFields (deferred) — the rest still count. Populate
  `PendingGpuPod.preferred_node_affinity`. Unit tests: a pod with a weight-10 `zone In [a]` preference
  yields one PreferredNodeTerm{10, [zone In a]}. Commit.

### Task 3: Builder computes soft_scores
- [ ] In `build_pending_input`, for each emitted workload, `soft_scores[n]` = Σ over the gang's
  preferred terms (member-agreed, like anti-affinity) of `weight` where ALL the term's exprs match
  node `n`'s labels (`node_affinity_expr_matches`), for each feasible node `n`. Store on the workload.
  (Gang members must agree on preferred terms — canonical compare — else drop the soft scores, not the
  gang; simplest: use member[0]'s terms if all agree, else empty soft_scores.)
- [ ] Unit test: two feasible nodes, one matching the preference ⇒ its soft_score = weight, the
  other 0. Commit.

### Task 4: Two-phase solver
- [ ] Factor the model construction in `cpsat_rust::solve` into a helper that builds the model and
  returns the var maps + the **objective as a term list** `Vec<ObjTerm>` where
  `ObjTerm { coeff: i64, var: ObjVar }` and `ObjVar` is `Int(IntVar) | Bool(BoolVar)` with a
  `value(&response) -> i64` method. The caller builds the `LinearExpr` for `minimize` from the terms
  AND can recompute the exact objective value from them. Phase 1: build + minimize + solve (unchanged).
- [ ] **Objective value must be recomputed as exact i64 (codex — the admission weight ~1e15 exceeds
  f64 integer precision at 2^53, so `response.objective_value()` is NOT reliable):**
  `phase1_obj: i128 = Σ term.coeff as i128 * term.var.value(&response) as i128`; guard it fits i64
  (bail if not, mirroring the existing admission-weight overflow guard); use that i64.
- [ ] If `scenario.enable_soft_affinity` AND status == Optimal AND any `soft_scores` non-empty:
  rebuild the model via the helper; `add_le(objective_expr, phase1_obj)`; for each placed var
  `add_eq(placed, phase1_value)`; build `soft_expr = Σ soft_scores[n]·x[w,n]`; `minimize(-soft_expr)`;
  solve Phase 2 (the Phase-1 assignment is always a feasible witness, so Phase 2 is feasible); extract
  from the Phase-2 response. Else keep Phase 1.
- [ ] Feature-gated tests: (a) **invariant** — two cost-equal 1-node-each options, a singleton with a
  preferred term matching only node B; soft-off ⇒ some node; soft-on ⇒ node B; admitted count and
  (recomputed) cost identical both ways. (b) soft never over-admits: a quota/capacity-limited case
  admits the same set soft-on vs off. Run → commit.

### Task 5: Wire shadow + docs
- [ ] `shadow::run_one_solve`: set `enable_soft_affinity: true` in the `ScenarioConfig`. Full tests +
  clippy. README: preferred node affinity now breaks cost-ties toward matching nodes (shadow-only,
  never changes admission/cost). Update memory. Note preferred pod (anti-)affinity is the next slice.

## Self-Review Notes
- Two-phase fixes admitted set + constrains the full Phase-1 objective ⇒ cannot change admission/cost
  (invariant test enforces it).
- Proven-optimal gate avoids pinning a non-optimal incumbent (codex).
- Node-affinity slice only; pod-affinity soft scoring deferred.
- Planner path untouched (`enable_soft_affinity` false by default).
