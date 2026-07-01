# GPU Scheduler — Phase 5d: Unmodeled-Constraint Caveats — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: superpowers:subagent-driven-development or superpowers:executing-plans. Steps use checkbox (`- [ ]`) syntax.

**Goal:** Make shadow decisions **honest about their known limits.** The feasibility path enforces nodeSelector, taints/tolerations, resource+GPU fit, volume topology, and required *node* affinity — but does NOT model required **pod affinity / pod anti-affinity / topology-spread** (the offline solver only warns about these). So a `placed` recommendation could violate one of those. This phase surfaces a per-pod **caveat** on any decision for a pod carrying such an unmodeled constraint, plus a metric, so operators can trust (or discount) each recommendation.

**Why now:** It's a real correctness/trust gap discovered while reviewing feasibility. Fully *modeling* pod affinity/anti-affinity/spread is a large, separate effort; disclosing the gap per-decision is bounded, low-risk, and immediately valuable for a shadow/advisory tool (the spec's explainability pillar).

**Architecture:** `classify` inspects the pod's `spec.affinity` (pod affinity/anti-affinity, required-during-scheduling) and `spec.topologySpreadConstraints`, recording which unmodeled constraints are present on `PendingGpuPod`. The decision builder attaches human-readable `caveats` to each `PodDecision`. A counter tracks caveated decisions. No solver/feasibility change — this is disclosure only.

**Tech Stack:** Rust; k8s-openapi v1_31 (`corev1::{Affinity, PodAffinity, PodAntiAffinity, TopologySpreadConstraint}`); existing `scheduler::{pod_filter, decision, trace, shadow}`, `metrics`.

## Global Constraints

- Verified facts:
  - `feasible_on_node` (normalizer) covers nodeSelector, taints/tolerations, cpu/mem/disk fit, extended (GPU) resources, volume topology, required node affinity. It does NOT cover pod affinity/anti-affinity or topology spread (the normalizer emits `warnings` that "solver does not model them exactly yet").
  - `corev1::PodSpec` has `affinity: Option<Affinity>` and `topology_spread_constraints: Option<Vec<TopologySpreadConstraint>>`. `Affinity` has `pod_affinity: Option<PodAffinity>` and `pod_anti_affinity: Option<PodAntiAffinity>`; each has `required_during_scheduling_ignored_during_execution: Option<Vec<PodAffinityTerm>>`. Node affinity is already modeled, so it is NOT a caveat.
  - `PodDecision` (in `trace.rs`) currently has `{uid, namespace, name, gpu_request, placement}`. Add `caveats: Vec<String>` (serde default so old traces deserialize).
  - `build_decision_trace(sequence, pending, input, solution, ...)` — `pending: &[PendingGpuPod]` is the source of caveats.
- Disclosure only: no change to feasibility, the solver, or placement. A caveat never changes placed/unplaced — it annotates it.
- Caveats are most meaningful on `placed` decisions (an unplaced pod's caveat is moot) — but attach to all decisions for completeness; tests assert on placed.
- Unit tests pass without the `rust-cp-sat` feature. `cargo fmt` + clean clippy. Still binds nothing.

## File Structure

- Modify `ksolver/src/scheduler/pod_filter.rs` — add `unmodeled_constraints: Vec<String>` to `PendingGpuPod`; `classify` populates it.
- Modify `ksolver/src/scheduler/trace.rs` — add `caveats: Vec<String>` to `PodDecision`.
- Modify `ksolver/src/scheduler/decision.rs` — copy caveats into each `PodDecision`.
- Modify `ksolver/src/metrics.rs` — add `ksolver_shadow_caveated_total` counter + accessor.
- Modify `ksolver/src/scheduler/shadow.rs` — increment the counter for caveated placed decisions.

---

## Task 1: Detect unmodeled constraints in classify

**Files:** `pod_filter.rs` (+ tests). Update every `PendingGpuPod` literal (pending_input.rs, decision.rs test helpers) to add `unmodeled_constraints: vec![]`.

**Interfaces:**
- `PendingGpuPod` gains `pub unmodeled_constraints: Vec<String>` — human-readable names of present-but-unmodeled scheduling constraints (subset of `["pod affinity", "pod anti-affinity", "topology spread"]`), sorted, empty when none.
- `classify` populates it (order: pod affinity, pod anti-affinity, topology spread).

- [ ] **Step 1: Failing tests.** In `pod_filter.rs` tests:
```rust
    #[test]
    fn detects_pod_anti_affinity_and_spread() {
        use k8s_openapi::api::core::v1 as corev1;
        let mut p = pod("ksolver", None, Some("Pending"),
            vec![container("main", Some(q(&[("nvidia.com/gpu", "1")])), None)], vec![]);
        if let Some(spec) = p.spec.as_mut() {
            spec.affinity = Some(corev1::Affinity {
                pod_anti_affinity: Some(corev1::PodAntiAffinity {
                    required_during_scheduling_ignored_during_execution: Some(vec![
                        corev1::PodAffinityTerm { topology_key: "kubernetes.io/hostname".to_string(), ..Default::default() }
                    ]),
                    ..Default::default()
                }),
                ..Default::default()
            });
            spec.topology_spread_constraints = Some(vec![corev1::TopologySpreadConstraint {
                max_skew: 1, topology_key: "zone".to_string(), when_unsatisfiable: "DoNotSchedule".to_string(), ..Default::default()
            }]);
        }
        let got = classify(&p, &cfg()).expect("classify");
        assert!(got.unmodeled_constraints.contains(&"pod anti-affinity".to_string()));
        assert!(got.unmodeled_constraints.contains(&"topology spread".to_string()));
    }

    #[test]
    fn no_caveats_for_plain_pod() {
        let p = pod("ksolver", None, Some("Pending"),
            vec![container("main", Some(q(&[("nvidia.com/gpu", "1")])), None)], vec![]);
        assert!(classify(&p, &cfg()).unwrap().unmodeled_constraints.is_empty());
    }

    #[test]
    fn schedule_anyway_spread_is_not_a_caveat() {
        use k8s_openapi::api::core::v1 as corev1;
        let mut p = pod("ksolver", None, Some("Pending"),
            vec![container("main", Some(q(&[("nvidia.com/gpu", "1")])), None)], vec![]);
        if let Some(spec) = p.spec.as_mut() {
            spec.topology_spread_constraints = Some(vec![corev1::TopologySpreadConstraint {
                max_skew: 1, topology_key: "zone".to_string(), when_unsatisfiable: "ScheduleAnyway".to_string(), ..Default::default()
            }]);
        }
        assert!(classify(&p, &cfg()).unwrap().unmodeled_constraints.is_empty());
    }
```

- [ ] **Step 2: Run → fail.** `cargo test -p ksolver scheduler::pod_filter` → FAIL.

- [ ] **Step 3: Implement.** Add the field to `PendingGpuPod`. In `classify`, before constructing the result:
```rust
    let mut unmodeled_constraints = Vec::new();
    if let Some(spec) = pod.spec.as_ref() {
        if let Some(aff) = spec.affinity.as_ref() {
            let has_terms = |t: &Option<Vec<k8s_openapi::api::core::v1::PodAffinityTerm>>| {
                t.as_ref().map(|v| !v.is_empty()).unwrap_or(false)
            };
            if aff.pod_affinity.as_ref().map(|a| has_terms(&a.required_during_scheduling_ignored_during_execution)).unwrap_or(false) {
                unmodeled_constraints.push("pod affinity".to_string());
            }
            if aff.pod_anti_affinity.as_ref().map(|a| has_terms(&a.required_during_scheduling_ignored_during_execution)).unwrap_or(false) {
                unmodeled_constraints.push("pod anti-affinity".to_string());
            }
        }
        // Only DoNotSchedule spread is a HARD feasibility constraint; ScheduleAnyway
        // is soft (scoring) and must NOT be flagged as a "could violate" caveat.
        let hard_spread = spec
            .topology_spread_constraints
            .as_ref()
            .map(|v| v.iter().any(|c| c.when_unsatisfiable == "DoNotSchedule"))
            .unwrap_or(false);
        if hard_spread {
            unmodeled_constraints.push("topology spread".to_string());
        }
    }
```
and add `unmodeled_constraints` to the returned struct. Fix the `PendingGpuPod` literals in `pending_input.rs`/`decision.rs` test helpers (`unmodeled_constraints: vec![]`).

- [ ] **Step 4: Run → pass.** `cargo test -p ksolver scheduler::pod_filter` → PASS.
- [ ] **Step 5: Commit.**
```bash
cargo fmt
git add ksolver/src/scheduler/pod_filter.rs ksolver/src/scheduler/pending_input.rs ksolver/src/scheduler/decision.rs
git commit -m "feat(scheduler): detect unmodeled pod affinity/anti-affinity/spread in classify"
```

---

## Task 2: Carry caveats into PodDecision

**Files:** `trace.rs`, `decision.rs` (+ tests).

- [ ] **Step 1: Add field.** In `trace.rs` `PodDecision`, add `#[serde(default)] pub caveats: Vec<String>,`. Update the `PodDecision` literal in `trace.rs` tests (`caveats: vec![]`). Add a serde backcompat test: deserialize a `PodDecision` JSON that omits `caveats` and assert it yields an empty vec (proves `#[serde(default)]`).

- [ ] **Step 2: Failing test.** In `decision.rs` tests, add a case where a pending pod has `unmodeled_constraints = vec!["pod anti-affinity".into()]` and is placed; assert the resulting `PodDecision.caveats` contains it. (Extend the `ppod` helper to accept caveats, or set the field on a built pod.)

- [ ] **Step 3: Run → fail.** `cargo test -p ksolver scheduler::decision` → FAIL.

- [ ] **Step 4: Implement.** In `build_decision_trace`, when pushing each `PodDecision`, set `caveats: p.unmodeled_constraints.clone()`.

- [ ] **Step 5: Run → pass.** `cargo test -p ksolver scheduler::decision scheduler::trace` → PASS.
- [ ] **Step 6: Commit.**
```bash
cargo fmt
git add ksolver/src/scheduler/trace.rs ksolver/src/scheduler/decision.rs
git commit -m "feat(scheduler): attach unmodeled-constraint caveats to pod decisions"
```

---

## Task 3: Caveat metric + shadow wiring

**Files:** `metrics.rs`, `shadow.rs`.

- [ ] **Step 1: Metric.** In `metrics.rs`, add `SHADOW_CAVEATED: IntCounter` (`ksolver_shadow_caveated_total`, "Placed shadow decisions carrying an unmodeled-constraint caveat"), register it in `register_metrics` via `register_ignoring_dup`, and add `pub fn inc_shadow_caveated(n: u64)`. Extend the existing `shadow_metric_tests` render assertion to include the new metric.

- [ ] **Step 2: Wire.** In `shadow.rs` `run_shadow`, after building the trace, count decisions that are `Placed` and have non-empty `caveats`, and call `metrics::inc_shadow_caveated(that_count)`; include a `caveated` field in the existing `info!` log line.

- [ ] **Step 3: Build + test + clippy.** `cargo build -p ksolver --features rust-cp-sat`; `cargo test -p ksolver`; `cargo clippy -p ksolver --features rust-cp-sat --all-targets` → green.
- [ ] **Step 4: Commit.**
```bash
cargo fmt
git add ksolver/src/metrics.rs ksolver/src/scheduler/shadow.rs
git commit -m "feat(scheduler): count and log caveated shadow decisions"
```

---

## Task 4: Cluster verify

- [ ] **Step 1:** On `kind-solver-lab`, add a GPU node; create a pending ksolver GPU pod with a required `podAntiAffinity` (topologyKey `kubernetes.io/hostname`). Confirm the trace decision for it is `placed` (feasibility unchanged) but carries `caveats: ["pod anti-affinity"]`, and `curl /metrics | grep ksolver_shadow_caveated_total` is ≥ 1.
- [ ] **Step 2:** Confirm `.spec.nodeName` empty (binds nothing). Clean up.

---

## Self-Review Notes

- Disclosure only: feasibility/solver/placement unchanged; a caveat annotates, never alters, a decision.
- Node affinity is already modeled → not a caveat; only pod affinity, pod anti-affinity, topology spread are flagged.
- New serde fields default-empty → backward compatible traces.
- Addresses the real gap (offline solver only warns about these) at the per-decision level; full modeling of pod affinity/anti-affinity/spread remains a deferred, larger phase. **Caveats disclose but do NOT close the correctness gap** (codex #7) — a future phase should actually enforce required pod anti-affinity against existing pods; this phase only makes the limitation honest.
- Topology-spread caveat is raised ONLY for `DoNotSchedule` (hard) constraints; `ScheduleAnyway` (soft/scoring) is not flagged (codex #3).
- Node affinity is already modeled → not a caveat; only pod affinity, pod anti-affinity, and hard topology spread are flagged.
- New serde fields default-empty → backward compatible; a round-trip test proves it.
- Still binds nothing; no-mutation guard unaffected.
```
