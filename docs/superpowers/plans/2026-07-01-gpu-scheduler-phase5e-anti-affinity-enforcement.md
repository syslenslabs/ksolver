# GPU Scheduler — Phase 5e: Best-Effort Pod Anti-Affinity Node Exclusion — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: superpowers:subagent-driven-development or superpowers:executing-plans. Steps use checkbox (`- [ ]`) syntax.

**Goal:** Make shadow recommendations *better* on required pod anti-affinity without overclaiming: for the common case (`topologyKey: kubernetes.io/hostname`, `matchLabels`, same namespace), **exclude nodes that already host a matching running pod** from a pending pod's feasible set, so shadow stops recommending obviously-violating placements. **Keep the Phase-5d caveat** for any pod with required anti-affinity, because two residual gaps remain unmodeled: *same-batch* anti-affinity (two pending pods placed together) and *symmetry* (an existing pod's anti-affinity forbidding the new pod). This is a best-effort correctness improvement, honestly disclosed — not full enforcement.

**Why now:** Codex flagged closing the 5d gap as highest-value, but also showed that "enforce + drop caveat" would be misleading given same-batch/symmetry. Best-effort exclusion + retained caveat captures most of the value (no clearly-invalid recommendations) while staying honest.

**Architecture:** `classify` extracts each pod's *modeled* anti-affinity host selectors (hostname topologyKey + non-empty `matchLabels` + no `matchExpressions` + no cross-namespace scoping) onto `PendingGpuPod`, computed from the raw `corev1::PodAffinityTerm` (NOT the collector's lossy `AffinityTerm`). The 5d caveat logic is **unchanged** (any required anti-affinity still caveated). `NormalizedWorkload` gains `labels` so the shadow builder can see running pods' labels. `build_pending_input` drops, from each pending pod/gang's feasible nodes, nodes hosting a matching same-namespace running pod. Shadow-local; the normalizer only gains a `labels` passthrough.

**Tech Stack:** Rust; k8s-openapi v1_31 (`corev1::{PodAffinityTerm, LabelSelector, LabelSelectorRequirement}`); existing `model`, `normalizer`, `scheduler::{pod_filter, pending_input}`.

## Global Constraints

- Verified facts:
  - Build selectors from the **raw** `corev1::Pod` in `classify` (the collector's model `AffinityTerm` drops `matchExpressions`, so never model from it).
  - `NormalizedWorkload` has no `labels` today (fields jump `name` → `owner_kind`); add it. `Pod.labels` exists on the model. Normalizer constructs `NormalizedWorkload` in the `for pod in modeled_pods` loop (~L641-666).
  - CP-SAT only creates assignment vars for `workload.feasible_nodes`, so removing a node there fully prevents placement on it. (Confirmed.)
  - Running pods = `NormalizedWorkload` with `current_node != ""`.
  - **Modeled (enforced) term** = raw `PodAffinityTerm` with `topology_key == "kubernetes.io/hostname"`, `label_selector.match_labels` non-empty, `label_selector.match_expressions` empty/none, `namespaces` empty/none, `namespace_selector` none. Empty matchLabels (incl. matchExpressions-only) is NOT modeled (avoids over-excluding / match-all). Non-hostname topologyKey is NOT modeled.
- **The 5d caveat is retained unchanged.** Any pod whose `podAntiAffinity.requiredDuringScheduling...` is non-empty still gets the "pod anti-affinity" caveat, because enforcement is partial (same-batch and symmetry are not modeled). Do NOT drop the caveat for modeled terms.
- **Documented residual gaps (still caveated):** anti-affinity among simultaneously-pending pods (solver `anti_affinity_pairs` stays empty); anti-affinity *symmetry* from existing pods' terms. This phase does not enforce those.
- Shadow-local: offline planner feasibility unchanged (normalizer only gains a `labels` passthrough).
- Unit tests pass without the `rust-cp-sat` feature. `cargo fmt` + clean clippy. Still binds nothing.

## File Structure

- Modify `ksolver/src/model.rs` — add `labels: BTreeMap<String,String>` to `NormalizedWorkload`.
- Modify `ksolver/src/normalizer.rs` — populate `labels` from the pod (passthrough).
- Modify `ksolver/src/scheduler/pod_filter.rs` — add `anti_affinity_host_selectors: Vec<BTreeMap<String,String>>` to `PendingGpuPod`; classify extracts modeled terms. Caveat logic unchanged.
- Modify `ksolver/src/scheduler/pending_input.rs` — exclude anti-affinity-violating nodes; gang members must agree on selectors (homogeneity), no lossy union.

---

## Task 1: `labels` on NormalizedWorkload

**Files:** `model.rs`, `normalizer.rs` (+ any literal/test updates).

- [ ] **Step 1:** Add `#[serde(default)] pub labels: BTreeMap<String, String>,` to `NormalizedWorkload` (after `name`).
- [ ] **Step 2:** In the normalizer's `NormalizedWorkload` construction, set `labels: pod.labels.clone(),`.
- [ ] **Step 3: Build.** `cargo build -p ksolver` → compiles (fix any full `NormalizedWorkload` literals in tests needing `labels`).
- [ ] **Step 4: Commit.**
```bash
cargo fmt
git add ksolver/src/model.rs ksolver/src/normalizer.rs
git commit -m "feat(model): carry pod labels on NormalizedWorkload"
```

---

## Task 2: Extract modeled anti-affinity host selectors in classify

**Files:** `pod_filter.rs` (+ tests). Update `PendingGpuPod` literals (pending_input.rs, decision.rs test helpers) to add `anti_affinity_host_selectors: vec![]`.

**Interfaces:**
- `PendingGpuPod` gains `pub anti_affinity_host_selectors: Vec<BTreeMap<String,String>>` — matchLabels maps of *modeled* hostname terms (empty when none/unmodeled).
- `classify` populates it from raw `spec.affinity.pod_anti_affinity.required_during_scheduling_ignored_during_execution`, per the modeled-term rule. **Caveat logic is unchanged** (5d still caveats any required anti-affinity).

- [ ] **Step 1: Failing tests.** Add a helper that can attach both matchLabels and matchExpressions terms:
```rust
    fn set_anti_affinity(pod: &mut corev1::Pod, terms: Vec<corev1::PodAffinityTerm>) {
        if let Some(spec) = pod.spec.as_mut() {
            spec.affinity = Some(corev1::Affinity {
                pod_anti_affinity: Some(corev1::PodAntiAffinity {
                    required_during_scheduling_ignored_during_execution: Some(terms),
                    ..Default::default()
                }),
                ..Default::default()
            });
        }
    }
    fn term(topology_key: &str, match_labels: &[(&str,&str)], with_expr: bool) -> corev1::PodAffinityTerm {
        use k8s_openapi::apimachinery::pkg::apis::meta::v1::{LabelSelector, LabelSelectorRequirement};
        let ml: std::collections::BTreeMap<String,String> = match_labels.iter().map(|(k,v)| (k.to_string(), v.to_string())).collect();
        corev1::PodAffinityTerm {
            topology_key: topology_key.to_string(),
            label_selector: Some(LabelSelector {
                match_labels: if ml.is_empty() { None } else { Some(ml) },
                match_expressions: if with_expr {
                    Some(vec![LabelSelectorRequirement { key: "team".into(), operator: "Exists".into(), values: None }])
                } else { None },
            }),
            ..Default::default()
        }
    }
```
Tests:
  - hostname + matchLabels, no expr → `anti_affinity_host_selectors == [{"app":"trainer"}]`, AND caveat "pod anti-affinity" STILL present (retained).
  - zone + matchLabels → selectors empty, caveat present.
  - hostname + matchLabels + matchExpressions → selectors empty (not modeled due to expressions), caveat present.
```rust
    #[test]
    fn hostname_matchlabels_is_extracted_but_still_caveated() {
        let mut p = pod("ksolver", None, Some("Pending"), vec![container("m", Some(q(&[("nvidia.com/gpu","1")])), None)], vec![]);
        set_anti_affinity(&mut p, vec![term("kubernetes.io/hostname", &[("app","trainer")], false)]);
        let got = classify(&p, &cfg()).unwrap();
        assert_eq!(got.anti_affinity_host_selectors.len(), 1);
        assert!(got.unmodeled_constraints.contains(&"pod anti-affinity".to_string()));
    }

    #[test]
    fn hostname_with_matchexpressions_is_not_modeled() {
        let mut p = pod("ksolver", None, Some("Pending"), vec![container("m", Some(q(&[("nvidia.com/gpu","1")])), None)], vec![]);
        set_anti_affinity(&mut p, vec![term("kubernetes.io/hostname", &[("app","trainer")], true)]);
        let got = classify(&p, &cfg()).unwrap();
        assert!(got.anti_affinity_host_selectors.is_empty());
        assert!(got.unmodeled_constraints.contains(&"pod anti-affinity".to_string()));
    }
```

- [ ] **Step 2: Run → fail.**

- [ ] **Step 3: Implement.** Add the field. Add a helper `modeled_anti_affinity_host_selectors(spec) -> Vec<BTreeMap<String,String>>` applying the modeled-term rule (check `term.namespaces` empty/none, `term.namespace_selector` none, `ls.match_expressions` empty/none, `ls.match_labels` non-empty, `term.topology_key == "kubernetes.io/hostname"`). Populate the new field. Leave `unmodeled_constraints`/caveat code as-is. Fix `PendingGpuPod` literals in `pending_input.rs`/`decision.rs`.

- [ ] **Step 4: Run → pass.** Existing 5d tests unchanged (the caveat still fires for all required anti-affinity, so `detects_pod_anti_affinity_and_spread` still passes).

- [ ] **Step 5: Commit.**
```bash
cargo fmt
git add ksolver/src/scheduler/pod_filter.rs ksolver/src/scheduler/pending_input.rs ksolver/src/scheduler/decision.rs
git commit -m "feat(scheduler): extract modeled hostname anti-affinity selectors (caveat retained)"
```

---

## Task 3: Best-effort anti-affinity node exclusion in the builder

**Files:** `pending_input.rs` (+ tests).

**Interfaces:** `build_pending_input` signature unchanged. New behavior: drop from a pending pod/gang's feasible nodes any node hosting a **running** (`current_node != ""`) pod in the **same namespace** whose `labels` match (are a superset of) any of the pod's `anti_affinity_host_selectors`.

- [ ] **Step 1: Matching helper.** `fn matches(selector: &BTreeMap<String,String>, w: &NormalizedWorkload) -> bool { selector.iter().all(|(k,v)| w.labels.get(k) == Some(v)) }` (selector non-empty by construction). Precompute running pods per node from `cluster.workloads` (current_node != "").
- [ ] **Step 2: Gang selector agreement (codex #5).** Fold `anti_affinity_host_selectors` into gang homogeneity: all members must have identical selector sets (sort each map's entries + the vector for comparison), else the gang is excluded (as with other heterogeneity). Then use the representative's selectors — no lossy union.
- [ ] **Step 3: Exclusion.** After computing residual-feasible nodes for a workload, remove any node `n` for which some running same-namespace pod on `n` matches any of the workload's selectors. If the set empties, the gang/pod is excluded (unplaced downstream) — consistent with existing behavior.
- [ ] **Step 4: Tests** (extend `pending_input.rs`; extend `ppod`/`workload` helpers to set selectors and workload `labels`):
  - pending pod anti-affine `{app:trainer}`, running pod `{app:trainer}` on n1, nodes n1+n2 → feasible == [n2].
  - running matching pod in a DIFFERENT namespace → not excluded (feasible keeps n1).
  - no selectors → no exclusion (unchanged).
  - gang whose members disagree on selectors → excluded.
  - gang (agreed selectors) anti-affine to a label on n1 → n1 dropped from the gang's feasible set.
- [ ] **Step 5: Run → pass.** `cargo test -p ksolver scheduler::pending_input`.
- [ ] **Step 6: Commit.**
```bash
cargo fmt
git add ksolver/src/scheduler/pending_input.rs
git commit -m "feat(scheduler): best-effort hostname anti-affinity node exclusion vs running pods"
```

---

## Task 4: Full gate + cluster verify

- [ ] **Step 1: Gate.** `cargo test -p ksolver`; `cargo test -p ksolver --features rust-cp-sat`; `cargo clippy -p ksolver --features rust-cp-sat --all-targets` → green.
- [ ] **Step 2: Cluster.** On `kind-solver-lab`: GPU node A hosts a `Running` pod (bound via `spec.nodeName`) labelled `app=trainer`; GPU node B has none. Create a pending ksolver GPU pod with hostname `podAntiAffinity` selecting `app=trainer`. Expect it `placed` on **B** (A excluded), still carrying the "pod anti-affinity" caveat (honest: same-batch/symmetry unmodeled). Then remove node B → expect `unplaced`. Confirm nothing bound; clean up.

---

## Self-Review Notes (incl. codex fixes)

- Best-effort, honest: excludes clearly-violating nodes (hostname vs running pods) AND **retains the caveat** — does not claim full enforcement (codex #1, #2 dissolved: same-batch & symmetry stay caveated, not silently ignored).
- Modeled from raw `corev1::PodAffinityTerm`, never the lossy collector `AffinityTerm`; matchExpressions/empty-selector/cross-namespace → not modeled (codex #1, real matchExpressions test added — codex #4).
- `NormalizedWorkload.labels` added + populated (codex #3); serde-default so normalized output stays backward compatible.
- Gang selector agreement via homogeneity, not a lossy union (codex #5).
- Node-exclusion fully prevents placement since CP-SAT only varies over `feasible_nodes` (codex #3 answer confirmed).
- Existing 5d tests unchanged because the caveat is retained for all required anti-affinity (codex #6).
- Still binds nothing; no-mutation guard unaffected.
```
