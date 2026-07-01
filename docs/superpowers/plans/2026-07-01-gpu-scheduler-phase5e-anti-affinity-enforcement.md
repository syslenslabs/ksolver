# GPU Scheduler — Phase 5e: Enforce Hostname Pod Anti-Affinity — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: superpowers:subagent-driven-development or superpowers:executing-plans. Steps use checkbox (`- [ ]`) syntax.

**Goal:** Close (partially) the gap Phase 5d discloses: actually **enforce required pod anti-affinity** for the common, tractable case — `topologyKey: kubernetes.io/hostname`, `matchLabels` selector, same namespace — by excluding, from a pending pod's feasible nodes, any node already hosting a running pod that matches the selector. Keep the 5d caveat only for anti-affinity terms this phase does NOT model. Shadow-local (no offline-planner behavior change).

**Why now:** Codex flagged this as the highest-value follow-up to 5d. Hostname anti-affinity against existing pods is the overwhelmingly common form ("spread my replicas across hosts") and is a hard feasibility constraint we can enforce by node exclusion without a solver change.

**Architecture:** `classify` extracts each pod's *modeled* anti-affinity host selectors (hostname topologyKey + non-empty matchLabels + no matchExpressions + no cross-namespace scoping) onto `PendingGpuPod`, and flags whether any *unmodeled* anti-affinity term remains (drives the 5d caveat). `NormalizedWorkload` gains `labels` so the shadow builder can see running pods' labels. `build_pending_input` excludes, from each pending pod/gang's feasible nodes, nodes hosting a matching same-namespace running pod. Enforcement is entirely in the shadow path; the normalizer only gains a label passthrough.

**Tech Stack:** Rust; k8s-openapi v1_31 (`corev1::{PodAffinityTerm, LabelSelector}`); existing `model`, `normalizer`, `scheduler::{pod_filter, pending_input, shadow}`.

## Global Constraints

- Verified facts:
  - Model `AffinityTerm { topology_key: String, selector: BTreeMap<String,String> }` (matchLabels only; matchExpressions are dropped by the collector → they appear as an empty/partial `selector`). Model `Pod.required_anti: Vec<AffinityTerm>`; collector populates it (`to_required_anti_affinity`).
  - `NormalizedWorkload` has `namespace`, `name`, `current_node` (empty = pending), `feasible_node_names`, but NO `labels` today — add it.
  - `feasible_on_node` (normalizer) does not enforce pod anti-affinity; `feasible_node_names` therefore ignores it. Shadow relies on `feasible_node_names` and its own residual filter.
  - Running pods = `NormalizedWorkload` with `current_node != ""`.
  - k8s: a `podAntiAffinity` requiredDuringScheduling term with `topologyKey: kubernetes.io/hostname` forbids scheduling onto a node whose (per-namespace) pods match `labelSelector`. An empty `labelSelector` matches ALL pods.
- **Modeled term** (enforced) = `topology_key == "kubernetes.io/hostname"` AND non-empty `match_labels` AND no `match_expressions` AND no `namespaces`/`namespace_selector` (same-namespace scope). Everything else (other topologyKey, empty/expression selector, cross-namespace) is **unmodeled** → still caveated (5d), NOT enforced. Empty matchLabels is treated as unmodeled to avoid over-excluding when matchExpressions were dropped.
- **Limitations (documented, not silently ignored):** anti-affinity *symmetry* (an existing pod's anti-affinity forbidding the new pod) is NOT modeled; anti-affinity *among simultaneously-placed pending pods* is NOT modeled (only against already-running pods). These remain — surface via the existing caveat where a term is unmodeled; otherwise document.
- Shadow-local: the offline planner/simulator are unaffected (only a `labels` passthrough is added to the normalizer, which does not change feasibility there).
- Unit tests pass without the `rust-cp-sat` feature. `cargo fmt` + clean clippy. Still binds nothing.

## File Structure

- Modify `ksolver/src/model.rs` — add `labels: BTreeMap<String,String>` to `NormalizedWorkload`.
- Modify `ksolver/src/normalizer.rs` — populate `labels` from the pod (passthrough; update any full `NormalizedWorkload` literals/tests).
- Modify `ksolver/src/scheduler/pod_filter.rs` — add `anti_affinity_host_selectors: Vec<BTreeMap<String,String>>` to `PendingGpuPod`; classify extracts modeled terms and only caveats unmodeled anti-affinity.
- Modify `ksolver/src/scheduler/pending_input.rs` — exclude anti-affinity-violating nodes from feasible sets (singletons and gangs).

---

## Task 1: `labels` on NormalizedWorkload

**Files:** `model.rs`, `normalizer.rs` (+ any literal/test updates).

- [ ] **Step 1:** Add `#[serde(default)] pub labels: BTreeMap<String, String>,` to `NormalizedWorkload` (near `namespace`/`name`).
- [ ] **Step 2:** In `normalizer.rs` where `NormalizedWorkload { .. }` is constructed (the `for pod in modeled_pods` loop, ~L641), set `labels: pod.labels.clone(),`. (Model `Pod` has `labels: BTreeMap<String,String>`.) If the construction uses a full literal, add the field; if it uses `..Default::default()`, still set it explicitly.
- [ ] **Step 3: Build.** `cargo build -p ksolver` → compiles (fix any full `NormalizedWorkload` literals in normalizer/tests that now need `labels`).
- [ ] **Step 4: Commit.**
```bash
cargo fmt
git add ksolver/src/model.rs ksolver/src/normalizer.rs
git commit -m "feat(model): carry pod labels on NormalizedWorkload"
```

---

## Task 2: Extract modeled anti-affinity + refine caveat in classify

**Files:** `pod_filter.rs` (+ tests). Update `PendingGpuPod` literals (pending_input.rs, decision.rs test helpers) to add `anti_affinity_host_selectors: vec![]`.

**Interfaces:**
- `PendingGpuPod` gains `pub anti_affinity_host_selectors: Vec<BTreeMap<String,String>>` — the matchLabels maps of *modeled* hostname anti-affinity terms (empty when none/unmodeled).
- `classify`: build these from `spec.affinity.pod_anti_affinity.required_during_scheduling_ignored_during_execution`. A term is modeled iff `topology_key == "kubernetes.io/hostname"`, `label_selector.match_labels` non-empty, `label_selector.match_expressions` empty/none, and `namespaces`/`namespace_selector` unset. Push its `match_labels` map.
- The "pod anti-affinity" caveat (from 5d) now fires ONLY if the pod has an anti-affinity term that is **not** modeled (so enforced terms are not double-flagged).

- [ ] **Step 1: Failing tests.** In `pod_filter.rs` tests:
  - hostname + matchLabels term → `anti_affinity_host_selectors == [{"app":"trainer"}]` and NO "pod anti-affinity" caveat.
  - zone topologyKey term → `anti_affinity_host_selectors` empty AND "pod anti-affinity" caveat present (unmodeled).
  - hostname term with only matchExpressions (empty matchLabels) → not modeled → empty selectors + caveat.
```rust
    fn anti_affinity(pod: &mut corev1::Pod, topology_key: &str, match_labels: &[(&str, &str)]) {
        use k8s_openapi::apimachinery::pkg::apis::meta::v1::LabelSelector;
        let ml: std::collections::BTreeMap<String, String> =
            match_labels.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect();
        if let Some(spec) = pod.spec.as_mut() {
            spec.affinity = Some(corev1::Affinity {
                pod_anti_affinity: Some(corev1::PodAntiAffinity {
                    required_during_scheduling_ignored_during_execution: Some(vec![corev1::PodAffinityTerm {
                        topology_key: topology_key.to_string(),
                        label_selector: Some(LabelSelector { match_labels: if ml.is_empty() { None } else { Some(ml) }, ..Default::default() }),
                        ..Default::default()
                    }]),
                    ..Default::default()
                }),
                ..Default::default()
            });
        }
    }

    #[test]
    fn hostname_anti_affinity_is_modeled_not_caveated() {
        let mut p = pod("ksolver", None, Some("Pending"), vec![container("m", Some(q(&[("nvidia.com/gpu","1")])), None)], vec![]);
        anti_affinity(&mut p, "kubernetes.io/hostname", &[("app","trainer")]);
        let got = classify(&p, &cfg()).unwrap();
        assert_eq!(got.anti_affinity_host_selectors.len(), 1);
        assert!(!got.unmodeled_constraints.contains(&"pod anti-affinity".to_string()));
    }

    #[test]
    fn zone_anti_affinity_is_caveated_not_modeled() {
        let mut p = pod("ksolver", None, Some("Pending"), vec![container("m", Some(q(&[("nvidia.com/gpu","1")])), None)], vec![]);
        anti_affinity(&mut p, "topology.kubernetes.io/zone", &[("app","trainer")]);
        let got = classify(&p, &cfg()).unwrap();
        assert!(got.anti_affinity_host_selectors.is_empty());
        assert!(got.unmodeled_constraints.contains(&"pod anti-affinity".to_string()));
    }
```

- [ ] **Step 2: Run → fail.**

- [ ] **Step 3: Implement.** Add the field. Refactor the anti-affinity detection in `classify` into a helper that returns `(modeled_selectors, has_unmodeled_anti)`. A term is modeled per the rule above (check `term.namespaces.as_ref().map_or(true, |n| n.is_empty())` and `term.namespace_selector.is_none()` and `label_selector.match_expressions` empty). Feed `has_unmodeled_anti` into the caveat push (replace the current unconditional "pod anti-affinity" push). Populate `anti_affinity_host_selectors` with modeled terms' match_labels.

- [ ] **Step 4: Run → pass.** Update the older 5d test `detects_pod_anti_affinity_and_spread` (its term has no topologyKey=hostname? it used `kubernetes.io/hostname` — that would now be MODELED, so it'd no longer caveat anti-affinity). Adjust that test: to keep asserting the anti-affinity *caveat*, give its term a non-hostname topologyKey (e.g. zone), or split into modeled/unmodeled assertions. Keep the topology-spread part unchanged.

- [ ] **Step 5: Commit.**
```bash
cargo fmt
git add ksolver/src/scheduler/pod_filter.rs ksolver/src/scheduler/pending_input.rs ksolver/src/scheduler/decision.rs
git commit -m "feat(scheduler): extract modeled hostname anti-affinity; caveat only unmodeled terms"
```

---

## Task 3: Enforce anti-affinity in the shadow builder

**Files:** `pending_input.rs` (+ tests).

**Interfaces:** `build_pending_input` signature unchanged. New behavior: exclude from a pending pod/gang's feasible nodes any node that hosts a **running** pod (`current_node != ""`) in the **same namespace** whose `labels` match (superset of) any of the pod's `anti_affinity_host_selectors`.

- [ ] **Step 1:** Build a helper: `node -> Vec<&NormalizedWorkload running there>` (or iterate). A selector `s` (BTreeMap) matches a workload `w` iff `s.iter().all(|(k,v)| w.labels.get(k) == Some(v))` AND same namespace.
- [ ] **Step 2:** For each gang, compute the **union** of members' `anti_affinity_host_selectors` (gang members normally share them; union is safe). After computing residual-feasible nodes, drop any node where a running same-namespace pod matches any union selector. If that empties the feasible set, the gang is excluded (reported unplaced downstream), consistent with existing behavior.
- [ ] **Step 3: Tests** (extend `pending_input.rs`):
  - a pending pod with anti-affinity `{app:trainer}` and a running pod `{app:trainer}` on n1 (2 nodes) → feasible excludes n1, keeps n2.
  - running matching pod in a DIFFERENT namespace → not excluded.
  - no anti-affinity selectors → no exclusion (unchanged).
  - gang: all members anti-affine to a label present on n1 → n1 excluded for the gang.
  (Extend `ppod`/`workload` helpers to set `anti_affinity_host_selectors` and workload `labels`.)
- [ ] **Step 4: Run → pass.** `cargo test -p ksolver scheduler::pending_input`.
- [ ] **Step 5: Commit.**
```bash
cargo fmt
git add ksolver/src/scheduler/pending_input.rs
git commit -m "feat(scheduler): enforce hostname pod anti-affinity against running pods"
```

---

## Task 4: Full gate + cluster verify

- [ ] **Step 1: Gate.** `cargo test -p ksolver`; `cargo test -p ksolver --features rust-cp-sat`; `cargo clippy -p ksolver --features rust-cp-sat --all-targets` → green (incl. no-mutation guard).
- [ ] **Step 2: Cluster.** On `kind-solver-lab`, one GPU node hosting an existing `Running` pod labelled `app=trainer` (bound to the node via `spec.nodeName`), plus a pending ksolver GPU pod with hostname `podAntiAffinity` selecting `app=trainer`. Expect the pending pod `unplaced` ("no feasible placement") — the only GPU node is excluded by anti-affinity — with NO "pod anti-affinity" caveat (it's now modeled). Add a second GPU node without the matching pod → expect placement there. Confirm nothing bound; clean up.

---

## Self-Review Notes

- Enforces the common case (hostname + matchLabels + same-namespace) against running pods; other forms remain caveated (5d), not silently dropped.
- Empty matchLabels treated as unmodeled (avoids over-excluding when matchExpressions were dropped by the collector).
- Shadow-local: normalizer only gains a `labels` passthrough; offline planner feasibility unchanged.
- Documented residual limitations: anti-affinity symmetry and anti-affinity among simultaneously-placed pending pods are not modeled.
- Still binds nothing; no-mutation guard unaffected.
```
