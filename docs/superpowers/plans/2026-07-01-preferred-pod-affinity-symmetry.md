# Preferred (Soft) Pod Affinity Symmetry Implementation Plan

> **For agentic workers:** Steps use checkbox (`- [ ]`) syntax. TDD, frequent commits.

**Goal:** Honor a *running* pod's `preferredDuringScheduling` pod affinity/anti-affinity when scoring a pending pod's candidate nodes — the soft mirror of required-anti-affinity symmetry (Phase 5h) — contributing to the same `soft_scores`, never changing admission or cost.

**Architecture:** The collector already extracts running pods' *required* anti-affinity from raw corev1 onto `Pod` → `NormalizedWorkload`. This slice adds running pods' *preferred* pod (anti-)affinity terms along the identical path (`Pod.preferred_pod_affinity` → `NormalizedWorkload.preferred_pod_affinity`). In `pending_input`, a new symmetry block scores each candidate node `cn` by each running pod `w` (on node `rn`) whose preferred term's selector matches ALL pending members and whose topology domain equals `cn`'s: `+weight` (affinity) / `-weight` (anti-affinity). Parser is shared with `pod_filter` (DRY).

**Tech Stack:** Rust, existing shadow scheduler modules.

## Global Constraints

- Shadow-only; binds nothing. Contributions flow ONLY into `soft_scores` (two-phase pass pins cost + admitted set ⇒ admission/cost cannot change; cpsat already handles negative scores).
- Symmetry matches a running pod's term against the pending pod: the selector must scope to the pending pod's namespace AND match EVERY pending member's labels (mirrors 5h exactness — partial-gang match does not score).
- Domain is label-based for ALL topology keys (`node.labels[topologyKey]`, incl. `kubernetes.io/hostname`); a node lacking the key scores nothing. Scores ACCUMULATE per matching running pod (kube sums).
- Best-effort — still NOT full kube-scheduler score parity (co-placement between two pending pods remains deferred).

---

### Task 1: Collect running pods' preferred pod terms (shared parser)

**Files:**
- Modify: `ksolver/src/collector.rs` (add `pub(crate) fn modeled_preferred_pod_terms`, inject into `Pod`)
- Modify: `ksolver/src/model.rs` (add `Pod.preferred_pod_affinity`, `NormalizedWorkload.preferred_pod_affinity`)
- Modify: `ksolver/src/normalizer.rs` (copy into `NormalizedWorkload`)
- Modify: `ksolver/src/scheduler/pod_filter.rs` (refactor `modeled_preferred_pod_affinity` to delegate)

**Interfaces:**
- Produces: `pub(crate) fn collector::modeled_preferred_pod_terms(affinity: Option<&corev1::Affinity>) -> Vec<crate::model::PreferredPodTerm>` — parses `podAffinity` + `podAntiAffinity` `preferredDuringScheduling…` (weight>0, modelable label + namespace selectors; `anti=true` for anti-affinity). `Pod.preferred_pod_affinity: Vec<PreferredPodTerm>`; `NormalizedWorkload.preferred_pod_affinity: Vec<PreferredPodTerm>`.

- [ ] **Step 1: Write failing collector test**

```rust
#[test]
fn collects_running_pod_preferred_pod_terms() {
    use k8s_openapi::api::core::v1 as corev1;
    let aff = corev1::Affinity {
        pod_anti_affinity: Some(corev1::PodAntiAffinity {
            preferred_during_scheduling_ignored_during_execution: Some(vec![
                corev1::WeightedPodAffinityTerm {
                    weight: 50,
                    pod_affinity_term: corev1::PodAffinityTerm {
                        label_selector: Some(
                            k8s_openapi::apimachinery::pkg::apis::meta::v1::LabelSelector {
                                match_labels: Some([("app".to_string(), "trainer".to_string())].into()),
                                ..Default::default()
                            },
                        ),
                        topology_key: "kubernetes.io/hostname".to_string(),
                        ..Default::default()
                    },
                },
            ]),
            ..Default::default()
        }),
        ..Default::default()
    };
    let got = modeled_preferred_pod_terms(Some(&aff));
    assert_eq!(got.len(), 1);
    assert_eq!(got[0].weight, 50);
    assert!(got[0].anti);
    assert_eq!(got[0].topology_key, "kubernetes.io/hostname");
}
```

- [ ] **Step 2: Run — expect FAIL** (`modeled_preferred_pod_terms` undefined)
- [ ] **Step 3: Implement `modeled_preferred_pod_terms` in collector** (mirror `modeled_anti_selectors_all`, over weighted terms of both podAffinity/podAntiAffinity; reuse `label_selector_to_reqs`/`namespace_selector_to_reqs`):

```rust
/// Running pods' preferred (soft) pod affinity + anti-affinity terms (for symmetry scoring).
/// weight>0, modelable label + namespace selectors; `anti=true` for anti-affinity; unmodelable
/// selectors skipped. Shared with pod_filter's pending-pod extractor.
pub(crate) fn modeled_preferred_pod_terms(
    affinity: Option<&corev1::Affinity>,
) -> Vec<crate::model::PreferredPodTerm> {
    let Some(aff) = affinity else {
        return Vec::new();
    };
    let mut out = Vec::new();
    let mut consume = |terms: Option<&Vec<corev1::WeightedPodAffinityTerm>>, anti: bool| {
        let Some(terms) = terms else { return };
        for wt in terms {
            if wt.weight <= 0 {
                continue;
            }
            let t = &wt.pod_affinity_term;
            let namespace_selector = match t.namespace_selector.as_ref() {
                None => None,
                Some(ns_ls) => match namespace_selector_to_reqs(ns_ls) {
                    Some(reqs) => Some(reqs),
                    None => continue,
                },
            };
            let Some(ls) = t.label_selector.as_ref() else {
                continue;
            };
            let Some(reqs) = label_selector_to_reqs(ls) else {
                continue;
            };
            out.push(crate::model::PreferredPodTerm {
                weight: i64::from(wt.weight),
                topology_key: t.topology_key.clone(),
                selector: crate::model::AntiAffinitySelector {
                    reqs,
                    namespaces: t.namespaces.clone().unwrap_or_default(),
                    namespace_selector,
                },
                anti,
            });
        }
    };
    consume(
        aff.pod_affinity
            .as_ref()
            .and_then(|a| a.preferred_during_scheduling_ignored_during_execution.as_ref()),
        false,
    );
    consume(
        aff.pod_anti_affinity
            .as_ref()
            .and_then(|a| a.preferred_during_scheduling_ignored_during_execution.as_ref()),
        true,
    );
    out
}
```

- [ ] **Step 4: Add model fields** — `Pod.preferred_pod_affinity: Vec<PreferredPodTerm>` (serde default) and `NormalizedWorkload.preferred_pod_affinity: Vec<PreferredPodTerm>` (serde default).
- [ ] **Step 5: Inject in collector** — at the `Pod { … }` literal (~line 591): `preferred_pod_affinity: modeled_preferred_pod_terms(affinity),`.
- [ ] **Step 6: Copy in normalizer** — at the `NormalizedWorkload { … }` literal (~line 659): `preferred_pod_affinity: pod.preferred_pod_affinity.clone(),`.
- [ ] **Step 7: Refactor pod_filter** — replace `modeled_preferred_pod_affinity`'s body with `crate::collector::modeled_preferred_pod_terms(spec.affinity.as_ref())` (delete the duplicated parser). Its existing test `preferred_pod_affinity_extracted_both_directions` must still pass.
- [ ] **Step 8: Ripple `Pod`/`NormalizedWorkload` literals** — add the new field (default `vec![]`) to any struct literals not using `..Default::default()` (search tests in collector/normalizer/model).
- [ ] **Step 9: Run — expect PASS**; full build.
- [ ] **Step 10: Commit** — `git commit -am "Collect running pods' preferred pod terms (shared parser) for symmetry"`

---

### Task 2: Symmetry soft-score contribution in `pending_input`

**Files:**
- Modify: `ksolver/src/scheduler/pending_input.rs` (add a symmetry block after the forward pod-affinity block, before `retain`)

**Interfaces:**
- Consumes: `NormalizedWorkload.preferred_pod_affinity` on running pods (via `running_by_node`), `member_labels`, `domain`, `selector_scopes_ns`, `selector_matches`.

- [ ] **Step 1: Write failing tests**

```rust
#[test]
fn symmetric_preferred_pod_anti_affinity_penalizes_running_pods_domain() {
    // running "guard" on n1 (hostname n1) softly prefers NOT to share a host with app=trainer.
    // pending is app=trainer with NO own preferred terms -> symmetry must still discourage n1.
    let mut n1 = node("n1", 16000, 64, 110, 8);
    n1.labels = [("kubernetes.io/hostname".into(), "n1".into())].into();
    let mut n2 = node("n2", 16000, 64, 110, 8);
    n2.labels = [("kubernetes.io/hostname".into(), "n2".into())].into();
    let mut guard = running_labeled("team", "guard", "n1", &[("role", "guard")]);
    guard.preferred_pod_affinity = vec![crate::model::PreferredPodTerm {
        weight: 30,
        topology_key: "kubernetes.io/hostname".into(),
        selector: sel(&[("app", "trainer")]),
        anti: true,
    }];
    let cluster = NormalizedCluster {
        nodes: vec![n1, n2],
        workloads: vec![
            guard,
            labeled_pending("team", "pending", &["n1", "n2"], &[("app", "trainer")]),
        ],
        ..Default::default()
    };
    let input = build_pending_input(&cluster, &[ppod("team", "pending", None)]);
    let w = &input.workloads[0];
    assert_eq!(w.soft_scores.get("n1"), Some(&-30)); // running guard discourages its host
    assert_eq!(w.soft_scores.get("n2"), None);
}

#[test]
fn symmetric_preferred_ignores_partial_gang_match() {
    // running guard forbids app=trainer softly; gang has one member app=trainer, one without.
    let mut n1 = node("n1", 16000, 64, 110, 8);
    n1.labels = [("kubernetes.io/hostname".into(), "n1".into())].into();
    let mut guard = running_labeled("team", "guard", "n1", &[("role", "guard")]);
    guard.preferred_pod_affinity = vec![crate::model::PreferredPodTerm {
        weight: 30,
        topology_key: "kubernetes.io/hostname".into(),
        selector: sel(&[("app", "trainer")]),
        anti: true,
    }];
    let mut m0 = workload("team", "m0", "", 1000, 2, 1, &["n1"]);
    m0.labels = [("app".to_string(), "trainer".to_string())].into();
    let m1 = workload("team", "m1", "", 1000, 2, 1, &["n1"]); // no labels
    let cluster = NormalizedCluster {
        nodes: vec![n1],
        workloads: vec![guard, m0, m1],
        ..Default::default()
    };
    let input = build_pending_input(
        &cluster,
        &[ppod("team", "m0", Some("job")), ppod("team", "m1", Some("job"))],
    );
    assert!(input.workloads[0].soft_scores.is_empty()); // not ALL members match -> no score
}
```

- [ ] **Step 2: Run — expect FAIL**
- [ ] **Step 3: Implement** — insert BEFORE `soft_scores.retain(...)`:

```rust
// Symmetric preferred pod (anti-)affinity: a RUNNING pod's own preferred term steers the pending
// pod (mirror of required-symmetry 5h, but soft). For each running pod w on node rn whose term's
// selector scopes to the pending namespace and matches EVERY pending member's labels, a candidate
// node cn sharing rn's topology domain accumulates +weight (affinity) / -weight (anti-affinity).
// Independent of the pending pod's own preferred terms (runs even when it has none).
for cn in &feasible_nodes {
    for (rn, pods) in &running_by_node {
        for w in pods {
            for term in &w.preferred_pod_affinity {
                let (Some(cd), Some(rd)) =
                    (domain(cn, &term.topology_key), domain(rn, &term.topology_key))
                else {
                    continue;
                };
                if cd != rd {
                    continue;
                }
                if selector_scopes_ns(&term.selector, &w.namespace, &rep.namespace, ns_labels)
                    && member_labels
                        .iter()
                        .all(|ml| selector_matches(&term.selector.reqs, ml))
                {
                    let delta = if term.anti { -term.weight } else { term.weight };
                    *soft_scores.entry(cn.clone()).or_default() += delta;
                }
            }
        }
    }
}
soft_scores.retain(|_, v| *v != 0);
```

  (Delete the existing standalone `soft_scores.retain(...)` line so it's not duplicated — keep the one after this block.)

- [ ] **Step 4: Run — expect PASS**; full `cargo test` + `cargo clippy` clean.
- [ ] **Step 5: Commit** — `git commit -am "Symmetric preferred pod (anti-)affinity soft scoring (forward+symmetric parity with 5h)"`

---

### Task 3: Docs + full verify

**Files:** `README.md`, `docs/superpowers/specs/2026-07-01-soft-affinity-scoring-design.md`

- [ ] **Step 1: README** — update the preferred pod-affinity bullet: symmetry now honored (a running pod's preferred terms steer incoming pods, matching all members). Co-placement between two pending pods remains the sole deferred item.
- [ ] **Step 2: Spec status** — mark symmetry DONE; co-placement the only remaining preferred follow-up.
- [ ] **Step 3: Full verify** — `cargo test --features rust-cp-sat` + `cargo clippy --features rust-cp-sat` clean.
- [ ] **Step 4: Commit** — `git commit -am "Docs: preferred pod-affinity symmetry"`

---

## Self-Review

- **Spec coverage:** collect running-pod preferred terms (T1) → symmetry scoring (T2) → docs (T3). Forward scoring already shipped; cpsat unchanged.
- **Type consistency:** `PreferredPodTerm` reused verbatim; new fields default-empty and serde-default (backcompat). Parser shared (collector) — no drift with pod_filter.
- **Admission/cost safety:** symmetry writes only to `soft_scores`; phase-1 pins cost + admitted set.
- **Exactness:** matches ALL members (partial-gang no-score) mirrors 5h; label-based domain uniform; per-pod accumulation.
- **No placeholders.**
