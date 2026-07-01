# Preferred (Soft) Pod Affinity / Anti-Affinity Implementation Plan

> **For agentic workers:** Steps use checkbox (`- [ ]`) syntax. TDD, frequent commits.

**Goal:** Honor `preferredDuringScheduling` pod affinity and pod anti-affinity as a cost-tie-break in shadow mode, reusing the existing two-phase soft-affinity machinery — never changing which pods are admitted or the total cost.

**Architecture:** A pending pod's preferred pod-affinity terms (weight + topologyKey + labelSelector + namespace scope) are parsed in `pod_filter`, then in `pending_input` each feasible node earns a net soft score: `+weight` when a matching running pod shares the candidate node's topology domain (affinity), `-weight` when one does (anti-affinity). These contributions are ADDED to the same `OptimizationWorkload.soft_scores` map already consumed by `cpsat_rust`'s phase-2 solve. cpsat needs NO change (it already applies `soft += (-score, x)`, so negative scores discourage a node).

**Tech Stack:** Rust, existing shadow scheduler modules.

## Global Constraints

- Shadow-only; binds nothing. No change to the no-mutation guard.
- MUST NOT change admission or cost: contributions flow only through `soft_scores`, which is used exclusively by the two-phase pass that pins the phase-1 cost optimum + admitted set.
- Forward-only (the pending pod's own preferences). Symmetry (a *running* pod's preferred anti-affinity steering incoming pods) is DEFERRED and documented — required pod anti-affinity already models symmetry for the hard case; preferred symmetry is a follow-up.
- Best-effort: terms with unmodelable label/namespace selectors are silently skipped (soft, so no caveat needed — required terms remain caveated as today).
- Gang members must AGREE on preferred pod-affinity terms; disagreement ⇒ no soft scores for that gang (drop scores, not the gang), mirroring the node-affinity soft slice. Agreement uses exact `Vec` equality (order-sensitive) — intentionally stricter than kube; it only ever drops soft scores, never affects admission, so no canonicalizer is warranted.
- Domain matching is label-based for ALL topology keys (including `kubernetes.io/hostname`): a candidate/running node's domain is `node.labels[topologyKey]`; a node lacking that label earns/contributes no score. Scores ACCUMULATE per matching running pod (kube's `interpodaffinity/scoring.go` sums weight per matching existing pod per topology), not once per term. This is best-effort soft scoring, NOT full kube-scheduler score parity.

---

### Task 1: Model type `PreferredPodTerm`

**Files:**
- Modify: `ksolver/src/model.rs` (near `PreferredNodeTerm`, ~line 350)

**Interfaces:**
- Produces: `pub struct PreferredPodTerm { pub weight: i64, pub topology_key: String, pub selector: AntiAffinitySelector, pub anti: bool }` — reuses `AntiAffinitySelector` for reqs + namespace scope. `anti=false` = affinity (encourage), `anti=true` = anti-affinity (discourage). Derives `Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq` (PartialEq/Eq required for gang-agreement comparison).

- [ ] **Step 1: Add the struct**

```rust
/// A `preferredDuringScheduling` pod (anti-)affinity term: a `weight` (1–100), a `topology_key`,
/// a label `selector` (reqs + namespace scope, reusing `AntiAffinitySelector`), and an `anti` flag.
/// A candidate node earns `+weight` (affinity) or `-weight` (anti-affinity) toward its soft score
/// when a matching running pod shares the node's topology domain. Best-effort, forward-only.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct PreferredPodTerm {
    #[serde(default)]
    pub weight: i64,
    #[serde(default)]
    pub topology_key: String,
    #[serde(default)]
    pub selector: AntiAffinitySelector,
    #[serde(default)]
    pub anti: bool,
}
```

- [ ] **Step 2: Build** — `CARGO_NET_OFFLINE=true cargo build --features rust-cp-sat`. Expected: compiles.
- [ ] **Step 3: Commit** — `git add -A && git commit -m "Add PreferredPodTerm model type"`

---

### Task 2: Parse preferred pod (anti-)affinity in `pod_filter`

**Files:**
- Modify: `ksolver/src/scheduler/pod_filter.rs` (add field to `PendingGpuPod`, add `modeled_preferred_pod_affinity`, populate in builder, ripple test constructors)
- Modify construction sites: `decision.rs:160`, `bench.rs:140`, `pending_input.rs` test constructors (add `preferred_pod_affinity: vec![]`)

**Interfaces:**
- Produces: `PendingGpuPod.preferred_pod_affinity: Vec<crate::model::PreferredPodTerm>`; `fn modeled_preferred_pod_affinity(spec: &corev1::PodSpec) -> Vec<PreferredPodTerm>`.

- [ ] **Step 1: Write failing test** (in `pod_filter.rs` tests)

```rust
#[test]
fn preferred_pod_affinity_extracted_both_directions() {
    use k8s_openapi::api::core::v1 as corev1;
    let term = |key: &str, val: &str, tk: &str| corev1::WeightedPodAffinityTerm {
        weight: 30,
        pod_affinity_term: corev1::PodAffinityTerm {
            label_selector: Some(k8s_openapi::apimachinery::pkg::apis::meta::v1::LabelSelector {
                match_labels: Some([(key.to_string(), val.to_string())].into()),
                ..Default::default()
            }),
            topology_key: tk.to_string(),
            ..Default::default()
        },
    };
    let spec = corev1::PodSpec {
        affinity: Some(corev1::Affinity {
            pod_affinity: Some(corev1::PodAffinity {
                preferred_during_scheduling_ignored_during_execution: Some(vec![term(
                    "app", "cache", "kubernetes.io/hostname",
                )]),
                ..Default::default()
            }),
            pod_anti_affinity: Some(corev1::PodAntiAffinity {
                preferred_during_scheduling_ignored_during_execution: Some(vec![term(
                    "app", "noisy", "topology.kubernetes.io/zone",
                )]),
                ..Default::default()
            }),
            ..Default::default()
        }),
        ..Default::default()
    };
    let got = modeled_preferred_pod_affinity(&spec);
    assert_eq!(got.len(), 2);
    let aff = got.iter().find(|t| !t.anti).unwrap();
    assert_eq!(aff.weight, 30);
    assert_eq!(aff.topology_key, "kubernetes.io/hostname");
    assert_eq!(aff.selector.reqs.len(), 1);
    let anti = got.iter().find(|t| t.anti).unwrap();
    assert_eq!(anti.topology_key, "topology.kubernetes.io/zone");
    assert!(anti.anti);
}
```

- [ ] **Step 2: Run — expect FAIL** (`modeled_preferred_pod_affinity` undefined)
- [ ] **Step 3: Implement**

```rust
/// Preferred (soft) pod affinity + anti-affinity terms from `podAffinity`/`podAntiAffinity`
/// `preferredDuringScheduling…`. Each `WeightedPodAffinityTerm` lowers to a `PreferredPodTerm`
/// (weight>0, modelable labelSelector, modelable namespaceSelector). Unmodelable selectors are
/// skipped (best-effort soft). `anti=true` for anti-affinity. Selector lowering shared with the
/// collector; namespace scope reuses `AntiAffinitySelector` (empty `namespaces` ⇒ own namespace).
fn modeled_preferred_pod_affinity(spec: &corev1::PodSpec) -> Vec<crate::model::PreferredPodTerm> {
    let mut out = Vec::new();
    let Some(aff) = spec.affinity.as_ref() else {
        return out;
    };
    let mut consume = |terms: &Option<Vec<corev1::WeightedPodAffinityTerm>>, anti: bool| {
        let Some(terms) = terms.as_ref() else { return };
        for wt in terms {
            if wt.weight <= 0 {
                continue;
            }
            let t = &wt.pod_affinity_term;
            let namespace_selector = match t.namespace_selector.as_ref() {
                None => None,
                Some(ns_ls) => match crate::collector::namespace_selector_to_reqs(ns_ls) {
                    Some(reqs) => Some(reqs),
                    None => continue,
                },
            };
            let Some(ls) = t.label_selector.as_ref() else {
                continue;
            };
            let Some(reqs) = crate::collector::label_selector_to_reqs(ls) else {
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
        &aff.pod_affinity
            .as_ref()
            .and_then(|a| a.preferred_during_scheduling_ignored_during_execution.clone()),
        false,
    );
    consume(
        &aff.pod_anti_affinity
            .as_ref()
            .and_then(|a| a.preferred_during_scheduling_ignored_during_execution.clone()),
        true,
    );
    out
}
```

- [ ] **Step 4: Populate in builder** — add `let preferred_pod_affinity = modeled_preferred_pod_affinity(spec);` and `preferred_pod_affinity,` to the `PendingGpuPod { ... }` literal at ~line 136.
- [ ] **Step 5: Add field to struct** — `pub preferred_pod_affinity: Vec<crate::model::PreferredPodTerm>,` on `PendingGpuPod`.
- [ ] **Step 6: Ripple constructors** — add `preferred_pod_affinity: vec![],` to `decision.rs:160`, `bench.rs:140`, and every `PendingGpuPod { ... }` literal in `pending_input.rs` tests.
- [ ] **Step 7: Run — expect PASS**; build clean.
- [ ] **Step 8: Commit** — `git commit -am "Parse preferred pod (anti-)affinity terms in pod_filter"`

---

### Task 3: Compute soft scores from preferred pod (anti-)affinity in `pending_input`

**Files:**
- Modify: `ksolver/src/scheduler/pending_input.rs` (soft-score block ~lines 556–584)

**Interfaces:**
- Consumes: `PendingGpuPod.preferred_pod_affinity`, existing `running_by_node`, `node_labels`, `domain(node,key)`, `selector_scopes_ns`, `selector_matches`.
- Produces: net contributions merged into `soft_scores` (the existing map); final retain of non-zero entries.

- [ ] **Step 1: Write failing tests** (in `pending_input.rs` tests)

```rust
#[test]
fn preferred_pod_affinity_scores_domain_with_matching_running_pod() {
    // running "cache" pod on n1 (zone za); pending prefers same-zone as app=cache (weight 40).
    let mut n1 = node("n1", 16000, 64, 110, 8);
    n1.labels = [("topology.kubernetes.io/zone".into(), "za".into())].into();
    let mut n2 = node("n2", 16000, 64, 110, 8);
    n2.labels = [("topology.kubernetes.io/zone".into(), "zb".into())].into();
    let cluster = NormalizedCluster {
        nodes: vec![n1, n2],
        workloads: vec![
            running_labeled("team", "cache", "n1", &[("app", "cache")]),
            workload("team", "pending", "", 1000, 2, 1, &["n1", "n2"]),
        ],
        ..Default::default()
    };
    let mut p = ppod("team", "pending", None);
    p.preferred_pod_affinity = vec![crate::model::PreferredPodTerm {
        weight: 40,
        topology_key: "topology.kubernetes.io/zone".into(),
        selector: sel(&[("app", "cache")]),
        anti: false,
    }];
    let input = build_pending_input(&cluster, &[p]);
    let w = &input.workloads[0];
    assert_eq!(w.soft_scores.get("n1"), Some(&40)); // za shares the cache pod's domain
    assert_eq!(w.soft_scores.get("n2"), None); // zb has no matching pod
}

#[test]
fn preferred_pod_anti_affinity_penalizes_node_with_matching_running_pod() {
    // hostname-key domain is label-based: give each node its own kubernetes.io/hostname label.
    let mut n1 = node("n1", 16000, 64, 110, 8);
    n1.labels = [("kubernetes.io/hostname".into(), "n1".into())].into();
    let mut n2 = node("n2", 16000, 64, 110, 8);
    n2.labels = [("kubernetes.io/hostname".into(), "n2".into())].into();
    let cluster = NormalizedCluster {
        nodes: vec![n1, n2],
        workloads: vec![
            running_labeled("team", "noisy", "n1", &[("app", "noisy")]),
            workload("team", "pending", "", 1000, 2, 1, &["n1", "n2"]),
        ],
        ..Default::default()
    };
    let mut p = ppod("team", "pending", None);
    p.preferred_pod_affinity = vec![crate::model::PreferredPodTerm {
        weight: 25,
        topology_key: "kubernetes.io/hostname".into(),
        selector: sel(&[("app", "noisy")]),
        anti: true,
    }];
    let input = build_pending_input(&cluster, &[p]);
    let w = &input.workloads[0];
    assert_eq!(w.soft_scores.get("n1"), Some(&-25)); // discourage the node with noisy
    assert_eq!(w.soft_scores.get("n2"), None);
}

#[test]
fn preferred_pod_affinity_accumulates_per_matching_pod() {
    // TWO matching cache pods in zone za -> candidate n1 earns 2*weight (kube accumulates).
    let mut n1 = node("n1", 16000, 64, 110, 8);
    n1.labels = [("topology.kubernetes.io/zone".into(), "za".into())].into();
    let mut n3 = node("n3", 16000, 64, 110, 8);
    n3.labels = [("topology.kubernetes.io/zone".into(), "za".into())].into();
    let cluster = NormalizedCluster {
        nodes: vec![n1, n3],
        workloads: vec![
            running_labeled("team", "cache0", "n1", &[("app", "cache")]),
            running_labeled("team", "cache1", "n3", &[("app", "cache")]),
            workload("team", "pending", "", 1000, 2, 1, &["n1"]),
        ],
        ..Default::default()
    };
    let mut p = ppod("team", "pending", None);
    p.preferred_pod_affinity = vec![crate::model::PreferredPodTerm {
        weight: 20,
        topology_key: "topology.kubernetes.io/zone".into(),
        selector: sel(&[("app", "cache")]),
        anti: false,
    }];
    let input = build_pending_input(&cluster, &[p]);
    // n1's zone za holds both cache pods -> 20 + 20.
    assert_eq!(input.workloads[0].soft_scores.get("n1"), Some(&40));
}

#[test]
fn preferred_pod_affinity_dropped_when_gang_disagrees() {
    let cluster = NormalizedCluster {
        nodes: vec![node("n1", 16000, 64, 110, 8), node("n2", 16000, 64, 110, 8)],
        workloads: vec![
            running_labeled("team", "cache", "n1", &[("app", "cache")]),
            workload("team", "m0", "", 1000, 2, 1, &["n1", "n2"]),
            workload("team", "m1", "", 1000, 2, 1, &["n1", "n2"]),
        ],
        ..Default::default()
    };
    let term = |w: i64| crate::model::PreferredPodTerm {
        weight: w,
        topology_key: "kubernetes.io/hostname".into(),
        selector: sel(&[("app", "cache")]),
        anti: false,
    };
    let mut m0 = ppod("team", "m0", Some("job"));
    m0.preferred_pod_affinity = vec![term(40)];
    let mut m1 = ppod("team", "m1", Some("job"));
    m1.preferred_pod_affinity = vec![term(10)]; // disagree on weight
    let input = build_pending_input(&cluster, &[m0, m1]);
    assert_eq!(input.workloads.len(), 1);
    assert!(input.workloads[0].soft_scores.is_empty());
}
```

- [ ] **Step 2: Run — expect FAIL** (scores not computed)
- [ ] **Step 3: Implement** — after the existing node-affinity soft-score block (before `workloads.push`), add pod-(anti-)affinity contributions and switch the final map to accumulate + retain non-zero. Because scores can now be negative and additive, change the node-affinity block to accumulate into `soft_scores` with `+=` (via entry) instead of `insert if >0`, then at the very end `soft_scores.retain(|_, v| *v != 0);`.

```rust
// Preferred pod (anti-)affinity: forward-only, domain-aware, label-based for ALL topology keys
// (incl. kubernetes.io/hostname). A candidate node accumulates +weight (affinity) / -weight
// (anti-affinity) for EACH matching running pod sharing the candidate's topology domain
// (node.labels[topologyKey]); kube's interpodaffinity scoring sums per matching pod. A node
// lacking the topology label earns no score; a running pod on a node lacking it contributes
// none. Requires gang-member agreement on the term list (else no scores). Best-effort — not
// full kube-scheduler score parity (symmetry via running pods' preferred terms is deferred).
let pref_pod = &members[0].preferred_pod_affinity;
let pref_pod_agree = members
    .iter()
    .all(|m| m.preferred_pod_affinity == *pref_pod);
if pref_pod_agree {
    for cn in &feasible_nodes {
        for term in pref_pod {
            let Some(cand_domain) = domain(cn, &term.topology_key) else {
                continue; // candidate node has no such topology domain -> no score
            };
            let delta = if term.anti { -term.weight } else { term.weight };
            for (rn, pods) in &running_by_node {
                if domain(rn, &term.topology_key).as_deref() != Some(cand_domain.as_str()) {
                    continue;
                }
                for w in pods {
                    if selector_scopes_ns(&term.selector, &rep.namespace, &w.namespace, ns_labels)
                        && selector_matches(&term.selector.reqs, &w.labels)
                    {
                        *soft_scores.entry(cn.clone()).or_default() += delta;
                    }
                }
            }
        }
    }
}
soft_scores.retain(|_, v| *v != 0);
```

  And change the node-affinity block insertion from:
  ```rust
  if score > 0 {
      soft_scores.insert(node_name.clone(), score);
  }
  ```
  to:
  ```rust
  if score != 0 {
      *soft_scores.entry(node_name.clone()).or_default() += score;
  }
  ```

- [ ] **Step 4: Run — expect PASS**; full `cargo test --features rust-cp-sat` green; `cargo clippy` clean.
- [ ] **Step 5: Commit** — `git commit -am "Compute soft scores from preferred pod (anti-)affinity (forward, domain-aware)"`

---

### Task 4: cpsat negative-score regression + README/docs

**Files:**
- Modify: `ksolver/src/cpsat_rust.rs` (one test)
- Modify: `README.md` (soft-affinity paragraph), `docs/superpowers/specs/2026-07-01-soft-affinity-scoring-design.md` (status)

- [ ] **Step 1: Add cpsat test** proving a NEGATIVE soft score steers admission-neutral placement away from a node (two cost-equal nodes, singleton feasible on both, `soft_scores = {n1: -10}` ⇒ lands on n2; admission unchanged). Mirror `soft_affinity_breaks_ties_without_changing_admission`.
- [ ] **Step 2: Run — expect PASS.**
- [ ] **Step 3: Update README** — extend the "Preferred (soft) node affinity" paragraph to note preferred **pod** affinity/anti-affinity is now honored too (forward-only, domain-aware, same admission+cost guarantee; symmetry deferred).
- [ ] **Step 4: Update the soft-affinity spec** status section: preferred pod (anti-)affinity implemented (forward-only); symmetry the remaining follow-up.
- [ ] **Step 5: Full verify** — `cargo test` + `cargo clippy` clean.
- [ ] **Step 6: Commit** — `git commit -am "Preferred pod affinity: negative-score cpsat test + docs"`

---

## Self-Review

- **Spec coverage:** parse (T2) → score (T3) → cpsat neutrality (already two-phase; T4 adds negative case) → docs (T4). Shadow already sets `enable_soft_affinity: true` and calls the diagnosed builder — no shadow.rs change.
- **Type consistency:** `PreferredPodTerm` fields used identically in model/pod_filter/pending_input/tests. `soft_scores` stays `BTreeMap<String,i64>`; cpsat unchanged.
- **Admission/cost safety:** contributions flow ONLY into `soft_scores`; phase-1 pins cost + admitted set. Negative scores only reorder within cost-equal placements.
- **No placeholders.**
