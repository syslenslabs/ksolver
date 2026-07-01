# Co-Placement Preferred Pod Affinity Implementation Plan

> **For agentic workers:** Steps use checkbox (`- [ ]`) syntax. TDD, frequent commits.

**Goal:** When two *pending* pods/gangs mutually (or one-directionally) express `preferredDuringScheduling` pod **affinity** toward each other, softly reward the solver for co-placing them in the same topology domain — jointly optimized in the batch, going beyond the sequential kube-scheduler (which can only score against already-existing pods). Never changes admission or cost.

**Architecture:** `pending_input` detects preferring pairs among emitted pending workloads and pre-computes their shared topology **domains** (as node-name lists — cpsat stays label-agnostic). A new `OptimizationInput.soft_coplacement_pairs` carries them. In the CP-SAT **Phase 2** soft pass (which already pins the Phase-1 cost optimum + admitted set), each pair×domain gets one boolean `both` with upper bounds `both ≤ Σ x_a[domain]` and `both ≤ Σ x_b[domain]`; the soft objective rewards `both` (maximization sets it to 1 iff both workloads place in that domain). Because these vars live only in the soft objective and admission/cost are pinned, co-placement can only reorder cost-equal, admission-equal placements.

**Tech Stack:** Rust, CP-SAT (`cp_sat` crate), existing shadow scheduler modules.

## Global Constraints

- Shadow-only; binds nothing. Co-placement vars/constraints are added ONLY inside the existing Phase-2 `want_soft` block (never Phase 1), so admission/cost are pinned before they exist.
- **Affinity (reward) only.** Soft pending-pending *anti*-affinity (penalize co-domain) needs the opposite (forced-up) linearization and is rare — its hard form is already handled by cross-workload anti-affinity (Phase 5g). Documented as out of scope.
- A pending workload's preferred terms must have gang-member agreement (reuse the forward-soft `pref_pod_agree` gate) to contribute.
- Domains are label-based (`node.labels[topologyKey]`) for all keys incl. `kubernetes.io/hostname`; a node lacking the key is in no domain. Only domains where BOTH workloads have ≥1 feasible node are emitted (others can never satisfy `both`).
- Both directions (a→b and b→a) are emitted as separate rewards when both preferences exist (kube sums directions).

---

### Task 1: Model types `SoftCoplacement` / `CoplacementDomain`

**Files:**
- Modify: `ksolver/src/model.rs` (near `OptimizationInput`, `PreferredPodTerm`)

**Interfaces:**
- Produces:
  ```rust
  pub struct CoplacementDomain { pub a_nodes: Vec<String>, pub b_nodes: Vec<String> }
  pub struct SoftCoplacement { pub a: String, pub b: String, pub weight: i64, pub domains: Vec<CoplacementDomain> }
  ```
  `a`/`b` are `OptimizationWorkload.id`s. `OptimizationInput.soft_coplacement_pairs: Vec<SoftCoplacement>` (serde default).

- [ ] **Step 1: Add structs + field**

```rust
/// One topology domain shared by a co-placement pair: the domain's nodes that are feasible for `a`
/// and (separately) for `b`. `both` can be rewarded only when `a` places in some `a_nodes` AND `b`
/// in some `b_nodes` (same domain).
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct CoplacementDomain {
    #[serde(default)]
    pub a_nodes: Vec<String>,
    #[serde(default)]
    pub b_nodes: Vec<String>,
}

/// A soft co-placement reward: workloads `a` and `b` prefer to share a topology domain. Phase 2
/// rewards `weight` for each domain both land in. Affinity (reward) only; never changes admission/cost.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct SoftCoplacement {
    #[serde(default)]
    pub a: String,
    #[serde(default)]
    pub b: String,
    #[serde(default)]
    pub weight: i64,
    #[serde(default)]
    pub domains: Vec<CoplacementDomain>,
}
```
  Add to `OptimizationInput`:
```rust
    #[serde(default)]
    pub soft_coplacement_pairs: Vec<SoftCoplacement>,
```

- [ ] **Step 2: Build** — `CARGO_NET_OFFLINE=true cargo build --features rust-cp-sat`. Fix any `OptimizationInput { … }` literals lacking `..Default::default()` (add the field `vec![]`).
- [ ] **Step 3: Commit** — `git commit -am "Add SoftCoplacement model types"`

---

### Task 2: CP-SAT Phase-2 co-placement reward

**Files:**
- Modify: `ksolver/src/cpsat_rust.rs` (the `want_soft` gate + the Phase-2 soft block, ~lines 598–639)

**Interfaces:**
- Consumes: `input.soft_coplacement_pairs`, existing `x_vars: HashMap<(String,String), IntVar>`.

- [ ] **Step 1: Add cpsat test** (feature-gated, mirrors `soft_affinity_breaks_ties_without_changing_admission`)

```rust
#[test]
fn coplacement_rewards_same_node_without_changing_admission() {
    use crate::model::{CoplacementDomain, ScenarioConfig, SoftCoplacement};
    use std::collections::BTreeMap;
    // Two singletons a,b each feasible on n1,n2 (cost-equal). Co-placement reward on the
    // per-node "domains" (hostname). With no other pressure the solver should co-locate them.
    let a = gpu_singleton("a", 1, &["n1", "n2"]);
    let b = gpu_singleton("b", 1, &["n1", "n2"]);
    let input = OptimizationInput {
        nodes: vec![gpu_node("n1", 4), gpu_node("n2", 4)],
        workloads: vec![a, b],
        soft_coplacement_pairs: vec![SoftCoplacement {
            a: "t/a".to_string(),
            b: "t/b".to_string(),
            weight: 50,
            domains: vec![
                CoplacementDomain { a_nodes: vec!["n1".into()], b_nodes: vec!["n1".into()] },
                CoplacementDomain { a_nodes: vec!["n2".into()], b_nodes: vec!["n2".into()] },
            ],
        }],
        ..Default::default()
    };
    let scenario = ScenarioConfig {
        solver: "cp-sat-rust".to_string(),
        partial_admission: true,
        enable_soft_affinity: true,
        ..Default::default()
    };
    let (sol, info) = super::enabled::solve(&input, &scenario).expect("solve");
    assert_eq!(admitted_count(&sol), 2, "admission unchanged; status={}", info.status);
    let a_node = sol.assignment_counts.get("t/a").unwrap().keys().next().unwrap().clone();
    let b_node = sol.assignment_counts.get("t/b").unwrap().keys().next().unwrap().clone();
    assert_eq!(a_node, b_node, "co-placement reward should put a and b on the same node");
}
```

- [ ] **Step 2: Run — expect FAIL** (reward not applied; placement arbitrary/split)
- [ ] **Step 3: Implement** — (a) widen the gate:

```rust
let want_soft = scenario.enable_soft_affinity
    && status == CpSolverStatus::Optimal
    && (input.workloads.iter().any(|w| !w.soft_scores.is_empty())
        || !input.soft_coplacement_pairs.is_empty());
```

  (b) inside the `else` branch, after building `soft` from `soft_scores` and BEFORE `model.minimize(soft)`, add co-placement reward vars:

```rust
// Co-placement rewards (Phase 2 only): reward `both` when a and b share a domain. Upper bounds
// only — maximization (minimize -weight) sets `both`=1 iff a and b both place in the domain.
// Admission/cost already pinned above, so this only reorders cost-equal placements.
for (ci, cp) in input.soft_coplacement_pairs.iter().enumerate() {
    if cp.weight <= 0 {
        continue;
    }
    for (di, dom) in cp.domains.iter().enumerate() {
        let mut sum_a = LinearExpr::default();
        for n in &dom.a_nodes {
            if let Some(x) = x_vars.get(&(cp.a.clone(), n.clone())) {
                sum_a += (1_i64, *x);
            }
        }
        let mut sum_b = LinearExpr::default();
        for n in &dom.b_nodes {
            if let Some(x) = x_vars.get(&(cp.b.clone(), n.clone())) {
                sum_b += (1_i64, *x);
            }
        }
        let both = model.new_bool_var_with_name(format!("coplace_{ci}_{di}"));
        let mut both_e = LinearExpr::default();
        both_e += (1_i64, both);
        model.add_le(both_e.clone(), sum_a); // both <= Σ x_a in domain
        model.add_le(both_e, sum_b); // both <= Σ x_b in domain
        soft += (-cp.weight, both); // minimize -weight*both == maximize reward
    }
}
```

- [ ] **Step 4: Run — expect PASS**; full `cargo test` + `cargo clippy --features rust-cp-sat` clean.
- [ ] **Step 5: Add a no-over-admit test** — same two singletons but one 1-GPU node; co-placement on `{n1}`; assert `admitted_count == 1` (capacity still caps; reward cannot force both on).
- [ ] **Step 6: Commit** — `git commit -am "CP-SAT Phase-2 co-placement reward (affinity, admission/cost-preserving)"`

---

### Task 3: Detect pairs + domains in `pending_input`

**Files:**
- Modify: `ksolver/src/scheduler/pending_input.rs`

**Interfaces:**
- Produces `soft_coplacement_pairs` on the returned `OptimizationInput`. Reuses `node_labels`, `domain`, `selector_scopes_ns`, `selector_matches`, and the per-emitted-workload metadata.

- [ ] **Step 1: Extend emitted metadata** — alongside `emitted_meta`, collect for each emitted workload a tuple `(id, namespace, feasible_nodes.clone(), member_labels_owned, preferred_pod_affinity_if_gang_agreed)`. Reuse the existing `pref_pod`/`pref_pod_agree` computed in the loop (store the agreed terms, else empty). Store `member_labels` as `Vec<BTreeMap<String,String>>` (owned).

- [ ] **Step 2: Write failing test**

```rust
#[test]
fn coplacement_pair_emitted_for_mutual_preferred_affinity() {
    // a prefers to be near app=b (hostname); a,b singletons feasible on n1,n2.
    let mut n1 = node("n1", 16000, 64, 110, 8);
    n1.labels = [("kubernetes.io/hostname".into(), "n1".into())].into();
    let mut n2 = node("n2", 16000, 64, 110, 8);
    n2.labels = [("kubernetes.io/hostname".into(), "n2".into())].into();
    let cluster = NormalizedCluster {
        nodes: vec![n1, n2],
        workloads: vec![
            labeled_pending("team", "a", &["n1", "n2"], &[("app", "a")]),
            labeled_pending("team", "b", &["n1", "n2"], &[("app", "b")]),
        ],
        ..Default::default()
    };
    let mut pa = ppod("team", "a", None);
    pa.preferred_pod_affinity = vec![crate::model::PreferredPodTerm {
        weight: 40,
        topology_key: "kubernetes.io/hostname".into(),
        selector: sel(&[("app", "b")]),
        anti: false,
    }];
    let input = build_pending_input(&cluster, &[pa, ppod("team", "b", None)]);
    let cps = &input.soft_coplacement_pairs;
    assert_eq!(cps.len(), 1);
    assert_eq!(cps[0].a, "pod:team/a");
    assert_eq!(cps[0].b, "pod:team/b");
    assert_eq!(cps[0].weight, 40);
    // two hostname domains (n1, n2), each with a and b feasible
    assert_eq!(cps[0].domains.len(), 2);
}

#[test]
fn no_coplacement_pair_when_no_preferred_affinity() {
    let cluster = NormalizedCluster {
        nodes: vec![node("n1", 16000, 64, 110, 8)],
        workloads: vec![
            labeled_pending("team", "a", &["n1"], &[("app", "a")]),
            labeled_pending("team", "b", &["n1"], &[("app", "b")]),
        ],
        ..Default::default()
    };
    let input = build_pending_input(&cluster, &[ppod("team", "a", None), ppod("team", "b", None)]);
    assert!(input.soft_coplacement_pairs.is_empty());
}

#[test]
fn coplacement_pair_skips_anti_affinity_terms() {
    // a has preferred ANTI-affinity toward b -> NOT a co-placement reward (out of scope).
    let cluster = NormalizedCluster {
        nodes: vec![node("n1", 16000, 64, 110, 8), node("n2", 16000, 64, 110, 8)],
        workloads: vec![
            labeled_pending("team", "a", &["n1", "n2"], &[("app", "a")]),
            labeled_pending("team", "b", &["n1", "n2"], &[("app", "b")]),
        ],
        ..Default::default()
    };
    let mut pa = ppod("team", "a", None);
    pa.preferred_pod_affinity = vec![crate::model::PreferredPodTerm {
        weight: 40,
        topology_key: "kubernetes.io/hostname".into(),
        selector: sel(&[("app", "b")]),
        anti: true, // anti -> skipped
    }];
    let input = build_pending_input(&cluster, &[pa, ppod("team", "b", None)]);
    assert!(input.soft_coplacement_pairs.is_empty());
}
```

- [ ] **Step 3: Run — expect FAIL**
- [ ] **Step 4: Implement** — after the cross-workload anti-affinity pairing loop, add co-placement detection. For each ordered pair (i, j), i ≠ j, of emitted workloads, for each of workload i's AGREED preferred terms with `!anti`, if `selector_scopes_ns(term.selector, ns_i, ns_j, ns_labels)` and the term's reqs match ALL of j's member labels, build domains by grouping i's and j's feasible nodes by `domain(node, term.topology_key)` (skip `None`); keep domains where both sides non-empty; if any, push `SoftCoplacement { a: id_i, b: id_j, weight: term.weight, domains }`.

```rust
// Soft co-placement (preferred pod AFFINITY between two pending workloads). Beyond kube (which
// scores only vs running pods). For each ordered pair, workload i's agreed preferred-affinity term
// matching ALL of j's member labels rewards co-placing them in a shared topology domain.
let mut soft_coplacement_pairs: Vec<QuotaGroupUnusedPlaceholder> = Vec::new(); // see note
for i in 0..emitted_pref.len() {
    for j in 0..emitted_pref.len() {
        if i == j {
            continue;
        }
        let (id_i, ns_i, _feas_i, _labels_i, terms_i) = &emitted_pref[i];
        let (id_j, ns_j, feas_j, labels_j, _terms_j) = &emitted_pref[j];
        for term in terms_i {
            if term.anti {
                continue;
            }
            if !selector_scopes_ns(&term.selector, ns_i, ns_j, ns_labels) {
                continue;
            }
            if !labels_j.iter().all(|ml| selector_matches(&term.selector.reqs, ml)) {
                continue;
            }
            // group feasible nodes by topology domain value
            let mut by_domain: BTreeMap<String, (Vec<String>, Vec<String>)> = BTreeMap::new();
            for n in &emitted_pref[i].2 {
                if let Some(d) = domain(n, &term.topology_key) {
                    by_domain.entry(d).or_default().0.push(n.clone());
                }
            }
            for n in feas_j {
                if let Some(d) = domain(n, &term.topology_key) {
                    by_domain.entry(d).or_default().1.push(n.clone());
                }
            }
            let domains: Vec<crate::model::CoplacementDomain> = by_domain
                .into_values()
                .filter(|(a, b)| !a.is_empty() && !b.is_empty())
                .map(|(a_nodes, b_nodes)| crate::model::CoplacementDomain { a_nodes, b_nodes })
                .collect();
            if !domains.is_empty() {
                soft_coplacement_pairs.push(crate::model::SoftCoplacement {
                    a: id_i.clone(),
                    b: id_j.clone(),
                    weight: term.weight,
                    domains,
                });
            }
        }
    }
}
```
  (Use the real type `Vec<crate::model::SoftCoplacement>`; the `QuotaGroupUnusedPlaceholder` name above is illustrative — declare `let mut soft_coplacement_pairs: Vec<crate::model::SoftCoplacement> = Vec::new();`.)

  Add `soft_coplacement_pairs` to the returned `OptimizationInput { … }` literal.

- [ ] **Step 5: Populate `emitted_pref`** — in the gang loop, after computing `pref_pod`/`pref_pod_agree` and `feasible_nodes`/`member_labels`, push `(id.clone(), rep.namespace.clone(), feasible_nodes.clone(), member_labels_owned, if pref_pod_agree { pref_pod.clone() } else { vec![] })` to a `Vec` declared before the loop. (`member_labels_owned` = `member_workloads.iter().map(|w| w.labels.clone()).collect()`.)
- [ ] **Step 6: Run — expect PASS**; full `cargo test` + clippy clean.
- [ ] **Step 7: Commit** — `git commit -am "Detect co-placement preferred-affinity pairs + domains in pending_input"`

---

### Task 4: Docs + full verify

**Files:** `README.md`, `docs/superpowers/specs/2026-07-01-soft-affinity-scoring-design.md`

- [ ] **Step 1: README** — note co-placement of two pending pods with mutual preferred affinity is now jointly rewarded (beyond kube's sequential scoring); soft pending-pending anti-affinity remains out of scope (hard form covered by cross-workload anti-affinity).
- [ ] **Step 2: Spec** — mark co-placement DONE; preferred/soft affinity feature area COMPLETE.
- [ ] **Step 3: Full verify** — `cargo test --features rust-cp-sat` + `cargo clippy --features rust-cp-sat` clean.
- [ ] **Step 4: Commit** — `git commit -am "Docs: co-placement preferred pod affinity (soft-affinity area complete)"`

---

## Self-Review

- **Spec coverage:** model (T1) → cpsat reward (T2) → detection (T3) → docs (T4).
- **Admission/cost safety:** co-placement vars exist ONLY in Phase 2 after cost + admitted set are pinned; they appear only in the soft objective. Upper-bound-only `both` cannot force placement, so capacity still caps admission (T2 step 5 proves it).
- **Linearization correctness:** reward + upper bounds ⇒ `both`=1 iff both present (maximization). Anti (forced-up) deliberately excluded.
- **Type consistency:** `SoftCoplacement`/`CoplacementDomain` used identically in model/pending_input/cpsat/tests. `emitted_pref` tuple shape fixed in T3.
- **No placeholders** (the `QuotaGroupUnusedPlaceholder` note is explicitly corrected to the real type).
