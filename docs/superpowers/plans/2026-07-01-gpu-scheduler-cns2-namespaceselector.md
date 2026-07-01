# GPU Scheduler — Cross-Namespace Anti-Affinity F-CNS-2 (namespaceSelector) — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: superpowers:subagent-driven-development or superpowers:executing-plans. Steps use checkbox (`- [ ]`) syntax.

**Goal:** Model pod anti-affinity terms scoped by `namespaceSelector` (label-select namespaces;
empty selector `{}` = ALL namespaces), completing cross-namespace anti-affinity (F-CNS-1 did explicit
`namespaces` lists). Per `docs/superpowers/specs/2026-07-01-cross-namespace-antiaffinity-design.md`.

**Why:** F-CNS-1 handles explicit `namespaces` lists; `namespaceSelector` (e.g. "all namespaces
labelled team=x", or empty = every namespace) is still unmodeled + caveated. Meaningful for
multi-tenant + cluster-wide anti-affinity. Additive/filtering (like all prior anti-affinity work),
verifiable — NOT a solver/objective change.

**Architecture:** Collect Namespace labels into the shadow path (new `NamespaceMeta{name, labels}` on
`ClusterSnapshot` → `NormalizedCluster.namespace_labels`). `AntiAffinitySelector` gains
`namespace_selector: Option<Vec<LabelSelectorReq>>` (None = not set ⇒ own-namespace/explicit-list
behavior unchanged; `Some(vec![])` = empty selector = ALL namespaces; `Some(reqs)` = label-matched).
The builder resolves scope against `namespace_labels`: a pod in `other_ns` is in a selector's scope
iff `other_ns` is in the explicit `namespaces` list OR (the namespaceSelector is set AND matches
`namespace_labels[other_ns]`, with empty-selector matching all). Extraction stops rejecting
namespaceSelector terms and captures the selector.

**Tech Stack:** Rust; `model.rs`, `collector.rs`, `normalizer.rs`, `scheduler/pod_filter.rs`,
`scheduler/pending_input.rs`. Additive collection; anti-affinity filtering only.

## Global Constraints

- **No regression:** `namespace_selector: None` + empty `namespaces` ⇒ own-namespace (byte-identical
  to F-CNS-1/today). All existing anti-affinity tests pass after helper adaptation (the new field is
  serde-default `None`).
- **Empty namespaceSelector `{}` = ALL namespaces** (Kubernetes rule); a non-empty selector matches
  namespaces whose labels satisfy it (reuse `req_matches` over namespace labels).
- **Additive collection:** Namespace listing is new + serde-default; `analyze`/offline path unaffected
  (it simply gets namespace labels too, harmlessly).
- `cargo fmt` + clean clippy; new namespaceSelector tests; binds nothing.

## Tasks

### Task 1: Namespace collection + model
- [ ] `model.rs`: add `pub struct NamespaceMeta { #[serde(default)] pub name: String, #[serde(default)] pub labels: BTreeMap<String,String> }`; add `#[serde(default)] pub namespaces: Vec<NamespaceMeta>` to `ClusterSnapshot`; add `#[serde(default)] pub namespace_labels: BTreeMap<String, BTreeMap<String,String>>` to `NormalizedCluster`. Also add `namespace_selector: Option<Vec<LabelSelectorReq>>` (serde-default `None`) to `AntiAffinitySelector`.
- [ ] `collector.rs` `collect()`: list `corev1::Namespace` (`Api::all`), map to `NamespaceMeta{ name, labels }`, set `snapshot.namespaces`. (Add to the `try_join!`/list set.)
- [ ] `normalizer.rs`: populate `NormalizedCluster.namespace_labels` from `snapshot.namespaces` (name→labels map).
- [ ] Build; fix literals (`AntiAffinitySelector` gains `namespace_selector` — most constructed via helpers/`..Default`). Commit.

### Task 2: Extraction captures namespaceSelector
- [ ] `collector::modeled_anti_selectors_all` + `pod_filter::modeled_anti_affinity_selectors`: STOP the `namespace_selector.is_some()` early-continue. Instead capture it: `namespace_selector = term.namespace_selector.as_ref().and_then(label_selector_to_reqs_optional)`. But note `{}` (present, empty) must become `Some(vec![])` (all-namespaces), while a namespaceSelector with **matchExpressions we can't model** ⇒ leave the whole term unmodeled + caveat. Add a helper `namespace_selector_to_reqs(ls) -> Option<Option<Vec<LabelSelectorReq>>>` returning `Some(Some(reqs))` for modelable, `Some(None)`… — simpler: reuse `label_selector_to_reqs` semantics but treat an EMPTY selector as `Some(vec![])` (all) rather than None. Define precisely: empty `{}` ⇒ `Some(vec![])`; modelable matchLabels/matchExpressions ⇒ `Some(reqs)`; unmodelable ⇒ term skipped (caveat).
- [ ] Set `AntiAffinitySelector.namespace_selector` accordingly (None when the term has no namespaceSelector).
- [ ] Unit tests (pod_filter): a term with empty `namespaceSelector {}` ⇒ modeled with `namespace_selector == Some(vec![])`; a term with `namespaceSelector matchLabels{team:x}` ⇒ `Some([team In x])`; an unmodelable namespaceSelector (matchExpressions we reject) ⇒ term not modeled + caveat. Commit.

### Task 3: Builder resolves scope with namespace labels
- [ ] In `pending_input`, replace `scope_matches(sel_ns, own_ns, other_ns)` with `selector_scopes_ns(sel: &AntiAffinitySelector, own_ns, other_ns, ns_labels: &BTreeMap<String, BTreeMap<String,String>>) -> bool`:
```
if sel.namespaces.is_empty() && sel.namespace_selector.is_none() { other_ns == own_ns }
else {
    sel.namespaces.iter().any(|n| n == other_ns)
    || match &sel.namespace_selector {
        None => false,
        Some(reqs) => reqs.is_empty() /* {} = all */
            || ns_labels.get(other_ns).map(|l| reqs.iter().all(|r| req_matches(r, l))).unwrap_or(false),
    }
}
```
- [ ] Thread `&cluster.namespace_labels` into the exclusion closures (forward/symmetric/self-anti/cross-workload) and call `selector_scopes_ns`. Self-anti still uses own-namespace (`selector_scopes_ns(s, rep.ns, rep.ns, ns_labels)` — empty-selector all-namespaces DOES include own ns, so a `{}`-scoped self-referential gang self-spreads, which is correct).
- [ ] `canonical_selector` includes `namespace_selector` (sorted reqs + a marker for None/Some(empty)/Some(reqs)) for gang agreement.
- [ ] Tests: a pending pod with empty-`{}` namespaceSelector hostname anti-affinity excludes a node hosting a matching pod in ANY namespace; a `team=x` namespaceSelector excludes only matching pods in namespaces labelled team=x (needs `namespace_labels`); own-namespace (None) unchanged. Commit.

### Task 4: Verify + docs
- [ ] Full `cargo test --features rust-cp-sat` + clippy. README: namespaceSelector (incl. empty = all namespaces) now modeled; only unmodelable namespaceSelector matchExpressions remain caveated. Update memory.

## Self-Review Notes
- `namespace_selector: None` + empty `namespaces` ⇒ own-namespace ⇒ no regression.
- Empty `{}` selector = all namespaces (kube); label selector matched via namespace labels.
- Namespace collection additive/serde-default; offline path unaffected.
- Filtering-only; no solver/objective change.
