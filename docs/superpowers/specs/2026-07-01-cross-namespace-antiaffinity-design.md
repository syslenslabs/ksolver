# Cross-Namespace Pod Anti-Affinity — Design Spec

**Status:** Draft for review (design pass; no implementation). Advances the deferred "cross-namespace
anti-affinity" item.
**Author:** autonomous design pass (2026-07-01), pending user review.
**Related:** anti-affinity phases 5e–5h, 12, matchExpressions; `scheduler/pending_input.rs`,
`scheduler/pod_filter.rs`, `collector.rs`.

## Problem

Modeled pod anti-affinity is currently **same-namespace only**: `collector::modeled_anti_selectors_all`
and `pod_filter::modeled_anti_affinity_selectors` reject any term with `namespaces` or
`namespaceSelector`, and the exclusion closures in `build_pending_input` guard `w.namespace ==
rep.namespace`. Kubernetes lets an anti-affinity term target other namespaces via `namespaces` (an
explicit list) and/or `namespaceSelector` (label-select namespaces). Such terms are today left
unmodeled but **disclosed** via the "pod anti-affinity" caveat — so we are not silently wrong, only
incomplete. This is a low-frequency feature; the design exists so it can be implemented cleanly when
warranted, without a rushed change to the verified selector machinery.

## Kubernetes semantics (what to model)

For a pod anti-affinity term, the set of namespaces it applies to is:
- `namespaces` and `namespaceSelector` both nil/empty ⇒ **the pod's own namespace** (current behavior).
- `namespaces` non-empty ⇒ exactly that explicit list (own namespace is NOT auto-included unless
  listed).
- `namespaceSelector` present ⇒ the namespaces whose labels match it; **empty selector `{}` ⇒ ALL
  namespaces**. Combined with `namespaces` as a **union**.
The term then matches pods (by its `labelSelector`) in that namespace set, in the given `topologyKey`.

## Scope decision (recommended)

- **F-CNS-1: explicit `namespaces` list only.** Model terms whose namespace scope is a concrete list
  (`namespaces: [a, b]`), no `namespaceSelector`. This is the common cross-namespace form and needs no
  namespace-label collection.
- **F-CNS-2: `namespaceSelector`.** Requires collecting **Namespace labels** and evaluating the
  selector (incl. the empty-selector = all-namespaces case). Deferred to a second step (adds a
  collection dependency + selector eval over namespaces).

## Data-model change

The modeled selector currently carries only the `labelSelector` requirements (`Vec<LabelSelectorReq>`
per selector; topology variants carry `(topologyKey, reqs)`). Add a **namespace scope** per selector:

```rust
pub struct AntiAffinitySelector {          // replaces the bare Vec<LabelSelectorReq> element
    pub reqs: Vec<LabelSelectorReq>,       // labelSelector requirements (unchanged semantics)
    pub namespaces: Vec<String>,           // explicit namespace scope; EMPTY = own namespace
}
```
The four fields (`Pod`/`NormalizedWorkload` host + topology, and `PendingGpuPod` host + topology)
become `Vec<AntiAffinitySelector>` / `Vec<(String, AntiAffinitySelector)>`. `matchLabels`-only terms
lower to `namespaces: []` (own namespace) ⇒ **byte-identical to today**. This is the same
representation-change pattern used for matchExpressions (which is exactly why it should be done
deliberately, not rushed — it touches the verified 5e–5h/12 tests again).

## Matching change (build_pending_input)

Replace the `w.namespace == rep.namespace` guards with a scope test:
```
fn scope_matches(sel_namespaces: &[String], own_ns: &str, running_ns: &str) -> bool {
    if sel_namespaces.is_empty() { running_ns == own_ns }      // own-namespace (today)
    else { sel_namespaces.iter().any(|n| n == running_ns) }    // explicit list
}
```
- **Forward (5e/5h/12):** a pending pod's selector `(reqs, namespaces)` excludes a candidate node if a
  running pod in a namespace within `namespaces` (or own ns if empty) matches `reqs` (hostname: same
  node; topology: same domain).
- **Symmetric (5h):** a running pod's selector scoped to a set including the pending pod's namespace
  excludes the running pod's node/domain for the pending pod.
- **Cross-workload (5g):** two pending workloads — the pairing namespace guard generalizes to
  "the other workload's namespace is within this selector's scope."

## Component impact

- `collector.rs` / `pod_filter.rs`: stop rejecting `namespaces`-list terms; capture the list into the
  new `AntiAffinitySelector.namespaces`. Keep rejecting `namespaceSelector` (F-CNS-2) with the caveat.
- `model.rs` + `pod_filter.rs`: the four selector-field type changes + `AntiAffinitySelector` type.
- `pending_input.rs`: `selector_matches` unchanged (still on `reqs`); the exclusion closures gain the
  scope test; `canonical_*` include `namespaces` for gang-member agreement; running-pod lookup must
  span all namespaces (today `running_by_node` already holds all running pods — fine).
- Retain the caveat for `namespaceSelector` and any still-unmodeled term.

## Risks / open questions

- **Re-churn of verified anti-affinity code** — this is the main cost: it re-touches the 5e–5h/12 +
  matchExpressions tests (helpers build the new struct). Do it as one disciplined change with all
  existing assertions preserved (matchLabels/own-namespace behavior must be byte-identical).
- **`namespaceSelector` + empty-selector-means-all** — deferred (F-CNS-2); needs Namespace-label
  collection; empty selector = all namespaces is a sharp edge to handle explicitly.
- **Value vs cost** — cross-namespace anti-affinity is uncommon and already caveated, so this is lower
  priority than same-namespace correctness (done). Implement when a target fleet actually needs it.

## Testing strategy
- Own-namespace terms (empty `namespaces`) behave exactly as today (all 5e–5h/12 tests pass unchanged
  after helper adaptation).
- A pending pod with `namespaces: [other]` anti-affinity excludes a node hosting a matching pod in
  `other` but NOT a matching pod in a third namespace.
- Symmetric + cross-workload variants across namespaces.
- `namespaceSelector` present ⇒ unmodeled + caveat (until F-CNS-2). Shadow-only; binds nothing.

## Out of scope
`namespaceSelector` evaluation (F-CNS-2), and any change to required same-namespace behavior.
