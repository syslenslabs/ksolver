# GPU Scheduler — Cross-Namespace Pod Anti-Affinity (F-CNS-1) — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: superpowers:subagent-driven-development or superpowers:executing-plans. Steps use checkbox (`- [ ]`) syntax.

**Goal:** Model pod anti-affinity terms scoped to an explicit `namespaces` list (not just the pod's
own namespace), per the design in `docs/superpowers/specs/2026-07-01-cross-namespace-antiaffinity-design.md`.
`namespaceSelector` stays unmodeled + caveated (F-CNS-2).

**Architecture:** Each modeled selector gains a namespace scope. Replace the selector element type
`Vec<LabelSelectorReq>` with `AntiAffinitySelector { reqs: Vec<LabelSelectorReq>, namespaces: Vec<String> }`
(empty `namespaces` = the pod's own namespace ⇒ byte-identical to today). The exclusion closures in
`build_pending_input` replace the `w.namespace == rep.namespace` guard with a `scope_matches` test.
Extraction (collector + pod_filter) captures the `namespaces` list; `namespaceSelector`-bearing terms
remain unmodeled (caveat retained).

**Tech Stack:** Rust; `model.rs`, `collector.rs`, `scheduler/pod_filter.rs`, `scheduler/pending_input.rs`.
Touches the verified anti-affinity path — every 5e–5h/12/matchExpressions test must stay green
(own-namespace behavior byte-identical).

## Global Constraints

- **Own-namespace behavior unchanged:** a selector with empty `namespaces` behaves exactly as today.
  All existing anti-affinity tests pass after adapting helpers to the new struct.
- **F-CNS-1 = explicit `namespaces` list only.** A term with `namespaceSelector` stays unmodeled +
  "pod anti-affinity" caveat (F-CNS-2 later).
- **Kube semantics:** non-empty `namespaces` is the explicit scope (own ns NOT auto-included unless
  listed); empty ⇒ own ns.
- **No feasibility/placement change beyond the scope generalization;** `selector_matches` (on `reqs`)
  is unchanged.
- `cargo fmt` + clean clippy; new cross-namespace tests; binds nothing.

## Tasks

### Task 1: Model type
- [ ] `model.rs`: add
```rust
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct AntiAffinitySelector {
    #[serde(default)] pub reqs: Vec<LabelSelectorReq>,
    #[serde(default)] pub namespaces: Vec<String>, // empty = own namespace
}
```
Change the six fields: `Pod.modeled_host_anti_selectors` + `Pod.anti_affinity_topology_selectors`,
same two on `NormalizedWorkload`, and `PendingGpuPod`'s two — host: `Vec<AntiAffinitySelector>`;
topology: `Vec<(String, AntiAffinitySelector)>`. Build; fix literals (mostly `..Default` / test
helpers). Commit.

### Task 2: Extraction (collector + pod_filter)
- [ ] `collector::modeled_anti_selectors_all` and `pod_filter::modeled_anti_affinity_selectors`:
  currently reject a term when `namespaces` non-empty OR `namespace_selector.is_some()`. Change to:
  reject only when `namespace_selector.is_some()` (F-CNS-2); capture `term.namespaces` (default empty)
  into `AntiAffinitySelector.namespaces`. Emit `(topology_key, AntiAffinitySelector{reqs, namespaces})`.
- [ ] Unit tests: a hostname term with `namespaces:[other]` + matchLabels ⇒ modeled with
  `namespaces == ["other"]`; a term with `namespaceSelector` ⇒ NOT modeled (caveat present); a plain
  term ⇒ `namespaces == []`. Run → commit.

### Task 3: Matching (pending_input)
- [ ] Add `fn scope_matches(sel_ns: &[String], own_ns: &str, other_ns: &str) -> bool { if sel_ns.is_empty() { other_ns == own_ns } else { sel_ns.iter().any(|n| n == other_ns) } }`.
- [ ] Forward closure (5e/12): for each pending selector `s`, a running pod `w` triggers exclusion iff
  `scope_matches(&s.namespaces, rep.namespace, &w.namespace) && selector_matches(&s.reqs, &w.labels)`
  (hostname: same node; topology: same domain). Remove the blanket `w.namespace != rep.namespace`
  early-return; apply the scope per selector instead.
- [ ] Symmetric closure (5h): a running pod's selector `rs` triggers iff
  `scope_matches(&rs.namespaces, w.namespace, rep.namespace) && member_labels.all(|ml| selector_matches(&rs.reqs, ml))`
  (the running pod's scope must include the PENDING pod's namespace).
- [ ] Cross-workload (5g): the pair guard `a.namespace == b.namespace` generalizes — a's selector
  applies to b iff `scope_matches(&a_sel.namespaces, a.namespace, &b.namespace)` (and symmetrically).
- [ ] **Self-anti-affine gang (5f) — codex:** the `self_anti` computation must also require the
  selector to apply to the gang's OWN namespace. A gang's members all share `rep.namespace`, so add
  `scope_matches(&s.namespaces, rep.namespace, rep.namespace)` (empty ⇒ own-ns ⇒ today's behavior;
  else own ns must be listed) alongside the existing `selector_matches(&s.reqs, member labels)`. This
  prevents a gang whose selector targets only other namespaces from wrongly self-spreading /
  triggering the colocate-vs-self-spread conflict.
- [ ] `canonical_selectors`/`canonical_topology_selectors`: include `namespaces` (sorted) in the
  canonical form for gang-member agreement.
- [ ] Update test helpers (`reqs`/`sel_list`/`ppod_aa`/`gang_member_aa`/`running_anti`/`ppod_topo`) to
  build `AntiAffinitySelector` with `namespaces: []`. All existing anti-affinity assertions unchanged.
- [ ] New tests: (a) pending pod with `namespaces:[other]` hostname anti-affinity excludes a node
  hosting a matching pod in `other`, but NOT one in a third namespace `third`; (b) same-namespace
  (empty ns) still works; (c) symmetric across namespaces. Run → commit.

### Task 4: Verify + docs
- [ ] Full `cargo test --features rust-cp-sat` + clippy. README: anti-affinity now models explicit
  cross-namespace `namespaces` lists (namespaceSelector still caveated). Update memory.

## Self-Review Notes
- Empty `namespaces` ⇒ own-namespace ⇒ no regression; all 5e–5h/12/matchExpr tests preserved.
- `namespaceSelector` remains unmodeled + caveated (F-CNS-2).
- Only the namespace scope generalizes; `reqs` matching unchanged.
- Canonical form includes namespaces for deterministic gang agreement.
- Self-anti-affine (5f) gates on the selector applying to the gang's own namespace (codex), so an
  other-namespace-scoped selector doesn't wrongly self-spread.
