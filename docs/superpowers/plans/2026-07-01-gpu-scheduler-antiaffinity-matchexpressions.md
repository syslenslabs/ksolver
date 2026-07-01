# GPU Scheduler — Model `matchExpressions` in Pod Anti-Affinity — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: superpowers:subagent-driven-development or superpowers:executing-plans. Steps use checkbox (`- [ ]`) syntax.

**Goal:** Extend best-effort pod anti-affinity (Phases 5e–5h, 12) from `matchLabels`-only to full label selectors — `matchExpressions` with `In`/`NotIn`/`Exists`/`DoesNotExist` — for both hostname and non-hostname topology, so more real anti-affinity rules are enforced instead of only caveated.

**Why:** Today the collector/pod_filter model a term ONLY when its selector is pure `matchLabels`; any `matchExpressions` makes the term unmodeled (still disclosed via the "pod anti-affinity" caveat). Real rules commonly use `app In [x,y]` / `Exists`. Modeling them tightens enforcement and conformance. Lower-frequency and already-caveated, so this is correctness-widening, not a silent-bug fix.

**Architecture:** Replace the `matchLabels`-only selector representation (`BTreeMap<String,String>`) with a requirement list. Add `model::LabelSelectorReq { key, operator, values }`. A modeled selector becomes `Vec<LabelSelectorReq>` (one labelSelector's requirements, ANDed). `matchLabels {k:v}` lowers to `LabelSelectorReq{key:k, operator:"In", values:[v]}`; `matchExpressions` are carried as-is for the four set operators. A term is modeled iff EVERY requirement uses a supported operator (In/NotIn/Exists/DoesNotExist) — any other operator (or namespaces/namespaceSelector, unchanged) leaves the term unmodeled (caveat retained). Matching (`selector_matches`) evaluates all requirements against a pod's labels: `In` ⇒ label present and in values; `NotIn` ⇒ label absent OR not in values; `Exists` ⇒ key present; `DoesNotExist` ⇒ key absent. This unifies matchLabels + matchExpressions and preserves all Phase 5e–5h/12 behavior (matchLabels is just In-with-one-value).

**Tech Stack:** Rust; `model.rs`, `collector.rs`, `scheduler/pod_filter.rs`, `scheduler/pending_input.rs`, `normalizer.rs` (passthrough only). Touches the verified anti-affinity path — keep every existing 5e–5h/12 test green.

## Global Constraints

- **Zero behavior change for matchLabels:** `matchLabels {k:v}` ⇒ `In [v]` ⇒ identical matching to today. All existing anti-affinity tests must pass after adapting their helpers to the new type.
- **Supported operators only:** In/NotIn/Exists/DoesNotExist. A term with any other operator is NOT modeled (term dropped, "pod anti-affinity" caveat retained). Exists/DoesNotExist ignore `values`.
- **NotIn/DoesNotExist semantics:** a pod MISSING the key satisfies `NotIn` and `DoesNotExist` (matches Kubernetes label-selector semantics). This matters: it can EXCLUDE more nodes than matchLabels, so verify against the anti-affinity direction (a running pod matching a NotIn selector still triggers exclusion).
- **Same strictness otherwise:** `namespaces`/`namespaceSelector` still make a term unmodeled; hostname vs non-hostname topology split unchanged.
- **Gang member agreement** (canonical comparison) must use the new representation deterministically.
- `cargo fmt` + clean clippy; all existing anti-affinity tests adapted + new matchExpressions tests. Binds nothing.

## File Structure

- `ksolver/src/model.rs` — add `LabelSelectorReq`; change 4 fields: `Pod.modeled_host_anti_selectors` + `Pod.anti_affinity_topology_selectors`, and the same two on `NormalizedWorkload`, from `BTreeMap`/`(String,BTreeMap)` to `Vec<LabelSelectorReq>`/`(String, Vec<LabelSelectorReq>)`.
- `ksolver/src/scheduler/pod_filter.rs` — `PendingGpuPod`'s two selector fields + `modeled_anti_affinity_selectors` build requirement lists (matchLabels + matchExpressions).
- `ksolver/src/collector.rs` — `modeled_anti_selectors_all` builds requirement lists; keep hostname/topology split + strictness.
- `ksolver/src/scheduler/pending_input.rs` — `selector_matches(&[LabelSelectorReq], labels)`, `canonical_selectors`/`canonical_topology_selectors` over the new type; the exclusion closures unchanged in structure.
- `ksolver/src/normalizer.rs` — passthrough of the renamed-type fields (no logic change).

## Tasks

### Task 1: Model type + fields
- [ ] Add `LabelSelectorReq { key: String, operator: String, values: Vec<String> }` (serde-default, like `NodeAffinityTerm`). Change the 4 fields (Pod ×2, NormalizedWorkload ×2) to `Vec<Vec<LabelSelectorReq>>`? NO — each field is a LIST of selectors; a selector is a `Vec<LabelSelectorReq>`. So: host selectors `Vec<Vec<LabelSelectorReq>>`; topology selectors `Vec<(String, Vec<LabelSelectorReq>)>`. Update docs. Build; fix literals. Commit.

### Task 2: Shared extraction (collector + pod_filter)
- [ ] Add a helper mapping a `corev1::LabelSelector` to `Option<Vec<LabelSelectorReq>>` (None ⇒ unmodeled): start from `match_labels` (each ⇒ `In [v]`), then fold in `match_expressions` — accept In/NotIn/Exists/DoesNotExist, return None on any other operator or empty result. Reject when the term has `namespaces`/`namespaceSelector`.
- [ ] `collector::modeled_anti_selectors_all` and `pod_filter::modeled_anti_affinity_selectors` use it, still splitting hostname vs non-hostname by `topology_key`. Unit tests: matchLabels-only ⇒ In reqs; matchExpressions In/Exists ⇒ carried; unsupported operator ⇒ term unmodeled (empty + caveat). Commit.

### Task 3: Matching + canonical (pending_input)
- [ ] Rewrite `selector_matches(reqs: &[LabelSelectorReq], labels: &BTreeMap<String,String>) -> bool`: In ⇒ `labels.get(key)` in values; NotIn ⇒ not in values (missing key ⇒ true); Exists ⇒ key present; DoesNotExist ⇒ key absent; unknown op ⇒ false (defensive). All reqs must hold.
- [ ] Update `canonical_selectors`/`canonical_topology_selectors` to canonicalize `Vec<LabelSelectorReq>` (sort reqs by (key,operator,sorted values); outer sort) for gang-member agreement.
- [ ] The forward/symmetric/self-anti/cross-workload closures keep their structure; only the selector type changes. Unit tests: an `Exists` host selector excludes a node with any matching-key running pod; a `NotIn` selector excludes a node whose running pod LACKS the key (NotIn-missing semantics). Commit.

### Task 4: Normalizer passthrough + adapt existing tests
- [ ] `normalizer.rs`: the two `anti_affinity_*` passthrough lines compile with the new types (no logic change).
- [ ] Adapt existing 5e–5h/12 test helpers (`ppod_aa`, `running_labeled`/`running_anti`, `labeled_pending`, `gang_member_aa`, collector/pod_filter test builders) to construct `LabelSelectorReq` (`&[("app","trainer")]` ⇒ `In` reqs). Keep every assertion. Run the FULL suite — all prior anti-affinity tests must pass. Commit.

### Task 5: Verify + docs
- [ ] Full `cargo test --features rust-cp-sat` + clippy clean. Cluster smoke (optional, mirrors Phase 12): a running pod + a pending pod with a `matchExpressions` (`app In [trainer]`) hostname anti-affinity ⇒ node excluded; binds nothing.
- [ ] README: note anti-affinity now models `matchExpressions` (In/NotIn/Exists/DoesNotExist), not just matchLabels. Update memory status.

## Self-Review Notes
- matchLabels ⇒ In-single-value ⇒ byte-for-byte same matching (no regression); all 5e–5h/12 tests adapted, not weakened.
- NotIn/DoesNotExist missing-key semantics matched to Kubernetes; verified in the anti-affinity (exclusion) direction.
- Unsupported operators / cross-namespace remain unmodeled + caveated (no overclaim).
- Selector representation unified; canonical comparison deterministic for gang agreement.
