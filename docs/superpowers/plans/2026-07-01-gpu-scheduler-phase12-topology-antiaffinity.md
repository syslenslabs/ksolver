# GPU Scheduler — Phase 12: Non-Hostname Topology Anti-Affinity (zone/rack) — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: superpowers:subagent-driven-development or superpowers:executing-plans. Steps use checkbox (`- [ ]`) syntax.

**Goal:** Extend the shadow scheduler's best-effort pod anti-affinity beyond `kubernetes.io/hostname` to arbitrary topology keys (e.g. `topology.kubernetes.io/zone`, a rack label), so a pending pod is kept out of a whole topology *domain* that already holds a matching pod — not just off the exact node.

**Why:** GPU fleets spread replicas across failure/network domains (zone, rack) for availability and to avoid oversubscribing a rack's fabric. Hostname-only anti-affinity (Phases 5e–5h) can't express "one replica per zone". This extends coverage toward the north-star ("account for everything") and is shadow-only (binds nothing), so it's safe.

**Architecture:** ADDITIVE — leave the verified hostname path (Phases 5e–5h: `anti_affinity_host_selectors`, its collector/pod_filter extraction, and the node-name exclusion in `pending_input`) completely untouched to avoid regressing 27 passing tests. Add a parallel channel for non-hostname terms: `anti_affinity_topology_selectors: Vec<(String /*topologyKey*/, BTreeMap<String,String> /*matchLabels*/)>` on `Pod`, `NormalizedWorkload`, and `PendingGpuPod`. The collector and pod_filter extraction (same strict rules: matchLabels only, no matchExpressions, same-namespace) route hostname terms to the existing field and non-hostname terms to the new one. In `build_pending_input`, generalize exclusion to topology *domains*: for a selector `(key, labels)`, a candidate node `n` is excluded if some running pod in the SAME namespace matches (forward) or the running pod's own topology selector matches all pending members (symmetry) AND that running pod's node shares the same `node.labels[key]` value as `n`. Nodes lacking the topology label are treated as their own singleton domain (never excluded by a domain match, since a missing key can't equal another node's present value). Hostname domain matching continues to use node NAME (unchanged), so synthetic/test nodes without a `kubernetes.io/hostname` label keep working.

**Tech Stack:** Rust; `model.rs`, `collector.rs`, `normalizer.rs`, `scheduler/{pod_filter,pending_input}.rs`.

## Global Constraints

- **Zero regression to hostname path:** do not change `anti_affinity_host_selectors`, its extraction, or the existing name-based exclusion closure. All Phase 5e–5h tests must still pass unchanged.
- **Same strictness as hostname terms:** only fully-modeled terms are enforced — `requiredDuringScheduling…`, `matchLabels` non-empty, no `matchExpressions`, no `namespaces`/`namespaceSelector` (same-namespace only). Anything else is left to the disclosed "pod anti-affinity" caveat (still emitted).
- **Best-effort, disclosed:** cross-batch/symmetry gaps and partial-gang cases remain; the existing caveat is retained. Do not overclaim.
- **Node without the topology label** ⇒ its domain value is treated as absent; it is never excluded via a domain equality (absent ≠ any present value) and never causes exclusions of others.
- **Additive literals:** new struct fields are `#[serde(default)]`; update any exhaustive literals with `..Default::default()`.
- `cargo fmt` + clean clippy; feature-agnostic unit tests; binds nothing.

## File Structure

- Modify `ksolver/src/model.rs` — add `anti_affinity_topology_selectors` to `Pod`, `NormalizedWorkload`, `PendingGpuPod` (Vec<(String, BTreeMap<String,String>)>), serde-default.
- Modify `ksolver/src/collector.rs` — generalize term extraction: route hostname→existing, non-hostname→new field (a shared helper returning `(key, matchLabels)` for every fully-modeled term).
- Modify `ksolver/src/scheduler/pod_filter.rs` — same generalization for pending pods in `classify`.
- Modify `ksolver/src/normalizer.rs` — pass `anti_affinity_topology_selectors` through (Pod→NormalizedWorkload).
- Modify `ksolver/src/scheduler/pending_input.rs` — add topology-domain exclusion (new closure), reusing `selector_matches`; build a `node_name -> &NormalizedNode` (or label) lookup for domain values.

## Tasks

### Task 1: Model fields
- [ ] Add `#[serde(default)] pub anti_affinity_topology_selectors: Vec<(String, std::collections::BTreeMap<String, String>)>` to `Pod` (raw), `NormalizedWorkload`, and `PendingGpuPod`. For `PendingGpuPod` (constructed by `classify`, not serde) just add the field. Build; fix any exhaustive literals with `..Default::default()` (compiler lists them). Commit.

### Task 2: Collector + pod_filter extraction (shared rules)
- [ ] In `collector.rs`, refactor the fully-modeled-term extraction so it yields `(topology_key, match_labels)` for every term passing the strict checks (matchLabels non-empty, no matchExpressions, no namespaces/namespaceSelector). Keep `modeled_host_anti_selectors` returning the hostname subset (unchanged output), and add `modeled_topology_anti_selectors(affinity) -> Vec<(String, BTreeMap<String,String>)>` returning the NON-hostname subset. Populate `Pod.anti_affinity_topology_selectors`.
- [ ] In `pod_filter.rs`, mirror this: extend the extractor to also collect non-hostname `(key, matchLabels)` into `PendingGpuPod.anti_affinity_topology_selectors`. The existing hostname field/logic stays.
- [ ] Unit tests: a pod with a `topology.kubernetes.io/zone` required anti-affinity (matchLabels) yields one topology selector `("topology.kubernetes.io/zone", {…})` and an EMPTY hostname selector list; a hostname term still yields only a hostname selector; a matchExpressions term yields neither. Commit.

### Task 3: Normalizer passthrough
- [ ] In `normalizer.rs`, set `anti_affinity_topology_selectors: pod.anti_affinity_topology_selectors.clone()` on the `NormalizedWorkload` (next to the existing `anti_affinity_host_selectors` line). Build. Commit.

### Task 4: Topology-domain exclusion in builder
- [ ] In `build_pending_input`, build a `node_labels: BTreeMap<String, &BTreeMap<String,String>>` (node name → its labels) from `cluster.nodes` (or reuse a node lookup). Add a `topology_domain(node_name, key)` helper: returns `Some(value)` if that node has `labels[key]`, else `None`.
- [ ] Add a second exclusion closure (after the existing hostname closure) that, for the pending workload's `anti_affinity_topology_selectors` and each running pod on some node `rn` in the same namespace:
  - forward: pending selector `(key, s)` matches running `w.labels` ⇒ exclude candidate node `cn` iff `topology_domain(cn,key) == topology_domain(rn,key)` and that value is `Some` (both in the same, present domain).
  - symmetric: running pod's topology selector `(key, rs)` matches ALL pending member labels ⇒ same domain-equality exclusion.
  Gang members must agree on `anti_affinity_topology_selectors` (extend the existing member-agreement check via `canonical` comparison) else exclude the gang, mirroring the hostname rule.
- [ ] Retain the existing "pod anti-affinity" caveat (best-effort; same-batch cross-domain spread among pending gangs is still not modeled here — only pending-vs-running domain exclusion).
- [ ] Tests (pure, no solver): two nodes in zone `z1` (n1,n2) and one in `z2` (n3); a running pod labelled `app=trainer` on n1; a pending pod with zone anti-affinity `app=trainer` ⇒ feasible nodes exclude BOTH n1 and n2 (all of z1), keep n3. A node lacking the zone label is never excluded. Symmetry: running pod with a zone anti-affinity selector excludes the pending pod's whole zone even when the pending pod has no own selector. Different namespace ⇒ no exclusion. Commit.

### Task 5: Verify + docs + memory
- [ ] Full `cargo test --features rust-cp-sat` + clippy clean; confirm all Phase 5e–5h tests still pass (hostname path untouched).
- [ ] Cluster smoke: two fake nodes labelled `topology.kubernetes.io/zone=za` + one `=zb`; a running `app=trainer` pod in `za`; a pending ksolver `app=trainer` pod with a required zone anti-affinity ⇒ shadow trace places it in `zb` (or unplaced if `zb` full), never in `za`; binds nothing. Clean up.
- [ ] README: note anti-affinity now covers non-hostname topology keys (best-effort, pending-vs-running). Update the memory status file.

## Self-Review Notes
- Additive: hostname path (5e–5h) and its tests untouched → no regression.
- Domain equality uses node topology labels; hostname stays name-based; missing label ⇒ singleton domain (never excluded by equality).
- Same strict modeled-term rules as hostname; caveat retained (best-effort, cross-batch spread still unmodeled).
- New fields serde-default; literals `..Default::default()`.
- Binds nothing; pure unit tests + one cluster smoke.
