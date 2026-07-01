# GPU Scheduler — Phase 5h: Anti-Affinity Symmetry (running-pod terms) — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: superpowers:subagent-driven-development or superpowers:executing-plans. Steps use checkbox (`- [ ]`) syntax.

**Goal:** Enforce anti-affinity **symmetry**: a *running* pod's own required hostname anti-affinity term forbids a new pending pod (matching that term's selector) from landing on the running pod's node — the reverse of Phase 5e. Best-effort node exclusion for the **fully-modeled** case only (hostname topology, matchLabels-only, no matchExpressions, no namespace scoping, same namespace). Correct for fully-modeled terms; conservative (no exclusion) otherwise — never falsely excludes.

**Why now:** k8s pod anti-affinity is symmetric — the scheduler rejects an incoming pod if an existing pod's required anti-affinity would be violated. Shadow ignores that direction today.

**Architecture:** The collector's model `AffinityTerm` is **lossy** (drops matchExpressions/namespaces/namespaceSelector), so we must NOT derive enforced selectors from it (that could enforce a broader-than-k8s selector → false exclusion). Instead, the **collector** computes a `modeled_host_anti_selectors` list from the **raw** `corev1` anti-affinity terms, applying the exact same strict criteria as the pending path (`pod_filter::modeled_anti_affinity_host_selectors`). This flows model `Pod` → `NormalizedWorkload` → the shadow builder, which excludes a node when a running pod's modeled selector matches **every** pending member's labels (same namespace).

**Tech Stack:** Rust; k8s-openapi v1_31; existing `collector`, `model`, `normalizer`, `scheduler::pending_input`.

## Global Constraints

- Verified facts:
  - `collector::to_required_anti_affinity` + `selector_to_map` keep only matchLabels (drop matchExpressions/namespaces/namespaceSelector) — DO NOT enforce from `Pod.required_anti`.
  - The collector has the raw `corev1::Affinity` in scope where it builds the model `Pod` (`let affinity = spec...affinity`; `required_anti: to_required_anti_affinity(affinity)`), so it can compute modeled selectors precisely.
  - `NormalizedWorkload` already carries `labels` (5e). `build_pending_input` already has `running_by_node` and a 5e node-exclusion filter with an `aa_selectors.is_empty() -> return true` early return.
- **Modeled term** (identical to `pod_filter`): `topology_key == "kubernetes.io/hostname"`, `label_selector.match_labels` non-empty, `match_expressions` empty/none, `namespaces` empty/none, `namespace_selector` none.
- **MUST remove** the `aa_selectors.is_empty() -> return true` early return (symmetry fires when the pending pod has NO anti-affinity of its own) — codex #1.
- **Exactness (all-members):** exclude a node for the pending workload only when a running modeled selector matches EVERY member's labels — avoids false unplaced. Residual: a running selector matching only *some* aggregated gang members is left un-excluded (documented limitation; the aggregated model loses per-member identity) — codex #3.
- **Scoped correctness claim** (codex #4): strictly-better only for fully-modeled same-namespace hostname matchLabels-only terms; other running-pod anti-affinity forms remain unmodeled (documented). No new per-pod caveat (the affected pending pod may have no anti-affinity of its own to annotate).
- Offline planner unaffected (new fields are serde-default passthroughs; the enforcement is shadow-only in `pending_input`).
- Unit tests pass without the `rust-cp-sat` feature. `cargo fmt` + clean clippy. Still binds nothing.

## File Structure

- Modify `ksolver/src/model.rs` — add `modeled_host_anti_selectors: Vec<BTreeMap<String,String>>` to `Pod`, and `anti_affinity_host_selectors: Vec<BTreeMap<String,String>>` to `NormalizedWorkload`.
- Modify `ksolver/src/collector.rs` — compute `modeled_host_anti_selectors` from the raw `corev1::Affinity` (strict rule).
- Modify `ksolver/src/normalizer.rs` — pass model `Pod.modeled_host_anti_selectors` → `NormalizedWorkload.anti_affinity_host_selectors`.
- Modify `ksolver/src/scheduler/pending_input.rs` — extend the exclusion filter with symmetry; remove the empty early return.

---

## Task 1: Collector computes modeled running-pod anti-affinity selectors

**Files:** `model.rs`, `collector.rs`.

- [ ] **Step 1:** Add `#[serde(default)] pub modeled_host_anti_selectors: Vec<BTreeMap<String, String>>,` to model `Pod`.
- [ ] **Step 2:** In `collector.rs`, add a helper mirroring `pod_filter::modeled_anti_affinity_host_selectors` but on raw corev1:
```rust
fn modeled_host_anti_selectors(affinity: Option<&corev1::Affinity>) -> Vec<BTreeMap<String, String>> {
    let Some(terms) = affinity
        .and_then(|a| a.pod_anti_affinity.as_ref())
        .and_then(|pa| pa.required_during_scheduling_ignored_during_execution.as_ref())
    else {
        return Vec::new();
    };
    let mut out = Vec::new();
    for term in terms {
        if term.topology_key != "kubernetes.io/hostname" {
            continue;
        }
        if term.namespaces.as_ref().map(|n| !n.is_empty()).unwrap_or(false)
            || term.namespace_selector.is_some()
        {
            continue;
        }
        let Some(ls) = term.label_selector.as_ref() else { continue };
        if ls.match_expressions.as_ref().map(|e| !e.is_empty()).unwrap_or(false) {
            continue;
        }
        match ls.match_labels.as_ref() {
            Some(ml) if !ml.is_empty() => out.push(ml.clone()),
            _ => {}
        }
    }
    out
}
```
Set `modeled_host_anti_selectors: modeled_host_anti_selectors(affinity),` in the model `Pod { .. }` construction (next to `required_anti`).
- [ ] **Step 3: Build.** `cargo build -p ksolver` → compiles (fix full `Pod` literals in tests if any).
- [ ] **Step 4: Commit.**
```bash
cargo fmt
git add ksolver/src/model.rs ksolver/src/collector.rs
git commit -m "feat(collector): capture modeled hostname anti-affinity selectors from raw affinity"
```

---

## Task 2: Normalizer passthrough to NormalizedWorkload

**Files:** `model.rs`, `normalizer.rs`.

- [ ] **Step 1:** Add `#[serde(default)] pub anti_affinity_host_selectors: Vec<BTreeMap<String, String>>,` to `NormalizedWorkload` (near `labels`).
- [ ] **Step 2:** In the normalizer's `NormalizedWorkload` construction, set `anti_affinity_host_selectors: pod.modeled_host_anti_selectors.clone(),`.
- [ ] **Step 3: Build + existing tests.** `cargo build -p ksolver` and `cargo test -p ksolver --lib` → green.
- [ ] **Step 4: Commit.**
```bash
cargo fmt
git add ksolver/src/model.rs ksolver/src/normalizer.rs
git commit -m "feat(model): pass modeled anti-affinity selectors onto NormalizedWorkload"
```

---

## Task 3: Enforce symmetry in the shadow builder

**Files:** `pending_input.rs` (+ tests).

- [ ] **Step 1: Replace the 5e-only exclusion closure** (remove the `aa_selectors.is_empty()` early return). Precompute `let member_labels: Vec<&BTreeMap<String,String>> = member_workloads.iter().map(|w| &w.labels).collect();`. A node `n` is kept unless some running pod on `n` (same ns) violates EITHER direction:
```rust
            .filter(|node| {
                let running = match running_by_node.get(*node) {
                    Some(r) => r,
                    None => return true,
                };
                let violates = running.iter().any(|w| {
                    if w.namespace != rep.namespace {
                        return false;
                    }
                    // (5e) pending pod's own anti-affinity vs this running pod's labels
                    let forward = aa_selectors.iter().any(|s| selector_matches(s, &w.labels));
                    // (5h) this running pod's anti-affinity vs EVERY pending member's labels
                    let symmetric = w
                        .anti_affinity_host_selectors
                        .iter()
                        .any(|rs| member_labels.iter().all(|ml| selector_matches(rs, ml)));
                    forward || symmetric
                });
                !violates
            })
```
- [ ] **Step 2: Tests** (extend `pending_input.rs`; add a helper to build a running `NormalizedWorkload` with `anti_affinity_host_selectors`):
  - running pod on n1 with `anti_affinity_host_selectors=[{app:trainer}]` (no labels of its own); pending pod labelled `app=trainer` with NO own anti-affinity; nodes n1,n2 → feasible == [n2] (symmetry excludes n1 even though pending has no selectors — proves the early-return removal).
  - running selector matches only some gang members → node NOT excluded (all-members rule).
  - running pod in a different namespace → not excluded.
  - pending labels don't match the running selector → not excluded.
- [ ] **Step 3: Run → pass.** `cargo test -p ksolver scheduler::pending_input`. Also re-run the 5e forward-direction tests to confirm no regression from removing the early return.
- [ ] **Step 4: Commit.**
```bash
cargo fmt
git add ksolver/src/scheduler/pending_input.rs
git commit -m "feat(scheduler): enforce anti-affinity symmetry (running-pod hostname terms)"
```

---

## Task 4: Full gate + cluster verify

- [ ] **Step 1: Gate.** `cargo test -p ksolver`; `cargo test -p ksolver --features rust-cp-sat`; `cargo clippy -p ksolver --features rust-cp-sat --all-targets` → green.
- [ ] **Step 2: Cluster.** On `kind-solver-lab`: GPU node A hosts a `Running` pod (bound via `spec.nodeName`) whose spec has a hostname `podAntiAffinity` selecting `app=trainer`; GPU node B has none. Create a pending ksolver GPU pod labelled `app=trainer` **with no anti-affinity of its own**. Expect `placed` on **B** (A excluded by symmetry). Remove B → `unplaced`. Confirm nothing bound; clean up.

---

## Self-Review Notes (incl. codex fixes)

- Modeled running selectors computed in the **collector from raw corev1** (matchExpressions/namespaces/namespaceSelector aware) — never from the lossy `Pod.required_anti` (codex #2). Avoids over-enforcing broader-than-k8s selectors.
- Removed the `aa_selectors.is_empty()` early return (codex #1) so symmetry fires when the pending pod has no anti-affinity of its own.
- Exactness via all-members (codex #3): a running selector matching only some aggregated gang members does NOT exclude (documented residual limitation — aggregated model lacks per-member identity).
- Scoped claim (codex #4): correct for fully-modeled same-namespace hostname matchLabels-only terms; other forms unmodeled and documented; no false exclusion.
- Offline planner unaffected (serde-default passthrough fields; enforcement shadow-only).
- Still binds nothing; no-mutation guard unaffected.
```
