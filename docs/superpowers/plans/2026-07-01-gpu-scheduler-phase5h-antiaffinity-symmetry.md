# GPU Scheduler — Phase 5h: Anti-Affinity Symmetry (running-pod terms) — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: superpowers:subagent-driven-development or superpowers:executing-plans. Steps use checkbox (`- [ ]`) syntax.

**Goal:** Enforce anti-affinity **symmetry**: a *running* pod's own required hostname anti-affinity term forbids a new pending pod (matching that term's selector) from landing on the running pod's node. This is the reverse of Phase 5e (which handled the pending pod's own terms vs running pods). Best-effort node exclusion (hostname topology, matchLabels, same namespace); strictly more correct — never causes a wrong placement, only avoids some.

**Why now:** k8s pod anti-affinity is symmetric — the scheduler rejects an incoming pod if an existing pod's required anti-affinity would be violated. Shadow currently ignores that direction. Closing it removes a class of invalid recommendations.

**Architecture:** Carry each running pod's *modeled* hostname anti-affinity selectors on `NormalizedWorkload` (populated by the normalizer from the model `Pod.required_anti`). In `build_pending_input`, extend the existing anti-affinity node-exclusion filter with the symmetry direction: drop node `n` from a pending workload's feasible set if some running pod on `n` (same namespace) has a modeled selector matching **every** member's labels of the pending workload.

**Tech Stack:** Rust; existing `model`, `normalizer`, `scheduler::pending_input`.

## Global Constraints

- Verified facts:
  - Collector captures `Pod.required_anti: Vec<AffinityTerm { topology_key, selector: matchLabels }>` (matchExpressions dropped → empty selector). `NormalizedWorkload` already carries `labels` (5e).
  - Model `AffinityTerm` has NO namespaces/namespaceSelector info (collector drops it) → assume **same namespace** (document; cross-namespace symmetry stays unmodeled).
  - `build_pending_input` already builds `running_by_node` and a per-node anti-affinity exclusion filter (5e). Extend it, don't duplicate.
- **Modeled running term** = `topology_key == "kubernetes.io/hostname"` AND non-empty `selector`. Empty selector (matchExpressions-only or truly empty) is skipped (unmodeled) to avoid match-all over-exclusion.
- **Exactness rule:** exclude node `n` only when a running pod's modeled selector matches **every** member's labels of the pending workload (so the whole workload is genuinely forbidden on `n`) — mirrors 5g/5f, avoids false unplaced from partial matches.
- Strictly-better semantics: excluding an anti-affinity-forbidden node is always correct (k8s would reject too); unmodeled cases (non-hostname, matchExpressions, cross-namespace) simply aren't excluded (no new incorrectness). No new caveat needed.
- Offline planner unaffected (normalizer only gains a field passthrough; feasibility there is unchanged — the exclusion lives in `pending_input`, shadow-only).
- Unit tests pass without the `rust-cp-sat` feature. `cargo fmt` + clean clippy. Still binds nothing.

## File Structure

- Modify `ksolver/src/model.rs` — add `anti_affinity_host_selectors: Vec<BTreeMap<String,String>>` to `NormalizedWorkload`.
- Modify `ksolver/src/normalizer.rs` — populate it from `pod.required_anti` (modeled hostname terms).
- Modify `ksolver/src/scheduler/pending_input.rs` — extend the anti-affinity exclusion filter with the symmetry direction.

---

## Task 1: Carry running-pod anti-affinity selectors on NormalizedWorkload

**Files:** `model.rs`, `normalizer.rs`.

- [ ] **Step 1:** Add `#[serde(default)] pub anti_affinity_host_selectors: Vec<BTreeMap<String, String>>,` to `NormalizedWorkload` (near `labels`).
- [ ] **Step 2:** In the normalizer's `NormalizedWorkload` construction, set it from `pod.required_anti`:
```rust
                anti_affinity_host_selectors: pod
                    .required_anti
                    .iter()
                    .filter(|t| t.topology_key == "kubernetes.io/hostname" && !t.selector.is_empty())
                    .map(|t| t.selector.clone())
                    .collect(),
```
- [ ] **Step 3: Build.** `cargo build -p ksolver` → compiles (the construction is a full literal; add the field).
- [ ] **Step 4: Commit.**
```bash
cargo fmt
git add ksolver/src/model.rs ksolver/src/normalizer.rs
git commit -m "feat(model): carry running-pod hostname anti-affinity selectors on NormalizedWorkload"
```

---

## Task 2: Enforce symmetry in the shadow builder

**Files:** `pending_input.rs` (+ tests).

- [ ] **Step 1:** In the feasible-node filter (currently the 5e best-effort exclusion), add the symmetry direction. Precompute the pending workload's member label maps once: `let member_labels: Vec<&BTreeMap<String,String>> = member_workloads.iter().map(|w| &w.labels).collect();`. A node `n` is excluded when EITHER:
  - (5e) some running pod on `n` (same ns) matches one of the pending pod's `aa_selectors`; OR
  - (5h) some running pod `r` on `n` (same ns) has a selector in `r.anti_affinity_host_selectors` that matches **every** entry of `member_labels`.
```rust
            // best-effort anti-affinity node exclusion (both directions).
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
                    // (5h) this running pod's anti-affinity vs every pending member's labels
                    let symmetric = w.anti_affinity_host_selectors.iter().any(|rs| {
                        member_labels.iter().all(|ml| selector_matches(rs, ml))
                    });
                    forward || symmetric
                });
                !violates
            })
```
(Replace the existing 5e-only closure body with this combined version; keep the `aa_selectors.is_empty()` fast-path only if it still short-circuits correctly — note symmetry can fire even when `aa_selectors` is empty, so do NOT early-return on empty `aa_selectors`.)

- [ ] **Step 2: Tests** (extend `pending_input.rs`; reuse `running_labeled`/`labeled_pending`, and set `anti_affinity_host_selectors` on the running workload):
  - running pod on n1 (labels none) with `anti_affinity_host_selectors=[{app:trainer}]`; pending pod labelled `app=trainer` (no own anti-affinity), nodes n1,n2 → feasible == [n2] (symmetry excludes n1).
  - running pod's selector matches only some gang members → node NOT excluded (all-members rule).
  - running pod in different namespace → not excluded.
  - pending pod not matching the running selector → not excluded.
  (Add a helper to build a running `NormalizedWorkload` with `anti_affinity_host_selectors`.)
- [ ] **Step 3: Run → pass.** `cargo test -p ksolver scheduler::pending_input`.
- [ ] **Step 4: Commit.**
```bash
cargo fmt
git add ksolver/src/scheduler/pending_input.rs
git commit -m "feat(scheduler): enforce anti-affinity symmetry (running-pod terms vs pending)"
```

---

## Task 3: Full gate + cluster verify

- [ ] **Step 1: Gate.** `cargo test -p ksolver`; `cargo test -p ksolver --features rust-cp-sat`; `cargo clippy -p ksolver --features rust-cp-sat --all-targets` → green.
- [ ] **Step 2: Cluster.** On `kind-solver-lab`: GPU node A hosts a `Running` pod (bound via `spec.nodeName`, no special labels) whose spec has a hostname `podAntiAffinity` selecting `app=trainer`; GPU node B has none. Create a pending ksolver GPU pod labelled `app=trainer` **with no anti-affinity of its own**. Expect it `placed` on **B** (A excluded by the running pod's anti-affinity — symmetry). Remove B → `unplaced`. Confirm nothing bound; clean up.

---

## Self-Review Notes

- Symmetry is the reverse of 5e; both live in one combined node-exclusion filter (no early-return on empty pending selectors, since symmetry can fire independently).
- Exactness via "matches every member's labels" (mirrors 5f/5g) — no false unplaced from partial matches.
- Strictly-better: excluding an anti-affinity-forbidden node is always correct; unmodeled cases (non-hostname, matchExpressions, cross-namespace) are simply not excluded.
- Offline planner unaffected (normalizer field passthrough only; exclusion is shadow-only).
- Same-namespace assumed (model term lacks namespace info); documented.
- Still binds nothing; no-mutation guard unaffected.
```
