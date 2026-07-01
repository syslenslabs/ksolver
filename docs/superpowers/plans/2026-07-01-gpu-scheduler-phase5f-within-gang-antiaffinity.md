# GPU Scheduler — Phase 5f: Within-Gang Anti-Affinity Spread — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: superpowers:subagent-driven-development or superpowers:executing-plans. Steps use checkbox (`- [ ]`) syntax.

**Goal:** Enforce the most common same-batch anti-affinity case: a gang whose pods are anti-affine to *their own* label (hostname topology) must place **≤1 replica per node** (spread across hosts) — e.g. a distributed-training job. Use the solver's existing `anti_affinity_pairs` mechanism (per-workload `x ≤ 1` per node) — no solver change. Guard the colocate-vs-spread contradiction. Cross-workload same-batch and symmetry remain unmodeled (still caveated).

**Why now:** Phase 5e handles anti-affinity against *running* pods; this closes the within-gang (same-batch) part for the self-referential case, which is the dominant real usage ("spread my N replicas over N hosts"). The solver already supports it; shadow just isn't populating `anti_affinity_pairs`.

**Architecture:** A gang is *self-anti-affine* when one of its modeled hostname anti-affinity selectors matches the gang's own pods' labels (`NormalizedWorkload.labels`, added in 5e). For such a gang (and only if NOT co-located — the two are contradictory), `build_pending_input` adds `(gang_id, gang_id)` to `OptimizationInput.anti_affinity_pairs`; the solver then forces ≤1 replica per node. A self-anti-affine + co-located gang is contradictory → excluded. The 5d/5e caveat is retained (cross-workload/symmetry/non-hostname still unmodeled).

**Tech Stack:** Rust; `cp_sat` (behind `rust-cp-sat`); existing `scheduler::pending_input`, `cpsat_rust`.

## Global Constraints

- Verified facts:
  - Solver: `if !scenario.relax_required_anti_affinity` then for each workload with `group_size > 1` whose `id` (or a member `"ns/name"`) is in `anti_affinity_pairs`, add `x[w,n] <= 1` for every feasible node. This spreads the gang ≤1/node. Keying by `(gang_id, gang_id)` is sufficient (solver uses `pairs.map(|(id,_)| id)`).
  - `build_pending_input` currently sets `anti_affinity_pairs: Vec::new()`. Populate it.
  - `NormalizedWorkload.labels` (5e) gives the gang members' own labels; `PendingGpuPod.anti_affinity_host_selectors` (5e) gives modeled hostname selectors.
- **Self-anti-affine** = some modeled `aa_selector` matches **every** member's labels (`aa_selectors.iter().any(|s| member_workloads.iter().all(|w| selector_matches(s, &w.labels)))`). Rep-only matching is unsound because gang homogeneity does NOT include labels (members can differ) — using all-members avoids over-restricting mixed-label gangs and avoids missing conflicts (codex must-fix).
- **Contradiction guard:** a gang that is both `colocate` and self-anti-affine is impossible (all-on-one-node vs ≤1/node with N>1). Exclude it (emit nothing) — the pod already carries the anti-affinity caveat.
- Singletons (`group_size == 1`) are unaffected (solver skips `group_size <= 1`; one replica can't self-spread).
- **Retained caveat:** cross-workload same-batch anti-affinity (pod in gang A vs pod in gang B) and symmetry are still NOT modeled → the 5d/5e "pod anti-affinity" caveat stays.
- Unit tests pass without the `rust-cp-sat` feature; solver spread behavior test is feature-gated. `cargo fmt` + clean clippy. Still binds nothing.

## File Structure

- Modify `ksolver/src/scheduler/pending_input.rs` — collect and set `anti_affinity_pairs` for self-anti-affine non-colocated gangs; exclude the contradictory colocate+self-anti-affine case.
- Modify `ksolver/src/cpsat_rust.rs` — feature-gated test only (behavior already supported).

---

## Task 1: Populate anti_affinity_pairs for self-anti-affine gangs

**Files:** `pending_input.rs` (+ tests).

**Interfaces:** `build_pending_input` unchanged signature; now returns `OptimizationInput.anti_affinity_pairs` populated with `(gang_id, gang_id)` for each emitted gang that is self-anti-affine and not colocated.

- [ ] **Step 1: Member-safe self-anti-affine predicate.** Compute `let self_anti = members.len() > 1 && aa_selectors.iter().any(|s| member_workloads.iter().all(|w| selector_matches(s, &w.labels)));` (matches the term against EVERY member's labels; `members.len() > 1` since a singleton can't self-spread).
- [ ] **Step 2: Contradiction guard.** After the existing colocate/anti-affinity agreement checks and after computing `self_anti`: if `colocate && self_anti` → `continue` (exclude the contradictory gang — colocate = one node, self-spread = ≤1/node).
- [ ] **Step 3: Collect pairs.** Maintain `let mut anti_affinity_pairs: Vec<(String, String)> = Vec::new();` before the gang loop. When pushing an emitted gang workload, if `self_anti && !colocate`, push `(id.clone(), id.clone())`. Set `anti_affinity_pairs` on the returned `OptimizationInput` (replace the current `Vec::new()`).
- [ ] **Step 4: Tests** (extend `pending_input.rs`; use `running_labeled`/`ppod_aa` helpers from 5e, plus set workload `labels` on the pending members so the selector matches their own labels):
  - self-anti-affine gang (ALL members labelled `app=trainer`, selector `app=trainer`, 3 members, feasible n1..n3, not colocated) → `input.anti_affinity_pairs` contains `("gang:team/job","gang:team/job")`.
  - **mixed-label gang** (selector `app=trainer`, but only some members carry `app=trainer`) → NO pair (predicate requires ALL members match).
  - gang whose selector does NOT match any member labels (e.g. selector `app=other`) → no pair.
  - colocate + self-anti-affine gang → excluded (`workloads` empty).
  - singleton with a self-matching selector → no pair (`members.len() == 1`).
  Note: to make a pending member's `NormalizedWorkload` carry labels, extend the `workload(..)` helper usage (set `.labels`) for the gang members, mirroring `running_labeled`.
- [ ] **Step 5: Run → pass.** `cargo test -p ksolver scheduler::pending_input`.
- [ ] **Step 6: Commit.**
```bash
cargo fmt
git add ksolver/src/scheduler/pending_input.rs
git commit -m "feat(scheduler): spread self-anti-affine gangs via anti_affinity_pairs"
```

---

## Task 2: Feature-gated solver spread test

**Files:** `cpsat_rust.rs` (test only).

- [ ] **Step 1:** Add a feature-gated test: a `group_size=3` gang, total 3 GPU, feasible on `n1,n2,n3` (each 1 GPU), `partial_admission=true`, with `anti_affinity_pairs = [(id,id)]`. Assert admitted with each node count ≤ 1 (spread). Then the same on only `n1,n2` → NOT admitted (can't fit 3 with ≤1/node on 2 nodes). Reuse `gang_workload`/`gpu_node` helpers (set the workload id to match the pair, e.g. `gang:t/job`).
```rust
    #[test]
    fn self_anti_affine_gang_spreads_one_per_node() {
        use crate::model::ScenarioConfig;
        let w = gang_workload(3, 3, &["n1", "n2", "n3"]); // id = "gang:t/job"
        let input = OptimizationInput {
            nodes: vec![gpu_node("n1", 4), gpu_node("n2", 4), gpu_node("n3", 4)],
            workloads: vec![w],
            anti_affinity_pairs: vec![("gang:t/job".to_string(), "gang:t/job".to_string())],
        };
        let scenario = ScenarioConfig { solver: "cp-sat-rust".to_string(), partial_admission: true, ..Default::default() };
        let (sol, _i) = super::enabled::solve(&input, &scenario).expect("solve");
        let counts = sol.assignment_counts.get("gang:t/job").expect("admitted");
        assert_eq!(counts.values().sum::<i32>(), 3);
        assert!(counts.values().all(|c| *c <= 1), "spread should be <=1 per node");
    }

    #[test]
    fn self_anti_affine_gang_rejected_when_too_few_nodes() {
        use crate::model::ScenarioConfig;
        let w = gang_workload(3, 3, &["n1", "n2"]);
        let input = OptimizationInput {
            nodes: vec![gpu_node("n1", 4), gpu_node("n2", 4)],
            workloads: vec![w],
            anti_affinity_pairs: vec![("gang:t/job".to_string(), "gang:t/job".to_string())],
        };
        let scenario = ScenarioConfig { solver: "cp-sat-rust".to_string(), partial_admission: true, ..Default::default() };
        let (sol, _i) = super::enabled::solve(&input, &scenario).expect("solve");
        assert!(!sol.assignment_counts.contains_key("gang:t/job"), "3-replica spread cannot fit ≤1/node on 2 nodes");
    }
```
(Confirm `gang_workload` sets `id == "gang:t/job"`; it does in the 5b/5c test helpers.)
- [ ] **Step 2: Run.** `cargo test -p ksolver --features rust-cp-sat cpsat_rust` → PASS.
- [ ] **Step 3: Commit.**
```bash
cargo fmt
git add ksolver/src/cpsat_rust.rs
git commit -m "test(solver): self-anti-affine gang spreads one replica per node"
```

---

## Task 3: Full gate + cluster verify

- [ ] **Step 1: Gate.** `cargo test -p ksolver`; `cargo test -p ksolver --features rust-cp-sat`; `cargo clippy -p ksolver --features rust-cp-sat --all-targets` → green.
- [ ] **Step 2: Cluster.** On `kind-solver-lab`, three 1-GPU nodes. A 3-pod gang (label `scheduling.x-k8s.io/pod-group=job`, label `app=trainer`, hostname `podAntiAffinity` selecting `app=trainer`, `schedulerName: ksolver`, 1 GPU each). Expect all 3 `placed` on **distinct** nodes (spread). Then remove one node → expect all 3 `unplaced` (can't spread 3 onto 2 nodes). Confirm nothing bound; clean up.

---

## Self-Review Notes (incl. codex fixes)

- Uses the solver's existing per-workload `x ≤ 1` anti-affinity (no solver change); shadow now populates `anti_affinity_pairs` for self-anti-affine, non-colocated gangs.
- **Self-anti-affine requires a selector matching EVERY member's labels** (codex #2), not just the representative — gang homogeneity excludes labels, so mixed-label gangs must not wrongly trigger/miss spread. Mixed-label test added.
- `members.len() > 1` guard on both exclusion and pair collection (codex).
- Contradiction (colocate + self-anti-affine) excluded rather than silently mis-solved.
- Offline planner unaffected — it builds its own pairs in `optimizer_input.rs` (codex #5).
- Scope is narrow and honestly worded: **self-referential within-gang spread is enforced; cross-workload same-batch (gang A vs gang B / singletons) and symmetry remain unmodeled and caveated** (codex #6).
- Singletons unaffected. Still binds nothing; no-mutation guard unaffected.
```
