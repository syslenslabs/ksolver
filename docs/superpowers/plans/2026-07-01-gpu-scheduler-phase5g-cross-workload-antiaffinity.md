# GPU Scheduler — Phase 5g: Cross-Workload Same-Batch Anti-Affinity — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: superpowers:subagent-driven-development or superpowers:executing-plans. Steps use checkbox (`- [ ]`) syntax.

**Goal:** Enforce required pod anti-affinity **between two different pending workloads** in the same solve (gang↔gang, singleton↔singleton, gang↔singleton) for the modeled hostname case: if workload A's anti-affinity selector matches all of workload B's pods (or vice-versa), the solver must not place an A-replica and a B-replica on the same node. Uses the existing `(id1,id2)` pair structure with a small, bounded solver extension (`x_a[n] + x_b[n] ≤ 1`). Self-spread (5f) and running-pod exclusion (5e) are unchanged. Symmetry vs *running* pods remains unmodeled (caveat retained).

**Why now:** Completes the same-batch anti-affinity story. 5f handles a gang's self-spread; this handles distinct workloads that must not co-locate. The `anti_affinity_pairs` field already carries `(String,String)` tuples — the solver currently ignores the second element; this gives it meaning for cross pairs.

**Architecture:** In `cpsat_rust::solve`, split the anti-affinity handling: **self-pairs** (`a == b`) keep today's `x ≤ 1` per node (group_size > 1); **cross-pairs** (`a != b`) use per-(workload,node) **presence booleans**: `x_w[n] ≤ group_size_w · present_w_n`, then `present_a_n + present_b_n ≤ 1` on each common node. This is correct for gangs (a workload may pack up to `group_size` replicas on a node it is *present* on; at most one of the two workloads is present per node) — a plain `x_a[n]+x_b[n] ≤ 1` would wrongly forbid a colocated gang (`x=group_size>1`). In `build_pending_input`, after emitting workloads, compute cross-pairs: for each unordered pair of distinct emitted workloads in the same namespace, if one's modeled hostname selector matches **all** the other's member labels, add `(A.id, B.id)` (single push, `i < j`). The "all members" rule keeps the constraint exact.

**Tech Stack:** Rust; `cp_sat` (behind `rust-cp-sat`); existing `scheduler::pending_input`, `cpsat_rust`.

## Global Constraints

- Verified facts:
  - Existing anti-affinity block (`cpsat_rust.rs` ~164) collects first elements of all pairs and applies `x ≤ 1` to any `group_size > 1` workload in that set. This must be scoped to **self-pairs only** (`a == b`) so cross-pairs don't accidentally trigger self-spread.
  - The offline planner (`optimizer_input.rs`) only emits **self**-pairs `(id, id)`, so the new cross-pair constraint never fires for it (guarded by `a != b`). Planner unchanged.
  - `x_vars` is keyed `(workload.id, node_name)`, created only for `workload.feasible_nodes`. For a cross-pair constraint on node `n`, both `(a,n)` and `(b,n)` must exist (n feasible to both).
  - Shadow gang/pod ids are `gang:{ns}/{val}` / `pod:{ns}/{name}`; cross-pairs reference these ids directly (not member keys).
- **Correctness rule (avoid over-constraining):** add a cross-pair `(A,B)` only when the anti-affinity is *total* — some modeled selector of A matches **every** member label set of B (or symmetric). Then `x_a[n]+x_b[n] ≤ 1` is exact (every A-pod truly conflicts with every B-pod). Partial matches are left to the retained caveat (no false unplaced).
- Same-namespace only (matches the modeled-term rule from 5e).
- Retained caveat: symmetry vs already-running pods, non-hostname topology, and partial-label cases remain unmodeled → the "pod anti-affinity" caveat stays.
- Unit tests pass without the `rust-cp-sat` feature; the cross-constraint solver behavior test is feature-gated. `cargo fmt` + clean clippy. Still binds nothing.

## File Structure

- Modify `ksolver/src/cpsat_rust.rs` — split self/cross anti-affinity; add cross `x_a+x_b ≤ 1`; feature-gated test.
- Modify `ksolver/src/scheduler/pending_input.rs` — compute cross-pairs from emitted workloads.

---

## Task 1: Solver cross-pair constraint

**Files:** `cpsat_rust.rs`.

- [ ] **Step 1: Split self vs cross.** Replace the current anti-affinity block with:
```rust
        if !scenario.relax_required_anti_affinity {
            // Self-pairs (a == a): spread this workload's own replicas <=1 per node.
            let self_ids: std::collections::HashSet<&str> = input
                .anti_affinity_pairs
                .iter()
                .filter(|(a, b)| a == b)
                .map(|(a, _)| a.as_str())
                .collect();
            for workload in &input.workloads {
                if workload.group_size <= 1 {
                    continue;
                }
                let has_anti = self_ids.contains(workload.id.as_str())
                    || workload.members.iter().any(|m| {
                        let key = format!("{}/{}", m.namespace, m.name);
                        self_ids.contains(key.as_str())
                    });
                if !has_anti {
                    continue;
                }
                for node_name in &workload.feasible_nodes {
                    let x = x_vars[&(workload.id.clone(), node_name.clone())];
                    model.add_le(x, 1_i64);
                }
            }
            // Cross-pairs (a != b): at most one of workloads {a,b} may be PRESENT on a node.
            // Presence bool per (workload,node): x_w[n] <= group_size_w * present. Then
            // present_a + present_b <= 1. (Counts, not presence, would break colocated gangs.)
            let meta: HashMap<&str, (&Vec<String>, i64)> = input
                .workloads
                .iter()
                .map(|w| (w.id.as_str(), (&w.feasible_nodes, i64::from(workload_group_size(w)))))
                .collect();
            let mut presence: HashMap<(String, String), BoolVar> = HashMap::new();
            // helper closure would borrow model mutably twice; inline instead.
            for (a, b) in &input.anti_affinity_pairs {
                if a == b {
                    continue;
                }
                let (Some((fa, ga)), Some((fb, gb))) =
                    (meta.get(a.as_str()), meta.get(b.as_str()))
                else {
                    continue;
                };
                let bset: std::collections::HashSet<&str> = fb.iter().map(|s| s.as_str()).collect();
                for node_name in fa.iter().filter(|n| bset.contains(n.as_str())) {
                    // ensure presence var for (a,node)
                    let pa = *presence
                        .entry((a.clone(), node_name.clone()))
                        .or_insert_with(|| {
                            let p = model.new_bool_var_with_name(format!(
                                "present_{}__{}",
                                sanitize(a),
                                sanitize(node_name)
                            ));
                            model.add_le(x_vars[&(a.clone(), node_name.clone())], (*ga, p));
                            p
                        });
                    let pb = *presence
                        .entry((b.clone(), node_name.clone()))
                        .or_insert_with(|| {
                            let p = model.new_bool_var_with_name(format!(
                                "present_{}__{}",
                                sanitize(b),
                                sanitize(node_name)
                            ));
                            model.add_le(x_vars[&(b.clone(), node_name.clone())], (*gb, p));
                            p
                        });
                    let expr: LinearExpr = [pa, pb].into_iter().collect();
                    model.add_le(expr, 1_i64);
                }
            }
        }
```
(`HashMap`/`HashSet`/`LinearExpr`/`BoolVar` already imported. `workload_group_size(w)` = `w.group_size`; inline `w.group_size` directly. The two `.or_insert_with` closures each borrow `model` mutably in sequence, which is fine since they don't overlap; if the borrow checker complains, compute `pa` fully in its own block before `pb`.)

- [ ] **Step 2: Feature-gated test.** Two singleton workloads A,B feasible only on `n1`, cross-pair `(A,B)`, `partial_admission=true` → exactly one admitted (both can't share the single node). With two nodes → both admitted on different nodes. Build minimal `OptimizationWorkload`s (group_size 1, 1 GPU) with ids `t/a`,`t/b`; nodes via `gpu_node`.
```rust
    #[test]
    fn cross_pair_forbids_shared_node() {
        use crate::model::{OptimizationWorkload, OptimizationWorkloadMember, ResourceList, ScenarioConfig};
        use std::collections::BTreeMap;
        let mk = |name: &str, feas: &[&str]| {
            let mut ext = BTreeMap::new();
            ext.insert("nvidia.com/gpu".to_string(), 1);
            OptimizationWorkload {
                id: format!("t/{name}"), namespace: "t".into(), name: name.into(), group_size: 1,
                members: vec![OptimizationWorkloadMember { namespace: "t".into(), name: name.into(), current_node: String::new() }],
                requests: ResourceList { milli_cpu: 1000, memory_bytes: 1<<30, ephemeral_storage: 0, pods: 1 },
                extended_resource_requests: ext, feasible_nodes: feas.iter().map(|s| s.to_string()).collect(),
                ..Default::default()
            }
        };
        let scenario = ScenarioConfig { solver: "cp-sat-rust".into(), partial_admission: true, ..Default::default() };
        // one node -> only one of the pair admitted
        let input1 = OptimizationInput {
            nodes: vec![gpu_node("n1", 4)],
            workloads: vec![mk("a", &["n1"]), mk("b", &["n1"])],
            anti_affinity_pairs: vec![("t/a".into(), "t/b".into())],
        };
        let (s1, _) = super::enabled::solve(&input1, &scenario).expect("solve");
        let admitted1 = s1.assignment_counts.values().filter(|c| c.values().any(|v| *v>0)).count();
        assert_eq!(admitted1, 1, "cross-pair on one node admits only one");
        // two nodes -> both admitted
        let input2 = OptimizationInput {
            nodes: vec![gpu_node("n1", 4), gpu_node("n2", 4)],
            workloads: vec![mk("a", &["n1","n2"]), mk("b", &["n1","n2"])],
            anti_affinity_pairs: vec![("t/a".into(), "t/b".into())],
        };
        let (s2, _) = super::enabled::solve(&input2, &scenario).expect("solve");
        let admitted2 = s2.assignment_counts.values().filter(|c| c.values().any(|v| *v>0)).count();
        assert_eq!(admitted2, 2, "cross-pair with two nodes admits both");
    }
```

- [ ] **Step 2b: Gang↔singleton presence test (codex must-fix — singleton-only tests miss the bug).** A colocated gang `A` (`group_size=2`, `colocate=true`, total 2 GPU) cross-paired with singleton `B` (1 GPU), both feasible on a single 4-GPU node `n1`, `partial_admission=true`. Assert: the solve succeeds and **at most one** of {A,B} is admitted, AND `A` is admissible when solved alone (proving the presence-var model didn't wrongly forbid the colocated gang). Also a two-node variant: A (colocated, on n1) and B (on n2) both admitted.
```rust
    #[test]
    fn cross_pair_presence_allows_colocated_gang_alone() {
        use crate::model::ScenarioConfig;
        let scenario = ScenarioConfig { solver: "cp-sat-rust".into(), partial_admission: true, ..Default::default() };
        // A: colocated gang gs=2 (total 2 GPU); B: singleton 1 GPU; both on n1 (4 GPU).
        let mut a = gang_workload(2, 2, &["n1"]); a.colocate = true; a.id = "gang:t/a".into();
        // give B a distinct id
        let mut b = gang_workload(1, 1, &["n1"]); b.id = "gang:t/b".into();
        let input = OptimizationInput {
            nodes: vec![gpu_node("n1", 4)],
            workloads: vec![a, b],
            anti_affinity_pairs: vec![("gang:t/a".into(), "gang:t/b".into())],
        };
        let (sol, _i) = super::enabled::solve(&input, &scenario).expect("solve");
        let admitted = sol.assignment_counts.values().filter(|c| c.values().any(|v| *v>0)).count();
        assert!(admitted <= 1, "cross-paired workloads must not share the single node");
        // A alone must be admissible (presence model does not forbid the colocated gang).
        let mut a2 = gang_workload(2, 2, &["n1"]); a2.colocate = true; a2.id = "gang:t/a".into();
        let solo = OptimizationInput { nodes: vec![gpu_node("n1", 4)], workloads: vec![a2], anti_affinity_pairs: vec![] };
        let (s3, _i) = super::enabled::solve(&solo, &scenario).expect("solve");
        assert_eq!(s3.assignment_counts.get("gang:t/a").map(|c| c.values().sum::<i32>()).unwrap_or(0), 2);
    }
```
(`gang_workload` sets id `gang:t/job`; override `.id` as shown so the two workloads/pair ids line up.)

- [ ] **Step 3: Run + regression.** `cargo test -p ksolver --features rust-cp-sat cpsat_rust` → PASS (self-spread tests from 5f still green — they use `a==b` pairs).
- [ ] **Step 4: Commit.**
```bash
cargo fmt
git add ksolver/src/cpsat_rust.rs
git commit -m "feat(solver): enforce cross-workload anti-affinity pairs (x_a+x_b<=1)"
```

---

## Task 2: Compute cross-pairs in the shadow builder

**Files:** `pending_input.rs` (+ tests).

- [ ] **Step 1: Collect emitted metadata.** While emitting each gang workload, record `(id, namespace, selectors: Vec<BTreeMap>, member_labels: Vec<BTreeMap>)` (member_labels = each member's `NormalizedWorkload.labels`). Push into a `Vec` alongside `workloads`.
- [ ] **Step 2: Pairwise cross-pairs.** After the gang loop, for each `i < j` in the emitted list with equal namespace: `a_forbids_b = a.selectors.iter().any(|s| b.member_labels.iter().all(|l| selector_matches(s, l)))` (and symmetric `b_forbids_a`). If either holds, push `(a.id.clone(), b.id.clone())` to `anti_affinity_pairs`. (The "all members" rule keeps the workload-granularity constraint exact.)
- [ ] **Step 3: Tests** (extend `pending_input.rs`):
  - two distinct singletons, A anti-affine `{app:b}`, B labelled `{app:b}`, same ns → `anti_affinity_pairs` contains `("pod:team/a","pod:team/b")`.
  - partial match (B is a gang where only one member has `{app:b}`) → NO cross-pair (all-members rule).
  - different namespaces → no cross-pair.
  - unrelated selectors → no cross-pair.
- [ ] **Step 4: Run → pass.** `cargo test -p ksolver scheduler::pending_input`.
- [ ] **Step 5: Commit.**
```bash
cargo fmt
git add ksolver/src/scheduler/pending_input.rs
git commit -m "feat(scheduler): compute cross-workload anti-affinity pairs (all-members rule)"
```

---

## Task 3: Full gate + cluster verify

- [ ] **Step 1: Gate.** `cargo test -p ksolver`; `cargo test -p ksolver --features rust-cp-sat`; `cargo clippy -p ksolver --features rust-cp-sat --all-targets` → green.
- [ ] **Step 2: Cluster.** On `kind-solver-lab`, one GPU node. Two distinct pending ksolver GPU pods (different pod-group values so they are separate workloads), pod `a` with hostname `podAntiAffinity` selecting `app=b`, pod `b` labelled `app=b`. Expect only ONE of the two placed (they can't share the single node); add a second node → both placed on different nodes. Confirm nothing bound; clean up.

---

## Self-Review Notes (incl. codex fixes)

- **Presence-var model** (codex #1/#5): cross anti-affinity uses per-(workload,node) presence bools (`x ≤ group_size·present`, `present_a+present_b ≤ 1`), NOT `x_a+x_b ≤ 1`, so colocated/gang workloads aren't wrongly forbidden. Gang↔singleton test added (Step 2b).
- Presence vars cached per `(id,node)` so multiple cross-pairs sharing a workload/node reuse them.
- Pair generation: single push per unordered pair (`i < j`), deterministic (BTreeMap gang order); no `(b,a)` duplicates (codex #6).
- Cross-pairs use the previously-ignored second element of `anti_affinity_pairs`; self-spread (5f) scoped to `a == b` so it's unaffected.
- Offline planner emits only self-pairs → cross constraint never fires there (guarded `a != b`).
- "All members" matching keeps `x_a+x_b ≤ 1` exact (no over-constraining / false unplaced); partial cases fall to the retained caveat.
- Same-namespace only; symmetry vs running pods and non-hostname topology remain caveated.
- Still binds nothing; no-mutation guard unaffected.
```
