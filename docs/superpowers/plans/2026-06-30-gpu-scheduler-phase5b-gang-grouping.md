# GPU Scheduler — Phase 5b: Gang Grouping — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: superpowers:subagent-driven-development or superpowers:executing-plans. Steps use checkbox (`- [ ]`) syntax.

**Goal:** Group pending ksolver pods that belong to the same gang (multi-pod training job) into a single `group_size > 1`, all-or-nothing workload, so the solver admits a gang only if *all* its replicas fit (on the Phase-5a admission latch) and the shadow trace reports every member consistently placed or unplaced.

**Architecture:** A configurable **gang label** identifies gang membership (pods sharing `{namespace}/{label-value}` form one gang). `classify` extracts the gang key onto `PendingGpuPod`. The pending-only input builder groups pods into gangs, emitting one `OptimizationWorkload` per gang with `group_size = member count`, `members` = the member pods, and representative per-replica requests. The decision builder maps each observed pod to its gang via `OptimizationWorkload.members` and reports placed/unplaced by gang admission. Pods without the label remain singleton gangs (`group_size = 1`), preserving today's behavior.

**Tech Stack:** Rust; existing `model`, `scheduler::{config, pod_filter, pending_input, decision, shadow}`; `cpsat_rust::solve` with `partial_admission` (Phase 5a).

## Global Constraints

- Verified facts:
  - `cpsat_rust::solve` gang semantics: `sum_over_nodes x[w,n] == group_size * placed[w]` (Phase 5a latch), and per-node `x[w,n] <= group_size * y[n]`. Capacity constraints use `requests * x` per node, so **all replicas of a gang share one per-replica `requests`/`extended_resource_requests`** — the gang model assumes homogeneous members.
  - `OptimizationWorkload.members: Vec<OptimizationWorkloadMember { namespace, name, current_node }>` — use this to carry gang membership through to the decision builder.
  - `OptimizationSolution`: `assignment_counts[id]: HashMap<node,count>` is authoritative; with the latch an admitted gang has `sum(counts) == group_size`, an unadmitted one has no entry. `assignments[id]` is the single best node (may undercount a spread gang).
  - `NormalizedWorkload` has no `labels` field — the gang key must be read from the pod in `classify` and carried on `PendingGpuPod`.
  - `PendingGpuPod` today: `{ uid, namespace, name, gpu_request }`. `build_pending_input(cluster, pending_ids: &HashSet<String>)`. `build_decision_trace(seq, pending, input, solution, status, solve_millis, snapshot_age)`.
- Homogeneous-gang assumption: all members of a gang are modeled with the representative (first, by sorted pod name) member's requests. If members differ, this is an approximation; note it and keep it deterministic (sort members by name).
- Unit tests pass without the `rust-cp-sat` feature. `cargo fmt` + clean clippy per commit. Still binds nothing (shadow); the no-mutation guard test must keep passing.

## File Structure

- Modify `ksolver/src/scheduler/config.rs` — add `gang_label_key: String`.
- Modify `ksolver/src/scheduler/pod_filter.rs` — add `gang_key: Option<String>` to `PendingGpuPod`; `classify` reads the label.
- Modify `ksolver/src/scheduler/pending_input.rs` — group pending pods into gangs; signature takes `&[PendingGpuPod]`.
- Modify `ksolver/src/scheduler/decision.rs` — map pods to gangs via `members` + admission.
- Modify `ksolver/src/scheduler/shadow.rs` — pass pods (with gang keys) to the builder & decision.

---

## Task 1: Gang label config + classify gang key

**Files:** Modify `config.rs`, `pod_filter.rs`; extend their inline tests.

**Interfaces:**
- `ShadowConfig` gains `pub gang_label_key: String` (from `KSOLVER_SHADOW_GANG_LABEL`, default `"scheduling.x-k8s.io/pod-group"`). Empty string disables grouping (all pods singletons).
- `PendingGpuPod` gains `pub gang_key: Option<String>` — `Some("{namespace}/{label-value}")` when the configured label is present and non-empty, else `None`.
- `classify(pod, cfg)` sets `gang_key`.

- [ ] **Step 1: Add config field.** In `config.rs` `ShadowConfig`, add `pub gang_label_key: String`. In `from_env`, set:
```rust
            gang_label_key: std::env::var("KSOLVER_SHADOW_GANG_LABEL")
                .unwrap_or_else(|_| "scheduling.x-k8s.io/pod-group".to_string()),
```
Update the two test `ShadowConfig { .. }` literals in `config.rs` and everywhere else a `ShadowConfig` literal is built in tests (pod_filter.rs, watch_state.rs) to include `gang_label_key: "scheduling.x-k8s.io/pod-group".to_string(),` (or `String::new()` where grouping should be off). Compile errors will list every site.

- [ ] **Step 2: Failing test for gang key extraction.** In `pod_filter.rs` tests, add:
```rust
    #[test]
    fn extracts_gang_key_from_label() {
        let mut p = pod("ksolver", None, Some("Pending"), vec![container("main", Some(q(&[("nvidia.com/gpu", "1")])), None)], vec![]);
        p.metadata.labels = Some(std::collections::BTreeMap::from([
            ("scheduling.x-k8s.io/pod-group".to_string(), "job-7".to_string()),
        ]));
        let got = classify(&p, &cfg()).expect("classify");
        assert_eq!(got.gang_key.as_deref(), Some("team-a/job-7"));
    }

    #[test]
    fn no_gang_key_when_label_absent() {
        let p = pod("ksolver", None, Some("Pending"), vec![container("main", Some(q(&[("nvidia.com/gpu", "1")])), None)], vec![]);
        assert_eq!(classify(&p, &cfg()).unwrap().gang_key, None);
    }
```
(Ensure the `cfg()` helper sets `gang_label_key: "scheduling.x-k8s.io/pod-group".to_string()`.)

- [ ] **Step 3: Run → fail.** `cargo test -p ksolver scheduler::pod_filter` → FAIL (field/logic missing).

- [ ] **Step 4: Implement.** Add `pub gang_key: Option<String>` to `PendingGpuPod`. In `classify`, after computing `namespace` and before returning, compute:
```rust
    let gang_key = if cfg.gang_label_key.is_empty() {
        None
    } else {
        pod.metadata
            .labels
            .as_ref()
            .and_then(|l| l.get(&cfg.gang_label_key))
            .filter(|v| !v.is_empty())
            .map(|v| format!("{namespace}/{v}"))
    };
```
and include `gang_key` in the returned `PendingGpuPod`. Update the existing `classify` test(s) that construct expected `PendingGpuPod` — the extraction tests above cover the new field; other tests only check individual fields so they still pass.

- [ ] **Step 5: Run → pass.** `cargo test -p ksolver scheduler::pod_filter scheduler::config scheduler::watch_state` → PASS.

- [ ] **Step 6: Commit.**
```bash
cargo fmt
git add ksolver/src/scheduler/config.rs ksolver/src/scheduler/pod_filter.rs
git commit -m "feat(scheduler): configurable gang label, classify extracts gang key"
```

---

## Task 2: Group pending pods into gangs in the input builder

**Files:** Modify `pending_input.rs`; rewrite its tests to the new signature.

**Interfaces:**
- Change signature to `pub fn build_pending_input(cluster: &NormalizedCluster, pending: &[PendingGpuPod]) -> OptimizationInput`.
- Grouping: `gang_id = pod.gang_key.clone().unwrap_or_else(|| "{ns}/{name}")`. Pods sharing a `gang_id` form one gang. For each gang, sort members by `name` for determinism; representative = first.
- Per gang emit one `OptimizationWorkload` iff its representative has ≥1 residual-feasible node: `id = gang_id`, `group_size = members.len()`, `members = [OptimizationWorkloadMember{ns,name,current_node:""} ..]`, `requests`/`extended_resource_requests` = representative member's `NormalizedWorkload`, `feasible_nodes` = representative's `feasible_node_names` filtered by residual `fits(requests, ext)`.
- Running-usage subtraction (Phase 4) unchanged: running = `current_node != ""`.
- A pending pod whose `NormalizedWorkload` is missing (not found by `{ns}/{name}`) or has empty residual-feasible set → its gang is excluded (reported "not submitted" downstream). If ANY member of a gang is infeasible/missing, exclude the whole gang (all-or-nothing) so the solver never sees a partial gang.

- [ ] **Step 1: Failing tests.** Rewrite `pending_input.rs` tests to pass `&[PendingGpuPod]`. Add a `pod(ns,name,gang)` helper building `PendingGpuPod { uid, namespace, name, gpu_request:1, gang_key }`. Cases:
  - two pods same gang label → one workload, `group_size == 2`, `members.len() == 2`.
  - gang where one member is infeasible (no residual-feasible node) → whole gang excluded (0 workloads).
  - two pods, no gang label → two singleton workloads (`group_size == 1` each).
  - residual subtraction unchanged (keep an existing residual test, adapted to the new signature).
```rust
    fn ppod(ns: &str, name: &str, gang: Option<&str>) -> crate::scheduler::pod_filter::PendingGpuPod {
        crate::scheduler::pod_filter::PendingGpuPod {
            uid: format!("uid-{name}"), namespace: ns.into(), name: name.into(),
            gpu_request: 1, gang_key: gang.map(|g| format!("{ns}/{g}")),
        }
    }

    #[test]
    fn groups_same_gang_into_one_workload() {
        let cluster = NormalizedCluster {
            nodes: vec![node("n1", 16000, 64, 110, 8)],
            workloads: vec![
                workload("team", "m0", "", 1000, 2, 1, &["n1"]),
                workload("team", "m1", "", 1000, 2, 1, &["n1"]),
            ],
            ..Default::default()
        };
        let input = build_pending_input(&cluster, &[ppod("team","m0",Some("job")), ppod("team","m1",Some("job"))]);
        assert_eq!(input.workloads.len(), 1);
        assert_eq!(input.workloads[0].group_size, 2);
        assert_eq!(input.workloads[0].members.len(), 2);
    }

    #[test]
    fn gang_excluded_if_any_member_infeasible() {
        let cluster = NormalizedCluster {
            nodes: vec![node("n1", 16000, 64, 110, 8)],
            workloads: vec![
                workload("team", "m0", "", 1000, 2, 1, &["n1"]),
                workload("team", "m1", "", 1000, 2, 1, &[]), // no feasible nodes
            ],
            ..Default::default()
        };
        let input = build_pending_input(&cluster, &[ppod("team","m0",Some("job")), ppod("team","m1",Some("job"))]);
        assert_eq!(input.workloads.len(), 0);
    }

    #[test]
    fn no_label_yields_singletons() {
        let cluster = NormalizedCluster {
            nodes: vec![node("n1", 16000, 64, 110, 8)],
            workloads: vec![
                workload("team", "a", "", 1000, 2, 1, &["n1"]),
                workload("team", "b", "", 1000, 2, 1, &["n1"]),
            ],
            ..Default::default()
        };
        let input = build_pending_input(&cluster, &[ppod("team","a",None), ppod("team","b",None)]);
        assert_eq!(input.workloads.len(), 2);
        assert!(input.workloads.iter().all(|w| w.group_size == 1));
    }
```

- [ ] **Step 2: Run → fail.** `cargo test -p ksolver scheduler::pending_input` → FAIL (signature).

- [ ] **Step 3: Implement.** Keep the residual computation (Phase 4). Replace the workload-building section: build a per-pod `NormalizedWorkload` lookup keyed by `{ns}/{name}`; group `pending` by `gang_id`; for each gang, gather members sorted by name, look up each member's workload, compute the representative's residual-feasible nodes, and — only if every member is found AND the representative has ≥1 residual-feasible node — push one `OptimizationWorkload` with `group_size = members.len()`. (Exclude the whole gang otherwise.)

- [ ] **Step 4: Run → pass.** `cargo test -p ksolver scheduler::pending_input` → PASS.

- [ ] **Step 5: Commit.**
```bash
cargo fmt
git add ksolver/src/scheduler/pending_input.rs
git commit -m "feat(scheduler): group pending pods into gangs (group_size, all-or-nothing)"
```

---

## Task 3: Member-based decision mapping

**Files:** Modify `decision.rs`; extend tests.

**Interfaces:** `build_decision_trace` signature unchanged. New logic: build a map `pod_key ("{ns}/{name}") -> (gang_id, admitted, node)` from `input.workloads` (iterating each workload's `members`), where `admitted = assignment_counts[id]` sums to `> 0` (latch ⇒ full group_size) and `node = assignments[id]` (best node, may be one of several for a spread gang). For each observed pod: if in the map → placed (with node) or unplaced ("gang not admitted (insufficient capacity for all replicas)"); else → unplaced ("not submitted (filtered as unschedulable during input build)").

- [ ] **Step 1: Failing test.** Add a gang case: input has one gang workload `id="team/job"`, `group_size=2`, `members=[m0,m1]`; `assignment_counts["team/job"] = {n1:2}`. Observed pods m0,m1 → both `Placed { node: n1 }`. A second gang `team/job2` with members present in input but NO assignment_counts → both members `Unplaced` with a "gang not admitted" reason. A pod not in any workload's members → "not submitted".
```rust
    #[test]
    fn gang_members_share_admission() {
        use crate::model::{OptimizationInput, OptimizationSolution, OptimizationWorkload, OptimizationWorkloadMember};
        use std::collections::HashMap;
        let member = |ns: &str, n: &str| OptimizationWorkloadMember { namespace: ns.into(), name: n.into(), current_node: String::new() };
        let gang = OptimizationWorkload {
            id: "team/job".into(), namespace: "team".into(), name: "job".into(), group_size: 2,
            members: vec![member("team","m0"), member("team","m1")], feasible_nodes: vec!["n1".into()], ..Default::default()
        };
        let input = OptimizationInput { workloads: vec![gang], ..Default::default() };
        let mut counts = HashMap::new(); counts.insert("n1".to_string(), 2);
        let mut assignment_counts = HashMap::new(); assignment_counts.insert("team/job".to_string(), counts);
        let mut assignments = HashMap::new(); assignments.insert("team/job".to_string(), "n1".to_string());
        let solution = OptimizationSolution { assignments, assignment_counts, ..Default::default() };
        let pending = vec![ppod("team","m0"), ppod("team","m1")];
        let t = build_decision_trace(1, &pending, &input, &solution, "OPTIMAL", 5, 1);
        assert!(t.decisions.iter().all(|d| matches!(&d.placement, crate::scheduler::trace::PodPlacement::Placed { node } if node == "n1")));
    }
```
(Add a `ppod(ns,name)` test helper building a `PendingGpuPod` with `gang_key: Some("{ns}/job")`.)

- [ ] **Step 2: Run → fail.** `cargo test -p ksolver scheduler::decision` → FAIL.

- [ ] **Step 3: Implement.** Build the `pod_key -> (admitted, node)` map from `input.workloads[].members` + `solution.assignment_counts`/`assignments`. Replace the per-pod-id lookup with a member-map lookup. Keep the "not submitted" branch for pods absent from all members. Use a distinct reason for "gang not admitted".

- [ ] **Step 4: Run → pass.** `cargo test -p ksolver scheduler::decision` → PASS.

- [ ] **Step 5: Commit.**
```bash
cargo fmt
git add ksolver/src/scheduler/decision.rs
git commit -m "feat(scheduler): map decisions by gang membership and admission"
```

---

## Task 4: Wire gangs through shadow

**Files:** Modify `shadow.rs`.

- [ ] **Step 1: Pass pods (with gang keys) to the builder & decision.** In `run_one_solve`, replace the `pending_ids` construction + `build_pending_input(&normalized, &pending_ids)` call with `build_pending_input(&normalized, pending)`, and keep the existing `build_decision_trace(seq, pending, &input, &solution, ...)` call (its signature is unchanged). Remove the now-unused `pending_ids`/`HashSet` code.
- [ ] **Step 2: Feature build + full tests + clippy.** `cargo build -p ksolver --features rust-cp-sat`; `cargo test -p ksolver`; `cargo clippy -p ksolver --features rust-cp-sat --all-targets` → all green (incl. the no-mutation guard).
- [ ] **Step 3: Commit.**
```bash
cargo fmt
git add ksolver/src/scheduler/shadow.rs
git commit -m "feat(scheduler): shadow groups pending pods into gangs"
```

---

## Task 5: Verify against a cluster

- [ ] **Step 1:** On `kind-solver-lab`, add a GPU node with 4 GPUs. Create a **3-pod gang** (all with label `scheduling.x-k8s.io/pod-group=job1`, each 1 GPU, `schedulerName: ksolver`). Expect the trace to show all 3 members `placed` (gang admitted).
- [ ] **Step 2:** Add a **5-pod gang** on a 4-GPU node (can't fully fit). Expect all 5 members `unplaced` with the "gang not admitted" reason (all-or-nothing — not a partial 4/5 placement).
- [ ] **Step 3:** Confirm `.spec.nodeName` empty for all (binds nothing). Clean up (delete pods + node).

---

## Self-Review Notes

- Gang all-or-nothing relies on the Phase-5a latch (`sum = group_size * placed`) — a gang that can't fully fit is admitted=0, never partially placed.
- Homogeneous-gang approximation documented (representative requests, deterministic by sorted member name).
- Whole-gang exclusion when any member is infeasible/missing prevents the solver seeing a partial gang.
- Singletons (no label) preserve Phase-4 behavior (`group_size = 1`).
- Decision mapping is member-based (via `OptimizationWorkload.members`), distinguishing "not submitted", "gang not admitted", and "placed".
- Still binds nothing; no-mutation guard unaffected.
- Deferred: gang co-location/topology (all replicas on NVLink-connected GPUs / same node) — a later topology phase; this phase only guarantees all-or-nothing admission across the fleet.
```
