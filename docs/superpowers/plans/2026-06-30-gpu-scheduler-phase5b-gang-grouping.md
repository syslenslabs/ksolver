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
- **Prefixed keys (codex fix #6):** `gang_id = match pod.gang_key { Some(v) => "gang:{v}", None => "pod:{ns}/{name}" }` (`gang_key` is already `"{ns}/{value}"`). This prevents a singleton named `job` colliding with a gang labelled `job`.
- **CRITICAL — scale requests by group_size (codex fix #2).** The solver divides `workload.requests`/`extended_resource_requests` by `group_size` (`per_replica_requests`/`per_replica_scalar_requests`, integer division). Existing grouped input scales by member count (`optimizer_input::scale_requests`/`scale_extended_requests`, which also sets `pods = group_size`). The gang workload MUST store the **total**: `requests = scale_requests(representative.requests, N)`, `extended_resource_requests = scale_extended_requests(representative.ext, N)`, `recommended_requests = scale_requests(representative.recommended_requests, N)`. Storing per-replica would make a 5×1-GPU gang compute `1/5 = 0` GPU/replica and wrongly admit onto a 4-GPU node.
- **Enforce homogeneity (codex fix #3).** A gang is modeled as one `group_size = N` workload ONLY if all members share an identical signature: same `requests`, same `extended_resource_requests`, and same `feasible_node_names` (sorted). If members are heterogeneous, exclude the whole gang (do not silently mis-model) — its members are reported unplaced downstream. (A shared-admission multi-workload model is deferred.)
- **Check EVERY member (codex fix #4).** Exclude the whole gang unless: every member's `NormalizedWorkload` is found by `{ns}/{name}`, the members are homogeneous, and the representative has ≥1 residual-feasible node (which, given homogeneity, equals every member's). Determinism: sort members by `name`; representative = first.
- Per included gang emit one `OptimizationWorkload`: `id = gang_id`, `group_size = members.len()`, `members = [OptimizationWorkloadMember{ns,name,current_node:""} ..]` (all members), scaled requests as above, `feasible_nodes` = representative's `feasible_node_names` filtered by residual `fits(per-replica requests, ext)` — filter with the **per-replica** (unscaled representative) requests, since `fits` checks one replica.
- Running-usage subtraction (Phase 4) unchanged: running = `current_node != ""`.
- Singletons (`gang_key == None`) → `group_size = 1`, unscaled requests (scale-by-1 is identity) — preserves Phase-4 behavior.

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
    fn groups_same_gang_and_scales_requests() {
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
        let w = &input.workloads[0];
        assert_eq!(w.id, "gang:team/job");
        assert_eq!(w.group_size, 2);
        assert_eq!(w.members.len(), 2);
        // requests are TOTAL (scaled by group_size) — solver divides back per replica.
        assert_eq!(w.requests.milli_cpu, 2000); // 1000 * 2
        assert_eq!(*w.extended_resource_requests.get("nvidia.com/gpu").unwrap(), 2); // 1 * 2
        assert_eq!(w.requests.pods, 2);
    }

    #[test]
    fn heterogeneous_gang_is_excluded() {
        // members differ in cpu request -> cannot be modeled as one group_size workload.
        let cluster = NormalizedCluster {
            nodes: vec![node("n1", 16000, 64, 110, 8)],
            workloads: vec![
                workload("team", "m0", "", 1000, 2, 1, &["n1"]),
                workload("team", "m1", "", 4000, 2, 1, &["n1"]),
            ],
            ..Default::default()
        };
        let input = build_pending_input(&cluster, &[ppod("team","m0",Some("job")), ppod("team","m1",Some("job"))]);
        assert_eq!(input.workloads.len(), 0);
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

- [ ] **Step 2b: Local scale helpers.** `optimizer_input::scale_requests`/`scale_extended_requests` are private — define local equivalents in `pending_input.rs` (mirror them: `scale_requests` multiplies cpu/mem/disk by N and sets `pods = N`; `scale_extended_requests` multiplies each value by N). Feature test executes via `cargo test -p ksolver --features rust-cp-sat cpsat_rust`.

- [ ] **Step 3: Implement.** Keep the residual computation (Phase 4). Replace the workload-building section: build a per-pod `NormalizedWorkload` lookup keyed by `{ns}/{name}`; group `pending` by `gang_id`; for each gang, gather members sorted by name, look up each member's workload, compute the representative's residual-feasible nodes, and — only if every member is found AND the representative has ≥1 residual-feasible node — push one `OptimizationWorkload` with `group_size = members.len()`. (Exclude the whole gang otherwise.)

- [ ] **Step 4: Run → pass.** `cargo test -p ksolver scheduler::pending_input` → PASS.

- [ ] **Step 4b: Feature-gated solver test proving the scaling contract (codex must-fix).** In `cpsat_rust.rs`'s `#[cfg(all(test, feature = "rust-cp-sat"))]` tests, add: a 4-GPU node and one `group_size=5` workload whose `extended_resource_requests["nvidia.com/gpu"] = 5` (total, i.e. 1/replica) with `partial_admission=true`; assert the solve succeeds and produces **no** `assignment_counts` entry for it (5 replicas cannot fit in 4 GPUs — gang not admitted). Then a sibling with a 5-GPU node asserts it **is** admitted with `sum == 5`. This catches the divide-by-group_size bug directly.

- [ ] **Step 5: Commit.**
```bash
cargo fmt
git add ksolver/src/scheduler/pending_input.rs ksolver/src/cpsat_rust.rs
git commit -m "feat(scheduler): group pending pods into gangs (scaled requests, all-or-nothing)"
```

---

## Task 3: Member-based decision mapping

**Files:** Modify `decision.rs`; extend tests.

**Interfaces:** `build_decision_trace` signature unchanged. New logic:
- For each workload in `input.workloads`, read `assignment_counts[id]`. **Admitted iff `sum(counts) == group_size`** (the latch guarantees 0 or group_size; a nonzero partial is anomalous — treat as NOT admitted, do not report partial gangs as placed) (codex fix #5).
- **Per-member node from `assignment_counts`, not `assignments[id]`** (codex fix #5 — `assignments` is a single best node and misreports a spread gang). Distribute deterministically: sort the workload's `members` by `name`, sort `counts` node keys by name, and fill members into nodes according to each node's count. This gives every member a concrete node consistent with the spread.
- Build `pod_key ("{ns}/{name}") -> placement` from all workloads' members. For each observed pod: if present → its computed `Placed { node }` (admitted) or `Unplaced { reason: "gang not admitted (insufficient capacity for all replicas)" }` (not admitted); else → `Unplaced { reason: "not submitted (filtered as unschedulable during input build)" }`.

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

## Self-Review Notes (incl. codex review fixes)

- **Scaled requests (codex #2):** gang requests/extended stored as TOTAL (`× group_size`) matching `optimizer_input` + the solver's per-replica division; feature-gated 5-on-4 solver test proves a 5×1-GPU gang is rejected on 4 GPUs (Task 2 Step 4b).
- **Homogeneity enforced (codex #3), not just documented:** heterogeneous gangs (differing requests/ext/feasible sets) are excluded, not mis-modeled; test in Task 2.
- **Every member checked (codex #4):** whole-gang exclusion requires all members found + homogeneous + representative residual-feasible.
- **Prefixed keys (codex #6):** `gang:{ns}/{value}` vs `pod:{ns}/{name}` — no singleton/gang collision; asserted in Task 2 test.
- **Admission = `sum == group_size` (codex #5):** partial counts never reported as placed; per-member nodes distributed deterministically from `assignment_counts` (not the single-best `assignments`), so a spread gang reports honest per-member nodes.
- Gang all-or-nothing relies on the Phase-5a latch — a gang that can't fully fit is admitted=0, never partial (verified by constraint analysis: `x ∈ [0,group_size]`, `sum == group_size * placed`).
- Singletons (no label) preserve Phase-4 behavior (`group_size = 1`, identity scaling).
- Still binds nothing; no-mutation guard unaffected. Offline planner untouched (partial_admission off there; gang builder is shadow-only).
- Deferred: gang co-location/topology (all replicas on NVLink-connected GPUs / same node) — a later topology phase; this phase only guarantees all-or-nothing admission across the fleet. Shared-admission modeling of heterogeneous gangs also deferred.
```
