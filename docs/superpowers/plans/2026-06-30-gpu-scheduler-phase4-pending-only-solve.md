# GPU Scheduler — Phase 4 (brought forward): Pending-Only Solve — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development or superpowers:executing-plans. Steps use checkbox (`- [ ]`) syntax.

**Goal:** Replace shadow mode's whole-cluster scaffolding solve with a **pending-only** solve: build an `OptimizationInput` containing only the pending ksolver pods, with running pods subtracted from node capacity (residual). Small and fast — turns multi-minute whole-cluster solves into sub-second placement, and gives correct per-pod, real-node results.

**Architecture:** A new pure builder `scheduler::pending_input::build_pending_input(cluster, pending_ids) -> OptimizationInput` that reuses the existing `cpsat_rust::solve`. Running (already-placed) workloads become fixed context by subtracting their requests from their node's capacity; only pending workloads are decision variables. `shadow::run_one_solve` swaps `build_input_strict` for this builder.

**Tech Stack:** Rust; existing `model` types (`NormalizedCluster`, `NormalizedNode`, `NormalizedWorkload`, `OptimizationInput`, `OptimizationNode`, `OptimizationWorkload`, `ResourceList`); existing `cpsat_rust::solve`.

## Global Constraints

- Verified facts:
  - `NormalizedCluster { nodes: Vec<NormalizedNode>, workloads: Vec<NormalizedWorkload>, .. }` (confirm `cluster_name`/`nodes`/`workloads` field names via `sed -n '/pub struct NormalizedCluster/,/^}/p' ksolver/src/model.rs`).
  - `NormalizedNode { name, effective_capacity: ResourceList, extended_resources: BTreeMap<String,i64>, .. }`.
  - `NormalizedWorkload { namespace, name, current_node: String (empty = pending), requests: ResourceList, extended_resource_requests: BTreeMap<String,i64>, feasible_node_names: Vec<String>, .. }`. A workload is **running/placed** iff `current_node` is non-empty.
  - `ResourceList { milli_cpu: i64, memory_bytes: i64, ephemeral_storage: i64, pods: i64 }` — no helper arithmetic; subtract field-wise and clamp at 0.
  - `OptimizationNode { name, pool, count, members, price, effective_capacity, extended_resources }`.
  - `OptimizationWorkload { id, namespace, name, group_size, members, current_node, current_counts, requests, recommended_requests, extended_resource_requests, feasible_nodes, candidate_levels }` — id in strict mode is `"{namespace}/{name}"`.
  - `OptimizationInput { nodes, workloads, anti_affinity_pairs }`.
  - `cpsat_rust::solve` **bails** if any workload has `feasible_nodes` empty — so infeasible pending workloads MUST be excluded from the input (the decision builder already reports excluded pods as "not submitted").
- Unit tests must pass WITHOUT the `rust-cp-sat` feature (pure builder only).
- Workload id form is `"{namespace}/{name}"` (matches `decision::build_decision_trace`).
- `cargo fmt` + clean `cargo clippy` before each commit.

## File Structure

- Create `ksolver/src/scheduler/pending_input.rs` — pure `build_pending_input` + tests.
- Modify `ksolver/src/scheduler/mod.rs` — add `pub mod pending_input;`.
- Modify `ksolver/src/scheduler/shadow.rs` — `run_one_solve` uses `build_pending_input`.

---

## Task 1: Pure pending-only input builder

**Files:** Create `ksolver/src/scheduler/pending_input.rs`; inline tests.

**Interfaces:**
- Consumes: `crate::model::{NormalizedCluster, OptimizationInput, OptimizationNode, OptimizationWorkload, ResourceList}`.
- Produces:
  - `pub fn build_pending_input(cluster: &NormalizedCluster, pending_ids: &std::collections::HashSet<String>) -> OptimizationInput`
  - Behavior: nodes carry residual capacity (effective_capacity and extended_resources minus the sum of requests of running workloads — `current_node` non-empty AND id not in `pending_ids` — placed on that node, clamped at 0). Workloads = only those whose id (`"{ns}/{name}"`) is in `pending_ids` and whose `feasible_node_names` is non-empty; each mapped to an `OptimizationWorkload` with `group_size = 1`, its `requests`, `extended_resource_requests`, `feasible_nodes = feasible_node_names`. `anti_affinity_pairs` empty.

- [ ] **Step 1: Confirm `NormalizedCluster` field names.**

Run: `sed -n '/pub struct NormalizedCluster/,/^}/p' ksolver/src/model.rs`
Expected: fields include `nodes: Vec<NormalizedNode>` and `workloads: Vec<NormalizedWorkload>`. Adjust code below if the names differ.

- [ ] **Step 2: Write the failing tests.** Create `ksolver/src/scheduler/pending_input.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{NormalizedCluster, NormalizedNode, NormalizedWorkload, ResourceList};
    use std::collections::{BTreeMap, HashSet};

    fn rl(cpu: i64, mem: i64) -> ResourceList {
        ResourceList { milli_cpu: cpu, memory_bytes: mem, ephemeral_storage: 0, pods: 0 }
    }

    fn node(name: &str, cpu: i64, mem: i64, gpu: i64) -> NormalizedNode {
        let mut ext = BTreeMap::new();
        if gpu > 0 {
            ext.insert("nvidia.com/gpu".to_string(), gpu);
        }
        NormalizedNode {
            name: name.to_string(),
            effective_capacity: rl(cpu, mem),
            extended_resources: ext,
            ..Default::default()
        }
    }

    fn workload(ns: &str, name: &str, node: &str, cpu: i64, mem: i64, gpu: i64, feasible: &[&str]) -> NormalizedWorkload {
        let mut ext = BTreeMap::new();
        if gpu > 0 {
            ext.insert("nvidia.com/gpu".to_string(), gpu);
        }
        NormalizedWorkload {
            namespace: ns.to_string(),
            name: name.to_string(),
            current_node: node.to_string(),
            requests: rl(cpu, mem),
            extended_resource_requests: ext,
            feasible_node_names: feasible.iter().map(|s| s.to_string()).collect(),
            ..Default::default()
        }
    }

    fn ids(v: &[&str]) -> HashSet<String> {
        v.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn residual_capacity_subtracts_running_pods() {
        // node has 8 GPU, 16000m cpu; a running pod uses 3 GPU + 4000m.
        let cluster = NormalizedCluster {
            nodes: vec![node("n1", 16000, 64, 8)],
            workloads: vec![
                workload("prod", "running", "n1", 4000, 8, 3, &["n1"]),
                workload("team", "pending", "", 1000, 2, 1, &["n1"]),
            ],
            ..Default::default()
        };
        let input = build_pending_input(&cluster, &ids(&["team/pending"]));
        assert_eq!(input.nodes.len(), 1);
        let n = &input.nodes[0];
        assert_eq!(n.effective_capacity.milli_cpu, 12000); // 16000 - 4000
        assert_eq!(*n.extended_resources.get("nvidia.com/gpu").unwrap(), 5); // 8 - 3
    }

    #[test]
    fn only_pending_workloads_included() {
        let cluster = NormalizedCluster {
            nodes: vec![node("n1", 16000, 64, 8)],
            workloads: vec![
                workload("prod", "running", "n1", 4000, 8, 1, &["n1"]),
                workload("team", "pending", "", 1000, 2, 1, &["n1"]),
            ],
            ..Default::default()
        };
        let input = build_pending_input(&cluster, &ids(&["team/pending"]));
        assert_eq!(input.workloads.len(), 1);
        assert_eq!(input.workloads[0].id, "team/pending");
        assert_eq!(input.workloads[0].group_size, 1);
        assert_eq!(input.workloads[0].feasible_nodes, vec!["n1".to_string()]);
    }

    #[test]
    fn infeasible_pending_is_excluded() {
        // pending pod with no feasible nodes must NOT be submitted (solver would bail).
        let cluster = NormalizedCluster {
            nodes: vec![node("n1", 16000, 64, 8)],
            workloads: vec![workload("team", "pending", "", 1000, 2, 1, &[])],
            ..Default::default()
        };
        let input = build_pending_input(&cluster, &ids(&["team/pending"]));
        assert_eq!(input.workloads.len(), 0);
    }

    #[test]
    fn residual_clamps_at_zero() {
        // running pod requests more than capacity (shouldn't happen, but clamp).
        let cluster = NormalizedCluster {
            nodes: vec![node("n1", 1000, 2, 1)],
            workloads: vec![
                workload("prod", "big", "n1", 5000, 10, 4, &["n1"]),
                workload("team", "pending", "", 1, 1, 1, &["n1"]),
            ],
            ..Default::default()
        };
        let input = build_pending_input(&cluster, &ids(&["team/pending"]));
        let n = &input.nodes[0];
        assert_eq!(n.effective_capacity.milli_cpu, 0);
        assert_eq!(*n.extended_resources.get("nvidia.com/gpu").unwrap_or(&0), 0);
    }
}
```

- [ ] **Step 3: Run to verify failure.** `cargo test -p ksolver scheduler::pending_input` → FAIL.

- [ ] **Step 4: Implement.** Prepend to `pending_input.rs`:

```rust
use crate::model::{
    NormalizedCluster, OptimizationInput, OptimizationNode, OptimizationWorkload, ResourceList,
};
use std::collections::{BTreeMap, HashSet};

fn workload_id(namespace: &str, name: &str) -> String {
    format!("{namespace}/{name}")
}

fn sub_clamp(a: i64, b: i64) -> i64 {
    (a - b).max(0)
}

/// Build an optimization input that places ONLY the pending pods, treating all
/// running (already-placed) pods as fixed context by subtracting their requests
/// from their node's capacity (residual-capacity model).
pub fn build_pending_input(
    cluster: &NormalizedCluster,
    pending_ids: &HashSet<String>,
) -> OptimizationInput {
    // 1. Accumulate running usage per node (running = current_node set AND not pending).
    let mut used_cpu: BTreeMap<String, i64> = BTreeMap::new();
    let mut used_mem: BTreeMap<String, i64> = BTreeMap::new();
    let mut used_disk: BTreeMap<String, i64> = BTreeMap::new();
    let mut used_ext: BTreeMap<String, BTreeMap<String, i64>> = BTreeMap::new();
    for w in &cluster.workloads {
        if w.current_node.is_empty() {
            continue;
        }
        if pending_ids.contains(&workload_id(&w.namespace, &w.name)) {
            continue;
        }
        *used_cpu.entry(w.current_node.clone()).or_default() += w.requests.milli_cpu;
        *used_mem.entry(w.current_node.clone()).or_default() += w.requests.memory_bytes;
        *used_disk.entry(w.current_node.clone()).or_default() += w.requests.ephemeral_storage;
        let node_ext = used_ext.entry(w.current_node.clone()).or_default();
        for (res, qty) in &w.extended_resource_requests {
            *node_ext.entry(res.clone()).or_default() += *qty;
        }
    }

    // 2. Build nodes with residual capacity.
    let nodes = cluster
        .nodes
        .iter()
        .map(|node| {
            let cpu = sub_clamp(node.effective_capacity.milli_cpu, *used_cpu.get(&node.name).unwrap_or(&0));
            let mem = sub_clamp(node.effective_capacity.memory_bytes, *used_mem.get(&node.name).unwrap_or(&0));
            let disk = sub_clamp(node.effective_capacity.ephemeral_storage, *used_disk.get(&node.name).unwrap_or(&0));
            let mut ext = BTreeMap::new();
            for (res, cap) in &node.extended_resources {
                let used = used_ext.get(&node.name).and_then(|m| m.get(res)).copied().unwrap_or(0);
                ext.insert(res.clone(), sub_clamp(*cap, used));
            }
            OptimizationNode {
                name: node.name.clone(),
                pool: node.pool.clone(),
                count: 1,
                members: vec![node.name.clone()],
                price: node.price.clone(),
                effective_capacity: ResourceList {
                    milli_cpu: cpu,
                    memory_bytes: mem,
                    ephemeral_storage: disk,
                    pods: node.effective_capacity.pods,
                },
                extended_resources: ext,
            }
        })
        .collect::<Vec<_>>();

    // 3. Build workloads for the pending pods only (skip infeasible ones — the solver bails on empty feasible sets).
    let mut workloads = Vec::new();
    for w in &cluster.workloads {
        let id = workload_id(&w.namespace, &w.name);
        if !pending_ids.contains(&id) {
            continue;
        }
        if w.feasible_node_names.is_empty() {
            continue;
        }
        workloads.push(OptimizationWorkload {
            id,
            namespace: w.namespace.clone(),
            name: w.name.clone(),
            group_size: 1,
            requests: w.requests.clone(),
            extended_resource_requests: w.extended_resource_requests.clone(),
            feasible_nodes: w.feasible_node_names.clone(),
            ..Default::default()
        });
    }

    OptimizationInput {
        nodes,
        workloads,
        anti_affinity_pairs: Vec::new(),
    }
}
```

- [ ] **Step 5: Run to verify pass.** `cargo test -p ksolver scheduler::pending_input` → PASS (4).

- [ ] **Step 6: Commit.**
```bash
cargo fmt
git add ksolver/src/scheduler/pending_input.rs ksolver/src/scheduler/mod.rs
git commit -m "feat(scheduler): pure pending-only input builder (residual capacity)"
```
(Add `pub mod pending_input;` to `mod.rs` in this task.)

---

## Task 2: Wire pending-only solve into shadow

**Files:** Modify `ksolver/src/scheduler/shadow.rs`.

- [ ] **Step 1: Replace the input build in `run_one_solve`.** Change the normalize/build/solve block so it computes pending ids from the observed pods and calls the new builder:

Replace:
```rust
    // Strict (ungrouped) ...
    let input = optimizer_input::build_input_strict(&normalized, true);
```
with:
```rust
    // Pending-only solve: place only the observed ksolver pods; running pods are
    // fixed context (subtracted from node capacity). Small and fast vs whole-cluster.
    let pending_ids: std::collections::HashSet<String> = pending
        .iter()
        .map(|p| format!("{}/{}", p.namespace, p.name))
        .collect();
    let input = crate::scheduler::pending_input::build_pending_input(&normalized, &pending_ids);
```
Remove the now-unused `optimizer_input` import if the compiler flags it (leave it if still used elsewhere in the file — it is not, so drop it from the `use crate::{...}` line).

- [ ] **Step 2: Feature build.** `cargo build -p ksolver --features rust-cp-sat` → compiles (fix unused-import if flagged).

- [ ] **Step 3: Full unit tests (no feature).** `cargo test -p ksolver` → all green.

- [ ] **Step 4: Clippy.** `cargo clippy -p ksolver --features rust-cp-sat --all-targets` → clean.

- [ ] **Step 5: Commit.**
```bash
cargo fmt
git add ksolver/src/scheduler/shadow.rs
git commit -m "feat(scheduler): shadow uses pending-only solve for fast per-pod placement"
```

---

## Task 3: Verify against a cluster

- [ ] **Step 1:** Reuse the Phase-1 Task-10 tiny-cluster flow (GPU node + pending pod). Now the solve should complete in well under a second even on the large `kind-solver-lab` cluster, and the trace should show `placed` on a real node.
- [ ] **Step 2:** Confirm on `kind-solver-lab` (the 300-pod fleet) that a shadow solve now returns quickly (seconds, not minutes) with a valid trace, proving the residual-capacity pending-only approach scales.
- [ ] **Step 3:** Confirm `.spec.nodeName` stays empty (still binds nothing).

---

## Self-Review Notes

- Spec §4 (pending-as-variables, running-as-fixed-residual) → Task 1 builder. ✅
- Solver-bail-on-empty-feasible pitfall → infeasible pending excluded (Task 1), reported "not submitted" by existing decision builder. ✅
- Fixes the empirically-observed whole-cluster slowness (Phase-1 verification finding) → Task 2. ✅
- Still binds nothing (shadow) → unchanged; guard test still holds. ✅
- Deferred (not here): gang all-or-nothing grouping (group_size > 1), topology, preemption, quota — later phases.
```
