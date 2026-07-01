use crate::model::{
    NormalizedCluster, OptimizationInput, OptimizationNode, OptimizationWorkload,
    OptimizationWorkloadMember, ResourceList,
};
use std::collections::{BTreeMap, HashSet};

fn workload_id(namespace: &str, name: &str) -> String {
    format!("{namespace}/{name}")
}

fn sub_clamp(a: i64, b: i64) -> i64 {
    (a - b).max(0)
}

/// Residual (free) capacity of a node after running pods are accounted for.
struct Residual {
    cpu: i64,
    mem: i64,
    disk: i64,
    pods: i64,
    ext: BTreeMap<String, i64>,
}

impl Residual {
    /// Whether one copy of a workload's requests fits in this residual capacity.
    /// This mirrors the constraints cpsat_rust::solve enforces, and crucially
    /// closes the gap where the solver SKIPS a constraint whose node capacity is
    /// <= 0 (which would otherwise let a pod land on a node with no free GPU/slot).
    fn fits(&self, requests: &ResourceList, ext_requests: &BTreeMap<String, i64>) -> bool {
        if self.cpu < requests.milli_cpu
            || self.mem < requests.memory_bytes
            || self.disk < requests.ephemeral_storage
            || self.pods < 1
        {
            return false;
        }
        for (res, qty) in ext_requests {
            if self.ext.get(res).copied().unwrap_or(0) < *qty {
                return false;
            }
        }
        true
    }
}

/// Build an optimization input that places ONLY the pending pods, treating every
/// already-placed pod as fixed context by subtracting its requests (and one pod
/// slot) from its node's capacity (residual-capacity model).
///
/// A workload is "running" iff `current_node` is non-empty (race-safe: a pod the
/// watch still thinks is pending but the fresh snapshot shows bound is treated as
/// running). Pending decision workloads are exactly those with an empty
/// `current_node` whose id is in `pending_ids`, and only if they still fit
/// somewhere against residual capacity (otherwise excluded — the solver bails on
/// empty feasible sets, and the decision builder reports them honestly).
pub fn build_pending_input(
    cluster: &NormalizedCluster,
    pending_ids: &HashSet<String>,
) -> OptimizationInput {
    // 1. Accumulate running usage per node (running = current_node non-empty).
    let mut used_cpu: BTreeMap<String, i64> = BTreeMap::new();
    let mut used_mem: BTreeMap<String, i64> = BTreeMap::new();
    let mut used_disk: BTreeMap<String, i64> = BTreeMap::new();
    let mut used_pods: BTreeMap<String, i64> = BTreeMap::new();
    let mut used_ext: BTreeMap<String, BTreeMap<String, i64>> = BTreeMap::new();
    for w in &cluster.workloads {
        if w.current_node.is_empty() {
            continue;
        }
        let node = w.current_node.clone();
        *used_cpu.entry(node.clone()).or_default() += w.requests.milli_cpu;
        *used_mem.entry(node.clone()).or_default() += w.requests.memory_bytes;
        *used_disk.entry(node.clone()).or_default() += w.requests.ephemeral_storage;
        *used_pods.entry(node.clone()).or_default() += 1;
        let node_ext = used_ext.entry(node).or_default();
        for (res, qty) in &w.extended_resource_requests {
            *node_ext.entry(res.clone()).or_default() += *qty;
        }
    }

    // 2. Build residual capacity per node + the OptimizationNode list.
    let mut residual: BTreeMap<String, Residual> = BTreeMap::new();
    let mut nodes = Vec::with_capacity(cluster.nodes.len());
    for node in &cluster.nodes {
        let cpu = sub_clamp(
            node.effective_capacity.milli_cpu,
            *used_cpu.get(&node.name).unwrap_or(&0),
        );
        let mem = sub_clamp(
            node.effective_capacity.memory_bytes,
            *used_mem.get(&node.name).unwrap_or(&0),
        );
        let disk = sub_clamp(
            node.effective_capacity.ephemeral_storage,
            *used_disk.get(&node.name).unwrap_or(&0),
        );
        let pods = sub_clamp(
            node.effective_capacity.pods,
            *used_pods.get(&node.name).unwrap_or(&0),
        );
        let mut ext = BTreeMap::new();
        for (res, cap) in &node.extended_resources {
            let used = used_ext
                .get(&node.name)
                .and_then(|m| m.get(res))
                .copied()
                .unwrap_or(0);
            ext.insert(res.clone(), sub_clamp(*cap, used));
        }
        residual.insert(
            node.name.clone(),
            Residual {
                cpu,
                mem,
                disk,
                pods,
                ext: ext.clone(),
            },
        );
        nodes.push(OptimizationNode {
            name: node.name.clone(),
            pool: node.pool.clone(),
            count: 1,
            members: vec![node.name.clone()],
            price: node.price.clone(),
            effective_capacity: ResourceList {
                milli_cpu: cpu,
                memory_bytes: mem,
                ephemeral_storage: disk,
                pods,
            },
            extended_resources: ext,
        });
    }

    // 3. Build workloads for the pending pods only, filtering feasible nodes by
    //    residual capacity and excluding any that no longer fit anywhere.
    let mut workloads = Vec::new();
    for w in &cluster.workloads {
        if !w.current_node.is_empty() {
            continue;
        }
        let id = workload_id(&w.namespace, &w.name);
        if !pending_ids.contains(&id) {
            continue;
        }
        let feasible_nodes: Vec<String> = w
            .feasible_node_names
            .iter()
            .filter(|n| {
                residual
                    .get(*n)
                    .map(|r| r.fits(&w.requests, &w.extended_resource_requests))
                    .unwrap_or(false)
            })
            .cloned()
            .collect();
        if feasible_nodes.is_empty() {
            continue;
        }
        workloads.push(OptimizationWorkload {
            id,
            namespace: w.namespace.clone(),
            name: w.name.clone(),
            group_size: 1,
            members: vec![OptimizationWorkloadMember {
                namespace: w.namespace.clone(),
                name: w.name.clone(),
                current_node: String::new(),
            }],
            requests: w.requests.clone(),
            recommended_requests: w.recommended_requests.clone(),
            extended_resource_requests: w.extended_resource_requests.clone(),
            feasible_nodes,
            ..Default::default()
        });
    }

    OptimizationInput {
        nodes,
        workloads,
        anti_affinity_pairs: Vec::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{NormalizedCluster, NormalizedNode, NormalizedWorkload, ResourceList};
    use std::collections::{BTreeMap, HashSet};

    fn rl(cpu: i64, mem: i64, pods: i64) -> ResourceList {
        ResourceList {
            milli_cpu: cpu,
            memory_bytes: mem,
            ephemeral_storage: 0,
            pods,
        }
    }

    fn node(name: &str, cpu: i64, mem: i64, pods: i64, gpu: i64) -> NormalizedNode {
        let mut ext = BTreeMap::new();
        if gpu > 0 {
            ext.insert("nvidia.com/gpu".to_string(), gpu);
        }
        NormalizedNode {
            name: name.to_string(),
            effective_capacity: rl(cpu, mem, pods),
            extended_resources: ext,
            ..Default::default()
        }
    }

    fn workload(
        ns: &str,
        name: &str,
        current_node: &str,
        cpu: i64,
        mem: i64,
        gpu: i64,
        feasible: &[&str],
    ) -> NormalizedWorkload {
        let mut ext = BTreeMap::new();
        if gpu > 0 {
            ext.insert("nvidia.com/gpu".to_string(), gpu);
        }
        NormalizedWorkload {
            namespace: ns.to_string(),
            name: name.to_string(),
            current_node: current_node.to_string(),
            requests: rl(cpu, mem, 0),
            extended_resource_requests: ext,
            feasible_node_names: feasible.iter().map(|s| s.to_string()).collect(),
            ..Default::default()
        }
    }

    fn ids(v: &[&str]) -> HashSet<String> {
        v.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn residual_subtracts_running_cpu_and_gpu() {
        let cluster = NormalizedCluster {
            nodes: vec![node("n1", 16000, 64, 110, 8)],
            workloads: vec![
                workload("prod", "running", "n1", 4000, 8, 3, &["n1"]),
                workload("team", "pending", "", 1000, 2, 1, &["n1"]),
            ],
            ..Default::default()
        };
        let input = build_pending_input(&cluster, &ids(&["team/pending"]));
        let n = &input.nodes[0];
        assert_eq!(n.effective_capacity.milli_cpu, 12000);
        assert_eq!(*n.extended_resources.get("nvidia.com/gpu").unwrap(), 5);
    }

    #[test]
    fn residual_subtracts_pod_slots() {
        // node has 1 pod slot, already used by a running pod -> pending can't fit.
        let cluster = NormalizedCluster {
            nodes: vec![node("n1", 16000, 64, 1, 8)],
            workloads: vec![
                workload("prod", "running", "n1", 100, 1, 0, &["n1"]),
                workload("team", "pending", "", 1000, 2, 1, &["n1"]),
            ],
            ..Default::default()
        };
        let input = build_pending_input(&cluster, &ids(&["team/pending"]));
        assert_eq!(input.nodes[0].effective_capacity.pods, 0);
        // no residual pod slot -> pending excluded
        assert_eq!(input.workloads.len(), 0);
    }

    #[test]
    fn zero_residual_gpu_makes_pending_infeasible() {
        // all 8 GPUs used by running pods -> a 1-GPU pending pod must be excluded
        // (guards the solver's skip-constraint-when-capacity<=0 behavior).
        let cluster = NormalizedCluster {
            nodes: vec![node("n1", 16000, 64, 110, 8)],
            workloads: vec![
                workload("prod", "running", "n1", 1000, 2, 8, &["n1"]),
                workload("team", "pending", "", 1000, 2, 1, &["n1"]),
            ],
            ..Default::default()
        };
        let input = build_pending_input(&cluster, &ids(&["team/pending"]));
        assert_eq!(input.workloads.len(), 0);
    }

    #[test]
    fn residual_feasibility_filters_full_nodes() {
        // pending is nominally feasible on n1,n2 but n1 has no residual GPU.
        let cluster = NormalizedCluster {
            nodes: vec![node("n1", 16000, 64, 110, 8), node("n2", 16000, 64, 110, 8)],
            workloads: vec![
                workload("prod", "running", "n1", 1000, 2, 8, &["n1"]),
                workload("team", "pending", "", 1000, 2, 1, &["n1", "n2"]),
            ],
            ..Default::default()
        };
        let input = build_pending_input(&cluster, &ids(&["team/pending"]));
        assert_eq!(input.workloads.len(), 1);
        assert_eq!(input.workloads[0].feasible_nodes, vec!["n2".to_string()]);
    }

    #[test]
    fn only_pending_workloads_included() {
        let cluster = NormalizedCluster {
            nodes: vec![node("n1", 16000, 64, 110, 8)],
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
        assert_eq!(input.workloads[0].members.len(), 1);
    }

    #[test]
    fn stale_pending_id_with_node_is_treated_as_running() {
        // watch thought it pending, but snapshot shows it bound to n1.
        // It must be subtracted (running) and NOT submitted as a decision workload.
        let cluster = NormalizedCluster {
            nodes: vec![node("n1", 16000, 64, 110, 8)],
            workloads: vec![workload("team", "pending", "n1", 4000, 8, 2, &["n1"])],
            ..Default::default()
        };
        let input = build_pending_input(&cluster, &ids(&["team/pending"]));
        assert_eq!(input.workloads.len(), 0); // not submitted
        assert_eq!(input.nodes[0].effective_capacity.milli_cpu, 12000); // subtracted
        assert_eq!(
            *input.nodes[0]
                .extended_resources
                .get("nvidia.com/gpu")
                .unwrap(),
            6
        );
    }
}
