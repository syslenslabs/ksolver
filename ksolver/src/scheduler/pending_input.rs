use crate::model::{
    NormalizedCluster, NormalizedWorkload, OptimizationInput, OptimizationNode,
    OptimizationWorkload, OptimizationWorkloadMember, ResourceList,
};
use crate::scheduler::pod_filter::PendingGpuPod;
use std::collections::BTreeMap;

fn workload_id(namespace: &str, name: &str) -> String {
    format!("{namespace}/{name}")
}

/// Stable id for the gang a pod belongs to. Prefixed to avoid a singleton pod
/// named `job` colliding with a gang labelled `job`.
fn gang_id(pod: &PendingGpuPod) -> String {
    match &pod.gang_key {
        Some(v) => format!("gang:{v}"),
        None => format!("pod:{}/{}", pod.namespace, pod.name),
    }
}

fn sub_clamp(a: i64, b: i64) -> i64 {
    (a - b).max(0)
}

/// Scale a workload's requests to a gang TOTAL (mirrors optimizer_input::scale_requests).
/// The solver divides `requests` by `group_size` per replica, so gang inputs store totals.
fn scale_requests(requests: &ResourceList, factor: i64) -> ResourceList {
    ResourceList {
        milli_cpu: requests.milli_cpu * factor,
        memory_bytes: requests.memory_bytes * factor,
        ephemeral_storage: requests.ephemeral_storage * factor,
        pods: factor,
    }
}

fn scale_extended(requests: &BTreeMap<String, i64>, factor: i64) -> BTreeMap<String, i64> {
    requests
        .iter()
        .map(|(k, v)| (k.clone(), v * factor))
        .collect()
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
    /// Whether the given requests fit in this residual capacity. Honors `requests.pods`
    /// (so a whole-gang total with pods=N requires N free slots); per-replica callers
    /// pass pods=0 and still require >=1. Mirrors the solver's per-node constraints and
    /// closes the skip-constraint-when-capacity<=0 gap.
    fn fits(&self, requests: &ResourceList, ext_requests: &BTreeMap<String, i64>) -> bool {
        if self.cpu < requests.milli_cpu
            || self.mem < requests.memory_bytes
            || self.disk < requests.ephemeral_storage
            || self.pods < requests.pods.max(1)
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

/// Homogeneity signature of a workload: gang members must match on all of these to be
/// modeled as one group_size workload (else the gang is excluded, not mis-modeled).
fn signature(w: &NormalizedWorkload) -> (i64, i64, i64, i64, BTreeMap<String, i64>, Vec<String>) {
    let mut feasible = w.feasible_node_names.clone();
    feasible.sort();
    (
        w.requests.milli_cpu,
        w.requests.memory_bytes,
        w.requests.ephemeral_storage,
        w.requests.pods,
        w.extended_resource_requests.clone(),
        feasible,
    )
}

/// Build an optimization input that places ONLY the pending pods, grouping pods that
/// share a gang key into a single all-or-nothing `group_size` workload. Running
/// (already-placed) pods are fixed context, subtracted from node capacity (residual).
pub fn build_pending_input(
    cluster: &NormalizedCluster,
    pending: &[PendingGpuPod],
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

    // 2. Residual capacity per node + OptimizationNode list.
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

    // 3. Per-pod NormalizedWorkload lookup by "{ns}/{name}".
    let mut norm: BTreeMap<String, &NormalizedWorkload> = BTreeMap::new();
    for w in &cluster.workloads {
        norm.insert(workload_id(&w.namespace, &w.name), w);
    }

    // 4. Group pending pods into gangs (only unbound pods; a stale pod already bound was
    //    subtracted above and must not be a decision variable).
    let mut gangs: BTreeMap<String, Vec<&PendingGpuPod>> = BTreeMap::new();
    for p in pending {
        gangs.entry(gang_id(p)).or_default().push(p);
    }

    // 5. Build one workload per feasible, homogeneous gang.
    let mut workloads = Vec::new();
    for (id, mut members) in gangs {
        members.sort_by(|a, b| a.name.cmp(&b.name));
        // Look up every member's normalized workload; skip whole gang if any missing.
        let mut member_workloads = Vec::with_capacity(members.len());
        let mut all_found = true;
        for m in &members {
            match norm.get(&workload_id(&m.namespace, &m.name)) {
                Some(w) => member_workloads.push(*w),
                None => {
                    all_found = false;
                    break;
                }
            }
        }
        if !all_found {
            continue;
        }
        // Enforce homogeneity: identical requests, extended requests, feasible sets.
        let rep = member_workloads[0];
        let rep_sig = signature(rep);
        if member_workloads.iter().any(|w| signature(w) != rep_sig) {
            continue;
        }
        // Members must agree on co-location; disagreement excludes the gang.
        let colocate = members[0].colocate;
        if members.iter().any(|m| m.colocate != colocate) {
            continue;
        }
        let n = members.len() as i64;
        // Co-located gangs must fit the WHOLE gang on one node -> filter by total;
        // spread gangs need one replica per feasible node -> filter per replica.
        let (fit_req, fit_ext) = if colocate {
            (
                scale_requests(&rep.requests, n),
                scale_extended(&rep.extended_resource_requests, n),
            )
        } else {
            (rep.requests.clone(), rep.extended_resource_requests.clone())
        };
        let feasible_nodes: Vec<String> = rep
            .feasible_node_names
            .iter()
            .filter(|node| {
                residual
                    .get(*node)
                    .map(|r| r.fits(&fit_req, &fit_ext))
                    .unwrap_or(false)
            })
            .cloned()
            .collect();
        if feasible_nodes.is_empty() {
            continue;
        }
        workloads.push(OptimizationWorkload {
            id: id.clone(),
            namespace: rep.namespace.clone(),
            name: rep.name.clone(),
            group_size: members.len() as i32,
            members: members
                .iter()
                .map(|m| OptimizationWorkloadMember {
                    namespace: m.namespace.clone(),
                    name: m.name.clone(),
                    current_node: String::new(),
                })
                .collect(),
            requests: scale_requests(&rep.requests, n),
            recommended_requests: scale_requests(&rep.recommended_requests, n),
            extended_resource_requests: scale_extended(&rep.extended_resource_requests, n),
            feasible_nodes,
            colocate,
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
    use crate::scheduler::pod_filter::PendingGpuPod;
    use std::collections::BTreeMap;

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

    fn ppod(ns: &str, name: &str, gang: Option<&str>) -> PendingGpuPod {
        ppod_co(ns, name, gang, false)
    }

    fn ppod_co(ns: &str, name: &str, gang: Option<&str>, colocate: bool) -> PendingGpuPod {
        PendingGpuPod {
            uid: format!("uid-{name}"),
            namespace: ns.into(),
            name: name.into(),
            gpu_request: 1,
            gang_key: gang.map(|g| format!("{ns}/{g}")),
            colocate,
            unmodeled_constraints: vec![],
            anti_affinity_host_selectors: vec![],
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
        let input = build_pending_input(
            &cluster,
            &[
                ppod("team", "m0", Some("job")),
                ppod("team", "m1", Some("job")),
            ],
        );
        assert_eq!(input.workloads.len(), 1);
        let w = &input.workloads[0];
        assert_eq!(w.id, "gang:team/job");
        assert_eq!(w.group_size, 2);
        assert_eq!(w.members.len(), 2);
        assert_eq!(w.requests.milli_cpu, 2000);
        assert_eq!(
            *w.extended_resource_requests.get("nvidia.com/gpu").unwrap(),
            2
        );
        assert_eq!(w.requests.pods, 2);
    }

    #[test]
    fn heterogeneous_gang_is_excluded() {
        let cluster = NormalizedCluster {
            nodes: vec![node("n1", 16000, 64, 110, 8)],
            workloads: vec![
                workload("team", "m0", "", 1000, 2, 1, &["n1"]),
                workload("team", "m1", "", 4000, 2, 1, &["n1"]),
            ],
            ..Default::default()
        };
        let input = build_pending_input(
            &cluster,
            &[
                ppod("team", "m0", Some("job")),
                ppod("team", "m1", Some("job")),
            ],
        );
        assert_eq!(input.workloads.len(), 0);
    }

    #[test]
    fn gang_excluded_if_any_member_infeasible() {
        let cluster = NormalizedCluster {
            nodes: vec![node("n1", 16000, 64, 110, 8)],
            workloads: vec![
                workload("team", "m0", "", 1000, 2, 1, &["n1"]),
                workload("team", "m1", "", 1000, 2, 1, &[]),
            ],
            ..Default::default()
        };
        let input = build_pending_input(
            &cluster,
            &[
                ppod("team", "m0", Some("job")),
                ppod("team", "m1", Some("job")),
            ],
        );
        // heterogeneous feasible sets (one empty) -> excluded
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
        let input = build_pending_input(
            &cluster,
            &[ppod("team", "a", None), ppod("team", "b", None)],
        );
        assert_eq!(input.workloads.len(), 2);
        assert!(input.workloads.iter().all(|w| w.group_size == 1));
        assert!(input.workloads.iter().all(|w| w.id.starts_with("pod:")));
    }

    #[test]
    fn residual_subtracts_running_and_filters() {
        let cluster = NormalizedCluster {
            nodes: vec![node("n1", 16000, 64, 110, 8)],
            workloads: vec![
                workload("prod", "running", "n1", 1000, 2, 8, &["n1"]),
                workload("team", "pending", "", 1000, 2, 1, &["n1"]),
            ],
            ..Default::default()
        };
        // all 8 GPUs used by the running pod -> pending 1-GPU singleton excluded.
        let input = build_pending_input(&cluster, &[ppod("team", "pending", None)]);
        assert_eq!(input.workloads.len(), 0);
        assert_eq!(
            *input.nodes[0]
                .extended_resources
                .get("nvidia.com/gpu")
                .unwrap(),
            0
        );
    }

    #[test]
    fn colocated_gang_excluded_when_no_single_node_fits() {
        // 2-pod co-located gang (1 GPU each) needs a node with 2 free GPUs; n1 has 1.
        let cluster = NormalizedCluster {
            nodes: vec![node("n1", 16000, 64, 110, 1)],
            workloads: vec![
                workload("team", "m0", "", 1000, 2, 1, &["n1"]),
                workload("team", "m1", "", 1000, 2, 1, &["n1"]),
            ],
            ..Default::default()
        };
        let input = build_pending_input(
            &cluster,
            &[
                ppod_co("team", "m0", Some("job"), true),
                ppod_co("team", "m1", Some("job"), true),
            ],
        );
        assert_eq!(input.workloads.len(), 0);
    }

    #[test]
    fn colocated_gang_included_when_single_node_fits() {
        let cluster = NormalizedCluster {
            nodes: vec![node("n1", 16000, 64, 110, 2)],
            workloads: vec![
                workload("team", "m0", "", 1000, 2, 1, &["n1"]),
                workload("team", "m1", "", 1000, 2, 1, &["n1"]),
            ],
            ..Default::default()
        };
        let input = build_pending_input(
            &cluster,
            &[
                ppod_co("team", "m0", Some("job"), true),
                ppod_co("team", "m1", Some("job"), true),
            ],
        );
        assert_eq!(input.workloads.len(), 1);
        assert!(input.workloads[0].colocate);
        assert_eq!(input.workloads[0].group_size, 2);
    }

    #[test]
    fn colocated_gang_excluded_when_pod_slots_insufficient() {
        // node has plenty of GPU but only 1 pod slot; a 2-pod co-located gang needs 2.
        let cluster = NormalizedCluster {
            nodes: vec![node("n1", 16000, 64, 1, 8)],
            workloads: vec![
                workload("team", "m0", "", 1000, 2, 1, &["n1"]),
                workload("team", "m1", "", 1000, 2, 1, &["n1"]),
            ],
            ..Default::default()
        };
        let input = build_pending_input(
            &cluster,
            &[
                ppod_co("team", "m0", Some("job"), true),
                ppod_co("team", "m1", Some("job"), true),
            ],
        );
        assert_eq!(input.workloads.len(), 0);
    }

    #[test]
    fn gang_excluded_when_members_disagree_on_colocate() {
        let cluster = NormalizedCluster {
            nodes: vec![node("n1", 16000, 64, 110, 8)],
            workloads: vec![
                workload("team", "m0", "", 1000, 2, 1, &["n1"]),
                workload("team", "m1", "", 1000, 2, 1, &["n1"]),
            ],
            ..Default::default()
        };
        let input = build_pending_input(
            &cluster,
            &[
                ppod_co("team", "m0", Some("job"), true),
                ppod_co("team", "m1", Some("job"), false),
            ],
        );
        assert_eq!(input.workloads.len(), 0);
    }

    #[test]
    fn non_colocated_gang_included_on_spread_nodes() {
        let cluster = NormalizedCluster {
            nodes: vec![node("n1", 16000, 64, 110, 1), node("n2", 16000, 64, 110, 1)],
            workloads: vec![
                workload("team", "m0", "", 1000, 2, 1, &["n1", "n2"]),
                workload("team", "m1", "", 1000, 2, 1, &["n1", "n2"]),
            ],
            ..Default::default()
        };
        let input = build_pending_input(
            &cluster,
            &[
                ppod("team", "m0", Some("job")),
                ppod("team", "m1", Some("job")),
            ],
        );
        assert_eq!(input.workloads.len(), 1);
        assert!(!input.workloads[0].colocate);
        assert_eq!(input.workloads[0].feasible_nodes.len(), 2);
    }
}
