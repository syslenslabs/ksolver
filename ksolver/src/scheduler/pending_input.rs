use crate::model::{
    NormalizedCluster, NormalizedWorkload, OptimizationInput, OptimizationNode,
    OptimizationWorkload, OptimizationWorkloadMember, QuotaGroup, ResourceList,
};
use crate::scheduler::pod_filter::PendingGpuPod;
use std::collections::BTreeMap;

/// Resource name used for per-namespace quotas (MVP: GPUs only).
const GPU_RESOURCE: &str = "nvidia.com/gpu";

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

/// A matchLabels selector matches a workload's labels iff every selector entry is
/// present with the same value (superset match).
fn selector_matches(
    selector: &BTreeMap<String, String>,
    labels: &BTreeMap<String, String>,
) -> bool {
    selector.iter().all(|(k, v)| labels.get(k) == Some(v))
}

/// Deterministic canonical form of a selector set for order-insensitive comparison
/// (each map -> sorted key/value pairs; outer vector sorted).
fn canonical_selectors(sels: &[BTreeMap<String, String>]) -> Vec<Vec<(String, String)>> {
    let mut out: Vec<Vec<(String, String)>> = sels
        .iter()
        .map(|m| m.iter().map(|(k, v)| (k.clone(), v.clone())).collect())
        .collect();
    out.sort();
    out
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
    quotas: &BTreeMap<String, i64>,
) -> OptimizationInput {
    // 1. Accumulate running usage per node (running = current_node non-empty). In the same
    //    pass, sum each namespace's running GPU usage so quotas count existing consumption
    //    (computed here, not in a second loop, to avoid drift from the residual math).
    let mut used_cpu: BTreeMap<String, i64> = BTreeMap::new();
    let mut used_mem: BTreeMap<String, i64> = BTreeMap::new();
    let mut used_disk: BTreeMap<String, i64> = BTreeMap::new();
    let mut used_pods: BTreeMap<String, i64> = BTreeMap::new();
    let mut used_ext: BTreeMap<String, BTreeMap<String, i64>> = BTreeMap::new();
    let mut running_gpu_by_ns: BTreeMap<String, i64> = BTreeMap::new();
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
        if let Some(gpu) = w.extended_resource_requests.get(GPU_RESOURCE) {
            *running_gpu_by_ns.entry(w.namespace.clone()).or_default() += *gpu;
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

    // 3. Per-pod NormalizedWorkload lookup by "{ns}/{name}", and running pods per node
    //    (for best-effort anti-affinity node exclusion).
    let mut norm: BTreeMap<String, &NormalizedWorkload> = BTreeMap::new();
    let mut running_by_node: BTreeMap<String, Vec<&NormalizedWorkload>> = BTreeMap::new();
    for w in &cluster.workloads {
        norm.insert(workload_id(&w.namespace, &w.name), w);
        if !w.current_node.is_empty() {
            running_by_node
                .entry(w.current_node.clone())
                .or_default()
                .push(w);
        }
    }

    // 4. Group pending pods into gangs (only unbound pods; a stale pod already bound was
    //    subtracted above and must not be a decision variable).
    let mut gangs: BTreeMap<String, Vec<&PendingGpuPod>> = BTreeMap::new();
    for p in pending {
        gangs.entry(gang_id(p)).or_default().push(p);
    }

    // 5. Build one workload per feasible, homogeneous gang.
    let mut workloads = Vec::new();
    let mut anti_affinity_pairs: Vec<(String, String)> = Vec::new();
    // (id, namespace, selectors, member_labels) for each emitted workload, for cross-pairs.
    type EmittedMeta = (
        String,
        String,
        Vec<BTreeMap<String, String>>,
        Vec<BTreeMap<String, String>>,
    );
    let mut emitted_meta: Vec<EmittedMeta> = Vec::new();
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
        // Members must agree on anti-affinity selectors (order-insensitive); else exclude.
        let rep_aa = canonical_selectors(&members[0].anti_affinity_host_selectors);
        if members
            .iter()
            .any(|m| canonical_selectors(&m.anti_affinity_host_selectors) != rep_aa)
        {
            continue;
        }
        let aa_selectors = &members[0].anti_affinity_host_selectors;
        // Self-anti-affine: a modeled selector matches EVERY member's own labels, so the
        // gang's replicas must spread (<=1 per node). Requires >1 member. Matching all
        // members (not just the representative) is required because gang homogeneity does
        // not include labels.
        let self_anti = members.len() > 1
            && aa_selectors.iter().any(|s| {
                member_workloads
                    .iter()
                    .all(|w| selector_matches(s, &w.labels))
            });
        // Co-location (one node) and self-spread (<=1 per node) are contradictory.
        if colocate && self_anti {
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
        let member_labels: Vec<&BTreeMap<String, String>> =
            member_workloads.iter().map(|w| &w.labels).collect();
        let feasible_nodes: Vec<String> = rep
            .feasible_node_names
            .iter()
            .filter(|node| {
                residual
                    .get(*node)
                    .map(|r| r.fits(&fit_req, &fit_ext))
                    .unwrap_or(false)
            })
            // Best-effort hostname anti-affinity node exclusion, both directions:
            //  (5e) the pending pod's own anti-affinity vs a running pod's labels, and
            //  (5h) a running pod's anti-affinity vs EVERY pending member's labels (symmetry).
            .filter(|node| {
                let running = match running_by_node.get(*node) {
                    Some(r) => r,
                    None => return true,
                };
                let violates = running.iter().any(|w| {
                    if w.namespace != rep.namespace {
                        return false;
                    }
                    let forward = aa_selectors.iter().any(|s| selector_matches(s, &w.labels));
                    let symmetric = w
                        .anti_affinity_host_selectors
                        .iter()
                        .any(|rs| member_labels.iter().all(|ml| selector_matches(rs, ml)));
                    forward || symmetric
                });
                !violates
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
        // Self-anti-affine, non-colocated gang -> solver spreads it <=1 replica per node.
        if self_anti && !colocate {
            anti_affinity_pairs.push((id.clone(), id.clone()));
        }
        emitted_meta.push((
            id,
            rep.namespace.clone(),
            aa_selectors.clone(),
            member_workloads.iter().map(|w| w.labels.clone()).collect(),
        ));
    }

    // Cross-workload same-batch anti-affinity: at most one of two distinct workloads per
    // node when one's selector matches ALL the other's member labels (same namespace).
    for i in 0..emitted_meta.len() {
        for j in (i + 1)..emitted_meta.len() {
            let (a, b) = (&emitted_meta[i], &emitted_meta[j]);
            if a.1 != b.1 {
                continue;
            }
            let a_forbids_b =
                a.2.iter()
                    .any(|s| b.3.iter().all(|l| selector_matches(s, l)));
            let b_forbids_a =
                b.2.iter()
                    .any(|s| a.3.iter().all(|l| selector_matches(s, l)));
            if a_forbids_b || b_forbids_a {
                anti_affinity_pairs.push((a.0.clone(), b.0.clone()));
            }
        }
    }

    // Per-namespace GPU quota groups: for each configured namespace, cap the total GPUs of
    // its admitted pending workloads at (configured cap - already-running GPUs), clamped ≥0.
    // Only emit a group when that namespace actually has pending workloads to constrain.
    let mut quota_groups: Vec<QuotaGroup> = Vec::new();
    for (ns, cap) in quotas {
        let remaining = (cap - running_gpu_by_ns.get(ns).copied().unwrap_or(0)).max(0);
        let workload_ids: Vec<String> = emitted_meta
            .iter()
            .filter(|m| &m.1 == ns)
            .map(|m| m.0.clone())
            .collect();
        if workload_ids.is_empty() {
            continue;
        }
        quota_groups.push(QuotaGroup {
            workload_ids,
            resource: GPU_RESOURCE.to_string(),
            limit: remaining,
        });
    }

    OptimizationInput {
        nodes,
        workloads,
        anti_affinity_pairs,
        quota_groups,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{NormalizedCluster, NormalizedNode, NormalizedWorkload, ResourceList};
    use crate::scheduler::pod_filter::PendingGpuPod;
    use std::collections::BTreeMap;

    /// Test wrapper: most tests don't exercise quotas. Shadows the real (3-arg)
    /// `build_pending_input` (explicit item wins over the `use super::*` glob) with an
    /// empty-quota call. Quota tests call `super::build_pending_input` directly.
    fn build_pending_input(
        cluster: &NormalizedCluster,
        pending: &[PendingGpuPod],
    ) -> OptimizationInput {
        super::build_pending_input(cluster, pending, &BTreeMap::new())
    }

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

    fn running_labeled(
        ns: &str,
        name: &str,
        node: &str,
        labels: &[(&str, &str)],
    ) -> NormalizedWorkload {
        let mut w = workload(ns, name, node, 1000, 2, 1, &[node]);
        w.labels = labels
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        w
    }

    fn ppod_aa(ns: &str, name: &str, selectors: &[&[(&str, &str)]]) -> PendingGpuPod {
        let mut p = ppod(ns, name, None);
        p.anti_affinity_host_selectors = selectors
            .iter()
            .map(|s| {
                s.iter()
                    .map(|(k, v)| (k.to_string(), v.to_string()))
                    .collect()
            })
            .collect();
        p
    }

    #[test]
    fn anti_affinity_excludes_node_with_matching_running_pod() {
        let cluster = NormalizedCluster {
            nodes: vec![node("n1", 16000, 64, 110, 8), node("n2", 16000, 64, 110, 8)],
            workloads: vec![
                running_labeled("team", "peer", "n1", &[("app", "trainer")]),
                workload("team", "pending", "", 1000, 2, 1, &["n1", "n2"]),
            ],
            ..Default::default()
        };
        let input = build_pending_input(
            &cluster,
            &[ppod_aa("team", "pending", &[&[("app", "trainer")]])],
        );
        assert_eq!(input.workloads.len(), 1);
        assert_eq!(input.workloads[0].feasible_nodes, vec!["n2".to_string()]);
    }

    #[test]
    fn anti_affinity_ignores_other_namespace_running_pod() {
        let cluster = NormalizedCluster {
            nodes: vec![node("n1", 16000, 64, 110, 8), node("n2", 16000, 64, 110, 8)],
            workloads: vec![
                running_labeled("other", "peer", "n1", &[("app", "trainer")]),
                workload("team", "pending", "", 1000, 2, 1, &["n1", "n2"]),
            ],
            ..Default::default()
        };
        let input = build_pending_input(
            &cluster,
            &[ppod_aa("team", "pending", &[&[("app", "trainer")]])],
        );
        assert_eq!(input.workloads[0].feasible_nodes.len(), 2);
    }

    #[test]
    fn no_selectors_means_no_exclusion() {
        let cluster = NormalizedCluster {
            nodes: vec![node("n1", 16000, 64, 110, 8)],
            workloads: vec![
                running_labeled("team", "peer", "n1", &[("app", "trainer")]),
                workload("team", "pending", "", 1000, 2, 1, &["n1"]),
            ],
            ..Default::default()
        };
        let input = build_pending_input(&cluster, &[ppod("team", "pending", None)]);
        assert_eq!(input.workloads[0].feasible_nodes, vec!["n1".to_string()]);
    }

    fn labeled_pending(
        ns: &str,
        name: &str,
        feasible: &[&str],
        labels: &[(&str, &str)],
    ) -> NormalizedWorkload {
        let mut w = workload(ns, name, "", 1000, 2, 1, feasible);
        w.labels = labels
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        w
    }

    fn gang_member_aa(
        ns: &str,
        name: &str,
        gang: &str,
        selectors: &[&[(&str, &str)]],
        colocate: bool,
    ) -> PendingGpuPod {
        PendingGpuPod {
            uid: format!("uid-{name}"),
            namespace: ns.into(),
            name: name.into(),
            gpu_request: 1,
            gang_key: Some(format!("{ns}/{gang}")),
            colocate,
            unmodeled_constraints: vec![],
            anti_affinity_host_selectors: selectors
                .iter()
                .map(|s| {
                    s.iter()
                        .map(|(k, v)| (k.to_string(), v.to_string()))
                        .collect()
                })
                .collect(),
        }
    }

    #[test]
    fn self_anti_affine_gang_gets_anti_affinity_pair() {
        let cluster = NormalizedCluster {
            nodes: vec![
                node("n1", 16000, 64, 110, 8),
                node("n2", 16000, 64, 110, 8),
                node("n3", 16000, 64, 110, 8),
            ],
            workloads: vec![
                labeled_pending("team", "m0", &["n1", "n2", "n3"], &[("app", "trainer")]),
                labeled_pending("team", "m1", &["n1", "n2", "n3"], &[("app", "trainer")]),
                labeled_pending("team", "m2", &["n1", "n2", "n3"], &[("app", "trainer")]),
            ],
            ..Default::default()
        };
        let sel: &[&[(&str, &str)]] = &[&[("app", "trainer")]];
        let input = build_pending_input(
            &cluster,
            &[
                gang_member_aa("team", "m0", "job", sel, false),
                gang_member_aa("team", "m1", "job", sel, false),
                gang_member_aa("team", "m2", "job", sel, false),
            ],
        );
        assert_eq!(input.workloads.len(), 1);
        assert!(input
            .anti_affinity_pairs
            .contains(&("gang:team/job".to_string(), "gang:team/job".to_string())));
    }

    #[test]
    fn mixed_label_gang_gets_no_anti_affinity_pair() {
        // selector app=trainer but only m0 carries the label -> not all members match.
        let cluster = NormalizedCluster {
            nodes: vec![node("n1", 16000, 64, 110, 8), node("n2", 16000, 64, 110, 8)],
            workloads: vec![
                labeled_pending("team", "m0", &["n1", "n2"], &[("app", "trainer")]),
                labeled_pending("team", "m1", &["n1", "n2"], &[("app", "other")]),
            ],
            ..Default::default()
        };
        let sel: &[&[(&str, &str)]] = &[&[("app", "trainer")]];
        let input = build_pending_input(
            &cluster,
            &[
                gang_member_aa("team", "m0", "job", sel, false),
                gang_member_aa("team", "m1", "job", sel, false),
            ],
        );
        assert!(input.anti_affinity_pairs.is_empty());
    }

    #[test]
    fn colocate_plus_self_anti_affine_gang_excluded() {
        let cluster = NormalizedCluster {
            nodes: vec![node("n1", 16000, 64, 110, 8), node("n2", 16000, 64, 110, 8)],
            workloads: vec![
                labeled_pending("team", "m0", &["n1", "n2"], &[("app", "trainer")]),
                labeled_pending("team", "m1", &["n1", "n2"], &[("app", "trainer")]),
            ],
            ..Default::default()
        };
        let sel: &[&[(&str, &str)]] = &[&[("app", "trainer")]];
        let input = build_pending_input(
            &cluster,
            &[
                gang_member_aa("team", "m0", "job", sel, true),
                gang_member_aa("team", "m1", "job", sel, true),
            ],
        );
        assert_eq!(input.workloads.len(), 0);
    }

    fn running_anti(
        ns: &str,
        name: &str,
        node: &str,
        selectors: &[&[(&str, &str)]],
    ) -> NormalizedWorkload {
        let mut w = workload(ns, name, node, 1000, 2, 1, &[node]);
        w.anti_affinity_host_selectors = selectors
            .iter()
            .map(|s| {
                s.iter()
                    .map(|(k, v)| (k.to_string(), v.to_string()))
                    .collect()
            })
            .collect();
        w
    }

    #[test]
    fn symmetry_excludes_node_even_without_pending_selectors() {
        // running pod on n1 forbids app=trainer; pending pod is app=trainer with NO own
        // anti-affinity -> symmetry must still exclude n1.
        let cluster = NormalizedCluster {
            nodes: vec![node("n1", 16000, 64, 110, 8), node("n2", 16000, 64, 110, 8)],
            workloads: vec![
                running_anti("team", "peer", "n1", &[&[("app", "trainer")]]),
                labeled_pending("team", "p", &["n1", "n2"], &[("app", "trainer")]),
            ],
            ..Default::default()
        };
        let input = build_pending_input(&cluster, &[ppod("team", "p", None)]);
        assert_eq!(input.workloads.len(), 1);
        assert_eq!(input.workloads[0].feasible_nodes, vec!["n2".to_string()]);
    }

    #[test]
    fn symmetry_not_excluded_on_partial_gang_match() {
        // running forbids app=trainer; gang has one member app=trainer, one without -> not all.
        let mut m0 = workload("team", "m0", "", 1000, 2, 1, &["n1"]);
        m0.labels = [("app".to_string(), "trainer".to_string())].into();
        let m1 = workload("team", "m1", "", 1000, 2, 1, &["n1"]); // no labels
        let cluster = NormalizedCluster {
            nodes: vec![node("n1", 16000, 64, 110, 8)],
            workloads: vec![
                running_anti("team", "peer", "n1", &[&[("app", "trainer")]]),
                m0,
                m1,
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
        // not excluded -> gang stays feasible on n1
        assert_eq!(input.workloads.len(), 1);
        assert_eq!(input.workloads[0].feasible_nodes, vec!["n1".to_string()]);
    }

    #[test]
    fn symmetry_ignores_other_namespace() {
        let cluster = NormalizedCluster {
            nodes: vec![node("n1", 16000, 64, 110, 8)],
            workloads: vec![
                running_anti("other", "peer", "n1", &[&[("app", "trainer")]]),
                labeled_pending("team", "p", &["n1"], &[("app", "trainer")]),
            ],
            ..Default::default()
        };
        let input = build_pending_input(&cluster, &[ppod("team", "p", None)]);
        assert_eq!(input.workloads[0].feasible_nodes, vec!["n1".to_string()]);
    }

    #[test]
    fn symmetry_no_exclusion_when_labels_dont_match() {
        let cluster = NormalizedCluster {
            nodes: vec![node("n1", 16000, 64, 110, 8)],
            workloads: vec![
                running_anti("team", "peer", "n1", &[&[("app", "trainer")]]),
                labeled_pending("team", "p", &["n1"], &[("app", "other")]),
            ],
            ..Default::default()
        };
        let input = build_pending_input(&cluster, &[ppod("team", "p", None)]);
        assert_eq!(input.workloads[0].feasible_nodes, vec!["n1".to_string()]);
    }

    #[test]
    fn cross_workload_pair_when_selector_matches_other() {
        // singleton A anti-affine {app:b}; singleton B labelled {app:b}; same ns.
        let cluster = NormalizedCluster {
            nodes: vec![node("n1", 16000, 64, 110, 8)],
            workloads: vec![
                labeled_pending("team", "a", &["n1"], &[]),
                labeled_pending("team", "b", &["n1"], &[("app", "b")]),
            ],
            ..Default::default()
        };
        let input = build_pending_input(
            &cluster,
            &[
                ppod_aa("team", "a", &[&[("app", "b")]]),
                ppod("team", "b", None),
            ],
        );
        assert!(input
            .anti_affinity_pairs
            .contains(&("pod:team/a".to_string(), "pod:team/b".to_string())));
    }

    #[test]
    fn cross_workload_no_pair_on_partial_member_match() {
        // A anti-affine {app:b}; B is a gang where only one member carries {app:b}.
        let cluster = NormalizedCluster {
            nodes: vec![node("n1", 16000, 64, 110, 8), node("n2", 16000, 64, 110, 8)],
            workloads: vec![
                labeled_pending("team", "a", &["n1", "n2"], &[]),
                labeled_pending("team", "b0", &["n1", "n2"], &[("app", "b")]),
                labeled_pending("team", "b1", &["n1", "n2"], &[]),
            ],
            ..Default::default()
        };
        let input = build_pending_input(
            &cluster,
            &[
                ppod_aa("team", "a", &[&[("app", "b")]]),
                ppod("team", "b0", Some("bjob")),
                ppod("team", "b1", Some("bjob")),
            ],
        );
        assert!(input.anti_affinity_pairs.is_empty());
    }

    #[test]
    fn cross_workload_no_pair_across_namespaces() {
        let cluster = NormalizedCluster {
            nodes: vec![node("n1", 16000, 64, 110, 8)],
            workloads: vec![
                labeled_pending("team", "a", &["n1"], &[]),
                labeled_pending("other", "b", &["n1"], &[("app", "b")]),
            ],
            ..Default::default()
        };
        let input = build_pending_input(
            &cluster,
            &[
                ppod_aa("team", "a", &[&[("app", "b")]]),
                ppod("other", "b", None),
            ],
        );
        assert!(input.anti_affinity_pairs.is_empty());
    }

    #[test]
    fn gang_members_disagreeing_on_anti_affinity_excluded() {
        let cluster = NormalizedCluster {
            nodes: vec![node("n1", 16000, 64, 110, 8)],
            workloads: vec![
                workload("team", "m0", "", 1000, 2, 1, &["n1"]),
                workload("team", "m1", "", 1000, 2, 1, &["n1"]),
            ],
            ..Default::default()
        };
        let mut m0 = ppod("team", "m0", Some("job"));
        m0.anti_affinity_host_selectors = vec![[("app".to_string(), "x".to_string())].into()];
        let m1 = ppod("team", "m1", Some("job")); // no selectors -> disagreement
        let input = build_pending_input(&cluster, &[m0, m1]);
        assert_eq!(input.workloads.len(), 0);
    }

    #[test]
    fn quota_group_emitted_with_pending_ids_and_full_limit() {
        // Two 1-GPU pending singletons in `team`, nothing running -> one quota group over
        // both ids with the full configured limit.
        let cluster = NormalizedCluster {
            nodes: vec![node("n1", 16000, 64, 110, 8)],
            workloads: vec![
                workload("team", "a", "", 1000, 2, 1, &["n1"]),
                workload("team", "b", "", 1000, 2, 1, &["n1"]),
            ],
            ..Default::default()
        };
        let quotas = BTreeMap::from([("team".to_string(), 1_i64)]);
        let input = super::build_pending_input(
            &cluster,
            &[ppod("team", "a", None), ppod("team", "b", None)],
            &quotas,
        );
        assert_eq!(input.quota_groups.len(), 1);
        let g = &input.quota_groups[0];
        assert_eq!(g.resource, "nvidia.com/gpu");
        assert_eq!(g.limit, 1);
        let mut ids = g.workload_ids.clone();
        ids.sort();
        assert_eq!(
            ids,
            vec!["pod:team/a".to_string(), "pod:team/b".to_string()]
        );
    }

    #[test]
    fn quota_limit_clamped_by_running_usage() {
        // Cap 2, but a running 1-GPU pod in `team` already consumes 1 -> remaining 1.
        // A second running pod would drive it to 0 (clamped, never negative).
        let cluster = NormalizedCluster {
            nodes: vec![node("n1", 16000, 64, 110, 8)],
            workloads: vec![
                workload("team", "running", "n1", 1000, 2, 1, &["n1"]),
                workload("team", "pending", "", 1000, 2, 1, &["n1"]),
            ],
            ..Default::default()
        };
        let quotas = BTreeMap::from([("team".to_string(), 2_i64)]);
        let input = super::build_pending_input(&cluster, &[ppod("team", "pending", None)], &quotas);
        assert_eq!(input.quota_groups.len(), 1);
        assert_eq!(input.quota_groups[0].limit, 1);
        assert_eq!(
            input.quota_groups[0].workload_ids,
            vec!["pod:team/pending".to_string()]
        );

        // Same cap but 2 running GPUs -> remaining clamped to 0.
        let cluster2 = NormalizedCluster {
            nodes: vec![node("n1", 16000, 64, 110, 8)],
            workloads: vec![
                workload("team", "r0", "n1", 1000, 2, 1, &["n1"]),
                workload("team", "r1", "n1", 1000, 2, 1, &["n1"]),
                workload("team", "pending", "", 1000, 2, 1, &["n1"]),
            ],
            ..Default::default()
        };
        let input2 =
            super::build_pending_input(&cluster2, &[ppod("team", "pending", None)], &quotas);
        assert_eq!(input2.quota_groups[0].limit, 0);
    }

    #[test]
    fn no_quota_group_for_unconfigured_namespace() {
        let cluster = NormalizedCluster {
            nodes: vec![node("n1", 16000, 64, 110, 8)],
            workloads: vec![workload("team", "a", "", 1000, 2, 1, &["n1"])],
            ..Default::default()
        };
        // quota configured for a different namespace -> no group for `team`.
        let quotas = BTreeMap::from([("other".to_string(), 5_i64)]);
        let input = super::build_pending_input(&cluster, &[ppod("team", "a", None)], &quotas);
        assert!(input.quota_groups.is_empty());
    }
}
