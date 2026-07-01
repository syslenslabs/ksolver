use crate::model::{
    LabelSelectorReq, NormalizedCluster, NormalizedWorkload, OptimizationInput, OptimizationNode,
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

/// Whether one label-selector requirement holds against a pod's labels (Kubernetes semantics:
/// NotIn/DoesNotExist are satisfied by a MISSING key).
fn req_matches(req: &LabelSelectorReq, labels: &BTreeMap<String, String>) -> bool {
    match req.operator.as_str() {
        "In" => labels
            .get(&req.key)
            .map(|v| req.values.contains(v))
            .unwrap_or(false),
        "NotIn" => labels
            .get(&req.key)
            .map(|v| !req.values.contains(v))
            .unwrap_or(true),
        "Exists" => labels.contains_key(&req.key),
        "DoesNotExist" => !labels.contains_key(&req.key),
        _ => false,
    }
}

/// A modeled selector (a list of requirements, ANDed) matches a workload's labels iff every
/// requirement holds.
fn selector_matches(selector: &[LabelSelectorReq], labels: &BTreeMap<String, String>) -> bool {
    selector.iter().all(|req| req_matches(req, labels))
}

/// Deterministic canonical form of a requirement so gang-member selector sets compare
/// order-insensitively (values sorted within a requirement).
fn canonical_req(r: &LabelSelectorReq) -> (String, String, Vec<String>) {
    let mut vals = r.values.clone();
    vals.sort();
    (r.key.clone(), r.operator.clone(), vals)
}

/// Canonical form of a selector set (list of selectors) for order-insensitive comparison.
#[allow(clippy::type_complexity)]
fn canonical_selectors(sels: &[Vec<LabelSelectorReq>]) -> Vec<Vec<(String, String, Vec<String>)>> {
    let mut out: Vec<Vec<(String, String, Vec<String>)>> = sels
        .iter()
        .map(|sel| {
            let mut reqs: Vec<(String, String, Vec<String>)> =
                sel.iter().map(canonical_req).collect();
            reqs.sort();
            reqs
        })
        .collect();
    out.sort();
    out
}

/// Canonical form of a `(topologyKey, selector)` set for order-insensitive gang-member
/// agreement comparison (Phase 12).
#[allow(clippy::type_complexity)]
fn canonical_topology_selectors(
    sels: &[(String, Vec<LabelSelectorReq>)],
) -> Vec<(String, Vec<(String, String, Vec<String>)>)> {
    let mut out: Vec<(String, Vec<(String, String, Vec<String>)>)> = sels
        .iter()
        .map(|(k, sel)| {
            let mut reqs: Vec<(String, String, Vec<String>)> =
                sel.iter().map(canonical_req).collect();
            reqs.sort();
            (k.clone(), reqs)
        })
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

/// Why a pending pod (or gang) was dropped during input build and never submitted to the
/// solver. `pod_scopes` are the affected pending pods as `namespace/name` (decision.rs keying).
#[derive(Debug, Clone)]
pub struct DropInfo {
    pub pod_scopes: Vec<String>,
    pub reason: String,
}

/// Build an optimization input that places ONLY the pending pods (see
/// `build_pending_input_diagnosed`); returns just the input for callers that don't need the
/// drop diagnostics (preserves the original signature — zero ripple).
pub fn build_pending_input(
    cluster: &NormalizedCluster,
    pending: &[PendingGpuPod],
    quotas: &BTreeMap<String, i64>,
) -> OptimizationInput {
    build_pending_input_diagnosed(cluster, pending, quotas).0
}

/// Build an optimization input that places ONLY the pending pods, grouping pods that
/// share a gang key into a single all-or-nothing `group_size` workload. Running
/// (already-placed) pods are fixed context, subtracted from node capacity (residual).
/// Also returns a `DropInfo` per gang/pod that was excluded during input build, with a
/// specific reason (for the shadow decision trace). Placement/feasibility is unchanged.
pub fn build_pending_input_diagnosed(
    cluster: &NormalizedCluster,
    pending: &[PendingGpuPod],
    quotas: &BTreeMap<String, i64>,
) -> (OptimizationInput, Vec<DropInfo>) {
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

    // 3. Per-pod NormalizedWorkload lookup by "{ns}/{name}", running pods per node, and a
    //    node -> labels map (for topology-domain anti-affinity exclusion, Phase 12).
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
    let node_labels: BTreeMap<&str, &BTreeMap<String, String>> = cluster
        .nodes
        .iter()
        .map(|n| (n.name.as_str(), &n.labels))
        .collect();
    // Topology domain value of a node for a topology key: Some(value) iff the node carries
    // that label. A node without the label is its own singleton domain (never equal to a
    // present value), so it is never excluded by domain equality.
    let domain = |node: &str, key: &str| -> Option<String> {
        node_labels.get(node).and_then(|l| l.get(key).cloned())
    };

    // 4. Group pending pods into gangs (only unbound pods; a stale pod already bound was
    //    subtracted above and must not be a decision variable).
    let mut gangs: BTreeMap<String, Vec<&PendingGpuPod>> = BTreeMap::new();
    for p in pending {
        gangs.entry(gang_id(p)).or_default().push(p);
    }

    // 5. Build one workload per feasible, homogeneous gang.
    let mut workloads = Vec::new();
    let mut dropped: Vec<DropInfo> = Vec::new();
    let scopes = |ms: &[&PendingGpuPod]| -> Vec<String> {
        ms.iter()
            .map(|m| format!("{}/{}", m.namespace, m.name))
            .collect()
    };
    let mut anti_affinity_pairs: Vec<(String, String)> = Vec::new();
    // (id, namespace, selectors, member_labels) for each emitted workload, for cross-pairs.
    type EmittedMeta = (
        String,
        String,
        Vec<Vec<LabelSelectorReq>>,
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
            dropped.push(DropInfo {
                pod_scopes: scopes(&members),
                reason: "gang member missing from cluster snapshot".to_string(),
            });
            continue;
        }
        // Enforce homogeneity: identical requests, extended requests, feasible sets.
        let rep = member_workloads[0];
        let rep_sig = signature(rep);
        if member_workloads.iter().any(|w| signature(w) != rep_sig) {
            dropped.push(DropInfo {
                pod_scopes: scopes(&members),
                reason: "gang members have heterogeneous requests or feasible sets".to_string(),
            });
            continue;
        }
        // Members must agree on co-location; disagreement excludes the gang.
        let colocate = members[0].colocate;
        if members.iter().any(|m| m.colocate != colocate) {
            dropped.push(DropInfo {
                pod_scopes: scopes(&members),
                reason: "gang members disagree on co-location".to_string(),
            });
            continue;
        }
        // Members must agree on anti-affinity selectors (order-insensitive); else exclude.
        let rep_aa = canonical_selectors(&members[0].anti_affinity_host_selectors);
        if members
            .iter()
            .any(|m| canonical_selectors(&m.anti_affinity_host_selectors) != rep_aa)
        {
            dropped.push(DropInfo {
                pod_scopes: scopes(&members),
                reason: "gang members disagree on anti-affinity selectors".to_string(),
            });
            continue;
        }
        // Members must also agree on non-hostname (topology) anti-affinity selectors.
        let rep_aa_topo =
            canonical_topology_selectors(&members[0].anti_affinity_topology_selectors);
        if members.iter().any(|m| {
            canonical_topology_selectors(&m.anti_affinity_topology_selectors) != rep_aa_topo
        }) {
            dropped.push(DropInfo {
                pod_scopes: scopes(&members),
                reason: "gang members disagree on topology anti-affinity selectors".to_string(),
            });
            continue;
        }
        let aa_selectors = &members[0].anti_affinity_host_selectors;
        let aa_topo_selectors = &members[0].anti_affinity_topology_selectors;
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
            dropped.push(DropInfo {
                pod_scopes: scopes(&members),
                reason: "co-location conflicts with self-spread anti-affinity".to_string(),
            });
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
            // Best-effort NON-hostname topology anti-affinity (Phase 12): exclude a candidate
            // node whose topology domain (node.labels[key]) matches that of a node hosting a
            // matching same-namespace running pod — forward (pending selector vs running labels)
            // and symmetric (running selector vs ALL pending members). A node lacking the key
            // is a singleton domain (domain == None) and is never excluded by equality.
            .filter(|cn| {
                if aa_topo_selectors.is_empty()
                    && !running_by_node.values().flatten().any(|w| {
                        w.namespace == rep.namespace
                            && !w.anti_affinity_topology_selectors.is_empty()
                    })
                {
                    return true;
                }
                let violates = running_by_node.iter().any(|(rn, pods)| {
                    pods.iter().any(|w| {
                        if w.namespace != rep.namespace {
                            return false;
                        }
                        let forward = aa_topo_selectors.iter().any(|(key, s)| {
                            selector_matches(s, &w.labels)
                                && domain(cn, key).is_some()
                                && domain(cn, key) == domain(rn, key)
                        });
                        let symmetric =
                            w.anti_affinity_topology_selectors.iter().any(|(key, rs)| {
                                member_labels.iter().all(|ml| selector_matches(rs, ml))
                                    && domain(cn, key).is_some()
                                    && domain(cn, key) == domain(rn, key)
                            });
                        forward || symmetric
                    })
                });
                !violates
            })
            .cloned()
            .collect();
        if feasible_nodes.is_empty() {
            dropped.push(DropInfo {
                pod_scopes: scopes(&members),
                reason:
                    "no feasible node (insufficient residual capacity or excluded by anti-affinity)"
                        .to_string(),
            });
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

    (
        OptimizationInput {
            nodes,
            workloads,
            anti_affinity_pairs,
            quota_groups,
        },
        dropped,
    )
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
            anti_affinity_topology_selectors: vec![],
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

    /// matchLabels pairs -> a modeled selector (each pair as `In [v]`).
    fn reqs(pairs: &[(&str, &str)]) -> Vec<LabelSelectorReq> {
        pairs
            .iter()
            .map(|(k, v)| LabelSelectorReq {
                key: k.to_string(),
                operator: "In".to_string(),
                values: vec![v.to_string()],
            })
            .collect()
    }
    /// A list of matchLabels selectors -> modeled selector list.
    fn sel_list(selectors: &[&[(&str, &str)]]) -> Vec<Vec<LabelSelectorReq>> {
        selectors.iter().map(|s| reqs(s)).collect()
    }

    fn ppod_aa(ns: &str, name: &str, selectors: &[&[(&str, &str)]]) -> PendingGpuPod {
        let mut p = ppod(ns, name, None);
        p.anti_affinity_host_selectors = sel_list(selectors);
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
            anti_affinity_host_selectors: sel_list(selectors),
            anti_affinity_topology_selectors: vec![],
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
        w.anti_affinity_host_selectors = sel_list(selectors);
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
        m0.anti_affinity_host_selectors = vec![reqs(&[("app", "x")])];
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

    // ---- Phase 12: non-hostname topology anti-affinity ----

    const ZONE: &str = "topology.kubernetes.io/zone";

    fn zoned_node(name: &str, gpu: i64, zone: Option<&str>) -> NormalizedNode {
        let mut n = node(name, 16000, 64, 110, gpu);
        if let Some(z) = zone {
            n.labels = [(ZONE.to_string(), z.to_string())].into();
        }
        n
    }

    fn ppod_topo(ns: &str, name: &str, key: &str, labels: &[(&str, &str)]) -> PendingGpuPod {
        let mut p = ppod(ns, name, None);
        p.anti_affinity_topology_selectors = vec![(key.to_string(), reqs(labels))];
        p
    }

    #[test]
    fn zone_anti_affinity_excludes_whole_zone() {
        // n1,n2 in zone za; n3 in zb. A running app=trainer pod sits on n1 (za). A pending
        // pod with a zone anti-affinity on app=trainer must avoid ALL of za (n1 AND n2),
        // landing only in zb (n3).
        let cluster = NormalizedCluster {
            nodes: vec![
                zoned_node("n1", 8, Some("za")),
                zoned_node("n2", 8, Some("za")),
                zoned_node("n3", 8, Some("zb")),
            ],
            workloads: vec![
                running_labeled("team", "peer", "n1", &[("app", "trainer")]),
                workload("team", "pending", "", 1000, 2, 1, &["n1", "n2", "n3"]),
            ],
            ..Default::default()
        };
        let input = super::build_pending_input(
            &cluster,
            &[ppod_topo("team", "pending", ZONE, &[("app", "trainer")])],
            &BTreeMap::new(),
        );
        assert_eq!(input.workloads.len(), 1);
        assert_eq!(input.workloads[0].feasible_nodes, vec!["n3".to_string()]);
    }

    #[test]
    fn zone_anti_affinity_ignores_node_without_zone_label() {
        // Running pod on n1 (no zone label = singleton domain). A pending zone-anti-affine
        // pod is NOT excluded from n2 (different/absent domain) — only exact domain matches.
        let cluster = NormalizedCluster {
            nodes: vec![zoned_node("n1", 8, None), zoned_node("n2", 8, Some("zb"))],
            workloads: vec![
                running_labeled("team", "peer", "n1", &[("app", "trainer")]),
                workload("team", "pending", "", 1000, 2, 1, &["n1", "n2"]),
            ],
            ..Default::default()
        };
        let input = super::build_pending_input(
            &cluster,
            &[ppod_topo("team", "pending", ZONE, &[("app", "trainer")])],
            &BTreeMap::new(),
        );
        // n1 has no zone -> domain(n1)=None -> not excluded by equality; n2 is a different
        // domain (zb) with no matching peer -> both remain feasible.
        assert_eq!(input.workloads[0].feasible_nodes.len(), 2);
    }

    #[test]
    fn zone_anti_affinity_symmetry_excludes_zone() {
        // A running pod carries a ZONE anti-affinity on app=trainer and sits in za (n1).
        // A pending app=trainer pod with NO own selector must still avoid all of za (n2 too).
        let mut peer = running_labeled("team", "peer", "n1", &[]);
        peer.anti_affinity_topology_selectors =
            vec![(ZONE.to_string(), reqs(&[("app", "trainer")]))];
        let cluster = NormalizedCluster {
            nodes: vec![
                zoned_node("n1", 8, Some("za")),
                zoned_node("n2", 8, Some("za")),
                zoned_node("n3", 8, Some("zb")),
            ],
            workloads: vec![
                peer,
                labeled_pending("team", "p", &["n1", "n2", "n3"], &[("app", "trainer")]),
            ],
            ..Default::default()
        };
        let input =
            super::build_pending_input(&cluster, &[ppod("team", "p", None)], &BTreeMap::new());
        assert_eq!(input.workloads[0].feasible_nodes, vec!["n3".to_string()]);
    }

    #[test]
    fn zone_anti_affinity_ignores_other_namespace() {
        let cluster = NormalizedCluster {
            nodes: vec![
                zoned_node("n1", 8, Some("za")),
                zoned_node("n2", 8, Some("zb")),
            ],
            workloads: vec![
                running_labeled("other", "peer", "n1", &[("app", "trainer")]),
                workload("team", "pending", "", 1000, 2, 1, &["n1", "n2"]),
            ],
            ..Default::default()
        };
        let input = super::build_pending_input(
            &cluster,
            &[ppod_topo("team", "pending", ZONE, &[("app", "trainer")])],
            &BTreeMap::new(),
        );
        assert_eq!(input.workloads[0].feasible_nodes.len(), 2);
    }

    // ---- matchExpressions in anti-affinity ----

    fn one_req(key: &str, op: &str, values: &[&str]) -> Vec<Vec<LabelSelectorReq>> {
        vec![vec![LabelSelectorReq {
            key: key.to_string(),
            operator: op.to_string(),
            values: values.iter().map(|v| v.to_string()).collect(),
        }]]
    }

    #[test]
    fn exists_selector_excludes_node_with_matching_key() {
        // Pending pod's hostname anti-affinity: `app Exists`. A running pod on n1 carries any
        // `app` label ⇒ n1 excluded; n2 (no matching running pod) stays.
        let cluster = NormalizedCluster {
            nodes: vec![node("n1", 16000, 64, 110, 8), node("n2", 16000, 64, 110, 8)],
            workloads: vec![
                running_labeled("team", "peer", "n1", &[("app", "anything")]),
                workload("team", "pending", "", 1000, 2, 1, &["n1", "n2"]),
            ],
            ..Default::default()
        };
        let mut p = ppod("team", "pending", None);
        p.anti_affinity_host_selectors = one_req("app", "Exists", &[]);
        let input = super::build_pending_input(&cluster, &[p], &BTreeMap::new());
        assert_eq!(input.workloads[0].feasible_nodes, vec!["n2".to_string()]);
    }

    #[test]
    fn notin_selector_excludes_node_when_key_missing() {
        // Pending pod's anti-affinity: `tier NotIn [db]`. A running pod on n1 LACKS `tier`, so
        // per kube NotIn-missing semantics it MATCHES ⇒ n1 excluded; n2 stays.
        let cluster = NormalizedCluster {
            nodes: vec![node("n1", 16000, 64, 110, 8), node("n2", 16000, 64, 110, 8)],
            workloads: vec![
                running_labeled("team", "peer", "n1", &[("app", "x")]), // no `tier` label
                workload("team", "pending", "", 1000, 2, 1, &["n1", "n2"]),
            ],
            ..Default::default()
        };
        let mut p = ppod("team", "pending", None);
        p.anti_affinity_host_selectors = one_req("tier", "NotIn", &["db"]);
        let input = super::build_pending_input(&cluster, &[p], &BTreeMap::new());
        assert_eq!(input.workloads[0].feasible_nodes, vec!["n2".to_string()]);
    }

    #[test]
    fn in_selector_multivalue_matches_any() {
        // `app In [trainer, infer]` matches a running pod with app=infer ⇒ node excluded.
        let cluster = NormalizedCluster {
            nodes: vec![node("n1", 16000, 64, 110, 8), node("n2", 16000, 64, 110, 8)],
            workloads: vec![
                running_labeled("team", "peer", "n1", &[("app", "infer")]),
                workload("team", "pending", "", 1000, 2, 1, &["n1", "n2"]),
            ],
            ..Default::default()
        };
        let mut p = ppod("team", "pending", None);
        p.anti_affinity_host_selectors = one_req("app", "In", &["trainer", "infer"]);
        let input = super::build_pending_input(&cluster, &[p], &BTreeMap::new());
        assert_eq!(input.workloads[0].feasible_nodes, vec!["n2".to_string()]);
    }

    // ---- Phase 13: drop diagnostics ----

    #[test]
    fn diagnosed_reports_heterogeneous_gang() {
        let cluster = NormalizedCluster {
            nodes: vec![node("n1", 16000, 64, 110, 8)],
            workloads: vec![
                workload("team", "m0", "", 1000, 2, 1, &["n1"]),
                workload("team", "m1", "", 4000, 2, 1, &["n1"]), // different cpu -> heterogeneous
            ],
            ..Default::default()
        };
        let (input, drops) = super::build_pending_input_diagnosed(
            &cluster,
            &[
                ppod("team", "m0", Some("job")),
                ppod("team", "m1", Some("job")),
            ],
            &BTreeMap::new(),
        );
        assert_eq!(input.workloads.len(), 0);
        assert_eq!(drops.len(), 1);
        assert!(drops[0].reason.contains("heterogeneous"));
        let mut scopes = drops[0].pod_scopes.clone();
        scopes.sort();
        assert_eq!(scopes, vec!["team/m0".to_string(), "team/m1".to_string()]);
    }

    #[test]
    fn diagnosed_reports_no_feasible_node_on_full_cluster() {
        // The only node's GPUs are fully consumed by a running pod ⇒ pending 1-GPU pod drops.
        let cluster = NormalizedCluster {
            nodes: vec![node("n1", 16000, 64, 110, 8)],
            workloads: vec![
                workload("prod", "running", "n1", 1000, 2, 8, &["n1"]),
                workload("team", "pending", "", 1000, 2, 1, &["n1"]),
            ],
            ..Default::default()
        };
        let (input, drops) = super::build_pending_input_diagnosed(
            &cluster,
            &[ppod("team", "pending", None)],
            &BTreeMap::new(),
        );
        assert_eq!(input.workloads.len(), 0);
        assert_eq!(drops.len(), 1);
        assert!(drops[0].reason.contains("no feasible node"));
        assert_eq!(drops[0].pod_scopes, vec!["team/pending".to_string()]);
    }

    #[test]
    fn mig_slice_pod_places_via_generic_extended_resource_path() {
        // A MIG node advertises a slice resource; a pending pod requesting that slice is
        // emitted and feasible — no GPU-specific solver/builder code, just the generic path.
        let mut mig_node = node("n1", 16000, 64, 110, 0);
        mig_node
            .extended_resources
            .insert("nvidia.com/mig-1g.5gb".to_string(), 7);
        let mut w = workload("team", "slice", "", 1000, 2, 0, &["n1"]);
        w.extended_resource_requests
            .insert("nvidia.com/mig-1g.5gb".to_string(), 1);
        let cluster = NormalizedCluster {
            nodes: vec![mig_node],
            workloads: vec![w],
            ..Default::default()
        };
        let input = build_pending_input(&cluster, &[ppod("team", "slice", None)]);
        assert_eq!(input.workloads.len(), 1);
        assert_eq!(input.workloads[0].feasible_nodes, vec!["n1".to_string()]);
        assert_eq!(
            *input.workloads[0]
                .extended_resource_requests
                .get("nvidia.com/mig-1g.5gb")
                .unwrap(),
            1
        );
    }

    // End-to-end pipeline: build_pending_input -> cpsat_rust::solve -> build_decision_trace,
    // exercising per-namespace quota + partial admission together (needs the CP-SAT backend).
    #[cfg(feature = "rust-cp-sat")]
    #[test]
    fn pipeline_quota_caps_admissions_end_to_end() {
        use crate::model::ScenarioConfig;
        use crate::scheduler::decision::build_decision_trace;
        use std::collections::HashMap;

        // Two 4-GPU nodes (ample capacity), three 1-GPU singletons in `team`, quota team=2.
        let cluster = NormalizedCluster {
            nodes: vec![node("n1", 16000, 64, 110, 4), node("n2", 16000, 64, 110, 4)],
            workloads: vec![
                workload("team", "a", "", 1000, 2, 1, &["n1", "n2"]),
                workload("team", "b", "", 1000, 2, 1, &["n1", "n2"]),
                workload("team", "c", "", 1000, 2, 1, &["n1", "n2"]),
            ],
            ..Default::default()
        };
        let pending = vec![
            ppod("team", "a", None),
            ppod("team", "b", None),
            ppod("team", "c", None),
        ];
        let quotas = BTreeMap::from([("team".to_string(), 2_i64)]);
        let (input, drops) = super::build_pending_input_diagnosed(&cluster, &pending, &quotas);
        assert_eq!(input.workloads.len(), 3); // all fit capacity; quota limits admission, not build
        assert!(drops.is_empty());

        let scenario = ScenarioConfig {
            solver: "cp-sat-rust".to_string(),
            partial_admission: true,
            ..Default::default()
        };
        let (solution, info) = crate::cpsat_rust::solve(&input, &scenario).expect("solve");

        let drop_reasons: HashMap<String, String> = HashMap::new();
        let trace = build_decision_trace(
            1,
            &pending,
            &input,
            &solution,
            &info.status,
            true,
            5,
            5,
            1,
            &drop_reasons,
        );
        let placed = trace
            .decisions
            .iter()
            .filter(|d| {
                matches!(
                    d.placement,
                    crate::scheduler::trace::PodPlacement::Placed { .. }
                )
            })
            .count();
        let unplaced = trace
            .decisions
            .iter()
            .filter(|d| {
                matches!(
                    d.placement,
                    crate::scheduler::trace::PodPlacement::Unplaced { .. }
                )
            })
            .count();
        // Quota of 2 GPUs => exactly 2 of the 3 singletons admitted end-to-end.
        assert_eq!(placed, 2, "status={}", info.status);
        assert_eq!(unplaced, 1);
    }
}
