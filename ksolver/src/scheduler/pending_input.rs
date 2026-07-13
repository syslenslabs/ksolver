use crate::model::{
    AntiAffinitySelector, LabelSelectorReq, NormalizedCluster, NormalizedWorkload,
    OptimizationInput, OptimizationNode, OptimizationSolution, OptimizationWorkload,
    OptimizationWorkloadMember, QuotaGroup, ResourceList,
};
use crate::scheduler::pod_filter::PendingGpuPod;
use std::collections::{BTreeMap, BTreeSet};
use std::hash::{Hash, Hasher};

/// Resource name used for per-namespace quotas (MVP: GPUs only).
const GPU_RESOURCE: &str = "nvidia.com/gpu";

fn default_is_gpu_resource(name: &str) -> bool {
    name == GPU_RESOURCE || name.starts_with("nvidia.com/mig-")
}

fn is_dra_resource(name: &str) -> bool {
    name.starts_with("dra.ksolver/")
}

fn has_modeled_gpu_or_dra_demand(
    workload: &NormalizedWorkload,
    is_gpu_resource: &dyn Fn(&str) -> bool,
) -> bool {
    workload
        .extended_resource_requests
        .iter()
        .any(|(name, qty)| *qty > 0 && (is_gpu_resource(name) || is_dra_resource(name)))
}

fn pending_requires_unmodeled_dra(pending: &PendingGpuPod) -> bool {
    pending.gpu_request <= 0
        && pending
            .unmodeled_constraints
            .iter()
            .any(|c| c.starts_with("DRA:"))
}

/// Whether an anti-affinity selector applies to a pod in `other_ns`, given the selector owner's
/// namespace `own_ns` and the cluster's namespace labels. Kubernetes namespace scoping:
/// - no `namespaces` list AND no `namespaceSelector` ⇒ the owner's own namespace;
/// - otherwise the UNION of the explicit `namespaces` list and the `namespaceSelector` match
///   (empty selector `Some([])` = ALL namespaces; else namespaces whose labels satisfy the reqs).
fn selector_scopes_ns(
    sel: &AntiAffinitySelector,
    own_ns: &str,
    other_ns: &str,
    ns_labels: &BTreeMap<String, BTreeMap<String, String>>,
) -> bool {
    if sel.namespaces.is_empty() && sel.namespace_selector.is_none() {
        return other_ns == own_ns;
    }
    if sel.namespaces.iter().any(|n| n == other_ns) {
        return true;
    }
    match &sel.namespace_selector {
        None => false,
        Some(reqs) if reqs.is_empty() => true, // empty selector `{}` = all namespaces
        Some(reqs) => ns_labels
            .get(other_ns)
            .map(|l| reqs.iter().all(|r| req_matches(r, l)))
            .unwrap_or(false),
    }
}

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

fn match_labels_selector_matches(
    selector: &BTreeMap<String, String>,
    labels: &BTreeMap<String, String>,
) -> bool {
    !selector.is_empty()
        && selector
            .iter()
            .all(|(key, value)| labels.get(key) == Some(value))
}

fn topology_spread_selector_matches(
    rule: &crate::model::TopologySpreadRule,
    labels: &BTreeMap<String, String>,
) -> bool {
    if !rule.selector_reqs.is_empty() {
        selector_matches(&rule.selector_reqs, labels)
    } else {
        match_labels_selector_matches(&rule.selector, labels)
    }
}

fn modeled_topology_spread_rule(rule: &crate::model::TopologySpreadRule) -> bool {
    rule.when_unsatisfiable == "DoNotSchedule"
        && rule.max_skew > 0
        && !rule.topology_key.is_empty()
        && (!rule.selector_reqs.is_empty() || !rule.selector.is_empty())
        && rule.min_domains.is_none()
        && rule.node_affinity_policy.is_none()
        && rule.node_taints_policy.is_none()
        && rule.match_label_keys.is_empty()
}

#[allow(clippy::too_many_arguments)]
fn topology_spread_allows_node(
    candidate_node: &str,
    pending_namespace: &str,
    member_labels: &[&BTreeMap<String, String>],
    rules: &[crate::model::TopologySpreadRule],
    added_matching_pods: i64,
    running_by_node: &BTreeMap<String, Vec<&NormalizedWorkload>>,
    node_labels: &BTreeMap<&str, &BTreeMap<String, String>>,
) -> bool {
    for rule in rules {
        if !modeled_topology_spread_rule(rule) {
            continue;
        }
        if !member_labels
            .iter()
            .all(|labels| topology_spread_selector_matches(rule, labels))
        {
            continue;
        }
        let Some(candidate_domain) = node_labels
            .get(candidate_node)
            .and_then(|labels| labels.get(&rule.topology_key))
        else {
            return false;
        };

        let mut counts: BTreeMap<String, i64> = node_labels
            .values()
            .filter_map(|labels| labels.get(&rule.topology_key).cloned())
            .map(|domain| (domain, 0))
            .collect();
        if counts.is_empty() {
            return false;
        }

        for (node_name, pods) in running_by_node {
            let Some(domain) = node_labels
                .get(node_name.as_str())
                .and_then(|labels| labels.get(&rule.topology_key))
            else {
                continue;
            };
            for pod in pods {
                if pod.namespace == pending_namespace
                    && topology_spread_selector_matches(rule, &pod.labels)
                {
                    *counts.entry(domain.clone()).or_default() += 1;
                }
            }
        }

        let min_count = counts.values().copied().min().unwrap_or(0);
        let candidate_count = counts.get(candidate_domain).copied().unwrap_or(0);
        if candidate_count + added_matching_pods - min_count > i64::from(rule.max_skew) {
            return false;
        }
    }
    true
}

fn pod_affinity_allows_node(
    candidate_node: &str,
    pending_namespace: &str,
    selectors: &[(String, AntiAffinitySelector)],
    running_by_node: &BTreeMap<String, Vec<&NormalizedWorkload>>,
    node_labels: &BTreeMap<&str, &BTreeMap<String, String>>,
    namespace_labels: &BTreeMap<String, BTreeMap<String, String>>,
) -> bool {
    for (topology_key, selector) in selectors {
        if topology_key.is_empty() {
            continue;
        }
        let mut matching_domains = std::collections::BTreeSet::new();
        for (node_name, pods) in running_by_node {
            let Some(domain) = node_labels
                .get(node_name.as_str())
                .and_then(|labels| labels.get(topology_key))
            else {
                continue;
            };
            if pods.iter().any(|pod| {
                selector_scopes_ns(
                    selector,
                    pending_namespace,
                    &pod.namespace,
                    namespace_labels,
                ) && selector_matches(&selector.reqs, &pod.labels)
            }) {
                matching_domains.insert(domain.clone());
            }
        }
        if matching_domains.is_empty() {
            // Kubernetes allows the first pod in a self-affine group to bootstrap in some cases.
            // Keep this best-effort filter from over-constraining when no existing peer exists.
            continue;
        }
        let Some(candidate_domain) = node_labels
            .get(candidate_node)
            .and_then(|labels| labels.get(topology_key))
        else {
            return false;
        };
        if !matching_domains.contains(candidate_domain) {
            return false;
        }
    }
    true
}

/// Deterministic canonical form of a requirement so gang-member selector sets compare
/// order-insensitively (values sorted within a requirement).
fn canonical_req(r: &LabelSelectorReq) -> (String, String, Vec<String>) {
    let mut vals = r.values.clone();
    vals.sort();
    (r.key.clone(), r.operator.clone(), vals)
}

/// Canonical form of one selector (sorted reqs + sorted namespace scope + canonical
/// namespaceSelector) for order-insensitive gang-member agreement comparison.
type CanonicalReqs = Vec<(String, String, Vec<String>)>;
type CanonicalSelector = (CanonicalReqs, Vec<String>, Option<CanonicalReqs>);
fn canonical_selector(sel: &AntiAffinitySelector) -> CanonicalSelector {
    let mut reqs: CanonicalReqs = sel.reqs.iter().map(canonical_req).collect();
    reqs.sort();
    let mut ns = sel.namespaces.clone();
    ns.sort();
    let ns_sel = sel.namespace_selector.as_ref().map(|rs| {
        let mut r: CanonicalReqs = rs.iter().map(canonical_req).collect();
        r.sort();
        r
    });
    (reqs, ns, ns_sel)
}

/// Canonical form of a selector set for order-insensitive comparison.
fn canonical_selectors(sels: &[AntiAffinitySelector]) -> Vec<CanonicalSelector> {
    let mut out: Vec<CanonicalSelector> = sels.iter().map(canonical_selector).collect();
    out.sort();
    out
}

/// Canonical form of a `(topologyKey, selector)` set for gang-member agreement (Phase 12).
fn canonical_topology_selectors(
    sels: &[(String, AntiAffinitySelector)],
) -> Vec<(String, CanonicalSelector)> {
    let mut out: Vec<(String, CanonicalSelector)> = sels
        .iter()
        .map(|(k, sel)| (k.clone(), canonical_selector(sel)))
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

fn parse_vram_label_bytes(raw: &str) -> i64 {
    let value = raw.trim();
    if value.is_empty() {
        return 0;
    }
    let split = value
        .find(|c: char| !(c.is_ascii_digit() || c == '.'))
        .unwrap_or(value.len());
    let (num, suffix) = value.split_at(split);
    let Ok(parsed) = num.parse::<f64>() else {
        return 0;
    };
    if !parsed.is_finite() || parsed <= 0.0 {
        return 0;
    }
    let multiplier = match suffix.trim().to_ascii_lowercase().as_str() {
        "b" | "bytes" => 1.0,
        "ki" | "kib" => 1024.0,
        "mi" | "mib" => 1024.0 * 1024.0,
        "gi" | "gib" => 1024.0 * 1024.0 * 1024.0,
        "kb" => 1000.0,
        "mb" => 1000.0 * 1000.0,
        "gb" => 1000.0 * 1000.0 * 1000.0,
        "" => {
            if parsed <= 2_048.0 {
                1024.0 * 1024.0 * 1024.0
            } else if parsed < 1_000_000_000.0 {
                1024.0 * 1024.0
            } else {
                1.0
            }
        }
        _ => 0.0,
    };
    (parsed * multiplier).round() as i64
}

pub(crate) fn node_peak_vram_bytes(labels: &BTreeMap<String, String>) -> i64 {
    [
        "ksolver.dev/gpu-vram-bytes",
        "ksolver.dev/gpu-vram-gib",
        "nvidia.com/gpu.memory",
    ]
    .iter()
    .filter_map(|key| labels.get(*key).map(|v| (*key, v.as_str())))
    .find_map(|(key, value)| {
        let bytes = if key.ends_with("-gib") {
            parse_vram_label_bytes(&format!("{value}Gi"))
        } else {
            parse_vram_label_bytes(value)
        };
        (bytes > 0).then_some(bytes)
    })
    .unwrap_or(0)
}

/// The core per-GPU VRAM feasibility criterion, in one place so every caller (the scheduler AND the
/// gang-aware baseline scorer) applies the SAME rule: a pod fits a node's per-GPU VRAM unless its
/// predicted peak strictly exceeds it. Unknown prediction or unknown node capacity ⇒ fits (advisory,
/// never a hard block on a guess).
pub(crate) fn vram_fits(predicted_peak_vram_bytes: i64, node_vram_bytes: i64) -> bool {
    predicted_peak_vram_bytes <= 0
        || node_vram_bytes <= 0
        || predicted_peak_vram_bytes <= node_vram_bytes
}

fn vram_fits_node(
    predicted_peak_vram_bytes: i64,
    node_vram_bytes: i64,
    ext_requests: &BTreeMap<String, i64>,
    is_gpu_resource: &dyn Fn(&str) -> bool,
) -> bool {
    let gpu_units: i64 = ext_requests
        .iter()
        .filter(|(res, _)| is_gpu_resource(res))
        .map(|(_, qty)| *qty)
        .sum();
    gpu_units <= 0 || vram_fits(predicted_peak_vram_bytes, node_vram_bytes)
}

fn vram_rightsizing_score(predicted_peak_vram_bytes: i64, node_vram_bytes: i64) -> i64 {
    if predicted_peak_vram_bytes <= 0 || node_vram_bytes < predicted_peak_vram_bytes {
        return 0;
    }
    let gib = 1024_i64 * 1024 * 1024;
    let excess_gib = (node_vram_bytes - predicted_peak_vram_bytes + gib - 1) / gib;
    (1000 - excess_gib).clamp(1, 1000)
}

fn stable_hash<T: Hash + ?Sized>(v: &T) -> u64 {
    let mut h = std::collections::hash_map::DefaultHasher::new();
    v.hash(&mut h);
    h.finish()
}

fn residual_after_fit_score(
    residual: &Residual,
    requests: &ResourceList,
    ext_requests: &BTreeMap<String, i64>,
) -> (i64, i64, i64, i64, i64) {
    let gpu_left: i64 = ext_requests
        .iter()
        .filter(|(name, _)| name.as_str() == GPU_RESOURCE || name.starts_with("nvidia.com/mig-"))
        .map(|(name, qty)| residual.ext.get(name).copied().unwrap_or(0) - *qty)
        .sum();
    (
        gpu_left.max(0),
        sub_clamp(residual.pods, requests.pods.max(1)),
        sub_clamp(residual.cpu, requests.milli_cpu),
        sub_clamp(residual.mem, requests.memory_bytes),
        sub_clamp(residual.disk, requests.ephemeral_storage),
    )
}

fn prune_candidate_nodes(
    workload_id: &str,
    feasible_nodes: &mut Vec<String>,
    residual: &BTreeMap<String, Residual>,
    running_by_node: &BTreeMap<String, Vec<&NormalizedWorkload>>,
    requests: &ResourceList,
    ext_requests: &BTreeMap<String, i64>,
    limit: usize,
) {
    if limit == 0 || feasible_nodes.len() <= limit {
        return;
    }
    feasible_nodes.sort_by(|a, b| {
        let a_residual = residual.get(a);
        let b_residual = residual.get(b);
        let a_score = a_residual
            .map(|r| residual_after_fit_score(r, requests, ext_requests))
            .unwrap_or((i64::MAX, i64::MAX, i64::MAX, i64::MAX, i64::MAX));
        let b_score = b_residual
            .map(|r| residual_after_fit_score(r, requests, ext_requests))
            .unwrap_or((i64::MAX, i64::MAX, i64::MAX, i64::MAX, i64::MAX));
        let a_active = if running_by_node.contains_key(a) {
            0
        } else {
            1
        };
        let b_active = if running_by_node.contains_key(b) {
            0
        } else {
            1
        };
        (
            a_active,
            a_score,
            stable_hash(&(workload_id, a.as_str())),
            a,
        )
            .cmp(&(
                b_active,
                b_score,
                stable_hash(&(workload_id, b.as_str())),
                b,
            ))
    });
    feasible_nodes.truncate(limit);
    feasible_nodes.sort();
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

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CandidateDiagnostics {
    pub candidate_edges_before_prune: usize,
    pub candidate_edges_after_prune: usize,
    pub pruned_workloads: usize,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct NodeGroupingDiagnostics {
    pub eligible_group_count: usize,
    pub eligible_node_count: usize,
    pub max_group_size: usize,
    pub disabled_reasons: Vec<String>,
}

fn grouping_node_signature(input: &OptimizationInput, node: &OptimizationNode) -> Vec<String> {
    let mut sig = vec![
        format!("pool={}", node.pool),
        format!("price_currency={}", node.price.currency),
        format!("price_monthly={}", node.price.monthly.to_bits()),
        format!("cpu={}", node.effective_capacity.milli_cpu),
        format!("memory={}", node.effective_capacity.memory_bytes),
        format!("disk={}", node.effective_capacity.ephemeral_storage),
        format!("pods={}", node.effective_capacity.pods),
    ];
    for (name, qty) in &node.extended_resources {
        sig.push(format!("resource:{name}={qty}"));
    }
    for workload in &input.workloads {
        let feasible = workload.feasible_nodes.iter().any(|n| n == &node.name);
        let soft_score = workload.soft_scores.get(&node.name).copied().unwrap_or(0);
        let current_count = workload
            .current_counts
            .get(&node.name)
            .copied()
            .unwrap_or(0);
        sig.push(format!(
            "workload:{}:feasible={feasible}:soft={soft_score}:current={current_count}",
            workload.id
        ));
    }
    sig
}

pub fn analyze_node_grouping(input: &OptimizationInput) -> NodeGroupingDiagnostics {
    let mut reasons = BTreeSet::new();
    if input.nodes.iter().any(|n| n.count != 1) {
        reasons.insert("input already contains grouped nodes".to_string());
    }
    if input.workloads.iter().any(|w| w.colocate) {
        reasons.insert("co-located workloads require physical-node identity".to_string());
    }
    if !input.anti_affinity_pairs.is_empty() {
        reasons.insert("anti-affinity constraints require physical-node identity".to_string());
    }
    if !input.soft_coplacement_pairs.is_empty() {
        reasons
            .insert("same-batch preferred co-placement uses physical topology domains".to_string());
    }

    let disabled_reasons: Vec<String> = reasons.into_iter().collect();
    if !disabled_reasons.is_empty() {
        return NodeGroupingDiagnostics {
            disabled_reasons,
            ..Default::default()
        };
    }

    let mut by_signature: BTreeMap<Vec<String>, Vec<String>> = BTreeMap::new();
    for node in &input.nodes {
        by_signature
            .entry(grouping_node_signature(input, node))
            .or_default()
            .push(node.name.clone());
    }

    let eligible_groups: Vec<Vec<String>> = by_signature
        .into_values()
        .filter(|members| members.len() > 1)
        .collect();
    NodeGroupingDiagnostics {
        eligible_group_count: eligible_groups.len(),
        eligible_node_count: eligible_groups.iter().map(Vec::len).sum(),
        max_group_size: eligible_groups.iter().map(Vec::len).max().unwrap_or(0),
        disabled_reasons,
    }
}

pub fn group_pending_input_by_node_symmetry(
    input: &OptimizationInput,
) -> (OptimizationInput, NodeGroupingDiagnostics) {
    let diagnostics = analyze_node_grouping(input);
    if !diagnostics.disabled_reasons.is_empty() || diagnostics.eligible_group_count == 0 {
        return (input.clone(), diagnostics);
    }

    let mut by_signature: BTreeMap<Vec<String>, Vec<OptimizationNode>> = BTreeMap::new();
    for node in &input.nodes {
        by_signature
            .entry(grouping_node_signature(input, node))
            .or_default()
            .push(node.clone());
    }

    let mut physical_to_group = BTreeMap::new();
    let mut grouped_nodes = Vec::new();
    for mut members in by_signature.into_values() {
        members.sort_by(|a, b| a.name.cmp(&b.name));
        if members.len() == 1 {
            let node = members.remove(0);
            physical_to_group.insert(node.name.clone(), node.name.clone());
            grouped_nodes.push(node);
            continue;
        }

        let first = members[0].clone();
        let group_name = format!("node-group-{}", first.name);
        let member_names: Vec<String> = members.iter().map(|n| n.name.clone()).collect();
        for name in &member_names {
            physical_to_group.insert(name.clone(), group_name.clone());
        }
        grouped_nodes.push(OptimizationNode {
            name: group_name,
            count: member_names.len() as i32,
            members: member_names,
            ..first
        });
    }
    grouped_nodes.sort_by(|a, b| a.name.cmp(&b.name));

    let mut grouped = input.clone();
    grouped.nodes = grouped_nodes;
    for workload in &mut grouped.workloads {
        let mut feasible: Vec<String> = workload
            .feasible_nodes
            .iter()
            .filter_map(|name| physical_to_group.get(name).cloned())
            .collect();
        feasible.sort();
        feasible.dedup();
        workload.feasible_nodes = feasible;

        let mut current_counts = std::collections::HashMap::new();
        for (node_name, count) in &workload.current_counts {
            if let Some(group_name) = physical_to_group.get(node_name) {
                *current_counts.entry(group_name.clone()).or_insert(0) += *count;
            }
        }
        workload.current_counts = current_counts;
        if let Some(group_name) = physical_to_group.get(&workload.current_node) {
            workload.current_node = group_name.clone();
        }

        let mut soft_scores: BTreeMap<String, i64> = BTreeMap::new();
        for (node_name, score) in &workload.soft_scores {
            if let Some(group_name) = physical_to_group.get(node_name) {
                soft_scores
                    .entry(group_name.clone())
                    .and_modify(|existing| *existing = (*existing).max(*score))
                    .or_insert(*score);
            }
        }
        workload.soft_scores = soft_scores;
    }

    (grouped, diagnostics)
}

pub fn expand_grouped_solution_to_physical(
    input: &OptimizationInput,
    solution: &OptimizationSolution,
) -> Result<OptimizationSolution, String> {
    let grouped_nodes: Vec<&OptimizationNode> = input
        .nodes
        .iter()
        .filter(|node| node.count > 1 || node.members.len() > 1)
        .collect();
    if grouped_nodes.is_empty() {
        return Ok(solution.clone());
    }

    let mut node_by_name = BTreeMap::new();
    let mut residual = BTreeMap::new();
    for node in &input.nodes {
        node_by_name.insert(node.name.as_str(), node);
        for member in physical_members(node) {
            residual.insert(
                member,
                Residual {
                    cpu: node.effective_capacity.milli_cpu,
                    mem: node.effective_capacity.memory_bytes,
                    disk: node.effective_capacity.ephemeral_storage,
                    pods: node.effective_capacity.pods,
                    ext: node.extended_resources.clone(),
                },
            );
        }
    }

    let mut expanded = solution.clone();
    expanded.assignments.clear();
    expanded.assignment_counts.clear();
    expanded.active_nodes.clear();

    let mut workload_by_id: BTreeMap<&str, &OptimizationWorkload> = BTreeMap::new();
    for workload in &input.workloads {
        workload_by_id.insert(workload.id.as_str(), workload);
    }

    let mut units = Vec::new();
    for (workload_id, counts) in &solution.assignment_counts {
        let workload = workload_by_id
            .get(workload_id.as_str())
            .ok_or_else(|| format!("solution references unknown workload {workload_id}"))?;
        for (node_name, count) in counts {
            if *count <= 0 {
                continue;
            }
            let node = node_by_name
                .get(node_name.as_str())
                .ok_or_else(|| format!("solution references unknown node {node_name}"))?;
            for _ in 0..*count {
                units.push((
                    workload_id.clone(),
                    (*workload).clone(),
                    physical_members(node),
                ));
            }
        }
    }
    units.sort_by(|a, b| {
        let a_gpu = gpu_request_sum(&a.1.extended_resource_requests);
        let b_gpu = gpu_request_sum(&b.1.extended_resource_requests);
        b_gpu
            .cmp(&a_gpu)
            .then_with(|| b.1.requests.memory_bytes.cmp(&a.1.requests.memory_bytes))
            .then_with(|| b.1.requests.milli_cpu.cmp(&a.1.requests.milli_cpu))
            .then_with(|| a.0.cmp(&b.0))
    });

    for (workload_id, workload, members) in units {
        let mut placed = None;
        for member in members {
            if let Some(r) = residual.get_mut(&member) {
                if residual_fits(r, &workload.requests, &workload.extended_resource_requests) {
                    consume_residual(r, &workload.requests, &workload.extended_resource_requests);
                    placed = Some(member);
                    break;
                }
            }
        }
        let Some(member) = placed else {
            return Err(format!(
                "grouped assignment for workload {workload_id} could not be expanded to a physical node"
            ));
        };
        expanded
            .assignments
            .entry(workload_id.clone())
            .or_insert_with(|| member.clone());
        *expanded
            .assignment_counts
            .entry(workload_id)
            .or_default()
            .entry(member)
            .or_insert(0) += 1;
    }

    for (node_name, r) in &residual {
        let Some(original) = input
            .nodes
            .iter()
            .find(|n| n.name == *node_name || n.members.iter().any(|m| m == node_name))
        else {
            continue;
        };
        let used = original.effective_capacity.pods.saturating_sub(r.pods);
        if used > 0 {
            expanded.active_nodes.insert(node_name.clone(), 1);
        }
    }

    Ok(expanded)
}

fn physical_members(node: &OptimizationNode) -> Vec<String> {
    if node.members.is_empty() {
        vec![node.name.clone()]
    } else {
        let mut members = node.members.clone();
        members.sort();
        members
    }
}

fn gpu_request_sum(ext: &BTreeMap<String, i64>) -> i64 {
    ext.iter()
        .filter(|(name, _)| name.as_str() == GPU_RESOURCE || name.starts_with("nvidia.com/mig-"))
        .map(|(_, qty)| *qty)
        .sum()
}

fn residual_fits(
    residual: &Residual,
    requests: &ResourceList,
    ext_requests: &BTreeMap<String, i64>,
) -> bool {
    residual.pods >= requests.pods.max(1)
        && residual.cpu >= requests.milli_cpu
        && residual.mem >= requests.memory_bytes
        && residual.disk >= requests.ephemeral_storage
        && ext_requests
            .iter()
            .all(|(name, qty)| residual.ext.get(name).copied().unwrap_or(0) >= *qty)
}

fn consume_residual(
    residual: &mut Residual,
    requests: &ResourceList,
    ext_requests: &BTreeMap<String, i64>,
) {
    residual.pods = sub_clamp(residual.pods, requests.pods.max(1));
    residual.cpu = sub_clamp(residual.cpu, requests.milli_cpu);
    residual.mem = sub_clamp(residual.mem, requests.memory_bytes);
    residual.disk = sub_clamp(residual.disk, requests.ephemeral_storage);
    for (name, qty) in ext_requests {
        let entry = residual.ext.entry(name.clone()).or_default();
        *entry = sub_clamp(*entry, *qty);
    }
}

/// Build an optimization input that places ONLY the pending pods (see
/// `build_pending_input_diagnosed`); returns just the input for callers that don't need the
/// drop diagnostics (preserves the original signature — zero ripple).
pub fn build_pending_input(
    cluster: &NormalizedCluster,
    pending: &[PendingGpuPod],
    quotas: &BTreeMap<String, i64>,
) -> OptimizationInput {
    build_pending_input_with_candidate_limit(cluster, pending, quotas, 0)
}

pub fn build_pending_input_with_candidate_limit(
    cluster: &NormalizedCluster,
    pending: &[PendingGpuPod],
    quotas: &BTreeMap<String, i64>,
    candidate_node_limit: usize,
) -> OptimizationInput {
    // Default GPU-resource matcher follows the shadow scheduler contract: whole GPUs plus
    // NVIDIA MIG mixed-strategy slice resources. Callers needing a custom resource policy use
    // the diagnosed builder and pass their own matcher.
    build_pending_input_diagnosed_with_candidate_limit(
        cluster,
        pending,
        quotas,
        &default_is_gpu_resource,
        candidate_node_limit,
    )
    .0
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
    is_gpu_resource: &dyn Fn(&str) -> bool,
) -> (OptimizationInput, Vec<DropInfo>) {
    build_pending_input_diagnosed_with_candidate_limit(cluster, pending, quotas, is_gpu_resource, 0)
}

pub fn build_pending_input_diagnosed_with_candidate_limit(
    cluster: &NormalizedCluster,
    pending: &[PendingGpuPod],
    quotas: &BTreeMap<String, i64>,
    is_gpu_resource: &dyn Fn(&str) -> bool,
    candidate_node_limit: usize,
) -> (OptimizationInput, Vec<DropInfo>) {
    let (input, drops, _) = build_pending_input_diagnosed_with_candidate_limit_and_stats(
        cluster,
        pending,
        quotas,
        is_gpu_resource,
        candidate_node_limit,
    );
    (input, drops)
}

pub fn build_pending_input_diagnosed_with_candidate_limit_and_stats(
    cluster: &NormalizedCluster,
    pending: &[PendingGpuPod],
    quotas: &BTreeMap<String, i64>,
    is_gpu_resource: &dyn Fn(&str) -> bool,
    candidate_node_limit: usize,
) -> (OptimizationInput, Vec<DropInfo>, CandidateDiagnostics) {
    // 1. Accumulate running usage per node (running = current_node non-empty). In the same
    //    pass, sum each namespace's running GPU usage so quotas count existing consumption
    //    (computed here, not in a second loop, to avoid drift from the residual math).
    let mut used_cpu: BTreeMap<String, i64> = BTreeMap::new();
    let mut used_mem: BTreeMap<String, i64> = BTreeMap::new();
    let mut used_disk: BTreeMap<String, i64> = BTreeMap::new();
    let mut used_pods: BTreeMap<String, i64> = BTreeMap::new();
    let mut used_ext: BTreeMap<String, BTreeMap<String, i64>> = BTreeMap::new();
    let mut running_gpu_by_ns: BTreeMap<String, i64> = BTreeMap::new();
    // Set of GPU resource names seen anywhere (whole GPUs + MIG slices), for quota groups.
    let mut gpu_resource_set: std::collections::BTreeSet<String> =
        std::collections::BTreeSet::new();
    for w in &cluster.workloads {
        // Collect GPU resource names from every workload (running or pending) for quota scope.
        for res in w.extended_resource_requests.keys() {
            if is_gpu_resource(res) {
                gpu_resource_set.insert(res.clone());
            }
        }
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
        // MIG-aware quota: sum ALL GPU resources (whole + slices), each unit = 1.
        let running_gpu: i64 = w
            .extended_resource_requests
            .iter()
            .filter(|(res, _)| is_gpu_resource(res))
            .map(|(_, qty)| *qty)
            .sum();
        if running_gpu > 0 {
            *running_gpu_by_ns.entry(w.namespace.clone()).or_default() += running_gpu;
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
    let node_vram_bytes: BTreeMap<&str, i64> = cluster
        .nodes
        .iter()
        .map(|n| (n.name.as_str(), node_peak_vram_bytes(&n.labels)))
        .collect();
    // Topology domain value of a node for a topology key: Some(value) iff the node carries
    // that label. A node without the label is its own singleton domain (never equal to a
    // present value), so it is never excluded by domain equality.
    let domain = |node: &str, key: &str| -> Option<String> {
        node_labels.get(node).and_then(|l| l.get(key).cloned())
    };
    // Namespace labels for namespaceSelector-scoped anti-affinity (F-CNS-2).
    let ns_labels = &cluster.namespace_labels;

    // 4. Group pending pods into gangs (only unbound pods; a stale pod already bound was
    //    subtracted above and must not be a decision variable).
    let mut gangs: BTreeMap<String, Vec<&PendingGpuPod>> = BTreeMap::new();
    for p in pending {
        gangs.entry(gang_id(p)).or_default().push(p);
    }

    // 5. Build one workload per feasible, homogeneous gang.
    let mut workloads = Vec::new();
    let mut dropped: Vec<DropInfo> = Vec::new();
    let mut candidate_diagnostics = CandidateDiagnostics::default();
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
        Vec<AntiAffinitySelector>,
        Vec<BTreeMap<String, String>>,
    );
    let mut emitted_meta: Vec<EmittedMeta> = Vec::new();
    // (id, namespace, feasible_nodes, member_labels, agreed preferred-affinity terms) per emitted
    // workload, for co-placement preferred-affinity pairing.
    type EmittedPref = (
        String,
        String,
        Vec<String>,
        Vec<BTreeMap<String, String>>,
        Vec<crate::model::PreferredPodTerm>,
    );
    let mut emitted_pref: Vec<EmittedPref> = Vec::new();
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
        if members.iter().any(|m| pending_requires_unmodeled_dra(m))
            && member_workloads
                .iter()
                .any(|w| !has_modeled_gpu_or_dra_demand(w, is_gpu_resource))
        {
            dropped.push(DropInfo {
                pod_scopes: scopes(&members),
                reason: "DRA device demand was not modeled; refusing to treat pod as zero-GPU work"
                    .to_string(),
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
        let required_gpu_topology = &members[0].required_gpu_topology;
        if members
            .iter()
            .any(|m| m.required_gpu_topology != *required_gpu_topology)
        {
            dropped.push(DropInfo {
                pod_scopes: scopes(&members),
                reason: "gang members disagree on required GPU topology".to_string(),
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
        let affinity_selectors = if members
            .iter()
            .all(|m| m.affinity_topology_selectors == members[0].affinity_topology_selectors)
        {
            members[0].affinity_topology_selectors.as_slice()
        } else {
            &[]
        };
        // Self-anti-affine: a modeled selector matches EVERY member's own labels, so the
        // gang's replicas must spread (<=1 per node). Requires >1 member. Matching all
        // members (not just the representative) is required because gang homogeneity does
        // not include labels.
        // A self-anti-affine selector must also APPLY to the gang's own namespace (all members
        // share rep.namespace); a selector scoped only to other namespaces does not self-spread.
        let self_anti = members.len() > 1
            && aa_selectors.iter().any(|s| {
                selector_scopes_ns(s, &rep.namespace, &rep.namespace, ns_labels)
                    && member_workloads
                        .iter()
                        .all(|w| selector_matches(&s.reqs, &w.labels))
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
        let priority = members.iter().map(|m| m.priority).max().unwrap_or(0);
        let priority_class_name = members
            .iter()
            .filter_map(|m| m.priority_class_name.as_deref())
            .find(|v| !v.is_empty())
            .unwrap_or_default()
            .to_string();
        let team = members
            .iter()
            .filter_map(|m| m.team.as_deref())
            .find(|v| !v.is_empty())
            .unwrap_or_default()
            .to_string();
        let queue = members
            .iter()
            .filter_map(|m| m.queue.as_deref())
            .find(|v| !v.is_empty())
            .unwrap_or_default()
            .to_string();
        let business_value = members.iter().map(|m| m.business_value).max().unwrap_or(0);
        let queue_wait_seconds = members
            .iter()
            .map(|m| m.queue_wait_seconds)
            .max()
            .unwrap_or(0);
        let deadline_unix_seconds = members
            .iter()
            .filter_map(|m| (m.deadline_unix_seconds > 0).then_some(m.deadline_unix_seconds))
            .min()
            .unwrap_or(0);
        let min_gpus = members.iter().map(|m| m.min_gpus).max().unwrap_or(0);
        let max_gpus = members
            .iter()
            .filter_map(|m| (m.max_gpus > 0).then_some(m.max_gpus))
            .min()
            .unwrap_or(0);
        let preferred_gpus = members.iter().map(|m| m.preferred_gpus).max().unwrap_or(0);
        let flexible = members.iter().any(|m| m.flexible);
        let predicted_runtime_seconds = members
            .iter()
            .map(|m| m.predicted_runtime_seconds)
            .max()
            .unwrap_or(0);
        let predicted_peak_vram_bytes = members
            .iter()
            .map(|m| m.predicted_peak_vram_bytes)
            .max()
            .unwrap_or(0);
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
        let topology_spread_rules = if member_workloads
            .iter()
            .all(|w| w.topology_spread_rules == rep.topology_spread_rules)
        {
            rep.topology_spread_rules.as_slice()
        } else {
            &[]
        };
        let topology_spread_added_pods = if colocate { n } else { 1 };
        let mut feasible_nodes: Vec<String> = rep
            .feasible_node_names
            .iter()
            .filter(|node| {
                residual
                    .get(*node)
                    .map(|r| r.fits(&fit_req, &fit_ext))
                    .unwrap_or(false)
            })
            // Explicit GPU locality hints: require candidate nodes to carry the requested
            // topology labels (for example an NVLink/NVSwitch island label). This is a
            // deterministic hard filter layered on top of Kubernetes' scalar feasibility.
            .filter(|node| {
                if required_gpu_topology.is_empty() {
                    return true;
                }
                let Some(labels) = node_labels.get(node.as_str()) else {
                    return false;
                };
                required_gpu_topology
                    .iter()
                    .all(|(key, value)| labels.get(key).map(|v| v == value).unwrap_or(false))
            })
            // If node GPU VRAM capacity is known, exclude candidates whose per-GPU predicted peak
            // VRAM cannot fit. Unknown node VRAM remains eligible to avoid false negatives on
            // clusters that do not expose NVIDIA GPU memory labels.
            .filter(|node| {
                vram_fits_node(
                    predicted_peak_vram_bytes,
                    node_vram_bytes.get(node.as_str()).copied().unwrap_or(0),
                    &fit_ext,
                    is_gpu_resource,
                )
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
                    // Forward: the pending pod's selector (owned by rep.namespace) applies to the
                    // running pod's namespace AND its reqs match the running pod's labels.
                    let forward = aa_selectors.iter().any(|s| {
                        selector_scopes_ns(s, &rep.namespace, &w.namespace, ns_labels)
                            && selector_matches(&s.reqs, &w.labels)
                    });
                    // Symmetric: the running pod's selector (owned by w.namespace) applies to the
                    // pending pod's namespace AND its reqs match EVERY pending member's labels.
                    let symmetric = w.anti_affinity_host_selectors.iter().any(|rs| {
                        selector_scopes_ns(rs, &w.namespace, &rep.namespace, ns_labels)
                            && member_labels
                                .iter()
                                .all(|ml| selector_matches(&rs.reqs, ml))
                    });
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
                    && !running_by_node
                        .values()
                        .flatten()
                        .any(|w| !w.anti_affinity_topology_selectors.is_empty())
                {
                    return true;
                }
                let violates = running_by_node.iter().any(|(rn, pods)| {
                    pods.iter().any(|w| {
                        // Forward: pending topology selector (owned by rep.namespace) applies to
                        // the running pod's namespace, matches its labels, same domain.
                        let forward = aa_topo_selectors.iter().any(|(key, s)| {
                            selector_scopes_ns(s, &rep.namespace, &w.namespace, ns_labels)
                                && selector_matches(&s.reqs, &w.labels)
                                && domain(cn, key).is_some()
                                && domain(cn, key) == domain(rn, key)
                        });
                        // Symmetric: running pod's topology selector (owned by w.namespace) applies
                        // to the pending pod's namespace, matches ALL members, same domain.
                        let symmetric =
                            w.anti_affinity_topology_selectors.iter().any(|(key, rs)| {
                                selector_scopes_ns(rs, &w.namespace, &rep.namespace, ns_labels)
                                    && member_labels
                                        .iter()
                                        .all(|ml| selector_matches(&rs.reqs, ml))
                                    && domain(cn, key).is_some()
                                    && domain(cn, key) == domain(rn, key)
                            });
                        forward || symmetric
                    })
                });
                !violates
            })
            // Best-effort required pod-affinity: if a modeled term has matching already-running
            // pods, the candidate node must share that term's topology domain with at least one of
            // them. Terms with no existing match are skipped to avoid over-constraining Kubernetes'
            // self-affinity bootstrap case; the pod-affinity caveat remains.
            .filter(|node| {
                pod_affinity_allows_node(
                    node,
                    &rep.namespace,
                    affinity_selectors,
                    &running_by_node,
                    &node_labels,
                    ns_labels,
                )
            })
            // Best-effort hard topology-spread filtering for the modeled subset: DoNotSchedule +
            // supported label selector + same namespace + node label present for the topology key.
            // This accounts for already-running pods only; same-batch spread remains broader than
            // this local feasibility filter and unsupported shapes are still disclosed by caveats.
            .filter(|node| {
                topology_spread_allows_node(
                    node,
                    &rep.namespace,
                    &member_labels,
                    topology_spread_rules,
                    topology_spread_added_pods,
                    &running_by_node,
                    &node_labels,
                )
            })
            .cloned()
            .collect();
        if feasible_nodes.is_empty() {
            let has_capacity_without_vram = rep.feasible_node_names.iter().any(|node| {
                residual
                    .get(node)
                    .map(|r| r.fits(&fit_req, &fit_ext))
                    .unwrap_or(false)
            });
            let has_capacity_and_topology = rep.feasible_node_names.iter().any(|node| {
                residual
                    .get(node)
                    .map(|r| r.fits(&fit_req, &fit_ext))
                    .unwrap_or(false)
                    && required_gpu_topology.iter().all(|(key, value)| {
                        node_labels
                            .get(node.as_str())
                            .and_then(|labels| labels.get(key))
                            .map(|v| v == value)
                            .unwrap_or(false)
                    })
            });
            dropped.push(DropInfo {
                pod_scopes: scopes(&members),
                reason: if !required_gpu_topology.is_empty() && !has_capacity_and_topology {
                    format!(
                        "no feasible node (required GPU topology label {} not present on any residual-capacity candidate)",
                        required_gpu_topology
                            .iter()
                            .map(|(k, v)| format!("{k}={v}"))
                            .collect::<Vec<_>>()
                            .join(",")
                    )
                } else if predicted_peak_vram_bytes > 0 && has_capacity_without_vram {
                    "no feasible node (predicted peak VRAM exceeds known node GPU memory)"
                        .to_string()
                } else {
                    "no feasible node (insufficient residual capacity or excluded by anti-affinity)"
                        .to_string()
                },
            });
            continue;
        }
        let candidate_edges_before_prune = feasible_nodes.len();
        prune_candidate_nodes(
            &id,
            &mut feasible_nodes,
            &residual,
            &running_by_node,
            &fit_req,
            &fit_ext,
            candidate_node_limit,
        );
        candidate_diagnostics.candidate_edges_before_prune += candidate_edges_before_prune;
        candidate_diagnostics.candidate_edges_after_prune += feasible_nodes.len();
        if feasible_nodes.len() < candidate_edges_before_prune {
            candidate_diagnostics.pruned_workloads += 1;
        }
        // Soft (preferred) node-affinity scores per feasible node: Σ weight of the gang's preferred
        // terms whose expressions ALL match the node's labels. Requires gang-member agreement on
        // preferred terms (else no soft scores — soft is best-effort, so we drop scores not the gang).
        let mut soft_scores: BTreeMap<String, i64> = BTreeMap::new();
        // VRAM right-sizing score: after Phase 1 pins admission/cost, prefer the smallest known
        // per-GPU memory capacity that still fits the predicted peak. Unknown memory remains
        // neutral; too-small known nodes were already filtered out above.
        if predicted_peak_vram_bytes > 0 {
            for node_name in &feasible_nodes {
                let score = vram_rightsizing_score(
                    predicted_peak_vram_bytes,
                    node_vram_bytes
                        .get(node_name.as_str())
                        .copied()
                        .unwrap_or(0),
                );
                if score != 0 {
                    *soft_scores.entry(node_name.clone()).or_default() += score;
                }
            }
        }
        let preferred = &members[0].preferred_node_affinity;
        let preferred_agree = members
            .iter()
            .all(|m| m.preferred_node_affinity == *preferred);
        if preferred_agree && !preferred.is_empty() {
            for node_name in &feasible_nodes {
                let empty = BTreeMap::new();
                let labels = node_labels
                    .get(node_name.as_str())
                    .copied()
                    .unwrap_or(&empty);
                let score: i64 = preferred
                    .iter()
                    .filter(|t| {
                        t.exprs
                            .iter()
                            .all(|e| crate::normalizer::node_affinity_expr_matches(labels, e))
                            && t.fields.iter().all(|f| {
                                crate::normalizer::node_affinity_field_matches(node_name, f)
                            })
                    })
                    .map(|t| t.weight)
                    .sum();
                if score != 0 {
                    *soft_scores.entry(node_name.clone()).or_default() += score;
                }
            }
        }
        // Preferred pod (anti-)affinity: forward-only, domain-aware, label-based for ALL topology
        // keys (incl. kubernetes.io/hostname). A candidate node accumulates +weight (affinity) /
        // -weight (anti-affinity) for EACH matching running pod sharing the candidate's topology
        // domain (node.labels[topologyKey]); kube's interpodaffinity scoring sums per matching pod.
        // A node lacking the topology label earns no score; a running pod on a node lacking it
        // contributes none. Requires gang-member agreement on the term list (else no scores).
        // Best-effort — NOT full kube-scheduler score parity (co-placement between two pending
        // pods remains deferred; symmetry via running pods' preferred terms is handled below).
        let pref_pod = &members[0].preferred_pod_affinity;
        let pref_pod_agree = members
            .iter()
            .all(|m| m.preferred_pod_affinity == *pref_pod);
        if pref_pod_agree {
            for cn in &feasible_nodes {
                for term in pref_pod {
                    let Some(cand_domain) = domain(cn, &term.topology_key) else {
                        continue; // candidate node has no such topology domain -> no score
                    };
                    let delta = if term.anti { -term.weight } else { term.weight };
                    for (rn, pods) in &running_by_node {
                        if domain(rn, &term.topology_key).as_deref() != Some(cand_domain.as_str()) {
                            continue;
                        }
                        for w in pods {
                            if selector_scopes_ns(
                                &term.selector,
                                &rep.namespace,
                                &w.namespace,
                                ns_labels,
                            ) && selector_matches(&term.selector.reqs, &w.labels)
                            {
                                *soft_scores.entry(cn.clone()).or_default() += delta;
                            }
                        }
                    }
                }
            }
        }
        // Symmetric preferred pod (anti-)affinity: a RUNNING pod's own preferred term steers the
        // pending pod (soft mirror of required-symmetry 5h). For each running pod w on node rn whose
        // term's selector scopes to the pending namespace and matches EVERY pending member's labels,
        // a candidate node cn sharing rn's topology domain accumulates +weight (affinity) / -weight
        // (anti-affinity). Runs independently of the pending pod's own preferred terms/agreement.
        for cn in &feasible_nodes {
            for (rn, pods) in &running_by_node {
                for w in pods {
                    for term in &w.preferred_pod_affinity {
                        let (Some(cd), Some(rd)) = (
                            domain(cn, &term.topology_key),
                            domain(rn, &term.topology_key),
                        ) else {
                            continue;
                        };
                        if cd != rd {
                            continue;
                        }
                        if selector_scopes_ns(
                            &term.selector,
                            &w.namespace,
                            &rep.namespace,
                            ns_labels,
                        ) && member_labels
                            .iter()
                            .all(|ml| selector_matches(&term.selector.reqs, ml))
                        {
                            let delta = if term.anti { -term.weight } else { term.weight };
                            *soft_scores.entry(cn.clone()).or_default() += delta;
                        }
                    }
                }
            }
        }
        soft_scores.retain(|_, v| *v != 0);
        // Record co-placement metadata BEFORE `feasible_nodes`/`id` are moved into the workload.
        emitted_pref.push((
            id.clone(),
            rep.namespace.clone(),
            feasible_nodes.clone(),
            member_workloads.iter().map(|w| w.labels.clone()).collect(),
            if pref_pod_agree {
                pref_pod.clone()
            } else {
                Vec::new()
            },
        ));
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
            priority,
            priority_class_name,
            team,
            queue,
            business_value,
            queue_wait_seconds,
            deadline_unix_seconds,
            min_gpus,
            max_gpus,
            preferred_gpus,
            flexible,
            predicted_runtime_seconds,
            predicted_peak_vram_bytes,
            feasible_nodes,
            colocate,
            soft_scores,
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

    // Cross-workload same-batch anti-affinity: at most one of two distinct workloads per node when
    // one's selector applies to the other's namespace and matches ALL its member labels. The
    // namespace scope (empty = own ns) generalizes the former same-namespace-only guard.
    for i in 0..emitted_meta.len() {
        for j in (i + 1)..emitted_meta.len() {
            let (a, b) = (&emitted_meta[i], &emitted_meta[j]);
            let a_forbids_b = a.2.iter().any(|s| {
                selector_scopes_ns(s, &a.1, &b.1, ns_labels)
                    && b.3.iter().all(|l| selector_matches(&s.reqs, l))
            });
            let b_forbids_a = b.2.iter().any(|s| {
                selector_scopes_ns(s, &b.1, &a.1, ns_labels)
                    && a.3.iter().all(|l| selector_matches(&s.reqs, l))
            });
            if a_forbids_b || b_forbids_a {
                anti_affinity_pairs.push((a.0.clone(), b.0.clone()));
            }
        }
    }

    // Per-namespace GPU quota groups: for each configured namespace, cap the total GPUs of
    // its admitted pending workloads at (configured cap - already-running GPUs), clamped ≥0.
    // Only emit a group when that namespace actually has pending workloads to constrain.
    let mut quota_groups: Vec<QuotaGroup> = Vec::new();
    // GPU resource names counted toward every namespace quota (whole GPUs + MIG slices). Always
    // include the whole-GPU name so a quota is meaningful even if only slices were observed.
    let mut quota_resources: Vec<String> = gpu_resource_set.iter().cloned().collect();
    if !quota_resources.iter().any(|r| r == GPU_RESOURCE) {
        quota_resources.push(GPU_RESOURCE.to_string());
    }
    quota_resources.sort();
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
            resources: quota_resources.clone(),
            limit: remaining,
        });
    }

    // Soft co-placement (preferred pod AFFINITY between two PENDING workloads). Beyond kube (which
    // scores only vs running pods): jointly reward co-placing two pending pods that prefer each
    // other. For each ordered pair (i,j), workload i's AGREED preferred-affinity term (anti=false)
    // whose selector scopes to j's namespace and matches ALL of j's member labels rewards sharing a
    // topology domain. Both directions emitted separately (kube sums directions). Applied only in
    // the Phase-2 soft pass, so admission/cost are unaffected.
    let mut soft_coplacement_pairs: Vec<crate::model::SoftCoplacement> = Vec::new();
    for i in 0..emitted_pref.len() {
        for j in 0..emitted_pref.len() {
            if i == j {
                continue;
            }
            let (id_i, ns_i, feas_i, _labels_i, terms_i) = &emitted_pref[i];
            let (id_j, ns_j, feas_j, labels_j, _terms_j) = &emitted_pref[j];
            for term in terms_i {
                if term.anti {
                    continue; // co-placement rewards affinity only (anti is out of scope)
                }
                if !selector_scopes_ns(&term.selector, ns_i, ns_j, ns_labels) {
                    continue;
                }
                if !labels_j
                    .iter()
                    .all(|ml| selector_matches(&term.selector.reqs, ml))
                {
                    continue;
                }
                // Group i's and j's feasible nodes by topology domain value (skip nodes lacking it).
                let mut by_domain: BTreeMap<String, (Vec<String>, Vec<String>)> = BTreeMap::new();
                for nn in feas_i {
                    if let Some(d) = domain(nn, &term.topology_key) {
                        by_domain.entry(d).or_default().0.push(nn.clone());
                    }
                }
                for nn in feas_j {
                    if let Some(d) = domain(nn, &term.topology_key) {
                        by_domain.entry(d).or_default().1.push(nn.clone());
                    }
                }
                let domains: Vec<crate::model::CoplacementDomain> = by_domain
                    .into_values()
                    .filter(|(a, b)| !a.is_empty() && !b.is_empty())
                    .map(|(a_nodes, b_nodes)| crate::model::CoplacementDomain { a_nodes, b_nodes })
                    .collect();
                if !domains.is_empty() {
                    soft_coplacement_pairs.push(crate::model::SoftCoplacement {
                        a: id_i.clone(),
                        b: id_j.clone(),
                        weight: term.weight,
                        domains,
                    });
                }
            }
        }
    }

    (
        OptimizationInput {
            nodes,
            workloads,
            anti_affinity_pairs,
            quota_groups,
            budget_groups: Vec::new(),
            soft_coplacement_pairs,
        },
        dropped,
        candidate_diagnostics,
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
            priority: 0,
            priority_class_name: None,
            team: None,
            queue: None,
            business_value: 0,
            queue_wait_seconds: 0,
            deadline_unix_seconds: 0,
            min_gpus: 0,
            max_gpus: 0,
            preferred_gpus: 0,
            flexible: false,
            predicted_runtime_seconds: 0,
            predicted_peak_vram_bytes: 0,
            required_gpu_topology: vec![],
            gang_key: gang.map(|g| format!("{ns}/{g}")),
            colocate,
            unmodeled_constraints: vec![],
            anti_affinity_host_selectors: vec![],
            affinity_topology_selectors: vec![],
            anti_affinity_topology_selectors: vec![],
            preferred_node_affinity: vec![],
            preferred_pod_affinity: vec![],
        }
    }

    fn ppod_dra(ns: &str, name: &str) -> PendingGpuPod {
        let mut p = ppod(ns, name, None);
        p.gpu_request = 0;
        p.unmodeled_constraints = vec!["DRA: device demand modeled as scalar approximation".into()];
        p
    }

    #[test]
    fn candidate_diagnostics_count_pruned_edges() {
        let cluster = NormalizedCluster {
            nodes: vec![
                node("n1", 8_000, 32, 10, 1),
                node("n2", 8_000, 32, 10, 1),
                node("n3", 8_000, 32, 10, 1),
            ],
            workloads: vec![workload("team", "p0", "", 1_000, 1, 1, &["n1", "n2", "n3"])],
            ..Default::default()
        };
        let pending = vec![ppod("team", "p0", None)];
        let (input, drops, diag) = build_pending_input_diagnosed_with_candidate_limit_and_stats(
            &cluster,
            &pending,
            &BTreeMap::new(),
            &|n| n == GPU_RESOURCE,
            2,
        );

        assert!(drops.is_empty());
        assert_eq!(input.workloads.len(), 1);
        assert_eq!(input.workloads[0].feasible_nodes.len(), 2);
        assert_eq!(diag.candidate_edges_before_prune, 3);
        assert_eq!(diag.candidate_edges_after_prune, 2);
        assert_eq!(diag.pruned_workloads, 1);
    }

    #[test]
    fn unmodeled_dra_pod_is_dropped_instead_of_treated_as_free_work() {
        let cluster = NormalizedCluster {
            nodes: vec![node("n1", 8_000, 32, 10, 1)],
            workloads: vec![workload("team", "dra", "", 1_000, 1, 0, &["n1"])],
            ..Default::default()
        };

        let (input, drops) = super::build_pending_input_diagnosed(
            &cluster,
            &[ppod_dra("team", "dra")],
            &BTreeMap::new(),
            &|name| name == "nvidia.com/gpu",
        );

        assert!(input.workloads.is_empty());
        assert_eq!(drops.len(), 1);
        assert!(drops[0]
            .reason
            .contains("DRA device demand was not modeled"));
    }

    #[test]
    fn modeled_dra_resource_request_is_not_dropped_as_free_work() {
        let mut node = node("n1", 8_000, 32, 10, 0);
        node.extended_resources
            .insert("dra.ksolver/gpu.example.com".to_string(), 1);
        let mut workload = workload("team", "dra", "", 1_000, 1, 0, &["n1"]);
        workload
            .extended_resource_requests
            .insert("dra.ksolver/gpu.example.com".to_string(), 1);
        let cluster = NormalizedCluster {
            nodes: vec![node],
            workloads: vec![workload],
            ..Default::default()
        };

        let (input, drops) = super::build_pending_input_diagnosed(
            &cluster,
            &[ppod_dra("team", "dra")],
            &BTreeMap::new(),
            &|name| name == "nvidia.com/gpu",
        );

        assert!(drops.is_empty());
        assert_eq!(input.workloads.len(), 1);
        assert_eq!(
            input.workloads[0]
                .extended_resource_requests
                .get("dra.ksolver/gpu.example.com"),
            Some(&1)
        );
    }

    #[test]
    fn grouping_analysis_finds_homogeneous_node_pool() {
        let cluster = NormalizedCluster {
            nodes: vec![
                node("n1", 8_000, 32, 10, 1),
                node("n2", 8_000, 32, 10, 1),
                node("n3", 8_000, 32, 10, 1),
            ],
            workloads: vec![workload("team", "p0", "", 1_000, 1, 1, &["n1", "n2", "n3"])],
            ..Default::default()
        };
        let input = build_pending_input(&cluster, &[ppod("team", "p0", None)]);

        let grouping = analyze_node_grouping(&input);

        assert!(grouping.disabled_reasons.is_empty());
        assert_eq!(grouping.eligible_group_count, 1);
        assert_eq!(grouping.eligible_node_count, 3);
        assert_eq!(grouping.max_group_size, 3);
    }

    #[test]
    fn grouping_analysis_separates_nodes_with_different_soft_scores() {
        let cluster = NormalizedCluster {
            nodes: vec![
                node("n1", 8_000, 32, 10, 1),
                node("n2", 8_000, 32, 10, 1),
                node("n3", 8_000, 32, 10, 1),
            ],
            workloads: vec![workload("team", "p0", "", 1_000, 1, 1, &["n1", "n2", "n3"])],
            ..Default::default()
        };
        let mut input = build_pending_input(&cluster, &[ppod("team", "p0", None)]);
        input.workloads[0].soft_scores.insert("n1".to_string(), 10);

        let grouping = analyze_node_grouping(&input);

        assert!(grouping.disabled_reasons.is_empty());
        assert_eq!(grouping.eligible_group_count, 1);
        assert_eq!(grouping.eligible_node_count, 2);
        assert_eq!(grouping.max_group_size, 2);
    }

    #[test]
    fn grouping_analysis_disables_for_colocation() {
        let cluster = NormalizedCluster {
            nodes: vec![node("n1", 8_000, 32, 10, 4), node("n2", 8_000, 32, 10, 4)],
            workloads: vec![
                workload("team", "m0", "", 1_000, 1, 1, &["n1", "n2"]),
                workload("team", "m1", "", 1_000, 1, 1, &["n1", "n2"]),
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

        let grouping = analyze_node_grouping(&input);

        assert_eq!(grouping.eligible_group_count, 0);
        assert!(grouping
            .disabled_reasons
            .contains(&"co-located workloads require physical-node identity".to_string()));
    }

    #[test]
    fn node_grouping_collapses_safe_homogeneous_nodes() {
        let cluster = NormalizedCluster {
            nodes: vec![
                node("n1", 8_000, 32, 10, 1),
                node("n2", 8_000, 32, 10, 1),
                node("n3", 8_000, 32, 10, 1),
            ],
            workloads: vec![workload("team", "p0", "", 1_000, 1, 1, &["n1", "n2", "n3"])],
            ..Default::default()
        };
        let input = build_pending_input(&cluster, &[ppod("team", "p0", None)]);

        let (grouped, diagnostics) = group_pending_input_by_node_symmetry(&input);

        assert!(diagnostics.disabled_reasons.is_empty());
        assert_eq!(diagnostics.eligible_group_count, 1);
        assert_eq!(grouped.nodes.len(), 1);
        assert_eq!(grouped.nodes[0].count, 3);
        assert_eq!(grouped.nodes[0].members, vec!["n1", "n2", "n3"]);
        assert_eq!(
            grouped.workloads[0].feasible_nodes,
            vec!["node-group-n1".to_string()]
        );
    }

    #[test]
    fn grouped_solution_expands_to_physical_nodes_when_packable() {
        let cluster = NormalizedCluster {
            nodes: vec![node("n1", 8_000, 32, 10, 1), node("n2", 8_000, 32, 10, 1)],
            workloads: vec![
                workload("team", "p0", "", 1_000, 1, 1, &["n1", "n2"]),
                workload("team", "p1", "", 1_000, 1, 1, &["n1", "n2"]),
            ],
            ..Default::default()
        };
        let input = build_pending_input(
            &cluster,
            &[ppod("team", "p0", None), ppod("team", "p1", None)],
        );
        let (grouped, _) = group_pending_input_by_node_symmetry(&input);
        let group_name = grouped.nodes[0].name.clone();
        let workload_ids: Vec<String> = grouped.workloads.iter().map(|w| w.id.clone()).collect();
        let mut solution = OptimizationSolution::default();
        solution.assignment_counts.insert(
            workload_ids[0].clone(),
            std::collections::HashMap::from([(group_name.clone(), 1)]),
        );
        solution.assignment_counts.insert(
            workload_ids[1].clone(),
            std::collections::HashMap::from([(group_name, 1)]),
        );

        let expanded = expand_grouped_solution_to_physical(&grouped, &solution)
            .expect("grouped solution should expand");

        assert_eq!(
            expanded.assignment_counts[&workload_ids[0]]
                .values()
                .sum::<i32>(),
            1
        );
        assert_eq!(
            expanded.assignment_counts[&workload_ids[1]]
                .values()
                .sum::<i32>(),
            1
        );
        let used_nodes: BTreeSet<String> = expanded
            .assignment_counts
            .values()
            .flat_map(|counts| counts.keys().cloned())
            .collect();
        assert_eq!(
            used_nodes,
            BTreeSet::from(["n1".to_string(), "n2".to_string()])
        );
    }

    #[test]
    fn grouped_solution_expansion_rejects_unphysical_aggregate_pack() {
        let cluster = NormalizedCluster {
            nodes: vec![
                node("n1", 8_000, 32, 10, 4),
                node("n2", 8_000, 32, 10, 4),
                node("n3", 8_000, 32, 10, 4),
            ],
            workloads: (0..4)
                .map(|i| {
                    workload(
                        "team",
                        &format!("p{i}"),
                        "",
                        1_000,
                        1,
                        3,
                        &["n1", "n2", "n3"],
                    )
                })
                .collect(),
            ..Default::default()
        };
        let pending: Vec<PendingGpuPod> = (0..4)
            .map(|i| PendingGpuPod {
                gpu_request: 3,
                ..ppod("team", &format!("p{i}"), None)
            })
            .collect();
        let input = build_pending_input(&cluster, &pending);
        let (grouped, _) = group_pending_input_by_node_symmetry(&input);
        let group_name = grouped.nodes[0].name.clone();
        let mut solution = OptimizationSolution::default();
        for workload in &grouped.workloads {
            solution.assignment_counts.insert(
                workload.id.clone(),
                std::collections::HashMap::from([(group_name.clone(), 1)]),
            );
        }

        let err = expand_grouped_solution_to_physical(&grouped, &solution)
            .expect_err("aggregate-only grouped placement must be rejected");

        assert!(err.contains("could not be expanded"));
    }

    #[test]
    fn groups_same_gang_and_scales_requests() {
        let cluster = NormalizedCluster {
            nodes: vec![node("n1", 16000, 64, 110, 8)],
            workloads: vec![
                workload("team", "m0", "", 1000, 2, 1, &["n1"]),
                workload("team", "m1", "", 1000, 2, 1, &["n1"]),
                workload("team", "m2", "", 1000, 2, 1, &["n1"]),
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
    fn gang_priority_uses_max_member_priority() {
        let cluster = NormalizedCluster {
            nodes: vec![node("n1", 16000, 64, 110, 8)],
            workloads: vec![
                workload("team", "m0", "", 1000, 2, 1, &["n1"]),
                workload("team", "m1", "", 1000, 2, 1, &["n1"]),
            ],
            ..Default::default()
        };
        let mut low = ppod("team", "m0", Some("job"));
        low.deadline_unix_seconds = 1783252800;
        low.max_gpus = 6;
        low.predicted_runtime_seconds = 3600;
        low.predicted_peak_vram_bytes = 24 * 1024 * 1024 * 1024;
        let mut high = ppod("team", "m1", Some("job"));
        high.priority = 9;
        high.priority_class_name = Some("research-high".to_string());
        high.team = Some("research".to_string());
        high.queue = Some("urgent".to_string());
        high.queue_wait_seconds = 900;
        high.business_value = 42;
        high.deadline_unix_seconds = 1783339200;
        high.min_gpus = 2;
        high.max_gpus = 8;
        high.preferred_gpus = 4;
        high.flexible = true;
        high.predicted_runtime_seconds = 7200;
        high.predicted_peak_vram_bytes = 48 * 1024 * 1024 * 1024;
        let input = build_pending_input(&cluster, &[low, high]);
        assert_eq!(input.workloads.len(), 1);
        assert_eq!(input.workloads[0].priority, 9);
        assert_eq!(input.workloads[0].priority_class_name, "research-high");
        assert_eq!(input.workloads[0].team, "research");
        assert_eq!(input.workloads[0].queue, "urgent");
        assert_eq!(input.workloads[0].queue_wait_seconds, 900);
        assert_eq!(input.workloads[0].business_value, 42);
        assert_eq!(input.workloads[0].deadline_unix_seconds, 1783252800);
        assert_eq!(input.workloads[0].min_gpus, 2);
        assert_eq!(input.workloads[0].max_gpus, 6);
        assert_eq!(input.workloads[0].preferred_gpus, 4);
        assert!(input.workloads[0].flexible);
        assert_eq!(input.workloads[0].predicted_runtime_seconds, 7200);
        assert_eq!(
            input.workloads[0].predicted_peak_vram_bytes,
            48 * 1024 * 1024 * 1024
        );
    }

    #[test]
    fn parse_vram_label_bytes_handles_units_and_bare_number_heuristic() {
        let gib = 1024_i64 * 1024 * 1024;
        let mib = 1024_i64 * 1024;
        // Explicit suffixes.
        assert_eq!(parse_vram_label_bytes("24Gi"), 24 * gib);
        assert_eq!(parse_vram_label_bytes("24576Mi"), 24576 * mib);
        assert_eq!(parse_vram_label_bytes("1Ki"), 1024);
        assert_eq!(parse_vram_label_bytes("1Gb"), 1_000_000_000);
        // Bare-number heuristic that maps real GPU label values to the right unit:
        //   small -> GiB (a GPU advertised as "24"),
        assert_eq!(parse_vram_label_bytes("24"), 24 * gib);
        //   mid-range -> MiB (nvidia.com/gpu.memory is MiB; 24576 MiB = 24 GiB),
        assert_eq!(parse_vram_label_bytes("24576"), 24576 * mib);
        //   large -> raw bytes.
        assert_eq!(parse_vram_label_bytes("25769803776"), 25_769_803_776);
        // Junk / non-positive -> 0 (unknown; never a fabricated capacity).
        assert_eq!(parse_vram_label_bytes(""), 0);
        assert_eq!(parse_vram_label_bytes("garbage"), 0);
        assert_eq!(parse_vram_label_bytes("-5"), 0);
    }

    #[test]
    fn node_peak_vram_bytes_prefers_bytes_then_gib_then_nvidia_mib() {
        let gib = 1024_i64 * 1024 * 1024;
        let mib = 1024_i64 * 1024;
        let label = |k: &str, v: &str| BTreeMap::from([(k.to_string(), v.to_string())]);
        assert_eq!(
            node_peak_vram_bytes(&label("nvidia.com/gpu.memory", "40960")),
            40960 * mib
        );
        assert_eq!(
            node_peak_vram_bytes(&label("ksolver.dev/gpu-vram-gib", "80")),
            80 * gib
        );
        // Priority: explicit bytes label wins over the GiB and NVIDIA MiB labels.
        let all = BTreeMap::from([
            (
                "ksolver.dev/gpu-vram-bytes".to_string(),
                (24 * gib).to_string(),
            ),
            ("ksolver.dev/gpu-vram-gib".to_string(), "80".to_string()),
            ("nvidia.com/gpu.memory".to_string(), "40960".to_string()),
        ]);
        assert_eq!(node_peak_vram_bytes(&all), 24 * gib);
        // No recognized label -> 0 (unknown capacity; never blocks or fabricates).
        assert_eq!(
            node_peak_vram_bytes(&BTreeMap::from([("x".to_string(), "1".to_string())])),
            0
        );
    }

    #[test]
    fn vram_fits_node_is_the_oom_prevention_boundary() {
        let gib = 1024_i64 * 1024 * 1024;
        let gpu = |n: i64| BTreeMap::from([("nvidia.com/gpu".to_string(), n)]);
        let is_gpu = |r: &str| r == "nvidia.com/gpu";
        // Fits when predicted <= node capacity, including the exact-fit boundary.
        assert!(vram_fits_node(20 * gib, 24 * gib, &gpu(1), &is_gpu));
        assert!(vram_fits_node(24 * gib, 24 * gib, &gpu(1), &is_gpu));
        // Does NOT fit when predicted exceeds capacity — the OOM case ksolver must refuse.
        assert!(!vram_fits_node(25 * gib, 24 * gib, &gpu(1), &is_gpu));
        // Unknown prediction OR unknown node capacity -> don't block (advisory / fail-open).
        assert!(vram_fits_node(0, 24 * gib, &gpu(1), &is_gpu));
        assert!(vram_fits_node(25 * gib, 0, &gpu(1), &is_gpu));
        // No GPU requested -> VRAM feasibility is not applicable.
        assert!(vram_fits_node(
            25 * gib,
            24 * gib,
            &BTreeMap::new(),
            &is_gpu
        ));
    }

    #[test]
    fn predicted_vram_filters_known_too_small_gpu_nodes() {
        let mut small = node("small", 16000, 64, 110, 8);
        small
            .labels
            .insert("nvidia.com/gpu.memory".to_string(), "24576".to_string());
        let mut large = node("large", 16000, 64, 110, 8);
        large
            .labels
            .insert("nvidia.com/gpu.memory".to_string(), "81920".to_string());
        let cluster = NormalizedCluster {
            nodes: vec![small, large],
            workloads: vec![workload("team", "p0", "", 1000, 2, 1, &["small", "large"])],
            ..Default::default()
        };
        let mut pending = ppod("team", "p0", None);
        pending.predicted_peak_vram_bytes = 40 * 1024 * 1024 * 1024;

        let input = build_pending_input(&cluster, &[pending]);

        assert_eq!(input.workloads.len(), 1);
        assert_eq!(input.workloads[0].feasible_nodes, vec!["large".to_string()]);
    }

    #[test]
    fn predicted_vram_does_not_filter_unknown_node_memory() {
        let cluster = NormalizedCluster {
            nodes: vec![node("unknown", 16000, 64, 110, 8)],
            workloads: vec![workload("team", "p0", "", 1000, 2, 1, &["unknown"])],
            ..Default::default()
        };
        let mut pending = ppod("team", "p0", None);
        pending.predicted_peak_vram_bytes = 120 * 1024 * 1024 * 1024;

        let input = build_pending_input(&cluster, &[pending]);

        assert_eq!(input.workloads.len(), 1);
        assert_eq!(
            input.workloads[0].feasible_nodes,
            vec!["unknown".to_string()]
        );
    }

    #[test]
    fn predicted_vram_soft_score_prefers_smallest_adequate_gpu_memory() {
        let mut l40 = node("l40", 16000, 64, 110, 8);
        l40.labels
            .insert("nvidia.com/gpu.memory".to_string(), "49152".to_string());
        let mut h100 = node("h100", 16000, 64, 110, 8);
        h100.labels
            .insert("nvidia.com/gpu.memory".to_string(), "81920".to_string());
        let cluster = NormalizedCluster {
            nodes: vec![l40, h100],
            workloads: vec![workload("team", "p0", "", 1000, 2, 1, &["l40", "h100"])],
            ..Default::default()
        };
        let mut pending = ppod("team", "p0", None);
        pending.predicted_peak_vram_bytes = 40 * 1024 * 1024 * 1024;

        let input = build_pending_input(&cluster, &[pending]);
        let scores = &input.workloads[0].soft_scores;

        assert!(scores["l40"] > scores["h100"]);
    }

    #[cfg(feature = "rust-cp-sat")]
    #[test]
    fn predicted_vram_rightsizing_drives_soft_solver_tiebreak() {
        let mut l40 = node("l40", 16000, 64, 110, 8);
        l40.labels
            .insert("nvidia.com/gpu.memory".to_string(), "49152".to_string());
        let mut h100 = node("h100", 16000, 64, 110, 8);
        h100.labels
            .insert("nvidia.com/gpu.memory".to_string(), "81920".to_string());
        let cluster = NormalizedCluster {
            nodes: vec![l40, h100],
            workloads: vec![workload("team", "p0", "", 1000, 2, 1, &["l40", "h100"])],
            ..Default::default()
        };
        let mut pending = ppod("team", "p0", None);
        pending.predicted_peak_vram_bytes = 40 * 1024 * 1024 * 1024;
        let input = build_pending_input(&cluster, &[pending]);
        let scenario = crate::model::ScenarioConfig {
            solver: "cp-sat-rust".to_string(),
            partial_admission: true,
            enable_soft_affinity: true,
            ..Default::default()
        };

        let (solution, info) = crate::cpsat_rust::solve(&input, &scenario).expect("solve");
        let counts = solution
            .assignment_counts
            .get("pod:team/p0")
            .unwrap_or_else(|| panic!("workload should be admitted; status={}", info.status));

        assert!(
            counts.contains_key("l40") && !counts.contains_key("h100"),
            "VRAM right-sizing should choose the smaller adequate GPU, got {counts:?}"
        );
    }

    #[test]
    fn predicted_vram_soft_score_ignores_unknown_gpu_memory() {
        let cluster = NormalizedCluster {
            nodes: vec![node("unknown", 16000, 64, 110, 8)],
            workloads: vec![workload("team", "p0", "", 1000, 2, 1, &["unknown"])],
            ..Default::default()
        };
        let mut pending = ppod("team", "p0", None);
        pending.predicted_peak_vram_bytes = 40 * 1024 * 1024 * 1024;

        let input = build_pending_input(&cluster, &[pending]);

        assert!(input.workloads[0].soft_scores.is_empty());
    }

    #[test]
    fn predicted_vram_drop_reason_is_specific_when_capacity_otherwise_fits() {
        let mut small = node("small", 16000, 64, 110, 8);
        small
            .labels
            .insert("ksolver.dev/gpu-vram-gib".to_string(), "24".to_string());
        let cluster = NormalizedCluster {
            nodes: vec![small],
            workloads: vec![workload("team", "p0", "", 1000, 2, 1, &["small"])],
            ..Default::default()
        };
        let mut pending = ppod("team", "p0", None);
        pending.predicted_peak_vram_bytes = 40 * 1024 * 1024 * 1024;

        let (input, drops) =
            build_pending_input_diagnosed(&cluster, &[pending], &BTreeMap::new(), &|n| {
                n == GPU_RESOURCE
            });

        assert!(input.workloads.is_empty());
        assert_eq!(drops.len(), 1);
        assert!(drops[0].reason.contains("predicted peak VRAM"));
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
    /// A matchLabels selector with own-namespace scope.
    fn sel(pairs: &[(&str, &str)]) -> AntiAffinitySelector {
        AntiAffinitySelector {
            reqs: reqs(pairs),
            namespaces: Vec::new(),
            namespace_selector: None,
        }
    }
    /// A list of matchLabels selectors -> modeled selector list (own-namespace scope).
    fn sel_list(selectors: &[&[(&str, &str)]]) -> Vec<AntiAffinitySelector> {
        selectors.iter().map(|s| sel(s)).collect()
    }

    fn ppod_aa(ns: &str, name: &str, selectors: &[&[(&str, &str)]]) -> PendingGpuPod {
        let mut p = ppod(ns, name, None);
        p.anti_affinity_host_selectors = sel_list(selectors);
        p
    }

    fn ppod_affinity(
        ns: &str,
        name: &str,
        topology_key: &str,
        labels: &[(&str, &str)],
    ) -> PendingGpuPod {
        let mut p = ppod(ns, name, None);
        p.affinity_topology_selectors = vec![(topology_key.to_string(), sel(labels))];
        p
    }

    #[test]
    fn required_pod_affinity_keeps_candidate_in_matching_running_domain() {
        let cluster = NormalizedCluster {
            nodes: vec![
                node_with_label(
                    "zone-a-node",
                    16000,
                    64,
                    110,
                    8,
                    &[("topology.kubernetes.io/zone", "zone-a")],
                ),
                node_with_label(
                    "zone-b-node",
                    16000,
                    64,
                    110,
                    8,
                    &[("topology.kubernetes.io/zone", "zone-b")],
                ),
            ],
            workloads: vec![
                running_labeled("team", "peer", "zone-a-node", &[("app", "trainer")]),
                workload(
                    "team",
                    "pending",
                    "",
                    1000,
                    2,
                    1,
                    &["zone-a-node", "zone-b-node"],
                ),
            ],
            ..Default::default()
        };

        let input = build_pending_input(
            &cluster,
            &[ppod_affinity(
                "team",
                "pending",
                "topology.kubernetes.io/zone",
                &[("app", "trainer")],
            )],
        );

        assert_eq!(input.workloads.len(), 1);
        assert_eq!(
            input.workloads[0].feasible_nodes,
            vec!["zone-a-node".to_string()]
        );
    }

    #[test]
    fn required_pod_affinity_without_existing_match_does_not_block_bootstrap() {
        let cluster = NormalizedCluster {
            nodes: vec![
                node_with_label(
                    "zone-a-node",
                    16000,
                    64,
                    110,
                    8,
                    &[("topology.kubernetes.io/zone", "zone-a")],
                ),
                node_with_label(
                    "zone-b-node",
                    16000,
                    64,
                    110,
                    8,
                    &[("topology.kubernetes.io/zone", "zone-b")],
                ),
            ],
            workloads: vec![workload(
                "team",
                "pending",
                "",
                1000,
                2,
                1,
                &["zone-a-node", "zone-b-node"],
            )],
            ..Default::default()
        };

        let input = build_pending_input(
            &cluster,
            &[ppod_affinity(
                "team",
                "pending",
                "topology.kubernetes.io/zone",
                &[("app", "trainer")],
            )],
        );

        assert_eq!(input.workloads.len(), 1);
        assert_eq!(
            input.workloads[0].feasible_nodes,
            vec!["zone-a-node".to_string(), "zone-b-node".to_string()]
        );
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

    fn node_with_label(
        name: &str,
        cpu: i64,
        mem: i64,
        pods: i64,
        gpu: i64,
        labels: &[(&str, &str)],
    ) -> NormalizedNode {
        let mut n = node(name, cpu, mem, pods, gpu);
        n.labels = labels
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        n
    }

    fn topology_spread_rule(
        topology_key: &str,
        selector: &[(&str, &str)],
    ) -> crate::model::TopologySpreadRule {
        crate::model::TopologySpreadRule {
            max_skew: 1,
            topology_key: topology_key.to_string(),
            when_unsatisfiable: "DoNotSchedule".to_string(),
            selector: selector
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
            selector_reqs: selector
                .iter()
                .map(|(k, v)| crate::model::LabelSelectorReq {
                    key: k.to_string(),
                    operator: "In".to_string(),
                    values: vec![v.to_string()],
                })
                .collect(),
            ..Default::default()
        }
    }

    fn topology_spread_rule_expr(
        topology_key: &str,
        key: &str,
        operator: &str,
        values: &[&str],
    ) -> crate::model::TopologySpreadRule {
        crate::model::TopologySpreadRule {
            max_skew: 1,
            topology_key: topology_key.to_string(),
            when_unsatisfiable: "DoNotSchedule".to_string(),
            selector_reqs: vec![crate::model::LabelSelectorReq {
                key: key.to_string(),
                operator: operator.to_string(),
                values: values.iter().map(|v| v.to_string()).collect(),
            }],
            ..Default::default()
        }
    }

    #[test]
    fn hard_topology_spread_filters_domains_over_max_skew() {
        let mut pending = labeled_pending(
            "team",
            "pending",
            &["zone-a-node", "zone-b-node"],
            &[("app", "trainer")],
        );
        pending.topology_spread_rules = vec![topology_spread_rule(
            "topology.kubernetes.io/zone",
            &[("app", "trainer")],
        )];
        pending.topology_spread_constraints = 1;
        let cluster = NormalizedCluster {
            nodes: vec![
                node_with_label(
                    "zone-a-node",
                    16000,
                    64,
                    110,
                    8,
                    &[("topology.kubernetes.io/zone", "zone-a")],
                ),
                node_with_label(
                    "zone-b-node",
                    16000,
                    64,
                    110,
                    8,
                    &[("topology.kubernetes.io/zone", "zone-b")],
                ),
            ],
            workloads: vec![
                running_labeled("team", "running", "zone-a-node", &[("app", "trainer")]),
                pending,
            ],
            ..Default::default()
        };

        let input = build_pending_input(&cluster, &[ppod("team", "pending", None)]);

        assert_eq!(input.workloads.len(), 1);
        assert_eq!(
            input.workloads[0].feasible_nodes,
            vec!["zone-b-node".to_string()]
        );
    }

    #[test]
    fn hard_topology_spread_match_expression_filters_domains_over_max_skew() {
        let mut pending = labeled_pending(
            "team",
            "pending",
            &["zone-a-node", "zone-b-node"],
            &[("app", "trainer")],
        );
        pending.topology_spread_rules = vec![topology_spread_rule_expr(
            "topology.kubernetes.io/zone",
            "app",
            "In",
            &["trainer", "worker"],
        )];
        pending.topology_spread_constraints = 1;
        let cluster = NormalizedCluster {
            nodes: vec![
                node_with_label(
                    "zone-a-node",
                    16000,
                    64,
                    110,
                    8,
                    &[("topology.kubernetes.io/zone", "zone-a")],
                ),
                node_with_label(
                    "zone-b-node",
                    16000,
                    64,
                    110,
                    8,
                    &[("topology.kubernetes.io/zone", "zone-b")],
                ),
            ],
            workloads: vec![
                running_labeled("team", "running", "zone-a-node", &[("app", "worker")]),
                pending,
            ],
            ..Default::default()
        };

        let input = build_pending_input(&cluster, &[ppod("team", "pending", None)]);

        assert_eq!(input.workloads.len(), 1);
        assert_eq!(
            input.workloads[0].feasible_nodes,
            vec!["zone-b-node".to_string()]
        );
    }

    #[test]
    fn advanced_hard_topology_spread_is_not_partially_enforced() {
        let mut pending = labeled_pending(
            "team",
            "pending",
            &["zone-a-node", "zone-b-node"],
            &[("app", "trainer")],
        );
        let mut rule = topology_spread_rule("topology.kubernetes.io/zone", &[("app", "trainer")]);
        rule.min_domains = Some(2);
        pending.topology_spread_rules = vec![rule];
        pending.topology_spread_constraints = 1;
        let cluster = NormalizedCluster {
            nodes: vec![
                node_with_label(
                    "zone-a-node",
                    16000,
                    64,
                    110,
                    8,
                    &[("topology.kubernetes.io/zone", "zone-a")],
                ),
                node_with_label(
                    "zone-b-node",
                    16000,
                    64,
                    110,
                    8,
                    &[("topology.kubernetes.io/zone", "zone-b")],
                ),
            ],
            workloads: vec![
                running_labeled("team", "running", "zone-a-node", &[("app", "trainer")]),
                pending,
            ],
            ..Default::default()
        };

        let input = build_pending_input(&cluster, &[ppod("team", "pending", None)]);

        assert_eq!(input.workloads.len(), 1);
        assert_eq!(
            input.workloads[0].feasible_nodes,
            vec!["zone-a-node".to_string(), "zone-b-node".to_string()]
        );
    }

    #[test]
    fn hard_topology_spread_ignores_pending_pod_outside_selector() {
        let mut pending = labeled_pending(
            "team",
            "pending",
            &["zone-a-node", "zone-b-node"],
            &[("app", "other")],
        );
        pending.topology_spread_rules = vec![topology_spread_rule(
            "topology.kubernetes.io/zone",
            &[("app", "trainer")],
        )];
        pending.topology_spread_constraints = 1;
        let cluster = NormalizedCluster {
            nodes: vec![
                node_with_label(
                    "zone-a-node",
                    16000,
                    64,
                    110,
                    8,
                    &[("topology.kubernetes.io/zone", "zone-a")],
                ),
                node_with_label(
                    "zone-b-node",
                    16000,
                    64,
                    110,
                    8,
                    &[("topology.kubernetes.io/zone", "zone-b")],
                ),
            ],
            workloads: vec![
                running_labeled("team", "running", "zone-a-node", &[("app", "trainer")]),
                pending,
            ],
            ..Default::default()
        };

        let input = build_pending_input(&cluster, &[ppod("team", "pending", None)]);

        assert_eq!(input.workloads.len(), 1);
        assert_eq!(
            input.workloads[0].feasible_nodes,
            vec!["zone-a-node".to_string(), "zone-b-node".to_string()]
        );
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
            priority: 0,
            priority_class_name: None,
            team: None,
            queue: None,
            business_value: 0,
            queue_wait_seconds: 0,
            deadline_unix_seconds: 0,
            min_gpus: 0,
            max_gpus: 0,
            preferred_gpus: 0,
            flexible: false,
            predicted_runtime_seconds: 0,
            predicted_peak_vram_bytes: 0,
            required_gpu_topology: vec![],
            gang_key: Some(format!("{ns}/{gang}")),
            colocate,
            unmodeled_constraints: vec![],
            anti_affinity_host_selectors: sel_list(selectors),
            affinity_topology_selectors: vec![],
            anti_affinity_topology_selectors: vec![],
            preferred_node_affinity: vec![],
            preferred_pod_affinity: vec![],
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
        m0.anti_affinity_host_selectors = vec![sel(&[("app", "x")])];
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
        assert!(g.resources.contains(&"nvidia.com/gpu".to_string()));
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
        p.anti_affinity_topology_selectors = vec![(key.to_string(), sel(labels))];
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
            vec![(ZONE.to_string(), sel(&[("app", "trainer")]))];
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

    fn one_req(key: &str, op: &str, values: &[&str]) -> Vec<AntiAffinitySelector> {
        vec![AntiAffinitySelector {
            reqs: vec![LabelSelectorReq {
                key: key.to_string(),
                operator: op.to_string(),
                values: values.iter().map(|v| v.to_string()).collect(),
            }],
            namespaces: Vec::new(),
            namespace_selector: None,
        }]
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
            &|n| n == "nvidia.com/gpu",
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
            &|n| n == "nvidia.com/gpu",
        );
        assert_eq!(input.workloads.len(), 0);
        assert_eq!(drops.len(), 1);
        assert!(drops[0].reason.contains("no feasible node"));
        assert_eq!(drops[0].pod_scopes, vec!["team/pending".to_string()]);
    }

    #[test]
    fn quota_group_counts_mig_slices_with_matcher() {
        // A running MIG-slice pod + a pending MIG-slice pod in `team`; a MIG-aware matcher makes
        // the quota count the slice — running usage reflects it and the group's resources include
        // the MIG name.
        let mut mig_node = node("n1", 16000, 64, 110, 0);
        mig_node
            .extended_resources
            .insert("nvidia.com/mig-1g.5gb".to_string(), 7);
        let mut running = workload("team", "run", "n1", 1000, 2, 0, &["n1"]);
        running
            .extended_resource_requests
            .insert("nvidia.com/mig-1g.5gb".to_string(), 1);
        let mut pending_w = workload("team", "pending", "", 1000, 2, 0, &["n1"]);
        pending_w
            .extended_resource_requests
            .insert("nvidia.com/mig-1g.5gb".to_string(), 1);
        let cluster = NormalizedCluster {
            nodes: vec![mig_node],
            workloads: vec![running, pending_w],
            ..Default::default()
        };
        let quotas = BTreeMap::from([("team".to_string(), 3_i64)]);
        // MIG-aware matcher (whole GPU + mig prefix).
        let (input, _) = super::build_pending_input_diagnosed(
            &cluster,
            &[ppod("team", "pending", None)],
            &quotas,
            &|n| n == "nvidia.com/gpu" || n.starts_with("nvidia.com/mig-"),
        );
        assert_eq!(input.quota_groups.len(), 1);
        let g = &input.quota_groups[0];
        // Group counts the MIG resource; remaining = cap(3) - running slice(1) = 2.
        assert!(g.resources.contains(&"nvidia.com/mig-1g.5gb".to_string()));
        assert_eq!(g.limit, 2);
    }

    #[test]
    fn quota_group_counts_mig_slices_through_default_builder() {
        let mut mig_node = node("n1", 16000, 64, 110, 0);
        mig_node
            .extended_resources
            .insert("nvidia.com/mig-1g.5gb".to_string(), 7);
        let mut running = workload("team", "run", "n1", 1000, 2, 0, &["n1"]);
        running
            .extended_resource_requests
            .insert("nvidia.com/mig-1g.5gb".to_string(), 1);
        let mut pending_w = workload("team", "pending", "", 1000, 2, 0, &["n1"]);
        pending_w
            .extended_resource_requests
            .insert("nvidia.com/mig-1g.5gb".to_string(), 1);
        let cluster = NormalizedCluster {
            nodes: vec![mig_node],
            workloads: vec![running, pending_w],
            ..Default::default()
        };
        let quotas = BTreeMap::from([("team".to_string(), 3_i64)]);

        let input = super::build_pending_input(&cluster, &[ppod("team", "pending", None)], &quotas);

        assert_eq!(input.quota_groups.len(), 1);
        let g = &input.quota_groups[0];
        assert!(g.resources.contains(&"nvidia.com/gpu".to_string()));
        assert!(g.resources.contains(&"nvidia.com/mig-1g.5gb".to_string()));
        assert_eq!(g.limit, 2);
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

    #[test]
    fn required_gpu_topology_filters_candidate_nodes_by_label() {
        let mut island_a = node("island-a-0", 16000, 64, 110, 4);
        island_a.labels.insert(
            "topology.gpu.ksolver.dev/island".to_string(),
            "island-a".to_string(),
        );
        let mut island_b = node("island-b-0", 16000, 64, 110, 4);
        island_b.labels.insert(
            "topology.gpu.ksolver.dev/island".to_string(),
            "island-b".to_string(),
        );
        let cluster = NormalizedCluster {
            nodes: vec![island_a, island_b],
            workloads: vec![workload(
                "team",
                "trainer",
                "",
                1000,
                2,
                1,
                &["island-a-0", "island-b-0"],
            )],
            ..Default::default()
        };
        let mut pending = ppod("team", "trainer", None);
        pending.required_gpu_topology = vec![(
            "topology.gpu.ksolver.dev/island".to_string(),
            "island-b".to_string(),
        )];

        let input = build_pending_input(&cluster, &[pending]);

        assert_eq!(input.workloads.len(), 1);
        assert_eq!(
            input.workloads[0].feasible_nodes,
            vec!["island-b-0".to_string()]
        );
    }

    #[test]
    fn required_gpu_topology_drop_reports_missing_label() {
        let mut island_a = node("island-a-0", 16000, 64, 110, 4);
        island_a.labels.insert(
            "topology.gpu.ksolver.dev/island".to_string(),
            "island-a".to_string(),
        );
        let cluster = NormalizedCluster {
            nodes: vec![island_a],
            workloads: vec![workload("team", "trainer", "", 1000, 2, 1, &["island-a-0"])],
            ..Default::default()
        };
        let mut pending = ppod("team", "trainer", None);
        pending.required_gpu_topology = vec![(
            "topology.gpu.ksolver.dev/island".to_string(),
            "island-b".to_string(),
        )];

        let (input, drops) =
            super::build_pending_input_diagnosed(&cluster, &[pending], &BTreeMap::new(), &|n| {
                n == GPU_RESOURCE
            });

        assert!(input.workloads.is_empty());
        assert_eq!(drops.len(), 1);
        assert!(drops[0]
            .reason
            .contains("required GPU topology label topology.gpu.ksolver.dev/island=island-b"));
    }

    #[test]
    fn gang_members_must_agree_on_required_gpu_topology() {
        let cluster = NormalizedCluster {
            nodes: vec![node("n1", 16000, 64, 110, 4)],
            workloads: vec![
                workload("team", "a", "", 1000, 2, 1, &["n1"]),
                workload("team", "b", "", 1000, 2, 1, &["n1"]),
            ],
            ..Default::default()
        };
        let mut a = ppod("team", "a", Some("gang"));
        a.required_gpu_topology = vec![(
            "topology.gpu.ksolver.dev/island".to_string(),
            "island-a".to_string(),
        )];
        let mut b = ppod("team", "b", Some("gang"));
        b.required_gpu_topology = vec![(
            "topology.gpu.ksolver.dev/island".to_string(),
            "island-b".to_string(),
        )];

        let (input, drops) =
            super::build_pending_input_diagnosed(&cluster, &[a, b], &BTreeMap::new(), &|n| {
                n == GPU_RESOURCE
            });

        assert!(input.workloads.is_empty());
        assert_eq!(drops.len(), 1);
        assert_eq!(
            drops[0].reason,
            "gang members disagree on required GPU topology"
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
        let (input, drops) =
            super::build_pending_input_diagnosed(&cluster, &pending, &quotas, &|n| {
                n == "nvidia.com/gpu"
            });
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
            &std::collections::HashSet::new(),
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

    // ---- F-CNS-1: cross-namespace anti-affinity (explicit namespaces list) ----

    #[test]
    fn cross_namespace_explicit_list_excludes_only_scoped_ns() {
        // Pending pod in `team` with hostname anti-affinity scoped to namespaces=[other], app=x.
        // A matching running pod in `other` (n1) excludes n1; a matching pod in `third` (n2) does
        // NOT (not in scope); n3 stays free.
        let cluster = NormalizedCluster {
            nodes: vec![
                node("n1", 16000, 64, 110, 8),
                node("n2", 16000, 64, 110, 8),
                node("n3", 16000, 64, 110, 8),
            ],
            workloads: vec![
                running_labeled("other", "peer", "n1", &[("app", "x")]),
                running_labeled("third", "peer2", "n2", &[("app", "x")]),
                workload("team", "pending", "", 1000, 2, 1, &["n1", "n2", "n3"]),
            ],
            ..Default::default()
        };
        let mut p = ppod("team", "pending", None);
        p.anti_affinity_host_selectors = vec![AntiAffinitySelector {
            reqs: reqs(&[("app", "x")]),
            namespaces: vec!["other".to_string()],
            namespace_selector: None,
        }];
        let input = super::build_pending_input(&cluster, &[p], &BTreeMap::new());
        let f = &input.workloads[0].feasible_nodes;
        assert!(
            !f.contains(&"n1".to_string()),
            "n1 (other) should be excluded"
        );
        assert!(f.contains(&"n2".to_string()), "n2 (third) not in scope");
        assert!(f.contains(&"n3".to_string()));
    }

    #[test]
    fn own_namespace_selector_does_not_apply_cross_namespace() {
        // Empty namespaces = own namespace only: a `team` pod's selector must NOT be triggered by
        // a matching pod in `other` (byte-identical to pre-F-CNS-1 behavior).
        let cluster = NormalizedCluster {
            nodes: vec![node("n1", 16000, 64, 110, 8), node("n2", 16000, 64, 110, 8)],
            workloads: vec![
                running_labeled("other", "peer", "n1", &[("app", "x")]),
                workload("team", "pending", "", 1000, 2, 1, &["n1", "n2"]),
            ],
            ..Default::default()
        };
        let mut p = ppod("team", "pending", None);
        p.anti_affinity_host_selectors = vec![sel(&[("app", "x")])]; // own-ns scope
        let input = super::build_pending_input(&cluster, &[p], &BTreeMap::new());
        // other/peer is NOT in the own-namespace scope ⇒ n1 not excluded.
        assert_eq!(input.workloads[0].feasible_nodes.len(), 2);
    }

    #[test]
    fn cross_namespace_symmetry_running_selector_scopes_to_pending_ns() {
        // A running pod in `other` (n1) carries a hostname anti-affinity scoped to
        // namespaces=[team] on app=trainer; a pending `team` pod labelled app=trainer must avoid n1.
        let mut peer = running_labeled("other", "peer", "n1", &[]);
        peer.anti_affinity_host_selectors = vec![AntiAffinitySelector {
            reqs: reqs(&[("app", "trainer")]),
            namespaces: vec!["team".to_string()],
            namespace_selector: None,
        }];
        let cluster = NormalizedCluster {
            nodes: vec![node("n1", 16000, 64, 110, 8), node("n2", 16000, 64, 110, 8)],
            workloads: vec![
                peer,
                labeled_pending("team", "p", &["n1", "n2"], &[("app", "trainer")]),
            ],
            ..Default::default()
        };
        let input =
            super::build_pending_input(&cluster, &[ppod("team", "p", None)], &BTreeMap::new());
        assert_eq!(input.workloads[0].feasible_nodes, vec!["n2".to_string()]);
    }

    // ---- Soft (preferred) node affinity ----

    #[test]
    fn builder_computes_soft_scores_for_preferred_node_affinity() {
        use crate::model::{NodeAffinityTerm, PreferredNodeTerm};
        // n1 labelled zone=a; n2 zone=b. Pending pod prefers zone=a (weight 10).
        let mut n1 = node("n1", 16000, 64, 110, 8);
        n1.labels = [("zone".to_string(), "a".to_string())].into();
        let mut n2 = node("n2", 16000, 64, 110, 8);
        n2.labels = [("zone".to_string(), "b".to_string())].into();
        let cluster = NormalizedCluster {
            nodes: vec![n1, n2],
            workloads: vec![workload("team", "p", "", 1000, 2, 1, &["n1", "n2"])],
            ..Default::default()
        };
        let mut p = ppod("team", "p", None);
        p.preferred_node_affinity = vec![PreferredNodeTerm {
            weight: 10,
            exprs: vec![NodeAffinityTerm {
                key: "zone".to_string(),
                operator: "In".to_string(),
                values: vec!["a".to_string()],
            }],
            fields: vec![],
        }];
        let input = super::build_pending_input(&cluster, &[p], &BTreeMap::new());
        let w = &input.workloads[0];
        assert_eq!(w.soft_scores.get("n1"), Some(&10));
        assert!(!w.soft_scores.contains_key("n2")); // zone=b doesn't match ⇒ no score
    }

    #[test]
    fn builder_computes_soft_scores_for_preferred_node_match_fields() {
        use crate::model::{NodeAffinityTerm, PreferredNodeTerm};
        let n1 = node("node-a", 16000, 64, 110, 8);
        let n2 = node("node-b", 16000, 64, 110, 8);
        let cluster = NormalizedCluster {
            nodes: vec![n1, n2],
            workloads: vec![workload("team", "p", "", 1000, 2, 1, &["node-a", "node-b"])],
            ..Default::default()
        };
        let mut p = ppod("team", "p", None);
        p.preferred_node_affinity = vec![PreferredNodeTerm {
            weight: 20,
            exprs: vec![],
            fields: vec![NodeAffinityTerm {
                key: "metadata.name".to_string(),
                operator: "In".to_string(),
                values: vec!["node-a".to_string()],
            }],
        }];
        let input = super::build_pending_input(&cluster, &[p], &BTreeMap::new());
        let w = &input.workloads[0];
        assert_eq!(w.soft_scores.get("node-a"), Some(&20));
        assert!(!w.soft_scores.contains_key("node-b"));
    }

    #[test]
    fn preferred_pod_affinity_scores_domain_with_matching_running_pod() {
        // running "cache" pod on n1 (zone za); pending prefers same-zone as app=cache (weight 40).
        let mut n1 = node("n1", 16000, 64, 110, 8);
        n1.labels = [("topology.kubernetes.io/zone".into(), "za".into())].into();
        let mut n2 = node("n2", 16000, 64, 110, 8);
        n2.labels = [("topology.kubernetes.io/zone".into(), "zb".into())].into();
        let cluster = NormalizedCluster {
            nodes: vec![n1, n2],
            workloads: vec![
                running_labeled("team", "cache", "n1", &[("app", "cache")]),
                workload("team", "pending", "", 1000, 2, 1, &["n1", "n2"]),
            ],
            ..Default::default()
        };
        let mut p = ppod("team", "pending", None);
        p.preferred_pod_affinity = vec![crate::model::PreferredPodTerm {
            weight: 40,
            topology_key: "topology.kubernetes.io/zone".into(),
            selector: sel(&[("app", "cache")]),
            anti: false,
        }];
        let input = build_pending_input(&cluster, &[p]);
        let w = &input.workloads[0];
        assert_eq!(w.soft_scores.get("n1"), Some(&40)); // za shares the cache pod's domain
        assert_eq!(w.soft_scores.get("n2"), None); // zb has no matching pod
    }

    #[test]
    fn preferred_pod_anti_affinity_penalizes_node_with_matching_running_pod() {
        // hostname-key domain is label-based: give each node its own kubernetes.io/hostname label.
        let mut n1 = node("n1", 16000, 64, 110, 8);
        n1.labels = [("kubernetes.io/hostname".into(), "n1".into())].into();
        let mut n2 = node("n2", 16000, 64, 110, 8);
        n2.labels = [("kubernetes.io/hostname".into(), "n2".into())].into();
        let cluster = NormalizedCluster {
            nodes: vec![n1, n2],
            workloads: vec![
                running_labeled("team", "noisy", "n1", &[("app", "noisy")]),
                workload("team", "pending", "", 1000, 2, 1, &["n1", "n2"]),
            ],
            ..Default::default()
        };
        let mut p = ppod("team", "pending", None);
        p.preferred_pod_affinity = vec![crate::model::PreferredPodTerm {
            weight: 25,
            topology_key: "kubernetes.io/hostname".into(),
            selector: sel(&[("app", "noisy")]),
            anti: true,
        }];
        let input = build_pending_input(&cluster, &[p]);
        let w = &input.workloads[0];
        assert_eq!(w.soft_scores.get("n1"), Some(&-25)); // discourage the node with noisy
        assert_eq!(w.soft_scores.get("n2"), None);
    }

    #[test]
    fn preferred_pod_affinity_accumulates_per_matching_pod() {
        // TWO matching cache pods in zone za -> candidate n1 earns 2*weight (kube accumulates).
        let mut n1 = node("n1", 16000, 64, 110, 8);
        n1.labels = [("topology.kubernetes.io/zone".into(), "za".into())].into();
        let mut n3 = node("n3", 16000, 64, 110, 8);
        n3.labels = [("topology.kubernetes.io/zone".into(), "za".into())].into();
        let cluster = NormalizedCluster {
            nodes: vec![n1, n3],
            workloads: vec![
                running_labeled("team", "cache0", "n1", &[("app", "cache")]),
                running_labeled("team", "cache1", "n3", &[("app", "cache")]),
                workload("team", "pending", "", 1000, 2, 1, &["n1"]),
            ],
            ..Default::default()
        };
        let mut p = ppod("team", "pending", None);
        p.preferred_pod_affinity = vec![crate::model::PreferredPodTerm {
            weight: 20,
            topology_key: "topology.kubernetes.io/zone".into(),
            selector: sel(&[("app", "cache")]),
            anti: false,
        }];
        let input = build_pending_input(&cluster, &[p]);
        // n1's zone za holds both cache pods -> 20 + 20.
        assert_eq!(input.workloads[0].soft_scores.get("n1"), Some(&40));
    }

    #[test]
    fn preferred_pod_affinity_dropped_when_gang_disagrees() {
        let cluster = NormalizedCluster {
            nodes: vec![node("n1", 16000, 64, 110, 8), node("n2", 16000, 64, 110, 8)],
            workloads: vec![
                running_labeled("team", "cache", "n1", &[("app", "cache")]),
                workload("team", "m0", "", 1000, 2, 1, &["n1", "n2"]),
                workload("team", "m1", "", 1000, 2, 1, &["n1", "n2"]),
            ],
            ..Default::default()
        };
        let term = |w: i64| crate::model::PreferredPodTerm {
            weight: w,
            topology_key: "kubernetes.io/hostname".into(),
            selector: sel(&[("app", "cache")]),
            anti: false,
        };
        let mut m0 = ppod("team", "m0", Some("job"));
        m0.preferred_pod_affinity = vec![term(40)];
        let mut m1 = ppod("team", "m1", Some("job"));
        m1.preferred_pod_affinity = vec![term(10)]; // disagree on weight
        let input = build_pending_input(&cluster, &[m0, m1]);
        assert_eq!(input.workloads.len(), 1);
        assert!(input.workloads[0].soft_scores.is_empty());
    }

    #[test]
    fn symmetric_preferred_pod_anti_affinity_penalizes_running_pods_domain() {
        // running "guard" on n1 (hostname n1) softly prefers NOT to share a host with app=trainer.
        // pending is app=trainer with NO own preferred terms -> symmetry must still discourage n1.
        let mut n1 = node("n1", 16000, 64, 110, 8);
        n1.labels = [("kubernetes.io/hostname".into(), "n1".into())].into();
        let mut n2 = node("n2", 16000, 64, 110, 8);
        n2.labels = [("kubernetes.io/hostname".into(), "n2".into())].into();
        let mut guard = running_labeled("team", "guard", "n1", &[("role", "guard")]);
        guard.preferred_pod_affinity = vec![crate::model::PreferredPodTerm {
            weight: 30,
            topology_key: "kubernetes.io/hostname".into(),
            selector: sel(&[("app", "trainer")]),
            anti: true,
        }];
        let cluster = NormalizedCluster {
            nodes: vec![n1, n2],
            workloads: vec![
                guard,
                labeled_pending("team", "pending", &["n1", "n2"], &[("app", "trainer")]),
            ],
            ..Default::default()
        };
        let input = build_pending_input(&cluster, &[ppod("team", "pending", None)]);
        let w = &input.workloads[0];
        assert_eq!(w.soft_scores.get("n1"), Some(&-30)); // running guard discourages its host
        assert_eq!(w.soft_scores.get("n2"), None);
    }

    #[test]
    fn symmetric_preferred_ignores_partial_gang_match() {
        // running guard forbids app=trainer softly; gang has one member app=trainer, one without.
        let mut n1 = node("n1", 16000, 64, 110, 8);
        n1.labels = [("kubernetes.io/hostname".into(), "n1".into())].into();
        let mut guard = running_labeled("team", "guard", "n1", &[("role", "guard")]);
        guard.preferred_pod_affinity = vec![crate::model::PreferredPodTerm {
            weight: 30,
            topology_key: "kubernetes.io/hostname".into(),
            selector: sel(&[("app", "trainer")]),
            anti: true,
        }];
        let mut m0 = workload("team", "m0", "", 1000, 2, 1, &["n1"]);
        m0.labels = [("app".to_string(), "trainer".to_string())].into();
        let m1 = workload("team", "m1", "", 1000, 2, 1, &["n1"]); // no labels
        let cluster = NormalizedCluster {
            nodes: vec![n1],
            workloads: vec![guard, m0, m1],
            ..Default::default()
        };
        let input = build_pending_input(
            &cluster,
            &[
                ppod("team", "m0", Some("job")),
                ppod("team", "m1", Some("job")),
            ],
        );
        assert!(input.workloads[0].soft_scores.is_empty()); // not ALL members match -> no score
    }

    #[test]
    fn coplacement_pair_emitted_for_preferred_affinity() {
        // a prefers to be near app=b (hostname); a,b singletons feasible on n1,n2.
        let mut n1 = node("n1", 16000, 64, 110, 8);
        n1.labels = [("kubernetes.io/hostname".into(), "n1".into())].into();
        let mut n2 = node("n2", 16000, 64, 110, 8);
        n2.labels = [("kubernetes.io/hostname".into(), "n2".into())].into();
        let cluster = NormalizedCluster {
            nodes: vec![n1, n2],
            workloads: vec![
                labeled_pending("team", "a", &["n1", "n2"], &[("app", "a")]),
                labeled_pending("team", "b", &["n1", "n2"], &[("app", "b")]),
            ],
            ..Default::default()
        };
        let mut pa = ppod("team", "a", None);
        pa.preferred_pod_affinity = vec![crate::model::PreferredPodTerm {
            weight: 40,
            topology_key: "kubernetes.io/hostname".into(),
            selector: sel(&[("app", "b")]),
            anti: false,
        }];
        let input = build_pending_input(&cluster, &[pa, ppod("team", "b", None)]);
        let cps = &input.soft_coplacement_pairs;
        assert_eq!(cps.len(), 1);
        assert_eq!(cps[0].a, "pod:team/a");
        assert_eq!(cps[0].b, "pod:team/b");
        assert_eq!(cps[0].weight, 40);
        assert_eq!(cps[0].domains.len(), 2); // two hostname domains (n1, n2), both feasible for a,b
    }

    #[test]
    fn no_coplacement_pair_when_no_preferred_affinity() {
        let cluster = NormalizedCluster {
            nodes: vec![node("n1", 16000, 64, 110, 8)],
            workloads: vec![
                labeled_pending("team", "a", &["n1"], &[("app", "a")]),
                labeled_pending("team", "b", &["n1"], &[("app", "b")]),
            ],
            ..Default::default()
        };
        let input = build_pending_input(
            &cluster,
            &[ppod("team", "a", None), ppod("team", "b", None)],
        );
        assert!(input.soft_coplacement_pairs.is_empty());
    }

    #[test]
    fn coplacement_pair_skips_anti_affinity_terms() {
        // a has preferred ANTI-affinity toward b -> NOT a co-placement reward (out of scope).
        let cluster = NormalizedCluster {
            nodes: vec![node("n1", 16000, 64, 110, 8), node("n2", 16000, 64, 110, 8)],
            workloads: vec![
                labeled_pending("team", "a", &["n1", "n2"], &[("app", "a")]),
                labeled_pending("team", "b", &["n1", "n2"], &[("app", "b")]),
            ],
            ..Default::default()
        };
        let mut pa = ppod("team", "a", None);
        pa.preferred_pod_affinity = vec![crate::model::PreferredPodTerm {
            weight: 40,
            topology_key: "kubernetes.io/hostname".into(),
            selector: sel(&[("app", "b")]),
            anti: true, // anti -> skipped
        }];
        let input = build_pending_input(&cluster, &[pa, ppod("team", "b", None)]);
        assert!(input.soft_coplacement_pairs.is_empty());
    }

    // ---- F-CNS-2: namespaceSelector ----

    #[test]
    fn empty_namespace_selector_matches_all_namespaces() {
        // Pending pod in `team` with hostname anti-affinity, empty namespaceSelector {} (= ALL
        // namespaces): a matching running pod in ANY namespace (here `other`, n1) excludes n1.
        let cluster = NormalizedCluster {
            nodes: vec![node("n1", 16000, 64, 110, 8), node("n2", 16000, 64, 110, 8)],
            workloads: vec![
                running_labeled("other", "peer", "n1", &[("app", "x")]),
                workload("team", "pending", "", 1000, 2, 1, &["n1", "n2"]),
            ],
            ..Default::default()
        };
        let mut p = ppod("team", "pending", None);
        p.anti_affinity_host_selectors = vec![AntiAffinitySelector {
            reqs: reqs(&[("app", "x")]),
            namespaces: Vec::new(),
            namespace_selector: Some(Vec::new()), // {} = all namespaces
        }];
        let input = super::build_pending_input(&cluster, &[p], &BTreeMap::new());
        assert_eq!(input.workloads[0].feasible_nodes, vec!["n2".to_string()]);
    }

    #[test]
    fn label_namespace_selector_scopes_by_namespace_labels() {
        // namespaceSelector team=x: only namespaces labelled team=x are in scope. `other` is
        // labelled team=x (n1 excluded); `third` is not (n2 kept).
        let cluster = NormalizedCluster {
            nodes: vec![
                node("n1", 16000, 64, 110, 8),
                node("n2", 16000, 64, 110, 8),
                node("n3", 16000, 64, 110, 8),
            ],
            workloads: vec![
                running_labeled("other", "peer", "n1", &[("app", "x")]),
                running_labeled("third", "peer2", "n2", &[("app", "x")]),
                workload("team", "pending", "", 1000, 2, 1, &["n1", "n2", "n3"]),
            ],
            namespace_labels: BTreeMap::from([
                (
                    "other".to_string(),
                    BTreeMap::from([("team".to_string(), "x".to_string())]),
                ),
                (
                    "third".to_string(),
                    BTreeMap::from([("team".to_string(), "y".to_string())]),
                ),
            ]),
            ..Default::default()
        };
        let mut p = ppod("team", "pending", None);
        p.anti_affinity_host_selectors = vec![AntiAffinitySelector {
            reqs: reqs(&[("app", "x")]),
            namespaces: Vec::new(),
            namespace_selector: Some(reqs(&[("team", "x")])),
        }];
        let input = super::build_pending_input(&cluster, &[p], &BTreeMap::new());
        let f = &input.workloads[0].feasible_nodes;
        assert!(
            !f.contains(&"n1".to_string()),
            "other (team=x) in scope ⇒ n1 excluded"
        );
        assert!(f.contains(&"n2".to_string()), "third (team=y) not in scope");
        assert!(f.contains(&"n3".to_string()));
    }
}
