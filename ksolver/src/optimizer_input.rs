use crate::model::{
    NormalizedCluster, NormalizedNode, NormalizedWorkload, OptimizationInput, OptimizationNode,
    OptimizationWorkload, OptimizationWorkloadMember, ResourceList,
};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::hash::{Hash, Hasher};
use tracing::{debug, info, warn};

pub fn build_input(
    cluster: &NormalizedCluster,
    ignore_unschedulable_workloads: bool,
) -> OptimizationInput {
    build_input_with_grouping(cluster, ignore_unschedulable_workloads, true, true)
}

pub fn build_input_strict(
    cluster: &NormalizedCluster,
    ignore_unschedulable_workloads: bool,
) -> OptimizationInput {
    build_input_with_grouping(cluster, ignore_unschedulable_workloads, false, false)
}

fn build_input_with_grouping(
    cluster: &NormalizedCluster,
    ignore_unschedulable_workloads: bool,
    allow_grouping: bool,
    allow_node_grouping: bool,
) -> OptimizationInput {
    info!(
        cluster = %cluster.cluster_name,
        nodes = cluster.nodes.len(),
        workloads = cluster.workloads.len(),
        blockers = cluster.blockers.len(),
        ignore_unschedulable_workloads,
        allow_grouping,
        allow_node_grouping,
        "building optimization input"
    );
    let (node_groups, node_to_group) = build_nodes(&cluster.nodes, allow_node_grouping);

    let mut input = OptimizationInput {
        nodes: node_groups,
        workloads: Vec::with_capacity(cluster.workloads.len()),
        anti_affinity_pairs: Vec::new(),
    };

    let mut grouped: BTreeMap<String, Vec<NormalizedWorkload>> = BTreeMap::new();
    let mut ungrouped = Vec::new();
    let mut skipped_unschedulable = 0;

    for workload in &cluster.workloads {
        if ignore_unschedulable_workloads && workload.feasible_node_names.is_empty() {
            skipped_unschedulable += 1;
            warn!(
                workload = %format!("{}/{}", workload.namespace, workload.name),
                owner_kind = %workload.owner_kind,
                owner_name = %workload.owner_name,
                reasons = if workload.reasons.is_empty() {
                    "<none>".to_string()
                } else {
                    workload.reasons.join(" | ")
                },
                "skipping unschedulable workload from optimization input"
            );
            continue;
        }
        if !allow_grouping || !can_group(workload) {
            ungrouped.push(workload.clone());
            continue;
        }
        grouped
            .entry(group_key(workload))
            .or_default()
            .push(workload.clone());
    }

    for (key, mut members) in grouped {
        members.sort_by(|a, b| a.name.cmp(&b.name));
        let base = members[0].clone();
        let current_node = dominant_current_node(&members);
        let feasible_nodes = feasible_groups(&base.feasible_node_names, &node_to_group);
        if feasible_nodes.is_empty() {
            warn!(
                workload = %format!("{}/{}", base.namespace, base.name),
                owner_kind = %base.owner_kind,
                owner_name = %base.owner_name,
                group_size = members.len(),
                feasible_nodes = if base.feasible_node_names.is_empty() {
                    "<none>".to_string()
                } else {
                    base.feasible_node_names.join(",")
                },
                current_node = if current_node.is_empty() {
                    "<unassigned>"
                } else {
                    current_node.as_str()
                },
                reasons = if base.reasons.is_empty() {
                    "<none>".to_string()
                } else {
                    base.reasons.join(" | ")
                },
                "grouped workload has no feasible node groups"
            );
        }
        input.workloads.push(OptimizationWorkload {
            id: grouped_id(&base, members.len() as i32, &key),
            namespace: base.namespace.clone(),
            name: grouped_name(&base, members.len() as i32),
            group_size: members.len() as i32,
            members: grouped_members(&members),
            current_node: node_to_group
                .get(&current_node)
                .cloned()
                .unwrap_or_default(),
            current_counts: grouped_current_counts(&members, &node_to_group),
            requests: scale_requests(&base.requests, members.len() as i32),
            recommended_requests: scale_requests(&base.recommended_requests, members.len() as i32),
            extended_resource_requests: scale_extended_requests(
                &base.extended_resource_requests,
                members.len() as i32,
            ),
            feasible_nodes,
            candidate_levels: scale_candidate_levels(&base.candidate_levels, members.len() as i32),
        });
    }

    ungrouped.sort_by(|a, b| match a.namespace.cmp(&b.namespace) {
        std::cmp::Ordering::Equal => a.name.cmp(&b.name),
        other => other,
    });

    for workload in ungrouped {
        let current_group = node_to_group
            .get(&workload.current_node)
            .cloned()
            .unwrap_or_default();
        let feasible_nodes = feasible_groups(&workload.feasible_node_names, &node_to_group);
        if feasible_nodes.is_empty() {
            warn!(
                workload = %format!("{}/{}", workload.namespace, workload.name),
                owner_kind = %workload.owner_kind,
                owner_name = %workload.owner_name,
                feasible_nodes = if workload.feasible_node_names.is_empty() {
                    "<none>".to_string()
                } else {
                    workload.feasible_node_names.join(",")
                },
                current_node = if workload.current_node.is_empty() {
                    "<unassigned>"
                } else {
                    workload.current_node.as_str()
                },
                reasons = if workload.reasons.is_empty() {
                    "<none>".to_string()
                } else {
                    workload.reasons.join(" | ")
                },
                "workload has no feasible node groups"
            );
        }
        input.workloads.push(OptimizationWorkload {
            id: format!("{}/{}", workload.namespace, workload.name),
            namespace: workload.namespace.clone(),
            name: workload.name.clone(),
            group_size: 1,
            members: vec![OptimizationWorkloadMember {
                namespace: workload.namespace.clone(),
                name: workload.name.clone(),
                current_node: workload.current_node.clone(),
            }],
            current_node: current_group.clone(),
            current_counts: single_current_count(&current_group),
            requests: workload.requests.clone(),
            recommended_requests: workload.recommended_requests.clone(),
            extended_resource_requests: workload.extended_resource_requests.clone(),
            feasible_nodes,
            candidate_levels: workload.candidate_levels.clone(),
        });
    }

    let mut anti_affinity_pairs = Vec::new();
    for workload in &cluster.workloads {
        if workload.has_required_anti_affinity {
            let id = format!("{}/{}", workload.namespace, workload.name);
            anti_affinity_pairs.push((id.clone(), id));
        }
    }
    input.anti_affinity_pairs = anti_affinity_pairs;

    info!(
        node_groups = input.nodes.len(),
        grouped_workloads = input.workloads.len(),
        raw_workloads = cluster.workloads.len(),
        skipped_unschedulable,
        allow_grouping,
        allow_node_grouping,
        "optimization input built"
    );
    input
}

fn build_nodes(
    nodes: &[NormalizedNode],
    allow_node_grouping: bool,
) -> (Vec<OptimizationNode>, HashMap<String, String>) {
    if !allow_node_grouping {
        let out = nodes
            .iter()
            .map(|node| OptimizationNode {
                name: node.name.clone(),
                pool: node.pool.clone(),
                count: 1,
                members: vec![node.name.clone()],
                price: node.price.clone(),
                effective_capacity: node.effective_capacity.clone(),
                extended_resources: node.extended_resources.clone(),
            })
            .collect::<Vec<_>>();
        let node_to_group = nodes
            .iter()
            .map(|node| (node.name.clone(), node.name.clone()))
            .collect::<HashMap<_, _>>();
        return (out, node_to_group);
    }

    let mut group_members: BTreeMap<String, Vec<NormalizedNode>> = BTreeMap::new();
    for node in nodes {
        group_members
            .entry(node_group_key(node))
            .or_default()
            .push(node.clone());
    }

    let mut out = Vec::with_capacity(group_members.len());
    let mut node_to_group = HashMap::with_capacity(nodes.len());

    for (idx, (_key, mut members)) in group_members.into_iter().enumerate() {
        members.sort_by(|a, b| a.name.cmp(&b.name));
        let base = members[0].clone();
        let group_id = format!("ng-{idx:03}", idx = idx + 1);
        let member_names: Vec<String> = members.iter().map(|m| m.name.clone()).collect();
        for member in &members {
            node_to_group.insert(member.name.clone(), group_id.clone());
        }
        out.push(OptimizationNode {
            name: group_id,
            pool: base.pool,
            count: members.len() as i32,
            members: member_names,
            price: base.price,
            effective_capacity: base.effective_capacity,
            extended_resources: base.extended_resources,
        });
    }

    (out, node_to_group)
}

fn node_group_key(node: &NormalizedNode) -> String {
    let mut parts = vec![
        node.pool.clone(),
        node.instance_type.clone(),
        node.labels
            .get("topology.kubernetes.io/zone")
            .cloned()
            .unwrap_or_default(),
        node.labels
            .get("failure-domain.beta.kubernetes.io/zone")
            .cloned()
            .unwrap_or_default(),
        format!("{:.4}", node.price.monthly),
        node.effective_capacity.milli_cpu.to_string(),
        node.effective_capacity.memory_bytes.to_string(),
        node.effective_capacity.ephemeral_storage.to_string(),
        node.effective_capacity.pods.to_string(),
        format_extended_resources(&node.extended_resources),
    ];
    for taint in &node.taints {
        parts.push(format!("{}={}:{}", taint.key, taint.value, taint.effect));
    }
    parts.join("|")
}

fn feasible_groups(node_names: &[String], node_to_group: &HashMap<String, String>) -> Vec<String> {
    let mut set = HashSet::new();
    for node_name in node_names {
        if let Some(group) = node_to_group.get(node_name) {
            if !group.is_empty() {
                set.insert(group.clone());
            }
        }
    }
    let mut out: Vec<String> = set.into_iter().collect();
    out.sort();
    out
}

fn can_group(workload: &NormalizedWorkload) -> bool {
    if workload.owner_kind == "StatefulSet"
        || workload.owner_kind == "DaemonSet"
        || workload.pinned_by_volume
    {
        return false;
    }
    if workload.owner_kind.is_empty() {
        return false;
    }
    if workload.has_required_affinity || workload.has_required_anti_affinity {
        return false;
    }
    !workload.feasible_node_names.is_empty()
}

fn group_key(workload: &NormalizedWorkload) -> String {
    let mut parts = vec![
        workload.namespace.clone(),
        workload.owner_kind.clone(),
        workload.owner_name.clone(),
        workload.requests.milli_cpu.to_string(),
        workload.requests.memory_bytes.to_string(),
        workload.requests.ephemeral_storage.to_string(),
        format_extended_resources(&workload.extended_resource_requests),
        workload.qos_class.clone(),
    ];
    parts.extend(workload.feasible_node_names.clone());
    parts.join("|")
}

fn grouped_id(workload: &NormalizedWorkload, size: i32, signature: &str) -> String {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    signature.hash(&mut hasher);
    format!(
        "{}/{}/{}#{}-{:x}",
        workload.namespace,
        workload.owner_kind,
        workload.owner_name,
        size,
        hasher.finish()
    )
}

fn grouped_name(workload: &NormalizedWorkload, size: i32) -> String {
    format!("{} x{}", workload.owner_name, size)
}

fn dominant_current_node(workloads: &[NormalizedWorkload]) -> String {
    let mut counts: HashMap<String, i32> = HashMap::new();
    let mut best = String::new();
    let mut best_count = 0;
    for workload in workloads {
        if workload.current_node.is_empty() {
            continue;
        }
        let count = counts.entry(workload.current_node.clone()).or_insert(0);
        *count += 1;
        if *count > best_count {
            best = workload.current_node.clone();
            best_count = *count;
        }
    }
    best
}

fn grouped_members(workloads: &[NormalizedWorkload]) -> Vec<OptimizationWorkloadMember> {
    workloads
        .iter()
        .map(|workload| OptimizationWorkloadMember {
            namespace: workload.namespace.clone(),
            name: workload.name.clone(),
            current_node: workload.current_node.clone(),
        })
        .collect()
}

fn grouped_current_counts(
    workloads: &[NormalizedWorkload],
    node_to_group: &HashMap<String, String>,
) -> HashMap<String, i32> {
    let mut counts = HashMap::new();
    for workload in workloads {
        if let Some(group) = node_to_group.get(&workload.current_node) {
            if !group.is_empty() {
                *counts.entry(group.clone()).or_insert(0) += 1;
            }
        }
    }
    counts
}

fn single_current_count(group: &str) -> HashMap<String, i32> {
    if group.is_empty() {
        return HashMap::new();
    }
    HashMap::from([(group.to_string(), 1)])
}

fn scale_requests(requests: &ResourceList, factor: i32) -> ResourceList {
    ResourceList {
        milli_cpu: requests.milli_cpu * i64::from(factor),
        memory_bytes: requests.memory_bytes * i64::from(factor),
        ephemeral_storage: requests.ephemeral_storage * i64::from(factor),
        pods: i64::from(factor),
    }
}

fn scale_candidate_levels(
    levels: &[crate::model::CandidateLevel],
    factor: i32,
) -> Vec<crate::model::CandidateLevel> {
    levels
        .iter()
        .map(|l| crate::model::CandidateLevel {
            key: l.key.clone(),
            label: l.label.clone(),
            requests: scale_requests(&l.requests, factor),
            risk_score: l.risk_score,
        })
        .collect()
}

fn scale_extended_requests(requests: &BTreeMap<String, i64>, factor: i32) -> BTreeMap<String, i64> {
    requests
        .iter()
        .map(|(name, value)| (name.clone(), *value * i64::from(factor)))
        .collect()
}

fn format_extended_resources(resources: &BTreeMap<String, i64>) -> String {
    resources
        .iter()
        .map(|(name, value)| format!("{name}={value}"))
        .collect::<Vec<_>>()
        .join(",")
}

fn per_replica_extended_requests(workload: &OptimizationWorkload) -> BTreeMap<String, i64> {
    if workload.group_size <= 1 {
        return workload.extended_resource_requests.clone();
    }
    let group_size = i64::from(workload.group_size);
    workload
        .extended_resource_requests
        .iter()
        .map(|(name, value)| (name.clone(), *value / group_size))
        .collect()
}

pub fn inject_candidate_nodes(
    input: &mut OptimizationInput,
    candidates: &[crate::model::CandidateInstanceType],
) {
    for candidate in candidates {
        let max_count = if candidate.max_count > 0 {
            candidate.max_count
        } else {
            3
        };
        let node_name = format!("candidate-{}", candidate.name);
        let node = OptimizationNode {
            name: node_name.clone(),
            pool: format!("candidate-{}", candidate.name),
            count: max_count,
            members: (0..max_count)
                .map(|i| format!("candidate-{}-{}", candidate.name, i))
                .collect(),
            price: crate::model::Money {
                currency: "USD".to_string(),
                monthly: candidate.monthly_price,
            },
            effective_capacity: crate::model::ResourceList {
                milli_cpu: candidate.cpu_milli,
                memory_bytes: candidate.memory_bytes,
                ephemeral_storage: 0,
                pods: 110,
            },
            extended_resources: std::collections::BTreeMap::new(),
        };
        input.nodes.push(node);
        for workload in &mut input.workloads {
            workload.feasible_nodes.push(node_name.clone());
        }
    }
}

pub fn baseline_solution(input: &OptimizationInput) -> crate::model::OptimizationSolution {
    debug!(
        node_groups = input.nodes.len(),
        workload_groups = input.workloads.len(),
        "building baseline solution"
    );
    let mut solution = crate::model::OptimizationSolution::default();
    for node in &input.nodes {
        solution.active_nodes.insert(node.name.clone(), node.count);
    }
    for workload in &input.workloads {
        if !workload.current_node.is_empty() {
            solution
                .assignments
                .insert(workload.id.clone(), workload.current_node.clone());
        }
        if !workload.current_counts.is_empty() {
            solution
                .assignment_counts
                .insert(workload.id.clone(), workload.current_counts.clone());
        }
    }
    solution
}

pub fn diagnose_current_assignment(input: &OptimizationInput) -> Vec<String> {
    let mut issues = Vec::new();

    for workload in &input.workloads {
        if workload.feasible_nodes.is_empty() {
            issues.push(format!(
                "workload {} has no feasible node groups",
                workload.id
            ));
            continue;
        }

        let assigned: i32 = workload.current_counts.values().copied().sum();
        if assigned != workload.group_size {
            issues.push(format!(
                "workload {} current assignment count {} does not match group_size {}",
                workload.id, assigned, workload.group_size
            ));
        }

        for node_name in workload.current_counts.keys() {
            if !workload.feasible_nodes.iter().any(|n| n == node_name) {
                issues.push(format!(
                    "workload {} currently assigned to infeasible node group {}",
                    workload.id, node_name
                ));
            }
        }
    }

    for node in &input.nodes {
        let mut cpu = 0_i64;
        let mut mem = 0_i64;
        let mut disk = 0_i64;
        let mut pods = 0_i64;
        let mut scalar_usage: BTreeMap<String, i64> = BTreeMap::new();

        for workload in &input.workloads {
            let count = i64::from(*workload.current_counts.get(&node.name).unwrap_or(&0));
            if count == 0 {
                continue;
            }
            let req = if workload.group_size <= 1 {
                workload.requests.clone()
            } else {
                let size = i64::from(workload.group_size);
                ResourceList {
                    milli_cpu: workload.requests.milli_cpu / size,
                    memory_bytes: workload.requests.memory_bytes / size,
                    ephemeral_storage: workload.requests.ephemeral_storage / size,
                    pods: 1,
                }
            };
            cpu += req.milli_cpu * count;
            mem += req.memory_bytes * count;
            disk += req.ephemeral_storage * count;
            pods += count;
            for (name, value) in per_replica_extended_requests(workload) {
                *scalar_usage.entry(name).or_insert(0) += value * count;
            }
        }

        let count = i64::from(node.count);
        let cpu_cap = node.effective_capacity.milli_cpu * count;
        let mem_cap = node.effective_capacity.memory_bytes * count;
        let disk_cap = node.effective_capacity.ephemeral_storage * count;
        let pod_cap = node.effective_capacity.pods * count;

        if cpu > cpu_cap {
            issues.push(format!(
                "node group {} current cpu {} exceeds capacity {}",
                node.name, cpu, cpu_cap
            ));
        }
        if mem > mem_cap {
            issues.push(format!(
                "node group {} current memory {} exceeds capacity {}",
                node.name, mem, mem_cap
            ));
        }
        if disk_cap > 0 && disk > disk_cap {
            issues.push(format!(
                "node group {} current ephemeral storage {} exceeds capacity {}",
                node.name, disk, disk_cap
            ));
        }
        if pod_cap > 0 && pods > pod_cap {
            issues.push(format!(
                "node group {} current pods {} exceeds capacity {}",
                node.name, pods, pod_cap
            ));
        }
        for (name, used) in scalar_usage {
            let cap = node.extended_resources.get(&name).copied().unwrap_or(0) * count;
            if used > cap {
                issues.push(format!(
                    "node group {} current resource {} {} exceeds capacity {}",
                    node.name, name, used, cap
                ));
            }
        }
    }

    issues.sort();
    issues.truncate(10);
    issues
}

pub fn summarize_current_assignment(input: &OptimizationInput) -> String {
    let mut no_feasible_groups = 0;
    let mut assignment_mismatches = 0;
    let mut infeasible_assignments = 0;
    let mut cpu_over = Vec::new();
    let mut mem_over = Vec::new();
    let mut disk_over = Vec::new();
    let mut pods_over = Vec::new();
    let mut scalar_over = Vec::new();

    for workload in &input.workloads {
        if workload.feasible_nodes.is_empty() {
            no_feasible_groups += 1;
            continue;
        }

        let assigned: i32 = workload.current_counts.values().copied().sum();
        if assigned != workload.group_size {
            assignment_mismatches += 1;
        }

        for node_name in workload.current_counts.keys() {
            if !workload.feasible_nodes.iter().any(|n| n == node_name) {
                infeasible_assignments += 1;
            }
        }
    }

    for node in &input.nodes {
        let mut cpu = 0_i64;
        let mut mem = 0_i64;
        let mut disk = 0_i64;
        let mut pods = 0_i64;
        let mut scalar_usage: BTreeMap<String, i64> = BTreeMap::new();

        for workload in &input.workloads {
            let count = i64::from(*workload.current_counts.get(&node.name).unwrap_or(&0));
            if count == 0 {
                continue;
            }
            let req = if workload.group_size <= 1 {
                workload.requests.clone()
            } else {
                let size = i64::from(workload.group_size);
                ResourceList {
                    milli_cpu: workload.requests.milli_cpu / size,
                    memory_bytes: workload.requests.memory_bytes / size,
                    ephemeral_storage: workload.requests.ephemeral_storage / size,
                    pods: 1,
                }
            };
            cpu += req.milli_cpu * count;
            mem += req.memory_bytes * count;
            disk += req.ephemeral_storage * count;
            pods += count;
            for (name, value) in per_replica_extended_requests(workload) {
                *scalar_usage.entry(name).or_insert(0) += value * count;
            }
        }

        let count = i64::from(node.count);
        let cpu_cap = node.effective_capacity.milli_cpu * count;
        let mem_cap = node.effective_capacity.memory_bytes * count;
        let disk_cap = node.effective_capacity.ephemeral_storage * count;
        let pod_cap = node.effective_capacity.pods * count;

        if cpu > cpu_cap {
            cpu_over.push(format!("{} {}/{}", node.name, cpu, cpu_cap));
        }
        if mem > mem_cap {
            mem_over.push(format!("{} {}/{}", node.name, mem, mem_cap));
        }
        if disk_cap > 0 && disk > disk_cap {
            disk_over.push(format!("{} {}/{}", node.name, disk, disk_cap));
        }
        if pod_cap > 0 && pods > pod_cap {
            pods_over.push(format!("{} {}/{}", node.name, pods, pod_cap));
        }
        for (name, used) in scalar_usage {
            let cap = node.extended_resources.get(&name).copied().unwrap_or(0) * count;
            if used > cap {
                scalar_over.push(format!("{} {} {}/{}", node.name, name, used, cap));
            }
        }
    }

    let mut parts = Vec::new();
    if no_feasible_groups > 0 {
        parts.push(format!(
            "{no_feasible_groups} workloads have no feasible node groups"
        ));
    }
    if assignment_mismatches > 0 {
        parts.push(format!(
            "{assignment_mismatches} workloads have broken current assignment counts"
        ));
    }
    if !cpu_over.is_empty() {
        parts.push(format!(
            "{} node groups are already over CPU in the current baseline ({})",
            cpu_over.len(),
            cpu_over
                .iter()
                .take(3)
                .cloned()
                .collect::<Vec<_>>()
                .join(", ")
        ));
    }
    if !mem_over.is_empty() {
        parts.push(format!(
            "{} node groups are already over memory in the current baseline ({})",
            mem_over.len(),
            mem_over
                .iter()
                .take(3)
                .cloned()
                .collect::<Vec<_>>()
                .join(", ")
        ));
    }
    if !disk_over.is_empty() {
        parts.push(format!(
            "{} node groups are already over ephemeral storage in the current baseline ({})",
            disk_over.len(),
            disk_over
                .iter()
                .take(3)
                .cloned()
                .collect::<Vec<_>>()
                .join(", ")
        ));
    }
    if !pods_over.is_empty() {
        parts.push(format!(
            "{} node groups are already over pod capacity in the current baseline ({})",
            pods_over.len(),
            pods_over
                .iter()
                .take(3)
                .cloned()
                .collect::<Vec<_>>()
                .join(", ")
        ));
    }
    if !scalar_over.is_empty() {
        parts.push(format!(
            "{} node groups are already over extended resource capacity in the current baseline ({})",
            scalar_over.len(),
            scalar_over.iter().take(3).cloned().collect::<Vec<_>>().join(", ")
        ));
    }
    if infeasible_assignments > 0 {
        parts.push(format!(
            "{infeasible_assignments} current workload assignments land on infeasible node groups"
        ));
    }

    if parts.is_empty() {
        "current baseline satisfies all modeled constraints".to_string()
    } else {
        format!(
            "current baseline is infeasible under modeled constraints: {}",
            parts.join("; ")
        )
    }
}

#[cfg(test)]
mod tests {
    use super::{build_input, build_input_strict, summarize_current_assignment};
    use crate::model::{
        Money, NormalizedCluster, NormalizedNode, NormalizedWorkload, OptimizationInput,
        OptimizationNode, OptimizationWorkload, ResourceList,
    };
    use std::collections::HashMap;

    #[test]
    fn skips_unschedulable_workloads_when_enabled() {
        let cluster = NormalizedCluster {
            nodes: vec![NormalizedNode {
                name: "node-a".to_string(),
                effective_capacity: ResourceList {
                    milli_cpu: 1000,
                    memory_bytes: 4096,
                    ephemeral_storage: 0,
                    pods: 32,
                },
                price: Money {
                    currency: "USD".to_string(),
                    monthly: 10.0,
                },
                ..Default::default()
            }],
            workloads: vec![
                NormalizedWorkload {
                    namespace: "ns".to_string(),
                    name: "schedulable".to_string(),
                    owner_kind: "Deployment".to_string(),
                    owner_name: "schedulable".to_string(),
                    feasible_node_names: vec!["node-a".to_string()],
                    requests: ResourceList {
                        milli_cpu: 100,
                        memory_bytes: 128,
                        ephemeral_storage: 0,
                        pods: 1,
                    },
                    ..Default::default()
                },
                NormalizedWorkload {
                    namespace: "ns".to_string(),
                    name: "blocked".to_string(),
                    owner_kind: "StatefulSet".to_string(),
                    owner_name: "blocked".to_string(),
                    reasons: vec!["insufficient effective capacity".to_string()],
                    ..Default::default()
                },
            ],
            ..Default::default()
        };

        let input = build_input(&cluster, true);

        assert_eq!(input.workloads.len(), 1);
        assert_eq!(input.workloads[0].name, "schedulable x1");
    }

    #[test]
    fn summarizes_baseline_capacity_and_feasibility_failures() {
        let input = OptimizationInput {
            nodes: vec![OptimizationNode {
                name: "ng-001".to_string(),
                count: 1,
                effective_capacity: ResourceList {
                    milli_cpu: 100,
                    memory_bytes: 200,
                    ephemeral_storage: 0,
                    pods: 10,
                },
                ..Default::default()
            }],
            workloads: vec![OptimizationWorkload {
                id: "ns/pod-a".to_string(),
                namespace: "ns".to_string(),
                name: "pod-a".to_string(),
                group_size: 1,
                current_node: "ng-001".to_string(),
                current_counts: HashMap::from([("ng-001".to_string(), 1)]),
                requests: ResourceList {
                    milli_cpu: 150,
                    memory_bytes: 250,
                    ephemeral_storage: 0,
                    pods: 1,
                },
                feasible_nodes: vec!["ng-002".to_string()],
                ..Default::default()
            }],
            anti_affinity_pairs: Vec::new(),
        };

        let summary = summarize_current_assignment(&input);

        assert!(summary.contains("current baseline is infeasible under modeled constraints"));
        assert!(summary.contains("over CPU"));
        assert!(summary.contains("over memory"));
        assert!(summary.contains("infeasible node groups"));
    }

    #[test]
    fn strict_input_keeps_nodes_ungrouped() {
        let cluster = NormalizedCluster {
            nodes: vec![
                NormalizedNode {
                    name: "node-a".to_string(),
                    pool: "pool-1".to_string(),
                    effective_capacity: ResourceList {
                        milli_cpu: 1000,
                        memory_bytes: 4096,
                        ephemeral_storage: 0,
                        pods: 32,
                    },
                    price: Money {
                        currency: "USD".to_string(),
                        monthly: 10.0,
                    },
                    ..Default::default()
                },
                NormalizedNode {
                    name: "node-b".to_string(),
                    pool: "pool-1".to_string(),
                    effective_capacity: ResourceList {
                        milli_cpu: 1000,
                        memory_bytes: 4096,
                        ephemeral_storage: 0,
                        pods: 32,
                    },
                    price: Money {
                        currency: "USD".to_string(),
                        monthly: 10.0,
                    },
                    ..Default::default()
                },
            ],
            workloads: vec![NormalizedWorkload {
                namespace: "ns".to_string(),
                name: "pod-a".to_string(),
                owner_kind: "Deployment".to_string(),
                owner_name: "app".to_string(),
                current_node: "node-a".to_string(),
                feasible_node_names: vec!["node-a".to_string(), "node-b".to_string()],
                requests: ResourceList {
                    milli_cpu: 100,
                    memory_bytes: 128,
                    ephemeral_storage: 0,
                    pods: 1,
                },
                ..Default::default()
            }],
            ..Default::default()
        };

        let input = build_input_strict(&cluster, false);

        assert_eq!(input.nodes.len(), 2);
        assert_eq!(input.nodes[0].count, 1);
        assert_eq!(input.nodes[1].count, 1);
        assert_eq!(
            input.workloads[0].feasible_nodes,
            vec!["node-a".to_string(), "node-b".to_string()]
        );
        assert_eq!(input.workloads[0].current_node, "node-a");
    }
}
