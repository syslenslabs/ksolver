use crate::model::{
    AutoscalerBlockerSummary, Money, NormalizedCluster, OptimizationInput, OptimizationPlan,
    OptimizationSolution, OptimizationWorkloadMember, PodMove, ResourceAllocation, ResourceList,
    ResourceSummary, SolverInfo,
};
use std::collections::{HashMap, HashSet};

#[derive(Default)]
pub struct Planner;

impl Planner {
    pub fn new() -> Self {
        Self
    }

    pub fn build_plan(
        &self,
        cluster: &NormalizedCluster,
        input: &OptimizationInput,
        solution: &OptimizationSolution,
        info: SolverInfo,
    ) -> OptimizationPlan {
        let mut plan = OptimizationPlan {
            current_monthly_cost: cluster.current_monthly_cost.clone(),
            blockers: cluster.blockers.clone(),
            solver: info,
            ..Default::default()
        };

        let mut group_members: HashMap<String, Vec<String>> = HashMap::new();
        let mut member_price: HashMap<String, Money> = HashMap::new();
        for node in &cluster.nodes {
            member_price.insert(node.name.clone(), node.price.clone());
        }
        for node in &input.nodes {
            let mut members = node.members.clone();
            members.sort();
            for member in &members {
                if !member_price.contains_key(member) {
                    member_price.insert(member.clone(), node.price.clone());
                }
            }
            group_members.insert(node.name.clone(), members);
        }

        let mut active = Vec::new();
        for (node_name, count) in &solution.active_nodes {
            if *count <= 0 {
                continue;
            }
            let members = group_members
                .get(node_name)
                .cloned()
                .unwrap_or_else(|| vec![node_name.clone()]);
            let limit = (*count as usize).min(members.len());
            for member_name in members.into_iter().take(limit) {
                active.push(member_name.clone());
                if let Some(price) = member_price.get(&member_name) {
                    plan.optimized_monthly_cost.monthly += price.monthly;
                    if plan.optimized_monthly_cost.currency.is_empty() {
                        plan.optimized_monthly_cost.currency = price.currency.clone();
                    }
                }
            }
        }
        active.sort();
        plan.active_nodes = active;

        for workload in &input.workloads {
            let mut target_counts = solution
                .assignment_counts
                .get(&workload.id)
                .cloned()
                .unwrap_or_default();
            if target_counts.is_empty() {
                if let Some(target) = solution.assignments.get(&workload.id) {
                    target_counts.insert(target.clone(), workload.group_size);
                } else {
                    continue;
                }
            }
            let target_nodes =
                expand_target_nodes(&target_counts, &group_members, &solution.active_nodes);
            if target_nodes.is_empty() {
                continue;
            }
            plan.recommended_moves
                .extend(build_moves_for_members(&workload.members, &target_nodes));
        }

        if plan.optimized_monthly_cost.currency.is_empty() {
            plan.optimized_monthly_cost.currency = plan.current_monthly_cost.currency.clone();
        }
        plan.savings_monthly = Money {
            currency: plan.current_monthly_cost.currency.clone(),
            monthly: plan.current_monthly_cost.monthly - plan.optimized_monthly_cost.monthly,
        };
        plan.resource_summary = build_resource_summary(cluster, input, solution);

        let mut fleet = Vec::new();
        for node in &input.nodes {
            let count = solution.active_nodes.get(&node.name).copied().unwrap_or(0);
            if count > 0 {
                let is_candidate = node.name.starts_with("candidate-");
                fleet.push(crate::model::FleetNode {
                    instance_type: node.name.clone(),
                    count,
                    monthly_cost: Money {
                        currency: node.price.currency.clone(),
                        monthly: node.price.monthly * count as f64,
                    },
                    is_candidate,
                });
            }
        }
        fleet.sort_by(|a, b| {
            b.monthly_cost
                .monthly
                .partial_cmp(&a.monthly_cost.monthly)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        plan.fleet_recommendation = fleet;

        plan
    }
}

fn build_resource_summary(
    cluster: &NormalizedCluster,
    input: &OptimizationInput,
    solution: &OptimizationSolution,
) -> ResourceSummary {
    let current_resources = current_node_resources(cluster);
    let optimized_resources = optimized_node_resources(cluster, input, solution);

    ResourceSummary {
        current: summarize_allocation(
            &current_resources,
            cluster
                .workloads
                .iter()
                .map(|w| w.requests.clone())
                .collect(),
        ),
        optimized: summarize_allocation(&optimized_resources, assigned_requests(input, solution)),
        current_fragmentation_percent: fragmentation_percent(&current_resources),
        optimized_fragmentation_percent: fragmentation_percent(&optimized_resources),
        pool_summaries: cluster.pool_summaries.clone(),
        autoscaler_blockers: summarize_autoscaler_blockers(cluster, solution),
    }
}

fn summarize_autoscaler_blockers(
    cluster: &NormalizedCluster,
    solution: &OptimizationSolution,
) -> Vec<AutoscalerBlockerSummary> {
    let active_nodes = solution
        .active_nodes
        .iter()
        .filter(|(_, count)| **count > 0)
        .map(|(name, _)| name.as_str())
        .collect::<HashSet<_>>();

    let removable_nodes = cluster
        .nodes
        .iter()
        .filter(|node| !active_nodes.contains(node.name.as_str()))
        .map(|node| (node.name.as_str(), node))
        .collect::<HashMap<_, _>>();

    let mut blocked_nodes = HashSet::new();
    let mut blocked_workloads = HashSet::new();
    let mut blocked_monthly_cost = 0.0_f64;
    let mut currency = cluster.current_monthly_cost.currency.clone();

    for workload in &cluster.workloads {
        if !workload.autoscaler_not_safe_to_evict {
            continue;
        }
        let Some(node) = removable_nodes.get(workload.current_node.as_str()) else {
            continue;
        };
        blocked_workloads.insert(format!("{}/{}", workload.namespace, workload.name));
        if blocked_nodes.insert(node.name.clone()) {
            blocked_monthly_cost += node.price.monthly;
            if currency.is_empty() {
                currency = node.price.currency.clone();
            }
        }
    }

    if blocked_nodes.is_empty() {
        return Vec::new();
    }

    let mut blocked_nodes = blocked_nodes.into_iter().collect::<Vec<_>>();
    blocked_nodes.sort();

    vec![AutoscalerBlockerSummary {
        kind: "cluster autoscaler safe-to-evict=false".to_string(),
        blocked_monthly_cost: Money {
            currency,
            monthly: blocked_monthly_cost,
        },
        blocked_node_count: blocked_nodes.len() as i32,
        blocked_workload_count: blocked_workloads.len() as i32,
        blocked_nodes,
    }]
}

fn current_node_resources(
    cluster: &NormalizedCluster,
) -> Vec<(String, ResourceList, ResourceList)> {
    let effective_by_node = cluster
        .nodes
        .iter()
        .map(|node| (node.name.clone(), node.effective_capacity.clone()))
        .collect::<HashMap<_, _>>();

    let mut used_by_node: HashMap<String, ResourceList> = HashMap::new();
    for workload in &cluster.workloads {
        if workload.current_node.is_empty() {
            continue;
        }
        let entry = used_by_node
            .entry(workload.current_node.clone())
            .or_default();
        *entry = add_resources(entry, &workload.requests);
    }

    let mut out = Vec::new();
    for node in &cluster.nodes {
        let used = used_by_node.remove(&node.name).unwrap_or_default();
        let capacity = effective_by_node
            .get(&node.name)
            .cloned()
            .unwrap_or_else(ResourceList::default);
        out.push((node.name.clone(), capacity, used));
    }
    out
}

fn optimized_node_resources(
    cluster: &NormalizedCluster,
    input: &OptimizationInput,
    solution: &OptimizationSolution,
) -> Vec<(String, ResourceList, ResourceList)> {
    let effective_by_node = cluster
        .nodes
        .iter()
        .map(|node| (node.name.clone(), node.effective_capacity.clone()))
        .collect::<HashMap<_, _>>();

    let mut used_by_node: HashMap<String, ResourceList> = HashMap::new();
    let active_members = active_member_nodes(input, solution);

    for workload in &input.workloads {
        let target_counts = solution
            .assignment_counts
            .get(&workload.id)
            .cloned()
            .or_else(|| {
                solution
                    .assignments
                    .get(&workload.id)
                    .map(|node| HashMap::from([(node.clone(), workload.group_size)]))
            })
            .unwrap_or_default();
        if target_counts.is_empty() {
            continue;
        }
        let per_replica = per_replica_requests(&workload.requests, workload.group_size);
        let target_nodes =
            expand_target_nodes(&target_counts, &active_members, &solution.active_nodes);
        for node_name in target_nodes {
            let entry = used_by_node.entry(node_name).or_default();
            *entry = add_resources(entry, &per_replica);
        }
    }

    let mut active_nodes = solution
        .active_nodes
        .iter()
        .filter(|(_, count)| **count > 0)
        .flat_map(|(group_name, count)| {
            active_members
                .get(group_name)
                .cloned()
                .unwrap_or_else(|| vec![group_name.clone()])
                .into_iter()
                .take(*count as usize)
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    active_nodes.sort();
    active_nodes.dedup();

    active_nodes
        .into_iter()
        .map(|node_name| {
            let capacity = effective_by_node
                .get(&node_name)
                .cloned()
                .unwrap_or_else(ResourceList::default);
            let used = used_by_node.remove(&node_name).unwrap_or_default();
            (node_name, capacity, used)
        })
        .collect()
}

fn summarize_allocation(
    node_resources: &[(String, ResourceList, ResourceList)],
    requests: Vec<ResourceList>,
) -> ResourceAllocation {
    let total_capacity = node_resources
        .iter()
        .fold(ResourceList::default(), |acc, (_, capacity, _)| {
            add_resources(&acc, capacity)
        });
    let allocated = requests
        .into_iter()
        .fold(ResourceList::default(), |acc, req| {
            add_resources(&acc, &req)
        });

    ResourceAllocation {
        allocated_cpu_request_milli: allocated.milli_cpu,
        unallocated_cpu_request_milli: (total_capacity.milli_cpu - allocated.milli_cpu).max(0),
        total_cpu_capacity_milli: total_capacity.milli_cpu,
        allocated_memory_request_bytes: allocated.memory_bytes,
        unallocated_memory_request_bytes: (total_capacity.memory_bytes - allocated.memory_bytes)
            .max(0),
        total_memory_capacity_bytes: total_capacity.memory_bytes,
    }
}

fn fragmentation_percent(node_resources: &[(String, ResourceList, ResourceList)]) -> f64 {
    let mut weighted = 0.0_f64;
    let mut total_weight = 0.0_f64;

    for (_, capacity, used) in node_resources {
        let cpu_cap = capacity.milli_cpu.max(0) as f64;
        let mem_cap = capacity.memory_bytes.max(0) as f64;
        if cpu_cap <= 0.0 || mem_cap <= 0.0 {
            continue;
        }
        let cpu_unused = ((capacity.milli_cpu - used.milli_cpu).max(0) as f64) / cpu_cap;
        let mem_unused = ((capacity.memory_bytes - used.memory_bytes).max(0) as f64) / mem_cap;
        let imbalance = (cpu_unused - mem_unused).abs();
        let weight = cpu_cap + mem_cap;
        weighted += imbalance * weight;
        total_weight += weight;
    }

    if total_weight == 0.0 {
        0.0
    } else {
        (weighted / total_weight) * 100.0
    }
}

fn active_member_nodes(
    input: &OptimizationInput,
    _solution: &OptimizationSolution,
) -> HashMap<String, Vec<String>> {
    input
        .nodes
        .iter()
        .map(|node| {
            let mut members = node.members.clone();
            members.sort();
            (node.name.clone(), members)
        })
        .collect()
}

fn assigned_requests(
    input: &OptimizationInput,
    solution: &OptimizationSolution,
) -> Vec<ResourceList> {
    let active_members = active_member_nodes(input, solution);
    let mut out = Vec::new();

    for workload in &input.workloads {
        let target_counts = solution
            .assignment_counts
            .get(&workload.id)
            .cloned()
            .or_else(|| {
                solution
                    .assignments
                    .get(&workload.id)
                    .map(|node| HashMap::from([(node.clone(), workload.group_size)]))
            })
            .unwrap_or_default();
        if target_counts.is_empty() {
            continue;
        }
        let per_replica = per_replica_requests(&workload.requests, workload.group_size);
        let target_nodes =
            expand_target_nodes(&target_counts, &active_members, &solution.active_nodes);
        out.extend(target_nodes.into_iter().map(|_| per_replica.clone()));
    }

    out
}

fn per_replica_requests(requests: &ResourceList, group_size: i32) -> ResourceList {
    if group_size <= 1 {
        return requests.clone();
    }
    let group_size = i64::from(group_size);
    ResourceList {
        milli_cpu: requests.milli_cpu / group_size,
        memory_bytes: requests.memory_bytes / group_size,
        ephemeral_storage: requests.ephemeral_storage / group_size,
        pods: 1,
    }
}

fn add_resources(left: &ResourceList, right: &ResourceList) -> ResourceList {
    ResourceList {
        milli_cpu: left.milli_cpu + right.milli_cpu,
        memory_bytes: left.memory_bytes + right.memory_bytes,
        ephemeral_storage: left.ephemeral_storage + right.ephemeral_storage,
        pods: left.pods + right.pods,
    }
}

fn expand_target_nodes(
    target_counts: &HashMap<String, i32>,
    group_members: &HashMap<String, Vec<String>>,
    active_counts: &HashMap<String, i32>,
) -> Vec<String> {
    let mut group_names: Vec<String> = target_counts.keys().cloned().collect();
    group_names.sort();
    let mut targets = Vec::new();
    for group in group_names {
        let count = *target_counts.get(&group).unwrap_or(&0);
        if count <= 0 {
            continue;
        }
        let mut members = group_members
            .get(&group)
            .cloned()
            .unwrap_or_else(|| vec![group.clone()]);
        if members.is_empty() {
            for _ in 0..count {
                targets.push(group.clone());
            }
            continue;
        }
        let limit = active_counts
            .get(&group)
            .copied()
            .filter(|v| *v > 0)
            .map(|v| (v as usize).min(members.len()))
            .unwrap_or(members.len());
        members.truncate(limit);
        for i in 0..count {
            targets.push(members[(i as usize) % members.len()].clone());
        }
    }
    targets
}

fn build_moves_for_members(
    members: &[OptimizationWorkloadMember],
    target_nodes: &[String],
) -> Vec<PodMove> {
    if members.is_empty() || target_nodes.is_empty() {
        return Vec::new();
    }
    let slots = target_nodes.to_vec();
    let mut used = vec![false; slots.len()];
    let mut moves = Vec::new();

    for member in members {
        if member.current_node.is_empty() {
            continue;
        }
        if let Some(idx) = first_unused_matching_slot(&slots, &used, &member.current_node) {
            used[idx] = true;
            continue;
        }
        let Some(idx) = first_unused_slot(&used) else {
            break;
        };
        used[idx] = true;
        if member.current_node == slots[idx] {
            continue;
        }
        moves.push(PodMove {
            namespace: member.namespace.clone(),
            pod: member.name.clone(),
            from_node: member.current_node.clone(),
            to_node: slots[idx].clone(),
            reason: "cost and packing optimization".to_string(),
        });
    }

    moves
}

fn first_unused_matching_slot(slots: &[String], used: &[bool], node: &str) -> Option<usize> {
    slots.iter().enumerate().find_map(|(i, slot)| {
        if !used[i] && slot == node {
            Some(i)
        } else {
            None
        }
    })
}

fn first_unused_slot(used: &[bool]) -> Option<usize> {
    used.iter()
        .enumerate()
        .find_map(|(i, used)| if !used { Some(i) } else { None })
}

#[cfg(test)]
mod tests {
    use super::Planner;
    use crate::model::{
        Money, NormalizedCluster, NormalizedNode, NormalizedWorkload, OptimizationInput,
        OptimizationNode, OptimizationSolution, OptimizationWorkload, ResourceList, SolverInfo,
    };
    use std::collections::HashMap;

    #[test]
    fn current_fragmentation_uses_real_current_node_assignments() {
        let cluster = NormalizedCluster {
            current_monthly_cost: Money {
                currency: "USD".to_string(),
                monthly: 100.0,
            },
            nodes: vec![
                NormalizedNode {
                    name: "node-a".to_string(),
                    effective_capacity: ResourceList {
                        milli_cpu: 1000,
                        memory_bytes: 1000,
                        ..Default::default()
                    },
                    ..Default::default()
                },
                NormalizedNode {
                    name: "node-b".to_string(),
                    effective_capacity: ResourceList {
                        milli_cpu: 1000,
                        memory_bytes: 1000,
                        ..Default::default()
                    },
                    ..Default::default()
                },
            ],
            workloads: vec![
                NormalizedWorkload {
                    namespace: "ns".to_string(),
                    name: "w1".to_string(),
                    current_node: "node-a".to_string(),
                    requests: ResourceList {
                        milli_cpu: 900,
                        memory_bytes: 100,
                        pods: 1,
                        ..Default::default()
                    },
                    ..Default::default()
                },
                NormalizedWorkload {
                    namespace: "ns".to_string(),
                    name: "w2".to_string(),
                    current_node: "node-b".to_string(),
                    requests: ResourceList {
                        milli_cpu: 100,
                        memory_bytes: 900,
                        pods: 1,
                        ..Default::default()
                    },
                    ..Default::default()
                },
            ],
            ..Default::default()
        };

        let input = OptimizationInput {
            nodes: vec![OptimizationNode {
                name: "group-1".to_string(),
                count: 2,
                members: vec!["node-a".to_string(), "node-b".to_string()],
                effective_capacity: ResourceList {
                    milli_cpu: 1000,
                    memory_bytes: 1000,
                    ..Default::default()
                },
                ..Default::default()
            }],
            workloads: vec![OptimizationWorkload {
                id: "grouped".to_string(),
                namespace: "ns".to_string(),
                name: "grouped".to_string(),
                group_size: 2,
                requests: ResourceList {
                    milli_cpu: 1000,
                    memory_bytes: 1000,
                    pods: 2,
                    ..Default::default()
                },
                current_counts: HashMap::from([("group-1".to_string(), 2)]),
                feasible_nodes: vec!["group-1".to_string()],
                ..Default::default()
            }],
            anti_affinity_pairs: Vec::new(),
        };

        let solution = OptimizationSolution {
            active_nodes: HashMap::from([("group-1".to_string(), 2)]),
            assignment_counts: HashMap::from([(
                "grouped".to_string(),
                HashMap::from([("group-1".to_string(), 2)]),
            )]),
            ..Default::default()
        };

        let plan = Planner::default().build_plan(
            &cluster,
            &input,
            &solution,
            SolverInfo {
                name: "cp-sat-rust".to_string(),
                available: true,
                status: "ok".to_string(),
            },
        );

        assert!(plan.resource_summary.current_fragmentation_percent > 0.0);
    }
}
