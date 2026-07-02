use crate::model::{DisruptionBudget, LabelSelectorReq, NormalizedCluster, NormalizedWorkload};
use crate::scheduler::pod_filter::PendingGpuPod;
use crate::scheduler::trace::{
    DecisionTrace, PodPlacement, RepairAction, RepairMetrics, RepairPlan, RepairSkip,
};
use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};

const GPU_RESOURCE: &str = "nvidia.com/gpu";
const PREEMPTION_DISRUPTION_PENALTY: i32 = 100;

fn is_gpu_resource(name: &str) -> bool {
    name == GPU_RESOURCE || name.starts_with("nvidia.com/mig-")
}

fn workload_gpu(w: &NormalizedWorkload) -> i64 {
    w.extended_resource_requests
        .iter()
        .filter(|(name, _)| is_gpu_resource(name))
        .map(|(_, qty)| *qty)
        .sum()
}

fn pending_key(p: &PendingGpuPod) -> String {
    match &p.gang_key {
        Some(g) => format!("gang:{g}"),
        None => format!("pod:{}/{}", p.namespace, p.name),
    }
}

fn pending_target_name(p: &PendingGpuPod) -> String {
    match &p.gang_key {
        Some(g) => g.clone(),
        None => format!("{}/{}", p.namespace, p.name),
    }
}

fn node_gpu_capacity(cluster: &NormalizedCluster) -> BTreeMap<String, i64> {
    cluster
        .nodes
        .iter()
        .map(|n| {
            let gpu = n
                .extended_resources
                .iter()
                .filter(|(name, _)| is_gpu_resource(name))
                .map(|(_, qty)| *qty)
                .sum();
            (n.name.clone(), gpu)
        })
        .collect()
}

fn running_gpu_by_node<'a>(
    cluster: &'a NormalizedCluster,
) -> BTreeMap<String, Vec<&'a NormalizedWorkload>> {
    let mut out: BTreeMap<String, Vec<&NormalizedWorkload>> = BTreeMap::new();
    for w in &cluster.workloads {
        if w.current_node.is_empty() || workload_gpu(w) <= 0 {
            continue;
        }
        out.entry(w.current_node.clone()).or_default().push(w);
    }
    out
}

fn free_gpu_by_node(cluster: &NormalizedCluster) -> BTreeMap<String, i64> {
    let capacity = node_gpu_capacity(cluster);
    let mut used: BTreeMap<String, i64> = BTreeMap::new();
    for w in &cluster.workloads {
        if !w.current_node.is_empty() {
            *used.entry(w.current_node.clone()).or_default() += workload_gpu(w);
        }
    }
    capacity
        .into_iter()
        .map(|(node, cap)| {
            let free = cap - used.get(&node).copied().unwrap_or(0);
            (node, free.max(0))
        })
        .collect()
}

fn reserve_migration_target(
    free_by_node: &mut BTreeMap<String, i64>,
    w: &NormalizedWorkload,
    from_node: &str,
) -> Option<String> {
    let needed = workload_gpu(w);
    if needed <= 0 {
        return None;
    }
    let target = free_by_node
        .iter()
        .find(|(node, free)| {
            node.as_str() != from_node
                && **free >= needed
                && (w.feasible_node_names.is_empty() || w.feasible_node_names.contains(*node))
        })
        .map(|(node, _)| node.clone())?;
    if let Some(free) = free_by_node.get_mut(&target) {
        *free -= needed;
    }
    Some(target)
}

#[derive(Default)]
struct PendingGroup {
    target: String,
    gpu_request: i64,
    unmodeled_dra_members: usize,
    priority: i64,
    business_value: i64,
    deadline_unix_seconds: i64,
    latest_start_unix_seconds: i64,
    queue_wait_seconds: i64,
    all_unplaced: bool,
    vram_blocked: bool,
    modeled_workload_models: usize,
    missing_workload_models: usize,
    feasible_nodes: Option<BTreeSet<String>>,
}

#[derive(Debug, Clone, Copy)]
pub struct RepairOptions {
    pub max_candidates_per_node: usize,
}

impl Default for RepairOptions {
    fn default() -> Self {
        Self {
            max_candidates_per_node: 8,
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct RepairAdvice {
    pub plans: Vec<RepairPlan>,
    pub notes: Vec<String>,
    pub metrics: RepairMetrics,
}

#[derive(Clone)]
struct RepairCandidate<'a> {
    workload: &'a NormalizedWorkload,
    gpu: i64,
    disruption_cost: i32,
    pdb_keys: Vec<String>,
}

fn repair_disruption_cost(w: &NormalizedWorkload) -> i32 {
    let checkpoint_minutes = (w.checkpoint_age_seconds.max(0) / 60).min(i32::MAX as i64) as i32;
    let running_age_hours = (w.running_age_seconds.max(0) / 3600).min(24) as i32;
    w.disruption_cost
        .max(0)
        .saturating_add(checkpoint_minutes)
        .saturating_add(running_age_hours)
        .saturating_add(w.progress_percent.clamp(0, 100))
}

fn priority_repair_skip_reason(
    target_priority: i64,
    candidate_priority: i64,
) -> Option<&'static str> {
    if candidate_priority > target_priority {
        Some("blocked by higher-priority running workload")
    } else if target_priority > 0 && candidate_priority == target_priority {
        Some("blocked by equal-priority running workload")
    } else {
        None
    }
}

fn latest_start_unix_seconds(w: &NormalizedWorkload) -> i64 {
    if w.deadline_unix_seconds <= 0 || w.predicted_runtime_seconds <= 0 {
        0
    } else {
        w.deadline_unix_seconds
            .saturating_sub(w.predicted_runtime_seconds)
    }
}

fn policy_repair_skip_reason(
    target: &PendingGroup,
    candidate: &NormalizedWorkload,
) -> Option<&'static str> {
    if candidate.priority != target.priority {
        return None;
    }
    if candidate.business_value > target.business_value {
        return Some("blocked by higher-business-value running workload");
    }
    if candidate.business_value < target.business_value {
        return None;
    }

    let candidate_latest_start = latest_start_unix_seconds(candidate);
    match (
        target.latest_start_unix_seconds > 0,
        candidate_latest_start > 0,
    ) {
        (false, true) => Some("blocked by more urgent deadline running workload"),
        (true, true) if candidate_latest_start < target.latest_start_unix_seconds => {
            Some("blocked by more urgent deadline running workload")
        }
        _ => None,
    }
}

fn repair_deadline_order(a: &RepairPlan, b: &RepairPlan) -> Ordering {
    match (
        a.target_latest_start_unix_seconds > 0,
        b.target_latest_start_unix_seconds > 0,
    ) {
        (true, true) => a
            .target_latest_start_unix_seconds
            .cmp(&b.target_latest_start_unix_seconds),
        (true, false) => Ordering::Less,
        (false, true) => Ordering::Greater,
        (false, false) => Ordering::Equal,
    }
}

fn req_matches(req: &LabelSelectorReq, labels: &BTreeMap<String, String>) -> bool {
    match req.operator.as_str() {
        "In" => labels
            .get(&req.key)
            .map(|v| req.values.iter().any(|allowed| allowed == v))
            .unwrap_or(false),
        "NotIn" => labels
            .get(&req.key)
            .map(|v| !req.values.iter().any(|blocked| blocked == v))
            .unwrap_or(true),
        "Exists" => labels.contains_key(&req.key),
        "DoesNotExist" => !labels.contains_key(&req.key),
        _ => false,
    }
}

fn selector_matches(selector: &[LabelSelectorReq], labels: &BTreeMap<String, String>) -> bool {
    selector.iter().all(|req| req_matches(req, labels))
}

fn pdb_key(pdb: &DisruptionBudget) -> String {
    format!("{}/{}", pdb.namespace, pdb.name)
}

fn pdb_budget_by_key(pdbs: &[DisruptionBudget]) -> BTreeMap<String, i32> {
    pdbs.iter()
        .filter(|pdb| pdb.selector_modeled)
        .map(|pdb| (pdb_key(pdb), pdb.disruptions_allowed.max(0)))
        .collect()
}

fn matching_pdb_keys(
    workload: &NormalizedWorkload,
    pdbs: &[DisruptionBudget],
) -> Result<Vec<String>, String> {
    let mut keys = Vec::new();
    for pdb in pdbs
        .iter()
        .filter(|pdb| pdb.namespace == workload.namespace)
    {
        if !pdb.selector_modeled {
            return Err(format!(
                "blocked by unmodeled PDB selector {}/{}",
                pdb.namespace, pdb.name
            ));
        }
        if selector_matches(&pdb.selector, &workload.labels) {
            if pdb.disruptions_allowed <= 0 {
                return Err(format!(
                    "blocked by PDB {}/{} budget",
                    pdb.namespace, pdb.name
                ));
            }
            keys.push(pdb_key(pdb));
        }
    }
    Ok(keys)
}

fn repair_action_for_selected(
    candidate: &RepairCandidate<'_>,
    source_node: &str,
    migration_free: &mut BTreeMap<String, i64>,
    target: &str,
) -> Option<RepairAction> {
    let w = candidate.workload;
    let migration_target = if w.migration_allowed {
        reserve_migration_target(migration_free, w, source_node)
    } else {
        None
    };
    let (action, disruption_cost) = if migration_target.is_some() {
        ("migrate".to_string(), candidate.disruption_cost)
    } else if w.preemption_allowed {
        (
            "preempt".to_string(),
            candidate
                .disruption_cost
                .saturating_add(PREEMPTION_DISRUPTION_PENALTY),
        )
    } else {
        return None;
    };
    let reason = if let Some(to_node) = &migration_target {
        format!(
            "move to {to_node} to free {} GPU on {source_node} for pending {target}",
            candidate.gpu
        )
    } else {
        format!(
            "free {} GPU on {source_node} for pending {target}",
            candidate.gpu
        )
    };
    Some(RepairAction {
        action,
        namespace: w.namespace.clone(),
        pod: w.name.clone(),
        node: source_node.to_string(),
        to_node: migration_target.unwrap_or_default(),
        gpu_request: candidate.gpu,
        disruption_cost,
        reason,
    })
}

fn solve_repair_subset(
    candidates: &[RepairCandidate<'_>],
    deficit: i64,
    node: &str,
    target: &str,
    free_by_node: &BTreeMap<String, i64>,
    pdb_budget: &BTreeMap<String, i32>,
) -> Option<(Vec<RepairAction>, i64, i32)> {
    fn candidate_plan_is_better(
        actions_len: usize,
        freed: i64,
        disruption_cost: i32,
        best: &Option<(Vec<RepairAction>, i64, i32)>,
    ) -> bool {
        best.as_ref()
            .map(|(best_actions, best_freed, best_cost)| {
                disruption_cost < *best_cost
                    || (disruption_cost == *best_cost && actions_len < best_actions.len())
                    || (disruption_cost == *best_cost
                        && actions_len == best_actions.len()
                        && freed < *best_freed)
            })
            .unwrap_or(true)
    }

    struct Search<'a, 'b> {
        candidates: &'a [RepairCandidate<'b>],
        suffix_gpu: Vec<i64>,
        deficit: i64,
        node: &'a str,
        target: &'a str,
        best: Option<(Vec<RepairAction>, i64, i32)>,
    }

    impl<'a, 'b> Search<'a, 'b> {
        fn branch(
            &mut self,
            idx: usize,
            actions: &mut Vec<RepairAction>,
            freed: i64,
            disruption_cost: i32,
            migration_free: &mut BTreeMap<String, i64>,
            remaining_pdb_budget: &mut BTreeMap<String, i32>,
        ) {
            if freed >= self.deficit {
                if candidate_plan_is_better(actions.len(), freed, disruption_cost, &self.best) {
                    self.best = Some((actions.clone(), freed, disruption_cost));
                }
                return;
            }
            if idx >= self.candidates.len() || freed + self.suffix_gpu[idx] < self.deficit {
                return;
            }
            if let Some((best_actions, _, best_cost)) = &self.best {
                if disruption_cost > *best_cost
                    || (disruption_cost == *best_cost && actions.len() >= best_actions.len())
                {
                    return;
                }
            }

            let candidate = &self.candidates[idx];
            let mut include_pdb_budget = remaining_pdb_budget.clone();
            let mut include_valid = true;
            for pdb_key in &candidate.pdb_keys {
                let Some(remaining) = include_pdb_budget.get_mut(pdb_key) else {
                    include_valid = false;
                    break;
                };
                if *remaining <= 0 {
                    include_valid = false;
                    break;
                }
                *remaining -= 1;
            }
            if include_valid {
                let mut include_migration_free = migration_free.clone();
                if let Some(action) = repair_action_for_selected(
                    candidate,
                    self.node,
                    &mut include_migration_free,
                    self.target,
                ) {
                    let action_cost = action.disruption_cost;
                    actions.push(action);
                    self.branch(
                        idx + 1,
                        actions,
                        freed + candidate.gpu,
                        disruption_cost.saturating_add(action_cost),
                        &mut include_migration_free,
                        &mut include_pdb_budget,
                    );
                    actions.pop();
                }
            }

            self.branch(
                idx + 1,
                actions,
                freed,
                disruption_cost,
                migration_free,
                remaining_pdb_budget,
            );
        }
    }

    let mut suffix_gpu = vec![0_i64; candidates.len() + 1];
    for idx in (0..candidates.len()).rev() {
        suffix_gpu[idx] = suffix_gpu[idx + 1].saturating_add(candidates[idx].gpu);
    }
    let mut search = Search {
        candidates,
        suffix_gpu,
        deficit,
        node,
        target,
        best: None,
    };
    search.branch(
        0,
        &mut Vec::new(),
        0,
        0,
        &mut free_by_node.clone(),
        &mut pdb_budget.clone(),
    );
    search.best
}

fn summarize_repair_metrics(plans: &[RepairPlan], notes: &[String]) -> RepairMetrics {
    let mut metrics = RepairMetrics {
        repairable_targets: plans.len(),
        unrepairable_targets: notes.len(),
        ..Default::default()
    };
    for plan in plans {
        metrics.disruption_cost += i64::from(plan.disruption_cost.max(0));
        metrics.skipped_candidates += plan.skipped_candidates.len();
        for skipped in &plan.skipped_candidates {
            let reason = skipped.reason.as_str();
            if reason.contains("priority") {
                metrics.priority_blocked_candidates += 1;
            } else if reason.contains("business-value") || reason.contains("deadline") {
                metrics.value_policy_blocked_candidates += 1;
            } else if reason.contains("safe-to-evict")
                || reason.contains("volume attachment")
                || reason.contains("do-not-disrupt")
                || reason.contains("near-complete")
                || reason.contains("migration/preemption")
            {
                metrics.disruption_policy_blocked_candidates += 1;
            } else if reason.contains("PDB") {
                metrics.pdb_blocked_candidates += 1;
            } else if reason.contains("bounded repair candidate set") {
                metrics.candidate_budget_skipped_candidates += 1;
            }
        }
        for action in &plan.actions {
            match action.action.as_str() {
                "migrate" => metrics.migration_actions += 1,
                "preempt" => metrics.preemption_actions += 1,
                _ => {}
            }
        }
    }
    for note in notes {
        if note.contains("blocked by predicted peak VRAM") {
            metrics.vram_blocked_targets += 1;
        } else if note.contains("no node has enough total GPU capacity")
            || note.contains("no feasible node has enough total GPU capacity")
        {
            metrics.not_enough_total_gpu_targets += 1;
        } else if note.contains("no repair plan was found within policy and candidate budget") {
            metrics.policy_or_candidate_blocked_targets += 1;
        } else if note.contains("missing normalized workload model data")
            || note.contains("DRA device requests")
        {
            metrics.incomplete_model_targets += 1;
        }
    }
    metrics
}

fn pending_workload_by_scope<'a>(
    cluster: &'a NormalizedCluster,
) -> BTreeMap<String, &'a NormalizedWorkload> {
    cluster
        .workloads
        .iter()
        .filter(|w| w.current_node.is_empty())
        .map(|w| (format!("{}/{}", w.namespace, w.name), w))
        .collect()
}

fn merge_group_feasible_nodes(
    current: &mut Option<BTreeSet<String>>,
    workload: Option<&NormalizedWorkload>,
) {
    let Some(workload) = workload else {
        return;
    };
    if workload.feasible_node_names.is_empty() {
        return;
    }
    let nodes: BTreeSet<String> = workload.feasible_node_names.iter().cloned().collect();
    match current {
        Some(existing) => {
            *existing = existing.intersection(&nodes).cloned().collect();
        }
        None => {
            *current = Some(nodes);
        }
    }
}

fn group_node_feasible(group: &PendingGroup, node: &str) -> bool {
    group
        .feasible_nodes
        .as_ref()
        .map(|nodes| nodes.contains(node))
        .unwrap_or(true)
}

/// Builds dry-run defragmentation advice. This does not mutate the solve or bind/evict anything.
/// It asks: can an unplaced pending GPU group fit on a single node if we move/preempt a bounded set
/// of currently running GPU pods from that node?
pub fn advise_repairs(
    cluster: &NormalizedCluster,
    pending: &[PendingGpuPod],
    trace: &DecisionTrace,
) -> RepairAdvice {
    advise_repairs_with_options(cluster, pending, trace, RepairOptions::default())
}

pub fn advise_repairs_with_options(
    cluster: &NormalizedCluster,
    pending: &[PendingGpuPod],
    trace: &DecisionTrace,
    options: RepairOptions,
) -> RepairAdvice {
    let max_candidates = options.max_candidates_per_node.max(1);
    let mut decision_by_scope = BTreeMap::new();
    for d in &trace.decisions {
        decision_by_scope.insert(format!("{}/{}", d.namespace, d.name), d);
    }
    let workload_by_scope = pending_workload_by_scope(cluster);

    let mut groups: BTreeMap<String, PendingGroup> = BTreeMap::new();
    for p in pending {
        let key = pending_key(p);
        let scope = format!("{}/{}", p.namespace, p.name);
        let placed = decision_by_scope
            .get(&scope)
            .map(|d| matches!(d.placement, PodPlacement::Placed { .. }))
            .unwrap_or(false);
        let vram_blocked = decision_by_scope
            .get(&scope)
            .and_then(|d| match &d.placement {
                PodPlacement::Unplaced { reason } => Some(reason),
                PodPlacement::Placed { .. } => None,
            })
            .map(|reason| reason.contains("predicted peak VRAM"))
            .unwrap_or(false);
        let g = groups.entry(key).or_insert_with(|| PendingGroup {
            target: pending_target_name(p),
            gpu_request: 0,
            unmodeled_dra_members: 0,
            priority: 0,
            business_value: 0,
            deadline_unix_seconds: 0,
            latest_start_unix_seconds: 0,
            queue_wait_seconds: 0,
            all_unplaced: true,
            vram_blocked: false,
            modeled_workload_models: 0,
            missing_workload_models: 0,
            feasible_nodes: None,
        });
        g.gpu_request += p.gpu_request;
        if p.gpu_request <= 0
            && p.unmodeled_constraints
                .iter()
                .any(|c| c.starts_with("DRA:"))
        {
            g.unmodeled_dra_members += 1;
        }
        g.priority = g.priority.max(p.priority);
        g.business_value = g.business_value.max(p.business_value);
        if p.deadline_unix_seconds > 0 {
            g.deadline_unix_seconds = if g.deadline_unix_seconds > 0 {
                g.deadline_unix_seconds.min(p.deadline_unix_seconds)
            } else {
                p.deadline_unix_seconds
            };
            let latest_start = p
                .deadline_unix_seconds
                .saturating_sub(p.predicted_runtime_seconds.max(0));
            g.latest_start_unix_seconds = if g.latest_start_unix_seconds > 0 {
                g.latest_start_unix_seconds.min(latest_start)
            } else {
                latest_start
            };
        }
        g.queue_wait_seconds = g.queue_wait_seconds.max(p.queue_wait_seconds);
        g.all_unplaced &= !placed;
        g.vram_blocked |= vram_blocked;
        let modeled_workload = workload_by_scope.get(&scope).copied();
        if modeled_workload.is_some() {
            g.modeled_workload_models += 1;
        } else {
            g.missing_workload_models += 1;
        }
        merge_group_feasible_nodes(&mut g.feasible_nodes, modeled_workload);
    }

    let capacity = node_gpu_capacity(cluster);
    let free_by_node = free_gpu_by_node(cluster);
    let running_by_node = running_gpu_by_node(cluster);
    let pdb_budget = pdb_budget_by_key(&cluster.pdbs);
    let mut plans = Vec::new();
    let mut notes = Vec::new();

    for group in groups.values().filter(|g| g.all_unplaced) {
        if group.gpu_request <= 0 {
            if group.unmodeled_dra_members > 0 {
                notes.push(format!(
                    "{} uses DRA device requests that were not reduced to a modeled GPU scalar; repair advice cannot safely treat it as zero-GPU work",
                    group.target
                ));
            }
            continue;
        }
        if group.modeled_workload_models > 0 && group.missing_workload_models > 0 {
            notes.push(format!(
                "{} is missing normalized workload model data for {} pending member(s); repair advice would only know a partial feasible-node intersection",
                group.target, group.missing_workload_models
            ));
            continue;
        }
        if group.vram_blocked {
            notes.push(format!(
                "{} is blocked by predicted peak VRAM exceeding known node GPU memory; freeing occupied GPU slots will not make a too-small GPU fit",
                group.target
            ));
            continue;
        }
        let candidate_node_count = capacity
            .iter()
            .filter(|(node, cap)| **cap >= group.gpu_request && group_node_feasible(group, node))
            .count();
        if candidate_node_count == 0 {
            if group.feasible_nodes.is_some() {
                notes.push(format!(
                    "{} needs {} GPUs on one feasible node, but no feasible node has enough total GPU capacity",
                    group.target, group.gpu_request
                ));
            } else {
                notes.push(format!(
                    "{} needs {} GPUs on one node, but no node has enough total GPU capacity",
                    group.target, group.gpu_request
                ));
            }
            continue;
        }
        let mut best: Option<RepairPlan> = None;
        for (node, cap) in &capacity {
            if *cap < group.gpu_request || !group_node_feasible(group, node) {
                continue;
            }
            let free = free_by_node.get(node).copied().unwrap_or(0);
            if free >= group.gpu_request {
                continue;
            }
            let deficit = group.gpu_request - free;
            let mut running = running_by_node.get(node).cloned().unwrap_or_default();
            running.sort_by(|a, b| {
                repair_disruption_cost(a)
                    .cmp(&repair_disruption_cost(b))
                    .then_with(|| a.requests.milli_cpu.cmp(&b.requests.milli_cpu))
                    .then_with(|| a.namespace.cmp(&b.namespace))
                    .then_with(|| a.name.cmp(&b.name))
            });

            let mut skipped_candidates = Vec::new();
            let mut candidates = Vec::new();
            let mut considered = 0usize;
            for w in running {
                let gpu = workload_gpu(w);
                if gpu <= 0 {
                    continue;
                }
                let matching_pdbs = matching_pdb_keys(w, &cluster.pdbs);
                let skip_reason = if w.autoscaler_not_safe_to_evict {
                    Some("blocked by safe-to-evict=false")
                } else if w.pinned_by_volume {
                    Some("blocked by volume attachment")
                } else if let Some(reason) = priority_repair_skip_reason(group.priority, w.priority)
                {
                    Some(reason)
                } else if let Some(reason) = policy_repair_skip_reason(group, w) {
                    Some(reason)
                } else if w.do_not_disrupt {
                    Some("blocked by do-not-disrupt policy")
                } else if w.progress_percent >= 95 {
                    Some("blocked by near-complete progress")
                } else if !w.migration_allowed && !w.preemption_allowed {
                    Some("blocked by migration/preemption policy")
                } else if let Err(reason) = &matching_pdbs {
                    Some(reason.as_str())
                } else if considered >= max_candidates {
                    Some("outside bounded repair candidate set")
                } else {
                    None
                };
                if let Some(reason) = skip_reason {
                    skipped_candidates.push(RepairSkip {
                        namespace: w.namespace.clone(),
                        pod: w.name.clone(),
                        node: node.clone(),
                        gpu_request: gpu,
                        reason: reason.to_string(),
                    });
                    continue;
                }
                considered += 1;
                candidates.push(RepairCandidate {
                    workload: w,
                    gpu,
                    disruption_cost: repair_disruption_cost(w),
                    pdb_keys: matching_pdbs.unwrap_or_default(),
                });
            }

            let Some((actions, freed, disruption_cost)) = solve_repair_subset(
                &candidates,
                deficit,
                node,
                &group.target,
                &free_by_node,
                &pdb_budget,
            ) else {
                continue;
            };
            let plan = RepairPlan {
                target: group.target.clone(),
                target_gpu_request: group.gpu_request,
                target_priority: group.priority,
                target_business_value: group.business_value,
                target_deadline_unix_seconds: group.deadline_unix_seconds,
                target_latest_start_unix_seconds: group.latest_start_unix_seconds,
                target_queue_wait_seconds: group.queue_wait_seconds,
                node: node.clone(),
                freed_gpu: freed,
                disruption_cost,
                explanation: format!(
                    "enough GPUs exist, but {} needs {} GPUs on one node; free {} GPUs on {} via dry-run repair with disruption cost {}",
                    group.target, group.gpu_request, freed, node, disruption_cost
                ),
                actions,
                skipped_candidates,
            };
            let replace = best
                .as_ref()
                .map(|b| {
                    plan.actions.len() < b.actions.len()
                        || (plan.actions.len() == b.actions.len()
                            && plan.disruption_cost < b.disruption_cost)
                        || (plan.actions.len() == b.actions.len()
                            && plan.disruption_cost == b.disruption_cost
                            && plan.freed_gpu < b.freed_gpu)
                })
                .unwrap_or(true);
            if replace {
                best = Some(plan);
            }
        }
        if let Some(plan) = best {
            plans.push(plan);
        } else {
            notes.push(format!(
                "{} has enough total node capacity somewhere, but no repair plan was found within policy and candidate budget",
                group.target
            ));
        }
    }

    plans.sort_by(|a, b| {
        b.target_priority
            .cmp(&a.target_priority)
            .then_with(|| b.target_business_value.cmp(&a.target_business_value))
            .then_with(|| repair_deadline_order(a, b))
            .then_with(|| {
                b.target_queue_wait_seconds
                    .cmp(&a.target_queue_wait_seconds)
            })
            .then_with(|| b.target_gpu_request.cmp(&a.target_gpu_request))
            .then_with(|| a.disruption_cost.cmp(&b.disruption_cost))
            .then_with(|| a.target.cmp(&b.target))
    });
    plans.truncate(8);
    let metrics = summarize_repair_metrics(&plans, &notes);
    RepairAdvice {
        plans,
        notes,
        metrics,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{DisruptionBudget, LabelSelectorReq, NormalizedNode, ResourceList};
    use crate::scheduler::trace::{DeadlineMetrics, DecisionTrace, PodDecision, QuotaMetrics};

    fn node(name: &str, gpu: i64) -> NormalizedNode {
        NormalizedNode {
            name: name.to_string(),
            effective_capacity: ResourceList {
                milli_cpu: 64000,
                memory_bytes: 512 << 30,
                pods: 110,
                ..Default::default()
            },
            extended_resources: BTreeMap::from([(GPU_RESOURCE.to_string(), gpu)]),
            ..Default::default()
        }
    }

    fn running(name: &str, node: &str) -> NormalizedWorkload {
        running_gpu(name, node, 1)
    }

    fn running_gpu(name: &str, node: &str, gpu: i64) -> NormalizedWorkload {
        NormalizedWorkload {
            namespace: "team".to_string(),
            name: name.to_string(),
            labels: BTreeMap::from([("app".to_string(), "trainer".to_string())]),
            current_node: node.to_string(),
            requests: ResourceList {
                milli_cpu: 1000,
                memory_bytes: 1 << 30,
                pods: 1,
                ..Default::default()
            },
            extended_resource_requests: BTreeMap::from([(GPU_RESOURCE.to_string(), gpu)]),
            feasible_node_names: vec!["n2".to_string()],
            ..Default::default()
        }
    }

    fn pending_workload(name: &str, gpu: i64, feasible: &[&str]) -> NormalizedWorkload {
        NormalizedWorkload {
            namespace: "team".to_string(),
            name: name.to_string(),
            current_node: String::new(),
            requests: ResourceList {
                milli_cpu: 1000,
                memory_bytes: 1 << 30,
                pods: 1,
                ..Default::default()
            },
            extended_resource_requests: BTreeMap::from([(GPU_RESOURCE.to_string(), gpu)]),
            feasible_node_names: feasible.iter().map(|node| node.to_string()).collect(),
            ..Default::default()
        }
    }

    fn pdb(name: &str, disruptions_allowed: i32) -> DisruptionBudget {
        DisruptionBudget {
            namespace: "team".to_string(),
            name: name.to_string(),
            selector: vec![LabelSelectorReq {
                key: "app".to_string(),
                operator: "In".to_string(),
                values: vec!["trainer".to_string()],
            }],
            selector_modeled: true,
            disruptions_allowed,
            ..Default::default()
        }
    }

    fn pending(name: &str, priority: i64) -> PendingGpuPod {
        PendingGpuPod {
            uid: format!("uid-{name}"),
            namespace: "team".to_string(),
            name: name.to_string(),
            gpu_request: 1,
            priority,
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
            gang_key: Some("team/urgent".to_string()),
            colocate: true,
            unmodeled_constraints: vec![],
            anti_affinity_host_selectors: vec![],
            affinity_topology_selectors: vec![],
            anti_affinity_topology_selectors: vec![],
            preferred_node_affinity: vec![],
            preferred_pod_affinity: vec![],
        }
    }

    fn unplaced_trace(pending: &[PendingGpuPod]) -> DecisionTrace {
        unplaced_trace_with_reason(pending, "gang not admitted")
    }

    fn unplaced_trace_with_reason(pending: &[PendingGpuPod], reason: &str) -> DecisionTrace {
        DecisionTrace {
            sequence: 1,
            observed_pods: pending.len(),
            decisions: pending
                .iter()
                .map(|p| PodDecision {
                    uid: p.uid.clone(),
                    namespace: p.namespace.clone(),
                    name: p.name.clone(),
                    binding_group: String::new(),
                    gpu_request: p.gpu_request,
                    priority: p.priority,
                    priority_class_name: String::new(),
                    team: String::new(),
                    queue: String::new(),
                    queue_score: 0,
                    business_value: p.business_value,
                    queue_wait_seconds: p.queue_wait_seconds,
                    deadline_unix_seconds: p.deadline_unix_seconds,
                    min_gpus: 0,
                    max_gpus: 0,
                    preferred_gpus: 0,
                    flexible: false,
                    predicted_runtime_seconds: p.predicted_runtime_seconds,
                    predicted_peak_vram_bytes: 0,
                    deadline_slack_seconds: 0,
                    predicted_finish_unix_seconds: 0,
                    predicted_deadline_miss: false,
                    placement: PodPlacement::Unplaced {
                        reason: reason.to_string(),
                    },
                    caveats: vec![],
                })
                .collect(),
            solver_status: "status=Optimal".to_string(),
            objective_profile: Default::default(),
            objective_weights: Default::default(),
            solve_millis: 10,
            solve_core_millis: 5,
            snapshot_age_millis: 1,
            note: String::new(),
            repair_plans: Vec::new(),
            repair_notes: Vec::new(),
            repair_metrics: Default::default(),
            deadline_metrics: DeadlineMetrics::default(),
            quota_metrics: QuotaMetrics::default(),
            admission_metrics: Default::default(),
            queue_wait_metrics: Default::default(),
            tenant_fairness_metrics: Default::default(),
            gpu_utilization_metrics: Default::default(),
            outcome_summary: Default::default(),
            job_observation_metrics: Default::default(),
            prediction_audit_metrics: Default::default(),
            prediction_audit_details: Vec::new(),
            node_grouping_metrics: Default::default(),
            candidate_quality_metrics: Default::default(),
            binding_reservation_metrics: Default::default(),
            binding_outcome_metrics: Default::default(),
            candidate_node_limit: 0,
            retry_count: 0,
            unpruned_candidate_edges: 0,
            initial_candidate_edges: 0,
            final_candidate_edges: 0,
            candidate_pruned_workloads: 0,
            widening_reason: String::new(),
        }
    }

    #[test]
    fn advises_repair_for_fragmented_colocated_gang() {
        let cluster = NormalizedCluster {
            nodes: vec![node("n1", 4), node("n2", 2)],
            workloads: vec![
                running("low-a", "n1"),
                running("low-b", "n1"),
                running("low-c", "n1"),
                running("low-d", "n1"),
            ],
            ..Default::default()
        };
        let pending = vec![
            pending("urgent-0", 9),
            pending("urgent-1", 9),
            pending("urgent-2", 9),
            pending("urgent-3", 9),
        ];
        let trace = unplaced_trace(&pending);
        let advice = advise_repairs(&cluster, &pending, &trace);
        assert_eq!(advice.metrics.repairable_targets, 1);
        assert_eq!(advice.metrics.unrepairable_targets, 0);
        assert_eq!(advice.metrics.migration_actions, 2);
        assert_eq!(advice.metrics.preemption_actions, 2);
        assert_eq!(advice.metrics.disruption_cost, 200);
        let plans = advice.plans;
        assert!(advice.notes.is_empty());
        assert_eq!(plans.len(), 1);
        assert_eq!(plans[0].target, "team/urgent");
        assert_eq!(plans[0].target_gpu_request, 4);
        assert_eq!(plans[0].actions.len(), 4);
        assert_eq!(
            plans[0]
                .actions
                .iter()
                .filter(|a| a.action == "migrate")
                .count(),
            2
        );
        assert_eq!(
            plans[0]
                .actions
                .iter()
                .filter(|a| a.action == "preempt")
                .count(),
            2
        );
        assert_eq!(plans[0].disruption_cost, 200);
        assert_eq!(
            plans[0]
                .actions
                .iter()
                .filter(|a| a.action == "migrate" && a.to_node == "n2")
                .count(),
            2
        );
    }

    #[test]
    fn repair_orders_equal_priority_targets_by_queue_wait() {
        let cluster = NormalizedCluster {
            nodes: vec![node("n1", 4), node("n2", 4), node("n3", 4)],
            workloads: vec![
                running("low-a", "n1"),
                running("low-b", "n1"),
                running("low-c", "n1"),
                running("low-d", "n1"),
                running("low-e", "n2"),
                running("low-f", "n2"),
                running("low-g", "n2"),
                running("low-h", "n2"),
            ],
            ..Default::default()
        };
        let mut old_pending = vec![
            pending("old-0", 5),
            pending("old-1", 5),
            pending("old-2", 5),
            pending("old-3", 5),
        ];
        for p in &mut old_pending {
            p.gang_key = Some("team/old".to_string());
            p.queue_wait_seconds = 3_600;
        }
        let mut fresh_pending = vec![
            pending("fresh-0", 5),
            pending("fresh-1", 5),
            pending("fresh-2", 5),
            pending("fresh-3", 5),
        ];
        for p in &mut fresh_pending {
            p.gang_key = Some("team/fresh".to_string());
            p.queue_wait_seconds = 60;
        }
        let pending: Vec<_> = old_pending.into_iter().chain(fresh_pending).collect();
        let trace = unplaced_trace(&pending);

        let advice = advise_repairs(&cluster, &pending, &trace);

        assert!(advice.notes.is_empty());
        assert_eq!(advice.plans.len(), 2);
        assert_eq!(advice.plans[0].target, "team/old");
        assert_eq!(advice.plans[0].target_queue_wait_seconds, 3_600);
        assert_eq!(advice.plans[1].target, "team/fresh");
        assert_eq!(advice.plans[1].target_queue_wait_seconds, 60);
    }

    #[test]
    fn repair_orders_equal_priority_targets_by_business_value() {
        let cluster = NormalizedCluster {
            nodes: vec![node("n1", 4), node("n2", 4), node("n3", 4)],
            workloads: vec![
                running("low-a", "n1"),
                running("low-b", "n1"),
                running("low-c", "n1"),
                running("low-d", "n1"),
                running("low-e", "n2"),
                running("low-f", "n2"),
                running("low-g", "n2"),
                running("low-h", "n2"),
            ],
            ..Default::default()
        };
        let mut high_value = vec![
            pending("high-value-0", 5),
            pending("high-value-1", 5),
            pending("high-value-2", 5),
            pending("high-value-3", 5),
        ];
        for p in &mut high_value {
            p.gang_key = Some("team/high-value".to_string());
            p.business_value = 100;
            p.queue_wait_seconds = 60;
        }
        let mut low_value = vec![
            pending("low-value-0", 5),
            pending("low-value-1", 5),
            pending("low-value-2", 5),
            pending("low-value-3", 5),
        ];
        for p in &mut low_value {
            p.gang_key = Some("team/low-value".to_string());
            p.business_value = 10;
            p.queue_wait_seconds = 3_600;
        }
        let pending: Vec<_> = high_value.into_iter().chain(low_value).collect();
        let trace = unplaced_trace(&pending);

        let advice = advise_repairs(&cluster, &pending, &trace);

        assert_eq!(advice.plans.len(), 2);
        assert_eq!(advice.plans[0].target, "team/high-value");
        assert_eq!(advice.plans[0].target_business_value, 100);
        assert_eq!(advice.plans[1].target, "team/low-value");
        assert_eq!(advice.plans[1].target_business_value, 10);
    }

    #[test]
    fn repair_orders_equal_policy_targets_by_deadline_urgency() {
        let cluster = NormalizedCluster {
            nodes: vec![node("n1", 4), node("n2", 4), node("n3", 4)],
            workloads: vec![
                running("low-a", "n1"),
                running("low-b", "n1"),
                running("low-c", "n1"),
                running("low-d", "n1"),
                running("low-e", "n2"),
                running("low-f", "n2"),
                running("low-g", "n2"),
                running("low-h", "n2"),
            ],
            ..Default::default()
        };
        let mut urgent = vec![
            pending("urgent-deadline-0", 5),
            pending("urgent-deadline-1", 5),
            pending("urgent-deadline-2", 5),
            pending("urgent-deadline-3", 5),
        ];
        for p in &mut urgent {
            p.gang_key = Some("team/urgent-deadline".to_string());
            p.business_value = 50;
            p.deadline_unix_seconds = 1_800_000_000;
            p.predicted_runtime_seconds = 7_200;
        }
        let mut relaxed = vec![
            pending("relaxed-deadline-0", 5),
            pending("relaxed-deadline-1", 5),
            pending("relaxed-deadline-2", 5),
            pending("relaxed-deadline-3", 5),
        ];
        for p in &mut relaxed {
            p.gang_key = Some("team/relaxed-deadline".to_string());
            p.business_value = 50;
            p.deadline_unix_seconds = 1_800_086_400;
            p.predicted_runtime_seconds = 7_200;
        }
        let pending: Vec<_> = relaxed.into_iter().chain(urgent).collect();
        let trace = unplaced_trace(&pending);

        let advice = advise_repairs(&cluster, &pending, &trace);

        assert_eq!(advice.plans.len(), 2);
        assert_eq!(advice.plans[0].target, "team/urgent-deadline");
        assert_eq!(advice.plans[0].target_deadline_unix_seconds, 1_800_000_000);
        assert_eq!(
            advice.plans[0].target_latest_start_unix_seconds,
            1_799_992_800
        );
        assert_eq!(advice.plans[1].target, "team/relaxed-deadline");
    }

    #[test]
    fn repair_respects_disruption_policy_and_cost() {
        let mut blocked = running("blocked", "n1");
        blocked.migration_allowed = false;
        blocked.preemption_allowed = false;
        blocked.disruption_cost = 1;
        let mut expensive = running("expensive", "n1");
        expensive.migration_allowed = false;
        expensive.disruption_cost = 99;
        let mut cheap = running("cheap", "n1");
        cheap.migration_allowed = false;
        cheap.disruption_cost = 3;

        let cluster = NormalizedCluster {
            nodes: vec![node("n1", 4)],
            workloads: vec![blocked, expensive, cheap],
            ..Default::default()
        };
        let pending = vec![
            pending("urgent-0", 9),
            pending("urgent-1", 9),
            pending("urgent-2", 9),
        ];
        let trace = unplaced_trace(&pending);
        let advice = advise_repairs(&cluster, &pending, &trace);
        let plans = advice.plans;
        assert_eq!(plans.len(), 1);
        assert_eq!(
            plans[0]
                .actions
                .iter()
                .map(|a| a.pod.as_str())
                .collect::<Vec<_>>(),
            vec!["cheap", "expensive"]
        );
        assert_eq!(plans[0].disruption_cost, 302);
        assert!(!plans[0].actions.iter().any(|a| a.pod == "blocked"));
        assert_eq!(plans[0].skipped_candidates.len(), 1);
        assert_eq!(
            plans[0].skipped_candidates[0].reason,
            "blocked by migration/preemption policy"
        );
        assert_eq!(advice.metrics.skipped_candidates, 1);
        assert_eq!(advice.metrics.disruption_policy_blocked_candidates, 1);
    }

    #[test]
    fn repair_prefers_migration_over_preemption_when_base_cost_ties() {
        let mut preempt_only = running("a-preempt-only", "n1");
        preempt_only.migration_allowed = false;
        preempt_only.disruption_cost = 0;
        let mut migratable = running("z-migratable", "n1");
        migratable.disruption_cost = 0;

        let cluster = NormalizedCluster {
            nodes: vec![node("n1", 2), node("n2", 1)],
            workloads: vec![preempt_only, migratable],
            ..Default::default()
        };
        let pending = vec![pending("urgent-0", 9)];
        let trace = unplaced_trace(&pending);

        let advice = advise_repairs(&cluster, &pending, &trace);

        assert_eq!(advice.plans.len(), 1);
        assert_eq!(advice.plans[0].actions.len(), 1);
        assert_eq!(advice.plans[0].actions[0].pod, "z-migratable");
        assert_eq!(advice.plans[0].actions[0].action, "migrate");
        assert_eq!(advice.plans[0].actions[0].to_node, "n2");
        assert_eq!(advice.plans[0].disruption_cost, 0);
    }

    #[test]
    fn repair_skips_higher_priority_running_workloads() {
        let mut high = running("critical-running", "n1");
        high.priority = 10;
        let low = running("low-running", "n1");
        let cluster = NormalizedCluster {
            nodes: vec![node("n1", 2)],
            workloads: vec![high, low],
            ..Default::default()
        };
        let pending = vec![pending("urgent-0", 9)];
        let trace = unplaced_trace(&pending);

        let advice = advise_repairs(&cluster, &pending, &trace);

        assert_eq!(advice.plans.len(), 1);
        assert_eq!(advice.plans[0].actions[0].pod, "low-running");
        assert_eq!(
            advice.plans[0].skipped_candidates[0].reason,
            "blocked by higher-priority running workload"
        );
        assert_eq!(advice.metrics.skipped_candidates, 1);
        assert_eq!(advice.metrics.priority_blocked_candidates, 1);
    }

    #[test]
    fn repair_skips_equal_priority_running_workloads_for_positive_priority_target() {
        let mut equal = running("equal-running", "n1");
        equal.priority = 9;
        let cluster = NormalizedCluster {
            nodes: vec![node("n1", 1)],
            workloads: vec![equal],
            ..Default::default()
        };
        let pending = vec![pending("urgent-0", 9)];
        let trace = unplaced_trace(&pending);

        let advice = advise_repairs(&cluster, &pending, &trace);

        assert!(advice.plans.is_empty());
        assert_eq!(advice.notes.len(), 1);
        assert!(advice.notes[0].contains("policy and candidate budget"));
    }

    #[test]
    fn repair_skips_equal_priority_running_workload_with_higher_business_value() {
        let mut protected = running("business-critical", "n1");
        protected.business_value = 100;
        let mut available = running("best-effort", "n1");
        available.business_value = 10;
        let cluster = NormalizedCluster {
            nodes: vec![node("n1", 2)],
            workloads: vec![protected, available],
            ..Default::default()
        };
        let mut target = pending("waiting-0", 0);
        target.business_value = 25;
        let pending = vec![target];
        let trace = unplaced_trace(&pending);

        let advice = advise_repairs(&cluster, &pending, &trace);

        assert_eq!(advice.plans.len(), 1);
        assert_eq!(advice.plans[0].actions[0].pod, "best-effort");
        assert_eq!(
            advice.plans[0].skipped_candidates[0].reason,
            "blocked by higher-business-value running workload"
        );
        assert_eq!(advice.metrics.skipped_candidates, 1);
        assert_eq!(advice.metrics.value_policy_blocked_candidates, 1);
    }

    #[test]
    fn repair_skips_equal_policy_running_workload_with_more_urgent_deadline() {
        let mut protected = running("earlier-deadline", "n1");
        protected.deadline_unix_seconds = 1_800_000_000;
        protected.predicted_runtime_seconds = 7_200;
        let mut available = running("later-deadline", "n1");
        available.deadline_unix_seconds = 1_800_086_400;
        available.predicted_runtime_seconds = 3_600;
        let cluster = NormalizedCluster {
            nodes: vec![node("n1", 2)],
            workloads: vec![protected, available],
            ..Default::default()
        };
        let mut target = pending("waiting-0", 0);
        target.deadline_unix_seconds = 1_800_043_200;
        target.predicted_runtime_seconds = 3_600;
        let pending = vec![target];
        let trace = unplaced_trace(&pending);

        let advice = advise_repairs(&cluster, &pending, &trace);

        assert_eq!(advice.plans.len(), 1);
        assert_eq!(advice.plans[0].actions[0].pod, "later-deadline");
        assert_eq!(
            advice.plans[0].skipped_candidates[0].reason,
            "blocked by more urgent deadline running workload"
        );
        assert_eq!(advice.metrics.skipped_candidates, 1);
        assert_eq!(advice.metrics.value_policy_blocked_candidates, 1);
    }

    #[test]
    fn repair_priority_dominates_candidate_business_value() {
        let mut high_value = running("valuable-lower-priority", "n1");
        high_value.business_value = 1_000;
        let cluster = NormalizedCluster {
            nodes: vec![node("n1", 1)],
            workloads: vec![high_value],
            ..Default::default()
        };
        let mut target = pending("urgent-0", 9);
        target.business_value = 1;
        let pending = vec![target];
        let trace = unplaced_trace(&pending);

        let advice = advise_repairs(&cluster, &pending, &trace);

        assert_eq!(advice.plans.len(), 1);
        assert_eq!(advice.plans[0].actions[0].pod, "valuable-lower-priority");
        assert!(advice.plans[0].skipped_candidates.is_empty());
    }

    #[test]
    fn repair_skips_candidates_when_matching_pdb_has_no_budget() {
        let cluster = NormalizedCluster {
            nodes: vec![node("n1", 2)],
            workloads: vec![running("low-a", "n1"), running("low-b", "n1")],
            pdbs: vec![pdb("trainers", 0)],
            ..Default::default()
        };
        let pending = vec![pending("urgent-0", 9), pending("urgent-1", 9)];
        let trace = unplaced_trace(&pending);

        let advice = advise_repairs(&cluster, &pending, &trace);

        assert!(advice.plans.is_empty());
        assert_eq!(advice.notes.len(), 1);
        assert!(advice.notes[0].contains("policy and candidate budget"));
        assert_eq!(advice.metrics.skipped_candidates, 0);
        assert_eq!(advice.metrics.pdb_blocked_candidates, 0);
    }

    #[test]
    fn repair_metrics_count_pdb_blocked_skipped_candidates_when_plan_exists() {
        let protected = running("protected", "n1");
        let mut available = running("available", "n1");
        available.labels.clear();
        let cluster = NormalizedCluster {
            nodes: vec![node("n1", 2)],
            workloads: vec![protected, available],
            pdbs: vec![pdb("trainers", 0)],
            ..Default::default()
        };
        let pending = vec![pending("urgent-0", 9)];
        let trace = unplaced_trace(&pending);

        let advice = advise_repairs(&cluster, &pending, &trace);

        assert_eq!(advice.plans.len(), 1);
        assert_eq!(advice.plans[0].actions[0].pod, "available");
        assert_eq!(
            advice.plans[0].skipped_candidates[0].reason,
            "blocked by PDB team/trainers budget"
        );
        assert_eq!(advice.metrics.skipped_candidates, 1);
        assert_eq!(advice.metrics.pdb_blocked_candidates, 1);
    }

    #[test]
    fn repair_subset_respects_shared_pdb_budget() {
        let mut protected_a = running_gpu("protected-a", "n1", 1);
        protected_a.migration_allowed = false;
        protected_a.disruption_cost = 1;
        let mut protected_b = running_gpu("protected-b", "n1", 1);
        protected_b.migration_allowed = false;
        protected_b.disruption_cost = 1;
        let mut unprotected = running_gpu("unprotected", "n1", 2);
        unprotected.labels.clear();
        unprotected.migration_allowed = false;
        unprotected.disruption_cost = 5;
        let cluster = NormalizedCluster {
            nodes: vec![node("n1", 4)],
            workloads: vec![protected_a, protected_b, unprotected],
            pdbs: vec![pdb("trainers", 1)],
            ..Default::default()
        };
        let pending = vec![pending("urgent-0", 9), pending("urgent-1", 9)];
        let trace = unplaced_trace(&pending);

        let advice = advise_repairs(&cluster, &pending, &trace);

        assert_eq!(advice.plans.len(), 1);
        assert_eq!(
            advice.plans[0]
                .actions
                .iter()
                .map(|a| a.pod.as_str())
                .collect::<Vec<_>>(),
            vec!["unprotected"]
        );
        assert_eq!(advice.plans[0].disruption_cost, 105);
        assert_eq!(advice.metrics.skipped_candidates, 0);
    }

    #[test]
    fn repair_skips_do_not_disrupt_and_near_complete_candidates() {
        let mut protected = running_gpu("protected", "n1", 1);
        protected.do_not_disrupt = true;
        let mut near_complete = running_gpu("near-complete", "n1", 1);
        near_complete.progress_percent = 98;
        let mut available = running_gpu("available", "n1", 1);
        available.migration_allowed = false;
        available.disruption_cost = 4;
        let cluster = NormalizedCluster {
            nodes: vec![node("n1", 3)],
            workloads: vec![protected, near_complete, available],
            ..Default::default()
        };
        let pending = vec![pending("urgent-0", 9)];
        let trace = unplaced_trace(&pending);

        let advice = advise_repairs(&cluster, &pending, &trace);

        assert_eq!(advice.plans.len(), 1);
        assert_eq!(advice.plans[0].actions[0].pod, "available");
        assert_eq!(advice.plans[0].disruption_cost, 104);
        assert_eq!(
            advice.plans[0]
                .skipped_candidates
                .iter()
                .map(|s| s.reason.as_str())
                .collect::<Vec<_>>(),
            vec![
                "blocked by do-not-disrupt policy",
                "blocked by near-complete progress"
            ]
        );
        assert_eq!(advice.metrics.skipped_candidates, 2);
        assert_eq!(advice.metrics.disruption_policy_blocked_candidates, 2);
    }

    #[test]
    fn repair_cost_accounts_for_checkpoint_age_and_progress() {
        let mut old_checkpoint = running_gpu("old-checkpoint", "n1", 1);
        old_checkpoint.migration_allowed = false;
        old_checkpoint.disruption_cost = 1;
        old_checkpoint.checkpoint_age_seconds = 60 * 45;
        let mut partial_progress = running_gpu("partial-progress", "n1", 1);
        partial_progress.migration_allowed = false;
        partial_progress.disruption_cost = 1;
        partial_progress.progress_percent = 40;
        let mut compact = running_gpu("compact", "n1", 2);
        compact.migration_allowed = false;
        compact.disruption_cost = 20;

        let cluster = NormalizedCluster {
            nodes: vec![node("n1", 4)],
            workloads: vec![old_checkpoint, partial_progress, compact],
            ..Default::default()
        };
        let pending = vec![pending("urgent-0", 9), pending("urgent-1", 9)];
        let trace = unplaced_trace(&pending);

        let advice = advise_repairs(&cluster, &pending, &trace);

        assert_eq!(advice.plans.len(), 1);
        assert_eq!(
            advice.plans[0]
                .actions
                .iter()
                .map(|a| a.pod.as_str())
                .collect::<Vec<_>>(),
            vec!["compact"]
        );
        assert_eq!(advice.plans[0].disruption_cost, 120);
    }

    #[test]
    fn repair_cost_accounts_for_running_age() {
        let mut old_running = running_gpu("old-running", "n1", 1);
        old_running.migration_allowed = false;
        old_running.disruption_cost = 1;
        old_running.running_age_seconds = 24 * 3600;
        let mut fresh_running = running_gpu("fresh-running", "n1", 1);
        fresh_running.migration_allowed = false;
        fresh_running.disruption_cost = 1;

        let cluster = NormalizedCluster {
            nodes: vec![node("n1", 2)],
            workloads: vec![old_running, fresh_running],
            ..Default::default()
        };
        let pending = vec![pending("urgent-0", 9)];
        let trace = unplaced_trace(&pending);

        let advice = advise_repairs(&cluster, &pending, &trace);

        assert_eq!(advice.plans.len(), 1);
        assert_eq!(advice.plans[0].actions[0].pod, "fresh-running");
        assert_eq!(
            advice.plans[0].disruption_cost,
            1 + PREEMPTION_DISRUPTION_PENALTY
        );
    }

    #[test]
    fn repair_notes_when_no_node_has_total_capacity() {
        let cluster = NormalizedCluster {
            nodes: vec![node("n1", 2), node("n2", 2)],
            workloads: vec![],
            ..Default::default()
        };
        let pending = vec![
            pending("urgent-0", 9),
            pending("urgent-1", 9),
            pending("urgent-2", 9),
        ];
        let trace = unplaced_trace(&pending);
        let advice = advise_repairs(&cluster, &pending, &trace);
        assert!(advice.plans.is_empty());
        assert_eq!(advice.notes.len(), 1);
        assert!(advice.notes[0].contains("no node has enough total GPU capacity"));
        assert_eq!(advice.metrics.repairable_targets, 0);
        assert_eq!(advice.metrics.unrepairable_targets, 1);
        assert_eq!(advice.metrics.not_enough_total_gpu_targets, 1);
    }

    #[test]
    fn repair_does_not_clear_node_outside_pending_feasible_set() {
        let cluster = NormalizedCluster {
            nodes: vec![node("n1", 4), node("n2", 2)],
            workloads: vec![
                running("low-a", "n1"),
                running("low-b", "n1"),
                running("low-c", "n1"),
                running("low-d", "n1"),
                pending_workload("urgent-0", 1, &["n2"]),
                pending_workload("urgent-1", 1, &["n2"]),
                pending_workload("urgent-2", 1, &["n2"]),
                pending_workload("urgent-3", 1, &["n2"]),
            ],
            ..Default::default()
        };
        let pending = vec![
            pending("urgent-0", 9),
            pending("urgent-1", 9),
            pending("urgent-2", 9),
            pending("urgent-3", 9),
        ];
        let trace = unplaced_trace(&pending);

        let advice = advise_repairs(&cluster, &pending, &trace);

        assert!(advice.plans.is_empty());
        assert_eq!(advice.notes.len(), 1);
        assert!(advice.notes[0].contains("no feasible node has enough total GPU capacity"));
        assert_eq!(advice.metrics.not_enough_total_gpu_targets, 1);
    }

    #[test]
    fn repair_does_not_advise_from_partial_pending_workload_model() {
        let cluster = NormalizedCluster {
            nodes: vec![node("n1", 4), node("n2", 4)],
            workloads: vec![
                running("low-a", "n1"),
                running("low-b", "n1"),
                pending_workload("urgent-0", 1, &["n1"]),
            ],
            ..Default::default()
        };
        let pending = vec![pending("urgent-0", 9), pending("urgent-1", 9)];
        let trace = unplaced_trace(&pending);

        let advice = advise_repairs(&cluster, &pending, &trace);

        assert!(advice.plans.is_empty());
        assert_eq!(advice.notes.len(), 1);
        assert!(advice.notes[0].contains("missing normalized workload model data"));
        assert!(advice.notes[0].contains("partial feasible-node intersection"));
        assert_eq!(advice.metrics.repairable_targets, 0);
        assert_eq!(advice.metrics.unrepairable_targets, 1);
        assert_eq!(advice.metrics.incomplete_model_targets, 1);
    }

    #[test]
    fn repair_notes_unmodeled_dra_zero_gpu_target_instead_of_dropping_it() {
        let cluster = NormalizedCluster {
            nodes: vec![node("n1", 4)],
            workloads: vec![],
            ..Default::default()
        };
        let mut target = pending("urgent-dra", 9);
        target.gpu_request = 0;
        target.gang_key = None;
        target.unmodeled_constraints =
            vec!["DRA: device demand modeled as scalar approximation".to_string()];
        let pending = vec![target];
        let trace = unplaced_trace(&pending);

        let advice = advise_repairs(&cluster, &pending, &trace);

        assert!(advice.plans.is_empty());
        assert_eq!(advice.notes.len(), 1);
        assert!(advice.notes[0].contains("DRA device requests"));
        assert!(advice.notes[0].contains("zero-GPU work"));
        assert_eq!(advice.metrics.repairable_targets, 0);
        assert_eq!(advice.metrics.unrepairable_targets, 1);
        assert_eq!(advice.metrics.incomplete_model_targets, 1);
    }

    #[test]
    fn repair_candidate_limit_bounds_actions() {
        let cluster = NormalizedCluster {
            nodes: vec![node("n1", 4)],
            workloads: vec![
                running("low-a", "n1"),
                running("low-b", "n1"),
                running("low-c", "n1"),
                running("low-d", "n1"),
            ],
            ..Default::default()
        };
        let pending = vec![pending("urgent-0", 9), pending("urgent-1", 9)];
        let trace = unplaced_trace(&pending);
        let advice = advise_repairs_with_options(
            &cluster,
            &pending,
            &trace,
            RepairOptions {
                max_candidates_per_node: 1,
            },
        );
        assert!(advice.plans.is_empty());
        assert_eq!(advice.notes.len(), 1);
        assert!(advice.notes[0].contains("candidate budget"));
        assert_eq!(advice.metrics.repairable_targets, 0);
        assert_eq!(advice.metrics.unrepairable_targets, 1);
        assert_eq!(advice.metrics.policy_or_candidate_blocked_targets, 1);
    }

    #[test]
    fn repair_metrics_count_candidate_budget_skips_when_plan_exists() {
        let cluster = NormalizedCluster {
            nodes: vec![node("n1", 2)],
            workloads: vec![running("low-a", "n1"), running("low-b", "n1")],
            ..Default::default()
        };
        let pending = vec![pending("urgent-0", 9)];
        let trace = unplaced_trace(&pending);
        let advice = advise_repairs_with_options(
            &cluster,
            &pending,
            &trace,
            RepairOptions {
                max_candidates_per_node: 1,
            },
        );

        assert_eq!(advice.plans.len(), 1);
        assert_eq!(advice.plans[0].skipped_candidates.len(), 1);
        assert_eq!(
            advice.plans[0].skipped_candidates[0].reason,
            "outside bounded repair candidate set"
        );
        assert_eq!(advice.metrics.skipped_candidates, 1);
        assert_eq!(advice.metrics.candidate_budget_skipped_candidates, 1);
    }

    #[test]
    fn repair_does_not_suggest_evictions_for_vram_blocked_group() {
        let cluster = NormalizedCluster {
            nodes: vec![node("n1", 4)],
            workloads: vec![
                running("low-a", "n1"),
                running("low-b", "n1"),
                running("low-c", "n1"),
                running("low-d", "n1"),
            ],
            ..Default::default()
        };
        let mut pending = vec![
            pending("urgent-0", 9),
            pending("urgent-1", 9),
            pending("urgent-2", 9),
            pending("urgent-3", 9),
        ];
        for p in &mut pending {
            p.predicted_peak_vram_bytes = 80 * 1024 * 1024 * 1024;
        }
        let trace = unplaced_trace_with_reason(
            &pending,
            "no feasible node (predicted peak VRAM exceeds known node GPU memory)",
        );

        let advice = advise_repairs(&cluster, &pending, &trace);

        assert!(advice.plans.is_empty());
        assert_eq!(advice.notes.len(), 1);
        assert!(advice.notes[0].contains("predicted peak VRAM"));
        assert!(advice.notes[0].contains("freeing occupied GPU slots will not"));
        assert_eq!(advice.metrics.repairable_targets, 0);
        assert_eq!(advice.metrics.unrepairable_targets, 1);
        assert_eq!(advice.metrics.vram_blocked_targets, 1);
    }

    #[test]
    fn repair_subset_solver_minimizes_disruption_not_greedy_order() {
        let mut tiny_a = running_gpu("tiny-a", "n1", 1);
        tiny_a.migration_allowed = false;
        tiny_a.disruption_cost = 1;
        let mut tiny_b = running_gpu("tiny-b", "n1", 1);
        tiny_b.migration_allowed = false;
        tiny_b.disruption_cost = 1;
        let mut compact = running_gpu("compact", "n1", 3);
        compact.migration_allowed = false;
        compact.disruption_cost = 3;

        let cluster = NormalizedCluster {
            nodes: vec![node("n1", 5)],
            workloads: vec![tiny_a, tiny_b, compact],
            ..Default::default()
        };
        let pending = vec![
            pending("urgent-0", 9),
            pending("urgent-1", 9),
            pending("urgent-2", 9),
        ];
        let trace = unplaced_trace(&pending);
        let advice = advise_repairs(&cluster, &pending, &trace);
        assert_eq!(advice.plans.len(), 1);
        assert_eq!(
            advice.plans[0]
                .actions
                .iter()
                .map(|a| a.pod.as_str())
                .collect::<Vec<_>>(),
            vec!["compact"]
        );
        assert_eq!(advice.plans[0].freed_gpu, 3);
        assert_eq!(advice.plans[0].disruption_cost, 103);
    }
}
