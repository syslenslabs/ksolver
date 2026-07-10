use crate::model::{
    deadline_adjusted_flexible_replica_bounds, flexible_replica_bounds, OptimizationInput,
    OptimizationSolution,
};
use crate::scheduler::pod_filter::PendingGpuPod;
use crate::scheduler::trace::{
    summarize_scheduling_outcome, DeadlineMetrics, DecisionTrace, PodDecision, PodPlacement,
    QuotaMetrics, TenantFairnessMetrics, TenantQueueMetric,
};
use chrono::Utc;
use std::collections::{BTreeMap, HashMap};

fn pod_key(namespace: &str, name: &str) -> String {
    format!("{namespace}/{name}")
}

fn node_gpu_capacity(node: &crate::model::OptimizationNode) -> i64 {
    node.extended_resources
        .iter()
        .filter(|(name, _)| {
            name.as_str() == "nvidia.com/gpu"
                || name.starts_with("nvidia.com/mig-")
                || name.contains("/gpu")
        })
        .map(|(_, value)| (*value).max(0))
        .sum()
}

fn ceil_div(a: i64, b: i64) -> i64 {
    (a + b - 1) / b
}

fn per_replica_quota_units(
    workload: &crate::model::OptimizationWorkload,
    resources: &[String],
) -> i64 {
    let group_size = i64::from(workload.group_size).max(1);
    resources
        .iter()
        .map(|resource| {
            let total = workload
                .extended_resource_requests
                .get(resource)
                .copied()
                .unwrap_or(0)
                .max(0);
            if total == 0 {
                0
            } else {
                ceil_div(total, group_size)
            }
        })
        .sum()
}

fn quota_exhausted_notes(
    input: &OptimizationInput,
    solution: &OptimizationSolution,
) -> HashMap<String, Vec<String>> {
    let by_id: HashMap<&str, &crate::model::OptimizationWorkload> =
        input.workloads.iter().map(|w| (w.id.as_str(), w)).collect();
    let mut notes: HashMap<String, Vec<String>> = HashMap::new();

    for group in &input.quota_groups {
        if group.limit < 0 || group.resources.is_empty() {
            continue;
        }
        let mut used = 0_i64;
        for workload_id in &group.workload_ids {
            let Some(workload) = by_id.get(workload_id.as_str()) else {
                continue;
            };
            let placed_replicas: i64 = solution
                .assignment_counts
                .get(workload_id)
                .map(|counts| counts.values().map(|count| i64::from(*count).max(0)).sum())
                .unwrap_or(0);
            used += per_replica_quota_units(workload, &group.resources) * placed_replicas;
        }
        if used < group.limit {
            continue;
        }
        let note = format!(
            "quota exhausted: {} selected / {} allowed for resources {}",
            used,
            group.limit,
            group.resources.join(",")
        );
        for workload_id in &group.workload_ids {
            notes
                .entry(workload_id.clone())
                .or_default()
                .push(note.clone());
        }
    }

    notes
}

fn placement_cost_milli_for_workload_on_node(
    workload: &crate::model::OptimizationWorkload,
    node: &crate::model::OptimizationNode,
) -> i64 {
    let group_size = i64::from(workload.group_size).max(1);
    let total_gpu = crate::model::optimization_workload_gpu_request(workload);
    let per_replica_gpu = if total_gpu == 0 {
        0
    } else {
        ceil_div(total_gpu, group_size)
    };
    let gpu_capacity = node_gpu_capacity(node);
    if per_replica_gpu <= 0 || gpu_capacity <= 0 || node.price.monthly <= 0.0 {
        return 0;
    }
    let cost = (node.price.monthly * 1000.0 * per_replica_gpu as f64 / gpu_capacity as f64).round();
    if cost.is_finite() && cost > 0.0 {
        cost as i64
    } else {
        0
    }
}

fn budget_exhausted_notes(
    input: &OptimizationInput,
    solution: &OptimizationSolution,
) -> HashMap<String, Vec<String>> {
    let by_id: HashMap<&str, &crate::model::OptimizationWorkload> =
        input.workloads.iter().map(|w| (w.id.as_str(), w)).collect();
    let node_by_name: HashMap<&str, &crate::model::OptimizationNode> =
        input.nodes.iter().map(|n| (n.name.as_str(), n)).collect();
    let mut notes: HashMap<String, Vec<String>> = HashMap::new();

    for group in &input.budget_groups {
        if group.limit_milli < 0 {
            continue;
        }
        let mut used = 0_i64;
        for workload_id in &group.workload_ids {
            let Some(workload) = by_id.get(workload_id.as_str()) else {
                continue;
            };
            if let Some(counts) = solution.assignment_counts.get(workload_id) {
                for (node_name, count) in counts {
                    let Some(node) = node_by_name.get(node_name.as_str()) else {
                        continue;
                    };
                    let cost = placement_cost_milli_for_workload_on_node(workload, node);
                    used = used.saturating_add(cost.saturating_mul(i64::from(*count).max(0)));
                }
            }
        }
        if used < group.limit_milli {
            continue;
        }
        let subject = if group.name.is_empty() {
            "tenant".to_string()
        } else {
            format!("tenant {}", group.name)
        };
        let note = format!(
            "budget exhausted: {used} selected / {} monthly milli-units for {subject}",
            group.limit_milli
        );
        for workload_id in &group.workload_ids {
            notes
                .entry(workload_id.clone())
                .or_default()
                .push(note.clone());
        }
    }

    notes
}

/// Whether a node is a time-sliced (oversubscribed, no-isolation) GPU node, from its labels.
/// If the NVIDIA `nvidia.com/gpu.sharing-strategy` label is present it is authoritative (only
/// `time-slicing` counts — `mps`/`none` do not, and MPS also uses replicas); otherwise fall
/// back to `nvidia.com/gpu.replicas > 1` (legacy time-slicing without the strategy label).
pub(crate) fn is_time_sliced_node(labels: &std::collections::BTreeMap<String, String>) -> bool {
    match labels.get("nvidia.com/gpu.sharing-strategy") {
        Some(s) => s == "time-slicing",
        None => labels
            .get("nvidia.com/gpu.replicas")
            .and_then(|v| v.parse::<i64>().ok())
            .map(|n| n > 1)
            .unwrap_or(false),
    }
}

/// Map the solver's per-gang output back to per-pod placement decisions.
///
/// A gang (`OptimizationWorkload`, possibly `group_size > 1`) is admitted iff its
/// `assignment_counts` sum equals `group_size`, except flexible deadline-aware gangs may be
/// admitted within their deadline-adjusted min/max replica bounds. For an admitted gang, members are
/// distributed deterministically across the assigned nodes (sorted members filled into sorted
/// nodes by count), so a spread gang reports honest per-member nodes rather than a single "best"
/// node.
#[allow(clippy::too_many_arguments)]
pub fn build_decision_trace(
    sequence: u64,
    pending: &[PendingGpuPod],
    input: &OptimizationInput,
    solution: &OptimizationSolution,
    solver_status: &str,
    solve_ok: bool,
    solve_millis: u64,
    solve_core_millis: u64,
    snapshot_age_millis: u64,
    drop_reasons: &HashMap<String, String>,
    time_sliced_nodes: &std::collections::HashSet<String>,
) -> DecisionTrace {
    // When the solver returned no usable result (Err: timeout/no incumbent/infeasible/
    // backend error), a submitted pod being unresolved does NOT mean it is unschedulable
    // — the solver simply produced nothing. Generic reason; solver_status carries detail.
    let unresolved_reason = |admitted_case: &str| -> String {
        if solve_ok {
            admitted_case.to_string()
        } else {
            "solver produced no usable solution (see solver_status)".to_string()
        }
    };
    // pod "{ns}/{name}" -> resolved placement.
    let mut placement_for: HashMap<String, PodPlacement> = HashMap::new();
    let mut caveats_for: HashMap<String, Vec<String>> = HashMap::new();
    let mut binding_group_for: HashMap<String, String> = HashMap::new();
    let mut queue_score_by_scope: HashMap<String, i64> = HashMap::new();
    let mut fair_share_deficit_by_scope: HashMap<String, i64> = HashMap::new();
    let mut admitted_priority_by_scope: HashMap<String, i64> = HashMap::new();
    let mut admitted_business_value_by_scope: HashMap<String, i64> = HashMap::new();
    let mut admitted_queue_score_by_scope: HashMap<String, i64> = HashMap::new();
    let mut admitted_queue_wait_by_scope: HashMap<String, i64> = HashMap::new();
    let mut admitted_latest_start_by_scope: HashMap<String, i64> = HashMap::new();
    let mut admitted_fair_share_deficit_by_scope: HashMap<String, i64> = HashMap::new();
    let quota_notes_by_workload = quota_exhausted_notes(input, solution);
    let budget_notes_by_workload = budget_exhausted_notes(input, solution);
    let now_unix = Utc::now().timestamp();

    for workload in &input.workloads {
        let group_size = workload.group_size.max(0) as i64;
        let counts = solution.assignment_counts.get(&workload.id);
        let placed_total: i64 = counts
            .map(|c| c.values().map(|v| i64::from(*v)).sum())
            .unwrap_or(0);
        let base_flexible_bounds = flexible_replica_bounds(workload);
        let flexible_bounds = deadline_adjusted_flexible_replica_bounds(workload, now_unix);
        let admitted = group_size > 0
            && match flexible_bounds {
                Some((min_replicas, max_replicas)) => {
                    placed_total >= min_replicas && placed_total <= max_replicas
                }
                None => placed_total == group_size,
            };

        // Deterministic member order.
        let mut members: Vec<&crate::model::OptimizationWorkloadMember> =
            workload.members.iter().collect();
        members.sort_by(|a, b| a.name.cmp(&b.name));
        for m in &members {
            queue_score_by_scope.insert(pod_key(&m.namespace, &m.name), workload.queue_score);
            fair_share_deficit_by_scope
                .insert(pod_key(&m.namespace, &m.name), workload.fair_share_deficit);
        }

        if admitted {
            // Expand assignment_counts into a per-replica node list (sorted node order).
            let mut nodes: Vec<String> = Vec::with_capacity(placed_total as usize);
            if let Some(counts) = counts {
                let mut keyed: Vec<(&String, &i32)> = counts.iter().collect();
                keyed.sort_by(|a, b| a.0.cmp(b.0));
                for (node, count) in keyed {
                    for _ in 0..(*count).max(0) {
                        nodes.push(node.clone());
                    }
                }
            }
            let flexible_note = flexible_bounds.map(|_| {
                if let (Some((_, base_max)), Some((_, adjusted_max))) =
                    (base_flexible_bounds, flexible_bounds)
                {
                    if workload.deadline_unix_seconds > 0
                        && workload.predicted_runtime_seconds > 0
                        && adjusted_max < base_max
                    {
                        return format!(
                            "flexible deadline job selected {placed_total}/{group_size} replicas; deadline slack capped eligible replicas at {adjusted_max}"
                        );
                    }
                }
                format!("flexible deadline job selected {placed_total}/{group_size} replicas")
            });
            for (i, m) in members.iter().enumerate() {
                let node = nodes.get(i).cloned().unwrap_or_default();
                let placement = if node.is_empty() {
                    PodPlacement::Unplaced {
                        reason: flexible_note.clone().unwrap_or_else(|| {
                            "gang admitted but replica node unresolved".to_string()
                        }),
                    }
                } else {
                    PodPlacement::Placed { node }
                };
                if let Some(note) = &flexible_note {
                    caveats_for
                        .entry(pod_key(&m.namespace, &m.name))
                        .or_default()
                        .push(note.clone());
                }
                if matches!(placement, PodPlacement::Placed { .. }) {
                    binding_group_for.insert(pod_key(&m.namespace, &m.name), workload.id.clone());
                    admitted_priority_by_scope
                        .insert(pod_key(&m.namespace, &m.name), workload.priority);
                    admitted_business_value_by_scope
                        .insert(pod_key(&m.namespace, &m.name), workload.business_value);
                    admitted_queue_score_by_scope
                        .insert(pod_key(&m.namespace, &m.name), workload.queue_score);
                    admitted_queue_wait_by_scope
                        .insert(pod_key(&m.namespace, &m.name), workload.queue_wait_seconds);
                    admitted_fair_share_deficit_by_scope
                        .insert(pod_key(&m.namespace, &m.name), workload.fair_share_deficit);
                    if workload.deadline_unix_seconds > 0 {
                        admitted_latest_start_by_scope.insert(
                            pod_key(&m.namespace, &m.name),
                            workload
                                .deadline_unix_seconds
                                .saturating_sub(workload.predicted_runtime_seconds.max(0)),
                        );
                    }
                }
                placement_for.insert(pod_key(&m.namespace, &m.name), placement);
            }
        } else {
            let reason = quota_notes_by_workload
                .get(&workload.id)
                .or_else(|| budget_notes_by_workload.get(&workload.id))
                .and_then(|notes| notes.first())
                .map(|note| unresolved_reason(&format!("gang not admitted ({note})")))
                .unwrap_or_else(|| {
                    unresolved_reason("gang not admitted (insufficient capacity, quota, or budget)")
                });
            for m in &members {
                if let Some(notes) = quota_notes_by_workload.get(&workload.id) {
                    caveats_for
                        .entry(pod_key(&m.namespace, &m.name))
                        .or_default()
                        .extend(notes.iter().cloned());
                }
                if let Some(notes) = budget_notes_by_workload.get(&workload.id) {
                    caveats_for
                        .entry(pod_key(&m.namespace, &m.name))
                        .or_default()
                        .extend(notes.iter().cloned());
                }
                placement_for.insert(
                    pod_key(&m.namespace, &m.name),
                    PodPlacement::Unplaced {
                        reason: reason.clone(),
                    },
                );
            }
        }
    }

    let max_admitted_priority = admitted_priority_by_scope
        .values()
        .copied()
        .max()
        .unwrap_or(0);
    let max_admitted_business_value = admitted_business_value_by_scope
        .values()
        .copied()
        .max()
        .unwrap_or(0);
    let max_admitted_queue_score = admitted_queue_score_by_scope
        .values()
        .copied()
        .max()
        .unwrap_or(0);
    let max_admitted_queue_wait_seconds = admitted_queue_wait_by_scope
        .values()
        .copied()
        .max()
        .unwrap_or(0);
    let max_admitted_fair_share_deficit = admitted_fair_share_deficit_by_scope
        .values()
        .copied()
        .max()
        .unwrap_or(0);
    let earliest_admitted_latest_start = admitted_latest_start_by_scope.values().copied().min();
    let mut decisions = Vec::with_capacity(pending.len());
    for p in pending {
        let predicted_finish_unix_seconds = if p.predicted_runtime_seconds > 0 {
            now_unix.saturating_add(p.predicted_runtime_seconds)
        } else {
            0
        };
        let deadline_slack_seconds = if p.deadline_unix_seconds > 0 {
            p.deadline_unix_seconds
                .saturating_sub(now_unix)
                .saturating_sub(p.predicted_runtime_seconds.max(0))
        } else {
            0
        };
        let predicted_deadline_miss = p.deadline_unix_seconds > 0 && deadline_slack_seconds < 0;
        let scope = pod_key(&p.namespace, &p.name);
        let placement = placement_for.get(&scope).cloned().unwrap_or_else(|| {
            // Never submitted to the solver — use the specific input-build drop reason if we
            // recorded one, else a generic fallback.
            let reason = drop_reasons.get(&scope).cloned().unwrap_or_else(|| {
                "not submitted to solver (filtered as unschedulable during input build)".to_string()
            });
            PodPlacement::Unplaced { reason }
        });
        // Disclose time-sliced (shared, no-isolation) GPU placements.
        let mut caveats = p.unmodeled_constraints.clone();
        if let Some(extra) = caveats_for.get(&scope) {
            caveats.extend(extra.iter().cloned());
        }
        if let PodPlacement::Placed { node } = &placement {
            if time_sliced_nodes.contains(node) {
                caveats.push("time-sliced GPU: shared, no isolation".to_string());
            }
        } else if solve_ok && p.priority < max_admitted_priority {
            caveats.push(format!(
                "deferred below admitted higher-priority work (max admitted priority {max_admitted_priority})"
            ));
        } else if solve_ok && p.business_value < max_admitted_business_value {
            caveats.push(format!(
                "deferred below admitted higher-business-value work (max admitted business value {max_admitted_business_value})"
            ));
        } else if solve_ok
            && queue_score_by_scope.get(&scope).copied().unwrap_or(0) < max_admitted_queue_score
        {
            caveats.push(format!(
                "deferred below admitted higher-queue work (max admitted queue score {max_admitted_queue_score})"
            ));
        } else if solve_ok && p.queue_wait_seconds < max_admitted_queue_wait_seconds {
            caveats.push(format!(
                "deferred below admitted longer-waiting work (max admitted queue wait {max_admitted_queue_wait_seconds}s)"
            ));
        } else if solve_ok
            && fair_share_deficit_by_scope.get(&scope).copied().unwrap_or(0)
                < max_admitted_fair_share_deficit
        {
            caveats.push(format!(
                "deferred below admitted more under-fair-share work (max admitted fair-share deficit {max_admitted_fair_share_deficit})"
            ));
        } else if solve_ok && p.deadline_unix_seconds > 0 {
            if let Some(earliest_latest_start) = earliest_admitted_latest_start {
                let latest_start = p
                    .deadline_unix_seconds
                    .saturating_sub(p.predicted_runtime_seconds.max(0));
                if latest_start > earliest_latest_start {
                    caveats.push(format!(
                        "deferred below admitted more urgent deadline work (earliest admitted latest start {earliest_latest_start})"
                    ));
                }
            }
        }
        decisions.push(PodDecision {
            uid: p.uid.clone(),
            namespace: p.namespace.clone(),
            name: p.name.clone(),
            binding_group: binding_group_for.get(&scope).cloned().unwrap_or_default(),
            gpu_request: p.gpu_request,
            priority: p.priority,
            priority_class_name: p.priority_class_name.clone().unwrap_or_default(),
            team: p.team.clone().unwrap_or_default(),
            queue: p.queue.clone().unwrap_or_default(),
            queue_score: queue_score_by_scope.get(&scope).copied().unwrap_or(0),
            business_value: p.business_value,
            queue_wait_seconds: p.queue_wait_seconds,
            deadline_unix_seconds: p.deadline_unix_seconds,
            min_gpus: p.min_gpus,
            max_gpus: p.max_gpus,
            preferred_gpus: p.preferred_gpus,
            flexible: p.flexible,
            predicted_runtime_seconds: p.predicted_runtime_seconds,
            predicted_peak_vram_bytes: p.predicted_peak_vram_bytes,
            deadline_slack_seconds,
            predicted_finish_unix_seconds,
            predicted_deadline_miss,
            placement,
            caveats,
        });
    }

    let deadline_metrics = deadline_metrics(&decisions);
    let quota_metrics = quota_metrics(&decisions);
    let queue_wait_metrics = queue_wait_metrics(&decisions);
    let tenant_fairness_metrics = tenant_fairness_metrics(&decisions);
    apply_tenant_policy_caveats(&mut decisions, &tenant_fairness_metrics);

    let mut trace = DecisionTrace {
        sequence,
        observed_pods: pending.len(),
        decisions,
        solver_status: solver_status.to_string(),
        objective_profile: Default::default(),
        objective_weights: Default::default(),
        solve_millis,
        solve_core_millis,
        snapshot_age_millis,
        note: String::new(),
        repair_plans: Vec::new(),
        repair_notes: Vec::new(),
        repair_metrics: Default::default(),
        deadline_metrics,
        quota_metrics,
        admission_metrics: Default::default(),
        queue_wait_metrics,
        tenant_fairness_metrics,
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
    };
    trace.outcome_summary = summarize_scheduling_outcome(&trace);
    trace
}

#[allow(clippy::too_many_arguments)]
pub fn build_decision_trace_with_tenant_weights(
    sequence: u64,
    pending: &[PendingGpuPod],
    input: &OptimizationInput,
    solution: &OptimizationSolution,
    solver_status: &str,
    solve_ok: bool,
    solve_millis: u64,
    solve_core_millis: u64,
    snapshot_age_millis: u64,
    drop_reasons: &HashMap<String, String>,
    time_sliced_nodes: &std::collections::HashSet<String>,
    tenant_share_weights: &BTreeMap<String, i64>,
) -> DecisionTrace {
    build_decision_trace_with_tenant_policy(
        sequence,
        pending,
        input,
        solution,
        solver_status,
        solve_ok,
        solve_millis,
        solve_core_millis,
        snapshot_age_millis,
        drop_reasons,
        time_sliced_nodes,
        tenant_share_weights,
        &BTreeMap::new(),
    )
}

#[allow(clippy::too_many_arguments)]
pub fn build_decision_trace_with_tenant_policy(
    sequence: u64,
    pending: &[PendingGpuPod],
    input: &OptimizationInput,
    solution: &OptimizationSolution,
    solver_status: &str,
    solve_ok: bool,
    solve_millis: u64,
    solve_core_millis: u64,
    snapshot_age_millis: u64,
    drop_reasons: &HashMap<String, String>,
    time_sliced_nodes: &std::collections::HashSet<String>,
    tenant_share_weights: &BTreeMap<String, i64>,
    tenant_monthly_budgets_milli: &BTreeMap<String, i64>,
) -> DecisionTrace {
    let mut trace = build_decision_trace(
        sequence,
        pending,
        input,
        solution,
        solver_status,
        solve_ok,
        solve_millis,
        solve_core_millis,
        snapshot_age_millis,
        drop_reasons,
        time_sliced_nodes,
    );
    trace.tenant_fairness_metrics = tenant_fairness_metrics_with_policy(
        &trace.decisions,
        input,
        tenant_share_weights,
        tenant_monthly_budgets_milli,
    );
    apply_tenant_policy_caveats(&mut trace.decisions, &trace.tenant_fairness_metrics);
    trace.outcome_summary = summarize_scheduling_outcome(&trace);
    trace
}

fn quota_metrics(decisions: &[PodDecision]) -> QuotaMetrics {
    let mut exhausted_groups = std::collections::BTreeSet::new();
    let throttled_pods = decisions
        .iter()
        .filter(|d| {
            if !matches!(d.placement, PodPlacement::Unplaced { .. }) {
                return false;
            }
            let mut throttled = false;
            for caveat in &d.caveats {
                if caveat.contains("quota exhausted") {
                    exhausted_groups.insert(caveat.clone());
                    throttled = true;
                }
            }
            throttled
        })
        .count();
    QuotaMetrics {
        throttled_pods,
        exhausted_groups: exhausted_groups.len(),
    }
}

fn deadline_metrics(decisions: &[PodDecision]) -> DeadlineMetrics {
    let mut metrics = DeadlineMetrics::default();
    let mut worst_slack: Option<i64> = None;

    for d in decisions.iter().filter(|d| d.deadline_unix_seconds > 0) {
        metrics.deadline_jobs += 1;
        let placed = matches!(&d.placement, PodPlacement::Placed { .. });
        if placed {
            metrics.placed_deadline_jobs += 1;
        } else {
            metrics.unplaced_deadline_jobs += 1;
        }
        if d.predicted_deadline_miss {
            metrics.predicted_misses += 1;
            if placed {
                metrics.placed_predicted_misses += 1;
            } else {
                metrics.unplaced_predicted_misses += 1;
            }
        }
        worst_slack = Some(worst_slack.map_or(d.deadline_slack_seconds, |w| {
            w.min(d.deadline_slack_seconds)
        }));
    }

    metrics.worst_slack_seconds = worst_slack.unwrap_or(0);
    metrics
}

fn queue_wait_metrics(decisions: &[PodDecision]) -> crate::scheduler::trace::QueueWaitMetrics {
    let mut metrics = crate::scheduler::trace::QueueWaitMetrics {
        pending_pods: decisions.len(),
        ..Default::default()
    };
    for d in decisions {
        let wait = d.queue_wait_seconds.max(0);
        metrics.max_queue_wait_seconds = metrics.max_queue_wait_seconds.max(wait);
        if d.priority > 0 {
            metrics.high_priority_pending_pods += 1;
            metrics.high_priority_max_queue_wait_seconds =
                metrics.high_priority_max_queue_wait_seconds.max(wait);
        }
        if matches!(d.placement, PodPlacement::Unplaced { .. }) {
            metrics.unplaced_max_queue_wait_seconds =
                metrics.unplaced_max_queue_wait_seconds.max(wait);
        }
    }
    metrics
}

fn is_quota_throttled(d: &PodDecision) -> bool {
    matches!(d.placement, PodPlacement::Unplaced { .. })
        && d.caveats.iter().any(|c| c.contains("quota exhausted"))
}

fn decision_tenant(d: &PodDecision) -> String {
    if d.team.trim().is_empty() {
        d.namespace.clone()
    } else {
        d.team.clone()
    }
}

fn tenant_fairness_metrics(decisions: &[PodDecision]) -> TenantFairnessMetrics {
    tenant_fairness_metrics_with_policy(
        decisions,
        &OptimizationInput::default(),
        &BTreeMap::new(),
        &BTreeMap::new(),
    )
}

fn tenant_fairness_metrics_with_policy(
    decisions: &[PodDecision],
    input: &OptimizationInput,
    tenant_share_weights: &BTreeMap<String, i64>,
    tenant_monthly_budgets_milli: &BTreeMap<String, i64>,
) -> TenantFairnessMetrics {
    let mut by_tenant: BTreeMap<String, TenantQueueMetric> = BTreeMap::new();
    let per_placement_monthly_cost_milli = placement_monthly_cost_milli(decisions, input);

    for d in decisions {
        let tenant = decision_tenant(d);
        let wait = d.queue_wait_seconds.max(0);
        let gpu_request = d.gpu_request.max(0);
        let entry = by_tenant
            .entry(tenant.clone())
            .or_insert_with(|| TenantQueueMetric {
                fair_share_weight: tenant_share_weights
                    .get(&tenant)
                    .copied()
                    .unwrap_or(1)
                    .max(1),
                tenant,
                ..Default::default()
            });
        entry.pending_pods += 1;
        entry.requested_gpu_demand += gpu_request;
        entry.max_queue_wait_seconds = entry.max_queue_wait_seconds.max(wait);

        match d.placement {
            PodPlacement::Placed { .. } => {
                entry.placed_pods += 1;
                entry.admitted_gpu_demand += gpu_request;
                entry.admitted_monthly_cost_milli += per_placement_monthly_cost_milli
                    .get(&pod_key(&d.namespace, &d.name))
                    .copied()
                    .unwrap_or(0);
            }
            PodPlacement::Unplaced { .. } => {
                entry.unplaced_pods += 1;
                entry.denied_gpu_demand += gpu_request;
            }
        }

        if is_quota_throttled(d) {
            entry.throttled_pods += 1;
            entry.throttled_max_queue_wait_seconds =
                entry.throttled_max_queue_wait_seconds.max(wait);
        }
    }

    let total_admitted_gpu_demand: i64 = by_tenant
        .values()
        .map(|t| t.admitted_gpu_demand.max(0))
        .sum();
    let total_weight: i64 = by_tenant.values().map(|t| t.fair_share_weight.max(1)).sum();

    for t in by_tenant.values_mut() {
        if total_admitted_gpu_demand > 0 {
            t.admitted_share_milli =
                t.admitted_gpu_demand.max(0).saturating_mul(1000) / total_admitted_gpu_demand;
        }
        if total_admitted_gpu_demand > 0 && total_weight > 0 {
            t.fair_share_gpu_milli = total_admitted_gpu_demand
                .saturating_mul(1000)
                .saturating_mul(t.fair_share_weight.max(1))
                / total_weight;
            t.fair_share_delta_gpu_milli = t
                .admitted_gpu_demand
                .max(0)
                .saturating_mul(1000)
                .saturating_sub(t.fair_share_gpu_milli);
            t.under_fair_share_gpu_milli = (-t.fair_share_delta_gpu_milli).max(0);
            t.borrowed_gpu_milli = t.fair_share_delta_gpu_milli.max(0);
        }
    }

    let mut reclaimable_demand_gpu_milli: i64 = by_tenant
        .values()
        .filter(|t| t.denied_gpu_demand > 0)
        .map(|t| t.under_fair_share_gpu_milli)
        .sum();
    for t in by_tenant.values_mut().filter(|t| t.borrowed_gpu_milli > 0) {
        if reclaimable_demand_gpu_milli <= 0 {
            break;
        }
        let reclaimable = t.borrowed_gpu_milli.min(reclaimable_demand_gpu_milli);
        t.reclaimable_borrowed_gpu_milli = reclaimable;
        reclaimable_demand_gpu_milli = reclaimable_demand_gpu_milli.saturating_sub(reclaimable);
    }

    let mut tenants: Vec<TenantQueueMetric> = by_tenant.into_values().collect();
    let throttled_pods = tenants.iter().map(|t| t.throttled_pods).sum();
    let throttled_max_queue_wait_seconds = tenants
        .iter()
        .map(|t| t.throttled_max_queue_wait_seconds)
        .max()
        .unwrap_or(0);
    let under_fair_share_tenants = tenants
        .iter()
        .filter(|t| t.under_fair_share_gpu_milli > 0)
        .count();
    let over_fair_share_tenants = tenants.iter().filter(|t| t.borrowed_gpu_milli > 0).count();
    let total_borrowed_gpu_milli = tenants.iter().map(|t| t.borrowed_gpu_milli).sum();
    let reclaimable_borrowed_gpu_milli = tenants
        .iter()
        .map(|t| t.reclaimable_borrowed_gpu_milli)
        .sum();
    for t in &mut tenants {
        if let Some(budget) = tenant_monthly_budgets_milli.get(&t.tenant) {
            t.budget_monthly_milli = (*budget).max(0);
            t.budget_overage_monthly_milli = t
                .admitted_monthly_cost_milli
                .saturating_sub(t.budget_monthly_milli);
        }
    }
    let budget_over_tenants = tenants
        .iter()
        .filter(|t| t.budget_overage_monthly_milli > 0)
        .count();
    let total_budget_overage_monthly_milli =
        tenants.iter().map(|t| t.budget_overage_monthly_milli).sum();

    TenantFairnessMetrics {
        tenants,
        throttled_pods,
        throttled_max_queue_wait_seconds,
        under_fair_share_tenants,
        over_fair_share_tenants,
        total_borrowed_gpu_milli,
        reclaimable_borrowed_gpu_milli,
        budget_over_tenants,
        total_budget_overage_monthly_milli,
    }
}

fn apply_tenant_policy_caveats(decisions: &mut [PodDecision], metrics: &TenantFairnessMetrics) {
    let by_tenant: BTreeMap<&str, &TenantQueueMetric> = metrics
        .tenants
        .iter()
        .map(|t| (t.tenant.as_str(), t))
        .collect();
    for d in decisions {
        let tenant = decision_tenant(d);
        let Some(t) = by_tenant.get(tenant.as_str()) else {
            continue;
        };
        d.caveats.retain(|c| {
            !(c.starts_with("tenant ")
                && (c.contains("below fair share") || c.contains("over monthly budget")))
        });
        if matches!(d.placement, PodPlacement::Unplaced { .. })
            && t.denied_gpu_demand > 0
            && t.under_fair_share_gpu_milli > 0
            && metrics.reclaimable_borrowed_gpu_milli > 0
        {
            d.caveats.push(format!(
                "tenant {tenant} is below fair share by {} GPU-milli while {} borrowed GPU-milli is reclaimable",
                t.under_fair_share_gpu_milli,
                metrics.reclaimable_borrowed_gpu_milli
            ));
        }
        if t.budget_overage_monthly_milli > 0 {
            d.caveats.push(format!(
                "tenant {tenant} is over monthly budget by {} milli-units",
                t.budget_overage_monthly_milli
            ));
        }
    }
}

fn placement_monthly_cost_milli(
    decisions: &[PodDecision],
    input: &OptimizationInput,
) -> BTreeMap<String, i64> {
    let node_by_name: BTreeMap<&str, &crate::model::OptimizationNode> =
        input.nodes.iter().map(|n| (n.name.as_str(), n)).collect();
    let mut out = BTreeMap::new();
    for d in decisions {
        let PodPlacement::Placed { node } = &d.placement else {
            continue;
        };
        let Some(n) = node_by_name.get(node.as_str()) else {
            continue;
        };
        let gpu_capacity = node_gpu_capacity(n);
        if gpu_capacity <= 0 || n.price.monthly <= 0.0 || d.gpu_request <= 0 {
            continue;
        }
        let cost = (n.price.monthly * 1000.0 * d.gpu_request as f64 / gpu_capacity as f64).round();
        if cost.is_finite() && cost > 0.0 {
            out.insert(pod_key(&d.namespace, &d.name), cost as i64);
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{
        Money, OptimizationInput, OptimizationNode, OptimizationSolution, OptimizationWorkload,
        OptimizationWorkloadMember,
    };
    use crate::scheduler::pod_filter::PendingGpuPod;
    use crate::scheduler::trace::PodPlacement;
    use std::collections::{BTreeMap, HashMap, HashSet};

    fn ppod(ns: &str, name: &str) -> PendingGpuPod {
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
            gang_key: Some(format!("{ns}/job")),
            colocate: false,
            unmodeled_constraints: vec![],
            anti_affinity_host_selectors: vec![],
            affinity_topology_selectors: vec![],
            anti_affinity_topology_selectors: vec![],
            preferred_node_affinity: vec![],
            preferred_pod_affinity: vec![],
        }
    }

    fn member(ns: &str, n: &str) -> OptimizationWorkloadMember {
        OptimizationWorkloadMember {
            namespace: ns.into(),
            name: n.into(),
            current_node: String::new(),
        }
    }

    #[test]
    fn gang_members_share_admission() {
        let gang = OptimizationWorkload {
            id: "gang:team/job".into(),
            namespace: "team".into(),
            name: "m0".into(),
            group_size: 2,
            members: vec![member("team", "m0"), member("team", "m1")],
            feasible_nodes: vec!["n1".into()],
            ..Default::default()
        };
        let input = OptimizationInput {
            workloads: vec![gang],
            ..Default::default()
        };
        let mut counts = HashMap::new();
        counts.insert("n1".to_string(), 2);
        let mut assignment_counts = HashMap::new();
        assignment_counts.insert("gang:team/job".to_string(), counts);
        let solution = OptimizationSolution {
            assignment_counts,
            ..Default::default()
        };
        let pending = vec![ppod("team", "m0"), ppod("team", "m1")];
        let t = build_decision_trace(
            1,
            &pending,
            &input,
            &solution,
            "OPTIMAL",
            true,
            5,
            5,
            1,
            &HashMap::new(),
            &HashSet::new(),
        );
        assert!(t
            .decisions
            .iter()
            .all(|d| matches!(&d.placement, PodPlacement::Placed { node } if node == "n1")));
        assert!(t
            .decisions
            .iter()
            .all(|d| d.binding_group == "gang:team/job"));
    }

    #[test]
    fn flexible_partial_gang_marks_selected_and_deferred_members() {
        let mut ext = std::collections::BTreeMap::new();
        ext.insert("nvidia.com/gpu".to_string(), 8);
        let gang = OptimizationWorkload {
            id: "gang:team/job".into(),
            namespace: "team".into(),
            name: "m0".into(),
            group_size: 8,
            members: (0..8).map(|i| member("team", &format!("m{i}"))).collect(),
            extended_resource_requests: ext,
            min_gpus: 2,
            max_gpus: 8,
            preferred_gpus: 4,
            flexible: true,
            feasible_nodes: vec!["n1".into()],
            ..Default::default()
        };
        let input = OptimizationInput {
            workloads: vec![gang],
            ..Default::default()
        };
        let mut counts = HashMap::new();
        counts.insert("n1".to_string(), 4);
        let mut assignment_counts = HashMap::new();
        assignment_counts.insert("gang:team/job".to_string(), counts);
        let solution = OptimizationSolution {
            assignment_counts,
            ..Default::default()
        };
        let pending: Vec<_> = (0..8)
            .map(|i| {
                let mut p = ppod("team", &format!("m{i}"));
                p.min_gpus = 2;
                p.max_gpus = 8;
                p.preferred_gpus = 4;
                p.flexible = true;
                p
            })
            .collect();
        let t = build_decision_trace(
            1,
            &pending,
            &input,
            &solution,
            "OPTIMAL",
            true,
            5,
            5,
            1,
            &HashMap::new(),
            &HashSet::new(),
        );
        let placed = t
            .decisions
            .iter()
            .filter(|d| matches!(d.placement, PodPlacement::Placed { .. }))
            .count();
        let deferred = t
            .decisions
            .iter()
            .filter(|d| matches!(&d.placement, PodPlacement::Unplaced { reason } if reason.contains("selected 4/8")))
            .count();
        assert_eq!(placed, 4);
        assert_eq!(deferred, 4);
        assert!(t
            .decisions
            .iter()
            .filter(|d| matches!(d.placement, PodPlacement::Placed { .. }))
            .all(|d| d.caveats.iter().any(|c| c.contains("selected 4/8"))));
    }

    #[test]
    fn flexible_deadline_trace_explains_deadline_replica_cap() {
        let mut ext = std::collections::BTreeMap::new();
        ext.insert("nvidia.com/gpu".to_string(), 8);
        let gang = OptimizationWorkload {
            id: "gang:team/job".into(),
            namespace: "team".into(),
            name: "m0".into(),
            group_size: 8,
            members: (0..8).map(|i| member("team", &format!("m{i}"))).collect(),
            extended_resource_requests: ext,
            min_gpus: 2,
            max_gpus: 8,
            preferred_gpus: 8,
            flexible: true,
            predicted_runtime_seconds: 3600,
            deadline_unix_seconds: chrono::Utc::now().timestamp() + 10_000,
            feasible_nodes: vec!["n1".into()],
            ..Default::default()
        };
        let input = OptimizationInput {
            workloads: vec![gang],
            ..Default::default()
        };
        let mut counts = HashMap::new();
        counts.insert("n1".to_string(), 2);
        let mut assignment_counts = HashMap::new();
        assignment_counts.insert("gang:team/job".to_string(), counts);
        let solution = OptimizationSolution {
            assignment_counts,
            ..Default::default()
        };
        let pending: Vec<_> = (0..8)
            .map(|i| {
                let mut p = ppod("team", &format!("m{i}"));
                p.min_gpus = 2;
                p.max_gpus = 8;
                p.preferred_gpus = 8;
                p.flexible = true;
                p.predicted_runtime_seconds = 3600;
                p.deadline_unix_seconds = chrono::Utc::now().timestamp() + 10_000;
                p
            })
            .collect();

        let t = build_decision_trace(
            1,
            &pending,
            &input,
            &solution,
            "OPTIMAL",
            true,
            5,
            5,
            1,
            &HashMap::new(),
            &HashSet::new(),
        );

        assert!(t.decisions.iter().all(|d| {
            d.caveats.iter().any(|c| {
                c.contains("selected 2/8")
                    && c.contains("deadline slack capped eligible replicas at 2")
            })
        }));
    }

    #[test]
    fn lower_priority_unplaced_pod_gets_deferred_caveat() {
        let low = OptimizationWorkload {
            id: "team/low".into(),
            namespace: "team".into(),
            name: "low".into(),
            group_size: 1,
            members: vec![member("team", "low")],
            priority: 1,
            feasible_nodes: vec!["n1".into()],
            ..Default::default()
        };
        let high = OptimizationWorkload {
            id: "team/high".into(),
            namespace: "team".into(),
            name: "high".into(),
            group_size: 1,
            members: vec![member("team", "high")],
            priority: 9,
            feasible_nodes: vec!["n1".into()],
            ..Default::default()
        };
        let input = OptimizationInput {
            workloads: vec![low, high],
            ..Default::default()
        };
        let mut counts = HashMap::new();
        counts.insert("n1".to_string(), 1);
        let mut assignment_counts = HashMap::new();
        assignment_counts.insert("team/high".to_string(), counts);
        let solution = OptimizationSolution {
            assignment_counts,
            ..Default::default()
        };
        let mut low_pending = ppod("team", "low");
        low_pending.priority = 1;
        let mut high_pending = ppod("team", "high");
        high_pending.priority = 9;
        let t = build_decision_trace(
            1,
            &[low_pending, high_pending],
            &input,
            &solution,
            "OPTIMAL",
            true,
            5,
            5,
            1,
            &HashMap::new(),
            &HashSet::new(),
        );
        let low_decision = t.decisions.iter().find(|d| d.name == "low").unwrap();
        assert!(
            low_decision
                .caveats
                .iter()
                .any(|c| c.contains("deferred below admitted higher-priority work")),
            "low-priority unplaced pod should explain priority deferral"
        );
    }

    #[test]
    fn lower_business_value_unplaced_pod_gets_deferred_caveat() {
        let low = OptimizationWorkload {
            id: "team/low".into(),
            namespace: "team".into(),
            name: "low".into(),
            group_size: 1,
            members: vec![member("team", "low")],
            priority: 5,
            business_value: 1,
            feasible_nodes: vec!["n1".into()],
            ..Default::default()
        };
        let high = OptimizationWorkload {
            id: "team/high".into(),
            namespace: "team".into(),
            name: "high".into(),
            group_size: 1,
            members: vec![member("team", "high")],
            priority: 5,
            business_value: 20,
            feasible_nodes: vec!["n1".into()],
            ..Default::default()
        };
        let input = OptimizationInput {
            workloads: vec![low, high],
            ..Default::default()
        };
        let mut counts = HashMap::new();
        counts.insert("n1".to_string(), 1);
        let mut assignment_counts = HashMap::new();
        assignment_counts.insert("team/high".to_string(), counts);
        let solution = OptimizationSolution {
            assignment_counts,
            ..Default::default()
        };
        let mut low_pending = ppod("team", "low");
        low_pending.priority = 5;
        low_pending.business_value = 1;
        let mut high_pending = ppod("team", "high");
        high_pending.priority = 5;
        high_pending.business_value = 20;
        let t = build_decision_trace(
            1,
            &[low_pending, high_pending],
            &input,
            &solution,
            "OPTIMAL",
            true,
            5,
            5,
            1,
            &HashMap::new(),
            &HashSet::new(),
        );
        let low_decision = t.decisions.iter().find(|d| d.name == "low").unwrap();
        assert!(
            low_decision
                .caveats
                .iter()
                .any(|c| c.contains("deferred below admitted higher-business-value work")),
            "low-business-value unplaced pod should explain business-value deferral"
        );
    }

    #[test]
    fn lower_fair_share_deficit_unplaced_pod_gets_deferred_caveat() {
        // Equal on priority/business-value/queue/queue-wait, differing ONLY in fair-share deficit:
        // the deferred pod must be told it lost to more under-fair-share (under-served tenant) work.
        let low = OptimizationWorkload {
            id: "over-share/low".into(),
            namespace: "over-share".into(),
            name: "low".into(),
            group_size: 1,
            members: vec![member("over-share", "low")],
            priority: 5,
            business_value: 5,
            fair_share_deficit: 1,
            feasible_nodes: vec!["n1".into()],
            ..Default::default()
        };
        let high = OptimizationWorkload {
            id: "under-share/high".into(),
            namespace: "under-share".into(),
            name: "high".into(),
            group_size: 1,
            members: vec![member("under-share", "high")],
            priority: 5,
            business_value: 5,
            fair_share_deficit: 20,
            feasible_nodes: vec!["n1".into()],
            ..Default::default()
        };
        let input = OptimizationInput {
            workloads: vec![low, high],
            ..Default::default()
        };
        let mut counts = HashMap::new();
        counts.insert("n1".to_string(), 1);
        let mut assignment_counts = HashMap::new();
        assignment_counts.insert("under-share/high".to_string(), counts);
        let solution = OptimizationSolution {
            assignment_counts,
            ..Default::default()
        };
        let mut low_pending = ppod("over-share", "low");
        low_pending.priority = 5;
        low_pending.business_value = 5;
        let mut high_pending = ppod("under-share", "high");
        high_pending.priority = 5;
        high_pending.business_value = 5;
        let t = build_decision_trace(
            1,
            &[low_pending, high_pending],
            &input,
            &solution,
            "OPTIMAL",
            true,
            5,
            5,
            1,
            &HashMap::new(),
            &HashSet::new(),
        );
        let low_decision = t.decisions.iter().find(|d| d.name == "low").unwrap();
        assert!(
            low_decision
                .caveats
                .iter()
                .any(|c| c.contains("deferred below admitted more under-fair-share work")),
            "over-share unplaced pod should explain fair-share deferral, got: {:?}",
            low_decision.caveats
        );
    }

    #[test]
    fn lower_queue_score_unplaced_pod_gets_deferred_caveat() {
        let low = OptimizationWorkload {
            id: "team/low".into(),
            namespace: "team".into(),
            name: "low".into(),
            group_size: 1,
            members: vec![member("team", "low")],
            queue: "batch".into(),
            queue_score: 10,
            feasible_nodes: vec!["n1".into()],
            ..Default::default()
        };
        let high = OptimizationWorkload {
            id: "team/high".into(),
            namespace: "team".into(),
            name: "high".into(),
            group_size: 1,
            members: vec![member("team", "high")],
            queue: "urgent".into(),
            queue_score: 100,
            feasible_nodes: vec!["n1".into()],
            ..Default::default()
        };
        let input = OptimizationInput {
            workloads: vec![low, high],
            ..Default::default()
        };
        let mut counts = HashMap::new();
        counts.insert("n1".to_string(), 1);
        let mut assignment_counts = HashMap::new();
        assignment_counts.insert("team/high".to_string(), counts);
        let solution = OptimizationSolution {
            assignment_counts,
            ..Default::default()
        };
        let mut low_pending = ppod("team", "low");
        low_pending.queue = Some("batch".into());
        let mut high_pending = ppod("team", "high");
        high_pending.queue = Some("urgent".into());
        let t = build_decision_trace(
            1,
            &[low_pending, high_pending],
            &input,
            &solution,
            "OPTIMAL",
            true,
            5,
            5,
            1,
            &HashMap::new(),
            &HashSet::new(),
        );
        let low_decision = t.decisions.iter().find(|d| d.name == "low").unwrap();
        assert!(
            low_decision
                .caveats
                .iter()
                .any(|c| c.contains("deferred below admitted higher-queue work")),
            "low-queue-score unplaced pod should explain queue deferral"
        );
    }

    #[test]
    fn shorter_wait_unplaced_pod_gets_deferred_caveat() {
        let fresh = OptimizationWorkload {
            id: "team/fresh".into(),
            namespace: "team".into(),
            name: "fresh".into(),
            group_size: 1,
            members: vec![member("team", "fresh")],
            queue_wait_seconds: 60,
            feasible_nodes: vec!["n1".into()],
            ..Default::default()
        };
        let waiting = OptimizationWorkload {
            id: "team/waiting".into(),
            namespace: "team".into(),
            name: "waiting".into(),
            group_size: 1,
            members: vec![member("team", "waiting")],
            queue_wait_seconds: 3_600,
            feasible_nodes: vec!["n1".into()],
            ..Default::default()
        };
        let input = OptimizationInput {
            workloads: vec![fresh, waiting],
            ..Default::default()
        };
        let mut counts = HashMap::new();
        counts.insert("n1".to_string(), 1);
        let mut assignment_counts = HashMap::new();
        assignment_counts.insert("team/waiting".to_string(), counts);
        let solution = OptimizationSolution {
            assignment_counts,
            ..Default::default()
        };
        let mut fresh_pending = ppod("team", "fresh");
        fresh_pending.queue_wait_seconds = 60;
        let mut waiting_pending = ppod("team", "waiting");
        waiting_pending.queue_wait_seconds = 3_600;
        let t = build_decision_trace(
            1,
            &[fresh_pending, waiting_pending],
            &input,
            &solution,
            "OPTIMAL",
            true,
            5,
            5,
            1,
            &HashMap::new(),
            &HashSet::new(),
        );
        let fresh_decision = t.decisions.iter().find(|d| d.name == "fresh").unwrap();
        assert!(
            fresh_decision
                .caveats
                .iter()
                .any(|c| c.contains("deferred below admitted longer-waiting work")),
            "shorter-wait unplaced pod should explain starvation deferral"
        );
    }

    #[test]
    fn relaxed_deadline_unplaced_pod_gets_deferred_caveat() {
        let urgent_deadline = 1_800_000_000;
        let relaxed_deadline = urgent_deadline + 86_400;
        let urgent = OptimizationWorkload {
            id: "team/urgent".into(),
            namespace: "team".into(),
            name: "urgent".into(),
            group_size: 1,
            members: vec![member("team", "urgent")],
            priority: 5,
            business_value: 10,
            deadline_unix_seconds: urgent_deadline,
            predicted_runtime_seconds: 7_200,
            feasible_nodes: vec!["n1".into()],
            ..Default::default()
        };
        let relaxed = OptimizationWorkload {
            id: "team/relaxed".into(),
            namespace: "team".into(),
            name: "relaxed".into(),
            group_size: 1,
            members: vec![member("team", "relaxed")],
            priority: 5,
            business_value: 10,
            deadline_unix_seconds: relaxed_deadline,
            predicted_runtime_seconds: 7_200,
            feasible_nodes: vec!["n1".into()],
            ..Default::default()
        };
        let input = OptimizationInput {
            workloads: vec![relaxed, urgent],
            ..Default::default()
        };
        let mut counts = HashMap::new();
        counts.insert("n1".to_string(), 1);
        let mut assignment_counts = HashMap::new();
        assignment_counts.insert("team/urgent".to_string(), counts);
        let solution = OptimizationSolution {
            assignment_counts,
            ..Default::default()
        };
        let mut relaxed_pending = ppod("team", "relaxed");
        relaxed_pending.priority = 5;
        relaxed_pending.business_value = 10;
        relaxed_pending.deadline_unix_seconds = relaxed_deadline;
        relaxed_pending.predicted_runtime_seconds = 7_200;
        let mut urgent_pending = ppod("team", "urgent");
        urgent_pending.priority = 5;
        urgent_pending.business_value = 10;
        urgent_pending.deadline_unix_seconds = urgent_deadline;
        urgent_pending.predicted_runtime_seconds = 7_200;

        let t = build_decision_trace(
            1,
            &[relaxed_pending, urgent_pending],
            &input,
            &solution,
            "OPTIMAL",
            true,
            5,
            5,
            1,
            &HashMap::new(),
            &HashSet::new(),
        );

        let relaxed_decision = t.decisions.iter().find(|d| d.name == "relaxed").unwrap();
        assert!(
            relaxed_decision
                .caveats
                .iter()
                .any(|c| c.contains("deferred below admitted more urgent deadline work")),
            "relaxed-deadline unplaced pod should explain deadline deferral"
        );
    }

    #[test]
    fn queue_wait_metadata_propagates_to_trace_metrics() {
        let mut placed = ppod("team", "placed");
        placed.priority = 5;
        placed.queue_wait_seconds = 120;
        let mut unplaced = ppod("team", "unplaced");
        unplaced.priority = 0;
        unplaced.queue_wait_seconds = 3600;
        let input = OptimizationInput {
            workloads: vec![
                OptimizationWorkload {
                    id: "team/placed".into(),
                    namespace: "team".into(),
                    name: "placed".into(),
                    group_size: 1,
                    members: vec![member("team", "placed")],
                    feasible_nodes: vec!["n1".into()],
                    ..Default::default()
                },
                OptimizationWorkload {
                    id: "team/unplaced".into(),
                    namespace: "team".into(),
                    name: "unplaced".into(),
                    group_size: 1,
                    members: vec![member("team", "unplaced")],
                    feasible_nodes: vec!["n1".into()],
                    ..Default::default()
                },
            ],
            ..Default::default()
        };
        let mut counts = HashMap::new();
        counts.insert("n1".to_string(), 1);
        let mut assignment_counts = HashMap::new();
        assignment_counts.insert("team/placed".to_string(), counts);
        let solution = OptimizationSolution {
            assignment_counts,
            ..Default::default()
        };

        let t = build_decision_trace(
            1,
            &[placed, unplaced],
            &input,
            &solution,
            "OPTIMAL",
            true,
            5,
            5,
            1,
            &HashMap::new(),
            &HashSet::new(),
        );

        assert_eq!(t.decisions[0].queue_wait_seconds, 120);
        assert_eq!(t.decisions[1].queue_wait_seconds, 3600);
        assert_eq!(t.queue_wait_metrics.pending_pods, 2);
        assert_eq!(t.queue_wait_metrics.max_queue_wait_seconds, 3600);
        assert_eq!(t.queue_wait_metrics.high_priority_pending_pods, 1);
        assert_eq!(
            t.queue_wait_metrics.high_priority_max_queue_wait_seconds,
            120
        );
        assert_eq!(t.queue_wait_metrics.unplaced_max_queue_wait_seconds, 3600);
    }

    #[test]
    fn quota_exhaustion_adds_unplaced_caveat_and_metrics() {
        let a = OptimizationWorkload {
            id: "team/a".into(),
            namespace: "team".into(),
            name: "a".into(),
            group_size: 1,
            members: vec![member("team", "a")],
            extended_resource_requests: std::collections::BTreeMap::from([(
                "nvidia.com/gpu".to_string(),
                1,
            )]),
            feasible_nodes: vec!["n1".into()],
            ..Default::default()
        };
        let b = OptimizationWorkload {
            id: "team/b".into(),
            namespace: "team".into(),
            name: "b".into(),
            group_size: 1,
            members: vec![member("team", "b")],
            extended_resource_requests: std::collections::BTreeMap::from([(
                "nvidia.com/gpu".to_string(),
                1,
            )]),
            feasible_nodes: vec!["n1".into()],
            ..Default::default()
        };
        let input = OptimizationInput {
            workloads: vec![a, b],
            quota_groups: vec![crate::model::QuotaGroup {
                workload_ids: vec!["team/a".into(), "team/b".into()],
                resources: vec!["nvidia.com/gpu".into()],
                limit: 1,
            }],
            ..Default::default()
        };
        let mut counts = HashMap::new();
        counts.insert("n1".to_string(), 1);
        let mut assignment_counts = HashMap::new();
        assignment_counts.insert("team/a".to_string(), counts);
        let solution = OptimizationSolution {
            assignment_counts,
            ..Default::default()
        };
        let t = build_decision_trace(
            1,
            &[ppod("team", "a"), ppod("team", "b")],
            &input,
            &solution,
            "OPTIMAL",
            true,
            5,
            5,
            1,
            &HashMap::new(),
            &HashSet::new(),
        );
        let b_decision = t.decisions.iter().find(|d| d.name == "b").unwrap();
        assert!(matches!(
            &b_decision.placement,
            PodPlacement::Unplaced { reason }
                if reason.contains("quota exhausted")
                    && reason.contains("1 selected / 1 allowed")
        ));
        assert!(b_decision
            .caveats
            .iter()
            .any(|c| c.contains("quota exhausted")));
        assert_eq!(t.quota_metrics.throttled_pods, 1);
        assert_eq!(t.quota_metrics.exhausted_groups, 1);
    }

    #[test]
    fn tenant_fairness_metrics_summarize_quota_throttling() {
        let a = OptimizationWorkload {
            id: "team-a/a".into(),
            namespace: "team-a".into(),
            name: "a".into(),
            group_size: 1,
            members: vec![member("team-a", "a")],
            extended_resource_requests: std::collections::BTreeMap::from([(
                "nvidia.com/gpu".to_string(),
                2,
            )]),
            feasible_nodes: vec!["n1".into()],
            ..Default::default()
        };
        let b = OptimizationWorkload {
            id: "team-a/b".into(),
            namespace: "team-a".into(),
            name: "b".into(),
            group_size: 1,
            members: vec![member("team-a", "b")],
            extended_resource_requests: std::collections::BTreeMap::from([(
                "nvidia.com/gpu".to_string(),
                2,
            )]),
            feasible_nodes: vec!["n1".into()],
            ..Default::default()
        };
        let c = OptimizationWorkload {
            id: "team-b/c".into(),
            namespace: "team-b".into(),
            name: "c".into(),
            group_size: 1,
            members: vec![member("team-b", "c")],
            extended_resource_requests: std::collections::BTreeMap::from([(
                "nvidia.com/gpu".to_string(),
                1,
            )]),
            feasible_nodes: vec!["n1".into()],
            ..Default::default()
        };
        let input = OptimizationInput {
            workloads: vec![a, b, c],
            quota_groups: vec![crate::model::QuotaGroup {
                workload_ids: vec!["team-a/a".into(), "team-a/b".into()],
                resources: vec!["nvidia.com/gpu".into()],
                limit: 2,
            }],
            ..Default::default()
        };
        let mut a_counts = HashMap::new();
        a_counts.insert("n1".to_string(), 1);
        let mut assignment_counts = HashMap::new();
        assignment_counts.insert("team-a/a".to_string(), a_counts);
        let solution = OptimizationSolution {
            assignment_counts,
            ..Default::default()
        };
        let mut a_pending = ppod("team-a", "a");
        a_pending.team = Some("research".into());
        a_pending.gpu_request = 2;
        a_pending.queue_wait_seconds = 30;
        let mut b_pending = ppod("team-a", "b");
        b_pending.team = Some("research".into());
        b_pending.gpu_request = 2;
        b_pending.queue_wait_seconds = 120;
        let mut c_pending = ppod("team-b", "c");
        c_pending.gpu_request = 1;
        c_pending.queue_wait_seconds = 60;

        let t = build_decision_trace(
            1,
            &[a_pending, b_pending, c_pending],
            &input,
            &solution,
            "OPTIMAL",
            true,
            5,
            5,
            1,
            &HashMap::new(),
            &HashSet::new(),
        );

        assert_eq!(t.tenant_fairness_metrics.throttled_pods, 1);
        assert_eq!(
            t.tenant_fairness_metrics.throttled_max_queue_wait_seconds,
            120
        );
        assert_eq!(t.tenant_fairness_metrics.tenants.len(), 2);
        assert_eq!(t.tenant_fairness_metrics.tenants[0].tenant, "research");
        assert_eq!(t.tenant_fairness_metrics.tenants[0].pending_pods, 2);
        assert_eq!(t.tenant_fairness_metrics.tenants[0].placed_pods, 1);
        assert_eq!(t.tenant_fairness_metrics.tenants[0].unplaced_pods, 1);
        assert_eq!(t.tenant_fairness_metrics.tenants[0].requested_gpu_demand, 4);
        assert_eq!(t.tenant_fairness_metrics.tenants[0].admitted_gpu_demand, 2);
        assert_eq!(t.tenant_fairness_metrics.tenants[0].throttled_pods, 1);
        assert_eq!(
            t.tenant_fairness_metrics.tenants[0].throttled_max_queue_wait_seconds,
            120
        );
        assert_eq!(t.tenant_fairness_metrics.tenants[1].tenant, "team-b");
        assert_eq!(t.tenant_fairness_metrics.tenants[1].pending_pods, 1);
        assert_eq!(
            t.tenant_fairness_metrics.tenants[1].max_queue_wait_seconds,
            60
        );
    }

    #[test]
    fn tenant_fairness_metrics_include_weighted_share_targets() {
        let input = OptimizationInput {
            workloads: vec![
                OptimizationWorkload {
                    id: "team-a/a".into(),
                    namespace: "team-a".into(),
                    name: "a".into(),
                    group_size: 1,
                    members: vec![member("team-a", "a")],
                    feasible_nodes: vec!["n1".into()],
                    ..Default::default()
                },
                OptimizationWorkload {
                    id: "team-b/b".into(),
                    namespace: "team-b".into(),
                    name: "b".into(),
                    group_size: 1,
                    members: vec![member("team-b", "b")],
                    feasible_nodes: vec!["n1".into()],
                    ..Default::default()
                },
            ],
            ..Default::default()
        };
        let mut assignment_counts = HashMap::new();
        assignment_counts.insert(
            "team-a/a".to_string(),
            HashMap::from([("n1".to_string(), 1)]),
        );
        assignment_counts.insert(
            "team-b/b".to_string(),
            HashMap::from([("n1".to_string(), 1)]),
        );
        let solution = OptimizationSolution {
            assignment_counts,
            ..Default::default()
        };
        let mut a_pending = ppod("team-a", "a");
        a_pending.team = Some("research".into());
        a_pending.gpu_request = 2;
        let mut b_pending = ppod("team-b", "b");
        b_pending.gpu_request = 1;

        let t = build_decision_trace_with_tenant_weights(
            1,
            &[a_pending, b_pending],
            &input,
            &solution,
            "OPTIMAL",
            true,
            5,
            5,
            1,
            &HashMap::new(),
            &HashSet::new(),
            &BTreeMap::from([("research".to_string(), 3)]),
        );

        let research = &t.tenant_fairness_metrics.tenants[0];
        assert_eq!(research.tenant, "research");
        assert_eq!(research.fair_share_weight, 3);
        assert_eq!(research.admitted_gpu_demand, 2);
        assert_eq!(research.admitted_share_milli, 666);
        assert_eq!(research.fair_share_gpu_milli, 2250);
        assert_eq!(research.fair_share_delta_gpu_milli, -250);
        assert_eq!(research.under_fair_share_gpu_milli, 250);

        let team_b = &t.tenant_fairness_metrics.tenants[1];
        assert_eq!(team_b.tenant, "team-b");
        assert_eq!(team_b.fair_share_weight, 1);
        assert_eq!(team_b.admitted_share_milli, 333);
        assert_eq!(team_b.fair_share_gpu_milli, 750);
        assert_eq!(team_b.fair_share_delta_gpu_milli, 250);
        assert_eq!(team_b.under_fair_share_gpu_milli, 0);
    }

    #[test]
    fn tenant_fairness_metrics_mark_reclaimable_borrowed_capacity() {
        let input = OptimizationInput {
            workloads: vec![
                OptimizationWorkload {
                    id: "team-a/a".into(),
                    namespace: "team-a".into(),
                    name: "a".into(),
                    group_size: 1,
                    members: vec![member("team-a", "a")],
                    feasible_nodes: vec!["n1".into()],
                    ..Default::default()
                },
                OptimizationWorkload {
                    id: "team-b/b".into(),
                    namespace: "team-b".into(),
                    name: "b".into(),
                    group_size: 1,
                    members: vec![member("team-b", "b")],
                    feasible_nodes: vec!["n1".into()],
                    ..Default::default()
                },
            ],
            ..Default::default()
        };
        let mut assignment_counts = HashMap::new();
        assignment_counts.insert(
            "team-b/b".to_string(),
            HashMap::from([("n1".to_string(), 1)]),
        );
        let solution = OptimizationSolution {
            assignment_counts,
            ..Default::default()
        };
        let mut a_pending = ppod("team-a", "a");
        a_pending.team = Some("research".into());
        a_pending.gpu_request = 1;
        let mut b_pending = ppod("team-b", "b");
        b_pending.gpu_request = 3;

        let t = build_decision_trace_with_tenant_weights(
            1,
            &[a_pending, b_pending],
            &input,
            &solution,
            "OPTIMAL",
            true,
            5,
            5,
            1,
            &HashMap::new(),
            &HashSet::new(),
            &BTreeMap::from([("research".to_string(), 3)]),
        );

        let research = &t.tenant_fairness_metrics.tenants[0];
        assert_eq!(research.tenant, "research");
        assert_eq!(research.denied_gpu_demand, 1);
        assert_eq!(research.under_fair_share_gpu_milli, 2250);
        assert_eq!(research.borrowed_gpu_milli, 0);

        let team_b = &t.tenant_fairness_metrics.tenants[1];
        assert_eq!(team_b.tenant, "team-b");
        assert_eq!(team_b.denied_gpu_demand, 0);
        assert_eq!(team_b.borrowed_gpu_milli, 2250);
        assert_eq!(team_b.reclaimable_borrowed_gpu_milli, 2250);

        assert_eq!(t.tenant_fairness_metrics.under_fair_share_tenants, 1);
        assert_eq!(t.tenant_fairness_metrics.over_fair_share_tenants, 1);
        assert_eq!(t.tenant_fairness_metrics.total_borrowed_gpu_milli, 2250);
        assert_eq!(
            t.tenant_fairness_metrics.reclaimable_borrowed_gpu_milli,
            2250
        );
        let research_decision = t
            .decisions
            .iter()
            .find(|d| d.namespace == "team-a" && d.name == "a")
            .expect("research decision");
        assert!(matches!(
            research_decision.placement,
            PodPlacement::Unplaced { .. }
        ));
        assert!(
            research_decision.caveats.iter().any(|c| {
                c.contains("tenant research is below fair share by 2250 GPU-milli")
                    && c.contains("2250 borrowed GPU-milli is reclaimable")
            }),
            "under-share denied tenant should explain reclaimable borrowed capacity: {:?}",
            research_decision.caveats
        );
    }

    #[test]
    fn tenant_fairness_metrics_include_monthly_budget_overage() {
        let input = OptimizationInput {
            nodes: vec![OptimizationNode {
                name: "n1".into(),
                price: Money {
                    currency: "USD".into(),
                    monthly: 4000.0,
                },
                extended_resources: BTreeMap::from([("nvidia.com/gpu".to_string(), 4)]),
                ..Default::default()
            }],
            workloads: vec![OptimizationWorkload {
                id: "team-a/a".into(),
                namespace: "team-a".into(),
                name: "a".into(),
                group_size: 1,
                members: vec![member("team-a", "a")],
                feasible_nodes: vec!["n1".into()],
                ..Default::default()
            }],
            ..Default::default()
        };
        let solution = OptimizationSolution {
            assignment_counts: HashMap::from([(
                "team-a/a".to_string(),
                HashMap::from([("n1".to_string(), 1)]),
            )]),
            ..Default::default()
        };
        let mut pending = ppod("team-a", "a");
        pending.team = Some("research".into());
        pending.gpu_request = 2;

        let t = build_decision_trace_with_tenant_policy(
            1,
            &[pending],
            &input,
            &solution,
            "OPTIMAL",
            true,
            5,
            5,
            1,
            &HashMap::new(),
            &HashSet::new(),
            &BTreeMap::new(),
            &BTreeMap::from([("research".to_string(), 1_500_000)]),
        );

        let research = &t.tenant_fairness_metrics.tenants[0];
        assert_eq!(research.tenant, "research");
        assert_eq!(research.admitted_monthly_cost_milli, 2_000_000);
        assert_eq!(research.budget_monthly_milli, 1_500_000);
        assert_eq!(research.budget_overage_monthly_milli, 500_000);
        assert_eq!(t.tenant_fairness_metrics.budget_over_tenants, 1);
        assert_eq!(
            t.tenant_fairness_metrics.total_budget_overage_monthly_milli,
            500_000
        );
        let research_decision = t
            .decisions
            .iter()
            .find(|d| d.namespace == "team-a" && d.name == "a")
            .expect("research decision");
        assert!(
            research_decision
                .caveats
                .iter()
                .any(|c| c.contains("tenant research is over monthly budget by 500000 milli-units")),
            "budget-over tenant should be visible in decision caveats: {:?}",
            research_decision.caveats
        );
    }

    #[test]
    fn budget_group_exhaustion_explains_unplaced_workload() {
        let input = OptimizationInput {
            nodes: vec![OptimizationNode {
                name: "n1".into(),
                price: Money {
                    currency: "USD".into(),
                    monthly: 4000.0,
                },
                extended_resources: BTreeMap::from([("nvidia.com/gpu".to_string(), 4)]),
                ..Default::default()
            }],
            workloads: vec![
                OptimizationWorkload {
                    id: "team-a/a".into(),
                    namespace: "team-a".into(),
                    name: "a".into(),
                    group_size: 1,
                    members: vec![member("team-a", "a")],
                    extended_resource_requests: BTreeMap::from([("nvidia.com/gpu".to_string(), 1)]),
                    feasible_nodes: vec!["n1".into()],
                    ..Default::default()
                },
                OptimizationWorkload {
                    id: "team-a/b".into(),
                    namespace: "team-a".into(),
                    name: "b".into(),
                    group_size: 1,
                    members: vec![member("team-a", "b")],
                    extended_resource_requests: BTreeMap::from([("nvidia.com/gpu".to_string(), 1)]),
                    feasible_nodes: vec!["n1".into()],
                    ..Default::default()
                },
            ],
            budget_groups: vec![crate::model::BudgetGroup {
                name: "research".to_string(),
                workload_ids: vec!["team-a/a".to_string(), "team-a/b".to_string()],
                limit_milli: 1_000_000,
            }],
            ..Default::default()
        };
        let solution = OptimizationSolution {
            assignment_counts: HashMap::from([(
                "team-a/a".to_string(),
                HashMap::from([("n1".to_string(), 1)]),
            )]),
            ..Default::default()
        };
        let mut pending_a = ppod("team-a", "a");
        pending_a.team = Some("research".into());
        pending_a.gpu_request = 1;
        let mut pending_b = ppod("team-a", "b");
        pending_b.team = Some("research".into());
        pending_b.gpu_request = 1;

        let t = build_decision_trace_with_tenant_policy(
            1,
            &[pending_a, pending_b],
            &input,
            &solution,
            "OPTIMAL",
            true,
            5,
            5,
            1,
            &HashMap::new(),
            &HashSet::new(),
            &BTreeMap::new(),
            &BTreeMap::from([("research".to_string(), 1_000_000)]),
        );
        let denied = t
            .decisions
            .iter()
            .find(|d| d.namespace == "team-a" && d.name == "b")
            .expect("denied decision");

        let PodPlacement::Unplaced { reason } = &denied.placement else {
            panic!("expected team-a/b to be denied by budget");
        };
        assert!(reason.contains("budget exhausted"), "{reason}");
        assert!(
            denied.caveats.iter().any(|c| c.contains(
                "budget exhausted: 1000000 selected / 1000000 monthly milli-units for tenant research"
            )),
            "budget exhaustion should be visible in caveats: {:?}",
            denied.caveats
        );
    }

    #[test]
    fn deadline_metadata_propagates_to_trace() {
        let mut pod = ppod("team", "flex");
        pod.deadline_unix_seconds = 1_893_456_000;
        pod.min_gpus = 2;
        pod.max_gpus = 8;
        pod.preferred_gpus = 4;
        pod.flexible = true;
        pod.predicted_runtime_seconds = 7200;
        pod.predicted_peak_vram_bytes = 40 * 1024 * 1024 * 1024;
        let input = OptimizationInput {
            workloads: vec![OptimizationWorkload {
                id: "gang:team/job".into(),
                namespace: "team".into(),
                name: "flex".into(),
                group_size: 1,
                members: vec![member("team", "flex")],
                feasible_nodes: vec!["n1".into()],
                ..Default::default()
            }],
            ..Default::default()
        };
        let t = build_decision_trace(
            1,
            &[pod],
            &input,
            &OptimizationSolution::default(),
            "OPTIMAL",
            true,
            5,
            5,
            1,
            &HashMap::new(),
            &HashSet::new(),
        );
        let d = &t.decisions[0];
        assert_eq!(d.deadline_unix_seconds, 1_893_456_000);
        assert_eq!(d.min_gpus, 2);
        assert_eq!(d.max_gpus, 8);
        assert_eq!(d.preferred_gpus, 4);
        assert!(d.flexible);
        assert_eq!(d.predicted_runtime_seconds, 7200);
        assert_eq!(d.predicted_peak_vram_bytes, 40 * 1024 * 1024 * 1024);
        assert_ne!(d.deadline_slack_seconds, 0);
        assert!(d.predicted_finish_unix_seconds >= chrono::Utc::now().timestamp());
        assert!(!d.predicted_deadline_miss);
        assert_eq!(t.deadline_metrics.deadline_jobs, 1);
        assert_eq!(t.deadline_metrics.placed_deadline_jobs, 0);
        assert_eq!(t.deadline_metrics.unplaced_deadline_jobs, 1);
        assert_eq!(t.deadline_metrics.predicted_misses, 0);
        assert_eq!(
            t.deadline_metrics.worst_slack_seconds,
            d.deadline_slack_seconds
        );
    }

    #[test]
    fn predicted_deadline_miss_is_exposed_per_decision() {
        let mut pod = ppod("team", "late");
        pod.deadline_unix_seconds = chrono::Utc::now().timestamp() + 60;
        pod.predicted_runtime_seconds = 7200;
        let input = OptimizationInput {
            workloads: vec![OptimizationWorkload {
                id: "gang:team/job".into(),
                namespace: "team".into(),
                name: "late".into(),
                group_size: 1,
                members: vec![member("team", "late")],
                feasible_nodes: vec!["n1".into()],
                ..Default::default()
            }],
            ..Default::default()
        };

        let t = build_decision_trace(
            1,
            &[pod],
            &input,
            &OptimizationSolution::default(),
            "OPTIMAL",
            true,
            5,
            5,
            1,
            &HashMap::new(),
            &HashSet::new(),
        );

        let d = &t.decisions[0];
        assert!(d.deadline_slack_seconds < 0);
        assert!(d.predicted_deadline_miss);
        assert!(d.predicted_finish_unix_seconds > d.deadline_unix_seconds);
        assert_eq!(t.deadline_metrics.predicted_misses, 1);
        assert_eq!(t.deadline_metrics.placed_predicted_misses, 0);
        assert_eq!(t.deadline_metrics.unplaced_predicted_misses, 1);
    }

    #[test]
    fn deadline_metrics_split_predicted_misses_by_placement() {
        let now = chrono::Utc::now().timestamp();
        let mut placed = ppod("team", "placed-late");
        placed.deadline_unix_seconds = now + 60;
        placed.predicted_runtime_seconds = 7200;
        let mut unplaced = ppod("team", "unplaced-late");
        unplaced.deadline_unix_seconds = now + 60;
        unplaced.predicted_runtime_seconds = 7200;
        let input = OptimizationInput {
            workloads: vec![
                OptimizationWorkload {
                    id: "gang:team/placed".into(),
                    namespace: "team".into(),
                    name: "placed-late".into(),
                    group_size: 1,
                    members: vec![member("team", "placed-late")],
                    feasible_nodes: vec!["n1".into()],
                    ..Default::default()
                },
                OptimizationWorkload {
                    id: "gang:team/unplaced".into(),
                    namespace: "team".into(),
                    name: "unplaced-late".into(),
                    group_size: 1,
                    members: vec![member("team", "unplaced-late")],
                    feasible_nodes: vec!["n1".into()],
                    ..Default::default()
                },
            ],
            ..Default::default()
        };
        let solution = OptimizationSolution {
            assignment_counts: HashMap::from([(
                "gang:team/placed".to_string(),
                HashMap::from([("n1".to_string(), 1)]),
            )]),
            ..Default::default()
        };

        let t = build_decision_trace(
            1,
            &[placed, unplaced],
            &input,
            &solution,
            "OPTIMAL",
            true,
            5,
            5,
            1,
            &HashMap::new(),
            &HashSet::new(),
        );

        assert_eq!(t.deadline_metrics.predicted_misses, 2);
        assert_eq!(t.deadline_metrics.placed_predicted_misses, 1);
        assert_eq!(t.deadline_metrics.unplaced_predicted_misses, 1);
    }

    #[test]
    fn gang_not_admitted_marks_all_members_unplaced() {
        let gang = OptimizationWorkload {
            id: "gang:team/job".into(),
            namespace: "team".into(),
            name: "m0".into(),
            group_size: 2,
            members: vec![member("team", "m0"), member("team", "m1")],
            feasible_nodes: vec!["n1".into()],
            ..Default::default()
        };
        let input = OptimizationInput {
            workloads: vec![gang],
            ..Default::default()
        };
        // no assignment_counts entry -> not admitted
        let solution = OptimizationSolution::default();
        let pending = vec![ppod("team", "m0"), ppod("team", "m1")];
        let t = build_decision_trace(
            1,
            &pending,
            &input,
            &solution,
            "OPTIMAL",
            true,
            5,
            5,
            1,
            &HashMap::new(),
            &HashSet::new(),
        );
        assert!(t.decisions.iter().all(|d| matches!(
            &d.placement,
            PodPlacement::Unplaced { reason } if reason.contains("gang not admitted")
        )));
    }

    #[test]
    fn spread_gang_reports_per_member_nodes() {
        let gang = OptimizationWorkload {
            id: "gang:team/job".into(),
            namespace: "team".into(),
            name: "m0".into(),
            group_size: 3,
            members: vec![
                member("team", "m0"),
                member("team", "m1"),
                member("team", "m2"),
            ],
            feasible_nodes: vec!["n1".into(), "n2".into()],
            ..Default::default()
        };
        let input = OptimizationInput {
            workloads: vec![gang],
            ..Default::default()
        };
        let mut counts = HashMap::new();
        counts.insert("n1".to_string(), 2);
        counts.insert("n2".to_string(), 1);
        let mut assignment_counts = HashMap::new();
        assignment_counts.insert("gang:team/job".to_string(), counts);
        let solution = OptimizationSolution {
            assignment_counts,
            ..Default::default()
        };
        let pending = vec![ppod("team", "m0"), ppod("team", "m1"), ppod("team", "m2")];
        let t = build_decision_trace(
            1,
            &pending,
            &input,
            &solution,
            "OPTIMAL",
            true,
            5,
            5,
            1,
            &HashMap::new(),
            &HashSet::new(),
        );
        // sorted members m0,m1 -> n1 (count 2); m2 -> n2 (count 1)
        let by_name: HashMap<_, _> = t
            .decisions
            .iter()
            .map(|d| (d.name.clone(), d.placement.clone()))
            .collect();
        assert_eq!(by_name["m0"], PodPlacement::Placed { node: "n1".into() });
        assert_eq!(by_name["m1"], PodPlacement::Placed { node: "n1".into() });
        assert_eq!(by_name["m2"], PodPlacement::Placed { node: "n2".into() });
    }

    #[test]
    fn pod_absent_from_input_is_not_submitted() {
        let input = OptimizationInput::default();
        let solution = OptimizationSolution::default();
        let pending = vec![ppod("team", "ghost")];
        let t = build_decision_trace(
            1,
            &pending,
            &input,
            &solution,
            "OPTIMAL",
            true,
            5,
            5,
            1,
            &HashMap::new(),
            &HashSet::new(),
        );
        assert!(matches!(
            &t.decisions[0].placement,
            PodPlacement::Unplaced { reason } if reason.contains("not submitted")
        ));
    }

    #[test]
    fn caveats_propagate_to_placed_decision() {
        let gang = OptimizationWorkload {
            id: "gang:team/job".into(),
            namespace: "team".into(),
            name: "m0".into(),
            group_size: 1,
            members: vec![member("team", "m0")],
            feasible_nodes: vec!["n1".into()],
            ..Default::default()
        };
        let input = OptimizationInput {
            workloads: vec![gang],
            ..Default::default()
        };
        let mut counts = HashMap::new();
        counts.insert("n1".to_string(), 1);
        let mut assignment_counts = HashMap::new();
        assignment_counts.insert("gang:team/job".to_string(), counts);
        let solution = OptimizationSolution {
            assignment_counts,
            ..Default::default()
        };
        let mut p = ppod("team", "m0");
        p.unmodeled_constraints = vec!["pod anti-affinity".to_string()];
        let t = build_decision_trace(
            1,
            &[p],
            &input,
            &solution,
            "OPTIMAL",
            true,
            5,
            5,
            1,
            &HashMap::new(),
            &HashSet::new(),
        );
        assert!(matches!(
            &t.decisions[0].placement,
            PodPlacement::Placed { .. }
        ));
        assert_eq!(
            t.decisions[0].caveats,
            vec!["pod anti-affinity".to_string()]
        );
    }

    #[test]
    fn no_solution_reports_solver_reason_not_unschedulable() {
        // Submitted gang, solve_ok=false (empty solution) -> "no usable solution", NOT
        // "gang not admitted"/"no feasible placement".
        let gang = OptimizationWorkload {
            id: "gang:team/job".into(),
            namespace: "team".into(),
            name: "m0".into(),
            group_size: 2,
            members: vec![member("team", "m0"), member("team", "m1")],
            feasible_nodes: vec!["n1".into()],
            ..Default::default()
        };
        let input = OptimizationInput {
            workloads: vec![gang],
            ..Default::default()
        };
        let solution = OptimizationSolution::default();
        let pending = vec![ppod("team", "m0"), ppod("team", "m1")];
        let t = build_decision_trace(
            1,
            &pending,
            &input,
            &solution,
            "no-solution: x",
            false,
            5,
            5,
            1,
            &HashMap::new(),
            &HashSet::new(),
        );
        assert!(t.decisions.iter().all(|d| matches!(
            &d.placement,
            PodPlacement::Unplaced { reason } if reason.contains("no usable solution")
        )));
    }

    #[test]
    fn not_submitted_stays_not_submitted_even_when_solve_failed() {
        let input = OptimizationInput::default();
        let solution = OptimizationSolution::default();
        let pending = vec![ppod("team", "ghost")];
        let t = build_decision_trace(
            1,
            &pending,
            &input,
            &solution,
            "no-solution: x",
            false,
            5,
            5,
            1,
            &HashMap::new(),
            &HashSet::new(),
        );
        assert!(matches!(
            &t.decisions[0].placement,
            PodPlacement::Unplaced { reason } if reason.contains("not submitted")
        ));
    }

    #[test]
    fn drop_reason_is_surfaced_for_never_submitted_pod() {
        let input = OptimizationInput::default();
        let solution = OptimizationSolution::default();
        let pending = vec![ppod("team", "m0")];
        let mut drops = HashMap::new();
        drops.insert(
            "team/m0".to_string(),
            "no feasible node (insufficient residual capacity or excluded by anti-affinity)"
                .to_string(),
        );
        let t = build_decision_trace(
            1,
            &pending,
            &input,
            &solution,
            "OPTIMAL",
            true,
            5,
            5,
            1,
            &drops,
            &HashSet::new(),
        );
        assert!(matches!(
            &t.decisions[0].placement,
            PodPlacement::Unplaced { reason } if reason.contains("no feasible node")
        ));
    }

    #[test]
    fn is_time_sliced_node_detection() {
        use std::collections::BTreeMap;
        let l = |pairs: &[(&str, &str)]| -> BTreeMap<String, String> {
            pairs
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect()
        };
        assert!(super::is_time_sliced_node(&l(&[(
            "nvidia.com/gpu.sharing-strategy",
            "time-slicing"
        )])));
        // MPS (even with replicas) is NOT time-slicing.
        assert!(!super::is_time_sliced_node(&l(&[
            ("nvidia.com/gpu.sharing-strategy", "mps"),
            ("nvidia.com/gpu.replicas", "4"),
        ])));
        assert!(!super::is_time_sliced_node(&l(&[(
            "nvidia.com/gpu.sharing-strategy",
            "none"
        )])));
        // No strategy label -> replicas fallback.
        assert!(super::is_time_sliced_node(&l(&[(
            "nvidia.com/gpu.replicas",
            "4"
        )])));
        assert!(!super::is_time_sliced_node(&l(&[(
            "nvidia.com/gpu.replicas",
            "1"
        )])));
        assert!(!super::is_time_sliced_node(&l(&[(
            "nvidia.com/gpu.replicas",
            "x"
        )])));
        assert!(!super::is_time_sliced_node(&BTreeMap::new()));
    }

    #[test]
    fn placed_pod_on_time_sliced_node_gets_caveat() {
        let gang = OptimizationWorkload {
            id: "gang:team/job".into(),
            namespace: "team".into(),
            name: "m0".into(),
            group_size: 1,
            members: vec![member("team", "m0")],
            feasible_nodes: vec!["n1".into()],
            ..Default::default()
        };
        let input = OptimizationInput {
            workloads: vec![gang],
            ..Default::default()
        };
        let mut counts = HashMap::new();
        counts.insert("n1".to_string(), 1);
        let mut assignment_counts = HashMap::new();
        assignment_counts.insert("gang:team/job".to_string(), counts);
        let solution = OptimizationSolution {
            assignment_counts,
            ..Default::default()
        };
        let pending = vec![ppod("team", "m0")];
        let time_sliced: HashSet<String> = ["n1".to_string()].into_iter().collect();
        let t = build_decision_trace(
            1,
            &pending,
            &input,
            &solution,
            "OPTIMAL",
            true,
            5,
            5,
            1,
            &HashMap::new(),
            &time_sliced,
        );
        assert!(t.decisions[0]
            .caveats
            .iter()
            .any(|c| c.contains("time-sliced GPU")));

        // Same pod on a NON-time-sliced node: no caveat.
        let t2 = build_decision_trace(
            1,
            &pending,
            &input,
            &solution,
            "OPTIMAL",
            true,
            5,
            5,
            1,
            &HashMap::new(),
            &HashSet::new(),
        );
        assert!(!t2.decisions[0]
            .caveats
            .iter()
            .any(|c| c.contains("time-sliced GPU")));
    }
}
