#[cfg(feature = "rust-cp-sat")]
use crate::model::{
    deadline_adjusted_flexible_replica_bounds, is_gpu_resource_name,
    optimization_workload_gpu_request, ObjectiveProfile, OptimizationWorkload,
};
use crate::model::{OptimizationInput, OptimizationSolution, ScenarioConfig, SolverInfo};

#[cfg(feature = "rust-cp-sat")]
mod enabled {
    use super::{
        deadline_adjusted_flexible_replica_bounds, is_gpu_resource_name,
        optimization_workload_gpu_request, ObjectiveProfile, OptimizationInput,
        OptimizationSolution, OptimizationWorkload, ScenarioConfig, SolverInfo,
    };
    use anyhow::{bail, Result};
    use chrono::Utc;
    use cp_sat::builder::{BoolVar, CpModelBuilder, IntVar, LinearExpr};
    use cp_sat::ffi::cp_solver_response_stats;
    use cp_sat::proto::{CpSolverResponse, CpSolverStatus, SatParameters};
    use std::collections::{HashMap, HashSet};

    /// A variable appearing in the objective, so its exact integer solution value can be read back
    /// (the reported f64 objective_value is unreliable once the admission weight exceeds 2^53).
    #[derive(Clone, Copy)]
    enum ObjVar {
        Int(IntVar),
        Bool(BoolVar),
    }
    impl ObjVar {
        fn value(&self, response: &CpSolverResponse) -> i64 {
            match self {
                ObjVar::Int(v) => v.solution_value(response),
                ObjVar::Bool(b) => b.solution_value(response) as i64,
            }
        }
    }
    /// Build a `LinearExpr` from `(coeff, var)` terms (used for both the objective and, in the
    /// soft-affinity second phase, the cost-preserving `objective ≤ optimum` constraint).
    fn expr_from_terms(terms: &[(i64, ObjVar)]) -> LinearExpr {
        let mut e = LinearExpr::default();
        for (c, v) in terms {
            match v {
                ObjVar::Int(iv) => e += (*c, *iv),
                ObjVar::Bool(bv) => e += (*c, *bv),
            }
        }
        e
    }

    fn deadline_urgency_scores(
        input: &OptimizationInput,
        scenario: &ScenarioConfig,
    ) -> HashMap<String, i64> {
        let weight = scenario.objective_weights.deadline_urgency.max(0);
        if scenario.objective_profile != ObjectiveProfile::GpuGangAware || weight == 0 {
            return HashMap::new();
        }

        let latest_starts: Vec<(&str, i64)> = input
            .workloads
            .iter()
            .filter(|w| w.deadline_unix_seconds > 0)
            .map(|w| {
                (
                    w.id.as_str(),
                    w.deadline_unix_seconds
                        .saturating_sub(w.predicted_runtime_seconds.max(0)),
                )
            })
            .collect();
        let Some(max_latest_start) = latest_starts.iter().map(|(_, t)| *t).max() else {
            return HashMap::new();
        };

        latest_starts
            .into_iter()
            .map(|(id, latest_start)| {
                // One base point for having an explicit deadline, then one extra point per minute
                // earlier than the least-urgent deadline in this batch, capped at a week.
                let urgency_minutes = max_latest_start
                    .saturating_sub(latest_start)
                    .saturating_div(60)
                    .clamp(0, 7 * 24 * 60);
                (
                    id.to_string(),
                    weight.saturating_mul(1_i64.saturating_add(urgency_minutes)),
                )
            })
            .collect()
    }

    fn admission_score(
        workload: &OptimizationWorkload,
        scenario: &ScenarioConfig,
        deadline_urgency_score: i64,
        now_unix_seconds: i64,
    ) -> i64 {
        match scenario.objective_profile {
            ObjectiveProfile::CostBinpack => 1,
            ObjectiveProfile::GpuGangAware => {
                let w = &scenario.objective_weights;
                let base = w.admission.max(0);
                let gpu =
                    optimization_workload_gpu_request(workload).saturating_mul(w.gpu_demand.max(0));
                let gang_replicas = i64::from((workload.group_size - 1).max(0));
                let gang = gang_replicas.saturating_mul(w.gang_complete.max(0));
                let priority = workload.priority.max(0).saturating_mul(w.priority.max(0));
                let business_value = workload
                    .business_value
                    .max(0)
                    .saturating_mul(w.business_value.max(0));
                let queue = workload.queue_score.max(0).saturating_mul(w.queue.max(0));
                let queue_wait_minutes = workload
                    .queue_wait_seconds
                    .max(0)
                    .saturating_div(60)
                    .clamp(0, 7 * 24 * 60);
                let queue_wait = queue_wait_minutes.saturating_mul(w.queue_wait.max(0));
                let fair_share = workload
                    .fair_share_deficit
                    .max(0)
                    .saturating_mul(w.fair_share.max(0));
                let predicted_deadline_miss = workload.deadline_unix_seconds > 0
                    && workload.predicted_runtime_seconds > 0
                    && now_unix_seconds.saturating_add(workload.predicted_runtime_seconds)
                        > workload.deadline_unix_seconds;
                let deadline_miss = if predicted_deadline_miss {
                    w.deadline_miss.max(0)
                } else {
                    0
                };
                base.saturating_add(gpu)
                    .saturating_add(gang)
                    .saturating_add(priority)
                    .saturating_add(business_value)
                    .saturating_add(queue)
                    .saturating_add(queue_wait)
                    .saturating_add(fair_share)
                    .saturating_add(deadline_urgency_score.max(0))
                    .saturating_sub(deadline_miss)
                    .max(1)
            }
        }
    }

    pub fn solver_info() -> SolverInfo {
        SolverInfo {
            name: "cp-sat-rust".to_string(),
            available: true,
            status: "available via cp_sat crate".to_string(),
        }
    }

    pub fn solve(
        input: &OptimizationInput,
        scenario: &ScenarioConfig,
    ) -> Result<(OptimizationSolution, SolverInfo)> {
        if scenario.partial_admission && scenario.enable_joint_rightsizing {
            bail!("partial_admission is incompatible with enable_joint_rightsizing");
        }
        let mut model = CpModelBuilder::default();
        let mut y_vars = HashMap::new();
        let mut x_vars = HashMap::new();
        let mut cpu_slack_vars: HashMap<String, IntVar> = HashMap::new();
        let mut mem_slack_vars: HashMap<String, IntVar> = HashMap::new();
        let mut scalar_slack_vars: HashMap<(String, String), IntVar> = HashMap::new();
        let now_unix_seconds = Utc::now().timestamp();

        for node in &input.nodes {
            let var = model.new_int_var_with_name(
                [(0, i64::from(node.count))],
                format!("y_{}", sanitize(&node.name)),
            );
            y_vars.insert(node.name.clone(), var);
        }

        for workload in &input.workloads {
            if workload.feasible_nodes.is_empty() {
                bail!("workload {} has no feasible nodes", workload.id);
            }
            let upper = i64::from(workload.group_size).max(0);
            for node_name in &workload.feasible_nodes {
                let var = model.new_int_var_with_name(
                    [(0, upper)],
                    format!("x_{}__{}", sanitize(&workload.id), sanitize(node_name)),
                );
                x_vars.insert((workload.id.clone(), node_name.clone()), var);
            }
        }

        // Level selection variables for request quantization
        // level_vars[workload_id] = vec of (level_key, BoolVar) — exactly one selected
        // x_level_vars[(workload_id, level_key, node_name)] = IntVar — replicas at this level on this node
        let mut level_vars: HashMap<String, Vec<(String, BoolVar)>> = HashMap::new();
        let mut x_level_vars: HashMap<(String, String, String), IntVar> = HashMap::new();

        if scenario.enable_joint_rightsizing {
            for workload in &input.workloads {
                if workload.candidate_levels.len() < 2 {
                    continue;
                }
                let upper = i64::from(workload.group_size);
                let mut lvars = Vec::new();
                for level in &workload.candidate_levels {
                    let bv = model.new_bool_var_with_name(format!(
                        "level_{}__{}",
                        sanitize(&workload.id),
                        sanitize(&level.key)
                    ));
                    lvars.push((level.key.clone(), bv));

                    for node_name in &workload.feasible_nodes {
                        let xv = model.new_int_var_with_name(
                            [(0, upper)],
                            format!(
                                "xl_{}__{}_{}",
                                sanitize(&workload.id),
                                sanitize(&level.key),
                                sanitize(node_name)
                            ),
                        );
                        x_level_vars.insert(
                            (workload.id.clone(), level.key.clone(), node_name.clone()),
                            xv,
                        );
                        model.add_le(xv, (upper, bv));
                    }
                }
                let bool_vars: Vec<BoolVar> = lvars.iter().map(|(_, bv)| *bv).collect();
                model.add_exactly_one(bool_vars);
                model.add_hint(lvars[0].1, 1_i64);
                for (_, bv) in &lvars[1..] {
                    model.add_hint(*bv, 0_i64);
                }
                // Link: sum of x_level across levels = x for each node
                for node_name in &workload.feasible_nodes {
                    let x = x_vars[&(workload.id.clone(), node_name.clone())];
                    let level_sum: LinearExpr = workload
                        .candidate_levels
                        .iter()
                        .map(|l| {
                            x_level_vars[&(workload.id.clone(), l.key.clone(), node_name.clone())]
                        })
                        .collect();
                    model.add_eq(level_sum, x);
                }
                level_vars.insert(workload.id.clone(), lvars);
            }
        }

        let mut placed_vars: HashMap<String, BoolVar> = HashMap::new();
        for workload in &input.workloads {
            let group_size = i64::from(workload.group_size);
            let sum_expr: LinearExpr = workload
                .feasible_nodes
                .iter()
                .map(|node_name| x_vars[&(workload.id.clone(), node_name.clone())])
                .collect();
            if scenario.partial_admission && group_size > 0 {
                // All-or-nothing admission: sum of replicas == group_size * placed.
                let placed =
                    model.new_bool_var_with_name(format!("placed_{}", sanitize(&workload.id)));
                if scenario.objective_profile == ObjectiveProfile::GpuGangAware {
                    if let Some((min_replicas, max_replicas)) =
                        deadline_adjusted_flexible_replica_bounds(workload, now_unix_seconds)
                    {
                        model.add_ge(sum_expr.clone(), (min_replicas, placed));
                        model.add_le(sum_expr, (max_replicas, placed));
                    } else {
                        model.add_eq(sum_expr, (group_size, placed));
                    }
                } else {
                    model.add_eq(sum_expr, (group_size, placed));
                }
                placed_vars.insert(workload.id.clone(), placed);
            } else {
                model.add_eq(sum_expr, group_size);
            }

            for node_name in &workload.feasible_nodes {
                let x = x_vars[&(workload.id.clone(), node_name.clone())];
                let y = y_vars[node_name];
                model.add_le(x, (group_size, y));
            }

            if workload.colocate && group_size > 0 {
                // Single-node co-location: at most one node may hold this gang's
                // replicas, so combined with the latch (sum x = group_size * placed)
                // an admitted gang lands entirely on one node.
                let mut used_sum = LinearExpr::default();
                for node_name in &workload.feasible_nodes {
                    let x = x_vars[&(workload.id.clone(), node_name.clone())];
                    let used = model.new_bool_var_with_name(format!(
                        "used_{}__{}",
                        sanitize(&workload.id),
                        sanitize(node_name)
                    ));
                    // x > 0  =>  used = 1
                    model.add_le(x, (group_size, used));
                    used_sum += used;
                }
                if let Some(placed) = placed_vars.get(&workload.id) {
                    model.add_le(used_sum, *placed);
                } else {
                    model.add_le(used_sum, 1_i64);
                }
            }
        }

        if !scenario.relax_required_anti_affinity {
            // Self-pairs (a == a): spread this workload's own replicas <=1 per node.
            let self_ids: HashSet<&str> = input
                .anti_affinity_pairs
                .iter()
                .filter(|(a, b)| a == b)
                .map(|(a, _)| a.as_str())
                .collect();
            for workload in &input.workloads {
                if workload.group_size <= 1 {
                    continue;
                }
                let has_anti = self_ids.contains(workload.id.as_str())
                    || workload.members.iter().any(|m| {
                        let key = format!("{}/{}", m.namespace, m.name);
                        self_ids.contains(key.as_str())
                    });
                if !has_anti {
                    continue;
                }
                for node_name in &workload.feasible_nodes {
                    let x = x_vars[&(workload.id.clone(), node_name.clone())];
                    model.add_le(x, 1_i64);
                }
            }

            // Cross-pairs (a != b): at most one of workloads {a,b} may be PRESENT on a node.
            // Presence bool per (workload,node): x_w[n] <= group_size_w * present_w_n, then
            // present_a + present_b <= 1. (Using counts here would wrongly forbid gangs.)
            let meta: HashMap<&str, (&Vec<String>, i64)> = input
                .workloads
                .iter()
                .map(|w| (w.id.as_str(), (&w.feasible_nodes, i64::from(w.group_size))))
                .collect();
            let mut presence: HashMap<(String, String), BoolVar> = HashMap::new();
            for (a, b) in &input.anti_affinity_pairs {
                if a == b {
                    continue;
                }
                let (Some((fa, ga)), Some((fb, gb))) = (meta.get(a.as_str()), meta.get(b.as_str()))
                else {
                    continue;
                };
                let bset: HashSet<&str> = fb.iter().map(|s| s.as_str()).collect();
                for node_name in fa.iter().filter(|n| bset.contains(n.as_str())) {
                    let ga = *ga;
                    let gb = *gb;
                    let key_a = (a.clone(), node_name.clone());
                    let pa = match presence.get(&key_a) {
                        Some(p) => *p,
                        None => {
                            let p = model.new_bool_var_with_name(format!(
                                "present_{}__{}",
                                sanitize(a),
                                sanitize(node_name)
                            ));
                            model.add_le(x_vars[&key_a], (ga, p));
                            presence.insert(key_a, p);
                            p
                        }
                    };
                    let key_b = (b.clone(), node_name.clone());
                    let pb = match presence.get(&key_b) {
                        Some(p) => *p,
                        None => {
                            let p = model.new_bool_var_with_name(format!(
                                "present_{}__{}",
                                sanitize(b),
                                sanitize(node_name)
                            ));
                            model.add_le(x_vars[&key_b], (gb, p));
                            presence.insert(key_b, p);
                            p
                        }
                    };
                    let expr: LinearExpr = [pa, pb].into_iter().collect();
                    model.add_le(expr, 1_i64);
                }
            }
        }

        let mut hinted_assignments = 0_i64;
        let mut hinted_nodes = HashSet::new();
        for workload in &input.workloads {
            for node_name in &workload.feasible_nodes {
                let count = i64::from(*workload.current_counts.get(node_name).unwrap_or(&0));
                let x = x_vars[&(workload.id.clone(), node_name.clone())];
                model.add_hint(x, count);
                if count > 0 {
                    hinted_assignments += count;
                    hinted_nodes.insert(node_name.clone());
                }
            }
        }

        for node in &input.nodes {
            let total: i64 = input
                .workloads
                .iter()
                .map(|w| i64::from(*w.current_counts.get(&node.name).unwrap_or(&0)))
                .sum();
            let pod_cap = node.effective_capacity.pods;
            let mut active_hint = if hinted_nodes.contains(&node.name) {
                1
            } else {
                0
            };
            if pod_cap > 0 && total > 0 {
                active_hint = std::cmp::min(
                    i64::from(node.count),
                    std::cmp::max(1, ceil_div(total, pod_cap)),
                );
            }
            model.add_hint(y_vars[&node.name], active_hint);
        }

        for node in &input.nodes {
            let y = y_vars[&node.name];
            let mut cpu = LinearExpr::default();
            let mut mem = LinearExpr::default();
            let mut disk = LinearExpr::default();
            let mut pods = LinearExpr::default();

            for workload in &input.workloads {
                if !workload.feasible_nodes.iter().any(|n| n == &node.name) {
                    continue;
                }
                let x = x_vars[&(workload.id.clone(), node.name.clone())];
                pods += x;

                if level_vars.contains_key(&workload.id) {
                    let gs = i64::from(workload.group_size).max(1);
                    for level in &workload.candidate_levels {
                        let xl = x_level_vars
                            [&(workload.id.clone(), level.key.clone(), node.name.clone())];
                        let cpu_per = level.requests.milli_cpu / gs;
                        let mem_per = level.requests.memory_bytes / gs;
                        let disk_per = level.requests.ephemeral_storage / gs;
                        if cpu_per != 0 {
                            cpu += (cpu_per, xl);
                        }
                        if mem_per != 0 {
                            mem += (mem_per, xl);
                        }
                        if disk_per != 0 {
                            disk += (disk_per, xl);
                        }
                    }
                } else {
                    let req = per_replica_requests(workload);
                    if req.milli_cpu != 0 {
                        cpu += (req.milli_cpu, x);
                    }
                    if req.memory_bytes != 0 {
                        mem += (req.memory_bytes, x);
                    }
                    if req.ephemeral_storage != 0 {
                        disk += (req.ephemeral_storage, x);
                    }
                }
            }

            if node.effective_capacity.milli_cpu > 0 {
                model.add_le(cpu.clone(), (node.effective_capacity.milli_cpu, y));
                let slack = model.new_int_var_with_name(
                    [(0, node.effective_capacity.milli_cpu * i64::from(node.count))],
                    format!("cpu_slack_{}", sanitize(&node.name)),
                );
                model.add_eq(cpu + slack, (node.effective_capacity.milli_cpu, y));
                cpu_slack_vars.insert(node.name.clone(), slack);
            }
            if node.effective_capacity.memory_bytes > 0 {
                model.add_le(mem.clone(), (node.effective_capacity.memory_bytes, y));
                let slack = model.new_int_var_with_name(
                    [(
                        0,
                        node.effective_capacity.memory_bytes * i64::from(node.count),
                    )],
                    format!("mem_slack_{}", sanitize(&node.name)),
                );
                model.add_eq(mem + slack, (node.effective_capacity.memory_bytes, y));
                mem_slack_vars.insert(node.name.clone(), slack);
            }
            if node.effective_capacity.ephemeral_storage > 0 {
                model.add_le(disk, (node.effective_capacity.ephemeral_storage, y));
            }
            if node.effective_capacity.pods > 0 {
                model.add_le(pods, (node.effective_capacity.pods, y));
            }
            for (resource_name, capacity) in &node.extended_resources {
                if *capacity <= 0 {
                    continue;
                }
                let mut scalar = LinearExpr::default();
                for workload in &input.workloads {
                    if !workload.feasible_nodes.iter().any(|n| n == &node.name) {
                        continue;
                    }
                    let per_replica_scalar = per_replica_scalar_requests(workload);
                    let Some(req) = per_replica_scalar.get(resource_name) else {
                        continue;
                    };
                    if *req == 0 {
                        continue;
                    }
                    let x = x_vars[&(workload.id.clone(), node.name.clone())];
                    scalar += (*req, x);
                }
                model.add_le(scalar.clone(), (*capacity, y));
                let slack = model.new_int_var_with_name(
                    [(0, *capacity * i64::from(node.count))],
                    format!(
                        "scalar_slack_{}_{}",
                        sanitize(&node.name),
                        sanitize(resource_name)
                    ),
                );
                model.add_eq(scalar + slack, (*capacity, y));
                scalar_slack_vars.insert((node.name.clone(), resource_name.clone()), slack);
            }
        }

        // Quota groups: cap the total resource consumed by admitted workloads in each group
        // (e.g. a per-namespace GPU quota). Charge actual selected replicas so flexible partial
        // admission does not consume quota for replicas intentionally left deferred.
        if !input.quota_groups.is_empty() {
            let by_id: HashMap<&str, &crate::model::OptimizationWorkload> =
                input.workloads.iter().map(|w| (w.id.as_str(), w)).collect();
            for group in &input.quota_groups {
                if group.limit < 0 || group.resources.is_empty() {
                    continue;
                }
                let mut expr = LinearExpr::default();
                for wid in &group.workload_ids {
                    let Some(w) = by_id.get(wid.as_str()) else {
                        continue;
                    };
                    let per_replica = per_replica_scalar_requests(w);
                    let unit: i64 = group
                        .resources
                        .iter()
                        .map(|r| per_replica.get(r).copied().unwrap_or(0))
                        .sum();
                    if unit > 0 {
                        for node_name in &w.feasible_nodes {
                            if let Some(x) = x_vars.get(&(w.id.clone(), node_name.clone())) {
                                expr += (unit, *x);
                            }
                        }
                    }
                }
                model.add_le(expr, group.limit);
            }
        }

        // Budget groups: cap admitted monthly placement cost for selected workload groups.
        // Costs are per selected replica and node-dependent, so charge each assignment edge.
        if !input.budget_groups.is_empty() {
            let by_id: HashMap<&str, &crate::model::OptimizationWorkload> =
                input.workloads.iter().map(|w| (w.id.as_str(), w)).collect();
            let node_by_name: HashMap<&str, &crate::model::OptimizationNode> =
                input.nodes.iter().map(|n| (n.name.as_str(), n)).collect();
            for group in &input.budget_groups {
                if group.limit_milli < 0 {
                    continue;
                }
                let mut expr = LinearExpr::default();
                for wid in &group.workload_ids {
                    let Some(w) = by_id.get(wid.as_str()) else {
                        continue;
                    };
                    let per_replica_gpu = optimization_workload_gpu_request(w)
                        .checked_div(i64::from(w.group_size).max(1))
                        .unwrap_or(0)
                        .max(0);
                    if per_replica_gpu <= 0 {
                        continue;
                    }
                    for node_name in &w.feasible_nodes {
                        let Some(node) = node_by_name.get(node_name.as_str()) else {
                            continue;
                        };
                        let coeff = placement_cost_milli_per_replica(node, per_replica_gpu);
                        if coeff <= 0 {
                            continue;
                        }
                        if let Some(x) = x_vars.get(&(w.id.clone(), node_name.clone())) {
                            expr += (coeff, *x);
                        }
                    }
                }
                model.add_le(expr, group.limit_milli);
            }
        }

        // Collect the objective as `(coeff, var)` terms so its exact integer value can be read back
        // for the soft-affinity second phase (see ObjVar). The LinearExpr is derived from these.
        let mut obj_terms: Vec<(i64, ObjVar)> = Vec::new();
        for node in &input.nodes {
            let coeff = (node.price.monthly * scenario.cost_weight as f64).round() as i64;
            if coeff != 0 {
                obj_terms.push((coeff, ObjVar::Int(y_vars[&node.name])));
            }
            obj_terms.push((scenario.active_node_weight, ObjVar::Int(y_vars[&node.name])));
            if let Some(mem_slack) = mem_slack_vars.get(&node.name) {
                obj_terms.push((scenario.memory_slack_weight, ObjVar::Int(*mem_slack)));
            }
            if let Some(cpu_slack) = cpu_slack_vars.get(&node.name) {
                obj_terms.push((scenario.cpu_slack_weight, ObjVar::Int(*cpu_slack)));
            }
            for resource_name in node.extended_resources.keys() {
                if let Some(slack) =
                    scalar_slack_vars.get(&(node.name.clone(), resource_name.clone()))
                {
                    let gpu_fragmentation_weight = if scenario.objective_profile
                        == ObjectiveProfile::GpuGangAware
                        && is_gpu_resource_name(resource_name)
                    {
                        scenario.objective_weights.gpu_fragmentation.max(0)
                    } else {
                        0
                    };
                    obj_terms.push((
                        scenario
                            .memory_slack_weight
                            .saturating_add(gpu_fragmentation_weight),
                        ObjVar::Int(*slack),
                    ));
                }
            }
        }
        for workload in &input.workloads {
            for node_name in &workload.feasible_nodes {
                let current_count =
                    i64::from(*workload.current_counts.get(node_name).unwrap_or(&0));
                if current_count > 0 {
                    obj_terms.push((
                        -scenario.churn_weight,
                        ObjVar::Int(x_vars[&(workload.id.clone(), node_name.clone())]),
                    ));
                }
                if scenario.objective_profile == ObjectiveProfile::GpuGangAware
                    && deadline_adjusted_flexible_replica_bounds(workload, now_unix_seconds)
                        .is_some()
                {
                    // After admission is decided, prefer the lower flexible replica count. This
                    // must dominate the ordinary GPU slack penalty, which otherwise rewards filling
                    // idle GPUs on already-active nodes.
                    let per_replica_gpu = optimization_workload_gpu_request(workload)
                        .checked_div(i64::from(workload.group_size).max(1))
                        .unwrap_or(0)
                        .max(1);
                    let coeff = scenario
                        .memory_slack_weight
                        .saturating_add(scenario.objective_weights.gpu_fragmentation.max(0))
                        .saturating_add(1)
                        .saturating_mul(per_replica_gpu);
                    obj_terms.push((
                        coeff,
                        ObjVar::Int(x_vars[&(workload.id.clone(), node_name.clone())]),
                    ));
                }
            }
        }
        for (workload_id, lvars) in &level_vars {
            for (level_key, bv) in lvars {
                let workload = input.workloads.iter().find(|w| &w.id == workload_id);
                let risk = workload
                    .and_then(|w| w.candidate_levels.iter().find(|l| &l.key == level_key))
                    .map(|l| l.risk_score)
                    .unwrap_or(0);
                if risk > 0 {
                    obj_terms.push((scenario.rightsizing_weight * risk / 100, ObjVar::Bool(*bv)));
                }
            }
        }
        let deadline_urgency_scores = deadline_urgency_scores(input, scenario);
        let effective_admission_weight = if scenario.partial_admission && !placed_vars.is_empty() {
            // Conservative upper bound (i128) of the max magnitude of ALL non-admission
            // objective terms. Node terms use int vars y in [0, node.count], so every
            // per-node term scales by node.count; slack <= capacity * count. Use the
            // absolute value of each weight (weights are not validated nonnegative).
            let mut rest_bound: i128 = 0;
            for node in &input.nodes {
                let count = i128::from(node.count.max(0));
                let cost_coeff =
                    ((node.price.monthly * scenario.cost_weight as f64).round() as i128).abs();
                rest_bound = rest_bound.saturating_add(cost_coeff.saturating_mul(count));
                rest_bound = rest_bound.saturating_add(
                    (scenario.active_node_weight as i128)
                        .abs()
                        .saturating_mul(count),
                );
                rest_bound = rest_bound.saturating_add(
                    (scenario.memory_slack_weight as i128)
                        .abs()
                        .saturating_mul((node.effective_capacity.memory_bytes as i128).max(0))
                        .saturating_mul(count),
                );
                rest_bound = rest_bound.saturating_add(
                    (scenario.cpu_slack_weight as i128)
                        .abs()
                        .saturating_mul((node.effective_capacity.milli_cpu as i128).max(0))
                        .saturating_mul(count),
                );
                for cap in node.extended_resources.values() {
                    rest_bound = rest_bound.saturating_add(
                        (scenario.memory_slack_weight as i128)
                            .abs()
                            .saturating_mul((*cap).max(0) as i128)
                            .saturating_mul(count),
                    );
                }
            }
            // Churn reward: -churn_weight * x on edges with current_count>0; sum of x over
            // nodes <= group_size, so per workload the magnitude is <= churn_weight*group_size.
            for workload in &input.workloads {
                let gs = i128::from(workload.group_size.max(0));
                rest_bound = rest_bound
                    .saturating_add((scenario.churn_weight as i128).abs().saturating_mul(gs));
                if scenario.objective_profile == ObjectiveProfile::GpuGangAware
                    && deadline_adjusted_flexible_replica_bounds(workload, now_unix_seconds)
                        .is_some()
                {
                    let per_replica_gpu = optimization_workload_gpu_request(workload)
                        .checked_div(i64::from(workload.group_size).max(1))
                        .unwrap_or(0)
                        .max(1);
                    let coeff = scenario
                        .memory_slack_weight
                        .saturating_add(scenario.objective_weights.gpu_fragmentation.max(0))
                        .saturating_add(1)
                        .saturating_mul(per_replica_gpu);
                    rest_bound =
                        rest_bound.saturating_add((coeff as i128).abs().saturating_mul(gs));
                }
            }
            let w: i128 = if scenario.admission_weight > 0 {
                let explicit = scenario.admission_weight as i128;
                if explicit <= rest_bound {
                    bail!("admission_weight {explicit} does not dominate objective bound {rest_bound}; use 0 for auto or a larger value");
                }
                explicit
            } else {
                // rightsizing terms cannot coexist (guarded above), so rest_bound covers
                // the full objective. saturating_add guards the i128::MAX saturated case.
                rest_bound.saturating_add(1)
            };
            let admission_score_sum = input
                .workloads
                .iter()
                .filter(|w| placed_vars.contains_key(&w.id))
                .try_fold(0_i128, |acc, w| {
                    let deadline_score = *deadline_urgency_scores.get(&w.id).unwrap_or(&0);
                    acc.checked_add(i128::from(admission_score(
                        w,
                        scenario,
                        deadline_score,
                        now_unix_seconds,
                    )))
                })
                .unwrap_or(i128::MAX);
            let total = w
                .checked_mul(admission_score_sum)
                .and_then(|v| v.checked_add(rest_bound))
                .unwrap_or(i128::MAX);
            if w > i64::MAX as i128 || total > i64::MAX as i128 {
                bail!("partial_admission weight would overflow i64 objective (admission_score_sum={admission_score_sum}); reduce scope or set a smaller admission_weight");
            }
            w as i64
        } else {
            0
        };
        for (workload_id, placed) in &placed_vars {
            // Reward admitting a workload; weight dominates the rest of the objective so
            // the solver maximizes the configured admission score first, then minimizes cost.
            let score = input
                .workloads
                .iter()
                .find(|w| &w.id == workload_id)
                .map(|w| {
                    let deadline_score = *deadline_urgency_scores.get(&w.id).unwrap_or(&0);
                    admission_score(w, scenario, deadline_score, now_unix_seconds)
                })
                .unwrap_or(1);
            obj_terms.push((
                -effective_admission_weight.saturating_mul(score),
                ObjVar::Bool(*placed),
            ));
        }

        model.minimize(expr_from_terms(&obj_terms));

        let validation = model.validate_cp_model();
        if !validation.is_empty() {
            bail!("cp-sat rust model validation failed: {validation}");
        }

        let worker_count = recommended_worker_count(input);
        let time_limit = if scenario.solve_time_limit_secs > 0 {
            scenario.solve_time_limit_secs as f64
        } else {
            600.0
        };
        let params = SatParameters {
            max_time_in_seconds: Some(time_limit),
            num_search_workers: Some(worker_count),
            ..SatParameters::default()
        };
        let response = model.solve_with_parameters(&params);
        let status = response.status();
        let stats = cp_solver_response_stats(&response, true);

        if status != CpSolverStatus::Optimal && status != CpSolverStatus::Feasible {
            bail!("cp-sat rust solve failed with status {status:?}; {stats}");
        }

        // Soft-affinity tie-break (Phase 2): only when enabled, Phase 1 is proven OPTIMAL, and some
        // workload has preferred-node scores. Preserve the cost optimum and the admitted set, then
        // maximize soft score. Cannot change admission/cost (guarded by the constraint + fixed
        // placed vars, and by the admission-and-cost invariant test).
        let want_soft = scenario.enable_soft_affinity
            && status == CpSolverStatus::Optimal
            && (input.workloads.iter().any(|w| !w.soft_scores.is_empty())
                || !input.soft_coplacement_pairs.is_empty());
        let response = if want_soft {
            // Exact integer Phase-1 objective value (do NOT trust the reported f64; the admission
            // weight can exceed 2^53).
            let mut acc: i128 = 0;
            for (c, v) in &obj_terms {
                acc += *c as i128 * v.value(&response) as i128;
            }
            if acc < i64::MIN as i128 || acc > i64::MAX as i128 {
                // Objective magnitude out of range for the constraint; skip the soft pass safely.
                response
            } else {
                let phase1_obj = acc as i64;
                // Pin cost at its optimum and the admitted set at Phase-1's choice.
                model.add_le(expr_from_terms(&obj_terms), phase1_obj);
                for placed in placed_vars.values() {
                    model.add_eq(*placed, placed.solution_value(&response) as i64);
                }
                // Maximize soft score = minimize its negation.
                let mut soft = LinearExpr::default();
                for workload in &input.workloads {
                    for (node_name, score) in &workload.soft_scores {
                        if *score != 0 {
                            if let Some(x) = x_vars.get(&(workload.id.clone(), node_name.clone())) {
                                soft += (-*score, *x);
                            }
                        }
                    }
                }
                // Co-placement rewards (Phase 2 only): reward `both` when a and b share a domain.
                // Upper bounds only — maximization (minimize -weight) sets `both`=1 iff a and b BOTH
                // place in the domain. Admission/cost already pinned above, so this only reorders
                // cost-equal, admission-equal placements. weight>0 required (a negative weight would
                // flip the upper-bound reward into an unenforced penalty).
                for (ci, cp) in input.soft_coplacement_pairs.iter().enumerate() {
                    if cp.weight <= 0 {
                        continue;
                    }
                    for (di, dom) in cp.domains.iter().enumerate() {
                        let mut sum_a = LinearExpr::default();
                        for n in &dom.a_nodes {
                            if let Some(x) = x_vars.get(&(cp.a.clone(), n.clone())) {
                                sum_a += (1_i64, *x);
                            }
                        }
                        let mut sum_b = LinearExpr::default();
                        for n in &dom.b_nodes {
                            if let Some(x) = x_vars.get(&(cp.b.clone(), n.clone())) {
                                sum_b += (1_i64, *x);
                            }
                        }
                        let both = model.new_bool_var_with_name(format!("coplace_{ci}_{di}"));
                        let mut both_e = LinearExpr::default();
                        both_e += (1_i64, both);
                        model.add_le(both_e.clone(), sum_a); // both <= Σ x_a in domain
                        model.add_le(both_e, sum_b); // both <= Σ x_b in domain
                        soft += (-cp.weight, both); // minimize -weight*both == maximize reward
                    }
                }
                model.minimize(soft);
                let r2 = model.solve_with_parameters(&params);
                // The Phase-1 assignment is a feasible witness, so Phase 2 should succeed; if it
                // somehow doesn't, fall back to the (valid) Phase-1 response.
                if r2.status() == CpSolverStatus::Optimal || r2.status() == CpSolverStatus::Feasible
                {
                    r2
                } else {
                    response
                }
            }
        } else {
            response
        };

        let mut solution = OptimizationSolution::default();
        for workload in &input.workloads {
            let mut best_name = String::new();
            let mut best_value = -1_i64;
            let mut counts = HashMap::new();
            for node_name in &workload.feasible_nodes {
                let value =
                    x_vars[&(workload.id.clone(), node_name.clone())].solution_value(&response);
                if value > 0 {
                    counts.insert(node_name.clone(), value as i32);
                }
                if value > best_value {
                    best_value = value;
                    best_name = node_name.clone();
                }
            }
            if !counts.is_empty() {
                solution
                    .assignment_counts
                    .insert(workload.id.clone(), counts);
            }
            if best_value > 0 {
                solution.assignments.insert(workload.id.clone(), best_name);
            }
        }
        for node in &input.nodes {
            solution.active_nodes.insert(
                node.name.clone(),
                y_vars[&node.name].solution_value(&response) as i32,
            );
        }
        for (workload_id, lvars) in &level_vars {
            for (level_key, bv) in lvars {
                if bv.solution_value(&response) {
                    solution
                        .selected_levels
                        .insert(workload_id.clone(), level_key.clone());
                    if level_key != "current" {
                        solution.rightsized_workloads.push(workload_id.clone());
                    }
                }
            }
        }

        Ok((
            solution,
            SolverInfo {
                name: "cp-sat-rust".to_string(),
                available: true,
                status: format!(
                    "status={status:?}; workers={worker_count}; hinted_assignments={hinted_assignments}; hinted_nodes={}; objective_profile={:?}; cost_weight={}; active_node_weight={}; memory_slack_weight={}; cpu_slack_weight={}; churn_weight={}; partial_admission={}; admission_weight={effective_admission_weight}; {stats}",
                    hinted_nodes.len(),
                    scenario.objective_profile,
                    scenario.cost_weight,
                    scenario.active_node_weight,
                    scenario.memory_slack_weight,
                    scenario.cpu_slack_weight,
                    scenario.churn_weight,
                    scenario.partial_admission
                ),
            },
        ))
    }

    fn sanitize(value: &str) -> String {
        value
            .chars()
            .map(|ch| if ch.is_ascii_alphanumeric() { ch } else { '_' })
            .collect()
    }

    fn ceil_div(a: i64, b: i64) -> i64 {
        (a + b - 1) / b
    }

    fn per_replica_requests(
        workload: &crate::model::OptimizationWorkload,
    ) -> crate::model::ResourceList {
        if workload.group_size <= 1 {
            return workload.requests.clone();
        }
        let group_size = i64::from(workload.group_size);
        crate::model::ResourceList {
            milli_cpu: workload.requests.milli_cpu / group_size,
            memory_bytes: workload.requests.memory_bytes / group_size,
            ephemeral_storage: workload.requests.ephemeral_storage / group_size,
            pods: 1,
        }
    }

    fn per_replica_scalar_requests(
        workload: &crate::model::OptimizationWorkload,
    ) -> std::collections::BTreeMap<String, i64> {
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

    fn node_gpu_capacity(node: &crate::model::OptimizationNode) -> i64 {
        node.extended_resources
            .iter()
            .filter(|(name, _)| is_gpu_resource_name(name))
            .map(|(_, value)| (*value).max(0))
            .sum()
    }

    fn placement_cost_milli_per_replica(
        node: &crate::model::OptimizationNode,
        per_replica_gpu: i64,
    ) -> i64 {
        let gpu_capacity = node_gpu_capacity(node);
        if gpu_capacity <= 0 || node.price.monthly <= 0.0 || per_replica_gpu <= 0 {
            return 0;
        }
        let cost =
            (node.price.monthly * 1000.0 * per_replica_gpu as f64 / gpu_capacity as f64).round();
        if cost.is_finite() && cost > 0.0 {
            cost as i64
        } else {
            0
        }
    }

    /// Pure model-size worker heuristic (deterministic; no CPU/env coupling).
    /// Primary tier = workloads x assignment edges (true var/constraint count); node
    /// count is a secondary cap (per-node vars + per-worker model copies cost memory),
    /// but does not force single-worker for small models (the pending/shadow path).
    pub fn model_worker_count(input: &OptimizationInput) -> i32 {
        let node_count = input.nodes.len();
        let workload_count = input.workloads.len();
        let assignment_edges: usize = input.workloads.iter().map(|w| w.feasible_nodes.len()).sum();

        // Down-throttle only genuinely huge models (the offline planner's single big
        // grouped model), where per-worker model copies threaten memory. The pending/
        // shadow path is small (≤~1000 workloads × ~100 nodes ⇒ ≤~100k edges) and is
        // latency-bound, not memory-bound, so it now lands in the 8-worker tier — a
        // measured ~2.4x admission win at the shadow time cap. The final count is still
        // capped by available cores in max_worker_cap(), so we never oversubscribe.
        let by_model = if workload_count >= 20_000 || assignment_edges >= 1_000_000 {
            2
        } else if workload_count >= 8_000 || assignment_edges >= 400_000 {
            4
        } else {
            8
        };
        let by_nodes = if node_count >= 5_000 {
            2
        } else if node_count >= 2_000 {
            4
        } else {
            8
        };
        by_model.min(by_nodes)
    }

    fn max_worker_cap() -> i32 {
        if let Ok(v) = std::env::var("KSOLVER_SOLVER_MAX_WORKERS") {
            if let Ok(n) = v.parse::<i32>() {
                if n >= 1 {
                    return n;
                }
            }
        }
        std::thread::available_parallelism()
            .map(|n| (n.get().saturating_sub(1)).max(1) as i32)
            .unwrap_or(1)
    }

    pub fn recommended_worker_count(input: &OptimizationInput) -> i32 {
        model_worker_count(input).min(max_worker_cap()).max(1)
    }
}

#[cfg(not(feature = "rust-cp-sat"))]
mod enabled {
    use super::{OptimizationInput, OptimizationSolution, ScenarioConfig, SolverInfo};
    use anyhow::{bail, Result};

    pub fn solver_info() -> SolverInfo {
        SolverInfo {
            name: "cp-sat-rust".to_string(),
            available: false,
            status: "unavailable: build with --features rust-cp-sat and provide OR-Tools"
                .to_string(),
        }
    }

    pub fn solve(
        _input: &OptimizationInput,
        _scenario: &ScenarioConfig,
    ) -> Result<(OptimizationSolution, SolverInfo)> {
        bail!("cp-sat-rust unavailable: build with --features rust-cp-sat and provide OR-Tools")
    }

    pub fn model_worker_count(_input: &OptimizationInput) -> i32 {
        1
    }

    pub fn recommended_worker_count(_input: &OptimizationInput) -> i32 {
        1
    }
}

pub use enabled::{model_worker_count, recommended_worker_count, solve, solver_info};

#[cfg(all(test, feature = "rust-cp-sat"))]
mod tests {
    use super::enabled::{model_worker_count, recommended_worker_count};
    use crate::model::{OptimizationInput, OptimizationNode, OptimizationWorkload};

    fn nodes(n: usize) -> Vec<OptimizationNode> {
        (0..n)
            .map(|i| OptimizationNode {
                name: format!("n-{i}"),
                count: 1,
                ..Default::default()
            })
            .collect()
    }
    fn wls(w: usize, feas: usize) -> Vec<OptimizationWorkload> {
        (0..w)
            .map(|i| OptimizationWorkload {
                id: format!("w-{i}"),
                feasible_nodes: (0..feas).map(|k| format!("n-{k}")).collect(),
                ..Default::default()
            })
            .collect()
    }

    #[test]
    fn large_models_throttle_to_four() {
        // 8_000 workloads hits the middle (memory) tier -> 4 workers (was 1 pre-Phase-7b).
        let input = OptimizationInput {
            nodes: nodes(100),
            workloads: wls(8_000, 1),
            anti_affinity_pairs: Vec::new(),
            ..Default::default()
        };
        assert_eq!(model_worker_count(&input), 4);
    }

    #[test]
    fn huge_model_still_throttles() {
        // Planner protection preserved: an enormous model (>=20k workloads) stays at 2.
        let input = OptimizationInput {
            nodes: nodes(100),
            workloads: wls(25_000, 1),
            anti_affinity_pairs: Vec::new(),
            ..Default::default()
        };
        assert_eq!(model_worker_count(&input), 2);
    }

    #[test]
    fn many_nodes_few_workloads() {
        let input = OptimizationInput {
            nodes: nodes(100),
            workloads: wls(50, 100),
            anti_affinity_pairs: vec![],
            ..Default::default()
        };
        assert_eq!(model_worker_count(&input), 8);
    }

    #[test]
    fn pending_scale_model_uses_eight_workers() {
        // The Phase-7b win: a 100-node pending-style model with 900 singleton workloads
        // each feasible on all 100 nodes (~90k assignment edges) now gets 8 workers,
        // not the 2 the old edge-throttle gave it. Asserts the pure pre-cap value so this
        // passes regardless of CI core count.
        let input = OptimizationInput {
            nodes: nodes(100),
            workloads: wls(900, 100),
            anti_affinity_pairs: vec![],
            ..Default::default()
        };
        assert_eq!(model_worker_count(&input), 8);
    }

    #[test]
    fn extreme_nodes_capped() {
        // The by_nodes secondary cap still bites at >=5000 nodes -> 2 workers.
        let input = OptimizationInput {
            nodes: nodes(5000),
            workloads: wls(2, 5000),
            anti_affinity_pairs: vec![],
            ..Default::default()
        };
        assert_eq!(model_worker_count(&input), 2);
    }

    #[test]
    fn recommended_never_below_one() {
        let input = OptimizationInput {
            nodes: nodes(4),
            workloads: wls(2, 4),
            anti_affinity_pairs: vec![],
            ..Default::default()
        };
        assert!(recommended_worker_count(&input) >= 1);
    }

    fn two_competing_gpu_pods() -> OptimizationInput {
        use crate::model::ResourceList;
        use std::collections::BTreeMap;
        let mut node_ext = BTreeMap::new();
        node_ext.insert("nvidia.com/gpu".to_string(), 1);
        let node = OptimizationNode {
            name: "n1".to_string(),
            count: 1,
            members: vec!["n1".to_string()],
            effective_capacity: ResourceList {
                milli_cpu: 8000,
                memory_bytes: 32 << 30,
                ephemeral_storage: 0,
                pods: 110,
            },
            extended_resources: node_ext,
            ..Default::default()
        };
        let mk = |name: &str| {
            let mut ext = BTreeMap::new();
            ext.insert("nvidia.com/gpu".to_string(), 1);
            OptimizationWorkload {
                id: format!("t/{name}"),
                namespace: "t".to_string(),
                name: name.to_string(),
                group_size: 1,
                requests: ResourceList {
                    milli_cpu: 1000,
                    memory_bytes: 1 << 30,
                    ephemeral_storage: 0,
                    pods: 0,
                },
                extended_resource_requests: ext,
                feasible_nodes: vec!["n1".to_string()],
                ..Default::default()
            }
        };
        OptimizationInput {
            nodes: vec![node],
            workloads: vec![mk("a"), mk("b")],
            anti_affinity_pairs: vec![],
            ..Default::default()
        }
    }

    #[test]
    fn partial_admission_places_what_fits() {
        use crate::model::ScenarioConfig;
        let input = two_competing_gpu_pods();
        let scenario = ScenarioConfig {
            solver: "cp-sat-rust".to_string(),
            partial_admission: true,
            ..Default::default()
        };
        let (solution, info) =
            super::enabled::solve(&input, &scenario).expect("solve should succeed, not infeasible");
        // assignment_counts is authoritative: exactly one of the two pods is admitted.
        let admitted = solution
            .assignment_counts
            .values()
            .filter(|counts| counts.values().any(|c| *c > 0))
            .count();
        assert_eq!(
            admitted, 1,
            "expected exactly one admitted; status={}",
            info.status
        );
    }

    fn gang_workload(group_size: i32, total_gpu: i64, feasible: &[&str]) -> OptimizationWorkload {
        use crate::model::{OptimizationWorkloadMember, ResourceList};
        use std::collections::BTreeMap;
        let mut ext = BTreeMap::new();
        ext.insert("nvidia.com/gpu".to_string(), total_gpu); // TOTAL across the gang
        OptimizationWorkload {
            id: "gang:t/job".to_string(),
            namespace: "t".to_string(),
            name: "job".to_string(),
            group_size,
            members: (0..group_size)
                .map(|i| OptimizationWorkloadMember {
                    namespace: "t".to_string(),
                    name: format!("m{i}"),
                    current_node: String::new(),
                })
                .collect(),
            requests: ResourceList {
                milli_cpu: 1000 * i64::from(group_size),
                memory_bytes: (1 << 30) * i64::from(group_size),
                ephemeral_storage: 0,
                pods: i64::from(group_size),
            },
            extended_resource_requests: ext,
            feasible_nodes: feasible.iter().map(|s| s.to_string()).collect(),
            ..Default::default()
        }
    }

    fn gpu_node(name: &str, gpus: i64) -> OptimizationNode {
        use crate::model::ResourceList;
        use std::collections::BTreeMap;
        let mut node_ext = BTreeMap::new();
        node_ext.insert("nvidia.com/gpu".to_string(), gpus);
        OptimizationNode {
            name: name.to_string(),
            count: 1,
            members: vec![name.to_string()],
            effective_capacity: ResourceList {
                milli_cpu: 64000,
                memory_bytes: 256 << 30,
                ephemeral_storage: 0,
                pods: 110,
            },
            extended_resources: node_ext,
            ..Default::default()
        }
    }

    #[test]
    fn gang_of_five_rejected_on_four_gpus() {
        use crate::model::ScenarioConfig;
        // group_size=5, total 5 GPU (1/replica). 4-GPU node can't fit all 5 -> not admitted.
        let input = OptimizationInput {
            nodes: vec![gpu_node("n1", 4)],
            workloads: vec![gang_workload(5, 5, &["n1"])],
            anti_affinity_pairs: vec![],
            ..Default::default()
        };
        let scenario = ScenarioConfig {
            solver: "cp-sat-rust".to_string(),
            partial_admission: true,
            ..Default::default()
        };
        let (solution, info) =
            super::enabled::solve(&input, &scenario).expect("solve should succeed");
        assert!(
            !solution.assignment_counts.contains_key("gang:t/job"),
            "5-replica gang must not be admitted on 4 GPUs; status={}",
            info.status
        );
    }

    #[test]
    fn flexible_gang_can_admit_preferred_subset() {
        use crate::model::{ObjectiveProfile, ObjectiveWeights, ScenarioConfig};
        let mut flexible = gang_workload(8, 8, &["n1"]);
        flexible.flexible = true;
        flexible.min_gpus = 2;
        flexible.preferred_gpus = 4;
        flexible.max_gpus = 8;
        let input = OptimizationInput {
            nodes: vec![gpu_node("n1", 8)],
            workloads: vec![flexible],
            anti_affinity_pairs: vec![],
            ..Default::default()
        };
        let scenario = ScenarioConfig {
            solver: "cp-sat-rust".to_string(),
            partial_admission: true,
            objective_profile: ObjectiveProfile::GpuGangAware,
            objective_weights: ObjectiveWeights {
                admission: 1,
                gpu_demand: 0,
                gang_complete: 0,
                priority: 0,
                business_value: 0,
                queue: 0,
                queue_wait: 0,
                fair_share: 0,
                deadline_urgency: 0,
                ..Default::default()
            },
            ..Default::default()
        };
        let (solution, info) =
            super::enabled::solve(&input, &scenario).expect("flexible solve should succeed");
        let total: i64 = solution
            .assignment_counts
            .get("gang:t/job")
            .map(|c| c.values().map(|v| i64::from(*v)).sum())
            .unwrap_or(0);
        assert_eq!(
            total, 4,
            "flexible gang should use preferred 4/8 replicas; status={}",
            info.status
        );
    }

    #[test]
    fn flexible_deadline_job_uses_smallest_replicas_that_meet_slack() {
        use crate::model::{ObjectiveProfile, ObjectiveWeights, ScenarioConfig};
        let mut flexible = gang_workload(8, 8, &["n1"]);
        flexible.flexible = true;
        flexible.min_gpus = 2;
        flexible.preferred_gpus = 8;
        flexible.max_gpus = 8;
        flexible.predicted_runtime_seconds = 3600;
        flexible.deadline_unix_seconds = chrono::Utc::now().timestamp() + 10_000;
        let input = OptimizationInput {
            nodes: vec![gpu_node("n1", 8)],
            workloads: vec![flexible],
            anti_affinity_pairs: vec![],
            ..Default::default()
        };
        let scenario = ScenarioConfig {
            solver: "cp-sat-rust".to_string(),
            partial_admission: true,
            objective_profile: ObjectiveProfile::GpuGangAware,
            objective_weights: ObjectiveWeights {
                admission: 1,
                gpu_demand: 1,
                gang_complete: 0,
                priority: 0,
                business_value: 0,
                queue: 0,
                queue_wait: 0,
                fair_share: 0,
                deadline_urgency: 0,
                ..Default::default()
            },
            ..Default::default()
        };

        let (solution, info) =
            super::enabled::solve(&input, &scenario).expect("flexible solve should succeed");
        let total: i64 = solution
            .assignment_counts
            .get("gang:t/job")
            .map(|c| c.values().map(|v| i64::from(*v)).sum())
            .unwrap_or(0);

        assert_eq!(
            total, 2,
            "flexible job with enough deadline slack should use minimum replicas; status={}",
            info.status
        );
    }

    #[test]
    fn gang_of_five_admitted_on_five_gpus() {
        use crate::model::ScenarioConfig;
        let input = OptimizationInput {
            nodes: vec![gpu_node("n1", 5)],
            workloads: vec![gang_workload(5, 5, &["n1"])],
            anti_affinity_pairs: vec![],
            ..Default::default()
        };
        let scenario = ScenarioConfig {
            solver: "cp-sat-rust".to_string(),
            partial_admission: true,
            ..Default::default()
        };
        let (solution, _info) =
            super::enabled::solve(&input, &scenario).expect("solve should succeed");
        let total: i64 = solution
            .assignment_counts
            .get("gang:t/job")
            .map(|c| c.values().map(|v| i64::from(*v)).sum())
            .unwrap_or(0);
        assert_eq!(total, 5, "5-replica gang must be fully admitted on 5 GPUs");
    }

    fn gpu_singleton(name: &str, total_gpu: i64, feasible: &[&str]) -> OptimizationWorkload {
        use crate::model::ResourceList;
        use std::collections::BTreeMap;
        let mut ext = BTreeMap::new();
        ext.insert("nvidia.com/gpu".to_string(), total_gpu);
        OptimizationWorkload {
            id: format!("t/{name}"),
            namespace: "t".to_string(),
            name: name.to_string(),
            group_size: 1,
            requests: ResourceList {
                milli_cpu: 1000,
                memory_bytes: 1 << 30,
                ephemeral_storage: 0,
                pods: 0,
            },
            extended_resource_requests: ext,
            feasible_nodes: feasible.iter().map(|s| s.to_string()).collect(),
            ..Default::default()
        }
    }

    fn admitted_count(solution: &crate::model::OptimizationSolution) -> usize {
        solution
            .assignment_counts
            .values()
            .filter(|counts| counts.values().any(|c| *c > 0))
            .count()
    }

    #[test]
    fn gpu_profile_can_prioritize_admitted_gpu_demand_over_workload_count() {
        use crate::model::{ObjectiveProfile, ObjectiveWeights, ScenarioConfig};
        let input = OptimizationInput {
            nodes: vec![gpu_node("n1", 4)],
            workloads: vec![
                gpu_singleton("small-a", 1, &["n1"]),
                gpu_singleton("small-b", 1, &["n1"]),
                gpu_singleton("small-c", 1, &["n1"]),
                gpu_singleton("large", 4, &["n1"]),
            ],
            ..Default::default()
        };

        let cost = ScenarioConfig {
            solver: "cp-sat-rust".to_string(),
            partial_admission: true,
            ..Default::default()
        };
        let (cost_solution, cost_info) =
            super::enabled::solve(&input, &cost).expect("cost solve should succeed");
        assert_eq!(
            admitted_count(&cost_solution),
            3,
            "cost profile maximizes admitted workload count; status={}",
            cost_info.status
        );
        assert!(
            !cost_solution.assignment_counts.contains_key("t/large"),
            "large job should lose to three small admitted workloads under cost profile"
        );

        let gpu = ScenarioConfig {
            solver: "cp-sat-rust".to_string(),
            partial_admission: true,
            objective_profile: ObjectiveProfile::GpuGangAware,
            objective_weights: ObjectiveWeights {
                admission: 0,
                gpu_demand: 1,
                gang_complete: 0,
                priority: 0,
                business_value: 0,
                queue: 0,
                queue_wait: 0,
                fair_share: 0,
                deadline_urgency: 0,
                ..Default::default()
            },
            ..Default::default()
        };
        let (gpu_solution, gpu_info) =
            super::enabled::solve(&input, &gpu).expect("gpu solve should succeed");
        assert_eq!(
            admitted_count(&gpu_solution),
            1,
            "gpu profile should be allowed to trade workload count for GPU demand; status={}",
            gpu_info.status
        );
        assert!(
            gpu_solution.assignment_counts.contains_key("t/large"),
            "large 4-GPU job should be admitted under GPU-demand scoring"
        );
    }

    #[test]
    fn gpu_profile_can_prioritize_high_priority_workload() {
        use crate::model::{ObjectiveProfile, ObjectiveWeights, ScenarioConfig};
        let low = gpu_singleton("low", 1, &["n1"]);
        let mut high = gpu_singleton("high", 1, &["n1"]);
        high.priority = 10;
        let input = OptimizationInput {
            nodes: vec![gpu_node("n1", 1)],
            workloads: vec![low, high],
            ..Default::default()
        };
        let scenario = ScenarioConfig {
            solver: "cp-sat-rust".to_string(),
            partial_admission: true,
            objective_profile: ObjectiveProfile::GpuGangAware,
            objective_weights: ObjectiveWeights {
                admission: 1,
                gpu_demand: 0,
                gang_complete: 0,
                priority: 1,
                business_value: 0,
                queue: 0,
                queue_wait: 0,
                fair_share: 0,
                deadline_urgency: 0,
                ..Default::default()
            },
            ..Default::default()
        };
        let (solution, info) =
            super::enabled::solve(&input, &scenario).expect("priority solve should succeed");
        assert!(
            solution.assignment_counts.contains_key("t/high"),
            "high-priority workload should be admitted; status={}",
            info.status
        );
        assert!(
            !solution.assignment_counts.contains_key("t/low"),
            "low-priority workload should lose scarce capacity"
        );
    }

    #[test]
    fn gpu_profile_ignores_priority_when_priority_weight_is_zero() {
        use crate::model::{ObjectiveProfile, ObjectiveWeights, ScenarioConfig};
        let mut high_priority_large = gpu_singleton("high-priority-large", 4, &["n1"]);
        high_priority_large.priority = 10_000;
        let input = OptimizationInput {
            nodes: vec![gpu_node("n1", 4)],
            workloads: vec![
                gpu_singleton("small-a", 1, &["n1"]),
                gpu_singleton("small-b", 1, &["n1"]),
                gpu_singleton("small-c", 1, &["n1"]),
                high_priority_large,
            ],
            ..Default::default()
        };
        let scenario = ScenarioConfig {
            solver: "cp-sat-rust".to_string(),
            partial_admission: true,
            objective_profile: ObjectiveProfile::GpuGangAware,
            objective_weights: ObjectiveWeights {
                admission: 1,
                gpu_demand: 0,
                gang_complete: 0,
                priority: 0,
                business_value: 0,
                queue: 0,
                queue_wait: 0,
                fair_share: 0,
                deadline_urgency: 0,
                ..Default::default()
            },
            ..Default::default()
        };

        let (solution, info) =
            super::enabled::solve(&input, &scenario).expect("priority-zero solve should succeed");

        assert_eq!(
            admitted_count(&solution),
            3,
            "priority metadata should not alter admission when its weight is zero; status={}",
            info.status
        );
        assert!(
            !solution
                .assignment_counts
                .contains_key("t/high-priority-large"),
            "high-priority workload should not displace three lower-priority jobs unless priority is weighted"
        );
    }

    #[test]
    fn gpu_profile_can_prioritize_high_business_value_workload() {
        use crate::model::{ObjectiveProfile, ObjectiveWeights, ScenarioConfig};
        let low_value = gpu_singleton("low-value", 1, &["n1"]);
        let mut high_value = gpu_singleton("high-value", 1, &["n1"]);
        high_value.business_value = 25;
        let input = OptimizationInput {
            nodes: vec![gpu_node("n1", 1)],
            workloads: vec![low_value, high_value],
            ..Default::default()
        };
        let scenario = ScenarioConfig {
            solver: "cp-sat-rust".to_string(),
            partial_admission: true,
            objective_profile: ObjectiveProfile::GpuGangAware,
            objective_weights: ObjectiveWeights {
                admission: 1,
                gpu_demand: 0,
                gang_complete: 0,
                priority: 0,
                business_value: 1,
                queue: 0,
                queue_wait: 0,
                fair_share: 0,
                deadline_urgency: 0,
                ..Default::default()
            },
            ..Default::default()
        };
        let (solution, info) =
            super::enabled::solve(&input, &scenario).expect("business-value solve should succeed");
        assert!(
            solution.assignment_counts.contains_key("t/high-value"),
            "high-business-value workload should be admitted; status={}",
            info.status
        );
        assert!(
            !solution.assignment_counts.contains_key("t/low-value"),
            "low-business-value workload should lose scarce capacity"
        );
    }

    #[test]
    fn gpu_profile_can_prioritize_high_queue_score_workload() {
        use crate::model::{ObjectiveProfile, ObjectiveWeights, ScenarioConfig};
        let low_queue = gpu_singleton("batch", 1, &["n1"]);
        let mut high_queue = gpu_singleton("urgent", 1, &["n1"]);
        high_queue.queue = "urgent".to_string();
        high_queue.queue_score = 50;
        let input = OptimizationInput {
            nodes: vec![gpu_node("n1", 1)],
            workloads: vec![low_queue, high_queue],
            ..Default::default()
        };
        let scenario = ScenarioConfig {
            solver: "cp-sat-rust".to_string(),
            partial_admission: true,
            objective_profile: ObjectiveProfile::GpuGangAware,
            objective_weights: ObjectiveWeights {
                admission: 1,
                gpu_demand: 0,
                gang_complete: 0,
                priority: 0,
                business_value: 0,
                queue: 1,
                queue_wait: 0,
                fair_share: 0,
                deadline_urgency: 0,
                ..Default::default()
            },
            ..Default::default()
        };
        let (solution, info) =
            super::enabled::solve(&input, &scenario).expect("queue solve should succeed");
        assert!(
            solution.assignment_counts.contains_key("t/urgent"),
            "high-queue-score workload should be admitted; status={}",
            info.status
        );
        assert!(
            !solution.assignment_counts.contains_key("t/batch"),
            "low-queue-score workload should lose scarce capacity"
        );
    }

    #[test]
    fn gpu_profile_can_prioritize_long_waiting_workload() {
        use crate::model::{ObjectiveProfile, ObjectiveWeights, ScenarioConfig};
        let fresh = gpu_singleton("fresh", 1, &["n1"]);
        let mut waiting = gpu_singleton("waiting", 1, &["n1"]);
        waiting.queue_wait_seconds = 3_600;
        let input = OptimizationInput {
            nodes: vec![gpu_node("n1", 1)],
            workloads: vec![fresh, waiting],
            ..Default::default()
        };
        let scenario = ScenarioConfig {
            solver: "cp-sat-rust".to_string(),
            partial_admission: true,
            objective_profile: ObjectiveProfile::GpuGangAware,
            objective_weights: ObjectiveWeights {
                admission: 1,
                gpu_demand: 0,
                gang_complete: 0,
                priority: 0,
                business_value: 0,
                queue: 0,
                queue_wait: 1,
                fair_share: 0,
                deadline_urgency: 0,
                ..Default::default()
            },
            ..Default::default()
        };
        let (solution, info) =
            super::enabled::solve(&input, &scenario).expect("queue-wait solve should succeed");
        assert!(
            solution.assignment_counts.contains_key("t/waiting"),
            "long-waiting workload should be admitted; status={}",
            info.status
        );
        assert!(
            !solution.assignment_counts.contains_key("t/fresh"),
            "fresh workload should lose scarce capacity when queue-wait weight is enabled"
        );
    }

    #[test]
    fn gpu_profile_can_prioritize_under_share_workload() {
        use crate::model::{ObjectiveProfile, ObjectiveWeights, ScenarioConfig};
        let over_share = gpu_singleton("over-share", 1, &["n1"]);
        let mut under_share = gpu_singleton("under-share", 1, &["n1"]);
        under_share.fair_share_deficit = 1;
        let input = OptimizationInput {
            nodes: vec![gpu_node("n1", 1)],
            workloads: vec![over_share, under_share],
            ..Default::default()
        };
        let scenario = ScenarioConfig {
            solver: "cp-sat-rust".to_string(),
            partial_admission: true,
            objective_profile: ObjectiveProfile::GpuGangAware,
            objective_weights: ObjectiveWeights {
                admission: 1,
                gpu_demand: 0,
                gang_complete: 0,
                priority: 0,
                business_value: 0,
                queue: 0,
                queue_wait: 0,
                fair_share: 1,
                deadline_urgency: 0,
                ..Default::default()
            },
            ..Default::default()
        };
        let (solution, info) =
            super::enabled::solve(&input, &scenario).expect("fair-share solve should succeed");
        assert!(
            solution.assignment_counts.contains_key("t/under-share"),
            "under-share workload should be admitted; status={}",
            info.status
        );
        assert!(
            !solution.assignment_counts.contains_key("t/over-share"),
            "over-share workload should lose scarce capacity"
        );
    }

    #[test]
    fn gpu_profile_can_prioritize_deadline_urgent_workload() {
        use crate::model::{ObjectiveProfile, ObjectiveWeights, ScenarioConfig};
        let no_deadline = gpu_singleton("no-deadline", 1, &["n1"]);
        let mut urgent = gpu_singleton("urgent", 1, &["n1"]);
        urgent.deadline_unix_seconds = 1_893_456_000;
        urgent.predicted_runtime_seconds = 7_200;
        let input = OptimizationInput {
            nodes: vec![gpu_node("n1", 1)],
            workloads: vec![no_deadline, urgent],
            ..Default::default()
        };
        let scenario = ScenarioConfig {
            solver: "cp-sat-rust".to_string(),
            partial_admission: true,
            objective_profile: ObjectiveProfile::GpuGangAware,
            objective_weights: ObjectiveWeights {
                admission: 1,
                gpu_demand: 0,
                gang_complete: 0,
                priority: 0,
                business_value: 0,
                queue: 0,
                queue_wait: 0,
                fair_share: 0,
                deadline_urgency: 5,
                ..Default::default()
            },
            ..Default::default()
        };
        let (solution, info) =
            super::enabled::solve(&input, &scenario).expect("deadline solve should succeed");
        assert!(
            solution.assignment_counts.contains_key("t/urgent"),
            "deadline workload should be admitted; status={}",
            info.status
        );
        assert!(
            !solution.assignment_counts.contains_key("t/no-deadline"),
            "no-deadline workload should lose scarce capacity"
        );
    }

    #[test]
    fn gpu_profile_can_penalize_predicted_deadline_miss() {
        use crate::model::{ObjectiveProfile, ObjectiveWeights, ScenarioConfig};
        let now = chrono::Utc::now().timestamp();
        let mut missed = gpu_singleton("missed-deadline", 1, &["n1"]);
        missed.deadline_unix_seconds = now + 60;
        missed.predicted_runtime_seconds = 3_600;
        let mut meetable = gpu_singleton("meetable-deadline", 1, &["n1"]);
        meetable.deadline_unix_seconds = now + 7_200;
        meetable.predicted_runtime_seconds = 3_600;
        let input = OptimizationInput {
            nodes: vec![gpu_node("n1", 1)],
            workloads: vec![missed, meetable],
            ..Default::default()
        };
        let scenario = ScenarioConfig {
            solver: "cp-sat-rust".to_string(),
            partial_admission: true,
            objective_profile: ObjectiveProfile::GpuGangAware,
            objective_weights: ObjectiveWeights {
                admission: 10,
                gpu_demand: 0,
                gang_complete: 0,
                priority: 0,
                business_value: 0,
                queue: 0,
                queue_wait: 0,
                fair_share: 0,
                deadline_urgency: 1,
                deadline_miss: 100_000,
                ..Default::default()
            },
            ..Default::default()
        };
        let (solution, info) =
            super::enabled::solve(&input, &scenario).expect("deadline-miss solve should succeed");
        assert!(
            solution
                .assignment_counts
                .contains_key("t/meetable-deadline"),
            "meetable deadline workload should be admitted; status={}",
            info.status
        );
        assert!(
            !solution.assignment_counts.contains_key("t/missed-deadline"),
            "predicted-missed deadline workload should lose scarce capacity when miss penalty is enabled"
        );
    }

    #[test]
    fn soft_affinity_breaks_ties_without_changing_admission() {
        use crate::model::ScenarioConfig;
        use std::collections::BTreeMap;
        // Two cost-equal nodes; a singleton feasible on both, preferring n2 (soft score 10).
        let mut w = gpu_singleton("a", 1, &["n1", "n2"]);
        w.soft_scores = BTreeMap::from([("n2".to_string(), 10)]);
        let input = OptimizationInput {
            nodes: vec![gpu_node("n1", 4), gpu_node("n2", 4)],
            workloads: vec![w],
            ..Default::default()
        };
        let base = ScenarioConfig {
            solver: "cp-sat-rust".to_string(),
            partial_admission: true,
            ..Default::default()
        };
        // Soft OFF: admitted (node arbitrary).
        let (off, _) = super::enabled::solve(&input, &base).expect("solve");
        assert_eq!(admitted_count(&off), 1);
        // Soft ON: same admission, and placed on the preferred node n2.
        let soft = ScenarioConfig {
            enable_soft_affinity: true,
            ..base
        };
        let (on, info) = super::enabled::solve(&input, &soft).expect("solve");
        assert_eq!(
            admitted_count(&on),
            1,
            "soft affinity must not change admission; status={}",
            info.status
        );
        let counts = on.assignment_counts.get("t/a").expect("workload admitted");
        assert!(
            counts.contains_key("n2"),
            "soft affinity should place on the preferred node n2, got {counts:?}"
        );
    }

    #[test]
    fn soft_affinity_negative_score_steers_away_without_changing_admission() {
        use crate::model::ScenarioConfig;
        use std::collections::BTreeMap;
        // Two cost-equal nodes; a singleton feasible on both, DISCOURAGED from n1 (score -10).
        // Models preferred pod anti-affinity: the pod should avoid n1, admission unchanged.
        let mut w = gpu_singleton("a", 1, &["n1", "n2"]);
        w.soft_scores = BTreeMap::from([("n1".to_string(), -10)]);
        let input = OptimizationInput {
            nodes: vec![gpu_node("n1", 4), gpu_node("n2", 4)],
            workloads: vec![w],
            ..Default::default()
        };
        let scenario = ScenarioConfig {
            solver: "cp-sat-rust".to_string(),
            partial_admission: true,
            enable_soft_affinity: true,
            ..Default::default()
        };
        let (sol, info) = super::enabled::solve(&input, &scenario).expect("solve");
        assert_eq!(
            admitted_count(&sol),
            1,
            "negative soft score must not change admission; status={}",
            info.status
        );
        let counts = sol.assignment_counts.get("t/a").expect("workload admitted");
        assert!(
            counts.contains_key("n2") && !counts.contains_key("n1"),
            "negative soft score should steer placement to n2, got {counts:?}"
        );
    }

    #[test]
    fn coplacement_rewards_same_node_without_changing_admission() {
        use crate::model::{CoplacementDomain, ScenarioConfig, SoftCoplacement};
        // Two singletons a,b each feasible on n1,n2 (cost-equal). Co-placement reward on the
        // per-node (hostname) domains -> the solver should co-locate them on one node.
        let a = gpu_singleton("a", 1, &["n1", "n2"]);
        let b = gpu_singleton("b", 1, &["n1", "n2"]);
        let input = OptimizationInput {
            nodes: vec![gpu_node("n1", 4), gpu_node("n2", 4)],
            workloads: vec![a, b],
            soft_coplacement_pairs: vec![SoftCoplacement {
                a: "t/a".to_string(),
                b: "t/b".to_string(),
                weight: 50,
                domains: vec![
                    CoplacementDomain {
                        a_nodes: vec!["n1".into()],
                        b_nodes: vec!["n1".into()],
                    },
                    CoplacementDomain {
                        a_nodes: vec!["n2".into()],
                        b_nodes: vec!["n2".into()],
                    },
                ],
            }],
            ..Default::default()
        };
        let scenario = ScenarioConfig {
            solver: "cp-sat-rust".to_string(),
            partial_admission: true,
            enable_soft_affinity: true,
            ..Default::default()
        };
        let (sol, info) = super::enabled::solve(&input, &scenario).expect("solve");
        assert_eq!(
            admitted_count(&sol),
            2,
            "co-placement must not change admission; status={}",
            info.status
        );
        let a_node = sol
            .assignment_counts
            .get("t/a")
            .unwrap()
            .keys()
            .next()
            .unwrap()
            .clone();
        let b_node = sol
            .assignment_counts
            .get("t/b")
            .unwrap()
            .keys()
            .next()
            .unwrap()
            .clone();
        assert_eq!(
            a_node, b_node,
            "co-placement reward should put a and b on the same node"
        );
    }

    #[test]
    fn coplacement_never_over_admits_under_capacity() {
        use crate::model::{CoplacementDomain, ScenarioConfig, SoftCoplacement};
        // One 1-GPU node; two 1-GPU singletons that prefer co-placement. Only one fits — the
        // reward (upper-bound only) cannot force both on. Admission stays capped at 1.
        let a = gpu_singleton("a", 1, &["n1"]);
        let b = gpu_singleton("b", 1, &["n1"]);
        let input = OptimizationInput {
            nodes: vec![gpu_node("n1", 1)],
            workloads: vec![a, b],
            soft_coplacement_pairs: vec![SoftCoplacement {
                a: "t/a".to_string(),
                b: "t/b".to_string(),
                weight: 50,
                domains: vec![CoplacementDomain {
                    a_nodes: vec!["n1".into()],
                    b_nodes: vec!["n1".into()],
                }],
            }],
            ..Default::default()
        };
        let scenario = ScenarioConfig {
            solver: "cp-sat-rust".to_string(),
            partial_admission: true,
            enable_soft_affinity: true,
            ..Default::default()
        };
        let (sol, info) = super::enabled::solve(&input, &scenario).expect("solve");
        assert_eq!(
            admitted_count(&sol),
            1,
            "capacity still caps admission with co-placement on; status={}",
            info.status
        );
    }

    #[test]
    fn soft_affinity_never_over_admits_under_capacity() {
        use crate::model::ScenarioConfig;
        use std::collections::BTreeMap;
        // One 1-GPU node; two 1-GPU singletons both preferring it. Only one can be admitted —
        // soft affinity must not change that (admission preserved).
        let mut a = gpu_singleton("a", 1, &["n1"]);
        a.soft_scores = BTreeMap::from([("n1".to_string(), 10)]);
        let mut b = gpu_singleton("b", 1, &["n1"]);
        b.soft_scores = BTreeMap::from([("n1".to_string(), 10)]);
        let input = OptimizationInput {
            nodes: vec![gpu_node("n1", 1)],
            workloads: vec![a, b],
            ..Default::default()
        };
        let scenario = ScenarioConfig {
            solver: "cp-sat-rust".to_string(),
            partial_admission: true,
            enable_soft_affinity: true,
            ..Default::default()
        };
        let (sol, info) = super::enabled::solve(&input, &scenario).expect("solve");
        assert_eq!(
            admitted_count(&sol),
            1,
            "capacity still caps admission with soft on; status={}",
            info.status
        );
    }

    #[test]
    fn quota_caps_admitted_singletons() {
        use crate::model::{QuotaGroup, ScenarioConfig};
        // Two 1-GPU singletons both fit on a 4-GPU node; a GPU quota of 1 over both
        // must admit exactly one. Raising the quota to 2 admits both.
        let make = |limit: i64| OptimizationInput {
            nodes: vec![gpu_node("n1", 4)],
            workloads: vec![
                gpu_singleton("a", 1, &["n1"]),
                gpu_singleton("b", 1, &["n1"]),
            ],
            quota_groups: vec![QuotaGroup {
                workload_ids: vec!["t/a".to_string(), "t/b".to_string()],
                resources: vec!["nvidia.com/gpu".to_string()],
                limit,
            }],
            ..Default::default()
        };
        let scenario = ScenarioConfig {
            solver: "cp-sat-rust".to_string(),
            partial_admission: true,
            ..Default::default()
        };
        let (sol1, info1) = super::enabled::solve(&make(1), &scenario).expect("solve");
        assert_eq!(
            admitted_count(&sol1),
            1,
            "quota 1 must admit exactly one; status={}",
            info1.status
        );
        let (sol2, info2) = super::enabled::solve(&make(2), &scenario).expect("solve");
        assert_eq!(
            admitted_count(&sol2),
            2,
            "quota 2 must admit both; status={}",
            info2.status
        );
    }

    #[test]
    fn quota_counts_whole_gang() {
        use crate::model::{QuotaGroup, ScenarioConfig};
        // A 2-replica gang consumes 2 GPUs as a unit; a quota of 1 rejects it entirely
        // even though the node has capacity — proves gang-aware quota accounting.
        let input = OptimizationInput {
            nodes: vec![gpu_node("n1", 8)],
            workloads: vec![gang_workload(2, 2, &["n1"])],
            quota_groups: vec![QuotaGroup {
                workload_ids: vec!["gang:t/job".to_string()],
                resources: vec!["nvidia.com/gpu".to_string()],
                limit: 1,
            }],
            ..Default::default()
        };
        let scenario = ScenarioConfig {
            solver: "cp-sat-rust".to_string(),
            partial_admission: true,
            ..Default::default()
        };
        let (solution, info) = super::enabled::solve(&input, &scenario).expect("solve");
        assert!(
            !solution.assignment_counts.contains_key("gang:t/job"),
            "2-GPU gang must be rejected under a 1-GPU quota; status={}",
            info.status
        );
    }

    #[test]
    fn budget_group_caps_admitted_monthly_cost() {
        use crate::model::{BudgetGroup, Money, ScenarioConfig};
        let mut node = gpu_node("n1", 4);
        node.price = Money {
            monthly: 4000.0,
            ..Default::default()
        };
        let make = |limit_milli: i64| OptimizationInput {
            nodes: vec![node.clone()],
            workloads: vec![
                gpu_singleton("a", 1, &["n1"]),
                gpu_singleton("b", 1, &["n1"]),
            ],
            budget_groups: vec![BudgetGroup {
                name: "research".to_string(),
                workload_ids: vec!["t/a".to_string(), "t/b".to_string()],
                limit_milli,
            }],
            ..Default::default()
        };
        let scenario = ScenarioConfig {
            solver: "cp-sat-rust".to_string(),
            partial_admission: true,
            ..Default::default()
        };

        let (capped, capped_info) =
            super::enabled::solve(&make(1_000_000), &scenario).expect("capped solve");
        assert_eq!(
            admitted_count(&capped),
            1,
            "one 1-GPU placement costs 1000 monthly units; status={}",
            capped_info.status
        );
        let (uncapped, uncapped_info) =
            super::enabled::solve(&make(2_000_000), &scenario).expect("uncapped solve");
        assert_eq!(
            admitted_count(&uncapped),
            2,
            "two 1-GPU placements fit a 2000 monthly unit cap; status={}",
            uncapped_info.status
        );
    }

    #[test]
    fn quota_is_hard_constraint_without_partial_admission() {
        use crate::model::{QuotaGroup, ScenarioConfig};
        // Without partial_admission every workload must be placed. Quota groups remain hard
        // constraints, so a quota that cannot admit all required work makes the strict model
        // infeasible instead of silently over-admitting.
        let input = OptimizationInput {
            nodes: vec![gpu_node("n1", 4)],
            workloads: vec![
                gpu_singleton("a", 1, &["n1"]),
                gpu_singleton("b", 1, &["n1"]),
            ],
            quota_groups: vec![QuotaGroup {
                workload_ids: vec!["t/a".to_string(), "t/b".to_string()],
                resources: vec!["nvidia.com/gpu".to_string()],
                limit: 1,
            }],
            ..Default::default()
        };
        let scenario = ScenarioConfig {
            solver: "cp-sat-rust".to_string(),
            partial_admission: false,
            ..Default::default()
        };
        assert!(super::enabled::solve(&input, &scenario).is_err());
    }

    #[test]
    fn quota_counts_multiple_resources_incl_mig() {
        use crate::model::{
            OptimizationNode, OptimizationWorkload, QuotaGroup, ResourceList, ScenarioConfig,
        };
        use std::collections::BTreeMap;
        // A node with a MIG slice resource; one pending pod requesting the slice; a quota group
        // that sums BOTH nvidia.com/gpu and the MIG slice, limit 0 -> the MIG pod is not admitted
        // (proves MIG slices count toward the quota).
        let node = OptimizationNode {
            name: "n1".to_string(),
            count: 1,
            members: vec!["n1".to_string()],
            effective_capacity: ResourceList {
                milli_cpu: 8000,
                memory_bytes: 32 << 30,
                ephemeral_storage: 0,
                pods: 110,
            },
            extended_resources: BTreeMap::from([("nvidia.com/mig-1g.5gb".to_string(), 7)]),
            ..Default::default()
        };
        let w = OptimizationWorkload {
            id: "t/slice".to_string(),
            namespace: "t".to_string(),
            name: "slice".to_string(),
            group_size: 1,
            requests: ResourceList {
                milli_cpu: 1000,
                memory_bytes: 1 << 30,
                ephemeral_storage: 0,
                pods: 0,
            },
            extended_resource_requests: BTreeMap::from([("nvidia.com/mig-1g.5gb".to_string(), 1)]),
            feasible_nodes: vec!["n1".to_string()],
            ..Default::default()
        };
        let input = OptimizationInput {
            nodes: vec![node],
            workloads: vec![w],
            quota_groups: vec![QuotaGroup {
                workload_ids: vec!["t/slice".to_string()],
                resources: vec![
                    "nvidia.com/gpu".to_string(),
                    "nvidia.com/mig-1g.5gb".to_string(),
                ],
                limit: 0,
            }],
            ..Default::default()
        };
        let scenario = ScenarioConfig {
            solver: "cp-sat-rust".to_string(),
            partial_admission: true,
            ..Default::default()
        };
        let (solution, info) = super::enabled::solve(&input, &scenario).expect("solve");
        assert_eq!(
            admitted_count(&solution),
            0,
            "MIG slice must count toward the quota (limit 0 -> not admitted); status={}",
            info.status
        );
    }

    fn colocate_gang_input(colocate: bool) -> OptimizationInput {
        let mut w = gang_workload(4, 4, &["n1", "n2"]);
        w.colocate = colocate;
        OptimizationInput {
            nodes: vec![gpu_node("n1", 2), gpu_node("n2", 2)],
            workloads: vec![w],
            anti_affinity_pairs: vec![],
            ..Default::default()
        }
    }

    #[test]
    fn colocated_gang_needs_single_node() {
        use crate::model::ScenarioConfig;
        let scenario = ScenarioConfig {
            solver: "cp-sat-rust".to_string(),
            partial_admission: true,
            ..Default::default()
        };
        let (sol, info) =
            super::enabled::solve(&colocate_gang_input(true), &scenario).expect("solve");
        assert!(
            !sol.assignment_counts.contains_key("gang:t/job"),
            "co-located 4-gang must not fit on 2-GPU nodes; status={}",
            info.status
        );
    }

    #[test]
    fn non_colocated_gang_spreads() {
        use crate::model::ScenarioConfig;
        let scenario = ScenarioConfig {
            solver: "cp-sat-rust".to_string(),
            partial_admission: true,
            ..Default::default()
        };
        let (sol, _info) =
            super::enabled::solve(&colocate_gang_input(false), &scenario).expect("solve");
        let total: i64 = sol
            .assignment_counts
            .get("gang:t/job")
            .map(|c| c.values().map(|v| i64::from(*v)).sum())
            .unwrap_or(0);
        assert_eq!(
            total, 4,
            "non-co-located 4-gang should spread 2+2 across the nodes"
        );
    }

    #[test]
    fn self_anti_affine_gang_spreads_one_per_node() {
        use crate::model::ScenarioConfig;
        let w = gang_workload(3, 3, &["n1", "n2", "n3"]);
        let input = OptimizationInput {
            nodes: vec![gpu_node("n1", 4), gpu_node("n2", 4), gpu_node("n3", 4)],
            workloads: vec![w],
            anti_affinity_pairs: vec![("gang:t/job".to_string(), "gang:t/job".to_string())],
            ..Default::default()
        };
        let scenario = ScenarioConfig {
            solver: "cp-sat-rust".to_string(),
            partial_admission: true,
            ..Default::default()
        };
        let (sol, _i) = super::enabled::solve(&input, &scenario).expect("solve");
        let counts = sol.assignment_counts.get("gang:t/job").expect("admitted");
        assert_eq!(counts.values().sum::<i32>(), 3);
        assert!(
            counts.values().all(|c| *c <= 1),
            "spread should be <=1 per node"
        );
    }

    #[test]
    fn self_anti_affine_gang_rejected_when_too_few_nodes() {
        use crate::model::ScenarioConfig;
        let w = gang_workload(3, 3, &["n1", "n2"]);
        let input = OptimizationInput {
            nodes: vec![gpu_node("n1", 4), gpu_node("n2", 4)],
            workloads: vec![w],
            anti_affinity_pairs: vec![("gang:t/job".to_string(), "gang:t/job".to_string())],
            ..Default::default()
        };
        let scenario = ScenarioConfig {
            solver: "cp-sat-rust".to_string(),
            partial_admission: true,
            ..Default::default()
        };
        let (sol, _i) = super::enabled::solve(&input, &scenario).expect("solve");
        assert!(
            !sol.assignment_counts.contains_key("gang:t/job"),
            "3-replica spread cannot fit <=1/node on 2 nodes"
        );
    }

    #[test]
    fn cross_pair_forbids_shared_node() {
        use crate::model::{
            OptimizationWorkload, OptimizationWorkloadMember, ResourceList, ScenarioConfig,
        };
        use std::collections::BTreeMap;
        let mk = |name: &str, feas: &[&str]| {
            let mut ext = BTreeMap::new();
            ext.insert("nvidia.com/gpu".to_string(), 1);
            OptimizationWorkload {
                id: format!("t/{name}"),
                namespace: "t".into(),
                name: name.into(),
                group_size: 1,
                members: vec![OptimizationWorkloadMember {
                    namespace: "t".into(),
                    name: name.into(),
                    current_node: String::new(),
                }],
                requests: ResourceList {
                    milli_cpu: 1000,
                    memory_bytes: 1 << 30,
                    ephemeral_storage: 0,
                    pods: 1,
                },
                extended_resource_requests: ext,
                feasible_nodes: feas.iter().map(|s| s.to_string()).collect(),
                ..Default::default()
            }
        };
        let scenario = ScenarioConfig {
            solver: "cp-sat-rust".into(),
            partial_admission: true,
            ..Default::default()
        };
        let input1 = OptimizationInput {
            nodes: vec![gpu_node("n1", 4)],
            workloads: vec![mk("a", &["n1"]), mk("b", &["n1"])],
            anti_affinity_pairs: vec![("t/a".into(), "t/b".into())],
            ..Default::default()
        };
        let (s1, _) = super::enabled::solve(&input1, &scenario).expect("solve");
        let admitted1 = s1
            .assignment_counts
            .values()
            .filter(|c| c.values().any(|v| *v > 0))
            .count();
        assert_eq!(admitted1, 1, "cross-pair on one node admits only one");
        let input2 = OptimizationInput {
            nodes: vec![gpu_node("n1", 4), gpu_node("n2", 4)],
            workloads: vec![mk("a", &["n1", "n2"]), mk("b", &["n1", "n2"])],
            anti_affinity_pairs: vec![("t/a".into(), "t/b".into())],
            ..Default::default()
        };
        let (s2, _) = super::enabled::solve(&input2, &scenario).expect("solve");
        let admitted2 = s2
            .assignment_counts
            .values()
            .filter(|c| c.values().any(|v| *v > 0))
            .count();
        assert_eq!(admitted2, 2, "cross-pair with two nodes admits both");
    }

    #[test]
    fn cross_pair_presence_allows_colocated_gang_alone() {
        use crate::model::ScenarioConfig;
        let scenario = ScenarioConfig {
            solver: "cp-sat-rust".into(),
            partial_admission: true,
            ..Default::default()
        };
        // A: colocated gang gs=2 (2 GPU); B: singleton 1 GPU; both on n1 (4 GPU).
        let mut a = gang_workload(2, 2, &["n1"]);
        a.colocate = true;
        a.id = "gang:t/a".into();
        let mut b = gang_workload(1, 1, &["n1"]);
        b.id = "gang:t/b".into();
        let input = OptimizationInput {
            nodes: vec![gpu_node("n1", 4)],
            workloads: vec![a, b],
            anti_affinity_pairs: vec![("gang:t/a".into(), "gang:t/b".into())],
            ..Default::default()
        };
        let (sol, _i) = super::enabled::solve(&input, &scenario).expect("solve");
        let admitted = sol
            .assignment_counts
            .values()
            .filter(|c| c.values().any(|v| *v > 0))
            .count();
        assert!(
            admitted <= 1,
            "cross-paired workloads must not share the single node"
        );
        // A alone must be admissible (presence model does not forbid the colocated gang).
        let mut a2 = gang_workload(2, 2, &["n1"]);
        a2.colocate = true;
        a2.id = "gang:t/a".into();
        let solo = OptimizationInput {
            nodes: vec![gpu_node("n1", 4)],
            workloads: vec![a2],
            anti_affinity_pairs: vec![],
            ..Default::default()
        };
        let (s3, _i) = super::enabled::solve(&solo, &scenario).expect("solve");
        assert_eq!(
            s3.assignment_counts
                .get("gang:t/a")
                .map(|c| c.values().sum::<i32>())
                .unwrap_or(0),
            2,
            "colocated gang admissible alone"
        );
        // two-node: A on n1, B on n2 -> both admitted.
        let mut a3 = gang_workload(2, 2, &["n1", "n2"]);
        a3.colocate = true;
        a3.id = "gang:t/a".into();
        let mut b3 = gang_workload(1, 1, &["n1", "n2"]);
        b3.id = "gang:t/b".into();
        let two = OptimizationInput {
            nodes: vec![gpu_node("n1", 4), gpu_node("n2", 4)],
            workloads: vec![a3, b3],
            anti_affinity_pairs: vec![("gang:t/a".into(), "gang:t/b".into())],
            ..Default::default()
        };
        let (s4, _i) = super::enabled::solve(&two, &scenario).expect("solve");
        let admitted4 = s4
            .assignment_counts
            .values()
            .filter(|c| c.values().any(|v| *v > 0))
            .count();
        assert_eq!(admitted4, 2, "two nodes admit both cross-paired workloads");
    }

    #[test]
    fn hard_equality_is_infeasible_when_flag_off() {
        use crate::model::ScenarioConfig;
        let input = two_competing_gpu_pods();
        let scenario = ScenarioConfig {
            solver: "cp-sat-rust".to_string(),
            partial_admission: false,
            ..Default::default()
        };
        // Both pods must place but only one fits -> hard equality makes the model infeasible.
        assert!(super::enabled::solve(&input, &scenario).is_err());
    }
}
