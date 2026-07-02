use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "lowercase")]
pub enum PodPlacement {
    Placed { node: String },
    Unplaced { reason: String },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PodDecision {
    pub uid: String,
    pub namespace: String,
    pub name: String,
    /// Solver workload id that produced this decision. Members of the same gang share this value,
    /// letting future binders preflight all members together. Empty for older traces.
    #[serde(default)]
    pub binding_group: String,
    pub gpu_request: i64,
    #[serde(default)]
    pub priority: i64,
    #[serde(default)]
    pub priority_class_name: String,
    #[serde(default)]
    pub team: String,
    #[serde(default)]
    pub queue: String,
    #[serde(default)]
    pub queue_score: i64,
    #[serde(default)]
    pub business_value: i64,
    #[serde(default)]
    pub queue_wait_seconds: i64,
    #[serde(default)]
    pub deadline_unix_seconds: i64,
    #[serde(default)]
    pub min_gpus: i64,
    #[serde(default)]
    pub max_gpus: i64,
    #[serde(default)]
    pub preferred_gpus: i64,
    #[serde(default)]
    pub flexible: bool,
    #[serde(default)]
    pub predicted_runtime_seconds: i64,
    #[serde(default)]
    pub predicted_peak_vram_bytes: i64,
    #[serde(default)]
    pub deadline_slack_seconds: i64,
    #[serde(default)]
    pub predicted_finish_unix_seconds: i64,
    #[serde(default)]
    pub predicted_deadline_miss: bool,
    pub placement: PodPlacement,
    /// Scheduling constraints shadow does not model (e.g. pod anti-affinity); a
    /// placed recommendation may violate these. Empty when none.
    #[serde(default)]
    pub caveats: Vec<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct RepairAction {
    pub action: String,
    pub namespace: String,
    pub pod: String,
    pub node: String,
    #[serde(default)]
    pub to_node: String,
    pub gpu_request: i64,
    #[serde(default)]
    pub disruption_cost: i32,
    #[serde(default)]
    pub reason: String,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct RepairSkip {
    pub namespace: String,
    pub pod: String,
    pub node: String,
    pub gpu_request: i64,
    pub reason: String,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct RepairPlan {
    pub target: String,
    pub target_gpu_request: i64,
    pub target_priority: i64,
    #[serde(default)]
    pub target_business_value: i64,
    #[serde(default)]
    pub target_deadline_unix_seconds: i64,
    #[serde(default)]
    pub target_latest_start_unix_seconds: i64,
    #[serde(default)]
    pub target_queue_wait_seconds: i64,
    pub node: String,
    pub freed_gpu: i64,
    #[serde(default)]
    pub disruption_cost: i32,
    pub actions: Vec<RepairAction>,
    #[serde(default)]
    pub skipped_candidates: Vec<RepairSkip>,
    pub explanation: String,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct RepairMetrics {
    pub repairable_targets: usize,
    pub unrepairable_targets: usize,
    pub vram_blocked_targets: usize,
    pub not_enough_total_gpu_targets: usize,
    pub policy_or_candidate_blocked_targets: usize,
    #[serde(default)]
    pub incomplete_model_targets: usize,
    pub migration_actions: usize,
    pub preemption_actions: usize,
    pub skipped_candidates: usize,
    #[serde(default)]
    pub priority_blocked_candidates: usize,
    #[serde(default)]
    pub value_policy_blocked_candidates: usize,
    #[serde(default)]
    pub disruption_policy_blocked_candidates: usize,
    #[serde(default)]
    pub pdb_blocked_candidates: usize,
    #[serde(default)]
    pub candidate_budget_skipped_candidates: usize,
    pub disruption_cost: i64,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct DeadlineMetrics {
    pub deadline_jobs: usize,
    pub placed_deadline_jobs: usize,
    pub unplaced_deadline_jobs: usize,
    pub predicted_misses: usize,
    #[serde(default)]
    pub placed_predicted_misses: usize,
    #[serde(default)]
    pub unplaced_predicted_misses: usize,
    pub worst_slack_seconds: i64,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct QuotaMetrics {
    pub throttled_pods: usize,
    pub exhausted_groups: usize,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct AdmissionMetrics {
    pub admitted_pods: usize,
    pub admitted_gpu_demand: i64,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct QueueWaitMetrics {
    pub pending_pods: usize,
    pub max_queue_wait_seconds: i64,
    pub high_priority_pending_pods: usize,
    pub high_priority_max_queue_wait_seconds: i64,
    pub unplaced_max_queue_wait_seconds: i64,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct TenantQueueMetric {
    pub tenant: String,
    #[serde(default)]
    pub fair_share_weight: i64,
    pub pending_pods: usize,
    pub placed_pods: usize,
    pub unplaced_pods: usize,
    pub requested_gpu_demand: i64,
    pub admitted_gpu_demand: i64,
    #[serde(default)]
    pub denied_gpu_demand: i64,
    #[serde(default)]
    pub admitted_monthly_cost_milli: i64,
    #[serde(default)]
    pub budget_monthly_milli: i64,
    #[serde(default)]
    pub budget_overage_monthly_milli: i64,
    #[serde(default)]
    pub admitted_share_milli: i64,
    #[serde(default)]
    pub fair_share_gpu_milli: i64,
    #[serde(default)]
    pub fair_share_delta_gpu_milli: i64,
    #[serde(default)]
    pub under_fair_share_gpu_milli: i64,
    #[serde(default)]
    pub borrowed_gpu_milli: i64,
    #[serde(default)]
    pub reclaimable_borrowed_gpu_milli: i64,
    pub max_queue_wait_seconds: i64,
    pub throttled_pods: usize,
    pub throttled_max_queue_wait_seconds: i64,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct TenantFairnessMetrics {
    pub tenants: Vec<TenantQueueMetric>,
    pub throttled_pods: usize,
    pub throttled_max_queue_wait_seconds: i64,
    #[serde(default)]
    pub under_fair_share_tenants: usize,
    #[serde(default)]
    pub over_fair_share_tenants: usize,
    #[serde(default)]
    pub total_borrowed_gpu_milli: i64,
    #[serde(default)]
    pub reclaimable_borrowed_gpu_milli: i64,
    #[serde(default)]
    pub budget_over_tenants: usize,
    #[serde(default)]
    pub total_budget_overage_monthly_milli: i64,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct GpuUtilizationMetrics {
    pub active_gpu_nodes: usize,
    pub stranded_gpu_on_active_nodes: i64,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct SchedulingOutcomeSummary {
    pub total_pods: usize,
    pub placed_pods: usize,
    pub unplaced_pods: usize,
    pub requested_gpu_demand: i64,
    pub admitted_gpu_demand: i64,
    pub unplaced_gpu_demand: i64,
    /// Percent as milli-percent, where 100000 means 100.000%.
    pub pod_admission_percent_milli: i64,
    /// Percent as milli-percent, where 100000 means 100.000%.
    pub gpu_admission_percent_milli: i64,
    #[serde(default)]
    pub admitted_monthly_cost_milli: i64,
    pub active_gpu_nodes: usize,
    pub stranded_gpu_on_active_nodes: i64,
    pub predicted_deadline_misses: usize,
    #[serde(default)]
    pub placed_predicted_deadline_misses: usize,
    #[serde(default)]
    pub unplaced_predicted_deadline_misses: usize,
    pub repairable_unplaced_targets: usize,
    pub unrepairable_unplaced_targets: usize,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct JobObservationMetrics {
    pub completed_gpu_pods: usize,
    pub runtime_observations: usize,
    pub failed_gpu_pods: usize,
    pub max_runtime_seconds: i64,
    pub max_peak_memory_bytes: i64,
    pub unique_command_hashes: usize,
    #[serde(default)]
    pub runtime_prediction_samples: usize,
    #[serde(default)]
    pub runtime_prediction_mape_milli: i64,
    #[serde(default)]
    pub max_runtime_prediction_error_seconds: i64,
    #[serde(default)]
    pub vram_prediction_samples: usize,
    #[serde(default)]
    pub vram_prediction_mape_milli: i64,
    #[serde(default)]
    pub max_vram_prediction_error_bytes: i64,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct PredictionAuditMetrics {
    pub pending_pods: usize,
    pub fingerprint_matched_pods: usize,
    pub history_exact_pods: usize,
    pub history_scaled_pods: usize,
    #[serde(default)]
    pub history_segment_pods: usize,
    pub hint_pods: usize,
    pub unknown_pods: usize,
    pub predicted_runtime_pods: usize,
    pub predicted_vram_pods: usize,
    pub average_confidence: i64,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct PredictionAuditDetail {
    pub uid: String,
    pub namespace: String,
    pub name: String,
    pub gpu_request: i64,
    pub command_fingerprint_matched: bool,
    #[serde(default)]
    pub framework: String,
    #[serde(default)]
    pub job_type: String,
    #[serde(default)]
    pub prediction_key: String,
    pub predicted_runtime_seconds: i64,
    #[serde(default)]
    pub predicted_runtime_lower_seconds: i64,
    #[serde(default)]
    pub predicted_runtime_upper_seconds: i64,
    pub predicted_peak_vram_bytes: i64,
    #[serde(default)]
    pub predicted_peak_vram_lower_bytes: i64,
    #[serde(default)]
    pub predicted_peak_vram_upper_bytes: i64,
    pub runtime_source: String,
    pub vram_source: String,
    pub sample_count: usize,
    /// 0..100 confidence score. This is an operator-facing calibration signal, not a probability.
    pub confidence: i64,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct NodeGroupingMetrics {
    pub enabled: bool,
    pub used: bool,
    pub eligible_group_count: usize,
    pub eligible_node_count: usize,
    pub max_group_size: usize,
    pub grouped_node_count: usize,
    pub grouped_candidate_edges: usize,
    pub disabled_reasons: Vec<String>,
    pub fallback_reason: String,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct CandidateQualityMetrics {
    pub pruning_active: bool,
    pub widened: bool,
    /// Candidate-edge reduction from the unpruned feasible graph in milli-percent.
    pub edge_reduction_milli: i64,
    pub regret_status: String,
    pub explanation: String,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct BindingReservationMetrics {
    pub active_reservations: usize,
    pub active_entries: usize,
    pub reserved_gpus: i64,
    pub created: usize,
    pub rejected: usize,
    pub expired: usize,
    pub observed_bound_entries: usize,
    pub stale_entries: usize,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct BindingOutcomeMetrics {
    pub bound: usize,
    pub validated: usize,
    pub skipped: usize,
    pub failed: usize,
    #[serde(default)]
    pub canary_skipped: usize,
    #[serde(default)]
    pub readiness_skipped: usize,
    #[serde(default)]
    pub identity_skipped: usize,
    #[serde(default)]
    pub scheduler_skipped: usize,
    #[serde(default)]
    pub already_bound_skipped: usize,
    #[serde(default)]
    pub dra_skipped: usize,
    #[serde(default)]
    pub throttle_skipped: usize,
    #[serde(default)]
    pub reservation_skipped: usize,
    #[serde(default)]
    pub disabled_skipped: usize,
    #[serde(default)]
    pub group_skipped: usize,
    #[serde(default)]
    pub other_skipped: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DecisionTrace {
    pub sequence: u64,
    pub observed_pods: usize,
    pub decisions: Vec<PodDecision>,
    pub solver_status: String,
    #[serde(default)]
    pub objective_profile: crate::model::ObjectiveProfile,
    #[serde(default)]
    pub objective_weights: crate::model::ObjectiveWeights,
    pub solve_millis: u64,
    /// Time spent strictly inside the CP-SAT solve call (excludes collect/normalize/build);
    /// use this to verify the configured solve time limit.
    #[serde(default)]
    pub solve_core_millis: u64,
    pub snapshot_age_millis: u64,
    pub note: String,
    #[serde(default)]
    pub repair_plans: Vec<RepairPlan>,
    #[serde(default)]
    pub repair_notes: Vec<String>,
    #[serde(default)]
    pub repair_metrics: RepairMetrics,
    #[serde(default)]
    pub deadline_metrics: DeadlineMetrics,
    #[serde(default)]
    pub quota_metrics: QuotaMetrics,
    #[serde(default)]
    pub admission_metrics: AdmissionMetrics,
    #[serde(default)]
    pub queue_wait_metrics: QueueWaitMetrics,
    #[serde(default)]
    pub tenant_fairness_metrics: TenantFairnessMetrics,
    #[serde(default)]
    pub gpu_utilization_metrics: GpuUtilizationMetrics,
    #[serde(default)]
    pub outcome_summary: SchedulingOutcomeSummary,
    #[serde(default)]
    pub job_observation_metrics: JobObservationMetrics,
    #[serde(default)]
    pub prediction_audit_metrics: PredictionAuditMetrics,
    #[serde(default)]
    pub prediction_audit_details: Vec<PredictionAuditDetail>,
    #[serde(default)]
    pub node_grouping_metrics: NodeGroupingMetrics,
    #[serde(default)]
    pub candidate_quality_metrics: CandidateQualityMetrics,
    #[serde(default)]
    pub binding_reservation_metrics: BindingReservationMetrics,
    #[serde(default)]
    pub binding_outcome_metrics: BindingOutcomeMetrics,
    #[serde(default)]
    pub candidate_node_limit: usize,
    #[serde(default)]
    pub retry_count: usize,
    #[serde(default)]
    pub unpruned_candidate_edges: usize,
    #[serde(default)]
    pub initial_candidate_edges: usize,
    #[serde(default)]
    pub final_candidate_edges: usize,
    #[serde(default)]
    pub candidate_pruned_workloads: usize,
    #[serde(default)]
    pub widening_reason: String,
}

pub fn summarize_candidate_quality(trace: &DecisionTrace) -> CandidateQualityMetrics {
    let pruning_active = trace.candidate_node_limit > 0
        && (trace.candidate_pruned_workloads > 0
            || trace.final_candidate_edges < trace.unpruned_candidate_edges);
    let widened = trace.retry_count > 0;
    let edge_reduction_milli = if trace.unpruned_candidate_edges == 0
        || trace.final_candidate_edges >= trace.unpruned_candidate_edges
    {
        0
    } else {
        let reduced = trace.unpruned_candidate_edges - trace.final_candidate_edges;
        (reduced as i64).saturating_mul(100_000) / trace.unpruned_candidate_edges as i64
    };
    let (regret_status, explanation) = if !pruning_active && trace.node_grouping_metrics.used {
        (
            "grouped_exact_if_equivalent",
            "node grouping was used; trace node-grouping fields describe the equivalence assumptions",
        )
    } else if !pruning_active {
        (
            "full_feasible_set",
            "no candidate pruning remained in the accepted solve",
        )
    } else if trace.final_candidate_edges >= trace.unpruned_candidate_edges {
        (
            "full_retry",
            "candidate pruning triggered widening to the full feasible edge set",
        )
    } else if widened {
        (
            "pruned_after_widening_regret_unknown",
            "candidate pruning remained after widening; compare with a full solve to measure regret",
        )
    } else {
        (
            "pruned_regret_unknown",
            "candidate pruning was active; compare with a full solve to measure regret",
        )
    };

    CandidateQualityMetrics {
        pruning_active,
        widened,
        edge_reduction_milli,
        regret_status: regret_status.to_string(),
        explanation: explanation.to_string(),
    }
}

pub fn summarize_scheduling_outcome(trace: &DecisionTrace) -> SchedulingOutcomeSummary {
    let total_pods = trace.decisions.len();
    let mut placed_pods = 0usize;
    let mut requested_gpu_demand = 0i64;
    let mut admitted_gpu_demand = 0i64;

    for decision in &trace.decisions {
        let gpu = decision.gpu_request.max(0);
        requested_gpu_demand += gpu;
        if matches!(decision.placement, PodPlacement::Placed { .. }) {
            placed_pods += 1;
            admitted_gpu_demand += gpu;
        }
    }

    let unplaced_pods = total_pods.saturating_sub(placed_pods);
    let unplaced_gpu_demand = requested_gpu_demand.saturating_sub(admitted_gpu_demand);
    let pod_admission_percent_milli = if total_pods == 0 {
        0
    } else {
        (placed_pods as i64).saturating_mul(100_000) / total_pods as i64
    };
    let gpu_admission_percent_milli = if requested_gpu_demand <= 0 {
        0
    } else {
        admitted_gpu_demand.saturating_mul(100_000) / requested_gpu_demand
    };

    SchedulingOutcomeSummary {
        total_pods,
        placed_pods,
        unplaced_pods,
        requested_gpu_demand,
        admitted_gpu_demand,
        unplaced_gpu_demand,
        pod_admission_percent_milli,
        gpu_admission_percent_milli,
        admitted_monthly_cost_milli: trace
            .tenant_fairness_metrics
            .tenants
            .iter()
            .map(|t| t.admitted_monthly_cost_milli)
            .sum(),
        active_gpu_nodes: trace.gpu_utilization_metrics.active_gpu_nodes,
        stranded_gpu_on_active_nodes: trace.gpu_utilization_metrics.stranded_gpu_on_active_nodes,
        predicted_deadline_misses: trace.deadline_metrics.predicted_misses,
        placed_predicted_deadline_misses: trace.deadline_metrics.placed_predicted_misses,
        unplaced_predicted_deadline_misses: trace.deadline_metrics.unplaced_predicted_misses,
        repairable_unplaced_targets: trace.repair_metrics.repairable_targets,
        unrepairable_unplaced_targets: trace.repair_metrics.unrepairable_targets,
    }
}

pub struct TraceStore {
    capacity: usize,
    inner: Mutex<VecDeque<DecisionTrace>>,
    seq: AtomicU64,
}

impl TraceStore {
    pub fn new(capacity: usize) -> Self {
        Self {
            capacity: capacity.max(1),
            inner: Mutex::new(VecDeque::new()),
            seq: AtomicU64::new(0),
        }
    }

    pub fn next_sequence(&self) -> u64 {
        self.seq.fetch_add(1, Ordering::SeqCst) + 1
    }

    pub fn push(&self, trace: DecisionTrace) {
        let mut g = self.inner.lock().expect("trace store poisoned");
        if g.len() == self.capacity {
            g.pop_front();
        }
        g.push_back(trace);
    }

    pub fn recent(&self) -> Vec<DecisionTrace> {
        let g = self.inner.lock().expect("trace store poisoned");
        g.iter().rev().cloned().collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn trace(seq: u64) -> DecisionTrace {
        DecisionTrace {
            sequence: seq,
            observed_pods: 1,
            decisions: vec![PodDecision {
                uid: "u1".into(),
                namespace: "team-a".into(),
                name: "job-0".into(),
                binding_group: String::new(),
                gpu_request: 4,
                priority: 0,
                priority_class_name: String::new(),
                team: String::new(),
                queue: String::new(),
                queue_score: 0,
                business_value: 0,
                queue_wait_seconds: 0,
                deadline_unix_seconds: 0,
                min_gpus: 0,
                max_gpus: 0,
                preferred_gpus: 0,
                flexible: false,
                predicted_runtime_seconds: 0,
                predicted_peak_vram_bytes: 0,
                deadline_slack_seconds: 0,
                predicted_finish_unix_seconds: 0,
                predicted_deadline_miss: false,
                placement: PodPlacement::Placed {
                    node: "node-1".into(),
                },
                caveats: vec![],
            }],
            solver_status: "OPTIMAL".into(),
            objective_profile: crate::model::ObjectiveProfile::CostBinpack,
            objective_weights: crate::model::ObjectiveWeights::default(),
            solve_millis: 12,
            solve_core_millis: 8,
            snapshot_age_millis: 3,
            note: String::new(),
            repair_plans: Vec::new(),
            repair_notes: Vec::new(),
            repair_metrics: RepairMetrics::default(),
            deadline_metrics: DeadlineMetrics::default(),
            quota_metrics: QuotaMetrics::default(),
            admission_metrics: AdmissionMetrics::default(),
            queue_wait_metrics: QueueWaitMetrics::default(),
            tenant_fairness_metrics: TenantFairnessMetrics::default(),
            gpu_utilization_metrics: GpuUtilizationMetrics::default(),
            outcome_summary: SchedulingOutcomeSummary::default(),
            job_observation_metrics: JobObservationMetrics::default(),
            prediction_audit_metrics: PredictionAuditMetrics::default(),
            prediction_audit_details: Vec::new(),
            node_grouping_metrics: NodeGroupingMetrics::default(),
            candidate_quality_metrics: CandidateQualityMetrics::default(),
            binding_reservation_metrics: BindingReservationMetrics::default(),
            binding_outcome_metrics: BindingOutcomeMetrics::default(),
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
    fn recent_is_newest_first() {
        let s = TraceStore::new(8);
        s.push(trace(1));
        s.push(trace(2));
        let r = s.recent();
        assert_eq!(r[0].sequence, 2);
        assert_eq!(r[1].sequence, 1);
    }

    #[test]
    fn evicts_oldest_beyond_capacity() {
        let s = TraceStore::new(2);
        s.push(trace(1));
        s.push(trace(2));
        s.push(trace(3));
        let r = s.recent();
        assert_eq!(r.len(), 2);
        assert_eq!(r[0].sequence, 3);
    }

    #[test]
    fn sequence_is_monotonic() {
        let s = TraceStore::new(4);
        assert_eq!(s.next_sequence(), 1);
        assert_eq!(s.next_sequence(), 2);
    }

    #[test]
    fn pod_decision_deserializes_without_caveats() {
        // Backward compatibility: older traces omit newer optional fields.
        let json = r#"{"uid":"u1","namespace":"team-a","name":"job-0","gpu_request":1,"placement":{"kind":"placed","node":"n1"}}"#;
        let d: PodDecision = serde_json::from_str(json).expect("deserialize");
        assert!(d.caveats.is_empty());
        assert_eq!(d.queue_score, 0);
    }

    #[test]
    fn repair_metrics_deserializes_without_skip_reason_buckets() {
        let json = r#"{"repairable_targets":1,"unrepairable_targets":2,"vram_blocked_targets":0,"not_enough_total_gpu_targets":0,"policy_or_candidate_blocked_targets":1,"incomplete_model_targets":0,"migration_actions":1,"preemption_actions":0,"skipped_candidates":3,"disruption_cost":10}"#;
        let metrics: RepairMetrics = serde_json::from_str(json).expect("deserialize");
        assert_eq!(metrics.skipped_candidates, 3);
        assert_eq!(metrics.priority_blocked_candidates, 0);
        assert_eq!(metrics.value_policy_blocked_candidates, 0);
        assert_eq!(metrics.disruption_policy_blocked_candidates, 0);
        assert_eq!(metrics.pdb_blocked_candidates, 0);
        assert_eq!(metrics.candidate_budget_skipped_candidates, 0);
    }

    #[test]
    fn deadline_metrics_deserializes_without_split_miss_counts() {
        let json = r#"{"deadline_jobs":2,"placed_deadline_jobs":1,"unplaced_deadline_jobs":1,"predicted_misses":1,"worst_slack_seconds":-60}"#;
        let metrics: DeadlineMetrics = serde_json::from_str(json).expect("deserialize");
        assert_eq!(metrics.predicted_misses, 1);
        assert_eq!(metrics.placed_predicted_misses, 0);
        assert_eq!(metrics.unplaced_predicted_misses, 0);
    }

    #[test]
    fn scheduling_outcome_summary_deserializes_without_split_miss_counts() {
        let json = r#"{"total_pods":2,"placed_pods":1,"unplaced_pods":1,"requested_gpu_demand":4,"admitted_gpu_demand":2,"unplaced_gpu_demand":2,"pod_admission_percent_milli":50000,"gpu_admission_percent_milli":50000,"active_gpu_nodes":1,"stranded_gpu_on_active_nodes":1,"predicted_deadline_misses":1,"repairable_unplaced_targets":0,"unrepairable_unplaced_targets":1}"#;
        let summary: SchedulingOutcomeSummary = serde_json::from_str(json).expect("deserialize");
        assert_eq!(summary.predicted_deadline_misses, 1);
        assert_eq!(summary.placed_predicted_deadline_misses, 0);
        assert_eq!(summary.unplaced_predicted_deadline_misses, 0);
    }

    #[test]
    fn scheduling_outcome_summary_counts_pods_and_gpu_demand() {
        let mut t = trace(1);
        t.decisions.push(PodDecision {
            uid: "u2".into(),
            namespace: "team-a".into(),
            name: "job-1".into(),
            binding_group: String::new(),
            gpu_request: 2,
            priority: 0,
            priority_class_name: String::new(),
            team: String::new(),
            queue: String::new(),
            queue_score: 0,
            business_value: 0,
            queue_wait_seconds: 0,
            deadline_unix_seconds: 0,
            min_gpus: 0,
            max_gpus: 0,
            preferred_gpus: 0,
            flexible: false,
            predicted_runtime_seconds: 0,
            predicted_peak_vram_bytes: 0,
            deadline_slack_seconds: 0,
            predicted_finish_unix_seconds: 0,
            predicted_deadline_miss: false,
            placement: PodPlacement::Unplaced {
                reason: "insufficient capacity".into(),
            },
            caveats: vec![],
        });
        t.gpu_utilization_metrics = GpuUtilizationMetrics {
            active_gpu_nodes: 1,
            stranded_gpu_on_active_nodes: 3,
        };
        t.deadline_metrics.predicted_misses = 1;
        t.deadline_metrics.placed_predicted_misses = 1;
        t.repair_metrics.repairable_targets = 1;
        t.tenant_fairness_metrics.tenants.push(TenantQueueMetric {
            tenant: "team-a".into(),
            admitted_monthly_cost_milli: 2_000_000,
            ..Default::default()
        });

        let summary = summarize_scheduling_outcome(&t);

        assert_eq!(summary.total_pods, 2);
        assert_eq!(summary.placed_pods, 1);
        assert_eq!(summary.unplaced_pods, 1);
        assert_eq!(summary.requested_gpu_demand, 6);
        assert_eq!(summary.admitted_gpu_demand, 4);
        assert_eq!(summary.unplaced_gpu_demand, 2);
        assert_eq!(summary.pod_admission_percent_milli, 50_000);
        assert_eq!(summary.gpu_admission_percent_milli, 66_666);
        assert_eq!(summary.admitted_monthly_cost_milli, 2_000_000);
        assert_eq!(summary.active_gpu_nodes, 1);
        assert_eq!(summary.stranded_gpu_on_active_nodes, 3);
        assert_eq!(summary.predicted_deadline_misses, 1);
        assert_eq!(summary.placed_predicted_deadline_misses, 1);
        assert_eq!(summary.unplaced_predicted_deadline_misses, 0);
        assert_eq!(summary.repairable_unplaced_targets, 1);
    }

    #[test]
    fn candidate_quality_reports_full_feasible_set_when_unpruned() {
        let mut t = trace(1);
        t.candidate_node_limit = 0;
        t.unpruned_candidate_edges = 16;
        t.final_candidate_edges = 16;

        let quality = summarize_candidate_quality(&t);

        assert!(!quality.pruning_active);
        assert!(!quality.widened);
        assert_eq!(quality.edge_reduction_milli, 0);
        assert_eq!(quality.regret_status, "full_feasible_set");
    }

    #[test]
    fn candidate_quality_reports_unknown_regret_for_pruned_solve() {
        let mut t = trace(1);
        t.candidate_node_limit = 8;
        t.unpruned_candidate_edges = 100;
        t.final_candidate_edges = 25;
        t.candidate_pruned_workloads = 3;

        let quality = summarize_candidate_quality(&t);

        assert!(quality.pruning_active);
        assert!(!quality.widened);
        assert_eq!(quality.edge_reduction_milli, 75_000);
        assert_eq!(quality.regret_status, "pruned_regret_unknown");
    }

    #[test]
    fn candidate_quality_reports_widened_pruned_solve() {
        let mut t = trace(1);
        t.candidate_node_limit = 16;
        t.retry_count = 1;
        t.unpruned_candidate_edges = 100;
        t.final_candidate_edges = 50;
        t.candidate_pruned_workloads = 2;

        let quality = summarize_candidate_quality(&t);

        assert!(quality.pruning_active);
        assert!(quality.widened);
        assert_eq!(quality.edge_reduction_milli, 50_000);
        assert_eq!(
            quality.regret_status,
            "pruned_after_widening_regret_unknown"
        );
    }

    #[test]
    fn binding_outcome_metrics_deserializes_without_skip_reason_buckets() {
        let json = r#"{"bound":1,"validated":2,"skipped":3,"failed":4,"canary_skipped":5}"#;

        let metrics: BindingOutcomeMetrics =
            serde_json::from_str(json).expect("legacy binding metrics should deserialize");

        assert_eq!(metrics.bound, 1);
        assert_eq!(metrics.validated, 2);
        assert_eq!(metrics.skipped, 3);
        assert_eq!(metrics.failed, 4);
        assert_eq!(metrics.canary_skipped, 5);
        assert_eq!(metrics.readiness_skipped, 0);
        assert_eq!(metrics.identity_skipped, 0);
        assert_eq!(metrics.scheduler_skipped, 0);
        assert_eq!(metrics.already_bound_skipped, 0);
        assert_eq!(metrics.dra_skipped, 0);
        assert_eq!(metrics.throttle_skipped, 0);
        assert_eq!(metrics.reservation_skipped, 0);
        assert_eq!(metrics.disabled_skipped, 0);
        assert_eq!(metrics.group_skipped, 0);
        assert_eq!(metrics.other_skipped, 0);
    }
}
