use crate::model::SolveRequest;
use lazy_static::lazy_static;
use prometheus::{
    Encoder, Histogram, HistogramOpts, HistogramVec, IntCounter, IntCounterVec, IntGauge,
    IntGaugeVec, Opts, Registry, TextEncoder,
};

const SOLVE_LABELS: &[&str] = &[
    "cluster",
    "solver",
    "scenario",
    "snapshot_mode",
    "use_usage_adjusted_requests",
    "ignore_taints",
    "relax_preferred_affinity",
    "relax_required_anti_affinity",
    "ignore_unschedulable_workloads",
];

const SOLVE_STATUS_LABELS: &[&str] = &[
    "cluster",
    "solver",
    "scenario",
    "snapshot_mode",
    "use_usage_adjusted_requests",
    "ignore_taints",
    "relax_preferred_affinity",
    "relax_required_anti_affinity",
    "ignore_unschedulable_workloads",
    "status",
];

lazy_static! {
    pub static ref REGISTRY: Registry = Registry::new();
    pub static ref SOLVE_DURATION_SECONDS: HistogramVec = HistogramVec::new(
        HistogramOpts::new(
            "solver_solve_duration_seconds",
            "End-to-end solver analysis duration in seconds"
        )
        .buckets(vec![
            0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0, 120.0, 300.0, 600.0, 1200.0
        ]),
        SOLVE_LABELS
    )
    .expect("metric can be created");
    pub static ref SOLVES_TOTAL: IntCounterVec = IntCounterVec::new(
        Opts::new("solver_solves_total", "Total number of solver analyses"),
        SOLVE_STATUS_LABELS
    )
    .expect("metric can be created");
    pub static ref SOLVES_IN_FLIGHT: IntGaugeVec = IntGaugeVec::new(
        Opts::new(
            "solver_solves_in_flight",
            "Current in-flight solver analyses"
        ),
        SOLVE_LABELS
    )
    .expect("metric can be created");
    pub static ref SHADOW_POD_OBSERVATIONS: IntCounter = IntCounter::new(
        "ksolver_shadow_pod_observations_total",
        "Pending GPU pod observations across shadow solve windows (not unique pods)"
    )
    .expect("metric can be created");
    pub static ref SHADOW_PENDING: IntGauge = IntGauge::new(
        "ksolver_shadow_pending_pods",
        "Current count of in-scope pending GPU pods observed"
    )
    .expect("metric can be created");
    pub static ref SHADOW_ADMITTED_PODS: IntGauge = IntGauge::new(
        "ksolver_shadow_admitted_pods",
        "Current count of pending GPU pods admitted by the latest shadow solve"
    )
    .expect("metric can be created");
    pub static ref SHADOW_ADMITTED_GPU_DEMAND: IntGauge = IntGauge::new(
        "ksolver_shadow_admitted_gpu_demand",
        "Current GPU demand admitted by the latest shadow solve"
    )
    .expect("metric can be created");
    pub static ref SHADOW_REQUESTED_GPU_DEMAND: IntGauge = IntGauge::new(
        "ksolver_shadow_requested_gpu_demand",
        "Current GPU demand requested by pending GPU pods in the latest shadow solve"
    )
    .expect("metric can be created");
    pub static ref SHADOW_UNPLACED_GPU_DEMAND: IntGauge = IntGauge::new(
        "ksolver_shadow_unplaced_gpu_demand",
        "Current GPU demand left unplaced by the latest shadow solve"
    )
    .expect("metric can be created");
    pub static ref SHADOW_POD_ADMISSION_PERCENT_MILLI: IntGauge = IntGauge::new(
        "ksolver_shadow_pod_admission_percent_milli",
        "Current pod admission rate in milli-percent for the latest shadow solve"
    )
    .expect("metric can be created");
    pub static ref SHADOW_GPU_ADMISSION_PERCENT_MILLI: IntGauge = IntGauge::new(
        "ksolver_shadow_gpu_admission_percent_milli",
        "Current GPU-demand admission rate in milli-percent for the latest shadow solve"
    )
    .expect("metric can be created");
    pub static ref SHADOW_ADMITTED_MONTHLY_COST_MILLI: IntGauge = IntGauge::new(
        "ksolver_shadow_admitted_monthly_cost_milli",
        "Current estimated monthly cost of admitted pending GPU work in milli-currency units"
    )
    .expect("metric can be created");
    pub static ref SHADOW_MAX_QUEUE_WAIT_SECONDS: IntGauge = IntGauge::new(
        "ksolver_shadow_max_queue_wait_seconds",
        "Maximum queue wait age in seconds among pending GPU pods in the latest shadow solve"
    )
    .expect("metric can be created");
    pub static ref SHADOW_HIGH_PRIORITY_MAX_QUEUE_WAIT_SECONDS: IntGauge = IntGauge::new(
        "ksolver_shadow_high_priority_max_queue_wait_seconds",
        "Maximum queue wait age in seconds among positive-priority pending GPU pods in the latest shadow solve"
    )
    .expect("metric can be created");
    pub static ref SHADOW_DEADLINE_JOBS: IntGauge = IntGauge::new(
        "ksolver_shadow_deadline_jobs",
        "Current count of deadline-bearing pending GPU pods in the latest shadow solve"
    )
    .expect("metric can be created");
    pub static ref SHADOW_UNPLACED_DEADLINE_JOBS: IntGauge = IntGauge::new(
        "ksolver_shadow_unplaced_deadline_jobs",
        "Current count of deadline-bearing pending GPU pods unplaced in the latest shadow solve"
    )
    .expect("metric can be created");
    pub static ref SHADOW_PREDICTED_DEADLINE_MISSES_CURRENT: IntGauge = IntGauge::new(
        "ksolver_shadow_predicted_deadline_misses",
        "Current count of deadline-bearing pending GPU pods predicted to miss their requested deadline in the latest shadow solve"
    )
    .expect("metric can be created");
    pub static ref SHADOW_PLACED_PREDICTED_DEADLINE_MISSES_CURRENT: IntGauge = IntGauge::new(
        "ksolver_shadow_placed_predicted_deadline_misses",
        "Current count of admitted deadline-bearing pending GPU pods predicted to miss their requested deadline in the latest shadow solve"
    )
    .expect("metric can be created");
    pub static ref SHADOW_UNPLACED_PREDICTED_DEADLINE_MISSES_CURRENT: IntGauge = IntGauge::new(
        "ksolver_shadow_unplaced_predicted_deadline_misses",
        "Current count of unplaced deadline-bearing pending GPU pods predicted to miss their requested deadline in the latest shadow solve"
    )
    .expect("metric can be created");
    pub static ref SHADOW_WORST_DEADLINE_SLACK_SECONDS: IntGauge = IntGauge::new(
        "ksolver_shadow_worst_deadline_slack_seconds",
        "Worst deadline slack in seconds among deadline-bearing pending GPU pods in the latest shadow solve"
    )
    .expect("metric can be created");
    pub static ref SHADOW_UNPLACED_CURRENT: IntGauge = IntGauge::new(
        "ksolver_shadow_unplaced",
        "Current count of pending GPU pods unplaced by the latest shadow solve"
    )
    .expect("metric can be created");
    pub static ref SHADOW_VRAM_BLOCKED_CURRENT: IntGauge = IntGauge::new(
        "ksolver_shadow_vram_blocked",
        "Current count of pending GPU pods unplaced because predicted peak VRAM exceeds known node GPU memory in the latest shadow solve"
    )
    .expect("metric can be created");
    pub static ref SHADOW_HIGH_PRIORITY_UNPLACED_CURRENT: IntGauge = IntGauge::new(
        "ksolver_shadow_high_priority_unplaced",
        "Current count of positive-priority pending GPU pods unplaced by the latest shadow solve"
    )
    .expect("metric can be created");
    pub static ref SHADOW_QUOTA_THROTTLED_PODS: IntGauge = IntGauge::new(
        "ksolver_shadow_quota_throttled_pods",
        "Current count of pending GPU pods throttled by configured namespace quota in the latest shadow solve"
    )
    .expect("metric can be created");
    pub static ref SHADOW_QUOTA_THROTTLED_MAX_QUEUE_WAIT_SECONDS: IntGauge = IntGauge::new(
        "ksolver_shadow_quota_throttled_max_queue_wait_seconds",
        "Maximum queue wait age in seconds among quota-throttled pending GPU pods in the latest shadow solve"
    )
    .expect("metric can be created");
    pub static ref SHADOW_FAIRNESS_UNDER_SHARE_TENANTS: IntGauge = IntGauge::new(
        "ksolver_shadow_fairness_under_share_tenants",
        "Current count of tenants below weighted fair share in the latest shadow solve"
    )
    .expect("metric can be created");
    pub static ref SHADOW_FAIRNESS_OVER_SHARE_TENANTS: IntGauge = IntGauge::new(
        "ksolver_shadow_fairness_over_share_tenants",
        "Current count of tenants above weighted fair share in the latest shadow solve"
    )
    .expect("metric can be created");
    pub static ref SHADOW_FAIRNESS_BORROWED_GPU_MILLI: IntGauge = IntGauge::new(
        "ksolver_shadow_fairness_borrowed_gpu_milli",
        "Current admitted GPU-milli above weighted fair share across tenants in the latest shadow solve"
    )
    .expect("metric can be created");
    pub static ref SHADOW_FAIRNESS_RECLAIMABLE_BORROWED_GPU_MILLI: IntGauge = IntGauge::new(
        "ksolver_shadow_fairness_reclaimable_borrowed_gpu_milli",
        "Current borrowed GPU-milli that is reclaimable because another tenant is denied while below share"
    )
    .expect("metric can be created");
    pub static ref SHADOW_BUDGET_OVER_TENANTS: IntGauge = IntGauge::new(
        "ksolver_shadow_budget_over_tenants",
        "Current count of tenants whose admitted monthly cost exceeds configured budget in the latest shadow solve"
    )
    .expect("metric can be created");
    pub static ref SHADOW_BUDGET_OVERAGE_MONTHLY_MILLI: IntGauge = IntGauge::new(
        "ksolver_shadow_budget_overage_monthly_milli",
        "Current total admitted monthly cost over configured tenant budgets in milli-currency units"
    )
    .expect("metric can be created");
    pub static ref SHADOW_ACTIVE_GPU_NODES: IntGauge = IntGauge::new(
        "ksolver_shadow_active_gpu_nodes",
        "Current active GPU nodes in the latest shadow placement"
    )
    .expect("metric can be created");
    pub static ref SHADOW_STRANDED_GPU_ON_ACTIVE_NODES: IntGauge = IntGauge::new(
        "ksolver_shadow_stranded_gpu_on_active_nodes",
        "Current free GPU slots stranded on active GPU nodes in the latest shadow placement"
    )
    .expect("metric can be created");
    pub static ref SHADOW_JOB_OBSERVATION_COMPLETED_GPU_PODS: IntGauge = IntGauge::new(
        "ksolver_shadow_job_observation_completed_gpu_pods",
        "Completed GPU pods observed in the latest cluster snapshot for prediction training"
    )
    .expect("metric can be created");
    pub static ref SHADOW_JOB_OBSERVATION_RUNTIME_SAMPLES: IntGauge = IntGauge::new(
        "ksolver_shadow_job_observation_runtime_samples",
        "Completed GPU pods with runtime observations in the latest cluster snapshot"
    )
    .expect("metric can be created");
    pub static ref SHADOW_JOB_OBSERVATION_FAILED_GPU_PODS: IntGauge = IntGauge::new(
        "ksolver_shadow_job_observation_failed_gpu_pods",
        "Failed completed GPU pods observed in the latest cluster snapshot"
    )
    .expect("metric can be created");
    pub static ref SHADOW_JOB_OBSERVATION_MAX_RUNTIME_SECONDS: IntGauge = IntGauge::new(
        "ksolver_shadow_job_observation_max_runtime_seconds",
        "Maximum completed GPU pod runtime observed in the latest cluster snapshot"
    )
    .expect("metric can be created");
    pub static ref SHADOW_JOB_OBSERVATION_MAX_PEAK_MEMORY_BYTES: IntGauge = IntGauge::new(
        "ksolver_shadow_job_observation_max_peak_memory_bytes",
        "Maximum completed GPU pod peak memory observed in the latest cluster snapshot"
    )
    .expect("metric can be created");
    pub static ref SHADOW_JOB_OBSERVATION_UNIQUE_COMMAND_HASHES: IntGauge = IntGauge::new(
        "ksolver_shadow_job_observation_unique_command_hashes",
        "Unique command hashes among completed GPU pod observations in the latest cluster snapshot"
    )
    .expect("metric can be created");
    pub static ref SHADOW_JOB_OBSERVATION_RUNTIME_PREDICTION_SAMPLES: IntGauge = IntGauge::new(
        "ksolver_shadow_job_observation_runtime_prediction_samples",
        "Completed GPU pod observations with both predicted and actual runtime in the latest cluster snapshot"
    )
    .expect("metric can be created");
    pub static ref SHADOW_JOB_OBSERVATION_RUNTIME_PREDICTION_MAPE_MILLI: IntGauge = IntGauge::new(
        "ksolver_shadow_job_observation_runtime_prediction_mape_milli",
        "Mean absolute runtime prediction percent error in milli-percent across completed GPU observations"
    )
    .expect("metric can be created");
    pub static ref SHADOW_JOB_OBSERVATION_MAX_RUNTIME_PREDICTION_ERROR_SECONDS: IntGauge = IntGauge::new(
        "ksolver_shadow_job_observation_max_runtime_prediction_error_seconds",
        "Maximum absolute runtime prediction error in seconds across completed GPU observations"
    )
    .expect("metric can be created");
    pub static ref SHADOW_JOB_OBSERVATION_VRAM_PREDICTION_SAMPLES: IntGauge = IntGauge::new(
        "ksolver_shadow_job_observation_vram_prediction_samples",
        "Completed GPU pod observations with both predicted and actual peak VRAM in the latest cluster snapshot"
    )
    .expect("metric can be created");
    pub static ref SHADOW_JOB_OBSERVATION_VRAM_PREDICTION_MAPE_MILLI: IntGauge = IntGauge::new(
        "ksolver_shadow_job_observation_vram_prediction_mape_milli",
        "Mean absolute VRAM prediction percent error in milli-percent across completed GPU observations"
    )
    .expect("metric can be created");
    pub static ref SHADOW_JOB_OBSERVATION_MAX_VRAM_PREDICTION_ERROR_BYTES: IntGauge = IntGauge::new(
        "ksolver_shadow_job_observation_max_vram_prediction_error_bytes",
        "Maximum absolute peak VRAM prediction error in bytes across completed GPU observations"
    )
    .expect("metric can be created");
    pub static ref SHADOW_PREDICTION_AUDIT_PENDING_PODS: IntGauge = IntGauge::new(
        "ksolver_shadow_prediction_audit_pending_pods",
        "Pending GPU pods included in the latest prediction audit"
    )
    .expect("metric can be created");
    pub static ref SHADOW_PREDICTION_AUDIT_FINGERPRINT_MATCHED_PODS: IntGauge = IntGauge::new(
        "ksolver_shadow_prediction_audit_fingerprint_matched_pods",
        "Pending GPU pods matched to a command fingerprint in the latest prediction audit"
    )
    .expect("metric can be created");
    pub static ref SHADOW_PREDICTION_AUDIT_HISTORY_EXACT_PODS: IntGauge = IntGauge::new(
        "ksolver_shadow_prediction_audit_history_exact_pods",
        "Pending GPU pods with exact command-hash and GPU-count history in the latest prediction audit"
    )
    .expect("metric can be created");
    pub static ref SHADOW_PREDICTION_AUDIT_HISTORY_SCALED_PODS: IntGauge = IntGauge::new(
        "ksolver_shadow_prediction_audit_history_scaled_pods",
        "Pending GPU pods with command-hash history scaled across GPU counts in the latest prediction audit"
    )
    .expect("metric can be created");
    pub static ref SHADOW_PREDICTION_AUDIT_HISTORY_SEGMENT_PODS: IntGauge = IntGauge::new(
        "ksolver_shadow_prediction_audit_history_segment_pods",
        "Pending GPU pods whose prediction came from job-type or framework history in the latest prediction audit"
    )
    .expect("metric can be created");
    pub static ref SHADOW_PREDICTION_AUDIT_HINT_PODS: IntGauge = IntGauge::new(
        "ksolver_shadow_prediction_audit_hint_pods",
        "Pending GPU pods whose prediction fallback came from training hints in the latest prediction audit"
    )
    .expect("metric can be created");
    pub static ref SHADOW_PREDICTION_AUDIT_UNKNOWN_PODS: IntGauge = IntGauge::new(
        "ksolver_shadow_prediction_audit_unknown_pods",
        "Pending GPU pods with no historical or hint prediction signal in the latest prediction audit"
    )
    .expect("metric can be created");
    pub static ref SHADOW_PREDICTION_AUDIT_PREDICTED_RUNTIME_PODS: IntGauge = IntGauge::new(
        "ksolver_shadow_prediction_audit_predicted_runtime_pods",
        "Pending GPU pods with a runtime prediction in the latest prediction audit"
    )
    .expect("metric can be created");
    pub static ref SHADOW_PREDICTION_AUDIT_PREDICTED_VRAM_PODS: IntGauge = IntGauge::new(
        "ksolver_shadow_prediction_audit_predicted_vram_pods",
        "Pending GPU pods with a peak VRAM prediction in the latest prediction audit"
    )
    .expect("metric can be created");
    pub static ref SHADOW_PREDICTION_AUDIT_AVERAGE_CONFIDENCE: IntGauge = IntGauge::new(
        "ksolver_shadow_prediction_audit_average_confidence",
        "Average 0-100 prediction confidence across pending GPU pods in the latest prediction audit"
    )
    .expect("metric can be created");
}

lazy_static! {
    pub static ref SHADOW_CANDIDATE_NODE_LIMIT: IntGauge = IntGauge::new(
        "ksolver_shadow_candidate_node_limit",
        "Candidate-node limit used for the latest shadow solve; 0 means unpruned full feasible set"
    )
    .expect("metric can be created");
    pub static ref SHADOW_CANDIDATE_EDGES_UNPRUNED: IntGauge = IntGauge::new(
        "ksolver_shadow_candidate_edges_unpruned",
        "Feasible workload-node assignment edges before candidate-node pruning in the latest shadow solve"
    )
    .expect("metric can be created");
    pub static ref SHADOW_CANDIDATE_EDGES_INITIAL: IntGauge = IntGauge::new(
        "ksolver_shadow_candidate_edges_initial",
        "Physical workload-node assignment edges submitted to the initial shadow solve attempt"
    )
    .expect("metric can be created");
    pub static ref SHADOW_CANDIDATE_EDGES_FINAL: IntGauge = IntGauge::new(
        "ksolver_shadow_candidate_edges_final",
        "Physical workload-node assignment edges represented by the final accepted shadow solve"
    )
    .expect("metric can be created");
    pub static ref SHADOW_CANDIDATE_PRUNED_WORKLOADS: IntGauge = IntGauge::new(
        "ksolver_shadow_candidate_pruned_workloads",
        "Workloads whose feasible candidate node set was pruned in the latest shadow solve"
    )
    .expect("metric can be created");
    pub static ref SHADOW_CANDIDATE_WIDENING_RETRIES: IntGauge = IntGauge::new(
        "ksolver_shadow_candidate_widening_retries",
        "Candidate-widening retries used by the latest shadow solve"
    )
    .expect("metric can be created");
    pub static ref SHADOW_CANDIDATE_WIDENING_ATTEMPTS: IntCounter = IntCounter::new(
        "ksolver_shadow_candidate_widening_attempts_total",
        "Candidate-widening retry attempts across shadow solves"
    )
    .expect("metric can be created");
    pub static ref SHADOW_CANDIDATE_PRUNING_ACTIVE: IntGauge = IntGauge::new(
        "ksolver_shadow_candidate_pruning_active",
        "Whether candidate pruning remained active in the latest accepted shadow solve"
    )
    .expect("metric can be created");
    pub static ref SHADOW_CANDIDATE_WIDENED: IntGauge = IntGauge::new(
        "ksolver_shadow_candidate_widened",
        "Whether the latest accepted shadow solve used candidate-widening retries"
    )
    .expect("metric can be created");
    pub static ref SHADOW_CANDIDATE_EDGE_REDUCTION_MILLI: IntGauge = IntGauge::new(
        "ksolver_shadow_candidate_edge_reduction_milli",
        "Candidate-edge reduction from the unpruned feasible graph in milli-percent for the latest accepted shadow solve"
    )
    .expect("metric can be created");
    pub static ref SHADOW_CANDIDATE_REGRET_STATUS: IntGaugeVec = IntGaugeVec::new(
        Opts::new(
            "ksolver_shadow_candidate_regret_status",
            "One-hot conservative regret status for candidate pruning in the latest accepted shadow solve"
        ),
        &["status"]
    )
    .expect("metric can be created");
    pub static ref SHADOW_NODE_GROUPING_ENABLED: IntGauge = IntGauge::new(
        "ksolver_shadow_node_grouping_enabled",
        "Whether node grouping was enabled for the latest shadow solve"
    )
    .expect("metric can be created");
    pub static ref SHADOW_NODE_GROUPING_USED: IntGauge = IntGauge::new(
        "ksolver_shadow_node_grouping_used",
        "Whether node grouping was used by the latest accepted shadow solve"
    )
    .expect("metric can be created");
    pub static ref SHADOW_NODE_GROUPING_ELIGIBLE_GROUPS: IntGauge = IntGauge::new(
        "ksolver_shadow_node_grouping_eligible_groups",
        "Homogeneous node groups eligible for grouping in the latest shadow solve"
    )
    .expect("metric can be created");
    pub static ref SHADOW_NODE_GROUPING_ELIGIBLE_NODES: IntGauge = IntGauge::new(
        "ksolver_shadow_node_grouping_eligible_nodes",
        "Physical nodes covered by eligible homogeneous node groups in the latest shadow solve"
    )
    .expect("metric can be created");
    pub static ref SHADOW_NODE_GROUPING_MAX_GROUP_SIZE: IntGauge = IntGauge::new(
        "ksolver_shadow_node_grouping_max_group_size",
        "Largest homogeneous node group size in the latest shadow solve"
    )
    .expect("metric can be created");
    pub static ref SHADOW_NODE_GROUPING_GROUPED_NODES: IntGauge = IntGauge::new(
        "ksolver_shadow_node_grouping_grouped_nodes",
        "Node count in the grouped optimization model for the latest shadow solve"
    )
    .expect("metric can be created");
    pub static ref SHADOW_NODE_GROUPING_GROUPED_CANDIDATE_EDGES: IntGauge = IntGauge::new(
        "ksolver_shadow_node_grouping_grouped_candidate_edges",
        "Candidate edges in the grouped optimization model for the latest shadow solve"
    )
    .expect("metric can be created");
    pub static ref SHADOW_NODE_GROUPING_USED_TOTAL: IntCounter = IntCounter::new(
        "ksolver_shadow_node_grouping_used_total",
        "Shadow solves whose accepted solution used node grouping"
    )
    .expect("metric can be created");
    pub static ref SHADOW_NODE_GROUPING_FALLBACK_TOTAL: IntCounter = IntCounter::new(
        "ksolver_shadow_node_grouping_fallback_total",
        "Shadow solves that fell back from grouped solving to the physical-node model"
    )
    .expect("metric can be created");
    pub static ref SHADOW_SOLVES: IntCounter =
        IntCounter::new("ksolver_shadow_solves_total", "Shadow solves started")
            .expect("metric can be created");
    pub static ref SHADOW_SOLVE_ERRORS: IntCounter = IntCounter::new(
        "ksolver_shadow_solve_errors_total",
        "Shadow solves that errored"
    )
    .expect("metric can be created");
    pub static ref SHADOW_SOLVE_SECONDS: Histogram = Histogram::with_opts(HistogramOpts::new(
        "ksolver_shadow_solve_seconds",
        "Shadow solve wall-clock seconds"
    ))
    .expect("metric can be created");
    pub static ref SHADOW_UNPLACED: IntCounter = IntCounter::new(
        "ksolver_shadow_unplaced_total",
        "Pending GPU pods with no placement in a solve"
    )
    .expect("metric can be created");
    pub static ref SHADOW_VRAM_BLOCKED: IntCounter = IntCounter::new(
        "ksolver_shadow_vram_blocked_total",
        "Pending GPU pods unplaced because predicted peak VRAM exceeds known node GPU memory"
    )
    .expect("metric can be created");
    pub static ref SHADOW_HIGH_PRIORITY_UNPLACED: IntCounter = IntCounter::new(
        "ksolver_shadow_high_priority_unplaced_total",
        "Positive-priority pending GPU pods with no placement in a solve"
    )
    .expect("metric can be created");
    pub static ref SHADOW_PREDICTED_DEADLINE_MISSES: IntCounter = IntCounter::new(
        "ksolver_shadow_predicted_deadline_misses_total",
        "Deadline-aware pending GPU pods predicted to miss their requested deadline across shadow solves"
    )
    .expect("metric can be created");
    pub static ref SHADOW_REPAIR_PLANS_CURRENT: IntGauge = IntGauge::new(
        "ksolver_shadow_repair_plans",
        "Current count of dry-run repair plans proposed in the latest shadow solve"
    )
    .expect("metric can be created");
    pub static ref SHADOW_REPAIR_MIGRATIONS_CURRENT: IntGauge = IntGauge::new(
        "ksolver_shadow_repair_migrations",
        "Current count of dry-run repair actions that would migrate a running GPU pod in the latest shadow solve"
    )
    .expect("metric can be created");
    pub static ref SHADOW_REPAIR_PREEMPTIONS_CURRENT: IntGauge = IntGauge::new(
        "ksolver_shadow_repair_preemptions",
        "Current count of dry-run repair actions that would preempt a running GPU pod in the latest shadow solve"
    )
    .expect("metric can be created");
    pub static ref SHADOW_REPAIR_DISRUPTION_COST_CURRENT: IntGauge = IntGauge::new(
        "ksolver_shadow_repair_disruption_cost",
        "Current dry-run repair disruption cost across proposed repair plans in the latest shadow solve"
    )
    .expect("metric can be created");
    pub static ref SHADOW_REPAIR_REPAIRABLE_TARGETS: IntGauge = IntGauge::new(
        "ksolver_shadow_repair_repairable_targets",
        "Current count of unplaced GPU targets that have a dry-run repair plan in the latest shadow solve"
    )
    .expect("metric can be created");
    pub static ref SHADOW_REPAIR_UNREPAIRABLE_TARGETS: IntGauge = IntGauge::new(
        "ksolver_shadow_repair_unrepairable_targets",
        "Current count of unplaced GPU targets with no dry-run repair plan in the latest shadow solve"
    )
    .expect("metric can be created");
    pub static ref SHADOW_REPAIR_VRAM_BLOCKED_TARGETS: IntGauge = IntGauge::new(
        "ksolver_shadow_repair_vram_blocked_targets",
        "Current count of unplaced GPU targets where repair will not help because predicted peak VRAM exceeds known GPU memory"
    )
    .expect("metric can be created");
    pub static ref SHADOW_REPAIR_NOT_ENOUGH_TOTAL_GPU_TARGETS: IntGauge = IntGauge::new(
        "ksolver_shadow_repair_not_enough_total_gpu_targets",
        "Current count of unplaced GPU targets that no node can fit even before considering fragmentation"
    )
    .expect("metric can be created");
    pub static ref SHADOW_REPAIR_POLICY_OR_CANDIDATE_BLOCKED_TARGETS: IntGauge = IntGauge::new(
        "ksolver_shadow_repair_policy_or_candidate_blocked_targets",
        "Current count of unplaced GPU targets with enough total node capacity but no repair plan within policy and candidate budget"
    )
    .expect("metric can be created");
    pub static ref SHADOW_REPAIR_INCOMPLETE_MODEL_TARGETS: IntGauge = IntGauge::new(
        "ksolver_shadow_repair_incomplete_model_targets",
        "Current count of unplaced GPU targets where dry-run repair advice is withheld because normalized pending workload model data is incomplete"
    )
    .expect("metric can be created");
    pub static ref SHADOW_REPAIR_SKIPPED_CANDIDATES: IntGauge = IntGauge::new(
        "ksolver_shadow_repair_skipped_candidates",
        "Current count of running GPU repair candidates skipped by policy, PDB, priority, or candidate budget in the latest shadow solve"
    )
    .expect("metric can be created");
    pub static ref SHADOW_REPAIR_SKIPPED_CANDIDATES_BY_REASON: IntGaugeVec = IntGaugeVec::new(
        Opts::new(
            "ksolver_shadow_repair_skipped_candidates_by_reason",
            "Current count of running GPU repair candidates skipped in the latest shadow solve by reason bucket",
        ),
        &["reason"],
    )
    .expect("metric can be created");
    pub static ref SHADOW_REPAIR_PLANS: IntCounter = IntCounter::new(
        "ksolver_shadow_repair_plans_total",
        "Dry-run repair plans proposed for unplaced GPU pods"
    )
    .expect("metric can be created");
    pub static ref SHADOW_REPAIR_MIGRATIONS: IntCounter = IntCounter::new(
        "ksolver_shadow_repair_migrations_total",
        "Dry-run repair actions that would migrate a running GPU pod"
    )
    .expect("metric can be created");
    pub static ref SHADOW_REPAIR_PREEMPTIONS: IntCounter = IntCounter::new(
        "ksolver_shadow_repair_preemptions_total",
        "Dry-run repair actions that would preempt a running GPU pod"
    )
    .expect("metric can be created");
    pub static ref SHADOW_REPAIR_DISRUPTION_COST: IntCounter = IntCounter::new(
        "ksolver_shadow_repair_disruption_cost_total",
        "Sum of dry-run repair disruption cost across proposed repair plans"
    )
    .expect("metric can be created");
    pub static ref SHADOW_CAVEATED: IntCounter = IntCounter::new(
        "ksolver_shadow_caveated_total",
        "Placed shadow decisions carrying an unmodeled-constraint caveat"
    )
    .expect("metric can be created");
    pub static ref SHADOW_BOUND: IntCounter = IntCounter::new(
        "ksolver_shadow_bound_total",
        "Pods actually bound to a node by the real-binding executor (Phase 3)"
    )
    .expect("metric can be created");
    pub static ref SHADOW_BIND_SKIPPED: IntCounter = IntCounter::new(
        "ksolver_shadow_bind_skipped_total",
        "Real-binding candidates skipped (not ready / stale / already bound / throttled)"
    )
    .expect("metric can be created");
    pub static ref SHADOW_BIND_CANARY_SKIPPED: IntCounter = IntCounter::new(
        "ksolver_shadow_bind_canary_skipped_total",
        "Real-binding candidates skipped by binding canary rollout policy"
    )
    .expect("metric can be created");
    pub static ref SHADOW_BIND_FAILED: IntCounter = IntCounter::new(
        "ksolver_shadow_bind_failed_total",
        "Real-binding attempts that failed against the API server"
    )
    .expect("metric can be created");
    pub static ref SHADOW_BIND_RESERVATIONS: IntGauge = IntGauge::new(
        "ksolver_shadow_bind_reservations",
        "Current active binding reservations held by the in-memory reservation ledger"
    )
    .expect("metric can be created");
    pub static ref SHADOW_BIND_RESERVED_ENTRIES: IntGauge = IntGauge::new(
        "ksolver_shadow_bind_reserved_entries",
        "Current pod binding entries held by the in-memory reservation ledger"
    )
    .expect("metric can be created");
    pub static ref SHADOW_BIND_RESERVED_GPUS: IntGauge = IntGauge::new(
        "ksolver_shadow_bind_reserved_gpus",
        "Current GPU units held by the in-memory reservation ledger"
    )
    .expect("metric can be created");
    pub static ref SHADOW_BIND_RESERVATION_CREATED: IntCounter = IntCounter::new(
        "ksolver_shadow_bind_reservation_created_total",
        "Binding reservations successfully created before real-binding attempts"
    )
    .expect("metric can be created");
    pub static ref SHADOW_BIND_RESERVATION_REJECTED: IntCounter = IntCounter::new(
        "ksolver_shadow_bind_reservation_rejected_total",
        "Binding plans rejected by the reservation ledger before any bind attempt"
    )
    .expect("metric can be created");
    pub static ref SHADOW_BIND_RESERVATION_EXPIRED: IntCounter = IntCounter::new(
        "ksolver_shadow_bind_reservation_expired_total",
        "Binding reservations released by TTL expiry"
    )
    .expect("metric can be created");
    pub static ref SHADOW_BIND_RESERVATION_OBSERVED: IntCounter = IntCounter::new(
        "ksolver_shadow_bind_reservation_observed_total",
        "Reserved binding entries released because the expected pod was observed bound"
    )
    .expect("metric can be created");
    pub static ref SHADOW_BIND_RESERVATION_STALE: IntCounter = IntCounter::new(
        "ksolver_shadow_bind_reservation_stale_total",
        "Reserved binding entries released because the pod disappeared, changed uid, or bound elsewhere"
    )
    .expect("metric can be created");
    pub static ref SHADOW_KUBERNETES_EVENTS: IntCounterVec = IntCounterVec::new(
        Opts::new(
            "ksolver_shadow_kubernetes_events_total",
            "Optional Kubernetes Event writes by event type and outcome"
        ),
        &["event_type", "outcome"]
    )
    .expect("metric can be created");
    pub static ref SHADOW_LEADER: IntGauge = IntGauge::new(
        "ksolver_shadow_leader",
        "Whether this scheduler replica currently holds the leader-election Lease (1=yes, 0=no)"
    )
    .expect("metric can be created");
    pub static ref SHADOW_LEADER_ACQUIRED: IntCounter = IntCounter::new(
        "ksolver_shadow_leader_acquired_total",
        "Leader-election Lease acquisitions by this scheduler replica"
    )
    .expect("metric can be created");
    pub static ref SHADOW_LEADER_RENEWED: IntCounter = IntCounter::new(
        "ksolver_shadow_leader_renewed_total",
        "Leader-election Lease renewals by this scheduler replica"
    )
    .expect("metric can be created");
    pub static ref SHADOW_LEADER_WAIT: IntCounter = IntCounter::new(
        "ksolver_shadow_leader_wait_total",
        "Leader-election renewal loops where another replica held an unexpired Lease"
    )
    .expect("metric can be created");
    pub static ref SHADOW_LEADER_RENEW_ERRORS: IntCounter = IntCounter::new(
        "ksolver_shadow_leader_renew_errors_total",
        "Leader-election renewal errors that caused this replica to fail closed as non-leader"
    )
    .expect("metric can be created");
    pub static ref SHADOW_LEADER_SKIPPED_SOLVES: IntCounter = IntCounter::new(
        "ksolver_shadow_leader_skipped_solves_total",
        "Shadow solve passes skipped because this replica was not the leader"
    )
    .expect("metric can be created");
}

lazy_static! {
    pub static ref SHADOW_BIND_SKIPPED_BY_REASON: IntGaugeVec = IntGaugeVec::new(
        Opts::new(
            "ksolver_shadow_bind_skipped_by_reason",
            "Current count of real-binding candidates skipped in the latest binding pass by reason bucket"
        ),
        &["reason"],
    )
    .expect("metric can be created");
}

fn register_ignoring_dup(c: Box<dyn prometheus::core::Collector>) {
    match REGISTRY.register(c) {
        Ok(()) => {}
        Err(prometheus::Error::AlreadyReg) => {}
        Err(e) => panic!("failed to register metric: {e}"),
    }
}

pub fn register_metrics() {
    register_ignoring_dup(Box::new(SOLVE_DURATION_SECONDS.clone()));
    register_ignoring_dup(Box::new(SOLVES_TOTAL.clone()));
    register_ignoring_dup(Box::new(SOLVES_IN_FLIGHT.clone()));
    register_ignoring_dup(Box::new(SHADOW_POD_OBSERVATIONS.clone()));
    register_ignoring_dup(Box::new(SHADOW_PENDING.clone()));
    register_ignoring_dup(Box::new(SHADOW_ADMITTED_PODS.clone()));
    register_ignoring_dup(Box::new(SHADOW_ADMITTED_GPU_DEMAND.clone()));
    register_ignoring_dup(Box::new(SHADOW_REQUESTED_GPU_DEMAND.clone()));
    register_ignoring_dup(Box::new(SHADOW_UNPLACED_GPU_DEMAND.clone()));
    register_ignoring_dup(Box::new(SHADOW_POD_ADMISSION_PERCENT_MILLI.clone()));
    register_ignoring_dup(Box::new(SHADOW_GPU_ADMISSION_PERCENT_MILLI.clone()));
    register_ignoring_dup(Box::new(SHADOW_ADMITTED_MONTHLY_COST_MILLI.clone()));
    register_ignoring_dup(Box::new(SHADOW_MAX_QUEUE_WAIT_SECONDS.clone()));
    register_ignoring_dup(Box::new(
        SHADOW_HIGH_PRIORITY_MAX_QUEUE_WAIT_SECONDS.clone(),
    ));
    register_ignoring_dup(Box::new(SHADOW_DEADLINE_JOBS.clone()));
    register_ignoring_dup(Box::new(SHADOW_UNPLACED_DEADLINE_JOBS.clone()));
    register_ignoring_dup(Box::new(SHADOW_PREDICTED_DEADLINE_MISSES_CURRENT.clone()));
    register_ignoring_dup(Box::new(
        SHADOW_PLACED_PREDICTED_DEADLINE_MISSES_CURRENT.clone(),
    ));
    register_ignoring_dup(Box::new(
        SHADOW_UNPLACED_PREDICTED_DEADLINE_MISSES_CURRENT.clone(),
    ));
    register_ignoring_dup(Box::new(SHADOW_WORST_DEADLINE_SLACK_SECONDS.clone()));
    register_ignoring_dup(Box::new(SHADOW_UNPLACED_CURRENT.clone()));
    register_ignoring_dup(Box::new(SHADOW_VRAM_BLOCKED_CURRENT.clone()));
    register_ignoring_dup(Box::new(SHADOW_HIGH_PRIORITY_UNPLACED_CURRENT.clone()));
    register_ignoring_dup(Box::new(SHADOW_QUOTA_THROTTLED_PODS.clone()));
    register_ignoring_dup(Box::new(
        SHADOW_QUOTA_THROTTLED_MAX_QUEUE_WAIT_SECONDS.clone(),
    ));
    register_ignoring_dup(Box::new(SHADOW_FAIRNESS_UNDER_SHARE_TENANTS.clone()));
    register_ignoring_dup(Box::new(SHADOW_FAIRNESS_OVER_SHARE_TENANTS.clone()));
    register_ignoring_dup(Box::new(SHADOW_FAIRNESS_BORROWED_GPU_MILLI.clone()));
    register_ignoring_dup(Box::new(
        SHADOW_FAIRNESS_RECLAIMABLE_BORROWED_GPU_MILLI.clone(),
    ));
    register_ignoring_dup(Box::new(SHADOW_BUDGET_OVER_TENANTS.clone()));
    register_ignoring_dup(Box::new(SHADOW_BUDGET_OVERAGE_MONTHLY_MILLI.clone()));
    register_ignoring_dup(Box::new(SHADOW_ACTIVE_GPU_NODES.clone()));
    register_ignoring_dup(Box::new(SHADOW_STRANDED_GPU_ON_ACTIVE_NODES.clone()));
    register_ignoring_dup(Box::new(SHADOW_JOB_OBSERVATION_COMPLETED_GPU_PODS.clone()));
    register_ignoring_dup(Box::new(SHADOW_JOB_OBSERVATION_RUNTIME_SAMPLES.clone()));
    register_ignoring_dup(Box::new(SHADOW_JOB_OBSERVATION_FAILED_GPU_PODS.clone()));
    register_ignoring_dup(Box::new(SHADOW_JOB_OBSERVATION_MAX_RUNTIME_SECONDS.clone()));
    register_ignoring_dup(Box::new(
        SHADOW_JOB_OBSERVATION_MAX_PEAK_MEMORY_BYTES.clone(),
    ));
    register_ignoring_dup(Box::new(
        SHADOW_JOB_OBSERVATION_UNIQUE_COMMAND_HASHES.clone(),
    ));
    register_ignoring_dup(Box::new(
        SHADOW_JOB_OBSERVATION_RUNTIME_PREDICTION_SAMPLES.clone(),
    ));
    register_ignoring_dup(Box::new(
        SHADOW_JOB_OBSERVATION_RUNTIME_PREDICTION_MAPE_MILLI.clone(),
    ));
    register_ignoring_dup(Box::new(
        SHADOW_JOB_OBSERVATION_MAX_RUNTIME_PREDICTION_ERROR_SECONDS.clone(),
    ));
    register_ignoring_dup(Box::new(
        SHADOW_JOB_OBSERVATION_VRAM_PREDICTION_SAMPLES.clone(),
    ));
    register_ignoring_dup(Box::new(
        SHADOW_JOB_OBSERVATION_VRAM_PREDICTION_MAPE_MILLI.clone(),
    ));
    register_ignoring_dup(Box::new(
        SHADOW_JOB_OBSERVATION_MAX_VRAM_PREDICTION_ERROR_BYTES.clone(),
    ));
    register_ignoring_dup(Box::new(SHADOW_PREDICTION_AUDIT_PENDING_PODS.clone()));
    register_ignoring_dup(Box::new(
        SHADOW_PREDICTION_AUDIT_FINGERPRINT_MATCHED_PODS.clone(),
    ));
    register_ignoring_dup(Box::new(SHADOW_PREDICTION_AUDIT_HISTORY_EXACT_PODS.clone()));
    register_ignoring_dup(Box::new(
        SHADOW_PREDICTION_AUDIT_HISTORY_SCALED_PODS.clone(),
    ));
    register_ignoring_dup(Box::new(
        SHADOW_PREDICTION_AUDIT_HISTORY_SEGMENT_PODS.clone(),
    ));
    register_ignoring_dup(Box::new(SHADOW_PREDICTION_AUDIT_HINT_PODS.clone()));
    register_ignoring_dup(Box::new(SHADOW_PREDICTION_AUDIT_UNKNOWN_PODS.clone()));
    register_ignoring_dup(Box::new(
        SHADOW_PREDICTION_AUDIT_PREDICTED_RUNTIME_PODS.clone(),
    ));
    register_ignoring_dup(Box::new(
        SHADOW_PREDICTION_AUDIT_PREDICTED_VRAM_PODS.clone(),
    ));
    register_ignoring_dup(Box::new(SHADOW_PREDICTION_AUDIT_AVERAGE_CONFIDENCE.clone()));
    register_ignoring_dup(Box::new(SHADOW_CANDIDATE_NODE_LIMIT.clone()));
    register_ignoring_dup(Box::new(SHADOW_CANDIDATE_EDGES_UNPRUNED.clone()));
    register_ignoring_dup(Box::new(SHADOW_CANDIDATE_EDGES_INITIAL.clone()));
    register_ignoring_dup(Box::new(SHADOW_CANDIDATE_EDGES_FINAL.clone()));
    register_ignoring_dup(Box::new(SHADOW_CANDIDATE_PRUNED_WORKLOADS.clone()));
    register_ignoring_dup(Box::new(SHADOW_CANDIDATE_WIDENING_RETRIES.clone()));
    register_ignoring_dup(Box::new(SHADOW_CANDIDATE_WIDENING_ATTEMPTS.clone()));
    register_ignoring_dup(Box::new(SHADOW_CANDIDATE_PRUNING_ACTIVE.clone()));
    register_ignoring_dup(Box::new(SHADOW_CANDIDATE_WIDENED.clone()));
    register_ignoring_dup(Box::new(SHADOW_CANDIDATE_EDGE_REDUCTION_MILLI.clone()));
    register_ignoring_dup(Box::new(SHADOW_CANDIDATE_REGRET_STATUS.clone()));
    register_ignoring_dup(Box::new(SHADOW_NODE_GROUPING_ENABLED.clone()));
    register_ignoring_dup(Box::new(SHADOW_NODE_GROUPING_USED.clone()));
    register_ignoring_dup(Box::new(SHADOW_NODE_GROUPING_ELIGIBLE_GROUPS.clone()));
    register_ignoring_dup(Box::new(SHADOW_NODE_GROUPING_ELIGIBLE_NODES.clone()));
    register_ignoring_dup(Box::new(SHADOW_NODE_GROUPING_MAX_GROUP_SIZE.clone()));
    register_ignoring_dup(Box::new(SHADOW_NODE_GROUPING_GROUPED_NODES.clone()));
    register_ignoring_dup(Box::new(
        SHADOW_NODE_GROUPING_GROUPED_CANDIDATE_EDGES.clone(),
    ));
    register_ignoring_dup(Box::new(SHADOW_NODE_GROUPING_USED_TOTAL.clone()));
    register_ignoring_dup(Box::new(SHADOW_NODE_GROUPING_FALLBACK_TOTAL.clone()));
    register_ignoring_dup(Box::new(SHADOW_SOLVES.clone()));
    register_ignoring_dup(Box::new(SHADOW_SOLVE_ERRORS.clone()));
    register_ignoring_dup(Box::new(SHADOW_SOLVE_SECONDS.clone()));
    register_ignoring_dup(Box::new(SHADOW_UNPLACED.clone()));
    register_ignoring_dup(Box::new(SHADOW_VRAM_BLOCKED.clone()));
    register_ignoring_dup(Box::new(SHADOW_HIGH_PRIORITY_UNPLACED.clone()));
    register_ignoring_dup(Box::new(SHADOW_PREDICTED_DEADLINE_MISSES.clone()));
    register_ignoring_dup(Box::new(SHADOW_REPAIR_PLANS_CURRENT.clone()));
    register_ignoring_dup(Box::new(SHADOW_REPAIR_MIGRATIONS_CURRENT.clone()));
    register_ignoring_dup(Box::new(SHADOW_REPAIR_PREEMPTIONS_CURRENT.clone()));
    register_ignoring_dup(Box::new(SHADOW_REPAIR_DISRUPTION_COST_CURRENT.clone()));
    register_ignoring_dup(Box::new(SHADOW_REPAIR_REPAIRABLE_TARGETS.clone()));
    register_ignoring_dup(Box::new(SHADOW_REPAIR_UNREPAIRABLE_TARGETS.clone()));
    register_ignoring_dup(Box::new(SHADOW_REPAIR_VRAM_BLOCKED_TARGETS.clone()));
    register_ignoring_dup(Box::new(SHADOW_REPAIR_NOT_ENOUGH_TOTAL_GPU_TARGETS.clone()));
    register_ignoring_dup(Box::new(
        SHADOW_REPAIR_POLICY_OR_CANDIDATE_BLOCKED_TARGETS.clone(),
    ));
    register_ignoring_dup(Box::new(SHADOW_REPAIR_INCOMPLETE_MODEL_TARGETS.clone()));
    register_ignoring_dup(Box::new(SHADOW_REPAIR_SKIPPED_CANDIDATES.clone()));
    register_ignoring_dup(Box::new(SHADOW_REPAIR_SKIPPED_CANDIDATES_BY_REASON.clone()));
    register_ignoring_dup(Box::new(SHADOW_REPAIR_PLANS.clone()));
    register_ignoring_dup(Box::new(SHADOW_REPAIR_MIGRATIONS.clone()));
    register_ignoring_dup(Box::new(SHADOW_REPAIR_PREEMPTIONS.clone()));
    register_ignoring_dup(Box::new(SHADOW_REPAIR_DISRUPTION_COST.clone()));
    register_ignoring_dup(Box::new(SHADOW_CAVEATED.clone()));
    register_ignoring_dup(Box::new(SHADOW_BOUND.clone()));
    register_ignoring_dup(Box::new(SHADOW_BIND_SKIPPED.clone()));
    register_ignoring_dup(Box::new(SHADOW_BIND_CANARY_SKIPPED.clone()));
    register_ignoring_dup(Box::new(SHADOW_BIND_SKIPPED_BY_REASON.clone()));
    register_ignoring_dup(Box::new(SHADOW_BIND_FAILED.clone()));
    register_ignoring_dup(Box::new(SHADOW_BIND_RESERVATIONS.clone()));
    register_ignoring_dup(Box::new(SHADOW_BIND_RESERVED_ENTRIES.clone()));
    register_ignoring_dup(Box::new(SHADOW_BIND_RESERVED_GPUS.clone()));
    register_ignoring_dup(Box::new(SHADOW_BIND_RESERVATION_CREATED.clone()));
    register_ignoring_dup(Box::new(SHADOW_BIND_RESERVATION_REJECTED.clone()));
    register_ignoring_dup(Box::new(SHADOW_BIND_RESERVATION_EXPIRED.clone()));
    register_ignoring_dup(Box::new(SHADOW_BIND_RESERVATION_OBSERVED.clone()));
    register_ignoring_dup(Box::new(SHADOW_BIND_RESERVATION_STALE.clone()));
    register_ignoring_dup(Box::new(SHADOW_KUBERNETES_EVENTS.clone()));
    register_ignoring_dup(Box::new(SHADOW_LEADER.clone()));
    register_ignoring_dup(Box::new(SHADOW_LEADER_ACQUIRED.clone()));
    register_ignoring_dup(Box::new(SHADOW_LEADER_RENEWED.clone()));
    register_ignoring_dup(Box::new(SHADOW_LEADER_WAIT.clone()));
    register_ignoring_dup(Box::new(SHADOW_LEADER_RENEW_ERRORS.clone()));
    register_ignoring_dup(Box::new(SHADOW_LEADER_SKIPPED_SOLVES.clone()));
}

pub fn inc_shadow_bound(n: u64) {
    SHADOW_BOUND.inc_by(n);
}

pub fn inc_shadow_bind_skipped(n: u64) {
    SHADOW_BIND_SKIPPED.inc_by(n);
}

pub fn inc_shadow_bind_canary_skipped(n: u64) {
    SHADOW_BIND_CANARY_SKIPPED.inc_by(n);
}

#[allow(clippy::too_many_arguments)]
pub fn set_shadow_bind_skipped_by_reason(
    canary: i64,
    readiness: i64,
    identity: i64,
    scheduler: i64,
    already_bound: i64,
    dra: i64,
    throttle: i64,
    reservation: i64,
    disabled: i64,
    group: i64,
    other: i64,
) {
    for (reason, value) in [
        ("canary", canary),
        ("readiness", readiness),
        ("identity", identity),
        ("scheduler", scheduler),
        ("already_bound", already_bound),
        ("dra", dra),
        ("throttle", throttle),
        ("reservation", reservation),
        ("disabled", disabled),
        ("group", group),
        ("other", other),
    ] {
        SHADOW_BIND_SKIPPED_BY_REASON
            .with_label_values(&[reason])
            .set(value);
    }
}

pub fn inc_shadow_bind_failed(n: u64) {
    SHADOW_BIND_FAILED.inc_by(n);
}

pub fn set_shadow_bind_reservation_state(
    active_reservations: i64,
    active_entries: i64,
    reserved_gpus: i64,
) {
    SHADOW_BIND_RESERVATIONS.set(active_reservations);
    SHADOW_BIND_RESERVED_ENTRIES.set(active_entries);
    SHADOW_BIND_RESERVED_GPUS.set(reserved_gpus);
}

pub fn inc_shadow_bind_reservation_created(n: u64) {
    SHADOW_BIND_RESERVATION_CREATED.inc_by(n);
}

pub fn inc_shadow_bind_reservation_rejected(n: u64) {
    SHADOW_BIND_RESERVATION_REJECTED.inc_by(n);
}

pub fn inc_shadow_bind_reservation_expired(n: u64) {
    SHADOW_BIND_RESERVATION_EXPIRED.inc_by(n);
}

pub fn inc_shadow_bind_reservation_observed(n: u64) {
    SHADOW_BIND_RESERVATION_OBSERVED.inc_by(n);
}

pub fn inc_shadow_bind_reservation_stale(n: u64) {
    SHADOW_BIND_RESERVATION_STALE.inc_by(n);
}

pub fn inc_shadow_kubernetes_events(
    event_type: &'static str,
    attempted: u64,
    created: u64,
    failed: u64,
) {
    SHADOW_KUBERNETES_EVENTS
        .with_label_values(&[event_type, "attempted"])
        .inc_by(attempted);
    SHADOW_KUBERNETES_EVENTS
        .with_label_values(&[event_type, "created"])
        .inc_by(created);
    SHADOW_KUBERNETES_EVENTS
        .with_label_values(&[event_type, "failed"])
        .inc_by(failed);
}

pub fn set_shadow_leader(is_leader: bool) {
    SHADOW_LEADER.set(if is_leader { 1 } else { 0 });
}

pub fn inc_shadow_leader_acquired() {
    SHADOW_LEADER_ACQUIRED.inc();
}

pub fn inc_shadow_leader_renewed() {
    SHADOW_LEADER_RENEWED.inc();
}

pub fn inc_shadow_leader_wait() {
    SHADOW_LEADER_WAIT.inc();
}

pub fn inc_shadow_leader_renew_errors() {
    SHADOW_LEADER_RENEW_ERRORS.inc();
}

pub fn inc_shadow_leader_skipped_solves() {
    SHADOW_LEADER_SKIPPED_SOLVES.inc();
}

pub fn inc_shadow_pod_observations(n: u64) {
    SHADOW_POD_OBSERVATIONS.inc_by(n);
}

pub fn set_shadow_pending(n: i64) {
    SHADOW_PENDING.set(n);
}

pub fn set_shadow_admission(admitted_pods: i64, admitted_gpu_demand: i64) {
    SHADOW_ADMITTED_PODS.set(admitted_pods);
    SHADOW_ADMITTED_GPU_DEMAND.set(admitted_gpu_demand);
}

pub fn set_shadow_outcome_summary(
    unplaced_pods: i64,
    requested_gpu_demand: i64,
    admitted_gpu_demand: i64,
    unplaced_gpu_demand: i64,
    pod_admission_percent_milli: i64,
    gpu_admission_percent_milli: i64,
    admitted_monthly_cost_milli: i64,
) {
    SHADOW_UNPLACED_CURRENT.set(unplaced_pods);
    SHADOW_REQUESTED_GPU_DEMAND.set(requested_gpu_demand);
    SHADOW_ADMITTED_GPU_DEMAND.set(admitted_gpu_demand);
    SHADOW_UNPLACED_GPU_DEMAND.set(unplaced_gpu_demand);
    SHADOW_POD_ADMISSION_PERCENT_MILLI.set(pod_admission_percent_milli);
    SHADOW_GPU_ADMISSION_PERCENT_MILLI.set(gpu_admission_percent_milli);
    SHADOW_ADMITTED_MONTHLY_COST_MILLI.set(admitted_monthly_cost_milli);
}

pub fn set_shadow_queue_wait(
    max_queue_wait_seconds: i64,
    high_priority_max_queue_wait_seconds: i64,
) {
    SHADOW_MAX_QUEUE_WAIT_SECONDS.set(max_queue_wait_seconds);
    SHADOW_HIGH_PRIORITY_MAX_QUEUE_WAIT_SECONDS.set(high_priority_max_queue_wait_seconds);
}

pub fn set_shadow_deadlines(
    deadline_jobs: i64,
    unplaced_deadline_jobs: i64,
    predicted_deadline_misses: i64,
    placed_predicted_deadline_misses: i64,
    unplaced_predicted_deadline_misses: i64,
    worst_deadline_slack_seconds: i64,
) {
    SHADOW_DEADLINE_JOBS.set(deadline_jobs);
    SHADOW_UNPLACED_DEADLINE_JOBS.set(unplaced_deadline_jobs);
    SHADOW_PREDICTED_DEADLINE_MISSES_CURRENT.set(predicted_deadline_misses);
    SHADOW_PLACED_PREDICTED_DEADLINE_MISSES_CURRENT.set(placed_predicted_deadline_misses);
    SHADOW_UNPLACED_PREDICTED_DEADLINE_MISSES_CURRENT.set(unplaced_predicted_deadline_misses);
    SHADOW_WORST_DEADLINE_SLACK_SECONDS.set(worst_deadline_slack_seconds);
}

pub fn set_shadow_placement_pressure(
    unplaced: i64,
    vram_blocked: i64,
    high_priority_unplaced: i64,
) {
    SHADOW_UNPLACED_CURRENT.set(unplaced);
    SHADOW_VRAM_BLOCKED_CURRENT.set(vram_blocked);
    SHADOW_HIGH_PRIORITY_UNPLACED_CURRENT.set(high_priority_unplaced);
}

pub fn set_shadow_quota_throttle(throttled_pods: i64, throttled_max_queue_wait_seconds: i64) {
    SHADOW_QUOTA_THROTTLED_PODS.set(throttled_pods);
    SHADOW_QUOTA_THROTTLED_MAX_QUEUE_WAIT_SECONDS.set(throttled_max_queue_wait_seconds);
}

pub fn set_shadow_fairness(
    under_share_tenants: i64,
    over_share_tenants: i64,
    borrowed_gpu_milli: i64,
    reclaimable_borrowed_gpu_milli: i64,
) {
    SHADOW_FAIRNESS_UNDER_SHARE_TENANTS.set(under_share_tenants);
    SHADOW_FAIRNESS_OVER_SHARE_TENANTS.set(over_share_tenants);
    SHADOW_FAIRNESS_BORROWED_GPU_MILLI.set(borrowed_gpu_milli);
    SHADOW_FAIRNESS_RECLAIMABLE_BORROWED_GPU_MILLI.set(reclaimable_borrowed_gpu_milli);
}

pub fn set_shadow_budget_pressure(
    budget_over_tenants: i64,
    total_budget_overage_monthly_milli: i64,
) {
    SHADOW_BUDGET_OVER_TENANTS.set(budget_over_tenants);
    SHADOW_BUDGET_OVERAGE_MONTHLY_MILLI.set(total_budget_overage_monthly_milli);
}

pub fn set_shadow_gpu_utilization(active_gpu_nodes: i64, stranded_gpu_on_active_nodes: i64) {
    SHADOW_ACTIVE_GPU_NODES.set(active_gpu_nodes);
    SHADOW_STRANDED_GPU_ON_ACTIVE_NODES.set(stranded_gpu_on_active_nodes);
}

#[allow(clippy::too_many_arguments)]
pub fn set_shadow_job_observations(
    completed_gpu_pods: i64,
    runtime_samples: i64,
    failed_gpu_pods: i64,
    max_runtime_seconds: i64,
    max_peak_memory_bytes: i64,
    unique_command_hashes: i64,
    runtime_prediction_samples: i64,
    runtime_prediction_mape_milli: i64,
    max_runtime_prediction_error_seconds: i64,
    vram_prediction_samples: i64,
    vram_prediction_mape_milli: i64,
    max_vram_prediction_error_bytes: i64,
) {
    SHADOW_JOB_OBSERVATION_COMPLETED_GPU_PODS.set(completed_gpu_pods);
    SHADOW_JOB_OBSERVATION_RUNTIME_SAMPLES.set(runtime_samples);
    SHADOW_JOB_OBSERVATION_FAILED_GPU_PODS.set(failed_gpu_pods);
    SHADOW_JOB_OBSERVATION_MAX_RUNTIME_SECONDS.set(max_runtime_seconds);
    SHADOW_JOB_OBSERVATION_MAX_PEAK_MEMORY_BYTES.set(max_peak_memory_bytes);
    SHADOW_JOB_OBSERVATION_UNIQUE_COMMAND_HASHES.set(unique_command_hashes);
    SHADOW_JOB_OBSERVATION_RUNTIME_PREDICTION_SAMPLES.set(runtime_prediction_samples);
    SHADOW_JOB_OBSERVATION_RUNTIME_PREDICTION_MAPE_MILLI.set(runtime_prediction_mape_milli);
    SHADOW_JOB_OBSERVATION_MAX_RUNTIME_PREDICTION_ERROR_SECONDS
        .set(max_runtime_prediction_error_seconds);
    SHADOW_JOB_OBSERVATION_VRAM_PREDICTION_SAMPLES.set(vram_prediction_samples);
    SHADOW_JOB_OBSERVATION_VRAM_PREDICTION_MAPE_MILLI.set(vram_prediction_mape_milli);
    SHADOW_JOB_OBSERVATION_MAX_VRAM_PREDICTION_ERROR_BYTES.set(max_vram_prediction_error_bytes);
}

#[allow(clippy::too_many_arguments)]
pub fn set_shadow_prediction_audit(
    pending_pods: i64,
    fingerprint_matched_pods: i64,
    history_exact_pods: i64,
    history_scaled_pods: i64,
    history_segment_pods: i64,
    hint_pods: i64,
    unknown_pods: i64,
    predicted_runtime_pods: i64,
    predicted_vram_pods: i64,
    average_confidence: i64,
) {
    SHADOW_PREDICTION_AUDIT_PENDING_PODS.set(pending_pods);
    SHADOW_PREDICTION_AUDIT_FINGERPRINT_MATCHED_PODS.set(fingerprint_matched_pods);
    SHADOW_PREDICTION_AUDIT_HISTORY_EXACT_PODS.set(history_exact_pods);
    SHADOW_PREDICTION_AUDIT_HISTORY_SCALED_PODS.set(history_scaled_pods);
    SHADOW_PREDICTION_AUDIT_HISTORY_SEGMENT_PODS.set(history_segment_pods);
    SHADOW_PREDICTION_AUDIT_HINT_PODS.set(hint_pods);
    SHADOW_PREDICTION_AUDIT_UNKNOWN_PODS.set(unknown_pods);
    SHADOW_PREDICTION_AUDIT_PREDICTED_RUNTIME_PODS.set(predicted_runtime_pods);
    SHADOW_PREDICTION_AUDIT_PREDICTED_VRAM_PODS.set(predicted_vram_pods);
    SHADOW_PREDICTION_AUDIT_AVERAGE_CONFIDENCE.set(average_confidence);
}

pub fn set_shadow_candidate_model(
    candidate_node_limit: i64,
    unpruned_edges: i64,
    initial_edges: i64,
    final_edges: i64,
    pruned_workloads: i64,
    widening_retries: i64,
) {
    SHADOW_CANDIDATE_NODE_LIMIT.set(candidate_node_limit);
    SHADOW_CANDIDATE_EDGES_UNPRUNED.set(unpruned_edges);
    SHADOW_CANDIDATE_EDGES_INITIAL.set(initial_edges);
    SHADOW_CANDIDATE_EDGES_FINAL.set(final_edges);
    SHADOW_CANDIDATE_PRUNED_WORKLOADS.set(pruned_workloads);
    SHADOW_CANDIDATE_WIDENING_RETRIES.set(widening_retries);
}

pub fn inc_shadow_candidate_widening_attempts(n: u64) {
    SHADOW_CANDIDATE_WIDENING_ATTEMPTS.inc_by(n);
}

pub fn set_shadow_candidate_quality(
    pruning_active: bool,
    widened: bool,
    edge_reduction_milli: i64,
    regret_status: &str,
) {
    SHADOW_CANDIDATE_PRUNING_ACTIVE.set(if pruning_active { 1 } else { 0 });
    SHADOW_CANDIDATE_WIDENED.set(if widened { 1 } else { 0 });
    SHADOW_CANDIDATE_EDGE_REDUCTION_MILLI.set(edge_reduction_milli);
    for status in [
        "full_feasible_set",
        "grouped_exact_if_equivalent",
        "full_retry",
        "pruned_after_widening_regret_unknown",
        "pruned_regret_unknown",
        "unknown",
    ] {
        SHADOW_CANDIDATE_REGRET_STATUS
            .with_label_values(&[status])
            .set(0);
    }
    let status = match regret_status {
        "full_feasible_set"
        | "grouped_exact_if_equivalent"
        | "full_retry"
        | "pruned_after_widening_regret_unknown"
        | "pruned_regret_unknown" => regret_status,
        _ => "unknown",
    };
    SHADOW_CANDIDATE_REGRET_STATUS
        .with_label_values(&[status])
        .set(1);
}

#[allow(clippy::too_many_arguments)]
pub fn set_shadow_node_grouping(
    enabled: bool,
    used: bool,
    eligible_group_count: i64,
    eligible_node_count: i64,
    max_group_size: i64,
    grouped_node_count: i64,
    grouped_candidate_edges: i64,
) {
    SHADOW_NODE_GROUPING_ENABLED.set(if enabled { 1 } else { 0 });
    SHADOW_NODE_GROUPING_USED.set(if used { 1 } else { 0 });
    SHADOW_NODE_GROUPING_ELIGIBLE_GROUPS.set(eligible_group_count);
    SHADOW_NODE_GROUPING_ELIGIBLE_NODES.set(eligible_node_count);
    SHADOW_NODE_GROUPING_MAX_GROUP_SIZE.set(max_group_size);
    SHADOW_NODE_GROUPING_GROUPED_NODES.set(grouped_node_count);
    SHADOW_NODE_GROUPING_GROUPED_CANDIDATE_EDGES.set(grouped_candidate_edges);
}

pub fn inc_shadow_node_grouping_used() {
    SHADOW_NODE_GROUPING_USED_TOTAL.inc();
}

pub fn inc_shadow_node_grouping_fallback() {
    SHADOW_NODE_GROUPING_FALLBACK_TOTAL.inc();
}

pub fn inc_shadow_solves() {
    SHADOW_SOLVES.inc();
}

pub fn inc_shadow_solve_errors() {
    SHADOW_SOLVE_ERRORS.inc();
}

pub fn observe_shadow_solve_seconds(secs: f64) {
    SHADOW_SOLVE_SECONDS.observe(secs);
}

pub fn inc_shadow_unplaced(n: u64) {
    SHADOW_UNPLACED.inc_by(n);
}

pub fn inc_shadow_vram_blocked(n: u64) {
    SHADOW_VRAM_BLOCKED.inc_by(n);
}

pub fn inc_shadow_high_priority_unplaced(n: u64) {
    SHADOW_HIGH_PRIORITY_UNPLACED.inc_by(n);
}

pub fn inc_shadow_predicted_deadline_misses(n: u64) {
    SHADOW_PREDICTED_DEADLINE_MISSES.inc_by(n);
}

#[allow(clippy::too_many_arguments)]
pub fn set_shadow_repairs(
    plans: i64,
    migrations: i64,
    preemptions: i64,
    disruption_cost: i64,
    repairable_targets: i64,
    unrepairable_targets: i64,
    vram_blocked_targets: i64,
    not_enough_total_gpu_targets: i64,
    policy_or_candidate_blocked_targets: i64,
    incomplete_model_targets: i64,
    skipped_candidates: i64,
    priority_blocked_candidates: i64,
    value_policy_blocked_candidates: i64,
    disruption_policy_blocked_candidates: i64,
    pdb_blocked_candidates: i64,
    candidate_budget_skipped_candidates: i64,
) {
    SHADOW_REPAIR_PLANS_CURRENT.set(plans);
    SHADOW_REPAIR_MIGRATIONS_CURRENT.set(migrations);
    SHADOW_REPAIR_PREEMPTIONS_CURRENT.set(preemptions);
    SHADOW_REPAIR_DISRUPTION_COST_CURRENT.set(disruption_cost);
    SHADOW_REPAIR_REPAIRABLE_TARGETS.set(repairable_targets);
    SHADOW_REPAIR_UNREPAIRABLE_TARGETS.set(unrepairable_targets);
    SHADOW_REPAIR_VRAM_BLOCKED_TARGETS.set(vram_blocked_targets);
    SHADOW_REPAIR_NOT_ENOUGH_TOTAL_GPU_TARGETS.set(not_enough_total_gpu_targets);
    SHADOW_REPAIR_POLICY_OR_CANDIDATE_BLOCKED_TARGETS.set(policy_or_candidate_blocked_targets);
    SHADOW_REPAIR_INCOMPLETE_MODEL_TARGETS.set(incomplete_model_targets);
    SHADOW_REPAIR_SKIPPED_CANDIDATES.set(skipped_candidates);
    for (reason, value) in [
        ("priority", priority_blocked_candidates),
        ("value_policy", value_policy_blocked_candidates),
        ("disruption_policy", disruption_policy_blocked_candidates),
        ("pdb", pdb_blocked_candidates),
        ("candidate_budget", candidate_budget_skipped_candidates),
    ] {
        SHADOW_REPAIR_SKIPPED_CANDIDATES_BY_REASON
            .with_label_values(&[reason])
            .set(value);
    }
}

pub fn inc_shadow_repair_plans(n: u64) {
    SHADOW_REPAIR_PLANS.inc_by(n);
}

pub fn inc_shadow_repair_migrations(n: u64) {
    SHADOW_REPAIR_MIGRATIONS.inc_by(n);
}

pub fn inc_shadow_repair_preemptions(n: u64) {
    SHADOW_REPAIR_PREEMPTIONS.inc_by(n);
}

pub fn inc_shadow_repair_disruption_cost(n: u64) {
    SHADOW_REPAIR_DISRUPTION_COST.inc_by(n);
}

pub fn inc_shadow_caveated(n: u64) {
    SHADOW_CAVEATED.inc_by(n);
}

pub fn render_metrics() -> String {
    let encoder = TextEncoder::new();
    let metric_families = REGISTRY.gather();
    let mut buffer = Vec::new();
    encoder.encode(&metric_families, &mut buffer).unwrap();
    String::from_utf8(buffer).unwrap()
}

#[derive(Clone)]
pub struct SolveMetricLabels {
    base: [String; 9],
}

impl SolveMetricLabels {
    pub fn from_request(cluster_name: &str, req: &SolveRequest) -> Self {
        let scenario = if req.scenario_name.is_empty() {
            "ad-hoc".to_string()
        } else {
            req.scenario_name.clone()
        };
        let solver = if req.scenario.solver.is_empty() {
            "cp-sat-rust".to_string()
        } else {
            req.scenario.solver.clone()
        };
        let snapshot_mode = if req.snapshot_file.is_empty() {
            "live"
        } else {
            "snapshot"
        }
        .to_string();
        Self {
            base: [
                cluster_name.to_string(),
                solver,
                scenario,
                snapshot_mode,
                bool_label(req.scenario.use_usage_adjusted_requests),
                bool_label(req.scenario.ignore_taints),
                bool_label(req.scenario.relax_preferred_affinity),
                bool_label(req.scenario.relax_required_anti_affinity),
                bool_label(req.scenario.ignore_unschedulable_workloads),
            ],
        }
    }

    pub fn base_values(&self) -> [&str; 9] {
        [
            &self.base[0],
            &self.base[1],
            &self.base[2],
            &self.base[3],
            &self.base[4],
            &self.base[5],
            &self.base[6],
            &self.base[7],
            &self.base[8],
        ]
    }

    pub fn status_values<'a>(&'a self, status: &'a str) -> [&'a str; 10] {
        [
            &self.base[0],
            &self.base[1],
            &self.base[2],
            &self.base[3],
            &self.base[4],
            &self.base[5],
            &self.base[6],
            &self.base[7],
            &self.base[8],
            status,
        ]
    }
}

pub fn solve_started(labels: &SolveMetricLabels) {
    SOLVES_IN_FLIGHT
        .with_label_values(&labels.base_values())
        .inc();
}

pub fn solve_finished(labels: &SolveMetricLabels, status: &str, elapsed_seconds: f64) {
    SOLVE_DURATION_SECONDS
        .with_label_values(&labels.base_values())
        .observe(elapsed_seconds);
    SOLVES_TOTAL
        .with_label_values(&labels.status_values(status))
        .inc();
    SOLVES_IN_FLIGHT
        .with_label_values(&labels.base_values())
        .dec();
}

fn bool_label(value: bool) -> String {
    if value { "true" } else { "false" }.to_string()
}

#[cfg(test)]
mod shadow_metric_tests {
    use super::*;

    #[test]
    fn register_is_idempotent_and_shadow_metrics_render() {
        register_metrics();
        register_metrics(); // must not panic
        inc_shadow_pod_observations(3);
        set_shadow_pending(2);
        set_shadow_admission(1, 4);
        set_shadow_outcome_summary(1, 6, 4, 2, 50_000, 66_666, 2_000_000);
        set_shadow_deadlines(2, 1, 1, 1, 0, -300);
        set_shadow_placement_pressure(1, 1, 1);
        set_shadow_quota_throttle(1, 60);
        set_shadow_fairness(1, 1, 2250, 1000);
        set_shadow_budget_pressure(1, 500_000);
        set_shadow_gpu_utilization(1, 3);
        set_shadow_job_observations(
            2,
            2,
            1,
            3600,
            40 * 1024 * 1024 * 1024,
            1,
            2,
            125,
            300,
            1,
            250,
            4 * 1024 * 1024 * 1024,
        );
        set_shadow_prediction_audit(3, 2, 1, 1, 1, 0, 1, 2, 1, 55);
        set_shadow_candidate_model(16, 100, 64, 64, 4, 1);
        set_shadow_candidate_quality(true, true, 36_000, "pruned_after_widening_regret_unknown");
        inc_shadow_candidate_widening_attempts(1);
        set_shadow_node_grouping(true, true, 2, 16, 8, 4, 32);
        inc_shadow_node_grouping_used();
        inc_shadow_node_grouping_fallback();
        inc_shadow_solves();
        inc_shadow_solve_errors();
        observe_shadow_solve_seconds(0.05);
        inc_shadow_unplaced(1);
        inc_shadow_vram_blocked(1);
        inc_shadow_high_priority_unplaced(1);
        inc_shadow_predicted_deadline_misses(1);
        set_shadow_repairs(1, 1, 1, 3, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12);
        set_shadow_bind_skipped_by_reason(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11);
        inc_shadow_repair_plans(1);
        inc_shadow_repair_migrations(1);
        inc_shadow_repair_preemptions(1);
        inc_shadow_repair_disruption_cost(3);
        inc_shadow_caveated(1);
        inc_shadow_kubernetes_events("decision", 3, 2, 1);
        inc_shadow_kubernetes_events("binding", 2, 1, 1);
        set_shadow_leader(true);
        inc_shadow_leader_acquired();
        inc_shadow_leader_renewed();
        inc_shadow_leader_wait();
        inc_shadow_leader_renew_errors();
        inc_shadow_leader_skipped_solves();
        let out = render_metrics();
        assert!(out.contains("ksolver_shadow_pod_observations_total"));
        assert!(out.contains("ksolver_shadow_pending_pods"));
        assert!(out.contains("ksolver_shadow_admitted_pods"));
        assert!(out.contains("ksolver_shadow_admitted_gpu_demand"));
        assert!(out.contains("ksolver_shadow_requested_gpu_demand"));
        assert!(out.contains("ksolver_shadow_unplaced_gpu_demand"));
        assert!(out.contains("ksolver_shadow_pod_admission_percent_milli"));
        assert!(out.contains("ksolver_shadow_gpu_admission_percent_milli"));
        assert!(out.contains("ksolver_shadow_admitted_monthly_cost_milli"));
        assert!(out.contains("ksolver_shadow_deadline_jobs"));
        assert!(out.contains("ksolver_shadow_unplaced_deadline_jobs"));
        assert!(out.contains("ksolver_shadow_predicted_deadline_misses"));
        assert!(out.contains("ksolver_shadow_placed_predicted_deadline_misses"));
        assert!(out.contains("ksolver_shadow_unplaced_predicted_deadline_misses"));
        assert!(out.contains("ksolver_shadow_worst_deadline_slack_seconds"));
        assert!(out.contains("ksolver_shadow_quota_throttled_pods"));
        assert!(out.contains("ksolver_shadow_quota_throttled_max_queue_wait_seconds"));
        assert!(out.contains("ksolver_shadow_fairness_under_share_tenants"));
        assert!(out.contains("ksolver_shadow_fairness_over_share_tenants"));
        assert!(out.contains("ksolver_shadow_fairness_borrowed_gpu_milli"));
        assert!(out.contains("ksolver_shadow_fairness_reclaimable_borrowed_gpu_milli"));
        assert!(out.contains("ksolver_shadow_budget_over_tenants"));
        assert!(out.contains("ksolver_shadow_budget_overage_monthly_milli"));
        assert!(out.contains("ksolver_shadow_active_gpu_nodes"));
        assert!(out.contains("ksolver_shadow_stranded_gpu_on_active_nodes"));
        assert!(out.contains("ksolver_shadow_job_observation_completed_gpu_pods"));
        assert!(out.contains("ksolver_shadow_job_observation_runtime_samples"));
        assert!(out.contains("ksolver_shadow_job_observation_failed_gpu_pods"));
        assert!(out.contains("ksolver_shadow_job_observation_max_runtime_seconds"));
        assert!(out.contains("ksolver_shadow_job_observation_max_peak_memory_bytes"));
        assert!(out.contains("ksolver_shadow_job_observation_unique_command_hashes"));
        assert!(out.contains("ksolver_shadow_job_observation_runtime_prediction_samples"));
        assert!(out.contains("ksolver_shadow_job_observation_runtime_prediction_mape_milli"));
        assert!(out.contains("ksolver_shadow_job_observation_max_runtime_prediction_error_seconds"));
        assert!(out.contains("ksolver_shadow_job_observation_vram_prediction_samples"));
        assert!(out.contains("ksolver_shadow_job_observation_vram_prediction_mape_milli"));
        assert!(out.contains("ksolver_shadow_job_observation_max_vram_prediction_error_bytes"));
        assert!(out.contains("ksolver_shadow_prediction_audit_pending_pods"));
        assert!(out.contains("ksolver_shadow_prediction_audit_fingerprint_matched_pods"));
        assert!(out.contains("ksolver_shadow_prediction_audit_history_exact_pods"));
        assert!(out.contains("ksolver_shadow_prediction_audit_history_scaled_pods"));
        assert!(out.contains("ksolver_shadow_prediction_audit_history_segment_pods"));
        assert!(out.contains("ksolver_shadow_prediction_audit_hint_pods"));
        assert!(out.contains("ksolver_shadow_prediction_audit_unknown_pods"));
        assert!(out.contains("ksolver_shadow_prediction_audit_predicted_runtime_pods"));
        assert!(out.contains("ksolver_shadow_prediction_audit_predicted_vram_pods"));
        assert!(out.contains("ksolver_shadow_prediction_audit_average_confidence"));
        assert!(out.contains("ksolver_shadow_candidate_node_limit"));
        assert!(out.contains("ksolver_shadow_candidate_edges_unpruned"));
        assert!(out.contains("ksolver_shadow_candidate_edges_initial"));
        assert!(out.contains("ksolver_shadow_candidate_edges_final"));
        assert!(out.contains("ksolver_shadow_candidate_pruned_workloads"));
        assert!(out.contains("ksolver_shadow_candidate_widening_retries"));
        assert!(out.contains("ksolver_shadow_candidate_widening_attempts_total"));
        assert!(out.contains("ksolver_shadow_candidate_pruning_active"));
        assert!(out.contains("ksolver_shadow_candidate_widened"));
        assert!(out.contains("ksolver_shadow_candidate_edge_reduction_milli"));
        assert!(out.contains("ksolver_shadow_candidate_regret_status"));
        assert!(out.contains("status=\"pruned_after_widening_regret_unknown\""));
        assert!(out.contains("ksolver_shadow_node_grouping_enabled"));
        assert!(out.contains("ksolver_shadow_node_grouping_used"));
        assert!(out.contains("ksolver_shadow_node_grouping_eligible_groups"));
        assert!(out.contains("ksolver_shadow_node_grouping_eligible_nodes"));
        assert!(out.contains("ksolver_shadow_node_grouping_max_group_size"));
        assert!(out.contains("ksolver_shadow_node_grouping_grouped_nodes"));
        assert!(out.contains("ksolver_shadow_node_grouping_grouped_candidate_edges"));
        assert!(out.contains("ksolver_shadow_node_grouping_used_total"));
        assert!(out.contains("ksolver_shadow_node_grouping_fallback_total"));
        assert!(out.contains("ksolver_shadow_solves_total"));
        assert!(out.contains("ksolver_shadow_solve_errors_total"));
        assert!(out.contains("ksolver_shadow_solve_seconds"));
        assert!(out.contains("ksolver_shadow_unplaced_total"));
        assert!(out.contains("ksolver_shadow_vram_blocked_total"));
        assert!(out.contains("ksolver_shadow_high_priority_unplaced_total"));
        assert!(out.contains("ksolver_shadow_unplaced"));
        assert!(out.contains("ksolver_shadow_vram_blocked"));
        assert!(out.contains("ksolver_shadow_high_priority_unplaced"));
        assert!(out.contains("ksolver_shadow_predicted_deadline_misses_total"));
        assert!(out.contains("ksolver_shadow_repair_plans"));
        assert!(out.contains("ksolver_shadow_repair_migrations"));
        assert!(out.contains("ksolver_shadow_repair_preemptions"));
        assert!(out.contains("ksolver_shadow_repair_disruption_cost"));
        assert!(out.contains("ksolver_shadow_repair_repairable_targets"));
        assert!(out.contains("ksolver_shadow_repair_unrepairable_targets"));
        assert!(out.contains("ksolver_shadow_repair_vram_blocked_targets"));
        assert!(out.contains("ksolver_shadow_repair_not_enough_total_gpu_targets"));
        assert!(out.contains("ksolver_shadow_repair_policy_or_candidate_blocked_targets"));
        assert!(out.contains("ksolver_shadow_repair_incomplete_model_targets"));
        assert!(out.contains("ksolver_shadow_repair_skipped_candidates"));
        assert!(out.contains("ksolver_shadow_repair_skipped_candidates_by_reason"));
        assert!(out.contains("reason=\"priority\""));
        assert!(out.contains("reason=\"value_policy\""));
        assert!(out.contains("reason=\"disruption_policy\""));
        assert!(out.contains("reason=\"pdb\""));
        assert!(out.contains("reason=\"candidate_budget\""));
        assert!(out.contains("ksolver_shadow_bind_skipped_by_reason"));
        assert!(out.contains("reason=\"canary\""));
        assert!(out.contains("reason=\"readiness\""));
        assert!(out.contains("reason=\"identity\""));
        assert!(out.contains("reason=\"scheduler\""));
        assert!(out.contains("reason=\"already_bound\""));
        assert!(out.contains("reason=\"dra\""));
        assert!(out.contains("reason=\"throttle\""));
        assert!(out.contains("reason=\"reservation\""));
        assert!(out.contains("reason=\"disabled\""));
        assert!(out.contains("reason=\"group\""));
        assert!(out.contains("reason=\"other\""));
        assert!(out.contains("ksolver_shadow_repair_plans_total"));
        assert!(out.contains("ksolver_shadow_repair_migrations_total"));
        assert!(out.contains("ksolver_shadow_repair_preemptions_total"));
        assert!(out.contains("ksolver_shadow_repair_disruption_cost_total"));
        assert!(out.contains("ksolver_shadow_caveated_total"));
        assert!(out.contains("ksolver_shadow_kubernetes_events_total"));
        assert!(out.contains("event_type=\"decision\",outcome=\"attempted\""));
        assert!(out.contains("event_type=\"binding\",outcome=\"failed\""));
        assert!(out.contains("ksolver_shadow_leader"));
        assert!(out.contains("ksolver_shadow_leader_acquired_total"));
        assert!(out.contains("ksolver_shadow_leader_renewed_total"));
        assert!(out.contains("ksolver_shadow_leader_wait_total"));
        assert!(out.contains("ksolver_shadow_leader_renew_errors_total"));
        assert!(out.contains("ksolver_shadow_leader_skipped_solves_total"));
    }
}
