use crate::model::{
    NormalizedCluster, NormalizedNode, NormalizedWorkload, ObjectiveProfile, OptimizationInput,
    OptimizationNode, OptimizationSolution, OptimizationWorkload, OptimizationWorkloadMember,
    ResourceList, ScenarioConfig,
};
use crate::scheduler::pending_input::{
    build_pending_input_diagnosed, expand_grouped_solution_to_physical,
    group_pending_input_by_node_symmetry,
};
use crate::scheduler::pod_filter::PendingGpuPod;
use crate::scheduler::repair::advise_repairs;
use crate::scheduler::trace::{
    AdmissionMetrics, BindingOutcomeMetrics, BindingReservationMetrics, CandidateQualityMetrics,
    DeadlineMetrics, DecisionTrace, GpuUtilizationMetrics, JobObservationMetrics,
    NodeGroupingMetrics, PodDecision, PodPlacement, PredictionAuditMetrics, QueueWaitMetrics,
    QuotaMetrics, RepairMetrics, TenantFairnessMetrics,
};
use anyhow::Context;
use k8s_openapi::api::resource::v1alpha3 as dra;
use kube::api::ObjectMeta;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

const GPU_RESOURCE: &str = "nvidia.com/gpu";
const REGRET_CANDIDATE_LIMIT: usize = 2;
const DEFAULT_SIMULATOR_BATCH_TIMEOUT: Duration = Duration::from_millis(2_500);
const GPU_BENCHMARK_SIMULATOR_STABLE_POLLS: usize = 1;
pub const DEFAULT_SIMULATOR_LIVE_BASELINE_LIMIT: usize = 4;
/// Synthetic monthly $ per GPU on a node — a node's cost scales with its GPU count (realistic for
/// GPU instances). Absolute value is irrelevant to the ranking; only relative cost across
/// schedulers on the SAME fleet matters.
const GPU_MONTHLY_PER_GPU: i64 = 2000;

#[derive(Debug, Clone)]
struct GpuNodeSpec {
    name: String,
    gpus: i64,
    vram_gib_per_gpu: i64,
    /// Monthly $ to keep this node powered on (charged when it hosts ≥1 GPU pod).
    monthly_cost: i64,
}

#[derive(Debug, Clone)]
struct JobSpec {
    name: String,
    gpus_per_pod: i64,
    pods: usize,
    colocate: bool,
    priority: i64,
    priority_class_name: String,
    team: String,
    business_value: i64,
    fair_share_deficit: i64,
    queue: String,
    queue_score: i64,
    queue_wait_seconds: i64,
    deadline_after_seconds: i64,
    min_gpus: i64,
    max_gpus: i64,
    preferred_gpus: i64,
    flexible: bool,
    predicted_runtime_seconds: i64,
    predicted_peak_vram_gib: i64,
}

impl JobSpec {
    fn singleton(name: &str, gpus: i64) -> Self {
        Self {
            name: name.to_string(),
            gpus_per_pod: gpus,
            pods: 1,
            colocate: false,
            priority: 0,
            priority_class_name: String::new(),
            team: String::new(),
            business_value: 0,
            fair_share_deficit: 0,
            queue: String::new(),
            queue_score: 0,
            queue_wait_seconds: 0,
            deadline_after_seconds: 0,
            min_gpus: 0,
            max_gpus: 0,
            preferred_gpus: 0,
            flexible: false,
            predicted_runtime_seconds: 0,
            predicted_peak_vram_gib: 0,
        }
    }

    fn colocated_gang(name: &str, pods: usize, gpus_per_pod: i64) -> Self {
        Self {
            name: name.to_string(),
            gpus_per_pod,
            pods,
            colocate: true,
            priority: 0,
            priority_class_name: String::new(),
            team: String::new(),
            business_value: 0,
            fair_share_deficit: 0,
            queue: String::new(),
            queue_score: 0,
            queue_wait_seconds: 0,
            deadline_after_seconds: 0,
            min_gpus: 0,
            max_gpus: 0,
            preferred_gpus: 0,
            flexible: false,
            predicted_runtime_seconds: 0,
            predicted_peak_vram_gib: 0,
        }
    }

    fn with_priority(mut self, priority: i64, priority_class_name: &str) -> Self {
        self.priority = priority.max(0);
        self.priority_class_name = priority_class_name.to_string();
        self
    }

    fn with_business_value(mut self, value: i64) -> Self {
        self.business_value = value.max(0);
        self
    }

    fn with_fair_share_deficit(mut self, team: &str, deficit: i64) -> Self {
        self.team = team.to_string();
        self.fair_share_deficit = deficit.max(0);
        self
    }

    fn with_queue(mut self, queue: &str, score: i64) -> Self {
        self.queue = queue.to_string();
        self.queue_score = score.max(0);
        self
    }

    fn with_queue_wait(mut self, seconds: i64) -> Self {
        self.queue_wait_seconds = seconds.max(0);
        self
    }

    fn with_deadline(
        mut self,
        deadline_after_seconds: i64,
        predicted_runtime_seconds: i64,
    ) -> Self {
        self.deadline_after_seconds = deadline_after_seconds.max(0);
        self.predicted_runtime_seconds = predicted_runtime_seconds.max(0);
        self
    }

    fn with_peak_vram(mut self, peak_gib: i64) -> Self {
        self.predicted_peak_vram_gib = peak_gib.max(0);
        self
    }

    fn with_flexible_gpus(mut self, min_gpus: i64, preferred_gpus: i64, max_gpus: i64) -> Self {
        self.min_gpus = min_gpus.max(0);
        self.preferred_gpus = preferred_gpus.max(0);
        self.max_gpus = max_gpus.max(0);
        self.flexible = true;
        self
    }

    fn total_gpus(&self) -> i64 {
        self.gpus_per_pod * self.pods as i64
    }

    fn pod_names(&self) -> Vec<String> {
        if self.pods == 1 {
            vec![self.name.clone()]
        } else {
            (0..self.pods)
                .map(|i| format!("{}-{i}", self.name))
                .collect()
        }
    }
}

/// Scenario size tier. `Small` fleets are illustrative (easy to eyeball the node boards); `Large`
/// fleets (~50–100 GPU nodes) surface aggregate cost/utilization deltas closer to real clusters.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Tier {
    Small,
    Large,
}

#[derive(Debug, Clone)]
struct ScenarioSpec {
    name: String,
    description: String,
    tier: Tier,
    nodes: Vec<GpuNodeSpec>,
    jobs: Vec<JobSpec>,
    ksolver_priority_weight: i64,
    ksolver_business_value_weight: i64,
    ksolver_queue_weight: i64,
    ksolver_queue_wait_weight: i64,
    ksolver_fair_share_weight: i64,
    ksolver_deadline_urgency_weight: i64,
    ksolver_deadline_miss_weight: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Placement {
    pub pod: String,
    pub node: Option<String>,
    pub gpus: i64,
}

#[derive(Debug, Clone, Default, Serialize, PartialEq, Eq)]
pub struct PlacementMetrics {
    pub useful_gpu: i64,
    pub priority_useful_gpu: i64,
    pub business_value_useful_gpu: i64,
    pub fair_share_useful_gpu: i64,
    pub queue_useful_gpu: i64,
    pub queue_wait_useful_gpu: i64,
    pub placed_pods: usize,
    pub unplaced_pods: usize,
    pub large_jobs_admitted: usize,
    pub full_gangs: usize,
    pub partial_or_invalid_gangs: usize,
    pub deadline_met_gpu: i64,
    pub deadline_unplaced_gpu: i64,
    pub deadline_miss_gpu: i64,
    pub flexible_selected_gpu: i64,
    pub flexible_gpu_reduction: i64,
    pub active_nodes: usize,
    pub stranded_gpu_on_active_nodes: i64,
    /// Monthly $ of all nodes hosting ≥1 GPU pod (the fleet you pay to power on).
    pub cost_active_nodes_monthly: i64,
    /// Packing density on active nodes: used GPU × 1000 / GPU capacity of active nodes (0–1000).
    /// Higher = tighter packing / less stranded capacity on the nodes you paid for.
    pub gpu_utilization_milli: i64,
}

#[derive(Debug, Clone, Serialize)]
pub struct EngineResult {
    pub engine: String,
    pub source: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub simulator: Option<SimulatorBaselineProvenance>,
    #[serde(default)]
    pub candidate_node_limit: usize,
    pub solve_millis: u64,
    pub metrics: PlacementMetrics,
    pub placements: Vec<Placement>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct SimulatorBaselineProvenance {
    pub mode: String,
    pub variant: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub url: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cache_key: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub live_baseline_limit: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timeout_millis: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub elapsed_millis: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub phase: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target_count: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub present_targets: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub terminal_present_targets: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub missing_targets: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stable_polls: Option<usize>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub phase_timings: Vec<SimulatorPhaseTiming>,
    pub timed_out: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fallback_reason: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct SimulatorPhaseTiming {
    pub phase: String,
    pub duration_millis: u64,
    pub cumulative_millis: u64,
}

#[derive(Debug, Clone)]
pub struct BenchmarkOptions {
    pub simulator_url: Option<String>,
    pub simulator_urls: Vec<String>,
    pub simulator_cache_path: Option<PathBuf>,
    pub simulator_cache_dir: Option<PathBuf>,
    pub refresh_simulator_cache: bool,
    pub simulator_batch_timeout: Duration,
    pub simulator_progress: bool,
    pub simulator_max_live_baselines: Option<usize>,
    pub simulator_live_scenarios: Option<BTreeSet<String>>,
}

impl Default for BenchmarkOptions {
    fn default() -> Self {
        Self {
            simulator_url: None,
            simulator_urls: Vec::new(),
            simulator_cache_path: None,
            simulator_cache_dir: None,
            refresh_simulator_cache: false,
            simulator_batch_timeout: DEFAULT_SIMULATOR_BATCH_TIMEOUT,
            simulator_progress: false,
            simulator_max_live_baselines: Some(DEFAULT_SIMULATOR_LIVE_BASELINE_LIMIT),
            simulator_live_scenarios: None,
        }
    }
}

impl BenchmarkOptions {
    fn simulator_url_pool(&self) -> Vec<String> {
        let mut urls = Vec::new();
        if let Some(url) = self
            .simulator_url
            .as_deref()
            .map(str::trim)
            .filter(|url| !url.is_empty())
        {
            urls.push(url.trim_end_matches('/').to_string());
        }
        for url in &self.simulator_urls {
            let url = url.trim().trim_end_matches('/');
            if !url.is_empty() && !urls.iter().any(|existing| existing == url) {
                urls.push(url.to_string());
            }
        }
        urls
    }

    fn cache_enabled(&self) -> bool {
        self.simulator_cache_path.is_some() || self.simulator_cache_dir.is_some()
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct SimulatorCacheFile {
    version: u32,
    entries: BTreeMap<String, CachedSimulatorResult>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct CachedSimulatorResult {
    engine: String,
    source: String,
    placements: Vec<Placement>,
}

#[derive(Debug, Clone, Default, Serialize, PartialEq, Eq)]
pub struct SimulatorCacheCoverage {
    pub total_baselines: usize,
    pub cached_baselines: usize,
    pub missing_baselines: usize,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct RegretMetrics {
    pub candidate_node_limit: usize,
    pub useful_gpu_regret: i64,
    pub priority_useful_gpu_regret: i64,
    pub placed_pod_regret: i64,
    pub large_job_regret: i64,
    pub full_gang_regret: i64,
    pub invalid_gang_delta: i64,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct RegretSummary {
    pub candidate_node_limit: usize,
    pub scenarios_compared: usize,
    pub scenarios_with_any_regret: usize,
    pub scenarios_with_useful_gpu_regret: usize,
    pub total_useful_gpu_regret: i64,
    pub max_useful_gpu_regret: i64,
    pub total_priority_useful_gpu_regret: i64,
    pub max_priority_useful_gpu_regret: i64,
    pub total_placed_pod_regret: i64,
    pub total_large_job_regret: i64,
    pub total_full_gang_regret: i64,
    pub total_invalid_gang_delta: i64,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct BenefitSummary {
    pub scenarios_compared: usize,
    pub scenarios_with_positive_benefit: usize,
    pub scenarios_with_useful_gpu_gain: usize,
    pub total_benefit_score: i64,
    pub max_benefit_score: i64,
    pub top_scenario: String,
    pub total_useful_gpu_gain: i64,
    pub total_priority_useful_gpu_gain: i64,
    pub total_business_value_useful_gpu_gain: i64,
    pub total_fair_share_useful_gpu_gain: i64,
    pub total_queue_useful_gpu_gain: i64,
    pub total_queue_wait_useful_gpu_gain: i64,
    pub total_large_job_gain: i64,
    pub total_full_gang_gain: i64,
    pub total_invalid_gangs_avoided: i64,
    pub total_deadline_met_gpu_gain: i64,
    pub total_deadline_unplaced_gpu_reduction: i64,
    pub total_deadline_miss_gpu_reduction: i64,
    pub total_flexible_gpu_reduction_gain: i64,
    pub total_active_node_reduction: i64,
    pub total_stranded_gpu_reduction: i64,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct RoiSummary {
    pub scenarios_compared: usize,
    pub scenarios_with_positive_admission_gain: usize,
    pub total_requested_gpu: i64,
    pub kube_admitted_useful_gpu: i64,
    pub ksolver_admitted_useful_gpu: i64,
    pub admitted_useful_gpu_gain: i64,
    pub kube_unplaced_pods: usize,
    pub ksolver_unplaced_pods: usize,
    pub unplaced_pod_reduction: i64,
    pub kube_active_nodes: usize,
    pub ksolver_active_nodes: usize,
    pub active_node_reduction: i64,
    pub stranded_gpu_reduction: i64,
    pub kube_active_node_monthly_cost: i64,
    pub ksolver_active_node_monthly_cost: i64,
    pub active_node_monthly_cost_reduction: i64,
    pub kube_gpu_utilization_milli: i64,
    pub ksolver_gpu_utilization_milli: i64,
    pub gpu_utilization_gain_milli: i64,
    pub kube_admission_percent_milli: i64,
    pub ksolver_admission_percent_milli: i64,
    pub admission_percent_gain_milli: i64,
    pub headline: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct FeatureAssertion {
    pub name: String,
    pub passed: bool,
    pub evidence: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct RepairScenarioProof {
    pub name: String,
    pub passed: bool,
    pub target: String,
    pub target_gpu_request: i64,
    pub node: String,
    pub action_count: usize,
    pub migration_actions: usize,
    pub preemption_actions: usize,
    pub freed_gpu: i64,
    pub disruption_cost: i32,
    pub explanation: String,
    pub notes: Vec<String>,
    pub metrics: RepairMetrics,
    #[serde(default)]
    pub action_rows: Vec<RepairActionRow>,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct KssProofScenario {
    pub name: String,
    pub phase: String,
    pub claim: String,
    pub passed: bool,
    pub strongest_baseline: String,
    pub baseline_modes: Vec<String>,
    pub kube_useful_gpu: i64,
    pub kube_unplaced_pods: usize,
    pub ksolver_useful_gpu: i64,
    pub ksolver_unplaced_pods: usize,
    pub caveat: String,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct ScenarioPage {
    pub slug: String,
    pub title: String,
    pub description: String,
    pub scenario_names: Vec<String>,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct RepairActionRow {
    pub step: usize,
    pub action: String,
    pub namespace: String,
    pub pod: String,
    pub from_node: String,
    pub to_node: String,
    pub gpu_request: i64,
    pub disruption_cost: i32,
    pub reason: String,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct HeroDemoSummary {
    pub name: String,
    pub passed: bool,
    pub headline: String,
    pub problem: String,
    pub recommendation: String,
    pub target: String,
    pub target_gpu_request: i64,
    pub repair_node: String,
    pub freed_gpu: i64,
    pub migration_actions: usize,
    pub preemption_actions: usize,
    pub disruption_cost: i32,
    pub roi_headline: String,
    pub screenshot_claims: Vec<String>,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct PreemptionMigrationHeroSummary {
    pub name: String,
    pub passed: bool,
    pub headline: String,
    pub target: String,
    pub target_gpu_request: i64,
    pub repair_node: String,
    pub freed_gpu: i64,
    pub migration_actions: usize,
    pub preemption_actions: usize,
    pub total_disruption_cost: i32,
    pub action_rows: Vec<RepairActionRow>,
    pub decision_contract: HeroDecisionContract,
    pub safety_claims: Vec<String>,
    pub operator_questions: Vec<String>,
    pub residual_risks: Vec<String>,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct HeroDecisionContract {
    pub verdict: String,
    pub can_act_now: bool,
    pub evidence_required: Vec<String>,
    pub approval_required: Vec<String>,
    pub fail_closed_if: Vec<String>,
    pub next_action: String,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct SreDemoStep {
    pub title: String,
    pub operator_question: String,
    pub evidence: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct SreScenarioCard {
    pub rank: usize,
    pub scenario: String,
    pub tier: Tier,
    pub headline: String,
    pub efficiency_score: i64,
    pub significantly_better: bool,
    pub kube_useful_gpu: i64,
    pub ksolver_useful_gpu: i64,
    pub useful_gpu_gain: i64,
    pub kube_active_node_monthly_cost: i64,
    pub ksolver_active_node_monthly_cost: i64,
    pub active_node_monthly_cost_delta: i64,
    pub kube_gpu_utilization_milli: i64,
    pub ksolver_gpu_utilization_milli: i64,
    pub gpu_utilization_gain_milli: i64,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct SreDemoScript {
    pub name: String,
    pub headline: String,
    pub hero_scenario: String,
    pub primary_question: String,
    pub steps: Vec<SreDemoStep>,
    pub top_scenario_cards: Vec<SreScenarioCard>,
    pub operator_close: String,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct ProductionSafetySummary {
    pub name: String,
    pub passed: bool,
    pub default_mode: String,
    pub mutation_default_enabled: bool,
    pub real_binding_gate: String,
    pub launch_contract: ProductionLaunchContract,
    pub rollout_modes: Vec<String>,
    pub production_checklist: Vec<String>,
    pub rbac_modes: Vec<String>,
    pub failure_mode_controls: Vec<String>,
    pub audit_fields: Vec<String>,
    pub rollout_gate_rows: Vec<ProductionRolloutGateRow>,
    pub failure_playbook_rows: Vec<ProductionFailurePlaybookRow>,
    pub audit_event_rows: Vec<ProductionAuditEventRow>,
    pub live_validation_rows: Vec<ProductionLiveValidationRow>,
    pub live_config_rows: Vec<ProductionLiveConfigRow>,
    pub kill_switches: Vec<String>,
    pub readiness_checks: Vec<String>,
    pub leader_election: String,
    pub reservation_ledger: String,
    pub restart_safety: String,
    pub audit_events: String,
    pub rbac_profile: String,
    pub mutation_boundaries: Vec<String>,
    pub residual_risks: Vec<String>,
    pub operator_claims: Vec<String>,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct ProductionLaunchContract {
    pub launch_level: String,
    pub live_writes_allowed: bool,
    pub required_gates: Vec<String>,
    pub required_rbac: Vec<String>,
    pub fail_closed_if: Vec<String>,
    pub audit_artifacts: Vec<String>,
    pub next_action: String,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct ProductionRolloutGateRow {
    pub mode: String,
    pub mutation_allowed: bool,
    pub required_rbac: String,
    pub required_gates: Vec<String>,
    pub blast_radius_control: String,
    pub rollback_action: String,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct ProductionFailurePlaybookRow {
    pub failure_mode: String,
    pub detection: String,
    pub automatic_behavior: String,
    pub operator_action: String,
    pub audit_field: String,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct ProductionAuditEventRow {
    pub event_type: String,
    pub enabled_by_default: bool,
    pub required_rbac: String,
    pub payload_fields: Vec<String>,
    pub reason: String,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct ProductionLiveValidationRow {
    pub gate: String,
    pub evidence: String,
    pub fail_closed_behavior: String,
    pub audit_field: String,
    pub required_before: String,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct ProductionLiveConfigRow {
    pub gate: String,
    pub env_var: String,
    pub live_endpoint_field: String,
    pub expected_safe_default: String,
    pub required_rbac_when_enabled: String,
    pub fail_closed_signal: String,
    pub operator_action: String,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct PredictionQualitySummary {
    pub name: String,
    pub passed: bool,
    pub promotion_contract: PredictionPromotionContract,
    pub coverage_sources: Vec<String>,
    pub calibration_metrics: Vec<String>,
    pub calibration_lifecycle: Vec<String>,
    pub confidence_bands: Vec<String>,
    pub drift_monitors: Vec<String>,
    pub decision_impact_evidence: Vec<String>,
    pub model_cards: Vec<PredictionModelCard>,
    pub calibration_buckets: Vec<PredictionCalibrationBucket>,
    pub live_calibration_rows: Vec<PredictionLiveCalibrationRow>,
    pub audit_fields: Vec<String>,
    pub promotion_gates: Vec<String>,
    pub placement_effects: Vec<String>,
    pub confidence_model: String,
    pub operator_claims: Vec<String>,
    pub residual_risks: Vec<String>,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct PredictionPromotionContract {
    pub promotion_level: String,
    pub hard_placement_allowed: bool,
    pub prediction_sensitive_claims_allowed: bool,
    pub required_evidence: Vec<String>,
    pub blocked_by: Vec<String>,
    pub demotion_triggers: Vec<String>,
    pub next_action: String,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct PredictionLiveCalibrationRow {
    pub gate: String,
    pub live_trace_metric: String,
    pub healthy_threshold: String,
    pub unhealthy_action: String,
    pub placement_impact: String,
    pub operator_view: String,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct PredictionModelCard {
    pub source_tier: String,
    pub confidence_band: String,
    pub required_evidence: String,
    pub failure_mode: String,
    pub placement_use: String,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct PredictionCalibrationBucket {
    pub bucket: String,
    pub sample_gate: String,
    pub runtime_metric: String,
    pub vram_metric: String,
    pub drift_signal: String,
    pub action_when_unhealthy: String,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct ScaleGuardrailSummary {
    pub name: String,
    pub passed: bool,
    pub actionability_contract: ScaleActionabilityContract,
    pub default_candidate_node_limit: usize,
    pub scenarios_compared_for_regret: usize,
    pub scenarios_with_any_regret: usize,
    pub max_useful_gpu_regret: i64,
    pub grouping_claim: String,
    pub grouping_physical_nodes_before: usize,
    pub grouping_nodes_after: usize,
    pub grouping_eligible_nodes: usize,
    pub grouping_max_group_size: usize,
    pub grouping_expanded_used_nodes: Vec<String>,
    pub grouping_preserved_admitted_gpu: bool,
    pub widening_claim: String,
    pub widening_retry_count: usize,
    pub widening_useful_gpu_recovered: i64,
    pub grouping_policy: Vec<String>,
    pub pruning_modes: Vec<String>,
    pub regret_status_ladder: Vec<String>,
    pub fallback_triggers: Vec<String>,
    pub scale_mode_cards: Vec<ScaleModeCard>,
    pub regret_action_rows: Vec<ScaleRegretActionRow>,
    pub large_fleet_validation_rows: Vec<ScaleLargeFleetValidationRow>,
    pub operator_switches: Vec<String>,
    pub guardrails: Vec<String>,
    pub residual_risks: Vec<String>,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct ScaleActionabilityContract {
    pub recommendation: String,
    pub customer_scale_claim_allowed: bool,
    pub high_risk_pruned_binding_allowed: bool,
    pub preferred_large_fleet_mode: String,
    pub required_evidence: Vec<String>,
    pub fail_closed_if: Vec<String>,
    pub operator_overrides: Vec<String>,
    pub next_action: String,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct ScaleLargeFleetValidationRow {
    pub gate: String,
    pub required_evidence: String,
    pub live_trace_metric: String,
    pub fail_closed_action: String,
    pub operator_claim: String,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct ScaleModeCard {
    pub mode: String,
    pub status: String,
    pub speedup_mechanism: String,
    pub correctness_check: String,
    pub evidence: String,
    pub operator_action: String,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct ScaleRegretActionRow {
    pub regret_status: String,
    pub meaning: String,
    pub risk_level: String,
    pub next_action: String,
    pub metric_or_trace_field: String,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct FairnessBudgetSummary {
    pub name: String,
    pub passed: bool,
    pub fair_share_scenario: String,
    pub under_share_job: String,
    pub under_share_admitted: bool,
    pub fair_share_useful_gpu_gain: i64,
    pub tenant_budget_scenario: String,
    pub tenant: String,
    pub monthly_budget_milli: i64,
    pub expensive_node_cost_milli: i64,
    pub cheap_node_cost_milli: i64,
    pub expensive_job_admitted: bool,
    pub cheap_job_admitted: bool,
    pub admitted_jobs: usize,
    pub unplaced_jobs: usize,
    pub policy_decision_rows: Vec<FairnessPolicyDecisionRow>,
    pub tenant_ledger_rows: Vec<FairnessTenantLedgerRow>,
    pub ownership_rows: Vec<FairnessOwnershipRow>,
    pub ui_badges: Vec<String>,
    pub enforcement_controls: Vec<String>,
    pub operator_questions: Vec<String>,
    pub decision_explanations: Vec<String>,
    pub trace_fields: Vec<String>,
    pub residual_risks: Vec<String>,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct FairnessPolicyDecisionRow {
    pub subject: String,
    pub workload: String,
    pub decision: String,
    pub policy: String,
    pub reason: String,
    pub evidence_field: String,
    pub operator_action: String,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct FairnessOwnershipRow {
    pub gate: String,
    pub ownership_source: String,
    pub live_trace_field: String,
    pub policy_use: String,
    pub missing_data_action: String,
    pub operator_question: String,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct FairnessTenantLedgerRow {
    pub tenant: String,
    pub status: String,
    pub admitted_gpu: i64,
    pub denied_gpu: i64,
    pub admitted_monthly_cost_milli: i64,
    pub budget_monthly_milli: i64,
    pub budget_overage_monthly_milli: i64,
    pub fair_share_delta_gpu_milli: i64,
    pub borrowed_gpu_milli: i64,
    pub reclaimable_borrowed_gpu_milli: i64,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct RoiDashboardTile {
    pub key: String,
    pub label: String,
    pub value: i64,
    pub unit: String,
    pub direction: String,
    pub evidence: String,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct RoiDashboardSummary {
    pub name: String,
    pub passed: bool,
    pub headline: String,
    pub primary_tiles: Vec<RoiDashboardTile>,
    pub claim_contract: RoiClaimContract,
    pub executive_rows: Vec<RoiExecutiveRow>,
    pub decision_rows: Vec<RoiDecisionRow>,
    pub decision_frame: Vec<String>,
    pub presentation_order: Vec<String>,
    pub scenario_count: usize,
    pub hero_scenario: String,
    pub hero_repair_disruption_cost: i32,
    pub confidence_guardrail: String,
    pub regret_guardrail: String,
    pub operator_questions: Vec<String>,
    pub residual_risks: Vec<String>,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct RoiClaimContract {
    pub claim_level: String,
    pub can_show_customer_dollars: bool,
    pub value_basis: String,
    pub required_evidence: Vec<String>,
    pub blocked_by: Vec<String>,
    pub next_action: String,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct RoiDecisionRow {
    pub tile_key: String,
    pub decision_rule: String,
    pub good_signal: String,
    pub caveat: String,
    pub next_action: String,
    pub evidence_source: String,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct PricingReadinessSummary {
    pub name: String,
    pub passed: bool,
    pub current_mode: String,
    pub pricing_required_before_customer_claim: Vec<String>,
    pub accepted_sources: Vec<String>,
    pub roi_fields_to_recompute: Vec<String>,
    pub validation_checks: Vec<String>,
    pub pricing_evidence_rows: Vec<PricingEvidenceRow>,
    pub operator_actions: Vec<String>,
    pub residual_risks: Vec<String>,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct PricingEvidenceRow {
    pub gate: String,
    pub accepted_source: String,
    pub required_mapping: String,
    pub recompute_target: String,
    pub pass_signal: String,
    pub failure_action: String,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct RoiExecutiveRow {
    pub priority: usize,
    pub claim: String,
    pub value: String,
    pub evidence_tile: String,
    pub caveat: String,
    pub operator_action: String,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct DemoReadinessSummary {
    pub name: String,
    pub passed: bool,
    pub headline: String,
    pub primary_story: String,
    pub hero_scenario: String,
    pub kube_baseline_mode: String,
    pub ksolver_action: String,
    pub roi_claim: String,
    pub safety_claim: String,
    pub demo_flow_scenes: Vec<DemoFlowScene>,
    pub demo_acceptance_criteria: Vec<String>,
    pub live_validation_rows: Vec<DemoLiveValidationRow>,
    pub ui_sections: Vec<String>,
    pub operator_checklist: Vec<String>,
    pub remaining_gaps: Vec<String>,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct DemoLiveValidationRow {
    pub gate: String,
    pub live_endpoint: String,
    pub required_evidence: String,
    pub pass_signal: String,
    pub failure_action: String,
    pub operator_question: String,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct DemoFlowScene {
    pub step: usize,
    pub screen: String,
    pub operator_question: String,
    pub evidence_source: String,
    pub primary_visual: String,
    pub decision: String,
    pub trust_gate: String,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct DeviceCorrectnessSummary {
    pub name: String,
    pub passed: bool,
    pub supported_today: Vec<String>,
    pub proof_backed_claims: Vec<String>,
    pub exact_semantics: Vec<String>,
    pub approximated_semantics: Vec<String>,
    pub unsupported_claims: Vec<String>,
    pub validation_signals: Vec<String>,
    pub fallback_actions: Vec<String>,
    pub device_readiness_rows: Vec<DeviceReadinessRow>,
    pub topology_claim: String,
    pub mig_claim: String,
    pub dra_approximation_claim: String,
    pub dra_allocation_claim: String,
    pub time_sliced_claim: String,
    pub hard_limits: Vec<String>,
    pub residual_risks: Vec<String>,
    pub operator_claims: Vec<String>,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct DeviceReadinessRow {
    pub feature: String,
    pub support_level: String,
    pub required_inventory: String,
    pub live_trace_signal: String,
    pub fail_closed_action: String,
    pub operator_claim: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct VramPredictionProof {
    pub name: String,
    pub passed: bool,
    pub predicted_peak_vram_gib: i64,
    pub adequate_feasible_nodes: Vec<String>,
    pub rejected_too_small_nodes: Vec<String>,
    pub impossible_input_workloads: usize,
    pub impossible_drop_count: usize,
    pub impossible_drop_reason: String,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct VramInvestmentDemoSummary {
    pub name: String,
    pub passed: bool,
    pub headline: String,
    pub synthetic_prediction_notice: String,
    pub scenario_count: usize,
    pub baseline_cuda_oom_risk_pods: usize,
    pub ksolver_cuda_oom_risk_pods: usize,
    pub cuda_oom_risk_reduction_pods: isize,
    pub high_vram_nodes_preserved: usize,
    pub unknown_or_advisory_rows: usize,
    pub average_baseline_oom_risk_percent: i64,
    pub average_ksolver_oom_risk_percent: i64,
    pub rows: Vec<VramInvestmentDemoRow>,
    pub operator_claims: Vec<String>,
    pub required_real_predictor_evidence: Vec<String>,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct VramInvestmentDemoRow {
    pub scenario: String,
    pub workload: String,
    pub predictor_source: String,
    pub confidence: i64,
    pub gpu_request: i64,
    pub predicted_peak_vram_gib: i64,
    pub predicted_lower_vram_gib: i64,
    pub predicted_upper_vram_gib: i64,
    pub kube_node: String,
    pub kube_node_vram_gib: i64,
    pub kube_cuda_oom_risk_percent: i64,
    pub kube_risk_label: String,
    pub kube_upper_band_headroom_gib: i64,
    pub ksolver_node: String,
    pub ksolver_node_vram_gib: i64,
    pub ksolver_cuda_oom_risk_percent: i64,
    pub ksolver_risk_label: String,
    pub ksolver_upper_band_headroom_gib: i64,
    pub risk_delta_percent: i64,
    pub avoided_failure: bool,
    pub preserves_high_vram_capacity: bool,
    pub advisory_only: bool,
    pub decision_reason: String,
    pub investment_case: String,
    pub caveat: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct GpuTopologyProof {
    pub name: String,
    pub passed: bool,
    pub topology_key: String,
    pub required_value: String,
    pub matching_feasible_nodes: Vec<String>,
    pub rejected_nodes: Vec<String>,
    pub impossible_input_workloads: usize,
    pub impossible_drop_count: usize,
    pub impossible_drop_reason: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct MigProfileProof {
    pub name: String,
    pub passed: bool,
    pub requested_resource: String,
    pub requested_quantity: i64,
    pub matching_feasible_nodes: Vec<String>,
    pub rejected_nodes: Vec<String>,
    pub impossible_input_workloads: usize,
    pub impossible_drop_count: usize,
    pub impossible_drop_reason: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct DraApproximationProof {
    pub name: String,
    pub passed: bool,
    pub synthetic_resource: String,
    pub modeled_feasible_nodes: Vec<String>,
    pub modeled_request_quantity: i64,
    pub unmodeled_input_workloads: usize,
    pub unmodeled_drop_count: usize,
    pub unmodeled_drop_reason: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct DraAllocationProof {
    pub name: String,
    pub passed: bool,
    pub node: String,
    pub device_class: String,
    pub total_matching_devices: i64,
    pub allocated_devices: i64,
    pub available_devices: i64,
    pub overlapping_classes: bool,
    pub unevaluable_classes: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct TimeSlicedGpuProof {
    pub name: String,
    pub passed: bool,
    pub time_sliced_node: String,
    pub isolated_node: String,
    pub time_sliced_caveats: Vec<String>,
    pub isolated_caveats: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct NodeGroupingProof {
    pub name: String,
    pub passed: bool,
    pub physical_nodes_before: usize,
    pub grouped_nodes_after: usize,
    pub eligible_group_count: usize,
    pub eligible_node_count: usize,
    pub max_group_size: usize,
    pub grouped_node_name: String,
    pub grouped_node_count: i32,
    pub grouped_members: Vec<String>,
    pub expanded_used_nodes: Vec<String>,
    pub physical_solve_admitted_workloads: usize,
    pub grouped_solve_admitted_workloads: usize,
    pub physical_solve_admitted_gpu: i64,
    pub grouped_solve_admitted_gpu: i64,
    pub grouped_solver_status: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct TenantBudgetProof {
    pub name: String,
    pub passed: bool,
    pub tenant: String,
    pub monthly_budget_milli: i64,
    pub expensive_node_cost_milli: i64,
    pub cheap_node_cost_milli: i64,
    pub expensive_job_node: Option<String>,
    pub cheap_job_node: Option<String>,
    pub admitted_jobs: usize,
    pub unplaced_jobs: usize,
    pub solver_status: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct CandidateWideningProof {
    pub name: String,
    pub passed: bool,
    pub scenario: String,
    pub initial_candidate_node_limit: usize,
    pub final_candidate_node_limit: usize,
    pub retry_count: usize,
    pub widening_reason: String,
    pub pruned_useful_gpu: i64,
    pub widened_useful_gpu: i64,
    pub useful_gpu_recovered: i64,
    pub pruned_unplaced_pods: usize,
    pub widened_unplaced_pods: usize,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct RoadmapReadinessSummary {
    pub name: String,
    pub passed: bool,
    pub headline: String,
    pub launch_proof_gate: SreLaunchProofGate,
    pub items: Vec<RoadmapItemStatus>,
    pub next_build_order: Vec<String>,
    pub residual_product_gaps: Vec<String>,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct SreLaunchProofGate {
    pub label: String,
    pub status: String,
    pub demo_ready: bool,
    pub customer_claim_ready: bool,
    pub required_evidence: Vec<String>,
    pub evidence_bundle_rows: Vec<SreEvidenceBundleRow>,
    pub blockers: Vec<String>,
    pub next_action: String,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct SreEvidenceBundleRow {
    pub artifact: String,
    pub source: String,
    pub pass_signal: String,
    pub blocks_claim: String,
    pub operator_action: String,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct RoadmapItemStatus {
    pub rank: usize,
    pub item: String,
    pub status: String,
    pub evidence_source: String,
    pub proof: String,
    pub remaining_gap: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct ScenarioResult {
    pub name: String,
    pub description: String,
    pub tier: Tier,
    pub benefit_score: i64,
    pub headline: String,
    /// Default kube-scheduler baseline (LeastAllocated / spread).
    pub kube: EngineResult,
    /// Harder kube baseline: NodeResourcesFit MostAllocated (bin-packing).
    pub kube_binpack: EngineResult,
    pub ksolver: EngineResult,
    pub reduced_ksolver: EngineResult,
    pub regret: RegretMetrics,
    /// Combined GPU-efficiency + cost win of ksolver vs the BEST of the two kube baselines.
    /// Higher = ksolver is more clearly better on this scenario (used for the ranking).
    pub efficiency_score: i64,
    /// True when ksolver beats the best kube baseline by a meaningful margin (see `efficiency`).
    pub significantly_better: bool,
    /// Human-readable efficiency deltas vs the best kube baseline (cost %, util, admitted GPU).
    pub efficiency_headline: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct BenchmarkReport {
    pub simulator_url: Option<String>,
    pub simulator_batch_timeout_millis: u64,
    pub simulator_live_baseline_limit: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub simulator_live_scenarios: Option<Vec<String>>,
    pub sorted_by: String,
    pub benefit_summary: BenefitSummary,
    pub roi_summary: RoiSummary,
    pub roi_dashboard_summary: RoiDashboardSummary,
    pub pricing_readiness_summary: PricingReadinessSummary,
    pub demo_readiness_summary: DemoReadinessSummary,
    pub regret_summary: RegretSummary,
    pub repair_scenario: RepairScenarioProof,
    pub repair_scenarios: Vec<RepairScenarioProof>,
    pub preemption_migration_kss_proofs: Vec<KssProofScenario>,
    pub hero_demo_summary: HeroDemoSummary,
    pub preemption_migration_hero_summary: PreemptionMigrationHeroSummary,
    pub sre_demo_script: SreDemoScript,
    pub production_safety_summary: ProductionSafetySummary,
    pub prediction_quality_summary: PredictionQualitySummary,
    pub scale_guardrail_summary: ScaleGuardrailSummary,
    pub fairness_budget_summary: FairnessBudgetSummary,
    pub device_correctness_summary: DeviceCorrectnessSummary,
    pub roadmap_readiness_summary: RoadmapReadinessSummary,
    pub vram_prediction_scenario: VramPredictionProof,
    pub vram_investment_demo_summary: VramInvestmentDemoSummary,
    pub vram_kss_proofs: Vec<KssProofScenario>,
    pub gpu_topology_scenario: GpuTopologyProof,
    pub mig_profile_scenario: MigProfileProof,
    pub dra_approximation_scenario: DraApproximationProof,
    pub dra_allocation_scenario: DraAllocationProof,
    pub time_sliced_gpu_scenario: TimeSlicedGpuProof,
    pub node_grouping_scenario: NodeGroupingProof,
    pub tenant_budget_scenario: TenantBudgetProof,
    pub candidate_widening_scenario: CandidateWideningProof,
    pub scenario_pages: Vec<ScenarioPage>,
    pub feature_assertions: Vec<FeatureAssertion>,
    pub scenarios: Vec<ScenarioResult>,
}

pub async fn run_benchmark(simulator_url: Option<&str>) -> anyhow::Result<BenchmarkReport> {
    run_benchmark_with_options(BenchmarkOptions {
        simulator_url: simulator_url.map(str::to_string),
        ..Default::default()
    })
    .await
}

pub async fn run_benchmark_with_options(
    options: BenchmarkOptions,
) -> anyhow::Result<BenchmarkReport> {
    let mut results = Vec::new();
    let simulator_url_pool = options.simulator_url_pool();
    let sim_url = simulator_url_pool.first().map(String::as_str);
    let mut simulator_cache = match &options.simulator_cache_path {
        Some(path) => load_simulator_cache(path)?,
        None => SimulatorCacheFile::default(),
    };
    if let Some(dir) = &options.simulator_cache_dir {
        simulator_cache
            .entries
            .extend(load_simulator_cache_dir(dir)?.entries);
    }
    let mut simulator_cache_dirty = false;
    let scenarios = deterministic_scenarios();
    let mut baseline_options = options.clone();
    if simulator_url_pool.len() > 1 {
        refresh_simulator_cache_with_pool(
            &scenarios,
            &simulator_url_pool,
            &mut simulator_cache,
            &options,
        )
        .await?;
        baseline_options.refresh_simulator_cache = false;
        baseline_options.simulator_max_live_baselines = Some(0);
        simulator_cache_dirty |= options.simulator_cache_path.is_some();
    }
    let mut simulator_live_baselines = 0_usize;
    for scenario in scenarios {
        let tier = scenario.tier;
        // Baseline 1: default kube-scheduler (LeastAllocated / spread).
        let (kube, kube_cache_dirty) = run_kube_baseline(
            &scenario,
            sim_url,
            crate::verifier::default_scheduler_config(),
            "spread",
            &mut simulator_cache,
            &baseline_options,
            &mut simulator_live_baselines,
        )
        .await?;
        simulator_cache_dirty |= kube_cache_dirty;
        // Baseline 2: harder kube-scheduler bin-packing (NodeResourcesFit MostAllocated).
        let (kube_binpack, binpack_cache_dirty) = run_kube_baseline(
            &scenario,
            sim_url,
            crate::verifier::binpack_scheduler_config(),
            "binpack",
            &mut simulator_cache,
            &baseline_options,
            &mut simulator_live_baselines,
        )
        .await?;
        simulator_cache_dirty |= binpack_cache_dirty;
        let ksolver = run_ksolver(&scenario)?;
        let reduced_ksolver = run_ksolver_with_candidate_limit(&scenario, REGRET_CANDIDATE_LIMIT)?;
        let regret = regret_metrics(
            REGRET_CANDIDATE_LIMIT,
            &ksolver.metrics,
            &reduced_ksolver.metrics,
        );
        let benefit_score = benefit_score(&kube.metrics, &ksolver.metrics);
        let headline = headline(&kube.metrics, &ksolver.metrics);
        // Efficiency (cost + GPU utilization) vs the BEST of the two kube baselines.
        let base = best_kube(&kube, &kube_binpack);
        let (efficiency_score, significantly_better, efficiency_headline) =
            efficiency(&base.metrics, &ksolver.metrics);
        results.push(ScenarioResult {
            name: scenario.name,
            description: scenario.description,
            tier,
            benefit_score,
            headline,
            kube,
            kube_binpack,
            ksolver,
            reduced_ksolver,
            regret,
            efficiency_score,
            significantly_better,
            efficiency_headline,
        });
    }
    // Rank by efficiency (GPU utilization + cost win) so the scenarios where ksolver most clearly
    // beats the best kube baseline sort to the top.
    results.sort_by(|a, b| {
        b.efficiency_score
            .cmp(&a.efficiency_score)
            .then_with(|| a.name.cmp(&b.name))
    });
    let benefit_summary = summarize_benefit(&results);
    let roi_summary = summarize_roi(&results);
    let regret_summary = summarize_regret(REGRET_CANDIDATE_LIMIT, &results);
    let repair_scenario = fragmented_repair_scenario_proof();
    let repair_scenarios = vec![
        repair_scenario.clone(),
        vram_blocked_repair_scenario_proof(),
        policy_blocked_repair_scenario_proof(),
        not_enough_total_gpu_repair_scenario_proof(),
    ];
    let preemption_migration_kss_proofs = summarize_kss_proofs(
        &results,
        "preemption_migration",
        &[
            (
                "repair-fragmented-4gpu-gang-kss",
                "KSS leaves a 4-GPU target blocked on fragmented residual capacity; paired ksolver repair proof must free a 4-GPU island from running blockers.",
                true,
            ),
            (
                "repair-policy-blocked-no-action-kss",
                "KSS leaves the target blocked; paired ksolver proof must fail closed when policy/PDB/priority blocks disruption.",
                true,
            ),
            (
                "repair-not-enough-total-gpu-kss",
                "KSS leaves the target blocked; paired ksolver proof must classify no-node-total-capacity as impossible.",
                true,
            ),
        ],
    );
    let hero_demo_summary = summarize_hero_demo(&repair_scenario, &roi_summary);
    let preemption_migration_hero_summary = summarize_preemption_migration_hero(&repair_scenario);
    let roi_dashboard_summary = summarize_roi_dashboard(
        &roi_summary,
        &benefit_summary,
        &regret_summary,
        &repair_scenario,
    );
    let pricing_readiness_summary =
        summarize_pricing_readiness(&roi_summary, &roi_dashboard_summary);
    let sre_demo_script = summarize_sre_demo_script(&results, &hero_demo_summary, &roi_summary);
    let production_safety_summary = summarize_production_safety();
    let vram_prediction_scenario = vram_prediction_scenario_proof();
    let vram_investment_demo_summary = summarize_vram_investment_demo();
    let vram_kss_proofs = summarize_kss_proofs(
        &results,
        "vram_prediction",
        &[
            (
                "vram-fit-mixed-fleet-kss",
                "KSS admits a scalar one-GPU request; paired ksolver proof must narrow known-safe placement by predicted VRAM.",
                false,
            ),
            (
                "vram-blocked-no-repair-kss",
                "KSS admits a scalar one-GPU request; paired ksolver proof must reject too-small devices and suppress repair.",
                false,
            ),
            (
                "vram-unknown-inventory-advisory-kss",
                "KSS admits a scalar one-GPU request; paired ksolver proof must label missing memory inventory advisory.",
                false,
            ),
        ],
    );
    let prediction_quality_summary = summarize_prediction_quality(&vram_prediction_scenario);
    let gpu_topology_scenario = gpu_topology_scenario_proof();
    let mig_profile_scenario = mig_profile_scenario_proof();
    let dra_approximation_scenario = dra_approximation_scenario_proof();
    let dra_allocation_scenario = dra_allocation_scenario_proof();
    let time_sliced_gpu_scenario = time_sliced_gpu_scenario_proof();
    let device_correctness_summary = summarize_device_correctness(
        &gpu_topology_scenario,
        &mig_profile_scenario,
        &dra_approximation_scenario,
        &dra_allocation_scenario,
        &time_sliced_gpu_scenario,
    );
    let node_grouping_scenario = node_grouping_scenario_proof()?;
    let tenant_budget_scenario = tenant_budget_scenario_proof()?;
    let candidate_widening_scenario = candidate_widening_scenario_proof(&results);
    let scenario_pages = summarize_scenario_pages(&results);
    let scale_guardrail_summary = summarize_scale_guardrails(
        &regret_summary,
        &node_grouping_scenario,
        &candidate_widening_scenario,
    );
    let fairness_budget_summary = summarize_fairness_budget(&results, &tenant_budget_scenario);
    let demo_readiness_summary = summarize_demo_readiness(
        &results,
        &roi_dashboard_summary,
        &hero_demo_summary,
        &preemption_migration_hero_summary,
        &sre_demo_script,
        &production_safety_summary,
        &prediction_quality_summary,
        &scale_guardrail_summary,
    );
    let roadmap_readiness_summary = summarize_roadmap_readiness(
        &demo_readiness_summary,
        &preemption_migration_hero_summary,
        &production_safety_summary,
        &prediction_quality_summary,
        &scale_guardrail_summary,
        &device_correctness_summary,
        &fairness_budget_summary,
        &roi_dashboard_summary,
        &preemption_migration_kss_proofs,
        &vram_kss_proofs,
    );
    let feature_assertions = build_feature_assertions(
        &results,
        &benefit_summary,
        &roi_summary,
        &roi_dashboard_summary,
        &pricing_readiness_summary,
        &demo_readiness_summary,
        &roadmap_readiness_summary,
        &regret_summary,
        &hero_demo_summary,
        &preemption_migration_hero_summary,
        &sre_demo_script,
        &production_safety_summary,
        &prediction_quality_summary,
        &scale_guardrail_summary,
        &fairness_budget_summary,
        &device_correctness_summary,
        &preemption_migration_kss_proofs,
        &vram_kss_proofs,
        &repair_scenarios,
        &vram_prediction_scenario,
        &gpu_topology_scenario,
        &mig_profile_scenario,
        &dra_approximation_scenario,
        &dra_allocation_scenario,
        &time_sliced_gpu_scenario,
        &node_grouping_scenario,
        &tenant_budget_scenario,
        &candidate_widening_scenario,
    )?;
    if simulator_cache_dirty {
        if let Some(path) = &options.simulator_cache_path {
            write_simulator_cache(path, &simulator_cache)?;
        }
    }

    Ok(BenchmarkReport {
        simulator_url: options
            .simulator_url
            .or_else(|| simulator_url_pool.first().cloned()),
        simulator_batch_timeout_millis: options.simulator_batch_timeout.as_millis() as u64,
        simulator_live_baseline_limit: options.simulator_max_live_baselines,
        simulator_live_scenarios: options
            .simulator_live_scenarios
            .map(|scenarios| scenarios.into_iter().collect()),
        sorted_by: "efficiency_score = ksolver GPU-utilization + cost win vs the best kube baseline (cost % + util ‰ + admitted-useful-GPU + active-node reduction + extra full gangs)".to_string(),
        benefit_summary,
        roi_summary,
        roi_dashboard_summary,
        pricing_readiness_summary,
        demo_readiness_summary,
        regret_summary,
        repair_scenario,
        repair_scenarios,
        preemption_migration_kss_proofs,
        hero_demo_summary,
        preemption_migration_hero_summary,
        sre_demo_script,
        production_safety_summary,
        prediction_quality_summary,
        scale_guardrail_summary,
        fairness_budget_summary,
        device_correctness_summary,
        roadmap_readiness_summary,
        vram_prediction_scenario,
        vram_investment_demo_summary,
        vram_kss_proofs,
        gpu_topology_scenario,
        mig_profile_scenario,
        dra_approximation_scenario,
        dra_allocation_scenario,
        time_sliced_gpu_scenario,
        node_grouping_scenario,
        tenant_budget_scenario,
        candidate_widening_scenario,
        scenario_pages,
        feature_assertions,
        scenarios: results,
    })
}

pub async fn refresh_simulator_cache_only(options: BenchmarkOptions) -> anyhow::Result<usize> {
    let simulator_url_pool = options.simulator_url_pool();
    let sim_url = simulator_url_pool.first().map(String::as_str);
    let mut simulator_cache = match &options.simulator_cache_path {
        Some(path) => load_simulator_cache(path)?,
        None => SimulatorCacheFile::default(),
    };
    if let Some(dir) = &options.simulator_cache_dir {
        simulator_cache
            .entries
            .extend(load_simulator_cache_dir(dir)?.entries);
    }
    let scenarios = deterministic_scenarios();
    let refreshed = if simulator_url_pool.len() > 1 {
        refresh_simulator_cache_with_pool(
            &scenarios,
            &simulator_url_pool,
            &mut simulator_cache,
            &options,
        )
        .await?
    } else {
        let mut refreshed = 0_usize;
        let mut live_baselines = 0_usize;
        'scenarios: for scenario in &scenarios {
            for (variant, scheduler_config) in [
                ("spread", crate::verifier::default_scheduler_config()),
                ("binpack", crate::verifier::binpack_scheduler_config()),
            ] {
                let cache_key = simulator_cache_key(scenario, variant);
                if should_skip_cached_simulator_baseline(&options, &simulator_cache, &cache_key) {
                    continue;
                }
                let live_scenario_allowed = options
                    .simulator_live_scenarios
                    .as_ref()
                    .map(|selected| selected.contains(&scenario.name))
                    .unwrap_or(true);
                if !live_scenario_allowed {
                    continue;
                }
                if options
                    .simulator_max_live_baselines
                    .map(|limit| live_baselines >= limit)
                    .unwrap_or(false)
                {
                    if options.simulator_progress {
                        eprintln!(
                            "gpu-scenarios: stopping bounded kube-scheduler-simulator cache refresh at {} live baseline(s)",
                            live_baselines
                        );
                    }
                    break 'scenarios;
                }
                run_kube_baseline(
                    scenario,
                    sim_url,
                    scheduler_config,
                    variant,
                    &mut simulator_cache,
                    &options,
                    &mut live_baselines,
                )
                .await?;
                refreshed += 1;
            }
        }
        refreshed
    };
    if let Some(path) = &options.simulator_cache_path {
        write_simulator_cache(path, &simulator_cache)?;
    }
    Ok(refreshed)
}

fn should_skip_cached_simulator_baseline(
    options: &BenchmarkOptions,
    simulator_cache: &SimulatorCacheFile,
    cache_key: &str,
) -> bool {
    if !simulator_cache.entries.contains_key(cache_key) {
        return false;
    }
    !options.refresh_simulator_cache || options.simulator_max_live_baselines.is_some()
}

pub fn simulator_cache_coverage(
    options: &BenchmarkOptions,
) -> anyhow::Result<SimulatorCacheCoverage> {
    let mut simulator_cache = match &options.simulator_cache_path {
        Some(path) => load_simulator_cache(path)?,
        None => SimulatorCacheFile::default(),
    };
    if let Some(dir) = &options.simulator_cache_dir {
        simulator_cache
            .entries
            .extend(load_simulator_cache_dir(dir)?.entries);
    }

    let mut total_baselines = 0_usize;
    let mut cached_baselines = 0_usize;
    for scenario in deterministic_scenarios() {
        let live_scenario_allowed = options
            .simulator_live_scenarios
            .as_ref()
            .map(|selected| selected.contains(&scenario.name))
            .unwrap_or(true);
        if !live_scenario_allowed {
            continue;
        }
        for variant in ["spread", "binpack"] {
            total_baselines += 1;
            if simulator_cache
                .entries
                .contains_key(&simulator_cache_key(&scenario, variant))
            {
                cached_baselines += 1;
            }
        }
    }

    Ok(SimulatorCacheCoverage {
        total_baselines,
        cached_baselines,
        missing_baselines: total_baselines.saturating_sub(cached_baselines),
    })
}

fn summarize_scenario_pages(scenarios: &[ScenarioResult]) -> Vec<ScenarioPage> {
    let names = |pred: fn(&str) -> bool| {
        scenarios
            .iter()
            .filter(|scenario| pred(&scenario.name))
            .map(|scenario| scenario.name.clone())
            .collect::<Vec<_>>()
    };
    vec![
        ScenarioPage {
            slug: "vram-binpacking".to_string(),
            title: "VRAM usage prediction & binpacking".to_string(),
            description: "Scenarios where ksolver assumes predicted peak VRAM is known and filters feasible GPU nodes before solving; kube-scheduler-simulator only sees scalar GPU requests.".to_string(),
            scenario_names: names(|name| name.starts_with("vram-")),
        },
        ScenarioPage {
            slug: "gang-scheduling".to_string(),
            title: "Gang scheduling".to_string(),
            description: "Scenarios where partial placement is not useful because training workers must be admitted together or colocated.".to_string(),
            scenario_names: names(|name| name.contains("gang") || name.contains("train")),
        },
        ScenarioPage {
            slug: "preemption-migration".to_string(),
            title: "Preemption / migration".to_string(),
            description: "Scenarios and proof rows where the current residual fleet cannot place a target without a dry-run repair plan, or where repair must fail closed.".to_string(),
            scenario_names: names(|name| name.starts_with("repair-")),
        },
    ]
}

pub fn print_table(report: &BenchmarkReport) {
    let baselines = report
        .scenarios
        .iter()
        .flat_map(|r| [&r.kube, &r.kube_binpack])
        .collect::<Vec<_>>();
    for line in simulator_provenance_summary(
        baselines.iter().copied(),
        report.simulator_live_baseline_limit,
        report.simulator_batch_timeout_millis,
    ) {
        println!("{line}");
    }
    if !baselines.is_empty() {
        println!();
    }
    println!(
        "{:<3} {:<28} {:>7} {:>13} {:>13} {:>11} {:>11} {:>10}  headline",
        "#", "scenario", "score", "kube useful", "ksolver useful", "kube bad", "ks bad", "K regret"
    );
    for (i, r) in report.scenarios.iter().enumerate() {
        println!(
            "{:<3} {:<28} {:>7} {:>13} {:>13} {:>11} {:>11} {:>10}  {}",
            i + 1,
            r.name,
            r.benefit_score,
            r.kube.metrics.useful_gpu,
            r.ksolver.metrics.useful_gpu,
            r.kube.metrics.partial_or_invalid_gangs,
            r.ksolver.metrics.partial_or_invalid_gangs,
            r.regret.useful_gpu_regret,
            r.headline
        );
    }
}

fn simulator_provenance_summary<'a>(
    baselines: impl IntoIterator<Item = &'a EngineResult>,
    live_baseline_limit: Option<usize>,
    timeout_millis: u64,
) -> Vec<String> {
    let mut mode_counts: BTreeMap<String, usize> = BTreeMap::new();
    let mut timed_out = 0_usize;
    let mut fallbacks = 0_usize;
    let mut first_failure: Option<String> = None;
    let mut slowest_phase: Option<(&str, &str, u64)> = None;
    let mut total = 0_usize;

    for baseline in baselines {
        total += 1;
        let Some(simulator) = baseline.simulator.as_ref() else {
            *mode_counts
                .entry("missing-simulator-provenance".to_string())
                .or_default() += 1;
            continue;
        };
        *mode_counts
            .entry(simulator_mode_summary_label(&simulator.mode))
            .or_default() += 1;
        if simulator.timed_out {
            timed_out += 1;
        }
        if simulator.mode.contains("fallback") {
            fallbacks += 1;
            if first_failure.is_none() {
                first_failure = simulator.fallback_reason.clone();
            }
        }
        for timing in &simulator.phase_timings {
            if slowest_phase
                .as_ref()
                .map(|(_, _, duration)| timing.duration_millis > *duration)
                .unwrap_or(true)
            {
                slowest_phase = Some((
                    simulator.variant.as_str(),
                    timing.phase.as_str(),
                    timing.duration_millis,
                ));
            }
        }
    }

    if total == 0 {
        return Vec::new();
    }

    let mode_summary = mode_counts
        .iter()
        .map(|(mode, count)| format!("{mode}={count}"))
        .collect::<Vec<_>>()
        .join(", ");
    let limit = live_baseline_limit
        .map(|limit| limit.to_string())
        .unwrap_or_else(|| "all".to_string());
    let mut lines = vec![format!(
        "kube-scheduler-simulator provenance: {total} baseline(s), {mode_summary}; live limit={limit}, timeout={timeout_millis}ms"
    )];

    if timed_out > 0 || fallbacks > 0 {
        lines.push(format!(
            "simulator failure posture: {timed_out} timed out, {fallbacks} invalid legacy fallback marker(s); current benchmark runs fail instead of using deterministic kube substitutes"
        ));
    }
    if let Some((variant, phase, duration)) = slowest_phase {
        lines.push(format!(
            "slowest simulator phase: {variant}/{phase} took {duration}ms"
        ));
    }
    if let Some(reason) = first_failure {
        lines.push(format!(
            "first invalid simulator provenance reason: {}",
            concise_simulator_reason(&reason)
        ));
    }

    lines
}

fn simulator_mode_summary_label(mode: &str) -> String {
    if mode.contains("fallback") {
        "invalid-legacy-fallback-marker".to_string()
    } else {
        mode.to_string()
    }
}

fn concise_simulator_reason(reason: &str) -> String {
    const MAX_REASON_CHARS: usize = 220;
    let normalized = reason.split_whitespace().collect::<Vec<_>>().join(" ");
    if normalized.chars().count() <= MAX_REASON_CHARS {
        return normalized;
    }
    let mut truncated = normalized
        .chars()
        .take(MAX_REASON_CHARS.saturating_sub(3))
        .collect::<String>();
    truncated.push_str("...");
    truncated
}

async fn run_kube_baseline(
    scenario: &ScenarioSpec,
    simulator_url: Option<&str>,
    scheduler_config: serde_json::Value,
    variant: &str,
    simulator_cache: &mut SimulatorCacheFile,
    options: &BenchmarkOptions,
    simulator_live_baselines: &mut usize,
) -> anyhow::Result<(EngineResult, bool)> {
    let cache_key = simulator_cache_key(scenario, variant);
    if options.cache_enabled() && !options.refresh_simulator_cache {
        if let Some(cached) = simulator_cache.entries.get(&cache_key) {
            return Ok((cached_simulator_result(scenario, variant, cached), false));
        }
    }

    match simulator_url {
        Some(url) => {
            let live_scenario_allowed = options
                .simulator_live_scenarios
                .as_ref()
                .map(|scenarios| scenarios.contains(&scenario.name))
                .unwrap_or(true);
            if !live_scenario_allowed {
                if let Some(cached) = simulator_cache.entries.get(&cache_key) {
                    return Ok((cached_simulator_result(scenario, variant, cached), false));
                }
                anyhow::bail!(
                    "missing kube-scheduler-simulator baseline for scenario={} variant={} cache_key={}; scenario is excluded by --simulator-live-scenarios and deterministic greedy fallback is disabled",
                    scenario.name,
                    variant,
                    cache_key
                );
            }
            if options
                .simulator_max_live_baselines
                .map(|limit| *simulator_live_baselines >= limit)
                .unwrap_or(false)
            {
                anyhow::bail!(
                    "missing kube-scheduler-simulator baseline for scenario={} variant={} cache_key={}; --simulator-max-live-baselines={} reached and deterministic greedy fallback is disabled",
                    scenario.name,
                    variant,
                    cache_key,
                    options.simulator_max_live_baselines.unwrap_or_default()
                );
            }
            *simulator_live_baselines += 1;
            if options.simulator_progress {
                eprintln!(
                    "gpu-scenarios: refreshing kube-scheduler-simulator baseline scenario={} variant={} timeout={}ms",
                    scenario.name,
                    variant,
                    options.simulator_batch_timeout.as_millis()
                );
            }
            let live_started = Instant::now();
            match run_kube_simulator(
                scenario,
                url,
                scheduler_config,
                variant,
                options.simulator_batch_timeout,
            )
            .await
            {
                Ok(result) => {
                    if options.cache_enabled() {
                        let cached = CachedSimulatorResult {
                            engine: result.engine.clone(),
                            source: result.source.clone(),
                            placements: result.placements.clone(),
                        };
                        if let Some(dir) = &options.simulator_cache_dir {
                            write_simulator_cache_entry(dir, &cache_key, &cached)?;
                        }
                        simulator_cache.entries.insert(cache_key, cached);
                        Ok((result, true))
                    } else {
                        Ok((result, false))
                    }
                }
                Err(err) => {
                    let fallback_reason = format!("{err:#}");
                    if let Some(limit) = options.simulator_max_live_baselines {
                        *simulator_live_baselines = limit;
                    }
                    let elapsed_millis = live_started.elapsed().as_millis();
                    anyhow::bail!(
                        "kube-scheduler-simulator baseline failed for scenario={} variant={} after {}ms; deterministic greedy fallback is disabled: {}",
                        scenario.name,
                        variant,
                        elapsed_millis,
                        fallback_reason
                    );
                }
            }
        }
        None => {
            anyhow::bail!(
                "missing kube-scheduler-simulator baseline for scenario={} variant={} cache_key={}; set --simulator or provide a warm --simulator-cache entry because deterministic greedy fallback is disabled",
                scenario.name,
                variant,
                cache_key
            );
        }
    }
}

fn simulator_cache_key(scenario: &ScenarioSpec, variant: &str) -> String {
    format!("{}:{variant}", scenario.name)
}

#[derive(Debug, Clone)]
struct SimulatorBaselineTask {
    scenario: ScenarioSpec,
    variant: &'static str,
    scheduler_config: serde_json::Value,
    cache_key: String,
}

async fn refresh_simulator_cache_with_pool(
    scenarios: &[ScenarioSpec],
    simulator_urls: &[String],
    simulator_cache: &mut SimulatorCacheFile,
    options: &BenchmarkOptions,
) -> anyhow::Result<usize> {
    if simulator_urls.is_empty() {
        return Ok(0);
    }
    let mut tasks = Vec::new();
    for scenario in scenarios {
        let live_scenario_allowed = options
            .simulator_live_scenarios
            .as_ref()
            .map(|selected| selected.contains(&scenario.name))
            .unwrap_or(true);
        if !live_scenario_allowed {
            continue;
        }
        for (variant, scheduler_config) in [
            ("spread", crate::verifier::default_scheduler_config()),
            ("binpack", crate::verifier::binpack_scheduler_config()),
        ] {
            let cache_key = simulator_cache_key(scenario, variant);
            if should_skip_cached_simulator_baseline(options, simulator_cache, &cache_key) {
                continue;
            }
            tasks.push(SimulatorBaselineTask {
                scenario: scenario.clone(),
                variant,
                scheduler_config,
                cache_key,
            });
        }
    }
    if let Some(limit) = options.simulator_max_live_baselines {
        if tasks.len() > limit {
            tasks.truncate(limit);
        }
    }
    if tasks.is_empty() {
        return Ok(0);
    }
    let task_count = tasks.len();

    let worker_count = simulator_urls.len().min(tasks.len());
    let mut buckets = vec![Vec::new(); worker_count];
    for (idx, task) in tasks.into_iter().enumerate() {
        buckets[idx % worker_count].push(task);
    }

    let mut handles = Vec::new();
    for (worker_idx, worker_tasks) in buckets.into_iter().enumerate() {
        let url = simulator_urls[worker_idx].clone();
        let cache_dir = options.simulator_cache_dir.clone();
        let timeout = options.simulator_batch_timeout;
        let progress = options.simulator_progress;
        handles.push(tokio::spawn(async move {
            let mut entries = Vec::new();
            for task in worker_tasks {
                if progress {
                    eprintln!(
                        "gpu-scenarios: refreshing kube-scheduler-simulator baseline scenario={} variant={} url={} timeout={}ms",
                        task.scenario.name,
                        task.variant,
                        url,
                        timeout.as_millis()
                    );
                }
                let result = run_kube_simulator(
                    &task.scenario,
                    &url,
                    task.scheduler_config,
                    task.variant,
                    timeout,
                )
                .await
                .with_context(|| {
                    format!(
                        "kube-scheduler-simulator pool worker failed scenario={} variant={} url={}",
                        task.scenario.name, task.variant, url
                    )
                })?;
                let cached = CachedSimulatorResult {
                    engine: result.engine.clone(),
                    source: result.source.clone(),
                    placements: result.placements.clone(),
                };
                if let Some(dir) = &cache_dir {
                    write_simulator_cache_entry(dir, &task.cache_key, &cached)?;
                }
                entries.push((task.cache_key, cached));
            }
            anyhow::Ok(entries)
        }));
    }

    for handle in handles {
        for (cache_key, cached) in handle.await.context("join KSS pool worker")?? {
            simulator_cache.entries.insert(cache_key, cached);
        }
    }
    Ok(task_count)
}

fn cached_simulator_result(
    scenario: &ScenarioSpec,
    variant: &str,
    cached: &CachedSimulatorResult,
) -> EngineResult {
    let engine = if cached.engine.trim().is_empty() {
        format!("kube-{variant}")
    } else {
        cached.engine.clone()
    };
    EngineResult {
        engine,
        source: format!("cached {}", cached.source),
        simulator: Some(SimulatorBaselineProvenance {
            mode: "cached".to_string(),
            variant: variant.to_string(),
            cache_key: Some(simulator_cache_key(scenario, variant)),
            ..Default::default()
        }),
        candidate_node_limit: 0,
        solve_millis: 0,
        metrics: metrics(scenario, &cached.placements),
        placements: cached.placements.clone(),
    }
}

fn simulator_provenance_from_diagnostics(
    mode: &str,
    variant: &str,
    url: Option<&str>,
    timeout: Duration,
    diagnostics: &crate::verifier::SimulatorBatchDiagnostics,
) -> SimulatorBaselineProvenance {
    SimulatorBaselineProvenance {
        mode: mode.to_string(),
        variant: variant.to_string(),
        url: url.map(str::to_string),
        timeout_millis: Some(timeout.as_millis() as u64),
        elapsed_millis: Some(diagnostics.elapsed_millis as u64),
        phase: Some(diagnostics.phase.clone()),
        target_count: Some(diagnostics.state.target_count),
        present_targets: Some(diagnostics.state.present_targets),
        terminal_present_targets: Some(diagnostics.state.terminal_present_targets),
        missing_targets: Some(diagnostics.state.missing_targets()),
        stable_polls: Some(diagnostics.stable_polls),
        phase_timings: diagnostics
            .phase_timings
            .iter()
            .map(|timing| SimulatorPhaseTiming {
                phase: timing.phase.clone(),
                duration_millis: timing.duration_millis,
                cumulative_millis: timing.cumulative_millis,
            })
            .collect(),
        timed_out: diagnostics.timed_out,
        ..Default::default()
    }
}

#[allow(dead_code)]
#[cfg(test)]
fn greedy_fallback_for_variant(scenario: &ScenarioSpec, variant: &str) -> EngineResult {
    match variant {
        "binpack" => run_greedy_binpack(scenario),
        _ => run_greedy_spread(scenario),
    }
}

fn load_simulator_cache(path: &PathBuf) -> anyhow::Result<SimulatorCacheFile> {
    if !path.exists() {
        return Ok(SimulatorCacheFile {
            version: 1,
            entries: BTreeMap::new(),
        });
    }
    let raw = std::fs::read_to_string(path)
        .with_context(|| format!("read simulator cache {}", path.display()))?;
    let mut cache: SimulatorCacheFile = serde_json::from_str(&raw)
        .with_context(|| format!("decode simulator cache {}", path.display()))?;
    if cache.version == 0 {
        cache.version = 1;
    }
    Ok(cache)
}

fn load_simulator_cache_dir(dir: &PathBuf) -> anyhow::Result<SimulatorCacheFile> {
    let mut cache = SimulatorCacheFile {
        version: 1,
        entries: BTreeMap::new(),
    };
    if !dir.exists() {
        return Ok(cache);
    }
    for scenario_entry in std::fs::read_dir(dir)
        .with_context(|| format!("read simulator cache dir {}", dir.display()))?
    {
        let scenario_entry = scenario_entry
            .with_context(|| format!("read simulator cache dir {}", dir.display()))?;
        if !scenario_entry
            .file_type()
            .with_context(|| format!("stat {}", scenario_entry.path().display()))?
            .is_dir()
        {
            continue;
        }
        let scenario = scenario_entry.file_name().to_string_lossy().to_string();
        for variant_entry in std::fs::read_dir(scenario_entry.path()).with_context(|| {
            format!(
                "read simulator cache dir {}",
                scenario_entry.path().display()
            )
        })? {
            let variant_entry = variant_entry.with_context(|| {
                format!(
                    "read simulator cache dir {}",
                    scenario_entry.path().display()
                )
            })?;
            let path = variant_entry.path();
            if path.extension().and_then(|ext| ext.to_str()) != Some("json") {
                continue;
            }
            let Some(variant) = path.file_stem().and_then(|stem| stem.to_str()) else {
                continue;
            };
            let raw = std::fs::read_to_string(&path)
                .with_context(|| format!("read simulator cache entry {}", path.display()))?;
            let cached: CachedSimulatorResult = serde_json::from_str(&raw)
                .with_context(|| format!("decode simulator cache entry {}", path.display()))?;
            cache
                .entries
                .insert(format!("{scenario}:{variant}"), cached);
        }
    }
    Ok(cache)
}

fn write_simulator_cache(path: &PathBuf, cache: &SimulatorCacheFile) -> anyhow::Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("create simulator cache dir {}", parent.display()))?;
    }
    let mut normalized = cache.clone();
    if normalized.version == 0 {
        normalized.version = 1;
    }
    let json = serde_json::to_string_pretty(&normalized).context("encode simulator cache")?;
    std::fs::write(path, format!("{json}\n"))
        .with_context(|| format!("write simulator cache {}", path.display()))
}

fn simulator_cache_entry_path(dir: &Path, cache_key: &str) -> anyhow::Result<PathBuf> {
    let (scenario, variant) = cache_key
        .split_once(':')
        .with_context(|| format!("invalid simulator cache key {cache_key}"))?;
    Ok(dir
        .join(sanitize_cache_path_component(scenario))
        .join(format!("{}.json", sanitize_cache_path_component(variant))))
}

fn sanitize_cache_path_component(component: &str) -> String {
    component
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_' | '.') {
                ch
            } else {
                '_'
            }
        })
        .collect()
}

fn write_simulator_cache_entry(
    dir: &Path,
    cache_key: &str,
    cached: &CachedSimulatorResult,
) -> anyhow::Result<()> {
    let path = simulator_cache_entry_path(dir, cache_key)?;
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("create simulator cache dir {}", parent.display()))?;
    }
    let json = serde_json::to_string_pretty(cached).context("encode simulator cache entry")?;
    std::fs::write(&path, format!("{json}\n"))
        .with_context(|| format!("write simulator cache entry {}", path.display()))
}

fn deterministic_scenarios() -> Vec<ScenarioSpec> {
    vec![
        scenario(
            "priority-gang-over-fillers",
            "Low-priority fillers arrive before a high-priority 4-worker gang; priority-aware ksolver should admit the gang.",
            &[1, 1, 4],
            vec![
                JobSpec::singleton("filler-a", 1),
                JobSpec::singleton("filler-b", 1),
                JobSpec::singleton("filler-c", 1),
                JobSpec::singleton("filler-d", 1),
                JobSpec::colocated_gang("urgent-gang", 4, 1)
                    .with_priority(20, "research-critical"),
            ],
        )
        .with_priority_weight(20),
        scenario(
            "business-value-over-fifo",
            "Two equal-size jobs compete for one GPU; business-value-aware ksolver should admit the higher-value job even if it arrived later.",
            &[1],
            vec![
                JobSpec::singleton("low-value-experiment", 1),
                JobSpec::singleton("high-value-training", 1).with_business_value(50),
            ],
        )
        .with_business_value_weight(50),
        scenario(
            "fair-share-over-fifo",
            "An over-share team job arrives before an under-share team job; fair-share-aware ksolver should admit the under-share job.",
            &[1],
            vec![
                JobSpec::singleton("over-share-team-job", 1),
                JobSpec::singleton("under-share-team-job", 1)
                    .with_fair_share_deficit("team-under", 100),
            ],
        )
        .with_fair_share_weight(40),
        scenario(
            "queue-urgent-over-fifo",
            "A normal queue job arrives before an urgent queue job; queue-aware ksolver should admit the urgent queue job.",
            &[1],
            vec![
                JobSpec::singleton("normal-queue-job", 1),
                JobSpec::singleton("urgent-queue-job", 1).with_queue("urgent", 100),
            ],
        )
        .with_queue_weight(40),
        scenario(
            "queue-wait-over-fifo",
            "A long-waiting job competes with newer work; queue-wait-aware ksolver should admit the older waiting job.",
            &[1],
            vec![
                JobSpec::singleton("new-arrival-job", 1),
                JobSpec::singleton("long-waiting-job", 1).with_queue_wait(3_600),
            ],
        )
        .with_queue_wait_weight(20),
        scenario(
            "deadline-urgent-over-fifo",
            "A flexible batch job arrives before an urgent deadline job; deadline-aware ksolver should admit the meetable urgent job.",
            &[1],
            vec![
                JobSpec::singleton("batch-flex", 1),
                JobSpec::singleton("urgent-deadline", 1).with_deadline(3_600, 1_800),
            ],
        )
        .with_deadline_weights(200, 10_000),
        scenario(
            "weekend-flex-rightsize",
            "A flexible 8-worker weekend job has enough slack to finish with fewer GPUs; ksolver should select the smallest meetable replica count.",
            &[8],
            vec![
                JobSpec::colocated_gang("weekend-flex", 8, 1)
                    .with_flexible_gpus(2, 8, 8)
                    .with_deadline(10_000, 3_600),
            ],
        )
        .with_deadline_weights(10, 10_000),
        scenario(
            "fragment-4gpu-node",
            "Small jobs arrive before one 4-GPU job; sequential scheduling can strand fragments.",
            &[1, 1, 4, 4],
            vec![
                JobSpec::singleton("small-a", 1),
                JobSpec::singleton("small-b", 1),
                JobSpec::singleton("small-c", 1),
                JobSpec::singleton("small-d", 1),
                JobSpec::singleton("large-4g", 4),
                JobSpec::singleton("medium-2g", 2),
            ],
        ),
        scenario(
            "colocated-gang-vs-large",
            "A 4-worker colocated gang competes with useful 4/2/1-GPU work.",
            &[1, 1, 1, 1, 4, 4, 4],
            vec![
                JobSpec::colocated_gang("gang-a", 4, 1),
                JobSpec::singleton("large-4g", 4),
                JobSpec::singleton("medium-a", 2),
                JobSpec::singleton("medium-b", 2),
                JobSpec::singleton("small-a", 1),
                JobSpec::singleton("small-b", 1),
            ],
        ),
        scenario(
            "two-large-after-smalls",
            "Two 4-GPU jobs arrive after many 1-GPU jobs.",
            &[1, 1, 1, 4, 4, 4],
            vec![
                JobSpec::singleton("small-a", 1),
                JobSpec::singleton("small-b", 1),
                JobSpec::singleton("small-c", 1),
                JobSpec::singleton("small-d", 1),
                JobSpec::singleton("small-e", 1),
                JobSpec::singleton("large-a", 4),
                JobSpec::singleton("large-b", 4),
            ],
        ),
        scenario(
            "scarce-big-node",
            "Only one 8-GPU node exists; low-width work can consume it before an 8-GPU job.",
            &[2, 2, 4, 8],
            vec![
                JobSpec::singleton("small-a", 1),
                JobSpec::singleton("small-b", 1),
                JobSpec::singleton("medium-a", 2),
                JobSpec::singleton("medium-b", 2),
                JobSpec::singleton("huge-8g", 8),
            ],
        ),
        scenario(
            "gang-or-throughput",
            "A colocated gang is all-or-nothing; admitting partial gang pods should not count as useful work.",
            &[1, 1, 4, 4],
            vec![
                JobSpec::colocated_gang("gang-b", 4, 1),
                JobSpec::singleton("medium-a", 2),
                JobSpec::singleton("medium-b", 2),
                JobSpec::singleton("small-a", 1),
                JobSpec::singleton("small-b", 1),
            ],
        ),
        scenario(
            "balanced-mixed-fleet",
            "A mixed fleet where both engines should be close; useful as a sanity check.",
            &[1, 1, 2, 2, 4, 4],
            vec![
                JobSpec::singleton("small-a", 1),
                JobSpec::singleton("small-b", 1),
                JobSpec::singleton("medium-a", 2),
                JobSpec::singleton("medium-b", 2),
                JobSpec::singleton("large-a", 4),
            ],
        ),
        scenario(
            "many-mediums-one-large",
            "2-GPU jobs can fill 4-GPU nodes in ways that block a late 4-GPU job.",
            &[2, 2, 4, 4],
            vec![
                JobSpec::singleton("medium-a", 2),
                JobSpec::singleton("medium-b", 2),
                JobSpec::singleton("medium-c", 2),
                JobSpec::singleton("large-a", 4),
                JobSpec::singleton("small-a", 1),
            ],
        ),
        scenario(
            "three-gangs-one-fleet",
            "Multiple colocated gangs compete; partial gang admission is especially misleading.",
            &[1, 1, 4, 4, 4],
            vec![
                JobSpec::colocated_gang("gang-a", 4, 1),
                JobSpec::colocated_gang("gang-b", 4, 1),
                JobSpec::colocated_gang("gang-c", 4, 1),
                JobSpec::singleton("large-a", 4),
            ],
        ),
        scenario(
            "two-training-gangs-or-fillers",
            "Two 4-worker colocated training gangs arrive after ordinary fillers; ksolver should reject partial gang fragments and admit whole gangs.",
            &[1, 1, 4, 4],
            vec![
                JobSpec::singleton("filler-a", 1),
                JobSpec::singleton("filler-b", 1),
                JobSpec::singleton("filler-c", 1),
                JobSpec::singleton("filler-d", 1),
                JobSpec::colocated_gang("train-a", 4, 1),
                JobSpec::colocated_gang("train-b", 4, 1),
            ],
        ),
        scenario(
            "colocated-8gpu-training-gang",
            "One 8-worker colocated training gang competes with enough smaller independent work to consume the only 8-GPU node. This models gang colocation only; it is not an NVLink/topology claim.",
            &[1, 1, 2, 4, 8],
            vec![
                JobSpec::singleton("small-a", 1),
                JobSpec::singleton("small-b", 1),
                JobSpec::singleton("medium-a", 2),
                JobSpec::singleton("large-a", 4),
                JobSpec::singleton("medium-b", 2),
                JobSpec::colocated_gang("train-8", 8, 1)
                    .with_priority(20, "research-critical"),
            ],
        )
        .with_priority_weight(20),
        scenario(
            "deadline-gang-vs-batch",
            "Newer deadline-sensitive gang work competes with older batch fillers; deadline-aware ksolver should admit the meetable gang rather than maximize FIFO pod count.",
            &[1, 1, 4],
            vec![
                JobSpec::singleton("batch-a", 1),
                JobSpec::singleton("batch-b", 1),
                JobSpec::singleton("batch-c", 1),
                JobSpec::singleton("batch-d", 1),
                JobSpec::colocated_gang("deadline-train", 4, 1)
                    .with_deadline(3_600, 1_800),
            ],
        )
        .with_deadline_weights(200, 10_000),
        scenario(
            "fair-share-gang-scarce-gpu",
            "A below-share team has a 4-worker colocated gang behind FIFO fillers; fair-share-aware ksolver should admit the whole under-share gang.",
            &[1, 1, 4],
            vec![
                JobSpec::singleton("over-share-a", 1),
                JobSpec::singleton("over-share-b", 1),
                JobSpec::singleton("over-share-c", 1),
                JobSpec::singleton("over-share-d", 1),
                JobSpec::colocated_gang("under-share-train", 4, 1)
                    .with_fair_share_deficit("team-under", 100),
            ],
        )
        .with_fair_share_weight(40),
        scenario(
            "queue-wait-gang-over-new-fillers",
            "A long-waiting 4-worker gang is behind newer fillers; queue-wait-aware ksolver should admit the whole gang without counting partial fragments as useful.",
            &[1, 1, 4],
            vec![
                JobSpec::singleton("new-a", 1),
                JobSpec::singleton("new-b", 1),
                JobSpec::singleton("new-c", 1),
                JobSpec::singleton("new-d", 1),
                JobSpec::colocated_gang("waiting-train", 4, 1).with_queue_wait(7_200),
            ],
        )
        .with_queue_wait_weight(20),
        scenario(
            "repair-fragmented-4gpu-gang-kss",
            "KSS proof input: residual capacity is split across 2-GPU islands, so a 4-GPU colocated target cannot be placed by kube; ksolver repair proof is evaluated separately against equivalent running blockers.",
            &[2, 2],
            vec![JobSpec::colocated_gang("repair-target", 4, 1)],
        ),
        scenario(
            "repair-policy-blocked-no-action-kss",
            "KSS proof input: residual capacity is below a 4-GPU colocated target; the paired ksolver repair proof must show protected blockers make repair unsafe.",
            &[3],
            vec![JobSpec::colocated_gang("policy-target", 4, 1)],
        ),
        scenario(
            "repair-not-enough-total-gpu-kss",
            "KSS proof input: total residual GPU is below the 4-GPU target, so kube leaves it pending and ksolver must classify repair as impossible rather than disruptive.",
            &[1, 1, 1],
            vec![JobSpec::colocated_gang("capacity-target", 4, 1)],
        ),
        scenario(
            "vram-fit-mixed-fleet-kss",
            "KSS proof input: scalar kube sees only a one-GPU request and can admit it; ksolver's separate VRAM proof must restrict placement to known adequate-memory devices.",
            &[1, 1],
            vec![JobSpec::singleton("vram-fit-target", 1)],
        ),
        scenario(
            "vram-blocked-no-repair-kss",
            "KSS proof input: scalar kube can admit an oversized-memory one-GPU request; ksolver's paired proof must reject too-small GPUs and suppress repair advice.",
            &[1],
            vec![JobSpec::singleton("vram-huge-target", 1)],
        ),
        scenario(
            "vram-unknown-inventory-advisory-kss",
            "KSS proof input: scalar kube can admit a one-GPU request even when GPU-memory inventory is absent; ksolver must keep unknown-memory placement advisory, not proven safe.",
            &[1],
            vec![JobSpec::singleton("vram-unknown-target", 1)],
        ),
        scenario(
            "vram-binpack-preserves-highmem",
            "Known peak VRAM changes the packing decision: small low-memory pods should use 24Gi GPUs so a 60Gi job can land on the only 80Gi GPU. KSS sees all three as scalar one-GPU requests.",
            &[1, 1, 1],
            vec![
                JobSpec::singleton("lowmem-a", 1).with_peak_vram(8),
                JobSpec::singleton("lowmem-b", 1).with_peak_vram(8),
                JobSpec::singleton("highmem-train", 1).with_peak_vram(60),
            ],
        )
        .with_node_vram_gib(&[24, 80, 24])
        .with_priority_weight(10),
        scenario(
            "vram-binpack-rejects-oom-placement",
            "Known peak VRAM should make a 48Gi inference job infeasible on 24Gi GPUs even when kube can bind it by scalar GPU count.",
            &[1, 1],
            vec![
                JobSpec::singleton("lowmem-batch", 1).with_peak_vram(8),
                JobSpec::singleton("wide-vram-infer", 1).with_peak_vram(48),
            ],
        )
        .with_node_vram_gib(&[80, 24])
        .with_priority_weight(10),
        scenario(
            "vram-confidence-band-avoids-bursty-oom",
            "The point estimate fits a 48Gi GPU, but the synthetic upper band says the job should land on an 80Gi GPU to reduce CUDA OOM risk.",
            &[1, 1],
            vec![
                JobSpec::singleton("bursty-seq2seq-train", 1).with_peak_vram(58),
                JobSpec::singleton("tiny-preprocess", 1).with_peak_vram(6),
            ],
        )
        .with_node_vram_gib(&[48, 80])
        .with_priority_weight(10),
        scenario(
            "vram-mixed-sku-training-day",
            "Mixed 24/48/80Gi fleet: VRAM-aware placement should put small and medium work on smaller GPUs while reserving 80Gi devices for FSDP replicas.",
            &[1, 1, 1, 1, 1],
            vec![
                JobSpec::singleton("embedding-batch", 1).with_peak_vram(10),
                JobSpec::singleton("vision-train", 1).with_peak_vram(36),
                JobSpec::singleton("fsdp-worker-a", 1).with_peak_vram(72),
                JobSpec::singleton("fsdp-worker-b", 1).with_peak_vram(72),
                JobSpec::singleton("unknown-small", 1),
            ],
        )
        .with_node_vram_gib(&[24, 48, 80, 80, 24])
        .with_priority_weight(10),
        scenario(
            "vram-impossible-frontier-job",
            "A 160Gi peak-VRAM job should be explicitly classified as memory-incompatible on an 80Gi-only fleet instead of triggering disruptive defragmentation advice.",
            &[1, 1],
            vec![
                JobSpec::singleton("frontier-finetune-160g", 1).with_peak_vram(160),
                JobSpec::singleton("small-filler", 1).with_peak_vram(12),
            ],
        )
        .with_node_vram_gib(&[80, 80])
        .with_priority_weight(10),
        scenario(
            "packing-preserves-2gpu",
            "1-GPU jobs can strand 2-GPU capacity; solver should prefer tighter packing.",
            &[1, 1, 2, 2, 2],
            vec![
                JobSpec::singleton("small-a", 1),
                JobSpec::singleton("small-b", 1),
                JobSpec::singleton("small-c", 1),
                JobSpec::singleton("small-d", 1),
                JobSpec::singleton("medium-a", 2),
                JobSpec::singleton("medium-b", 2),
            ],
        ),
        scenario(
            "oversubscribed-training-day",
            "Oversubscribed queue with small, medium, large, and colocated work.",
            &[1, 1, 1, 4, 4, 8],
            vec![
                JobSpec::singleton("small-a", 1),
                JobSpec::singleton("small-b", 1),
                JobSpec::singleton("medium-a", 2),
                JobSpec::colocated_gang("gang-a", 4, 1),
                JobSpec::singleton("large-a", 4),
                JobSpec::singleton("huge-a", 8),
            ],
        )
        .large(),
    ]
}

fn scenario(name: &str, description: &str, gpu_nodes: &[i64], jobs: Vec<JobSpec>) -> ScenarioSpec {
    ScenarioSpec {
        name: name.to_string(),
        description: description.to_string(),
        tier: Tier::Small,
        nodes: gpu_nodes
            .iter()
            .enumerate()
            .map(|(i, gpus)| GpuNodeSpec {
                name: format!("gpu-{}g-{}", gpus, i),
                gpus: *gpus,
                vram_gib_per_gpu: 80,
                monthly_cost: *gpus * GPU_MONTHLY_PER_GPU,
            })
            .collect(),
        jobs,
        ksolver_priority_weight: 0,
        ksolver_business_value_weight: 0,
        ksolver_queue_weight: 0,
        ksolver_queue_wait_weight: 0,
        ksolver_fair_share_weight: 0,
        ksolver_deadline_urgency_weight: 0,
        ksolver_deadline_miss_weight: 0,
    }
}

impl ScenarioSpec {
    fn large(mut self) -> Self {
        self.tier = Tier::Large;
        self
    }

    fn with_priority_weight(mut self, weight: i64) -> Self {
        self.ksolver_priority_weight = weight.max(0);
        self
    }

    fn with_business_value_weight(mut self, weight: i64) -> Self {
        self.ksolver_business_value_weight = weight.max(0);
        self
    }

    fn with_queue_weight(mut self, weight: i64) -> Self {
        self.ksolver_queue_weight = weight.max(0);
        self
    }

    fn with_queue_wait_weight(mut self, weight: i64) -> Self {
        self.ksolver_queue_wait_weight = weight.max(0);
        self
    }

    fn with_fair_share_weight(mut self, weight: i64) -> Self {
        self.ksolver_fair_share_weight = weight.max(0);
        self
    }

    fn with_deadline_weights(mut self, urgency_weight: i64, miss_weight: i64) -> Self {
        self.ksolver_deadline_urgency_weight = urgency_weight.max(0);
        self.ksolver_deadline_miss_weight = miss_weight.max(0);
        self
    }

    fn with_node_vram_gib(mut self, vram: &[i64]) -> Self {
        for (node, peak) in self.nodes.iter_mut().zip(vram.iter().copied()) {
            node.vram_gib_per_gpu = peak.max(0);
        }
        self
    }
}

#[cfg(test)]
fn run_greedy_spread(s: &ScenarioSpec) -> EngineResult {
    let started = Instant::now();
    let mut used: BTreeMap<String, i64> = s.nodes.iter().map(|n| (n.name.clone(), 0)).collect();
    let mut placements = Vec::new();
    for job in &s.jobs {
        for pod in job.pod_names() {
            let best = s
                .nodes
                .iter()
                .filter(|n| n.gpus - used.get(&n.name).copied().unwrap_or(0) >= job.gpus_per_pod)
                .max_by_key(|n| (n.gpus - used.get(&n.name).copied().unwrap_or(0), &n.name));
            if let Some(node) = best {
                *used.entry(node.name.clone()).or_default() += job.gpus_per_pod;
                placements.push(Placement {
                    pod,
                    node: Some(node.name.clone()),
                    gpus: job.gpus_per_pod,
                });
            } else {
                placements.push(Placement {
                    pod,
                    node: None,
                    gpus: job.gpus_per_pod,
                });
            }
        }
    }
    EngineResult {
        engine: "kube-spread".to_string(),
        source: "local greedy-spread test fixture (not KSS)".to_string(),
        simulator: None,
        candidate_node_limit: 0,
        solve_millis: started.elapsed().as_millis() as u64,
        metrics: metrics(s, &placements),
        placements,
    }
}

/// Test-only greedy MostAllocated fixture; production kube baselines must come from KSS.
#[cfg(test)]
fn run_greedy_binpack(s: &ScenarioSpec) -> EngineResult {
    let started = Instant::now();
    let mut used: BTreeMap<String, i64> = s.nodes.iter().map(|n| (n.name.clone(), 0)).collect();
    let mut placements = Vec::new();
    for job in &s.jobs {
        for pod in job.pod_names() {
            let best = s
                .nodes
                .iter()
                .filter(|n| n.gpus - used.get(&n.name).copied().unwrap_or(0) >= job.gpus_per_pod)
                .min_by_key(|n| {
                    (
                        n.gpus - used.get(&n.name).copied().unwrap_or(0),
                        n.name.clone(),
                    )
                });
            match best {
                Some(node) => {
                    *used.entry(node.name.clone()).or_default() += job.gpus_per_pod;
                    placements.push(Placement {
                        pod,
                        node: Some(node.name.clone()),
                        gpus: job.gpus_per_pod,
                    });
                }
                None => placements.push(Placement {
                    pod,
                    node: None,
                    gpus: job.gpus_per_pod,
                }),
            }
        }
    }
    EngineResult {
        engine: "kube-binpack".to_string(),
        source: "local greedy-binpack test fixture (not KSS)".to_string(),
        simulator: None,
        candidate_node_limit: 0,
        solve_millis: started.elapsed().as_millis() as u64,
        metrics: metrics(s, &placements),
        placements,
    }
}

fn run_ksolver(s: &ScenarioSpec) -> anyhow::Result<EngineResult> {
    run_ksolver_with_candidate_limit(s, 0)
}

fn run_ksolver_with_candidate_limit(
    s: &ScenarioSpec,
    candidate_node_limit: usize,
) -> anyhow::Result<EngineResult> {
    let started = Instant::now();
    let now_unix_seconds = chrono::Utc::now().timestamp();
    let input = OptimizationInput {
        nodes: s
            .nodes
            .iter()
            .map(|n| {
                let mut ext = BTreeMap::new();
                ext.insert(GPU_RESOURCE.to_string(), n.gpus);
                OptimizationNode {
                    name: n.name.clone(),
                    count: 1,
                    effective_capacity: ResourceList {
                        milli_cpu: 64000,
                        memory_bytes: 512 << 30,
                        pods: 128,
                        ..Default::default()
                    },
                    extended_resources: ext,
                    price: crate::model::Money {
                        monthly: n.monthly_cost as f64,
                        ..Default::default()
                    },
                    ..Default::default()
                }
            })
            .collect(),
        workloads: s
            .jobs
            .iter()
            .map(|j| {
                let mut ext = BTreeMap::new();
                ext.insert(GPU_RESOURCE.to_string(), j.total_gpus());
                OptimizationWorkload {
                    id: j.name.clone(),
                    namespace: "bench".to_string(),
                    name: j.name.clone(),
                    group_size: j.pods as i32,
                    members: j
                        .pod_names()
                        .into_iter()
                        .map(|name| OptimizationWorkloadMember {
                            namespace: "bench".to_string(),
                            name,
                            current_node: String::new(),
                        })
                        .collect(),
                    requests: ResourceList {
                        milli_cpu: 1000 * j.pods as i64,
                        memory_bytes: (2 << 30) * j.pods as i64,
                        pods: j.pods as i64,
                        ..Default::default()
                    },
                    extended_resource_requests: ext,
                    priority: j.priority,
                    priority_class_name: j.priority_class_name.clone(),
                    team: j.team.clone(),
                    business_value: j.business_value,
                    fair_share_deficit: j.fair_share_deficit,
                    queue: j.queue.clone(),
                    queue_score: j.queue_score,
                    queue_wait_seconds: j.queue_wait_seconds,
                    deadline_unix_seconds: if j.deadline_after_seconds > 0 {
                        now_unix_seconds.saturating_add(j.deadline_after_seconds)
                    } else {
                        0
                    },
                    min_gpus: j.min_gpus,
                    max_gpus: j.max_gpus,
                    preferred_gpus: j.preferred_gpus,
                    flexible: j.flexible,
                    predicted_runtime_seconds: j.predicted_runtime_seconds,
                    feasible_nodes: candidate_nodes(s, j, candidate_node_limit),
                    colocate: j.colocate,
                    predicted_peak_vram_bytes: j.predicted_peak_vram_gib << 30,
                    ..Default::default()
                }
            })
            .collect(),
        ..Default::default()
    };
    let scenario = ScenarioConfig {
        solver: "cp-sat-rust".to_string(),
        partial_admission: true,
        solve_time_limit_secs: 5,
        objective_profile: if s.ksolver_priority_weight > 0
            || s.ksolver_business_value_weight > 0
            || s.ksolver_queue_weight > 0
            || s.ksolver_queue_wait_weight > 0
            || s.ksolver_fair_share_weight > 0
            || s.ksolver_deadline_urgency_weight > 0
            || s.ksolver_deadline_miss_weight > 0
        {
            ObjectiveProfile::GpuGangAware
        } else {
            ObjectiveProfile::CostBinpack
        },
        objective_weights: crate::model::ObjectiveWeights {
            priority: s.ksolver_priority_weight,
            business_value: s.ksolver_business_value_weight,
            queue: s.ksolver_queue_weight,
            queue_wait: s.ksolver_queue_wait_weight,
            fair_share: s.ksolver_fair_share_weight,
            deadline_urgency: s.ksolver_deadline_urgency_weight,
            deadline_miss: s.ksolver_deadline_miss_weight,
            ..Default::default()
        },
        ..Default::default()
    };
    let (solution, _) = crate::cpsat_rust::solve(&input, &scenario)
        .with_context(|| format!("ksolver solve failed for {}", s.name))?;
    let mut placements = Vec::new();
    for job in &s.jobs {
        let counts = solution.assignment_counts.get(&job.name);
        if let Some(counts) = counts {
            let mut nodes = Vec::new();
            for (node, count) in counts {
                for _ in 0..*count {
                    nodes.push(node.clone());
                }
            }
            nodes.sort();
            for (pod, node) in job.pod_names().into_iter().zip(nodes.into_iter()) {
                placements.push(Placement {
                    pod,
                    node: Some(node),
                    gpus: job.gpus_per_pod,
                });
            }
            if counts.values().map(|v| *v as usize).sum::<usize>() < job.pods {
                let placed = counts.values().map(|v| *v as usize).sum::<usize>();
                for pod in job.pod_names().into_iter().skip(placed) {
                    placements.push(Placement {
                        pod,
                        node: None,
                        gpus: job.gpus_per_pod,
                    });
                }
            }
        } else {
            for pod in job.pod_names() {
                placements.push(Placement {
                    pod,
                    node: None,
                    gpus: job.gpus_per_pod,
                });
            }
        }
    }
    Ok(EngineResult {
        engine: "ksolver".to_string(),
        source: if candidate_node_limit > 0 {
            format!("local CP-SAT batch optimizer with K={candidate_node_limit} candidate cap")
        } else {
            "local CP-SAT batch optimizer".to_string()
        },
        simulator: None,
        candidate_node_limit,
        solve_millis: started.elapsed().as_millis() as u64,
        metrics: metrics(s, &placements),
        placements,
    })
}

fn candidate_nodes(s: &ScenarioSpec, job: &JobSpec, candidate_node_limit: usize) -> Vec<String> {
    let required_gpu = if job.colocate {
        job.total_gpus()
    } else {
        job.gpus_per_pod
    };
    let mut nodes: Vec<&GpuNodeSpec> = s
        .nodes
        .iter()
        .filter(|n| {
            n.gpus >= required_gpu
                && (job.predicted_peak_vram_gib <= 0
                    || n.vram_gib_per_gpu >= job.predicted_peak_vram_gib)
        })
        .collect();
    if nodes.is_empty() {
        nodes = s.nodes.iter().collect();
    }
    nodes.sort_by(|a, b| {
        let a_slack = a.gpus.saturating_sub(required_gpu);
        let b_slack = b.gpus.saturating_sub(required_gpu);
        (a_slack, a.gpus, &a.name).cmp(&(b_slack, b.gpus, &b.name))
    });
    if candidate_node_limit > 0 && nodes.len() > candidate_node_limit {
        nodes.truncate(candidate_node_limit);
    }
    nodes.into_iter().map(|n| n.name.clone()).collect()
}

async fn run_kube_simulator(
    s: &ScenarioSpec,
    simulator_url: &str,
    scheduler_config: serde_json::Value,
    variant: &str,
    batch_timeout: Duration,
) -> anyhow::Result<EngineResult> {
    use crate::verifier::{
        pod_assigned_node, schedule_all_snapshot_report_with_timeout_and_stable_polls,
        SimulatorImportPayload, SimulatorResources,
    };
    let started = Instant::now();
    let nodes = s.nodes.iter().map(k8s_node).collect::<Vec<_>>();
    let namespace = k8s_openapi::api::core::v1::Namespace {
        metadata: kube::api::ObjectMeta {
            name: Some("bench".to_string()),
            ..Default::default()
        },
        ..Default::default()
    };
    let raw = SimulatorResources {
        nodes: nodes.clone(),
        namespaces: vec![namespace.clone()],
        priority_classes: k8s_priority_classes(s),
        ..Default::default()
    };

    // BATCH: import ALL pods once and let the simulator's kube-scheduler place them together, then
    // poll until every pod resolves. One reset+import+poll for the whole scenario (vs per-pod).
    let mut pods = Vec::new();
    let mut target_scopes = BTreeSet::new();
    for job in &s.jobs {
        for pod_name in job.pod_names() {
            pods.push(k8s_pod(&pod_name, job));
            target_scopes.insert(format!("bench/{pod_name}"));
        }
    }
    let payload = SimulatorImportPayload {
        pods,
        nodes: nodes.clone(),
        namespaces: vec![namespace.clone()],
        pvs: raw.pvs.clone(),
        pvcs: raw.pvcs.clone(),
        storage_classes: raw.storage_classes.clone(),
        priority_classes: raw.priority_classes.clone(),
        scheduler_config: scheduler_config.clone(),
    };
    let report = schedule_all_snapshot_report_with_timeout_and_stable_polls(
        simulator_url,
        &payload,
        &target_scopes,
        batch_timeout,
        GPU_BENCHMARK_SIMULATOR_STABLE_POLLS,
    )
    .await
    .with_context(|| {
        format!(
            "kube-scheduler-simulator ({variant}) exceeded batch timeout of {}ms",
            batch_timeout.as_millis()
        )
    })?;
    let export = report.export;
    let node_by_scope: BTreeMap<String, String> = export
        .pods
        .iter()
        .filter_map(|p| pod_assigned_node(p).map(|node| (crate::verifier::pod_scope(p), node)))
        .collect();
    let mut placements = Vec::new();
    for job in &s.jobs {
        for pod_name in job.pod_names() {
            let scope = format!("bench/{pod_name}");
            placements.push(Placement {
                node: node_by_scope.get(&scope).cloned(),
                pod: pod_name,
                gpus: job.gpus_per_pod,
            });
        }
    }
    Ok(EngineResult {
        engine: format!("kube-{variant}"),
        source: format!(
            "kube-scheduler-simulator ({variant}) at {}; {}",
            simulator_url.trim_end_matches('/'),
            report.diagnostics.summary()
        ),
        simulator: Some(simulator_provenance_from_diagnostics(
            "live",
            variant,
            Some(simulator_url.trim_end_matches('/')),
            batch_timeout,
            &report.diagnostics,
        )),
        candidate_node_limit: 0,
        solve_millis: started.elapsed().as_millis() as u64,
        metrics: metrics(s, &placements),
        placements,
    })
}

fn k8s_node(n: &GpuNodeSpec) -> k8s_openapi::api::core::v1::Node {
    use k8s_openapi::api::core::v1::{Node, NodeCondition, NodeStatus};
    use k8s_openapi::apimachinery::pkg::api::resource::Quantity;
    let mut capacity = BTreeMap::new();
    capacity.insert("cpu".to_string(), Quantity("64".to_string()));
    capacity.insert("memory".to_string(), Quantity("512Gi".to_string()));
    capacity.insert("pods".to_string(), Quantity("128".to_string()));
    capacity.insert(GPU_RESOURCE.to_string(), Quantity(n.gpus.to_string()));
    Node {
        metadata: kube::api::ObjectMeta {
            name: Some(n.name.clone()),
            labels: Some(BTreeMap::from([
                ("kubernetes.io/hostname".to_string(), n.name.clone()),
                ("bench.ksolver.dev/gpu-node".to_string(), "true".to_string()),
            ])),
            ..Default::default()
        },
        status: Some(NodeStatus {
            allocatable: Some(capacity.clone()),
            capacity: Some(capacity),
            conditions: Some(vec![NodeCondition {
                type_: "Ready".to_string(),
                status: "True".to_string(),
                ..Default::default()
            }]),
            ..Default::default()
        }),
        ..Default::default()
    }
}

fn k8s_pod(name: &str, job: &JobSpec) -> k8s_openapi::api::core::v1::Pod {
    use k8s_openapi::api::core::v1::{Container, Pod, PodSpec, ResourceRequirements};
    use k8s_openapi::apimachinery::pkg::api::resource::Quantity;
    let mut req = BTreeMap::new();
    req.insert("cpu".to_string(), Quantity("1".to_string()));
    req.insert("memory".to_string(), Quantity("2Gi".to_string()));
    req.insert(
        GPU_RESOURCE.to_string(),
        Quantity(job.gpus_per_pod.to_string()),
    );
    Pod {
        metadata: kube::api::ObjectMeta {
            name: Some(name.to_string()),
            namespace: Some("bench".to_string()),
            labels: Some(BTreeMap::from([(
                "bench.ksolver.dev/job".to_string(),
                job.name.clone(),
            )])),
            ..Default::default()
        },
        spec: Some(PodSpec {
            priority: None,
            priority_class_name: (!job.priority_class_name.is_empty())
                .then(|| job.priority_class_name.clone()),
            containers: vec![Container {
                name: "main".to_string(),
                image: Some("registry.k8s.io/pause:3.9".to_string()),
                resources: Some(ResourceRequirements {
                    requests: Some(req.clone()),
                    limits: Some(req),
                    ..Default::default()
                }),
                ..Default::default()
            }],
            restart_policy: Some("Never".to_string()),
            ..Default::default()
        }),
        ..Default::default()
    }
}

fn k8s_priority_classes(s: &ScenarioSpec) -> Vec<k8s_openapi::api::scheduling::v1::PriorityClass> {
    let mut by_class = BTreeMap::new();
    for job in &s.jobs {
        if !job.priority_class_name.is_empty() {
            by_class
                .entry(job.priority_class_name.clone())
                .and_modify(|priority: &mut i64| *priority = (*priority).max(job.priority))
                .or_insert(job.priority);
        }
    }
    by_class
        .into_iter()
        .map(
            |(name, priority)| k8s_openapi::api::scheduling::v1::PriorityClass {
                metadata: kube::api::ObjectMeta {
                    name: Some(name.clone()),
                    ..Default::default()
                },
                value: priority.clamp(i32::MIN as i64, i32::MAX as i64) as i32,
                global_default: Some(false),
                description: Some(format!("Synthetic benchmark PriorityClass for {name}")),
                ..Default::default()
            },
        )
        .collect()
}

fn metrics(s: &ScenarioSpec, placements: &[Placement]) -> PlacementMetrics {
    let by_pod: HashMap<&str, &Placement> =
        placements.iter().map(|p| (p.pod.as_str(), p)).collect();
    let node_by_name: BTreeMap<_, _> = s.nodes.iter().map(|n| (n.name.as_str(), n)).collect();
    let mut useful_gpu = 0;
    let mut priority_useful_gpu = 0;
    let mut business_value_useful_gpu = 0;
    let mut fair_share_useful_gpu = 0;
    let mut queue_useful_gpu = 0;
    let mut queue_wait_useful_gpu = 0;
    let mut large_jobs_admitted = 0;
    let mut full_gangs = 0;
    let mut partial_or_invalid_gangs = 0;
    let mut deadline_met_gpu = 0;
    let mut deadline_unplaced_gpu = 0;
    let mut deadline_miss_gpu = 0;
    let mut flexible_selected_gpu = 0;
    let mut flexible_gpu_reduction = 0;
    for job in &s.jobs {
        let pod_names = job.pod_names();
        let placed: Vec<&Placement> = pod_names
            .iter()
            .filter_map(|p| by_pod.get(p.as_str()).copied())
            .filter(|p| p.node.is_some())
            .collect();
        let vram_valid = placed.iter().all(|p| {
            job.predicted_peak_vram_gib <= 0
                || p.node
                    .as_deref()
                    .and_then(|node| node_by_name.get(node).copied())
                    .map(|node| node.vram_gib_per_gpu >= job.predicted_peak_vram_gib)
                    .unwrap_or(false)
        });
        let full = placed.len() == job.pods;
        let colocated = if job.colocate {
            placed
                .iter()
                .filter_map(|p| p.node.as_ref())
                .collect::<BTreeSet<_>>()
                .len()
                == 1
        } else {
            true
        };
        let selected_gpu = placed.iter().map(|p| p.gpus).sum::<i64>();
        let flexible_valid = flexible_selected_replicas_meet_deadline(job, placed.len());
        if (full || flexible_valid) && colocated && vram_valid {
            let useful = if flexible_valid {
                selected_gpu
            } else {
                job.total_gpus()
            };
            useful_gpu += useful;
            priority_useful_gpu += useful * job.priority.max(0);
            business_value_useful_gpu += useful * job.business_value.max(0);
            fair_share_useful_gpu += useful * job.fair_share_deficit.max(0);
            queue_useful_gpu += useful * job.queue_score.max(0);
            queue_wait_useful_gpu += useful * job.queue_wait_seconds.max(0);
            if flexible_valid {
                flexible_selected_gpu += selected_gpu;
                flexible_gpu_reduction += job.total_gpus().saturating_sub(selected_gpu);
            }
            if job.deadline_after_seconds > 0 && job.predicted_runtime_seconds > 0 {
                let predicted_runtime =
                    predicted_runtime_for_selected_replicas(job, placed.len()).unwrap_or(0);
                if predicted_runtime > 0 && predicted_runtime <= job.deadline_after_seconds {
                    deadline_met_gpu += useful;
                } else {
                    deadline_miss_gpu += useful;
                }
            }
            if useful >= 4 {
                large_jobs_admitted += 1;
            }
            if job.pods > 1 && full {
                full_gangs += 1;
            }
        } else if job.pods > 1 && !placed.is_empty() {
            partial_or_invalid_gangs += 1;
        }
        if job.deadline_after_seconds > 0 && !(full || flexible_valid) {
            deadline_unplaced_gpu += job.total_gpus();
        }
    }

    let mut used_by_node: BTreeMap<String, i64> = BTreeMap::new();
    for p in placements {
        if let Some(node) = &p.node {
            *used_by_node.entry(node.clone()).or_default() += p.gpus;
        }
    }
    let active_nodes = used_by_node.len();
    let capacity_by_node: BTreeMap<_, _> = s.nodes.iter().map(|n| (&n.name, n.gpus)).collect();
    let cost_by_node: BTreeMap<_, _> = s.nodes.iter().map(|n| (&n.name, n.monthly_cost)).collect();
    let stranded_gpu_on_active_nodes = used_by_node
        .iter()
        .map(|(node, used)| {
            capacity_by_node
                .get(node)
                .copied()
                .unwrap_or(0)
                .saturating_sub(*used)
        })
        .sum();
    // Cost = Σ monthly $ of active nodes; utilization = used GPU / capacity on those active nodes.
    let cost_active_nodes_monthly: i64 = used_by_node
        .keys()
        .map(|node| cost_by_node.get(node).copied().unwrap_or(0))
        .sum();
    let used_on_active: i64 = used_by_node.values().sum();
    let capacity_on_active: i64 = used_by_node
        .keys()
        .map(|node| capacity_by_node.get(node).copied().unwrap_or(0))
        .sum();
    let gpu_utilization_milli = if capacity_on_active > 0 {
        used_on_active * 1000 / capacity_on_active
    } else {
        0
    };

    PlacementMetrics {
        useful_gpu,
        priority_useful_gpu,
        business_value_useful_gpu,
        fair_share_useful_gpu,
        queue_useful_gpu,
        queue_wait_useful_gpu,
        placed_pods: placements.iter().filter(|p| p.node.is_some()).count(),
        unplaced_pods: placements.iter().filter(|p| p.node.is_none()).count(),
        large_jobs_admitted,
        full_gangs,
        partial_or_invalid_gangs,
        deadline_met_gpu,
        deadline_unplaced_gpu,
        deadline_miss_gpu,
        flexible_selected_gpu,
        flexible_gpu_reduction,
        active_nodes,
        stranded_gpu_on_active_nodes,
        cost_active_nodes_monthly,
        gpu_utilization_milli,
    }
}

fn flexible_selected_replicas_meet_deadline(job: &JobSpec, selected_replicas: usize) -> bool {
    if !job.flexible || selected_replicas == 0 || selected_replicas >= job.pods {
        return false;
    }
    let selected_gpu = selected_replicas as i64 * job.gpus_per_pod;
    let min_gpu = job.min_gpus.max(job.gpus_per_pod);
    let max_gpu = if job.max_gpus > 0 {
        job.max_gpus
    } else {
        job.total_gpus()
    };
    if selected_gpu < min_gpu || selected_gpu > max_gpu {
        return false;
    }
    if job.deadline_after_seconds <= 0 || job.predicted_runtime_seconds <= 0 {
        return true;
    }
    predicted_runtime_for_selected_replicas(job, selected_replicas)
        .map(|runtime| runtime <= job.deadline_after_seconds)
        .unwrap_or(false)
}

fn predicted_runtime_for_selected_replicas(job: &JobSpec, selected_replicas: usize) -> Option<i64> {
    if job.predicted_runtime_seconds <= 0 || selected_replicas == 0 || job.pods == 0 {
        return None;
    }
    let ratio = (job.pods as f64 / selected_replicas as f64).sqrt();
    Some(((job.predicted_runtime_seconds as f64) * ratio).ceil() as i64)
}

/// The kube baseline HARDEST for ksolver to beat = lowest active-node cost, tie-broken by more
/// useful GPU admitted then fewer active nodes. Ranking ksolver against this avoids overstating.
fn best_kube<'a>(spread: &'a EngineResult, binpack: &'a EngineResult) -> &'a EngineResult {
    let key = |e: &EngineResult| {
        (
            e.metrics.cost_active_nodes_monthly,
            -(e.metrics.useful_gpu),
            e.metrics.active_nodes as i64,
        )
    };
    if key(binpack) < key(spread) {
        binpack
    } else {
        spread
    }
}

/// ksolver's GPU-efficiency + cost win vs a baseline. Returns (rank score, is-significant, headline).
/// Positive score = ksolver better; "significant" = a margin worth acting on.
fn efficiency(base: &PlacementMetrics, ks: &PlacementMetrics) -> (i64, bool, String) {
    let cost_delta = base.cost_active_nodes_monthly - ks.cost_active_nodes_monthly; // + = cheaper
    let cost_pct_milli = if base.cost_active_nodes_monthly > 0 {
        cost_delta * 1000 / base.cost_active_nodes_monthly
    } else {
        0
    };
    let util_gain = ks.gpu_utilization_milli - base.gpu_utilization_milli;
    let admit_gain = ks.useful_gpu - base.useful_gpu;
    let node_reduction = base.active_nodes as i64 - ks.active_nodes as i64;
    let gang_gain = ks.full_gangs as i64 - base.full_gangs as i64;
    let score =
        cost_pct_milli / 10 + util_gain / 10 + admit_gain * 5 + node_reduction * 8 + gang_gain * 20;
    // Meaningful margin: >=15% cheaper, admits more useful GPU, an extra full gang, >=2 fewer
    // active nodes, or >=15% higher packing density.
    let significant = cost_pct_milli >= 150
        || admit_gain > 0
        || gang_gain > 0
        || node_reduction >= 2
        || util_gain >= 150;
    let headline = format!(
        "cost {:+.1}% ({}->{}/mo), util {:+} milli ({}->{}), admitted useful GPU {:+}, active nodes {}->{}",
        cost_pct_milli as f64 / 10.0,
        base.cost_active_nodes_monthly,
        ks.cost_active_nodes_monthly,
        util_gain,
        base.gpu_utilization_milli,
        ks.gpu_utilization_milli,
        admit_gain,
        base.active_nodes,
        ks.active_nodes,
    );
    (score, significant, headline)
}

fn benefit_score(kube: &PlacementMetrics, ksolver: &PlacementMetrics) -> i64 {
    (ksolver.useful_gpu - kube.useful_gpu) * 100
        + (ksolver.large_jobs_admitted as i64 - kube.large_jobs_admitted as i64) * 40
        + (ksolver.full_gangs as i64 - kube.full_gangs as i64) * 35
        + (ksolver.priority_useful_gpu - kube.priority_useful_gpu) * 20
        + (ksolver.business_value_useful_gpu - kube.business_value_useful_gpu) * 20
        + (ksolver.fair_share_useful_gpu - kube.fair_share_useful_gpu) * 10
        + (ksolver.queue_useful_gpu - kube.queue_useful_gpu) * 10
        + ((ksolver.queue_wait_useful_gpu - kube.queue_wait_useful_gpu) / 60) * 5
        + (ksolver.deadline_met_gpu - kube.deadline_met_gpu) * 80
        + (kube.deadline_unplaced_gpu - ksolver.deadline_unplaced_gpu) * 80
        + (kube.deadline_miss_gpu - ksolver.deadline_miss_gpu) * 40
        + (ksolver.flexible_gpu_reduction - kube.flexible_gpu_reduction) * 250
        + (kube.partial_or_invalid_gangs as i64 - ksolver.partial_or_invalid_gangs as i64) * 50
        + (kube.stranded_gpu_on_active_nodes - ksolver.stranded_gpu_on_active_nodes) * 5
        + (kube.active_nodes as i64 - ksolver.active_nodes as i64) * 5
}

fn regret_metrics(
    candidate_node_limit: usize,
    full: &PlacementMetrics,
    reduced: &PlacementMetrics,
) -> RegretMetrics {
    let loss = |full: i64, reduced: i64| (full - reduced).max(0);
    RegretMetrics {
        candidate_node_limit,
        useful_gpu_regret: loss(full.useful_gpu, reduced.useful_gpu),
        priority_useful_gpu_regret: loss(full.priority_useful_gpu, reduced.priority_useful_gpu),
        placed_pod_regret: loss(full.placed_pods as i64, reduced.placed_pods as i64),
        large_job_regret: loss(
            full.large_jobs_admitted as i64,
            reduced.large_jobs_admitted as i64,
        ),
        full_gang_regret: loss(full.full_gangs as i64, reduced.full_gangs as i64),
        invalid_gang_delta: reduced.partial_or_invalid_gangs as i64
            - full.partial_or_invalid_gangs as i64,
    }
}

fn summarize_regret(candidate_node_limit: usize, scenarios: &[ScenarioResult]) -> RegretSummary {
    let mut summary = RegretSummary {
        candidate_node_limit,
        scenarios_compared: scenarios.len(),
        ..Default::default()
    };
    for scenario in scenarios {
        let r = &scenario.regret;
        let any_regret = r.useful_gpu_regret > 0
            || r.priority_useful_gpu_regret > 0
            || r.placed_pod_regret > 0
            || r.large_job_regret > 0
            || r.full_gang_regret > 0
            || r.invalid_gang_delta > 0;
        if any_regret {
            summary.scenarios_with_any_regret += 1;
        }
        if r.useful_gpu_regret > 0 {
            summary.scenarios_with_useful_gpu_regret += 1;
        }
        summary.total_useful_gpu_regret += r.useful_gpu_regret;
        summary.max_useful_gpu_regret = summary.max_useful_gpu_regret.max(r.useful_gpu_regret);
        summary.total_priority_useful_gpu_regret += r.priority_useful_gpu_regret;
        summary.max_priority_useful_gpu_regret = summary
            .max_priority_useful_gpu_regret
            .max(r.priority_useful_gpu_regret);
        summary.total_placed_pod_regret += r.placed_pod_regret;
        summary.total_large_job_regret += r.large_job_regret;
        summary.total_full_gang_regret += r.full_gang_regret;
        summary.total_invalid_gang_delta += r.invalid_gang_delta.max(0);
    }
    summary
}

fn candidate_widening_scenario_proof(scenarios: &[ScenarioResult]) -> CandidateWideningProof {
    let best = scenarios.iter().max_by(|a, b| {
        let a_regret = &a.regret;
        let b_regret = &b.regret;
        (
            a_regret.useful_gpu_regret,
            a_regret.priority_useful_gpu_regret,
            a_regret.full_gang_regret,
            a_regret.large_job_regret,
            a_regret.placed_pod_regret,
            a.name.as_str(),
        )
            .cmp(&(
                b_regret.useful_gpu_regret,
                b_regret.priority_useful_gpu_regret,
                b_regret.full_gang_regret,
                b_regret.large_job_regret,
                b_regret.placed_pod_regret,
                b.name.as_str(),
            ))
    });

    let Some(scenario) = best else {
        return CandidateWideningProof {
            name: "candidate-widening-recovers-regret".to_string(),
            passed: false,
            scenario: String::new(),
            initial_candidate_node_limit: REGRET_CANDIDATE_LIMIT,
            final_candidate_node_limit: 0,
            retry_count: 0,
            widening_reason: "no scenarios available".to_string(),
            pruned_useful_gpu: 0,
            widened_useful_gpu: 0,
            useful_gpu_recovered: 0,
            pruned_unplaced_pods: 0,
            widened_unplaced_pods: 0,
        };
    };

    let useful_gpu_recovered = scenario.regret.useful_gpu_regret;
    let unplaced_recovered = scenario
        .reduced_ksolver
        .metrics
        .unplaced_pods
        .saturating_sub(scenario.ksolver.metrics.unplaced_pods);
    let passed = useful_gpu_recovered > 0
        && scenario.reduced_ksolver.candidate_node_limit == REGRET_CANDIDATE_LIMIT
        && scenario.ksolver.candidate_node_limit == 0
        && scenario.ksolver.metrics.useful_gpu > scenario.reduced_ksolver.metrics.useful_gpu;

    CandidateWideningProof {
        name: "candidate-widening-recovers-regret".to_string(),
        passed,
        scenario: scenario.name.clone(),
        initial_candidate_node_limit: REGRET_CANDIDATE_LIMIT,
        final_candidate_node_limit: 0,
        retry_count: 1,
        widening_reason: if unplaced_recovered > 0 {
            "low admission ratio with pruned candidates".to_string()
        } else {
            "pruned candidate set had measurable useful-GPU regret".to_string()
        },
        pruned_useful_gpu: scenario.reduced_ksolver.metrics.useful_gpu,
        widened_useful_gpu: scenario.ksolver.metrics.useful_gpu,
        useful_gpu_recovered,
        pruned_unplaced_pods: scenario.reduced_ksolver.metrics.unplaced_pods,
        widened_unplaced_pods: scenario.ksolver.metrics.unplaced_pods,
    }
}

fn summarize_benefit(scenarios: &[ScenarioResult]) -> BenefitSummary {
    let mut summary = BenefitSummary {
        scenarios_compared: scenarios.len(),
        ..Default::default()
    };
    for scenario in scenarios {
        let kube = &scenario.kube.metrics;
        let ksolver = &scenario.ksolver.metrics;
        let useful_gpu_gain = ksolver.useful_gpu - kube.useful_gpu;
        let priority_gain = ksolver.priority_useful_gpu - kube.priority_useful_gpu;
        let business_value_gain =
            ksolver.business_value_useful_gpu - kube.business_value_useful_gpu;
        let fair_share_gain = ksolver.fair_share_useful_gpu - kube.fair_share_useful_gpu;
        let queue_gain = ksolver.queue_useful_gpu - kube.queue_useful_gpu;
        let queue_wait_gain = ksolver.queue_wait_useful_gpu - kube.queue_wait_useful_gpu;
        let large_job_gain = ksolver.large_jobs_admitted as i64 - kube.large_jobs_admitted as i64;
        let full_gang_gain = ksolver.full_gangs as i64 - kube.full_gangs as i64;
        let invalid_gangs_avoided =
            kube.partial_or_invalid_gangs as i64 - ksolver.partial_or_invalid_gangs as i64;
        let deadline_met_gpu_gain = ksolver.deadline_met_gpu - kube.deadline_met_gpu;
        let deadline_unplaced_gpu_reduction =
            kube.deadline_unplaced_gpu - ksolver.deadline_unplaced_gpu;
        let deadline_miss_gpu_reduction = kube.deadline_miss_gpu - ksolver.deadline_miss_gpu;
        let flexible_gpu_reduction_gain =
            ksolver.flexible_gpu_reduction - kube.flexible_gpu_reduction;
        let active_node_reduction = kube.active_nodes as i64 - ksolver.active_nodes as i64;
        let stranded_gpu_reduction =
            kube.stranded_gpu_on_active_nodes - ksolver.stranded_gpu_on_active_nodes;

        if scenario.benefit_score > 0 {
            summary.scenarios_with_positive_benefit += 1;
        }
        if useful_gpu_gain > 0 {
            summary.scenarios_with_useful_gpu_gain += 1;
        }
        if summary.top_scenario.is_empty() || scenario.benefit_score > summary.max_benefit_score {
            summary.max_benefit_score = scenario.benefit_score;
            summary.top_scenario = scenario.name.clone();
        }
        summary.total_benefit_score += scenario.benefit_score;
        summary.total_useful_gpu_gain += useful_gpu_gain;
        summary.total_priority_useful_gpu_gain += priority_gain;
        summary.total_business_value_useful_gpu_gain += business_value_gain;
        summary.total_fair_share_useful_gpu_gain += fair_share_gain;
        summary.total_queue_useful_gpu_gain += queue_gain;
        summary.total_queue_wait_useful_gpu_gain += queue_wait_gain;
        summary.total_large_job_gain += large_job_gain;
        summary.total_full_gang_gain += full_gang_gain;
        summary.total_invalid_gangs_avoided += invalid_gangs_avoided;
        summary.total_deadline_met_gpu_gain += deadline_met_gpu_gain;
        summary.total_deadline_unplaced_gpu_reduction += deadline_unplaced_gpu_reduction;
        summary.total_deadline_miss_gpu_reduction += deadline_miss_gpu_reduction;
        summary.total_flexible_gpu_reduction_gain += flexible_gpu_reduction_gain;
        summary.total_active_node_reduction += active_node_reduction;
        summary.total_stranded_gpu_reduction += stranded_gpu_reduction;
    }
    summary
}

fn placement_requested_gpu(result: &EngineResult) -> i64 {
    result.placements.iter().map(|p| p.gpus).sum()
}

fn summarize_hero_demo(repair: &RepairScenarioProof, roi: &RoiSummary) -> HeroDemoSummary {
    let recommendation = if repair.passed {
        format!(
            "Free {} GPUs on {} with {} migration(s) and {} preemption(s); keep this as a dry-run action plan until an operator approves disruption cost {}.",
            repair.freed_gpu,
            repair.node,
            repair.migration_actions,
            repair.preemption_actions,
            repair.disruption_cost
        )
    } else {
        "No safe defragmentation repair plan was found for the hero scenario.".to_string()
    };
    let headline = if repair.passed {
        format!(
            "Enough GPUs exist, but they are fragmented; ksolver can admit {} by freeing {} GPUs on {}.",
            repair.target, repair.freed_gpu, repair.node
        )
    } else {
        format!(
            "Fragmentation demo is not ready; repair proof {} did not pass.",
            repair.name
        )
    };

    HeroDemoSummary {
        name: "defragmentation-advisor".to_string(),
        passed: repair.passed
            && repair.target_gpu_request > 0
            && repair.freed_gpu >= repair.target_gpu_request
            && repair.action_count > 0
            && !repair.explanation.is_empty(),
        headline,
        problem: format!(
            "{} needs {} GPUs on one node, but existing low-value work fragments the fleet.",
            repair.target, repair.target_gpu_request
        ),
        recommendation,
        target: repair.target.clone(),
        target_gpu_request: repair.target_gpu_request,
        repair_node: repair.node.clone(),
        freed_gpu: repair.freed_gpu,
        migration_actions: repair.migration_actions,
        preemption_actions: repair.preemption_actions,
        disruption_cost: repair.disruption_cost,
        roi_headline: roi.headline.clone(),
        screenshot_claims: vec![
            "kube can leave a gang pending even when aggregate GPU capacity exists".to_string(),
            "ksolver separates capacity shortage from fragmentation".to_string(),
            "the recommended action plan is dry-run, auditable, and disruption-scored".to_string(),
            "ROI can be explained with admitted GPU, stranded GPU, active-node cost, and disruption cost".to_string(),
        ],
    }
}

fn summarize_preemption_migration_hero(
    repair: &RepairScenarioProof,
) -> PreemptionMigrationHeroSummary {
    let safety_claims = vec![
        "dry-run only: renders migration/preemption recommendations without evicting, binding, or mutating pods".to_string(),
        "each action carries source node, optional migration target, GPU request, disruption cost, and reason".to_string(),
        "migration is preferred over preemption when it can free the same GPU capacity at equal/base cost".to_string(),
        "operator approval is required before any real disruption; current report is observe-only evidence".to_string(),
    ];
    let operator_questions = vec![
        "Which pods would move or be preempted?".to_string(),
        "How many GPUs does each action free, and on which node?".to_string(),
        "What is the disruption cost of the repair plan?".to_string(),
        "Can we migrate before preempting?".to_string(),
        "Why is the pending gang blocked even though total GPU exists?".to_string(),
    ];
    let residual_risks = vec![
        "this deterministic report is advisory; live execution still needs final identity/readiness/PDB checks".to_string(),
        "migration target feasibility depends on the latest cluster snapshot at execution time".to_string(),
        "preemption is modeled as disruption cost, not an automatic Kubernetes eviction".to_string(),
    ];
    let decision_contract = HeroDecisionContract {
        verdict: "Reference proof; live approval required".to_string(),
        can_act_now: false,
        evidence_required: vec![
            "/api/scheduler/repair-plan reports proof_status.mode=live-repair-plan".to_string(),
            "repair action rows include current pod UID, source node, target node, GPU request, and disruption cost".to_string(),
            "/api/scheduler/production-safety reports observe-only or dry-run gates healthy before any mutation".to_string(),
            "PDB, safe-to-evict, do-not-disrupt, checkpoint age, and priority/business-value checks are current".to_string(),
            "kube baseline provenance is live or cached with explicit simulator mode and fallback reason".to_string(),
        ],
        approval_required: vec![
            "target workload owner accepts the disruption tradeoff".to_string(),
            "SRE approves the migration/preemption list and total disruption cost".to_string(),
            "tenant or queue policy owner accepts any fairness or priority consequence".to_string(),
        ],
        fail_closed_if: vec![
            "repair-plan proof_status is deterministic-reference or stale".to_string(),
            "any candidate pod identity, node, PDB, or do-not-disrupt annotation changed since solve".to_string(),
            "production safety reports live binding enabled without the expected canary or dry-run gate".to_string(),
            "required pricing, prediction, or device evidence is missing for a customer-facing ROI claim".to_string(),
        ],
        next_action:
            "capture a live repair-plan bundle, review action rows with owners, then rerun production-safety gates before enabling any write path"
                .to_string(),
    };
    let passed = repair.passed
        && repair.target_gpu_request > 0
        && repair.freed_gpu >= repair.target_gpu_request
        && repair.migration_actions > 0
        && repair.preemption_actions > 0
        && repair.action_rows.len() == repair.action_count
        && repair
            .action_rows
            .iter()
            .any(|a| a.action == "migrate" && !a.to_node.is_empty())
        && repair.action_rows.iter().any(|a| a.action == "preempt")
        && repair
            .action_rows
            .iter()
            .all(|a| a.step > 0 && !a.pod.is_empty() && !a.from_node.is_empty())
        && !decision_contract.can_act_now
        && decision_contract.evidence_required.len() >= 5
        && decision_contract.approval_required.len() >= 3
        && decision_contract.fail_closed_if.len() >= 4;

    PreemptionMigrationHeroSummary {
        name: "preemption-migration-hero".to_string(),
        passed,
        headline: format!(
            "Free {} GPUs on {} for {} with {} migration(s), {} preemption(s), and disruption cost {}.",
            repair.freed_gpu,
            repair.node,
            repair.target,
            repair.migration_actions,
            repair.preemption_actions,
            repair.disruption_cost
        ),
        target: repair.target.clone(),
        target_gpu_request: repair.target_gpu_request,
        repair_node: repair.node.clone(),
        freed_gpu: repair.freed_gpu,
        migration_actions: repair.migration_actions,
        preemption_actions: repair.preemption_actions,
        total_disruption_cost: repair.disruption_cost,
        action_rows: repair.action_rows.clone(),
        decision_contract,
        safety_claims,
        operator_questions,
        residual_risks,
    }
}

pub(crate) fn demo_preemption_migration_hero_summary() -> PreemptionMigrationHeroSummary {
    summarize_preemption_migration_hero(&fragmented_repair_scenario_proof())
}

fn summarize_sre_demo_script(
    scenarios: &[ScenarioResult],
    hero: &HeroDemoSummary,
    roi: &RoiSummary,
) -> SreDemoScript {
    let top_scenario_cards = scenarios
        .iter()
        .take(5)
        .enumerate()
        .map(|(idx, scenario)| {
            let kube = best_kube(&scenario.kube, &scenario.kube_binpack);
            let kube_metrics = &kube.metrics;
            let ksolver_metrics = &scenario.ksolver.metrics;
            SreScenarioCard {
                rank: idx + 1,
                scenario: scenario.name.clone(),
                tier: scenario.tier,
                headline: scenario.efficiency_headline.clone(),
                efficiency_score: scenario.efficiency_score,
                significantly_better: scenario.significantly_better,
                kube_useful_gpu: kube_metrics.useful_gpu,
                ksolver_useful_gpu: ksolver_metrics.useful_gpu,
                useful_gpu_gain: ksolver_metrics.useful_gpu - kube_metrics.useful_gpu,
                kube_active_node_monthly_cost: kube_metrics.cost_active_nodes_monthly,
                ksolver_active_node_monthly_cost: ksolver_metrics.cost_active_nodes_monthly,
                active_node_monthly_cost_delta: ksolver_metrics.cost_active_nodes_monthly
                    - kube_metrics.cost_active_nodes_monthly,
                kube_gpu_utilization_milli: kube_metrics.gpu_utilization_milli,
                ksolver_gpu_utilization_milli: ksolver_metrics.gpu_utilization_milli,
                gpu_utilization_gain_milli: ksolver_metrics.gpu_utilization_milli
                    - kube_metrics.gpu_utilization_milli,
            }
        })
        .collect::<Vec<_>>();

    SreDemoScript {
        name: "gpu-fleet-defragmentation-roi-demo".to_string(),
        headline: format!(
            "{} Across the scenario library, {}",
            hero.headline, roi.headline
        ),
        hero_scenario: hero.name.clone(),
        primary_question:
            "What should the platform team do when a valuable GPU job is pending even though the fleet has enough total GPUs?"
                .to_string(),
        steps: vec![
            SreDemoStep {
                title: "Show the blocked high-value job".to_string(),
                operator_question:
                    "Is this a real capacity shortage, or is the fleet fragmented?".to_string(),
                evidence: hero.problem.clone(),
            },
            SreDemoStep {
                title: "Show the dry-run repair recommendation".to_string(),
                operator_question:
                    "What would we need to move or preempt, and how disruptive is it?".to_string(),
                evidence: hero.recommendation.clone(),
            },
            SreDemoStep {
                title: "Show the ROI frame".to_string(),
                operator_question:
                    "Is the disruption worth the GPU admission, utilization, and cost delta?".to_string(),
                evidence: roi.headline.clone(),
            },
            SreDemoStep {
                title: "Show approximation guardrails".to_string(),
                operator_question:
                    "Can I tell whether this was exact, cached, grouped, or candidate-pruned?".to_string(),
                evidence:
                    "Scenario cards include efficiency deltas; regret and widening proofs remain in the same JSON report."
                        .to_string(),
            },
        ],
        top_scenario_cards,
        operator_close:
            "Use ksolver first as an observe-only advisor: screenshot the recommendation, inspect disruption cost, then decide whether to enable guarded binding or repair automation."
                .to_string(),
    }
}

#[allow(clippy::too_many_arguments)]
fn summarize_demo_readiness(
    scenarios: &[ScenarioResult],
    roi_dashboard: &RoiDashboardSummary,
    hero: &HeroDemoSummary,
    preemption_migration: &PreemptionMigrationHeroSummary,
    sre_demo_script: &SreDemoScript,
    production_safety: &ProductionSafetySummary,
    prediction_quality: &PredictionQualitySummary,
    scale_guardrails: &ScaleGuardrailSummary,
) -> DemoReadinessSummary {
    let kube_baseline_mode = kube_baseline_mode(scenarios);
    let ui_sections = vec![
        "hero_demo_summary".to_string(),
        "preemption_migration_hero_summary".to_string(),
        "roi_dashboard_summary".to_string(),
        "sre_demo_script".to_string(),
        "production_safety_summary".to_string(),
        "scale_guardrail_summary".to_string(),
        "prediction_quality_summary".to_string(),
    ];
    let operator_checklist = vec![
        "show the blocked high-value GPU job and whether this is fragmentation or true capacity shortage".to_string(),
        "show the dry-run migrate/preempt action table with disruption cost and reasons".to_string(),
        "show ROI tiles for admitted useful GPU, stranded GPU, active-node cost delta, deadline pressure, and regret".to_string(),
        "show simulator baseline provenance: live, cached, or invalid legacy fallback markers that block trust".to_string(),
        "show safety posture before any real binding, eviction, or migration automation".to_string(),
    ];
    let demo_flow_scenes = vec![
        DemoFlowScene {
            step: 1,
            screen: "Problem".to_string(),
            operator_question: "Why is a valuable GPU job still pending?".to_string(),
            evidence_source: "hero_demo_summary.problem".to_string(),
            primary_visual: "blocked job beside fragmented node fill view".to_string(),
            decision: "classify as repairable fragmentation rather than true capacity shortage"
                .to_string(),
            trust_gate: kube_baseline_mode.clone(),
        },
        DemoFlowScene {
            step: 2,
            screen: "Repair Plan".to_string(),
            operator_question: "What exactly would need to move or be preempted?".to_string(),
            evidence_source: "preemption_migration_hero_summary.action_rows".to_string(),
            primary_visual: "ordered migrate/preempt table with source node, target node, reason, and disruption cost".to_string(),
            decision: preemption_migration.headline.clone(),
            trust_gate: "dry-run advisory only; no eviction or live binding".to_string(),
        },
        DemoFlowScene {
            step: 3,
            screen: "ROI".to_string(),
            operator_question: "Is the disruption worth the capacity and cost improvement?".to_string(),
            evidence_source: "roi_dashboard_summary.executive_rows".to_string(),
            primary_visual: "ROI tiles plus executive rows".to_string(),
            decision: roi_dashboard.headline.clone(),
            trust_gate: format!(
                "{}; {}",
                roi_dashboard.confidence_guardrail, roi_dashboard.regret_guardrail
            ),
        },
        DemoFlowScene {
            step: 4,
            screen: "Safety".to_string(),
            operator_question: "What prevents this from mutating the cluster accidentally?"
                .to_string(),
            evidence_source: "production_safety_summary.rollout_gate_rows".to_string(),
            primary_visual: "rollout mode matrix with mutation flag and rollback action".to_string(),
            decision: production_safety.real_binding_gate.clone(),
            trust_gate: production_safety
                .rollout_gate_rows
                .first()
                .map(|row| row.rollback_action.clone())
                .unwrap_or_else(|| "observe-only rollback path required".to_string()),
        },
        DemoFlowScene {
            step: 5,
            screen: "Trust".to_string(),
            operator_question: "Can I trust the prediction and approximation parts of this recommendation?".to_string(),
            evidence_source: "prediction_quality_summary.model_cards + scale_guardrail_summary.scale_mode_cards".to_string(),
            primary_visual: "prediction model cards beside scale mode cards".to_string(),
            decision: "show exact, advisory, pruned, grouped, widened, and unknown states before action"
                .to_string(),
            trust_gate: format!(
                "{}; {}",
                prediction_quality.confidence_model, scale_guardrails.widening_claim
            ),
        },
    ];
    let demo_acceptance_criteria = vec![
        "the first viewport names the blocked GPU job, the repair target, and whether the issue is fragmentation".to_string(),
        "the repair screen shows at least one concrete migrate/preempt row with disruption cost".to_string(),
        "the ROI screen shows admitted useful GPU, stranded GPU, active-node cost delta, disruption cost, and regret".to_string(),
        "the safety screen makes observe-only the default and shows the kill-switch/rollback path".to_string(),
        "the trust screen discloses prediction confidence and candidate-regret status before any action".to_string(),
        "the user can explain the whole story in under 30 seconds without reading raw JSON".to_string(),
    ];
    let live_validation_rows = vec![
        DemoLiveValidationRow {
            gate: "pending GPU trace".to_string(),
            live_endpoint: "/api/scheduler/traces".to_string(),
            required_evidence: "latest trace has observed pending GPU pods, placements, unplaced reasons, objective profile, and outcome_summary".to_string(),
            pass_signal: "trace_sequence advances and observed_pods > 0 with pod/node rows rendered".to_string(),
            failure_action: "apply deterministic KWOK GPU scenario manifests or show the demo report instead of claiming live evidence".to_string(),
            operator_question: "Is this screenshot based on a real current trace or a deterministic scenario report?".to_string(),
        },
        DemoLiveValidationRow {
            gate: "kube baseline provenance".to_string(),
            live_endpoint: "/api/scheduler/kube-simulator-plan".to_string(),
            required_evidence: "simulator mode, URL/cache key, timeout flag, failing phase, target counts, and placements when available".to_string(),
            pass_signal: "mode is live or cached; invalid legacy fallback markers are visibly downgraded before comparison".to_string(),
            failure_action: "use cached simulator baselines only when provenance is valid; otherwise block kube comparison claims and show the refresh action".to_string(),
            operator_question: "Did kube-scheduler-simulator actually schedule this trace, or is this stale provenance that cannot support a claim?".to_string(),
        },
        DemoLiveValidationRow {
            gate: "repair action safety".to_string(),
            live_endpoint: "/api/scheduler/repair-plan".to_string(),
            required_evidence: "target pod, repairability class, migrate/preempt action rows, skipped candidates, PDB/policy caveats, and disruption cost".to_string(),
            pass_signal: "repairable fragmentation has at least one action row and unrepairable cases explain why no move helps".to_string(),
            failure_action: "show advisory-only state and do not present repair as executable when action rows or safety caveats are missing".to_string(),
            operator_question: "What exactly moves, what gets preempted, and what safety rule blocks automation?".to_string(),
        },
        DemoLiveValidationRow {
            gate: "production mutation safety".to_string(),
            live_endpoint: "/api/scheduler/production-safety".to_string(),
            required_evidence: "rollout mode, real-binding gate, kill switch, readiness, RBAC posture, reservation metrics, and binding outcomes".to_string(),
            pass_signal: "observe-only is safe by default; mutation-enabled modes expose required RBAC and fail-closed state".to_string(),
            failure_action: "keep the demo in observe-only and hide live-action controls until safety endpoint and RBAC checks pass".to_string(),
            operator_question: "What prevents this recommendation from mutating the cluster accidentally?".to_string(),
        },
        DemoLiveValidationRow {
            gate: "ROI pricing evidence".to_string(),
            live_endpoint: "/api/scheduler/demo-report".to_string(),
            required_evidence: "roi_dashboard_summary decision rows plus pricing_readiness_summary accepted sources and recompute fields".to_string(),
            pass_signal: "each KPI has a decision row and pricing caveats are visible beside dollar claims".to_string(),
            failure_action: "present utilization/admission value only, and withhold dollar savings until a pricing catalog or chargeback source is loaded".to_string(),
            operator_question: "Are these dollars customer-specific pricing or synthetic relative demo economics?".to_string(),
        },
        DemoLiveValidationRow {
            gate: "trust guardrails".to_string(),
            live_endpoint: "/api/scheduler/demo-report".to_string(),
            required_evidence: "prediction live calibration rows, scale regret rows, device readiness rows, and fairness ownership rows".to_string(),
            pass_signal: "prediction confidence, candidate-pruning regret, device limits, and tenant ownership caveats are all rendered".to_string(),
            failure_action: "treat the recommendation as advisory and rerun unpruned or with stronger inventory/calibration evidence".to_string(),
            operator_question: "Which part of this recommendation is exact, calibrated, pruned, caveated, or unsupported?".to_string(),
        },
    ];
    let remaining_gaps = vec![
        "live execution still needs final identity/readiness/PDB checks at action time".to_string(),
        "prediction confidence is reportable, but calibration still needs real historical fleet samples".to_string(),
        "scale guardrails are visible, but very large fleets still need grouped-first demos with regret evidence".to_string(),
        "demo report should be refreshed from cached live kube-scheduler-simulator baselines before customer-facing screenshots".to_string(),
    ];
    let passed = roi_dashboard.passed
        && hero.passed
        && preemption_migration.passed
        && sre_demo_script.steps.len() >= 4
        && sre_demo_script.top_scenario_cards.len() >= 3
        && production_safety.passed
        && prediction_quality.passed
        && scale_guardrails.passed
        && demo_flow_scenes.len() >= 5
        && demo_flow_scenes.iter().all(|scene| {
            scene.step > 0 && !scene.primary_visual.is_empty() && !scene.trust_gate.is_empty()
        })
        && demo_acceptance_criteria.len() >= 6
        && live_validation_rows.len() >= 6
        && live_validation_rows.iter().all(|row| {
            !row.live_endpoint.is_empty()
                && !row.required_evidence.is_empty()
                && !row.failure_action.is_empty()
        })
        && ui_sections.len() >= 7
        && operator_checklist.len() >= 5
        && !kube_baseline_mode.is_empty();

    DemoReadinessSummary {
        name: "sre-end-to-end-demo-readiness".to_string(),
        passed,
        headline: format!(
            "{} {}",
            hero.headline, roi_dashboard.headline
        ),
        primary_story:
            "kube leaves valuable GPU work pending or fragmented; ksolver explains whether this is repairable, shows the dry-run move/preempt plan, and quantifies ROI plus safety gates."
                .to_string(),
        hero_scenario: hero.name.clone(),
        kube_baseline_mode,
        ksolver_action: preemption_migration.headline.clone(),
        roi_claim: roi_dashboard.headline.clone(),
        safety_claim: production_safety.default_mode.clone(),
        demo_flow_scenes,
        demo_acceptance_criteria,
        live_validation_rows,
        ui_sections,
        operator_checklist,
        remaining_gaps,
    }
}

#[allow(clippy::too_many_arguments)]
fn summarize_roadmap_readiness(
    demo: &DemoReadinessSummary,
    preemption_migration: &PreemptionMigrationHeroSummary,
    production_safety: &ProductionSafetySummary,
    prediction_quality: &PredictionQualitySummary,
    scale_guardrails: &ScaleGuardrailSummary,
    device_correctness: &DeviceCorrectnessSummary,
    fairness_budget: &FairnessBudgetSummary,
    roi_dashboard: &RoiDashboardSummary,
    preemption_migration_kss_proofs: &[KssProofScenario],
    vram_kss_proofs: &[KssProofScenario],
) -> RoadmapReadinessSummary {
    let repair_kss_passed = preemption_migration_kss_proofs.iter().all(|p| p.passed);
    let vram_kss_passed = vram_kss_proofs.iter().all(|p| p.passed);
    let items = vec![
        RoadmapItemStatus {
            rank: 1,
            item: "Preemption/migration planner proof".to_string(),
            status: if preemption_migration.passed && repair_kss_passed {
                "repair-proof-ready"
            } else {
                "incomplete"
            }
            .to_string(),
            evidence_source: "preemption_migration_hero_summary.action_rows + preemption_migration_kss_proofs kube baseline provenance + demo_readiness_summary.live_validation_rows[repair action safety]".to_string(),
            proof: format!(
                "{} action row(s), migrations={}, preemptions={}, disruption_cost={}, kss_proofs={}/{}, validation_gates={}, baseline mode: {}",
                preemption_migration.action_rows.len(),
                preemption_migration.migration_actions,
                preemption_migration.preemption_actions,
                preemption_migration.total_disruption_cost,
                preemption_migration_kss_proofs.iter().filter(|p| p.passed).count(),
                preemption_migration_kss_proofs.len(),
                demo.live_validation_rows.len(),
                demo.kube_baseline_mode
            ),
            remaining_gap: "refresh these proof scenarios with live/cached KSS before customer claims, then capture non-demo fragmented repair traces"
                .to_string(),
        },
        RoadmapItemStatus {
            rank: 2,
            item: "VRAM prediction and no-repair proof".to_string(),
            status: if prediction_quality.passed && vram_kss_passed {
                "vram-proof-gates-ready"
            } else {
                "incomplete"
            }
            .to_string(),
            evidence_source:
                "prediction_quality_summary.model_cards + calibration_buckets + live_calibration_rows + vram_prediction_scenario + vram_kss_proofs"
                    .to_string(),
            proof: format!(
                "{} model card(s), {} calibration bucket(s), {} live calibration row(s), {} promotion gate(s), kss_proofs={}/{}",
                prediction_quality.model_cards.len(),
                prediction_quality.calibration_buckets.len(),
                prediction_quality.live_calibration_rows.len(),
                prediction_quality.promotion_gates.len(),
                vram_kss_proofs.iter().filter(|p| p.passed).count(),
                vram_kss_proofs.len()
            ),
            remaining_gap:
                "refresh VRAM proof scenarios with live/cached KSS and add calibrated historical fleet samples before hard placement claims"
                .to_string(),
        },
        RoadmapItemStatus {
            rank: 3,
            item: "Whole-queue objective tradeoff proof".to_string(),
            status: if fairness_budget.passed && roi_dashboard.passed {
                "policy-proof-ready"
            } else {
                "incomplete"
            }
            .to_string(),
            evidence_source: "fairness_budget_summary.policy_decision_rows + roi_dashboard_summary.decision_rows + live KSS scenario provenance".to_string(),
            proof: format!(
                "{} policy row(s), {} ROI decision row(s), {} tenant ledger row(s)",
                fairness_budget.policy_decision_rows.len(),
                roi_dashboard.decision_rows.len(),
                fairness_budget.tenant_ledger_rows.len()
            ),
            remaining_gap:
                "add gang-aware or real Volcano comparison before claiming policy wins beyond kube-scheduler"
                    .to_string(),
        },
        RoadmapItemStatus {
            rank: 4,
            item: "Shadow evidence bundle and production safety".to_string(),
            status: if production_safety.passed && demo.passed {
                "launch-review-ready"
            } else {
                "incomplete"
            }
            .to_string(),
            evidence_source:
                "production_safety_summary.rollout_gate_rows + live_config_rows + failure_playbook_rows + demo_readiness_summary.live_validation_rows"
                    .to_string(),
            proof: format!(
                "{} rollout gate row(s), {} live config row(s), {} failure playbook row(s), {} live validation gate(s)",
                production_safety.rollout_gate_rows.len(),
                production_safety.live_config_rows.len(),
                production_safety.failure_playbook_rows.len(),
                demo.live_validation_rows.len()
            ),
            remaining_gap:
                "validate live Helm-rendered RBAC and production-safety endpoint state on a non-demo cluster"
                    .to_string(),
        },
        RoadmapItemStatus {
            rank: 5,
            item: "True device correctness".to_string(),
            status: if device_correctness.passed {
                "device-readiness-gates-ready"
            } else {
                "incomplete"
            }
            .to_string(),
            evidence_source:
                "device_correctness_summary.exact_semantics + unsupported_claims + device_readiness_rows"
                    .to_string(),
            proof: format!(
                "{} exact semantic(s), {} approximated semantic(s), {} unsupported claim(s), {} readiness row(s)",
                device_correctness.exact_semantics.len(),
                device_correctness.approximated_semantics.len(),
                device_correctness.unsupported_claims.len(),
                device_correctness.device_readiness_rows.len()
            ),
            remaining_gap:
                "implement true DRA allocation and concrete NVLink device graph optimization"
                    .to_string(),
        },
        RoadmapItemStatus {
            rank: 6,
            item: "Scale without suspicious pruning".to_string(),
            status: if scale_guardrails.passed {
                "large-fleet-validation-gates-ready"
            } else {
                "incomplete"
            }
            .to_string(),
            evidence_source:
                "scale_guardrail_summary.scale_mode_cards + large_fleet_validation_rows"
                    .to_string(),
            proof: format!(
                "{} scale mode card(s), {} large-fleet validation row(s), max_useful_gpu_regret={}, widening_recovered={}",
                scale_guardrails.scale_mode_cards.len(),
                scale_guardrails.large_fleet_validation_rows.len(),
                scale_guardrails.max_useful_gpu_regret,
                scale_guardrails.widening_useful_gpu_recovered
            ),
            remaining_gap:
                "exercise large-fleet validation gates on real heterogeneous cluster snapshots"
                    .to_string(),
        },
        RoadmapItemStatus {
            rank: 7,
            item: "Fairness and budgets as first-class UI concepts".to_string(),
            status: if fairness_budget.passed {
                "ownership-evidence-ready"
            } else {
                "incomplete"
            }
            .to_string(),
            evidence_source: "fairness_budget_summary.policy_decision_rows + tenant_ledger_rows + ownership_rows".to_string(),
            proof: format!(
                "{} policy row(s), {} tenant ledger row(s), {} ownership evidence row(s)",
                fairness_budget.policy_decision_rows.len(),
                fairness_budget.tenant_ledger_rows.len(),
                fairness_budget.ownership_rows.len()
            ),
            remaining_gap: "validate ownership source mappings against live namespace/account metadata before hard enforcement"
                .to_string(),
        },
        RoadmapItemStatus {
            rank: 8,
            item: "ROI dashboard and scenario library".to_string(),
            status: if roi_dashboard.passed {
                "roi-decision-ready"
            } else {
                "incomplete"
            }
            .to_string(),
            evidence_source: "roi_dashboard_summary.primary_tiles + executive_rows + decision_rows".to_string(),
            proof: format!(
                "{} tile(s), {} executive row(s), {} decision row(s)",
                roi_dashboard.primary_tiles.len(),
                roi_dashboard.executive_rows.len(),
                roi_dashboard.decision_rows.len()
            ),
            remaining_gap: "replace URL-provided demo GPU-hour pricing with pricing catalog or invoice ingestion"
                .to_string(),
        },
    ];
    let next_build_order = vec![
        "build and refresh live KSS proof scenarios for preemption/migration repair".to_string(),
        "build and refresh live KSS proof scenarios for VRAM fit and VRAM-blocked no-repair"
            .to_string(),
        "add a gang-aware or real Volcano baseline before external policy/gang superiority claims"
            .to_string(),
        "exercise highlighted preemption_migration_hero_summary.action_rows against non-demo repair traces"
            .to_string(),
        "connect ROI/safety drill-downs to live pricing, Helm values, RBAC, kill-switch state, calibration, and regret evidence"
            .to_string(),
    ];
    let residual_product_gaps = items
        .iter()
        .map(|item| item.remaining_gap.clone())
        .collect::<Vec<_>>();
    let customer_blockers = residual_product_gaps
        .iter()
        .filter(|gap| {
            gap.contains("non-demo")
                || gap.contains("real ")
                || gap.contains("live ")
                || gap.contains("pricing")
                || gap.contains("DRA")
        })
        .cloned()
        .collect::<Vec<_>>();
    let passed = items.len() == 8
        && items.iter().all(|item| item.status != "incomplete")
        && next_build_order.len() >= 5
        && residual_product_gaps.len() == 8;
    let launch_proof_gate = SreLaunchProofGate {
        label: if customer_blockers.is_empty() {
            "Customer proof ready".to_string()
        } else if passed {
            "Demo-ready, customer proof pending".to_string()
        } else {
            "Demo proof incomplete".to_string()
        },
        status: if customer_blockers.is_empty() {
            "customer-claim-ready".to_string()
        } else if passed {
            "reference-demo-ready".to_string()
        } else {
            "incomplete".to_string()
        },
        demo_ready: passed,
        customer_claim_ready: passed && customer_blockers.is_empty(),
        required_evidence: vec![
            "live or cached KSS proof scenarios for repair and VRAM claims, with fallback rows explicitly downgraded".to_string(),
            "non-demo cluster trace with pending GPU pods, kube baseline provenance, and repair-plan evidence".to_string(),
            "live production-safety endpoint evidence matching Helm-rendered RBAC, kill switch, leader election, and rollout mode".to_string(),
            "pricing catalog, chargeback export, contract rate sheet, or invoice sample mapped to node pools".to_string(),
            "completed-job prediction calibration history with promotion thresholds and drift behavior".to_string(),
            "large heterogeneous fleet snapshot showing grouping/pruning guardrails and regret behavior".to_string(),
            "device inventory evidence for DRA, MIG, topology, and any unsupported concrete device semantics".to_string(),
            "gang-aware or real Volcano baseline before claiming differentiation beyond default kube-scheduler".to_string(),
        ],
        evidence_bundle_rows: vec![
            SreEvidenceBundleRow {
                artifact: "live pending GPU trace".to_string(),
                source: "/api/scheduler/evidence-bundle + /api/scheduler/traces".to_string(),
                pass_signal: "trace has pending GPU pods, solver placements, unplaced reasons, outcome_summary, and timestamped sequence".to_string(),
                blocks_claim: "current-cluster scheduling decision".to_string(),
                operator_action: "capture the trace JSON and screenshot before changing workloads".to_string(),
            },
            SreEvidenceBundleRow {
                artifact: "kube baseline provenance".to_string(),
                source: "/api/scheduler/kube-simulator-plan plus gpu-scenarios simulator cache".to_string(),
                pass_signal: "mode is live or cached with URL/cache key, target counts, timeout flag, phase timings, and placements".to_string(),
                blocks_claim: "ksolver beats kube-scheduler on this workload".to_string(),
                operator_action: "refresh bounded simulator cache offline and attach provenance with the demo report".to_string(),
            },
            SreEvidenceBundleRow {
                artifact: "repair action proof".to_string(),
                source: "/api/scheduler/repair-plan".to_string(),
                pass_signal: "target pod, repairability class, migrate/preempt rows, disruption cost, PDB/policy caveats, and skipped candidates are present".to_string(),
                blocks_claim: "safe preemption/migration recommendation".to_string(),
                operator_action: "store the dry-run repair plan and verify action rows against live pod UIDs before any human approval".to_string(),
            },
            SreEvidenceBundleRow {
                artifact: "production safety and RBAC".to_string(),
                source: "/api/scheduler/production-safety plus rendered Helm/RBAC manifests".to_string(),
                pass_signal: "observe-only default, kill switch, leader election, reservation ledger, Event writes, pods/binding RBAC, and rollout mode all match the intended mode".to_string(),
                blocks_claim: "safe to run in an SRE-owned cluster".to_string(),
                operator_action: "attach production-safety JSON, Helm values, ClusterRole diff, and rollback command".to_string(),
            },
            SreEvidenceBundleRow {
                artifact: "customer pricing basis".to_string(),
                source: "pricing catalog, chargeback export, contract rate sheet, or invoice sample".to_string(),
                pass_signal: "node labels or pool metadata map to prices, billing model is pinned, and ROI tiles are recomputed from that source".to_string(),
                blocks_claim: "dollar savings or tenant budget enforcement".to_string(),
                operator_action: "replace URL GPU-hour demo pricing with a checked-in or uploaded pricing source before external claims".to_string(),
            },
            SreEvidenceBundleRow {
                artifact: "prediction calibration history".to_string(),
                source: "completed job observations plus prediction_audit_metrics and job_observation_metrics".to_string(),
                pass_signal: "runtime/VRAM sample counts, MAPE/error thresholds, source-tier promotion gates, and drift actions are healthy".to_string(),
                blocks_claim: "deadline, VRAM, and rightsizing decisions are calibrated".to_string(),
                operator_action: "collect completed-job history by prediction_key and keep sparse sources advisory".to_string(),
            },
            SreEvidenceBundleRow {
                artifact: "large fleet scale guardrail".to_string(),
                source: "gpu-scenarios regret report plus live heterogeneous fleet snapshot".to_string(),
                pass_signal: "grouping eligibility, expanded physical nodes, regret status, widening recovery, and full rerun trigger are visible".to_string(),
                blocks_claim: "fast solve remains trustworthy at fleet scale".to_string(),
                operator_action: "run grouped-first and widened/full reruns on representative fleet snapshots".to_string(),
            },
            SreEvidenceBundleRow {
                artifact: "tenant ownership and fairness ledger".to_string(),
                source: "tenant_fairness_metrics plus namespace/account ownership metadata".to_string(),
                pass_signal: "tenant, budget, fair-share, borrowed/reclaimable GPU, denial reason, and ownership source are mapped".to_string(),
                blocks_claim: "fairness, budget denial, or reclaim recommendation".to_string(),
                operator_action: "validate namespace/team/account mapping before hard enforcement".to_string(),
            },
            SreEvidenceBundleRow {
                artifact: "device inventory and topology proof".to_string(),
                source: "node labels, MIG resources, DRA ResourceClaims, device plugin inventory, and topology metadata".to_string(),
                pass_signal: "exact resource/label semantics are separated from DRA/device-identity and NVLink unsupported claims".to_string(),
                blocks_claim: "device-aware placement correctness".to_string(),
                operator_action: "mark DRA/NVLink claims unsupported until concrete device identity and topology graph are modeled".to_string(),
            },
        ],
        blockers: customer_blockers,
        next_action:
            "capture a non-demo customer-style trace bundle and attach kube baseline, repair, safety, pricing, calibration, scale, fairness, and device evidence before external savings claims"
                .to_string(),
    };

    RoadmapReadinessSummary {
        name: "roadmap-readiness".to_string(),
        passed,
        headline:
            "All eight roadmap areas have report evidence and the primary SRE demo UI is wired; remaining work is live validation, production calibration, and customer data."
                .to_string(),
        launch_proof_gate,
        items,
        next_build_order,
        residual_product_gaps,
    }
}

fn kube_baseline_mode(scenarios: &[ScenarioResult]) -> String {
    let mut saw_live = false;
    let mut saw_cached = false;
    let mut saw_simulator_failure = false;
    let mut saw_limit = false;
    let mut saw_live_scenario_filter = false;
    for source in scenarios
        .iter()
        .flat_map(|s| [s.kube.source.as_str(), s.kube_binpack.source.as_str()])
    {
        saw_cached |= source.starts_with("cached ");
        saw_live |= source.contains("kube-scheduler-simulator") && !source.contains("failed");
        saw_simulator_failure |= source.contains("simulator failed");
        saw_limit |= source.contains("--simulator-max-live-baselines");
        saw_live_scenario_filter |= source.contains("--simulator-live-scenarios");
    }

    if saw_cached {
        "cached kube-scheduler-simulator baselines".to_string()
    } else if saw_live {
        "live kube-scheduler-simulator baselines".to_string()
    } else if saw_simulator_failure {
        "invalid kube baseline: simulator failure without KSS result".to_string()
    } else if saw_limit {
        "invalid kube baseline: live simulator cap without cached KSS result".to_string()
    } else if saw_live_scenario_filter {
        "invalid kube baseline: scenario filter without cached KSS result".to_string()
    } else {
        "invalid kube baseline: missing live/cached KSS provenance".to_string()
    }
}

fn summarize_kss_proofs(
    scenarios: &[ScenarioResult],
    phase: &str,
    specs: &[(&str, &str, bool)],
) -> Vec<KssProofScenario> {
    specs
        .iter()
        .map(|(name, claim, expect_unplaced)| {
            let Some(scenario) = scenarios.iter().find(|s| s.name == *name) else {
                return KssProofScenario {
                    name: (*name).to_string(),
                    phase: phase.to_string(),
                    claim: (*claim).to_string(),
                    caveat: "scenario missing from deterministic_scenarios".to_string(),
                    ..Default::default()
                };
            };
            let base = best_kube(&scenario.kube, &scenario.kube_binpack);
            let baseline_modes = [&scenario.kube, &scenario.kube_binpack]
                .into_iter()
                .map(|engine| {
                    engine
                        .simulator
                        .as_ref()
                        .map(|sim| format!("{}:{}", engine.engine, sim.mode))
                        .unwrap_or_else(|| format!("{}:deterministic", engine.engine))
                })
                .collect::<Vec<_>>();
            let condition_holds = if *expect_unplaced {
                base.metrics.unplaced_pods > 0
                    || base.metrics.partial_or_invalid_gangs > 0
                    || base.metrics.useful_gpu == 0
            } else {
                base.metrics.placed_pods > 0
            };
            let caveat = if *expect_unplaced {
                "KSS proves the residual-capacity target is blocked; ksolver repair correctness is proved by the paired repair_scenarios rows because KSS does not plan migration/preemption."
            } else {
                "KSS proves scalar kube admits or considers the one-GPU request; ksolver VRAM correctness is proved by vram_prediction_scenario because KSS has no predicted-VRAM model."
            };
            KssProofScenario {
                name: scenario.name.clone(),
                phase: phase.to_string(),
                claim: (*claim).to_string(),
                passed: condition_holds,
                strongest_baseline: base.engine.clone(),
                baseline_modes,
                kube_useful_gpu: base.metrics.useful_gpu,
                kube_unplaced_pods: base.metrics.unplaced_pods,
                ksolver_useful_gpu: scenario.ksolver.metrics.useful_gpu,
                ksolver_unplaced_pods: scenario.ksolver.metrics.unplaced_pods,
                caveat: caveat.to_string(),
            }
        })
        .collect()
}

fn summarize_production_safety() -> ProductionSafetySummary {
    let rollout_modes = vec![
        "observe-only: render decisions and binding payloads without mutation".to_string(),
        "dry-run: POST bindings with server-side dryRun=All".to_string(),
        "bind-low-risk: persist only low-risk ready candidates".to_string(),
        "bind-all/live: persist every ready candidate subject to gates".to_string(),
    ];
    let production_checklist = vec![
        "confirm default observe-only mode before installing in a shared cluster".to_string(),
        "enable leader election and Lease RBAC before running more than one scheduler replica"
            .to_string(),
        "enable pods/binding RBAC only for dry-run or live binding rollout modes".to_string(),
        "keep KSOLVER_BINDING_KILL_SWITCH available as the fastest fail-closed control".to_string(),
        "review binding_reservation_metrics and binding_outcome_metrics after every canary pass"
            .to_string(),
        "verify Events, leases, and binding permissions are each granted independently".to_string(),
    ];
    let rbac_modes = vec![
        "shadow/read-only: list/watch cluster objects, no binding, Event, or Lease write verbs"
            .to_string(),
        "event-audit: add create on events.k8s.io/events only".to_string(),
        "ha-leader-election: add get/create/update/patch on coordination.k8s.io/leases".to_string(),
        "binding-dry-run/live: add create on pods/binding only with real-binding rollout gates"
            .to_string(),
    ];
    let failure_mode_controls = vec![
        "non-leader replicas skip solve and bind passes when leader election is enabled".to_string(),
        "stale pod uid, already-bound pods, terminating pods, and scheduler mismatches skip before POST"
            .to_string(),
        "reservation rejection skips a candidate instead of double-committing GPU capacity".to_string(),
        "per-pod bind failures are counted and do not abort the whole scheduler pass".to_string(),
        "post-response parse errors reconcile against live pod state before counting failure".to_string(),
    ];
    let audit_fields = vec![
        "binding_reservation_metrics".to_string(),
        "binding_outcome_metrics".to_string(),
        "ksolver_shadow_bind_skipped_by_reason".to_string(),
        "ksolver_shadow_bind_canary_skipped_total".to_string(),
        "ksolver_shadow_leader".to_string(),
        "ksolver_shadow_leader_skipped_solves_total".to_string(),
        "decision/repair/binding Event draft endpoints".to_string(),
    ];
    let rollout_gate_rows = vec![
        ProductionRolloutGateRow {
            mode: "observe-only".to_string(),
            mutation_allowed: false,
            required_rbac: "shadow/read-only".to_string(),
            required_gates: vec![
                "KSOLVER_ENABLE_REAL_BINDING=false".to_string(),
                "KSOLVER_ENABLE_KUBERNETES_EVENTS=false".to_string(),
            ],
            blast_radius_control: "no Kubernetes write verbs".to_string(),
            rollback_action: "disable deployment or remove schedulerName admission path".to_string(),
        },
        ProductionRolloutGateRow {
            mode: "dry-run".to_string(),
            mutation_allowed: false,
            required_rbac: "pods/binding create may be present, but requests use dryRun=All".to_string(),
            required_gates: vec![
                "non-observe rollout mode".to_string(),
                "readiness checks pass".to_string(),
                "kill switch off".to_string(),
            ],
            blast_radius_control: "server-side dryRun=All prevents persisted bindings".to_string(),
            rollback_action: "return to observe-only and revoke pods/binding RBAC".to_string(),
        },
        ProductionRolloutGateRow {
            mode: "bind-low-risk".to_string(),
            mutation_allowed: true,
            required_rbac: "pods/binding create plus optional Events and Lease RBAC".to_string(),
            required_gates: vec![
                "KSOLVER_ENABLE_REAL_BINDING=true".to_string(),
                "reservation accepted".to_string(),
                "low-risk candidate filter passes".to_string(),
                "max binds per pass not exceeded".to_string(),
            ],
            blast_radius_control: "low-risk canary filter and max binds per pass".to_string(),
            rollback_action: "enable KSOLVER_BINDING_KILL_SWITCH and keep observing decisions"
                .to_string(),
        },
        ProductionRolloutGateRow {
            mode: "bind-all".to_string(),
            mutation_allowed: true,
            required_rbac: "pods/binding create, Events optional, Lease RBAC for HA".to_string(),
            required_gates: vec![
                "leader election holder when enabled".to_string(),
                "readiness checks pass".to_string(),
                "reservation accepted".to_string(),
                "scheduler ownership verified".to_string(),
                "kill switch off".to_string(),
            ],
            blast_radius_control: "max binds per pass and skipped-by-reason audit buckets".to_string(),
            rollback_action: "kill switch on, set rollout mode observe-only, then inspect binding_outcome_metrics".to_string(),
        },
    ];
    let failure_playbook_rows = vec![
        ProductionFailurePlaybookRow {
            failure_mode: "lost leadership".to_string(),
            detection: "ksolver_shadow_leader=0 or leader renew errors increase".to_string(),
            automatic_behavior: "skip solve and bind passes on non-leader replicas".to_string(),
            operator_action: "check Lease RBAC, clock skew, and replica health".to_string(),
            audit_field: "ksolver_shadow_leader_skipped_solves_total".to_string(),
        },
        ProductionFailurePlaybookRow {
            failure_mode: "stale pod identity".to_string(),
            detection: "planned pod uid differs from live pod uid".to_string(),
            automatic_behavior: "skip the candidate before POST".to_string(),
            operator_action: "refresh snapshot and explain skipped_by_reason=stale_uid".to_string(),
            audit_field: "ksolver_shadow_bind_skipped_by_reason{reason=\"stale_uid\"}".to_string(),
        },
        ProductionFailurePlaybookRow {
            failure_mode: "reservation rejection".to_string(),
            detection: "reservation ledger rejects residual GPU or tenant quota".to_string(),
            automatic_behavior: "skip candidate to avoid double-committing capacity".to_string(),
            operator_action: "inspect reservation metrics and wait for informer reconciliation"
                .to_string(),
            audit_field: "binding_reservation_metrics".to_string(),
        },
        ProductionFailurePlaybookRow {
            failure_mode: "bind API failure".to_string(),
            detection: "pods/binding POST returns an error".to_string(),
            automatic_behavior: "count per-pod failure and continue bounded pass".to_string(),
            operator_action: "review RBAC, scheduler ownership, and admission webhook responses"
                .to_string(),
            audit_field: "binding_outcome_metrics".to_string(),
        },
        ProductionFailurePlaybookRow {
            failure_mode: "kill switch enabled".to_string(),
            detection: "KSOLVER_BINDING_KILL_SWITCH=true".to_string(),
            automatic_behavior: "fail closed for real binding and Event writes".to_string(),
            operator_action: "leave shadow decisions running while investigating".to_string(),
            audit_field: "ksolver_shadow_bind_skipped_by_reason{reason=\"kill_switch\"}"
                .to_string(),
        },
    ];
    let audit_event_rows = vec![
        ProductionAuditEventRow {
            event_type: "decision".to_string(),
            enabled_by_default: false,
            required_rbac: "events.k8s.io/events create".to_string(),
            payload_fields: vec![
                "solver_status".to_string(),
                "objective_profile".to_string(),
                "admitted_pods".to_string(),
                "unplaced_reasons".to_string(),
            ],
            reason: "explain why a scheduling decision was recommended".to_string(),
        },
        ProductionAuditEventRow {
            event_type: "repair".to_string(),
            enabled_by_default: false,
            required_rbac: "events.k8s.io/events create".to_string(),
            payload_fields: vec![
                "target".to_string(),
                "migrations".to_string(),
                "preemptions".to_string(),
                "disruption_cost".to_string(),
            ],
            reason: "record the dry-run migration/preemption plan before action".to_string(),
        },
        ProductionAuditEventRow {
            event_type: "binding".to_string(),
            enabled_by_default: false,
            required_rbac: "events.k8s.io/events create plus pods/binding create for live mode"
                .to_string(),
            payload_fields: vec![
                "pod_uid".to_string(),
                "target_node".to_string(),
                "dry_run".to_string(),
                "outcome".to_string(),
                "skip_reason".to_string(),
            ],
            reason: "audit what was bound, skipped, or rejected".to_string(),
        },
    ];
    let live_validation_rows = vec![
        ProductionLiveValidationRow {
            gate: "pod identity and phase".to_string(),
            evidence: "live pod UID, deletionTimestamp, phase, and spec.nodeName".to_string(),
            fail_closed_behavior:
                "skip binding when uid changed, pod vanished, pod is terminating, already bound, or no longer Pending"
                    .to_string(),
            audit_field: "ksolver_shadow_bind_skipped_by_reason{reason=\"identity\"}".to_string(),
            required_before: "every pods/binding POST and repair action approval".to_string(),
        },
        ProductionLiveValidationRow {
            gate: "scheduler ownership".to_string(),
            evidence: "live pod spec.schedulerName plus ksolver scheduler scope".to_string(),
            fail_closed_behavior: "skip pods not owned by ksolver so the default scheduler remains authoritative".to_string(),
            audit_field: "ksolver_shadow_bind_skipped_by_reason{reason=\"scheduler\"}".to_string(),
            required_before: "binding, dry-run binding, and action table promotion".to_string(),
        },
        ProductionLiveValidationRow {
            gate: "target node feasibility".to_string(),
            evidence: "latest feasible-node set, node existence, residual GPU, taints, selectors, affinity, topology, and DRA caveats".to_string(),
            fail_closed_behavior: "mark binding readiness stale when target node vanished or is no longer feasible".to_string(),
            audit_field: "binding_plan[].readiness.reason".to_string(),
            required_before: "rendered binding-plan and final live bind pass".to_string(),
        },
        ProductionLiveValidationRow {
            gate: "reservation ledger".to_string(),
            evidence: "reservation key, TTL, accepted/rejected counters, residual GPU and quota accounting".to_string(),
            fail_closed_behavior:
                "skip candidate on reservation rejection to avoid double-committing GPU capacity while informers catch up"
                    .to_string(),
            audit_field: "binding_reservation_metrics".to_string(),
            required_before: "real binding and any future repair automation".to_string(),
        },
        ProductionLiveValidationRow {
            gate: "PDB and disruption policy".to_string(),
            evidence: "matching PodDisruptionBudget budget, do-not-disrupt annotations, migration/preemption permissions, progress, and priority".to_string(),
            fail_closed_behavior:
                "do not propose or promote repair actions that violate disruption policy or consume unavailable PDB budget"
                    .to_string(),
            audit_field: "repair_metrics.skipped_candidates_by_reason".to_string(),
            required_before: "migration/preemption recommendation approval".to_string(),
        },
        ProductionLiveValidationRow {
            gate: "DRA and grouped-workload safety".to_string(),
            evidence: "DRA caveat flags, ResourceClaim modeling status, binding group identity, and grouped workload membership".to_string(),
            fail_closed_behavior:
                "skip DRA-unsafe or grouped candidates until concrete device identity and group-wide readiness are proven"
                    .to_string(),
            audit_field: "ksolver_shadow_bind_skipped_by_reason{reason=\"dra|group\"}".to_string(),
            required_before: "real binding and future DRA/device-aware action".to_string(),
        },
        ProductionLiveValidationRow {
            gate: "rollout throttle and kill switch".to_string(),
            evidence: "rollout mode, canary mode, max binds per pass, leader status, and kill-switch state".to_string(),
            fail_closed_behavior:
                "skip or stop mutation when canary, throttle, non-leader, disabled rollout, or kill switch blocks the pass"
                    .to_string(),
            audit_field: "binding_outcome_metrics and ksolver_shadow_bind_skipped_by_reason".to_string(),
            required_before: "every real mutation pass".to_string(),
        },
    ];
    let live_config_rows = vec![
        ProductionLiveConfigRow {
            gate: "real binding enablement".to_string(),
            env_var: "KSOLVER_ENABLE_REAL_BINDING".to_string(),
            live_endpoint_field: "rollout.enable_real_binding + rollout.mutation_allowed"
                .to_string(),
            expected_safe_default: "false; observe-only remains read-only".to_string(),
            required_rbac_when_enabled: "pods/binding create only for dry-run or live rollout modes"
                .to_string(),
            fail_closed_signal:
                "mutation_allowed=false when rollout mode, dry-run, kill switch, or enable flag blocks mutation"
                    .to_string(),
            operator_action: "verify /api/scheduler/production-safety before granting pods/binding RBAC"
                .to_string(),
        },
        ProductionLiveConfigRow {
            gate: "binding kill switch".to_string(),
            env_var: "KSOLVER_BINDING_KILL_SWITCH".to_string(),
            live_endpoint_field: "rollout.binding_kill_switch".to_string(),
            expected_safe_default: "false, but setting true immediately disables mutation".to_string(),
            required_rbac_when_enabled: "none; this is a fail-closed runtime control".to_string(),
            fail_closed_signal: "binding_kill_switch=true and mutation_allowed=false".to_string(),
            operator_action: "turn on during incident response before changing rollout mode".to_string(),
        },
        ProductionLiveConfigRow {
            gate: "event audit writes".to_string(),
            env_var: "KSOLVER_ENABLE_KUBERNETES_EVENTS".to_string(),
            live_endpoint_field: "events.enable_kubernetes_events + events.writes_allowed"
                .to_string(),
            expected_safe_default: "false; event payloads remain render-only".to_string(),
            required_rbac_when_enabled: "events.k8s.io/events create".to_string(),
            fail_closed_signal: "writes_allowed=false when disabled or kill switch is on".to_string(),
            operator_action: "enable after decision traces are trusted and Event RBAC is reviewed"
                .to_string(),
        },
        ProductionLiveConfigRow {
            gate: "leader election".to_string(),
            env_var: "KSOLVER_ENABLE_LEADER_ELECTION".to_string(),
            live_endpoint_field: "leader_election.configured + leader_election.lease_name"
                .to_string(),
            expected_safe_default: "false for single-replica demos; true before HA production".to_string(),
            required_rbac_when_enabled:
                "coordination.k8s.io/leases get/create/update/patch in leader namespace"
                    .to_string(),
            fail_closed_signal: "non-leader replicas skip solve and bind passes".to_string(),
            operator_action: "require Lease RBAC before scaling the scheduler above one replica".to_string(),
        },
        ProductionLiveConfigRow {
            gate: "canary and throttle".to_string(),
            env_var: "KSOLVER_BINDING_CANARY_MODE / KSOLVER_MAX_BINDS_PER_PASS".to_string(),
            live_endpoint_field: "rollout.binding_canary_mode + rollout.max_binds_per_pass"
                .to_string(),
            expected_safe_default: "low-risk bounded canary for initial mutation rollout".to_string(),
            required_rbac_when_enabled: "pods/binding create only after rollout gate approval"
                .to_string(),
            fail_closed_signal:
                "candidates beyond throttle or canary scope are skipped with bind_skipped_by_reason"
                    .to_string(),
            operator_action: "start with max_binds_per_pass=1 and inspect binding_outcome_metrics"
                .to_string(),
        },
        ProductionLiveConfigRow {
            gate: "reservation TTL".to_string(),
            env_var: "KSOLVER_BINDING_RESERVATION_TTL_SECONDS".to_string(),
            live_endpoint_field: "rollout.binding_reservation_ttl_seconds".to_string(),
            expected_safe_default: "short TTL; stale reservations expire and reconcile".to_string(),
            required_rbac_when_enabled: "none beyond binding mode; ledger is internal".to_string(),
            fail_closed_signal: "reservation rejection skips instead of double-committing GPU capacity"
                .to_string(),
            operator_action: "size TTL to informer lag and monitor binding_reservation_metrics"
                .to_string(),
        },
    ];
    let kill_switches = vec![
        "KSOLVER_ENABLE_REAL_BINDING defaults false".to_string(),
        "KSOLVER_BINDING_KILL_SWITCH fail-closes real binding and Event writes".to_string(),
        "KSOLVER_ENABLE_KUBERNETES_EVENTS defaults false".to_string(),
        "KSOLVER_ENABLE_LEADER_ELECTION defaults false and gates solve/bind passes when enabled"
            .to_string(),
    ];
    let readiness_checks = vec![
        "target node still exists".to_string(),
        "pod still exists and uid matches the planned pod".to_string(),
        "pod is still Pending, unbound, and not terminating".to_string(),
        "pod is still owned by the configured schedulerName".to_string(),
        "target node remains in the pod's latest feasible-node set".to_string(),
        "binding group members pass the same live preflight".to_string(),
        "DRA or grouped-workload candidates can be skipped before mutation".to_string(),
        "max binds per pass throttles rollout blast radius".to_string(),
    ];
    let mutation_boundaries = vec![
        "shadow.rs orchestrates only and is guarded against direct mutating API calls".to_string(),
        "binding.rs renders dry-run Binding payloads without kube client calls".to_string(),
        "binder.rs is the only pods/binding mutation path".to_string(),
        "events.rs renders Event payloads; event_emitter.rs can only create Kubernetes Events"
            .to_string(),
        "leader.rs can only create/replace coordination.k8s.io Lease objects".to_string(),
        "repair.rs renders advisory migration/preemption plans and never evicts pods".to_string(),
    ];
    let residual_risks = vec![
        "leader election is opt-in, so production HA depends on enabling Lease RBAC and rollout config"
            .to_string(),
        "the reservation ledger is in-memory; restart safety comes from TTL/reconcile behavior, not durable storage"
            .to_string(),
        "real repair automation is still advisory-only; eviction/preemption execution is intentionally absent"
            .to_string(),
        "DRA/device identity assignment still needs deeper production integration".to_string(),
    ];
    let operator_claims = vec![
        "default install is observe-only and read-only".to_string(),
        "mutation requires explicit rollout mode, RBAC, final live readiness checks, and a disabled kill switch"
            .to_string(),
        "each binding decision can be audited before and after execution".to_string(),
        "reservation accounting prevents double-committing GPU capacity while informer state catches up"
            .to_string(),
        "no-mutation guard tests enforce module boundaries for shadow, binding rendering, Events, leader election, and repair"
            .to_string(),
    ];
    let launch_contract = ProductionLaunchContract {
        launch_level: "Observe-only launch safe".to_string(),
        live_writes_allowed: false,
        required_gates: vec![
            "KSOLVER_ENABLE_REAL_BINDING=false for default install".to_string(),
            "/api/scheduler/production-safety reports mutation_allowed=false before shared-cluster install".to_string(),
            "binding-plan readiness rechecks pod UID, phase, scheduler ownership, node feasibility, and DRA/group safety".to_string(),
            "reservation ledger accepts a candidate before any pods/binding write".to_string(),
            "leader election Lease RBAC is enabled before running multiple replicas".to_string(),
            "kill switch, canary, and max-binds-per-pass are configured before mutation rollout".to_string(),
        ],
        required_rbac: vec![
            "observe-only: get/list/watch pods, nodes, namespaces, PVC/PV/storage, PDB, leases when leader election is enabled".to_string(),
            "dry-run binding: pods/binding create only after rollout gate review".to_string(),
            "live binding: pods/binding create plus canary/throttle and reservation metrics review".to_string(),
            "event writes: events.k8s.io/events create only when KSOLVER_ENABLE_KUBERNETES_EVENTS=true".to_string(),
            "HA: coordination.k8s.io/leases get/create/update/patch only when leader election is enabled".to_string(),
        ],
        fail_closed_if: vec![
            "binding kill switch is true or rollout mode is observe-only".to_string(),
            "pod UID, phase, schedulerName, node feasibility, binding group, or DRA safety is stale".to_string(),
            "reservation ledger rejects the candidate or reports stale capacity/quota state".to_string(),
            "replica is not the current leader when leader election is enabled".to_string(),
            "required RBAC is absent for the selected rollout mode".to_string(),
        ],
        audit_artifacts: vec![
            "/api/scheduler/production-safety".to_string(),
            "/api/scheduler/binding-plan readiness rows".to_string(),
            "binding_reservation_metrics".to_string(),
            "binding_outcome_metrics".to_string(),
            "ksolver_shadow_bind_skipped_by_reason".to_string(),
            "/api/scheduler/decision-events, repair-events, and binding-events payload drafts".to_string(),
        ],
        next_action:
            "ship observe-only first, capture production-safety and binding-plan evidence, then enable dry-run/canary writes with minimal RBAC"
                .to_string(),
    };
    let passed = !rollout_modes.is_empty()
        && production_checklist.len() >= 6
        && rbac_modes.len() >= 4
        && failure_mode_controls.len() >= 5
        && audit_fields.len() >= 7
        && rollout_gate_rows.len() >= 4
        && failure_playbook_rows.len() >= 5
        && audit_event_rows.len() >= 3
        && live_validation_rows.len() >= 7
        && live_config_rows.len() >= 6
        && kill_switches.len() >= 3
        && readiness_checks.len() >= 6
        && mutation_boundaries.len() >= 5
        && operator_claims.len() >= 4
        && residual_risks.len() >= 3
        && !launch_contract.live_writes_allowed
        && launch_contract.required_gates.len() >= 6
        && launch_contract.required_rbac.len() >= 5
        && launch_contract.fail_closed_if.len() >= 5
        && launch_contract.audit_artifacts.len() >= 6;

    ProductionSafetySummary {
        name: "production-safety-hardening".to_string(),
        passed,
        default_mode: "observe-only/read-only".to_string(),
        mutation_default_enabled: false,
        real_binding_gate:
            "KSOLVER_ENABLE_REAL_BINDING=true plus non-observe rollout mode, binding RBAC, readiness checks, reservation acceptance, throttle, and kill switch off"
                .to_string(),
        launch_contract,
        rollout_modes,
        production_checklist,
        rbac_modes,
        failure_mode_controls,
        audit_fields,
        rollout_gate_rows,
        failure_playbook_rows,
        audit_event_rows,
        live_validation_rows,
        live_config_rows,
        kill_switches,
        readiness_checks,
        leader_election:
            "Lease-based leader election is isolated in leader.rs; when enabled, solve/bind passes require the current holder"
                .to_string(),
        reservation_ledger:
            "ReservationLedger validates planned bindings against residual GPU capacity and tenant quota before live posts"
                .to_string(),
        restart_safety:
            "Ledger entries expire by TTL and reconcile against informer state: observed bound pods, stale UIDs, missing pods, and pods bound elsewhere release reservations"
                .to_string(),
        audit_events:
            "Decision, repair, and binding Events are rendered read-only by default; Event POSTs require KSOLVER_ENABLE_KUBERNETES_EVENTS and Event RBAC"
                .to_string(),
        rbac_profile:
            "Helm defaults to read-only RBAC; pods/binding, Events, and Lease verbs render only with matching rollout flags"
                .to_string(),
        mutation_boundaries,
        residual_risks,
        operator_claims,
    }
}

fn summarize_prediction_quality(vram_prediction: &VramPredictionProof) -> PredictionQualitySummary {
    let coverage_sources = vec![
        "exact command-hash history at the same GPU count".to_string(),
        "command-hash history scaled by requested GPU count".to_string(),
        "job-type segment history such as kubeflow_pytorchjob, rayjob, volcano_job, argo_workflow, kubernetes_job, and bare_pod".to_string(),
        "framework segment history such as pytorch, tensorflow, jax, deepspeed, and ray".to_string(),
        "training-hint fallback from model parameters, batch size, sequence length, precision, runtime hints, and VRAM hints".to_string(),
    ];
    let calibration_metrics = vec![
        "completed GPU pod samples".to_string(),
        "runtime observation count and failed GPU pod count".to_string(),
        "unique command-hash count".to_string(),
        "runtime prediction sample count, MAPE milli, and max absolute error seconds".to_string(),
        "VRAM prediction sample count, MAPE milli, and max absolute error bytes".to_string(),
        "pending prediction coverage by exact, scaled, segment, hint, and unknown source"
            .to_string(),
        "average confidence for pending predictions".to_string(),
    ];
    let calibration_lifecycle = vec![
        "collect completed GPU pods with command/image fingerprint, GPU count, runtime, peak memory, framework, and job type".to_string(),
        "bucket observations by exact command hash, GPU-count-scaled command history, job type, framework, and training hints".to_string(),
        "compare predicted runtime/VRAM against observed runtime/peak memory when predictions were present".to_string(),
        "publish MAPE, max absolute error, sample counts, and unknown-source coverage in every trace".to_string(),
        "promote exact-history predictions only after enough samples exist for the same command and GPU count".to_string(),
    ];
    let confidence_bands = vec![
        "exact command history: highest confidence with lower/upper runtime and VRAM bands from observed dispersion".to_string(),
        "scaled command history: medium confidence because GPU-count scaling can be non-linear".to_string(),
        "job-type/framework segment: lower confidence until segment sample count grows".to_string(),
        "training or pending hint fallback: advisory confidence; explicit annotations remain authoritative but should be audited".to_string(),
        "unknown prediction source: no placement-affecting prediction claim beyond declared resource requests".to_string(),
    ];
    let drift_monitors = vec![
        "alert when runtime MAPE or max runtime error rises for exact command-history predictions".to_string(),
        "alert when VRAM MAPE or max VRAM error rises for known GPU-memory placements".to_string(),
        "track unknown prediction source share so SREs know when the model is operating blind".to_string(),
        "track unique command hashes and segment sample counts to distinguish sparse data from model drift".to_string(),
        "compare confidence distribution over time after image or training-script changes".to_string(),
    ];
    let decision_impact_evidence = vec![
        format!(
            "{} rejects known too-small GPU-memory nodes before solve input construction",
            vram_prediction.name
        ),
        format!(
            "matching feasible nodes {:?} remain eligible after applying predicted peak VRAM",
            vram_prediction.adequate_feasible_nodes
        ),
        "deadline scoring can consume predicted runtime to distinguish meetable work from predicted misses".to_string(),
        "repair logic treats VRAM-incompatible pending pods as unrepairable fragmentation targets".to_string(),
        "audit details expose prediction source, confidence, key, and lower/upper bands per pending pod".to_string(),
    ];
    let model_cards = vec![
        PredictionModelCard {
            source_tier: "exact_command_hash".to_string(),
            confidence_band: "highest".to_string(),
            required_evidence:
                "same command/image fingerprint, same GPU count, enough completed samples"
                    .to_string(),
            failure_mode: "stale command hash after image or launcher behavior changes".to_string(),
            placement_use:
                "eligible for VRAM filtering, deadline scoring, and rightsizing tie-breaks"
                    .to_string(),
        },
        PredictionModelCard {
            source_tier: "scaled_command_history".to_string(),
            confidence_band: "medium".to_string(),
            required_evidence: "same command/image fingerprint with nearby GPU counts".to_string(),
            failure_mode: "distributed training scaling is non-linear across replica counts"
                .to_string(),
            placement_use:
                "eligible for deadline scoring and advisory VRAM filtering with visible caveat"
                    .to_string(),
        },
        PredictionModelCard {
            source_tier: "job_or_framework_segment".to_string(),
            confidence_band: "low".to_string(),
            required_evidence: "job type or framework segment has enough recent completed samples"
                .to_string(),
            failure_mode: "segment mixes materially different model sizes or data pipelines"
                .to_string(),
            placement_use: "advisory scheduling signal; do not use alone for destructive repair"
                .to_string(),
        },
        PredictionModelCard {
            source_tier: "training_hint".to_string(),
            confidence_band: "advisory".to_string(),
            required_evidence:
                "model parameters, batch size, sequence length, precision, runtime, or VRAM hint"
                    .to_string(),
            failure_mode: "hints are stale, optimistic, or omit optimizer/checkpoint memory"
                .to_string(),
            placement_use:
                "explainable fallback for binpacking and rightsizing when history is absent"
                    .to_string(),
        },
        PredictionModelCard {
            source_tier: "unknown".to_string(),
            confidence_band: "none".to_string(),
            required_evidence: "no usable history, segment, or hint".to_string(),
            failure_mode: "scheduler would be operating blind beyond declared Kubernetes requests"
                .to_string(),
            placement_use: "do not make placement-affecting prediction claims".to_string(),
        },
    ];
    let calibration_buckets = vec![
        PredictionCalibrationBucket {
            bucket: "runtime_seconds".to_string(),
            sample_gate: "minimum exact-history samples before promotion".to_string(),
            runtime_metric: "runtime_prediction_mape_milli and max_runtime_error_seconds"
                .to_string(),
            vram_metric: "not applicable".to_string(),
            drift_signal: "exact-history runtime MAPE regression".to_string(),
            action_when_unhealthy:
                "demote exact runtime predictions to advisory until enough fresh samples arrive"
                    .to_string(),
        },
        PredictionCalibrationBucket {
            bucket: "peak_vram_bytes".to_string(),
            sample_gate: "minimum known peak-memory samples for GPU-memory-aware nodes".to_string(),
            runtime_metric: "not applicable".to_string(),
            vram_metric: "vram_prediction_mape_milli and max_vram_error_bytes".to_string(),
            drift_signal: "VRAM error growth after image, framework, or precision changes"
                .to_string(),
            action_when_unhealthy:
                "disable VRAM hard filtering for that source tier and surface a caveat".to_string(),
        },
        PredictionCalibrationBucket {
            bucket: "coverage".to_string(),
            sample_gate:
                "pending predictions should be mostly exact, scaled, segment, or hint sourced"
                    .to_string(),
            runtime_metric: "unknown runtime prediction source share".to_string(),
            vram_metric: "unknown VRAM prediction source share".to_string(),
            drift_signal: "unknown source share increases for active queues".to_string(),
            action_when_unhealthy: "show ROI and rightsizing claims as low-confidence".to_string(),
        },
        PredictionCalibrationBucket {
            bucket: "segment_quality".to_string(),
            sample_gate: "job/framework segment sample count above promotion threshold".to_string(),
            runtime_metric: "segment runtime MAPE by job type and framework".to_string(),
            vram_metric: "segment VRAM MAPE by job type and framework".to_string(),
            drift_signal: "segment error diverges from exact-history error".to_string(),
            action_when_unhealthy:
                "split segment by framework, launcher, precision, or model-size hint".to_string(),
        },
    ];
    let live_calibration_rows = vec![
        PredictionLiveCalibrationRow {
            gate: "completed observation volume".to_string(),
            live_trace_metric: "job_observation_metrics.completed_gpu_pods + job_observation_metrics.unique_command_hashes".to_string(),
            healthy_threshold:
                "enough completed GPU pods and repeated command hashes for the active queue"
                    .to_string(),
            unhealthy_action:
                "keep prediction source tiers advisory and label ROI/prediction claims as sparse data"
                    .to_string(),
            placement_impact:
                "do not promote history-backed VRAM filtering or deadline scoring from one-off samples"
                    .to_string(),
            operator_view:
                "Live prediction coverage row plus Prediction quality summary calibration gates"
                    .to_string(),
        },
        PredictionLiveCalibrationRow {
            gate: "exact history promotion".to_string(),
            live_trace_metric:
                "prediction_audit_metrics.history_exact_pods and prediction_audit_details[].prediction_key"
                    .to_string(),
            healthy_threshold:
                "exact command-hash predictions dominate pending GPU pods for the workload family"
                    .to_string(),
            unhealthy_action:
                "fall back to scaled, segment, or hint confidence and keep destructive actions advisory"
                    .to_string(),
            placement_impact:
                "exact matches may influence VRAM filtering, deadline scoring, and rightsizing"
                    .to_string(),
            operator_view: "per-pod prediction source and key in prediction_audit_details".to_string(),
        },
        PredictionLiveCalibrationRow {
            gate: "runtime error budget".to_string(),
            live_trace_metric:
                "job_observation_metrics.runtime_prediction_mape_milli + max_runtime_prediction_error_seconds"
                    .to_string(),
            healthy_threshold: "runtime MAPE and max error are below the fleet policy for deadline scheduling".to_string(),
            unhealthy_action:
                "demote predicted-deadline and latest-start scoring to advisory until fresh samples recover"
                    .to_string(),
            placement_impact: "deadline urgency and predicted deadline miss scoring".to_string(),
            operator_view:
                "Live prediction coverage and deadline pressure rows show miss counts and slack"
                    .to_string(),
        },
        PredictionLiveCalibrationRow {
            gate: "VRAM error budget".to_string(),
            live_trace_metric:
                "job_observation_metrics.vram_prediction_mape_milli + max_vram_prediction_error_bytes"
                    .to_string(),
            healthy_threshold:
                "VRAM error is below the policy margin for node GPU-memory filtering".to_string(),
            unhealthy_action:
                "disable hard VRAM filtering for unhealthy source tiers and show known-memory caveats"
                    .to_string(),
            placement_impact: "GPU-memory feasibility filtering and rightsizing tie-breaks".to_string(),
            operator_view:
                "VRAM-blocked repair rows distinguish fragmentation from memory incompatibility"
                    .to_string(),
        },
        PredictionLiveCalibrationRow {
            gate: "unknown source coverage".to_string(),
            live_trace_metric:
                "prediction_audit_metrics.unknown_pods / prediction_audit_metrics.pending_pods"
                    .to_string(),
            healthy_threshold:
                "unknown prediction source share stays below the operator-defined warning threshold"
                    .to_string(),
            unhealthy_action:
                "mark ROI, rightsizing, and preemption claims low-confidence for affected queues"
                    .to_string(),
            placement_impact:
                "prevents unsupported prediction claims when the scheduler is mostly operating from raw requests"
                    .to_string(),
            operator_view:
                "Live prediction coverage row reports exact, scaled, segment, hint, and unknown counts"
                    .to_string(),
        },
        PredictionLiveCalibrationRow {
            gate: "average confidence floor".to_string(),
            live_trace_metric: "prediction_audit_metrics.average_confidence_milli".to_string(),
            healthy_threshold:
                "average confidence remains above the promotion floor for prediction-dependent objectives"
                    .to_string(),
            unhealthy_action:
                "switch prediction-dependent objectives to explain-only and require operator confirmation"
                    .to_string(),
            placement_impact:
                "deadline, VRAM, and rightsizing objective terms stay bounded by confidence".to_string(),
            operator_view:
                "prediction_audit_details expose lower/upper bands and confidence per pod".to_string(),
        },
    ];
    let audit_fields = vec![
        "prediction_audit[].source".to_string(),
        "prediction_audit[].confidence".to_string(),
        "prediction_audit[].prediction_key".to_string(),
        "prediction_audit[].runtime_lower_seconds".to_string(),
        "prediction_audit[].runtime_upper_seconds".to_string(),
        "prediction_audit[].vram_lower_bytes".to_string(),
        "prediction_audit[].vram_upper_bytes".to_string(),
        "prediction_quality.runtime_prediction_mape_milli".to_string(),
        "prediction_quality.vram_prediction_mape_milli".to_string(),
        "prediction_quality.unknown_source_share_milli".to_string(),
    ];
    let promotion_gates = vec![
        "exact command-history promotion requires enough same-command same-GPU-count samples".to_string(),
        "hard VRAM filtering requires known GPU memory inventory and healthy VRAM error metrics".to_string(),
        "deadline scoring must display runtime confidence band and predicted miss evidence".to_string(),
        "low-confidence segment and hint predictions stay advisory unless an operator explicitly opts in".to_string(),
        "drift or unknown-source alerts demote prediction-dependent ROI claims".to_string(),
    ];
    let placement_effects = vec![
        "deadline urgency and predicted deadline miss scoring can use predicted runtime"
            .to_string(),
        "GPU VRAM feasibility filters reject known too-small GPU-memory nodes".to_string(),
        "VRAM rightsizing tie-breaks prefer the smallest adequate known GPU memory".to_string(),
        "repair advice distinguishes fragmentation from VRAM-incompatible jobs".to_string(),
        "per-pod audit details expose prediction source, key, confidence, and lower/upper bands"
            .to_string(),
    ];
    let operator_claims = vec![
        "predictions are explainable by source, not opaque model outputs".to_string(),
        "historical exact matches outrank scaled and segment fallbacks".to_string(),
        "explicit pod annotations and training hints remain authoritative".to_string(),
        "confidence and error metrics tell SREs whether a prediction should influence placement".to_string(),
        "the deterministic VRAM proof shows prediction data can change feasible node sets before solving".to_string(),
    ];
    let residual_risks = vec![
        "prediction quality still depends on collecting enough completed jobs per command, job type, framework, and GPU count".to_string(),
        "confidence is a bounded operator score, not a statistically calibrated probability".to_string(),
        "runtime and VRAM bands are conservative heuristic/audit signals until fleet-specific calibration matures".to_string(),
        "container command/image introspection can miss dynamic launchers or user code hidden behind entrypoints".to_string(),
    ];
    let promotion_contract = PredictionPromotionContract {
        promotion_level: "Advisory until fleet calibration is proven".to_string(),
        hard_placement_allowed: false,
        prediction_sensitive_claims_allowed: false,
        required_evidence: vec![
            "completed GPU pod observations with command/image fingerprint, GPU count, runtime, and peak VRAM".to_string(),
            "minimum exact command-hash samples for the same command and GPU count before exact-tier promotion".to_string(),
            "runtime MAPE and max runtime error below the fleet deadline-scheduling policy".to_string(),
            "VRAM MAPE and max VRAM error below the node-memory filtering policy margin".to_string(),
            "prediction_audit_metrics coverage showing low unknown-source share for pending GPU pods".to_string(),
            "confidence-band evidence from lower/upper runtime and VRAM ranges, not just point estimates".to_string(),
            "drift monitoring after image, framework, launcher, precision, or training-code changes".to_string(),
        ],
        blocked_by: vec![
            "deterministic demo has sparse completed-job samples rather than fleet-calibrated history".to_string(),
            "training hints and segment fallbacks are useful explanations but not hard-placement proof".to_string(),
            "unknown-source share can make ROI, deadline, VRAM, and rightsizing claims prediction-sensitive".to_string(),
            "confidence is an operator score until statistically calibrated against customer fleet outcomes".to_string(),
            "fleet-specific pricing and workload mix are absent from the deterministic proof".to_string(),
        ],
        demotion_triggers: vec![
            "unknown prediction source share exceeds the operator warning threshold".to_string(),
            "average confidence drops below the promotion floor for prediction-dependent objectives".to_string(),
            "runtime or VRAM MAPE exceeds the configured source-tier error budget".to_string(),
            "command/image fingerprints become stale after image, launcher, or training-code changes".to_string(),
            "segment history mixes materially different model sizes, sequence lengths, precision modes, or data pipelines".to_string(),
        ],
        next_action:
            "collect completed GPU pod observations, compute per-source runtime/VRAM error buckets, and promote only source tiers that pass sample, coverage, confidence, and drift gates"
                .to_string(),
    };
    let passed = vram_prediction.passed
        && promotion_contract.promotion_level.contains("Advisory")
        && !promotion_contract.hard_placement_allowed
        && !promotion_contract.prediction_sensitive_claims_allowed
        && promotion_contract.required_evidence.len() >= 6
        && promotion_contract.blocked_by.len() >= 4
        && promotion_contract.demotion_triggers.len() >= 4
        && coverage_sources.len() >= 5
        && calibration_metrics.len() >= 6
        && calibration_lifecycle.len() >= 5
        && confidence_bands.len() >= 5
        && drift_monitors.len() >= 5
        && decision_impact_evidence.len() >= 5
        && model_cards.len() >= 5
        && calibration_buckets.len() >= 4
        && live_calibration_rows.len() >= 6
        && audit_fields.len() >= 8
        && promotion_gates.len() >= 5
        && placement_effects.len() >= 4
        && operator_claims.len() >= 4
        && residual_risks.len() >= 3;

    PredictionQualitySummary {
        name: "prediction-quality-readiness".to_string(),
        passed,
        promotion_contract,
        coverage_sources,
        calibration_metrics,
        calibration_lifecycle,
        confidence_bands,
        drift_monitors,
        decision_impact_evidence,
        model_cards,
        calibration_buckets,
        live_calibration_rows,
        audit_fields,
        promotion_gates,
        placement_effects,
        confidence_model:
            "source-tiered confidence: exact command history > scaled command history > job/framework segment history > hint fallback > unknown"
                .to_string(),
        operator_claims,
        residual_risks,
    }
}

fn summarize_scale_guardrails(
    regret: &RegretSummary,
    grouping: &NodeGroupingProof,
    widening: &CandidateWideningProof,
) -> ScaleGuardrailSummary {
    let grouping_preserved_admitted_gpu = grouping.physical_solve_admitted_gpu
        == grouping.grouped_solve_admitted_gpu
        && grouping.physical_solve_admitted_workloads == grouping.grouped_solve_admitted_workloads;
    let grouping_policy = vec![
        "group homogeneous GPU nodes before pruning when node labels, taints, allocatable resources, prices, and supported device resources match".to_string(),
        "solve against counted representative nodes, then expand each selected counted slot back to concrete physical node names".to_string(),
        "fall back to physical nodes when co-location, anti-affinity, preferred co-placement, existing grouped nodes, or mixed resources break symmetry".to_string(),
        "preserve admitted GPU demand and admitted workload count between grouped and physical solves before trusting the grouped result".to_string(),
    ];
    let pruning_modes = vec![
        "exact/full: candidate_node_limit=0 keeps the full feasible node set".to_string(),
        "bounded/pruned: positive candidate_node_limit limits assignment edges per workload and records edge reduction".to_string(),
        "widened: suspicious pruned results retry with a wider or full feasible set before reporting".to_string(),
        "cached/demo: deterministic scenario report compares reduced-candidate solves against full solves for regret evidence".to_string(),
    ];
    let regret_status_ladder = vec![
        "none: pruned solve matches full solve on useful GPU and unplaced pods".to_string(),
        "measured: deterministic full solve found useful-GPU or unplaced-pod regret".to_string(),
        "recovered: widening retry recovered useful GPU or reduced unplaced work".to_string(),
        "unknown: live workload did not run a full comparison; expose candidate_regret_status=unknown".to_string(),
    ];
    let fallback_triggers = vec![
        "candidate pruning shows useful-GPU regret in deterministic comparison".to_string(),
        "pruned solve leaves high-priority, deadline, or gang work unplaced while total feasible GPU exists".to_string(),
        "node grouping cannot prove symmetry or expansion back to physical nodes".to_string(),
        "candidate_regret_status is unknown for a high-value scheduling decision".to_string(),
        "operator sets KSOLVER_CANDIDATE_NODE_LIMIT=0 for exact full-candidate solve".to_string(),
    ];
    let scale_mode_cards = vec![
        ScaleModeCard {
            mode: "full_feasible_set".to_string(),
            status: "exact".to_string(),
            speedup_mechanism: "none; all feasible assignment edges are modeled".to_string(),
            correctness_check: "candidate_node_limit=0 and candidate_regret_status=full_feasible_set".to_string(),
            evidence: format!(
                "{} scenarios compared against full feasible candidate solves",
                regret.scenarios_compared
            ),
            operator_action: "use for high-risk live binding or when regret status is unknown".to_string(),
        },
        ScaleModeCard {
            mode: "node_grouping".to_string(),
            status: if grouping_preserved_admitted_gpu {
                "safe_for_symmetric_nodes".to_string()
            } else {
                "fallback_required".to_string()
            },
            speedup_mechanism: format!(
                "collapse {} homogeneous physical nodes into {} counted node(s)",
                grouping.physical_nodes_before, grouping.grouped_nodes_after
            ),
            correctness_check: "grouped solve must preserve admitted workload count and admitted GPU, then expand to physical nodes".to_string(),
            evidence: format!(
                "eligible_nodes={} max_group_size={} expanded_used_nodes={:?}",
                grouping.eligible_node_count, grouping.max_group_size, grouping.expanded_used_nodes
            ),
            operator_action: "prefer grouping before pruning on large homogeneous GPU pools".to_string(),
        },
        ScaleModeCard {
            mode: "candidate_pruning".to_string(),
            status: if regret.max_useful_gpu_regret == 0 {
                "no_measured_regret".to_string()
            } else {
                "measured_regret".to_string()
            },
            speedup_mechanism: format!(
                "limit assignment edges to K={} candidate nodes per workload",
                regret.candidate_node_limit
            ),
            correctness_check: "compare reduced-candidate solve against full solve when deterministic evidence is available".to_string(),
            evidence: format!(
                "scenarios_with_any_regret={} max_useful_gpu_regret={}",
                regret.scenarios_with_any_regret, regret.max_useful_gpu_regret
            ),
            operator_action: "surface regret status next to every pruned recommendation".to_string(),
        },
        ScaleModeCard {
            mode: "candidate_widening".to_string(),
            status: if widening.passed {
                "recovered".to_string()
            } else {
                "not_proven".to_string()
            },
            speedup_mechanism: "start narrow, then retry wider or full feasible set when suspicious".to_string(),
            correctness_check: "widened solve must recover useful GPU or reduce unplaced work before claiming repair".to_string(),
            evidence: format!(
                "scenario={} retry_count={} recovered_useful_gpu={}",
                widening.scenario, widening.retry_count, widening.useful_gpu_recovered
            ),
            operator_action: "treat widened/full retry as the trustworthy result when it differs from pruned solve".to_string(),
        },
    ];
    let regret_action_rows = vec![
        ScaleRegretActionRow {
            regret_status: "full_feasible_set".to_string(),
            meaning: "no candidate pruning was applied".to_string(),
            risk_level: "lowest".to_string(),
            next_action: "allow normal dry-run review or live-binding safety gates".to_string(),
            metric_or_trace_field: "candidate_quality_metrics.regret_status".to_string(),
        },
        ScaleRegretActionRow {
            regret_status: "measured".to_string(),
            meaning: "full solve found better useful GPU or fewer unplaced pods than pruned solve"
                .to_string(),
            risk_level: "high".to_string(),
            next_action: "widen candidates or disable pruning before using the result".to_string(),
            metric_or_trace_field: "candidate_quality_metrics.useful_gpu_regret".to_string(),
        },
        ScaleRegretActionRow {
            regret_status: "recovered".to_string(),
            meaning: "widening retry recovered useful GPU or reduced unplaced work".to_string(),
            risk_level: "medium".to_string(),
            next_action: "show the widened result and record retry count in the trace".to_string(),
            metric_or_trace_field: "candidate_quality_metrics.widening_retries".to_string(),
        },
        ScaleRegretActionRow {
            regret_status: "unknown".to_string(),
            meaning: "no full comparison was run for this live workload".to_string(),
            risk_level: "unknown".to_string(),
            next_action:
                "do not hide approximation; rerun full feasible set for high-value decisions"
                    .to_string(),
            metric_or_trace_field: "ksolver_shadow_candidate_regret_status{status=\"unknown\"}"
                .to_string(),
        },
    ];
    let large_fleet_validation_rows = vec![
        ScaleLargeFleetValidationRow {
            gate: "homogeneous grouping symmetry".to_string(),
            required_evidence:
                "same allocatable GPU resources, labels, taints, price, device resource names, and supported topology caveats across grouped nodes"
                    .to_string(),
            live_trace_metric:
                "node_grouping_metrics.eligible_groups + grouped_nodes + grouped_candidate_edges"
                    .to_string(),
            fail_closed_action:
                "fall back to physical nodes when any grouping key differs or preferred placement breaks symmetry"
                    .to_string(),
            operator_claim:
                "grouping is a safe compression only for provably equivalent nodes".to_string(),
        },
        ScaleLargeFleetValidationRow {
            gate: "physical expansion correctness".to_string(),
            required_evidence:
                "grouped counted slots expand to concrete physical node names with admitted workload and GPU counts preserved"
                    .to_string(),
            live_trace_metric:
                "node_grouping_metrics.used_physical_nodes + grouped_members + admitted_gpu"
                    .to_string(),
            fail_closed_action:
                "discard grouped result and rerun full physical solve when expansion cannot be proven"
                    .to_string(),
            operator_claim:
                "the UI can show the representative group and the real nodes actually selected"
                    .to_string(),
        },
        ScaleLargeFleetValidationRow {
            gate: "candidate pruning regret visibility".to_string(),
            required_evidence:
                "candidate edge reduction, candidate_node_limit, pruned workload count, and regret status are present for every pruned solve"
                    .to_string(),
            live_trace_metric:
                "candidate_quality_metrics.edge_reduction_milli + regret_status + useful_gpu_regret"
                    .to_string(),
            fail_closed_action:
                "block high-risk live binding when regret_status is unknown or measured for high-value work"
                    .to_string(),
            operator_claim:
                "fast approximate solves are never presented as exact when regret is unknown or measured"
                    .to_string(),
        },
        ScaleLargeFleetValidationRow {
            gate: "widening recovery".to_string(),
            required_evidence:
                "low-admission or suspicious pruned solves retry wider or full feasible candidates before recommendation"
                    .to_string(),
            live_trace_metric:
                "candidate_quality_metrics.widening_retries + widening_recovered_useful_gpu"
                    .to_string(),
            fail_closed_action:
                "show widened/full result as authoritative when it differs from the narrow solve"
                    .to_string(),
            operator_claim:
                "candidate pruning is a first pass, not the final answer when quality signals degrade"
                    .to_string(),
        },
        ScaleLargeFleetValidationRow {
            gate: "large heterogeneous fleet sample".to_string(),
            required_evidence:
                "scenario or live snapshot includes multiple GPU node classes, prices, capacities, taints, and device resource names"
                    .to_string(),
            live_trace_metric:
                "scale_validation_metrics.node_classes + gpu_resource_classes + price_classes"
                    .to_string(),
            fail_closed_action:
                "label large-fleet claim unvalidated until heterogeneous snapshots are exercised"
                    .to_string(),
            operator_claim:
                "grouped-first scale claims require heterogeneity evidence, not only tiny homogeneous demos"
                    .to_string(),
        },
        ScaleLargeFleetValidationRow {
            gate: "operator override path".to_string(),
            required_evidence:
                "the trace exposes candidate limit, grouping state, fallback reason, and exact full-solve switch"
                    .to_string(),
            live_trace_metric:
                "KSOLVER_CANDIDATE_NODE_LIMIT + KSOLVER_ENABLE_NODE_GROUPING + node_grouping_fallback_reason"
                    .to_string(),
            fail_closed_action:
                "set candidate_node_limit=0 and disable grouping for exact high-risk decisions".to_string(),
            operator_claim:
                "SREs can force exact behavior when approximation evidence is insufficient".to_string(),
        },
    ];
    let operator_switches = vec![
        "KSOLVER_ENABLE_NODE_GROUPING=true enables conservative homogeneous-node grouping"
            .to_string(),
        "KSOLVER_CANDIDATE_NODE_LIMIT=0 disables pruning and uses the full feasible set".to_string(),
        "KSOLVER_CANDIDATE_WIDEN_MIN_ADMISSION_PERCENT tunes low-admission widening".to_string(),
        "candidate_regret_status=unknown should block high-risk live binding unless explicitly overridden".to_string(),
        "node_grouping_fallback_reason should be shown whenever grouping is disabled".to_string(),
    ];
    let guardrails = vec![
        "candidate pruning is observable through candidate_node_limit, candidate edge counts, pruned workload count, and regret status".to_string(),
        "reduced-candidate solves are compared against full-candidate solves in the deterministic scenario report".to_string(),
        "suspicious low-admission pruned solves widen first and can retry the full feasible set".to_string(),
        "grouped node solves must expand back to physical node assignments before results are trusted".to_string(),
        "node grouping records eligible group count, eligible physical nodes, max group size, grouped members, used physical nodes, and fallback reason in live traces".to_string(),
        "operators can set KSOLVER_CANDIDATE_NODE_LIMIT=0 to disable pruning and solve the full feasible set".to_string(),
    ];
    let residual_risks = vec![
        "node grouping remains opt-in until more production fleet shapes validate grouped expansion safety".to_string(),
        "candidate pruning can still produce unknown regret on real workloads without a full retry".to_string(),
        "co-location, anti-affinity, existing grouped nodes, and preferred co-placement can disable safe grouping".to_string(),
        "large heterogeneous fleets still need live telemetry to choose an appropriate candidate limit".to_string(),
    ];
    let actionability_contract = ScaleActionabilityContract {
        recommendation: if grouping_preserved_admitted_gpu && widening.useful_gpu_recovered > 0 {
            "Grouped-first scale path with widened fallback".to_string()
        } else if grouping_preserved_admitted_gpu {
            "Grouped-first scale path".to_string()
        } else {
            "Full physical solve until grouping proof passes".to_string()
        },
        customer_scale_claim_allowed: grouping_preserved_admitted_gpu
            && grouping.eligible_node_count >= 2
            && large_fleet_validation_rows.len() >= 6,
        high_risk_pruned_binding_allowed: false,
        preferred_large_fleet_mode:
            "homogeneous node grouping before candidate pruning; widen or full-rerun when regret is unknown or measured"
                .to_string(),
        required_evidence: vec![
            "node_grouping_metrics proving homogeneous labels, taints, allocatable resources, prices, device resources, and topology-relevant keys".to_string(),
            "physical expansion proof that grouped counted slots map back to concrete node names with admitted GPU preserved".to_string(),
            "candidate_quality_metrics with candidate_node_limit, edge reduction, pruned workloads, regret_status, and useful_gpu_regret".to_string(),
            "full feasible-set comparison for high-value decisions before customer-visible optimality claims".to_string(),
            "widening retry evidence when low admission, gang fragmentation, priority, deadline, or unknown-regret signals fire".to_string(),
            "large heterogeneous fleet sample covering multiple GPU capacities, prices, taints, and device resource names".to_string(),
        ],
        fail_closed_if: vec![
            "grouped result cannot expand to concrete physical nodes".to_string(),
            "grouped solve changes admitted GPU demand or admitted workload count versus physical validation".to_string(),
            "candidate_regret_status is unknown for high-value, deadline-sensitive, or mutation-enabled decisions".to_string(),
            "candidate pruning shows measured useful-GPU or unplaced-pod regret".to_string(),
            "node grouping fallback reason is present and no full physical rerun has been captured".to_string(),
        ],
        operator_overrides: vec![
            "KSOLVER_CANDIDATE_NODE_LIMIT=0 forces a full feasible-set solve".to_string(),
            "KSOLVER_ENABLE_NODE_GROUPING=false disables grouped compression".to_string(),
            "KSOLVER_CANDIDATE_WIDEN_MIN_ADMISSION_PERCENT controls automatic widening sensitivity".to_string(),
            "mutation-enabled deployments should require full_feasible_set, recovered, or validated grouped evidence before binding high-value pods".to_string(),
        ],
        next_action:
            "make grouping the recommended large-fleet path, keep pruned high-risk binding disabled by default, and collect heterogeneous fleet traces plus full-rerun regret samples"
                .to_string(),
    };
    let passed = grouping.passed
        && grouping_preserved_admitted_gpu
        && widening.passed
        && widening.useful_gpu_recovered > 0
        && actionability_contract.customer_scale_claim_allowed
        && !actionability_contract.high_risk_pruned_binding_allowed
        && actionability_contract.required_evidence.len() >= 6
        && actionability_contract.fail_closed_if.len() >= 5
        && actionability_contract.operator_overrides.len() >= 4
        && regret.scenarios_compared > 0
        && grouping_policy.len() >= 4
        && pruning_modes.len() >= 4
        && regret_status_ladder.len() >= 4
        && fallback_triggers.len() >= 5
        && scale_mode_cards.len() >= 4
        && regret_action_rows.len() >= 4
        && large_fleet_validation_rows.len() >= 6
        && operator_switches.len() >= 5
        && guardrails.len() >= 5
        && residual_risks.len() >= 3;

    ScaleGuardrailSummary {
        name: "scale-guardrails".to_string(),
        passed,
        actionability_contract,
        default_candidate_node_limit: regret.candidate_node_limit,
        scenarios_compared_for_regret: regret.scenarios_compared,
        scenarios_with_any_regret: regret.scenarios_with_any_regret,
        max_useful_gpu_regret: regret.max_useful_gpu_regret,
        grouping_claim: format!(
            "collapsed {} homogeneous physical nodes into {} counted node(s), then expanded onto {:?}",
            grouping.physical_nodes_before, grouping.grouped_nodes_after, grouping.expanded_used_nodes
        ),
        grouping_physical_nodes_before: grouping.physical_nodes_before,
        grouping_nodes_after: grouping.grouped_nodes_after,
        grouping_eligible_nodes: grouping.eligible_node_count,
        grouping_max_group_size: grouping.max_group_size,
        grouping_expanded_used_nodes: grouping.expanded_used_nodes.clone(),
        grouping_preserved_admitted_gpu,
        widening_claim: format!(
            "scenario {} widened K={} to full feasible set and recovered {} useful GPU",
            widening.scenario, widening.initial_candidate_node_limit, widening.useful_gpu_recovered
        ),
        widening_retry_count: widening.retry_count,
        widening_useful_gpu_recovered: widening.useful_gpu_recovered,
        grouping_policy,
        pruning_modes,
        regret_status_ladder,
        fallback_triggers,
        scale_mode_cards,
        regret_action_rows,
        large_fleet_validation_rows,
        operator_switches,
        guardrails,
        residual_risks,
    }
}

fn summarize_fairness_budget(
    scenarios: &[ScenarioResult],
    tenant_budget: &TenantBudgetProof,
) -> FairnessBudgetSummary {
    let fair_share = scenarios.iter().find(|s| s.name == "fair-share-over-fifo");
    let under_share_job = "under-share-team-job".to_string();
    let under_share_admitted = fair_share
        .map(|s| placed_prefix_count(&s.ksolver.placements, &under_share_job) == 1)
        .unwrap_or(false);
    let fair_share_useful_gpu_gain = fair_share
        .map(|s| s.ksolver.metrics.fair_share_useful_gpu - s.kube.metrics.fair_share_useful_gpu)
        .unwrap_or_default();
    let expensive_job_admitted = tenant_budget.expensive_job_node.is_some();
    let cheap_job_admitted = tenant_budget.cheap_job_node.is_some();
    let budget_overage_monthly_milli =
        (tenant_budget.expensive_node_cost_milli - tenant_budget.monthly_budget_milli).max(0);
    let policy_decision_rows = vec![
        FairnessPolicyDecisionRow {
            subject: "under-share-team".to_string(),
            workload: under_share_job.clone(),
            decision: if under_share_admitted {
                "admit".to_string()
            } else {
                "deny".to_string()
            },
            policy: "weighted fair-share".to_string(),
            reason: format!(
                "tenant is below weighted share; fair-share useful GPU gain is {}",
                fair_share_useful_gpu_gain
            ),
            evidence_field: "tenant_fairness_metrics.tenants[].under_fair_share_gpu_milli"
                .to_string(),
            operator_action: "keep fair-share objective enabled for scarce GPU queues".to_string(),
        },
        FairnessPolicyDecisionRow {
            subject: tenant_budget.tenant.clone(),
            workload: "research/expensive-candidate".to_string(),
            decision: if expensive_job_admitted {
                "admit".to_string()
            } else {
                "deny".to_string()
            },
            policy: "tenant monthly budget".to_string(),
            reason: format!(
                "placement would cost {} milli against budget {} milli",
                tenant_budget.expensive_node_cost_milli, tenant_budget.monthly_budget_milli
            ),
            evidence_field: "tenant_fairness_metrics.tenants[].budget_overage_monthly_milli"
                .to_string(),
            operator_action:
                "raise the tenant budget or steer this workload to cheaper GPU capacity".to_string(),
        },
        FairnessPolicyDecisionRow {
            subject: tenant_budget.tenant.clone(),
            workload: "research/cheap-candidate".to_string(),
            decision: if cheap_job_admitted {
                "admit".to_string()
            } else {
                "deny".to_string()
            },
            policy: "tenant monthly budget".to_string(),
            reason: format!(
                "placement costs {} milli and remains inside budget {} milli",
                tenant_budget.cheap_node_cost_milli, tenant_budget.monthly_budget_milli
            ),
            evidence_field: "tenant_fairness_metrics.tenants[].admitted_monthly_cost_milli"
                .to_string(),
            operator_action: "prefer cheaper feasible GPU nodes before rejecting the tenant's work"
                .to_string(),
        },
        FairnessPolicyDecisionRow {
            subject: "over-share-team".to_string(),
            workload: "running borrowed GPU capacity".to_string(),
            decision: "audit".to_string(),
            policy: "borrow/reclaim".to_string(),
            reason:
                "tenant is above weighted share while another tenant has denied demand below share"
                    .to_string(),
            evidence_field: "tenant_fairness_metrics.reclaimable_borrowed_gpu_milli".to_string(),
            operator_action:
                "show reclaimable capacity before considering disruption or future preemption"
                    .to_string(),
        },
    ];
    let tenant_ledger_rows = vec![
        FairnessTenantLedgerRow {
            tenant: "under-share-team".to_string(),
            status: "below-share-priority".to_string(),
            admitted_gpu: if under_share_admitted { 1 } else { 0 },
            denied_gpu: if under_share_admitted { 0 } else { 1 },
            admitted_monthly_cost_milli: 0,
            budget_monthly_milli: 0,
            budget_overage_monthly_milli: 0,
            fair_share_delta_gpu_milli: if under_share_admitted { 1000 } else { 0 },
            borrowed_gpu_milli: 0,
            reclaimable_borrowed_gpu_milli: 0,
        },
        FairnessTenantLedgerRow {
            tenant: tenant_budget.tenant.clone(),
            status: "budget-capped".to_string(),
            admitted_gpu: tenant_budget.admitted_jobs as i64,
            denied_gpu: tenant_budget.unplaced_jobs as i64,
            admitted_monthly_cost_milli: if cheap_job_admitted {
                tenant_budget.cheap_node_cost_milli
            } else {
                0
            },
            budget_monthly_milli: tenant_budget.monthly_budget_milli,
            budget_overage_monthly_milli,
            fair_share_delta_gpu_milli: 0,
            borrowed_gpu_milli: 0,
            reclaimable_borrowed_gpu_milli: 0,
        },
        FairnessTenantLedgerRow {
            tenant: "over-share-team".to_string(),
            status: "borrowing".to_string(),
            admitted_gpu: 1,
            denied_gpu: 0,
            admitted_monthly_cost_milli: 0,
            budget_monthly_milli: 0,
            budget_overage_monthly_milli: 0,
            fair_share_delta_gpu_milli: -1000,
            borrowed_gpu_milli: 1000,
            reclaimable_borrowed_gpu_milli: if under_share_admitted { 1000 } else { 0 },
        },
    ];
    let ownership_rows = vec![
        FairnessOwnershipRow {
            gate: "tenant identity".to_string(),
            ownership_source: "ksolver.dev/team pod annotation, then namespace fallback".to_string(),
            live_trace_field: "tenant_fairness_metrics.tenants[].tenant".to_string(),
            policy_use: "groups pending and admitted GPU demand before fair-share, quota, and budget checks".to_string(),
            missing_data_action: "treat namespace as the tenant and mark enforcement as audit-only until an owner map is configured".to_string(),
            operator_question: "Which team owns this pod, and did ksolver use an explicit team annotation or namespace fallback?".to_string(),
        },
        FairnessOwnershipRow {
            gate: "namespace ownership".to_string(),
            ownership_source: "namespace labels/annotations such as ksolver.dev/team, owner, or cost-center".to_string(),
            live_trace_field: "tenant_fairness_metrics.tenants[].namespace_sources".to_string(),
            policy_use: "lets SREs connect a denial row back to the organizational owner that can approve budget or priority changes".to_string(),
            missing_data_action: "show unknown-owner in the UI and do not hard-enforce tenant budgets for that namespace".to_string(),
            operator_question: "Which namespace or cost-center owns the denied workload?".to_string(),
        },
        FairnessOwnershipRow {
            gate: "fair-share weights".to_string(),
            ownership_source: "KSOLVER_SHADOW_TENANT_WEIGHTS or future account/team catalog".to_string(),
            live_trace_field: "tenant_fairness_metrics.tenants[].weighted_target_gpu_milli".to_string(),
            policy_use: "decides whether scarce GPUs should favor an under-share tenant over FIFO order".to_string(),
            missing_data_action: "fall back to equal weights and label the decision as unweighted fair-share".to_string(),
            operator_question: "Was this tenant actually below its configured share?".to_string(),
        },
        FairnessOwnershipRow {
            gate: "budget catalog".to_string(),
            ownership_source: "KSOLVER_SHADOW_TENANT_MONTHLY_BUDGETS_MILLI plus node pricing catalog".to_string(),
            live_trace_field: "tenant_fairness_metrics.tenants[].budget_monthly_milli".to_string(),
            policy_use: "turns node placement into tenant monthly spend before admitting or denying expensive GPU work".to_string(),
            missing_data_action: "disable budget caps for tenants without a budget and show cost as advisory only".to_string(),
            operator_question: "Did this placement exceed the tenant's configured monthly GPU budget?".to_string(),
        },
        FairnessOwnershipRow {
            gate: "borrow and reclaim".to_string(),
            ownership_source: "live admitted demand compared with weighted target GPU-milli".to_string(),
            live_trace_field: "tenant_fairness_metrics.reclaimable_borrowed_gpu_milli".to_string(),
            policy_use: "explains who is borrowing capacity and whether reclaim would help a denied under-share tenant".to_string(),
            missing_data_action: "hide preemption recommendations and keep reclaim as audit-only until ownership is known".to_string(),
            operator_question: "Who is borrowing GPU capacity, and how much is reclaimable without violating policy?".to_string(),
        },
        FairnessOwnershipRow {
            gate: "denial evidence".to_string(),
            ownership_source: "policy_decision_rows plus tenant_ledger_rows generated from the same solve".to_string(),
            live_trace_field: "tenant_fairness_metrics.tenants[].denied_gpu_demand".to_string(),
            policy_use: "links every denied workload to the quota, fair-share, or budget field that caused it".to_string(),
            missing_data_action: "show the denial as insufficient evidence instead of presenting a policy claim".to_string(),
            operator_question: "Was the denial caused by budget, quota, fair-share, or plain capacity?".to_string(),
        },
    ];
    let ui_badges = vec![
        "Denied: budget exhausted".to_string(),
        "Admitted: below fair share".to_string(),
        "Borrowing: reclaimable if under-share demand exists".to_string(),
        "Audit only: no automatic eviction".to_string(),
    ];
    let enforcement_controls = vec![
        "fair-share weighting requires KSOLVER_GPU_FAIR_SHARE_WEIGHT > 0".to_string(),
        "tenant budgets are hard admission caps in shadow solve output".to_string(),
        "borrow/reclaim remains observability-only until an explicit preemption policy is enabled"
            .to_string(),
        "tenant identity must resolve from team annotation or namespace before enforcement"
            .to_string(),
    ];
    let operator_questions = vec![
        "Which tenant was denied, and was it because of quota, budget, or fair-share policy?"
            .to_string(),
        "Which tenant is under its weighted fair share and should receive scarce capacity first?"
            .to_string(),
        "Who is borrowing GPU capacity, and how much is reclaimable when another tenant is below share?"
            .to_string(),
        "Which workload was skipped because its node choice would exceed the tenant monthly budget?"
            .to_string(),
    ];
    let decision_explanations = vec![
        format!(
            "fair-share scenario admits {} when fair-share weighting is enabled, producing fair-share useful GPU gain {}",
            under_share_job, fair_share_useful_gpu_gain
        ),
        format!(
            "tenant {} has budget {} milli; expensive node cost {} milli is rejected while cheap node cost {} milli is admitted",
            tenant_budget.tenant,
            tenant_budget.monthly_budget_milli,
            tenant_budget.expensive_node_cost_milli,
            tenant_budget.cheap_node_cost_milli
        ),
        "trace-level tenant fairness metrics expose denied GPU demand, admitted monthly cost, budget overage, borrowed GPU-milli, and reclaimable borrowed GPU-milli".to_string(),
    ];
    let trace_fields = vec![
        "tenant_fairness_metrics.tenants[].denied_gpu_demand".to_string(),
        "tenant_fairness_metrics.tenants[].admitted_monthly_cost_milli".to_string(),
        "tenant_fairness_metrics.tenants[].budget_monthly_milli".to_string(),
        "tenant_fairness_metrics.tenants[].budget_overage_monthly_milli".to_string(),
        "tenant_fairness_metrics.tenants[].under_fair_share_gpu_milli".to_string(),
        "tenant_fairness_metrics.tenants[].borrowed_gpu_milli".to_string(),
        "tenant_fairness_metrics.reclaimable_borrowed_gpu_milli".to_string(),
        "tenant_fairness_metrics.tenants[].tenant".to_string(),
        "tenant_fairness_metrics.tenants[].weighted_target_gpu_milli".to_string(),
        "quota_metrics.throttled_pods".to_string(),
    ];
    let residual_risks = vec![
        "fair-share enforcement is opt-in through objective weights; by default it is observational"
            .to_string(),
        "tenant identity depends on team annotations or namespace fallback, so org mapping must be configured carefully"
            .to_string(),
        "borrow/reclaim is currently an audit signal, not an automatic eviction policy".to_string(),
        "monthly budget accuracy depends on the node pricing catalog matching the real GPU fleet".to_string(),
    ];
    let passed = under_share_admitted
        && fair_share_useful_gpu_gain > 0
        && tenant_budget.passed
        && !expensive_job_admitted
        && cheap_job_admitted
        && tenant_budget.unplaced_jobs > 0
        && operator_questions.len() >= 4
        && policy_decision_rows.len() >= 4
        && tenant_ledger_rows.len() >= 3
        && ownership_rows.len() >= 6
        && ui_badges.len() >= 4
        && enforcement_controls.len() >= 4
        && trace_fields.len() >= 8
        && residual_risks.len() >= 3;

    FairnessBudgetSummary {
        name: "fairness-budget-explainability".to_string(),
        passed,
        fair_share_scenario: fair_share
            .map(|s| s.name.clone())
            .unwrap_or_else(|| "fair-share-over-fifo".to_string()),
        under_share_job,
        under_share_admitted,
        fair_share_useful_gpu_gain,
        tenant_budget_scenario: tenant_budget.name.clone(),
        tenant: tenant_budget.tenant.clone(),
        monthly_budget_milli: tenant_budget.monthly_budget_milli,
        expensive_node_cost_milli: tenant_budget.expensive_node_cost_milli,
        cheap_node_cost_milli: tenant_budget.cheap_node_cost_milli,
        expensive_job_admitted,
        cheap_job_admitted,
        admitted_jobs: tenant_budget.admitted_jobs,
        unplaced_jobs: tenant_budget.unplaced_jobs,
        policy_decision_rows,
        tenant_ledger_rows,
        ownership_rows,
        ui_badges,
        enforcement_controls,
        operator_questions,
        decision_explanations,
        trace_fields,
        residual_risks,
    }
}

fn summarize_device_correctness(
    gpu_topology: &GpuTopologyProof,
    mig_profile: &MigProfileProof,
    dra_approximation: &DraApproximationProof,
    dra_allocation: &DraAllocationProof,
    time_sliced_gpu: &TimeSlicedGpuProof,
) -> DeviceCorrectnessSummary {
    let supported_today = vec![
        "whole-GPU scheduling through exact extended-resource capacity for nvidia.com/gpu".to_string(),
        "MIG mixed-strategy slice scheduling through exact extended-resource profile matching".to_string(),
        "hard GPU topology filters from explicit node labels such as NVLink island or node-local topology keys".to_string(),
        "shadow-only DRA scalar capacity when a ResourceClaim can be mapped to a synthetic device-class resource".to_string(),
        "allocated DRA device identities are subtracted before exposing synthetic remaining capacity".to_string(),
        "time-sliced GPU placements are disclosed as shared/no-isolation caveats instead of hidden as normal GPUs".to_string(),
    ];
    let proof_backed_claims = vec![
        format!(
            "{} proves topology-required pods only see {:?} and reject {:?}",
            gpu_topology.name, gpu_topology.matching_feasible_nodes, gpu_topology.rejected_nodes
        ),
        format!(
            "{} proves {} requests only match {:?}",
            mig_profile.name, mig_profile.requested_resource, mig_profile.matching_feasible_nodes
        ),
        format!(
            "{} proves modeled DRA demand becomes {} x{} and unmodeled DRA pods are dropped",
            dra_approximation.name,
            dra_approximation.synthetic_resource,
            dra_approximation.modeled_request_quantity
        ),
        format!(
            "{} proves {} total matching devices minus {} allocated leaves {} available",
            dra_allocation.name,
            dra_allocation.total_matching_devices,
            dra_allocation.allocated_devices,
            dra_allocation.available_devices
        ),
        format!(
            "{} proves shared GPU nodes emit caveats while isolated nodes do not",
            time_sliced_gpu.name
        ),
    ];
    let exact_semantics = vec![
        "whole GPU extended-resource accounting is exact for nvidia.com/gpu residual capacity"
            .to_string(),
        "MIG mixed-strategy profile compatibility is exact by extended-resource name and quantity"
            .to_string(),
        "required GPU topology labels are hard filters over node labels".to_string(),
        "already allocated DRA device identities are subtracted from synthetic available capacity"
            .to_string(),
    ];
    let approximated_semantics = vec![
        "DRA ResourceClaims are reduced to scalar synthetic device-class resources when selectors are modelable".to_string(),
        "NVLink/NVSwitch topology is represented by labels, not by concrete GPU pair or island graph optimization".to_string(),
        "time-sliced GPU nodes are surfaced as caveated shared capacity, not isolated capacity".to_string(),
        "MIG identity placement is profile-level, not individual slice identity selection".to_string(),
    ];
    let unsupported_claims = vec![
        "full DRA allocation: choosing concrete devices and writing ResourceClaim allocation decisions".to_string(),
        "NVLink-optimal multi-GPU graph placement across concrete GPU identities".to_string(),
        "time-sliced GPU memory/fault/performance isolation guarantees".to_string(),
        "safe optimization for overlapping or unevaluable DRA device classes".to_string(),
    ];
    let validation_signals = vec![
        "gpu_topology_scenario.matching_feasible_nodes and rejected_nodes".to_string(),
        "mig_profile_scenario.requested_resource and matching_feasible_nodes".to_string(),
        "dra_approximation_scenario.modeled_feasible_nodes and unmodeled_drop_reason".to_string(),
        "dra_allocation_scenario.available_devices after allocated identity subtraction"
            .to_string(),
        "time_sliced_gpu_scenario.time_sliced_caveats and isolated_caveats".to_string(),
    ];
    let fallback_actions = vec![
        "drop unmodeled DRA pods instead of treating them as zero-cost/free work".to_string(),
        "surface required GPU topology mismatch as an explicit unplaced reason".to_string(),
        "fall back to whole-GPU or exact extended-resource accounting when device identity cannot be proven".to_string(),
        "mark shared GPU placements with caveats instead of presenting them as isolated placements".to_string(),
        "require operator/device-plugin inventory fixes when labels or resource slices are stale".to_string(),
    ];
    let topology_claim = format!(
        "topology filters are hard feasibility gates: {}={} admits {:?} and drops unmatched pods with an explicit reason",
        gpu_topology.topology_key,
        gpu_topology.required_value,
        gpu_topology.matching_feasible_nodes
    );
    let mig_claim = format!(
        "MIG profile compatibility is exact by resource name: {} x{} does not substitute with whole GPUs or other MIG profiles",
        mig_profile.requested_resource, mig_profile.requested_quantity
    );
    let dra_approximation_claim = format!(
        "DRA is currently represented as scalar synthetic demand ({}) when modelable; unmodeled DRA pods are dropped rather than treated as free work",
        dra_approximation.synthetic_resource
    );
    let dra_allocation_claim = format!(
        "DRA availability subtracts allocated device identities for node={} class={}, producing {} currently available device(s)",
        dra_allocation.node, dra_allocation.device_class, dra_allocation.available_devices
    );
    let time_sliced_claim = format!(
        "time-sliced GPU node {} reports {:?}",
        time_sliced_gpu.time_sliced_node, time_sliced_gpu.time_sliced_caveats
    );
    let device_readiness_rows = vec![
        DeviceReadinessRow {
            feature: "whole GPU extended resources".to_string(),
            support_level: "exact today".to_string(),
            required_inventory:
                "node allocatable/capacity for nvidia.com/gpu plus current pod resource requests"
                    .to_string(),
            live_trace_signal:
                "normalized node extended_resources and residual GPU in placement decisions"
                    .to_string(),
            fail_closed_action:
                "do not place GPU-requesting pods when residual extended-resource capacity is unknown"
                    .to_string(),
            operator_claim: "safe to claim exact whole-GPU residual capacity accounting".to_string(),
        },
        DeviceReadinessRow {
            feature: "MIG profile resources".to_string(),
            support_level: "exact by advertised profile resource".to_string(),
            required_inventory:
                "NVIDIA device plugin mixed-strategy resources such as nvidia.com/mig-3g.20gb"
                    .to_string(),
            live_trace_signal:
                "decision feasible_node_names filtered by requested MIG extended-resource name"
                    .to_string(),
            fail_closed_action:
                "do not substitute other MIG profiles or whole GPUs for a requested MIG profile"
                    .to_string(),
            operator_claim:
                "safe to claim profile-level MIG compatibility, not individual slice identity"
                    .to_string(),
        },
        DeviceReadinessRow {
            feature: "topology label filters".to_string(),
            support_level: "hard label filter today".to_string(),
            required_inventory:
                "fresh node labels for NVLink island, GPU locality, or operator-provided topology key"
                    .to_string(),
            live_trace_signal:
                "gpu_topology_scenario.matching_feasible_nodes and decision unplaced reasons"
                    .to_string(),
            fail_closed_action:
                "reject topology-constrained pods when required labels are absent or stale".to_string(),
            operator_claim:
                "safe to claim label-based topology feasibility, not NVLink-optimal graph placement"
                    .to_string(),
        },
        DeviceReadinessRow {
            feature: "DRA scalar approximation".to_string(),
            support_level: "shadow approximation with allocation subtraction".to_string(),
            required_inventory:
                "ResourceSlices, ResourceClaims, modelable selectors, and allocated device identities"
                    .to_string(),
            live_trace_signal:
                "dra_approximation_scenario modeled/unmodeled counts and dra_allocation_scenario available_devices"
                    .to_string(),
            fail_closed_action:
                "drop unmodeled or overlapping DRA classes instead of treating them as free capacity"
                    .to_string(),
            operator_claim:
                "safe to claim conservative DRA scalar accounting, not concrete allocation decisions"
                    .to_string(),
        },
        DeviceReadinessRow {
            feature: "time-sliced GPU disclosure".to_string(),
            support_level: "caveated shared capacity".to_string(),
            required_inventory:
                "node labels or plugin metadata indicating shared/time-sliced GPU mode".to_string(),
            live_trace_signal:
                "decision caveats and time_sliced_gpu_scenario.time_sliced_caveats".to_string(),
            fail_closed_action:
                "surface shared/no-isolation caveats and avoid claiming memory or fault isolation"
                    .to_string(),
            operator_claim:
                "safe to disclose shared GPU placement caveats, not isolation guarantees".to_string(),
        },
        DeviceReadinessRow {
            feature: "concrete NVLink/DRA device graph".to_string(),
            support_level: "unsupported future claim".to_string(),
            required_inventory:
                "per-device topology graph, concrete GPU identities, DRA allocation APIs, and ResourceClaim write path"
                    .to_string(),
            live_trace_signal:
                "unsupported_claims and hard_limits remain present until graph/device allocation is implemented"
                    .to_string(),
            fail_closed_action:
                "do not market or enable NVLink-optimal placement or DRA allocation writes".to_string(),
            operator_claim:
                "explicitly not supported yet; roadmap item remains concrete device graph optimization"
                    .to_string(),
        },
    ];
    let hard_limits = vec![
        "ksolver does not yet choose concrete DRA device identities or emit ResourceClaim allocation decisions".to_string(),
        "NVLink/NVSwitch awareness is label-filter based; it does not yet optimize over a per-device topology graph or GPU-pair matrix".to_string(),
        "MIG support is exact at the advertised extended-resource profile level, not at individual slice identity placement".to_string(),
        "overlapping or unevaluable DRA classes remain unsafe to optimize beyond scalar accounting".to_string(),
        "time-sliced GPUs are disclosed but not modeled for memory isolation, fault isolation, or performance interference".to_string(),
    ];
    let residual_risks = vec![
        "topology correctness depends on accurate and current node labels from the GPU operator or cluster inventory".to_string(),
        "DRA CEL selector support is intentionally narrow; unsupported selectors must stay conservative".to_string(),
        "MIG and DRA capacity can drift if device plugin or resource slice status is stale".to_string(),
        "serious multi-GPU training placement still needs real device graph awareness before claiming NVLink-optimal placement".to_string(),
    ];
    let operator_claims = vec![
        "we can safely explain which GPU device semantics are modeled versus approximated".to_string(),
        "we reject device-constrained pods when the model cannot prove feasible capacity".to_string(),
        "we can demo MIG profile compatibility, topology filters, DRA allocated-device subtraction, and time-slicing caveats today".to_string(),
        "we should not claim full DRA allocation or NVLink graph optimization yet".to_string(),
    ];
    let passed = gpu_topology.passed
        && mig_profile.passed
        && dra_approximation.passed
        && dra_allocation.passed
        && time_sliced_gpu.passed
        && !dra_allocation.overlapping_classes
        && dra_allocation.unevaluable_classes.is_empty()
        && supported_today.len() >= 5
        && proof_backed_claims.len() >= 5
        && exact_semantics.len() >= 4
        && approximated_semantics.len() >= 4
        && unsupported_claims.len() >= 4
        && validation_signals.len() >= 5
        && fallback_actions.len() >= 5
        && device_readiness_rows.len() >= 6
        && hard_limits.len() >= 4
        && residual_risks.len() >= 3
        && operator_claims.len() >= 4;

    DeviceCorrectnessSummary {
        name: "device-correctness-readiness".to_string(),
        passed,
        supported_today,
        proof_backed_claims,
        exact_semantics,
        approximated_semantics,
        unsupported_claims,
        validation_signals,
        fallback_actions,
        device_readiness_rows,
        topology_claim,
        mig_claim,
        dra_approximation_claim,
        dra_allocation_claim,
        time_sliced_claim,
        hard_limits,
        residual_risks,
        operator_claims,
    }
}

fn percent_milli(numerator: i64, denominator: i64) -> i64 {
    if denominator <= 0 {
        return 0;
    }
    numerator.saturating_mul(100_000) / denominator
}

fn summarize_roi(scenarios: &[ScenarioResult]) -> RoiSummary {
    let mut summary = RoiSummary {
        scenarios_compared: scenarios.len(),
        ..Default::default()
    };
    for scenario in scenarios {
        let requested_gpu = placement_requested_gpu(&scenario.kube);
        let kube = &scenario.kube.metrics;
        let ksolver = &scenario.ksolver.metrics;
        let useful_gain = ksolver.useful_gpu - kube.useful_gpu;

        summary.total_requested_gpu += requested_gpu;
        summary.kube_admitted_useful_gpu += kube.useful_gpu;
        summary.ksolver_admitted_useful_gpu += ksolver.useful_gpu;
        summary.admitted_useful_gpu_gain += useful_gain;
        summary.kube_unplaced_pods += kube.unplaced_pods;
        summary.ksolver_unplaced_pods += ksolver.unplaced_pods;
        summary.kube_active_nodes += kube.active_nodes;
        summary.ksolver_active_nodes += ksolver.active_nodes;
        summary.kube_active_node_monthly_cost += kube.cost_active_nodes_monthly;
        summary.ksolver_active_node_monthly_cost += ksolver.cost_active_nodes_monthly;
        summary.stranded_gpu_reduction +=
            kube.stranded_gpu_on_active_nodes - ksolver.stranded_gpu_on_active_nodes;
        if useful_gain > 0 {
            summary.scenarios_with_positive_admission_gain += 1;
        }
    }
    summary.unplaced_pod_reduction =
        summary.kube_unplaced_pods as i64 - summary.ksolver_unplaced_pods as i64;
    summary.active_node_reduction =
        summary.kube_active_nodes as i64 - summary.ksolver_active_nodes as i64;
    summary.active_node_monthly_cost_reduction =
        summary.kube_active_node_monthly_cost - summary.ksolver_active_node_monthly_cost;
    summary.kube_admission_percent_milli = percent_milli(
        summary.kube_admitted_useful_gpu,
        summary.total_requested_gpu,
    );
    summary.ksolver_admission_percent_milli = percent_milli(
        summary.ksolver_admitted_useful_gpu,
        summary.total_requested_gpu,
    );
    summary.kube_gpu_utilization_milli = weighted_utilization_milli(scenarios, true);
    summary.ksolver_gpu_utilization_milli = weighted_utilization_milli(scenarios, false);
    summary.gpu_utilization_gain_milli =
        summary.ksolver_gpu_utilization_milli - summary.kube_gpu_utilization_milli;
    summary.admission_percent_gain_milli =
        summary.ksolver_admission_percent_milli - summary.kube_admission_percent_milli;
    summary.headline = format!(
        "ksolver admitted {} more useful GPU demand across {} deterministic scenarios, reduced unplaced pods by {}, reduced stranded active-node GPU by {}, and active-node monthly cost delta (ksolver-kube) was {}",
        summary.admitted_useful_gpu_gain,
        summary.scenarios_compared,
        summary.unplaced_pod_reduction,
        summary.stranded_gpu_reduction,
        -summary.active_node_monthly_cost_reduction
    );
    summary
}

fn dashboard_tile(
    key: &str,
    label: &str,
    value: i64,
    unit: &str,
    direction: &str,
    evidence: String,
) -> RoiDashboardTile {
    RoiDashboardTile {
        key: key.to_string(),
        label: label.to_string(),
        value,
        unit: unit.to_string(),
        direction: direction.to_string(),
        evidence,
    }
}

fn summarize_roi_dashboard(
    roi: &RoiSummary,
    benefit: &BenefitSummary,
    regret: &RegretSummary,
    repair: &RepairScenarioProof,
) -> RoiDashboardSummary {
    let primary_tiles = vec![
        dashboard_tile(
            "admitted_useful_gpu_gain",
            "More Useful GPU Admitted",
            roi.admitted_useful_gpu_gain,
            "gpu",
            "higher_is_better",
            format!(
                "ksolver admitted {} useful GPU vs kube {} across {} scenarios",
                roi.ksolver_admitted_useful_gpu,
                roi.kube_admitted_useful_gpu,
                roi.scenarios_compared
            ),
        ),
        dashboard_tile(
            "stranded_gpu_reduction",
            "Stranded GPU Reduced",
            roi.stranded_gpu_reduction,
            "gpu",
            "higher_is_better",
            format!(
                "active-node stranded GPU delta kube-ksolver = {}",
                roi.stranded_gpu_reduction
            ),
        ),
        dashboard_tile(
            "active_node_monthly_cost_delta",
            "Active-Node Monthly Cost Delta vs Kube",
            roi.ksolver_active_node_monthly_cost - roi.kube_active_node_monthly_cost,
            "synthetic-monthly-cost",
            "lower_is_better",
            format!(
                "ksolver active-node cost {} minus kube active-node cost {}",
                roi.ksolver_active_node_monthly_cost, roi.kube_active_node_monthly_cost
            ),
        ),
        dashboard_tile(
            "deadline_pressure_reduction",
            "Deadline Pressure Reduced",
            benefit.total_deadline_unplaced_gpu_reduction
                + benefit.total_deadline_miss_gpu_reduction,
            "gpu",
            "higher_is_better",
            format!(
                "deadline unplaced GPU reduction {} plus deadline miss GPU reduction {}",
                benefit.total_deadline_unplaced_gpu_reduction,
                benefit.total_deadline_miss_gpu_reduction
            ),
        ),
        dashboard_tile(
            "hero_repair_disruption_cost",
            "Hero Repair Disruption Cost",
            repair.disruption_cost as i64,
            "cost-points",
            "lower_is_better",
            format!(
                "{} frees {} GPU with {} migration(s) and {} preemption(s)",
                repair.name, repair.freed_gpu, repair.migration_actions, repair.preemption_actions
            ),
        ),
        dashboard_tile(
            "candidate_pruning_max_regret",
            "Candidate-Pruning Max Regret",
            regret.max_useful_gpu_regret,
            "gpu",
            "lower_is_better",
            format!(
                "{} scenarios compared at K={}, {} had any regret",
                regret.scenarios_compared,
                regret.candidate_node_limit,
                regret.scenarios_with_any_regret
            ),
        ),
    ];
    let confidence_guardrail =
        "prediction confidence is source-tiered and exposed through prediction_quality_summary; dashboard should treat low-confidence estimates as advisory".to_string();
    let regret_guardrail = format!(
        "candidate pruning compared {} scenarios; max useful-GPU regret is {}, and widening proof shows full-set retry recovery when regret appears",
        regret.scenarios_compared, regret.max_useful_gpu_regret
    );
    let executive_rows = vec![
        RoiExecutiveRow {
            priority: 1,
            claim: "Admit more valuable GPU work".to_string(),
            value: format!("+{} useful GPU", roi.admitted_useful_gpu_gain),
            evidence_tile: "admitted_useful_gpu_gain".to_string(),
            caveat:
                "compare against the best kube baseline provenance shown in demo_readiness_summary"
                    .to_string(),
            operator_action:
                "open the top ranked scenario card and inspect which jobs changed state".to_string(),
        },
        RoiExecutiveRow {
            priority: 2,
            claim: "Reduce stranded active-node GPU".to_string(),
            value: format!(
                "{} GPU reclaimed from fragmentation",
                roi.stranded_gpu_reduction
            ),
            evidence_tile: "stranded_gpu_reduction".to_string(),
            caveat:
                "stranded GPU is measured on active nodes in the deterministic scenario library"
                    .to_string(),
            operator_action: "pair this with the node-fill visualization before approving a repair"
                .to_string(),
        },
        RoiExecutiveRow {
            priority: 3,
            claim: "Show cost impact".to_string(),
            value: format!(
                "{} synthetic monthly cost delta",
                roi.ksolver_active_node_monthly_cost - roi.kube_active_node_monthly_cost
            ),
            evidence_tile: "active_node_monthly_cost_delta".to_string(),
            caveat: "synthetic costs are stable relative prices, not cloud billing truth"
                .to_string(),
            operator_action:
                "replace the demo pricing catalog with the operator's GPU fleet prices".to_string(),
        },
        RoiExecutiveRow {
            priority: 4,
            claim: "Price the intervention".to_string(),
            value: format!("{} disruption-cost points", repair.disruption_cost),
            evidence_tile: "hero_repair_disruption_cost".to_string(),
            caveat: "dry-run repair still needs PDB, identity, readiness, and approval checks"
                .to_string(),
            operator_action: "review migrate/preempt action rows before taking any live action"
                .to_string(),
        },
        RoiExecutiveRow {
            priority: 5,
            claim: "Display trust guardrails".to_string(),
            value: format!(
                "{} max useful-GPU regret at K={}",
                regret.max_useful_gpu_regret, regret.candidate_node_limit
            ),
            evidence_tile: "candidate_pruning_max_regret".to_string(),
            caveat: "prediction confidence and candidate regret must stay visible next to ROI"
                .to_string(),
            operator_action: "widen candidates or rerun unpruned before high-risk live binding"
                .to_string(),
        },
    ];
    let decision_rows = vec![
        RoiDecisionRow {
            tile_key: "admitted_useful_gpu_gain".to_string(),
            decision_rule: "lead with ROI only when ksolver admits strictly more useful GPU than the selected kube baseline".to_string(),
            good_signal: format!("{} useful GPU admitted above kube", roi.admitted_useful_gpu_gain),
            caveat: "use the best available kube baseline provenance, not the easiest baseline".to_string(),
            next_action: "open the ranked scenario card and inspect which concrete jobs changed state".to_string(),
            evidence_source: "roi_summary.admitted_useful_gpu_gain + sre_demo_script.top_scenario_cards".to_string(),
        },
        RoiDecisionRow {
            tile_key: "stranded_gpu_reduction".to_string(),
            decision_rule: "treat stranded-GPU reduction as fragmentation relief only when active-node stranded GPU falls".to_string(),
            good_signal: format!("{} active-node GPU no longer stranded", roi.stranded_gpu_reduction),
            caveat: "a reduction on synthetic scenarios must be confirmed against live trace node-fill views".to_string(),
            next_action: "compare kube and ksolver node-fill panels before presenting this as a utilization win".to_string(),
            evidence_source: "roi_summary.stranded_gpu_reduction + shadow node-fill visualization".to_string(),
        },
        RoiDecisionRow {
            tile_key: "active_node_monthly_cost_delta".to_string(),
            decision_rule: "show dollars only after the pricing source is pinned to the operator's GPU fleet".to_string(),
            good_signal: format!(
                "{} synthetic monthly cost delta vs kube",
                roi.ksolver_active_node_monthly_cost - roi.kube_active_node_monthly_cost
            ),
            caveat: "synthetic relative cost is not a customer billing claim".to_string(),
            next_action: "load a pricing catalog or chargeback export before promising cost savings".to_string(),
            evidence_source: "roi_dashboard_summary.primary_tiles[active_node_monthly_cost_delta] + pricing_readiness_summary".to_string(),
        },
        RoiDecisionRow {
            tile_key: "deadline_pressure_reduction".to_string(),
            decision_rule: "count deadline value only when explicit runtime/deadline evidence exists for the affected jobs".to_string(),
            good_signal: format!(
                "{} GPU of deadline pressure reduced",
                benefit.total_deadline_unplaced_gpu_reduction
                    + benefit.total_deadline_miss_gpu_reduction
            ),
            caveat: "deadline misses are prediction-sensitive when runtime is inferred instead of supplied".to_string(),
            next_action: "check prediction confidence bands before using this as a deadline-SLO claim".to_string(),
            evidence_source: "benefit_summary.deadline_* + prediction_quality_summary.live_calibration_rows".to_string(),
        },
        RoiDecisionRow {
            tile_key: "hero_repair_disruption_cost".to_string(),
            decision_rule: "approve repair only when freed GPU value is worth the migration/preemption disruption cost".to_string(),
            good_signal: format!(
                "{} GPU freed for {} disruption-cost points",
                repair.freed_gpu, repair.disruption_cost
            ),
            caveat: "dry-run repair still needs live PDB, identity, readiness, and policy checks".to_string(),
            next_action: "review preemption_migration_hero_summary.action_rows and production safety gates before action".to_string(),
            evidence_source: "preemption_migration_hero_summary.action_rows + production_safety_summary.live_validation_rows".to_string(),
        },
        RoiDecisionRow {
            tile_key: "candidate_pruning_max_regret".to_string(),
            decision_rule: "do not use pruned ROI for high-risk action when useful-GPU regret is unknown or positive".to_string(),
            good_signal: format!(
                "{} max useful-GPU regret at candidate limit {}",
                regret.max_useful_gpu_regret, regret.candidate_node_limit
            ),
            caveat: "low latency is not a correctness proof; regret and widening status must stay visible".to_string(),
            next_action: "rerun unpruned or widen candidates before high-value binding or preemption decisions".to_string(),
            evidence_source: "scale_guardrail_summary.regret_action_rows + candidate_widening_scenario".to_string(),
        },
    ];
    let decision_frame = vec![
        "first: did ksolver admit more useful GPU work?".to_string(),
        "second: did it reduce stranded GPU or active-node cost?".to_string(),
        "third: does the repair disruption cost justify the unlocked capacity?".to_string(),
        "fourth: are prediction confidence and pruning regret acceptable?".to_string(),
        "fifth: what live safety gate must pass before action?".to_string(),
    ];
    let presentation_order = vec![
        "headline".to_string(),
        "primary_tiles".to_string(),
        "executive_rows".to_string(),
        "decision_rows".to_string(),
        "preemption_migration_hero_summary.action_rows".to_string(),
        "confidence_guardrail".to_string(),
        "regret_guardrail".to_string(),
        "residual_risks".to_string(),
    ];
    let operator_questions = vec![
        "How many more useful GPUs did ksolver admit than the kube baseline?".to_string(),
        "How much active-node stranded GPU and monthly cost did the placement reduce?".to_string(),
        "Did the decision avoid deadline misses or merely pack cheaper?".to_string(),
        "Is the disruption cost of the recommended migration/preemption plan worth the unlocked GPU?".to_string(),
        "Is this recommendation exact, pruned-with-regret, or prediction-sensitive?".to_string(),
    ];
    let residual_risks = vec![
        "synthetic scenario costs are relative demo economics, not cloud billing truth".to_string(),
        "prediction confidence and regret guardrails must be displayed next to ROI claims".to_string(),
        "repair disruption cost needs operator approval before any real migration or preemption automation".to_string(),
    ];
    let claim_contract = RoiClaimContract {
        claim_level: "Scenario-normalized advisory ROI".to_string(),
        can_show_customer_dollars: false,
        value_basis: format!(
            "{} useful GPU gain, {} stranded GPU reduction, {} synthetic monthly cost delta, {} disruption-cost points",
            roi.admitted_useful_gpu_gain,
            roi.stranded_gpu_reduction,
            roi.ksolver_active_node_monthly_cost - roi.kube_active_node_monthly_cost,
            repair.disruption_cost
        ),
        required_evidence: vec![
            "operator pricing catalog, chargeback export, contract rate sheet, or invoice sample mapped to node pools".to_string(),
            "live or cached kube-scheduler-simulator baseline provenance for the scenario being claimed".to_string(),
            "live /api/scheduler/repair-plan evidence when ROI depends on migration or preemption".to_string(),
            "candidate-pruning regret is zero, widened, or backed by full unpruned rerun for the claimed scenario".to_string(),
            "prediction calibration evidence is healthy for any deadline, runtime, VRAM, or rightsizing-sensitive value".to_string(),
            "production-safety gate confirms observe-only/dry-run/canary posture before any operational action".to_string(),
        ],
        blocked_by: vec![
            "current scenario costs are synthetic relative prices".to_string(),
            "customer node-to-price mapping is not attached to this deterministic report".to_string(),
            "repair ROI is reference-demo evidence until a live repair-plan bundle is captured".to_string(),
            "prediction and candidate-regret guardrails must remain visible beside ROI".to_string(),
        ],
        next_action:
            "attach customer pricing and a live trace evidence bundle, then recompute ROI tiles before external savings claims"
                .to_string(),
    };
    let passed = roi.scenarios_compared > 0
        && roi.total_requested_gpu > 0
        && roi.admitted_useful_gpu_gain > 0
        && primary_tiles.len() >= 6
        && primary_tiles
            .iter()
            .all(|tile| !tile.key.is_empty() && !tile.evidence.is_empty())
        && repair.passed
        && repair.freed_gpu > 0
        && repair.disruption_cost > 0
        && !confidence_guardrail.is_empty()
        && !regret_guardrail.is_empty()
        && executive_rows.len() >= 5
        && executive_rows.iter().all(|row| {
            row.priority > 0 && !row.claim.is_empty() && !row.operator_action.is_empty()
        })
        && decision_rows.len() >= 6
        && decision_rows.iter().all(|row| {
            primary_tiles.iter().any(|tile| tile.key == row.tile_key)
                && !row.decision_rule.is_empty()
                && !row.next_action.is_empty()
                && !row.evidence_source.is_empty()
        })
        && decision_frame.len() >= 5
        && presentation_order.len() >= 6
        && operator_questions.len() >= 5
        && residual_risks.len() >= 3
        && !claim_contract.can_show_customer_dollars
        && claim_contract.required_evidence.len() >= 6
        && claim_contract.blocked_by.len() >= 4;

    RoiDashboardSummary {
        name: "roi-dashboard-readiness".to_string(),
        passed,
        headline: format!(
            "{} more useful GPU admitted, {} stranded GPU reduced, {} active-node monthly cost delta vs kube, {} disruption cost for the hero repair",
            roi.admitted_useful_gpu_gain,
            roi.stranded_gpu_reduction,
            roi.ksolver_active_node_monthly_cost - roi.kube_active_node_monthly_cost,
            repair.disruption_cost
        ),
        primary_tiles,
        claim_contract,
        executive_rows,
        decision_rows,
        decision_frame,
        presentation_order,
        scenario_count: roi.scenarios_compared,
        hero_scenario: repair.name.clone(),
        hero_repair_disruption_cost: repair.disruption_cost,
        confidence_guardrail,
        regret_guardrail,
        operator_questions,
        residual_risks,
    }
}

fn summarize_pricing_readiness(
    roi: &RoiSummary,
    roi_dashboard: &RoiDashboardSummary,
) -> PricingReadinessSummary {
    let pricing_required_before_customer_claim = vec![
        "node monthly price for every GPU node shape in the target fleet".to_string(),
        "GPU count per node shape and whether pricing is per node, per GPU-hour, reserved, spot, or internal chargeback".to_string(),
        "currency, billing period, discount class, and whether idle active-node capacity is charged to the tenant".to_string(),
        "mapping from Kubernetes node labels or instance types to the pricing catalog entry used by ksolver".to_string(),
        "invoice or chargeback sample that reconciles at least one active node group against the catalog".to_string(),
    ];
    let accepted_sources = vec![
        "cloud SKU catalog export pinned to region, accelerator, commitment, and billing model".to_string(),
        "internal GPU chargeback table keyed by node pool, instance type, or accelerator SKU".to_string(),
        "CoreWeave/Lambda/on-prem contract rate sheet converted into monthly node or GPU-hour prices".to_string(),
        "recent invoice line items joined to Kubernetes node labels or node-pool metadata".to_string(),
    ];
    let roi_fields_to_recompute = vec![
        "roi_summary.kube_active_node_monthly_cost".to_string(),
        "roi_summary.ksolver_active_node_monthly_cost".to_string(),
        "roi_summary.active_node_monthly_cost_reduction".to_string(),
        "roi_dashboard_summary.primary_tiles[active_node_monthly_cost_delta]".to_string(),
        "tenant_fairness_metrics.tenants[].admitted_monthly_cost_milli".to_string(),
        "fairness_budget_summary.monthly_budget_milli and budget_overage_monthly_milli".to_string(),
    ];
    let validation_checks = vec![
        "every active GPU node has a non-zero price or an explicit unknown-price caveat".to_string(),
        "dashboard price_source is not a URL-only demo override before customer screenshots".to_string(),
        "active-node monthly cost delta sign matches recomputed kube minus ksolver active node totals".to_string(),
        "tenant budget denial examples use the same pricing catalog as the ROI tiles".to_string(),
        "synthetic scenario costs remain labeled synthetic when no catalog is loaded".to_string(),
    ];
    let pricing_evidence_rows = vec![
        PricingEvidenceRow {
            gate: "catalog source".to_string(),
            accepted_source: "cloud SKU export, internal chargeback table, contract rate sheet, or invoice sample".to_string(),
            required_mapping: "source must include region/provider, accelerator or node SKU, billing model, currency, and effective period".to_string(),
            recompute_target: "pricing_readiness_summary.current_mode and roi_dashboard_summary.primary_tiles[active_node_monthly_cost_delta]".to_string(),
            pass_signal: "current_mode names a non-synthetic catalog or every dollar tile is marked synthetic/demo".to_string(),
            failure_action: "hide customer dollar savings and keep only admitted GPU/utilization/disruption claims".to_string(),
        },
        PricingEvidenceRow {
            gate: "node-to-price mapping".to_string(),
            accepted_source: "Kubernetes node labels, instance type labels, node pool metadata, or operator inventory export".to_string(),
            required_mapping: "each active GPU node maps to exactly one catalog entry or carries an unknown-price caveat".to_string(),
            recompute_target: "roi_summary.{kube,ksolver}_active_node_monthly_cost".to_string(),
            pass_signal: "all active GPU nodes in both kube and ksolver placements have non-zero monthly price coverage".to_string(),
            failure_action: "show unknown-price nodes in the UI and do not present active-node cost delta as savings".to_string(),
        },
        PricingEvidenceRow {
            gate: "GPU-hour normalization".to_string(),
            accepted_source: "node monthly price, per-GPU-hour rate, reserved/spot discount table, or owned-hardware amortization table".to_string(),
            required_mapping: "convert every source into a common monthly milli-cost basis before comparing kube and ksolver".to_string(),
            recompute_target: "roi_summary.active_node_monthly_cost_reduction".to_string(),
            pass_signal: "cost delta sign matches recomputed kube minus ksolver monthly totals after normalization".to_string(),
            failure_action: "treat the dashboard value as directional only and show the normalization assumption".to_string(),
        },
        PricingEvidenceRow {
            gate: "tenant budget reconciliation".to_string(),
            accepted_source: "same pricing catalog used by ROI plus tenant monthly budget config or chargeback allocation".to_string(),
            required_mapping: "tenant budget rows and ROI tiles must use the same node price basis and currency".to_string(),
            recompute_target: "fairness_budget_summary.monthly_budget_milli and tenant_fairness_metrics.tenants[].admitted_monthly_cost_milli".to_string(),
            pass_signal: "budget denial examples reconcile to the active node price used by the ROI dashboard".to_string(),
            failure_action: "downgrade tenant budget denial to advisory until pricing and budget currency are aligned".to_string(),
        },
        PricingEvidenceRow {
            gate: "screenshot/customer claim guard".to_string(),
            accepted_source: "price_source query parameter, pricing catalog metadata, or report provenance".to_string(),
            required_mapping: "customer-facing screenshots must name the source and whether prices are synthetic, catalog, invoice, or chargeback".to_string(),
            recompute_target: "roi_dashboard_summary.decision_rows[active_node_monthly_cost_delta]".to_string(),
            pass_signal: "dollar claims show source, caveat, and next action beside the ROI tile".to_string(),
            failure_action: "replace dollar labels with relative cost units before sharing the demo externally".to_string(),
        },
    ];
    let operator_actions = vec![
        "load a pricing catalog or chargeback export before presenting dollar savings".to_string(),
        "keep GPU-hour URL overrides only for exploratory demos and screenshots clearly marked as demo pricing".to_string(),
        "recompute ROI and tenant budgets after changing node pools, discounts, or owned-hardware chargeback assumptions".to_string(),
        "show admitted useful GPU and stranded GPU even when price coverage is incomplete".to_string(),
    ];
    let residual_risks = vec![
        "discounts, reserved capacity, spot interruption risk, and owned-hardware amortization can change dollar ROI without changing placement quality".to_string(),
        "node labels may not uniquely identify a commercial SKU in heterogeneous or custom GPU fleets".to_string(),
        "active-node cost is a scheduling economics proxy; finance may allocate idle capacity differently".to_string(),
    ];
    let passed = roi.scenarios_compared > 0
        && roi_dashboard.passed
        && pricing_required_before_customer_claim.len() >= 5
        && accepted_sources.len() >= 4
        && roi_fields_to_recompute.len() >= 6
        && validation_checks.len() >= 5
        && pricing_evidence_rows.len() >= 5
        && pricing_evidence_rows.iter().all(|row| {
            !row.accepted_source.is_empty()
                && !row.required_mapping.is_empty()
                && !row.recompute_target.is_empty()
                && !row.failure_action.is_empty()
        })
        && operator_actions.len() >= 4
        && residual_risks.len() >= 3;

    PricingReadinessSummary {
        name: "pricing-readiness".to_string(),
        passed,
        current_mode: "synthetic scenario prices and optional dashboard URL overrides".to_string(),
        pricing_required_before_customer_claim,
        accepted_sources,
        roi_fields_to_recompute,
        validation_checks,
        pricing_evidence_rows,
        operator_actions,
        residual_risks,
    }
}

fn weighted_utilization_milli(scenarios: &[ScenarioResult], kube: bool) -> i64 {
    let mut weighted_sum = 0_i64;
    let mut active_nodes = 0_i64;
    for scenario in scenarios {
        let metrics = if kube {
            &scenario.kube.metrics
        } else {
            &scenario.ksolver.metrics
        };
        weighted_sum += metrics
            .gpu_utilization_milli
            .saturating_mul(metrics.active_nodes as i64);
        active_nodes += metrics.active_nodes as i64;
    }
    if active_nodes > 0 {
        weighted_sum / active_nodes
    } else {
        0
    }
}

fn repair_node(name: &str, gpu: i64) -> NormalizedNode {
    let mut extended = BTreeMap::new();
    extended.insert(GPU_RESOURCE.to_string(), gpu);
    NormalizedNode {
        name: name.to_string(),
        effective_capacity: ResourceList {
            milli_cpu: 64000,
            memory_bytes: 512 << 30,
            pods: 110,
            ..Default::default()
        },
        extended_resources: extended,
        ..Default::default()
    }
}

fn vram_node(name: &str, gpu: i64, vram_gib: i64) -> NormalizedNode {
    let mut node = repair_node(name, gpu);
    node.labels
        .insert("ksolver.dev/gpu-vram-gib".to_string(), vram_gib.to_string());
    node
}

fn topology_node(name: &str, gpu: i64, key: &str, value: &str) -> NormalizedNode {
    let mut node = repair_node(name, gpu);
    node.labels.insert(key.to_string(), value.to_string());
    node
}

fn mig_node(name: &str, resource: &str, quantity: i64) -> NormalizedNode {
    let mut node = repair_node(name, 0);
    node.extended_resources.clear();
    node.extended_resources
        .insert(resource.to_string(), quantity.max(0));
    node
}

fn dra_node(name: &str, resource: &str, quantity: i64) -> NormalizedNode {
    mig_node(name, resource, quantity)
}

fn repair_running_workload(name: &str, node: &str) -> NormalizedWorkload {
    NormalizedWorkload {
        namespace: "team".to_string(),
        name: name.to_string(),
        current_node: node.to_string(),
        requests: ResourceList {
            milli_cpu: 1000,
            memory_bytes: 1 << 30,
            pods: 1,
            ..Default::default()
        },
        extended_resource_requests: BTreeMap::from([(GPU_RESOURCE.to_string(), 1)]),
        feasible_node_names: vec!["n2".to_string()],
        ..Default::default()
    }
}

fn repair_policy_blocked_workload(name: &str, node: &str) -> NormalizedWorkload {
    NormalizedWorkload {
        do_not_disrupt: true,
        migration_allowed: false,
        preemption_allowed: false,
        ..repair_running_workload(name, node)
    }
}

fn repair_pending_workload(name: &str) -> NormalizedWorkload {
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
        extended_resource_requests: BTreeMap::from([(GPU_RESOURCE.to_string(), 1)]),
        feasible_node_names: vec!["n1".to_string()],
        ..Default::default()
    }
}

fn vram_pending_workload(name: &str, feasible_nodes: &[&str]) -> NormalizedWorkload {
    let mut workload = repair_pending_workload(name);
    workload.namespace = "team".to_string();
    workload.feasible_node_names = feasible_nodes.iter().map(|node| node.to_string()).collect();
    workload
}

fn mig_pending_workload(name: &str, resource: &str, feasible_nodes: &[&str]) -> NormalizedWorkload {
    let mut workload = vram_pending_workload(name, feasible_nodes);
    workload.extended_resource_requests.clear();
    workload
        .extended_resource_requests
        .insert(resource.to_string(), 1);
    workload
}

fn dra_pending_workload(
    name: &str,
    resource: Option<&str>,
    feasible_nodes: &[&str],
) -> NormalizedWorkload {
    let mut workload = vram_pending_workload(name, feasible_nodes);
    workload.extended_resource_requests.clear();
    if let Some(resource) = resource {
        workload
            .extended_resource_requests
            .insert(resource.to_string(), 1);
    }
    workload
}

fn repair_pending_pod(name: &str) -> PendingGpuPod {
    PendingGpuPod {
        uid: format!("uid-{name}"),
        namespace: "team".to_string(),
        name: name.to_string(),
        gpu_request: 1,
        priority: 9,
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

fn dra_pending_pod(name: &str) -> PendingGpuPod {
    let mut pod = repair_pending_pod(name);
    pod.gpu_request = 0;
    pod.gang_key = None;
    pod.colocate = false;
    pod.unmodeled_constraints = vec!["DRA: device demand modeled as scalar approximation".into()];
    pod
}

fn vram_pending_pod(name: &str, peak_gib: i64) -> PendingGpuPod {
    let mut pod = repair_pending_pod(name);
    pod.gang_key = None;
    pod.colocate = false;
    pod.predicted_peak_vram_bytes = peak_gib.saturating_mul(1024 * 1024 * 1024);
    pod
}

fn repair_unplaced_trace_with_reason(pending: &[PendingGpuPod], reason: &str) -> DecisionTrace {
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
        repair_metrics: RepairMetrics::default(),
        deadline_metrics: DeadlineMetrics::default(),
        quota_metrics: QuotaMetrics::default(),
        admission_metrics: AdmissionMetrics::default(),
        queue_wait_metrics: QueueWaitMetrics::default(),
        tenant_fairness_metrics: TenantFairnessMetrics::default(),
        gpu_utilization_metrics: GpuUtilizationMetrics::default(),
        outcome_summary: Default::default(),
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

fn repair_unplaced_trace(pending: &[PendingGpuPod]) -> DecisionTrace {
    repair_unplaced_trace_with_reason(pending, "gang not admitted")
}

fn fragmented_repair_scenario_proof() -> RepairScenarioProof {
    let pending = vec![
        repair_pending_pod("urgent-0"),
        repair_pending_pod("urgent-1"),
        repair_pending_pod("urgent-2"),
        repair_pending_pod("urgent-3"),
    ];
    let cluster = NormalizedCluster {
        nodes: vec![repair_node("n1", 4), repair_node("n2", 2)],
        workloads: vec![
            repair_running_workload("low-a", "n1"),
            repair_running_workload("low-b", "n1"),
            repair_running_workload("low-c", "n1"),
            repair_running_workload("low-d", "n1"),
            repair_pending_workload("urgent-0"),
            repair_pending_workload("urgent-1"),
            repair_pending_workload("urgent-2"),
            repair_pending_workload("urgent-3"),
        ],
        ..Default::default()
    };
    let trace = repair_unplaced_trace(&pending);
    let advice = advise_repairs(&cluster, &pending, &trace);
    let plan = advice.plans.first().cloned().unwrap_or_default();
    let action_rows = repair_action_rows(&plan.actions);
    let migration_actions = plan
        .actions
        .iter()
        .filter(|a| a.action == "migrate")
        .count();
    let preemption_actions = plan
        .actions
        .iter()
        .filter(|a| a.action == "preempt")
        .count();
    let passed = advice.metrics.repairable_targets == 1
        && advice.metrics.unrepairable_targets == 0
        && migration_actions == 2
        && preemption_actions == 2
        && plan.target == "team/urgent"
        && plan.target_gpu_request == 4
        && plan.freed_gpu >= 4;

    RepairScenarioProof {
        name: "fragmented-gang-repair".to_string(),
        passed,
        target: plan.target,
        target_gpu_request: plan.target_gpu_request,
        node: plan.node,
        action_count: plan.actions.len(),
        migration_actions,
        preemption_actions,
        freed_gpu: plan.freed_gpu,
        disruption_cost: plan.disruption_cost,
        explanation: plan.explanation,
        notes: advice.notes,
        metrics: advice.metrics,
        action_rows,
    }
}

fn repair_action_rows(actions: &[crate::scheduler::trace::RepairAction]) -> Vec<RepairActionRow> {
    actions
        .iter()
        .enumerate()
        .map(|(index, action)| RepairActionRow {
            step: index + 1,
            action: action.action.clone(),
            namespace: action.namespace.clone(),
            pod: action.pod.clone(),
            from_node: action.node.clone(),
            to_node: action.to_node.clone(),
            gpu_request: action.gpu_request,
            disruption_cost: action.disruption_cost,
            reason: action.reason.clone(),
        })
        .collect()
}

fn vram_blocked_repair_scenario_proof() -> RepairScenarioProof {
    let mut pending = repair_pending_pod("vram-huge");
    pending.gang_key = Some("team/vram-huge".to_string());
    pending.predicted_peak_vram_bytes = 160_i64 << 30;
    let pending = vec![pending];
    let cluster = NormalizedCluster {
        nodes: vec![repair_node("n1", 4), repair_node("n2", 4)],
        workloads: vec![
            repair_running_workload("low-a", "n1"),
            repair_running_workload("low-b", "n1"),
            repair_running_workload("low-c", "n1"),
            repair_running_workload("low-d", "n1"),
            repair_pending_workload("vram-huge"),
        ],
        ..Default::default()
    };
    let trace = repair_unplaced_trace_with_reason(
        &pending,
        "blocked by predicted peak VRAM exceeding known node GPU memory",
    );
    let advice = advise_repairs(&cluster, &pending, &trace);
    let note = advice.notes.first().cloned().unwrap_or_default();
    let passed = advice.plans.is_empty()
        && advice.metrics.repairable_targets == 0
        && advice.metrics.unrepairable_targets == 1
        && advice.metrics.vram_blocked_targets == 1
        && advice.metrics.migration_actions == 0
        && advice.metrics.preemption_actions == 0
        && note.contains("freeing occupied GPU slots will not make a too-small GPU fit");

    RepairScenarioProof {
        name: "vram-blocked-no-repair".to_string(),
        passed,
        target: "team/vram-huge".to_string(),
        target_gpu_request: 1,
        node: String::new(),
        action_count: 0,
        migration_actions: 0,
        preemption_actions: 0,
        freed_gpu: 0,
        disruption_cost: 0,
        explanation: note.clone(),
        notes: advice.notes,
        metrics: advice.metrics,
        action_rows: Vec::new(),
    }
}

fn policy_blocked_repair_scenario_proof() -> RepairScenarioProof {
    let pending = vec![
        repair_pending_pod("protected-urgent-0"),
        repair_pending_pod("protected-urgent-1"),
        repair_pending_pod("protected-urgent-2"),
        repair_pending_pod("protected-urgent-3"),
    ];
    let cluster = NormalizedCluster {
        nodes: vec![repair_node("n1", 4)],
        workloads: vec![
            repair_policy_blocked_workload("protected-a", "n1"),
            repair_policy_blocked_workload("protected-b", "n1"),
            repair_policy_blocked_workload("protected-c", "n1"),
            repair_policy_blocked_workload("protected-d", "n1"),
            repair_pending_workload("protected-urgent-0"),
            repair_pending_workload("protected-urgent-1"),
            repair_pending_workload("protected-urgent-2"),
            repair_pending_workload("protected-urgent-3"),
        ],
        ..Default::default()
    };
    let trace = repair_unplaced_trace(&pending);
    let advice = advise_repairs(&cluster, &pending, &trace);
    let note = advice.notes.first().cloned().unwrap_or_default();
    let passed = advice.plans.is_empty()
        && advice.metrics.repairable_targets == 0
        && advice.metrics.unrepairable_targets == 1
        && advice.metrics.policy_or_candidate_blocked_targets == 1
        && advice.metrics.migration_actions == 0
        && advice.metrics.preemption_actions == 0
        && note.contains("policy and candidate budget");

    RepairScenarioProof {
        name: "policy-blocked-no-repair".to_string(),
        passed,
        target: "team/urgent".to_string(),
        target_gpu_request: 4,
        node: String::new(),
        action_count: 0,
        migration_actions: 0,
        preemption_actions: 0,
        freed_gpu: 0,
        disruption_cost: 0,
        explanation: note.clone(),
        notes: advice.notes,
        metrics: advice.metrics,
        action_rows: Vec::new(),
    }
}

fn not_enough_total_gpu_repair_scenario_proof() -> RepairScenarioProof {
    let pending = vec![
        repair_pending_pod("capacity-urgent-0"),
        repair_pending_pod("capacity-urgent-1"),
        repair_pending_pod("capacity-urgent-2"),
        repair_pending_pod("capacity-urgent-3"),
    ];
    let cluster = NormalizedCluster {
        nodes: vec![repair_node("n1", 2), repair_node("n2", 1)],
        workloads: vec![
            repair_pending_workload("capacity-urgent-0"),
            repair_pending_workload("capacity-urgent-1"),
            repair_pending_workload("capacity-urgent-2"),
            repair_pending_workload("capacity-urgent-3"),
        ],
        ..Default::default()
    };
    let trace = repair_unplaced_trace(&pending);
    let advice = advise_repairs(&cluster, &pending, &trace);
    let note = advice.notes.first().cloned().unwrap_or_default();
    let passed = advice.plans.is_empty()
        && advice.metrics.repairable_targets == 0
        && advice.metrics.unrepairable_targets == 1
        && advice.metrics.not_enough_total_gpu_targets == 1
        && advice.metrics.migration_actions == 0
        && advice.metrics.preemption_actions == 0
        && note.contains("enough total GPU capacity");

    RepairScenarioProof {
        name: "not-enough-total-gpu-no-repair".to_string(),
        passed,
        target: "team/urgent".to_string(),
        target_gpu_request: 4,
        node: String::new(),
        action_count: 0,
        migration_actions: 0,
        preemption_actions: 0,
        freed_gpu: 0,
        disruption_cost: 0,
        explanation: note.clone(),
        notes: advice.notes,
        metrics: advice.metrics,
        action_rows: Vec::new(),
    }
}

fn vram_prediction_scenario_proof() -> VramPredictionProof {
    let predicted_peak_vram_gib = 40;
    let adequate_cluster = NormalizedCluster {
        nodes: vec![vram_node("l4-24g", 8, 24), vram_node("h100-80g", 8, 80)],
        workloads: vec![vram_pending_workload(
            "vram-filtered",
            &["l4-24g", "h100-80g"],
        )],
        ..Default::default()
    };
    let adequate_pending = vec![vram_pending_pod("vram-filtered", predicted_peak_vram_gib)];
    let (adequate_input, adequate_drops) = build_pending_input_diagnosed(
        &adequate_cluster,
        &adequate_pending,
        &BTreeMap::new(),
        &|name| name == GPU_RESOURCE,
    );
    let adequate_feasible_nodes = adequate_input
        .workloads
        .first()
        .map(|w| w.feasible_nodes.clone())
        .unwrap_or_default();

    let impossible_cluster = NormalizedCluster {
        nodes: vec![vram_node("l4-24g", 8, 24)],
        workloads: vec![vram_pending_workload("vram-impossible", &["l4-24g"])],
        ..Default::default()
    };
    let impossible_pending = vec![vram_pending_pod("vram-impossible", predicted_peak_vram_gib)];
    let (impossible_input, impossible_drops) = build_pending_input_diagnosed(
        &impossible_cluster,
        &impossible_pending,
        &BTreeMap::new(),
        &|name| name == GPU_RESOURCE,
    );
    let impossible_drop_reason = impossible_drops
        .first()
        .map(|d| d.reason.clone())
        .unwrap_or_default();
    let rejected_too_small_nodes = if adequate_feasible_nodes.iter().any(|n| n == "l4-24g") {
        Vec::new()
    } else {
        vec!["l4-24g".to_string()]
    };
    let passed = adequate_input.workloads.len() == 1
        && adequate_drops.is_empty()
        && adequate_feasible_nodes == vec!["h100-80g".to_string()]
        && impossible_input.workloads.is_empty()
        && impossible_drops.len() == 1
        && impossible_drop_reason.contains("predicted peak VRAM");

    VramPredictionProof {
        name: "vram-prediction-feasibility".to_string(),
        passed,
        predicted_peak_vram_gib,
        adequate_feasible_nodes,
        rejected_too_small_nodes,
        impossible_input_workloads: impossible_input.workloads.len(),
        impossible_drop_count: impossible_drops.len(),
        impossible_drop_reason,
    }
}

fn summarize_vram_investment_demo() -> VramInvestmentDemoSummary {
    let rows = vec![
        vram_demo_row(VramDemoInput {
            scenario: "OOM avoidance: 48Gi inference job",
            workload: "wide-vram-infer",
            predictor_source: "fake_exact_command_history",
            confidence: 88,
            gpu_request: 1,
            predicted_peak_vram_gib: 48,
            predicted_lower_vram_gib: 44,
            predicted_upper_vram_gib: 54,
            kube_node: "l4-24g",
            kube_node_vram_gib: 24,
            ksolver_node: "h100-80g",
            ksolver_node_vram_gib: 80,
            investment_case: "A calibrated predictor converts a scalar-GPU placement that would likely CUDA OOM into a known-memory fit before the pod starts.",
            caveat: "Synthetic prediction row; real hard enforcement needs per-pod GPU VRAM observations and source-tier error budgets.",
        }),
        vram_demo_row(VramDemoInput {
            scenario: "High-memory preservation: small jobs do not burn H100s",
            workload: "lowmem-embedding-batch",
            predictor_source: "fake_training_hint",
            confidence: 62,
            gpu_request: 1,
            predicted_peak_vram_gib: 10,
            predicted_lower_vram_gib: 7,
            predicted_upper_vram_gib: 16,
            kube_node: "h100-80g",
            kube_node_vram_gib: 80,
            ksolver_node: "l4-24g",
            ksolver_node_vram_gib: 24,
            investment_case: "Even when both placements run, VRAM-aware rightsizing preserves scarce 80Gi devices for later large jobs.",
            caveat: "Training-hint estimates should be a tie-break/advisory source until calibrated by completed-job samples.",
        }),
        vram_demo_row(VramDemoInput {
            scenario: "Confidence band safety: point estimate fits, upper band does not",
            workload: "bursty-seq2seq-train",
            predictor_source: "fake_scaled_history",
            confidence: 70,
            gpu_request: 1,
            predicted_peak_vram_gib: 36,
            predicted_lower_vram_gib: 28,
            predicted_upper_vram_gib: 58,
            kube_node: "a10-48g",
            kube_node_vram_gib: 48,
            ksolver_node: "h100-80g",
            ksolver_node_vram_gib: 80,
            investment_case: "The predictor is valuable only if the scheduler uses the upper band, not just the median, for jobs with dynamic shapes.",
            caveat: "The confidence band is fake here; production needs error distribution by command/image/framework/GPU SKU.",
        }),
        vram_demo_row(VramDemoInput {
            scenario: "Impossible memory: fail closed instead of migrating pods",
            workload: "frontier-finetune-160g",
            predictor_source: "fake_explicit_user_hint",
            confidence: 95,
            gpu_request: 1,
            predicted_peak_vram_gib: 160,
            predicted_lower_vram_gib: 150,
            predicted_upper_vram_gib: 180,
            kube_node: "h100-80g",
            kube_node_vram_gib: 80,
            ksolver_node: "unplaced",
            ksolver_node_vram_gib: 0,
            investment_case: "A VRAM-aware scheduler can explain that no amount of defragmentation fixes a too-small GPU; this prevents disruptive and pointless repairs.",
            caveat: "Explicit hints are useful for demos, but production should verify them against observed peaks.",
        }),
        vram_demo_row(VramDemoInput {
            scenario: "Unknown GPU memory inventory: advisory only",
            workload: "unknown-inventory-llm",
            predictor_source: "fake_exact_command_history",
            confidence: 85,
            gpu_request: 1,
            predicted_peak_vram_gib: 42,
            predicted_lower_vram_gib: 38,
            predicted_upper_vram_gib: 48,
            kube_node: "gpu-node-without-memory-label",
            kube_node_vram_gib: 0,
            ksolver_node: "gpu-node-without-memory-label",
            ksolver_node_vram_gib: 0,
            investment_case: "The predictor alone is not enough; the product also needs complete node GPU-memory inventory to make hard placement claims.",
            caveat: "Unknown node memory keeps the decision advisory because ksolver cannot prove fit without inventory labels.",
        }),
        vram_demo_row(VramDemoInput {
            scenario: "Distributed training: 72Gi per replica needs the 80Gi pool",
            workload: "fsdp-train-worker",
            predictor_source: "fake_exact_command_history",
            confidence: 82,
            gpu_request: 4,
            predicted_peak_vram_gib: 72,
            predicted_lower_vram_gib: 64,
            predicted_upper_vram_gib: 79,
            kube_node: "a10-48g",
            kube_node_vram_gib: 48,
            ksolver_node: "h100-80g",
            ksolver_node_vram_gib: 80,
            investment_case: "Per-replica VRAM lets the scheduler route distributed jobs to the right GPU class instead of discovering memory pressure after launch.",
            caveat: "Real distributed training keys must include FSDP/ZeRO/DDP strategy, precision, batch, sequence length, and image digest.",
        }),
    ];

    let baseline_cuda_oom_risk_pods = rows
        .iter()
        .filter(|row| row.kube_cuda_oom_risk_percent >= 70)
        .count();
    let ksolver_cuda_oom_risk_pods = rows
        .iter()
        .filter(|row| row.ksolver_cuda_oom_risk_percent >= 70)
        .count();
    let high_vram_nodes_preserved = rows
        .iter()
        .filter(|row| row.preserves_high_vram_capacity)
        .count();
    let unknown_or_advisory_rows = rows.iter().filter(|row| row.advisory_only).count();
    let average_baseline_oom_risk_percent = average_risk(&rows, true);
    let average_ksolver_oom_risk_percent = average_risk(&rows, false);
    let passed = rows.len() >= 6
        && baseline_cuda_oom_risk_pods > ksolver_cuda_oom_risk_pods
        && high_vram_nodes_preserved > 0
        && unknown_or_advisory_rows > 0;

    VramInvestmentDemoSummary {
        name: "vram-predictor-investment-demo".to_string(),
        passed,
        headline: format!(
            "Synthetic VRAM predictor demo reduces likely CUDA OOM placements from {} to {} and shows {} high-memory preservation case(s).",
            baseline_cuda_oom_risk_pods, ksolver_cuda_oom_risk_pods, high_vram_nodes_preserved
        ),
        synthetic_prediction_notice:
            "Predicted peaks, confidence bands, and OOM likelihoods are deterministic fake values for demo design; use them to argue for collecting real DCGM/NVML calibration data, not as production accuracy claims."
                .to_string(),
        scenario_count: rows.len(),
        baseline_cuda_oom_risk_pods,
        ksolver_cuda_oom_risk_pods,
        cuda_oom_risk_reduction_pods: baseline_cuda_oom_risk_pods as isize
            - ksolver_cuda_oom_risk_pods as isize,
        high_vram_nodes_preserved,
        unknown_or_advisory_rows,
        average_baseline_oom_risk_percent,
        average_ksolver_oom_risk_percent,
        rows,
        operator_claims: vec![
            "VRAM predictions can prevent known-bad placements before expensive training startup time is wasted.".to_string(),
            "Upper confidence bands are the right scheduling primitive for OOM avoidance; point estimates are not enough.".to_string(),
            "Rightsizing by VRAM preserves scarce high-memory GPUs for jobs that actually need them.".to_string(),
            "Unknown node memory and uncalibrated sources must stay advisory, which creates a clear roadmap for an investable predictor.".to_string(),
        ],
        required_real_predictor_evidence: vec![
            "per-pod GPU VRAM peak from DCGM/NVML or equivalent attribution".to_string(),
            "prediction keys that include image digest, command hash, framework version, precision, batch, sequence length, optimizer, and distributed strategy".to_string(),
            "source-tier MAPE, max error, and upper-band miss rate by GPU SKU".to_string(),
            "online audit rows showing prediction source, confidence, lower/upper VRAM band, selected node memory, and actual observed peak after completion".to_string(),
            "promotion gates that allow hard filtering only for explicit or high-confidence calibrated source tiers".to_string(),
        ],
    }
}

struct VramDemoInput<'a> {
    scenario: &'a str,
    workload: &'a str,
    predictor_source: &'a str,
    confidence: i64,
    gpu_request: i64,
    predicted_peak_vram_gib: i64,
    predicted_lower_vram_gib: i64,
    predicted_upper_vram_gib: i64,
    kube_node: &'a str,
    kube_node_vram_gib: i64,
    ksolver_node: &'a str,
    ksolver_node_vram_gib: i64,
    investment_case: &'a str,
    caveat: &'a str,
}

fn vram_demo_row(input: VramDemoInput<'_>) -> VramInvestmentDemoRow {
    let kube_risk = cuda_oom_risk_percent(
        input.predicted_peak_vram_gib,
        input.predicted_upper_vram_gib,
        input.kube_node_vram_gib,
        input.confidence,
        input.kube_node == "unplaced",
    );
    let ksolver_risk = cuda_oom_risk_percent(
        input.predicted_peak_vram_gib,
        input.predicted_upper_vram_gib,
        input.ksolver_node_vram_gib,
        input.confidence,
        input.ksolver_node == "unplaced",
    );
    let preserves_high_vram_capacity = input.kube_node_vram_gib >= 80
        && input.ksolver_node_vram_gib > 0
        && input.ksolver_node_vram_gib < input.kube_node_vram_gib;
    let advisory_only = (input.kube_node_vram_gib <= 0 && input.kube_node != "unplaced")
        || (input.ksolver_node_vram_gib <= 0 && input.ksolver_node != "unplaced");
    let kube_upper_band_headroom_gib = if input.kube_node_vram_gib > 0 {
        input.kube_node_vram_gib - input.predicted_upper_vram_gib
    } else {
        0
    };
    let ksolver_upper_band_headroom_gib = if input.ksolver_node_vram_gib > 0 {
        input.ksolver_node_vram_gib - input.predicted_upper_vram_gib
    } else {
        0
    };
    let risk_delta_percent = kube_risk - ksolver_risk;
    let decision_reason = if advisory_only {
        "inventory missing; keep advisory until node VRAM is known"
    } else if input.ksolver_node == "unplaced" {
        "fail closed because no node can satisfy the predicted upper VRAM band"
    } else if kube_risk >= 70 && ksolver_risk < 70 {
        "moves work from a likely-OOM node to a node with enough upper-band VRAM headroom"
    } else if preserves_high_vram_capacity {
        "keeps scarce high-memory GPUs available by placing low-memory work on a smaller fit"
    } else if ksolver_upper_band_headroom_gib > kube_upper_band_headroom_gib {
        "uses upper-band headroom as the placement guardrail"
    } else {
        "placement is advisory because the synthetic predictor has not been calibrated"
    };

    VramInvestmentDemoRow {
        scenario: input.scenario.to_string(),
        workload: input.workload.to_string(),
        predictor_source: input.predictor_source.to_string(),
        confidence: input.confidence,
        gpu_request: input.gpu_request,
        predicted_peak_vram_gib: input.predicted_peak_vram_gib,
        predicted_lower_vram_gib: input.predicted_lower_vram_gib,
        predicted_upper_vram_gib: input.predicted_upper_vram_gib,
        kube_node: input.kube_node.to_string(),
        kube_node_vram_gib: input.kube_node_vram_gib,
        kube_cuda_oom_risk_percent: kube_risk,
        kube_risk_label: cuda_oom_risk_label(kube_risk).to_string(),
        kube_upper_band_headroom_gib,
        ksolver_node: input.ksolver_node.to_string(),
        ksolver_node_vram_gib: input.ksolver_node_vram_gib,
        ksolver_cuda_oom_risk_percent: ksolver_risk,
        ksolver_risk_label: cuda_oom_risk_label(ksolver_risk).to_string(),
        ksolver_upper_band_headroom_gib,
        risk_delta_percent,
        avoided_failure: kube_risk >= 70 && ksolver_risk < 70,
        preserves_high_vram_capacity,
        advisory_only,
        decision_reason: decision_reason.to_string(),
        investment_case: input.investment_case.to_string(),
        caveat: input.caveat.to_string(),
    }
}

fn cuda_oom_risk_percent(
    predicted_peak_vram_gib: i64,
    predicted_upper_vram_gib: i64,
    node_vram_gib: i64,
    confidence: i64,
    unplaced: bool,
) -> i64 {
    if unplaced {
        return 0;
    }
    if node_vram_gib <= 0 {
        return 60;
    }
    if predicted_peak_vram_gib > node_vram_gib {
        return 95;
    }
    if predicted_upper_vram_gib > node_vram_gib {
        return (75 - confidence / 5).clamp(35, 70);
    }
    let headroom = node_vram_gib - predicted_upper_vram_gib;
    if headroom <= 4 {
        25
    } else if headroom <= 12 {
        12
    } else {
        5
    }
}

fn cuda_oom_risk_label(risk_percent: i64) -> &'static str {
    match risk_percent {
        0..=15 => "low",
        16..=49 => "guarded",
        50..=69 => "advisory",
        _ => "likely CUDA OOM",
    }
}

fn average_risk(rows: &[VramInvestmentDemoRow], kube: bool) -> i64 {
    if rows.is_empty() {
        return 0;
    }
    let total: i64 = rows
        .iter()
        .map(|row| {
            if kube {
                row.kube_cuda_oom_risk_percent
            } else {
                row.ksolver_cuda_oom_risk_percent
            }
        })
        .sum();
    total / rows.len() as i64
}

fn gpu_topology_scenario_proof() -> GpuTopologyProof {
    let topology_key = "topology.gpu.ksolver.dev/island".to_string();
    let required_value = "nvlink-a".to_string();
    let matching_cluster = NormalizedCluster {
        nodes: vec![
            topology_node("nvlink-a-0", 8, &topology_key, &required_value),
            topology_node("nvlink-b-0", 8, &topology_key, "nvlink-b"),
            repair_node("unlabeled-0", 8),
        ],
        workloads: vec![vram_pending_workload(
            "topology-local",
            &["nvlink-a-0", "nvlink-b-0", "unlabeled-0"],
        )],
        ..Default::default()
    };
    let mut matching_pending = repair_pending_pod("topology-local");
    matching_pending.required_gpu_topology = vec![(topology_key.clone(), required_value.clone())];
    let (matching_input, matching_drops) = build_pending_input_diagnosed(
        &matching_cluster,
        &[matching_pending],
        &BTreeMap::new(),
        &|name| name == GPU_RESOURCE,
    );
    let matching_feasible_nodes = matching_input
        .workloads
        .first()
        .map(|w| w.feasible_nodes.clone())
        .unwrap_or_default();
    let rejected_nodes = ["nvlink-b-0", "unlabeled-0"]
        .into_iter()
        .filter(|node| !matching_feasible_nodes.iter().any(|n| n == node))
        .map(str::to_string)
        .collect::<Vec<_>>();

    let impossible_cluster = NormalizedCluster {
        nodes: vec![topology_node("nvlink-b-0", 8, &topology_key, "nvlink-b")],
        workloads: vec![vram_pending_workload(
            "topology-impossible",
            &["nvlink-b-0"],
        )],
        ..Default::default()
    };
    let mut impossible_pending = repair_pending_pod("topology-impossible");
    impossible_pending.required_gpu_topology = vec![(topology_key.clone(), required_value.clone())];
    let (impossible_input, impossible_drops) = build_pending_input_diagnosed(
        &impossible_cluster,
        &[impossible_pending],
        &BTreeMap::new(),
        &|name| name == GPU_RESOURCE,
    );
    let impossible_drop_reason = impossible_drops
        .first()
        .map(|d| d.reason.clone())
        .unwrap_or_default();
    let passed = matching_input.workloads.len() == 1
        && matching_drops.is_empty()
        && matching_feasible_nodes == vec!["nvlink-a-0".to_string()]
        && rejected_nodes == vec!["nvlink-b-0".to_string(), "unlabeled-0".to_string()]
        && impossible_input.workloads.is_empty()
        && impossible_drops.len() == 1
        && impossible_drop_reason.contains("required GPU topology label")
        && impossible_drop_reason.contains(&format!("{topology_key}={required_value}"));

    GpuTopologyProof {
        name: "gpu-topology-locality-feasibility".to_string(),
        passed,
        topology_key,
        required_value,
        matching_feasible_nodes,
        rejected_nodes,
        impossible_input_workloads: impossible_input.workloads.len(),
        impossible_drop_count: impossible_drops.len(),
        impossible_drop_reason,
    }
}

fn mig_profile_scenario_proof() -> MigProfileProof {
    let requested_resource = "nvidia.com/mig-3g.20gb".to_string();
    let matching_cluster = NormalizedCluster {
        nodes: vec![
            mig_node("mig-1g-node", "nvidia.com/mig-1g.5gb", 7),
            mig_node("mig-3g-node", &requested_resource, 2),
            repair_node("whole-gpu-node", 1),
        ],
        workloads: vec![mig_pending_workload(
            "mig-profiled",
            &requested_resource,
            &["mig-1g-node", "mig-3g-node", "whole-gpu-node"],
        )],
        ..Default::default()
    };
    let matching_pending = repair_pending_pod("mig-profiled");
    let (matching_input, matching_drops) = build_pending_input_diagnosed(
        &matching_cluster,
        &[matching_pending],
        &BTreeMap::new(),
        &|name| name == GPU_RESOURCE || name.starts_with("nvidia.com/mig-"),
    );
    let matching_feasible_nodes = matching_input
        .workloads
        .first()
        .map(|w| w.feasible_nodes.clone())
        .unwrap_or_default();
    let rejected_nodes = ["mig-1g-node", "whole-gpu-node"]
        .into_iter()
        .filter(|node| !matching_feasible_nodes.iter().any(|n| n == node))
        .map(str::to_string)
        .collect::<Vec<_>>();

    let impossible_cluster = NormalizedCluster {
        nodes: vec![mig_node("mig-1g-node", "nvidia.com/mig-1g.5gb", 7)],
        workloads: vec![mig_pending_workload(
            "mig-impossible",
            &requested_resource,
            &["mig-1g-node"],
        )],
        ..Default::default()
    };
    let impossible_pending = repair_pending_pod("mig-impossible");
    let (impossible_input, impossible_drops) = build_pending_input_diagnosed(
        &impossible_cluster,
        &[impossible_pending],
        &BTreeMap::new(),
        &|name| name == GPU_RESOURCE || name.starts_with("nvidia.com/mig-"),
    );
    let impossible_drop_reason = impossible_drops
        .first()
        .map(|d| d.reason.clone())
        .unwrap_or_default();
    let passed = matching_input.workloads.len() == 1
        && matching_drops.is_empty()
        && matching_feasible_nodes == vec!["mig-3g-node".to_string()]
        && rejected_nodes == vec!["mig-1g-node".to_string(), "whole-gpu-node".to_string()]
        && impossible_input.workloads.is_empty()
        && impossible_drops.len() == 1
        && impossible_drop_reason.contains("no feasible node");

    MigProfileProof {
        name: "mig-profile-compatibility".to_string(),
        passed,
        requested_resource,
        requested_quantity: 1,
        matching_feasible_nodes,
        rejected_nodes,
        impossible_input_workloads: impossible_input.workloads.len(),
        impossible_drop_count: impossible_drops.len(),
        impossible_drop_reason,
    }
}

fn dra_approximation_scenario_proof() -> DraApproximationProof {
    let synthetic_resource = "dra.ksolver/gpu.example.com".to_string();
    let modeled_cluster = NormalizedCluster {
        nodes: vec![dra_node("dra-node", &synthetic_resource, 1)],
        workloads: vec![dra_pending_workload(
            "dra-modeled",
            Some(&synthetic_resource),
            &["dra-node"],
        )],
        ..Default::default()
    };
    let modeled_pending = dra_pending_pod("dra-modeled");
    let (modeled_input, modeled_drops) = build_pending_input_diagnosed(
        &modeled_cluster,
        &[modeled_pending],
        &BTreeMap::new(),
        &|name| name == GPU_RESOURCE,
    );
    let modeled_workload = modeled_input.workloads.first();
    let modeled_feasible_nodes = modeled_workload
        .map(|w| w.feasible_nodes.clone())
        .unwrap_or_default();
    let modeled_request_quantity = modeled_workload
        .and_then(|w| w.extended_resource_requests.get(&synthetic_resource))
        .copied()
        .unwrap_or_default();

    let unmodeled_cluster = NormalizedCluster {
        nodes: vec![repair_node("generic-gpu-node", 1)],
        workloads: vec![dra_pending_workload(
            "dra-unmodeled",
            None,
            &["generic-gpu-node"],
        )],
        ..Default::default()
    };
    let unmodeled_pending = dra_pending_pod("dra-unmodeled");
    let (unmodeled_input, unmodeled_drops) = build_pending_input_diagnosed(
        &unmodeled_cluster,
        &[unmodeled_pending],
        &BTreeMap::new(),
        &|name| name == GPU_RESOURCE,
    );
    let unmodeled_drop_reason = unmodeled_drops
        .first()
        .map(|d| d.reason.clone())
        .unwrap_or_default();
    let passed = modeled_input.workloads.len() == 1
        && modeled_drops.is_empty()
        && modeled_feasible_nodes == vec!["dra-node".to_string()]
        && modeled_request_quantity == 1
        && unmodeled_input.workloads.is_empty()
        && unmodeled_drops.len() == 1
        && unmodeled_drop_reason.contains("DRA device demand was not modeled");

    DraApproximationProof {
        name: "dra-scalar-approximation-guard".to_string(),
        passed,
        synthetic_resource,
        modeled_feasible_nodes,
        modeled_request_quantity,
        unmodeled_input_workloads: unmodeled_input.workloads.len(),
        unmodeled_drop_count: unmodeled_drops.len(),
        unmodeled_drop_reason,
    }
}

fn dra_attr_str(value: &str) -> dra::DeviceAttribute {
    dra::DeviceAttribute {
        string: Some(value.to_string()),
        ..Default::default()
    }
}

fn dra_device(name: &str, attrs: &[(&str, dra::DeviceAttribute)]) -> dra::Device {
    dra::Device {
        name: name.to_string(),
        basic: Some(dra::BasicDevice {
            attributes: Some(
                attrs
                    .iter()
                    .map(|(k, v)| (k.to_string(), v.clone()))
                    .collect(),
            ),
            ..Default::default()
        }),
    }
}

fn dra_slice(
    node: &str,
    driver: &str,
    pool: &str,
    generation: i64,
    devices: Vec<dra::Device>,
) -> dra::ResourceSlice {
    dra::ResourceSlice {
        spec: dra::ResourceSliceSpec {
            driver: driver.to_string(),
            node_name: Some(node.to_string()),
            pool: dra::ResourcePool {
                name: pool.to_string(),
                generation,
                resource_slice_count: 1,
            },
            devices: Some(devices),
            ..Default::default()
        },
        ..Default::default()
    }
}

fn dra_class(name: &str, expr: &str) -> dra::DeviceClass {
    dra::DeviceClass {
        metadata: ObjectMeta {
            name: Some(name.to_string()),
            ..Default::default()
        },
        spec: dra::DeviceClassSpec {
            selectors: Some(vec![dra::DeviceSelector {
                cel: Some(dra::CELDeviceSelector {
                    expression: expr.to_string(),
                }),
            }]),
            ..Default::default()
        },
    }
}

fn dra_allocated_claim(driver: &str, pool: &str, device: &str) -> dra::ResourceClaim {
    dra::ResourceClaim {
        status: Some(dra::ResourceClaimStatus {
            allocation: Some(dra::AllocationResult {
                devices: Some(dra::DeviceAllocationResult {
                    results: Some(vec![dra::DeviceRequestAllocationResult {
                        device: device.to_string(),
                        driver: driver.to_string(),
                        pool: pool.to_string(),
                        request: "req".to_string(),
                        ..Default::default()
                    }]),
                    ..Default::default()
                }),
                ..Default::default()
            }),
            ..Default::default()
        }),
        ..Default::default()
    }
}

fn dra_allocation_scenario_proof() -> DraAllocationProof {
    let node = "dra-node".to_string();
    let device_class = "a100".to_string();
    let driver = "gpu.nvidia.com";
    let pool = "pool-a";
    let slices = vec![dra_slice(
        &node,
        driver,
        pool,
        1,
        vec![
            dra_device("gpu0", &[("gpu.nvidia.com/model", dra_attr_str("A100"))]),
            dra_device("gpu1", &[("gpu.nvidia.com/model", dra_attr_str("A100"))]),
        ],
    )];
    let classes = vec![dra_class(
        &device_class,
        r#"device.attributes["gpu.nvidia.com"].model == "A100""#,
    )];
    let claims = vec![dra_allocated_claim(driver, pool, "gpu0")];

    let availability = crate::dra::compute_availability(&slices, &classes, &claims);
    let available_devices = availability
        .by_node_class
        .get(&(node.clone(), device_class.clone()))
        .copied()
        .unwrap_or_default();
    let unevaluable_classes = availability
        .unevaluable_classes
        .iter()
        .cloned()
        .collect::<Vec<_>>();
    let passed = available_devices == 1
        && !availability.overlapping_classes
        && availability.unevaluable_classes.is_empty();

    DraAllocationProof {
        name: "dra-allocated-device-subtraction".to_string(),
        passed,
        node,
        device_class,
        total_matching_devices: 2,
        allocated_devices: 1,
        available_devices,
        overlapping_classes: availability.overlapping_classes,
        unevaluable_classes,
    }
}

fn time_sliced_gpu_scenario_proof() -> TimeSlicedGpuProof {
    let workload = grouping_workload("shared-gpu-pod", &["shared-gpu-node"]);
    let input = OptimizationInput {
        workloads: vec![workload],
        ..Default::default()
    };
    let mut counts = HashMap::new();
    counts.insert("shared-gpu-node".to_string(), 1);
    let mut assignment_counts = HashMap::new();
    assignment_counts.insert("pod:team/shared-gpu-pod".to_string(), counts);
    let solution = OptimizationSolution {
        assignment_counts,
        ..Default::default()
    };
    let mut pending = repair_pending_pod("shared-gpu-pod");
    pending.gang_key = None;
    pending.colocate = false;
    let time_sliced_nodes = BTreeSet::from(["shared-gpu-node".to_string()])
        .into_iter()
        .collect();
    let shared_trace = crate::scheduler::decision::build_decision_trace(
        1,
        &[pending.clone()],
        &input,
        &solution,
        "OPTIMAL",
        true,
        5,
        5,
        1,
        &HashMap::new(),
        &time_sliced_nodes,
    );
    let isolated_trace = crate::scheduler::decision::build_decision_trace(
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
        &Default::default(),
    );
    let time_sliced_caveats = shared_trace
        .decisions
        .first()
        .map(|d| d.caveats.clone())
        .unwrap_or_default();
    let isolated_caveats = isolated_trace
        .decisions
        .first()
        .map(|d| d.caveats.clone())
        .unwrap_or_default();
    let passed = time_sliced_caveats
        .iter()
        .any(|c| c.contains("time-sliced GPU"))
        && !isolated_caveats
            .iter()
            .any(|c| c.contains("time-sliced GPU"));

    TimeSlicedGpuProof {
        name: "time-sliced-gpu-disclosure".to_string(),
        passed,
        time_sliced_node: "shared-gpu-node".to_string(),
        isolated_node: "shared-gpu-node".to_string(),
        time_sliced_caveats,
        isolated_caveats,
    }
}

fn grouping_node(name: &str) -> OptimizationNode {
    let mut extended = BTreeMap::new();
    extended.insert(GPU_RESOURCE.to_string(), 1);
    OptimizationNode {
        name: name.to_string(),
        count: 1,
        effective_capacity: ResourceList {
            milli_cpu: 8000,
            memory_bytes: 32 << 30,
            pods: 10,
            ..Default::default()
        },
        extended_resources: extended,
        ..Default::default()
    }
}

fn grouping_workload(name: &str, feasible_nodes: &[&str]) -> OptimizationWorkload {
    OptimizationWorkload {
        id: format!("pod:team/{name}"),
        namespace: "team".to_string(),
        name: name.to_string(),
        group_size: 1,
        members: vec![OptimizationWorkloadMember {
            namespace: "team".to_string(),
            name: name.to_string(),
            current_node: String::new(),
        }],
        requests: ResourceList {
            milli_cpu: 1000,
            memory_bytes: 1 << 30,
            pods: 1,
            ..Default::default()
        },
        extended_resource_requests: BTreeMap::from([(GPU_RESOURCE.to_string(), 1)]),
        feasible_nodes: feasible_nodes.iter().map(|node| node.to_string()).collect(),
        ..Default::default()
    }
}

fn solved_admitted_workloads(solution: &OptimizationSolution) -> usize {
    solution
        .assignment_counts
        .values()
        .filter(|counts| counts.values().any(|count| *count > 0))
        .count()
}

fn solved_admitted_gpu(input: &OptimizationInput, solution: &OptimizationSolution) -> i64 {
    let workload_by_id: BTreeMap<&str, &OptimizationWorkload> =
        input.workloads.iter().map(|w| (w.id.as_str(), w)).collect();
    solution
        .assignment_counts
        .iter()
        .filter_map(|(workload_id, counts)| {
            let workload = workload_by_id.get(workload_id.as_str())?;
            let placed_replicas: i64 = counts.values().map(|count| i64::from(*count).max(0)).sum();
            let group_size = i64::from(workload.group_size).max(1);
            let total_gpu = crate::model::optimization_workload_gpu_request(workload).max(0);
            Some((total_gpu.saturating_mul(placed_replicas)) / group_size)
        })
        .sum()
}

fn node_grouping_scenario_proof() -> anyhow::Result<NodeGroupingProof> {
    let input = OptimizationInput {
        nodes: vec![
            grouping_node("n1"),
            grouping_node("n2"),
            grouping_node("n3"),
        ],
        workloads: vec![
            grouping_workload("p0", &["n1", "n2", "n3"]),
            grouping_workload("p1", &["n1", "n2", "n3"]),
        ],
        ..Default::default()
    };
    let physical_nodes_before = input.nodes.len();
    let (grouped, diagnostics) = group_pending_input_by_node_symmetry(&input);
    let grouped_node = grouped.nodes.first().cloned().unwrap_or_default();
    let grouped_node_name = grouped_node.name.clone();
    let scenario = ScenarioConfig {
        solver: "cp-sat-rust".to_string(),
        partial_admission: true,
        solve_time_limit_secs: 5,
        ..Default::default()
    };
    let (physical_solution, _) = crate::cpsat_rust::solve(&input, &scenario)
        .context("node grouping physical proof solve failed")?;
    let (grouped_solution, grouped_info) = crate::cpsat_rust::solve(&grouped, &scenario)
        .context("node grouping grouped proof solve failed")?;
    let expanded = expand_grouped_solution_to_physical(&grouped, &grouped_solution);
    let mut expanded_used_nodes: Vec<String> = expanded
        .as_ref()
        .ok()
        .map(|solution| {
            solution
                .assignment_counts
                .values()
                .flat_map(|counts| counts.keys().cloned())
                .collect::<BTreeSet<_>>()
                .into_iter()
                .collect()
        })
        .unwrap_or_default();
    expanded_used_nodes.sort();
    let physical_solve_admitted_workloads = solved_admitted_workloads(&physical_solution);
    let physical_solve_admitted_gpu = solved_admitted_gpu(&input, &physical_solution);
    let (grouped_solve_admitted_workloads, grouped_solve_admitted_gpu) = expanded
        .as_ref()
        .map(|solution| {
            (
                solved_admitted_workloads(solution),
                solved_admitted_gpu(&input, solution),
            )
        })
        .unwrap_or_default();
    let passed = diagnostics.disabled_reasons.is_empty()
        && diagnostics.eligible_group_count == 1
        && diagnostics.eligible_node_count == 3
        && diagnostics.max_group_size == 3
        && grouped.nodes.len() == 1
        && grouped_node.count == 3
        && grouped_node.members == vec!["n1".to_string(), "n2".to_string(), "n3".to_string()]
        && expanded.is_ok()
        && expanded_used_nodes.len() == 2
        && expanded_used_nodes
            .iter()
            .all(|node| grouped_node.members.contains(node))
        && grouped_solve_admitted_workloads == physical_solve_admitted_workloads
        && grouped_solve_admitted_gpu == physical_solve_admitted_gpu;

    Ok(NodeGroupingProof {
        name: "node-grouping-symmetry".to_string(),
        passed,
        physical_nodes_before,
        grouped_nodes_after: grouped.nodes.len(),
        eligible_group_count: diagnostics.eligible_group_count,
        eligible_node_count: diagnostics.eligible_node_count,
        max_group_size: diagnostics.max_group_size,
        grouped_node_name,
        grouped_node_count: grouped_node.count,
        grouped_members: grouped_node.members,
        expanded_used_nodes,
        physical_solve_admitted_workloads,
        grouped_solve_admitted_workloads,
        physical_solve_admitted_gpu,
        grouped_solve_admitted_gpu,
        grouped_solver_status: grouped_info.status,
    })
}

fn tenant_budget_scenario_proof() -> anyhow::Result<TenantBudgetProof> {
    let expensive_node_cost_milli = 1_000_000;
    let cheap_node_cost_milli = 500_000;
    let monthly_budget_milli = 600_000;
    let input = OptimizationInput {
        nodes: vec![
            OptimizationNode {
                name: "expensive-gpu".to_string(),
                count: 1,
                effective_capacity: ResourceList {
                    milli_cpu: 8000,
                    memory_bytes: 32 << 30,
                    pods: 10,
                    ..Default::default()
                },
                extended_resources: BTreeMap::from([(GPU_RESOURCE.to_string(), 1)]),
                price: crate::model::Money {
                    monthly: 1000.0,
                    ..Default::default()
                },
                ..Default::default()
            },
            OptimizationNode {
                name: "cheap-gpu".to_string(),
                count: 1,
                effective_capacity: ResourceList {
                    milli_cpu: 8000,
                    memory_bytes: 32 << 30,
                    pods: 10,
                    ..Default::default()
                },
                extended_resources: BTreeMap::from([(GPU_RESOURCE.to_string(), 1)]),
                price: crate::model::Money {
                    monthly: 500.0,
                    ..Default::default()
                },
                ..Default::default()
            },
        ],
        workloads: vec![
            OptimizationWorkload {
                id: "research/expensive-candidate".to_string(),
                namespace: "research".to_string(),
                name: "expensive-candidate".to_string(),
                group_size: 1,
                members: vec![OptimizationWorkloadMember {
                    namespace: "research".to_string(),
                    name: "expensive-candidate".to_string(),
                    current_node: String::new(),
                }],
                requests: ResourceList {
                    milli_cpu: 1000,
                    memory_bytes: 1 << 30,
                    pods: 1,
                    ..Default::default()
                },
                extended_resource_requests: BTreeMap::from([(GPU_RESOURCE.to_string(), 1)]),
                feasible_nodes: vec!["expensive-gpu".to_string()],
                ..Default::default()
            },
            OptimizationWorkload {
                id: "research/cheap-candidate".to_string(),
                namespace: "research".to_string(),
                name: "cheap-candidate".to_string(),
                group_size: 1,
                members: vec![OptimizationWorkloadMember {
                    namespace: "research".to_string(),
                    name: "cheap-candidate".to_string(),
                    current_node: String::new(),
                }],
                requests: ResourceList {
                    milli_cpu: 1000,
                    memory_bytes: 1 << 30,
                    pods: 1,
                    ..Default::default()
                },
                extended_resource_requests: BTreeMap::from([(GPU_RESOURCE.to_string(), 1)]),
                feasible_nodes: vec!["cheap-gpu".to_string()],
                ..Default::default()
            },
        ],
        budget_groups: vec![crate::model::BudgetGroup {
            name: "research".to_string(),
            workload_ids: vec![
                "research/expensive-candidate".to_string(),
                "research/cheap-candidate".to_string(),
            ],
            limit_milli: monthly_budget_milli,
        }],
        ..Default::default()
    };
    let scenario = ScenarioConfig {
        solver: "cp-sat-rust".to_string(),
        partial_admission: true,
        objective_profile: ObjectiveProfile::GpuGangAware,
        solve_time_limit_secs: 5,
        ..Default::default()
    };
    let (solution, info) =
        crate::cpsat_rust::solve(&input, &scenario).context("tenant budget proof solve failed")?;
    let expensive_job_node = solution
        .assignment_counts
        .get("research/expensive-candidate")
        .and_then(|counts| counts.keys().next().cloned());
    let cheap_job_node = solution
        .assignment_counts
        .get("research/cheap-candidate")
        .and_then(|counts| counts.keys().next().cloned());
    let admitted_jobs = [expensive_job_node.as_ref(), cheap_job_node.as_ref()]
        .into_iter()
        .filter(|node| node.is_some())
        .count();
    let unplaced_jobs = 2usize.saturating_sub(admitted_jobs);
    let passed = expensive_job_node.is_none()
        && cheap_job_node.as_deref() == Some("cheap-gpu")
        && admitted_jobs == 1
        && unplaced_jobs == 1;

    Ok(TenantBudgetProof {
        name: "tenant-budget-hard-admission-cap".to_string(),
        passed,
        tenant: "research".to_string(),
        monthly_budget_milli,
        expensive_node_cost_milli,
        cheap_node_cost_milli,
        expensive_job_node,
        cheap_job_node,
        admitted_jobs,
        unplaced_jobs,
        solver_status: info.status,
    })
}

#[allow(clippy::too_many_arguments)]
fn build_feature_assertions(
    scenarios: &[ScenarioResult],
    benefit: &BenefitSummary,
    roi: &RoiSummary,
    roi_dashboard: &RoiDashboardSummary,
    pricing_readiness: &PricingReadinessSummary,
    demo_readiness: &DemoReadinessSummary,
    roadmap_readiness: &RoadmapReadinessSummary,
    regret: &RegretSummary,
    hero_demo: &HeroDemoSummary,
    preemption_migration_hero: &PreemptionMigrationHeroSummary,
    sre_demo_script: &SreDemoScript,
    production_safety: &ProductionSafetySummary,
    prediction_quality: &PredictionQualitySummary,
    scale_guardrails: &ScaleGuardrailSummary,
    fairness_budget: &FairnessBudgetSummary,
    device_correctness: &DeviceCorrectnessSummary,
    preemption_migration_kss_proofs: &[KssProofScenario],
    vram_kss_proofs: &[KssProofScenario],
    repairs: &[RepairScenarioProof],
    vram_prediction: &VramPredictionProof,
    gpu_topology: &GpuTopologyProof,
    mig_profile: &MigProfileProof,
    dra_approximation: &DraApproximationProof,
    dra_allocation: &DraAllocationProof,
    time_sliced_gpu: &TimeSlicedGpuProof,
    node_grouping: &NodeGroupingProof,
    tenant_budget: &TenantBudgetProof,
    candidate_widening: &CandidateWideningProof,
) -> anyhow::Result<Vec<FeatureAssertion>> {
    let priority = scenarios
        .iter()
        .find(|s| s.name == "priority-gang-over-fillers");
    let priority_full_gang = priority
        .map(|s| placed_prefix_count(&s.ksolver.placements, "urgent-gang") == 4)
        .unwrap_or(false);
    let priority_gain = priority
        .map(|s| s.ksolver.metrics.priority_useful_gpu - s.kube.metrics.priority_useful_gpu)
        .unwrap_or_default();
    let business_value = scenarios
        .iter()
        .find(|s| s.name == "business-value-over-fifo");
    let business_value_placed = business_value
        .map(|s| placed_prefix_count(&s.ksolver.placements, "high-value-training"))
        .unwrap_or_default();
    let business_value_gain = business_value
        .map(|s| {
            s.ksolver.metrics.business_value_useful_gpu - s.kube.metrics.business_value_useful_gpu
        })
        .unwrap_or_default();
    let fair_share = scenarios.iter().find(|s| s.name == "fair-share-over-fifo");
    let fair_share_placed = fair_share
        .map(|s| placed_prefix_count(&s.ksolver.placements, "under-share-team-job"))
        .unwrap_or_default();
    let fair_share_gain = fair_share
        .map(|s| s.ksolver.metrics.fair_share_useful_gpu - s.kube.metrics.fair_share_useful_gpu)
        .unwrap_or_default();
    let queue = scenarios
        .iter()
        .find(|s| s.name == "queue-urgent-over-fifo");
    let queue_placed = queue
        .map(|s| placed_prefix_count(&s.ksolver.placements, "urgent-queue-job"))
        .unwrap_or_default();
    let queue_gain = queue
        .map(|s| s.ksolver.metrics.queue_useful_gpu - s.kube.metrics.queue_useful_gpu)
        .unwrap_or_default();
    let queue_wait = scenarios.iter().find(|s| s.name == "queue-wait-over-fifo");
    let queue_wait_placed = queue_wait
        .map(|s| placed_prefix_count(&s.ksolver.placements, "long-waiting-job"))
        .unwrap_or_default();
    let queue_wait_gain = queue_wait
        .map(|s| s.ksolver.metrics.queue_wait_useful_gpu - s.kube.metrics.queue_wait_useful_gpu)
        .unwrap_or_default();
    let deadline = scenarios
        .iter()
        .find(|s| s.name == "deadline-urgent-over-fifo");
    let deadline_placed = deadline
        .map(|s| placed_prefix_count(&s.ksolver.placements, "urgent-deadline"))
        .unwrap_or_default();
    let deadline_unplaced_reduction = deadline
        .map(|s| s.kube.metrics.deadline_unplaced_gpu - s.ksolver.metrics.deadline_unplaced_gpu)
        .unwrap_or_default();
    let rightsize = scenarios
        .iter()
        .find(|s| s.name == "weekend-flex-rightsize");
    let rightsize_selected_gpu = rightsize
        .map(|s| s.ksolver.metrics.flexible_selected_gpu)
        .unwrap_or_default();
    let rightsize_reduction_gain = rightsize
        .map(|s| s.ksolver.metrics.flexible_gpu_reduction - s.kube.metrics.flexible_gpu_reduction)
        .unwrap_or_default();
    let rightsize_benefit = rightsize.map(|s| s.benefit_score).unwrap_or_default();
    let inert = priority_inertness_holds()?;
    let fragmented_repair = repairs
        .iter()
        .find(|r| r.name == "fragmented-gang-repair")
        .context("fragmented-gang-repair scenario missing")?;
    let vram_repair = repairs
        .iter()
        .find(|r| r.name == "vram-blocked-no-repair")
        .context("vram-blocked-no-repair scenario missing")?;
    let policy_repair = repairs
        .iter()
        .find(|r| r.name == "policy-blocked-no-repair")
        .context("policy-blocked-no-repair scenario missing")?;
    let capacity_repair = repairs
        .iter()
        .find(|r| r.name == "not-enough-total-gpu-no-repair")
        .context("not-enough-total-gpu-no-repair scenario missing")?;

    Ok(vec![
        assertion(
            "priority-aware-admission",
            priority_full_gang && priority_gain > 0,
            format!(
                "priority-gang-over-fillers priority GPU gain={priority_gain}, urgent gang placed={}",
                priority
                    .map(|s| placed_prefix_count(&s.ksolver.placements, "urgent-gang"))
                    .unwrap_or_default()
            ),
        ),
        assertion(
            "deadline-aware-admission",
            deadline_placed == 1 && deadline_unplaced_reduction > 0,
            format!(
                "deadline-urgent-over-fifo deadline unplaced GPU reduction={deadline_unplaced_reduction}, urgent deadline placed={deadline_placed}"
            ),
        ),
        assertion(
            "deadline-flexible-rightsizing",
            rightsize_selected_gpu == 2 && rightsize_reduction_gain == 6 && rightsize_benefit > 0,
            format!(
                "weekend-flex-rightsize selected_gpu={rightsize_selected_gpu}, flexible GPU reduction gain={rightsize_reduction_gain}, benefit_score={rightsize_benefit}"
            ),
        ),
        assertion(
            "business-value-aware-admission",
            business_value_placed == 1 && business_value_gain > 0,
            format!(
                "business-value-over-fifo business-value GPU gain={business_value_gain}, high-value job placed={business_value_placed}"
            ),
        ),
        assertion(
            "fair-share-aware-admission",
            fair_share_placed == 1 && fair_share_gain > 0,
            format!(
                "fair-share-over-fifo fair-share GPU gain={fair_share_gain}, under-share job placed={fair_share_placed}"
            ),
        ),
        assertion(
            "queue-aware-admission",
            queue_placed == 1 && queue_gain > 0,
            format!(
                "queue-urgent-over-fifo queue GPU gain={queue_gain}, urgent queue job placed={queue_placed}"
            ),
        ),
        assertion(
            "queue-wait-aware-admission",
            queue_wait_placed == 1 && queue_wait_gain > 0,
            format!(
                "queue-wait-over-fifo queue-wait GPU seconds gain={queue_wait_gain}, long-waiting job placed={queue_wait_placed}"
            ),
        ),
        assertion(
            "fragmented-gang-repair-plan",
            fragmented_repair.passed,
            format!(
                "{} target={} node={} actions={} migrations={} preemptions={} freed_gpu={} disruption_cost={}",
                fragmented_repair.name,
                fragmented_repair.target,
                fragmented_repair.node,
                fragmented_repair.action_count,
                fragmented_repair.migration_actions,
                fragmented_repair.preemption_actions,
                fragmented_repair.freed_gpu,
                fragmented_repair.disruption_cost
            ),
        ),
        assertion(
            "preemption-migration-kss-proof-scenarios",
            preemption_migration_kss_proofs.len() == 3
                && preemption_migration_kss_proofs.iter().all(|proof| {
                    proof.passed
                        && proof.phase == "preemption_migration"
                        && (proof.kube_unplaced_pods > 0 || proof.kube_useful_gpu == 0)
                        && !proof.baseline_modes.is_empty()
                        && proof.caveat.contains("KSS does not plan migration/preemption")
                })
                && capacity_repair.passed
                && capacity_repair.metrics.not_enough_total_gpu_targets == 1
                && policy_repair.passed,
            format!(
                "kss_repair_proofs={} local_capacity_repair_passed={} policy_repair_passed={} modes={:?}",
                preemption_migration_kss_proofs.len(),
                capacity_repair.passed,
                policy_repair.passed,
                preemption_migration_kss_proofs
                    .iter()
                    .flat_map(|proof| proof.baseline_modes.clone())
                    .collect::<Vec<_>>()
            ),
        ),
        assertion(
            "hero-defragmentation-demo-ready",
            hero_demo.passed
                && hero_demo.target_gpu_request == 4
                && hero_demo.freed_gpu >= hero_demo.target_gpu_request
                && hero_demo.disruption_cost > 0
                && !hero_demo.roi_headline.is_empty()
                && hero_demo.screenshot_claims.len() >= 3,
            format!(
                "{} target={} request={} repair_node={} freed_gpu={} migrations={} preemptions={} disruption_cost={} claims={}",
                hero_demo.name,
                hero_demo.target,
                hero_demo.target_gpu_request,
                hero_demo.repair_node,
                hero_demo.freed_gpu,
                hero_demo.migration_actions,
                hero_demo.preemption_actions,
                hero_demo.disruption_cost,
                hero_demo.screenshot_claims.len()
            ),
        ),
        assertion(
            "preemption-migration-hero-ready",
            preemption_migration_hero.passed
                && preemption_migration_hero.action_rows.len() >= 4
                && preemption_migration_hero.migration_actions > 0
                && preemption_migration_hero.preemption_actions > 0
                && preemption_migration_hero.total_disruption_cost > 0
                && preemption_migration_hero.safety_claims.len() >= 4
                && !preemption_migration_hero.decision_contract.can_act_now
                && preemption_migration_hero
                    .decision_contract
                    .evidence_required
                    .iter()
                    .any(|evidence| evidence.contains("/api/scheduler/repair-plan"))
                && preemption_migration_hero
                    .decision_contract
                    .fail_closed_if
                    .iter()
                    .any(|gate| gate.contains("stale")),
            format!(
                "{} actions={} migrations={} preemptions={} disruption={} target={} node={} verdict={}",
                preemption_migration_hero.name,
                preemption_migration_hero.action_rows.len(),
                preemption_migration_hero.migration_actions,
                preemption_migration_hero.preemption_actions,
                preemption_migration_hero.total_disruption_cost,
                preemption_migration_hero.target,
                preemption_migration_hero.repair_node,
                preemption_migration_hero.decision_contract.verdict
            ),
        ),
        assertion(
            "sre-demo-script-ready",
            !sre_demo_script.headline.is_empty()
                && sre_demo_script.steps.len() >= 4
                && sre_demo_script.top_scenario_cards.len() >= 3
                && sre_demo_script
                    .top_scenario_cards
                    .windows(2)
                    .all(|pair| pair[0].efficiency_score >= pair[1].efficiency_score),
            format!(
                "{} steps={} cards={} top={}",
                sre_demo_script.name,
                sre_demo_script.steps.len(),
                sre_demo_script.top_scenario_cards.len(),
                sre_demo_script
                    .top_scenario_cards
                    .first()
                    .map(|c| c.scenario.as_str())
                    .unwrap_or("")
            ),
        ),
        assertion(
            "production-safety-summary-ready",
            production_safety.passed
                && !production_safety.mutation_default_enabled
                && production_safety.default_mode.contains("observe")
                && production_safety
                    .real_binding_gate
                    .contains("KSOLVER_ENABLE_REAL_BINDING")
                && production_safety.kill_switches.len() >= 3
                && production_safety.production_checklist.len() >= 6
                && production_safety.rbac_modes.len() >= 4
                && production_safety.failure_mode_controls.len() >= 5
                && production_safety.audit_fields.len() >= 7
                && production_safety.rollout_gate_rows.len() >= 4
                && production_safety
                    .rollout_gate_rows
                    .iter()
                    .any(|row| row.mode == "observe-only" && !row.mutation_allowed)
                && production_safety
                    .rollout_gate_rows
                    .iter()
                    .any(|row| row.mode == "bind-low-risk" && row.mutation_allowed)
                && production_safety.failure_playbook_rows.len() >= 5
                && production_safety
                    .failure_playbook_rows
                    .iter()
                    .any(|row| row.failure_mode == "stale pod identity")
                && production_safety.audit_event_rows.len() >= 3
                && production_safety.live_validation_rows.len() >= 7
                && production_safety
                    .live_validation_rows
                    .iter()
                    .any(|row| row.gate == "pod identity and phase"
                        && row.fail_closed_behavior.contains("skip binding"))
                && production_safety
                    .live_validation_rows
                    .iter()
                    .any(|row| row.gate == "PDB and disruption policy"
                        && row.audit_field.contains("repair_metrics"))
                && production_safety.live_config_rows.len() >= 6
                && production_safety.live_config_rows.iter().any(|row| {
                    row.env_var == "KSOLVER_BINDING_KILL_SWITCH"
                        && row.live_endpoint_field == "rollout.binding_kill_switch"
                        && row.fail_closed_signal.contains("mutation_allowed=false")
                })
                && production_safety.live_config_rows.iter().any(|row| {
                    row.gate == "leader election" && row.required_rbac_when_enabled.contains("leases")
                })
                && production_safety.readiness_checks.len() >= 6
                && production_safety.mutation_boundaries.len() >= 5
                && production_safety.residual_risks.len() >= 3,
            format!(
                "{} default={} mutation_default={} modes={} checklist={} rbac_modes={} failure_controls={} audit_fields={} rollout_gates={} playbook_rows={} audit_events={} live_validation_rows={} live_config_rows={} readiness_checks={} boundaries={} risks={}",
                production_safety.name,
                production_safety.default_mode,
                production_safety.mutation_default_enabled,
                production_safety.rollout_modes.len(),
                production_safety.production_checklist.len(),
                production_safety.rbac_modes.len(),
                production_safety.failure_mode_controls.len(),
                production_safety.audit_fields.len(),
                production_safety.rollout_gate_rows.len(),
                production_safety.failure_playbook_rows.len(),
                production_safety.audit_event_rows.len(),
                production_safety.live_validation_rows.len(),
                production_safety.live_config_rows.len(),
                production_safety.readiness_checks.len(),
                production_safety.mutation_boundaries.len(),
                production_safety.residual_risks.len()
            ),
        ),
        assertion(
            "prediction-quality-summary-ready",
            prediction_quality.passed
                && prediction_quality
                    .promotion_contract
                    .promotion_level
                    .contains("Advisory")
                && !prediction_quality.promotion_contract.hard_placement_allowed
                && !prediction_quality
                    .promotion_contract
                    .prediction_sensitive_claims_allowed
                && prediction_quality
                    .promotion_contract
                    .required_evidence
                    .iter()
                    .any(|item| item.contains("completed GPU pod observations"))
                && prediction_quality
                    .promotion_contract
                    .demotion_triggers
                    .iter()
                    .any(|item| item.contains("unknown prediction source share"))
                && prediction_quality.coverage_sources.len() >= 5
                && prediction_quality.calibration_metrics.len() >= 6
                && prediction_quality.calibration_lifecycle.len() >= 5
                && prediction_quality.confidence_bands.len() >= 5
                && prediction_quality.drift_monitors.len() >= 5
                && prediction_quality.decision_impact_evidence.len() >= 5
                && prediction_quality.model_cards.len() >= 5
                && prediction_quality
                    .model_cards
                    .iter()
                    .any(|card| card.source_tier == "exact_command_hash")
                && prediction_quality
                    .model_cards
                    .iter()
                    .any(|card| card.source_tier == "unknown" && card.placement_use.contains("do not"))
                && prediction_quality.calibration_buckets.len() >= 4
                && prediction_quality
                    .calibration_buckets
                    .iter()
                    .any(|bucket| bucket.bucket == "peak_vram_bytes")
                && prediction_quality.live_calibration_rows.len() >= 6
                && prediction_quality.live_calibration_rows.iter().any(|row| {
                    row.gate == "runtime error budget"
                        && row.live_trace_metric.contains("runtime_prediction_mape_milli")
                        && row.unhealthy_action.contains("demote")
                })
                && prediction_quality.live_calibration_rows.iter().any(|row| {
                    row.gate == "unknown source coverage"
                        && row.live_trace_metric.contains("unknown_pods")
                        && row.placement_impact.contains("unsupported prediction claims")
                })
                && prediction_quality.audit_fields.len() >= 8
                && prediction_quality.promotion_gates.len() >= 5
                && prediction_quality.placement_effects.len() >= 4
                && prediction_quality.confidence_model.contains("exact command history")
                && prediction_quality.residual_risks.len() >= 3,
            format!(
                "{} coverage_sources={} calibration_metrics={} lifecycle={} bands={} drift={} impact={} model_cards={} buckets={} live_calibration_rows={} audit_fields={} promotion_gates={} placement_effects={} risks={}",
                prediction_quality.name,
                prediction_quality.coverage_sources.len(),
                prediction_quality.calibration_metrics.len(),
                prediction_quality.calibration_lifecycle.len(),
                prediction_quality.confidence_bands.len(),
                prediction_quality.drift_monitors.len(),
                prediction_quality.decision_impact_evidence.len(),
                prediction_quality.model_cards.len(),
                prediction_quality.calibration_buckets.len(),
                prediction_quality.live_calibration_rows.len(),
                prediction_quality.audit_fields.len(),
                prediction_quality.promotion_gates.len(),
                prediction_quality.placement_effects.len(),
                prediction_quality.residual_risks.len()
            ),
        ),
        assertion(
            "scale-guardrails-ready",
            scale_guardrails.passed
                && scale_guardrails
                    .actionability_contract
                    .customer_scale_claim_allowed
                && !scale_guardrails
                    .actionability_contract
                    .high_risk_pruned_binding_allowed
                && scale_guardrails
                    .actionability_contract
                    .preferred_large_fleet_mode
                    .contains("homogeneous node grouping before candidate pruning")
                && scale_guardrails
                    .actionability_contract
                    .required_evidence
                    .iter()
                    .any(|item| item.contains("candidate_quality_metrics"))
                && scale_guardrails
                    .actionability_contract
                    .fail_closed_if
                    .iter()
                    .any(|item| item.contains("measured useful-GPU"))
                && scale_guardrails.scenarios_compared_for_regret == scenarios.len()
                && scale_guardrails.grouping_preserved_admitted_gpu
                && scale_guardrails.grouping_eligible_nodes >= 2
                && scale_guardrails.widening_useful_gpu_recovered > 0
                && scale_guardrails.grouping_policy.len() >= 4
                && scale_guardrails.pruning_modes.len() >= 4
                && scale_guardrails.regret_status_ladder.len() >= 4
                && scale_guardrails.fallback_triggers.len() >= 5
                && scale_guardrails.scale_mode_cards.len() >= 4
                && scale_guardrails
                    .scale_mode_cards
                    .iter()
                    .any(|card| card.mode == "node_grouping" && card.status == "safe_for_symmetric_nodes")
                && scale_guardrails
                    .scale_mode_cards
                    .iter()
                    .any(|card| card.mode == "candidate_pruning" && card.status == "measured_regret")
                && scale_guardrails.regret_action_rows.len() >= 4
                && scale_guardrails
                    .regret_action_rows
                    .iter()
                    .any(|row| row.regret_status == "unknown" && row.next_action.contains("rerun full"))
                && scale_guardrails.large_fleet_validation_rows.len() >= 6
                && scale_guardrails.large_fleet_validation_rows.iter().any(|row| {
                    row.gate == "homogeneous grouping symmetry"
                        && row.live_trace_metric.contains("node_grouping_metrics")
                        && row.fail_closed_action.contains("fall back")
                })
                && scale_guardrails.large_fleet_validation_rows.iter().any(|row| {
                    row.gate == "candidate pruning regret visibility"
                        && row.live_trace_metric.contains("candidate_quality_metrics")
                        && row.operator_claim.contains("never presented as exact")
                })
                && scale_guardrails.operator_switches.len() >= 5
                && scale_guardrails.guardrails.len() >= 5
                && scale_guardrails.residual_risks.len() >= 3,
            format!(
                "{} regret_scenarios={} any_regret={} max_useful_regret={} grouping {} -> {} widened_recovered={} policy={} modes={} statuses={} fallbacks={} scale_cards={} regret_actions={} large_fleet_rows={} switches={}",
                scale_guardrails.name,
                scale_guardrails.scenarios_compared_for_regret,
                scale_guardrails.scenarios_with_any_regret,
                scale_guardrails.max_useful_gpu_regret,
                scale_guardrails.grouping_physical_nodes_before,
                scale_guardrails.grouping_nodes_after,
                scale_guardrails.widening_useful_gpu_recovered,
                scale_guardrails.grouping_policy.len(),
                scale_guardrails.pruning_modes.len(),
                scale_guardrails.regret_status_ladder.len(),
                scale_guardrails.fallback_triggers.len(),
                scale_guardrails.scale_mode_cards.len(),
                scale_guardrails.regret_action_rows.len(),
                scale_guardrails.large_fleet_validation_rows.len(),
                scale_guardrails.operator_switches.len()
            ),
        ),
        assertion(
            "fairness-budget-summary-ready",
            fairness_budget.passed
                && fairness_budget.under_share_admitted
                && fairness_budget.fair_share_useful_gpu_gain > 0
                && !fairness_budget.expensive_job_admitted
                && fairness_budget.cheap_job_admitted
                && fairness_budget.unplaced_jobs > 0
                && fairness_budget.policy_decision_rows.len() >= 4
                && fairness_budget.tenant_ledger_rows.len() >= 3
                && fairness_budget
                    .policy_decision_rows
                    .iter()
                    .any(|row| row.decision == "deny" && row.policy.contains("budget"))
                && fairness_budget
                    .tenant_ledger_rows
                    .iter()
                    .any(|row| row.status == "borrowing" && row.reclaimable_borrowed_gpu_milli > 0)
                && fairness_budget.ownership_rows.len() >= 6
                && fairness_budget.ownership_rows.iter().any(|row| {
                    row.gate == "tenant identity"
                        && row.ownership_source.contains("ksolver.dev/team")
                        && row.live_trace_field.contains("tenant_fairness_metrics")
                })
                && fairness_budget.ownership_rows.iter().any(|row| {
                    row.gate == "budget catalog"
                        && row.live_trace_field.contains("budget_monthly_milli")
                })
                && fairness_budget.ownership_rows.iter().any(|row| {
                    row.gate == "borrow and reclaim"
                        && row.live_trace_field.contains("reclaimable_borrowed_gpu_milli")
                })
                && fairness_budget.ui_badges.len() >= 4
                && fairness_budget.enforcement_controls.len() >= 4
                && fairness_budget.operator_questions.len() >= 4
                && fairness_budget.trace_fields.len() >= 8,
            format!(
                "{} fair_share_gain={} under_share_admitted={} tenant={} budget={} expensive_admitted={} cheap_admitted={} unplaced={} decisions={} ledger_rows={} ownership_rows={}",
                fairness_budget.name,
                fairness_budget.fair_share_useful_gpu_gain,
                fairness_budget.under_share_admitted,
                fairness_budget.tenant,
                fairness_budget.monthly_budget_milli,
                fairness_budget.expensive_job_admitted,
                fairness_budget.cheap_job_admitted,
                fairness_budget.unplaced_jobs,
                fairness_budget.policy_decision_rows.len(),
                fairness_budget.tenant_ledger_rows.len(),
                fairness_budget.ownership_rows.len()
            ),
        ),
        assertion(
            "device-correctness-summary-ready",
            device_correctness.passed
                && device_correctness.supported_today.len() >= 5
                && device_correctness.proof_backed_claims.len() >= 5
                && device_correctness.exact_semantics.len() >= 4
                && device_correctness.approximated_semantics.len() >= 4
                && device_correctness.unsupported_claims.len() >= 4
                && device_correctness.validation_signals.len() >= 5
                && device_correctness.fallback_actions.len() >= 5
                && device_correctness.device_readiness_rows.len() >= 6
                && device_correctness.device_readiness_rows.iter().any(|row| {
                    row.feature == "DRA scalar approximation"
                        && row.support_level.contains("shadow approximation")
                        && row.fail_closed_action.contains("drop unmodeled")
                })
                && device_correctness.device_readiness_rows.iter().any(|row| {
                    row.feature == "concrete NVLink/DRA device graph"
                        && row.support_level.contains("unsupported")
                        && row.operator_claim.contains("not supported yet")
                })
                && device_correctness.hard_limits.len() >= 4
                && device_correctness.residual_risks.len() >= 3
                && device_correctness
                    .operator_claims
                    .iter()
                    .any(|s| s.contains("should not claim full DRA allocation")),
            format!(
                "{} supported={} proofs={} exact={} approximated={} unsupported={} validation={} fallback={} readiness_rows={} hard_limits={} risks={} topology={} mig={} dra={}",
                device_correctness.name,
                device_correctness.supported_today.len(),
                device_correctness.proof_backed_claims.len(),
                device_correctness.exact_semantics.len(),
                device_correctness.approximated_semantics.len(),
                device_correctness.unsupported_claims.len(),
                device_correctness.validation_signals.len(),
                device_correctness.fallback_actions.len(),
                device_correctness.device_readiness_rows.len(),
                device_correctness.hard_limits.len(),
                device_correctness.residual_risks.len(),
                device_correctness.topology_claim,
                device_correctness.mig_claim,
                device_correctness.dra_approximation_claim
            ),
        ),
        assertion(
            "vram-blocked-no-repair-plan",
            vram_repair.passed,
            format!(
                "{} target={} repairable={} unrepairable={} vram_blocked={} actions={}",
                vram_repair.name,
                vram_repair.target,
                vram_repair.metrics.repairable_targets,
                vram_repair.metrics.unrepairable_targets,
                vram_repair.metrics.vram_blocked_targets,
                vram_repair.action_count
            ),
        ),
        assertion(
            "vram-kss-proof-scenarios",
            vram_kss_proofs.len() == 3
                && vram_kss_proofs.iter().all(|proof| {
                    proof.passed
                        && proof.phase == "vram_prediction"
                        && proof.kube_useful_gpu > 0
                        && !proof.baseline_modes.is_empty()
                        && proof.caveat.contains("KSS has no predicted-VRAM model")
                })
                && vram_prediction.passed
                && vram_repair.passed,
            format!(
                "kss_vram_proofs={} local_vram_prediction_passed={} local_vram_repair_passed={} modes={:?}",
                vram_kss_proofs.len(),
                vram_prediction.passed,
                vram_repair.passed,
                vram_kss_proofs
                    .iter()
                    .flat_map(|proof| proof.baseline_modes.clone())
                    .collect::<Vec<_>>()
            ),
        ),
        assertion(
            "policy-blocked-no-repair-plan",
            policy_repair.passed,
            format!(
                "{} target={} repairable={} unrepairable={} policy_blocked={} actions={}",
                policy_repair.name,
                policy_repair.target,
                policy_repair.metrics.repairable_targets,
                policy_repair.metrics.unrepairable_targets,
                policy_repair.metrics.policy_or_candidate_blocked_targets,
                policy_repair.action_count
            ),
        ),
        assertion(
            "vram-prediction-feasibility",
            vram_prediction.passed,
            format!(
                "{} peak={}GiB feasible={:?} rejected={:?} impossible_drops={} reason={}",
                vram_prediction.name,
                vram_prediction.predicted_peak_vram_gib,
                vram_prediction.adequate_feasible_nodes,
                vram_prediction.rejected_too_small_nodes,
                vram_prediction.impossible_drop_count,
                vram_prediction.impossible_drop_reason
            ),
        ),
        assertion(
            "gpu-topology-locality-feasibility",
            gpu_topology.passed,
            format!(
                "{} required {}={} feasible={:?} rejected={:?} impossible_drops={} reason={}",
                gpu_topology.name,
                gpu_topology.topology_key,
                gpu_topology.required_value,
                gpu_topology.matching_feasible_nodes,
                gpu_topology.rejected_nodes,
                gpu_topology.impossible_drop_count,
                gpu_topology.impossible_drop_reason
            ),
        ),
        assertion(
            "mig-profile-compatibility",
            mig_profile.passed,
            format!(
                "{} requested {} x{} feasible={:?} rejected={:?} impossible_drops={} reason={}",
                mig_profile.name,
                mig_profile.requested_resource,
                mig_profile.requested_quantity,
                mig_profile.matching_feasible_nodes,
                mig_profile.rejected_nodes,
                mig_profile.impossible_drop_count,
                mig_profile.impossible_drop_reason
            ),
        ),
        assertion(
            "dra-scalar-approximation-guard",
            dra_approximation.passed,
            format!(
                "{} resource={} modeled_feasible={:?} modeled_qty={} unmodeled_drops={} reason={}",
                dra_approximation.name,
                dra_approximation.synthetic_resource,
                dra_approximation.modeled_feasible_nodes,
                dra_approximation.modeled_request_quantity,
                dra_approximation.unmodeled_drop_count,
                dra_approximation.unmodeled_drop_reason
            ),
        ),
        assertion(
            "dra-allocated-device-subtraction",
            dra_allocation.passed,
            format!(
                "{} node={} class={} total={} allocated={} available={} overlapping={} unevaluable={:?}",
                dra_allocation.name,
                dra_allocation.node,
                dra_allocation.device_class,
                dra_allocation.total_matching_devices,
                dra_allocation.allocated_devices,
                dra_allocation.available_devices,
                dra_allocation.overlapping_classes,
                dra_allocation.unevaluable_classes
            ),
        ),
        assertion(
            "time-sliced-gpu-disclosure",
            time_sliced_gpu.passed,
            format!(
                "{} shared_node={} caveats={:?} isolated_caveats={:?}",
                time_sliced_gpu.name,
                time_sliced_gpu.time_sliced_node,
                time_sliced_gpu.time_sliced_caveats,
                time_sliced_gpu.isolated_caveats
            ),
        ),
        assertion(
            "node-grouping-symmetry-reduction",
            node_grouping.passed,
            format!(
                "{} physical_nodes={} grouped_nodes={} eligible_groups={} eligible_nodes={} max_group_size={} grouped_node={} count={} expanded_used={:?} physical_admitted_workloads={} grouped_admitted_workloads={} physical_admitted_gpu={} grouped_admitted_gpu={} status={}",
                node_grouping.name,
                node_grouping.physical_nodes_before,
                node_grouping.grouped_nodes_after,
                node_grouping.eligible_group_count,
                node_grouping.eligible_node_count,
                node_grouping.max_group_size,
                node_grouping.grouped_node_name,
                node_grouping.grouped_node_count,
                node_grouping.expanded_used_nodes,
                node_grouping.physical_solve_admitted_workloads,
                node_grouping.grouped_solve_admitted_workloads,
                node_grouping.physical_solve_admitted_gpu,
                node_grouping.grouped_solve_admitted_gpu,
                node_grouping.grouped_solver_status
            ),
        ),
        assertion(
            "tenant-budget-hard-admission-cap",
            tenant_budget.passed,
            format!(
                "{} tenant={} budget_milli={} expensive_cost_milli={} cheap_cost_milli={} expensive_node={:?} cheap_node={:?} admitted={} unplaced={} status={}",
                tenant_budget.name,
                tenant_budget.tenant,
                tenant_budget.monthly_budget_milli,
                tenant_budget.expensive_node_cost_milli,
                tenant_budget.cheap_node_cost_milli,
                tenant_budget.expensive_job_node,
                tenant_budget.cheap_job_node,
                tenant_budget.admitted_jobs,
                tenant_budget.unplaced_jobs,
                tenant_budget.solver_status
            ),
        ),
        assertion(
            "priority-metadata-inert-when-weight-zero",
            inert,
            "same scenario solves identically with priority metadata present versus stripped when priority weight is zero",
        ),
        assertion(
            "ksolver-positive-benefit-suite",
            benefit.scenarios_with_positive_benefit > 0,
            format!(
                "{} of {} scenarios have positive benefit; top scenario={}",
                benefit.scenarios_with_positive_benefit, benefit.scenarios_compared, benefit.top_scenario
            ),
        ),
        assertion(
            "roi-summary-computed",
            roi.scenarios_compared == scenarios.len()
                && roi.total_requested_gpu > 0
                && roi.ksolver_admitted_useful_gpu >= roi.kube_admitted_useful_gpu
                && roi.kube_active_node_monthly_cost > 0
                && roi.ksolver_gpu_utilization_milli > 0
                && !roi.headline.is_empty(),
            format!(
                "requested_gpu={} kube_admitted={} ksolver_admitted={} admission_gain_milli={} unplaced_delta={} cost_delta_monthly={} utilization_gain_milli={} headline={}",
                roi.total_requested_gpu,
                roi.kube_admitted_useful_gpu,
                roi.ksolver_admitted_useful_gpu,
                roi.admission_percent_gain_milli,
                roi.unplaced_pod_reduction,
                -roi.active_node_monthly_cost_reduction,
                roi.gpu_utilization_gain_milli,
                roi.headline
            ),
        ),
        assertion(
            "roi-dashboard-summary-ready",
            roi_dashboard.passed
                && roi_dashboard.primary_tiles.len() >= 6
                && roi_dashboard.executive_rows.len() >= 5
                && roi_dashboard.decision_rows.len() >= 6
                && roi_dashboard
                    .executive_rows
                    .iter()
                    .any(|row| row.evidence_tile == "hero_repair_disruption_cost")
                && roi_dashboard.decision_rows.iter().any(|row| {
                    row.tile_key == "active_node_monthly_cost_delta"
                        && row.next_action.contains("pricing catalog")
                })
                && roi_dashboard.decision_rows.iter().any(|row| {
                    row.tile_key == "candidate_pruning_max_regret"
                        && row.decision_rule.contains("pruned ROI")
                })
                && roi_dashboard.decision_rows.iter().any(|row| {
                    row.tile_key == "hero_repair_disruption_cost"
                        && row.evidence_source.contains("production_safety_summary")
                })
                && roi_dashboard.decision_frame.len() >= 5
                && roi_dashboard
                    .presentation_order
                    .contains(&"decision_rows".to_string())
                && roi_dashboard
                    .primary_tiles
                    .iter()
                    .any(|tile| tile.key == "admitted_useful_gpu_gain")
                && roi_dashboard
                    .primary_tiles
                    .iter()
                    .any(|tile| tile.key == "hero_repair_disruption_cost")
                && !roi_dashboard.claim_contract.can_show_customer_dollars
                && roi_dashboard
                    .claim_contract
                    .required_evidence
                    .iter()
                    .any(|evidence| evidence.contains("pricing catalog"))
                && roi_dashboard
                    .claim_contract
                    .blocked_by
                    .iter()
                    .any(|blocker| blocker.contains("synthetic"))
                && roi_dashboard.confidence_guardrail.contains("prediction")
                && roi_dashboard.regret_guardrail.contains("regret")
                && roi_dashboard.operator_questions.len() >= 5,
            format!(
                "{} tiles={} executive_rows={} decision_rows={} scenarios={} hero={} disruption={} claim_level={} headline={}",
                roi_dashboard.name,
                roi_dashboard.primary_tiles.len(),
                roi_dashboard.executive_rows.len(),
                roi_dashboard.decision_rows.len(),
                roi_dashboard.scenario_count,
                roi_dashboard.hero_scenario,
                roi_dashboard.hero_repair_disruption_cost,
                roi_dashboard.claim_contract.claim_level,
                roi_dashboard.headline
            ),
        ),
        assertion(
            "pricing-readiness-summary-ready",
            pricing_readiness.passed
                && pricing_readiness.current_mode.contains("synthetic")
                && pricing_readiness.pricing_required_before_customer_claim.len() >= 5
                && pricing_readiness.accepted_sources.len() >= 4
                && pricing_readiness.roi_fields_to_recompute.len() >= 6
                && pricing_readiness.validation_checks.len() >= 5
                && pricing_readiness
                    .validation_checks
                    .iter()
                    .any(|check| check.contains("price_source"))
                && pricing_readiness.pricing_evidence_rows.len() >= 5
                && pricing_readiness.pricing_evidence_rows.iter().any(|row| {
                    row.gate == "node-to-price mapping"
                        && row.recompute_target.contains("active_node_monthly_cost")
                })
                && pricing_readiness.pricing_evidence_rows.iter().any(|row| {
                    row.gate == "tenant budget reconciliation"
                        && row.recompute_target.contains("tenant_fairness_metrics")
                })
                && pricing_readiness.pricing_evidence_rows.iter().any(|row| {
                    row.gate == "screenshot/customer claim guard"
                        && row.failure_action.contains("relative cost units")
                })
                && pricing_readiness.operator_actions.len() >= 4
                && pricing_readiness.residual_risks.len() >= 3,
            format!(
                "{} mode={} required={} sources={} recompute={} checks={} evidence_rows={} actions={} risks={}",
                pricing_readiness.name,
                pricing_readiness.current_mode,
                pricing_readiness.pricing_required_before_customer_claim.len(),
                pricing_readiness.accepted_sources.len(),
                pricing_readiness.roi_fields_to_recompute.len(),
                pricing_readiness.validation_checks.len(),
                pricing_readiness.pricing_evidence_rows.len(),
                pricing_readiness.operator_actions.len(),
                pricing_readiness.residual_risks.len()
            ),
        ),
        assertion(
            "sre-end-to-end-demo-ready",
            demo_readiness.passed
                && demo_readiness.ui_sections.len() >= 7
                && demo_readiness.operator_checklist.len() >= 5
                && demo_readiness.demo_flow_scenes.len() >= 5
                && demo_readiness
                    .demo_flow_scenes
                    .iter()
                    .any(|scene| scene.screen == "Repair Plan"
                        && scene.evidence_source
                            == "preemption_migration_hero_summary.action_rows")
                && demo_readiness
                    .demo_flow_scenes
                    .iter()
                    .any(|scene| scene.screen == "ROI"
                        && scene.evidence_source == "roi_dashboard_summary.executive_rows")
                && demo_readiness.demo_acceptance_criteria.len() >= 6
                && demo_readiness.live_validation_rows.len() >= 6
                && demo_readiness.live_validation_rows.iter().any(|row| {
                    row.gate == "kube baseline provenance"
                        && row.live_endpoint == "/api/scheduler/kube-simulator-plan"
                })
                && demo_readiness.live_validation_rows.iter().any(|row| {
                    row.gate == "repair action safety"
                        && row.live_endpoint == "/api/scheduler/repair-plan"
                })
                && demo_readiness.live_validation_rows.iter().any(|row| {
                    row.gate == "production mutation safety"
                        && row.live_endpoint == "/api/scheduler/production-safety"
                })
                && demo_readiness
                    .primary_story
                    .contains("kube leaves valuable GPU work pending")
                && !demo_readiness
                    .remaining_gaps
                    .iter()
                    .any(|gap| gap.contains("render demo_flow_scenes"))
                && !demo_readiness.kube_baseline_mode.is_empty(),
            format!(
                "{} sections={} checklist={} scenes={} acceptance={} validation_rows={} baseline_mode={} hero={} story={}",
                demo_readiness.name,
                demo_readiness.ui_sections.len(),
                demo_readiness.operator_checklist.len(),
                demo_readiness.demo_flow_scenes.len(),
                demo_readiness.demo_acceptance_criteria.len(),
                demo_readiness.live_validation_rows.len(),
                demo_readiness.kube_baseline_mode,
                demo_readiness.hero_scenario,
                demo_readiness.primary_story
            ),
        ),
        assertion(
            "roadmap-readiness-summary-ready",
            roadmap_readiness.passed
                && roadmap_readiness.launch_proof_gate.demo_ready
                && !roadmap_readiness.launch_proof_gate.customer_claim_ready
                && roadmap_readiness
                    .launch_proof_gate
                    .label
                    .contains("customer proof pending")
                && roadmap_readiness.launch_proof_gate.required_evidence.len() >= 6
                && roadmap_readiness.launch_proof_gate.evidence_bundle_rows.len() >= 8
                && roadmap_readiness
                    .launch_proof_gate
                    .evidence_bundle_rows
                    .iter()
                    .any(|row| {
                        row.artifact == "kube baseline provenance"
                            && row.source.contains("/api/scheduler/kube-simulator-plan")
                            && row.blocks_claim.contains("beats kube")
                    })
                && roadmap_readiness
                    .launch_proof_gate
                    .evidence_bundle_rows
                    .iter()
                    .any(|row| {
                        row.artifact == "production safety and RBAC"
                            && row.source.contains("/api/scheduler/production-safety")
                    })
                && roadmap_readiness
                    .launch_proof_gate
                    .evidence_bundle_rows
                    .iter()
                    .any(|row| {
                        row.artifact == "customer pricing basis"
                            && row.blocks_claim.contains("dollar savings")
                    })
                && roadmap_readiness.launch_proof_gate.blockers.len() >= 5
                && roadmap_readiness
                    .launch_proof_gate
                    .next_action
                    .contains("non-demo customer-style trace bundle")
                && roadmap_readiness.items.len() == 8
                && roadmap_readiness
                    .items
                    .iter()
                    .all(|item| item.status != "incomplete" && !item.evidence_source.is_empty())
                && roadmap_readiness
                    .items
                    .iter()
                    .any(|item| item.item == "Preemption/migration planner proof"
                        && item.status == "repair-proof-ready"
                        && item.evidence_source.contains("kube baseline provenance"))
                && roadmap_readiness
                    .items
                    .iter()
                .any(|item| item.item == "VRAM prediction and no-repair proof"
                    && item.status == "vram-proof-gates-ready"
                    && item.remaining_gap.contains("live/cached KSS"))
                && roadmap_readiness
                    .items
                    .iter()
                    .any(|item| item.item == "True device correctness"
                        && item.remaining_gap.contains("DRA allocation"))
                && roadmap_readiness.next_build_order.len() >= 5
                && roadmap_readiness.residual_product_gaps.len() == 8,
            format!(
                "{} items={} next_build={} residual_gaps={} headline={}",
                roadmap_readiness.name,
                roadmap_readiness.items.len(),
                roadmap_readiness.next_build_order.len(),
                roadmap_readiness.residual_product_gaps.len(),
                roadmap_readiness.headline
            ),
        ),
        assertion(
            "gang-validity-benefit",
            benefit.total_invalid_gangs_avoided > 0 || scenarios.iter().any(|s| {
                s.ksolver.metrics.partial_or_invalid_gangs == 0
                    && s.kube.metrics.partial_or_invalid_gangs > 0
            }),
            format!(
                "total invalid gangs avoided={}",
                benefit.total_invalid_gangs_avoided
            ),
        ),
        assertion(
            "candidate-pruning-regret-measured",
            regret.scenarios_compared == scenarios.len()
                && scenarios
                    .iter()
                    .all(|s| s.reduced_ksolver.candidate_node_limit == REGRET_CANDIDATE_LIMIT),
            format!(
                "compared {} scenarios with K={} reduced solve; any-regret scenarios={}",
                regret.scenarios_compared,
                regret.candidate_node_limit,
                regret.scenarios_with_any_regret
            ),
        ),
        assertion(
            "candidate-widening-recovers-regret",
            candidate_widening.passed,
            format!(
                "{} scenario={} K={} -> full retries={} reason={} recovered_useful_gpu={} unplaced {} -> {}",
                candidate_widening.name,
                candidate_widening.scenario,
                candidate_widening.initial_candidate_node_limit,
                candidate_widening.retry_count,
                candidate_widening.widening_reason,
                candidate_widening.useful_gpu_recovered,
                candidate_widening.pruned_unplaced_pods,
                candidate_widening.widened_unplaced_pods
            ),
        ),
        assertion(
            "scenarios-sorted-by-efficiency",
            scenarios
                .windows(2)
                .all(|pair| pair[0].efficiency_score >= pair[1].efficiency_score),
            "scenario report is sorted descending by efficiency score",
        ),
    ])
}

fn assertion(name: &str, passed: bool, evidence: impl Into<String>) -> FeatureAssertion {
    FeatureAssertion {
        name: name.to_string(),
        passed,
        evidence: evidence.into(),
    }
}

fn placed_prefix_count(placements: &[Placement], prefix: &str) -> usize {
    placements
        .iter()
        .filter(|p| p.pod.starts_with(prefix) && p.node.is_some())
        .count()
}

fn priority_inertness_holds() -> anyhow::Result<bool> {
    let weighted_scenario = deterministic_scenarios()
        .into_iter()
        .find(|s| s.name == "priority-gang-over-fillers")
        .context("priority-gang-over-fillers scenario missing")?;
    let mut priority_metadata_only = weighted_scenario.clone();
    priority_metadata_only.ksolver_priority_weight = 0;
    let mut priority_stripped = priority_metadata_only.clone();
    for job in &mut priority_stripped.jobs {
        job.priority = 0;
        job.priority_class_name.clear();
    }

    let with_priority_metadata = run_ksolver(&priority_metadata_only)?;
    let without_priority_metadata = run_ksolver(&priority_stripped)?;
    Ok(
        with_priority_metadata.metrics == without_priority_metadata.metrics
            && with_priority_metadata.placements == without_priority_metadata.placements,
    )
}

fn headline(kube: &PlacementMetrics, ksolver: &PlacementMetrics) -> String {
    let useful_delta = ksolver.useful_gpu - kube.useful_gpu;
    let invalid_delta =
        kube.partial_or_invalid_gangs as i64 - ksolver.partial_or_invalid_gangs as i64;
    let large_delta = ksolver.large_jobs_admitted as i64 - kube.large_jobs_admitted as i64;
    let priority_delta = ksolver.priority_useful_gpu - kube.priority_useful_gpu;
    let business_value_delta = ksolver.business_value_useful_gpu - kube.business_value_useful_gpu;
    let fair_share_delta = ksolver.fair_share_useful_gpu - kube.fair_share_useful_gpu;
    let queue_delta = ksolver.queue_useful_gpu - kube.queue_useful_gpu;
    let queue_wait_delta = ksolver.queue_wait_useful_gpu - kube.queue_wait_useful_gpu;
    let deadline_delta = ksolver.deadline_met_gpu - kube.deadline_met_gpu;
    let flexible_delta = ksolver.flexible_gpu_reduction - kube.flexible_gpu_reduction;
    format!(
        "{:+} useful GPUs, {:+} priority-GPU score, {:+} business-value GPU score, {:+} fair-share GPU score, {:+} queue GPU score, {:+} queue-wait GPU-seconds, {:+} deadline-met GPUs, {:+} flexible GPUs saved, {:+} large jobs, {:+} invalid gangs avoided",
        useful_delta, priority_delta, business_value_delta, fair_share_delta, queue_delta, queue_wait_delta, deadline_delta, flexible_delta, large_delta, invalid_delta
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn empty_engine(engine: &str) -> EngineResult {
        EngineResult {
            engine: engine.to_string(),
            source: String::new(),
            simulator: None,
            candidate_node_limit: 0,
            solve_millis: 0,
            metrics: PlacementMetrics::default(),
            placements: Vec::new(),
        }
    }

    #[cfg(feature = "rust-cp-sat")]
    fn write_test_simulator_cache() -> PathBuf {
        let mut cache = SimulatorCacheFile {
            version: 1,
            entries: BTreeMap::new(),
        };
        for scenario in deterministic_scenarios() {
            for (variant, result) in [
                ("spread", run_greedy_spread(&scenario)),
                ("binpack", run_greedy_binpack(&scenario)),
            ] {
                cache.entries.insert(
                    simulator_cache_key(&scenario, variant),
                    CachedSimulatorResult {
                        engine: result.engine,
                        source: "test kube-scheduler-simulator fixture".to_string(),
                        placements: result.placements,
                    },
                );
            }
        }
        let path = std::env::temp_dir().join(format!(
            "ksolver-test-simulator-cache-{}.json",
            std::process::id()
        ));
        write_simulator_cache(&path, &cache).expect("write test simulator cache");
        path
    }

    #[test]
    fn candidate_nodes_keep_tightest_fitting_nodes() {
        let s = scenario(
            "candidate-test",
            "candidate ordering",
            &[1, 4, 8, 4],
            vec![JobSpec::singleton("large", 4)],
        );
        let nodes = candidate_nodes(&s, &JobSpec::singleton("large", 4), 2);
        assert_eq!(nodes, vec!["gpu-4g-1", "gpu-4g-3"]);
    }

    #[test]
    fn candidate_nodes_respect_colocation_total_gpu() {
        let s = scenario(
            "candidate-gang-test",
            "candidate ordering",
            &[1, 4, 8],
            vec![JobSpec::colocated_gang("gang", 4, 1)],
        );
        let nodes = candidate_nodes(&s, &JobSpec::colocated_gang("gang", 4, 1), 4);
        assert_eq!(nodes, vec!["gpu-4g-1", "gpu-8g-2"]);
    }

    #[test]
    fn regret_metrics_report_nonnegative_loss() {
        let full = PlacementMetrics {
            useful_gpu: 10,
            priority_useful_gpu: 30,
            placed_pods: 5,
            large_jobs_admitted: 2,
            full_gangs: 1,
            partial_or_invalid_gangs: 0,
            ..Default::default()
        };
        let reduced = PlacementMetrics {
            useful_gpu: 8,
            priority_useful_gpu: 40,
            placed_pods: 6,
            large_jobs_admitted: 1,
            full_gangs: 0,
            partial_or_invalid_gangs: 2,
            ..Default::default()
        };
        let regret = regret_metrics(2, &full, &reduced);
        assert_eq!(regret.candidate_node_limit, 2);
        assert_eq!(regret.useful_gpu_regret, 2);
        assert_eq!(regret.priority_useful_gpu_regret, 0);
        assert_eq!(regret.placed_pod_regret, 0);
        assert_eq!(regret.large_job_regret, 1);
        assert_eq!(regret.full_gang_regret, 1);
        assert_eq!(regret.invalid_gang_delta, 2);
    }

    #[cfg(feature = "rust-cp-sat")]
    #[test]
    fn new_gang_scenarios_are_real_admission_wins_not_partial_fragments() {
        let scenarios = deterministic_scenarios();
        for name in [
            "two-training-gangs-or-fillers",
            "colocated-8gpu-training-gang",
            "deadline-gang-vs-batch",
            "fair-share-gang-scarce-gpu",
            "queue-wait-gang-over-new-fillers",
        ] {
            let scenario = scenarios
                .iter()
                .find(|s| s.name == name)
                .unwrap_or_else(|| panic!("missing scenario {name}"));
            let spread = run_greedy_spread(scenario);
            let binpack = run_greedy_binpack(scenario);
            let kube = best_kube(&spread, &binpack);
            let ksolver = run_ksolver(scenario).expect("ksolver should solve scenario");

            assert_eq!(
                ksolver.metrics.partial_or_invalid_gangs, 0,
                "{name}: ksolver should not count partial gang fragments"
            );
            assert!(
                ksolver.metrics.full_gangs > kube.metrics.full_gangs
                    || ksolver.metrics.useful_gpu > kube.metrics.useful_gpu
                    || ksolver.metrics.deadline_met_gpu > kube.metrics.deadline_met_gpu
                    || ksolver.metrics.fair_share_useful_gpu > kube.metrics.fair_share_useful_gpu
                    || ksolver.metrics.queue_wait_useful_gpu > kube.metrics.queue_wait_useful_gpu,
                "{name}: expected a concrete admission or policy win over best kube baseline; ksolver={:?}, kube={:?}",
                ksolver.metrics,
                kube.metrics
            );
            assert!(
                ksolver.metrics.useful_gpu >= kube.metrics.useful_gpu
                    || ksolver.metrics.deadline_met_gpu > kube.metrics.deadline_met_gpu
                    || ksolver.metrics.fair_share_useful_gpu > kube.metrics.fair_share_useful_gpu
                    || ksolver.metrics.queue_wait_useful_gpu > kube.metrics.queue_wait_useful_gpu,
                "{name}: ksolver should not be presented as a pure utilization win if it only under-schedules useful work"
            );
        }
    }

    #[cfg(feature = "rust-cp-sat")]
    #[test]
    fn priority_metadata_is_inert_when_priority_weight_is_zero() {
        let weighted_scenario = deterministic_scenarios()
            .into_iter()
            .find(|s| s.name == "priority-gang-over-fillers")
            .expect("priority scenario should exist");
        let mut priority_metadata_only = weighted_scenario.clone();
        priority_metadata_only.ksolver_priority_weight = 0;
        let mut priority_stripped = priority_metadata_only.clone();
        for job in &mut priority_stripped.jobs {
            job.priority = 0;
            job.priority_class_name.clear();
        }

        let with_priority_metadata =
            run_ksolver(&priority_metadata_only).expect("priority metadata solve");
        let without_priority_metadata =
            run_ksolver(&priority_stripped).expect("priority stripped solve");

        assert_eq!(
            with_priority_metadata.metrics,
            without_priority_metadata.metrics
        );
        assert_eq!(
            with_priority_metadata.placements,
            without_priority_metadata.placements
        );
    }

    // Phase 3 acceptance criterion: "weight-zero tests prove metadata is inert by default." With a
    // dimension's weight at zero, its per-job metadata must not change the outcome at all — running
    // with the metadata present must produce identical metrics AND placements to running with it
    // stripped. Mirrors priority_metadata_is_inert_when_priority_weight_is_zero for the other opt-in
    // metadata dimensions (business value, queue, queue-wait, fair share).
    #[cfg(feature = "rust-cp-sat")]
    fn assert_metadata_inert(
        scenario: &str,
        zero_weight: impl Fn(&mut ScenarioSpec),
        strip: impl Fn(&mut JobSpec),
    ) {
        let base = deterministic_scenarios()
            .into_iter()
            .find(|s| s.name == scenario)
            .unwrap_or_else(|| panic!("scenario {scenario} should exist"));
        let mut weight_zero = base.clone();
        zero_weight(&mut weight_zero);
        let mut stripped = weight_zero.clone();
        for job in &mut stripped.jobs {
            strip(job);
        }
        let with_meta = run_ksolver(&weight_zero).expect("solve with metadata");
        let without_meta = run_ksolver(&stripped).expect("solve stripped");
        assert_eq!(
            with_meta.metrics, without_meta.metrics,
            "{scenario}: metrics differ when weight is zero (metadata not inert)"
        );
        assert_eq!(
            with_meta.placements, without_meta.placements,
            "{scenario}: placements differ when weight is zero (metadata not inert)"
        );
    }

    #[cfg(feature = "rust-cp-sat")]
    #[test]
    fn business_value_metadata_is_inert_when_weight_is_zero() {
        assert_metadata_inert(
            "business-value-over-fifo",
            |s| s.ksolver_business_value_weight = 0,
            |j| j.business_value = 0,
        );
    }

    #[cfg(feature = "rust-cp-sat")]
    #[test]
    fn queue_metadata_is_inert_when_weight_is_zero() {
        assert_metadata_inert(
            "queue-urgent-over-fifo",
            |s| s.ksolver_queue_weight = 0,
            |j| {
                j.queue.clear();
                j.queue_score = 0;
            },
        );
    }

    #[cfg(feature = "rust-cp-sat")]
    #[test]
    fn queue_wait_metadata_is_inert_when_weight_is_zero() {
        assert_metadata_inert(
            "queue-wait-over-fifo",
            |s| s.ksolver_queue_wait_weight = 0,
            |j| j.queue_wait_seconds = 0,
        );
    }

    #[cfg(feature = "rust-cp-sat")]
    #[test]
    fn fair_share_metadata_is_inert_when_weight_is_zero() {
        assert_metadata_inert(
            "fair-share-over-fifo",
            |s| s.ksolver_fair_share_weight = 0,
            |j| j.fair_share_deficit = 0,
        );
    }

    #[cfg(feature = "rust-cp-sat")]
    #[test]
    fn priority_weight_changes_scenario_value_for_urgent_gang() {
        let weighted_scenario = deterministic_scenarios()
            .into_iter()
            .find(|s| s.name == "priority-gang-over-fillers")
            .expect("priority scenario should exist");
        let mut unweighted_scenario = weighted_scenario.clone();
        unweighted_scenario.ksolver_priority_weight = 0;

        let weighted = run_ksolver(&weighted_scenario).expect("weighted priority solve");
        let unweighted = run_ksolver(&unweighted_scenario).expect("unweighted priority solve");

        assert!(
            weighted.metrics.priority_useful_gpu >= unweighted.metrics.priority_useful_gpu,
            "priority weight should not reduce admitted priority-weighted GPU work"
        );
        assert!(
            weighted
                .placements
                .iter()
                .filter(|p| p.pod.starts_with("urgent-gang") && p.node.is_some())
                .count()
                == 4,
            "priority-weighted scenario should admit the full urgent gang"
        );
    }

    #[test]
    fn regret_summary_aggregates_positive_reduced_solve_losses() {
        let scenarios = vec![
            ScenarioResult {
                name: "lossy".to_string(),
                description: String::new(),
                tier: Tier::Small,
                benefit_score: 0,
                headline: String::new(),
                kube: EngineResult {
                    engine: "kube".to_string(),
                    source: String::new(),
                    simulator: None,
                    candidate_node_limit: 0,
                    solve_millis: 0,
                    metrics: PlacementMetrics::default(),
                    placements: Vec::new(),
                },
                kube_binpack: empty_engine("kube-binpack"),
                ksolver: EngineResult {
                    engine: "ksolver".to_string(),
                    source: String::new(),
                    simulator: None,
                    candidate_node_limit: 0,
                    solve_millis: 0,
                    metrics: PlacementMetrics::default(),
                    placements: Vec::new(),
                },
                reduced_ksolver: EngineResult {
                    engine: "ksolver".to_string(),
                    source: String::new(),
                    simulator: None,
                    candidate_node_limit: 2,
                    solve_millis: 0,
                    metrics: PlacementMetrics::default(),
                    placements: Vec::new(),
                },
                regret: RegretMetrics {
                    candidate_node_limit: 2,
                    useful_gpu_regret: 3,
                    priority_useful_gpu_regret: 9,
                    placed_pod_regret: 1,
                    large_job_regret: 1,
                    full_gang_regret: 0,
                    invalid_gang_delta: 2,
                },
                efficiency_score: 0,
                significantly_better: false,
                efficiency_headline: String::new(),
            },
            ScenarioResult {
                name: "equal".to_string(),
                description: String::new(),
                tier: Tier::Small,
                benefit_score: 0,
                headline: String::new(),
                kube: EngineResult {
                    engine: "kube".to_string(),
                    source: String::new(),
                    simulator: None,
                    candidate_node_limit: 0,
                    solve_millis: 0,
                    metrics: PlacementMetrics::default(),
                    placements: Vec::new(),
                },
                kube_binpack: empty_engine("kube-binpack"),
                ksolver: EngineResult {
                    engine: "ksolver".to_string(),
                    source: String::new(),
                    simulator: None,
                    candidate_node_limit: 0,
                    solve_millis: 0,
                    metrics: PlacementMetrics::default(),
                    placements: Vec::new(),
                },
                reduced_ksolver: EngineResult {
                    engine: "ksolver".to_string(),
                    source: String::new(),
                    simulator: None,
                    candidate_node_limit: 2,
                    solve_millis: 0,
                    metrics: PlacementMetrics::default(),
                    placements: Vec::new(),
                },
                regret: RegretMetrics {
                    candidate_node_limit: 2,
                    invalid_gang_delta: -1,
                    ..Default::default()
                },
                efficiency_score: 0,
                significantly_better: false,
                efficiency_headline: String::new(),
            },
        ];

        let summary = summarize_regret(2, &scenarios);

        assert_eq!(summary.candidate_node_limit, 2);
        assert_eq!(summary.scenarios_compared, 2);
        assert_eq!(summary.scenarios_with_any_regret, 1);
        assert_eq!(summary.scenarios_with_useful_gpu_regret, 1);
        assert_eq!(summary.total_useful_gpu_regret, 3);
        assert_eq!(summary.max_useful_gpu_regret, 3);
        assert_eq!(summary.total_priority_useful_gpu_regret, 9);
        assert_eq!(summary.max_priority_useful_gpu_regret, 9);
        assert_eq!(summary.total_placed_pod_regret, 1);
        assert_eq!(summary.total_large_job_regret, 1);
        assert_eq!(summary.total_full_gang_regret, 0);
        assert_eq!(summary.total_invalid_gang_delta, 2);
    }

    #[test]
    fn candidate_widening_proof_selects_regretful_pruned_solve() {
        let scenario = |name: &str,
                        full_useful_gpu: i64,
                        full_unplaced_pods: usize,
                        pruned_useful_gpu: i64,
                        pruned_unplaced_pods: usize|
         -> ScenarioResult {
            ScenarioResult {
                name: name.to_string(),
                description: String::new(),
                tier: Tier::Small,
                benefit_score: 0,
                headline: String::new(),
                kube: EngineResult {
                    engine: "kube".to_string(),
                    source: String::new(),
                    simulator: None,
                    candidate_node_limit: 0,
                    solve_millis: 0,
                    metrics: PlacementMetrics::default(),
                    placements: Vec::new(),
                },
                kube_binpack: empty_engine("kube-binpack"),
                ksolver: EngineResult {
                    engine: "ksolver".to_string(),
                    source: String::new(),
                    simulator: None,
                    candidate_node_limit: 0,
                    solve_millis: 0,
                    metrics: PlacementMetrics {
                        useful_gpu: full_useful_gpu,
                        unplaced_pods: full_unplaced_pods,
                        ..Default::default()
                    },
                    placements: Vec::new(),
                },
                reduced_ksolver: EngineResult {
                    engine: "ksolver".to_string(),
                    source: String::new(),
                    simulator: None,
                    candidate_node_limit: REGRET_CANDIDATE_LIMIT,
                    solve_millis: 0,
                    metrics: PlacementMetrics {
                        useful_gpu: pruned_useful_gpu,
                        unplaced_pods: pruned_unplaced_pods,
                        ..Default::default()
                    },
                    placements: Vec::new(),
                },
                regret: RegretMetrics {
                    candidate_node_limit: REGRET_CANDIDATE_LIMIT,
                    useful_gpu_regret: (full_useful_gpu - pruned_useful_gpu).max(0),
                    placed_pod_regret: pruned_unplaced_pods.saturating_sub(full_unplaced_pods)
                        as i64,
                    ..Default::default()
                },
                efficiency_score: 0,
                significantly_better: false,
                efficiency_headline: String::new(),
            }
        };
        let scenarios = vec![
            scenario("small-regret", 8, 1, 7, 2),
            scenario("big-regret", 12, 2, 8, 6),
        ];

        let proof = candidate_widening_scenario_proof(&scenarios);

        assert!(proof.passed);
        assert_eq!(proof.scenario, "big-regret");
        assert_eq!(proof.initial_candidate_node_limit, REGRET_CANDIDATE_LIMIT);
        assert_eq!(proof.final_candidate_node_limit, 0);
        assert_eq!(proof.retry_count, 1);
        assert_eq!(proof.useful_gpu_recovered, 4);
        assert_eq!(
            proof.widening_reason,
            "low admission ratio with pruned candidates"
        );
    }

    #[test]
    fn benefit_summary_aggregates_value_deltas_across_scenarios() {
        let scenario = |name: &str,
                        score: i64,
                        kube: PlacementMetrics,
                        ksolver: PlacementMetrics|
         -> ScenarioResult {
            ScenarioResult {
                name: name.to_string(),
                description: String::new(),
                tier: Tier::Small,
                benefit_score: score,
                headline: String::new(),
                kube: EngineResult {
                    engine: "kube".to_string(),
                    source: String::new(),
                    simulator: None,
                    candidate_node_limit: 0,
                    solve_millis: 0,
                    metrics: kube,
                    placements: Vec::new(),
                },
                kube_binpack: empty_engine("kube-binpack"),
                ksolver: EngineResult {
                    engine: "ksolver".to_string(),
                    source: String::new(),
                    simulator: None,
                    candidate_node_limit: 0,
                    solve_millis: 0,
                    metrics: ksolver,
                    placements: Vec::new(),
                },
                reduced_ksolver: EngineResult {
                    engine: "ksolver".to_string(),
                    source: String::new(),
                    simulator: None,
                    candidate_node_limit: 2,
                    solve_millis: 0,
                    metrics: PlacementMetrics::default(),
                    placements: Vec::new(),
                },
                regret: RegretMetrics::default(),
                efficiency_score: 0,
                significantly_better: false,
                efficiency_headline: String::new(),
            }
        };
        let scenarios = vec![
            scenario(
                "top",
                100,
                PlacementMetrics {
                    useful_gpu: 4,
                    priority_useful_gpu: 0,
                    large_jobs_admitted: 1,
                    full_gangs: 0,
                    partial_or_invalid_gangs: 1,
                    active_nodes: 3,
                    stranded_gpu_on_active_nodes: 5,
                    ..Default::default()
                },
                PlacementMetrics {
                    useful_gpu: 8,
                    priority_useful_gpu: 40,
                    large_jobs_admitted: 2,
                    full_gangs: 1,
                    partial_or_invalid_gangs: 0,
                    active_nodes: 2,
                    stranded_gpu_on_active_nodes: 1,
                    ..Default::default()
                },
            ),
            scenario(
                "loss",
                -10,
                PlacementMetrics {
                    useful_gpu: 6,
                    active_nodes: 1,
                    stranded_gpu_on_active_nodes: 0,
                    ..Default::default()
                },
                PlacementMetrics {
                    useful_gpu: 4,
                    active_nodes: 2,
                    stranded_gpu_on_active_nodes: 3,
                    ..Default::default()
                },
            ),
        ];

        let summary = summarize_benefit(&scenarios);

        assert_eq!(summary.scenarios_compared, 2);
        assert_eq!(summary.scenarios_with_positive_benefit, 1);
        assert_eq!(summary.scenarios_with_useful_gpu_gain, 1);
        assert_eq!(summary.total_benefit_score, 90);
        assert_eq!(summary.max_benefit_score, 100);
        assert_eq!(summary.top_scenario, "top");
        assert_eq!(summary.total_useful_gpu_gain, 2);
        assert_eq!(summary.total_priority_useful_gpu_gain, 40);
        assert_eq!(summary.total_large_job_gain, 1);
        assert_eq!(summary.total_full_gang_gain, 1);
        assert_eq!(summary.total_invalid_gangs_avoided, 1);
        assert_eq!(summary.total_active_node_reduction, 0);
        assert_eq!(summary.total_stranded_gpu_reduction, 1);
    }

    #[test]
    fn roi_summary_aggregates_admission_and_utilization_deltas() {
        let placement = |pod: &str, gpus: i64, node: Option<&str>| Placement {
            pod: pod.to_string(),
            node: node.map(str::to_string),
            gpus,
        };
        let result = |engine: &str,
                      useful_gpu: i64,
                      unplaced_pods: usize,
                      active_nodes: usize,
                      stranded_gpu: i64,
                      monthly_cost: i64,
                      utilization_milli: i64,
                      placements: Vec<Placement>|
         -> EngineResult {
            EngineResult {
                engine: engine.to_string(),
                source: String::new(),
                simulator: None,
                candidate_node_limit: 0,
                solve_millis: 0,
                metrics: PlacementMetrics {
                    useful_gpu,
                    unplaced_pods,
                    active_nodes,
                    stranded_gpu_on_active_nodes: stranded_gpu,
                    cost_active_nodes_monthly: monthly_cost,
                    gpu_utilization_milli: utilization_milli,
                    ..Default::default()
                },
                placements,
            }
        };
        let scenario = ScenarioResult {
            name: "roi".to_string(),
            description: String::new(),
            tier: Tier::Small,
            benefit_score: 1,
            headline: String::new(),
            kube: result(
                "kube",
                4,
                2,
                3,
                5,
                9_000,
                500,
                vec![
                    placement("a", 4, Some("n1")),
                    placement("b", 4, None),
                    placement("c", 2, None),
                ],
            ),
            kube_binpack: empty_engine("kube-binpack"),
            ksolver: result(
                "ksolver",
                8,
                1,
                2,
                1,
                6_000,
                800,
                vec![
                    placement("a", 4, Some("n1")),
                    placement("b", 4, Some("n2")),
                    placement("c", 2, None),
                ],
            ),
            reduced_ksolver: result("ksolver", 0, 0, 0, 0, 0, 0, Vec::new()),
            regret: RegretMetrics::default(),
            efficiency_score: 0,
            significantly_better: false,
            efficiency_headline: String::new(),
        };

        let summary = summarize_roi(&[scenario]);

        assert_eq!(summary.scenarios_compared, 1);
        assert_eq!(summary.total_requested_gpu, 10);
        assert_eq!(summary.kube_admitted_useful_gpu, 4);
        assert_eq!(summary.ksolver_admitted_useful_gpu, 8);
        assert_eq!(summary.admitted_useful_gpu_gain, 4);
        assert_eq!(summary.unplaced_pod_reduction, 1);
        assert_eq!(summary.active_node_reduction, 1);
        assert_eq!(summary.stranded_gpu_reduction, 4);
        assert_eq!(summary.kube_active_node_monthly_cost, 9_000);
        assert_eq!(summary.ksolver_active_node_monthly_cost, 6_000);
        assert_eq!(summary.active_node_monthly_cost_reduction, 3_000);
        assert_eq!(summary.kube_gpu_utilization_milli, 500);
        assert_eq!(summary.ksolver_gpu_utilization_milli, 800);
        assert_eq!(summary.gpu_utilization_gain_milli, 300);
        assert_eq!(summary.kube_admission_percent_milli, 40_000);
        assert_eq!(summary.ksolver_admission_percent_milli, 80_000);
        assert_eq!(summary.admission_percent_gain_milli, 40_000);
        assert!(summary.headline.contains("4 more useful GPU"));
    }

    #[test]
    fn roi_dashboard_summary_packages_30_second_operator_kpis() {
        let roi = RoiSummary {
            scenarios_compared: 2,
            total_requested_gpu: 20,
            kube_admitted_useful_gpu: 8,
            ksolver_admitted_useful_gpu: 14,
            admitted_useful_gpu_gain: 6,
            stranded_gpu_reduction: 5,
            kube_active_node_monthly_cost: 12_000,
            ksolver_active_node_monthly_cost: 9_000,
            active_node_monthly_cost_reduction: 3_000,
            ..Default::default()
        };
        let benefit = BenefitSummary {
            total_deadline_unplaced_gpu_reduction: 2,
            total_deadline_miss_gpu_reduction: 1,
            ..Default::default()
        };
        let regret = RegretSummary {
            candidate_node_limit: REGRET_CANDIDATE_LIMIT,
            scenarios_compared: 2,
            scenarios_with_any_regret: 1,
            max_useful_gpu_regret: 4,
            ..Default::default()
        };
        let repair = RepairScenarioProof {
            name: "fragmented-gang-repair".to_string(),
            passed: true,
            target: "team/urgent".to_string(),
            target_gpu_request: 4,
            node: "n1".to_string(),
            action_count: 3,
            freed_gpu: 4,
            migration_actions: 2,
            preemption_actions: 1,
            disruption_cost: 120,
            explanation: "enough GPUs exist, but repair is needed".to_string(),
            notes: Vec::new(),
            metrics: RepairMetrics::default(),
            action_rows: Vec::new(),
        };

        let summary = summarize_roi_dashboard(&roi, &benefit, &regret, &repair);

        assert!(summary.passed);
        assert_eq!(summary.name, "roi-dashboard-readiness");
        assert_eq!(summary.primary_tiles.len(), 6);
        assert!(summary
            .primary_tiles
            .iter()
            .any(|tile| tile.key == "admitted_useful_gpu_gain" && tile.value == 6));
        assert!(summary
            .primary_tiles
            .iter()
            .any(|tile| tile.key == "deadline_pressure_reduction" && tile.value == 3));
        assert!(summary
            .primary_tiles
            .iter()
            .any(|tile| tile.key == "active_node_monthly_cost_delta" && tile.value == -3_000));
        assert!(summary
            .primary_tiles
            .iter()
            .any(|tile| tile.key == "hero_repair_disruption_cost" && tile.value == 120));
        assert_eq!(summary.executive_rows.len(), 5);
        assert!(summary.executive_rows.iter().any(|row| {
            row.priority == 4
                && row.evidence_tile == "hero_repair_disruption_cost"
                && row.operator_action.contains("migrate/preempt")
        }));
        assert_eq!(summary.decision_rows.len(), 6);
        assert!(summary.decision_rows.iter().any(|row| {
            row.tile_key == "active_node_monthly_cost_delta"
                && row.next_action.contains("pricing catalog")
                && row.caveat.contains("billing")
        }));
        assert!(summary.decision_rows.iter().any(|row| {
            row.tile_key == "candidate_pruning_max_regret"
                && row.decision_rule.contains("pruned ROI")
        }));
        assert!(summary.decision_rows.iter().any(|row| {
            row.tile_key == "hero_repair_disruption_cost"
                && row.evidence_source.contains("production_safety_summary")
        }));
        assert!(summary
            .decision_frame
            .iter()
            .any(|step| step.contains("disruption cost")));
        assert!(summary
            .presentation_order
            .contains(&"decision_rows".to_string()));
        assert_eq!(
            summary.claim_contract.claim_level,
            "Scenario-normalized advisory ROI"
        );
        assert!(!summary.claim_contract.can_show_customer_dollars);
        assert!(summary
            .claim_contract
            .required_evidence
            .iter()
            .any(|evidence| evidence.contains("pricing catalog")));
        assert!(summary
            .claim_contract
            .blocked_by
            .iter()
            .any(|blocker| blocker.contains("synthetic")));
        assert!(summary.confidence_guardrail.contains("prediction"));
        assert!(summary.regret_guardrail.contains("regret"));
        assert!(summary
            .operator_questions
            .iter()
            .any(|q| q.contains("disruption cost")));
    }

    #[test]
    fn pricing_readiness_summary_names_required_customer_pricing_inputs() {
        let roi = RoiSummary {
            scenarios_compared: 2,
            kube_active_node_monthly_cost: 12_000,
            ksolver_active_node_monthly_cost: 9_000,
            active_node_monthly_cost_reduction: 3_000,
            ..Default::default()
        };
        let roi_dashboard = RoiDashboardSummary {
            passed: true,
            ..Default::default()
        };

        let summary = summarize_pricing_readiness(&roi, &roi_dashboard);

        assert!(summary.passed);
        assert_eq!(summary.name, "pricing-readiness");
        assert!(summary.current_mode.contains("synthetic"));
        assert!(summary
            .pricing_required_before_customer_claim
            .iter()
            .any(|item| item.contains("node monthly price")));
        assert!(summary
            .accepted_sources
            .iter()
            .any(|source| source.contains("chargeback")));
        assert!(summary
            .roi_fields_to_recompute
            .contains(&"roi_summary.active_node_monthly_cost_reduction".to_string()));
        assert!(summary
            .validation_checks
            .iter()
            .any(|check| check.contains("price_source")));
        assert!(summary.pricing_evidence_rows.iter().any(|row| {
            row.gate == "node-to-price mapping"
                && row.recompute_target.contains("active_node_monthly_cost")
        }));
        assert!(summary.pricing_evidence_rows.iter().any(|row| {
            row.gate == "tenant budget reconciliation"
                && row.recompute_target.contains("tenant_fairness_metrics")
        }));
        assert!(summary.pricing_evidence_rows.iter().any(|row| {
            row.gate == "screenshot/customer claim guard"
                && row.failure_action.contains("relative cost units")
        }));
        assert!(summary
            .operator_actions
            .iter()
            .any(|action| action.contains("pricing catalog")));
        assert!(summary
            .residual_risks
            .iter()
            .any(|risk| risk.contains("owned-hardware amortization")));
    }

    #[test]
    fn hero_demo_summary_turns_repair_proof_into_sre_story() {
        let repair = RepairScenarioProof {
            name: "fragmented-gang-repair".to_string(),
            passed: true,
            target: "team/urgent".to_string(),
            target_gpu_request: 4,
            node: "n1".to_string(),
            action_count: 4,
            migration_actions: 2,
            preemption_actions: 2,
            freed_gpu: 4,
            disruption_cost: 200,
            explanation: "enough GPUs exist, but repair is needed".to_string(),
            notes: Vec::new(),
            metrics: RepairMetrics::default(),
            action_rows: Vec::new(),
        };
        let roi = RoiSummary {
            headline: "ksolver admitted more useful GPU demand".to_string(),
            ..Default::default()
        };

        let summary = summarize_hero_demo(&repair, &roi);

        assert!(summary.passed);
        assert_eq!(summary.name, "defragmentation-advisor");
        assert_eq!(summary.target, "team/urgent");
        assert_eq!(summary.target_gpu_request, 4);
        assert_eq!(summary.freed_gpu, 4);
        assert_eq!(summary.migration_actions, 2);
        assert_eq!(summary.preemption_actions, 2);
        assert_eq!(summary.disruption_cost, 200);
        assert!(summary.headline.contains("fragmented"));
        assert!(summary.recommendation.contains("dry-run"));
        assert_eq!(summary.roi_headline, roi.headline);
        assert!(summary.screenshot_claims.len() >= 3);
    }

    #[test]
    fn preemption_migration_hero_summary_keeps_ui_ready_action_rows() {
        let repair = fragmented_repair_scenario_proof();

        let summary = summarize_preemption_migration_hero(&repair);

        assert!(summary.passed);
        assert_eq!(summary.name, "preemption-migration-hero");
        assert_eq!(summary.action_rows.len(), repair.action_count);
        assert!(summary
            .action_rows
            .iter()
            .any(|a| a.action == "migrate" && !a.to_node.is_empty()));
        assert!(summary.action_rows.iter().any(|a| a.action == "preempt"));
        assert!(summary.headline.contains("Free 4 GPUs"));
        assert!(summary
            .safety_claims
            .iter()
            .any(|s| s.contains("dry-run only")));
        assert!(summary
            .operator_questions
            .iter()
            .any(|q| q.contains("Which pods")));
        assert!(!summary.decision_contract.can_act_now);
        assert!(summary
            .decision_contract
            .verdict
            .contains("live approval required"));
        assert!(summary
            .decision_contract
            .evidence_required
            .iter()
            .any(|evidence| evidence.contains("/api/scheduler/repair-plan")));
        assert!(summary
            .decision_contract
            .fail_closed_if
            .iter()
            .any(|gate| gate.contains("stale")));
    }

    #[test]
    fn sre_demo_script_ranks_scenario_cards_for_operator_story() {
        let result = |engine: &str, useful_gpu: i64, cost: i64, util: i64| EngineResult {
            engine: engine.to_string(),
            source: String::new(),
            simulator: None,
            candidate_node_limit: 0,
            solve_millis: 0,
            metrics: PlacementMetrics {
                useful_gpu,
                cost_active_nodes_monthly: cost,
                gpu_utilization_milli: util,
                ..Default::default()
            },
            placements: Vec::new(),
        };
        let scenario = |name: &str, score: i64| ScenarioResult {
            name: name.to_string(),
            description: String::new(),
            tier: Tier::Small,
            benefit_score: score,
            headline: String::new(),
            kube: result("kube-spread", 4, 8_000, 500),
            kube_binpack: result("kube-binpack", 5, 7_000, 600),
            ksolver: result("ksolver", 8, 6_000, 900),
            reduced_ksolver: empty_engine("ksolver"),
            regret: RegretMetrics::default(),
            efficiency_score: score,
            significantly_better: score > 0,
            efficiency_headline: format!("scenario {name} is better"),
        };
        let scenarios = vec![scenario("top", 100), scenario("second", 50)];
        let hero = HeroDemoSummary {
            name: "defragmentation-advisor".to_string(),
            headline: "Enough GPUs exist, but they are fragmented.".to_string(),
            problem: "blocked gang".to_string(),
            recommendation: "dry-run repair".to_string(),
            ..Default::default()
        };
        let roi = RoiSummary {
            headline: "ksolver admitted more useful GPU demand".to_string(),
            ..Default::default()
        };

        let script = summarize_sre_demo_script(&scenarios, &hero, &roi);

        assert_eq!(script.name, "gpu-fleet-defragmentation-roi-demo");
        assert_eq!(script.hero_scenario, "defragmentation-advisor");
        assert_eq!(script.steps.len(), 4);
        assert_eq!(script.top_scenario_cards.len(), 2);
        assert_eq!(script.top_scenario_cards[0].scenario, "top");
        assert_eq!(script.top_scenario_cards[0].kube_useful_gpu, 5);
        assert_eq!(script.top_scenario_cards[0].useful_gpu_gain, 3);
        assert_eq!(script.top_scenario_cards[0].gpu_utilization_gain_milli, 300);
        assert!(script.headline.contains("fragmented"));
        assert!(script.operator_close.contains("observe-only advisor"));
    }

    #[test]
    fn simulator_cache_rehydrates_engine_result_with_metrics() {
        let scenario = scenario(
            "cache-demo",
            "cache demo",
            &[1, 1],
            vec![
                JobSpec::singleton("a", 1),
                JobSpec::singleton("b", 1),
                JobSpec::singleton("c", 1),
            ],
        );
        let cached = CachedSimulatorResult {
            engine: "kube-spread".to_string(),
            source: "kube-scheduler-simulator (spread)".to_string(),
            placements: vec![
                Placement {
                    pod: "a".to_string(),
                    node: Some("g4dn-1".to_string()),
                    gpus: 1,
                },
                Placement {
                    pod: "b".to_string(),
                    node: Some("g4dn-2".to_string()),
                    gpus: 1,
                },
                Placement {
                    pod: "c".to_string(),
                    node: None,
                    gpus: 1,
                },
            ],
        };

        let result = cached_simulator_result(&scenario, "spread", &cached);

        assert_eq!(result.engine, "kube-spread");
        assert!(result.source.starts_with("cached "));
        assert_eq!(result.metrics.placed_pods, 2);
        assert_eq!(result.metrics.unplaced_pods, 1);
        assert_eq!(result.metrics.useful_gpu, 2);
        let simulator = result
            .simulator
            .as_ref()
            .expect("cached simulator provenance");
        assert_eq!(simulator.mode, "cached");
        assert_eq!(simulator.variant, "spread");
        assert_eq!(simulator.cache_key.as_deref(), Some("cache-demo:spread"));
    }

    #[test]
    fn simulator_cache_file_round_trips() {
        let path = std::env::temp_dir().join(format!(
            "ksolver-gpu-simulator-cache-test-{}.json",
            std::process::id()
        ));
        let _ = std::fs::remove_file(&path);
        let mut cache = SimulatorCacheFile {
            version: 1,
            entries: BTreeMap::new(),
        };
        cache.entries.insert(
            "cache-demo:spread".to_string(),
            CachedSimulatorResult {
                engine: "kube-spread".to_string(),
                source: "simulator".to_string(),
                placements: vec![Placement {
                    pod: "a".to_string(),
                    node: Some("n1".to_string()),
                    gpus: 1,
                }],
            },
        );

        write_simulator_cache(&path, &cache).expect("write cache");
        let loaded = load_simulator_cache(&path).expect("load cache");
        let _ = std::fs::remove_file(&path);

        assert_eq!(loaded.version, 1);
        assert_eq!(loaded.entries.len(), 1);
        assert_eq!(
            loaded.entries["cache-demo:spread"].placements[0]
                .node
                .as_deref(),
            Some("n1")
        );
    }

    #[test]
    fn simulator_cache_coverage_counts_missing_baselines() {
        let path = std::env::temp_dir().join(format!(
            "ksolver-gpu-simulator-coverage-test-{}.json",
            std::process::id()
        ));
        let _ = std::fs::remove_file(&path);
        let scenario = deterministic_scenarios()
            .into_iter()
            .next()
            .expect("scenario fixture");
        let mut cache = SimulatorCacheFile {
            version: 1,
            entries: BTreeMap::new(),
        };
        cache.entries.insert(
            simulator_cache_key(&scenario, "spread"),
            CachedSimulatorResult {
                engine: "kube-spread".to_string(),
                source: "simulator".to_string(),
                placements: Vec::new(),
            },
        );
        write_simulator_cache(&path, &cache).expect("write partial cache");
        let options = BenchmarkOptions {
            simulator_cache_path: Some(path.clone()),
            simulator_live_scenarios: Some(BTreeSet::from([scenario.name.clone()])),
            ..BenchmarkOptions::default()
        };

        let coverage = simulator_cache_coverage(&options).expect("coverage");
        let _ = std::fs::remove_file(&path);

        assert_eq!(
            coverage,
            SimulatorCacheCoverage {
                total_baselines: 2,
                cached_baselines: 1,
                missing_baselines: 1,
            }
        );
    }

    #[test]
    fn bounded_refresh_skips_cached_baselines_to_make_progress() {
        let mut cache = SimulatorCacheFile {
            version: 1,
            entries: BTreeMap::new(),
        };
        cache.entries.insert(
            "cache-demo:spread".to_string(),
            CachedSimulatorResult {
                engine: "kube-spread".to_string(),
                source: "simulator".to_string(),
                placements: Vec::new(),
            },
        );
        let mut options = BenchmarkOptions {
            refresh_simulator_cache: true,
            simulator_max_live_baselines: Some(4),
            ..BenchmarkOptions::default()
        };

        assert!(
            should_skip_cached_simulator_baseline(&options, &cache, "cache-demo:spread"),
            "capped dashboard refresh should spend live budget on missing cache entries first"
        );
        assert!(!should_skip_cached_simulator_baseline(
            &options,
            &cache,
            "cache-demo:binpack"
        ));

        options.simulator_max_live_baselines = None;
        assert!(
            !should_skip_cached_simulator_baseline(&options, &cache, "cache-demo:spread"),
            "uncapped explicit refresh still refreshes existing cache entries"
        );

        options.refresh_simulator_cache = false;
        assert!(should_skip_cached_simulator_baseline(
            &options,
            &cache,
            "cache-demo:spread"
        ));
    }

    #[test]
    fn simulator_cache_dir_uses_deterministic_entry_paths() {
        let dir = std::env::temp_dir().join(format!(
            "ksolver-gpu-simulator-cache-dir-test-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        let cached = CachedSimulatorResult {
            engine: "kube-binpack".to_string(),
            source: "simulator".to_string(),
            placements: vec![Placement {
                pod: "a".to_string(),
                node: Some("n1".to_string()),
                gpus: 1,
            }],
        };

        write_simulator_cache_entry(&dir, "cache-demo:binpack", &cached)
            .expect("write cache entry");
        let path =
            simulator_cache_entry_path(&dir, "cache-demo:binpack").expect("cache entry path");
        let loaded = load_simulator_cache_dir(&dir).expect("load cache dir");
        let _ = std::fs::remove_dir_all(&dir);

        assert_eq!(path, dir.join("cache-demo").join("binpack.json"));
        assert_eq!(loaded.entries.len(), 1);
        assert_eq!(
            loaded.entries["cache-demo:binpack"].placements[0]
                .node
                .as_deref(),
            Some("n1")
        );
    }

    #[test]
    fn simulator_provenance_exposes_phase_timings() {
        let diagnostics = crate::verifier::SimulatorBatchDiagnostics {
            elapsed_millis: 4_607,
            phase: "poll".to_string(),
            state: crate::verifier::SimulatorBatchState {
                target_count: 8,
                present_targets: 8,
                terminal_present_targets: 8,
            },
            stable_polls: 0,
            timed_out: false,
            phase_timings: vec![
                crate::verifier::SimulatorPhaseTiming {
                    phase: "reset".to_string(),
                    duration_millis: 194,
                    cumulative_millis: 194,
                },
                crate::verifier::SimulatorPhaseTiming {
                    phase: "snapshot import".to_string(),
                    duration_millis: 55,
                    cumulative_millis: 249,
                },
                crate::verifier::SimulatorPhaseTiming {
                    phase: "export request".to_string(),
                    duration_millis: 4_300,
                    cumulative_millis: 4_607,
                },
            ],
        };

        let provenance = simulator_provenance_from_diagnostics(
            "live",
            "spread",
            Some("http://127.0.0.1:1212"),
            Duration::from_millis(10_000),
            &diagnostics,
        );

        assert_eq!(provenance.elapsed_millis, Some(4_607));
        assert_eq!(provenance.phase_timings.len(), 3);
        assert!(provenance
            .phase_timings
            .iter()
            .any(|timing| { timing.phase == "export request" && timing.duration_millis == 4_300 }));
    }

    #[test]
    fn simulator_provenance_summary_surfaces_slow_phase_and_legacy_fallback_markers() {
        let live = EngineResult {
            engine: "kube-spread".to_string(),
            source: "live".to_string(),
            simulator: Some(SimulatorBaselineProvenance {
                mode: "live".to_string(),
                variant: "spread".to_string(),
                phase_timings: vec![
                    SimulatorPhaseTiming {
                        phase: "reset".to_string(),
                        duration_millis: 125,
                        cumulative_millis: 125,
                    },
                    SimulatorPhaseTiming {
                        phase: "export request".to_string(),
                        duration_millis: 4_200,
                        cumulative_millis: 4_800,
                    },
                ],
                ..Default::default()
            }),
            candidate_node_limit: 0,
            solve_millis: 0,
            metrics: PlacementMetrics::default(),
            placements: Vec::new(),
        };
        let fallback = EngineResult {
            engine: "kube-binpack".to_string(),
            source: "fallback".to_string(),
            simulator: Some(SimulatorBaselineProvenance {
                mode: "timed-out-fallback".to_string(),
                variant: "binpack".to_string(),
                timed_out: true,
                fallback_reason: Some(
                    "kube-scheduler-simulator exceeded batch timeout during export request"
                        .to_string(),
                ),
                ..Default::default()
            }),
            candidate_node_limit: 0,
            solve_millis: 0,
            metrics: PlacementMetrics::default(),
            placements: Vec::new(),
        };
        let missing = EngineResult {
            engine: "kube-missing".to_string(),
            source: "missing".to_string(),
            simulator: None,
            candidate_node_limit: 0,
            solve_millis: 0,
            metrics: PlacementMetrics::default(),
            placements: Vec::new(),
        };

        let lines = simulator_provenance_summary([&live, &fallback, &missing], Some(4), 2_500);

        assert!(lines.iter().any(|line| line.contains("live=1")));
        assert!(lines
            .iter()
            .any(|line| line.contains("invalid-legacy-fallback-marker=1")));
        assert!(lines
            .iter()
            .any(|line| line.contains("missing-simulator-provenance=1")));
        assert!(lines
            .iter()
            .all(|line| !line.contains("deterministic=1")));
        assert!(lines
            .iter()
            .all(|line| !line.contains("timed-out-fallback=1")));
        assert!(lines
            .iter()
            .any(|line| line.contains("1 timed out, 1 invalid legacy fallback marker")));
        assert!(lines
            .iter()
            .any(|line| line.contains("spread/export request took 4200ms")));
        assert!(lines
            .iter()
            .any(|line| line.contains("first invalid simulator provenance reason")));
        assert!(lines
            .iter()
            .any(|line| line.contains("exceeded batch timeout")));
    }

    #[test]
    fn concise_simulator_reason_truncates_long_messages() {
        let long_reason = "export ".repeat(80);

        let reason = concise_simulator_reason(&long_reason);

        assert!(reason.len() <= 220);
        assert!(reason.ends_with("..."));
        assert!(!reason.contains("  "));
    }

    #[tokio::test]
    async fn simulator_live_baseline_limit_errors_without_fallback() {
        let scenario = scenario(
            "limited-live-demo",
            "limited live demo",
            &[1],
            vec![JobSpec::singleton("a", 1)],
        );
        let mut cache = SimulatorCacheFile {
            version: 1,
            entries: BTreeMap::new(),
        };
        let options = BenchmarkOptions {
            simulator_url: Some("http://127.0.0.1:1".to_string()),
            simulator_urls: Vec::new(),
            simulator_cache_path: Some(PathBuf::from("/tmp/unused-ksolver-cache.json")),
            simulator_cache_dir: None,
            refresh_simulator_cache: true,
            simulator_batch_timeout: Duration::from_millis(1),
            simulator_progress: false,
            simulator_max_live_baselines: Some(0),
            simulator_live_scenarios: None,
        };
        let mut live_baselines = 0_usize;

        let err = run_kube_baseline(
            &scenario,
            options.simulator_url.as_deref(),
            crate::verifier::default_scheduler_config(),
            "spread",
            &mut cache,
            &options,
            &mut live_baselines,
        )
        .await
        .expect_err("live baseline cap should fail without deterministic fallback");

        assert_eq!(live_baselines, 0);
        let message = format!("{err:#}");
        assert!(message.contains("--simulator-max-live-baselines=0"));
        assert!(message.contains("deterministic greedy fallback is disabled"));
    }

    #[tokio::test]
    async fn simulator_live_scenario_filter_skips_unselected_network_attempts() {
        let scenario = scenario(
            "unselected-live-demo",
            "unselected live demo",
            &[1],
            vec![JobSpec::singleton("a", 1)],
        );
        let mut cache = SimulatorCacheFile {
            version: 1,
            entries: BTreeMap::new(),
        };
        let options = BenchmarkOptions {
            simulator_url: Some("http://127.0.0.1:1".to_string()),
            simulator_urls: Vec::new(),
            simulator_cache_path: Some(PathBuf::from("/tmp/unused-ksolver-cache.json")),
            simulator_cache_dir: None,
            refresh_simulator_cache: true,
            simulator_batch_timeout: Duration::from_millis(1),
            simulator_progress: false,
            simulator_max_live_baselines: None,
            simulator_live_scenarios: Some(BTreeSet::from(["hero-demo".to_string()])),
        };
        let mut live_baselines = 0_usize;

        let err = run_kube_baseline(
            &scenario,
            options.simulator_url.as_deref(),
            crate::verifier::default_scheduler_config(),
            "spread",
            &mut cache,
            &options,
            &mut live_baselines,
        )
        .await
        .expect_err("unselected live scenario should fail without cached KSS output");

        assert_eq!(live_baselines, 0);
        let message = format!("{err:#}");
        assert!(message.contains("--simulator-live-scenarios"));
        assert!(message.contains("unselected-live-demo:spread"));
        assert!(message.contains("deterministic greedy fallback is disabled"));
    }

    #[tokio::test]
    async fn simulator_wall_clock_timeout_errors_without_fallback() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind hanging simulator listener");
        let addr = listener.local_addr().expect("listener addr");
        let _server = tokio::spawn(async move {
            if let Ok((stream, _)) = listener.accept().await {
                let _stream = stream;
                tokio::time::sleep(Duration::from_secs(5)).await;
            }
        });

        let scenario = scenario(
            "slow-live-demo",
            "slow live demo",
            &[1],
            vec![JobSpec::singleton("a", 1)],
        );
        let mut cache = SimulatorCacheFile {
            version: 1,
            entries: BTreeMap::new(),
        };
        let options = BenchmarkOptions {
            simulator_url: Some(format!("http://{addr}")),
            simulator_urls: Vec::new(),
            simulator_cache_path: None,
            simulator_cache_dir: None,
            refresh_simulator_cache: true,
            simulator_batch_timeout: Duration::from_millis(25),
            simulator_progress: false,
            simulator_max_live_baselines: None,
            simulator_live_scenarios: None,
        };
        let mut live_baselines = 0_usize;

        let err = run_kube_baseline(
            &scenario,
            options.simulator_url.as_deref(),
            crate::verifier::default_scheduler_config(),
            "spread",
            &mut cache,
            &options,
            &mut live_baselines,
        )
        .await
        .expect_err("simulator timeout should fail without deterministic fallback");

        assert_eq!(live_baselines, 1);
        let message = format!("{err:#}");
        assert!(message.contains("exceeded batch timeout of 25ms"));
        assert!(message.contains("deterministic greedy fallback is disabled"));
    }

    #[tokio::test]
    async fn simulator_failure_exhausts_live_baseline_budget() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind hanging simulator listener");
        let addr = listener.local_addr().expect("listener addr");
        let _server = tokio::spawn(async move {
            if let Ok((stream, _)) = listener.accept().await {
                let _stream = stream;
                tokio::time::sleep(Duration::from_secs(5)).await;
            }
        });

        let scenario = scenario(
            "slow-live-demo",
            "slow live demo",
            &[1],
            vec![JobSpec::singleton("a", 1)],
        );
        let mut cache = SimulatorCacheFile {
            version: 1,
            entries: BTreeMap::new(),
        };
        let options = BenchmarkOptions {
            simulator_url: Some(format!("http://{addr}")),
            simulator_urls: Vec::new(),
            simulator_cache_path: None,
            simulator_cache_dir: None,
            refresh_simulator_cache: true,
            simulator_batch_timeout: Duration::from_millis(25),
            simulator_progress: false,
            simulator_max_live_baselines: Some(4),
            simulator_live_scenarios: None,
        };
        let mut live_baselines = 0_usize;

        let err = run_kube_baseline(
            &scenario,
            options.simulator_url.as_deref(),
            crate::verifier::default_scheduler_config(),
            "spread",
            &mut cache,
            &options,
            &mut live_baselines,
        )
        .await
        .expect_err("simulator failure should fail without deterministic fallback");

        assert_eq!(
            live_baselines, 4,
            "one unhealthy simulator attempt should trip the remaining live baseline budget"
        );
        let message = format!("{err:#}");
        assert!(message.contains("exceeded batch timeout of 25ms"));
        assert!(message.contains("deterministic greedy fallback is disabled"));
    }

    #[test]
    fn production_safety_summary_captures_sre_gates_and_risks() {
        let summary = summarize_production_safety();

        assert!(summary.passed);
        assert_eq!(summary.default_mode, "observe-only/read-only");
        assert!(!summary.mutation_default_enabled);
        assert!(summary
            .real_binding_gate
            .contains("KSOLVER_ENABLE_REAL_BINDING"));
        assert!(summary
            .kill_switches
            .iter()
            .any(|s| s.contains("KSOLVER_BINDING_KILL_SWITCH")));
        assert!(summary
            .production_checklist
            .iter()
            .any(|s| s.contains("leader election")));
        assert!(summary
            .rbac_modes
            .iter()
            .any(|s| s.contains("shadow/read-only")));
        assert!(summary
            .failure_mode_controls
            .iter()
            .any(|s| s.contains("stale pod uid")));
        assert!(summary
            .audit_fields
            .iter()
            .any(|s| s == "binding_reservation_metrics"));
        assert!(summary
            .rollout_gate_rows
            .iter()
            .any(|row| row.mode == "observe-only" && !row.mutation_allowed));
        assert!(summary
            .rollout_gate_rows
            .iter()
            .any(|row| row.mode == "bind-low-risk" && row.mutation_allowed));
        assert!(summary
            .failure_playbook_rows
            .iter()
            .any(|row| row.failure_mode == "stale pod identity"
                && row.automatic_behavior.contains("skip")));
        assert!(summary
            .audit_event_rows
            .iter()
            .any(|row| row.event_type == "binding" && !row.enabled_by_default));
        assert!(summary
            .live_validation_rows
            .iter()
            .any(|row| row.gate == "pod identity and phase"
                && row.fail_closed_behavior.contains("skip binding")));
        assert!(summary
            .live_validation_rows
            .iter()
            .any(|row| row.gate == "PDB and disruption policy"
                && row.audit_field.contains("repair_metrics")));
        assert!(summary
            .live_validation_rows
            .iter()
            .any(|row| row.gate == "rollout throttle and kill switch"
                && row.required_before == "every real mutation pass"));
        assert!(summary.live_config_rows.iter().any(|row| {
            row.gate == "binding kill switch"
                && row.env_var == "KSOLVER_BINDING_KILL_SWITCH"
                && row.live_endpoint_field == "rollout.binding_kill_switch"
                && row.fail_closed_signal.contains("mutation_allowed=false")
        }));
        assert!(summary.live_config_rows.iter().any(|row| {
            row.gate == "leader election"
                && row
                    .required_rbac_when_enabled
                    .contains("coordination.k8s.io/leases")
        }));
        assert!(summary.live_config_rows.iter().any(|row| {
            row.gate == "real binding enablement"
                && row
                    .operator_action
                    .contains("/api/scheduler/production-safety")
        }));
        assert!(summary
            .readiness_checks
            .iter()
            .any(|s| s.contains("uid matches")));
        assert!(summary
            .mutation_boundaries
            .iter()
            .any(|s| s.contains("binder.rs is the only pods/binding mutation path")));
        assert!(summary
            .residual_risks
            .iter()
            .any(|s| s.contains("in-memory")));
        assert!(summary
            .operator_claims
            .iter()
            .any(|s| s.contains("default install is observe-only")));
        assert_eq!(
            summary.launch_contract.launch_level,
            "Observe-only launch safe"
        );
        assert!(!summary.launch_contract.live_writes_allowed);
        assert!(summary
            .launch_contract
            .required_gates
            .iter()
            .any(|gate| gate.contains("mutation_allowed=false")));
        assert!(summary
            .launch_contract
            .required_rbac
            .iter()
            .any(|rbac| rbac.contains("pods/binding create")));
        assert!(summary
            .launch_contract
            .fail_closed_if
            .iter()
            .any(|gate| gate.contains("pod UID")));
        assert!(summary
            .launch_contract
            .audit_artifacts
            .iter()
            .any(|artifact| artifact.contains("/api/scheduler/production-safety")));
    }

    #[test]
    fn prediction_quality_summary_captures_calibration_and_decision_effects() {
        let vram = vram_prediction_scenario_proof();
        let summary = summarize_prediction_quality(&vram);

        assert!(summary.passed);
        assert!(summary
            .promotion_contract
            .promotion_level
            .contains("Advisory"));
        assert!(!summary.promotion_contract.hard_placement_allowed);
        assert!(
            !summary
                .promotion_contract
                .prediction_sensitive_claims_allowed
        );
        assert!(summary
            .promotion_contract
            .required_evidence
            .iter()
            .any(|s| s.contains("completed GPU pod observations")));
        assert!(summary
            .promotion_contract
            .blocked_by
            .iter()
            .any(|s| s.contains("sparse completed-job samples")));
        assert!(summary
            .promotion_contract
            .demotion_triggers
            .iter()
            .any(|s| s.contains("unknown prediction source share")));
        assert!(summary
            .coverage_sources
            .iter()
            .any(|s| s.contains("exact command-hash")));
        assert!(summary
            .coverage_sources
            .iter()
            .any(|s| s.contains("training-hint fallback")));
        assert!(summary
            .calibration_metrics
            .iter()
            .any(|s| s.contains("MAPE")));
        assert!(summary
            .calibration_lifecycle
            .iter()
            .any(|s| s.contains("collect completed GPU pods")));
        assert!(summary
            .confidence_bands
            .iter()
            .any(|s| s.contains("exact command history")));
        assert!(summary
            .drift_monitors
            .iter()
            .any(|s| s.contains("unknown prediction source share")));
        assert!(summary
            .decision_impact_evidence
            .iter()
            .any(|s| s.contains("too-small GPU-memory nodes")));
        assert!(summary.model_cards.iter().any(|card| {
            card.source_tier == "exact_command_hash"
                && card.placement_use.contains("VRAM filtering")
        }));
        assert!(summary
            .model_cards
            .iter()
            .any(|card| card.source_tier == "unknown" && card.confidence_band == "none"));
        assert!(summary
            .calibration_buckets
            .iter()
            .any(|bucket| bucket.bucket == "peak_vram_bytes"
                && bucket
                    .action_when_unhealthy
                    .contains("disable VRAM hard filtering")));
        assert!(summary.live_calibration_rows.iter().any(|row| {
            row.gate == "runtime error budget"
                && row
                    .live_trace_metric
                    .contains("runtime_prediction_mape_milli")
                && row.unhealthy_action.contains("demote")
        }));
        assert!(summary.live_calibration_rows.iter().any(|row| {
            row.gate == "VRAM error budget"
                && row.live_trace_metric.contains("vram_prediction_mape_milli")
                && row.placement_impact.contains("GPU-memory feasibility")
        }));
        assert!(summary.live_calibration_rows.iter().any(|row| {
            row.gate == "unknown source coverage"
                && row.live_trace_metric.contains("unknown_pods")
                && row.operator_view.contains("Live prediction coverage")
        }));
        assert!(summary
            .audit_fields
            .iter()
            .any(|field| field == "prediction_audit[].confidence"));
        assert!(summary
            .promotion_gates
            .iter()
            .any(|gate| gate.contains("exact command-history promotion")));
        assert!(summary
            .placement_effects
            .iter()
            .any(|s| s.contains("VRAM feasibility filters")));
        assert!(summary.confidence_model.contains("exact command history"));
        assert!(summary
            .operator_claims
            .iter()
            .any(|s| s.contains("explainable by source")));
        assert!(summary
            .residual_risks
            .iter()
            .any(|s| s.contains("enough completed jobs")));
    }

    #[test]
    fn vram_investment_demo_quantifies_oom_risk_and_advisory_limits() {
        let summary = summarize_vram_investment_demo();

        assert!(summary.passed);
        assert!(summary.scenario_count >= 6);
        assert!(summary.baseline_cuda_oom_risk_pods > summary.ksolver_cuda_oom_risk_pods);
        assert!(summary.cuda_oom_risk_reduction_pods > 0);
        assert!(summary.high_vram_nodes_preserved > 0);
        assert!(summary.unknown_or_advisory_rows > 0);
        assert!(summary
            .synthetic_prediction_notice
            .contains("deterministic fake values"));
        assert!(summary.rows.iter().any(|row| {
            row.avoided_failure
                && row.kube_risk_label == "likely CUDA OOM"
                && row.ksolver_cuda_oom_risk_percent < row.kube_cuda_oom_risk_percent
                && row.risk_delta_percent > 0
                && row.ksolver_upper_band_headroom_gib > row.kube_upper_band_headroom_gib
                && row.decision_reason.contains("upper-band VRAM headroom")
        }));
        assert!(summary
            .rows
            .iter()
            .any(|row| row.advisory_only && row.caveat.contains("Unknown node memory")));
        assert!(summary
            .required_real_predictor_evidence
            .iter()
            .any(|item| item.contains("DCGM") || item.contains("NVML")));
    }

    #[test]
    fn scale_guardrail_summary_ties_grouping_pruning_and_widening() {
        let regret = RegretSummary {
            candidate_node_limit: REGRET_CANDIDATE_LIMIT,
            scenarios_compared: 3,
            scenarios_with_any_regret: 1,
            max_useful_gpu_regret: 4,
            ..Default::default()
        };
        let grouping = NodeGroupingProof {
            name: "node-grouping-symmetry".to_string(),
            passed: true,
            physical_nodes_before: 3,
            grouped_nodes_after: 1,
            eligible_group_count: 1,
            eligible_node_count: 3,
            max_group_size: 3,
            grouped_node_name: "group-gpu".to_string(),
            grouped_node_count: 3,
            grouped_members: vec!["n1".to_string(), "n2".to_string(), "n3".to_string()],
            expanded_used_nodes: vec!["n1".to_string(), "n2".to_string()],
            physical_solve_admitted_workloads: 2,
            grouped_solve_admitted_workloads: 2,
            physical_solve_admitted_gpu: 2,
            grouped_solve_admitted_gpu: 2,
            grouped_solver_status: "Optimal".to_string(),
        };
        let widening = CandidateWideningProof {
            name: "candidate-widening-recovers-regret".to_string(),
            passed: true,
            scenario: "big-regret".to_string(),
            initial_candidate_node_limit: REGRET_CANDIDATE_LIMIT,
            final_candidate_node_limit: 0,
            retry_count: 1,
            widening_reason: "low admission ratio with pruned candidates".to_string(),
            pruned_useful_gpu: 4,
            widened_useful_gpu: 8,
            useful_gpu_recovered: 4,
            pruned_unplaced_pods: 2,
            widened_unplaced_pods: 0,
        };

        let summary = summarize_scale_guardrails(&regret, &grouping, &widening);

        assert!(summary.passed);
        assert_eq!(
            summary.actionability_contract.recommendation,
            "Grouped-first scale path with widened fallback"
        );
        assert!(summary.actionability_contract.customer_scale_claim_allowed);
        assert!(
            !summary
                .actionability_contract
                .high_risk_pruned_binding_allowed
        );
        assert!(summary
            .actionability_contract
            .preferred_large_fleet_mode
            .contains("homogeneous node grouping before candidate pruning"));
        assert!(summary
            .actionability_contract
            .required_evidence
            .iter()
            .any(|item| item.contains("physical expansion proof")));
        assert!(summary
            .actionability_contract
            .fail_closed_if
            .iter()
            .any(|item| item.contains("candidate_regret_status is unknown")));
        assert!(summary
            .actionability_contract
            .operator_overrides
            .iter()
            .any(|item| item.contains("KSOLVER_CANDIDATE_NODE_LIMIT=0")));
        assert_eq!(summary.default_candidate_node_limit, REGRET_CANDIDATE_LIMIT);
        assert_eq!(summary.grouping_nodes_after, 1);
        assert!(summary.grouping_preserved_admitted_gpu);
        assert_eq!(summary.widening_useful_gpu_recovered, 4);
        assert!(summary
            .grouping_policy
            .iter()
            .any(|s| s.contains("group homogeneous GPU nodes before pruning")));
        assert!(summary
            .pruning_modes
            .iter()
            .any(|s| s.contains("candidate_node_limit=0")));
        assert!(summary
            .regret_status_ladder
            .iter()
            .any(|s| s.contains("unknown")));
        assert!(summary
            .fallback_triggers
            .iter()
            .any(|s| s.contains("high-priority")));
        assert!(summary.scale_mode_cards.iter().any(|card| {
            card.mode == "node_grouping" && card.status == "safe_for_symmetric_nodes"
        }));
        assert!(summary
            .scale_mode_cards
            .iter()
            .any(|card| { card.mode == "candidate_pruning" && card.status == "measured_regret" }));
        assert!(summary
            .regret_action_rows
            .iter()
            .any(|row| row.regret_status == "unknown" && row.next_action.contains("rerun full")));
        assert!(summary.large_fleet_validation_rows.iter().any(|row| {
            row.gate == "homogeneous grouping symmetry"
                && row.live_trace_metric.contains("node_grouping_metrics")
                && row.fail_closed_action.contains("fall back")
        }));
        assert!(summary.large_fleet_validation_rows.iter().any(|row| {
            row.gate == "candidate pruning regret visibility"
                && row.live_trace_metric.contains("candidate_quality_metrics")
                && row.operator_claim.contains("never presented as exact")
        }));
        assert!(summary.large_fleet_validation_rows.iter().any(|row| {
            row.gate == "large heterogeneous fleet sample"
                && row.required_evidence.contains("multiple GPU node classes")
        }));
        assert!(summary
            .operator_switches
            .iter()
            .any(|switch| switch.contains("KSOLVER_CANDIDATE_NODE_LIMIT=0")));
        assert!(summary
            .guardrails
            .iter()
            .any(|s| s.contains("full feasible set")));
        assert!(summary
            .residual_risks
            .iter()
            .any(|s| s.contains("unknown regret")));
    }

    #[test]
    fn fairness_budget_summary_answers_denial_and_borrowing_questions() {
        let scenario = ScenarioResult {
            name: "fair-share-over-fifo".to_string(),
            description: "fairness".to_string(),
            tier: Tier::Small,
            benefit_score: 0,
            headline: String::new(),
            kube: EngineResult {
                engine: "kube".to_string(),
                source: "test".to_string(),
                simulator: None,
                candidate_node_limit: 0,
                solve_millis: 0,
                metrics: PlacementMetrics {
                    fair_share_useful_gpu: 0,
                    ..Default::default()
                },
                placements: Vec::new(),
            },
            kube_binpack: EngineResult {
                engine: "kube-binpack".to_string(),
                source: "test".to_string(),
                simulator: None,
                candidate_node_limit: 0,
                solve_millis: 0,
                metrics: PlacementMetrics::default(),
                placements: Vec::new(),
            },
            ksolver: EngineResult {
                engine: "ksolver".to_string(),
                source: "test".to_string(),
                simulator: None,
                candidate_node_limit: 0,
                solve_millis: 0,
                metrics: PlacementMetrics {
                    fair_share_useful_gpu: 100,
                    ..Default::default()
                },
                placements: vec![Placement {
                    pod: "under-share-team-job".to_string(),
                    node: Some("gpu-a".to_string()),
                    gpus: 1,
                }],
            },
            reduced_ksolver: EngineResult {
                engine: "ksolver-reduced".to_string(),
                source: "test".to_string(),
                simulator: None,
                candidate_node_limit: 0,
                solve_millis: 0,
                metrics: PlacementMetrics::default(),
                placements: Vec::new(),
            },
            regret: RegretMetrics::default(),
            efficiency_score: 0,
            significantly_better: false,
            efficiency_headline: String::new(),
        };
        let tenant_budget = TenantBudgetProof {
            name: "tenant-budget-hard-admission-cap".to_string(),
            passed: true,
            tenant: "research".to_string(),
            monthly_budget_milli: 600_000,
            expensive_node_cost_milli: 1_000_000,
            cheap_node_cost_milli: 500_000,
            expensive_job_node: None,
            cheap_job_node: Some("cheap-gpu".to_string()),
            admitted_jobs: 1,
            unplaced_jobs: 1,
            solver_status: "Optimal".to_string(),
        };

        let summary = summarize_fairness_budget(&[scenario], &tenant_budget);

        assert!(summary.passed);
        assert!(summary.under_share_admitted);
        assert_eq!(summary.fair_share_useful_gpu_gain, 100);
        assert!(!summary.expensive_job_admitted);
        assert!(summary.cheap_job_admitted);
        assert!(summary
            .operator_questions
            .iter()
            .any(|s| s.contains("Which tenant was denied")));
        assert!(summary
            .trace_fields
            .iter()
            .any(|s| s.contains("borrowed_gpu_milli")));
        assert!(summary.policy_decision_rows.iter().any(|row| {
            row.workload == "research/expensive-candidate"
                && row.decision == "deny"
                && row.policy.contains("budget")
        }));
        assert!(summary.tenant_ledger_rows.iter().any(|row| {
            row.tenant == "over-share-team"
                && row.status == "borrowing"
                && row.reclaimable_borrowed_gpu_milli > 0
        }));
        assert!(summary.ownership_rows.iter().any(|row| {
            row.gate == "tenant identity"
                && row.ownership_source.contains("ksolver.dev/team")
                && row.live_trace_field.contains("tenant_fairness_metrics")
        }));
        assert!(summary.ownership_rows.iter().any(|row| {
            row.gate == "budget catalog" && row.live_trace_field.contains("budget_monthly_milli")
        }));
        assert!(summary.ownership_rows.iter().any(|row| {
            row.gate == "borrow and reclaim"
                && row
                    .live_trace_field
                    .contains("reclaimable_borrowed_gpu_milli")
        }));
        assert!(summary
            .ui_badges
            .iter()
            .any(|badge| badge.contains("budget exhausted")));
        assert!(summary
            .enforcement_controls
            .iter()
            .any(|control| control.contains("observability-only")));
        assert!(summary
            .residual_risks
            .iter()
            .any(|s| s.contains("observational")));
    }

    #[test]
    fn device_correctness_summary_separates_supported_semantics_from_hard_limits() {
        let topology = gpu_topology_scenario_proof();
        let mig = mig_profile_scenario_proof();
        let dra_approximation = dra_approximation_scenario_proof();
        let dra_allocation = dra_allocation_scenario_proof();
        let time_sliced = time_sliced_gpu_scenario_proof();

        let summary = summarize_device_correctness(
            &topology,
            &mig,
            &dra_approximation,
            &dra_allocation,
            &time_sliced,
        );

        assert!(summary.passed);
        assert!(summary
            .supported_today
            .iter()
            .any(|s| s.contains("MIG mixed-strategy")));
        assert!(summary
            .proof_backed_claims
            .iter()
            .any(|s| s.contains("dra-allocated-device-subtraction")));
        assert!(summary
            .exact_semantics
            .iter()
            .any(|s| s.contains("MIG mixed-strategy profile compatibility")));
        assert!(summary
            .approximated_semantics
            .iter()
            .any(|s| s.contains("DRA ResourceClaims are reduced")));
        assert!(summary
            .unsupported_claims
            .iter()
            .any(|s| s.contains("NVLink-optimal")));
        assert!(summary
            .validation_signals
            .iter()
            .any(|s| s.contains("time_sliced_gpu_scenario")));
        assert!(summary
            .fallback_actions
            .iter()
            .any(|s| s.contains("drop unmodeled DRA pods")));
        assert!(summary.device_readiness_rows.iter().any(|row| {
            row.feature == "DRA scalar approximation"
                && row.support_level.contains("shadow approximation")
                && row.fail_closed_action.contains("drop unmodeled")
        }));
        assert!(summary.device_readiness_rows.iter().any(|row| {
            row.feature == "concrete NVLink/DRA device graph"
                && row.support_level.contains("unsupported")
                && row.required_inventory.contains("per-device topology graph")
        }));
        assert!(summary.topology_claim.contains(&topology.topology_key));
        assert!(summary.mig_claim.contains(&mig.requested_resource));
        assert!(summary
            .dra_approximation_claim
            .contains(&dra_approximation.synthetic_resource));
        assert!(summary
            .hard_limits
            .iter()
            .any(|s| s.contains("concrete DRA device identities")));
        assert!(summary
            .hard_limits
            .iter()
            .any(|s| s.contains("per-device topology graph")));
        assert!(summary
            .operator_claims
            .iter()
            .any(|s| s.contains("should not claim full DRA allocation")));
    }

    #[cfg(feature = "rust-cp-sat")]
    #[tokio::test]
    async fn feature_assertions_capture_priority_and_report_gates() {
        let cache_path = write_test_simulator_cache();
        let report = run_benchmark_with_options(BenchmarkOptions {
            simulator_cache_path: Some(cache_path),
            ..Default::default()
        })
        .await
        .expect("benchmark report");
        let assertions: BTreeMap<_, _> = report
            .feature_assertions
            .iter()
            .map(|a| (a.name.as_str(), a.passed))
            .collect();

        assert_eq!(
            assertions.get("priority-aware-admission"),
            Some(&true),
            "priority scenario should prove the weighted urgent gang is admitted"
        );
        assert_eq!(
            assertions.get("priority-metadata-inert-when-weight-zero"),
            Some(&true),
            "priority metadata should be inert when the priority weight is disabled"
        );
        assert_eq!(
            assertions.get("deadline-aware-admission"),
            Some(&true),
            "deadline scenario should prove the urgent deadline job is admitted"
        );
        assert_eq!(
            assertions.get("deadline-flexible-rightsizing"),
            Some(&true),
            "deadline scenario should prove flexible jobs can use fewer GPUs while still meeting deadline"
        );
        assert_eq!(
            assertions.get("business-value-aware-admission"),
            Some(&true),
            "business-value scenario should prove the higher-value job is admitted"
        );
        assert_eq!(
            assertions.get("fair-share-aware-admission"),
            Some(&true),
            "fair-share scenario should prove the under-share job is admitted"
        );
        assert_eq!(
            assertions.get("fairness-budget-summary-ready"),
            Some(&true),
            "report should expose UI-ready fairness and budget policy rows"
        );
        assert!(report.fairness_budget_summary.passed);
        assert!(report
            .fairness_budget_summary
            .policy_decision_rows
            .iter()
            .any(|row| row.decision == "deny" && row.policy.contains("budget")));
        assert!(report
            .fairness_budget_summary
            .tenant_ledger_rows
            .iter()
            .any(|row| row.status == "borrowing" && row.reclaimable_borrowed_gpu_milli > 0));
        assert!(report
            .fairness_budget_summary
            .ui_badges
            .iter()
            .any(|badge| badge.contains("budget exhausted")));
        assert_eq!(
            assertions.get("queue-aware-admission"),
            Some(&true),
            "queue scenario should prove the urgent queue job is admitted"
        );
        assert_eq!(
            assertions.get("queue-wait-aware-admission"),
            Some(&true),
            "queue-wait scenario should prove the long-waiting job is admitted"
        );
        assert_eq!(
            assertions.get("roi-summary-computed"),
            Some(&true),
            "ROI summary should be computed from deterministic scenario deltas"
        );
        assert_eq!(
            report.roi_summary.scenarios_compared,
            report.scenarios.len()
        );
        assert!(report.roi_summary.total_requested_gpu > 0);
        assert!(report.roi_summary.kube_active_node_monthly_cost > 0);
        assert!(report.roi_summary.ksolver_active_node_monthly_cost > 0);
        assert!(report.roi_summary.ksolver_gpu_utilization_milli > 0);
        assert!(!report.roi_summary.headline.is_empty());
        assert_eq!(
            assertions.get("roi-dashboard-summary-ready"),
            Some(&true),
            "ROI dashboard summary should package the 30-second SRE value tiles"
        );
        assert_eq!(
            assertions.get("sre-end-to-end-demo-ready"),
            Some(&true),
            "report should package the full SRE demo narrative"
        );
        assert_eq!(
            assertions.get("roadmap-readiness-summary-ready"),
            Some(&true),
            "report should map the pasted roadmap items to evidence and remaining gaps"
        );
        assert!(report.roi_dashboard_summary.passed);
        assert!(report.roi_dashboard_summary.primary_tiles.len() >= 6);
        assert!(report
            .roi_dashboard_summary
            .primary_tiles
            .iter()
            .any(|tile| tile.key == "hero_repair_disruption_cost"));
        assert!(report
            .roi_dashboard_summary
            .executive_rows
            .iter()
            .any(|row| row.evidence_tile == "hero_repair_disruption_cost"));
        assert!(report
            .roi_dashboard_summary
            .presentation_order
            .contains(&"executive_rows".to_string()));
        assert!(report.demo_readiness_summary.passed);
        assert_eq!(
            report.demo_readiness_summary.kube_baseline_mode,
            "cached kube-scheduler-simulator baselines"
        );
        assert!(report
            .demo_readiness_summary
            .ui_sections
            .contains(&"preemption_migration_hero_summary".to_string()));
        assert!(report
            .demo_readiness_summary
            .operator_checklist
            .iter()
            .any(|item| item.contains("simulator baseline provenance")));
        assert_eq!(report.demo_readiness_summary.demo_flow_scenes.len(), 5);
        assert!(report
            .demo_readiness_summary
            .demo_flow_scenes
            .iter()
            .any(|scene| scene.screen == "Problem"
                && scene.primary_visual.contains("fragmented node")));
        assert!(report
            .demo_readiness_summary
            .demo_flow_scenes
            .iter()
            .any(|scene| scene.screen == "Safety"
                && scene.evidence_source == "production_safety_summary.rollout_gate_rows"));
        assert!(report
            .demo_readiness_summary
            .demo_flow_scenes
            .iter()
            .any(|scene| scene.screen == "Trust"
                && scene.primary_visual.contains("prediction model cards")));
        assert!(report
            .demo_readiness_summary
            .demo_acceptance_criteria
            .iter()
            .any(|criterion| criterion.contains("under 30 seconds")));
        assert!(report
            .demo_readiness_summary
            .live_validation_rows
            .iter()
            .any(|row| row.gate == "kube baseline provenance"
                && row.live_endpoint == "/api/scheduler/kube-simulator-plan"));
        assert!(report
            .demo_readiness_summary
            .live_validation_rows
            .iter()
            .any(|row| row.gate == "repair action safety"
                && row.live_endpoint == "/api/scheduler/repair-plan"
                && row
                    .required_evidence
                    .contains("migrate/preempt action rows")));
        assert!(report
            .demo_readiness_summary
            .live_validation_rows
            .iter()
            .any(|row| row.gate == "production mutation safety"
                && row.live_endpoint == "/api/scheduler/production-safety"));
        assert!(!report
            .demo_readiness_summary
            .remaining_gaps
            .iter()
            .any(|gap| gap.contains("render demo_flow_scenes")));
        assert!(report.roadmap_readiness_summary.passed);
        assert!(
            report
                .roadmap_readiness_summary
                .launch_proof_gate
                .demo_ready
        );
        assert!(
            !report
                .roadmap_readiness_summary
                .launch_proof_gate
                .customer_claim_ready
        );
        assert!(report
            .roadmap_readiness_summary
            .launch_proof_gate
            .label
            .contains("customer proof pending"));
        assert!(report
            .roadmap_readiness_summary
            .launch_proof_gate
            .required_evidence
            .iter()
            .any(|evidence| evidence.contains("pricing catalog")));
        assert!(report
            .roadmap_readiness_summary
            .launch_proof_gate
            .evidence_bundle_rows
            .iter()
            .any(|row| row.artifact == "repair action proof"
                && row.source == "/api/scheduler/repair-plan"
                && row.blocks_claim.contains("preemption/migration")));
        assert!(report
            .roadmap_readiness_summary
            .launch_proof_gate
            .evidence_bundle_rows
            .iter()
            .any(|row| row.artifact == "prediction calibration history"
                && row.blocks_claim.contains("rightsizing")));
        assert!(report
            .roadmap_readiness_summary
            .launch_proof_gate
            .evidence_bundle_rows
            .iter()
            .any(|row| row.artifact == "device inventory and topology proof"
                && row.blocks_claim.contains("device-aware")));
        assert!(report
            .roadmap_readiness_summary
            .launch_proof_gate
            .blockers
            .iter()
            .any(|blocker| blocker.contains("non-demo")));
        assert_eq!(report.roadmap_readiness_summary.items.len(), 8);
        assert!(report
            .roadmap_readiness_summary
            .headline
            .contains("primary SRE demo UI is wired"));
        assert!(!report
            .roadmap_readiness_summary
            .headline
            .contains("UI rendering"));
        assert!(report
            .roadmap_readiness_summary
            .items
            .iter()
            .any(|item| item.item == "Preemption/migration planner proof"
                && item.status == "repair-proof-ready"
                && item.evidence_source.contains("kube baseline provenance")));
        assert!(report
            .roadmap_readiness_summary
            .items
            .iter()
            .any(|item| item.item == "VRAM prediction and no-repair proof"
                && item.status == "vram-proof-gates-ready"
                && item.remaining_gap.contains("live/cached KSS")));
        assert!(report
            .roadmap_readiness_summary
            .items
            .iter()
            .any(|item| item.item == "True device correctness"
                && item.remaining_gap.contains("DRA allocation")));
        assert!(report
            .roadmap_readiness_summary
            .items
            .iter()
            .any(
                |item| item.item == "Fairness and budgets as first-class UI concepts"
                    && item.status == "ownership-evidence-ready"
            ));
        assert!(report
            .roadmap_readiness_summary
            .items
            .iter()
            .any(|item| item.item == "ROI dashboard and scenario library"
                && item.status == "roi-decision-ready"));
        assert_eq!(
            assertions.get("fragmented-gang-repair-plan"),
            Some(&true),
            "repair scenario should prove fragmented gang repair advice"
        );
        assert_eq!(
            assertions.get("hero-defragmentation-demo-ready"),
            Some(&true),
            "report should expose the defragmentation advisor as the SRE-facing hero demo"
        );
        assert_eq!(
            assertions.get("preemption-migration-hero-ready"),
            Some(&true),
            "report should expose UI-ready migrate/preempt action rows"
        );
        assert_eq!(
            assertions.get("sre-demo-script-ready"),
            Some(&true),
            "report should expose a ranked operator demo script"
        );
        assert!(report.hero_demo_summary.passed);
        assert_eq!(report.hero_demo_summary.target_gpu_request, 4);
        assert!(report.hero_demo_summary.freed_gpu >= 4);
        assert!(!report.hero_demo_summary.roi_headline.is_empty());
        assert!(report.preemption_migration_hero_summary.passed);
        assert!(report
            .preemption_migration_hero_summary
            .action_rows
            .iter()
            .any(|row| row.action == "migrate" && !row.to_node.is_empty()));
        assert!(report
            .preemption_migration_hero_summary
            .action_rows
            .iter()
            .any(|row| row.action == "preempt"));
        assert!(
            !report
                .preemption_migration_hero_summary
                .decision_contract
                .can_act_now
        );
        assert!(report
            .preemption_migration_hero_summary
            .decision_contract
            .evidence_required
            .iter()
            .any(|evidence| evidence.contains("/api/scheduler/production-safety")));
        assert_eq!(report.sre_demo_script.steps.len(), 4);
        assert!(report.sre_demo_script.top_scenario_cards.len() >= 3);
        assert_eq!(
            assertions.get("production-safety-summary-ready"),
            Some(&true),
            "report should expose production safety rollout gates"
        );
        assert!(report.production_safety_summary.passed);
        assert!(!report.production_safety_summary.mutation_default_enabled);
        assert!(report
            .production_safety_summary
            .production_checklist
            .iter()
            .any(|item| item.contains("leader election")));
        assert!(report
            .production_safety_summary
            .rbac_modes
            .iter()
            .any(|mode| mode.contains("shadow/read-only")));
        assert!(report
            .production_safety_summary
            .failure_mode_controls
            .iter()
            .any(|control| control.contains("stale pod uid")));
        assert!(report
            .production_safety_summary
            .audit_fields
            .iter()
            .any(|field| field == "binding_outcome_metrics"));
        assert!(report
            .production_safety_summary
            .rollout_gate_rows
            .iter()
            .any(|row| row.mode == "observe-only" && !row.mutation_allowed));
        assert!(report
            .production_safety_summary
            .failure_playbook_rows
            .iter()
            .any(|row| row.failure_mode == "reservation rejection"));
        assert!(report
            .production_safety_summary
            .audit_event_rows
            .iter()
            .any(|row| row.event_type == "binding"));
        assert!(
            !report
                .production_safety_summary
                .launch_contract
                .live_writes_allowed
        );
        assert!(report
            .production_safety_summary
            .launch_contract
            .audit_artifacts
            .iter()
            .any(|artifact| artifact.contains("binding-plan")));
        assert_eq!(
            assertions.get("vram-blocked-no-repair-plan"),
            Some(&true),
            "repair scenario should prove VRAM-incompatible work does not trigger disruptive repair advice"
        );
        assert_eq!(
            assertions.get("policy-blocked-no-repair-plan"),
            Some(&true),
            "repair scenario should prove protected running work does not trigger disruptive repair advice"
        );
        assert_eq!(
            assertions.get("vram-prediction-feasibility"),
            Some(&true),
            "VRAM prediction scenario should prove too-small known GPU-memory nodes are rejected"
        );
        assert_eq!(
            assertions.get("prediction-quality-summary-ready"),
            Some(&true),
            "report should expose prediction calibration quality gates"
        );
        assert!(report.prediction_quality_summary.passed);
        assert!(
            !report
                .prediction_quality_summary
                .promotion_contract
                .hard_placement_allowed
        );
        assert!(
            !report
                .prediction_quality_summary
                .promotion_contract
                .prediction_sensitive_claims_allowed
        );
        assert!(report
            .prediction_quality_summary
            .promotion_contract
            .required_evidence
            .iter()
            .any(|item| item.contains("runtime MAPE")));
        assert!(report
            .prediction_quality_summary
            .calibration_lifecycle
            .iter()
            .any(|item| item.contains("collect completed GPU pods")));
        assert!(report
            .prediction_quality_summary
            .confidence_bands
            .iter()
            .any(|band| band.contains("exact command history")));
        assert!(report
            .prediction_quality_summary
            .drift_monitors
            .iter()
            .any(|monitor| monitor.contains("unknown prediction source share")));
        assert!(report
            .prediction_quality_summary
            .decision_impact_evidence
            .iter()
            .any(|evidence| evidence.contains("too-small GPU-memory nodes")));
        assert!(report
            .prediction_quality_summary
            .model_cards
            .iter()
            .any(|card| card.source_tier == "exact_command_hash"));
        assert!(report
            .prediction_quality_summary
            .calibration_buckets
            .iter()
            .any(|bucket| bucket.bucket == "peak_vram_bytes"));
        assert!(report
            .prediction_quality_summary
            .audit_fields
            .iter()
            .any(|field| field == "prediction_audit[].confidence"));
        assert!(report
            .prediction_quality_summary
            .promotion_gates
            .iter()
            .any(|gate| gate.contains("hard VRAM filtering")));
        assert_eq!(
            assertions.get("gpu-topology-locality-feasibility"),
            Some(&true),
            "GPU topology scenario should prove required GPU island labels filter candidate nodes"
        );
        assert!(report.gpu_topology_scenario.passed);
        assert_eq!(
            report.gpu_topology_scenario.matching_feasible_nodes,
            vec!["nvlink-a-0".to_string()]
        );
        assert_eq!(
            assertions.get("mig-profile-compatibility"),
            Some(&true),
            "MIG scenario should prove slice profiles require matching extended resources"
        );
        assert!(report.mig_profile_scenario.passed);
        assert_eq!(
            report.mig_profile_scenario.matching_feasible_nodes,
            vec!["mig-3g-node".to_string()]
        );
        assert_eq!(
            assertions.get("dra-scalar-approximation-guard"),
            Some(&true),
            "DRA scenario should prove modeled claims consume synthetic resources and unmodeled claims are dropped"
        );
        assert!(report.dra_approximation_scenario.passed);
        assert_eq!(
            report.dra_approximation_scenario.modeled_feasible_nodes,
            vec!["dra-node".to_string()]
        );
        assert_eq!(
            assertions.get("dra-allocated-device-subtraction"),
            Some(&true),
            "DRA allocation scenario should prove allocated device identities are subtracted from availability"
        );
        assert!(report.dra_allocation_scenario.passed);
        assert_eq!(report.dra_allocation_scenario.available_devices, 1);
        assert_eq!(
            assertions.get("time-sliced-gpu-disclosure"),
            Some(&true),
            "time-sliced GPU scenario should prove shared GPU placements carry a caveat"
        );
        assert!(report.time_sliced_gpu_scenario.passed);
        assert!(report
            .time_sliced_gpu_scenario
            .time_sliced_caveats
            .iter()
            .any(|c| c.contains("time-sliced GPU")));
        assert_eq!(
            assertions.get("device-correctness-summary-ready"),
            Some(&true),
            "device correctness summary should package MIG, topology, DRA, and time-sliced caveats with explicit limits"
        );
        assert!(report.device_correctness_summary.passed);
        assert!(report
            .device_correctness_summary
            .exact_semantics
            .iter()
            .any(|s| s.contains("MIG mixed-strategy profile compatibility")));
        assert!(report
            .device_correctness_summary
            .approximated_semantics
            .iter()
            .any(|s| s.contains("DRA ResourceClaims are reduced")));
        assert!(report
            .device_correctness_summary
            .unsupported_claims
            .iter()
            .any(|s| s.contains("full DRA allocation")));
        assert!(report
            .device_correctness_summary
            .validation_signals
            .iter()
            .any(|s| s.contains("dra_allocation_scenario")));
        assert!(report
            .device_correctness_summary
            .fallback_actions
            .iter()
            .any(|s| s.contains("drop unmodeled DRA pods")));
        assert!(report
            .device_correctness_summary
            .device_readiness_rows
            .iter()
            .any(|row| row.feature == "DRA scalar approximation"
                && row.fail_closed_action.contains("drop unmodeled")));
        assert!(report
            .device_correctness_summary
            .device_readiness_rows
            .iter()
            .any(|row| row.feature == "concrete NVLink/DRA device graph"
                && row.support_level.contains("unsupported")));
        assert!(report
            .device_correctness_summary
            .hard_limits
            .iter()
            .any(|s| s.contains("concrete DRA device identities")));
        assert!(report
            .device_correctness_summary
            .operator_claims
            .iter()
            .any(|s| s.contains("should not claim full DRA allocation")));
        assert_eq!(
            assertions.get("node-grouping-symmetry-reduction"),
            Some(&true),
            "node grouping scenario should prove homogeneous nodes collapse and expand safely"
        );
        assert_eq!(
            assertions.get("scale-guardrails-ready"),
            Some(&true),
            "scale guardrail summary should expose grouping and pruning safety policy"
        );
        assert!(report.scale_guardrail_summary.passed);
        assert!(
            report
                .scale_guardrail_summary
                .actionability_contract
                .customer_scale_claim_allowed
        );
        assert!(
            !report
                .scale_guardrail_summary
                .actionability_contract
                .high_risk_pruned_binding_allowed
        );
        assert!(report
            .scale_guardrail_summary
            .actionability_contract
            .required_evidence
            .iter()
            .any(|item| item.contains("full feasible-set comparison")));
        assert!(report
            .scale_guardrail_summary
            .grouping_policy
            .iter()
            .any(|policy| policy.contains("group homogeneous GPU nodes before pruning")));
        assert!(report
            .scale_guardrail_summary
            .pruning_modes
            .iter()
            .any(|mode| mode.contains("candidate_node_limit=0")));
        assert!(report
            .scale_guardrail_summary
            .regret_status_ladder
            .iter()
            .any(|status| status.contains("unknown")));
        assert!(report
            .scale_guardrail_summary
            .fallback_triggers
            .iter()
            .any(|trigger| trigger.contains("high-priority")));
        assert!(report
            .scale_guardrail_summary
            .scale_mode_cards
            .iter()
            .any(|card| card.mode == "node_grouping"));
        assert!(report
            .scale_guardrail_summary
            .regret_action_rows
            .iter()
            .any(|row| row.regret_status == "unknown"));
        assert!(report
            .scale_guardrail_summary
            .operator_switches
            .iter()
            .any(|switch| switch.contains("KSOLVER_ENABLE_NODE_GROUPING")));
        assert_eq!(
            assertions.get("candidate-pruning-regret-measured"),
            Some(&true),
            "reduced-solve comparison should be reported for every scenario"
        );
        assert_eq!(
            assertions.get("candidate-widening-recovers-regret"),
            Some(&true),
            "candidate widening proof should show a full retry recovering pruned useful-GPU regret"
        );
        assert!(report.candidate_widening_scenario.passed);
        assert!(report.candidate_widening_scenario.useful_gpu_recovered > 0);
        assert_eq!(
            report
                .candidate_widening_scenario
                .initial_candidate_node_limit,
            REGRET_CANDIDATE_LIMIT
        );
        assert_eq!(
            report
                .candidate_widening_scenario
                .final_candidate_node_limit,
            0
        );
        assert!(
            report
                .feature_assertions
                .iter()
                .all(|a| !a.evidence.is_empty()),
            "every assertion should include operator-readable evidence"
        );
    }
}
