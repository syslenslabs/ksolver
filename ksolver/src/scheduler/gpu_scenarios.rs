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
use serde::Serialize;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::time::Instant;

const GPU_RESOURCE: &str = "nvidia.com/gpu";
const REGRET_CANDIDATE_LIMIT: usize = 2;
/// Synthetic monthly $ per GPU on a node — a node's cost scales with its GPU count (realistic for
/// GPU instances). Absolute value is irrelevant to the ranking; only relative cost across
/// schedulers on the SAME fleet matters.
const GPU_MONTHLY_PER_GPU: i64 = 2000;

#[derive(Debug, Clone)]
struct GpuNodeSpec {
    name: String,
    gpus: i64,
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

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
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
    #[serde(default)]
    pub candidate_node_limit: usize,
    pub solve_millis: u64,
    pub metrics: PlacementMetrics,
    pub placements: Vec<Placement>,
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
    pub sorted_by: String,
    pub benefit_summary: BenefitSummary,
    pub roi_summary: RoiSummary,
    pub regret_summary: RegretSummary,
    pub repair_scenario: RepairScenarioProof,
    pub repair_scenarios: Vec<RepairScenarioProof>,
    pub vram_prediction_scenario: VramPredictionProof,
    pub gpu_topology_scenario: GpuTopologyProof,
    pub mig_profile_scenario: MigProfileProof,
    pub dra_approximation_scenario: DraApproximationProof,
    pub dra_allocation_scenario: DraAllocationProof,
    pub time_sliced_gpu_scenario: TimeSlicedGpuProof,
    pub node_grouping_scenario: NodeGroupingProof,
    pub tenant_budget_scenario: TenantBudgetProof,
    pub candidate_widening_scenario: CandidateWideningProof,
    pub feature_assertions: Vec<FeatureAssertion>,
    pub scenarios: Vec<ScenarioResult>,
}

pub async fn run_benchmark(simulator_url: Option<&str>) -> anyhow::Result<BenchmarkReport> {
    let mut results = Vec::new();
    let sim_url = simulator_url.filter(|u| !u.trim().is_empty());
    for scenario in deterministic_scenarios() {
        let tier = scenario.tier;
        // Baseline 1: default kube-scheduler (LeastAllocated / spread).
        let kube = match sim_url {
            Some(url) => run_kube_simulator(
                &scenario,
                url,
                crate::verifier::default_scheduler_config(),
                "spread",
            )
            .await
            .unwrap_or_else(|err| {
                let mut r = run_greedy_spread(&scenario);
                r.source = format!("greedy-spread fallback (simulator failed: {err})");
                r
            }),
            None => run_greedy_spread(&scenario),
        };
        // Baseline 2: harder kube-scheduler bin-packing (NodeResourcesFit MostAllocated).
        let kube_binpack = match sim_url {
            Some(url) => run_kube_simulator(
                &scenario,
                url,
                crate::verifier::binpack_scheduler_config(),
                "binpack",
            )
            .await
            .unwrap_or_else(|err| {
                let mut r = run_greedy_binpack(&scenario);
                r.source = format!("greedy-binpack fallback (simulator failed: {err})");
                r
            }),
            None => run_greedy_binpack(&scenario),
        };
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
    ];
    let vram_prediction_scenario = vram_prediction_scenario_proof();
    let gpu_topology_scenario = gpu_topology_scenario_proof();
    let mig_profile_scenario = mig_profile_scenario_proof();
    let dra_approximation_scenario = dra_approximation_scenario_proof();
    let dra_allocation_scenario = dra_allocation_scenario_proof();
    let time_sliced_gpu_scenario = time_sliced_gpu_scenario_proof();
    let node_grouping_scenario = node_grouping_scenario_proof()?;
    let tenant_budget_scenario = tenant_budget_scenario_proof()?;
    let candidate_widening_scenario = candidate_widening_scenario_proof(&results);
    let feature_assertions = build_feature_assertions(
        &results,
        &benefit_summary,
        &roi_summary,
        &regret_summary,
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
    Ok(BenchmarkReport {
        simulator_url: simulator_url.map(|s| s.to_string()),
        sorted_by: "efficiency_score = ksolver GPU-utilization + cost win vs the best kube baseline (cost % + util ‰ + admitted-useful-GPU + active-node reduction + extra full gangs)".to_string(),
        benefit_summary,
        roi_summary,
        regret_summary,
        repair_scenario,
        repair_scenarios,
        vram_prediction_scenario,
        gpu_topology_scenario,
        mig_profile_scenario,
        dra_approximation_scenario,
        dra_allocation_scenario,
        time_sliced_gpu_scenario,
        node_grouping_scenario,
        tenant_budget_scenario,
        candidate_widening_scenario,
        feature_assertions,
        scenarios: results,
    })
}

pub fn print_table(report: &BenchmarkReport) {
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
}

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
        source: "deterministic greedy-spread baseline (no simulator URL)".to_string(),
        candidate_node_limit: 0,
        solve_millis: started.elapsed().as_millis() as u64,
        metrics: metrics(s, &placements),
        placements,
    }
}

/// Greedy MostAllocated (bin-packing) baseline — picks the fitting node with the LEAST free GPU
/// (tightest fit). Offline stand-in for the simulator's bin-packing config.
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
        source: "deterministic greedy-binpack baseline (no simulator URL)".to_string(),
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
    let mut nodes: Vec<&GpuNodeSpec> = s.nodes.iter().filter(|n| n.gpus >= required_gpu).collect();
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
) -> anyhow::Result<EngineResult> {
    use crate::verifier::{
        pod_assigned_node, schedule_snapshot, SimulatorImportPayload, SimulatorResources,
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
    let mut bound_pods = Vec::new();
    let mut placements = Vec::new();
    let raw = SimulatorResources {
        nodes: nodes.clone(),
        namespaces: vec![namespace.clone()],
        ..Default::default()
    };

    for job in &s.jobs {
        for pod_name in job.pod_names() {
            let pod = k8s_pod(&pod_name, job);
            let scope = format!("bench/{pod_name}");
            let mut pods = bound_pods.clone();
            pods.push(pod.clone());
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
            let export = schedule_snapshot(simulator_url, &payload, &scope).await?;
            let scheduled = export
                .pods
                .iter()
                .find(|p| crate::verifier::pod_scope(p) == scope)
                .and_then(pod_assigned_node);
            if let Some(node) = scheduled.clone() {
                let mut bound = pod;
                if let Some(spec) = bound.spec.as_mut() {
                    spec.node_name = Some(node.clone());
                }
                bound_pods.push(bound);
            }
            placements.push(Placement {
                pod: pod_name,
                node: scheduled,
                gpus: job.gpus_per_pod,
            });
        }
    }
    Ok(EngineResult {
        engine: format!("kube-{variant}"),
        source: format!(
            "kube-scheduler-simulator ({variant}) at {}",
            simulator_url.trim_end_matches('/')
        ),
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
            priority: (job.priority > 0).then_some(job.priority as i32),
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

fn metrics(s: &ScenarioSpec, placements: &[Placement]) -> PlacementMetrics {
    let by_pod: HashMap<&str, &Placement> =
        placements.iter().map(|p| (p.pod.as_str(), p)).collect();
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
        if (full || flexible_valid) && colocated {
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
    }
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

fn build_feature_assertions(
    scenarios: &[ScenarioResult],
    benefit: &BenefitSummary,
    roi: &RoiSummary,
    regret: &RegretSummary,
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
            "scenarios-sorted-by-benefit",
            scenarios
                .windows(2)
                .all(|pair| pair[0].benefit_score >= pair[1].benefit_score),
            "scenario report is sorted descending by benefit score",
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
            candidate_node_limit: 0,
            solve_millis: 0,
            metrics: PlacementMetrics::default(),
            placements: Vec::new(),
        }
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
                    candidate_node_limit: 0,
                    solve_millis: 0,
                    metrics: PlacementMetrics::default(),
                    placements: Vec::new(),
                },
                kube_binpack: empty_engine("kube-binpack"),
                ksolver: EngineResult {
                    engine: "ksolver".to_string(),
                    source: String::new(),
                    candidate_node_limit: 0,
                    solve_millis: 0,
                    metrics: PlacementMetrics::default(),
                    placements: Vec::new(),
                },
                reduced_ksolver: EngineResult {
                    engine: "ksolver".to_string(),
                    source: String::new(),
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
                    candidate_node_limit: 0,
                    solve_millis: 0,
                    metrics: PlacementMetrics::default(),
                    placements: Vec::new(),
                },
                kube_binpack: empty_engine("kube-binpack"),
                ksolver: EngineResult {
                    engine: "ksolver".to_string(),
                    source: String::new(),
                    candidate_node_limit: 0,
                    solve_millis: 0,
                    metrics: PlacementMetrics::default(),
                    placements: Vec::new(),
                },
                reduced_ksolver: EngineResult {
                    engine: "ksolver".to_string(),
                    source: String::new(),
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
                    candidate_node_limit: 0,
                    solve_millis: 0,
                    metrics: PlacementMetrics::default(),
                    placements: Vec::new(),
                },
                kube_binpack: empty_engine("kube-binpack"),
                ksolver: EngineResult {
                    engine: "ksolver".to_string(),
                    source: String::new(),
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
                    candidate_node_limit: 0,
                    solve_millis: 0,
                    metrics: kube,
                    placements: Vec::new(),
                },
                kube_binpack: empty_engine("kube-binpack"),
                ksolver: EngineResult {
                    engine: "ksolver".to_string(),
                    source: String::new(),
                    candidate_node_limit: 0,
                    solve_millis: 0,
                    metrics: ksolver,
                    placements: Vec::new(),
                },
                reduced_ksolver: EngineResult {
                    engine: "ksolver".to_string(),
                    source: String::new(),
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

    #[cfg(feature = "rust-cp-sat")]
    #[tokio::test]
    async fn feature_assertions_capture_priority_and_report_gates() {
        let report = run_benchmark(None).await.expect("benchmark report");
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
            assertions.get("fragmented-gang-repair-plan"),
            Some(&true),
            "repair scenario should prove fragmented gang repair advice"
        );
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
            assertions.get("node-grouping-symmetry-reduction"),
            Some(&true),
            "node grouping scenario should prove homogeneous nodes collapse and expand safely"
        );
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
