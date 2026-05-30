use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ClusterSnapshot {
    #[serde(default)]
    pub metadata: ClusterMetadata,
    #[serde(default)]
    pub nodes: Vec<Node>,
    #[serde(default)]
    pub pods: Vec<Pod>,
    #[serde(default)]
    pub volumes: Vec<VolumeAttachment>,
    #[serde(default)]
    pub storage_classes: Vec<StorageClass>,
    #[serde(default)]
    pub daemon_sets: Vec<DaemonSet>,
    #[serde(default)]
    pub pdbs: Vec<DisruptionBudget>,
    #[serde(default)]
    pub vpas: Vec<VerticalPodAutoscaler>,
    #[serde(default)]
    pub vpa_recommender: VpaRecommenderConfig,
    #[serde(default)]
    pub warnings: Vec<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ClusterMetadata {
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub collected: Option<DateTime<Utc>>,
    #[serde(default)]
    pub schema_version: u32,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Node {
    pub name: String,
    #[serde(default)]
    pub pool: String,
    #[serde(default)]
    pub labels: BTreeMap<String, String>,
    #[serde(default)]
    pub taints: Vec<Taint>,
    #[serde(default)]
    pub allocatable: ResourceList,
    #[serde(default)]
    pub extended_resources: BTreeMap<String, i64>,
    #[serde(default)]
    pub usage: ResourceUsage,
    #[serde(default)]
    pub price: Money,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Pod {
    pub namespace: String,
    pub name: String,
    #[serde(default)]
    pub node_name: String,
    #[serde(default)]
    pub phase: String,
    #[serde(default)]
    pub owner_kind: String,
    #[serde(default)]
    pub owner_name: String,
    #[serde(default)]
    pub deleting: bool,
    #[serde(default)]
    pub labels: BTreeMap<String, String>,
    #[serde(default)]
    pub qos_class: String,
    pub requests: ResourceList,
    #[serde(default)]
    pub extended_resource_requests: BTreeMap<String, i64>,
    #[serde(default)]
    pub usage: ResourceUsage,
    #[serde(default)]
    pub memory_history: MemoryHistory,
    #[serde(default)]
    pub tolerations: Vec<Toleration>,
    #[serde(default)]
    pub node_selector: BTreeMap<String, String>,
    #[serde(default)]
    pub required_affinity: Vec<AffinityTerm>,
    #[serde(default)]
    pub required_anti: Vec<AffinityTerm>,
    #[serde(default)]
    pub required_node_affinity: Vec<NodeAffinityTerm>,
    #[serde(default)]
    pub topology_spread_constraints: i32,
    #[serde(default)]
    pub topology_spread_rules: Vec<TopologySpreadRule>,
    #[serde(default)]
    pub pvcs: Vec<String>,
    #[serde(default)]
    pub disruption_cost: i32,
    #[serde(default)]
    pub autoscaler_not_safe_to_evict: bool,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct MemoryHistory {
    #[serde(default)]
    pub sample_count: i32,
    #[serde(default)]
    pub mean_bytes: i64,
    #[serde(default)]
    pub max_bytes: i64,
    #[serde(default)]
    pub stddev_bytes: i64,
    #[serde(default)]
    pub samples: Vec<TimeSeriesPoint>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct TimeSeriesPoint {
    #[serde(default)]
    pub timestamp_unix: i64,
    #[serde(default)]
    pub value: i64,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct VolumeAttachment {
    pub claim_name: String,
    pub namespace: String,
    #[serde(default)]
    pub bound_node_zones: Vec<String>,
    #[serde(default)]
    pub storage_class: String,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct StorageClass {
    pub name: String,
    #[serde(default)]
    pub provisioner: String,
    #[serde(default)]
    pub volume_binding_mode: String,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct DaemonSet {
    pub namespace: String,
    pub name: String,
    #[serde(default)]
    pub node_selector: BTreeMap<String, String>,
    #[serde(default)]
    pub tolerations: Vec<Toleration>,
    pub requests: ResourceList,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct DisruptionBudget {
    pub namespace: String,
    pub name: String,
    #[serde(default)]
    pub min_available: String,
    #[serde(default)]
    pub max_unavailable: String,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct VerticalPodAutoscaler {
    #[serde(default)]
    pub namespace: String,
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub target_ref_kind: String,
    #[serde(default)]
    pub target_ref_name: String,
    #[serde(default)]
    pub update_mode: String,
    #[serde(default)]
    pub container_policies: Vec<VpaContainerPolicy>,
    #[serde(default)]
    pub container_recommendations: Vec<VpaContainerRecommendation>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct VpaContainerPolicy {
    #[serde(default)]
    pub container_name: String,
    #[serde(default)]
    pub controlled_resources: Vec<String>,
    #[serde(default)]
    pub min_allowed: ResourceList,
    #[serde(default)]
    pub max_allowed: ResourceList,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct VpaContainerRecommendation {
    #[serde(default)]
    pub container_name: String,
    #[serde(default)]
    pub lower_bound: ResourceList,
    #[serde(default)]
    pub target: ResourceList,
    #[serde(default)]
    pub upper_bound: ResourceList,
    #[serde(default)]
    pub uncapped_target: ResourceList,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct VpaRecommenderConfig {
    #[serde(default)]
    pub found: bool,
    #[serde(default)]
    pub source: String,
    #[serde(default = "default_vpa_safety_margin_fraction")]
    pub safety_margin_fraction: f64,
    #[serde(default)]
    pub args: Vec<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct ResourceList {
    #[serde(default, rename = "MilliCPU")]
    pub milli_cpu: i64,
    #[serde(default)]
    pub memory_bytes: i64,
    #[serde(default)]
    pub ephemeral_storage: i64,
    #[serde(default)]
    pub pods: i64,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct ResourceUsage {
    #[serde(default, rename = "CPUUsageMilli")]
    pub cpu_usage_milli: i64,
    #[serde(default)]
    pub memory_bytes: i64,
    #[serde(default)]
    pub ephemeral_bytes: i64,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Taint {
    #[serde(default)]
    pub key: String,
    #[serde(default)]
    pub value: String,
    #[serde(default)]
    pub effect: String,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Toleration {
    #[serde(default)]
    pub key: String,
    #[serde(default)]
    pub operator: String,
    #[serde(default)]
    pub value: String,
    #[serde(default)]
    pub effect: String,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct AffinityTerm {
    #[serde(default)]
    pub topology_key: String,
    #[serde(default)]
    pub selector: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct NodeAffinityTerm {
    #[serde(default)]
    pub key: String,
    #[serde(default)]
    pub operator: String,
    #[serde(default)]
    pub values: Vec<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TopologySpreadRule {
    #[serde(default)]
    pub max_skew: i32,
    #[serde(default)]
    pub topology_key: String,
    #[serde(default)]
    pub when_unsatisfiable: String,
    #[serde(default)]
    pub selector: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct Money {
    #[serde(default = "default_currency")]
    pub currency: String,
    #[serde(default)]
    pub monthly: f64,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct OptimizationPlan {
    #[serde(default)]
    pub current_monthly_cost: Money,
    #[serde(default)]
    pub optimized_monthly_cost: Money,
    #[serde(default)]
    pub savings_monthly: Money,
    #[serde(default)]
    pub active_nodes: Vec<String>,
    #[serde(default)]
    pub recommended_moves: Vec<PodMove>,
    #[serde(default)]
    pub blockers: Vec<Blocker>,
    #[serde(default)]
    pub solver: SolverInfo,
    #[serde(default)]
    pub verification: VerificationReport,
    #[serde(default)]
    pub resource_summary: ResourceSummary,
    #[serde(default)]
    pub fleet_recommendation: Vec<FleetNode>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct FleetNode {
    #[serde(default)]
    pub instance_type: String,
    #[serde(default)]
    pub count: i32,
    #[serde(default)]
    pub monthly_cost: Money,
    #[serde(default)]
    pub is_candidate: bool,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct PodMove {
    pub namespace: String,
    pub pod: String,
    pub from_node: String,
    pub to_node: String,
    #[serde(default)]
    pub reason: String,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct Blocker {
    #[serde(default)]
    pub scope: String,
    #[serde(default)]
    pub message: String,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct SolverInfo {
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub available: bool,
    #[serde(default)]
    pub status: String,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct VerificationReport {
    #[serde(default)]
    pub backend: String,
    #[serde(default)]
    pub status: String,
    #[serde(default)]
    pub confidence: String,
    #[serde(default)]
    pub verified_moves: i32,
    #[serde(default)]
    pub rejected_moves: i32,
    #[serde(default)]
    pub blocker_count: i32,
    #[serde(default)]
    pub checks: Vec<VerificationCheck>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct VerificationCheck {
    #[serde(default)]
    pub scope: String,
    #[serde(default)]
    pub kind: String,
    #[serde(default)]
    pub status: String,
    #[serde(default)]
    pub detail: String,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct ResourceSummary {
    #[serde(default)]
    pub current: ResourceAllocation,
    #[serde(default)]
    pub optimized: ResourceAllocation,
    #[serde(default)]
    pub current_fragmentation_percent: f64,
    #[serde(default)]
    pub optimized_fragmentation_percent: f64,
    #[serde(default)]
    pub pool_summaries: Vec<PoolSummary>,
    #[serde(default)]
    pub autoscaler_blockers: Vec<AutoscalerBlockerSummary>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct AutoscalerBlockerSummary {
    #[serde(default)]
    pub kind: String,
    #[serde(default)]
    pub blocked_monthly_cost: Money,
    #[serde(default)]
    pub blocked_node_count: i32,
    #[serde(default)]
    pub blocked_workload_count: i32,
    #[serde(default)]
    pub blocked_nodes: Vec<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct ResourceAllocation {
    #[serde(default, rename = "AllocatedCPURequestMilli")]
    pub allocated_cpu_request_milli: i64,
    #[serde(default, rename = "UnallocatedCPURequestMilli")]
    pub unallocated_cpu_request_milli: i64,
    #[serde(default, rename = "TotalCPUCapacityMilli")]
    pub total_cpu_capacity_milli: i64,
    #[serde(default)]
    pub allocated_memory_request_bytes: i64,
    #[serde(default)]
    pub unallocated_memory_request_bytes: i64,
    #[serde(default)]
    pub total_memory_capacity_bytes: i64,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct PoolSummary {
    #[serde(default)]
    pub pool: String,
    #[serde(default)]
    pub node_count: i32,
    #[serde(default)]
    pub current_monthly_cost: Money,
    #[serde(default)]
    pub allocated_cpu_request_milli: i64,
    #[serde(default)]
    pub allocated_memory_request_bytes: i64,
    #[serde(default)]
    pub total_cpu_capacity_milli: i64,
    #[serde(default)]
    pub total_memory_capacity_bytes: i64,
    #[serde(default)]
    pub fragmentation_percent: f64,
    #[serde(default)]
    pub emptiable: Option<bool>,
    #[serde(default)]
    pub emptiability_blockers: Vec<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct NormalizedCluster {
    #[serde(default)]
    pub cluster_name: String,
    #[serde(default)]
    pub collected: Option<DateTime<Utc>>,
    #[serde(default)]
    pub current_monthly_cost: Money,
    #[serde(default)]
    pub nodes: Vec<NormalizedNode>,
    #[serde(default)]
    pub workloads: Vec<NormalizedWorkload>,
    #[serde(default)]
    pub blockers: Vec<Blocker>,
    #[serde(default)]
    pub constraint_impacts: Vec<ConstraintImpact>,
    #[serde(default)]
    pub pool_summaries: Vec<PoolSummary>,
    #[serde(default)]
    pub warnings: Vec<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct NormalizedNode {
    pub name: String,
    #[serde(default)]
    pub pool: String,
    #[serde(default)]
    pub instance_type: String,
    #[serde(default)]
    pub labels: BTreeMap<String, String>,
    #[serde(default)]
    pub taints: Vec<Taint>,
    #[serde(default)]
    pub allocatable: ResourceList,
    #[serde(default)]
    pub extended_resources: BTreeMap<String, i64>,
    #[serde(default)]
    pub reserved: ResourceList,
    #[serde(default)]
    pub effective_capacity: ResourceList,
    #[serde(default)]
    pub usage: ResourceUsage,
    #[serde(default)]
    pub current_pods: i32,
    #[serde(default)]
    pub price: Money,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct NormalizedWorkload {
    pub namespace: String,
    pub name: String,
    #[serde(default)]
    pub owner_kind: String,
    #[serde(default)]
    pub owner_name: String,
    #[serde(default)]
    pub current_node: String,
    #[serde(default)]
    pub current_requests: ResourceList,
    #[serde(default)]
    pub recommended_requests: ResourceList,
    #[serde(default)]
    pub requests: ResourceList,
    #[serde(default)]
    pub extended_resource_requests: BTreeMap<String, i64>,
    #[serde(default)]
    pub usage: ResourceUsage,
    #[serde(default)]
    pub feasible_nodes: i32,
    #[serde(default)]
    pub feasible_node_names: Vec<String>,
    #[serde(default)]
    pub pinned_by_volume: bool,
    #[serde(default)]
    pub has_required_affinity: bool,
    #[serde(default)]
    pub has_required_anti_affinity: bool,
    #[serde(default)]
    pub has_required_node_affinity: bool,
    #[serde(default)]
    pub topology_spread_constraints: i32,
    #[serde(default)]
    pub qos_class: String,
    #[serde(default)]
    pub autoscaler_not_safe_to_evict: bool,
    #[serde(default)]
    pub reasons: Vec<String>,
    #[serde(default)]
    pub candidate_levels: Vec<CandidateLevel>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct AnalysisReport {
    #[serde(default)]
    pub snapshot: ClusterSnapshot,
    #[serde(default)]
    pub normalized: NormalizedCluster,
    #[serde(default)]
    pub optimization: Option<OptimizationPlan>,
    #[serde(default)]
    pub explainability: ExplainabilityReport,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct ExplainabilityReport {
    #[serde(default)]
    pub inflation_drivers: Vec<InflationDriver>,
    #[serde(default)]
    pub memory_risk: MemoryRiskSummary,
    #[serde(default)]
    pub has_usage_data: bool,
    #[serde(default)]
    pub pool_emptiability: Vec<PoolEmptiability>,
    #[serde(default)]
    pub node_drainability: Vec<NodeDrainability>,
    #[serde(default)]
    pub daemonset_consolidation_savings: Money,
    #[serde(default)]
    pub pdb_impacts: Vec<PdbImpact>,
    #[serde(default)]
    pub vpa_overview: VpaOverview,
    #[serde(default)]
    pub vpa_update_mode_impacts: Vec<VpaUpdateModeImpact>,
    #[serde(default)]
    pub vpa_policy_gaps: Vec<VpaPolicyGap>,
    #[serde(default)]
    pub missing_vpa_opportunities: MissingVpaOpportunity,
    #[serde(default)]
    pub action_items: Vec<ActionItem>,
    #[serde(default)]
    pub savings_waterfall: SavingsWaterfall,
    #[serde(default)]
    pub workload_cost_table: Vec<WorkloadCostRow>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct PoolEmptiability {
    #[serde(default)]
    pub pool: String,
    #[serde(default)]
    pub emptiable: bool,
    #[serde(default)]
    pub monthly_cost: Money,
    #[serde(default)]
    pub node_count: i32,
    #[serde(default)]
    pub pinned_workloads: Vec<String>,
    #[serde(default)]
    pub pin_reasons: Vec<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct PdbImpact {
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub namespace: String,
    #[serde(default)]
    pub min_available: String,
    #[serde(default)]
    pub max_unavailable: String,
    #[serde(default)]
    pub matched_pods: i32,
    #[serde(default)]
    pub disruption_budget: i32,
    #[serde(default)]
    pub affected_nodes: i32,
    #[serde(default)]
    pub summary: String,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct VpaOverview {
    #[serde(default)]
    pub total_workloads: i32,
    #[serde(default)]
    pub workloads_with_vpa: i32,
    #[serde(default)]
    pub workloads_without_vpa_overprovisioned: i32,
    #[serde(default)]
    pub missing_vpa_monthly_waste: Money,
    #[serde(default)]
    pub update_mode_counts: BTreeMap<String, i32>,
    #[serde(default)]
    pub safety_margin_fraction: f64,
    #[serde(default)]
    pub safety_margin_cost: Money,
    #[serde(default)]
    pub recommender_args: Vec<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct VpaUpdateModeImpact {
    #[serde(default)]
    pub vpa_name: String,
    #[serde(default)]
    pub namespace: String,
    #[serde(default)]
    pub update_mode: String,
    #[serde(default)]
    pub target_workload: String,
    #[serde(default)]
    pub savings_if_auto: Money,
    #[serde(default)]
    pub kubectl_command: String,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct VpaPolicyGap {
    #[serde(default)]
    pub vpa_name: String,
    #[serde(default)]
    pub namespace: String,
    #[serde(default)]
    pub container_name: String,
    #[serde(default)]
    pub resource: String,
    #[serde(default)]
    pub current_request: i64,
    #[serde(default)]
    pub vpa_target: i64,
    #[serde(default)]
    pub policy_bound: i64,
    #[serde(default)]
    pub bound_type: String,
    #[serde(default)]
    pub gap_cost: Money,
    #[serde(default)]
    pub summary: String,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct MissingVpaOpportunity {
    #[serde(default)]
    pub workloads_without_vpa: i32,
    #[serde(default)]
    pub overprovisioned_count: i32,
    #[serde(default)]
    pub total_waste: Money,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct NodeDrainability {
    #[serde(default)]
    pub node: String,
    #[serde(default)]
    pub pool: String,
    #[serde(default)]
    pub monthly_cost: f64,
    #[serde(default)]
    pub drainable: bool,
    #[serde(default)]
    pub pinned_workloads: Vec<String>,
    #[serde(default)]
    pub pin_reasons: Vec<String>,
    #[serde(default)]
    pub workload_count: i32,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct ActionItem {
    #[serde(default)]
    pub rank: i32,
    #[serde(default)]
    pub title: String,
    #[serde(default)]
    pub workload: String,
    #[serde(default)]
    pub resource: String,
    #[serde(default)]
    pub current_value: String,
    #[serde(default)]
    pub recommended_value: String,
    #[serde(default)]
    pub savings_monthly: Money,
    #[serde(default)]
    pub risk: String,
    #[serde(default)]
    pub kubectl_command: String,
    #[serde(default)]
    pub reason: String,
    #[serde(default)]
    pub source: String,
    #[serde(default)]
    pub effort: String,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct SavingsWaterfall {
    #[serde(default)]
    pub layers: Vec<SavingsLayer>,
    #[serde(default)]
    pub total: Money,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct SavingsLayer {
    #[serde(default)]
    pub key: String,
    #[serde(default)]
    pub label: String,
    #[serde(default)]
    pub savings: Money,
    #[serde(default)]
    pub cumulative: Money,
    #[serde(default)]
    pub confidence: String,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct WorkloadCostRow {
    #[serde(default)]
    pub namespace: String,
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub owner_kind: String,
    #[serde(default)]
    pub owner_name: String,
    #[serde(default)]
    pub current_cpu_milli: i64,
    #[serde(default)]
    pub current_memory_bytes: i64,
    #[serde(default)]
    pub recommended_cpu_milli: i64,
    #[serde(default)]
    pub recommended_memory_bytes: i64,
    #[serde(default)]
    pub usage_cpu_milli: i64,
    #[serde(default)]
    pub usage_memory_bytes: i64,
    #[serde(default)]
    pub cpu_usage_ratio: f64,
    #[serde(default)]
    pub memory_usage_ratio: f64,
    #[serde(default)]
    pub monthly_cost: Money,
    #[serde(default)]
    pub savings_if_rightsized: Money,
    #[serde(default)]
    pub constraints: Vec<String>,
    #[serde(default)]
    pub candidate_levels: Vec<CandidateLevel>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct MemoryRiskSummary {
    #[serde(default)]
    pub model: String,
    #[serde(default)]
    pub current: PlacementMemoryRisk,
    #[serde(default)]
    pub optimized: PlacementMemoryRisk,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct PlacementMemoryRisk {
    #[serde(default)]
    pub risk: String,
    #[serde(default)]
    pub overflow_node_count: i32,
    #[serde(default)]
    pub high_risk_node_count: i32,
    #[serde(default)]
    pub max_pressure_percent: f64,
    #[serde(default)]
    pub overflow_probability_percent: f64,
    #[serde(default)]
    pub summary: String,
    #[serde(default)]
    pub top_nodes: Vec<NodeMemoryRisk>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct NodeMemoryRisk {
    #[serde(default)]
    pub node_name: String,
    #[serde(default)]
    pub risk: String,
    #[serde(default)]
    pub pressure_percent: f64,
    #[serde(default)]
    pub overflow_percent: f64,
    #[serde(default)]
    pub overflow_probability_percent: f64,
    #[serde(default)]
    pub used_memory_bytes: i64,
    #[serde(default)]
    pub capacity_memory_bytes: i64,
    #[serde(default)]
    pub workload_count: i32,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct InflationDriver {
    #[serde(default)]
    pub key: String,
    #[serde(default)]
    pub display_name: String,
    #[serde(default)]
    pub method: String,
    #[serde(default)]
    pub confidence: String,
    #[serde(default)]
    pub blocked_monthly_cost: Money,
    #[serde(default)]
    pub affected_workload_count: i32,
    #[serde(default)]
    pub blocked_node_count: i32,
    #[serde(default)]
    pub summary: String,
    #[serde(default)]
    pub recommended_action: String,
    #[serde(default)]
    pub top_entities: Vec<String>,
    #[serde(default)]
    pub details: Vec<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ConstraintImpact {
    #[serde(default)]
    pub name: String,
    #[serde(default, rename = "workloadsAffected")]
    pub workloads_affected: i32,
    #[serde(default, rename = "reasonCount")]
    pub reason_count: i32,
    #[serde(default, rename = "estimatedMonthly")]
    pub estimated_monthly: f64,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct HistoricalUsageConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default = "default_historical_usage_provider")]
    pub provider: String,
    #[serde(default = "default_historical_usage_lookback")]
    pub lookback: String,
    #[serde(default = "default_historical_usage_step")]
    pub step: String,
    #[serde(default)]
    pub prometheus_url: String,
    #[serde(default)]
    pub prometheus_username: String,
    #[serde(default)]
    pub prometheus_token: String,
    #[serde(default)]
    pub secret_namespace: String,
    #[serde(default)]
    pub secret_name: String,
    #[serde(default = "default_prometheus_url_key")]
    pub secret_prometheus_url_key: String,
    #[serde(default = "default_prometheus_username_key")]
    pub secret_prometheus_username_key: String,
    #[serde(default = "default_prometheus_token_key")]
    pub secret_prometheus_token_key: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct ScenarioConfig {
    #[serde(default)]
    pub solver: String,
    #[serde(default)]
    pub cpu_headroom_percent: f64,
    #[serde(default)]
    pub memory_headroom_percent: f64,
    #[serde(default)]
    pub storage_headroom_percent: f64,
    #[serde(default)]
    pub pods_headroom: i64,
    #[serde(default)]
    pub namespace_include: Vec<String>,
    #[serde(default)]
    pub disallowed_pools: Vec<String>,
    #[serde(default = "default_node_pool_label_keys")]
    pub node_pool_label_keys: Vec<String>,
    #[serde(default)]
    pub max_workloads: i32,
    #[serde(default)]
    pub relax_preferred_affinity: bool,
    #[serde(default)]
    pub relax_required_anti_affinity: bool,
    #[serde(default)]
    pub ignore_taints: bool,
    #[serde(default = "default_true")]
    pub ignore_unschedulable_workloads: bool,
    #[serde(default = "default_cpu_overcommit_ratio")]
    pub cpu_overcommit_ratio: f64,
    #[serde(default = "default_memory_overcommit_ratio")]
    pub memory_overcommit_ratio: f64,
    #[serde(default)]
    pub use_usage_adjusted_requests: bool,
    #[serde(default = "default_usage_risk_preset")]
    pub usage_risk_preset: String,
    #[serde(default = "default_usage_request_floor_ratio")]
    pub usage_request_floor_ratio: f64,
    #[serde(default = "default_cpu_usage_safety_factor")]
    pub cpu_usage_safety_factor: f64,
    #[serde(default = "default_memory_usage_safety_factor")]
    pub memory_usage_safety_factor: f64,
    #[serde(default = "default_max_memory_overflow_probability_percent")]
    pub max_memory_overflow_probability_percent: f64,
    #[serde(default)]
    pub historical_usage: HistoricalUsageConfig,
    #[serde(default = "default_verification_backend")]
    pub verification_backend: String,
    #[serde(default)]
    pub verification_url: String,
    #[serde(default = "default_cost_weight")]
    pub cost_weight: i64,
    #[serde(default = "default_active_node_weight")]
    pub active_node_weight: i64,
    #[serde(default = "default_memory_slack_weight")]
    pub memory_slack_weight: i64,
    #[serde(default = "default_cpu_slack_weight")]
    pub cpu_slack_weight: i64,
    #[serde(default = "default_churn_weight")]
    pub churn_weight: i64,
    #[serde(default)]
    pub enable_joint_rightsizing: bool,
    #[serde(default = "default_rightsizing_weight")]
    pub rightsizing_weight: i64,
    #[serde(default)]
    pub candidate_instance_types: Vec<CandidateInstanceType>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct CandidateInstanceType {
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub cpu_milli: i64,
    #[serde(default)]
    pub memory_bytes: i64,
    #[serde(default)]
    pub monthly_price: f64,
    #[serde(default)]
    pub max_count: i32,
}

impl Default for ScenarioConfig {
    fn default() -> Self {
        Self {
            solver: String::new(),
            cpu_headroom_percent: 0.0,
            memory_headroom_percent: 0.0,
            storage_headroom_percent: 0.0,
            pods_headroom: 0,
            namespace_include: Vec::new(),
            disallowed_pools: Vec::new(),
            node_pool_label_keys: default_node_pool_label_keys(),
            max_workloads: 0,
            relax_preferred_affinity: false,
            relax_required_anti_affinity: false,
            ignore_taints: false,
            ignore_unschedulable_workloads: true,
            cpu_overcommit_ratio: default_cpu_overcommit_ratio(),
            memory_overcommit_ratio: default_memory_overcommit_ratio(),
            use_usage_adjusted_requests: false,
            usage_risk_preset: default_usage_risk_preset(),
            usage_request_floor_ratio: default_usage_request_floor_ratio(),
            cpu_usage_safety_factor: default_cpu_usage_safety_factor(),
            memory_usage_safety_factor: default_memory_usage_safety_factor(),
            max_memory_overflow_probability_percent:
                default_max_memory_overflow_probability_percent(),
            historical_usage: HistoricalUsageConfig::default(),
            verification_backend: default_verification_backend(),
            verification_url: String::new(),
            cost_weight: default_cost_weight(),
            active_node_weight: default_active_node_weight(),
            memory_slack_weight: default_memory_slack_weight(),
            cpu_slack_weight: default_cpu_slack_weight(),
            churn_weight: default_churn_weight(),
            enable_joint_rightsizing: false,
            rightsizing_weight: default_rightsizing_weight(),
            candidate_instance_types: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct SolveRequest {
    #[serde(default)]
    pub kubeconfig: String,
    #[serde(default)]
    pub pricing_file: String,
    #[serde(default)]
    pub snapshot_file: String,
    #[serde(default)]
    pub cluster_name: String,
    #[serde(default)]
    pub scenario_name: String,
    #[serde(default)]
    pub scenario: ScenarioConfig,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ProgressUpdate {
    #[serde(default)]
    pub stage: String,
    #[serde(default)]
    pub message: String,
    #[serde(default)]
    pub percent: i32,
    #[serde(default)]
    pub done: bool,
    #[serde(default)]
    pub error: String,
    #[serde(default, rename = "elapsedMs")]
    pub elapsed_ms: i64,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct OptimizationInput {
    #[serde(default)]
    pub nodes: Vec<OptimizationNode>,
    #[serde(default)]
    pub workloads: Vec<OptimizationWorkload>,
    #[serde(default)]
    pub anti_affinity_pairs: Vec<(String, String)>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct OptimizationNode {
    pub name: String,
    #[serde(default)]
    pub pool: String,
    #[serde(default)]
    pub count: i32,
    #[serde(default)]
    pub members: Vec<String>,
    #[serde(default)]
    pub price: Money,
    #[serde(default)]
    pub effective_capacity: ResourceList,
    #[serde(default)]
    pub extended_resources: BTreeMap<String, i64>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct OptimizationWorkload {
    pub id: String,
    pub namespace: String,
    pub name: String,
    #[serde(default)]
    pub group_size: i32,
    #[serde(default)]
    pub members: Vec<OptimizationWorkloadMember>,
    #[serde(default)]
    pub current_node: String,
    #[serde(default)]
    pub current_counts: HashMap<String, i32>,
    #[serde(default)]
    pub requests: ResourceList,
    #[serde(default)]
    pub recommended_requests: ResourceList,
    #[serde(default)]
    pub extended_resource_requests: BTreeMap<String, i64>,
    #[serde(default)]
    pub feasible_nodes: Vec<String>,
    #[serde(default)]
    pub candidate_levels: Vec<CandidateLevel>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct CandidateLevel {
    #[serde(default)]
    pub key: String,
    #[serde(default)]
    pub label: String,
    #[serde(default)]
    pub requests: ResourceList,
    #[serde(default)]
    pub risk_score: i64,
}

#[cfg(test)]
mod tests {
    use super::{
        AnalysisReport, AutoscalerBlockerSummary, ExplainabilityReport, InflationDriver,
        MemoryRiskSummary, Money, NormalizedCluster, NormalizedNode, NormalizedWorkload,
        OptimizationPlan, PlacementMemoryRisk, ResourceAllocation, ResourceList, ResourceSummary,
        ResourceUsage, SolverInfo,
    };

    #[test]
    fn analysis_report_serializes_with_ui_field_names() {
        let report = AnalysisReport {
            normalized: NormalizedCluster {
                current_monthly_cost: Money {
                    currency: "USD".to_string(),
                    monthly: 123.0,
                },
                nodes: vec![NormalizedNode {
                    name: "node-a".to_string(),
                    effective_capacity: ResourceList {
                        milli_cpu: 4000,
                        memory_bytes: 8192,
                        ephemeral_storage: 0,
                        pods: 32,
                    },
                    usage: ResourceUsage {
                        cpu_usage_milli: 500,
                        memory_bytes: 2048,
                        ephemeral_bytes: 0,
                    },
                    current_pods: 7,
                    price: Money {
                        currency: "USD".to_string(),
                        monthly: 42.0,
                    },
                    ..Default::default()
                }],
                workloads: vec![NormalizedWorkload {
                    namespace: "default".to_string(),
                    name: "app".to_string(),
                    current_requests: ResourceList {
                        milli_cpu: 500,
                        memory_bytes: 2048,
                        ephemeral_storage: 0,
                        pods: 1,
                    },
                    recommended_requests: ResourceList {
                        milli_cpu: 250,
                        memory_bytes: 1024,
                        ephemeral_storage: 0,
                        pods: 1,
                    },
                    requests: ResourceList {
                        milli_cpu: 250,
                        memory_bytes: 1024,
                        ephemeral_storage: 0,
                        pods: 1,
                    },
                    usage: ResourceUsage {
                        cpu_usage_milli: 125,
                        memory_bytes: 768,
                        ephemeral_bytes: 0,
                    },
                    ..Default::default()
                }],
                ..Default::default()
            },
            optimization: Some(OptimizationPlan {
                active_nodes: vec!["node-a".to_string()],
                solver: SolverInfo {
                    name: "cp-sat".to_string(),
                    status: "ok".to_string(),
                    available: true,
                },
                resource_summary: ResourceSummary {
                    current: ResourceAllocation {
                        allocated_cpu_request_milli: 1000,
                        unallocated_cpu_request_milli: 3000,
                        total_cpu_capacity_milli: 4000,
                        allocated_memory_request_bytes: 2048,
                        unallocated_memory_request_bytes: 6144,
                        total_memory_capacity_bytes: 8192,
                    },
                    optimized: ResourceAllocation {
                        allocated_cpu_request_milli: 1000,
                        unallocated_cpu_request_milli: 1000,
                        total_cpu_capacity_milli: 2000,
                        allocated_memory_request_bytes: 2048,
                        unallocated_memory_request_bytes: 2048,
                        total_memory_capacity_bytes: 4096,
                    },
                    current_fragmentation_percent: 21.5,
                    optimized_fragmentation_percent: 13.4,
                    pool_summaries: Vec::new(),
                    autoscaler_blockers: vec![AutoscalerBlockerSummary {
                        kind: "cluster autoscaler safe-to-evict=false".to_string(),
                        blocked_monthly_cost: Money {
                            currency: "USD".to_string(),
                            monthly: 123.0,
                        },
                        blocked_node_count: 2,
                        blocked_workload_count: 5,
                        blocked_nodes: vec!["node-a".to_string(), "node-b".to_string()],
                    }],
                },
                ..Default::default()
            }),
            explainability: ExplainabilityReport {
                inflation_drivers: vec![InflationDriver {
                    key: "requests".to_string(),
                    display_name: "Inflated requests".to_string(),
                    method: "demand_delta".to_string(),
                    confidence: "medium".to_string(),
                    blocked_monthly_cost: Money {
                        currency: "USD".to_string(),
                        monthly: 77.0,
                    },
                    affected_workload_count: 2,
                    blocked_node_count: 0,
                    summary: "summary".to_string(),
                    recommended_action: "action".to_string(),
                    top_entities: vec!["default/app".to_string()],
                    details: vec!["detail".to_string()],
                }],
                memory_risk: MemoryRiskSummary {
                    model: "observed_peak_proxy".to_string(),
                    current: PlacementMemoryRisk {
                        risk: "low".to_string(),
                        max_pressure_percent: 55.0,
                        ..Default::default()
                    },
                    optimized: PlacementMemoryRisk {
                        risk: "high".to_string(),
                        max_pressure_percent: 95.0,
                        ..Default::default()
                    },
                },
                ..Default::default()
            },
            ..Default::default()
        };

        let json = serde_json::to_value(report).unwrap();

        assert!(json.get("Normalized").is_some());
        assert!(json.get("Optimization").is_some());
        assert!(json.get("Explainability").is_some());

        let normalized = json.get("Normalized").unwrap();
        assert!(normalized.get("CurrentMonthlyCost").is_some());

        let node = normalized
            .get("Nodes")
            .and_then(|nodes| nodes.as_array())
            .and_then(|nodes| nodes.first())
            .unwrap();
        assert!(node.get("EffectiveCapacity").is_some());
        assert!(node.get("Usage").is_some());
        assert_eq!(node.get("CurrentPods").unwrap().as_i64(), Some(7));
        assert_eq!(
            node.get("EffectiveCapacity")
                .and_then(|v| v.get("MilliCPU"))
                .and_then(|v| v.as_i64()),
            Some(4000)
        );
        assert_eq!(
            node.get("Usage")
                .and_then(|v| v.get("CPUUsageMilli"))
                .and_then(|v| v.as_i64()),
            Some(500)
        );

        let workload = normalized
            .get("Workloads")
            .and_then(|workloads| workloads.as_array())
            .and_then(|workloads| workloads.first())
            .unwrap();
        assert!(workload.get("CurrentRequests").is_some());
        assert!(workload.get("RecommendedRequests").is_some());

        let optimization = json.get("Optimization").unwrap();
        assert!(optimization.get("ActiveNodes").is_some());
        assert!(optimization
            .get("Solver")
            .and_then(|v| v.get("Status"))
            .is_some());
        assert_eq!(
            optimization
                .get("ResourceSummary")
                .and_then(|v| v.get("CurrentFragmentationPercent"))
                .and_then(|v| v.as_f64()),
            Some(21.5)
        );
        assert_eq!(
            optimization
                .get("ResourceSummary")
                .and_then(|v| v.get("OptimizedFragmentationPercent"))
                .and_then(|v| v.as_f64()),
            Some(13.4)
        );
        assert_eq!(
            optimization
                .get("ResourceSummary")
                .and_then(|v| v.get("AutoscalerBlockers"))
                .and_then(|v| v.as_array())
                .map(|v| v.len()),
            Some(1)
        );
        assert_eq!(
            json.get("Explainability")
                .and_then(|v| v.get("InflationDrivers"))
                .and_then(|v| v.as_array())
                .map(|v| v.len()),
            Some(1)
        );
        assert_eq!(
            json.get("Explainability")
                .and_then(|v| v.get("MemoryRisk"))
                .and_then(|v| v.get("Model"))
                .and_then(|v| v.as_str()),
            Some("observed_peak_proxy")
        );
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct OptimizationWorkloadMember {
    pub namespace: String,
    pub name: String,
    #[serde(default)]
    pub current_node: String,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct OptimizationSolution {
    #[serde(default)]
    pub assignments: HashMap<String, String>,
    #[serde(default)]
    pub assignment_counts: HashMap<String, HashMap<String, i32>>,
    #[serde(default)]
    pub active_nodes: HashMap<String, i32>,
    #[serde(default)]
    pub rightsized_workloads: Vec<String>,
    #[serde(default)]
    pub selected_levels: HashMap<String, String>,
}

fn default_currency() -> String {
    "USD".to_string()
}

fn default_true() -> bool {
    true
}

fn default_verification_backend() -> String {
    "scheduler-simulator".to_string()
}

fn default_node_pool_label_keys() -> Vec<String> {
    vec![
        "eks.amazonaws.com/nodegroup".to_string(),
        "cloud.google.com/gke-nodepool".to_string(),
        "agentpool".to_string(),
    ]
}

fn default_cpu_overcommit_ratio() -> f64 {
    1.0
}

fn default_memory_overcommit_ratio() -> f64 {
    1.0
}

fn default_usage_request_floor_ratio() -> f64 {
    0.5
}

fn default_usage_risk_preset() -> String {
    "custom".to_string()
}

fn default_vpa_safety_margin_fraction() -> f64 {
    0.15
}

fn default_cpu_usage_safety_factor() -> f64 {
    1.5
}

fn default_memory_usage_safety_factor() -> f64 {
    2.0
}

fn default_max_memory_overflow_probability_percent() -> f64 {
    1.0
}

fn default_historical_usage_provider() -> String {
    "prometheus".to_string()
}

fn default_historical_usage_lookback() -> String {
    "24h".to_string()
}

fn default_historical_usage_step() -> String {
    "5m".to_string()
}

fn default_prometheus_url_key() -> String {
    "prom_host".to_string()
}

fn default_prometheus_username_key() -> String {
    "prom_username".to_string()
}

fn default_prometheus_token_key() -> String {
    "alloy_token".to_string()
}

fn default_cost_weight() -> i64 {
    10_000_000_000
}

fn default_active_node_weight() -> i64 {
    5_000_000_000
}

fn default_memory_slack_weight() -> i64 {
    1
}

fn default_cpu_slack_weight() -> i64 {
    1_000_000
}

fn default_churn_weight() -> i64 {
    10_000_000
}

fn default_rightsizing_weight() -> i64 {
    100_000_000
}
