use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};

/// A cluster Namespace with its labels (for `namespaceSelector`-scoped pod anti-affinity).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct NamespaceMeta {
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub labels: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ClusterSnapshot {
    #[serde(default)]
    pub metadata: ClusterMetadata,
    #[serde(default)]
    pub namespaces: Vec<NamespaceMeta>,
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Pod {
    pub namespace: String,
    pub name: String,
    /// Kubernetes `metadata.uid` — stable pod identity (distinguishes a recreated pod that reuses
    /// the same namespace/name). Empty if unavailable.
    #[serde(default)]
    pub uid: String,
    #[serde(default)]
    pub node_name: String,
    #[serde(default)]
    pub phase: String,
    /// Kubernetes `status.startTime` as unix seconds, when present.
    #[serde(default)]
    pub start_time_unix: i64,
    /// Best-effort latest container termination finish time as unix seconds, when present.
    #[serde(default)]
    pub finish_time_unix: i64,
    #[serde(default)]
    pub owner_kind: String,
    #[serde(default)]
    pub owner_name: String,
    #[serde(default)]
    pub deleting: bool,
    #[serde(default)]
    pub labels: BTreeMap<String, String>,
    /// Optional tenant/team owner hint from `ksolver.dev/team`.
    #[serde(default)]
    pub team: String,
    /// Container images from the pod spec. Used for prediction observation fingerprints.
    #[serde(default)]
    pub container_images: Vec<String>,
    /// Stable SHA-256 digest over container image/command/args tuples. Empty if no pod spec.
    #[serde(default)]
    pub command_hash: String,
    #[serde(default)]
    pub predicted_runtime_seconds: i64,
    #[serde(default)]
    pub predicted_peak_vram_bytes: i64,
    #[serde(default)]
    pub business_value: i64,
    #[serde(default)]
    pub deadline_unix_seconds: i64,
    /// Normalized scheduling priority. `ksolver.dev/priority` overrides Kubernetes `spec.priority`.
    #[serde(default)]
    pub priority: i64,
    #[serde(default)]
    pub priority_class_name: String,
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
    /// *Fully-modeled* hostname pod-anti-affinity selectors (In/NotIn/Exists/DoesNotExist, no
    /// namespace scoping), computed from the raw affinity by the collector. Each inner Vec is
    /// one labelSelector's requirements (ANDed). Used for anti-affinity symmetry enforcement.
    #[serde(default)]
    pub modeled_host_anti_selectors: Vec<AntiAffinitySelector>,
    /// `(topologyKey, selector)` of *fully-modeled* NON-hostname pod-anti-affinity terms (e.g.
    /// zone/rack). Same strict rules. Enforced best-effort by topology-domain exclusion (Phase 12).
    #[serde(default)]
    pub anti_affinity_topology_selectors: Vec<(String, AntiAffinitySelector)>,
    /// This pod's preferred (soft) pod affinity + anti-affinity terms, for SYMMETRIC soft scoring
    /// (a running pod's preferred terms steer an incoming matching pod). Collected from raw affinity.
    #[serde(default)]
    pub preferred_pod_affinity: Vec<PreferredPodTerm>,
    /// Required node affinity as OR-of-terms: the outer Vec is OR (nodeSelectorTerms), each
    /// `NodeAffinityGroup` is one term (its matchExpressions ANDed against labels, matchFields
    /// ANDed against node fields). No required affinity ⇒ unconstrained; required affinity whose
    /// terms are all empty ⇒ selects nothing (kube semantics).
    #[serde(default)]
    pub required_node_affinity: Vec<NodeAffinityGroup>,
    #[serde(default)]
    pub topology_spread_constraints: i32,
    #[serde(default)]
    pub topology_spread_rules: Vec<TopologySpreadRule>,
    #[serde(default)]
    pub pvcs: Vec<String>,
    #[serde(default)]
    pub disruption_cost: i32,
    #[serde(default = "default_true")]
    pub migration_allowed: bool,
    #[serde(default = "default_true")]
    pub preemption_allowed: bool,
    #[serde(default)]
    pub do_not_disrupt: bool,
    #[serde(default)]
    pub checkpoint_age_seconds: i64,
    #[serde(default)]
    pub progress_percent: i32,
    #[serde(default)]
    pub autoscaler_not_safe_to_evict: bool,
}

impl Default for Pod {
    fn default() -> Self {
        Self {
            namespace: String::new(),
            name: String::new(),
            uid: String::new(),
            node_name: String::new(),
            phase: String::new(),
            start_time_unix: 0,
            finish_time_unix: 0,
            owner_kind: String::new(),
            owner_name: String::new(),
            deleting: false,
            labels: BTreeMap::new(),
            team: String::new(),
            container_images: Vec::new(),
            command_hash: String::new(),
            predicted_runtime_seconds: 0,
            predicted_peak_vram_bytes: 0,
            business_value: 0,
            deadline_unix_seconds: 0,
            priority: 0,
            priority_class_name: String::new(),
            qos_class: String::new(),
            requests: ResourceList::default(),
            extended_resource_requests: BTreeMap::new(),
            usage: ResourceUsage::default(),
            memory_history: MemoryHistory::default(),
            tolerations: Vec::new(),
            node_selector: BTreeMap::new(),
            required_affinity: Vec::new(),
            required_anti: Vec::new(),
            modeled_host_anti_selectors: Vec::new(),
            anti_affinity_topology_selectors: Vec::new(),
            preferred_pod_affinity: Vec::new(),
            required_node_affinity: Vec::new(),
            topology_spread_constraints: 0,
            topology_spread_rules: Vec::new(),
            pvcs: Vec::new(),
            disruption_cost: 0,
            migration_allowed: true,
            preemption_allowed: true,
            do_not_disrupt: false,
            checkpoint_age_seconds: 0,
            progress_percent: 0,
            autoscaler_not_safe_to_evict: false,
        }
    }
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
    pub selector: Vec<LabelSelectorReq>,
    #[serde(default = "default_true")]
    pub selector_modeled: bool,
    #[serde(default)]
    pub min_available: String,
    #[serde(default)]
    pub max_unavailable: String,
    #[serde(default)]
    pub disruptions_allowed: i32,
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

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct NodeAffinityTerm {
    #[serde(default)]
    pub key: String,
    #[serde(default)]
    pub operator: String,
    #[serde(default)]
    pub values: Vec<String>,
}

/// One requirement of a pod label selector (matchLabels lowers to `In [v]`; matchExpressions
/// carried as-is). Supported operators: In, NotIn, Exists, DoesNotExist. Used for best-effort
/// pod anti-affinity selectors in the shadow scheduler.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct LabelSelectorReq {
    #[serde(default)]
    pub key: String,
    #[serde(default)]
    pub operator: String,
    #[serde(default)]
    pub values: Vec<String>,
}

/// A modeled pod-anti-affinity selector: label-selector requirements (ANDed) plus a namespace
/// scope. `namespaces` empty ⇒ the pod's own namespace (Kubernetes default); non-empty ⇒ that
/// explicit list (own namespace NOT auto-included unless listed). `namespaceSelector`-scoped
/// terms are not modeled (left to the "pod anti-affinity" caveat).
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct AntiAffinitySelector {
    #[serde(default)]
    pub reqs: Vec<LabelSelectorReq>,
    #[serde(default)]
    pub namespaces: Vec<String>,
    /// `namespaceSelector` scope (F-CNS-2): `None` = not set; `Some([])` = empty selector = ALL
    /// namespaces; `Some(reqs)` = namespaces whose labels match. Union'd with `namespaces`.
    #[serde(default)]
    pub namespace_selector: Option<Vec<LabelSelectorReq>>,
}

/// A `preferredDuringScheduling` node-affinity term: a `weight` (1–100) plus a preference selector.
/// `exprs` match node labels; `fields` match node fields. A node earns `weight` toward its soft
/// score when ALL requirements match. Field support follows required affinity's narrow Kubernetes
/// subset: `metadata.name` with `In`/`NotIn`.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct PreferredNodeTerm {
    #[serde(default)]
    pub weight: i64,
    #[serde(default)]
    pub exprs: Vec<NodeAffinityTerm>,
    #[serde(default)]
    pub fields: Vec<NodeAffinityTerm>,
}

/// A `preferredDuringScheduling` pod (anti-)affinity term: a `weight` (1–100), a `topology_key`,
/// a label `selector` (reqs + namespace scope, reusing `AntiAffinitySelector`), and an `anti` flag.
/// A candidate node accumulates `+weight` (affinity) or `-weight` (anti-affinity) toward its soft
/// score for each matching running pod sharing the node's topology domain. Used both forward (a
/// pending pod's own terms) and symmetrically (a running pod's terms steering the pending pod).
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct PreferredPodTerm {
    #[serde(default)]
    pub weight: i64,
    #[serde(default)]
    pub topology_key: String,
    #[serde(default)]
    pub selector: AntiAffinitySelector,
    #[serde(default)]
    pub anti: bool,
}

/// One `nodeSelectorTerm`: `match_expressions` are evaluated against node LABELS, `match_fields`
/// against node FIELDS (k8s allows only `metadata.name`, operators In/NotIn, exactly one value).
/// A term matches iff all its expressions AND all its fields match.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct NodeAffinityGroup {
    #[serde(default)]
    pub match_expressions: Vec<NodeAffinityTerm>,
    #[serde(default)]
    pub match_fields: Vec<NodeAffinityTerm>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct TopologySpreadRule {
    #[serde(default)]
    pub max_skew: i32,
    #[serde(default)]
    pub topology_key: String,
    #[serde(default)]
    pub when_unsatisfiable: String,
    /// Advanced Kubernetes topology-spread knobs. The shadow scheduler currently models only the
    /// base hard-spread semantics, so explicit advanced fields are carried for caveats rather than
    /// silently treated as exact.
    #[serde(default)]
    pub min_domains: Option<i32>,
    #[serde(default)]
    pub node_affinity_policy: Option<String>,
    #[serde(default)]
    pub node_taints_policy: Option<String>,
    #[serde(default)]
    pub match_label_keys: Vec<String>,
    /// Backward-compatible matchLabels map used by older traces and tests. New code should prefer
    /// `selector_reqs`, which also carries matchExpressions.
    #[serde(default)]
    pub selector: BTreeMap<String, String>,
    /// Modeled label selector requirements. `matchLabels` lowers to `In [value]`; supported
    /// matchExpressions are In, NotIn, Exists, and DoesNotExist.
    #[serde(default)]
    pub selector_reqs: Vec<LabelSelectorReq>,
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
    pub pdbs: Vec<DisruptionBudget>,
    /// Namespace name → labels, for `namespaceSelector`-scoped anti-affinity (F-CNS-2).
    #[serde(default)]
    pub namespace_labels: BTreeMap<String, BTreeMap<String, String>>,
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

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct NormalizedWorkload {
    pub namespace: String,
    pub name: String,
    /// Kubernetes `metadata.uid` (stable pod identity; empty if unavailable). Used to detect a pod
    /// recreated under the same namespace/name when validating a rendered binding.
    #[serde(default)]
    pub uid: String,
    #[serde(default)]
    pub labels: BTreeMap<String, String>,
    /// Optional tenant/team owner hint from `ksolver.dev/team`.
    #[serde(default)]
    pub team: String,
    /// Fully-modeled hostname pod-anti-affinity selectors (reqs + namespace scope) for symmetry
    /// enforcement in the shadow scheduler.
    #[serde(default)]
    pub anti_affinity_host_selectors: Vec<AntiAffinitySelector>,
    /// `(topologyKey, selector)` of this workload's fully-modeled NON-hostname pod-anti-affinity
    /// terms (zone/rack), for topology-domain exclusion (Phase 12).
    #[serde(default)]
    pub anti_affinity_topology_selectors: Vec<(String, AntiAffinitySelector)>,
    /// This workload's preferred (soft) pod affinity + anti-affinity terms, for SYMMETRIC soft
    /// scoring of pending pods (a running pod's preferred terms steer an incoming matching pod).
    #[serde(default)]
    pub preferred_pod_affinity: Vec<PreferredPodTerm>,
    #[serde(default)]
    pub owner_kind: String,
    #[serde(default)]
    pub owner_name: String,
    #[serde(default)]
    pub current_node: String,
    #[serde(default)]
    pub priority: i64,
    #[serde(default)]
    pub priority_class_name: String,
    #[serde(default)]
    pub business_value: i64,
    #[serde(default)]
    pub deadline_unix_seconds: i64,
    #[serde(default)]
    pub predicted_runtime_seconds: i64,
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
    pub topology_spread_rules: Vec<TopologySpreadRule>,
    #[serde(default)]
    pub qos_class: String,
    #[serde(default)]
    pub autoscaler_not_safe_to_evict: bool,
    #[serde(default = "default_true")]
    pub migration_allowed: bool,
    #[serde(default = "default_true")]
    pub preemption_allowed: bool,
    #[serde(default)]
    pub disruption_cost: i32,
    #[serde(default)]
    pub do_not_disrupt: bool,
    #[serde(default)]
    pub checkpoint_age_seconds: i64,
    #[serde(default)]
    pub progress_percent: i32,
    #[serde(default)]
    pub running_age_seconds: i64,
    #[serde(default)]
    pub reasons: Vec<String>,
    #[serde(default)]
    pub candidate_levels: Vec<CandidateLevel>,
}

impl Default for NormalizedWorkload {
    fn default() -> Self {
        Self {
            namespace: String::new(),
            name: String::new(),
            uid: String::new(),
            labels: BTreeMap::new(),
            team: String::new(),
            anti_affinity_host_selectors: Vec::new(),
            anti_affinity_topology_selectors: Vec::new(),
            preferred_pod_affinity: Vec::new(),
            owner_kind: String::new(),
            owner_name: String::new(),
            current_node: String::new(),
            priority: 0,
            priority_class_name: String::new(),
            business_value: 0,
            deadline_unix_seconds: 0,
            predicted_runtime_seconds: 0,
            current_requests: ResourceList::default(),
            recommended_requests: ResourceList::default(),
            requests: ResourceList::default(),
            extended_resource_requests: BTreeMap::new(),
            usage: ResourceUsage::default(),
            feasible_nodes: 0,
            feasible_node_names: Vec::new(),
            pinned_by_volume: false,
            has_required_affinity: false,
            has_required_anti_affinity: false,
            has_required_node_affinity: false,
            topology_spread_constraints: 0,
            topology_spread_rules: Vec::new(),
            qos_class: String::new(),
            autoscaler_not_safe_to_evict: false,
            migration_allowed: true,
            preemption_allowed: true,
            disruption_cost: 0,
            do_not_disrupt: false,
            checkpoint_age_seconds: 0,
            progress_percent: 0,
            running_age_seconds: 0,
            reasons: Vec::new(),
            candidate_levels: Vec::new(),
        }
    }
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
    #[serde(default)]
    pub constraint_cost_table: ConstraintCostTable,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct ConstraintCostRow {
    #[serde(default)]
    pub constraint_key: String,
    #[serde(default)]
    pub display_name: String,
    #[serde(default)]
    pub baseline_savings: Money,
    #[serde(default)]
    pub relaxed_savings: Money,
    #[serde(default)]
    pub delta: Money,
    #[serde(default)]
    pub affected_workload_count: i32,
    #[serde(default)]
    pub affected_node_count: i32,
    #[serde(default)]
    pub nodes_removable_baseline: i32,
    #[serde(default)]
    pub nodes_removable_relaxed: i32,
    #[serde(default)]
    pub action: String,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct ConstraintCostTable {
    #[serde(default)]
    pub rows: Vec<ConstraintCostRow>,
    #[serde(default)]
    pub baseline_savings: Money,
    #[serde(default)]
    pub theoretical_max_savings: Money,
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
    /// Scheduler mode: place what fits and leave the rest unplaced instead of
    /// failing the whole solve. Off for the offline planner. See cpsat_rust::solve.
    #[serde(default)]
    pub partial_admission: bool,
    /// Per-workload admission reward when `partial_admission` is set. 0 = auto-compute
    /// a weight that dominates the rest of the objective (maximize admitted count first).
    #[serde(default)]
    pub admission_weight: i64,
    /// CP-SAT wall-clock time limit in seconds. 0 = default (600). Lets benchmarks cap solves.
    #[serde(default)]
    pub solve_time_limit_secs: i64,
    /// Enable a soft-affinity tie-break pass: after the cost-optimal solve, among equally-optimal
    /// placements prefer higher preferred-affinity score. Shadow-only; never changes admission/cost.
    #[serde(default)]
    pub enable_soft_affinity: bool,
    /// Objective semantics for the solve. The default keeps the original cost/binpack objective.
    #[serde(default)]
    pub objective_profile: ObjectiveProfile,
    /// Profile-specific objective weights. These are additive; legacy top-level weights remain the
    /// source of truth for the cost/binpack objective.
    #[serde(default)]
    pub objective_weights: ObjectiveWeights,
}

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum ObjectiveProfile {
    /// Original objective: maximize admitted workload count in partial-admission mode, then minimize
    /// node cost, active nodes, slack, and churn.
    #[default]
    CostBinpack,
    /// GPU scheduler objective: maximize admitted GPU work/gangs in partial-admission mode, then
    /// minimize the same cost/binpack terms.
    GpuGangAware,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ObjectiveWeights {
    /// Base score for admitting any workload under GPU-aware partial admission.
    #[serde(default = "default_gpu_admission_score")]
    pub admission: i64,
    /// Additional score per requested GPU under GPU-aware partial admission.
    #[serde(default = "default_gpu_demand_score")]
    pub gpu_demand: i64,
    /// Additional score for each replica in a multi-pod gang under GPU-aware partial admission.
    #[serde(default = "default_gpu_gang_complete_score")]
    pub gang_complete: i64,
    /// Additional score per normalized workload priority point under GPU-aware partial admission.
    /// Defaults to 0 so priority is inert unless explicitly enabled.
    #[serde(default)]
    pub priority: i64,
    /// Additional score per `ksolver.dev/business-value` point under GPU-aware partial admission.
    /// Defaults to 0 so business value is inert unless explicitly enabled.
    #[serde(default)]
    pub business_value: i64,
    /// Additional score per configured `ksolver.dev/queue` point under GPU-aware partial admission.
    /// Defaults to 0 so queue policy is inert unless explicitly enabled.
    #[serde(default)]
    pub queue: i64,
    /// Additional score per queued minute under GPU-aware partial admission. Queue wait is derived
    /// from Kubernetes `metadata.creationTimestamp` and bounded before scoring. Defaults to 0.
    #[serde(default)]
    pub queue_wait: i64,
    /// Additional score per fair-share deficit GPU under GPU-aware partial admission. The shadow
    /// scheduler computes a bounded per-workload deficit from configured tenant weights and current
    /// running GPU usage. Defaults to 0 so fair-share remains observational unless enabled.
    #[serde(default)]
    pub fair_share: i64,
    /// Additional score for explicit-deadline workloads under GPU-aware partial admission.
    /// Deadline urgency is computed from latest start time (`deadline - predicted_runtime`);
    /// defaults to 0 so deadlines are observational unless explicitly enabled.
    #[serde(default)]
    pub deadline_urgency: i64,
    /// Admission-score penalty for explicit-deadline workloads whose predicted runtime already
    /// exceeds the remaining time. Defaults to 0 so misses remain observational unless enabled.
    #[serde(default)]
    pub deadline_miss: i64,
    /// Extra penalty for idle GPU slots on active nodes under GPU-aware profiles. This is added to
    /// the existing scalar slack penalty, so cost/binpack callers are unaffected.
    #[serde(default)]
    pub gpu_fragmentation: i64,
}

impl Default for ObjectiveWeights {
    fn default() -> Self {
        Self {
            admission: default_gpu_admission_score(),
            gpu_demand: default_gpu_demand_score(),
            gang_complete: default_gpu_gang_complete_score(),
            priority: 0,
            business_value: 0,
            queue: 0,
            queue_wait: 0,
            fair_share: 0,
            deadline_urgency: 0,
            deadline_miss: 0,
            gpu_fragmentation: 0,
        }
    }
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
            partial_admission: false,
            admission_weight: 0,
            solve_time_limit_secs: 0,
            enable_soft_affinity: false,
            objective_profile: ObjectiveProfile::CostBinpack,
            objective_weights: ObjectiveWeights::default(),
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
    /// Hard resource caps over groups of workloads (e.g. per-namespace GPU quota).
    /// Empty by default; only the shadow scheduler sets these. Enforced by the
    /// solver as `Σ total_resource_w · placed[w] ≤ limit` (requires partial_admission).
    #[serde(default)]
    pub quota_groups: Vec<QuotaGroup>,
    /// Hard monthly-cost caps over groups of workloads (e.g. per-tenant budget).
    /// Costs are expressed in milli-currency units and charged per selected replica/node edge.
    #[serde(default)]
    pub budget_groups: Vec<BudgetGroup>,
    /// Soft co-placement rewards between two *pending* workloads that prefer each other
    /// (`preferredDuringScheduling` pod affinity). Empty by default; only the shadow scheduler
    /// sets these. Applied ONLY in the Phase-2 soft pass — never changes admission or cost.
    #[serde(default)]
    pub soft_coplacement_pairs: Vec<SoftCoplacement>,
}

/// One topology domain shared by a co-placement pair: the domain's nodes that are feasible for `a`
/// and (separately) for `b`. `both` can be rewarded only when `a` places in some `a_nodes` AND `b`
/// in some `b_nodes` (same domain).
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct CoplacementDomain {
    #[serde(default)]
    pub a_nodes: Vec<String>,
    #[serde(default)]
    pub b_nodes: Vec<String>,
}

/// A soft co-placement reward: workloads `a` and `b` (by `OptimizationWorkload.id`) prefer to share
/// a topology domain. Phase 2 rewards `weight` for each domain both land in. Affinity (reward) only;
/// never changes admission/cost (added after the cost + admitted set are pinned).
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct SoftCoplacement {
    #[serde(default)]
    pub a: String,
    #[serde(default)]
    pub b: String,
    #[serde(default)]
    pub weight: i64,
    #[serde(default)]
    pub domains: Vec<CoplacementDomain>,
}

/// A hard cap on the total amount of `resource` consumed by admitted workloads in
/// `workload_ids`. Used for per-namespace (tenant) GPU quotas in the shadow scheduler.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct QuotaGroup {
    #[serde(default)]
    pub workload_ids: Vec<String>,
    /// Resource names summed toward this quota (e.g. `nvidia.com/gpu` + `nvidia.com/mig-*`
    /// slices). A workload's contribution is the sum of its totals over these resources.
    #[serde(default)]
    pub resources: Vec<String>,
    #[serde(default)]
    pub limit: i64,
}

/// A hard cap on admitted monthly placement cost for a workload group. `limit_milli` is the
/// remaining budget after already-running work is charged by the caller.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct BudgetGroup {
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub workload_ids: Vec<String>,
    #[serde(default)]
    pub limit_milli: i64,
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
    /// Normalized priority score used only by GPU-aware admission objectives.
    #[serde(default)]
    pub priority: i64,
    #[serde(default)]
    pub priority_class_name: String,
    #[serde(default)]
    pub team: String,
    #[serde(default)]
    pub queue: String,
    /// Bounded queue-policy score, stamped by shadow mode from operator config.
    #[serde(default)]
    pub queue_score: i64,
    /// Seconds since Kubernetes `metadata.creationTimestamp` for pending workloads.
    #[serde(default)]
    pub queue_wait_seconds: i64,
    #[serde(default)]
    pub business_value: i64,
    /// Bounded fair-share deficit score, stamped by shadow mode before solving.
    #[serde(default)]
    pub fair_share_deficit: i64,
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
    pub feasible_nodes: Vec<String>,
    #[serde(default)]
    pub candidate_levels: Vec<CandidateLevel>,
    /// Require all replicas of this gang on a single node (co-location). Assumes
    /// physical-node inputs (OptimizationNode.count == 1); set only by the shadow
    /// scheduler, never by the offline planner.
    #[serde(default)]
    pub colocate: bool,
    /// Per-node soft (preferred) affinity score: node name → summed preferred weight if a replica
    /// lands there. Used only by the soft-affinity tie-break pass; never affects admission/cost.
    #[serde(default)]
    pub soft_scores: BTreeMap<String, i64>,
}

pub fn is_gpu_resource_name(name: &str) -> bool {
    name == "nvidia.com/gpu" || name.starts_with("nvidia.com/mig-") || name.contains("/gpu")
}

pub fn optimization_workload_gpu_request(workload: &OptimizationWorkload) -> i64 {
    workload
        .extended_resource_requests
        .iter()
        .filter(|(name, _)| is_gpu_resource_name(name))
        .map(|(_, value)| (*value).max(0))
        .sum()
}

fn ceil_div_positive(a: i64, b: i64) -> i64 {
    (a + b - 1) / b
}

pub fn flexible_replica_bounds(workload: &OptimizationWorkload) -> Option<(i64, i64)> {
    let group_size = i64::from(workload.group_size).max(0);
    if !workload.flexible || group_size <= 1 {
        return None;
    }
    if workload.min_gpus <= 0 && workload.preferred_gpus <= 0 && workload.max_gpus <= 0 {
        return None;
    }
    let total_gpu = optimization_workload_gpu_request(workload);
    if total_gpu <= 0 {
        return None;
    }
    let per_replica_gpu = ceil_div_positive(total_gpu, group_size).max(1);
    let min_replicas = if workload.min_gpus > 0 {
        ceil_div_positive(workload.min_gpus, per_replica_gpu)
    } else {
        1
    }
    .clamp(1, group_size);
    let mut max_gpu = if workload.preferred_gpus > 0 {
        workload.preferred_gpus
    } else if workload.max_gpus > 0 {
        workload.max_gpus
    } else {
        total_gpu
    };
    if workload.max_gpus > 0 {
        max_gpu = max_gpu.min(workload.max_gpus);
    }
    let max_replicas = ceil_div_positive(max_gpu.max(workload.min_gpus).max(1), per_replica_gpu)
        .clamp(min_replicas, group_size);
    (min_replicas < group_size || max_replicas < group_size).then_some((min_replicas, max_replicas))
}

fn predicted_runtime_for_replicas(
    full_runtime_seconds: i64,
    full_replicas: i64,
    replicas: i64,
) -> i64 {
    if full_runtime_seconds <= 0 || full_replicas <= 0 || replicas <= 0 {
        return 0;
    }
    let ratio = (full_replicas as f64 / replicas as f64).sqrt();
    ((full_runtime_seconds as f64) * ratio).ceil() as i64
}

pub fn deadline_adjusted_flexible_replica_bounds(
    workload: &OptimizationWorkload,
    now_unix_seconds: i64,
) -> Option<(i64, i64)> {
    let (min_replicas, max_replicas) = flexible_replica_bounds(workload)?;
    if workload.deadline_unix_seconds <= 0 || workload.predicted_runtime_seconds <= 0 {
        return Some((min_replicas, max_replicas));
    }
    let remaining = workload
        .deadline_unix_seconds
        .saturating_sub(now_unix_seconds);
    if remaining <= 0 {
        return Some((min_replicas, max_replicas));
    }
    let group_size = i64::from(workload.group_size).max(1);
    for replicas in min_replicas..=max_replicas {
        let predicted_runtime = predicted_runtime_for_replicas(
            workload.predicted_runtime_seconds,
            group_size,
            replicas,
        );
        if predicted_runtime > 0 && predicted_runtime <= remaining {
            return Some((min_replicas, replicas));
        }
    }
    Some((min_replicas, max_replicas))
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
        deadline_adjusted_flexible_replica_bounds, flexible_replica_bounds,
        optimization_workload_gpu_request, AnalysisReport, AutoscalerBlockerSummary,
        ExplainabilityReport, InflationDriver, MemoryRiskSummary, Money, NormalizedCluster,
        NormalizedNode, NormalizedWorkload, OptimizationPlan, OptimizationWorkload,
        PlacementMemoryRisk, ResourceAllocation, ResourceList, ResourceSummary, ResourceUsage,
        SolverInfo,
    };
    use std::collections::BTreeMap;

    fn flexible_gpu_workload(group_size: i32, total_gpu: i64) -> OptimizationWorkload {
        OptimizationWorkload {
            group_size,
            extended_resource_requests: BTreeMap::from([("nvidia.com/gpu".to_string(), total_gpu)]),
            flexible: true,
            ..Default::default()
        }
    }

    #[test]
    fn optimization_workload_gpu_request_counts_whole_mig_and_gpu_like_resources() {
        let workload = OptimizationWorkload {
            extended_resource_requests: BTreeMap::from([
                ("nvidia.com/gpu".to_string(), 2),
                ("nvidia.com/mig-1g.5gb".to_string(), 3),
                ("vendor.example/gpu".to_string(), 4),
                ("cpu".to_string(), 99),
            ]),
            ..Default::default()
        };

        assert_eq!(optimization_workload_gpu_request(&workload), 9);
    }

    #[test]
    fn flexible_replica_bounds_convert_gpu_hints_to_replica_bounds() {
        let mut workload = flexible_gpu_workload(8, 8);
        workload.min_gpus = 2;
        workload.preferred_gpus = 4;
        workload.max_gpus = 8;

        assert_eq!(flexible_replica_bounds(&workload), Some((2, 4)));
    }

    #[test]
    fn flexible_replica_bounds_disabled_for_nonflexible_singletons_and_full_size() {
        let mut nonflexible = flexible_gpu_workload(8, 8);
        nonflexible.flexible = false;
        assert_eq!(flexible_replica_bounds(&nonflexible), None);

        let singleton = flexible_gpu_workload(1, 1);
        assert_eq!(flexible_replica_bounds(&singleton), None);

        let full_size = flexible_gpu_workload(8, 8);
        assert_eq!(flexible_replica_bounds(&full_size), None);
    }

    #[test]
    fn deadline_adjusted_flexible_bounds_cap_to_smallest_replicas_that_meet_deadline() {
        let mut workload = flexible_gpu_workload(8, 8);
        workload.min_gpus = 2;
        workload.preferred_gpus = 8;
        workload.max_gpus = 8;
        workload.predicted_runtime_seconds = 3600;
        workload.deadline_unix_seconds = 10_000;

        assert_eq!(
            deadline_adjusted_flexible_replica_bounds(&workload, 0),
            Some((2, 2))
        );
        assert_eq!(
            deadline_adjusted_flexible_replica_bounds(&workload, 4_800),
            Some((2, 4))
        );
        assert_eq!(
            deadline_adjusted_flexible_replica_bounds(&workload, 9_000),
            Some((2, 8))
        );
    }

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

fn default_gpu_admission_score() -> i64 {
    1
}

fn default_gpu_demand_score() -> i64 {
    1
}

fn default_gpu_gang_complete_score() -> i64 {
    2
}
