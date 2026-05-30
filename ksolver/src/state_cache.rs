use crate::model::{
    AffinityTerm, AnalysisReport, ClusterSnapshot, NormalizedCluster, OptimizationInput, Pod,
    SolveRequest, Taint, Toleration,
};
use crate::normalizer::Options as NormalizerOptions;
use crate::pricing::PricingCatalog;
use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::path::{Path, PathBuf};
use std::time::Duration;
use tokio::fs;

const DEFAULT_CACHE_ROOT: &str = "/tmp";
const DEFAULT_MAX_AGE_HOURS: u64 = 12;
const SNAPSHOT_SCHEMA_VERSION: u32 = 1;
const NORMALIZATION_CACHE_SCHEMA_VERSION: u32 = 3;
const OPTIMIZATION_INPUT_CACHE_SCHEMA_VERSION: u32 = 2;

pub fn snapshot_schema_version() -> u32 {
    SNAPSHOT_SCHEMA_VERSION
}

#[derive(Debug, Clone)]
pub struct LatestRun {
    pub request: Option<SolveRequest>,
    pub report: AnalysisReport,
    pub state_root: PathBuf,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SnapshotEntry {
    #[serde(default)]
    pub cluster_name: String,
    #[serde(default)]
    pub snapshot_file: String,
    #[serde(default)]
    pub state_root: String,
    #[serde(default)]
    pub collected_at: String,
}

#[derive(Debug, Clone)]
pub struct ScenarioRun {
    pub scenario_name: String,
    pub request: Option<SolveRequest>,
    pub report: AnalysisReport,
    pub state_root: PathBuf,
    pub modified: std::time::SystemTime,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ScenarioSummary {
    #[serde(default)]
    pub cluster_name: String,
    #[serde(default)]
    pub generated_at_epoch_ms: i64,
    #[serde(default)]
    pub baseline_scenario: String,
    #[serde(default)]
    pub ranked_levers: Vec<serde_json::Value>,
    #[serde(default)]
    pub pool_impacts: Vec<PoolImpactSummary>,
    #[serde(default)]
    pub scenarios: Vec<serde_json::Value>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PoolImpactSummary {
    #[serde(default)]
    pub pool: String,
    #[serde(default)]
    pub scenario_name: String,
    #[serde(default)]
    pub current_monthly_cost: f64,
    #[serde(default)]
    pub current_node_count: i32,
    #[serde(default)]
    pub removable: bool,
    #[serde(default)]
    pub optimized_monthly: f64,
    #[serde(default)]
    pub savings_monthly: f64,
    #[serde(default)]
    pub delta_vs_strict_savings: f64,
    #[serde(default)]
    pub delta_vs_strict_active_nodes: i32,
    #[serde(default)]
    pub moves: i32,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct HistoryEntry {
    #[serde(default)]
    pub timestamp: String,
    #[serde(default)]
    pub cluster_name: String,
    #[serde(default)]
    pub node_count: i32,
    #[serde(default)]
    pub workload_count: i32,
    #[serde(default)]
    pub current_monthly_cost: f64,
    #[serde(default)]
    pub optimized_monthly_cost: f64,
    #[serde(default)]
    pub savings_monthly: f64,
    #[serde(default)]
    pub active_nodes: i32,
    #[serde(default)]
    pub moves: i32,
}

#[derive(Debug, Clone)]
pub struct StateCache {
    root: PathBuf,
    max_age: Duration,
}

impl StateCache {
    pub fn for_request(req: &SolveRequest) -> Self {
        let cache_root = std::env::var("SYSLENS_SOLVER_STATE_ROOT")
            .map(PathBuf::from)
            .unwrap_or_else(|_| PathBuf::from(DEFAULT_CACHE_ROOT));
        let max_age_hours = std::env::var("SYSLENS_SOLVER_STATE_MAX_AGE_HOURS")
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
            .unwrap_or(DEFAULT_MAX_AGE_HOURS);
        let cluster = sanitize_cluster_name(&req.cluster_name);

        Self {
            root: cache_root.join(cluster).join("state"),
            max_age: Duration::from_secs(max_age_hours * 60 * 60),
        }
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    pub async fn load_snapshot_if_fresh(&self) -> Result<Option<ClusterSnapshot>> {
        self.load_snapshot_from_path(&self.snapshot_path(), true)
            .await
    }

    pub async fn load_snapshot_from_file<P: AsRef<Path>>(
        &self,
        path: P,
    ) -> Result<ClusterSnapshot> {
        self.load_snapshot_from_path(path.as_ref(), false)
            .await?
            .context("snapshot file not found or unreadable")
    }

    async fn load_snapshot_from_path(
        &self,
        path: &Path,
        require_fresh: bool,
    ) -> Result<Option<ClusterSnapshot>> {
        let metadata = match fs::metadata(&path).await {
            Ok(metadata) => metadata,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(err) => return Err(err).with_context(|| format!("stat {}", path.display())),
        };

        if require_fresh {
            if let Ok(modified) = metadata.modified() {
                if modified.elapsed().unwrap_or(Duration::MAX) > self.max_age {
                    return Ok(None);
                }
            }
        }

        let bytes = fs::read(&path)
            .await
            .with_context(|| format!("read {}", path.display()))?;
        let snapshot = serde_json::from_slice::<ClusterSnapshot>(&bytes)
            .with_context(|| format!("decode {}", path.display()))?;
        if require_fresh && snapshot.metadata.schema_version != snapshot_schema_version() {
            return Ok(None);
        }
        Ok(Some(snapshot))
    }

    pub async fn write_snapshot(&self, snapshot: &ClusterSnapshot) -> Result<()> {
        let mut snapshot = snapshot.clone();
        snapshot.metadata.schema_version = snapshot_schema_version();
        self.write_json("snapshot.json", &snapshot).await
    }

    pub async fn write_request(&self, req: &SolveRequest) -> Result<()> {
        self.write_json("latest-request.json", req).await
    }

    pub async fn write_report(&self, report: &AnalysisReport) -> Result<()> {
        self.write_json("latest-report.json", report).await
    }

    // @lineage
    // reads: report.normalized.*, report.optimization.*
    pub async fn append_history(&self, report: &AnalysisReport) -> Result<()> {
        let opt = report.optimization.as_ref();
        let entry = HistoryEntry {
            timestamp: chrono::Utc::now().to_rfc3339(),
            cluster_name: report.normalized.cluster_name.clone(),
            node_count: report.normalized.nodes.len() as i32,
            workload_count: report.normalized.workloads.len() as i32,
            current_monthly_cost: opt.map(|o| o.current_monthly_cost.monthly).unwrap_or(0.0),
            optimized_monthly_cost: opt.map(|o| o.optimized_monthly_cost.monthly).unwrap_or(0.0),
            savings_monthly: opt.map(|o| o.savings_monthly.monthly).unwrap_or(0.0),
            active_nodes: opt.map(|o| o.active_nodes.len() as i32).unwrap_or(0),
            moves: opt.map(|o| o.recommended_moves.len() as i32).unwrap_or(0),
        };
        let line = serde_json::to_string(&entry)?;
        let path = self.root.join("history.jsonl");
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).await.ok();
        }
        use tokio::io::AsyncWriteExt;
        let mut file = tokio::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .await?;
        file.write_all(format!("{line}\n").as_bytes()).await?;
        Ok(())
    }

    pub async fn load_history(&self) -> Result<Vec<HistoryEntry>> {
        let path = self.root.join("history.jsonl");
        let data = match fs::read_to_string(&path).await {
            Ok(data) => data,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(err) => return Err(err.into()),
        };
        let entries: Vec<HistoryEntry> = data
            .lines()
            .filter_map(|line| serde_json::from_str(line).ok())
            .collect();
        Ok(entries)
    }

    pub async fn write_scenario_request(
        &self,
        scenario_name: &str,
        req: &SolveRequest,
    ) -> Result<()> {
        self.write_json_at(&self.scenario_request_path(scenario_name), req)
            .await
    }

    pub async fn write_scenario_report(
        &self,
        scenario_name: &str,
        report: &AnalysisReport,
    ) -> Result<()> {
        self.write_json_at(&self.scenario_report_path(scenario_name), report)
            .await
    }

    pub async fn load_latest_request(&self) -> Result<Option<SolveRequest>> {
        self.load_json_from_path(&self.root.join("latest-request.json"))
            .await
    }

    pub async fn load_latest_report(&self) -> Result<Option<AnalysisReport>> {
        self.load_json_from_path(&self.root.join("latest-report.json"))
            .await
    }

    pub async fn load_scenario_runs(&self) -> Result<Vec<ScenarioRun>> {
        let scenarios_root = self.root.join("scenarios");
        let mut entries = match fs::read_dir(&scenarios_root).await {
            Ok(entries) => entries,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(err) => {
                return Err(err).with_context(|| format!("read_dir {}", scenarios_root.display()));
            }
        };

        let mut runs = Vec::new();
        while let Some(entry) = entries.next_entry().await? {
            let entry_type = entry
                .file_type()
                .await
                .with_context(|| format!("file_type {}", entry.path().display()))?;
            if !entry_type.is_dir() {
                continue;
            }

            let scenario_root = entry.path();
            let report_path = scenario_root.join("report.json");
            let metadata = match fs::metadata(&report_path).await {
                Ok(metadata) => metadata,
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => continue,
                Err(err) => {
                    return Err(err).with_context(|| format!("stat {}", report_path.display()));
                }
            };
            let modified = metadata
                .modified()
                .unwrap_or(std::time::SystemTime::UNIX_EPOCH);
            let Some(report) = self
                .load_json_from_path::<AnalysisReport>(&report_path)
                .await?
            else {
                continue;
            };
            let request = self
                .load_json_from_path::<SolveRequest>(&scenario_root.join("request.json"))
                .await?;
            runs.push(ScenarioRun {
                scenario_name: entry.file_name().to_string_lossy().to_string(),
                request,
                report,
                state_root: self.root.clone(),
                modified,
            });
        }

        runs.sort_by(|left, right| right.modified.cmp(&left.modified));
        Ok(runs)
    }

    pub async fn load_scenario_summary(&self) -> Result<Option<ScenarioSummary>> {
        self.load_json_from_path(&self.root.join("scenario-summary.json"))
            .await
    }

    pub async fn load_latest_run_any() -> Result<Option<LatestRun>> {
        let cache_root = std::env::var("SYSLENS_SOLVER_STATE_ROOT")
            .map(PathBuf::from)
            .unwrap_or_else(|_| PathBuf::from(DEFAULT_CACHE_ROOT));
        Self::load_latest_run_from_root(&cache_root).await
    }

    pub async fn list_snapshots_any() -> Result<Vec<SnapshotEntry>> {
        let cache_root = std::env::var("SYSLENS_SOLVER_STATE_ROOT")
            .map(PathBuf::from)
            .unwrap_or_else(|_| PathBuf::from(DEFAULT_CACHE_ROOT));

        let mut entries = match fs::read_dir(&cache_root).await {
            Ok(entries) => entries,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(err) => {
                return Err(err).with_context(|| format!("read_dir {}", cache_root.display()));
            }
        };

        let mut snapshots = Vec::new();
        while let Some(entry) = entries.next_entry().await? {
            let entry_type = entry
                .file_type()
                .await
                .with_context(|| format!("file_type {}", entry.path().display()))?;
            if !entry_type.is_dir() {
                continue;
            }

            let state_root = entry.path().join("state");
            let snapshot_path = state_root.join("snapshot.json");
            let snapshot = match fs::read(&snapshot_path).await {
                Ok(bytes) => serde_json::from_slice::<ClusterSnapshot>(&bytes)
                    .with_context(|| format!("decode {}", snapshot_path.display()))?,
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => continue,
                Err(err) => {
                    return Err(err).with_context(|| format!("read {}", snapshot_path.display()));
                }
            };

            snapshots.push(SnapshotEntry {
                cluster_name: snapshot.metadata.name.clone(),
                snapshot_file: snapshot_path.display().to_string(),
                state_root: state_root.display().to_string(),
                collected_at: snapshot
                    .metadata
                    .collected
                    .map(|ts| ts.to_rfc3339())
                    .unwrap_or_default(),
            });
        }

        snapshots.sort_by(|left, right| {
            right
                .collected_at
                .cmp(&left.collected_at)
                .then_with(|| left.cluster_name.cmp(&right.cluster_name))
        });
        Ok(snapshots)
    }

    async fn load_latest_run_from_root(cache_root: &Path) -> Result<Option<LatestRun>> {
        let mut entries = match fs::read_dir(&cache_root).await {
            Ok(entries) => entries,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(err) => {
                return Err(err).with_context(|| format!("read_dir {}", cache_root.display()));
            }
        };

        let mut newest: Option<(std::time::SystemTime, PathBuf)> = None;
        while let Some(entry) = entries.next_entry().await? {
            let entry_type = entry
                .file_type()
                .await
                .with_context(|| format!("file_type {}", entry.path().display()))?;
            if !entry_type.is_dir() {
                continue;
            }

            let state_root = entry.path().join("state");
            let report_path = state_root.join("latest-report.json");
            let metadata = match fs::metadata(&report_path).await {
                Ok(metadata) => metadata,
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => continue,
                Err(err) if err.kind() == std::io::ErrorKind::NotADirectory => continue,
                Err(err) => {
                    return Err(err).with_context(|| format!("stat {}", report_path.display()));
                }
            };
            let modified = metadata
                .modified()
                .unwrap_or(std::time::SystemTime::UNIX_EPOCH);
            if newest
                .as_ref()
                .map(|(current, _)| modified > *current)
                .unwrap_or(true)
            {
                newest = Some((modified, state_root));
            }
        }

        let Some((_, state_root)) = newest else {
            return Ok(None);
        };
        let cache = StateCache {
            root: state_root.clone(),
            max_age: Duration::from_secs(DEFAULT_MAX_AGE_HOURS * 60 * 60),
        };
        let Some(report) = cache.load_latest_report().await? else {
            return Ok(None);
        };
        let request = cache.load_latest_request().await?;

        Ok(Some(LatestRun {
            request,
            report,
            state_root,
        }))
    }

    pub fn snapshot_hash(&self, snapshot: &ClusterSnapshot) -> Result<String> {
        hash_json(&canonical_snapshot(snapshot))
    }

    pub fn pricing_hash(&self, pricing: &PricingCatalog) -> Result<String> {
        hash_json(pricing)
    }

    pub fn normalization_key(
        &self,
        snapshot_hash: &str,
        pricing_hash: &str,
        options: &NormalizerOptions,
    ) -> Result<String> {
        hash_json(&NormalizationCacheKey {
            schema_version: NORMALIZATION_CACHE_SCHEMA_VERSION,
            snapshot_hash: snapshot_hash.to_string(),
            pricing_hash: pricing_hash.to_string(),
            cpu_headroom_percent: canonical_float(options.cpu_headroom_percent),
            memory_headroom_percent: canonical_float(options.memory_headroom_percent),
            storage_headroom_percent: canonical_float(options.storage_headroom_percent),
            pods_headroom: options.pods_headroom,
            disallowed_pools: {
                let mut pools = options.disallowed_pools.clone();
                pools.sort();
                pools
            },
            node_pool_label_keys: {
                let mut keys = options.node_pool_label_keys.clone();
                keys.sort();
                keys
            },
            ignore_taints: options.ignore_taints,
            relax_preferred_affinity: options.relax_preferred_affinity,
            relax_required_anti_affinity: options.relax_required_anti_affinity,
            cpu_overcommit_ratio: canonical_float(options.cpu_overcommit_ratio),
            memory_overcommit_ratio: canonical_float(options.memory_overcommit_ratio),
            use_usage_adjusted_requests: options.use_usage_adjusted_requests,
            usage_request_floor_ratio: canonical_float(options.usage_request_floor_ratio),
            cpu_usage_safety_factor: canonical_float(options.cpu_usage_safety_factor),
            memory_usage_safety_factor: canonical_float(options.memory_usage_safety_factor),
        })
    }

    pub fn optimization_input_key(
        &self,
        normalization_key: &str,
        ignore_unschedulable_workloads: bool,
        allow_grouping: bool,
        allow_node_grouping: bool,
    ) -> Result<String> {
        hash_json(&OptimizationInputCacheKey {
            schema_version: OPTIMIZATION_INPUT_CACHE_SCHEMA_VERSION,
            normalization_key: normalization_key.to_string(),
            ignore_unschedulable_workloads,
            allow_grouping,
            allow_node_grouping,
        })
    }

    pub async fn load_normalized(&self, key: &str) -> Result<Option<NormalizedCluster>> {
        self.load_json_from_path(&self.normalized_path(key)).await
    }

    pub async fn write_normalized(&self, key: &str, normalized: &NormalizedCluster) -> Result<()> {
        self.write_json_at(&self.normalized_path(key), normalized)
            .await
    }

    pub async fn load_optimization_input(&self, key: &str) -> Result<Option<OptimizationInput>> {
        self.load_json_from_path(&self.optimization_input_path(key))
            .await
    }

    pub async fn write_optimization_input(
        &self,
        key: &str,
        input: &OptimizationInput,
    ) -> Result<()> {
        self.write_json_at(&self.optimization_input_path(key), input)
            .await
    }

    fn snapshot_path(&self) -> PathBuf {
        self.root.join("snapshot.json")
    }

    fn normalized_path(&self, key: &str) -> PathBuf {
        self.root.join("normalized").join(format!("{key}.json"))
    }

    fn optimization_input_path(&self, key: &str) -> PathBuf {
        self.root.join("input").join(format!("{key}.json"))
    }

    fn scenario_request_path(&self, scenario_name: &str) -> PathBuf {
        self.root
            .join("scenarios")
            .join(sanitize_cluster_name(scenario_name))
            .join("request.json")
    }

    fn scenario_report_path(&self, scenario_name: &str) -> PathBuf {
        self.root
            .join("scenarios")
            .join(sanitize_cluster_name(scenario_name))
            .join("report.json")
    }

    async fn write_json<T: serde::Serialize>(&self, file_name: &str, value: &T) -> Result<()> {
        let path = self.root.join(file_name);
        self.write_json_at(&path, value).await
    }

    async fn write_json_at<T: serde::Serialize>(&self, path: &Path, value: &T) -> Result<()> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)
                .await
                .with_context(|| format!("create {}", parent.display()))?;
        }
        let tmp = path.with_extension("tmp");
        let bytes = serde_json::to_vec_pretty(value)?;
        fs::write(&tmp, bytes)
            .await
            .with_context(|| format!("write {}", tmp.display()))?;
        fs::rename(&tmp, &path)
            .await
            .with_context(|| format!("rename {} -> {}", tmp.display(), path.display()))?;
        Ok(())
    }

    async fn load_json_from_path<T: serde::de::DeserializeOwned>(
        &self,
        path: &Path,
    ) -> Result<Option<T>> {
        match fs::read(path).await {
            Ok(bytes) => Ok(Some(
                serde_json::from_slice::<T>(&bytes)
                    .with_context(|| format!("decode {}", path.display()))?,
            )),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(err) => Err(err).with_context(|| format!("read {}", path.display())),
        }
    }
}

#[derive(Serialize)]
struct NormalizationCacheKey {
    schema_version: u32,
    snapshot_hash: String,
    pricing_hash: String,
    cpu_headroom_percent: String,
    memory_headroom_percent: String,
    storage_headroom_percent: String,
    pods_headroom: i64,
    disallowed_pools: Vec<String>,
    node_pool_label_keys: Vec<String>,
    ignore_taints: bool,
    relax_preferred_affinity: bool,
    relax_required_anti_affinity: bool,
    cpu_overcommit_ratio: String,
    memory_overcommit_ratio: String,
    use_usage_adjusted_requests: bool,
    usage_request_floor_ratio: String,
    cpu_usage_safety_factor: String,
    memory_usage_safety_factor: String,
}

#[derive(Serialize)]
struct OptimizationInputCacheKey {
    schema_version: u32,
    normalization_key: String,
    ignore_unschedulable_workloads: bool,
    allow_grouping: bool,
    allow_node_grouping: bool,
}

fn hash_json<T: Serialize>(value: &T) -> Result<String> {
    let bytes = serde_json::to_vec(value)?;
    let digest = Sha256::digest(bytes);
    Ok(format!("{digest:x}"))
}

fn canonical_float(value: f64) -> String {
    format!("{value:.9}")
}

fn canonical_snapshot(snapshot: &ClusterSnapshot) -> ClusterSnapshot {
    let mut snapshot = snapshot.clone();
    snapshot.nodes.sort_by(|a, b| a.name.cmp(&b.name));
    snapshot.pods.sort_by(|a, b| {
        (&a.namespace, &a.name, &a.node_name).cmp(&(&b.namespace, &b.name, &b.node_name))
    });
    snapshot
        .volumes
        .sort_by(|a, b| (&a.namespace, &a.claim_name).cmp(&(&b.namespace, &b.claim_name)));
    snapshot.storage_classes.sort_by(|a, b| a.name.cmp(&b.name));
    snapshot
        .daemon_sets
        .sort_by(|a, b| (&a.namespace, &a.name).cmp(&(&b.namespace, &b.name)));
    snapshot
        .pdbs
        .sort_by(|a, b| (&a.namespace, &a.name).cmp(&(&b.namespace, &b.name)));
    snapshot.warnings.sort();

    for node in &mut snapshot.nodes {
        node.taints.sort_by(canonical_taint_cmp);
    }
    for pod in &mut snapshot.pods {
        canonicalize_pod(pod);
    }
    for ds in &mut snapshot.daemon_sets {
        ds.tolerations.sort_by(canonical_toleration_cmp);
    }

    snapshot
}

fn canonicalize_pod(pod: &mut Pod) {
    pod.tolerations.sort_by(canonical_toleration_cmp);
    pod.required_affinity.sort_by(canonical_affinity_cmp);
    pod.required_anti.sort_by(canonical_affinity_cmp);
    pod.pvcs.sort();
}

fn canonical_taint_cmp(left: &Taint, right: &Taint) -> std::cmp::Ordering {
    (&left.key, &left.value, &left.effect).cmp(&(&right.key, &right.value, &right.effect))
}

fn canonical_toleration_cmp(left: &Toleration, right: &Toleration) -> std::cmp::Ordering {
    (&left.key, &left.operator, &left.value, &left.effect).cmp(&(
        &right.key,
        &right.operator,
        &right.value,
        &right.effect,
    ))
}

fn canonical_affinity_cmp(left: &AffinityTerm, right: &AffinityTerm) -> std::cmp::Ordering {
    left.topology_key.cmp(&right.topology_key).then_with(|| {
        serde_json::to_string(&left.selector)
            .unwrap_or_default()
            .cmp(&serde_json::to_string(&right.selector).unwrap_or_default())
    })
}

fn sanitize_cluster_name(name: &str) -> String {
    let trimmed = name.trim();
    if trimmed.is_empty() {
        return "default".to_string();
    }

    let sanitized: String = trimmed
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || ch == '-' || ch == '_' || ch == '.' {
                ch
            } else {
                '_'
            }
        })
        .collect();

    if sanitized.is_empty() {
        "default".to_string()
    } else {
        sanitized
    }
}

#[cfg(test)]
mod tests {
    use super::StateCache;
    use crate::model::{AnalysisReport, ClusterMetadata, ClusterSnapshot, Node, Pod, SolveRequest};
    use crate::normalizer::Options as NormalizerOptions;
    use crate::pricing::PricingCatalog;
    use std::time::{SystemTime, UNIX_EPOCH};
    use tokio::fs;

    #[tokio::test]
    async fn round_trips_fresh_snapshot() {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let root = std::env::temp_dir()
            .join(format!("syslens-solver-cache-test-{unique}"))
            .join("state");
        let cache = StateCache {
            root,
            max_age: std::time::Duration::from_secs(60),
        };
        let snapshot = ClusterSnapshot {
            metadata: ClusterMetadata {
                name: "cluster-a".to_string(),
                ..Default::default()
            },
            ..Default::default()
        };

        cache.write_snapshot(&snapshot).await.unwrap();
        let loaded = cache.load_snapshot_if_fresh().await.unwrap().unwrap();

        assert_eq!(loaded.metadata.name, "cluster-a");
        let _ = fs::remove_dir_all(cache.root()).await;
    }

    #[tokio::test]
    async fn loads_snapshot_from_explicit_file() {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let root = std::env::temp_dir()
            .join(format!("syslens-solver-cache-test-{unique}"))
            .join("state");
        let cache = StateCache {
            root: root.clone(),
            max_age: std::time::Duration::from_secs(1),
        };
        fs::create_dir_all(&root).await.unwrap();
        let path = root.join("manual-snapshot.json");
        let snapshot = ClusterSnapshot {
            metadata: ClusterMetadata {
                name: "cluster-b".to_string(),
                ..Default::default()
            },
            ..Default::default()
        };
        fs::write(&path, serde_json::to_vec_pretty(&snapshot).unwrap())
            .await
            .unwrap();

        let loaded = cache.load_snapshot_from_file(&path).await.unwrap();

        assert_eq!(loaded.metadata.name, "cluster-b");
        let _ = fs::remove_dir_all(cache.root()).await;
    }

    #[tokio::test]
    async fn latest_run_scan_ignores_non_directory_tmp_entries() {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let root = std::env::temp_dir().join(format!("syslens-latest-run-test-{unique}"));
        fs::create_dir_all(&root).await.unwrap();

        let bogus = root.join("not-a-directory");
        fs::write(&bogus, b"bogus").await.unwrap();

        let state_root = root.join("cluster-a").join("state");
        fs::create_dir_all(&state_root).await.unwrap();
        let request = SolveRequest {
            cluster_name: "cluster-a".to_string(),
            ..Default::default()
        };
        let report = AnalysisReport {
            snapshot: ClusterSnapshot::default(),
            ..Default::default()
        };
        fs::write(
            state_root.join("latest-request.json"),
            serde_json::to_vec_pretty(&request).unwrap(),
        )
        .await
        .unwrap();
        fs::write(
            state_root.join("latest-report.json"),
            serde_json::to_vec_pretty(&report).unwrap(),
        )
        .await
        .unwrap();

        let latest = StateCache::load_latest_run_from_root(&root)
            .await
            .unwrap()
            .unwrap();

        assert_eq!(latest.request.unwrap().cluster_name, "cluster-a");

        let _ = fs::remove_dir_all(&root).await;
    }

    #[tokio::test]
    async fn scenario_runs_round_trip_and_sort_by_modified_time() {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let root = std::env::temp_dir()
            .join(format!("syslens-solver-scenarios-test-{unique}"))
            .join("state");
        let cache = StateCache {
            root: root.clone(),
            max_age: std::time::Duration::from_secs(60),
        };

        let request_a = SolveRequest {
            scenario_name: "strict".to_string(),
            ..Default::default()
        };
        cache
            .write_scenario_request("strict", &request_a)
            .await
            .unwrap();
        cache
            .write_scenario_report("strict", &AnalysisReport::default())
            .await
            .unwrap();

        tokio::time::sleep(std::time::Duration::from_millis(5)).await;

        let request_b = SolveRequest {
            scenario_name: "theoretical-max".to_string(),
            ..Default::default()
        };
        cache
            .write_scenario_request("theoretical-max", &request_b)
            .await
            .unwrap();
        cache
            .write_scenario_report("theoretical-max", &AnalysisReport::default())
            .await
            .unwrap();

        let runs = cache.load_scenario_runs().await.unwrap();

        assert_eq!(runs.len(), 2);
        assert_eq!(runs[0].scenario_name, "theoretical-max");
        assert_eq!(runs[1].scenario_name, "strict");

        let _ = fs::remove_dir_all(cache.root()).await;
    }

    #[test]
    fn request_uses_tmp_cluster_state_layout() {
        let req = SolveRequest {
            cluster_name: "my-cluster".to_string(),
            ..Default::default()
        };

        let cache = StateCache::for_request(&req);

        assert_eq!(
            cache.root().to_string_lossy(),
            "/tmp/my-cluster/state".to_string()
        );
    }

    #[test]
    fn snapshot_hash_is_stable_across_collection_order() {
        let cache = StateCache::for_request(&SolveRequest {
            cluster_name: "hash-test".to_string(),
            ..Default::default()
        });
        let snapshot_a = ClusterSnapshot {
            metadata: ClusterMetadata {
                name: "cluster-a".to_string(),
                ..Default::default()
            },
            nodes: vec![
                Node {
                    name: "node-b".to_string(),
                    ..Default::default()
                },
                Node {
                    name: "node-a".to_string(),
                    ..Default::default()
                },
            ],
            pods: vec![
                Pod {
                    namespace: "ns".to_string(),
                    name: "pod-b".to_string(),
                    node_name: "node-b".to_string(),
                    pvcs: vec!["b".to_string(), "a".to_string()],
                    ..Default::default()
                },
                Pod {
                    namespace: "ns".to_string(),
                    name: "pod-a".to_string(),
                    node_name: "node-a".to_string(),
                    ..Default::default()
                },
            ],
            warnings: vec!["b".to_string(), "a".to_string()],
            ..Default::default()
        };
        let snapshot_b = ClusterSnapshot {
            metadata: snapshot_a.metadata.clone(),
            nodes: snapshot_a.nodes.iter().cloned().rev().collect(),
            pods: snapshot_a.pods.iter().cloned().rev().collect(),
            warnings: snapshot_a.warnings.iter().cloned().rev().collect(),
            ..Default::default()
        };

        let hash_a = cache.snapshot_hash(&snapshot_a).unwrap();
        let hash_b = cache.snapshot_hash(&snapshot_b).unwrap();

        assert_eq!(hash_a, hash_b);
    }

    #[test]
    fn normalization_key_changes_when_model_inputs_change() {
        let cache = StateCache::for_request(&SolveRequest::default());
        let pricing = PricingCatalog::default();
        let pricing_hash = cache.pricing_hash(&pricing).unwrap();

        let base = cache
            .normalization_key(
                "snapshot",
                &pricing_hash,
                &NormalizerOptions {
                    cpu_overcommit_ratio: 1.0,
                    ..Default::default()
                },
            )
            .unwrap();
        let changed = cache
            .normalization_key(
                "snapshot",
                &pricing_hash,
                &NormalizerOptions {
                    cpu_overcommit_ratio: 1.1,
                    ..Default::default()
                },
            )
            .unwrap();

        assert_ne!(base, changed);
    }
}
