use crate::model::{ClusterSnapshot, Money, Node};
use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use csv::ReaderBuilder;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct PricingCatalog {
    #[serde(default = "default_currency")]
    pub currency: String,
    #[serde(default)]
    pub default_monthly: f64,
    #[serde(default)]
    pub node_pools: BTreeMap<String, f64>,
    #[serde(default)]
    pub instance_types: BTreeMap<String, f64>,
    #[serde(default)]
    pub nodes: BTreeMap<String, f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct AwsCache {
    region: String,
    fetched_at: DateTime<Utc>,
    catalog: PricingCatalog,
}

pub fn load_pricing_catalog(path: &str) -> Result<PricingCatalog> {
    if path.is_empty() {
        return Ok(PricingCatalog::default());
    }
    let data = fs::read_to_string(path).with_context(|| format!("read pricing file: {path}"))?;
    let mut catalog: PricingCatalog = serde_json::from_str(&data).context("decode pricing file")?;
    if catalog.currency.is_empty() {
        catalog.currency = default_currency();
    }
    Ok(catalog)
}

pub fn write_pricing_catalog(path: &Path, catalog: &PricingCatalog) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::write(path, serde_json::to_vec_pretty(catalog)?)?;
    Ok(())
}

pub fn workspace_pricing_path(cluster_name: &str) -> Result<PathBuf> {
    let cwd = std::env::current_dir()?;
    let name = sanitize_file_name(cluster_name);
    let base = if name.is_empty() { "default" } else { &name };
    Ok(cwd
        .join(".ksolver")
        .join("pricing")
        .join(format!("{base}.json")))
}

pub async fn resolve_pricing_catalog(
    path: &str,
    snapshot: &ClusterSnapshot,
) -> Result<PricingCatalog> {
    if !path.is_empty() {
        return load_pricing_catalog(path);
    }
    if let Some(catalog) = auto_aws_pricing(snapshot).await? {
        return Ok(catalog);
    }
    Ok(PricingCatalog {
        currency: default_currency(),
        ..Default::default()
    })
}

impl PricingCatalog {
    pub fn price_for_node(&self, node: &Node) -> Money {
        let currency = if self.currency.is_empty() {
            default_currency()
        } else {
            self.currency.clone()
        };
        if let Some(v) = self.nodes.get(&node.name) {
            return Money {
                currency,
                monthly: *v,
            };
        }
        let node_pool = derived_node_pool(node);
        if let Some(v) = self.node_pools.get(&node_pool) {
            return Money {
                currency,
                monthly: *v,
            };
        }
        let instance_type = node
            .labels
            .get("node.kubernetes.io/instance-type")
            .or_else(|| node.labels.get("beta.kubernetes.io/instance-type"))
            .cloned()
            .unwrap_or_default();
        if let Some(v) = self.instance_types.get(&instance_type) {
            return Money {
                currency,
                monthly: *v,
            };
        }
        Money {
            currency,
            monthly: self.default_monthly,
        }
    }
}

fn derived_node_pool(node: &Node) -> String {
    for key in [
        "eks.amazonaws.com/nodegroup",
        "cloud.google.com/gke-nodepool",
        "agentpool",
    ] {
        if let Some(value) = node.labels.get(key) {
            if !value.trim().is_empty() {
                return value.clone();
            }
        }
    }
    node.pool.clone()
}

async fn auto_aws_pricing(snapshot: &ClusterSnapshot) -> Result<Option<PricingCatalog>> {
    let mut region = String::new();
    let mut instance_types = BTreeSet::new();
    let mut aws_detected = false;

    for node in &snapshot.nodes {
        if node
            .labels
            .get("k8s.io/cloud-provider-aws")
            .map(|v| !v.is_empty())
            .unwrap_or(false)
        {
            aws_detected = true;
        }
        if node.name.starts_with("ip-") && node.name.contains(".compute.internal") {
            aws_detected = true;
        }
        if region.is_empty() {
            region = node
                .labels
                .get("topology.kubernetes.io/region")
                .or_else(|| node.labels.get("failure-domain.beta.kubernetes.io/region"))
                .cloned()
                .unwrap_or_default();
        }
        if let Some(instance_type) = node
            .labels
            .get("node.kubernetes.io/instance-type")
            .or_else(|| node.labels.get("beta.kubernetes.io/instance-type"))
        {
            if !instance_type.is_empty() {
                instance_types.insert(instance_type.clone());
            }
        }
    }

    if !aws_detected || region.is_empty() || instance_types.is_empty() {
        return Ok(None);
    }

    Ok(Some(
        load_aws_regional_pricing(&region, &instance_types).await?,
    ))
}

async fn load_aws_regional_pricing(
    region: &str,
    instance_types: &BTreeSet<String>,
) -> Result<PricingCatalog> {
    let cache_path = aws_cache_path(region)?;
    if let Some(cached) = read_aws_cache(&cache_path, region, instance_types)? {
        return Ok(cached.catalog);
    }

    let url = format!(
        "https://pricing.us-east-1.amazonaws.com/offers/v1.0/aws/AmazonEC2/current/{region}/index.csv"
    );
    let resp = reqwest::get(&url)
        .await
        .with_context(|| format!("fetch aws pricing: {url}"))?;
    let status = resp.status();
    if !status.is_success() {
        anyhow::bail!("fetch aws pricing: status {status}");
    }
    let text = resp.text().await.context("read aws pricing response")?;
    let catalog = parse_aws_pricing_csv(&text, instance_types)?;

    if catalog.instance_types.is_empty() {
        anyhow::bail!("no aws linux on-demand pricing found for region {region}");
    }

    let cache = AwsCache {
        region: region.to_string(),
        fetched_at: Utc::now(),
        catalog: catalog.clone(),
    };
    let _ = fs::write(&cache_path, serde_json::to_vec_pretty(&cache)?);
    Ok(catalog)
}

// @lineage
// reads: aws.pricing.csv
fn parse_aws_pricing_csv(text: &str, instance_types: &BTreeSet<String>) -> Result<PricingCatalog> {
    let mut reader = ReaderBuilder::new()
        .has_headers(false)
        .flexible(true)
        .from_reader(text.as_bytes());

    let mut catalog = PricingCatalog {
        currency: default_currency(),
        instance_types: BTreeMap::new(),
        ..Default::default()
    };
    let mut headers = BTreeMap::new();

    for (idx, rec) in reader.records().enumerate() {
        let row = rec.context("read aws pricing csv")?;
        if idx == 5 {
            for (i, field) in row.iter().enumerate() {
                headers.insert(field.to_string(), i);
            }
            continue;
        }
        if idx < 6 || headers.is_empty() || !aws_row_matches(&row, &headers) {
            continue;
        }
        let instance_type = field(&row, &headers, "Instance Type");
        if !instance_types.contains(instance_type)
            || catalog.instance_types.contains_key(instance_type)
        {
            continue;
        }
        if let Ok(hourly) = field(&row, &headers, "PricePerUnit").parse::<f64>() {
            catalog
                .instance_types
                .insert(instance_type.to_string(), hourly * 730.0);
        }
        if catalog.instance_types.len() == instance_types.len() {
            break;
        }
    }

    Ok(catalog)
}

fn aws_row_matches(row: &csv::StringRecord, headers: &BTreeMap<String, usize>) -> bool {
    field(row, headers, "TermType") == "OnDemand"
        && field(row, headers, "Unit") == "Hrs"
        && field(row, headers, "Currency") == "USD"
        && field(row, headers, "Operating System") == "Linux"
        && field(row, headers, "Pre Installed S/W") == "NA"
        && field(row, headers, "Tenancy") == "Shared"
        && field(row, headers, "CapacityStatus") == "Used"
}

fn field<'a>(row: &'a csv::StringRecord, headers: &BTreeMap<String, usize>, name: &str) -> &'a str {
    headers
        .get(name)
        .and_then(|idx| row.get(*idx))
        .unwrap_or("")
}

fn aws_cache_path(region: &str) -> Result<PathBuf> {
    let dir = dirs::cache_dir().unwrap_or_else(|| PathBuf::from("."));
    let path = dir.join("ksolver");
    fs::create_dir_all(&path)?;
    Ok(path.join(format!("aws-pricing-{region}.json")))
}

fn read_aws_cache(
    path: &Path,
    region: &str,
    required: &BTreeSet<String>,
) -> Result<Option<AwsCache>> {
    let data = match fs::read_to_string(path) {
        Ok(v) => v,
        Err(_) => return Ok(None),
    };
    let cache: AwsCache = match serde_json::from_str(&data) {
        Ok(v) => v,
        Err(_) => return Ok(None),
    };
    if cache.region != region
        || Utc::now()
            .signed_duration_since(cache.fetched_at)
            .num_hours()
            > 24
    {
        return Ok(None);
    }
    if required
        .iter()
        .any(|it| !cache.catalog.instance_types.contains_key(it))
    {
        return Ok(None);
    }
    Ok(Some(cache))
}

pub fn suggest_candidate_instance_types(
    catalog: &PricingCatalog,
    snapshot: &ClusterSnapshot,
) -> Vec<crate::model::CandidateInstanceType> {
    let mut candidates = Vec::new();
    let current_types: BTreeSet<String> = snapshot
        .nodes
        .iter()
        .filter_map(|n| {
            n.labels
                .get("node.kubernetes.io/instance-type")
                .or_else(|| n.labels.get("beta.kubernetes.io/instance-type"))
                .cloned()
        })
        .collect();

    for (instance_type, monthly_price) in &catalog.instance_types {
        if current_types.contains(instance_type) {
            continue;
        }
        if let Some((cpu, mem)) = estimate_instance_resources(instance_type) {
            candidates.push(crate::model::CandidateInstanceType {
                name: instance_type.clone(),
                cpu_milli: cpu,
                memory_bytes: mem,
                monthly_price: *monthly_price,
                max_count: 3,
            });
        }
    }

    candidates.sort_by(|a, b| {
        a.monthly_price
            .partial_cmp(&b.monthly_price)
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    candidates.truncate(8);
    candidates
}

fn estimate_instance_resources(instance_type: &str) -> Option<(i64, i64)> {
    let size = instance_type.rsplit('.').next()?;
    let (cpu, mem_gib): (i64, i64) = match size {
        "medium" => (1000, 4),
        "large" => (2000, 8),
        "xlarge" => (4000, 16),
        "2xlarge" => (8000, 32),
        "4xlarge" => (16000, 64),
        "8xlarge" => (32000, 128),
        "12xlarge" => (48000, 192),
        "16xlarge" => (64000, 256),
        "24xlarge" => (96000, 384),
        "metal" => (96000, 384),
        _ => return None,
    };
    let family = instance_type.split('.').next()?;
    let mem_ratio = if family.starts_with('r') || family.starts_with('x') {
        2.0
    } else if family.starts_with('c') {
        0.5
    } else {
        1.0
    };
    Some((
        cpu,
        (mem_gib as f64 * mem_ratio * 1024.0 * 1024.0 * 1024.0) as i64,
    ))
}

fn sanitize_file_name(value: &str) -> String {
    value
        .trim()
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '-' || c == '_' {
                c
            } else {
                '-'
            }
        })
        .collect::<String>()
        .trim_matches('-')
        .to_string()
}

fn default_currency() -> String {
    "USD".to_string()
}

#[cfg(test)]
mod tests {
    use super::parse_aws_pricing_csv;
    use std::collections::BTreeSet;

    #[test]
    fn parses_aws_csv_with_mixed_width_preamble_rows() {
        let csv = concat!(
            "\"FormatVersion\",\"v1.0\"\n",
            "\"Disclaimer\",\"Example\"\n",
            "\"Publication Date\",\"2026-03-20T04:29:25Z\"\n",
            "\"Version\",\"20260320042925\"\n",
            "\"OfferCode\",\"AmazonEC2\"\n",
            "\"TermType\",\"Unit\",\"PricePerUnit\",\"Currency\",\"Instance Type\",\"Operating System\",\"Pre Installed S/W\",\"Tenancy\",\"CapacityStatus\"\n",
            "\"OnDemand\",\"Hrs\",\"0.4200000000\",\"USD\",\"x2gd.2xlarge\",\"Linux\",\"NA\",\"Shared\",\"Used\"\n"
        );
        let instance_types = BTreeSet::from([String::from("x2gd.2xlarge")]);

        let catalog = parse_aws_pricing_csv(csv, &instance_types).unwrap();

        assert_eq!(catalog.currency, "USD");
        assert_eq!(
            catalog.instance_types.get("x2gd.2xlarge"),
            Some(&(0.42 * 730.0))
        );
    }

    #[test]
    fn ignores_non_matching_rows_and_collects_requested_instance_types() {
        let csv = concat!(
            "\"FormatVersion\",\"v1.0\"\n",
            "\"Disclaimer\",\"Example\"\n",
            "\"Publication Date\",\"2026-03-20T04:29:25Z\"\n",
            "\"Version\",\"20260320042925\"\n",
            "\"OfferCode\",\"AmazonEC2\"\n",
            "\"TermType\",\"Unit\",\"PricePerUnit\",\"Currency\",\"Instance Type\",\"Operating System\",\"Pre Installed S/W\",\"Tenancy\",\"CapacityStatus\"\n",
            "\"Reserved\",\"Hrs\",\"0.1100000000\",\"USD\",\"x2gd.2xlarge\",\"Linux\",\"NA\",\"Shared\",\"Used\"\n",
            "\"OnDemand\",\"Hrs\",\"0.3300000000\",\"USD\",\"m7g.large\",\"Linux\",\"NA\",\"Shared\",\"Used\"\n"
        );
        let instance_types = BTreeSet::from([String::from("m7g.large")]);

        let catalog = parse_aws_pricing_csv(csv, &instance_types).unwrap();

        assert_eq!(catalog.instance_types.len(), 1);
        assert_eq!(
            catalog.instance_types.get("m7g.large"),
            Some(&(0.33 * 730.0))
        );
    }

    #[test]
    fn suggest_candidates_excludes_current_types() {
        use super::{suggest_candidate_instance_types, PricingCatalog};
        use crate::model::ClusterSnapshot;
        use std::collections::BTreeMap;

        let mut instance_types = BTreeMap::new();
        instance_types.insert("m5.xlarge".to_string(), 140.0);
        instance_types.insert("m5.2xlarge".to_string(), 270.0);
        instance_types.insert("r5.2xlarge".to_string(), 450.0);

        let catalog = PricingCatalog {
            currency: "USD".to_string(),
            instance_types,
            ..Default::default()
        };

        let mut node_labels = BTreeMap::new();
        node_labels.insert(
            "node.kubernetes.io/instance-type".to_string(),
            "m5.xlarge".to_string(),
        );

        let snapshot = ClusterSnapshot {
            nodes: vec![crate::model::Node {
                name: "node-1".to_string(),
                labels: node_labels,
                ..Default::default()
            }],
            ..Default::default()
        };

        let candidates = suggest_candidate_instance_types(&catalog, &snapshot);
        assert!(!candidates.is_empty());
        assert!(
            candidates.iter().all(|c| c.name != "m5.xlarge"),
            "should not suggest current instance types"
        );
        assert!(candidates.iter().any(|c| c.name == "m5.2xlarge"));
    }

    #[test]
    fn estimate_instance_resources_handles_common_sizes() {
        use super::estimate_instance_resources;

        let (cpu, mem) = estimate_instance_resources("m5.xlarge").unwrap();
        assert_eq!(cpu, 4000);
        assert_eq!(mem, 16 * 1024 * 1024 * 1024);

        let (cpu, mem) = estimate_instance_resources("r5.2xlarge").unwrap();
        assert_eq!(cpu, 8000);
        assert_eq!(mem, 64 * 1024 * 1024 * 1024);

        let (cpu, mem) = estimate_instance_resources("c5.4xlarge").unwrap();
        assert_eq!(cpu, 16000);
        assert_eq!(mem, 32 * 1024 * 1024 * 1024);
    }
}
