use crate::collector::build_client;
use crate::model::{
    ClusterSnapshot, MemoryHistory, ResourceUsage, ScenarioConfig, TimeSeriesPoint,
};
use anyhow::{Context, Result};
use chrono::{Duration, Utc};
use k8s_openapi::api::core::v1::Secret;
use kube::Api;
use reqwest::Client;
use serde::Deserialize;
use std::collections::BTreeMap;

#[derive(Debug, Clone, Default)]
pub struct ResolvedHistoricalUsageConfig {
    pub provider: String,
    pub lookback: String,
    pub step: String,
    pub prometheus_url: String,
    pub prometheus_username: String,
    pub prometheus_token: String,
    pub source: String,
}

// @lineage
// reads: scenario.historical_usage.*, kube secret data
pub async fn resolve_historical_usage_config(
    kubeconfig: &str,
    scenario: &ScenarioConfig,
) -> Result<Option<ResolvedHistoricalUsageConfig>> {
    let config = &scenario.historical_usage;
    if !config.enabled {
        return Ok(None);
    }

    let mut resolved = ResolvedHistoricalUsageConfig {
        provider: config.provider.clone(),
        lookback: config.lookback.clone(),
        step: config.step.clone(),
        prometheus_url: config.prometheus_url.clone(),
        prometheus_username: config.prometheus_username.clone(),
        prometheus_token: config.prometheus_token.clone(),
        source: "inline".to_string(),
    };

    if resolved.prometheus_url.is_empty()
        || resolved.prometheus_username.is_empty()
        || resolved.prometheus_token.is_empty()
    {
        let namespace = config.secret_namespace.trim();
        let name = config.secret_name.trim();
        if !namespace.is_empty() && !name.is_empty() {
            let client = build_client(kubeconfig).await?;
            let secrets: Api<Secret> = Api::namespaced(client, namespace);
            let secret = secrets
                .get(name)
                .await
                .with_context(|| format!("load secret {namespace}/{name} for historical usage"))?;
            let data = secret.data.unwrap_or_default();

            if resolved.prometheus_url.is_empty() {
                resolved.prometheus_url =
                    decode_secret_string(&data, &config.secret_prometheus_url_key)?;
            }
            if resolved.prometheus_username.is_empty() {
                resolved.prometheus_username =
                    decode_secret_string(&data, &config.secret_prometheus_username_key)?;
            }
            if resolved.prometheus_token.is_empty() {
                resolved.prometheus_token =
                    decode_secret_string(&data, &config.secret_prometheus_token_key)?;
            }
            resolved.source = format!("secret:{namespace}/{name}");
        }
    }

    Ok(Some(resolved))
}

// @lineage
// reads: resolved prometheus config, snapshot.metadata.name, snapshot.pods.*
pub async fn overlay_historical_pod_usage(
    snapshot: &mut ClusterSnapshot,
    resolved: &ResolvedHistoricalUsageConfig,
) -> Result<usize> {
    let http = Client::builder()
        .build()
        .context("build prometheus client")?;
    let memory_range_query =
        "sum by (namespace,pod) (container_memory_working_set_bytes{namespace!=\"\",pod!=\"\",container!=\"POD\",image!=\"\"})"
            .to_string();
    let memory_query = format!(
        "sum by (namespace,pod) (max_over_time(container_memory_working_set_bytes{{namespace!=\"\",pod!=\"\",container!=\"POD\",image!=\"\"}}[{}]))",
        resolved.lookback
    );
    let cpu_query = format!(
        "sum by (namespace,pod) (max_over_time(rate(container_cpu_usage_seconds_total{{namespace!=\"\",pod!=\"\",container!=\"POD\",image!=\"\"}}[{}])[{}:{}]))",
        resolved.step, resolved.lookback, resolved.step
    );

    let memory = query_prometheus_vector(&http, resolved, &memory_query)
        .await
        .context("query historical pod memory")?;
    let memory_history = query_prometheus_matrix(&http, resolved, &memory_range_query)
        .await
        .context("query historical pod memory range")?;
    let cpu = query_prometheus_vector(&http, resolved, &cpu_query)
        .await
        .context("query historical pod cpu")?;

    let mut usage_by_pod: BTreeMap<String, ResourceUsage> = BTreeMap::new();
    for (key, value) in memory {
        usage_by_pod.entry(key).or_default().memory_bytes = value.round() as i64;
    }
    for (key, value) in cpu {
        usage_by_pod.entry(key).or_default().cpu_usage_milli = (value * 1000.0).round() as i64;
    }

    let mut updated = 0usize;
    for pod in &mut snapshot.pods {
        let key = format!("{}/{}", pod.namespace, pod.name);
        if let Some(usage) = usage_by_pod.get(&key) {
            pod.usage.cpu_usage_milli = usage.cpu_usage_milli;
            pod.usage.memory_bytes = usage.memory_bytes;
            updated += 1;
        }
        if let Some(history) = memory_history.get(&key) {
            pod.memory_history = history.clone();
        }
    }

    Ok(updated)
}

fn decode_secret_string(
    data: &std::collections::BTreeMap<String, k8s_openapi::ByteString>,
    key: &str,
) -> Result<String> {
    let Some(value) = data.get(key) else {
        return Ok(String::new());
    };
    String::from_utf8(value.0.clone())
        .with_context(|| format!("decode secret field {key} as utf-8"))
}

#[derive(Debug, Deserialize)]
struct PrometheusQueryEnvelope {
    data: PrometheusQueryData,
}

#[derive(Debug, Deserialize)]
struct PrometheusQueryData {
    result: Vec<PrometheusVectorSample>,
}

#[derive(Debug, Deserialize)]
struct PrometheusVectorSample {
    metric: BTreeMap<String, String>,
    value: (f64, String),
}

#[derive(Debug, Deserialize)]
struct PrometheusRangeEnvelope {
    data: PrometheusRangeData,
}

#[derive(Debug, Deserialize)]
struct PrometheusRangeData {
    result: Vec<PrometheusMatrixSeries>,
}

#[derive(Debug, Deserialize)]
struct PrometheusMatrixSeries {
    metric: BTreeMap<String, String>,
    values: Vec<(f64, String)>,
}

async fn query_prometheus_vector(
    http: &Client,
    resolved: &ResolvedHistoricalUsageConfig,
    query: &str,
) -> Result<BTreeMap<String, f64>> {
    let response = http
        .get(format!(
            "{}/api/prom/api/v1/query",
            resolved.prometheus_url.trim_end_matches('/')
        ))
        .basic_auth(
            resolved.prometheus_username.clone(),
            Some(resolved.prometheus_token.clone()),
        )
        .query(&[("query", query)])
        .send()
        .await
        .context("send prometheus query")?
        .error_for_status()
        .context("prometheus query status")?;

    let payload: PrometheusQueryEnvelope = response
        .json()
        .await
        .context("decode prometheus query response")?;

    let mut out = BTreeMap::new();
    for sample in payload.data.result {
        let namespace = sample.metric.get("namespace").cloned().unwrap_or_default();
        let pod = sample.metric.get("pod").cloned().unwrap_or_default();
        if namespace.is_empty() || pod.is_empty() {
            continue;
        }
        if let Ok(value) = sample.value.1.parse::<f64>() {
            out.insert(format!("{namespace}/{pod}"), value);
        }
    }
    Ok(out)
}

async fn query_prometheus_matrix(
    http: &Client,
    resolved: &ResolvedHistoricalUsageConfig,
    query: &str,
) -> Result<BTreeMap<String, MemoryHistory>> {
    let end = Utc::now();
    let start = end - parse_prometheus_duration(&resolved.lookback)?;
    let start_s = start.timestamp().to_string();
    let end_s = end.timestamp().to_string();
    let step_s = resolved.step.clone();
    let response = http
        .get(format!(
            "{}/api/prom/api/v1/query_range",
            resolved.prometheus_url.trim_end_matches('/')
        ))
        .basic_auth(
            resolved.prometheus_username.clone(),
            Some(resolved.prometheus_token.clone()),
        )
        .query(&[
            ("query", query),
            ("start", &start_s),
            ("end", &end_s),
            ("step", &step_s),
        ])
        .send()
        .await
        .context("send prometheus range query")?
        .error_for_status()
        .context("prometheus range query status")?;

    let payload: PrometheusRangeEnvelope = response
        .json()
        .await
        .context("decode prometheus range response")?;

    let mut out = BTreeMap::new();
    for series in payload.data.result {
        let namespace = series.metric.get("namespace").cloned().unwrap_or_default();
        let pod = series.metric.get("pod").cloned().unwrap_or_default();
        if namespace.is_empty() || pod.is_empty() {
            continue;
        }
        let mut samples = Vec::new();
        let mut sum = 0.0_f64;
        let mut max = 0.0_f64;
        for (ts, raw_value) in series.values {
            let Ok(value) = raw_value.parse::<f64>() else {
                continue;
            };
            sum += value;
            max = max.max(value);
            samples.push(TimeSeriesPoint {
                timestamp_unix: ts as i64,
                value: value.round() as i64,
            });
        }
        if samples.is_empty() {
            continue;
        }
        let mean = sum / samples.len() as f64;
        let variance = samples
            .iter()
            .map(|sample| {
                let delta = sample.value as f64 - mean;
                delta * delta
            })
            .sum::<f64>()
            / samples.len() as f64;
        out.insert(
            format!("{namespace}/{pod}"),
            MemoryHistory {
                sample_count: samples.len() as i32,
                mean_bytes: mean.round() as i64,
                max_bytes: max.round() as i64,
                stddev_bytes: variance.sqrt().round() as i64,
                samples,
            },
        );
    }
    Ok(out)
}

fn parse_prometheus_duration(input: &str) -> Result<Duration> {
    let trimmed = input.trim();
    if trimmed.is_empty() {
        return Ok(Duration::hours(24));
    }

    let unit = trimmed
        .chars()
        .last()
        .context("duration missing unit suffix")?;
    let value = trimmed[..trimmed.len().saturating_sub(1)]
        .parse::<i64>()
        .with_context(|| format!("parse duration value from {trimmed}"))?;

    let duration = match unit {
        's' => Duration::seconds(value),
        'm' => Duration::minutes(value),
        'h' => Duration::hours(value),
        'd' => Duration::days(value),
        _ => anyhow::bail!("unsupported duration unit in {trimmed}"),
    };
    Ok(duration)
}
