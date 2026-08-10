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
            let client = build_client(kubeconfig, None).await?;
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

/// Standard kube-integrated dcgm-exporter metric: GPU framebuffer memory *used*, reported in MiB.
pub const DCGM_FB_USED_METRIC: &str = "DCGM_FI_DEV_FB_USED";

/// PromQL for each pod's peak GPU-framebuffer usage (MiB) over `window`. dcgm-exporter exposes
/// `DCGM_FI_DEV_FB_USED` per GPU; the kube integration relabels it with `namespace`/`pod` (and
/// sometimes `exported_pod`). We take `max_over_time` over the window and `max` across a pod's GPUs,
/// so a multi-GPU pod resolves to its single-device peak — matching the per-device VRAM model.
pub fn pod_peak_vram_query(window: &str) -> String {
    format!(
        "max by (namespace, pod, exported_pod) (max_over_time({DCGM_FB_USED_METRIC}{{pod!=\"\"}}[{window}]))"
    )
}

/// Pure parse of a Prometheus vector response into `"namespace/pod" -> peak MiB`, taking the max
/// across a pod's GPU series. Prefers the `pod` label, falling back to `exported_pod` (some
/// dcgm-exporter relabelings only carry the latter). Factored out so the DCGM label handling is
/// unit-testable against a mock response without a live exporter.
fn parse_pod_vram_peaks(envelope: PrometheusQueryEnvelope) -> BTreeMap<String, f64> {
    let mut out: BTreeMap<String, f64> = BTreeMap::new();
    for sample in envelope.data.result {
        let namespace = sample.metric.get("namespace").cloned().unwrap_or_default();
        let pod = sample
            .metric
            .get("pod")
            .filter(|value| !value.is_empty())
            .or_else(|| sample.metric.get("exported_pod"))
            .cloned()
            .unwrap_or_default();
        if namespace.is_empty() || pod.is_empty() {
            continue;
        }
        let Ok(value) = sample.value.1.parse::<f64>() else {
            continue;
        };
        if !(value.is_finite()) || value <= 0.0 {
            continue;
        }
        let key = format!("{namespace}/{pod}");
        let entry = out.entry(key).or_insert(0.0);
        if value > *entry {
            *entry = value;
        }
    }
    out
}

/// Query the configured Prometheus for each pod's peak GPU-VRAM usage (MiB) over `window`, via the
/// standard dcgm-exporter metric. Returns `"namespace/pod" -> peak MiB`. Gated entirely on a real
/// Prometheus/dcgm-exporter being configured — it returns an empty map only when the exporter has
/// no matching series, and never fabricates observations.
pub async fn query_pod_peak_vram_mib(
    resolved: &ResolvedHistoricalUsageConfig,
    window: &str,
) -> Result<BTreeMap<String, f64>> {
    let http = Client::builder()
        .build()
        .context("build prometheus client for dcgm vram query")?;
    let query = pod_peak_vram_query(window);
    let response = http
        .get(format!(
            "{}/api/prom/api/v1/query",
            resolved.prometheus_url.trim_end_matches('/')
        ))
        .basic_auth(
            resolved.prometheus_username.clone(),
            Some(resolved.prometheus_token.clone()),
        )
        .query(&[("query", query.as_str())])
        .send()
        .await
        .context("send dcgm vram prometheus query")?
        .error_for_status()
        .context("dcgm vram prometheus query status")?;
    let payload: PrometheusQueryEnvelope = response
        .json()
        .await
        .context("decode dcgm vram prometheus response")?;
    Ok(parse_pod_vram_peaks(payload))
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_dcgm_pod_peaks_taking_max_across_gpus_with_exported_pod_fallback() {
        // Mock dcgm-exporter vector response: two GPU series for one pod (take the max), one pod
        // labeled only via `exported_pod`, a series missing pod labels (dropped), and a
        // non-positive reading (dropped). Validates the DCGM label handling without a live exporter.
        let json = r#"{
          "status":"success",
          "data":{"resultType":"vector","result":[
            {"metric":{"namespace":"team","pod":"job-a","gpu":"0"},"value":[1710000000,"12000"]},
            {"metric":{"namespace":"team","pod":"job-a","gpu":"1"},"value":[1710000000,"15000"]},
            {"metric":{"namespace":"team","exported_pod":"job-b"},"value":[1710000000,"8000"]},
            {"metric":{"namespace":"team"},"value":[1710000000,"9999"]},
            {"metric":{"namespace":"team","pod":"job-c"},"value":[1710000000,"0"]}
          ]}
        }"#;
        let envelope: PrometheusQueryEnvelope = serde_json::from_str(json).unwrap();
        let peaks = parse_pod_vram_peaks(envelope);
        assert_eq!(peaks.get("team/job-a"), Some(&15000.0)); // max across the pod's two GPUs
        assert_eq!(peaks.get("team/job-b"), Some(&8000.0)); // exported_pod fallback
        assert!(!peaks.contains_key("team/")); // no pod label -> dropped
        assert!(!peaks.contains_key("team/job-c")); // non-positive -> dropped
        assert_eq!(peaks.len(), 2);
    }

    #[test]
    fn pod_peak_vram_query_uses_dcgm_metric_and_window() {
        let query = pod_peak_vram_query("30m");
        assert!(query.contains(DCGM_FB_USED_METRIC));
        assert!(query.contains("[30m]"));
        assert!(query.contains("max_over_time"));
    }
}
