use crate::model::SolveRequest;
use lazy_static::lazy_static;
use prometheus::{
    Encoder, HistogramOpts, HistogramVec, IntCounterVec, IntGaugeVec, Opts, Registry, TextEncoder,
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
}

pub fn register_metrics() {
    REGISTRY
        .register(Box::new(SOLVE_DURATION_SECONDS.clone()))
        .expect("solver metric can be registered");
    REGISTRY
        .register(Box::new(SOLVES_TOTAL.clone()))
        .expect("solver metric can be registered");
    REGISTRY
        .register(Box::new(SOLVES_IN_FLIGHT.clone()))
        .expect("solver metric can be registered");
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
