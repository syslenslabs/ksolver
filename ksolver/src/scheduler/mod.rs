//! Online GPU scheduler components. Phase 1 = shadow mode only:
//! observe and compute placement decisions; never bind pods.

pub mod config;
pub mod pod_filter;
pub mod watch_state;
