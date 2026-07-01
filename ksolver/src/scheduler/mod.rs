//! Online GPU scheduler components. Phase 1 = shadow mode only:
//! observe and compute placement decisions; never bind pods.

pub mod config;
pub mod decision;
pub mod pod_filter;
pub mod shadow;
pub mod trace;
pub mod watch_state;
