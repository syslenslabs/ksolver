//! Online GPU scheduler components. Phase 1 = shadow mode only:
//! observe and compute placement decisions; never bind pods.

pub mod config;
pub mod decision;
pub mod pending_input;
pub mod pod_filter;
pub mod shadow;
pub mod trace;
pub mod watch_state;

#[cfg(test)]
mod no_mutation_guard {
    // This source must never call cluster-mutating APIs in Phase 1 (shadow mode).
    const SHADOW: &str = include_str!("shadow.rs");

    #[test]
    fn shadow_has_no_binding_or_mutation_calls() {
        for needle in [
            "Binding",
            ".evict(",
            ".create(",
            ".replace(",
            ".patch(",
            ".delete(",
        ] {
            assert!(
                !SHADOW.contains(needle),
                "shadow.rs must not contain `{needle}` in Phase 1"
            );
        }
    }
}
