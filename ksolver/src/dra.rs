//! DRA (Dynamic Resource Allocation) F3a — scalar approximation.
//!
//! DRA is a matching/assignment problem (claims ↔ devices via CEL selectors), not a scalar
//! resource. F3a approximates it as synthetic integer extended resources so DRA-requesting pods can
//! ride the existing generic solver path: per `(node, DeviceClass)` we count the *unallocated*
//! devices that match the class, and per pod we sum claim demand per class. This is SHADOW-ONLY and
//! deliberately NARROW — see the contract below. When we cannot evaluate something precisely we fail
//! SAFE (don't count capacity, caveat the pod) rather than overcounting, EXCEPT the inherent
//! optimism of a scalar collapse (overlapping classes / request selectors), which is disclosed via
//! caveats — never silently trusted.
//!
//! Contract (v1alpha3):
//! - **Selector evaluation:** a `DeviceClass` matches a device iff every class selector matches. A
//!   selector's CEL `expression` is supported ONLY when it reduces to a single equality
//!   `device.driver == "<lit>"` or `device.attributes["<domain>"].<name> == <lit>`. Empty selectors
//!   ⇒ matches all. Any other expression ⇒ the class is **unevaluable** (its devices are not counted;
//!   pods requesting it are caveated).
//! - **Allocation source of truth = `ResourceClaim.status.allocation`** (NOT slices). Device identity
//!   is `(driver, pool, device)`. Availability = matching slice devices − allocated identities.
//! - **Node scoping:** only `spec.node_name`-scoped slices are attributed to a node (MVP). Only the
//!   highest `pool.generation` per `(driver, pool)` is trusted (stale slices ignored).
//! - **Demand:** `allocationMode` ExactCount/absent ⇒ `count` (default 1); `All` or unknown ⇒
//!   caveat + not counted. Request-level selectors / device constraints ⇒ caveat (placement optimistic).

use k8s_openapi::api::resource::v1alpha3 as dra;
use std::collections::{BTreeMap, BTreeSet};

/// Per-node, per-class available device counts plus disclosure flags.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DraAvailability {
    /// `(node_name, device_class_name) -> unallocated matching device count`.
    pub by_node_class: BTreeMap<(String, String), i64>,
    /// Classes with at least one selector we could not evaluate (devices NOT counted for them).
    pub unevaluable_classes: BTreeSet<String>,
    /// True if some device on some node matched more than one counted class (scalar collapse may
    /// overestimate — the same physical device is counted toward multiple synthetic resources).
    pub overlapping_classes: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SelMatch {
    Yes,
    No,
    Unevaluable,
}

/// Synthetic extended-resource key for a DeviceClass (fed to the generic solver path).
pub fn class_resource_key(class: &str) -> String {
    format!("dra.ksolver/{class}")
}

/// Parse an RHS CEL literal.
enum Literal {
    Str(String),
    Int(i64),
    Bool(bool),
}

fn parse_literal(rhs: &str) -> Option<Literal> {
    let r = rhs.trim();
    if (r.starts_with('"') && r.ends_with('"') && r.len() >= 2)
        || (r.starts_with('\'') && r.ends_with('\'') && r.len() >= 2)
    {
        return Some(Literal::Str(r[1..r.len() - 1].to_string()));
    }
    if r == "true" {
        return Some(Literal::Bool(true));
    }
    if r == "false" {
        return Some(Literal::Bool(false));
    }
    if let Ok(n) = r.parse::<i64>() {
        return Some(Literal::Int(n));
    }
    None
}

/// Compare a device attribute's typed value to a literal.
fn attr_equals(attr: &dra::DeviceAttribute, lit: &Literal) -> bool {
    match lit {
        Literal::Str(s) => {
            attr.string.as_deref() == Some(s.as_str())
                || attr.version.as_deref() == Some(s.as_str())
        }
        Literal::Int(n) => attr.int == Some(*n),
        Literal::Bool(b) => attr.bool == Some(*b),
    }
}

/// Look up a `device.attributes["<domain>"].<name>` reference in the API attribute map, which keys
/// entries as `"<domain>/<name>"` (fully qualified) or bare `"<name>"`.
fn lookup_attr<'a>(
    attrs: &'a BTreeMap<String, dra::DeviceAttribute>,
    domain: &str,
    name: &str,
) -> Option<&'a dra::DeviceAttribute> {
    attrs
        .get(&format!("{domain}/{name}"))
        .or_else(|| attrs.get(name))
}

/// Evaluate one DeviceClass selector CEL `expression` against a device (its owning slice `driver`
/// and its `attributes`). Supports only single-equality forms; anything else ⇒ `Unevaluable`.
fn eval_selector(
    expr: &str,
    slice_driver: &str,
    attrs: &BTreeMap<String, dra::DeviceAttribute>,
) -> SelMatch {
    // Strip an optional single wrapping paren pair.
    let mut e = expr.trim();
    if e.starts_with('(') && e.ends_with(')') && e.len() >= 2 {
        e = e[1..e.len() - 1].trim();
    }
    // Reject compound/unsupported expressions early (only a single `==` equality is supported).
    if e.contains("&&")
        || e.contains("||")
        || e.contains("!=")
        || e.contains(">=")
        || e.contains("<=")
        || e.contains('>')
        || e.contains('<')
        || e.contains('!')
        || e.contains(" in ")
        || e.contains(".contains(")
        || e.contains(".matches(")
    {
        return SelMatch::Unevaluable;
    }
    let Some((lhs_raw, rhs_raw)) = e.split_once("==") else {
        return SelMatch::Unevaluable;
    };
    let lhs = lhs_raw.trim();
    let Some(lit) = parse_literal(rhs_raw) else {
        return SelMatch::Unevaluable;
    };

    // Form 1: device.driver == "<lit>"
    if lhs == "device.driver" {
        return match &lit {
            Literal::Str(s) if s == slice_driver => SelMatch::Yes,
            Literal::Str(_) => SelMatch::No,
            _ => SelMatch::Unevaluable,
        };
    }

    // Form 2: device.attributes["<domain>"].<name> == <lit>
    if let Some(rest) = lhs.strip_prefix("device.attributes[") {
        // rest = `"<domain>"].<name>`
        let bytes = rest.as_bytes();
        let quote = bytes.first().copied();
        if quote != Some(b'"') && quote != Some(b'\'') {
            return SelMatch::Unevaluable;
        }
        let q = quote.unwrap() as char;
        let rest2 = &rest[1..];
        let Some(end_q) = rest2.find(q) else {
            return SelMatch::Unevaluable;
        };
        let domain = &rest2[..end_q];
        let after = rest2[end_q + 1..].trim_start();
        // after must be `].<name>`
        let Some(after2) = after.strip_prefix("].") else {
            return SelMatch::Unevaluable;
        };
        let name = after2.trim();
        if name.is_empty()
            || !name
                .chars()
                .all(|c| c.is_alphanumeric() || c == '_' || c == '-')
        {
            return SelMatch::Unevaluable;
        }
        return match lookup_attr(attrs, domain, name) {
            Some(attr) if attr_equals(attr, &lit) => SelMatch::Yes,
            Some(_) => SelMatch::No,
            None => SelMatch::No, // attribute absent ⇒ CEL predicate is false ⇒ device doesn't match
        };
    }

    SelMatch::Unevaluable
}

/// Whether a class matches a device (selectors are ANDed). CEL AND semantics: a definite `No`
/// makes the class NOT match (returned even if other selectors are Unevaluable — `false && unknown`
/// is `false`, so the device is safely excluded without a spurious unevaluable caveat). Otherwise
/// any Unevaluable ⇒ Unevaluable; empty/all-Yes ⇒ Yes.
fn class_matches(
    class: &dra::DeviceClass,
    slice_driver: &str,
    attrs: &BTreeMap<String, dra::DeviceAttribute>,
) -> SelMatch {
    let spec = &class.spec;
    let Some(selectors) = spec.selectors.as_ref() else {
        return SelMatch::Yes;
    };
    if selectors.is_empty() {
        return SelMatch::Yes;
    }
    let mut any_unevaluable = false;
    for sel in selectors {
        let Some(cel) = sel.cel.as_ref() else {
            any_unevaluable = true;
            continue;
        };
        match eval_selector(&cel.expression, slice_driver, attrs) {
            SelMatch::Yes => {}
            // A definite No short-circuits the AND: the device does not match this class.
            SelMatch::No => return SelMatch::No,
            SelMatch::Unevaluable => any_unevaluable = true,
        }
    }
    // Reached only if no selector was a definite No.
    if any_unevaluable {
        SelMatch::Unevaluable
    } else {
        SelMatch::Yes
    }
}

/// Highest `pool.generation` per `(driver, pool_name)` — later slices supersede stale ones.
fn latest_generations(slices: &[dra::ResourceSlice]) -> BTreeMap<(String, String), i64> {
    let mut g: BTreeMap<(String, String), i64> = BTreeMap::new();
    for s in slices {
        let spec = &s.spec;
        let key = (spec.driver.clone(), spec.pool.name.clone());
        let gen = spec.pool.generation;
        g.entry(key)
            .and_modify(|cur| {
                if gen > *cur {
                    *cur = gen
                }
            })
            .or_insert(gen);
    }
    g
}

/// Allocated device identities `(driver, pool, device)` from all claim statuses.
fn allocated_identities(claims: &[dra::ResourceClaim]) -> BTreeSet<(String, String, String)> {
    let mut out = BTreeSet::new();
    for c in claims {
        let Some(alloc) = c.status.as_ref().and_then(|s| s.allocation.as_ref()) else {
            continue;
        };
        let Some(dev) = alloc.devices.as_ref() else {
            continue;
        };
        let Some(results) = dev.results.as_ref() else {
            continue;
        };
        for r in results {
            out.insert((r.driver.clone(), r.pool.clone(), r.device.clone()));
        }
    }
    out
}

/// Compute per-node, per-class available (unallocated, matching) device counts.
pub fn compute_availability(
    slices: &[dra::ResourceSlice],
    classes: &[dra::DeviceClass],
    claims: &[dra::ResourceClaim],
) -> DraAvailability {
    let latest = latest_generations(slices);
    let allocated = allocated_identities(claims);
    let mut out = DraAvailability::default();

    for s in slices {
        let spec = &s.spec;
        // MVP node scoping: only nodeName-scoped slices are attributed to a node.
        let Some(node) = spec.node_name.as_ref().filter(|n| !n.is_empty()) else {
            continue;
        };
        // Trust only the newest generation for this (driver, pool).
        let key = (spec.driver.clone(), spec.pool.name.clone());
        if latest.get(&key).copied() != Some(spec.pool.generation) {
            continue;
        }
        let Some(devices) = spec.devices.as_ref() else {
            continue;
        };
        for device in devices {
            let id = (
                spec.driver.clone(),
                spec.pool.name.clone(),
                device.name.clone(),
            );
            if allocated.contains(&id) {
                continue; // already allocated to a claim
            }
            let empty = BTreeMap::new();
            let attrs = device
                .basic
                .as_ref()
                .and_then(|b| b.attributes.as_ref())
                .unwrap_or(&empty);
            let mut matched_here = 0;
            for class in classes {
                let Some(class_name) = class.metadata.name.as_ref() else {
                    continue;
                };
                match class_matches(class, &spec.driver, attrs) {
                    SelMatch::Yes => {
                        *out.by_node_class
                            .entry((node.to_string(), class_name.clone()))
                            .or_default() += 1;
                        matched_here += 1;
                    }
                    SelMatch::No => {}
                    SelMatch::Unevaluable => {
                        out.unevaluable_classes.insert(class_name.clone());
                    }
                }
            }
            if matched_here > 1 {
                out.overlapping_classes = true;
            }
        }
    }
    out
}

/// Per-class device demand of a claim, plus caveats for anything not precisely modeled.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ClaimDemand {
    pub by_class: BTreeMap<String, i64>,
    pub caveats: Vec<String>,
}

/// Sum a claim's device demand per DeviceClass. ExactCount/absent allocationMode ⇒ count (default
/// 1); `All`/unknown ⇒ caveat + not counted. Request selectors / device constraints ⇒ caveat
/// (placement is optimistic — the scalar model ignores which specific devices are eligible).
pub fn claim_demand(claim: &dra::ResourceClaim) -> ClaimDemand {
    match claim.spec.devices.as_ref() {
        Some(devices) => demand_from_device_claim(devices),
        None => ClaimDemand::default(),
    }
}

/// Same as [`claim_demand`] but over a `DeviceClaim` directly — so a pending pod's
/// `ResourceClaimTemplate` (whose embedded `spec.devices` has no materialized claim yet) can be
/// scored identically to a live `ResourceClaim`.
pub fn demand_from_device_claim(devices: &dra::DeviceClaim) -> ClaimDemand {
    let mut out = ClaimDemand::default();
    if devices
        .constraints
        .as_ref()
        .map(|c| !c.is_empty())
        .unwrap_or(false)
    {
        out.caveats
            .push("DRA: device constraints not modeled (placement optimistic)".to_string());
    }
    let Some(requests) = devices.requests.as_ref() else {
        return out;
    };
    for req in requests {
        let mode = req.allocation_mode.as_deref().unwrap_or("ExactCount");
        if mode != "ExactCount" {
            out.caveats.push(format!(
                "DRA: request '{}' allocationMode={} not modeled",
                req.name, mode
            ));
            continue;
        }
        if req
            .selectors
            .as_ref()
            .map(|s| !s.is_empty())
            .unwrap_or(false)
        {
            out.caveats.push(format!(
                "DRA: request '{}' selector not fully evaluated (placement optimistic)",
                req.name
            ));
        }
        let count = req.count.unwrap_or(1).max(0);
        *out.by_class
            .entry(req.device_class_name.clone())
            .or_default() += count;
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use k8s_openapi::api::resource::v1alpha3 as dra;
    use kube::api::ObjectMeta;

    fn attr_str(s: &str) -> dra::DeviceAttribute {
        dra::DeviceAttribute {
            string: Some(s.to_string()),
            ..Default::default()
        }
    }

    fn device(name: &str, attrs: &[(&str, dra::DeviceAttribute)]) -> dra::Device {
        dra::Device {
            name: name.to_string(),
            basic: Some(dra::BasicDevice {
                attributes: Some(
                    attrs
                        .iter()
                        .map(|(k, v)| (k.to_string(), v.clone()))
                        .collect(),
                ),
                ..Default::default()
            }),
        }
    }

    fn slice(
        node: &str,
        driver: &str,
        pool: &str,
        gen: i64,
        devices: Vec<dra::Device>,
    ) -> dra::ResourceSlice {
        dra::ResourceSlice {
            spec: dra::ResourceSliceSpec {
                driver: driver.to_string(),
                node_name: Some(node.to_string()),
                pool: dra::ResourcePool {
                    name: pool.to_string(),
                    generation: gen,
                    resource_slice_count: 1,
                },
                devices: Some(devices),
                ..Default::default()
            },
            ..Default::default()
        }
    }

    fn class(name: &str, exprs: &[&str]) -> dra::DeviceClass {
        dra::DeviceClass {
            metadata: ObjectMeta {
                name: Some(name.to_string()),
                ..Default::default()
            },
            spec: dra::DeviceClassSpec {
                selectors: Some(
                    exprs
                        .iter()
                        .map(|e| dra::DeviceSelector {
                            cel: Some(dra::CELDeviceSelector {
                                expression: e.to_string(),
                            }),
                        })
                        .collect(),
                ),
                ..Default::default()
            },
        }
    }

    #[test]
    fn selector_driver_equality() {
        let a = BTreeMap::new();
        assert_eq!(
            eval_selector(r#"device.driver == "gpu.nvidia.com""#, "gpu.nvidia.com", &a),
            SelMatch::Yes
        );
        assert_eq!(
            eval_selector(r#"device.driver == "other""#, "gpu.nvidia.com", &a),
            SelMatch::No
        );
    }

    #[test]
    fn selector_attribute_equality_grouped() {
        let mut a = BTreeMap::new();
        a.insert("gpu.nvidia.com/model".to_string(), attr_str("A100"));
        assert_eq!(
            eval_selector(
                r#"device.attributes["gpu.nvidia.com"].model == "A100""#,
                "d",
                &a
            ),
            SelMatch::Yes
        );
        assert_eq!(
            eval_selector(
                r#"device.attributes["gpu.nvidia.com"].model == "H100""#,
                "d",
                &a
            ),
            SelMatch::No
        );
        // absent attribute ⇒ predicate false ⇒ No
        assert_eq!(
            eval_selector(
                r#"device.attributes["gpu.nvidia.com"].vendor == "x""#,
                "d",
                &a
            ),
            SelMatch::No
        );
    }

    #[test]
    fn selector_unsupported_is_unevaluable() {
        let a = BTreeMap::new();
        for e in [
            r#"device.attributes["x"].count > 5"#,
            r#"device.driver == "a" && device.attributes["x"].y == "z""#,
            r#"device.attributes["x"].y in ["a","b"]"#,
            r#"has(device.attributes["x"].y)"#,
        ] {
            assert_eq!(
                eval_selector(e, "d", &a),
                SelMatch::Unevaluable,
                "expr: {e}"
            );
        }
    }

    #[test]
    fn availability_counts_and_subtracts_allocation() {
        let slices = vec![slice(
            "n1",
            "gpu.nvidia.com",
            "p",
            1,
            vec![
                device("gpu0", &[("gpu.nvidia.com/model", attr_str("A100"))]),
                device("gpu1", &[("gpu.nvidia.com/model", attr_str("A100"))]),
            ],
        )];
        let classes = vec![class(
            "a100",
            &[r#"device.attributes["gpu.nvidia.com"].model == "A100""#],
        )];
        // gpu0 already allocated to a claim.
        let claims = vec![dra::ResourceClaim {
            status: Some(dra::ResourceClaimStatus {
                allocation: Some(dra::AllocationResult {
                    devices: Some(dra::DeviceAllocationResult {
                        results: Some(vec![dra::DeviceRequestAllocationResult {
                            device: "gpu0".into(),
                            driver: "gpu.nvidia.com".into(),
                            pool: "p".into(),
                            request: "req".into(),
                            ..Default::default()
                        }]),
                        ..Default::default()
                    }),
                    ..Default::default()
                }),
                ..Default::default()
            }),
            ..Default::default()
        }];
        let avail = compute_availability(&slices, &classes, &claims);
        assert_eq!(
            avail.by_node_class.get(&("n1".into(), "a100".into())),
            Some(&1)
        );
        assert!(!avail.overlapping_classes);
        assert!(avail.unevaluable_classes.is_empty());
    }

    #[test]
    fn stale_generation_ignored() {
        let slices = vec![
            slice("n1", "d", "p", 1, vec![device("g0", &[])]),
            slice(
                "n1",
                "d",
                "p",
                2,
                vec![device("g0", &[]), device("g1", &[])],
            ),
        ];
        let classes = vec![class("all", &[])]; // empty selectors ⇒ matches all
        let avail = compute_availability(&slices, &classes, &[]);
        // only gen 2 counted ⇒ 2 devices
        assert_eq!(
            avail.by_node_class.get(&("n1".into(), "all".into())),
            Some(&2)
        );
    }

    #[test]
    fn overlap_flagged_when_device_matches_two_classes() {
        let slices = vec![slice("n1", "d", "p", 1, vec![device("g0", &[])])];
        let classes = vec![class("c1", &[]), class("c2", &[])]; // both empty ⇒ both match g0
        let avail = compute_availability(&slices, &classes, &[]);
        assert!(avail.overlapping_classes);
    }

    #[test]
    fn definite_no_wins_over_unevaluable_selector() {
        // A class whose first selector is a definite No (driver mismatch) plus a second unevaluable
        // selector must resolve to No (device excluded) — NOT unevaluable — so it is neither counted
        // nor flagged unevaluable.
        let slices = vec![slice(
            "n1",
            "gpu.other.com",
            "p",
            1,
            vec![device("g0", &[])],
        )];
        let classes = vec![class(
            "c",
            &[
                r#"device.driver == "gpu.nvidia.com""#, // No for driver gpu.other.com
                r#"device.attributes["x"].y > 3"#,      // Unevaluable
            ],
        )];
        let avail = compute_availability(&slices, &classes, &[]);
        assert!(avail.by_node_class.is_empty());
        assert!(
            avail.unevaluable_classes.is_empty(),
            "definite No must not mark the class unevaluable"
        );
    }

    #[test]
    fn unevaluable_class_not_counted() {
        let slices = vec![slice("n1", "d", "p", 1, vec![device("g0", &[])])];
        let classes = vec![class("weird", &[r#"device.attributes["x"].y > 3"#])];
        let avail = compute_availability(&slices, &classes, &[]);
        assert!(avail.unevaluable_classes.contains("weird"));
        assert!(avail.by_node_class.is_empty());
    }

    fn claim_with(reqs: Vec<dra::DeviceRequest>) -> dra::ResourceClaim {
        dra::ResourceClaim {
            spec: dra::ResourceClaimSpec {
                devices: Some(dra::DeviceClaim {
                    requests: Some(reqs),
                    ..Default::default()
                }),
            },
            ..Default::default()
        }
    }

    #[test]
    fn claim_demand_sums_exactcount() {
        let c = claim_with(vec![
            dra::DeviceRequest {
                name: "r1".into(),
                device_class_name: "a100".into(),
                count: Some(2),
                ..Default::default()
            },
            dra::DeviceRequest {
                name: "r2".into(),
                device_class_name: "a100".into(),
                count: None, // default 1
                ..Default::default()
            },
        ]);
        let d = claim_demand(&c);
        assert_eq!(d.by_class.get("a100"), Some(&3));
        assert!(d.caveats.is_empty());
    }

    #[test]
    fn claim_demand_caveats_all_mode_and_request_selector() {
        let c = claim_with(vec![
            dra::DeviceRequest {
                name: "all".into(),
                device_class_name: "a100".into(),
                allocation_mode: Some("All".into()),
                ..Default::default()
            },
            dra::DeviceRequest {
                name: "sel".into(),
                device_class_name: "a100".into(),
                count: Some(1),
                selectors: Some(vec![dra::DeviceSelector {
                    cel: Some(dra::CELDeviceSelector {
                        expression: "true".into(),
                    }),
                }]),
                ..Default::default()
            },
        ]);
        let d = claim_demand(&c);
        // "All" not counted; selector request counted (1) but caveated.
        assert_eq!(d.by_class.get("a100"), Some(&1));
        assert_eq!(d.caveats.len(), 2);
    }
}
