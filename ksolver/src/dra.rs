//! DRA (Dynamic Resource Allocation) F3a — scalar approximation, version-adaptive.
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
//! **Version-adaptive (k8s 1.31–1.35).** The `resource.k8s.io` API changes group-version across this
//! range (`v1alpha3` → `v1beta1` → `v1beta2` → `v1` GA) and no single served version spans it, so a
//! single typed `k8s-openapi` build cannot. Instead this module parses DRA objects from
//! `serde_json::Value` (the collector lists them as `DynamicObject` at the cluster's discovered
//! served version), navigating fields shape-tolerantly:
//! - device attributes at `.attributes` (v1beta2/v1) OR `.basic.attributes` (v1alpha3/v1beta1);
//! - a request's device fields flat on the request (v1alpha3/v1beta1) OR nested under `.exactly`
//!   (v1beta2/v1), with `.firstAvailable[]` handled as worst-case (first alternative) + caveat.
//!
//! JSON keys are Kubernetes camelCase (`nodeName`, `deviceClassName`, `allocationMode`).
//!
//! Contract:
//! - **Selector evaluation:** a `DeviceClass` matches a device iff every class selector matches. A
//!   selector's CEL `expression` is supported ONLY when it reduces to a single equality
//!   `device.driver == "<lit>"` or `device.attributes["<domain>"].<name> == <lit>`. Empty selectors
//!   ⇒ matches all. Any other expression ⇒ the class is **unevaluable** (its devices are not counted;
//!   pods requesting it are caveated).
//! - **Allocation source of truth = `ResourceClaim.status.allocation`** (NOT slices). Device identity
//!   is `(driver, pool, device)`. Availability = matching slice devices − allocated identities.
//! - **Node scoping:** only `spec.nodeName`-scoped slices are attributed to a node (MVP). Only the
//!   highest `pool.generation` per `(driver, pool)` is trusted (stale slices ignored).
//! - **Demand:** `allocationMode` ExactCount/absent ⇒ `count` (default 1); `All` or unknown ⇒
//!   caveat + not counted. Request-level selectors / device constraints ⇒ caveat (placement optimistic).

use serde_json::{Map, Value};
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

/// Compare a device attribute's typed value (a JSON object like `{"string":"A100"}`) to a literal.
fn attr_equals(attr: &Value, lit: &Literal) -> bool {
    match lit {
        Literal::Str(s) => {
            attr.get("string").and_then(Value::as_str) == Some(s.as_str())
                || attr.get("version").and_then(Value::as_str) == Some(s.as_str())
        }
        Literal::Int(n) => attr.get("int").and_then(Value::as_i64) == Some(*n),
        Literal::Bool(b) => attr.get("bool").and_then(Value::as_bool) == Some(*b),
    }
}

/// Look up a `device.attributes["<domain>"].<name>` reference in the API attribute map, which keys
/// entries as `"<domain>/<name>"` (fully qualified) or bare `"<name>"`.
fn lookup_attr<'a>(attrs: &'a Map<String, Value>, domain: &str, name: &str) -> Option<&'a Value> {
    attrs.get(&format!("{domain}/{name}")).or_else(|| attrs.get(name))
}

/// Evaluate one DeviceClass selector CEL `expression` against a device (its owning slice `driver`
/// and its `attributes`). Supports only single-equality forms; anything else ⇒ `Unevaluable`.
fn eval_selector(expr: &str, slice_driver: &str, attrs: &Map<String, Value>) -> SelMatch {
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

/// The attribute map of a device, tolerating both shapes: `.attributes` (v1beta2/v1) or
/// `.basic.attributes` (v1alpha3/v1beta1).
fn device_attributes(device: &Value) -> Option<&Map<String, Value>> {
    device
        .get("attributes")
        .and_then(Value::as_object)
        .or_else(|| {
            device
                .get("basic")
                .and_then(|b| b.get("attributes"))
                .and_then(Value::as_object)
        })
}

/// Whether a class matches a device (selectors are ANDed). CEL AND semantics: a definite `No`
/// makes the class NOT match (returned even if other selectors are Unevaluable — `false && unknown`
/// is `false`, so the device is safely excluded without a spurious unevaluable caveat). Otherwise
/// any Unevaluable ⇒ Unevaluable; empty/all-Yes ⇒ Yes.
fn class_matches(class_spec: &Value, slice_driver: &str, attrs: &Map<String, Value>) -> SelMatch {
    let selectors = class_spec.get("selectors").and_then(Value::as_array);
    let Some(selectors) = selectors else {
        return SelMatch::Yes;
    };
    if selectors.is_empty() {
        return SelMatch::Yes;
    }
    let mut any_unevaluable = false;
    for sel in selectors {
        let Some(expr) = sel
            .get("cel")
            .and_then(|c| c.get("expression"))
            .and_then(Value::as_str)
        else {
            any_unevaluable = true;
            continue;
        };
        match eval_selector(expr, slice_driver, attrs) {
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
fn latest_generations(slices: &[Value]) -> BTreeMap<(String, String), i64> {
    let mut g: BTreeMap<(String, String), i64> = BTreeMap::new();
    for s in slices {
        let spec = s.get("spec").unwrap_or(&Value::Null);
        let driver = spec.get("driver").and_then(Value::as_str).unwrap_or_default();
        let pool = spec.get("pool").unwrap_or(&Value::Null);
        let pool_name = pool.get("name").and_then(Value::as_str).unwrap_or_default();
        let gen = pool.get("generation").and_then(Value::as_i64).unwrap_or(0);
        let key = (driver.to_string(), pool_name.to_string());
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
fn allocated_identities(claims: &[Value]) -> BTreeSet<(String, String, String)> {
    let mut out = BTreeSet::new();
    for c in claims {
        let Some(results) = c
            .get("status")
            .and_then(|s| s.get("allocation"))
            .and_then(|a| a.get("devices"))
            .and_then(|d| d.get("results"))
            .and_then(Value::as_array)
        else {
            continue;
        };
        for r in results {
            let driver = r.get("driver").and_then(Value::as_str).unwrap_or_default();
            let pool = r.get("pool").and_then(Value::as_str).unwrap_or_default();
            let device = r.get("device").and_then(Value::as_str).unwrap_or_default();
            out.insert((driver.to_string(), pool.to_string(), device.to_string()));
        }
    }
    out
}

/// Compute per-node, per-class available (unallocated, matching) device counts. Each input is a full
/// DRA object as `serde_json::Value` (ResourceSlice / DeviceClass / ResourceClaim), at whatever
/// `resource.k8s.io` version the cluster serves — field access is shape-tolerant.
pub fn compute_availability(slices: &[Value], classes: &[Value], claims: &[Value]) -> DraAvailability {
    let latest = latest_generations(slices);
    let allocated = allocated_identities(claims);
    let mut out = DraAvailability::default();
    let empty = Map::new();

    for s in slices {
        let spec = s.get("spec").unwrap_or(&Value::Null);
        // MVP node scoping: only nodeName-scoped slices are attributed to a node.
        let node = match spec.get("nodeName").and_then(Value::as_str) {
            Some(n) if !n.is_empty() => n,
            _ => continue,
        };
        let driver = spec.get("driver").and_then(Value::as_str).unwrap_or_default();
        let pool = spec.get("pool").unwrap_or(&Value::Null);
        let pool_name = pool.get("name").and_then(Value::as_str).unwrap_or_default();
        let generation = pool.get("generation").and_then(Value::as_i64).unwrap_or(0);
        // Trust only the newest generation for this (driver, pool).
        let key = (driver.to_string(), pool_name.to_string());
        if latest.get(&key).copied() != Some(generation) {
            continue;
        }
        let Some(devices) = spec.get("devices").and_then(Value::as_array) else {
            continue;
        };
        for device in devices {
            let dev_name = device.get("name").and_then(Value::as_str).unwrap_or_default();
            let id = (
                driver.to_string(),
                pool_name.to_string(),
                dev_name.to_string(),
            );
            if allocated.contains(&id) {
                continue; // already allocated to a claim
            }
            let attrs = device_attributes(device).unwrap_or(&empty);
            let mut matched_here = 0;
            for class in classes {
                let Some(class_name) = class
                    .get("metadata")
                    .and_then(|m| m.get("name"))
                    .and_then(Value::as_str)
                else {
                    continue;
                };
                let class_spec = class.get("spec").unwrap_or(&Value::Null);
                match class_matches(class_spec, driver, attrs) {
                    SelMatch::Yes => {
                        *out.by_node_class
                            .entry((node.to_string(), class_name.to_string()))
                            .or_default() += 1;
                        matched_here += 1;
                    }
                    SelMatch::No => {}
                    SelMatch::Unevaluable => {
                        out.unevaluable_classes.insert(class_name.to_string());
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

/// Sum a claim's device demand per DeviceClass from a full `ResourceClaim` JSON object.
pub fn claim_demand(claim: &Value) -> ClaimDemand {
    match claim.get("spec").and_then(|s| s.get("devices")) {
        Some(devices) => demand_from_device_claim(devices),
        None => ClaimDemand::default(),
    }
}

/// Add one request's device fields (flat/`exactly` shape) to the demand. `req` here is the object
/// carrying `deviceClassName` / `count` / `allocationMode` / `selectors` — either the request itself
/// (v1alpha3/v1beta1) or its `.exactly` sub-object (v1beta2/v1). Preserves the caveat discipline.
fn add_exact_request(out: &mut ClaimDemand, name: &str, req: &Value) {
    let mode = req
        .get("allocationMode")
        .and_then(Value::as_str)
        .unwrap_or("ExactCount");
    if mode != "ExactCount" {
        out.caveats.push(format!(
            "DRA: request '{name}' allocationMode={mode} not modeled"
        ));
        return;
    }
    if req
        .get("selectors")
        .and_then(Value::as_array)
        .map(|s| !s.is_empty())
        .unwrap_or(false)
    {
        out.caveats.push(format!(
            "DRA: request '{name}' selector not fully evaluated (placement optimistic)"
        ));
    }
    let count = req.get("count").and_then(Value::as_i64).unwrap_or(1).max(0);
    let Some(class) = req.get("deviceClassName").and_then(Value::as_str) else {
        out.caveats.push(format!(
            "DRA: request '{name}' has no deviceClassName; not counted"
        ));
        return;
    };
    *out.by_class.entry(class.to_string()).or_default() += count;
}

/// Sum a `DeviceClaim` JSON object's demand per DeviceClass. Shape-tolerant across versions:
/// - `ExactCount`/absent allocationMode ⇒ `count` (default 1); `All`/unknown ⇒ caveat + not counted.
/// - request device fields flat on the request (v1alpha3/v1beta1) OR under `.exactly` (v1beta2/v1).
/// - `.firstAvailable[]` (v1beta2/v1 alternatives) ⇒ count only the first (worst-case) + caveat.
/// - request selectors / device constraints ⇒ caveat (placement optimistic).
pub fn demand_from_device_claim(devices: &Value) -> ClaimDemand {
    let mut out = ClaimDemand::default();
    if devices
        .get("constraints")
        .and_then(Value::as_array)
        .map(|c| !c.is_empty())
        .unwrap_or(false)
    {
        out.caveats
            .push("DRA: device constraints not modeled (placement optimistic)".to_string());
    }
    let Some(requests) = devices.get("requests").and_then(Value::as_array) else {
        return out;
    };
    for req in requests {
        let name = req.get("name").and_then(Value::as_str).unwrap_or_default();
        if let Some(exactly) = req.get("exactly") {
            // v1beta2 / v1 basic request.
            add_exact_request(&mut out, name, exactly);
        } else if let Some(alts) = req.get("firstAvailable").and_then(Value::as_array) {
            // v1beta2 / v1 alternatives: model the first (highest-priority) as worst-case demand
            // and disclose that the rest aren't modeled — fail-safe rather than sum (overcount) or
            // drop (undercount).
            out.caveats.push(format!(
                "DRA: request '{name}' firstAvailable alternatives not fully modeled (counted first only)"
            ));
            if let Some(first) = alts.first() {
                let sub_name = first.get("name").and_then(Value::as_str).unwrap_or(name);
                add_exact_request(&mut out, sub_name, first);
            }
        } else {
            // v1alpha3 / v1beta1 flat request.
            add_exact_request(&mut out, name, req);
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    // ---- v1alpha3/v1beta1 "flat + basic" shape helpers ----
    fn slice_basic(node: &str, driver: &str, pool: &str, gen: i64, devices: Value) -> Value {
        json!({
            "spec": {
                "driver": driver,
                "nodeName": node,
                "pool": {"name": pool, "generation": gen, "resourceSliceCount": 1},
                "devices": devices,
            }
        })
    }
    fn device_basic(name: &str, attrs: Value) -> Value {
        json!({"name": name, "basic": {"attributes": attrs}})
    }
    // ---- v1beta2/v1 "nested + direct attributes" shape helpers ----
    fn device_v1(name: &str, attrs: Value) -> Value {
        json!({"name": name, "attributes": attrs})
    }
    fn class(name: &str, exprs: &[&str]) -> Value {
        let selectors: Vec<Value> = exprs
            .iter()
            .map(|e| json!({"cel": {"expression": e}}))
            .collect();
        json!({"metadata": {"name": name}, "spec": {"selectors": selectors}})
    }

    #[test]
    fn selector_driver_equality() {
        let a = Map::new();
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
        let a = json!({"gpu.nvidia.com/model": {"string": "A100"}})
            .as_object()
            .unwrap()
            .clone();
        assert_eq!(
            eval_selector(r#"device.attributes["gpu.nvidia.com"].model == "A100""#, "d", &a),
            SelMatch::Yes
        );
        assert_eq!(
            eval_selector(r#"device.attributes["gpu.nvidia.com"].model == "H100""#, "d", &a),
            SelMatch::No
        );
        // absent attribute ⇒ predicate false ⇒ No
        assert_eq!(
            eval_selector(r#"device.attributes["gpu.nvidia.com"].vendor == "x""#, "d", &a),
            SelMatch::No
        );
    }

    #[test]
    fn selector_unsupported_is_unevaluable() {
        let a = Map::new();
        for e in [
            r#"device.attributes["x"].count > 5"#,
            r#"device.driver == "a" && device.attributes["x"].y == "z""#,
            r#"device.attributes["x"].y in ["a","b"]"#,
            r#"has(device.attributes["x"].y)"#,
        ] {
            assert_eq!(eval_selector(e, "d", &a), SelMatch::Unevaluable, "expr: {e}");
        }
    }

    #[test]
    fn availability_counts_and_subtracts_allocation_v1alpha3_shape() {
        let slices = vec![slice_basic(
            "n1",
            "gpu.nvidia.com",
            "p",
            1,
            json!([
                device_basic("gpu0", json!({"gpu.nvidia.com/model": {"string": "A100"}})),
                device_basic("gpu1", json!({"gpu.nvidia.com/model": {"string": "A100"}})),
            ]),
        )];
        let classes = vec![class(
            "a100",
            &[r#"device.attributes["gpu.nvidia.com"].model == "A100""#],
        )];
        // gpu0 already allocated to a claim.
        let claims = vec![json!({
            "status": {"allocation": {"devices": {"results": [
                {"device": "gpu0", "driver": "gpu.nvidia.com", "pool": "p", "request": "req"}
            ]}}}
        })];
        let avail = compute_availability(&slices, &classes, &claims);
        assert_eq!(avail.by_node_class.get(&("n1".into(), "a100".into())), Some(&1));
        assert!(!avail.overlapping_classes);
        assert!(avail.unevaluable_classes.is_empty());
    }

    #[test]
    fn availability_v1_shape_direct_attributes_matches_v1alpha3() {
        // Same devices, but expressed in the v1 shape (direct .attributes, no .basic wrapper).
        let slices = vec![slice_basic(
            "n1",
            "gpu.nvidia.com",
            "p",
            1,
            json!([
                device_v1("gpu0", json!({"gpu.nvidia.com/model": {"string": "A100"}})),
                device_v1("gpu1", json!({"gpu.nvidia.com/model": {"string": "A100"}})),
            ]),
        )];
        let classes = vec![class(
            "a100",
            &[r#"device.attributes["gpu.nvidia.com"].model == "A100""#],
        )];
        let avail = compute_availability(&slices, &classes, &[]);
        // both devices counted (v1 attribute shape read correctly)
        assert_eq!(avail.by_node_class.get(&("n1".into(), "a100".into())), Some(&2));
    }

    #[test]
    fn stale_generation_ignored() {
        let slices = vec![
            slice_basic("n1", "d", "p", 1, json!([device_basic("g0", json!({}))])),
            slice_basic(
                "n1",
                "d",
                "p",
                2,
                json!([device_basic("g0", json!({})), device_basic("g1", json!({}))]),
            ),
        ];
        let classes = vec![class("all", &[])]; // empty selectors ⇒ matches all
        let avail = compute_availability(&slices, &classes, &[]);
        assert_eq!(avail.by_node_class.get(&("n1".into(), "all".into())), Some(&2));
    }

    #[test]
    fn overlap_flagged_when_device_matches_two_classes() {
        let slices = vec![slice_basic("n1", "d", "p", 1, json!([device_basic("g0", json!({}))]))];
        let classes = vec![class("c1", &[]), class("c2", &[])]; // both empty ⇒ both match g0
        let avail = compute_availability(&slices, &classes, &[]);
        assert!(avail.overlapping_classes);
    }

    #[test]
    fn definite_no_wins_over_unevaluable_selector() {
        let slices = vec![slice_basic("n1", "gpu.other.com", "p", 1, json!([device_basic("g0", json!({}))]))];
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
        let slices = vec![slice_basic("n1", "d", "p", 1, json!([device_basic("g0", json!({}))]))];
        let classes = vec![class("weird", &[r#"device.attributes["x"].y > 3"#])];
        let avail = compute_availability(&slices, &classes, &[]);
        assert!(avail.unevaluable_classes.contains("weird"));
        assert!(avail.by_node_class.is_empty());
    }

    fn claim_with_requests(requests: Value) -> Value {
        json!({"spec": {"devices": {"requests": requests}}})
    }

    #[test]
    fn claim_demand_sums_exactcount_flat() {
        let c = claim_with_requests(json!([
            {"name": "r1", "deviceClassName": "a100", "count": 2},
            {"name": "r2", "deviceClassName": "a100"}, // default 1
        ]));
        let d = claim_demand(&c);
        assert_eq!(d.by_class.get("a100"), Some(&3));
        assert!(d.caveats.is_empty());
    }

    #[test]
    fn claim_demand_sums_exactcount_nested_v1() {
        // v1 shape: fields under `exactly`.
        let c = claim_with_requests(json!([
            {"name": "r1", "exactly": {"deviceClassName": "a100", "count": 2}},
            {"name": "r2", "exactly": {"deviceClassName": "a100"}},
        ]));
        let d = claim_demand(&c);
        assert_eq!(d.by_class.get("a100"), Some(&3));
        assert!(d.caveats.is_empty());
    }

    #[test]
    fn claim_demand_first_available_counts_first_with_caveat() {
        let c = claim_with_requests(json!([
            {"name": "r1", "firstAvailable": [
                {"name": "big", "deviceClassName": "a100", "count": 2},
                {"name": "small", "deviceClassName": "t4", "count": 1},
            ]},
        ]));
        let d = claim_demand(&c);
        assert_eq!(d.by_class.get("a100"), Some(&2)); // first alternative counted
        assert_eq!(d.by_class.get("t4"), None); // rest not modeled
        assert!(d.caveats.iter().any(|c| c.contains("firstAvailable")));
    }

    #[test]
    fn claim_demand_caveats_all_mode_and_request_selector() {
        let c = claim_with_requests(json!([
            {"name": "all", "deviceClassName": "a100", "allocationMode": "All"},
            {"name": "sel", "deviceClassName": "a100", "count": 1,
             "selectors": [{"cel": {"expression": "true"}}]},
        ]));
        let d = claim_demand(&c);
        // "All" not counted; selector request counted (1) but caveated.
        assert_eq!(d.by_class.get("a100"), Some(&1));
        assert_eq!(d.caveats.len(), 2);
    }

    #[test]
    fn template_devices_scored_via_demand_from_device_claim() {
        // A ResourceClaimTemplate's embedded DeviceClaim (spec.spec.devices) scores like a claim.
        let template = json!({"spec": {"spec": {"devices": {"requests": [
            {"name": "r", "deviceClassName": "a100", "count": 4}
        ]}}}});
        let devices = template.get("spec").unwrap().get("spec").unwrap().get("devices").unwrap();
        let d = demand_from_device_claim(devices);
        assert_eq!(d.by_class.get("a100"), Some(&4));
    }
}
