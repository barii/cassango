//! MongoDB-flavoured query filters, sorting, and update operators on JSON documents.
//!
//! Supported filter syntax:
//!   { "field": value }                      equality (matches array elements too)
//!   { "a.b.0": value }                      dotted paths, numeric segments index arrays
//!   { "field": { "$eq|$ne|$gt|$gte|$lt|$lte": v } }
//!   { "field": { "$in|$nin": [..] } }
//!   { "field": { "$exists": bool } }
//!   { "field": { "$size": n } }
//!   { "field": { "$not": { <operators> } } }
//!   { "$and|$or|$nor": [ filter, ... ] }
//!
//! Supported update operators: $set, $unset, $inc, $push.

use serde_json::{Map, Value};
use std::cmp::Ordering;

// ---------------------------------------------------------------- paths

pub fn get_path<'a>(doc: &'a Value, path: &str) -> Option<&'a Value> {
    let mut cur = doc;
    for seg in path.split('.') {
        cur = match cur {
            Value::Object(m) => m.get(seg)?,
            Value::Array(a) => a.get(seg.parse::<usize>().ok()?)?,
            _ => return None,
        };
    }
    Some(cur)
}

fn get_path_mut<'a>(doc: &'a mut Value, path: &str) -> Option<&'a mut Value> {
    let mut cur = doc;
    for seg in path.split('.') {
        cur = match cur {
            Value::Object(m) => m.get_mut(seg)?,
            Value::Array(a) => a.get_mut(seg.parse::<usize>().ok()?)?,
            _ => return None,
        };
    }
    Some(cur)
}

/// Navigate to `path`, creating intermediate objects (and the leaf as `null`) as needed.
fn path_mut_create<'a>(doc: &'a mut Value, path: &str) -> Result<&'a mut Value, String> {
    let mut cur = doc;
    for seg in path.split('.') {
        if cur.is_null() {
            *cur = Value::Object(Map::new());
        }
        cur = match cur {
            Value::Object(m) => m.entry(seg.to_string()).or_insert(Value::Null),
            Value::Array(a) => {
                let i: usize = seg
                    .parse()
                    .map_err(|_| format!("'{seg}' is not an array index in path '{path}'"))?;
                a.get_mut(i)
                    .ok_or_else(|| format!("index {i} out of bounds in path '{path}'"))?
            }
            _ => return Err(format!("cannot traverse into scalar at '{seg}' in path '{path}'")),
        };
    }
    Ok(cur)
}

// ---------------------------------------------------------------- validation

fn is_operator_obj(v: &Value) -> bool {
    matches!(v, Value::Object(m) if !m.is_empty() && m.keys().all(|k| k.starts_with('$')))
}

/// Validate a filter up front so that matching can be infallible.
pub fn validate(filter: &Value) -> Result<(), String> {
    let f = filter.as_object().ok_or("filter must be a JSON object")?;
    for (key, cond) in f {
        match key.as_str() {
            "$and" | "$or" | "$nor" => {
                let arr = cond
                    .as_array()
                    .filter(|a| !a.is_empty())
                    .ok_or_else(|| format!("{key} requires a non-empty array"))?;
                for sub in arr {
                    validate(sub)?;
                }
            }
            k if k.starts_with('$') => return Err(format!("unknown top-level operator {k}")),
            _ => validate_cond(cond)?,
        }
    }
    Ok(())
}

fn validate_cond(cond: &Value) -> Result<(), String> {
    if !is_operator_obj(cond) {
        return Ok(()); // literal equality
    }
    for (op, arg) in cond.as_object().unwrap() {
        match op.as_str() {
            "$eq" | "$ne" | "$gt" | "$gte" | "$lt" | "$lte" => {}
            "$in" | "$nin" => {
                if !arg.is_array() {
                    return Err(format!("{op} requires an array"));
                }
            }
            "$exists" => {
                if !arg.is_boolean() {
                    return Err("$exists requires a boolean".into());
                }
            }
            "$size" => {
                if arg.as_u64().is_none() {
                    return Err("$size requires a non-negative integer".into());
                }
            }
            "$not" => {
                if !is_operator_obj(arg) {
                    return Err("$not requires an operator object".into());
                }
                validate_cond(arg)?;
            }
            _ => return Err(format!("unknown operator {op}")),
        }
    }
    Ok(())
}

// ---------------------------------------------------------------- matching

/// Does `doc` match `filter`? Assumes `validate(filter)` succeeded.
pub fn matches(doc: &Value, filter: &Value) -> bool {
    let Some(f) = filter.as_object() else { return false };
    f.iter().all(|(key, cond)| match key.as_str() {
        "$and" => list(cond).iter().all(|s| matches(doc, s)),
        "$or" => list(cond).iter().any(|s| matches(doc, s)),
        "$nor" => !list(cond).iter().any(|s| matches(doc, s)),
        k if k.starts_with('$') => false,
        path => match_field(get_path(doc, path), cond),
    })
}

fn list(v: &Value) -> &[Value] {
    v.as_array().map(Vec::as_slice).unwrap_or(&[])
}

fn match_field(field: Option<&Value>, cond: &Value) -> bool {
    if is_operator_obj(cond) {
        cond.as_object()
            .unwrap()
            .iter()
            .all(|(op, arg)| apply_op(field, op, arg))
    } else {
        eq_match(field, cond)
    }
}

fn values_equal(a: &Value, b: &Value) -> bool {
    match (a, b) {
        (Value::Number(x), Value::Number(y)) => x.as_f64() == y.as_f64(),
        _ => a == b,
    }
}

/// Mongo equality: a missing field equals null; an array field matches if any element matches.
fn eq_match(field: Option<&Value>, v: &Value) -> bool {
    match field {
        None => v.is_null(),
        Some(f) => {
            values_equal(f, v)
                || matches!(f, Value::Array(a) if a.iter().any(|e| values_equal(e, v)))
        }
    }
}

fn cmp_same_type(a: &Value, b: &Value) -> Option<Ordering> {
    match (a, b) {
        (Value::Number(x), Value::Number(y)) => x.as_f64()?.partial_cmp(&y.as_f64()?),
        (Value::String(x), Value::String(y)) => Some(x.cmp(y)),
        (Value::Bool(x), Value::Bool(y)) => Some(x.cmp(y)),
        _ => None,
    }
}

fn cmp_match(field: Option<&Value>, arg: &Value, pred: impl Fn(Ordering) -> bool) -> bool {
    let test = |v: &Value| cmp_same_type(v, arg).is_some_and(&pred);
    match field {
        None => false,
        Some(Value::Array(a)) => a.iter().any(test),
        Some(v) => test(v),
    }
}

fn apply_op(field: Option<&Value>, op: &str, arg: &Value) -> bool {
    match op {
        "$eq" => eq_match(field, arg),
        "$ne" => !eq_match(field, arg),
        "$gt" => cmp_match(field, arg, |o| o == Ordering::Greater),
        "$gte" => cmp_match(field, arg, |o| o != Ordering::Less),
        "$lt" => cmp_match(field, arg, |o| o == Ordering::Less),
        "$lte" => cmp_match(field, arg, |o| o != Ordering::Greater),
        "$in" => list(arg).iter().any(|v| eq_match(field, v)),
        "$nin" => !list(arg).iter().any(|v| eq_match(field, v)),
        "$exists" => field.is_some() == arg.as_bool().unwrap_or(true),
        "$size" => matches!(field, Some(Value::Array(a)) if Some(a.len() as u64) == arg.as_u64()),
        "$not" => !match_field(field, arg),
        _ => false,
    }
}

// ---------------------------------------------------------------- sorting

fn type_rank(v: Option<&Value>) -> u8 {
    match v {
        None | Some(Value::Null) => 0,
        Some(Value::Number(_)) => 1,
        Some(Value::String(_)) => 2,
        Some(Value::Object(_)) => 3,
        Some(Value::Array(_)) => 4,
        Some(Value::Bool(_)) => 5,
    }
}

/// Total order across JSON types, loosely following MongoDB's BSON comparison order.
pub fn total_cmp(a: Option<&Value>, b: Option<&Value>) -> Ordering {
    let (ra, rb) = (type_rank(a), type_rank(b));
    if ra != rb {
        return ra.cmp(&rb);
    }
    match (a, b) {
        (Some(x), Some(y)) => cmp_same_type(x, y).unwrap_or_else(|| x.to_string().cmp(&y.to_string())),
        _ => Ordering::Equal,
    }
}

/// Sort documents by a spec like `{"age": -1, "name": 1}`.
pub fn sort_docs(docs: &mut [Value], spec: &Map<String, Value>) -> Result<(), String> {
    let keys: Vec<(&str, bool)> = spec
        .iter()
        .map(|(k, v)| match v.as_i64() {
            Some(1) => Ok((k.as_str(), false)),
            Some(-1) => Ok((k.as_str(), true)),
            _ => Err(format!("sort direction for '{k}' must be 1 or -1")),
        })
        .collect::<Result<_, _>>()?;
    docs.sort_by(|x, y| {
        for (k, desc) in &keys {
            let o = total_cmp(get_path(x, k), get_path(y, k));
            let o = if *desc { o.reverse() } else { o };
            if o != Ordering::Equal {
                return o;
            }
        }
        Ordering::Equal
    });
    Ok(())
}

// ---------------------------------------------------------------- updates

/// Apply a Mongo-style update document (e.g. `{"$set": {"a.b": 1}, "$inc": {"n": 2}}`).
pub fn apply_update(doc: &mut Value, update: &Value) -> Result<(), String> {
    let ops = update.as_object().ok_or("update must be a JSON object")?;
    if ops.is_empty() {
        return Err("update must contain at least one operator".into());
    }
    for (op, fields) in ops {
        let fields = fields
            .as_object()
            .ok_or_else(|| format!("{op} requires an object of field paths"))?;
        for (path, val) in fields {
            if path == "_id" || path.starts_with("_id.") {
                return Err("cannot modify _id".into());
            }
            match op.as_str() {
                "$set" => *path_mut_create(doc, path)? = val.clone(),
                "$unset" => unset_path(doc, path),
                "$inc" => {
                    let slot = path_mut_create(doc, path)?;
                    *slot = add_numbers(slot, val)
                        .ok_or_else(|| format!("$inc on '{path}' requires numeric values"))?;
                }
                "$push" => {
                    let slot = path_mut_create(doc, path)?;
                    match slot {
                        Value::Null => *slot = Value::Array(vec![val.clone()]),
                        Value::Array(a) => a.push(val.clone()),
                        _ => return Err(format!("$push target '{path}' is not an array")),
                    }
                }
                _ => return Err(format!("unknown update operator {op}")),
            }
        }
    }
    Ok(())
}

fn add_numbers(cur: &Value, delta: &Value) -> Option<Value> {
    let delta_n = delta.as_number()?;
    let cur_n = match cur {
        Value::Null => return Some(delta.clone()),
        Value::Number(n) => n,
        _ => return None,
    };
    if let (Some(a), Some(b)) = (cur_n.as_i64(), delta_n.as_i64()) {
        if let Some(s) = a.checked_add(b) {
            return Some(Value::from(s));
        }
    }
    serde_json::Number::from_f64(cur_n.as_f64()? + delta_n.as_f64()?).map(Value::Number)
}

fn unset_path(doc: &mut Value, path: &str) {
    let (parent, last) = match path.rsplit_once('.') {
        Some((p, l)) => (get_path_mut(doc, p), l),
        None => (Some(doc), path),
    };
    if let Some(Value::Object(m)) = parent {
        m.shift_remove(last);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn m(doc: &Value, f: Value) -> bool {
        validate(&f).unwrap();
        matches(doc, &f)
    }

    #[test]
    fn filters() {
        let d = json!({"name": "ann", "age": 31, "tags": ["a", "b"], "addr": {"city": "Leeds"}});
        assert!(m(&d, json!({})));
        assert!(m(&d, json!({"name": "ann"})));
        assert!(m(&d, json!({"tags": "b"})));
        assert!(m(&d, json!({"addr.city": "Leeds"})));
        assert!(m(&d, json!({"tags.1": "b"})));
        assert!(m(&d, json!({"age": {"$gt": 30, "$lte": 31}})));
        assert!(!m(&d, json!({"age": {"$gt": 31}})));
        assert!(m(&d, json!({"age": 31.0})));
        assert!(m(&d, json!({"name": {"$in": ["bob", "ann"]}})));
        assert!(m(&d, json!({"missing": {"$exists": false}})));
        assert!(m(&d, json!({"missing": null})));
        assert!(m(&d, json!({"tags": {"$size": 2}})));
        assert!(m(&d, json!({"$or": [{"name": "bob"}, {"age": 31}]})));
        assert!(!m(&d, json!({"$nor": [{"name": "ann"}]})));
        assert!(m(&d, json!({"age": {"$not": {"$lt": 18}}})));
        assert!(!m(&d, json!({"age": {"$gt": "30"}})), "no cross-type comparison");
    }

    #[test]
    fn invalid_filters() {
        assert!(validate(&json!({"a": {"$bogus": 1}})).is_err());
        assert!(validate(&json!({"$or": []})).is_err());
        assert!(validate(&json!({"a": {"$in": 3}})).is_err());
        assert!(validate(&json!([1])).is_err());
    }

    #[test]
    fn updates() {
        let mut d = json!({"_id": "1", "n": 1, "a": {"b": 2}});
        apply_update(
            &mut d,
            &json!({"$inc": {"n": 2, "new": 1.5}, "$set": {"x.y": true}, "$unset": {"a.b": ""}, "$push": {"l": 7}}),
        )
        .unwrap();
        assert_eq!(d, json!({"_id": "1", "n": 3, "a": {}, "new": 1.5, "x": {"y": true}, "l": [7]}));
        assert!(apply_update(&mut d, &json!({"$set": {"_id": "2"}})).is_err());
        assert!(apply_update(&mut d, &json!({"$inc": {"x": 1}})).is_err());
    }

    #[test]
    fn sorting() {
        let mut docs = vec![json!({"a": 2, "b": "x"}), json!({"a": 1}), json!({"a": 2, "b": "y"}), json!({})];
        sort_docs(&mut docs, json!({"a": -1, "b": 1}).as_object().unwrap()).unwrap();
        assert_eq!(docs, vec![json!({"a": 2, "b": "x"}), json!({"a": 2, "b": "y"}), json!({"a": 1}), json!({})]);
    }
}
