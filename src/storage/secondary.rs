//! Secondary index key encoding and query planning.
//!
//! Index entries live in the same LSM tree as documents, in their own keyspace:
//!
//! ```text
//! data key : <collection> \0 <id>
//! index key: \x01 \0 <collection> \0 <field> \0 <type tag><encoded value> \0 <id>
//! ```
//!
//! Collections can only contain `[A-Za-z0-9_-]`, so the `\x01` prefix never collides
//! with data and sorts before all of it. Values are encoded so that byte order equals
//! value order within a type, which turns `=`, `$in` and range predicates into key-range
//! scans:
//!
//! * number → `n` + 16 hex digits of an order-preserving transform of the f64
//! * string → `s` + the string with `\0` → `\x01\x01` and `\x01` → `\x01\x02`
//! * bool   → `b0` / `b1`
//!
//! Every encoding is valid UTF-8 and contains no raw `\0`, so an index key can travel
//! as an ordinary `Mutation { c: "\u{1}", k: <rest> }` through the commit log.
//! The entry's value is `true` (live) or a tombstone, versioned like the document.

use crate::query::get_path;
use serde_json::Value;
use std::collections::BTreeSet;

/// Pseudo-collection holding all index entries.
pub const INDEX_C: &str = "\u{1}";
/// First byte of every data key; anything below it is index space.
pub const DATA_START: &[u8] = &[2];

fn escape(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for ch in s.chars() {
        match ch {
            '\0' => out.push_str("\u{1}\u{1}"),
            '\u{1}' => out.push_str("\u{1}\u{2}"),
            c => out.push(c),
        }
    }
    out
}

fn encode_f64(f: f64) -> String {
    let f = if f == 0.0 { 0.0 } else { f }; // fold -0.0 into 0.0
    let bits = f.to_bits();
    let ordered = if bits >> 63 == 1 { !bits } else { bits | (1 << 63) };
    format!("{ordered:016x}")
}

/// Order-preserving encoding of a scalar; `None` for values we don't index
/// (null, objects, arrays).
pub fn encode_value(v: &Value) -> Option<String> {
    match v {
        Value::Number(n) => Some(format!("n{}", encode_f64(n.as_f64()?))),
        Value::String(s) => Some(format!("s{}", escape(s))),
        Value::Bool(b) => Some(if *b { "b1".into() } else { "b0".into() }),
        _ => None,
    }
}

/// `<collection>\0<field>\0`: the part of an index key's `k` shared by one index.
fn index_prefix_k(c: &str, field: &str) -> String {
    format!("{c}\0{field}\0")
}

/// Raw-key prefix of every entry of one index.
pub fn index_prefix(c: &str, field: &str) -> Vec<u8> {
    let mut p = vec![1u8, 0];
    p.extend(index_prefix_k(c, field).as_bytes());
    p
}

/// The `k` parts of the index entries a document produces for `field`.
/// Arrays produce one entry per scalar element (Mongo's multikey indexes).
pub fn index_keys(c: &str, field: &str, doc: &Value, id: &str) -> BTreeSet<String> {
    let mut out = BTreeSet::new();
    let mut add = |v: &Value| {
        if let Some(enc) = encode_value(v) {
            out.insert(format!("{}{enc}\0{id}", index_prefix_k(c, field)));
        }
    };
    match get_path(doc, field) {
        Some(Value::Array(items)) => items.iter().for_each(&mut add),
        Some(v) => add(v),
        None => {}
    }
    out
}

/// Split a raw index key into (collection, field, id).
pub fn parse_index_key(key: &[u8]) -> Option<(&[u8], &[u8], &[u8])> {
    let rest = key.strip_prefix(&[1u8, 0][..])?;
    let mut parts = rest.splitn(4, |b| *b == 0);
    let c = parts.next()?;
    let field = parts.next()?;
    let _value = parts.next()?;
    let id = parts.next()?;
    Some((c, field, id))
}

// ------------------------------------------------------------------ planning

/// A raw key range `[start, end)`.
pub type KeyRange = (Vec<u8>, Vec<u8>);

/// A chosen index and the key ranges to scan in it.
#[derive(Debug, PartialEq)]
pub struct Plan {
    pub field: String,
    pub ranges: Vec<KeyRange>,
}

fn eq_range(prefix: &[u8], enc: &str) -> KeyRange {
    let mut start = prefix.to_vec();
    start.extend(enc.as_bytes());
    let mut end = start.clone();
    start.push(0);
    end.push(1);
    (start, end)
}

fn is_operator_obj(v: &Value) -> bool {
    matches!(v, Value::Object(m) if !m.is_empty() && m.keys().all(|k| k.starts_with('$')))
}

/// Key ranges for one field condition, plus a cost rank (lower is more selective).
fn ranges_for(prefix: &[u8], cond: &Value) -> Option<(u8, Vec<KeyRange>)> {
    if !is_operator_obj(cond) {
        // Literal equality. Null means "missing or null", which the index can't see.
        return encode_value(cond).map(|e| (0, vec![eq_range(prefix, &e)]));
    }
    let ops = cond.as_object().unwrap();
    if let Some(e) = ops.get("$eq").and_then(encode_value) {
        return Some((0, vec![eq_range(prefix, &e)]));
    }
    if let Some(Value::Array(vals)) = ops.get("$in") {
        let encs: Option<Vec<String>> = vals.iter().map(encode_value).collect();
        if let Some(encs) = encs.filter(|e| !e.is_empty()) {
            return Some((1, encs.iter().map(|e| eq_range(prefix, e)).collect()));
        }
    }
    // Range: bounds must share a type, and the scan stays within that type.
    // (bound, strict). Either bound alone is a superset of the full condition.
    let lower = ops.get("$gt").map(|v| (v, true)).or(ops.get("$gte").map(|v| (v, false)));
    let upper = ops.get("$lt").map(|v| (v, true)).or(ops.get("$lte").map(|v| (v, false)));
    let tag = match (lower, upper) {
        (None, None) => return None,
        (Some((a, _)), Some((b, _))) if std::mem::discriminant(a) != std::mem::discriminant(b) => return None,
        (Some((v, _)), _) | (None, Some((v, _))) => match v {
            Value::Number(_) => 'n',
            Value::String(_) => 's',
            _ => return None,
        },
    };
    // Entries for value v are `enc(v) \0 id`, so `enc(v)` sorts just before them
    // and `enc(v) \x01` just after them.
    let mut start = prefix.to_vec();
    match lower {
        Some((v, strict)) => {
            start.extend(encode_value(v)?.as_bytes());
            if strict {
                start.push(1); // skip entries for exactly v
            }
        }
        None => start.push(tag as u8),
    }
    let mut end = prefix.to_vec();
    match upper {
        Some((v, strict)) => {
            end.extend(encode_value(v)?.as_bytes());
            if !strict {
                end.push(1); // include entries for exactly v
            }
        }
        None => end.push(tag as u8 + 1), // past every value of this type
    }
    Some((2, vec![(start, end)]))
}

/// Pick the most selective usable index for `filter`, considering top-level
/// conditions and those inside a top-level `$and` (all of which must hold).
pub fn choose(c: &str, filter: &Value, ready_fields: &[String]) -> Option<Plan> {
    let mut conjuncts: Vec<(&String, &Value)> = Vec::new();
    if let Some(f) = filter.as_object() {
        for (k, v) in f {
            if k == "$and" {
                for sub in v.as_array().into_iter().flatten().filter_map(|s| s.as_object()) {
                    conjuncts.extend(sub.iter().filter(|(k, _)| !k.starts_with('$')));
                }
            } else if !k.starts_with('$') {
                conjuncts.push((k, v));
            }
        }
    }
    conjuncts
        .into_iter()
        .filter(|(field, _)| ready_fields.contains(field))
        .filter_map(|(field, cond)| {
            ranges_for(&index_prefix(c, field), cond).map(|(rank, ranges)| (rank, Plan { field: field.clone(), ranges }))
        })
        .min_by_key(|(rank, _)| *rank)
        .map(|(_, plan)| plan)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn number_encoding_preserves_order() {
        let vals = [-1e12, -5.0, -1.5, -0.0, 0.0, 1e-9, 2.0, 10.0, 31.0, 1e12];
        let encs: Vec<String> = vals.iter().map(|v| encode_value(&json!(v)).unwrap()).collect();
        for w in encs.windows(2) {
            assert!(w[0] <= w[1], "{w:?}");
        }
        assert_eq!(encode_value(&json!(31)), encode_value(&json!(31.0)), "int and float equal");
        assert_eq!(encode_value(&json!(-0.0)), encode_value(&json!(0)));
    }

    #[test]
    fn string_encoding_preserves_order_and_has_no_nul() {
        let vals = ["", "\0", "\0z", "\u{1}", "a", "a\0", "a\0b", "ab", "b", "é"];
        let encs: Vec<String> = vals.iter().map(|v| encode_value(&json!(v)).unwrap() + "\0").collect();
        for w in encs.windows(2) {
            assert!(w[0] < w[1], "{w:?}");
        }
        assert!(vals.iter().all(|v| !encode_value(&json!(v)).unwrap().contains('\0')));
    }

    #[test]
    fn keys_and_parsing() {
        let doc = json!({"tags": ["a", "b", {"x": 1}], "n": 5, "o": {"p": true}});
        assert_eq!(index_keys("c", "tags", &doc, "id1").len(), 2, "multikey, objects skipped");
        assert_eq!(index_keys("c", "o.p", &doc, "id1").len(), 1);
        assert!(index_keys("c", "missing", &doc, "id1").is_empty());
        let k = index_keys("c", "n", &doc, "we\0ird").into_iter().next().unwrap();
        let mut raw = vec![1u8, 0];
        raw.extend(k.as_bytes());
        assert_eq!(parse_index_key(&raw), Some((&b"c"[..], &b"n"[..], &b"we\0ird"[..])));
    }

    fn in_ranges(plan: &Plan, c: &str, field: &str, v: Value) -> bool {
        let k = index_keys(c, field, &json!({ field: v }), "id").into_iter().next().unwrap();
        let mut raw = vec![1u8, 0];
        raw.extend(k.as_bytes());
        plan.ranges.iter().any(|(s, e)| raw >= *s && raw < *e)
    }

    #[test]
    fn planner_ranges_cover_exactly_what_they_should() {
        let f = vec!["age".to_string(), "name".to_string()];
        let p = choose("u", &json!({"age": {"$gt": 30, "$lte": 40}}), &f).unwrap();
        assert!(in_ranges(&p, "u", "age", json!(35)));
        assert!(in_ranges(&p, "u", "age", json!(40)), "$lte is inclusive");
        assert!(!in_ranges(&p, "u", "age", json!(30)), "$gt is exclusive");
        let p2 = choose("u", &json!({"age": {"$gte": 30, "$lt": 40}}), &f).unwrap();
        assert!(in_ranges(&p2, "u", "age", json!(30)) && !in_ranges(&p2, "u", "age", json!(40)));
        let p3 = choose("u", &json!({"name": {"$gt": "ab", "$lt": "b"}}), &f).unwrap();
        assert!(!in_ranges(&p3, "u", "name", json!("ab")) && in_ranges(&p3, "u", "name", json!("ab\u{0}")));
        assert!(in_ranges(&p3, "u", "name", json!("abc")) && !in_ranges(&p3, "u", "name", json!("b")));
        assert!(in_ranges(&p3, "u", "name", json!("a\u{ff}")), "between ab and b");
        assert!(!in_ranges(&p3, "u", "name", json!("aa")), "below ab");
        assert!(!in_ranges(&p, "u", "age", json!(41)));
        assert!(!in_ranges(&p, "u", "age", json!(29)));
        assert!(!in_ranges(&p, "u", "age", json!("35")), "range stays within type");

        let p = choose("u", &json!({"name": {"$gte": "m"}}), &f).unwrap();
        assert!(in_ranges(&p, "u", "name", json!("zed")) && !in_ranges(&p, "u", "name", json!("al")));
        assert!(!in_ranges(&p, "u", "name", json!(true)));

        let p = choose("u", &json!({"name": {"$in": ["a", "b"]}}), &f).unwrap();
        assert_eq!(p.ranges.len(), 2);
        assert!(in_ranges(&p, "u", "name", json!("b")) && !in_ranges(&p, "u", "name", json!("ab")));

        // Equality beats range; unindexed / unusable conditions are ignored.
        let p = choose("u", &json!({"age": {"$gt": 1}, "$and": [{"name": "x"}], "zip": 1}), &f).unwrap();
        assert_eq!(p.field, "name");
        assert!(choose("u", &json!({"age": null}), &f).is_none());
        assert!(choose("u", &json!({"age": {"$ne": 3}}), &f).is_none());
        assert!(choose("u", &json!({"$or": [{"age": 1}]}), &f).is_none());
        assert!(choose("u", &json!({"zip": 1}), &f).is_none());
    }
}
