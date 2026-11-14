//! Optional builders for filters, updates and sort specs.
//!
//! Everything that takes a filter or update accepts `impl Into<serde_json::Value>`, so
//! you can use `serde_json::json!` directly or these builders, whichever reads better:
//!
//! ```
//! use cassa_client::{Filter, Update};
//! use serde_json::json;
//!
//! let a = Filter::new().eq("city", "Leeds").gte("age", 18).lt("age", 65);
//! assert_eq!(serde_json::Value::from(a), json!({"city": "Leeds", "age": {"$gte": 18, "$lt": 65}}));
//!
//! let u = Update::new().set("addr.zip", "LS1").inc("visits", 1).push("tags", "vip");
//! assert_eq!(serde_json::Value::from(u), json!({"$set": {"addr.zip": "LS1"}, "$inc": {"visits": 1}, "$push": {"tags": "vip"}}));
//! ```

use serde_json::{Map, Value};

/// A MongoDB-style filter. Conditions on different fields are ANDed; several
/// operators on the same field are merged (`gte` + `lt` → `{"$gte": .., "$lt": ..}`).
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Filter(Map<String, Value>);

impl Filter {
    /// An empty filter, which matches every document.
    pub fn new() -> Self {
        Filter(Map::new())
    }

    fn op(mut self, field: &str, op: &str, v: Value) -> Self {
        let entry = self.0.entry(field.to_string()).or_insert_with(|| Value::Object(Map::new()));
        if !entry.is_object() || entry.as_object().is_some_and(|m| m.keys().any(|k| !k.starts_with('$'))) {
            *entry = Value::Object(Map::new()); // replace a literal equality on the same field
        }
        entry.as_object_mut().unwrap().insert(op.to_string(), v);
        self
    }

    /// `field == value` (also matches arrays containing `value`).
    pub fn eq(mut self, field: &str, value: impl Into<Value>) -> Self {
        self.0.insert(field.to_string(), value.into());
        self
    }
    pub fn ne(self, field: &str, value: impl Into<Value>) -> Self {
        self.op(field, "$ne", value.into())
    }
    pub fn gt(self, field: &str, value: impl Into<Value>) -> Self {
        self.op(field, "$gt", value.into())
    }
    pub fn gte(self, field: &str, value: impl Into<Value>) -> Self {
        self.op(field, "$gte", value.into())
    }
    pub fn lt(self, field: &str, value: impl Into<Value>) -> Self {
        self.op(field, "$lt", value.into())
    }
    pub fn lte(self, field: &str, value: impl Into<Value>) -> Self {
        self.op(field, "$lte", value.into())
    }
    /// `field` is any of `values`.
    pub fn is_in<V: Into<Value>>(self, field: &str, values: impl IntoIterator<Item = V>) -> Self {
        self.op(field, "$in", Value::Array(values.into_iter().map(Into::into).collect()))
    }
    /// `field` is none of `values`.
    pub fn not_in<V: Into<Value>>(self, field: &str, values: impl IntoIterator<Item = V>) -> Self {
        self.op(field, "$nin", Value::Array(values.into_iter().map(Into::into).collect()))
    }
    pub fn exists(self, field: &str, exists: bool) -> Self {
        self.op(field, "$exists", Value::Bool(exists))
    }
    /// The array in `field` has exactly `len` elements.
    pub fn size(self, field: &str, len: usize) -> Self {
        self.op(field, "$size", Value::from(len))
    }
    /// At least one of `alternatives` matches.
    pub fn or(mut self, alternatives: impl IntoIterator<Item = Filter>) -> Self {
        self.0.insert("$or".into(), Value::Array(alternatives.into_iter().map(Value::from).collect()));
        self
    }
    /// None of `alternatives` matches.
    pub fn nor(mut self, alternatives: impl IntoIterator<Item = Filter>) -> Self {
        self.0.insert("$nor".into(), Value::Array(alternatives.into_iter().map(Value::from).collect()));
        self
    }
}

impl From<Filter> for Value {
    fn from(f: Filter) -> Value {
        Value::Object(f.0)
    }
}

/// A MongoDB-style update made of `$set`, `$unset`, `$inc` and `$push`.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Update(Map<String, Value>);

impl Update {
    pub fn new() -> Self {
        Update(Map::new())
    }

    fn op(mut self, op: &str, field: &str, v: Value) -> Self {
        self.0
            .entry(op.to_string())
            .or_insert_with(|| Value::Object(Map::new()))
            .as_object_mut()
            .unwrap()
            .insert(field.to_string(), v);
        self
    }

    /// Set a field (dotted paths create nested objects).
    pub fn set(self, field: &str, value: impl Into<Value>) -> Self {
        self.op("$set", field, value.into())
    }
    /// Remove a field.
    pub fn unset(self, field: &str) -> Self {
        self.op("$unset", field, Value::String(String::new()))
    }
    /// Add to a number (a missing field counts as 0).
    pub fn inc(self, field: &str, by: impl Into<Value>) -> Self {
        self.op("$inc", field, by.into())
    }
    /// Append to an array (created if missing).
    pub fn push(self, field: &str, value: impl Into<Value>) -> Self {
        self.op("$push", field, value.into())
    }
}

impl From<Update> for Value {
    fn from(u: Update) -> Value {
        Value::Object(u.0)
    }
}

/// Sort direction for [`crate::Find::sort_by`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Order {
    Asc,
    Desc,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn filters() {
        let f = Filter::new()
            .eq("city", "Leeds")
            .gt("age", 30)
            .lte("age", 40)
            .is_in("tier", ["gold", "silver"])
            .exists("email", true)
            .or([Filter::new().eq("vip", true), Filter::new().size("tags", 0)]);
        assert_eq!(
            Value::from(f),
            json!({"city": "Leeds", "age": {"$gt": 30, "$lte": 40}, "tier": {"$in": ["gold", "silver"]},
                   "email": {"$exists": true}, "$or": [{"vip": true}, {"tags": {"$size": 0}}]})
        );
        // An operator after an equality on the same field replaces the equality.
        assert_eq!(Value::from(Filter::new().eq("a", 1).ne("a", 2)), json!({"a": {"$ne": 2}}));
        assert_eq!(Value::from(Filter::new()), json!({}));
    }

    #[test]
    fn updates() {
        let u = Update::new().set("a", 1).set("b.c", "x").unset("old").inc("n", -2.5).push("tags", json!({"k": 1}));
        assert_eq!(
            Value::from(u),
            json!({"$set": {"a": 1, "b.c": "x"}, "$unset": {"old": ""}, "$inc": {"n": -2.5}, "$push": {"tags": {"k": 1}}})
        );
    }
}
