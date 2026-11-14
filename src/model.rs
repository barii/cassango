//! Shared data types: versioned records, mutations, consistency levels, errors.

use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

/// A versioned cell. `value == None` is a tombstone (a delete marker).
///
/// Conflicts are resolved with last-write-wins on `ts` (microseconds since epoch,
/// assigned by the coordinator), exactly like Cassandra.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct Record {
    pub ts: u64,
    #[serde(rename = "v")]
    pub value: Option<Value>,
}

impl Record {
    /// True if `self` should replace `other`.
    /// Ties on timestamp are broken deterministically: tombstones win, then the
    /// lexicographically larger serialized value wins.
    pub fn supersedes(&self, other: &Record) -> bool {
        if self.ts != other.ts {
            return self.ts > other.ts;
        }
        match (&self.value, &other.value) {
            (None, None) => false,
            (None, Some(_)) => true,
            (Some(_), None) => false,
            (Some(a), Some(b)) => a.to_string() > b.to_string(),
        }
    }

    pub fn is_tombstone(&self) -> bool {
        self.value.is_none()
    }
}

/// Pick the newest of a set of optional records.
pub fn newest<'a>(records: impl IntoIterator<Item = &'a Option<Record>>) -> Option<Record> {
    records
        .into_iter()
        .flatten()
        .fold(None::<&Record>, |best, r| match best {
            Some(b) if !r.supersedes(b) => Some(b),
            _ => Some(r),
        })
        .cloned()
}

/// A single write of a record to `collection/key`.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Mutation {
    pub c: String,
    pub k: String,
    pub r: Record,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq, Default)]
#[serde(rename_all = "lowercase")]
pub enum Consistency {
    #[serde(alias = "ONE")]
    One,
    #[default]
    #[serde(alias = "QUORUM")]
    Quorum,
    #[serde(alias = "ALL")]
    All,
}

impl Consistency {
    /// Number of replica acknowledgements required out of `replicas`.
    pub fn required(self, replicas: usize) -> usize {
        match self {
            Consistency::One => 1.min(replicas),
            Consistency::Quorum => replicas / 2 + 1,
            Consistency::All => replicas,
        }
    }
}

// ---- internal RPC payloads ----

#[derive(Serialize, Deserialize)]
pub struct GetReq {
    pub c: String,
    pub k: String,
}

#[derive(Serialize, Deserialize)]
pub struct ScanReq {
    pub c: String,
    pub filter: Value,
}

#[derive(Serialize, Deserialize)]
pub struct MultigetReq {
    pub c: String,
    pub ids: Vec<String>,
}

// ---- errors ----

#[derive(Debug)]
pub enum DbError {
    BadRequest(String),
    NotFound,
    /// Not enough live replicas to even attempt the operation.
    Unavailable { required: usize, alive: usize },
    /// Attempted, but too few replicas responded successfully.
    Timeout { required: usize, acked: usize },
    Internal(String),
}

impl std::fmt::Display for DbError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DbError::BadRequest(m) => write!(f, "bad request: {m}"),
            DbError::NotFound => write!(f, "not found"),
            DbError::Unavailable { required, alive } => write!(
                f,
                "unavailable: {required} replicas required but only {alive} alive"
            ),
            DbError::Timeout { required, acked } => write!(
                f,
                "timeout: {required} replica responses required but only {acked} succeeded"
            ),
            DbError::Internal(m) => write!(f, "internal error: {m}"),
        }
    }
}

// ---- small utilities ----

pub fn now_micros() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock before epoch")
        .as_micros() as u64
}

/// Tiny xorshift PRNG; good enough for picking gossip targets.
pub fn rand_below(n: usize) -> usize {
    static STATE: AtomicU64 = AtomicU64::new(0);
    if n == 0 {
        return 0;
    }
    let mut x = STATE.load(Ordering::Relaxed);
    if x == 0 {
        x = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64
            | 1;
    }
    x ^= x << 13;
    x ^= x >> 7;
    x ^= x << 17;
    STATE.store(x, Ordering::Relaxed);
    (x % n as u64) as usize
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn lww_and_tombstones() {
        let a = Record { ts: 1, value: Some(json!({"a": 1})) };
        let b = Record { ts: 2, value: Some(json!({"a": 0})) };
        let t = Record { ts: 1, value: None };
        assert!(b.supersedes(&a));
        assert!(!a.supersedes(&b));
        assert!(t.supersedes(&a), "tombstone wins a tie");
        assert_eq!(newest([&Some(a.clone()), &None, &Some(b.clone())]), Some(b));
    }

    #[test]
    fn consistency_levels() {
        assert_eq!(Consistency::One.required(3), 1);
        assert_eq!(Consistency::Quorum.required(3), 2);
        assert_eq!(Consistency::Quorum.required(1), 1);
        assert_eq!(Consistency::All.required(3), 3);
    }
}
