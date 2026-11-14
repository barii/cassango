//! Consistent-hash token ring with virtual nodes (Cassandra-style partitioning).

use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};

/// A token range `(start, end]`, possibly wrapping around the end of the ring.
/// `start == end` means the whole ring.
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct TokenRange {
    pub start: u64,
    pub end: u64,
}

impl TokenRange {
    fn span(&self) -> u128 {
        match self.end.wrapping_sub(self.start) {
            0 => 1u128 << 64,
            s => s as u128,
        }
    }

    /// Position of `token` within the range, 0-based; only meaningful if contained.
    fn offset(&self, token: u64) -> u128 {
        token.wrapping_sub(self.start).wrapping_sub(1) as u128
    }

    pub fn contains(&self, token: u64) -> bool {
        self.offset(token) < self.span()
    }

    /// Which of `leaves` equal-width sub-ranges `token` falls into.
    pub fn leaf(&self, token: u64, leaves: usize) -> usize {
        (self.offset(token) * leaves as u128 / self.span()) as usize
    }
}

/// 64-bit FNV-1a followed by a splitmix64 finalizer for good avalanche.
/// Must be stable across processes and versions, so we don't use std's hasher.
pub fn hash64(data: &[u8]) -> u64 {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for b in data {
        h ^= *b as u64;
        h = h.wrapping_mul(0x0000_0100_0000_01b3);
    }
    let mut z = h;
    z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    z ^ (z >> 31)
}

/// The partition token for a document.
pub fn key_token(collection: &str, id: &str) -> u64 {
    hash64(format!("{collection}\0{id}").as_bytes())
}

/// Deterministic vnode tokens for a node.
pub fn tokens_for(node_id: &str, vnodes: usize) -> Vec<u64> {
    let mut t: Vec<u64> = (0..vnodes.max(1))
        .map(|i| hash64(format!("{node_id}#{i}").as_bytes()))
        .collect();
    t.sort_unstable();
    t
}

#[derive(Debug, Clone, Default)]
pub struct Ring {
    /// Sorted by token. Each token is owned by one node.
    tokens: Vec<(u64, String)>,
}

impl Ring {
    pub fn new(entries: impl IntoIterator<Item = (String, Vec<u64>)>) -> Self {
        let mut tokens: Vec<(u64, String)> = entries
            .into_iter()
            .flat_map(|(node, toks)| toks.into_iter().map(move |t| (t, node.clone())))
            .collect();
        tokens.sort();
        Ring { tokens }
    }

    /// Walk clockwise from `token`, collecting the first `rf` distinct nodes.
    /// The first node is the primary owner; the rest are replicas.
    pub fn replicas(&self, token: u64, rf: usize) -> Vec<String> {
        let mut out: Vec<String> = Vec::with_capacity(rf);
        if self.tokens.is_empty() {
            return out;
        }
        let start = self.tokens.partition_point(|(t, _)| *t < token);
        let len = self.tokens.len();
        for i in 0..len {
            let node = &self.tokens[(start + i) % len].1;
            if !out.contains(node) {
                out.push(node.clone());
                if out.len() == rf {
                    break;
                }
            }
        }
        out
    }

    /// Ranges for which `node` is the primary owner: `(previous token, node's token]`.
    pub fn primary_ranges(&self, node: &str) -> Vec<TokenRange> {
        let len = self.tokens.len();
        (0..len)
            .filter(|i| self.tokens[*i].1 == node)
            .map(|i| TokenRange { start: self.tokens[(i + len - 1) % len].0, end: self.tokens[i].0 })
            .collect()
    }

    /// Fraction of the token space for which each node is the primary owner.
    pub fn ownership(&self) -> BTreeMap<String, f64> {
        let mut own: HashMap<&str, f64> = HashMap::new();
        let len = self.tokens.len();
        if len == 1 {
            own.insert(&self.tokens[0].1, 1.0);
        } else {
            for i in 0..len {
                let prev = self.tokens[(i + len - 1) % len].0;
                let (tok, node) = &self.tokens[i];
                // Range (prev, tok] is owned by `node`.
                let size = tok.wrapping_sub(prev) as f64 / 2f64.powi(64);
                *own.entry(node).or_default() += size;
            }
        }
        own.into_iter().map(|(k, v)| (k.to_string(), v)).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ring(n: usize) -> Ring {
        Ring::new((0..n).map(|i| {
            let id = format!("node{i}");
            let t = tokens_for(&id, 16);
            (id, t)
        }))
    }

    #[test]
    fn replicas_are_distinct_and_bounded() {
        let r = ring(5);
        for k in 0..100 {
            let reps = r.replicas(key_token("c", &k.to_string()), 3);
            assert_eq!(reps.len(), 3);
            let mut d = reps.clone();
            d.sort();
            d.dedup();
            assert_eq!(d.len(), 3);
        }
        assert_eq!(ring(2).replicas(42, 3).len(), 2, "rf capped by node count");
    }

    #[test]
    fn ownership_sums_to_one_and_is_balanced() {
        let own = ring(4).ownership();
        let total: f64 = own.values().sum();
        assert!((total - 1.0).abs() < 1e-9);
        for v in own.values() {
            assert!(*v > 0.1 && *v < 0.45, "unbalanced: {own:?}");
        }
    }

    #[test]
    fn primary_ranges_partition_the_ring() {
        let r = ring(3);
        let ranges: Vec<(String, TokenRange)> = (0..3)
            .flat_map(|i| r.primary_ranges(&format!("node{i}")).into_iter().map(move |t| (format!("node{i}"), t)))
            .collect();
        assert_eq!(ranges.len(), 48);
        for k in 0..500 {
            let t = key_token("c", &k.to_string());
            let owners: Vec<&String> = ranges.iter().filter(|(_, rg)| rg.contains(t)).map(|(n, _)| n).collect();
            assert_eq!(owners.len(), 1, "each token in exactly one range");
            assert_eq!(*owners[0], r.replicas(t, 1)[0], "range owner is the primary replica");
        }
    }

    #[test]
    fn token_range_wraps_and_leaves() {
        let wrap = TokenRange { start: u64::MAX - 10, end: 10 };
        assert!(wrap.contains(u64::MAX) && wrap.contains(0) && wrap.contains(10));
        assert!(!wrap.contains(11) && !wrap.contains(u64::MAX - 10));
        assert_eq!(wrap.leaf(u64::MAX - 9, 4), 0);
        assert_eq!(wrap.leaf(10, 4), 3);
        let full = TokenRange { start: 5, end: 5 };
        assert!(full.contains(5) && full.contains(0));
        assert!(full.leaf(4, 1024) < 1024);
    }

    #[test]
    fn deterministic() {
        assert_eq!(hash64(b"hello"), hash64(b"hello"));
        assert_eq!(tokens_for("a", 4), tokens_for("a", 4));
    }
}
