//! Consistent-hash token ring with virtual nodes (Cassandra-style partitioning).

use std::collections::{BTreeMap, HashMap};

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
    fn deterministic() {
        assert_eq!(hash64(b"hello"), hash64(b"hello"));
        assert_eq!(tokens_for("a", 4), tokens_for("a", 4));
    }
}
