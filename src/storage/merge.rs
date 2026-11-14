//! K-way merge of sorted entry streams with last-write-wins reconciliation.
//!
//! Each source yields strictly ascending keys. When several sources hold the same key,
//! the merge emits a single entry with the record that `supersedes` the others.

use super::sstable::Entry;
use crate::model::Record;
use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::io;

pub type Source = Box<dyn Iterator<Item = io::Result<Entry>> + Send>;

struct Head {
    key: Vec<u8>,
    rec: Record,
    src: usize,
}

impl PartialEq for Head {
    fn eq(&self, o: &Self) -> bool {
        self.key == o.key && self.src == o.src
    }
}
impl Eq for Head {}
impl PartialOrd for Head {
    fn partial_cmp(&self, o: &Self) -> Option<Ordering> {
        Some(self.cmp(o))
    }
}
impl Ord for Head {
    // BinaryHeap is a max-heap; reverse so the smallest key pops first.
    fn cmp(&self, o: &Self) -> Ordering {
        o.key.cmp(&self.key).then(o.src.cmp(&self.src))
    }
}

pub struct Merge {
    heap: BinaryHeap<Head>,
    sources: Vec<Source>,
    /// Exclusive upper bound on keys.
    end: Option<Vec<u8>>,
    done: bool,
}

impl Merge {
    pub fn new(sources: Vec<Source>, end: Option<Vec<u8>>) -> io::Result<Self> {
        let mut m = Merge { heap: BinaryHeap::with_capacity(sources.len()), sources, end, done: false };
        for i in 0..m.sources.len() {
            m.advance(i)?;
        }
        Ok(m)
    }

    fn advance(&mut self, src: usize) -> io::Result<()> {
        if let Some(next) = self.sources[src].next() {
            let (key, rec) = next?;
            self.heap.push(Head { key, rec, src });
        }
        Ok(())
    }

    fn step(&mut self) -> io::Result<Option<Entry>> {
        let Some(top) = self.heap.pop() else { return Ok(None) };
        if self.end.as_ref().is_some_and(|e| top.key >= *e) {
            return Ok(None);
        }
        self.advance(top.src)?;
        let mut best = top.rec;
        while self.heap.peek().is_some_and(|h| h.key == top.key) {
            let h = self.heap.pop().unwrap();
            self.advance(h.src)?;
            if h.rec.supersedes(&best) {
                best = h.rec;
            }
        }
        Ok(Some((top.key, best)))
    }
}

impl Iterator for Merge {
    type Item = io::Result<Entry>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.done {
            return None;
        }
        match self.step() {
            Ok(Some(e)) => Some(Ok(e)),
            Ok(None) => {
                self.done = true;
                None
            }
            Err(e) => {
                self.done = true;
                Some(Err(e))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn src(items: &[(&str, u64)]) -> Source {
        let v: Vec<io::Result<Entry>> = items
            .iter()
            .map(|(k, ts)| Ok((k.as_bytes().to_vec(), Record { ts: *ts, value: Some(serde_json::json!(ts)) })))
            .collect();
        Box::new(v.into_iter())
    }

    #[test]
    fn merges_and_reconciles() {
        let m = Merge::new(vec![src(&[("a", 1), ("c", 5)]), src(&[("b", 2), ("c", 9), ("d", 1)]), src(&[("c", 3)])], Some(b"d".to_vec()))
            .unwrap();
        let out: Vec<(String, u64)> = m.map(|e| e.unwrap()).map(|(k, r)| (String::from_utf8(k).unwrap(), r.ts)).collect();
        assert_eq!(out, vec![("a".into(), 1), ("b".into(), 2), ("c".into(), 9)]);
    }
}
