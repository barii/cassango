//! Sorted String Tables: immutable, sorted, on-disk files of `(key, Record)` entries.
//!
//! File layout (all integers little-endian):
//!
//! ```text
//! +--------------------------------------------------------------+
//! | data block 0 | crc32 | data block 1 | crc32 | ...            |  ~4 KiB blocks of entries
//! +--------------------------------------------------------------+
//! | index section | crc32 |   per block: first key, offset, len  |
//! +--------------------------------------------------------------+
//! | bloom section | crc32 |   bloom filter over all keys         |
//! +--------------------------------------------------------------+
//! | footer (52 bytes)                                            |
//! |   index_off u64, index_len u32, bloom_off u64, bloom_len u32,|
//! |   entries u64, max_ts u64, version u32, magic u64            |
//! +--------------------------------------------------------------+
//!
//! entry := key_len u32 | key | ts u64 | val_len u32 (u32::MAX = tombstone) | JSON value
//! ```
//!
//! The sparse index and bloom filter are loaded into memory on open; data blocks are
//! read on demand with positional reads, so many readers can share one file handle.

use crate::model::Record;
use crate::ring::hash64;
use serde_json::Value;
use std::fs::{self, File};
use std::io::{self, BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

const MAGIC: u64 = 0x5453_5341_5353_4143; // "CASSASST" read as little-endian bytes
const VERSION: u32 = 1;
const BLOCK_SIZE: usize = 4096;
const BLOOM_BITS_PER_KEY: usize = 10;
const BLOOM_HASHES: u32 = 7;
const FOOTER_LEN: usize = 8 + 4 + 8 + 4 + 8 + 8 + 4 + 8;
const TOMBSTONE: u32 = u32::MAX;

pub type Entry = (Vec<u8>, Record);

pub fn corrupt(msg: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, msg.into())
}

// ------------------------------------------------------------------ crc32 (IEEE)

const CRC_TABLE: [u32; 256] = {
    let mut t = [0u32; 256];
    let mut i = 0;
    while i < 256 {
        let mut c = i as u32;
        let mut k = 0;
        while k < 8 {
            c = if c & 1 != 0 { 0xEDB8_8320 ^ (c >> 1) } else { c >> 1 };
            k += 1;
        }
        t[i] = c;
        i += 1;
    }
    t
};

pub fn crc32(data: &[u8]) -> u32 {
    let mut c = !0u32;
    for b in data {
        c = CRC_TABLE[((c ^ *b as u32) & 0xff) as usize] ^ (c >> 8);
    }
    !c
}

// ------------------------------------------------------------------ bloom filter

pub struct Bloom {
    bits: Vec<u64>,
    k: u32,
}

impl Bloom {
    pub fn new(expected_keys: usize) -> Self {
        let nbits = (expected_keys.max(1) * BLOOM_BITS_PER_KEY).max(64);
        Bloom { bits: vec![0; nbits.div_ceil(64)], k: BLOOM_HASHES }
    }

    /// Kirsch–Mitzenmacher double hashing: k probes derived from one 64-bit hash.
    fn probes(nbits: u64, k: u32, key: &[u8]) -> impl Iterator<Item = usize> {
        let h = hash64(key);
        let (h1, h2) = (h, h.rotate_left(32) | 1);
        (0..k as u64).map(move |i| (h1.wrapping_add(i.wrapping_mul(h2)) % nbits) as usize)
    }

    pub fn insert(&mut self, key: &[u8]) {
        for p in Self::probes(self.bits.len() as u64 * 64, self.k, key) {
            self.bits[p / 64] |= 1 << (p % 64);
        }
    }

    pub fn may_contain(&self, key: &[u8]) -> bool {
        Self::probes(self.bits.len() as u64 * 64, self.k, key).all(|p| self.bits[p / 64] & (1 << (p % 64)) != 0)
    }

    fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(4 + self.bits.len() * 8);
        out.extend(self.k.to_le_bytes());
        for w in &self.bits {
            out.extend(w.to_le_bytes());
        }
        out
    }

    fn decode(b: &[u8]) -> io::Result<Self> {
        let mut r = Cursor::new(b);
        let k = r.u32()?;
        let words = r.rest();
        if words.is_empty() || words.len() % 8 != 0 || k == 0 {
            return Err(corrupt("bad bloom filter"));
        }
        let bits = words.chunks_exact(8).map(|c| u64::from_le_bytes(c.try_into().unwrap())).collect();
        Ok(Bloom { bits, k })
    }
}

// ------------------------------------------------------------------ encoding helpers

struct Cursor<'a> {
    b: &'a [u8],
    pos: usize,
}

impl<'a> Cursor<'a> {
    fn new(b: &'a [u8]) -> Self {
        Cursor { b, pos: 0 }
    }
    fn take(&mut self, n: usize) -> io::Result<&'a [u8]> {
        let end = self.pos.checked_add(n).filter(|e| *e <= self.b.len()).ok_or_else(|| corrupt("truncated data"))?;
        let s = &self.b[self.pos..end];
        self.pos = end;
        Ok(s)
    }
    fn u32(&mut self) -> io::Result<u32> {
        Ok(u32::from_le_bytes(self.take(4)?.try_into().unwrap()))
    }
    fn u64(&mut self) -> io::Result<u64> {
        Ok(u64::from_le_bytes(self.take(8)?.try_into().unwrap()))
    }
    fn done(&self) -> bool {
        self.pos >= self.b.len()
    }
    fn rest(&mut self) -> &'a [u8] {
        let s = &self.b[self.pos..];
        self.pos = self.b.len();
        s
    }
}

fn encode_entry(buf: &mut Vec<u8>, key: &[u8], r: &Record) -> io::Result<()> {
    buf.extend((key.len() as u32).to_le_bytes());
    buf.extend(key);
    buf.extend(r.ts.to_le_bytes());
    match &r.value {
        None => buf.extend(TOMBSTONE.to_le_bytes()),
        Some(v) => {
            let bytes = serde_json::to_vec(v)?;
            if bytes.len() >= TOMBSTONE as usize {
                return Err(io::Error::new(io::ErrorKind::InvalidInput, "document too large"));
            }
            buf.extend((bytes.len() as u32).to_le_bytes());
            buf.extend(bytes);
        }
    }
    Ok(())
}

fn decode_block(b: &[u8]) -> io::Result<Vec<Entry>> {
    let mut r = Cursor::new(b);
    let mut out = Vec::new();
    while !r.done() {
        let klen = r.u32()? as usize;
        let key = r.take(klen)?.to_vec();
        let ts = r.u64()?;
        let vlen = r.u32()?;
        let value = if vlen == TOMBSTONE {
            None
        } else {
            Some(serde_json::from_slice::<Value>(r.take(vlen as usize)?)?)
        };
        out.push((key, Record { ts, value }));
    }
    Ok(out)
}

fn read_exact_at(file: &File, buf: &mut [u8], offset: u64) -> io::Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::FileExt;
        file.read_exact_at(buf, offset)
    }
    #[cfg(windows)]
    {
        use std::os::windows::fs::FileExt;
        let mut done = 0;
        while done < buf.len() {
            let n = file.seek_read(&mut buf[done..], offset + done as u64)?;
            if n == 0 {
                return Err(io::ErrorKind::UnexpectedEof.into());
            }
            done += n;
        }
        Ok(())
    }
}

/// fsync a directory so that renames/creates inside it are durable (no-op where unsupported).
pub fn sync_dir(dir: &Path) {
    if let Ok(d) = File::open(dir) {
        let _ = d.sync_all();
    }
}

// ------------------------------------------------------------------ writer

struct Writer {
    w: BufWriter<File>,
    off: u64,
    index: Vec<(Vec<u8>, u64, u32)>,
    block: Vec<u8>,
    block_first: Option<Vec<u8>>,
}

impl Writer {
    fn finish_block(&mut self) -> io::Result<()> {
        if self.block.is_empty() {
            return Ok(());
        }
        let mut block = std::mem::take(&mut self.block);
        let (off, len) = self.section(&block)?;
        self.index.push((self.block_first.take().unwrap(), off, len));
        block.clear();
        self.block = block; // reuse the allocation
        Ok(())
    }

    /// Write `data` followed by its crc32; returns (offset, len excluding crc).
    fn section(&mut self, data: &[u8]) -> io::Result<(u64, u32)> {
        let off = self.off;
        self.w.write_all(data)?;
        self.w.write_all(&crc32(data).to_le_bytes())?;
        self.off += data.len() as u64 + 4;
        Ok((off, data.len() as u32))
    }
}

/// Write a new SSTable at `path` from strictly ascending, de-duplicated entries.
/// The file is written to a temp name, fsynced, then atomically renamed into place.
/// Returns the number of entries written.
pub fn write(path: &Path, expected_keys: usize, entries: impl Iterator<Item = io::Result<Entry>>) -> io::Result<u64> {
    let tmp = path.with_extension("db.tmp");
    let mut w = Writer {
        w: BufWriter::new(File::create(&tmp)?),
        off: 0,
        index: Vec::new(),
        block: Vec::with_capacity(BLOCK_SIZE * 2),
        block_first: None,
    };
    let mut bloom = Bloom::new(expected_keys);
    let (mut count, mut max_ts) = (0u64, 0u64);
    let mut last: Option<Vec<u8>> = None;

    for e in entries {
        let (key, rec) = e?;
        if last.as_ref().is_some_and(|l| *l >= key) {
            return Err(io::Error::new(io::ErrorKind::InvalidInput, "sstable keys must be strictly ascending"));
        }
        bloom.insert(&key);
        max_ts = max_ts.max(rec.ts);
        if w.block_first.is_none() {
            w.block_first = Some(key.clone());
        }
        encode_entry(&mut w.block, &key, &rec)?;
        count += 1;
        if w.block.len() >= BLOCK_SIZE {
            w.finish_block()?;
        }
        last = Some(key);
    }
    w.finish_block()?;

    let mut idx = Vec::new();
    idx.extend((w.index.len() as u32).to_le_bytes());
    for (k, off, len) in &w.index {
        idx.extend((k.len() as u32).to_le_bytes());
        idx.extend(k);
        idx.extend(off.to_le_bytes());
        idx.extend(len.to_le_bytes());
    }
    let (index_off, index_len) = w.section(&idx)?;
    let (bloom_off, bloom_len) = w.section(&bloom.encode())?;

    let mut footer = Vec::with_capacity(FOOTER_LEN);
    footer.extend(index_off.to_le_bytes());
    footer.extend(index_len.to_le_bytes());
    footer.extend(bloom_off.to_le_bytes());
    footer.extend(bloom_len.to_le_bytes());
    footer.extend(count.to_le_bytes());
    footer.extend(max_ts.to_le_bytes());
    footer.extend(VERSION.to_le_bytes());
    footer.extend(MAGIC.to_le_bytes());
    w.w.write_all(&footer)?;
    w.w.flush()?;
    w.w.get_ref().sync_all()?;
    drop(w);

    fs::rename(&tmp, path)?;
    if let Some(dir) = path.parent() {
        sync_dir(dir);
    }
    Ok(count)
}

// ------------------------------------------------------------------ reader

pub struct SsTable {
    pub generation: u64,
    pub path: PathBuf,
    file: File,
    /// (first key of block, offset, length excluding crc)
    index: Vec<(Vec<u8>, u64, u32)>,
    bloom: Bloom,
    pub entries: u64,
    pub max_ts: u64,
    pub size: u64,
    /// Set when compacted away; the file is deleted once the last reader drops it.
    obsolete: AtomicBool,
}

impl SsTable {
    pub fn open(path: &Path, generation: u64) -> io::Result<Self> {
        let file = File::open(path)?;
        let size = file.metadata()?.len();
        if size < FOOTER_LEN as u64 {
            return Err(corrupt(format!("{}: too small to be an sstable", path.display())));
        }
        let mut footer = [0u8; FOOTER_LEN];
        read_exact_at(&file, &mut footer, size - FOOTER_LEN as u64)?;
        let mut f = Cursor::new(&footer);
        let (index_off, index_len) = (f.u64()?, f.u32()?);
        let (bloom_off, bloom_len) = (f.u64()?, f.u32()?);
        let (entries, max_ts, version, magic) = (f.u64()?, f.u64()?, f.u32()?, f.u64()?);
        if magic != MAGIC {
            return Err(corrupt(format!("{}: bad magic", path.display())));
        }
        if version != VERSION {
            return Err(corrupt(format!("{}: unsupported sstable version {version}", path.display())));
        }

        let idx = read_section(&file, index_off, index_len)?;
        let mut r = Cursor::new(&idx);
        let n = r.u32()? as usize;
        let mut index = Vec::with_capacity(n);
        for _ in 0..n {
            let klen = r.u32()? as usize;
            let key = r.take(klen)?.to_vec();
            index.push((key, r.u64()?, r.u32()?));
        }
        let bloom = Bloom::decode(&read_section(&file, bloom_off, bloom_len)?)?;

        Ok(SsTable {
            generation,
            path: path.to_path_buf(),
            file,
            index,
            bloom,
            entries,
            max_ts,
            size,
            obsolete: AtomicBool::new(false),
        })
    }

    pub fn may_contain(&self, key: &[u8]) -> bool {
        self.bloom.may_contain(key)
    }

    /// Index of the block that could contain `key` (the last block whose first key <= key).
    fn block_for(&self, key: &[u8]) -> Option<usize> {
        self.index.partition_point(|(first, _, _)| first.as_slice() <= key).checked_sub(1)
    }

    fn read_block(&self, i: usize) -> io::Result<Vec<Entry>> {
        let (_, off, len) = self.index[i];
        decode_block(&read_section(&self.file, off, len).map_err(|e| {
            corrupt(format!("{} block {i}: {e}", self.path.display()))
        })?)
    }

    pub fn get(&self, key: &[u8]) -> io::Result<Option<Record>> {
        if !self.bloom.may_contain(key) {
            return Ok(None);
        }
        let Some(b) = self.block_for(key) else { return Ok(None) };
        let block = self.read_block(b)?;
        Ok(block
            .binary_search_by(|(k, _)| k.as_slice().cmp(key))
            .ok()
            .map(|i| block[i].1.clone()))
    }

    /// Iterate entries with key >= `start`, in order.
    pub fn iter_from(self: &Arc<Self>, start: &[u8]) -> SsIter {
        SsIter {
            next_block: self.block_for(start).unwrap_or(0),
            table: self.clone(),
            cur: Vec::new().into_iter(),
            skip_below: Some(start.to_vec()),
            failed: false,
        }
    }

    pub fn mark_obsolete(&self) {
        self.obsolete.store(true, Ordering::SeqCst);
    }
}

impl Drop for SsTable {
    fn drop(&mut self) {
        if self.obsolete.load(Ordering::SeqCst) {
            let _ = fs::remove_file(&self.path);
        }
    }
}

fn read_section(file: &File, off: u64, len: u32) -> io::Result<Vec<u8>> {
    let mut buf = vec![0u8; len as usize + 4];
    read_exact_at(file, &mut buf, off)?;
    let stored = u32::from_le_bytes(buf[len as usize..].try_into().unwrap());
    buf.truncate(len as usize);
    if crc32(&buf) != stored {
        return Err(corrupt("checksum mismatch"));
    }
    Ok(buf)
}

pub struct SsIter {
    table: Arc<SsTable>,
    next_block: usize,
    cur: std::vec::IntoIter<Entry>,
    skip_below: Option<Vec<u8>>,
    failed: bool,
}

impl Iterator for SsIter {
    type Item = io::Result<Entry>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if self.failed {
                return None;
            }
            if let Some(e) = self.cur.next() {
                if let Some(s) = &self.skip_below {
                    if e.0 < *s {
                        continue;
                    }
                    self.skip_below = None;
                }
                return Some(Ok(e));
            }
            if self.next_block >= self.table.index.len() {
                return None;
            }
            match self.table.read_block(self.next_block) {
                Ok(entries) => {
                    self.cur = entries.into_iter();
                    self.next_block += 1;
                }
                Err(e) => {
                    self.failed = true;
                    return Some(Err(e));
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    pub fn tmpdir(name: &str) -> PathBuf {
        let d = std::env::temp_dir().join(format!("cassa-sst-{name}-{}", crate::model::now_micros()));
        fs::create_dir_all(&d).unwrap();
        d
    }

    fn entries(n: usize) -> Vec<Entry> {
        (0..n)
            .map(|i| {
                let v = if i % 7 == 0 { None } else { Some(json!({"i": i, "pad": "x".repeat(i % 50)})) };
                (format!("users\0{i:06}").into_bytes(), Record { ts: 1000 + i as u64, value: v })
            })
            .collect()
    }

    #[test]
    fn crc_known_value() {
        assert_eq!(crc32(b"123456789"), 0xCBF4_3926);
    }

    #[test]
    fn roundtrip_get_and_iter() {
        let dir = tmpdir("rt");
        let path = dir.join("sst-1.db");
        let data = entries(5000);
        assert_eq!(write(&path, data.len(), data.clone().into_iter().map(Ok)).unwrap(), 5000);
        let t = Arc::new(SsTable::open(&path, 1).unwrap());
        assert_eq!(t.entries, 5000);
        assert_eq!(t.max_ts, 1000 + 4999);
        assert!(t.index.len() > 10, "multiple blocks");
        for (k, r) in data.iter().step_by(37) {
            assert_eq!(t.get(k).unwrap().as_ref(), Some(r));
        }
        assert_eq!(t.get(b"users\0zzz").unwrap(), None);
        assert_eq!(t.get(b"aaa").unwrap(), None);

        let all: Vec<Entry> = t.iter_from(b"").map(Result::unwrap).collect();
        assert_eq!(all, data);
        let tail: Vec<Entry> = t.iter_from(b"users\0004990").map(Result::unwrap).collect();
        assert_eq!(tail, data[4990..]);
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn bloom_false_positive_rate() {
        let mut b = Bloom::new(10_000);
        for i in 0..10_000 {
            b.insert(format!("k{i}").as_bytes());
        }
        assert!((0..10_000).all(|i| b.may_contain(format!("k{i}").as_bytes())));
        let fp = (0..10_000).filter(|i| b.may_contain(format!("x{i}").as_bytes())).count();
        assert!(fp < 300, "false positive rate too high: {fp}/10000");
    }

    #[test]
    fn detects_corruption() {
        let dir = tmpdir("corrupt");
        let path = dir.join("sst-1.db");
        write(&path, 100, entries(100).into_iter().map(Ok)).unwrap();
        let mut bytes = fs::read(&path).unwrap();
        bytes[10] ^= 0xff; // flip a bit inside the first data block
        fs::write(&path, &bytes).unwrap();
        let t = Arc::new(SsTable::open(&path, 1).unwrap());
        assert!(t.iter_from(b"").any(|r| r.is_err()));
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn rejects_unsorted_input() {
        let dir = tmpdir("unsorted");
        let mut data = entries(3);
        data.swap(0, 1);
        assert!(write(&dir.join("sst-1.db"), 3, data.into_iter().map(Ok)).is_err());
        fs::remove_dir_all(dir).unwrap();
    }
}
