//! Write-ahead commit log, split into segments.
//!
//! A new segment is started every time the memtable is frozen for flushing; once that
//! memtable is safely in an SSTable, the segment (and any older ones) is deleted.
//!
//! record := len u32 | crc32(payload) u32 | payload (JSON-encoded Mutation)
//!
//! On replay a short or checksum-failing record marks a torn write from a crash:
//! everything before it is recovered and the rest of that segment is ignored.

use super::sstable::{crc32, sync_dir};
use crate::model::Mutation;
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufWriter, Write};
use std::path::{Path, PathBuf};
use tracing::warn;

pub struct CommitLog {
    pub id: u64,
    w: BufWriter<File>,
    fsync: bool,
}

pub fn segment_path(dir: &Path, id: u64) -> PathBuf {
    dir.join(format!("commitlog-{id:010}.log"))
}

/// Ids of all commit log segments in `dir`, ascending.
pub fn list_segments(dir: &Path) -> io::Result<Vec<u64>> {
    let mut ids: Vec<u64> = fs::read_dir(dir)?
        .filter_map(|e| e.ok())
        .filter_map(|e| {
            let name = e.file_name().into_string().ok()?;
            name.strip_prefix("commitlog-")?.strip_suffix(".log")?.parse().ok()
        })
        .collect();
    ids.sort_unstable();
    Ok(ids)
}

impl CommitLog {
    pub fn create(dir: &Path, id: u64, fsync: bool) -> io::Result<Self> {
        let f = OpenOptions::new().create(true).append(true).open(segment_path(dir, id))?;
        sync_dir(dir);
        Ok(CommitLog { id, w: BufWriter::new(f), fsync })
    }

    /// Buffer one mutation; returns the bytes it occupies. Call `sync` to make it durable.
    pub fn append(&mut self, m: &Mutation) -> io::Result<usize> {
        let payload = serde_json::to_vec(m)?;
        self.w.write_all(&(payload.len() as u32).to_le_bytes())?;
        self.w.write_all(&crc32(&payload).to_le_bytes())?;
        self.w.write_all(&payload)?;
        Ok(payload.len() + 8)
    }

    pub fn sync(&mut self) -> io::Result<()> {
        self.w.flush()?;
        if self.fsync {
            self.w.get_ref().sync_data()?;
        }
        Ok(())
    }
}

pub fn replay(dir: &Path, id: u64) -> io::Result<Vec<Mutation>> {
    let path = segment_path(dir, id);
    let bytes = fs::read(&path)?;
    let mut out = Vec::new();
    let mut pos = 0usize;
    while pos < bytes.len() {
        let header = bytes.get(pos..pos + 8);
        let Some(h) = header else {
            warn!(segment = %path.display(), offset = pos, "torn record header; ignoring rest of segment");
            break;
        };
        let len = u32::from_le_bytes(h[..4].try_into().unwrap()) as usize;
        let crc = u32::from_le_bytes(h[4..].try_into().unwrap());
        let Some(payload) = bytes.get(pos + 8..pos + 8 + len) else {
            warn!(segment = %path.display(), offset = pos, "torn record; ignoring rest of segment");
            break;
        };
        if crc32(payload) != crc {
            warn!(segment = %path.display(), offset = pos, "checksum mismatch; ignoring rest of segment");
            break;
        }
        out.push(serde_json::from_slice(payload)?);
        pos += 8 + len;
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::Record;
    use serde_json::json;

    #[test]
    fn append_replay_and_torn_tail() {
        let dir = std::env::temp_dir().join(format!("cassa-cl-{}", crate::model::now_micros()));
        fs::create_dir_all(&dir).unwrap();
        {
            let mut log = CommitLog::create(&dir, 7, false).unwrap();
            for i in 0..10 {
                let m = Mutation { c: "c".into(), k: i.to_string(), r: Record { ts: i, value: Some(json!({"i": i})) } };
                log.append(&m).unwrap();
            }
            log.sync().unwrap();
        }
        assert_eq!(list_segments(&dir).unwrap(), vec![7]);
        assert_eq!(replay(&dir, 7).unwrap().len(), 10);

        // Simulate a crash mid-write: chop off the last few bytes.
        let p = segment_path(&dir, 7);
        let b = fs::read(&p).unwrap();
        fs::write(&p, &b[..b.len() - 3]).unwrap();
        let ms = replay(&dir, 7).unwrap();
        assert_eq!(ms.len(), 9);
        assert_eq!(ms[8].k, "8");
        fs::remove_dir_all(dir).unwrap();
    }
}
