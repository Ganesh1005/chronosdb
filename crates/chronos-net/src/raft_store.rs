//! Production [`PersistentStore`] for Raft: the log lives in a
//! [`chronos_storage::Wal`], while `(term, voted_for, base)` lives in a small
//! atomic meta file.
//!
//! # Save strategy
//!
//! [`RaftStore::save`] receives the full [`PersistedState`] and diffs it
//! against the last durable state:
//!
//! * log prefix compaction → [`Wal::compact_prefix`],
//! * conflicting suffix → [`Wal::truncate_from`] + re-append,
//! * new tail entries → [`Wal::append`],
//!
//! then fsyncs the WAL and atomically replaces the meta file. Only after both
//! are durable is the save considered complete, so a crash can never leave
//! the node believing it persisted something it didn't.
//!
//! Sequence mapping: Raft indices are 1-based, WAL seqs are 0-based, so
//! `wal_seq = raft_index - 1` and `wal.base_seq() == raft base_index`.

use std::fs::{self, File};
use std::io::Write;
use std::path::{Path, PathBuf};

use chronos_raft::{Index, LogEntry, NodeId, PersistedState, PersistentStore, Term};
use chronos_storage::Wal;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum StoreError {
    #[error("storage: {0}")]
    Storage(#[from] chronos_storage::StorageError),
    #[error("codec: {0}")]
    Codec(String),
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("log inconsistency: {0}")]
    Inconsistent(String),
}

const META_NAME: &str = "raft.meta";
const META_MAGIC: u32 = 0x5241_4654; // "RAFT"
/// magic(4) + term(8) + voted_for_present(1) + voted_for(8) + base_index(8)
/// + base_term(8) + crc32c(4) = 41 bytes.
const META_LEN: usize = 41;

fn fsync_dir(dir: &Path) -> Result<(), StoreError> {
    File::open(dir)?.sync_all()?;
    Ok(())
}

fn encode_meta(term: Term, voted_for: Option<NodeId>, base_index: Index, base_term: Term) -> Vec<u8> {
    let mut buf = Vec::with_capacity(META_LEN);
    buf.extend_from_slice(&META_MAGIC.to_le_bytes());
    buf.extend_from_slice(&term.to_le_bytes());
    match voted_for {
        Some(id) => {
            buf.push(1);
            buf.extend_from_slice(&id.to_le_bytes());
        }
        None => {
            buf.push(0);
            buf.extend_from_slice(&0u64.to_le_bytes());
        }
    }
    buf.extend_from_slice(&base_index.to_le_bytes());
    buf.extend_from_slice(&base_term.to_le_bytes());
    let crc = crc32c(&buf);
    buf.extend_from_slice(&crc.to_le_bytes());
    buf
}

// Small local crc32c to avoid adding a dependency here (chronos-storage
// already depends on the crate, but re-exporting is cleaner avoided).
fn crc32c(data: &[u8]) -> u32 {
    // Castagnoli polynomial, software fallback (matches the `crc32c` crate).
    const POLY: u32 = 0x82F63B78;
    let mut crc = 0xFFFF_FFFFu32;
    for &b in data {
        crc ^= b as u32;
        for _ in 0..8 {
            let mask = (crc & 1).wrapping_neg();
            crc = (crc >> 1) ^ (POLY & mask);
        }
    }
    !crc
}

fn decode_meta(buf: &[u8]) -> Result<(Term, Option<NodeId>, Index, Term), StoreError> {
    if buf.len() != META_LEN {
        return Err(StoreError::Inconsistent(format!(
            "meta length {} != {META_LEN}",
            buf.len()
        )));
    }
    let magic = u32::from_le_bytes(buf[0..4].try_into().unwrap());
    let crc = u32::from_le_bytes(buf[37..41].try_into().unwrap());
    if magic != META_MAGIC || crc32c(&buf[..37]) != crc {
        return Err(StoreError::Inconsistent("meta checksum/magic mismatch".into()));
    }
    let term = u64::from_le_bytes(buf[4..12].try_into().unwrap());
    let voted_for = match buf[12] {
        1 => Some(u64::from_le_bytes(buf[13..21].try_into().unwrap())),
        0 => None,
        _ => return Err(StoreError::Inconsistent("bad voted_for flag".into())),
    };
    let base_index = u64::from_le_bytes(buf[21..29].try_into().unwrap());
    let base_term = u64::from_le_bytes(buf[29..37].try_into().unwrap());
    Ok((term, voted_for, base_index, base_term))
}

pub struct RaftStore {
    dir: PathBuf,
    wal: Wal,
    last_saved: Option<PersistedState>,
}

impl RaftStore {
    pub fn open(dir: &Path) -> Result<Self, StoreError> {
        fs::create_dir_all(dir)?;
        let wal = Wal::open(&dir.join("raftlog"), 64 * 1024 * 1024)?;
        Ok(RaftStore { dir: dir.to_path_buf(), wal, last_saved: None })
    }

    fn write_meta(
        &self,
        term: Term,
        voted_for: Option<NodeId>,
        base_index: Index,
        base_term: Term,
    ) -> Result<(), StoreError> {
        let tmp = self.dir.join("raft.meta.tmp");
        {
            let mut f = File::create(&tmp)?;
            f.write_all(&encode_meta(term, voted_for, base_index, base_term))?;
            f.sync_all()?;
        }
        fs::rename(&tmp, self.dir.join(META_NAME))?;
        fsync_dir(&self.dir)?;
        Ok(())
    }

    fn read_meta(&self) -> Result<Option<(Term, Option<NodeId>, Index, Term)>, StoreError> {
        let path = self.dir.join(META_NAME);
        if !path.exists() {
            return Ok(None);
        }
        Ok(Some(decode_meta(&fs::read(&path)?)?))
    }

    fn ser_entry(e: &LogEntry) -> Result<Vec<u8>, StoreError> {
        bincode::serialize(e).map_err(|e| StoreError::Codec(e.to_string()))
    }

    fn de_entry(bytes: &[u8]) -> Result<LogEntry, StoreError> {
        bincode::deserialize(bytes).map_err(|e| StoreError::Codec(e.to_string()))
    }

    /// Verify the WAL content matches `s.log` (used in tests).
    #[cfg(test)]
    fn verify_against(&self, s: &PersistedState) -> Result<(), StoreError> {
        if self.wal.base_seq() != s.base_index {
            return Err(StoreError::Inconsistent(format!(
                "wal base {} != raft base {}",
                self.wal.base_seq(),
                s.base_index
            )));
        }
        let n = (self.wal.len() - self.wal.base_seq()) as usize;
        if n != s.log.len() {
            return Err(StoreError::Inconsistent(format!(
                "wal has {n} entries, state has {}",
                s.log.len()
            )));
        }
        for (i, e) in s.log.iter().enumerate() {
            let bytes = self.wal.get(s.base_index + i as u64)?;
            let back = Self::de_entry(&bytes)?;
            if &back != e {
                return Err(StoreError::Inconsistent(format!("entry mismatch at {i}")));
            }
            if back.index != s.base_index + 1 + i as u64 {
                return Err(StoreError::Inconsistent(format!("index mismatch at {i}")));
            }
        }
        Ok(())
    }
}

impl PersistentStore for RaftStore {
    type Error = StoreError;

    fn save(&mut self, s: &PersistedState) -> Result<(), StoreError> {
        let last = self.last_saved.clone().unwrap_or(PersistedState {
            current_term: 0,
            voted_for: None,
            base_index: 0,
            base_term: 0,
            log: Vec::new(),
        });

        // 1. Log prefix compaction.
        if s.base_index != last.base_index {
            if s.base_index < last.base_index {
                return Err(StoreError::Inconsistent("base_index moved backward".into()));
            }
            self.wal.compact_prefix(s.base_index)?;
        }

        // 2. Diff the overlapping region (in Raft indices).
        let last_end = last.base_index + last.log.len() as u64;
        let new_end = s.base_index + s.log.len() as u64;
        let overlap_end = last_end.min(new_end);
        let mut first_diff: Option<u64> = None;
        let mut idx = s.base_index + 1;
        while idx <= overlap_end {
            let old_e = &last.log[(idx - last.base_index - 1) as usize];
            let new_e = &s.log[(idx - s.base_index - 1) as usize];
            if old_e != new_e {
                first_diff = Some(idx);
                break;
            }
            idx += 1;
        }

        match first_diff {
            Some(idx) => {
                // Conflicting suffix: truncate the WAL from this Raft index
                // (wal seq = raft index - 1) and re-append the new suffix.
                self.wal.truncate_from(idx - 1)?;
                for e in &s.log[(idx - s.base_index - 1) as usize..] {
                    self.wal.append(&Self::ser_entry(e)?)?;
                }
            }
            None => {
                if new_end < last_end {
                    // New log is a strict prefix: drop the tail.
                    self.wal.truncate_from(new_end)?;
                }
                // Append whatever is new beyond the current WAL length.
                let wal_len = self.wal.len();
                let start = wal_len.saturating_sub(s.base_index) as usize;
                let start = start.min(s.log.len());
                for e in &s.log[start..] {
                    self.wal.append(&Self::ser_entry(e)?)?;
                }
            }
        }

        // 3. Durability: WAL fsync first, then the meta file. A crash between
        // them leaves the old meta pointing at a WAL prefix that is still
        // valid (the new tail is simply unreferenced).
        self.wal.sync()?;
        self.write_meta(s.current_term, s.voted_for, s.base_index, s.base_term)?;
        self.last_saved = Some(s.clone());
        Ok(())
    }

    fn load(&mut self) -> Result<Option<PersistedState>, StoreError> {
        let Some((term, voted_for, base_index, base_term)) = self.read_meta()? else {
            return Ok(None);
        };
        if self.wal.base_seq() != base_index {
            return Err(StoreError::Inconsistent(format!(
                "wal base_seq {} != meta base_index {base_index}",
                self.wal.base_seq()
            )));
        }
        let mut log = Vec::new();
        let mut seq = self.wal.base_seq();
        while seq < self.wal.len() {
            let e = Self::de_entry(&self.wal.get(seq)?)?;
            if e.index != seq + 1 {
                return Err(StoreError::Inconsistent(format!(
                    "entry index {} != wal seq {seq} + 1",
                    e.index
                )));
            }
            log.push(e);
            seq += 1;
        }
        // Contiguity check.
        for (i, e) in log.iter().enumerate() {
            if e.index != base_index + 1 + i as u64 {
                return Err(StoreError::Inconsistent("non-contiguous log".into()));
            }
        }
        let state = PersistedState { current_term: term, voted_for, base_index, base_term, log };
        self.last_saved = Some(state.clone());
        Ok(Some(state))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chronos_raft::LogEntry;
    use tempfile::TempDir;

    fn entry(term: u64, index: u64) -> LogEntry {
        LogEntry { term, index, payload: format!("e{index}").into_bytes() }
    }

    fn state(term: u64, base: u64, base_term: u64, log: Vec<LogEntry>) -> PersistedState {
        PersistedState { current_term: term, voted_for: None, base_index: base, base_term: base_term, log }
    }

    #[test]
    fn save_load_roundtrip() {
        let dir = TempDir::new().unwrap();
        let mut store = RaftStore::open(dir.path()).unwrap();
        assert!(store.load().unwrap().is_none());
        let s = state(3, 0, 0, vec![entry(1, 1), entry(1, 2), entry(3, 3)]);
        store.save(&s).unwrap();
        store.verify_against(&s).unwrap();
        // Reopen: full recovery.
        let mut store2 = RaftStore::open(dir.path()).unwrap();
        let loaded = store2.load().unwrap().unwrap();
        assert_eq!(loaded, s);
    }

    #[test]
    fn append_is_incremental() {
        let dir = TempDir::new().unwrap();
        let mut store = RaftStore::open(dir.path()).unwrap();
        let mut s = state(1, 0, 0, vec![]);
        for i in 1..=20u64 {
            s.log.push(entry(1, i));
            s.current_term = 1;
            store.save(&s).unwrap();
        }
        store.verify_against(&s).unwrap();
        // Each save appended exactly one record: 20 records total.
        assert_eq!(store.wal.len(), 20);
    }

    #[test]
    fn conflicting_suffix_truncates() {
        let dir = TempDir::new().unwrap();
        let mut store = RaftStore::open(dir.path()).unwrap();
        let s1 = state(2, 0, 0, vec![entry(1, 1), entry(2, 2), entry(2, 3)]);
        store.save(&s1).unwrap();
        // New leader overwrites from index 2.
        let s2 = state(3, 0, 0, vec![entry(1, 1), entry(3, 2)]);
        store.save(&s2).unwrap();
        store.verify_against(&s2).unwrap();
        let mut store2 = RaftStore::open(dir.path()).unwrap();
        assert_eq!(store2.load().unwrap().unwrap(), s2);
    }

    #[test]
    fn compaction_then_recovery() {
        let dir = TempDir::new().unwrap();
        let mut store = RaftStore::open(dir.path()).unwrap();
        let s1 = state(2, 0, 0, (1..=10).map(|i| entry(2, i)).collect());
        store.save(&s1).unwrap();
        let s2 = state(2, 7, 2, (8..=10).map(|i| entry(2, i)).collect());
        store.save(&s2).unwrap();
        store.verify_against(&s2).unwrap();
        let mut store2 = RaftStore::open(dir.path()).unwrap();
        let loaded = store2.load().unwrap().unwrap();
        assert_eq!(loaded.base_index, 7);
        assert_eq!(loaded.log.len(), 3);
        assert_eq!(loaded.log[0].index, 8);
    }

    #[test]
    fn vote_persists_without_log_change() {
        let dir = TempDir::new().unwrap();
        let mut store = RaftStore::open(dir.path()).unwrap();
        let s1 = state(4, 0, 0, vec![entry(4, 1)]);
        store.save(&s1).unwrap();
        let mut s2 = s1.clone();
        s2.voted_for = Some(99);
        s2.current_term = 5;
        store.save(&s2).unwrap();
        // Log untouched: still exactly 1 record.
        assert_eq!(store.wal.len(), 1);
        let mut store2 = RaftStore::open(dir.path()).unwrap();
        let loaded = store2.load().unwrap().unwrap();
        assert_eq!(loaded.voted_for, Some(99));
        assert_eq!(loaded.current_term, 5);
    }
}
