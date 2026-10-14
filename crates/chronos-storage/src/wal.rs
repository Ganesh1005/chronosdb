//! Append-only write-ahead log.
//!
//! # Record format (little-endian)
//!
//! ```text
//! +--------+--------+--------+-------------+
//! | magic  |  len   | crc32c |   payload   |
//! | u32    |  u32   |  u32   | len bytes   |
//! +--------+--------+--------+-------------+
//! ```
//!
//! `crc32c` covers the payload only. `len` is bounded (16 MiB) so a corrupt
//! length field cannot cause huge allocations during recovery.
//!
//! # Durability boundary
//!
//! `wal.meta` (fsynced on every `sync()` and segment roll) records
//! `durable_offset`: every byte before it in the current segment survived an
//! fsync. Recovery uses it to distinguish the two failure modes:
//!
//! * Invalid record **at/after** the boundary: torn tail from a crash after
//!   the last sync. Those bytes were never acknowledged → truncate.
//! * Invalid record **before** the boundary: acknowledged data was damaged →
//!   fail with [`StorageError::Corrupt`], never silently drop.
//!
//! Segments older than the one named in `wal.meta` were fully synced before
//! the roll, so any invalid record in them is corruption, not a torn write.

use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use crc32c::crc32c;

use crate::error::{Result, StorageError};

/// Magic bytes at the start of every record: ASCII "CHRO".
const MAGIC: u32 = 0x4348_524f;
/// Upper bound for a single record payload.
const MAX_RECORD_LEN: u32 = 16 * 1024 * 1024;
/// magic(4) + len(4) + crc32c(4).
const HEADER_LEN: u64 = 12;

const META_NAME: &str = "wal.meta";
/// magic(4) + cur_seg(4) + cur_offset(8) + durable_offset(8) + base_seq(8) + crc(4).
const META_LEN: usize = 36;

/// Location of one record inside the log.
#[derive(Clone, Copy, Debug)]
struct RecordLoc {
    seg: u32,
    offset: u64,
    len: u32,
}

/// The fsynced durability metadata.
#[derive(Clone, Copy, Debug)]
struct WalMeta {
    cur_seg: u32,
    cur_offset: u64,
    durable_offset: u64,
    base_seq: u64,
}

fn seg_name(seg: u32) -> String {
    format!("wal-{seg:08}.log")
}

fn seg_num(file_name: &str) -> Option<u32> {
    file_name
        .strip_prefix("wal-")?
        .strip_suffix(".log")?
        .parse()
        .ok()
}

/// Fsync a directory so file creations, renames and deletions are durable.
pub(crate) fn fsync_dir(dir: &Path) -> Result<()> {
    File::open(dir)?.sync_all()?;
    Ok(())
}

fn truncate_file(path: &Path, len: u64) -> Result<()> {
    OpenOptions::new().write(true).open(path)?.set_len(len)?;
    Ok(())
}

fn write_meta(dir: &Path, meta: &WalMeta) -> Result<()> {
    let mut buf = [0u8; META_LEN];
    buf[0..4].copy_from_slice(&MAGIC.to_le_bytes());
    buf[4..8].copy_from_slice(&meta.cur_seg.to_le_bytes());
    buf[8..16].copy_from_slice(&meta.cur_offset.to_le_bytes());
    buf[16..24].copy_from_slice(&meta.durable_offset.to_le_bytes());
    buf[24..32].copy_from_slice(&meta.base_seq.to_le_bytes());
    let crc = crc32c(&buf[..32]);
    buf[32..36].copy_from_slice(&crc.to_le_bytes());
    let tmp = dir.join("wal.meta.tmp");
    {
        let mut f = File::create(&tmp)?;
        f.write_all(&buf)?;
        f.sync_all()?;
    }
    fs::rename(&tmp, dir.join(META_NAME))?;
    fsync_dir(dir)?;
    Ok(())
}

fn read_meta(dir: &Path) -> Result<Option<WalMeta>> {
    let path = dir.join(META_NAME);
    if !path.exists() {
        return Ok(None);
    }
    let buf = fs::read(&path)?;
    if buf.len() != META_LEN {
        return Err(StorageError::CorruptMeta(format!(
            "expected {META_LEN} bytes, found {}",
            buf.len()
        )));
    }
    let magic = u32::from_le_bytes(buf[0..4].try_into().unwrap());
    let crc = u32::from_le_bytes(buf[32..36].try_into().unwrap());
    if magic != MAGIC || crc32c(&buf[..32]) != crc {
        return Err(StorageError::CorruptMeta(
            "bad magic or checksum".to_string(),
        ));
    }
    Ok(Some(WalMeta {
        cur_seg: u32::from_le_bytes(buf[4..8].try_into().unwrap()),
        cur_offset: u64::from_le_bytes(buf[8..16].try_into().unwrap()),
        durable_offset: u64::from_le_bytes(buf[16..24].try_into().unwrap()),
        base_seq: u64::from_le_bytes(buf[24..32].try_into().unwrap()),
    }))
}

enum ScanOutcome {
    /// (location, offset of the next record)
    Valid(RecordLoc, u64),
    /// (reason, offset where the bad record starts)
    Invalid(String, u64),
}

/// Validate the single record starting at `off`. Never reads past the buffer.
fn scan_one(data: &[u8], seg: u32, off: u64) -> ScanOutcome {
    let total = data.len() as u64;
    let remaining = total - off;
    if remaining < HEADER_LEN {
        return ScanOutcome::Invalid(
            format!("torn header: only {remaining} bytes remain"),
            off,
        );
    }
    let o = off as usize;
    let magic = u32::from_le_bytes(data[o..o + 4].try_into().unwrap());
    let len = u32::from_le_bytes(data[o + 4..o + 8].try_into().unwrap());
    let crc = u32::from_le_bytes(data[o + 8..o + 12].try_into().unwrap());
    if magic != MAGIC {
        return ScanOutcome::Invalid(format!("bad magic {magic:#010x}"), off);
    }
    if len > MAX_RECORD_LEN {
        return ScanOutcome::Invalid(
            format!("record length {len} exceeds maximum {MAX_RECORD_LEN}"),
            off,
        );
    }
    let frame = HEADER_LEN + len as u64;
    if remaining < frame {
        return ScanOutcome::Invalid(
            format!("torn payload: need {frame} bytes, have {remaining}"),
            off,
        );
    }
    let payload = &data[o + 12..o + 12 + len as usize];
    if crc32c(payload) != crc {
        return ScanOutcome::Invalid("crc32c mismatch".to_string(), off);
    }
    ScanOutcome::Valid(RecordLoc { seg, offset: off, len }, off + frame)
}

/// Scan one segment file. Returns the valid record locations and the offset one
/// past the last valid byte. A torn tail at/after `durable_limit` is truncated;
/// any invalid record before it is corruption and fails the open.
fn scan_segment(dir: &Path, seg: u32, durable_limit: u64) -> Result<(Vec<RecordLoc>, u64)> {
    let path = dir.join(seg_name(seg));
    let data = fs::read(&path)?;
    let mut locs = Vec::new();
    let mut off: u64 = 0;
    let total = data.len() as u64;
    while off < total {
        match scan_one(&data, seg, off) {
            ScanOutcome::Valid(loc, next) => {
                locs.push(loc);
                off = next;
            }
            ScanOutcome::Invalid(reason, bad_off) => {
                if bad_off < durable_limit {
                    return Err(StorageError::Corrupt {
                        segment: seg,
                        offset: bad_off,
                        reason,
                    });
                }
                truncate_file(&path, bad_off)?;
                return Ok((locs, bad_off));
            }
        }
    }
    Ok((locs, total))
}

/// Append-only write-ahead log.
///
/// Sequence numbers are `u64`, dense from `base_seq` (which advances on
/// [`Wal::compact_prefix`]). `append()` returns the sequence number but does
/// not make the record durable; call [`Wal::sync`] and only acknowledge the
/// write after it returns `Ok`.
#[derive(Debug)]
pub struct Wal {
    dir: PathBuf,
    seg_target: u64,
    file: File,
    cur_seg: u32,
    cur_offset: u64,
    /// Prefix `[0, durable_offset)` of the current segment survived an fsync.
    durable_offset: u64,
    index: Vec<RecordLoc>,
    base_seq: u64,
}

impl Wal {
    /// Open (creating if needed) the WAL in `dir`, recovering existing data.
    ///
    /// Recovery replays every valid record, truncates a torn tail that was
    /// never synced, and fails loudly on corruption of synced data.
    pub fn open(dir: &Path, seg_target: u64) -> Result<Self> {
        fs::create_dir_all(dir)?;
        // Remove leftover temp files from crashed compactions/snapshots.
        for entry in fs::read_dir(dir)? {
            let entry = entry?;
            let name = entry.file_name().to_string_lossy().into_owned();
            if name.ends_with(".tmp") {
                let _ = fs::remove_file(entry.path());
            }
        }
        let mut segs: Vec<u32> = fs::read_dir(dir)?
            .filter_map(|e| e.ok())
            .filter_map(|e| e.file_name().into_string().ok())
            .filter_map(|n| seg_num(&n))
            .collect();
        segs.sort_unstable();

        if segs.is_empty() {
            let file = OpenOptions::new()
                .create(true)
                .append(true)
                .open(dir.join(seg_name(0)))?;
            fsync_dir(dir)?;
            let wal = Wal {
                dir: dir.to_path_buf(),
                seg_target,
                file,
                cur_seg: 0,
                cur_offset: 0,
                durable_offset: 0,
                index: Vec::new(),
                base_seq: 0,
            };
            wal.persist_meta()?;
            return Ok(wal);
        }

        let m = read_meta(dir)?.ok_or_else(|| {
            StorageError::CorruptMeta("WAL segments exist but wal.meta is missing".to_string())
        })?;

        let mut index = Vec::new();
        let mut last_seg = 0u32;
        let mut last_len = 0u64;
        for &seg in &segs {
            // Durability boundary per segment: older segments were fully synced
            // before the roll; newer-than-meta segments had nothing synced.
            let durable_limit = if seg < m.cur_seg {
                u64::MAX
            } else if seg == m.cur_seg {
                m.durable_offset
            } else {
                0
            };
            let (locs, valid_len) = scan_segment(dir, seg, durable_limit)?;
            index.extend(locs);
            last_seg = seg;
            last_len = valid_len;
        }

        let file = OpenOptions::new()
            .append(true)
            .open(dir.join(seg_name(last_seg)))?;
        let durable_offset = if last_seg == m.cur_seg {
            m.durable_offset.min(last_len)
        } else {
            0
        };
        let mut wal = Wal {
            dir: dir.to_path_buf(),
            seg_target,
            file,
            cur_seg: last_seg,
            cur_offset: last_len,
            durable_offset,
            index,
            base_seq: m.base_seq,
        };
        // Reconcile meta with what the scan actually found (self-healing after
        // a crash between segment creation and meta write).
        wal.persist_meta()?;
        Ok(wal)
    }

    fn persist_meta(&self) -> Result<()> {
        write_meta(
            &self.dir,
            &WalMeta {
                cur_seg: self.cur_seg,
                cur_offset: self.cur_offset,
                durable_offset: self.durable_offset,
                base_seq: self.base_seq,
            },
        )
    }

    /// Next sequence number that will be assigned (== number of live records + base).
    pub fn len(&self) -> u64 {
        self.base_seq + self.index.len() as u64
    }

    pub fn is_empty(&self) -> bool {
        self.index.is_empty()
    }

    pub fn base_seq(&self) -> u64 {
        self.base_seq
    }

    /// Append a record. Returns its sequence number. The record is NOT durable
    /// until [`Wal::sync`] returns `Ok`; do not acknowledge it before that.
    ///
    /// On I/O error (including disk-full) the partial frame is rolled back
    /// and the error propagated; the caller must not acknowledge the write.
    pub fn append(&mut self, payload: &[u8]) -> Result<u64> {
        if payload.len() > MAX_RECORD_LEN as usize {
            return Err(StorageError::RecordTooLarge(payload.len()));
        }
        let frame_len = HEADER_LEN + payload.len() as u64;
        if self.cur_offset > 0 && self.cur_offset + frame_len > self.seg_target {
            self.roll()?;
        }
        let seq = self.base_seq + self.index.len() as u64;
        let offset = self.cur_offset;

        let mut hdr = [0u8; HEADER_LEN as usize];
        hdr[0..4].copy_from_slice(&MAGIC.to_le_bytes());
        hdr[4..8].copy_from_slice(&(payload.len() as u32).to_le_bytes());
        hdr[8..12].copy_from_slice(&crc32c(payload).to_le_bytes());

        if let Err(e) = self.file.write_all(&hdr).and_then(|_| self.file.write_all(payload)) {
            // Best effort: remove the partial frame so the file never holds a
            // torn record we didn't intend.
            let _ = self.file.set_len(offset);
            let _ = self.file.seek(SeekFrom::Start(offset));
            return Err(StorageError::Io(e));
        }
        self.index.push(RecordLoc {
            seg: self.cur_seg,
            offset,
            len: payload.len() as u32,
        });
        self.cur_offset = offset + frame_len;
        Ok(seq)
    }

    /// Make all appended records durable: flush, fsync the segment, and fsync
    /// the updated durability boundary. Only acknowledge writes after this
    /// returns `Ok`.
    pub fn sync(&mut self) -> Result<()> {
        self.file.sync_all()?;
        self.durable_offset = self.cur_offset;
        self.persist_meta()?;
        Ok(())
    }

    /// Read a record by sequence number, re-validating its framing and checksum
    /// (defense in depth: the OS or disk may have damaged it since recovery).
    pub fn get(&self, seq: u64) -> Result<Vec<u8>> {
        let pos = seq
            .checked_sub(self.base_seq)
            .ok_or(StorageError::InvalidSequence(seq))? as usize;
        let loc = self
            .index
            .get(pos)
            .copied()
            .ok_or(StorageError::InvalidSequence(seq))?;
        let mut f = File::open(self.dir.join(seg_name(loc.seg)))?;
        f.seek(SeekFrom::Start(loc.offset))?;
        let mut hdr = [0u8; HEADER_LEN as usize];
        f.read_exact(&mut hdr)?;
        let magic = u32::from_le_bytes(hdr[0..4].try_into().unwrap());
        let len = u32::from_le_bytes(hdr[4..8].try_into().unwrap());
        let crc = u32::from_le_bytes(hdr[8..12].try_into().unwrap());
        if magic != MAGIC || len != loc.len {
            return Err(StorageError::Corrupt {
                segment: loc.seg,
                offset: loc.offset,
                reason: "framing mismatch on read".to_string(),
            });
        }
        let mut payload = vec![0u8; len as usize];
        f.read_exact(&mut payload)?;
        if crc32c(&payload) != crc {
            return Err(StorageError::Corrupt {
                segment: loc.seg,
                offset: loc.offset,
                reason: "crc32c mismatch on read".to_string(),
            });
        }
        Ok(payload)
    }

    /// Roll to a fresh segment. The current segment is fully synced first, so
    /// every record in it is durable before we move on.
    fn roll(&mut self) -> Result<()> {
        self.sync()?;
        let new_seg = self.cur_seg + 1;
        let path = self.dir.join(seg_name(new_seg));
        self.file = OpenOptions::new()
            .create_new(true)
            .append(true)
            .open(&path)?;
        fsync_dir(&self.dir)?;
        self.cur_seg = new_seg;
        self.cur_offset = 0;
        self.durable_offset = 0;
        self.persist_meta()?;
        Ok(())
    }

    /// Delete the log suffix starting at `seq` (Raft log-conflict repair).
    /// Truncating below the durability boundary is safe: the truncated records
    /// were either uncommitted or will be re-replicated; acknowledged *state*
    /// is only ever derived from committed prefixes.
    pub fn truncate_from(&mut self, seq: u64) -> Result<()> {
        if seq < self.base_seq {
            return Err(StorageError::InvalidSequence(seq));
        }
        let pos = (seq - self.base_seq) as usize;
        if pos >= self.index.len() {
            return Ok(()); // nothing to truncate
        }
        if pos == 0 {
            return self.reset(seq);
        }
        let loc = self.index[pos];
        for entry in fs::read_dir(&self.dir)? {
            let entry = entry?;
            if let Some(n) = entry.file_name().into_string().ok().and_then(|s| seg_num(&s)) {
                if n > loc.seg {
                    fs::remove_file(entry.path())?;
                }
            }
        }
        truncate_file(&self.dir.join(seg_name(loc.seg)), loc.offset)?;
        fsync_dir(&self.dir)?;
        self.index.truncate(pos);
        self.file = OpenOptions::new()
            .append(true)
            .open(self.dir.join(seg_name(loc.seg)))?;
        self.file.sync_all()?;
        self.cur_seg = loc.seg;
        self.cur_offset = loc.offset;
        self.durable_offset = self.durable_offset.min(loc.offset);
        self.persist_meta()?;
        Ok(())
    }

    /// Drop every record, restarting the log at `new_base`. Used when the
    /// entire suffix is discarded.
    fn reset(&mut self, new_base: u64) -> Result<()> {
        for entry in fs::read_dir(&self.dir)? {
            let entry = entry?;
            if let Some(n) = entry
                .file_name()
                .into_string()
                .ok()
                .and_then(|s| seg_num(&s))
            {
                let _ = n;
                fs::remove_file(entry.path())?;
            }
        }
        let new_seg = self.cur_seg + 1;
        self.file = OpenOptions::new()
            .create_new(true)
            .append(true)
            .open(self.dir.join(seg_name(new_seg)))?;
        fsync_dir(&self.dir)?;
        self.cur_seg = new_seg;
        self.cur_offset = 0;
        self.durable_offset = 0;
        self.index.clear();
        self.base_seq = new_base;
        self.persist_meta()?;
        Ok(())
    }

    /// Discard records with sequence `< keep_from` (log compaction after a
    /// snapshot). Whole segments are deleted; a straddling segment is
    /// rewritten. The surviving records keep their sequence numbers.
    pub fn compact_prefix(&mut self, keep_from: u64) -> Result<()> {
        if keep_from < self.base_seq {
            return Err(StorageError::InvalidSequence(keep_from));
        }
        if keep_from == self.base_seq {
            return Ok(());
        }
        let end = self.len();
        if keep_from > end {
            return Err(StorageError::InvalidSequence(keep_from));
        }
        let drop_n = (keep_from - self.base_seq) as usize;

        // Group surviving/dropped records by segment.
        let mut seg_first: std::collections::BTreeMap<u32, usize> = std::collections::BTreeMap::new();
        let mut seg_last: std::collections::BTreeMap<u32, usize> = std::collections::BTreeMap::new();
        for (i, loc) in self.index.iter().enumerate() {
            seg_first.entry(loc.seg).or_insert(i);
            seg_last.insert(loc.seg, i);
        }

        let mut rewritten_cur = false;
        for (&seg, &first) in &seg_first {
            let last = seg_last[&seg];
            let seg_end_seq = self.base_seq + last as u64 + 1; // one past last
            let seg_start_seq = self.base_seq + first as u64;
            if seg_end_seq <= keep_from {
                // Whole segment is below the horizon: delete it.
                if seg == self.cur_seg {
                    // Can't delete the open segment; rewrite it empty below.
                    continue;
                }
                fs::remove_file(self.dir.join(seg_name(seg)))?;
            } else if seg_start_seq < keep_from {
                // Straddling segment: rewrite the surviving tail.
                let keep_idx_start = (keep_from - self.base_seq) as usize;
                let tmp_path = self.dir.join(format!("wal-{seg:08}.tmp"));
                {
                    let mut tmp = File::create(&tmp_path)?;
                    let mut new_off: u64 = 0;
                    for i in keep_idx_start..=last {
                        let payload = self.get(self.base_seq + i as u64)?;
                        let mut hdr = [0u8; HEADER_LEN as usize];
                        hdr[0..4].copy_from_slice(&MAGIC.to_le_bytes());
                        hdr[4..8].copy_from_slice(&(payload.len() as u32).to_le_bytes());
                        hdr[8..12].copy_from_slice(&crc32c(&payload).to_le_bytes());
                        tmp.write_all(&hdr)?;
                        tmp.write_all(&payload)?;
                        self.index[i] = RecordLoc {
                            seg,
                            offset: new_off,
                            len: payload.len() as u32,
                        };
                        new_off += HEADER_LEN + payload.len() as u64;
                    }
                    tmp.sync_all()?;
                    if seg == self.cur_seg {
                        rewritten_cur = true;
                        self.cur_offset = new_off;
                        self.durable_offset = new_off;
                    }
                }
                fs::rename(&tmp_path, self.dir.join(seg_name(seg)))?;
            }
        }
        fsync_dir(&self.dir)?;

        // If the current segment ended up fully below the horizon, it was
        // skipped above; handle it by resetting the tail.
        self.index.drain(..drop_n);
        self.base_seq = keep_from;

        if self.index.is_empty() {
            // Everything compacted away: start a fresh segment.
            let new_seg = self.cur_seg + 1;
            self.file = OpenOptions::new()
                .create_new(true)
                .append(true)
                .open(self.dir.join(seg_name(new_seg)))?;
            fsync_dir(&self.dir)?;
            self.cur_seg = new_seg;
            self.cur_offset = 0;
            self.durable_offset = 0;
        } else if rewritten_cur {
            self.file = OpenOptions::new()
                .append(true)
                .open(self.dir.join(seg_name(self.cur_seg)))?;
            self.file.sync_all()?;
        }
        self.persist_meta()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;
    use tempfile::TempDir;

    fn open(dir: &TempDir) -> Wal {
        Wal::open(dir.path(), 1024).unwrap()
    }

    #[test]
    fn append_sync_get_roundtrip() {
        let dir = TempDir::new().unwrap();
        let mut wal = open(&dir);
        let s0 = wal.append(b"hello").unwrap();
        let s1 = wal.append(b"world").unwrap();
        assert_eq!((s0, s1), (0, 1));
        wal.sync().unwrap();
        assert_eq!(wal.get(0).unwrap(), b"hello");
        assert_eq!(wal.get(1).unwrap(), b"world");
        assert_eq!(wal.len(), 2);
    }

    #[test]
    fn recovery_replays_synced_records() {
        let dir = TempDir::new().unwrap();
        {
            let mut wal = open(&dir);
            for i in 0..50u32 {
                wal.append(format!("value-{i}").as_bytes()).unwrap();
            }
            wal.sync().unwrap();
        }
        let wal = open(&dir);
        assert_eq!(wal.len(), 50);
        for i in 0..50u32 {
            assert_eq!(wal.get(i as u64).unwrap(), format!("value-{i}").into_bytes());
        }
    }

    #[test]
    fn valid_unsynced_tail_is_kept() {
        // A fully-written but not-yet-synced record was never acknowledged,
        // but keeping it is safe (it represents a real submitted operation).
        let dir = TempDir::new().unwrap();
        {
            let mut wal = open(&dir);
            wal.append(b"one").unwrap();
            wal.sync().unwrap();
            wal.append(b"two").unwrap();
            wal.sync().unwrap();
            wal.append(b"three").unwrap(); // no sync: simulating crash before fsync
        }
        let wal = open(&dir);
        assert_eq!(wal.len(), 3);
        assert_eq!(wal.get(2).unwrap(), b"three");
    }

    #[test]
    fn torn_tail_is_truncated_on_recovery() {
        let dir = TempDir::new().unwrap();
        {
            let mut wal = open(&dir);
            wal.append(b"one").unwrap();
            wal.sync().unwrap();
            wal.append(b"two").unwrap();
            wal.sync().unwrap();
            wal.append(b"three-hundred-bytes-of-payload-xxxxxxxx").unwrap();
            // Crash mid-write: tear the last record (it was never synced).
            let seg = dir.path().join(seg_name(0));
            let len = fs::metadata(&seg).unwrap().len();
            OpenOptions::new()
                .write(true)
                .open(&seg)
                .unwrap()
                .set_len(len - 10)
                .unwrap();
        }
        let wal = open(&dir);
        assert_eq!(wal.len(), 2);
        assert_eq!(wal.get(0).unwrap(), b"one");
        assert_eq!(wal.get(1).unwrap(), b"two");
        // The log is still writable after torn-tail truncation.
        let mut wal = wal;
        wal.append(b"four").unwrap();
        wal.sync().unwrap();
        assert_eq!(wal.get(2).unwrap(), b"four");
    }

    #[test]
    fn corrupt_synced_record_fails_loudly() {
        // Damaging an acknowledged (synced) record must NEVER be silently
        // truncated: recovery refuses to open.
        let dir = TempDir::new().unwrap();
        {
            let mut wal = open(&dir);
            wal.append(b"one").unwrap();
            wal.append(b"two").unwrap();
            wal.sync().unwrap();
            // Flip a byte inside the first record's payload (after the header).
            let seg = dir.path().join(seg_name(0));
            let mut data = fs::read(&seg).unwrap();
            data[HEADER_LEN as usize + 1] ^= 0xff;
            fs::write(&seg, &data).unwrap();
        }
        let err = Wal::open(dir.path(), 1024).unwrap_err();
        match err {
            StorageError::Corrupt { segment: 0, offset: 0, .. } => {}
            other => panic!("expected loud Corrupt error, got: {other:?}"),
        }
    }

    #[test]
    fn segment_rollover() {
        let dir = TempDir::new().unwrap();
        let mut wal = Wal::open(dir.path(), 64).unwrap(); // tiny segments
        for i in 0..20 {
            wal.append(format!("record-{i:02}").as_bytes()).unwrap();
        }
        wal.sync().unwrap();
        let segs: Vec<_> = fs::read_dir(dir.path())
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.file_name().to_string_lossy().starts_with("wal-"))
            .collect();
        assert!(segs.len() > 1, "expected rollover, found {:?}", segs.len());
        assert_eq!(wal.len(), 20);
        drop(wal);
        let wal = Wal::open(dir.path(), 64).unwrap();
        assert_eq!(wal.len(), 20);
        assert_eq!(wal.get(19).unwrap(), b"record-19");
    }

    #[test]
    fn truncate_from_drops_suffix() {
        let dir = TempDir::new().unwrap();
        let mut wal = open(&dir);
        for i in 0..10 {
            wal.append(format!("v{i}").as_bytes()).unwrap();
        }
        wal.sync().unwrap();
        wal.truncate_from(7).unwrap();
        assert_eq!(wal.len(), 7);
        assert!(matches!(
            wal.get(7),
            Err(StorageError::InvalidSequence(7))
        ));
        // Still writable; new records continue the sequence.
        let s = wal.append(b"new").unwrap();
        assert_eq!(s, 7);
        wal.sync().unwrap();
        assert_eq!(wal.get(7).unwrap(), b"new");
        // Survives recovery too.
        drop(wal);
        let wal = open(&dir);
        assert_eq!(wal.len(), 8);
        assert_eq!(wal.get(7).unwrap(), b"new");
    }

    #[test]
    fn compact_prefix_drops_old_segments() {
        let dir = TempDir::new().unwrap();
        let mut wal = Wal::open(dir.path(), 128).unwrap();
        for i in 0..40u32 {
            wal.append(format!("value-{i:02}").as_bytes()).unwrap();
        }
        wal.sync().unwrap();
        assert!(wal.len() == 40);
        wal.compact_prefix(30).unwrap();
        assert_eq!(wal.base_seq(), 30);
        assert_eq!(wal.len(), 40);
        assert_eq!(wal.get(30).unwrap(), b"value-30");
        assert_eq!(wal.get(39).unwrap(), b"value-39");
        assert!(matches!(
            wal.get(29),
            Err(StorageError::InvalidSequence(29))
        ));
        // New appends continue from the end.
        let s = wal.append(b"fresh").unwrap();
        assert_eq!(s, 40);
        wal.sync().unwrap();
        drop(wal);
        let wal = Wal::open(dir.path(), 128).unwrap();
        assert_eq!(wal.base_seq(), 30);
        assert_eq!(wal.len(), 41);
        assert_eq!(wal.get(40).unwrap(), b"fresh");
    }

    #[test]
    fn record_too_large_rejected() {
        let dir = TempDir::new().unwrap();
        let mut wal = open(&dir);
        let big = vec![0u8; MAX_RECORD_LEN as usize + 1];
        assert!(matches!(
            wal.append(&big),
            Err(StorageError::RecordTooLarge(_))
        ));
        assert_eq!(wal.len(), 0);
    }

    #[test]
    fn binary_keys_and_values() {
        let dir = TempDir::new().unwrap();
        let mut wal = open(&dir);
        let bin: Vec<u8> = (0..=255u8).collect();
        wal.append(&bin).unwrap();
        wal.append(&[0u8; 0]).unwrap(); // empty payload is legal
        wal.sync().unwrap();
        assert_eq!(wal.get(0).unwrap(), bin);
        assert_eq!(wal.get(1).unwrap(), Vec::<u8>::new());
    }

    proptest! {
        #[test]
        fn prop_append_get_survives_reopen(
            ops in prop::collection::vec(prop::collection::vec(any::<u8>(), 0..64), 0..30)
        ) {
            let dir = TempDir::new().unwrap();
            let mut wal = Wal::open(dir.path(), 4096).unwrap();
            for op in &ops {
                wal.append(op).unwrap();
            }
            wal.sync().unwrap();
            let len = wal.len();
            drop(wal);
            let wal2 = Wal::open(dir.path(), 4096).unwrap();
            prop_assert_eq!(wal2.len(), len);
            for (i, op) in ops.iter().enumerate() {
                prop_assert_eq!(&wal2.get(i as u64).unwrap(), op);
            }
        }
    }
}
