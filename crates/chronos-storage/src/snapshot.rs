//! Atomic, checksummed whole-state snapshots.
//!
//! A snapshot is written to a temp file (with fsync), then atomically renamed
//! into place, then the directory is fsynced. Readers only ever see complete,
//! checksummed snapshots; a corrupt snapshot fails loudly instead of being
//! partially used.

use std::fs::{self, File};
use std::io::Write;
use std::path::Path;

use crc32c::crc32c;

use crate::error::{Result, StorageError};
use crate::wal::fsync_dir;

/// Magic bytes: ASCII "SNAP".
const MAGIC: u32 = 0x534E_4150;

fn snap_name(seq: u64) -> String {
    format!("snapshot-{seq:016}.snap")
}

fn snap_seq(file_name: &str) -> Option<u64> {
    file_name
        .strip_prefix("snapshot-")?
        .strip_suffix(".snap")?
        .parse()
        .ok()
}

/// Write `data` as the snapshot for `seq`, atomically replacing older ones.
/// Keeps the two most recent snapshots so an operator has a fallback.
pub fn write_snapshot(dir: &Path, seq: u64, data: &[u8]) -> Result<()> {
    // Remove stale temp files from crashed writes.
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        if entry
            .file_name()
            .to_string_lossy()
            .ends_with(".tmp")
        {
            let _ = fs::remove_file(entry.path());
        }
    }
    let tmp = dir.join(format!("snapshot-{seq:016}.tmp"));
    let mut buf = Vec::with_capacity(16 + data.len());
    buf.extend_from_slice(&MAGIC.to_le_bytes());
    buf.extend_from_slice(&(data.len() as u64).to_le_bytes());
    buf.extend_from_slice(&crc32c(data).to_le_bytes());
    buf.extend_from_slice(data);
    {
        let mut f = File::create(&tmp)?;
        f.write_all(&buf)?;
        f.sync_all()?;
    }
    fs::rename(&tmp, dir.join(snap_name(seq)))?;
    fsync_dir(dir)?;

    // Retain the two newest snapshots, delete the rest.
    let mut seqs: Vec<u64> = fs::read_dir(dir)?
        .filter_map(|e| e.ok())
        .filter_map(|e| e.file_name().into_string().ok())
        .filter_map(|n| snap_seq(&n))
        .collect();
    seqs.sort_unstable();
    while seqs.len() > 2 {
        let old = seqs.remove(0);
        let _ = fs::remove_file(dir.join(snap_name(old)));
    }
    fsync_dir(dir)?;
    Ok(())
}

/// Read the newest snapshot, validating magic, length and checksum.
/// Returns `Ok(None)` when no snapshot exists.
pub fn read_latest(dir: &Path) -> Result<Option<(u64, Vec<u8>)>> {
    let mut seqs: Vec<u64> = fs::read_dir(dir)?
        .filter_map(|e| e.ok())
        .filter_map(|e| e.file_name().into_string().ok())
        .filter_map(|n| snap_seq(&n))
        .collect();
    seqs.sort_unstable();
    let Some(&seq) = seqs.last() else {
        return Ok(None);
    };
    let raw = fs::read(dir.join(snap_name(seq)))?;
    if raw.len() < 16 {
        return Err(StorageError::BadSnapshot(format!(
            "snapshot {seq}: truncated header"
        )));
    }
    let magic = u32::from_le_bytes(raw[0..4].try_into().unwrap());
    let len = u64::from_le_bytes(raw[4..12].try_into().unwrap()) as usize;
    let crc = u32::from_le_bytes(raw[12..16].try_into().unwrap());
    if magic != MAGIC {
        return Err(StorageError::BadSnapshot(format!(
            "snapshot {seq}: bad magic"
        )));
    }
    if raw.len() != 16 + len {
        return Err(StorageError::BadSnapshot(format!(
            "snapshot {seq}: length mismatch (header {len}, file {})",
            raw.len() - 16
        )));
    }
    let payload = &raw[16..];
    if crc32c(payload) != crc {
        return Err(StorageError::BadSnapshot(format!(
            "snapshot {seq}: checksum mismatch"
        )));
    }
    Ok(Some((seq, payload.to_vec())))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn snapshot_roundtrip() {
        let dir = TempDir::new().unwrap();
        assert!(read_latest(dir.path()).unwrap().is_none());
        write_snapshot(dir.path(), 10, b"state-at-10").unwrap();
        write_snapshot(dir.path(), 20, b"state-at-20").unwrap();
        let (seq, data) = read_latest(dir.path()).unwrap().unwrap();
        assert_eq!(seq, 20);
        assert_eq!(data, b"state-at-20");
    }

    #[test]
    fn snapshot_corruption_fails_loudly() {
        let dir = TempDir::new().unwrap();
        write_snapshot(dir.path(), 5, b"precious-state").unwrap();
        let path = dir.path().join(snap_name(5));
        let mut raw = fs::read(&path).unwrap();
        raw[20] ^= 0xff; // damage the payload
        fs::write(&path, &raw).unwrap();
        let err = read_latest(dir.path()).unwrap_err();
        assert!(matches!(err, StorageError::BadSnapshot(_)), "{err:?}");
    }

    #[test]
    fn snapshot_keeps_two_most_recent() {
        let dir = TempDir::new().unwrap();
        for seq in [1u64, 2, 3, 4] {
            write_snapshot(dir.path(), seq, format!("s{seq}").as_bytes()).unwrap();
        }
        let seqs: Vec<u64> = fs::read_dir(dir.path())
            .unwrap()
            .filter_map(|e| e.ok())
            .filter_map(|e| e.file_name().into_string().ok())
            .filter_map(|n| snap_seq(&n))
            .collect();
        assert_eq!(seqs.len(), 2);
        assert!(seqs.contains(&3) && seqs.contains(&4));
    }

    #[test]
    fn empty_snapshot_roundtrip() {
        let dir = TempDir::new().unwrap();
        write_snapshot(dir.path(), 0, b"").unwrap();
        let (seq, data) = read_latest(dir.path()).unwrap().unwrap();
        assert_eq!((seq, data.len()), (0, 0));
    }
}
