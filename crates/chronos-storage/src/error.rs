//! Error types for the storage engine.

use thiserror::Error;

/// All errors the storage engine can produce.
///
/// Design note: corruption of data that was previously acknowledged as durable
/// is *always* surfaced as an error and never silently truncated. Only bytes
/// written after the last successful `sync()` (and therefore never
/// acknowledged) may be discarded during recovery.
#[derive(Debug, Error)]
pub enum StorageError {
    /// Underlying OS / filesystem error (includes disk-full: `ErrorKind::StorageFull`).
    /// The operation was not acknowledged; in-memory state was rolled back.
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    /// A record that was previously synced (hence possibly acknowledged) failed
    /// validation. Recovery refuses to open rather than silently dropping it.
    #[error("corruption in segment {segment} at offset {offset}: {reason}")]
    Corrupt {
        segment: u32,
        offset: u64,
        reason: String,
    },

    /// The WAL metadata file (`wal.meta`) is missing or unreadable when WAL
    /// segments exist. Without it the durability boundary is unknown, so we
    /// fail closed instead of guessing.
    #[error("WAL metadata unreadable: {0}")]
    CorruptMeta(String),

    /// A single record payload exceeds the 16 MiB limit.
    #[error("record too large: {0} bytes")]
    RecordTooLarge(usize),

    /// Sequence number is below the compaction horizon or beyond the log end.
    #[error("invalid sequence number: {0}")]
    InvalidSequence(u64),

    /// A snapshot file failed validation. Snapshots are never partially used.
    #[error("snapshot corruption: {0}")]
    BadSnapshot(String),
}

pub type Result<T> = std::result::Result<T, StorageError>;
