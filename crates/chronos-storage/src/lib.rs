//! `chronos-storage`: crash-safe persistence primitives for ChronosDB.
//!
//! - [`wal::Wal`]: append-only write-ahead log with checksummed framing,
//!   fsync durability boundaries, torn-write recovery, and prefix compaction.
//! - [`snapshot`]: atomic, checksummed whole-state snapshots.
//! - [`error`]: error types.
//!
//! # Durability contract
//!
//! * `append()` stages bytes in the OS page cache; nothing is durable yet.
//! * `sync()` flushes and fsyncs, then records the new durability boundary in
//!   `wal.meta` (itself fsynced). Only after `sync()` returns `Ok` may the
//!   caller acknowledge the write to a client.
//! * On recovery, bytes at or after the recorded boundary that fail validation
//!   are treated as a torn tail from a crash and truncated — they were never
//!   acknowledged. Bytes *before* the boundary that fail validation are
//!   acknowledged data that got damaged: recovery fails loudly instead of
//!   silently dropping them.

pub mod error;
pub mod snapshot;
pub mod wal;

pub use error::{Result, StorageError};
pub use wal::Wal;
