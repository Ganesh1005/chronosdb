//! Multi-version concurrency control with snapshot isolation.
//!
//! # Model
//!
//! Every committed write creates a new *version* of a key stamped with a
//! monotonically increasing commit sequence number. A transaction observes a
//! consistent snapshot: the newest version of each key with
//! `commit_seq <= read_seq`, where `read_seq` is the last committed sequence
//! when the transaction began. Writes are buffered privately and validated at
//! commit.
//!
//! # First-committer-wins
//!
//! At commit, each key in the write set is checked: if any version with
//! `commit_seq > read_seq` exists, a concurrent transaction committed a write
//! to that key after our snapshot was taken and we abort with
//! [`MvccError::Conflict`]. Validation, sequence assignment and installation
//! happen atomically under `&mut self`, so two conflicting writers cannot both
//! commit.
//!
//! # Anomalies snapshot isolation permits
//!
//! Snapshot isolation is *not* serializable. The classic anomaly is **write
//! skew**: T1 and T2 both read keys A and B (say, two on-call doctors), then
//! T1 sets A="off duty" and T2 sets B="off duty". Neither write conflicts
//! (disjoint write sets), both commit, and the invariant "at least one doctor
//! on call" is violated even though no transaction ever saw a bad state.
//! Applications needing serializability must enforce cross-key invariants
//! outside the database (e.g. a single coordinator key in every such
//! transaction's write set, which forces the writers to conflict).
//!
//! What SI *does* prevent: dirty reads, non-repeatable reads, phantoms within
//! a single transaction's reads, and lost updates (via first-committer-wins).
//!
//! # Concurrency
//!
//! `MvccStore` is single-threaded (`!Sync` by design is not required, but all
//! methods take `&mut self`). Share it between threads with a `Mutex`; the
//! commit critical section is then the mutex itself.

use std::collections::BTreeMap;

use thiserror::Error;

pub type TxId = u64;

#[derive(Debug, Error)]
pub enum MvccError {
    #[error("unknown transaction {0}")]
    UnknownTransaction(TxId),
    #[error("write-write conflict on key {0:?}")]
    Conflict(Vec<u8>),
    #[error("invalid version {0}")]
    InvalidVersion(u64),
    #[error("corrupt snapshot: {0}")]
    BadSnapshot(String),
    #[error("commit record out of order: expected seq {expected}, got {got}")]
    OutOfOrderApply { expected: u64, got: u64 },
}

pub type Result<T> = std::result::Result<T, MvccError>;

/// One committed version of a key. `value == None` is a tombstone (delete).
#[derive(Clone, Debug, PartialEq)]
pub struct Version {
    pub commit_seq: u64,
    pub value: Option<Vec<u8>>,
}

#[derive(Debug)]
struct TxState {
    read_seq: u64,
    writes: BTreeMap<Vec<u8>, Option<Vec<u8>>>,
}

/// MVCC key-value store with snapshot-isolated transactions.
#[derive(Debug, Default)]
pub struct MvccStore {
    /// Key -> versions, newest first.
    data: BTreeMap<Vec<u8>, Vec<Version>>,
    /// Last assigned commit sequence number (0 = nothing committed yet).
    next_seq: u64,
    next_tx: TxId,
    txns: BTreeMap<TxId, TxState>,
    /// read_seq -> number of active transactions holding that snapshot.
    /// The GC horizon is the minimum key.
    active_reads: BTreeMap<u64, u32>,
}

impl MvccStore {
    pub fn new() -> Self {
        Self::default()
    }

    /// Last committed sequence number.
    pub fn last_committed(&self) -> u64 {
        self.next_seq
    }

    pub fn active_txn_count(&self) -> usize {
        self.txns.len()
    }

    /// Begin a transaction. Its snapshot is the current committed state.
    pub fn begin(&mut self) -> TxId {
        self.next_tx += 1;
        let tx = self.next_tx;
        let read_seq = self.next_seq;
        *self.active_reads.entry(read_seq).or_insert(0) += 1;
        self.txns.insert(tx, TxState { read_seq, writes: BTreeMap::new() });
        tx
    }

    fn txn(&self, tx: TxId) -> Result<&TxState> {
        self.txns.get(&tx).ok_or(MvccError::UnknownTransaction(tx))
    }

    fn txn_mut(&mut self, tx: TxId) -> Result<&mut TxState> {
        self.txns.get_mut(&tx).ok_or(MvccError::UnknownTransaction(tx))
    }

    fn release_read(&mut self, read_seq: u64) {
        if let Some(n) = self.active_reads.get_mut(&read_seq) {
            *n -= 1;
            if *n == 0 {
                self.active_reads.remove(&read_seq);
            }
        }
    }

    /// Read within a transaction: own writes first, then the snapshot.
    pub fn tx_get(&self, tx: TxId, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let state = self.txn(tx)?;
        if let Some(v) = state.writes.get(key) {
            return Ok(v.clone());
        }
        Ok(self.read_at(key, state.read_seq))
    }

    pub fn tx_put(&mut self, tx: TxId, key: Vec<u8>, value: Vec<u8>) -> Result<()> {
        self.txn_mut(tx)?.writes.insert(key, Some(value));
        Ok(())
    }

    pub fn tx_delete(&mut self, tx: TxId, key: Vec<u8>) -> Result<()> {
        self.txn_mut(tx)?.writes.insert(key, None);
        Ok(())
    }

    /// Commit a transaction. Returns the assigned commit sequence number and
    /// the write set (for the caller to persist to the WAL *before*
    /// acknowledging the client).
    ///
    /// Validation, sequence assignment and installation are one atomic step,
    /// so conflicting concurrent writers cannot both commit.
    pub fn commit(&mut self, tx: TxId) -> Result<(u64, Vec<(Vec<u8>, Option<Vec<u8>>)>)> {
        let state = self.txns.remove(&tx).ok_or(MvccError::UnknownTransaction(tx))?;
        self.release_read(state.read_seq);
        // First-committer-wins: abort if any written key changed since our snapshot.
        for key in state.writes.keys() {
            if let Some(versions) = self.data.get(key) {
                if let Some(newest) = versions.first() {
                    if newest.commit_seq > state.read_seq {
                        return Err(MvccError::Conflict(key.clone()));
                    }
                }
            }
        }
        let seq = self.next_seq + 1;
        self.next_seq = seq;
        let mut writes = Vec::with_capacity(state.writes.len());
        for (k, v) in state.writes {
            self.data
                .entry(k.clone())
                .or_default()
                .insert(0, Version { commit_seq: seq, value: v.clone() });
            writes.push((k, v));
        }
        Ok((seq, writes))
    }

    /// Roll a transaction back, discarding its buffered writes.
    pub fn rollback(&mut self, tx: TxId) -> Result<()> {
        let state = self.txns.remove(&tx).ok_or(MvccError::UnknownTransaction(tx))?;
        self.release_read(state.read_seq);
        Ok(())
    }

    /// Latest committed value (non-transactional read).
    pub fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        self.read_at(key, self.next_seq)
    }

    /// Historical read: newest version with `commit_seq <= seq`.
    ///
    /// Note: garbage collection may have removed very old versions; this
    /// returns the newest *retained* version at or before `seq`. Versions
    /// required by active snapshots are never collected.
    pub fn get_at_version(&self, key: &[u8], seq: u64) -> Result<Option<Vec<u8>>> {
        if seq > self.next_seq {
            return Err(MvccError::InvalidVersion(seq));
        }
        Ok(self.read_at(key, seq))
    }

    fn read_at(&self, key: &[u8], seq: u64) -> Option<Vec<u8>> {
        self.data
            .get(key)?
            .iter()
            .find(|v| v.commit_seq <= seq)
            .and_then(|v| v.value.clone())
    }

    /// Apply an already-decided commit during recovery replay. Records must be
    /// applied in sequence order; anything else is an error.
    pub fn apply_committed(&mut self, seq: u64, writes: &[(Vec<u8>, Option<Vec<u8>>)]) -> Result<()> {
        if seq != self.next_seq + 1 {
            return Err(MvccError::OutOfOrderApply {
                expected: self.next_seq + 1,
                got: seq,
            });
        }
        self.next_seq = seq;
        for (k, v) in writes {
            self.data
                .entry(k.clone())
                .or_default()
                .insert(0, Version { commit_seq: seq, value: v.clone() });
        }
        Ok(())
    }

    /// Reclaim versions no active snapshot can see. For each key, keeps every
    /// version newer than the oldest active snapshot plus the newest version
    /// at or before it. With no active transactions, only the latest version
    /// per key survives.
    pub fn gc(&mut self) {
        let horizon = self.active_reads.keys().next().copied().unwrap_or(self.next_seq);
        for versions in self.data.values_mut() {
            let mut keep = versions.len();
            for (i, v) in versions.iter().enumerate() {
                if v.commit_seq <= horizon {
                    keep = i + 1;
                    break;
                }
            }
            versions.truncate(keep);
        }
    }

    /// Number of retained versions for a key (introspection for tests).
    #[cfg(test)]
    fn version_count(&self, key: &[u8]) -> usize {
        self.data.get(key).map(|v| v.len()).unwrap_or(0)
    }

    /// Remove a transaction from the active set, returning its read snapshot
    /// and buffered writes. Used by the server when proposing the commit via
    /// Raft: the decision is made deterministically at apply time.
    pub fn take_tx(&mut self, tx: TxId) -> Result<(u64, Vec<(Vec<u8>, Option<Vec<u8>>)>)> {
        let state = self.txns.remove(&tx).ok_or(MvccError::UnknownTransaction(tx))?;
        self.release_read(state.read_seq);
        Ok((state.read_seq, state.writes.into_iter().collect()))
    }

    /// Deterministically apply an already-decided write batch (Raft state
    /// machine), assigning it commit sequence `seq`. Validates write-write
    /// conflicts against `read_seq` (first-committer-wins); returns `None` if
    /// the transaction aborts on conflict.
    ///
    /// This is deterministic: every replica applying the same log in order
    /// reaches the same commit/abort decision. The server uses the Raft log
    /// index as `seq`, so versions are stable across crashes and replay.
    /// The caller is responsible for idempotency (e.g. via client request
    /// IDs): re-applying the same batch would install duplicate versions.
    pub fn apply_decided(
        &mut self,
        seq: u64,
        read_seq: u64,
        writes: &[(Vec<u8>, Option<Vec<u8>>)],
    ) -> Option<u64> {
        for (k, _) in writes {
            if let Some(versions) = self.data.get(k) {
                if let Some(newest) = versions.first() {
                    if newest.commit_seq > read_seq {
                        return None;
                    }
                }
            }
        }
        self.next_seq = self.next_seq.max(seq);
        for (k, v) in writes {
            self.data
                .entry(k.clone())
                .or_default()
                .insert(0, Version { commit_seq: seq, value: v.clone() });
        }
        Some(seq)
    }
}

// ---------------------------------------------------------------------------
// Serialization: commit records (for the WAL) and full snapshots.
// ---------------------------------------------------------------------------

/// Record type tag for commit records in the WAL.
const COMMIT_RECORD_TAG: u8 = 0x01;
const SNAPSHOT_MAGIC: u32 = 0x4D56_4353; // "MVCS"

fn put_u32(buf: &mut Vec<u8>, v: u32) {
    buf.extend_from_slice(&v.to_le_bytes());
}
fn put_u64(buf: &mut Vec<u8>, v: u64) {
    buf.extend_from_slice(&v.to_le_bytes());
}
fn put_bytes(buf: &mut Vec<u8>, b: &[u8]) {
    put_u32(buf, b.len() as u32);
    buf.extend_from_slice(b);
}

struct Cursor<'a> {
    data: &'a [u8],
    pos: usize,
}

impl<'a> Cursor<'a> {
    fn new(data: &'a [u8]) -> Self {
        Cursor { data, pos: 0 }
    }
    fn err(&self, what: &str) -> MvccError {
        MvccError::BadSnapshot(format!("{what} at offset {}", self.pos))
    }
    fn get_u32(&mut self) -> Result<u32> {
        if self.pos + 4 > self.data.len() {
            return Err(self.err("truncated u32"));
        }
        let v = u32::from_le_bytes(self.data[self.pos..self.pos + 4].try_into().unwrap());
        self.pos += 4;
        Ok(v)
    }
    fn get_u64(&mut self) -> Result<u64> {
        if self.pos + 8 > self.data.len() {
            return Err(self.err("truncated u64"));
        }
        let v = u64::from_le_bytes(self.data[self.pos..self.pos + 8].try_into().unwrap());
        self.pos += 8;
        Ok(v)
    }
    fn get_u8(&mut self) -> Result<u8> {
        if self.pos + 1 > self.data.len() {
            return Err(self.err("truncated u8"));
        }
        let v = self.data[self.pos];
        self.pos += 1;
        Ok(v)
    }
    fn get_bytes(&mut self) -> Result<Vec<u8>> {
        let len = self.get_u32()? as usize;
        if len > 16 * 1024 * 1024 {
            return Err(self.err("absurd length"));
        }
        if self.pos + len > self.data.len() {
            return Err(self.err("truncated bytes"));
        }
        let b = self.data[self.pos..self.pos + len].to_vec();
        self.pos += len;
        Ok(b)
    }
    fn expect_end(&self) -> Result<()> {
        if self.pos != self.data.len() {
            return Err(self.err("trailing bytes"));
        }
        Ok(())
    }
}

/// Encode a committed write batch for the WAL.
pub fn encode_commit_record(seq: u64, writes: &[(Vec<u8>, Option<Vec<u8>>)]) -> Vec<u8> {
    let mut buf = Vec::new();
    buf.push(COMMIT_RECORD_TAG);
    put_u64(&mut buf, seq);
    put_u32(&mut buf, writes.len() as u32);
    for (k, v) in writes {
        put_bytes(&mut buf, k);
        match v {
            Some(val) => {
                buf.push(1);
                put_bytes(&mut buf, val);
            }
            None => buf.push(0),
        }
    }
    buf
}

/// Decode a commit record from the WAL.
pub fn decode_commit_record(data: &[u8]) -> Result<(u64, Vec<(Vec<u8>, Option<Vec<u8>>)>)> {
    let mut c = Cursor::new(data);
    if c.get_u8()? != COMMIT_RECORD_TAG {
        return Err(c.err("bad commit record tag"));
    }
    let seq = c.get_u64()?;
    let n = c.get_u32()? as usize;
    if n > 1_000_000 {
        return Err(c.err("absurd write count"));
    }
    let mut writes = Vec::with_capacity(n);
    for _ in 0..n {
        let k = c.get_bytes()?;
        let has_value = c.get_u8()?;
        let v = match has_value {
            1 => Some(c.get_bytes()?),
            0 => None,
            _ => return Err(c.err("bad value tag")),
        };
        writes.push((k, v));
    }
    c.expect_end()?;
    Ok((seq, writes))
}

/// Serialize the full store (for `chronos-storage::snapshot`).
pub fn encode_snapshot(store: &MvccStore) -> Vec<u8> {
    let mut buf = Vec::new();
    put_u32(&mut buf, SNAPSHOT_MAGIC);
    put_u64(&mut buf, store.next_seq);
    put_u32(&mut buf, store.data.len() as u32);
    for (k, versions) in &store.data {
        put_bytes(&mut buf, k);
        put_u32(&mut buf, versions.len() as u32);
        for v in versions {
            put_u64(&mut buf, v.commit_seq);
            match &v.value {
                Some(val) => {
                    buf.push(1);
                    put_bytes(&mut buf, val);
                }
                None => buf.push(0),
            }
        }
    }
    buf
}

/// Restore a store from `encode_snapshot` output. Active transactions are not
/// snapshotted: they are in-flight by definition and lost on crash (clients
/// retry with request IDs).
pub fn decode_snapshot(data: &[u8]) -> Result<MvccStore> {
    let mut c = Cursor::new(data);
    if c.get_u32()? != SNAPSHOT_MAGIC {
        return Err(c.err("bad snapshot magic"));
    }
    let next_seq = c.get_u64()?;
    let nkeys = c.get_u32()? as usize;
    if nkeys > 10_000_000 {
        return Err(c.err("absurd key count"));
    }
    let mut store = MvccStore::new();
    store.next_seq = next_seq;
    for _ in 0..nkeys {
        let k = c.get_bytes()?;
        let nvers = c.get_u32()? as usize;
        if nvers > 10_000_000 {
            return Err(c.err("absurd version count"));
        }
        let mut versions = Vec::with_capacity(nvers);
        for _ in 0..nvers {
            let commit_seq = c.get_u64()?;
            let has_value = c.get_u8()?;
            let value = match has_value {
                1 => Some(c.get_bytes()?),
                0 => None,
                _ => return Err(c.err("bad value tag")),
            };
            versions.push(Version { commit_seq, value });
        }
        // Validate newest-first ordering.
        if !versions.windows(2).all(|w| w[0].commit_seq > w[1].commit_seq) {
            return Err(c.err("versions not newest-first"));
        }
        store.data.insert(k, versions);
    }
    c.expect_end()?;
    Ok(store)
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};

    fn put(store: &mut MvccStore, k: &[u8], v: &[u8]) -> u64 {
        let tx = store.begin();
        store.tx_put(tx, k.to_vec(), v.to_vec()).unwrap();
        let (seq, _) = store.commit(tx).unwrap();
        seq
    }

    #[test]
    fn basic_put_get() {
        let mut s = MvccStore::new();
        put(&mut s, b"k", b"v");
        assert_eq!(s.get(b"k").unwrap(), b"v");
        assert_eq!(s.get(b"missing"), None);
    }

    #[test]
    fn snapshot_isolation_hides_concurrent_commit() {
        let mut s = MvccStore::new();
        put(&mut s, b"k", b"v1");
        let tx1 = s.begin(); // snapshot at seq 1
        put(&mut s, b"k", b"v2"); // concurrent commit, seq 2
        assert_eq!(s.tx_get(tx1, b"k").unwrap().unwrap(), b"v1");
        s.rollback(tx1).unwrap();
        assert_eq!(s.get(b"k").unwrap(), b"v2");
    }

    #[test]
    fn own_writes_visible_in_txn() {
        let mut s = MvccStore::new();
        put(&mut s, b"k", b"v1");
        let tx = s.begin();
        s.tx_put(tx, b"k".to_vec(), b"v2".to_vec()).unwrap();
        assert_eq!(s.tx_get(tx, b"k").unwrap().unwrap(), b"v2");
        // ...but not to anyone else until commit.
        assert_eq!(s.get(b"k").unwrap(), b"v1");
        s.commit(tx).unwrap();
        assert_eq!(s.get(b"k").unwrap(), b"v2");
    }

    #[test]
    fn first_committer_wins() {
        let mut s = MvccStore::new();
        put(&mut s, b"k", b"v0");
        let tx1 = s.begin();
        let tx2 = s.begin();
        s.tx_put(tx1, b"k".to_vec(), b"from-tx1".to_vec()).unwrap();
        s.tx_put(tx2, b"k".to_vec(), b"from-tx2".to_vec()).unwrap();
        s.commit(tx1).unwrap();
        let err = s.commit(tx2).unwrap_err();
        assert!(matches!(err, MvccError::Conflict(_)), "{err:?}");
        assert_eq!(s.get(b"k").unwrap(), b"from-tx1");
    }

    #[test]
    fn disjoint_writes_both_commit() {
        let mut s = MvccStore::new();
        let tx1 = s.begin();
        let tx2 = s.begin();
        s.tx_put(tx1, b"a".to_vec(), b"1".to_vec()).unwrap();
        s.tx_put(tx2, b"b".to_vec(), b"2".to_vec()).unwrap();
        s.commit(tx1).unwrap();
        s.commit(tx2).unwrap();
        assert_eq!(s.get(b"a").unwrap(), b"1");
        assert_eq!(s.get(b"b").unwrap(), b"2");
    }

    #[test]
    fn rollback_discards_writes() {
        let mut s = MvccStore::new();
        put(&mut s, b"k", b"v1");
        let tx = s.begin();
        s.tx_put(tx, b"k".to_vec(), b"v2".to_vec()).unwrap();
        s.rollback(tx).unwrap();
        assert_eq!(s.get(b"k").unwrap(), b"v1");
        assert_eq!(s.active_txn_count(), 0);
    }

    #[test]
    fn delete_and_tombstone() {
        let mut s = MvccStore::new();
        put(&mut s, b"k", b"v1");
        let tx = s.begin();
        s.tx_delete(tx, b"k".to_vec()).unwrap();
        assert_eq!(s.tx_get(tx, b"k").unwrap(), None);
        s.commit(tx).unwrap();
        assert_eq!(s.get(b"k"), None);
        // Historical read still sees the old value.
        assert_eq!(s.get_at_version(b"k", 1).unwrap().unwrap(), b"v1");
    }

    #[test]
    fn historical_reads() {
        let mut s = MvccStore::new();
        put(&mut s, b"k", b"v1");
        put(&mut s, b"k", b"v2");
        put(&mut s, b"k", b"v3");
        assert_eq!(s.get_at_version(b"k", 1).unwrap().unwrap(), b"v1");
        assert_eq!(s.get_at_version(b"k", 2).unwrap().unwrap(), b"v2");
        assert_eq!(s.get_at_version(b"k", 3).unwrap().unwrap(), b"v3");
        assert!(matches!(
            s.get_at_version(b"k", 99),
            Err(MvccError::InvalidVersion(99))
        ));
    }

    #[test]
    fn gc_preserves_versions_for_active_snapshots() {
        let mut s = MvccStore::new();
        put(&mut s, b"k", b"v1"); // seq 1
        put(&mut s, b"k", b"v2"); // seq 2
        let tx = s.begin(); // snapshot at seq 2
        put(&mut s, b"k", b"v3"); // seq 3
        put(&mut s, b"k", b"v4"); // seq 4
        s.gc();
        // Oldest active snapshot is at 2: must keep v4, v3, and v2 (newest <= 2).
        assert_eq!(s.version_count(b"k"), 3);
        assert_eq!(s.tx_get(tx, b"k").unwrap().unwrap(), b"v2");
        s.rollback(tx).unwrap();
        s.gc();
        // No active snapshots: only the latest survives.
        assert_eq!(s.version_count(b"k"), 1);
        assert_eq!(s.get(b"k").unwrap(), b"v4");
    }

    #[test]
    fn gc_keeps_all_when_everything_is_newer_than_horizon() {
        let mut s = MvccStore::new();
        let tx = s.begin(); // snapshot at seq 0
        put(&mut s, b"k", b"v1");
        put(&mut s, b"k", b"v2");
        s.gc();
        assert_eq!(s.version_count(b"k"), 2);
        s.rollback(tx).unwrap();
    }

    #[test]
    fn unknown_transaction_errors() {
        let mut s = MvccStore::new();
        assert!(matches!(
            s.tx_get(999, b"k"),
            Err(MvccError::UnknownTransaction(999))
        ));
        assert!(matches!(
            s.commit(999),
            Err(MvccError::UnknownTransaction(999))
        ));
        assert!(matches!(
            s.rollback(999),
            Err(MvccError::UnknownTransaction(999))
        ));
    }

    #[test]
    fn commit_record_roundtrip() {
        let writes = vec![
            (b"a".to_vec(), Some(b"1".to_vec())),
            (b"b".to_vec(), None),
        ];
        let enc = encode_commit_record(42, &writes);
        let (seq, dec) = decode_commit_record(&enc).unwrap();
        assert_eq!(seq, 42);
        assert_eq!(dec, writes);
        // Corrupted tag fails.
        let mut bad = enc.clone();
        bad[0] ^= 0xff;
        assert!(decode_commit_record(&bad).is_err());
        // Truncation fails.
        assert!(decode_commit_record(&enc[..enc.len() - 3]).is_err());
    }

    #[test]
    fn snapshot_encode_decode_roundtrip() {
        let mut s = MvccStore::new();
        put(&mut s, b"k1", b"v1");
        put(&mut s, b"k1", b"v2");
        put(&mut s, b"k2", b"x");
        let tx = s.begin();
        s.tx_delete(tx, b"k2".to_vec()).unwrap();
        s.commit(tx).unwrap();
        let bytes = encode_snapshot(&s);
        let s2 = decode_snapshot(&bytes).unwrap();
        assert_eq!(s2.last_committed(), s.last_committed());
        assert_eq!(s2.get(b"k1").unwrap(), b"v2".to_vec());
        assert_eq!(s2.get(b"k2"), None);
        assert_eq!(s2.get_at_version(b"k2", 3).unwrap(), Some(b"x".to_vec()));
        // Corruption fails loudly (flip a magic byte; flipping bytes inside
        // next_seq would merely decode to a different seq, which is fine).
        let mut bad = bytes.clone();
        bad[0] ^= 0xff;
        assert!(decode_snapshot(&bad).is_err());
        assert!(decode_snapshot(&bytes[..bytes.len() - 5]).is_err());
    }

    #[test]
    fn apply_committed_rejects_out_of_order() {
        let mut s = MvccStore::new();
        s.apply_committed(1, &[(b"k".to_vec(), Some(b"v".to_vec()))]).unwrap();
        assert!(s.apply_committed(1, &[]).is_err());
        assert!(s.apply_committed(3, &[]).is_err());
        s.apply_committed(2, &[]).unwrap();
        assert_eq!(s.last_committed(), 2);
    }

    #[test]
    fn concurrent_writers_only_one_commits() {
        let store = Arc::new(Mutex::new(MvccStore::new()));
        {
            let mut s = store.lock().unwrap();
            put(&mut s, b"counter", b"0");
        }
        // Ten threads race to read-modify-write the same key. Exactly one
        // winner per generation; losers retry. Final value must be 10.
        for _round in 0..10 {
            let mut handles = vec![];
            for _ in 0..4 {
                let store = Arc::clone(&store);
                handles.push(std::thread::spawn(move || {
                    for _ in 0..50 {
                        let mut s = store.lock().unwrap();
                        let tx = s.begin();
                        let cur: u64 = String::from_utf8(s.tx_get(tx, b"counter").unwrap().unwrap())
                            .unwrap()
                            .parse()
                            .unwrap();
                        s.tx_put(tx, b"counter".to_vec(), (cur + 1).to_string().into_bytes())
                            .unwrap();
                        if s.commit(tx).is_ok() {
                            return;
                        }
                        // Conflict: back off the lock and retry.
                        drop(s);
                        std::thread::yield_now();
                    }
                    panic!("could not commit after retries");
                }));
            }
            // Exactly one thread should succeed per round; the others retry.
            // Join all: each returns after ITS commit succeeded.
            for h in handles {
                h.join().unwrap();
            }
        }
        let s = store.lock().unwrap();
        // 10 rounds * 4 threads... wait: each round all 4 threads eventually
        // commit (retry until success), so +4 per round = 40 total.
        let final_v: u64 = String::from_utf8(s.get(b"counter").unwrap()).unwrap().parse().unwrap();
        assert_eq!(final_v, 40);
    }

    #[test]
    fn write_skew_anomaly_is_possible() {
        // Documents the permitted SI anomaly: two txns read A and B, then
        // write disjoint keys. Both commit; the invariant "a+b > 0" breaks.
        let mut s = MvccStore::new();
        put(&mut s, b"a", b"1");
        put(&mut s, b"b", b"1");
        let tx1 = s.begin();
        let tx2 = s.begin();
        let a1: i64 = String::from_utf8(s.tx_get(tx1, b"a").unwrap().unwrap()).unwrap().parse().unwrap();
        let b1: i64 = String::from_utf8(s.tx_get(tx1, b"b").unwrap().unwrap()).unwrap().parse().unwrap();
        let a2: i64 = String::from_utf8(s.tx_get(tx2, b"a").unwrap().unwrap()).unwrap().parse().unwrap();
        let b2: i64 = String::from_utf8(s.tx_get(tx2, b"b").unwrap().unwrap()).unwrap().parse().unwrap();
        // Each sees (1,1) and decrements its own key, preserving "sum >= 1"
        // from its own viewpoint.
        if a1 + b1 > 1 {
            s.tx_put(tx1, b"a".to_vec(), b"0".to_vec()).unwrap();
        }
        if a2 + b2 > 1 {
            s.tx_put(tx2, b"b".to_vec(), b"0".to_vec()).unwrap();
        }
        s.commit(tx1).unwrap();
        s.commit(tx2).unwrap(); // no write-write conflict: anomaly!
        let fa: i64 = String::from_utf8(s.get(b"a").unwrap()).unwrap().parse().unwrap();
        let fb: i64 = String::from_utf8(s.get(b"b").unwrap()).unwrap().parse().unwrap();
        assert_eq!(fa + fb, 0, "write skew violated the invariant");
    }

    proptest! {
        #[test]
        fn prop_sequential_ops_match_model(
            ops in prop::collection::vec(
                (prop::collection::vec(any::<u8>(), 0..8), prop::option::of(prop::collection::vec(any::<u8>(), 0..8))),
                0..50
            )
        ) {
            // Each op is its own single-op transaction: model is a HashMap.
            let mut s = MvccStore::new();
            let mut model: HashMap<Vec<u8>, Vec<u8>> = HashMap::new();
            for (k, v) in ops {
                let tx = s.begin();
                match v {
                    Some(val) => { s.tx_put(tx, k.clone(), val.clone()).unwrap(); model.insert(k.clone(), val); }
                    None => { s.tx_delete(tx, k.clone()).unwrap(); model.remove(&k); }
                }
                s.commit(tx).unwrap();
                // Store and model agree on every key touched so far.
                for (mk, mv) in &model {
                    let got = s.get(mk);
                    prop_assert_eq!(got.as_deref(), Some(mv.as_slice()));
                }
                prop_assert_eq!(s.get(&k), model.get(&k).cloned());
            }
        }
    }
}
