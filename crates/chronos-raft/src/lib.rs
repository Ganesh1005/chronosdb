//! Raft consensus, implemented from scratch.
//!
//! # Design
//!
//! The core ([`Node`]) is pure logic: no sockets, no threads, no wall-clock.
//! Time advances via [`Node::tick`] with an explicit `now_ms`, messages arrive
//! via [`Node::on_message`], and everything the node wants the outside world
//! to do comes back as [`Effect`]s. This makes the consensus logic
//! deterministically testable: see [`sim`].
//!
//! # Persistence contract
//!
//! `current_term`, `voted_for` and the log are saved through
//! [`PersistentStore`] **before** the node acts on them: before voting,
//! before responding to `AppendEntries`, before requesting votes, and before
//! acknowledging a client write. A node that cannot persist halts
//! ([`NodeHalted`]) rather than making promises its disk cannot keep.
//!
//! # Key correctness rules enforced
//!
//! * **§5.4.2 (Figure 8):** only entries from the *current* term are committed
//!   by counting replicas. Older entries commit transitively when a newer
//!   current-term entry commits. Committing an old-term entry by quorum count
//!   is the classic Raft implementation bug and would allow two leaders to
//!   commit different values at the same index.
//! * A leader commits a no-op entry in its own term on election, so pending
//!   prior-term entries can commit transitively.
//! * Client writes are acknowledged only after the entry is committed on a
//!   quorum (see [`Effect::ClientResponse`]).
//! * Uncommitted entries are never applied to the state machine and never
//!   returned to clients.

pub mod sim;

use std::collections::{HashMap, HashSet};

use serde::{Deserialize, Serialize};
use thiserror::Error;

pub type NodeId = u64;
pub type Term = u64;
/// 1-based log index. `0` means "no entry" (genesis / empty prefix).
pub type Index = u64;

/// Randomized election timeout bounds (ms). Wide enough to make
/// simultaneous elections unlikely, tight enough for fast failover.
const ELECTION_TIMEOUT_MIN: u64 = 150;
const ELECTION_TIMEOUT_MAX: u64 = 300;
/// How often a leader sends heartbeats (ms).
const HEARTBEAT_INTERVAL: u64 = 50;

#[derive(Debug, Error)]
#[error("node halted: {0}")]
pub struct NodeHalted(pub String);

/// What the driver of a [`Node`] must do. The node never touches the network,
/// the clock, or the state machine directly.
#[derive(Debug)]
pub enum Effect {
    /// Deliver `msg` to node `to`.
    Send { to: NodeId, msg: Message },
    /// Deliver a committed log entry to the state machine, in index order.
    /// Only committed entries are ever applied.
    Apply { index: Index, payload: Vec<u8> },
    /// Replace state-machine state with a snapshot. Only the bytes the
    /// leader's state machine produced; Raft guarantees the snapshot
    /// reflects committed entries only.
    ApplySnapshot {
        last_included_index: Index,
        last_included_term: Term,
        data: Vec<u8>,
    },
    /// The node needs snapshot bytes for `to`, whose log is behind this
    /// node's compaction horizon. The driver should obtain them from the
    /// state machine and call [`Node::provide_snapshot`].
    RequestSnapshot { to: NodeId },
    /// Answer a client write. `Committed` is emitted only after the entry
    /// committed on a quorum.
    ClientResponse { req_id: u64, result: ClientWriteResult },
}

#[derive(Clone, Debug, PartialEq)]
pub enum ClientWriteResult {
    Committed { index: Index },
    NotLeader { leader_hint: Option<NodeId> },
    /// The node lost leadership before the write committed. Retry (the
    /// server layer deduplicates by request ID).
    LeadershipLost,
}

/// A single log entry. Indices are 1-based and contiguous from
/// `base_index + 1` (entries at or below `base_index` were compacted into a
/// snapshot).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct LogEntry {
    pub term: Term,
    pub index: Index,
    pub payload: Vec<u8>,
}

/// RPC messages. Also used on the wire by `chronos-net` (bincode).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum Message {
    RequestVote {
        term: Term,
        candidate_id: NodeId,
        last_log_index: Index,
        last_log_term: Term,
    },
    RequestVoteResponse {
        term: Term,
        vote_granted: bool,
    },
    AppendEntries {
        term: Term,
        leader_id: NodeId,
        prev_log_index: Index,
        prev_log_term: Term,
        entries: Vec<LogEntry>,
        leader_commit: Index,
    },
    AppendEntriesResponse {
        term: Term,
        success: bool,
        /// On success: how much of the leader's log the follower now holds
        /// (`prev_log_index + entries.len()`). On failure: the follower's last
        /// log index, used as a backtrack hint.
        match_index: Index,
    },
    InstallSnapshot {
        term: Term,
        leader_id: NodeId,
        last_included_index: Index,
        last_included_term: Term,
        data: Vec<u8>,
    },
    InstallSnapshotResponse {
        term: Term,
    },
}

/// The durable part of a node's state.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct PersistedState {
    pub current_term: Term,
    pub voted_for: Option<NodeId>,
    pub base_index: Index,
    pub base_term: Term,
    pub log: Vec<LogEntry>,
}

/// Stable storage for [`PersistedState`]. Production uses the WAL in
/// `chronos-storage`; tests use [`sim::MemStore`].
pub trait PersistentStore {
    type Error: std::fmt::Debug;
    fn save(&mut self, state: &PersistedState) -> Result<(), Self::Error>;
    fn load(&mut self) -> Result<Option<PersistedState>, Self::Error>;
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Role {
    Follower,
    Candidate,
    Leader,
}

struct PendingWrite {
    index: Index,
    req_id: u64,
}

/// Deterministic PRNG (xorshift64*) so election timeouts are reproducible in
/// tests while still randomized per node.
#[derive(Clone, Debug)]
pub struct XorShift64(u64);

impl XorShift64 {
    pub fn new(seed: u64) -> Self {
        XorShift64(seed | 1)
    }
    pub fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x
    }
    pub fn range(&mut self, lo: u64, hi: u64) -> u64 {
        lo + self.next() % (hi - lo)
    }
}

pub struct Node<S: PersistentStore> {
    id: NodeId,
    peers: Vec<NodeId>,
    role: Role,
    current_term: Term,
    voted_for: Option<NodeId>,
    /// Entries are contiguous: `log[i].index == base_index + 1 + i`.
    log: Vec<LogEntry>,
    base_index: Index,
    base_term: Term,
    commit_index: Index,
    last_applied: Index,
    next_index: HashMap<NodeId, Index>,
    match_index: HashMap<NodeId, Index>,
    /// Index of the snapshot currently being sent to each follower.
    snapshot_inflight: HashMap<NodeId, Index>,
    votes: HashSet<NodeId>,
    election_at: u64,
    heartbeat_at: u64,
    rng: XorShift64,
    leader_id: Option<NodeId>,
    pending: Vec<PendingWrite>,
    store: S,
    halted: Option<String>,
}

impl<S: PersistentStore> Node<S> {
    /// Create a node, recovering durable state from `store` if present.
    pub fn new(id: NodeId, peers: Vec<NodeId>, mut store: S, rng_seed: u64) -> Result<Self, NodeHalted> {
        let persisted = store
            .load()
            .map_err(|e| NodeHalted(format!("persistent load failed: {e:?}")))?;
        let (current_term, voted_for, base_index, base_term, log) = match persisted {
            Some(p) => (p.current_term, p.voted_for, p.base_index, p.base_term, p.log),
            None => (0, None, 0, 0, Vec::new()),
        };
        let mut rng = XorShift64::new(rng_seed);
        let election_at = rng.range(ELECTION_TIMEOUT_MIN, ELECTION_TIMEOUT_MAX);
        Ok(Node {
            id,
            peers,
            role: Role::Follower,
            current_term,
            voted_for,
            log,
            base_index,
            base_term,
            commit_index: 0,
            last_applied: 0,
            next_index: HashMap::new(),
            match_index: HashMap::new(),
            snapshot_inflight: HashMap::new(),
            votes: HashSet::new(),
            election_at,
            heartbeat_at: 0,
            rng,
            leader_id: None,
            pending: Vec::new(),
            store,
            halted: None,
        })
    }

    // -- introspection (used by drivers and tests) -------------------------

    pub fn id(&self) -> NodeId {
        self.id
    }
    pub fn role(&self) -> Role {
        self.role
    }
    pub fn term(&self) -> Term {
        self.current_term
    }
    pub fn commit_index(&self) -> Index {
        self.commit_index
    }
    pub fn last_index(&self) -> Index {
        self.base_index + self.log.len() as u64
    }
    pub fn leader_id(&self) -> Option<NodeId> {
        self.leader_id
    }
    pub fn is_halted(&self) -> bool {
        self.halted.is_some()
    }
    pub fn snapshot_status(&self) -> (Index, Term) {
        (self.base_index, self.base_term)
    }

    /// Log entries with index in `(from, to]`, for state-machine replay on
    /// startup. Read-only; does not affect Raft state.
    pub fn log_entries(&self, from: Index, to: Index) -> Vec<LogEntry> {
        self.log
            .iter()
            .filter(|e| e.index > from && e.index <= to)
            .cloned()
            .collect()
    }
    pub fn cluster_size(&self) -> usize {
        self.peers.len() + 1
    }
    pub fn majority(&self) -> usize {
        self.cluster_size() / 2 + 1
    }

    // -- internal helpers --------------------------------------------------

    fn last_term(&self) -> Term {
        self.log.last().map(|e| e.term).unwrap_or(self.base_term)
    }

    /// Term of the entry at `index`. `Some(0)` for the genesis prefix (0).
    /// `None` for compacted-away or beyond-end indices.
    fn term_at(&self, index: Index) -> Option<Term> {
        if index == 0 {
            return Some(0);
        }
        if index == self.base_index {
            return Some(self.base_term);
        }
        if index < self.base_index {
            return None;
        }
        self.log
            .get((index - self.base_index - 1) as usize)
            .map(|e| e.term)
    }

    fn entry(&self, index: Index) -> Option<&LogEntry> {
        if index <= self.base_index {
            return None;
        }
        self.log.get((index - self.base_index - 1) as usize)
    }

    fn reset_election_timer(&mut self, now: u64) {
        self.election_at = now + self.rng.range(ELECTION_TIMEOUT_MIN, ELECTION_TIMEOUT_MAX);
    }

    /// Save durable state. On failure the node halts: it must not vote,
    /// replicate, or acknowledge anything it cannot prove is on disk.
    fn save_persistent(&mut self) -> Result<(), NodeHalted> {
        let state = PersistedState {
            current_term: self.current_term,
            voted_for: self.voted_for,
            base_index: self.base_index,
            base_term: self.base_term,
            log: self.log.clone(),
        };
        match self.store.save(&state) {
            Ok(()) => Ok(()),
            Err(e) => {
                let msg = format!("persistent store failed: {e:?}");
                self.halted = Some(msg.clone());
                Err(NodeHalted(msg))
            }
        }
    }

    fn step_down(&mut self, term: Term, now: u64, fx: &mut Vec<Effect>) -> Result<(), NodeHalted> {
        let was_leader = self.role == Role::Leader;
        self.role = Role::Follower;
        self.current_term = term;
        self.voted_for = None;
        self.leader_id = None;
        self.votes.clear();
        self.next_index.clear();
        self.match_index.clear();
        self.snapshot_inflight.clear();
        self.reset_election_timer(now);
        self.save_persistent()?;
        if was_leader {
            for p in self.pending.drain(..) {
                fx.push(Effect::ClientResponse {
                    req_id: p.req_id,
                    result: ClientWriteResult::LeadershipLost,
                });
            }
        }
        Ok(())
    }

    // -- message handlers --------------------------------------------------

    pub fn on_message(
        &mut self,
        from: NodeId,
        msg: Message,
        now: u64,
        fx: &mut Vec<Effect>,
    ) -> Result<(), NodeHalted> {
        if self.halted.is_some() {
            return Err(NodeHalted("node is halted".to_string()));
        }
        match msg {
            Message::RequestVote { term, candidate_id, last_log_index, last_log_term } => {
                self.on_request_vote(from, now, term, candidate_id, last_log_index, last_log_term, fx)
            }
            Message::RequestVoteResponse { term, vote_granted } => {
                self.on_request_vote_response(from, now, term, vote_granted, fx)
            }
            Message::AppendEntries { term, leader_id, prev_log_index, prev_log_term, entries, leader_commit } => {
                self.on_append_entries(from, now, term, leader_id, prev_log_index, prev_log_term, entries, leader_commit, fx)
            }
            Message::AppendEntriesResponse { term, success, match_index } => {
                self.on_append_entries_response(from, now, term, success, match_index, fx)
            }
            Message::InstallSnapshot { term, leader_id, last_included_index, last_included_term, data } => {
                self.on_install_snapshot(from, now, term, leader_id, last_included_index, last_included_term, data, fx)
            }
            Message::InstallSnapshotResponse { term } => {
                self.on_install_snapshot_response(from, now, term, fx)
            }
        }
    }

    fn on_request_vote(
        &mut self,
        from: NodeId,
        now: u64,
        term: Term,
        candidate_id: NodeId,
        last_log_index: Index,
        last_log_term: Term,
        fx: &mut Vec<Effect>,
    ) -> Result<(), NodeHalted> {
        if term < self.current_term {
            fx.push(Effect::Send {
                to: from,
                msg: Message::RequestVoteResponse { term: self.current_term, vote_granted: false },
            });
            return Ok(());
        }
        if term > self.current_term {
            self.step_down(term, now, fx)?;
        }
        let our_last_term = self.last_term();
        let our_last_index = self.last_index();
        let up_to_date = last_log_term > our_last_term
            || (last_log_term == our_last_term && last_log_index >= our_last_index);
        let grant = (self.voted_for.is_none() || self.voted_for == Some(candidate_id)) && up_to_date;
        if grant {
            self.voted_for = Some(candidate_id);
            self.save_persistent()?;
            self.reset_election_timer(now);
        }
        fx.push(Effect::Send {
            to: from,
            msg: Message::RequestVoteResponse { term: self.current_term, vote_granted: grant },
        });
        Ok(())
    }

    fn on_request_vote_response(
        &mut self,
        from: NodeId,
        now: u64,
        term: Term,
        granted: bool,
        fx: &mut Vec<Effect>,
    ) -> Result<(), NodeHalted> {
        if term > self.current_term {
            return self.step_down(term, now, fx);
        }
        if term < self.current_term || self.role != Role::Candidate {
            return Ok(());
        }
        if granted {
            self.votes.insert(from);
            if self.votes.len() >= self.majority() {
                self.become_leader(now, fx)?;
            }
        }
        Ok(())
    }

    fn on_append_entries(
        &mut self,
        from: NodeId,
        now: u64,
        term: Term,
        leader_id: NodeId,
        prev_log_index: Index,
        prev_log_term: Term,
        entries: Vec<LogEntry>,
        leader_commit: Index,
        fx: &mut Vec<Effect>,
    ) -> Result<(), NodeHalted> {
        let reject = |s: &Self, fx: &mut Vec<Effect>| {
            fx.push(Effect::Send {
                to: from,
                msg: Message::AppendEntriesResponse {
                    term: s.current_term,
                    success: false,
                    match_index: s.last_index(),
                },
            });
        };
        if term < self.current_term {
            reject(self, fx);
            return Ok(());
        }
        if term > self.current_term || self.role != Role::Follower {
            self.step_down(term, now, fx)?;
        }
        self.leader_id = Some(leader_id);
        self.reset_election_timer(now);

        // Log matching: our log must contain prev_log_index with prev_log_term.
        // (prev_log_index == 0 is the genesis marker and only matches when we
        // hold no snapshot; otherwise prev_log_index must be >= base_index.)
        let log_ok = match self.term_at(prev_log_index) {
            Some(t) if t == prev_log_term => {
                prev_log_index == 0 || prev_log_index >= self.base_index
            }
            _ => false,
        };
        if !log_ok {
            reject(self, fx);
            return Ok(());
        }
        // Merge entries: skip the prefix we already store identically, truncate
        // only on a genuine conflict (same index, different term), then append
        // the rest. Unconditional truncation would let a stale or duplicated
        // RPC delete entries we already hold.
        let mut start = 0;
        while start < entries.len() {
            let e = &entries[start];
            debug_assert!(e.index > self.base_index);
            match self.term_at(e.index) {
                Some(t) if t == e.term => start += 1,
                Some(_) => {
                    let keep = (e.index - self.base_index - 1) as usize;
                    self.log.truncate(keep);
                    break;
                }
                None => break, // beyond our log; append from here
            }
        }
        for e in &entries[start..] {
            debug_assert_eq!(e.index, self.base_index + self.log.len() as u64 + 1);
            self.log.push(e.clone());
        }
        self.save_persistent()?;

        if leader_commit > self.commit_index {
            let new_commit = leader_commit.min(self.last_index());
            self.advance_commit(new_commit, fx);
        }
        // Report exactly how much of the *leader's* log we now hold, not our
        // absolute last index (we may hold extra uncommitted entries from an
        // old leader that the leader must not count).
        let matched = prev_log_index + entries.len() as u64;
        fx.push(Effect::Send {
            to: from,
            msg: Message::AppendEntriesResponse {
                term: self.current_term,
                success: true,
                match_index: matched,
            },
        });
        Ok(())
    }

    fn on_append_entries_response(
        &mut self,
        from: NodeId,
        now: u64,
        term: Term,
        success: bool,
        match_index: Index,
        fx: &mut Vec<Effect>,
    ) -> Result<(), NodeHalted> {
        if term > self.current_term {
            return self.step_down(term, now, fx);
        }
        if term < self.current_term || self.role != Role::Leader {
            return Ok(());
        }
        if success {
            let m = self.match_index.entry(from).or_insert(0);
            *m = (*m).max(match_index);
            self.next_index.insert(from, match_index + 1);
            self.snapshot_inflight.remove(&from);
            self.try_advance_commit(fx);
        } else {
            // Back up one entry at a time; strictly decreases, so this
            // converges. If we reach the compaction horizon, the follower
            // needs entries we no longer have: snapshot it instead of
            // retrying AppendEntries forever.
            let next = self.next_index.entry(from).or_insert(self.base_index + 1);
            if *next > self.base_index + 1 {
                *next -= 1;
                self.send_append(from, fx);
            } else {
                self.snapshot_inflight.insert(from, self.base_index);
                fx.push(Effect::RequestSnapshot { to: from });
            }
        }
        Ok(())
    }

    fn on_install_snapshot(
        &mut self,
        from: NodeId,
        now: u64,
        term: Term,
        leader_id: NodeId,
        last_included_index: Index,
        last_included_term: Term,
        data: Vec<u8>,
        fx: &mut Vec<Effect>,
    ) -> Result<(), NodeHalted> {
        if term < self.current_term {
            fx.push(Effect::Send {
                to: from,
                msg: Message::InstallSnapshotResponse { term: self.current_term },
            });
            return Ok(());
        }
        if term > self.current_term || self.role != Role::Follower {
            self.step_down(term, now, fx)?;
        }
        self.leader_id = Some(leader_id);
        self.reset_election_timer(now);

        self.log.clear();
        self.base_index = last_included_index;
        self.base_term = last_included_term;
        self.save_persistent()?;
        if last_included_index > self.commit_index {
            self.commit_index = last_included_index;
        }
        if last_included_index > self.last_applied {
            self.last_applied = last_included_index;
        }
        fx.push(Effect::ApplySnapshot {
            last_included_index,
            last_included_term,
            data,
        });
        fx.push(Effect::Send {
            to: from,
            msg: Message::InstallSnapshotResponse { term: self.current_term },
        });
        Ok(())
    }

    fn on_install_snapshot_response(
        &mut self,
        from: NodeId,
        now: u64,
        term: Term,
        fx: &mut Vec<Effect>,
    ) -> Result<(), NodeHalted> {
        if term > self.current_term {
            return self.step_down(term, now, fx);
        }
        if term < self.current_term || self.role != Role::Leader {
            return Ok(());
        }
        if let Some(sent_idx) = self.snapshot_inflight.remove(&from) {
            let m = self.match_index.entry(from).or_insert(0);
            *m = (*m).max(sent_idx);
            self.next_index.insert(from, sent_idx + 1);
            self.try_advance_commit(fx);
        }
        Ok(())
    }

    // -- leader logic ------------------------------------------------------

    fn become_leader(&mut self, now: u64, fx: &mut Vec<Effect>) -> Result<(), NodeHalted> {
        self.role = Role::Leader;
        self.leader_id = Some(self.id);
        let last = self.last_index();
        let peers: Vec<NodeId> = self.peers.clone();
        for p in peers.iter().chain(std::iter::once(&self.id)) {
            self.next_index.insert(*p, last + 1);
            self.match_index.insert(*p, 0);
        }
        // No-op entry in the new term: lets prior-term entries commit
        // transitively (required for §5.4.2 correctness). The state machine
        // treats an empty payload as a no-op.
        let idx = last + 1;
        self.log.push(LogEntry { term: self.current_term, index: idx, payload: Vec::new() });
        self.save_persistent()?;
        self.match_index.insert(self.id, idx);
        self.heartbeat_at = now + HEARTBEAT_INTERVAL;
        let peers: Vec<NodeId> = self.peers.clone();
        for p in peers {
            self.send_append(p, fx);
        }
        Ok(())
    }

    /// Send AppendEntries (or a snapshot request) to one follower, based on
    /// what we believe it has.
    fn send_append(&mut self, to: NodeId, fx: &mut Vec<Effect>) {
        let next = self.next_index.get(&to).copied().unwrap_or(self.base_index + 1);
        if next <= self.base_index {
            // Follower needs entries we compacted away: snapshot it.
            self.snapshot_inflight.insert(to, self.base_index);
            fx.push(Effect::RequestSnapshot { to });
            return;
        }
        let prev_log_index = next - 1;
        let prev_log_term = self.term_at(prev_log_index).unwrap_or(0);
        let entries: Vec<LogEntry> = self
            .log
            .iter()
            .filter(|e| e.index >= next)
            .cloned()
            .collect();
        fx.push(Effect::Send {
            to,
            msg: Message::AppendEntries {
                term: self.current_term,
                leader_id: self.id,
                prev_log_index,
                prev_log_term,
                entries,
                leader_commit: self.commit_index,
            },
        });
    }

    /// Advance the commit index following §5.4.2: only current-term entries
    /// may be committed by counting replicas; older entries commit
    /// transitively.
    fn try_advance_commit(&mut self, fx: &mut Vec<Effect>) {
        let majority = self.majority();
        let mut new_commit = self.commit_index;
        for n in (self.commit_index + 1)..=self.last_index() {
            let term = self.term_at(n).expect("commit scan: entry must exist");
            if term != self.current_term {
                continue;
            }
            let count = std::iter::once(&self.id)
                .chain(self.peers.iter())
                .filter(|id| self.match_index.get(id).copied().unwrap_or(0) >= n)
                .count();
            if count >= majority {
                new_commit = n;
            }
        }
        if new_commit > self.commit_index {
            self.advance_commit(new_commit, fx);
        }
    }

    /// Move commit_index forward and emit Apply / ClientResponse effects.
    /// Precondition: `new_commit` is committed on a quorum.
    fn advance_commit(&mut self, new_commit: Index, fx: &mut Vec<Effect>) {
        assert!(
            new_commit >= self.commit_index,
            "commit index must never go backwards"
        );
        self.commit_index = new_commit;
        while self.last_applied < self.commit_index {
            self.last_applied += 1;
            let payload = self
                .entry(self.last_applied)
                .expect("applied entry must exist in the log")
                .payload
                .clone();
            // Empty payload = leader's no-op entry: committed, but the state
            // machine has nothing to do.
            if !payload.is_empty() {
                fx.push(Effect::Apply {
                    index: self.last_applied,
                    payload: payload.clone(),
                });
            }
            if let Some(pos) = self.pending.iter().position(|p| p.index == self.last_applied) {
                let p = self.pending.remove(pos);
                fx.push(Effect::ClientResponse {
                    req_id: p.req_id,
                    result: ClientWriteResult::Committed { index: p.index },
                });
            }
        }
    }

    // -- timers and client -------------------------------------------------

    /// Advance time. Fires election timeouts and leader heartbeats.
    pub fn tick(&mut self, now: u64, fx: &mut Vec<Effect>) -> Result<(), NodeHalted> {
        if self.halted.is_some() {
            return Err(NodeHalted("node is halted".to_string()));
        }
        match self.role {
            Role::Leader => {
                if now >= self.heartbeat_at {
                    self.heartbeat_at = now + HEARTBEAT_INTERVAL;
                    let peers: Vec<NodeId> = self.peers.clone();
                    for p in peers {
                        self.send_append(p, fx);
                    }
                }
            }
            _ => {
                if now >= self.election_at {
                    self.start_election(now, fx)?;
                }
            }
        }
        Ok(())
    }

    fn start_election(&mut self, now: u64, fx: &mut Vec<Effect>) -> Result<(), NodeHalted> {
        self.role = Role::Candidate;
        self.current_term += 1;
        self.voted_for = Some(self.id);
        self.leader_id = None;
        self.votes.clear();
        self.votes.insert(self.id);
        self.save_persistent()?;
        self.reset_election_timer(now);
        let msg = Message::RequestVote {
            term: self.current_term,
            candidate_id: self.id,
            last_log_index: self.last_index(),
            last_log_term: self.last_term(),
        };
        if self.peers.is_empty() {
            // Single-node cluster wins immediately.
            self.become_leader(now, fx)?;
            return Ok(());
        }
        let peers: Vec<NodeId> = self.peers.clone();
        for p in peers {
            fx.push(Effect::Send { to: p, msg: msg.clone() });
        }
        Ok(())
    }

    /// Submit a client write. Only the leader accepts it; it is appended,
    /// persisted, and replicated, and the client is answered only after a
    /// quorum commits it.
    pub fn client_write(
        &mut self,
        req_id: u64,
        payload: Vec<u8>,
        fx: &mut Vec<Effect>,
    ) -> Result<(), NodeHalted> {
        if self.halted.is_some() {
            return Err(NodeHalted("node is halted".to_string()));
        }
        if self.role != Role::Leader {
            fx.push(Effect::ClientResponse {
                req_id,
                result: ClientWriteResult::NotLeader { leader_hint: self.leader_id },
            });
            return Ok(());
        }
        let index = self.last_index() + 1;
        self.log.push(LogEntry { term: self.current_term, index, payload });
        self.save_persistent()?;
        self.match_index.insert(self.id, index);
        self.pending.push(PendingWrite { index, req_id });
        // Replicate immediately instead of waiting for the next heartbeat.
        let peers: Vec<NodeId> = self.peers.clone();
        for p in peers {
            self.send_append(p, fx);
        }
        // Single-node cluster commits instantly.
        if self.peers.is_empty() {
            self.try_advance_commit(fx);
        }
        Ok(())
    }

    // -- snapshots (driver-assisted) ---------------------------------------

    /// The driver calls this after durably snapshotting state-machine state
    /// through `commit_index`. The log prefix is discarded.
    pub fn compact_log(&mut self, last_included_index: Index) -> Result<(), NodeHalted> {
        assert!(
            last_included_index <= self.commit_index,
            "cannot compact uncommitted entries"
        );
        assert!(
            last_included_index >= self.base_index,
            "cannot compact below the existing base"
        );
        if last_included_index == self.base_index {
            return Ok(());
        }
        let drop_n = (last_included_index - self.base_index) as usize;
        let term = self.log[drop_n - 1].term;
        self.log.drain(..drop_n);
        self.base_index = last_included_index;
        self.base_term = term;
        self.save_persistent()?;
        Ok(())
    }

    /// The driver calls this in response to [`Effect::RequestSnapshot`],
    /// supplying the state machine's snapshot bytes.
    pub fn provide_snapshot(&mut self, to: NodeId, data: Vec<u8>, fx: &mut Vec<Effect>) {
        self.snapshot_inflight.insert(to, self.base_index);
        fx.push(Effect::Send {
            to,
            msg: Message::InstallSnapshot {
                term: self.current_term,
                leader_id: self.id,
                last_included_index: self.base_index,
                last_included_term: self.base_term,
                data,
            },
        });
    }
}
