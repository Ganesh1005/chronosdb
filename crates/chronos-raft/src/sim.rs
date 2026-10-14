//! Deterministic simulation harness for Raft.
//!
//! [`Sim`] drives [`Node`]s with a virtual clock and an explicit network, so
//! tests control exactly when messages arrive. Faults are injectable and
//! reproducible: partitions, message drop / duplication / delay / reorder,
//! node crash + restart (durable state survives via the node's store), node
//! pause, and storage faults.
//!
//! The harness independently verifies the core safety invariants on every
//! applied entry and every client acknowledgement:
//!
//! * an applied entry must exist with identical (term, payload) on a quorum of
//!   nodes (live logs, retained applied history, or durable stores),
//! * an acknowledged write must have had quorum replication at ack time,
//! * no index is ever applied with *divergent* data; re-applying identical
//!   (term, payload) after a restart is legal (commit_index/last_applied are
//!   volatile in Raft) and is why real state machines must apply idempotently.

use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::sync::{Arc, Mutex};

use super::{
    ClientWriteResult, Effect, Index, Message, Node, NodeId, PersistedState, PersistentStore,
    Role, Term, XorShift64,
};

/// In-memory [`PersistentStore`] with injectable save faults. Cloneable: all
/// clones share the same underlying durable state, which is what makes
/// crash/restart testing faithful (the restarted node reloads the same bytes).
#[derive(Clone, Debug)]
pub struct MemStore {
    inner: Arc<Mutex<MemStoreInner>>,
}

#[derive(Debug)]
struct MemStoreInner {
    state: Option<PersistedState>,
    fail_saves: bool,
    saves: u64,
}

impl MemStore {
    pub fn new() -> Self {
        MemStore {
            inner: Arc::new(Mutex::new(MemStoreInner {
                state: None,
                fail_saves: false,
                saves: 0,
            })),
        }
    }

    pub fn set_fail_saves(&self, fail: bool) {
        self.inner.lock().unwrap().fail_saves = fail;
    }

    pub fn save_count(&self) -> u64 {
        self.inner.lock().unwrap().saves
    }

    pub fn saved_state(&self) -> Option<PersistedState> {
        self.inner.lock().unwrap().state.clone()
    }
}

impl Default for MemStore {
    fn default() -> Self {
        Self::new()
    }
}

impl PersistentStore for MemStore {
    type Error = String;

    fn save(&mut self, state: &PersistedState) -> Result<(), String> {
        let mut inner = self.inner.lock().unwrap();
        if inner.fail_saves {
            return Err("injected storage fault".to_string());
        }
        inner.state = Some(state.clone());
        inner.saves += 1;
        Ok(())
    }

    fn load(&mut self) -> Result<Option<PersistedState>, String> {
        Ok(self.inner.lock().unwrap().state.clone())
    }
}

fn pair(a: NodeId, b: NodeId) -> (NodeId, NodeId) {
    (a.min(b), a.max(b))
}

/// Deterministic Raft cluster simulation.
pub struct Sim {
    nodes: HashMap<NodeId, Node<MemStore>>,
    stores: HashMap<NodeId, MemStore>,
    peers_of: HashMap<NodeId, Vec<NodeId>>,
    net: HashMap<(NodeId, NodeId), VecDeque<(u64, Message)>>,
    partitions: HashSet<(NodeId, NodeId)>,
    paused: HashSet<NodeId>,
    /// Retained applied history per node (never pruned): index -> (term, payload).
    applied: HashMap<NodeId, BTreeMap<Index, (Term, Vec<u8>)>>,
    snapshots: HashMap<NodeId, (Index, Term)>,
    now: u64,
    rng: XorShift64,
    drop_rate_pct: u64,
    dup_rate_pct: u64,
    /// req_id -> (answering node, result). Only the latest response per req_id.
    pub responses: HashMap<u64, (NodeId, ClientWriteResult)>,
    pub drops: u64,
}

impl Sim {
    pub fn new(ids: &[NodeId], seed: u64) -> Self {
        let mut sim = Sim {
            nodes: HashMap::new(),
            stores: HashMap::new(),
            peers_of: HashMap::new(),
            net: HashMap::new(),
            partitions: HashSet::new(),
            paused: HashSet::new(),
            applied: HashMap::new(),
            snapshots: HashMap::new(),
            now: 0,
            rng: XorShift64::new(seed),
            drop_rate_pct: 0,
            dup_rate_pct: 0,
            responses: HashMap::new(),
            drops: 0,
        };
        for &id in ids {
            let peers: Vec<NodeId> = ids.iter().copied().filter(|p| *p != id).collect();
            let store = MemStore::new();
            let node = Node::new(id, peers.clone(), store.clone(), seed + id * 0x9e37)
                .expect("node creation");
            sim.peers_of.insert(id, peers);
            sim.nodes.insert(id, node);
            sim.stores.insert(id, store);
            sim.applied.insert(id, BTreeMap::new());
        }
        sim
    }

    pub fn now(&self) -> u64 {
        self.now
    }

    pub fn node_ids(&self) -> Vec<NodeId> {
        let mut ids: Vec<NodeId> = self.peers_of.keys().copied().collect();
        ids.sort_unstable();
        ids
    }

    pub fn majority(&self) -> usize {
        self.node_ids().len() / 2 + 1
    }

    // -- fault injection -------------------------------------------------

    /// Block all traffic between a and b (both directions).
    pub fn partition(&mut self, a: NodeId, b: NodeId) {
        self.partitions.insert(pair(a, b));
    }

    pub fn heal(&mut self, a: NodeId, b: NodeId) {
        self.partitions.remove(&pair(a, b));
    }

    pub fn heal_all(&mut self) {
        self.partitions.clear();
    }

    /// Drop the next queued message from `from` to `to`.
    pub fn drop_next(&mut self, from: NodeId, to: NodeId) {
        if let Some(q) = self.net.get_mut(&(from, to)) {
            if q.pop_front().is_some() {
                self.drops += 1;
            }
        }
    }

    /// Duplicate the next queued message from `from` to `to`.
    pub fn duplicate_next(&mut self, from: NodeId, to: NodeId) {
        if let Some(q) = self.net.get_mut(&(from, to)) {
            if let Some((at, msg)) = q.front().cloned() {
                q.push_back((at + 1, msg));
            }
        }
    }

    /// Swap the next two queued messages from `from` to `to` (reorder).
    pub fn reorder_next(&mut self, from: NodeId, to: NodeId) {
        if let Some(q) = self.net.get_mut(&(from, to)) {
            if q.len() >= 2 {
                let a = q.pop_front().unwrap();
                let b = q.pop_front().unwrap();
                // Deliver b first, then a (keep original timestamps).
                q.push_front((b.0.min(a.0), a.1));
                q.push_front((b.0.min(a.0), b.1));
            }
        }
    }

    /// Random packet loss / duplication for chaos tests (percent 0..=100).
    pub fn set_chaos(&mut self, drop_pct: u64, dup_pct: u64) {
        self.drop_rate_pct = drop_pct.min(100);
        self.dup_rate_pct = dup_pct.min(100);
    }

    /// Crash a node: it stops ticking and its queued inbound messages are
    /// dropped. Durable state survives in its store.
    pub fn crash(&mut self, id: NodeId) {
        self.nodes.remove(&id);
        self.net.retain(|(from, to), _| *from != id && *to != id);
    }

    /// Restart a crashed node, reloading durable state from its store.
    pub fn restart(&mut self, id: NodeId, seed: u64) {
        let peers = self.peers_of[&id].clone();
        let store = self.stores[&id].clone();
        let node = Node::new(id, peers, store, seed).expect("restart");
        self.nodes.insert(id, node);
    }

    pub fn pause(&mut self, id: NodeId) {
        self.paused.insert(id);
    }

    pub fn resume(&mut self, id: NodeId) {
        self.paused.remove(&id);
    }

    pub fn is_live(&self, id: NodeId) -> bool {
        self.nodes.contains_key(&id)
    }

    pub fn fail_saves(&mut self, id: NodeId, fail: bool) {
        self.stores[&id].set_fail_saves(fail);
    }

    // -- driving ----------------------------------------------------------

    fn net_send(&mut self, from: NodeId, to: NodeId, msg: Message) {
        if !self.nodes.contains_key(&to) {
            return; // crashed: message lost
        }
        if self.partitions.contains(&pair(from, to)) {
            self.drops += 1;
            return;
        }
        if self.rng.next() % 100 < self.drop_rate_pct {
            self.drops += 1;
            return;
        }
        let latency = 1 + self.rng.range(0, 5);
        let q = self.net.entry((from, to)).or_default();
        q.push_back((self.now + latency, msg.clone()));
        if self.rng.next() % 100 < self.dup_rate_pct {
            q.push_back((self.now + latency + self.rng.range(0, 3), msg));
        }
    }

    fn process_effects(&mut self, id: NodeId, fx: Vec<Effect>) {
        let mut snapshot_requests = Vec::new();
        for e in fx {
            match e {
                Effect::Send { to, msg } => self.net_send(id, to, msg),
                Effect::Apply { index, payload } => {
                    let term = self.nodes[&id]
                        .term_at(index)
                        .expect("applied entry must be in the log");
                    assert!(
                        self.entry_on_quorum(index, term, &payload, Some(id)),
                        "node {id} applied uncommitted entry at index {index}"
                    );
                    // Re-applying identical data after a restart is legal (state
                    // machines must be idempotent); divergent data is not.
                    if let Some((t, p)) = self.applied.get(&id).unwrap().get(&index) {
                        assert!(
                            *t == term && *p == payload,
                            "node {id} applied DIVERGENT data at index {index}"
                        );
                    } else {
                        self.applied.get_mut(&id).unwrap().insert(index, (term, payload));
                    }
                }
                Effect::ApplySnapshot { last_included_index, last_included_term, .. } => {
                    self.snapshots.insert(id, (last_included_index, last_included_term));
                }
                Effect::RequestSnapshot { to } => snapshot_requests.push(to),
                Effect::ClientResponse { req_id, result } => {
                    if let ClientWriteResult::Committed { index } = &result {
                        let node = &self.nodes[&id];
                        let entry = node.entry(*index).expect("acked entry must exist");
                        assert!(
                            self.entry_on_quorum(*index, entry.term, &entry.payload, Some(id)),
                            "node {id} acknowledged uncommitted write at index {index}"
                        );
                    }
                    self.responses.insert(req_id, (id, result));
                }
            }
        }
        for to in snapshot_requests {
            let (base_index, base_term) = {
                let n = &self.nodes[&id];
                n.snapshot_status()
            };
            let data = format!("snapshot@{base_index}:{base_term}").into_bytes();
            let mut fx = Vec::new();
            self.nodes
                .get_mut(&id)
                .unwrap()
                .provide_snapshot(to, data, &mut fx);
            self.process_effects(id, fx);
        }
    }

    /// Does a quorum of the cluster hold exactly (index, term, payload)?
    /// Checks live logs, retained applied history, and crashed nodes' durable
    /// stores (a crashed quorum member still counts: it had the entry).
    fn entry_on_quorum(
        &self,
        index: Index,
        term: Term,
        payload: &[u8],
        _exclude: Option<NodeId>,
    ) -> bool {
        let mut count = 0;
        for id in self.node_ids() {
            let mut has = false;
            if let Some(n) = self.nodes.get(&id) {
                if let Some(e) = n.entry(index) {
                    has = e.term == term && e.payload == payload;
                }
            }
            if !has {
                if let Some((t, p)) = self.applied[&id].get(&index) {
                    has = *t == term && p == payload;
                }
            }
            if !has {
                // Crashed node's durable state.
                if let Some(saved) = self.stores[&id].saved_state() {
                    has = saved.log.iter().any(|e| {
                        e.index == index && e.term == term && e.payload == payload
                    });
                }
            }
            if has {
                count += 1;
            }
        }
        count >= self.majority()
    }

    fn step_ms(&mut self) {
        // 1. Tick live, unpaused, non-halted nodes.
        let ids: Vec<NodeId> = self.nodes.keys().copied().collect();
        for id in ids {
            if self.paused.contains(&id) {
                continue;
            }
            let mut fx = Vec::new();
            let res = self.nodes.get_mut(&id).unwrap().tick(self.now, &mut fx);
            match res {
                Ok(()) => self.process_effects(id, fx),
                Err(_) => {
                    // Halted (e.g. storage fault): the node stops participating.
                }
            }
        }
        // 2. Deliver due messages.
        let mut due = Vec::new();
        let keys: Vec<(NodeId, NodeId)> = self.net.keys().copied().collect();
        for k in keys {
            let (_, to) = k;
            if self.paused.contains(&to) || !self.nodes.contains_key(&to) {
                continue;
            }
            if let Some(q) = self.net.get_mut(&k) {
                while let Some((at, _)) = q.front() {
                    if *at > self.now {
                        break;
                    }
                    let (_, msg) = q.pop_front().unwrap();
                    due.push((k.0, to, msg));
                }
            }
        }
        for (from, to, msg) in due {
            let mut fx = Vec::new();
            let res = self
                .nodes
                .get_mut(&to)
                .unwrap()
                .on_message(from, msg, self.now, &mut fx);
            if res.is_ok() {
                self.process_effects(to, fx);
            }
        }
        self.now += 1;
    }

    pub fn run_for(&mut self, ms: u64) {
        for _ in 0..ms {
            self.step_ms();
        }
    }

    /// The current leader, if exactly one exists. Panics if two nodes both
    /// consider themselves leader *in the same term* (a safety violation).
    /// Halted nodes are ignored.
    pub fn leader(&self) -> Option<NodeId> {
        let mut by_term: HashMap<Term, Vec<NodeId>> = HashMap::new();
        for (id, n) in &self.nodes {
            if n.role() == Role::Leader && !self.paused.contains(id) && !n.is_halted() {
                by_term.entry(n.term()).or_default().push(*id);
            }
        }
        for (term, ids) in &by_term {
            assert!(
                ids.len() <= 1,
                "SAFETY VIOLATION: two leaders in term {term}: {ids:?}"
            );
        }
        by_term.values().next().and_then(|v| v.first().copied())
    }

    /// Leader among a subset of nodes (e.g. the non-partitioned side).
    /// A partitioned-away old leader in an older term does not count.
    pub fn leader_in(&self, ids: &[NodeId]) -> Option<NodeId> {
        let mut best: Option<(Term, NodeId)> = None;
        for id in ids {
            if let Some(n) = self.nodes.get(id) {
                if n.role() == Role::Leader && !self.paused.contains(id) && !n.is_halted() {
                    let term = n.term();
                    if best.map(|(t, _)| term > t).unwrap_or(true) {
                        best = Some((term, *id));
                    }
                }
            }
        }
        best.map(|(_, id)| id)
    }

    pub fn run_until_leader(&mut self, timeout_ms: u64) -> NodeId {
        let start = self.now;
        while self.now - start < timeout_ms {
            if let Some(l) = self.leader() {
                return l;
            }
            self.run_for(10);
        }
        panic!("no leader elected within {timeout_ms}ms");
    }

    pub fn client_write(&mut self, to: NodeId, req_id: u64, payload: Vec<u8>) {
        let mut fx = Vec::new();
        let res = self
            .nodes
            .get_mut(&to)
            .unwrap()
            .client_write(req_id, payload, &mut fx);
        if res.is_ok() {
            self.process_effects(to, fx);
        }
    }

    /// Compact a node's log through `index` (simulates the server taking a
    /// state-machine snapshot and handing it to Raft).
    pub fn compact_log(&mut self, id: NodeId, index: Index) {
        self.nodes
            .get_mut(&id)
            .unwrap()
            .compact_log(index)
            .expect("compact ok");
    }

    /// Assert no two nodes applied different (term, payload) at the same
    /// index, and that applied entries in live logs still match.
    pub fn assert_no_divergent_commits(&self) {
        let mut at_index: HashMap<Index, (Term, Vec<u8>, NodeId)> = HashMap::new();
        for (id, map) in &self.applied {
            for (idx, (term, payload)) in map {
                if payload.is_empty() {
                    continue; // leader no-op entries are never surfaced
                }
                if let Some((t, p, other)) = at_index.get(idx) {
                    assert!(
                        t == term && p == payload,
                        "DIVERGENT COMMIT at index {idx}: node {other} applied ({t}, {p:?}), node {id} applied ({term}, {payload:?})"
                    );
                } else {
                    at_index.insert(*idx, (*term, payload.clone(), *id));
                }
            }
        }
        // Cross-check: a live node's log must not contradict an applied entry.
        for (id, n) in &self.nodes {
            for (idx, (term, payload, _)) in at_index.iter() {
                if let Some(e) = n.entry(*idx) {
                    assert!(
                        &e.term == term && &e.payload == payload,
                        "node {id} log contradicts committed entry at index {idx}"
                    );
                }
            }
        }
    }

    pub fn applied_on(&self, id: NodeId) -> Vec<(Index, Term, Vec<u8>)> {
        self.applied[&id]
            .iter()
            .map(|(i, (t, p))| (*i, *t, p.clone()))
            .collect()
    }

    pub fn role_of(&self, id: NodeId) -> Option<Role> {
        self.nodes.get(&id).map(|n| n.role())
    }

    pub fn term_of(&self, id: NodeId) -> Option<Term> {
        self.nodes.get(&id).map(|n| n.term())
    }
}

#[cfg(test)]
mod tests {
    use super::super::{LogEntry, Node};
    use super::*;

    fn cluster3(seed: u64) -> Sim {
        Sim::new(&[1, 2, 3], seed)
    }

    #[test]
    fn elects_a_single_leader() {
        let mut sim = cluster3(7);
        let leader = sim.run_until_leader(3000);
        // Exactly one leader, everyone agrees on the term.
        let term = sim.term_of(leader).unwrap();
        for id in sim.node_ids() {
            assert_eq!(sim.term_of(id).unwrap(), term);
        }
        assert!(matches!(sim.role_of(leader), Some(Role::Leader)));
        // Leadership is stable for a while (no churn without faults).
        sim.run_for(2000);
        assert_eq!(sim.leader(), Some(leader));
    }

    #[test]
    fn replicates_client_write_to_all_nodes() {
        let mut sim = cluster3(11);
        let leader = sim.run_until_leader(3000);
        sim.client_write(leader, 1, b"hello".to_vec());
        sim.run_for(500);
        let (node, result) = sim.responses.get(&1).expect("response").clone();
        assert_eq!(node, leader);
        assert!(matches!(result, ClientWriteResult::Committed { .. }));
        // Every node applied the same entry.
        for id in sim.node_ids() {
            let applied = sim.applied_on(id);
            assert_eq!(applied.len(), 1);
            assert_eq!(applied[0].2, b"hello");
        }
        sim.assert_no_divergent_commits();
    }

    #[test]
    fn minority_cannot_acknowledge_writes() {
        let mut sim = cluster3(21);
        let leader = sim.run_until_leader(3000);
        // Isolate the leader: it can reach nobody.
        let others: Vec<NodeId> = sim.node_ids().into_iter().filter(|id| *id != leader).collect();
        for o in &others {
            sim.partition(leader, *o);
        }
        sim.client_write(leader, 1, b"lonely-write".to_vec());
        sim.run_for(1000);
        // No acknowledgement: a quorum is unreachable.
        assert!(
            !matches!(
                sim.responses.get(&1),
                Some((_, ClientWriteResult::Committed { .. }))
            ),
            "minority acknowledged a write!"
        );
        // Heal: the partitioned leader discovers the higher term, steps down,
        // and its uncommitted write is correctly reported as lost (never
        // acknowledged). A retry against the new leader commits.
        sim.heal_all();
        sim.run_for(1500);
        assert!(
            !matches!(
                sim.responses.get(&1),
                Some((_, ClientWriteResult::Committed { .. }))
            ),
            "uncommitted minority write was acknowledged after heal!"
        );
        let new_leader = sim.run_until_leader(3000);
        sim.client_write(new_leader, 2, b"lonely-write".to_vec());
        sim.run_for(500);
        assert!(matches!(
            sim.responses.get(&2),
            Some((_, ClientWriteResult::Committed { .. }))
        ));
        sim.assert_no_divergent_commits();
    }

    #[test]
    fn leader_crash_before_quorum_commit_loses_the_write() {
        let mut sim = cluster3(31);
        let leader = sim.run_until_leader(3000);
        // Freeze the followers so the entry cannot reach a quorum...
        let others: Vec<NodeId> = sim.node_ids().into_iter().filter(|id| *id != leader).collect();
        for o in &others {
            sim.pause(*o);
        }
        sim.client_write(leader, 1, b"doomed".to_vec());
        sim.run_for(200);
        assert!(
            !matches!(
                sim.responses.get(&1),
                Some((_, ClientWriteResult::Committed { .. }))
            ),
            "write was acknowledged without quorum"
        );
        // ...then crash the leader before it replicates anything.
        sim.crash(leader);
        for o in &others {
            sim.resume(*o);
        }
        let new_leader = sim.run_until_leader(5000);
        assert_ne!(new_leader, leader);
        // The uncommitted entry is gone everywhere: never applied, never acked.
        sim.run_for(1000);
        for id in sim.node_ids() {
            for (_, _, payload) in sim.applied_on(id) {
                assert_ne!(payload, b"doomed", "uncommitted entry was applied!");
            }
        }
        assert!(
            !matches!(
                sim.responses.get(&1),
                Some((_, ClientWriteResult::Committed { .. }))
            ),
            "crashed leader's uncommitted write was acknowledged"
        );
        // The cluster still works.
        sim.client_write(new_leader, 2, b"alive".to_vec());
        sim.run_for(500);
        assert!(matches!(
            sim.responses.get(&2),
            Some((_, ClientWriteResult::Committed { .. }))
        ));
        sim.assert_no_divergent_commits();
    }

    #[test]
    fn leader_crash_after_quorum_commit_preserves_the_write() {
        let mut sim = cluster3(41);
        let leader = sim.run_until_leader(3000);
        sim.client_write(leader, 1, b"durable".to_vec());
        sim.run_for(500);
        assert!(matches!(
            sim.responses.get(&1),
            Some((_, ClientWriteResult::Committed { .. }))
        ));
        // Crash the leader after the quorum commit.
        sim.crash(leader);
        sim.run_until_leader(5000);
        sim.run_for(1000);
        // Election safety: the new leader's log contains every committed entry,
        // so every live node converges on it.
        for id in sim.node_ids() {
            let payloads: Vec<Vec<u8>> =
                sim.applied_on(id).into_iter().map(|(_, _, p)| p).collect();
            assert!(
                payloads.contains(&b"durable".to_vec()),
                "node {id} lost a committed write after failover"
            );
        }
        sim.assert_no_divergent_commits();
    }

    #[test]
    fn figure_8_never_commits_old_term_entry_by_counting() {
        // Direct unit test of Raft §5.4.2 (Figure 8 in the dissertation):
        // a leader must not commit an entry from a previous term merely
        // because a quorum of nodes store it. We craft state (c): leader in
        // term 4 whose log ends with a term-2 entry present on a majority.
        let store = MemStore::new();
        let mut node: Node<MemStore> = Node::new(1, vec![2, 3, 4, 5], store, 99).unwrap();
        node.current_term = 4;
        node.role = Role::Leader;
        node.log = vec![
            LogEntry { term: 1, index: 1, payload: b"a".to_vec() },
            LogEntry { term: 2, index: 2, payload: b"b".to_vec() },
        ];
        node.commit_index = 1;
        node.last_applied = 1;
        for id in [1u64, 2, 3, 4, 5] {
            node.match_index.insert(id, if id <= 3 { 2 } else { 0 });
        }
        let mut fx = Vec::new();
        node.try_advance_commit(&mut fx);
        assert_eq!(
            node.commit_index(),
            1,
            "§5.4.2 violated: committed a term-2 entry by counting replicas"
        );
        assert!(fx.is_empty(), "nothing may be applied");

        // A new term-4 entry replicated on a majority commits, transitively
        // committing the term-2 entry. This is the ONLY legal way.
        node.log.push(LogEntry { term: 4, index: 3, payload: b"c".to_vec() });
        for id in [1u64, 2, 3] {
            node.match_index.insert(id, 3);
        }
        node.try_advance_commit(&mut fx);
        assert_eq!(node.commit_index(), 3);
        let applied: Vec<Index> = fx
            .iter()
            .filter_map(|e| match e {
                Effect::Apply { index, .. } => Some(*index),
                _ => None,
            })
            .collect();
        assert_eq!(applied, vec![2, 3], "entries must apply in order");
    }

    #[test]
    fn stale_leader_steps_down_on_higher_term() {
        let mut sim = cluster3(51);
        let old_leader = sim.run_until_leader(3000);
        let old_term = sim.term_of(old_leader).unwrap();
        // Partition the leader away; the rest elect someone new.
        let others: Vec<NodeId> = sim.node_ids().into_iter().filter(|id| *id != old_leader).collect();
        for o in &others {
            sim.partition(old_leader, *o);
        }
        sim.run_for(1500);
        // The non-partitioned side elects its own leader at a higher term.
        // (The old leader still considers itself leader of its 1-node
        // partition at the old term, which is legal Raft behavior.)
        let new_leader = sim
            .leader_in(&others)
            .expect("partitioned majority elects a leader");
        assert_ne!(new_leader, old_leader);
        assert!(sim.term_of(new_leader).unwrap() > old_term);
        // Heal: the stale leader's heartbeat is rejected with a higher term,
        // forcing it to step down.
        sim.heal_all();
        sim.run_for(1500);
        assert_eq!(sim.role_of(old_leader), Some(Role::Follower));
        assert_eq!(sim.term_of(old_leader), sim.term_of(new_leader));
        sim.assert_no_divergent_commits();
    }

    #[test]
    fn follower_crash_and_restart_recovers() {
        let mut sim = cluster3(61);
        let leader = sim.run_until_leader(3000);
        sim.client_write(leader, 1, b"before-crash".to_vec());
        sim.run_for(500);
        let follower = sim.node_ids().into_iter().find(|id| *id != leader).unwrap();
        sim.crash(follower);
        sim.client_write(leader, 2, b"during-outage".to_vec());
        sim.run_for(500);
        sim.restart(follower, 6101);
        sim.run_for(1500);
        // The restarted follower catches up via log replication.
        let payloads: Vec<Vec<u8>> = sim
            .applied_on(follower)
            .into_iter()
            .map(|(_, _, p)| p)
            .collect();
        assert!(payloads.contains(&b"before-crash".to_vec()));
        assert!(payloads.contains(&b"during-outage".to_vec()));
        sim.assert_no_divergent_commits();
    }

    #[test]
    fn lagging_node_recovers_via_snapshot() {
        let mut sim = cluster3(71);
        let leader = sim.run_until_leader(3000);
        let lagger = sim.node_ids().into_iter().find(|id| *id != leader).unwrap();
        // Crash the lagger, advance the log, then compact past its position.
        sim.crash(lagger);
        for i in 0..12u64 {
            sim.client_write(leader, 100 + i, format!("v{i}").into_bytes());
        }
        sim.run_for(1000);
        for i in 0..12u64 {
            assert!(
                matches!(
                    sim.responses.get(&(100 + i)),
                    Some((_, ClientWriteResult::Committed { .. }))
                ),
                "write {i} not committed"
            );
        }
        let commit = sim.nodes[&leader].commit_index();
        assert!(commit >= 12, "expected progress, got commit {commit}");
        sim.compact_log(leader, commit);
        let (base, _) = sim.nodes[&leader].snapshot_status();
        assert!(base >= 12);
        // Restart the lagger far behind the compaction horizon.
        sim.restart(lagger, 7101);
        sim.run_for(2000);
        // It must install the snapshot and converge on the leader's commit.
        assert_eq!(
            sim.nodes[&lagger].commit_index(),
            sim.nodes[&leader].commit_index()
        );
        sim.assert_no_divergent_commits();
    }

    #[test]
    fn persistent_state_survives_restart() {
        let mut sim = cluster3(81);
        let leader = sim.run_until_leader(3000);
        let term_before = sim.term_of(leader).unwrap();
        sim.client_write(leader, 1, b"keep-me".to_vec());
        sim.run_for(500);
        // Crash the *leader* and restart it: term, vote and log survive.
        sim.crash(leader);
        sim.restart(leader, 8101);
        assert_eq!(sim.term_of(leader).unwrap(), term_before);
        let log_len = sim.nodes[&leader].last_index();
        assert!(log_len >= 2, "log lost entries across restart");
        sim.run_for(2000);
        sim.assert_no_divergent_commits();
    }

    #[test]
    fn duplicate_and_reordered_messages_are_safe() {
        let mut sim = cluster3(91);
        let leader = sim.run_until_leader(3000);
        // Queue duplicates and reorders on the replication path.
        sim.client_write(leader, 1, b"once".to_vec());
        for _ in 0..5 {
            for from in sim.node_ids() {
                for to in sim.node_ids() {
                    if from != to {
                        sim.duplicate_next(from, to);
                    }
                }
            }
            sim.run_for(30);
            for from in sim.node_ids() {
                for to in sim.node_ids() {
                    if from != to {
                        sim.reorder_next(from, to);
                    }
                }
            }
            sim.run_for(30);
        }
        sim.run_for(1000);
        assert!(matches!(
            sim.responses.get(&1),
            Some((_, ClientWriteResult::Committed { .. }))
        ));
        // Applied exactly once per node, in order, no divergence.
        for id in sim.node_ids() {
            let applied = sim.applied_on(id);
            let ones: Vec<_> = applied.iter().filter(|(_, _, p)| p == b"once").collect();
            assert_eq!(ones.len(), 1, "node {id} applied the write {ones_len} times", ones_len = ones.len());
        }
        sim.assert_no_divergent_commits();
    }

    #[test]
    fn storage_fault_halts_node_safely() {
        let mut sim = cluster3(101);
        let leader = sim.run_until_leader(3000);
        // Inject storage faults on a follower: it must halt, not corrupt.
        let follower = sim.node_ids().into_iter().find(|id| *id != leader).unwrap();
        sim.fail_saves(follower, true);
        // Force the follower to persist (a heartbeat with new entries will do).
        sim.client_write(leader, 1, b"trigger".to_vec());
        sim.run_for(500);
        assert!(
            sim.nodes[&follower].is_halted(),
            "node with failing storage must halt"
        );
        // The remaining two nodes still form a quorum and make progress.
        sim.client_write(leader, 2, b"quorum-alive".to_vec());
        sim.run_for(500);
        assert!(matches!(
            sim.responses.get(&2),
            Some((_, ClientWriteResult::Committed { .. }))
        ));
    }

    #[test]
    fn single_node_cluster_commits_immediately() {
        let mut sim = Sim::new(&[1], 111);
        let leader = sim.run_until_leader(1000);
        assert_eq!(leader, 1);
        sim.client_write(1, 1, b"solo".to_vec());
        sim.run_for(100);
        assert!(matches!(
            sim.responses.get(&1),
            Some((_, ClientWriteResult::Committed { .. }))
        ));
        assert_eq!(sim.applied_on(1).len(), 1);
    }

    #[test]
    fn chaos_no_safety_violation() {
        // Randomized fault injection: drops, duplicates, crashes, partitions.
        let mut sim = Sim::new(&[1, 2, 3, 4, 5], 2024);
        sim.set_chaos(10, 5);
        let mut req: u64 = 1;
        let mut rng = XorShift64::new(777);
        for round in 0..30 {
            // Random fault.
            match rng.next() % 5 {
                0 => {
                    let a = 1 + rng.next() % 5;
                    let mut b = 1 + rng.next() % 5;
                    if b == a {
                        b = b % 5 + 1;
                    }
                    sim.partition(a, b);
                }
                1 => sim.heal_all(),
                2 => {
                    let v = 1 + rng.next() % 5;
                    if sim.is_live(v) {
                        sim.crash(v);
                    }
                }
                3 => {
                    let v = 1 + rng.next() % 5;
                    if !sim.is_live(v) {
                        sim.restart(v, 9000 + round * 10 + v);
                    }
                }
                _ => {}
            }
            // Try a write against whoever claims leadership.
            if let Some(l) = sim.leader() {
                sim.client_write(l, req, format!("r{round}").into_bytes());
                req += 1;
            }
            sim.run_for(150);
            // Continuous safety checks.
            sim.leader(); // panics on same-term dual leaders
            sim.assert_no_divergent_commits();
        }
        // Heal everything and let the cluster converge.
        sim.heal_all();
        for id in sim.node_ids() {
            if !sim.is_live(id) {
                sim.restart(id, 9500 + id);
            }
        }
        sim.set_chaos(0, 0);
        sim.run_for(3000);
        sim.leader();
        sim.assert_no_divergent_commits();
    }
}
