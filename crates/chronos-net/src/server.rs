//! ChronosDB server.
//!
//! Architecture: a dedicated driver thread owns the Raft [`Node`], the MVCC
//! state machine, and all durability. Tokio tasks handle TCP (client API and
//! Raft peer transport) and forward to the driver over channels. The driver
//! never blocks on the network; file I/O (fsync) happens on the driver
//! thread.
//!
//! # Crash recovery
//!
//! * The Raft log (WAL) and `applied.index` file tell the driver exactly
//!   which entries were applied: on startup it loads the latest MVCC
//!   snapshot (index `S`), replays Raft log entries `(S, applied]`, and only
//!   then serves clients.
//! * MVCC commit seqs are Raft log indices, so versions are stable across
//!   crashes and replay is deterministic.
//! * Every state-machine op carries the client's `req_id`; the applier
//!   records `req_id -> outcome`, giving exactly-once semantics across
//!   client retries and leader failover.

use std::collections::HashMap;
use std::fs::{self, File};
use std::io::Write;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::mpsc as stdmpsc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use chronos_mvcc::{self, MvccError, MvccStore};
use chronos_raft::{ClientWriteResult, Effect, Index, Message, Node, NodeHalted, NodeId, Role};
use chronos_storage::snapshot;
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, oneshot};

use crate::proto::{
    framing, ApiError, ApiResult, ReqId, Request, Response, ResponseBody, StateMachineOp, TxOutcome,
};
use crate::raft_store::RaftStore;

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

fn fsync_dir(dir: &Path) -> std::io::Result<()> {
    File::open(dir)?.sync_all()?;
    Ok(())
}

#[derive(Clone, Debug)]
pub struct ServerConfig {
    pub id: NodeId,
    pub client_addr: SocketAddr,
    pub peer_addr: SocketAddr,
    /// peer id -> peer transport address
    pub peers: HashMap<NodeId, SocketAddr>,
    pub data_dir: PathBuf,
    /// Take an MVCC snapshot every N applied Raft entries.
    pub snapshot_threshold: u64,
}

/// Durable snapshot of the state machine. `index` is the Raft log index whose
/// application produced this state.
#[derive(Serialize, Deserialize)]
struct MvccSnapshot {
    index: Index,
    mvcc: Vec<u8>,
    completed: HashMap<ReqId, (Index, TxOutcome)>,
}

fn ok_response(req_id: ReqId, body: ResponseBody) -> Response {
    Response::Result { req_id, result: ApiResult::Ok(body) }
}

fn err_response(req_id: ReqId, err: ApiError) -> Response {
    Response::Result { req_id, result: ApiResult::Err(err) }
}

/// Build the response for a deduplicated (retried) mutating request from the
/// recorded outcome.
fn dedup_response(req_id: ReqId, outcome: &TxOutcome) -> Response {
    match outcome {
        TxOutcome::Committed { seq } => ok_response(req_id, ResponseBody::CommitVersion(*seq)),
        TxOutcome::Aborted => err_response(req_id, ApiError::Conflict),
    }
}

enum DriverMsg {
    Client { req: Request, respond: oneshot::Sender<Response> },
    Peer { from: NodeId, msg: Message },
    Shutdown,
}

struct Driver {
    node: Node<RaftStore>,
    mvcc: MvccStore,
    /// req_id -> (raft index, outcome). The exactly-once record.
    completed: HashMap<ReqId, (Index, TxOutcome)>,
    /// Clients waiting for their Raft write to commit.
    pending: HashMap<ReqId, oneshot::Sender<Response>>,
    peer_tx: HashMap<NodeId, mpsc::Sender<Message>>,
    inbox: stdmpsc::Receiver<DriverMsg>,
    data_dir: PathBuf,
    /// Highest Raft index applied to `mvcc` (durable in `applied.index`).
    applied: Index,
    /// Index of the latest durable MVCC snapshot.
    last_mvcc_snapshot: Index,
    snapshot_threshold: u64,
    dropped_sends: u64,
    failed: bool,
}

impl Driver {
    fn mvcc_snap_dir(&self) -> PathBuf {
        self.data_dir.join("mvcc_snaps")
    }

    fn applied_path(&self) -> PathBuf {
        self.data_dir.join("applied.index")
    }

    /// Make `self.applied` durable. Called after each batch of Apply effects,
    /// before any client waiting on those entries is answered (Apply effects
    /// precede ClientResponse in the effect list, and this runs first).
    fn persist_applied(&mut self) {
        let tmp = self.data_dir.join("applied.tmp");
        let ok = (|| -> std::io::Result<()> {
            let mut f = File::create(&tmp)?;
            f.write_all(&self.applied.to_le_bytes())?;
            f.sync_all()?;
            drop(f);
            fs::rename(&tmp, self.applied_path())?;
            fsync_dir(&self.data_dir)?;
            Ok(())
        })();
        if let Err(e) = ok {
            eprintln!("FATAL: cannot persist applied index: {e}; shutting down");
            self.failed = true;
        }
    }

    fn run(mut self) {
        loop {
            if self.failed {
                break;
            }
            match self.inbox.recv_timeout(Duration::from_millis(10)) {
                Ok(DriverMsg::Client { req, respond }) => self.handle_client(req, respond),
                Ok(DriverMsg::Shutdown) => break,
                Ok(DriverMsg::Peer { from, msg }) => {
                    let mut fx = Vec::new();
                    match self.node.on_message(from, msg, now_ms(), &mut fx) {
                        Ok(()) => self.process_effects(fx),
                        Err(NodeHalted(reason)) => {
                            eprintln!("FATAL: raft node halted: {reason}; shutting down");
                            break;
                        }
                    }
                }
                Err(stdmpsc::RecvTimeoutError::Timeout) => {
                    let mut fx = Vec::new();
                    match self.node.tick(now_ms(), &mut fx) {
                        Ok(()) => self.process_effects(fx),
                        Err(NodeHalted(reason)) => {
                            eprintln!("FATAL: raft node halted: {reason}; shutting down");
                            break;
                        }
                    }
                }
                Err(stdmpsc::RecvTimeoutError::Disconnected) => break,
            }
        }
        eprintln!("driver exiting");
    }

    fn handle_client(&mut self, req: Request, respond: oneshot::Sender<Response>) {
        let req_id = req.req_id();
        // Exactly-once: a retried mutating request returns the recorded
        // outcome without re-executing, even on a different leader.
        if req.is_mutating() {
            if let Some((_, outcome)) = self.completed.get(&req_id) {
                let _ = respond.send(dedup_response(req_id, outcome));
                return;
            }
        }
        if self.node.role() != Role::Leader {
            let _ = respond.send(Response::NotLeader {
                req_id,
                leader_hint: self.node.leader_id(),
            });
            return;
        }
        match req {
            Request::Put { req_id, key, value } => {
                self.propose(req_id, StateMachineOp::Put { req_id, key, value }, respond)
            }
            Request::Delete { req_id, key } => {
                self.propose(req_id, StateMachineOp::Delete { req_id, key }, respond)
            }
            Request::Commit { req_id, tx } => match self.mvcc.take_tx(tx) {
                Ok((read_seq, writes)) => self.propose(
                    req_id,
                    StateMachineOp::TxCommit { req_id, read_seq, writes },
                    respond,
                ),
                Err(_) => {
                    let _ = respond.send(err_response(req_id, ApiError::UnknownTransaction));
                }
            },
            Request::Get { req_id, key } => {
                let _ = respond.send(ok_response(req_id, ResponseBody::Value(self.mvcc.get(&key))));
            }
            Request::GetAtVersion { req_id, key, version } => {
                let r = match self.mvcc.get_at_version(&key, version) {
                    Ok(v) => ApiResult::Ok(ResponseBody::Value(v)),
                    Err(MvccError::InvalidVersion(_)) => ApiResult::Err(ApiError::InvalidVersion),
                    Err(e) => ApiResult::Err(ApiError::Internal(e.to_string())),
                };
                let _ = respond.send(Response::Result { req_id, result: r });
            }
            Request::Begin { req_id } => {
                let tx = self.mvcc.begin();
                let _ = respond.send(ok_response(req_id, ResponseBody::TxId(tx)));
            }
            Request::TxGet { req_id, tx, key } => {
                let r = match self.mvcc.tx_get(tx, &key) {
                    Ok(v) => ApiResult::Ok(ResponseBody::Value(v)),
                    Err(MvccError::UnknownTransaction(_)) => {
                        ApiResult::Err(ApiError::UnknownTransaction)
                    }
                    Err(e) => ApiResult::Err(ApiError::Internal(e.to_string())),
                };
                let _ = respond.send(Response::Result { req_id, result: r });
            }
            Request::TxPut { req_id, tx, key, value } => {
                let r = match self.mvcc.tx_put(tx, key, value) {
                    Ok(()) => ApiResult::Ok(ResponseBody::Empty),
                    Err(MvccError::UnknownTransaction(_)) => {
                        ApiResult::Err(ApiError::UnknownTransaction)
                    }
                    Err(e) => ApiResult::Err(ApiError::Internal(e.to_string())),
                };
                let _ = respond.send(Response::Result { req_id, result: r });
            }
            Request::TxDelete { req_id, tx, key } => {
                let r = match self.mvcc.tx_delete(tx, key) {
                    Ok(()) => ApiResult::Ok(ResponseBody::Empty),
                    Err(MvccError::UnknownTransaction(_)) => {
                        ApiResult::Err(ApiError::UnknownTransaction)
                    }
                    Err(e) => ApiResult::Err(ApiError::Internal(e.to_string())),
                };
                let _ = respond.send(Response::Result { req_id, result: r });
            }
            Request::Rollback { req_id, tx } => {
                let r = match self.mvcc.rollback(tx) {
                    Ok(()) => ApiResult::Ok(ResponseBody::Empty),
                    Err(MvccError::UnknownTransaction(_)) => {
                        ApiResult::Err(ApiError::UnknownTransaction)
                    }
                    Err(e) => ApiResult::Err(ApiError::Internal(e.to_string())),
                };
                let _ = respond.send(Response::Result { req_id, result: r });
            }
        }
    }

    fn propose(&mut self, req_id: ReqId, op: StateMachineOp, respond: oneshot::Sender<Response>) {
        if self.pending.len() > 50_000 {
            let _ = respond.send(err_response(
                req_id,
                ApiError::Internal("server overloaded: too many pending writes".into()),
            ));
            return;
        }
        let payload = bincode::serialize(&op).expect("op serializes");
        let mut fx = Vec::new();
        match self.node.client_write(req_id, payload, &mut fx) {
            Ok(()) => {
                // A stale-leader race may have emitted NotLeader already;
                // process_effects routes it to this sender.
                self.pending.insert(req_id, respond);
                self.process_effects(fx);
            }
            Err(NodeHalted(reason)) => {
                eprintln!("FATAL: raft node halted: {reason}; shutting down");
                let _ = respond.send(err_response(req_id, ApiError::Internal("node halted".into())));
                self.failed = true;
            }
        }
    }

    fn process_effects(&mut self, fx: Vec<Effect>) {
        let mut applied_any = false;
        for e in fx {
            if self.failed {
                return;
            }
            match e {
                Effect::Send { to, msg } => {
                    if let Some(tx) = self.peer_tx.get(&to) {
                        // Raft tolerates message loss (retries/heartbeats
                        // recover); a full channel drops and counts.
                        if tx.try_send(msg).is_err() {
                            self.dropped_sends += 1;
                        }
                    }
                }
                Effect::Apply { index, payload } => {
                    self.apply_committed(index, &payload);
                    applied_any = true;
                }
                Effect::ApplySnapshot { last_included_index, data, .. } => {
                    self.install_snapshot(last_included_index, data);
                }
                Effect::RequestSnapshot { to } => {
                    let snap = MvccSnapshot {
                        index: self.applied,
                        mvcc: chronos_mvcc::encode_snapshot(&self.mvcc),
                        completed: self.completed.clone(),
                    };
                    match bincode::serialize(&snap) {
                        Ok(data) => {
                            let mut fx2 = Vec::new();
                            self.node.provide_snapshot(to, data, &mut fx2);
                            self.process_effects(fx2);
                        }
                        Err(e) => eprintln!("snapshot encode failed: {e}"),
                    }
                }
                Effect::ClientResponse { req_id, result } => {
                    self.on_client_response(req_id, result)
                }
            }
        }
        if applied_any {
            // Make the new applied index durable. A crash before this point
            // is still safe: applied entries are committed (hence in the
            // Raft log), so recovery replays/re-commits them and the req_id
            // record deduplicates.
            self.persist_applied();
        }
    }

    /// Apply a committed Raft entry to the MVCC state machine. Deterministic
    /// across replicas; idempotent per req_id.
    fn apply_committed(&mut self, index: Index, payload: &[u8]) {
        if payload.is_empty() {
            return; // leader no-op entry
        }
        let op: StateMachineOp = match bincode::deserialize(payload) {
            Ok(op) => op,
            Err(e) => {
                eprintln!("FATAL: corrupt state machine op at index {index}: {e}");
                self.failed = true;
                return;
            }
        };
        let req_id = op.req_id();
        if self.completed.contains_key(&req_id) {
            return; // already applied (replay after restart, or duplicate)
        }
        let outcome = match op {
            StateMachineOp::Put { key, value, .. } => {
                // Blind write: no read set, always commits. read_seq is only
                // used for conflict detection, which cannot trigger here.
                let read_seq = self.mvcc.last_committed();
                match self.mvcc.apply_decided(index, read_seq, &[(key, Some(value))]) {
                    Some(seq) => TxOutcome::Committed { seq },
                    None => TxOutcome::Aborted, // unreachable
                }
            }
            StateMachineOp::Delete { key, .. } => {
                let read_seq = self.mvcc.last_committed();
                match self.mvcc.apply_decided(index, read_seq, &[(key, None)]) {
                    Some(seq) => TxOutcome::Committed { seq },
                    None => TxOutcome::Aborted, // unreachable
                }
            }
            StateMachineOp::TxCommit { read_seq, writes, .. } => {
                match self.mvcc.apply_decided(index, read_seq, &writes) {
                    Some(seq) => TxOutcome::Committed { seq },
                    None => TxOutcome::Aborted,
                }
            }
        };
        self.completed.insert(req_id, (index, outcome));
        self.applied = self.applied.max(index);

        if index - self.last_mvcc_snapshot >= self.snapshot_threshold {
            self.take_mvcc_snapshot(index);
        }
    }

    fn on_client_response(&mut self, req_id: ReqId, result: ClientWriteResult) {
        let respond = match self.pending.remove(&req_id) {
            Some(r) => r,
            None => return,
        };
        let resp = match result {
            ClientWriteResult::Committed { .. } => match self.completed.get(&req_id) {
                Some((_, outcome)) => dedup_response(req_id, outcome),
                None => err_response(req_id, ApiError::Internal("missing apply outcome".into())),
            },
            ClientWriteResult::NotLeader { leader_hint } => {
                Response::NotLeader { req_id, leader_hint }
            }
            ClientWriteResult::LeadershipLost => Response::NotLeader {
                req_id,
                leader_hint: self.node.leader_id(),
            },
        };
        let _ = respond.send(resp);
    }

    fn install_snapshot(&mut self, index: Index, data: Vec<u8>) {
        let snap: MvccSnapshot = match bincode::deserialize(&data) {
            Ok(s) => s,
            Err(e) => {
                eprintln!("FATAL: bad snapshot envelope: {e}");
                self.failed = true;
                return;
            }
        };
        match chronos_mvcc::decode_snapshot(&snap.mvcc) {
            Ok(mvcc) => {
                self.mvcc = mvcc;
                self.completed = snap.completed;
                self.applied = self.applied.max(index);
                self.last_mvcc_snapshot = self.last_mvcc_snapshot.max(snap.index);
                // Persist locally so a crash right after doesn't replay.
                let dir = self.mvcc_snap_dir();
                if let Err(e) = snapshot::write_snapshot(&dir, snap.index, &data) {
                    eprintln!("WARNING: failed to persist installed snapshot: {e:?}");
                }
                self.persist_applied();
            }
            Err(e) => {
                eprintln!("FATAL: bad MVCC snapshot: {e}");
                self.failed = true;
            }
        }
    }

    /// Snapshot the MVCC state machine, then compact the Raft log. The order
    /// is load-bearing: the log is compacted only after the snapshot covering
    /// it is durable, so `snapshot_index >= raft base` always holds.
    fn take_mvcc_snapshot(&mut self, index: Index) {
        let snap = MvccSnapshot {
            index,
            mvcc: chronos_mvcc::encode_snapshot(&self.mvcc),
            completed: self.completed.clone(),
        };
        let bytes = match bincode::serialize(&snap) {
            Ok(b) => b,
            Err(e) => {
                eprintln!("snapshot encode failed: {e}; skipping");
                return;
            }
        };
        if let Err(e) = snapshot::write_snapshot(&self.mvcc_snap_dir(), index, &bytes) {
            eprintln!("mvcc snapshot failed: {e:?}; continuing without snapshot");
            return;
        }
        match self.node.compact_log(index) {
            Ok(()) => self.last_mvcc_snapshot = index,
            Err(NodeHalted(reason)) => {
                eprintln!("FATAL: {reason}");
                self.failed = true;
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Startup / recovery
// ---------------------------------------------------------------------------

fn read_applied(data_dir: &Path) -> Index {
    fs::read(data_dir.join("applied.index"))
        .ok()
        .and_then(|b| b.try_into().ok())
        .map(u64::from_le_bytes)
        .unwrap_or(0)
}

/// Recover state and build the driver. Must complete before serving clients:
/// after this returns, `completed` accurately reflects every applied entry,
// so client retries are deduplicated correctly.
fn recover(cfg: &ServerConfig) -> Result<Driver, String> {
    fs::create_dir_all(&cfg.data_dir).map_err(|e| e.to_string())?;
    let snap_dir = cfg.data_dir.join("mvcc_snaps");
    fs::create_dir_all(&snap_dir).map_err(|e| e.to_string())?;

    // 1. Latest MVCC snapshot.
    let (mut mvcc, mut completed, mut snap_index) =
        match snapshot::read_latest(&snap_dir).map_err(|e| e.to_string())? {
            Some((idx, bytes)) => {
                let snap: MvccSnapshot =
                    bincode::deserialize(&bytes).map_err(|e| format!("bad snapshot: {e}"))?;
                let mvcc =
                    chronos_mvcc::decode_snapshot(&snap.mvcc).map_err(|e| e.to_string())?;
                (mvcc, snap.completed, idx)
            }
            None => (MvccStore::new(), HashMap::new(), 0),
        };

    // 2. Raft node (recovers term/vote/log from the WAL).
    let store = RaftStore::open(&cfg.data_dir.join("raft")).map_err(|e| e.to_string())?;
    let peer_ids: Vec<NodeId> = cfg.peers.keys().copied().collect();
    let node = Node::new(cfg.id, peer_ids, store, 0x9E37_79B9_7F4A_7C15 ^ cfg.id)
        .map_err(|e| format!("raft recovery failed: {e:?}"))?;

    // 3. Replay Raft log entries (snap_index, applied] into the MVCC store.
    let applied = read_applied(&cfg.data_dir);
    let (base, _) = node.snapshot_status();
    if snap_index < base {
        return Err(format!(
            "snapshot index {snap_index} < raft base {base}: snapshot protocol violated"
        ));
    }
    let mut driver = Driver {
        node,
        mvcc,
        completed,
        pending: HashMap::new(),
        peer_tx: HashMap::new(),
        inbox: {
            let (_, rx) = stdmpsc::channel::<DriverMsg>();
            rx
        },
        data_dir: cfg.data_dir.clone(),
        applied,
        last_mvcc_snapshot: snap_index,
        snapshot_threshold: cfg.snapshot_threshold,
        dropped_sends: 0,
        failed: false,
    };
    // Temporarily swap mvcc/completed out for replay.
    let entries = driver.node.log_entries(snap_index, applied);
    for e in entries {
        if e.payload.is_empty() {
            continue;
        }
        driver.apply_committed(e.index, &e.payload);
        if driver.failed {
            return Err("replay failed".into());
        }
    }
    // `applied` may have advanced if the log had more than the file said
    // (file is only a hint); persist the truth.
    driver.persist_applied();
    Ok(driver)
}

// ---------------------------------------------------------------------------
// TCP front ends
// ---------------------------------------------------------------------------

async fn read_frame(rd: &mut tokio::net::tcp::OwnedReadHalf) -> Option<Vec<u8>> {
    let mut hdr = [0u8; 4];
    rd.read_exact(&mut hdr).await.ok()?;
    let len = framing::decode_len(&hdr);
    if len == 0 || len > framing::MAX_FRAME {
        return None;
    }
    let mut payload = vec![0u8; len];
    rd.read_exact(&mut payload).await.ok()?;
    Some(payload)
}

async fn handle_client_conn(stream: TcpStream, driver_tx: stdmpsc::Sender<DriverMsg>) {
    let (mut rd, mut wr) = stream.into_split();
    loop {
        let payload = match read_frame(&mut rd).await {
            Some(p) => p,
            None => break,
        };
        let req: Request = match bincode::deserialize(&payload) {
            Ok(r) => r,
            Err(_) => break,
        };
        let (tx, rx) = oneshot::channel();
        if driver_tx.send(DriverMsg::Client { req, respond: tx }).is_err() {
            break;
        }
        let resp = match tokio::time::timeout(Duration::from_secs(30), rx).await {
            Ok(Ok(r)) => r,
            _ => break, // driver gone or overloaded; drop the connection
        };
        if wr.write_all(&framing::encode(&resp)).await.is_err() {
            break;
        }
    }
}

async fn handle_peer_conn(stream: TcpStream, driver_tx: stdmpsc::Sender<DriverMsg>) {
    let (mut rd, _) = stream.into_split();
    loop {
        let payload = match read_frame(&mut rd).await {
            Some(p) => p,
            None => break,
        };
        let (from, msg): (NodeId, Message) = match bincode::deserialize(&payload) {
            Ok(m) => m,
            Err(_) => break,
        };
        if driver_tx.send(DriverMsg::Peer { from, msg }).is_err() {
            break;
        }
    }
}

/// Maintain an outbound connection to one peer, sending Raft messages.
async fn peer_sender(
    my_id: NodeId,
    _peer: NodeId,
    addr: SocketAddr,
    mut rx: mpsc::Receiver<Message>,
) {
    loop {
        match TcpStream::connect(addr).await {
            Ok(stream) => {
                let (_, mut wr) = stream.into_split();
                while let Some(msg) = rx.recv().await {
                    let frame = framing::encode(&(my_id, msg));
                    if wr.write_all(&frame).await.is_err() {
                        break; // reconnect; queued messages are retained
                    }
                }
            }
            Err(_) => tokio::time::sleep(Duration::from_millis(200)).await,
        }
    }
}

/// Run the server until `shutdown` fires or the driver fails.
pub async fn run_server(cfg: ServerConfig, mut shutdown: oneshot::Receiver<()>) -> Result<(), String> {
    let mut driver = recover(&cfg)?;
    let (driver_tx, driver_rx) = stdmpsc::channel::<DriverMsg>();
    driver.inbox = driver_rx;

    // Peer senders.
    for (peer, addr) in &cfg.peers {
        let (tx, rx) = mpsc::channel::<Message>(4096);
        driver.peer_tx.insert(*peer, tx);
        tokio::spawn(peer_sender(cfg.id, *peer, *addr, rx));
    }

    // Driver thread.
    let driver_handle = std::thread::Builder::new()
        .name(format!("chronos-driver-{}", cfg.id))
        .spawn(move || driver.run())
        .map_err(|e| e.to_string())?;

    // Client listener.
    let client_l = TcpListener::bind(cfg.client_addr)
        .await
        .map_err(|e| e.to_string())?;
    // Peer listener.
    let peer_l = TcpListener::bind(cfg.peer_addr).await.map_err(|e| e.to_string())?;
    eprintln!(
        "chronos node {}: client={} peer={} peers={:?}",
        cfg.id,
        cfg.client_addr,
        cfg.peer_addr,
        cfg.peers.keys().collect::<Vec<_>>(),
    );

    loop {
        tokio::select! {
            _ = &mut shutdown => break,
            Ok((stream, _)) = client_l.accept() => {
                let tx = driver_tx.clone();
                tokio::spawn(handle_client_conn(stream, tx));
            }
            Ok((stream, _)) = peer_l.accept() => {
                let tx = driver_tx.clone();
                tokio::spawn(handle_peer_conn(stream, tx));
            }
        }
    }
    let _ = driver_tx.send(DriverMsg::Shutdown);
    drop(driver_tx);
    let _ = tokio::task::spawn_blocking(move || driver_handle.join()).await;
    Ok(())
}
