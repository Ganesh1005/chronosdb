//! Async ChronosDB client.
//!
//! # Retry semantics
//!
//! Every logical operation gets one `req_id`. The client follows
//! `NotLeader` hints and retries transport failures against other nodes with
//! the SAME `req_id`; the server deduplicates, so retries are safe and
//! never double-apply. A transaction sticks to the node that began it; if
//! that node stops being the leader mid-transaction the client sees
//! `UnknownTransaction` and must re-begin (the helper [`Client::transaction`]
//! does this automatically, with bounded retries).

use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use thiserror::Error;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use crate::proto::{framing, ApiError, ApiResult, ReqId, Request, Response, ResponseBody, TxId};

#[derive(Debug, Error)]
pub enum ClientError {
    #[error("transport: {0}")]
    Transport(String),
    #[error("server: {0:?}")]
    Server(ApiError),
    #[error("no node reachable")]
    Unreachable,
    #[error("transaction aborted, re-begin required")]
    TxLost,
}

pub struct Client {
    /// (node id, client address) for every known node.
    nodes: Vec<(u64, SocketAddr)>,
    next_req_id: AtomicU64,
    /// Last known leader; tried first.
    leader: Option<SocketAddr>,
}

impl Client {
    pub fn new(addrs: Vec<SocketAddr>) -> Self {
        let nodes = addrs.into_iter().enumerate().map(|(i, a)| (i as u64, a)).collect();
        Client { nodes, next_req_id: AtomicU64::new(Self::initial_req_id()), leader: None }
    }

    /// Build a client when node IDs are known (so `NotLeader` hints can be
    /// followed directly).
    pub fn with_ids(nodes: Vec<(u64, SocketAddr)>) -> Self {
        Client { nodes, next_req_id: AtomicU64::new(Self::initial_req_id()), leader: None }
    }

    /// Request IDs must be unique across client processes: the server treats
    /// a repeated ID as a retry and returns the recorded outcome. Seed from
    /// the clock (nanoseconds); production clients should use UUIDs.
    fn initial_req_id() -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos() as u64)
            .unwrap_or(0x1234_5678)
            | 1
    }

    fn addr_for(&self, id: u64) -> Option<SocketAddr> {
        self.nodes.iter().find(|(i, _)| *i == id).map(|(_, a)| *a)
    }

    fn req_id(&self) -> ReqId {
        self.next_req_id.fetch_add(1, Ordering::Relaxed)
    }

    /// Send one request, following leader hints and retrying transport
    /// failures with the same req_id. Returns the API result.
    pub async fn call(&mut self, req: Request) -> Result<ApiResult, ClientError> {
        let mut addrs: Vec<SocketAddr> = self.nodes.iter().map(|(_, a)| *a).collect();
        if let Some(l) = self.leader {
            addrs.retain(|a| *a != l);
            addrs.insert(0, l);
        }
        let mut last_err = ClientError::Unreachable;
        // Sweep the cluster; NotLeader hints jump straight to the leader.
        // To avoid livelock on stale hints (e.g. followers hinting at a dead
        // leader), we remove addresses that refuse connections and bound the
        // number of hint-follows per sweep.
        for _ in 0..8 {
            let mut i = 0;
            let mut hint_follows = 0;
            const MAX_HINT_FOLLOWS: usize = 4;
            while i < addrs.len() {
                let addr = addrs[i];
                match self.call_once(addr, &req).await {
                    Ok(Response::Result { result, .. }) => return Ok(result),
                    Ok(Response::NotLeader { leader_hint, .. }) => {
                        if let Some(hint) = leader_hint.and_then(|id| self.addr_for(id)) {
                            hint_follows += 1;
                            if hint_follows > MAX_HINT_FOLLOWS {
                                // Stop trusting hints; try remaining addrs.
                                i += 1;
                                continue;
                            }
                            // Jump to the hinted leader.
                            addrs.retain(|a| *a != hint);
                            addrs.insert(0, hint);
                            i = 0;
                            continue;
                        }
                        i += 1;
                    }
                    Err(e) => {
                        last_err = e;
                        // Drop dead addresses so a stale hint can't livelock
                        // us on a node that refuses connections.
                        addrs.remove(i);
                        // Do not increment i; the next address shifted into
                        // this position.
                        continue;
                    }
                }
            }
            // Restore the full address list for the next sweep (a node may
            // have come back).
            addrs = self.nodes.iter().map(|(_, a)| *a).collect();
            if let Some(l) = self.leader {
                addrs.retain(|a| *a != l);
                addrs.insert(0, l);
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        Err(last_err)
    }

    async fn call_once(&mut self, addr: SocketAddr, req: &Request) -> Result<Response, ClientError> {
        let stream = tokio::time::timeout(Duration::from_secs(3), TcpStream::connect(addr))
            .await
            .map_err(|_| ClientError::Transport("connect timeout".into()))?
            .map_err(|e| ClientError::Transport(e.to_string()))?;
        let (mut rd, mut wr) = stream.into_split();
        wr.write_all(&framing::encode(req))
            .await
            .map_err(|e| ClientError::Transport(e.to_string()))?;
        let mut hdr = [0u8; 4];
        tokio::time::timeout(Duration::from_secs(30), rd.read_exact(&mut hdr))
            .await
            .map_err(|_| ClientError::Transport("read timeout".into()))?
            .map_err(|e| ClientError::Transport(e.to_string()))?;
        let len = framing::decode_len(&hdr);
        if len == 0 || len > framing::MAX_FRAME {
            return Err(ClientError::Transport("bad frame".into()));
        }
        let mut payload = vec![0u8; len];
        rd.read_exact(&mut payload)
            .await
            .map_err(|e| ClientError::Transport(e.to_string()))?;
        let resp: Response =
            bincode::deserialize(&payload).map_err(|e| ClientError::Transport(e.to_string()))?;
        // Remember who the leader is for the next call.
        if !matches!(resp, Response::NotLeader { .. }) {
            self.leader = Some(addr);
        }
        Ok(resp)
    }

    async fn api(&mut self, req: Request) -> Result<ResponseBody, ClientError> {
        match self.call(req).await? {
            ApiResult::Ok(b) => Ok(b),
            ApiResult::Err(e) => Err(ClientError::Server(e)),
        }
    }

    // -- convenience operations -------------------------------------------

    pub async fn put(&mut self, key: Vec<u8>, value: Vec<u8>) -> Result<u64, ClientError> {
        match self.api(Request::Put { req_id: self.req_id(), key, value }).await? {
            ResponseBody::CommitVersion(v) => Ok(v),
            _ => Err(ClientError::Transport("unexpected response".into())),
        }
    }

    pub async fn get(&mut self, key: Vec<u8>) -> Result<Option<Vec<u8>>, ClientError> {
        match self.api(Request::Get { req_id: self.req_id(), key }).await? {
            ResponseBody::Value(v) => Ok(v),
            _ => Err(ClientError::Transport("unexpected response".into())),
        }
    }

    pub async fn delete(&mut self, key: Vec<u8>) -> Result<u64, ClientError> {
        match self.api(Request::Delete { req_id: self.req_id(), key }).await? {
            ResponseBody::CommitVersion(v) => Ok(v),
            _ => Err(ClientError::Transport("unexpected response".into())),
        }
    }

    pub async fn get_at_version(
        &mut self,
        key: Vec<u8>,
        version: u64,
    ) -> Result<Option<Vec<u8>>, ClientError> {
        match self
            .api(Request::GetAtVersion { req_id: self.req_id(), key, version })
            .await?
        {
            ResponseBody::Value(v) => Ok(v),
            _ => Err(ClientError::Transport("unexpected response".into())),
        }
    }

    pub async fn begin(&mut self) -> Result<TxId, ClientError> {
        match self.api(Request::Begin { req_id: self.req_id() }).await? {
            ResponseBody::TxId(t) => Ok(t),
            _ => Err(ClientError::Transport("unexpected response".into())),
        }
    }

    pub async fn tx_get(&mut self, tx: TxId, key: Vec<u8>) -> Result<Option<Vec<u8>>, ClientError> {
        match self.api(Request::TxGet { req_id: self.req_id(), tx, key }).await? {
            ResponseBody::Value(v) => Ok(v),
            _ => Err(ClientError::Transport("unexpected response".into())),
        }
    }

    pub async fn tx_put(&mut self, tx: TxId, key: Vec<u8>, value: Vec<u8>) -> Result<(), ClientError> {
        match self.api(Request::TxPut { req_id: self.req_id(), tx, key, value }).await? {
            ResponseBody::Empty => Ok(()),
            _ => Err(ClientError::Transport("unexpected response".into())),
        }
    }

    pub async fn tx_delete(&mut self, tx: TxId, key: Vec<u8>) -> Result<(), ClientError> {
        match self.api(Request::TxDelete { req_id: self.req_id(), tx, key }).await? {
            ResponseBody::Empty => Ok(()),
            _ => Err(ClientError::Transport("unexpected response".into())),
        }
    }

    /// Commit. Returns the commit version, or `ClientError::Server(Conflict)`
    /// if a write-write conflict aborted the transaction.
    pub async fn commit(&mut self, tx: TxId) -> Result<u64, ClientError> {
        match self.api(Request::Commit { req_id: self.req_id(), tx }).await? {
            ResponseBody::CommitVersion(v) => Ok(v),
            _ => Err(ClientError::Transport("unexpected response".into())),
        }
    }

    pub async fn rollback(&mut self, tx: TxId) -> Result<(), ClientError> {
        match self.api(Request::Rollback { req_id: self.req_id(), tx }).await? {
            ResponseBody::Empty => Ok(()),
            _ => Err(ClientError::Transport("unexpected response".into())),
        }
    }

    /// Run a transactional closure with automatic re-begin on
    /// `UnknownTransaction` (leader failover mid-transaction). The closure
    /// receives a fresh tx each attempt; SI conflicts (`Conflict`) are
    /// returned to the caller, not retried.
    pub async fn transaction<F, Fut, T>(&mut self, mut f: F) -> Result<T, ClientError>
    where
        F: FnMut(&mut Self, TxId) -> Fut,
        Fut: std::future::Future<Output = Result<T, ClientError>>,
    {
        for _ in 0..3 {
            let tx = self.begin().await?;
            match f(self, tx).await {
                Ok(v) => match self.commit(tx).await {
                    Ok(_) => return Ok(v),
                    Err(ClientError::Server(ApiError::UnknownTransaction)) => continue,
                    Err(e) => return Err(e),
                },
                Err(ClientError::Server(ApiError::UnknownTransaction)) => continue,
                Err(e) => {
                    let _ = self.rollback(tx).await;
                    return Err(e);
                }
            }
        }
        Err(ClientError::TxLost)
    }
}
