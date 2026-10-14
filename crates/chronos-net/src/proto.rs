//! Wire protocol: client API types and Raft state-machine operations.
//!
//! Framing on both the client port and the peer port is identical:
//! `u32` big-endian length followed by a bincode payload.

use serde::{Deserialize, Serialize};

pub type ReqId = u64;
pub type TxId = u64;

/// Client -> server.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Request {
    Put { req_id: ReqId, key: Vec<u8>, value: Vec<u8> },
    Get { req_id: ReqId, key: Vec<u8> },
    Delete { req_id: ReqId, key: Vec<u8> },
    Begin { req_id: ReqId },
    TxGet { req_id: ReqId, tx: TxId, key: Vec<u8> },
    TxPut { req_id: ReqId, tx: TxId, key: Vec<u8>, value: Vec<u8> },
    TxDelete { req_id: ReqId, tx: TxId, key: Vec<u8> },
    Commit { req_id: ReqId, tx: TxId },
    Rollback { req_id: ReqId, tx: TxId },
    GetAtVersion { req_id: ReqId, key: Vec<u8>, version: u64 },
}

impl Request {
    pub fn req_id(&self) -> ReqId {
        match self {
            Request::Put { req_id, .. }
            | Request::Get { req_id, .. }
            | Request::Delete { req_id, .. }
            | Request::Begin { req_id }
            | Request::TxGet { req_id, .. }
            | Request::TxPut { req_id, .. }
            | Request::TxDelete { req_id, .. }
            | Request::Commit { req_id, .. }
            | Request::Rollback { req_id, .. }
            | Request::GetAtVersion { req_id, .. } => *req_id,
        }
    }

    /// True for operations that go through Raft (durable, deduplicated).
    pub fn is_mutating(&self) -> bool {
        matches!(
            self,
            Request::Put { .. } | Request::Delete { .. } | Request::Commit { .. }
        )
    }
}

/// Server -> client.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Response {
    Result { req_id: ReqId, result: ApiResult },
    /// This node is not the leader. Retry against `leader_hint` (if known)
    /// with the SAME req_id; deduplication makes the retry safe.
    NotLeader { req_id: ReqId, leader_hint: Option<u64> },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ApiResult {
    Ok(ResponseBody),
    Err(ApiError),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ResponseBody {
    Value(Option<Vec<u8>>),
    TxId(TxId),
    CommitVersion(u64),
    Empty,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum ApiError {
    UnknownTransaction,
    Conflict,
    InvalidVersion,
    Storage(String),
    Internal(String),
}

/// Operations replicated through the Raft log. Every variant carries the
/// client's `req_id` so the applier can deduplicate retried requests and
/// followers converge on identical state.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum StateMachineOp {
    Put { req_id: ReqId, key: Vec<u8>, value: Vec<u8> },
    Delete { req_id: ReqId, key: Vec<u8> },
    TxCommit {
        req_id: ReqId,
        read_seq: u64,
        writes: Vec<(Vec<u8>, Option<Vec<u8>>)>,
    },
}

impl StateMachineOp {
    pub fn req_id(&self) -> ReqId {
        match self {
            StateMachineOp::Put { req_id, .. }
            | StateMachineOp::Delete { req_id, .. }
            | StateMachineOp::TxCommit { req_id, .. } => *req_id,
        }
    }
}

/// Outcome of applying a `StateMachineOp`, recorded per req_id for
/// exactly-once semantics across leader failover and client retries.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum TxOutcome {
    Committed { seq: u64 },
    Aborted,
}

/// Length-prefixed framing helpers shared by client and peer transports.
pub mod framing {
    use super::*;

    pub fn encode<T: Serialize>(msg: &T) -> Vec<u8> {
        let payload = bincode::serialize(msg).expect("bincode encode");
        let mut buf = Vec::with_capacity(4 + payload.len());
        buf.extend_from_slice(&(payload.len() as u32).to_be_bytes());
        buf.extend_from_slice(&payload);
        buf
    }

    pub fn decode_len(hdr: &[u8; 4]) -> usize {
        u32::from_be_bytes(*hdr) as usize
    }

    pub const MAX_FRAME: usize = 64 * 1024 * 1024;
}
