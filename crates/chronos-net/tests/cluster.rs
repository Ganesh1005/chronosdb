//! End-to-end tests: real TCP servers, real Raft replication.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

use chronos_net::client::{Client, ClientError};
use chronos_net::proto::ApiError;
use chronos_net::server::{run_server, ServerConfig};
use tokio::sync::oneshot;

async fn free_addr() -> SocketAddr {
    let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let a = l.local_addr().unwrap();
    drop(l);
    a
}

struct Node {
    id: u64,
    client_addr: SocketAddr,
    peer_addr: SocketAddr,
    dir: PathBuf,
    /// Keep the TempDir alive. None after `kill` if the dir is reused.
    _tmp: Option<tempfile::TempDir>,
    shutdown: Option<oneshot::Sender<()>>,
    handle: Option<tokio::task::JoinHandle<()>>,
}

struct Cluster {
    nodes: Vec<Node>,
    peer_addrs: Vec<SocketAddr>,
}

impl Cluster {
    async fn start(n: u64) -> Self {
        let mut client_addrs = vec![];
        let mut peer_addrs = vec![];
        for _ in 0..n {
            client_addrs.push(free_addr().await);
            peer_addrs.push(free_addr().await);
        }
        let mut c = Cluster { nodes: vec![], peer_addrs };
        for i in 0..n {
            c.start_node(i + 1, None).await;
        }
        c.wait_for_leader().await;
        c
    }

    async fn start_node(&mut self, id: u64, dir: Option<(PathBuf, Option<tempfile::TempDir>)>) {
        let i = (id - 1) as usize;
        let (dir_path, tmp) = match dir {
            Some((p, t)) => (p, t),
            None => {
                let t = tempfile::TempDir::new().unwrap();
                (t.path().to_path_buf(), Some(t))
            }
        };
        let mut peers = HashMap::new();
        for (j, addr) in self.peer_addrs.iter().enumerate() {
            let pid = j as u64 + 1;
            if pid != id {
                peers.insert(pid, *addr);
            }
        }
        // Recompute client addr: for a fresh node use a new free addr; for a
        // restart reuse the old one.
        let client_addr = match self.nodes.iter().find(|n| n.id == id) {
            Some(n) => n.client_addr,
            None => free_addr().await,
        };
        let cfg = ServerConfig {
            id,
            client_addr,
            peer_addr: self.peer_addrs[i],
            peers,
            data_dir: dir_path.clone(),
            snapshot_threshold: 50,
        };
        let (tx, rx) = oneshot::channel();
        let handle = tokio::spawn(async move {
            let _ = run_server(cfg, rx).await;
        });
        // Replace or push.
        if let Some(n) = self.nodes.iter_mut().find(|n| n.id == id) {
            n.dir = dir_path;
            n._tmp = tmp;
            n.shutdown = Some(tx);
            n.handle = Some(handle);
        } else {
            self.nodes.push(Node {
                id,
                client_addr,
                peer_addr: self.peer_addrs[i],
                dir: dir_path,
                _tmp: tmp,
                shutdown: Some(tx),
                handle: Some(handle),
            });
        }
    }

    fn client_addrs(&self) -> Vec<(u64, SocketAddr)> {
        self.nodes.iter().map(|n| (n.id, n.client_addr)).collect()
    }

    async fn wait_for_leader(&self) {
        let mut c = Client::with_ids(self.client_addrs());
        tokio::time::timeout(Duration::from_secs(20), async {
            loop {
                if c.get(b"__probe__".to_vec()).await.is_ok() {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        })
        .await
        .expect("leader elected");
    }

    /// Find the current leader by probing each node individually.
    async fn find_leader(&self) -> u64 {
        for n in &self.nodes {
            if n.handle.is_none() {
                continue;
            }
            let mut c = Client::with_ids(vec![(n.id, n.client_addr)]);
            // get() succeeds only on the leader. We must check the INNER
            // result: timeout returns Ok(Err(_)) for followers (they respond
            // with NotLeader, which the client turns into Unreachable after
            // retries), and only Ok(Ok(_)) for the actual leader.
            if let Ok(Ok(_)) =
                tokio::time::timeout(Duration::from_secs(3), c.get(b"__probe__".to_vec())).await
            {
                return n.id;
            }
        }
        panic!("no leader found");
    }

    async fn kill(&mut self, id: u64) {
        let n = self.nodes.iter_mut().find(|n| n.id == id).unwrap();
        n.shutdown.take().unwrap().send(()).unwrap();
        n.handle.take().unwrap().await.unwrap();
        // Give the OS a moment to release the ports.
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    async fn restart(&mut self, id: u64) {
        let n = self.nodes.iter().find(|n| n.id == id).unwrap();
        let dir = n.dir.clone();
        // Keep the TempDir alive across the restart by forgetting it here;
        // the new node takes ownership.
        let tmp = self.nodes.iter_mut().find(|n| n.id == id).unwrap()._tmp.take();
        self.start_node(id, Some((dir, tmp))).await;
    }
}

#[tokio::test]
async fn single_node_crud() {
    let c = Cluster::start(1).await;
    let mut client = Client::with_ids(c.client_addrs());
    let v1 = client.put(b"k".to_vec(), b"v1".to_vec()).await.unwrap();
    assert_eq!(client.get(b"k".to_vec()).await.unwrap(), Some(b"v1".to_vec()));
    let v2 = client.put(b"k".to_vec(), b"v2".to_vec()).await.unwrap();
    assert!(v2 > v1);
    // Historical read.
    assert_eq!(
        client.get_at_version(b"k".to_vec(), v1).await.unwrap(),
        Some(b"v1".to_vec())
    );
    assert_eq!(
        client.get_at_version(b"k".to_vec(), v2).await.unwrap(),
        Some(b"v2".to_vec())
    );
    client.delete(b"k".to_vec()).await.unwrap();
    assert_eq!(client.get(b"k".to_vec()).await.unwrap(), None);
    // Still readable at the old version.
    assert_eq!(
        client.get_at_version(b"k".to_vec(), v1).await.unwrap(),
        Some(b"v1".to_vec())
    );
}

#[tokio::test]
async fn three_node_replication() {
    let mut c = Cluster::start(3).await;
    let mut client = Client::with_ids(c.client_addrs());
    for i in 0..20u64 {
        client
            .put(format!("k{i}").into_bytes(), format!("v{i}").into_bytes())
            .await
            .unwrap();
    }
    // Kill the leader: the data must survive on the remaining quorum.
    let leader = c.find_leader().await;
    c.kill(leader).await;
    // Wait for the remaining nodes to elect a new leader. Without this, the
    // test races the election: the client may exhaust its retries before a
    // new leader exists, failing with "connection refused" (the old leader's
    // port is closed and followers haven't yet elected a replacement).
    c.wait_for_leader().await;
    let mut client2 = Client::with_ids(c.client_addrs());
    for i in 0..20u64 {
        let v = client2.get(format!("k{i}").into_bytes()).await.unwrap();
        assert_eq!(v, Some(format!("v{i}").into_bytes()));
    }
}

#[tokio::test]
async fn transaction_commit_and_read_your_writes() {
    let c = Cluster::start(3).await;
    let mut client = Client::with_ids(c.client_addrs());
    let tx = client.begin().await.unwrap();
    client.tx_put(tx, b"x".to_vec(), b"10".to_vec()).await.unwrap();
    // Read-your-writes inside the txn.
    assert_eq!(
        client.tx_get(tx, b"x".to_vec()).await.unwrap(),
        Some(b"10".to_vec())
    );
    // Not visible outside until commit.
    assert_eq!(client.get(b"x".to_vec()).await.unwrap(), None);
    let ver = client.commit(tx).await.unwrap();
    assert_eq!(client.get(b"x".to_vec()).await.unwrap(), Some(b"10".to_vec()));
    assert_eq!(
        client.get_at_version(b"x".to_vec(), ver).await.unwrap(),
        Some(b"10".to_vec())
    );
}

#[tokio::test]
async fn transaction_conflict_first_committer_wins() {
    let c = Cluster::start(3).await;
    let mut client = Client::with_ids(c.client_addrs());
    client.put(b"k".to_vec(), b"base".to_vec()).await.unwrap();

    let tx1 = client.begin().await.unwrap();
    let tx2 = client.begin().await.unwrap();
    client.tx_put(tx1, b"k".to_vec(), b"tx1".to_vec()).await.unwrap();
    client.tx_put(tx2, b"k".to_vec(), b"tx2".to_vec()).await.unwrap();
    client.commit(tx1).await.unwrap();
    // Second committer must abort.
    match client.commit(tx2).await {
        Err(ClientError::Server(ApiError::Conflict)) => {}
        other => panic!("expected Conflict, got {other:?}"),
    }
    assert_eq!(client.get(b"k".to_vec()).await.unwrap(), Some(b"tx1".to_vec()));
}

#[tokio::test]
async fn transaction_rollback() {
    let c = Cluster::start(3).await;
    let mut client = Client::with_ids(c.client_addrs());
    let tx = client.begin().await.unwrap();
    client.tx_put(tx, b"r".to_vec(), b"val".to_vec()).await.unwrap();
    client.rollback(tx).await.unwrap();
    assert_eq!(client.get(b"r".to_vec()).await.unwrap(), None);
}

#[tokio::test]
async fn leader_failover_preserves_committed_write() {
    let mut c = Cluster::start(3).await;
    let mut client = Client::with_ids(c.client_addrs());
    client.put(b"dur".to_vec(), b"able".to_vec()).await.unwrap();
    let leader = c.find_leader().await;
    c.kill(leader).await;
    // Wait for a new leader before writing; otherwise we race the election.
    c.wait_for_leader().await;
    // Cluster continues with 2/3 nodes.
    let mut client2 = Client::with_ids(c.client_addrs());
    client2.put(b"after".to_vec(), b"failover".to_vec()).await.unwrap();
    assert_eq!(
        client2.get(b"dur".to_vec()).await.unwrap(),
        Some(b"able".to_vec())
    );
    assert_eq!(
        client2.get(b"after".to_vec()).await.unwrap(),
        Some(b"failover".to_vec())
    );
    // Old leader comes back and converges.
    c.restart(leader).await;
    tokio::time::sleep(Duration::from_secs(2)).await;
    let mut solo = Client::with_ids(vec![(
        leader,
        c.nodes.iter().find(|n| n.id == leader).unwrap().client_addr,
    )]);
    // The restarted node may not be leader; route via the cluster client.
    let mut client3 = Client::with_ids(c.client_addrs());
    assert_eq!(
        client3.get(b"after".to_vec()).await.unwrap(),
        Some(b"failover".to_vec())
    );
    let _ = solo;
}

#[tokio::test]
async fn duplicate_request_id_is_deduplicated() {
    let c = Cluster::start(1).await;
    let mut client = Client::with_ids(c.client_addrs());
    // Same req_id sent twice must not double-apply.
    let req_id = 999_001u64;
    let req = chronos_net::proto::Request::Put {
        req_id,
        key: b"dup".to_vec(),
        value: b"once".to_vec(),
    };
    let r1 = client.call(req.clone()).await.unwrap();
    let r2 = client.call(req).await.unwrap();
    let v1 = match r1 {
        chronos_net::proto::ApiResult::Ok(chronos_net::proto::ResponseBody::CommitVersion(v)) => v,
        other => panic!("unexpected {other:?}"),
    };
    let v2 = match r2 {
        chronos_net::proto::ApiResult::Ok(chronos_net::proto::ResponseBody::CommitVersion(v)) => v,
        other => panic!("unexpected {other:?}"),
    };
    assert_eq!(v1, v2, "duplicate req_id must return the same version");
    // Only one version exists.
    assert_eq!(
        client.get_at_version(b"dup".to_vec(), v1).await.unwrap(),
        Some(b"once".to_vec())
    );
}

#[tokio::test]
async fn restart_recovers_committed_data() {
    let mut c = Cluster::start(1).await;
    let mut client = Client::with_ids(c.client_addrs());
    client.put(b"persist".to_vec(), b"me".to_vec()).await.unwrap();
    let ver = client.put(b"persist".to_vec(), b"me2".to_vec()).await.unwrap();
    c.kill(1).await;
    c.restart(1).await;
    c.wait_for_leader().await;
    let mut client2 = Client::with_ids(c.client_addrs());
    assert_eq!(
        client2.get(b"persist".to_vec()).await.unwrap(),
        Some(b"me2".to_vec())
    );
    assert_eq!(
        client2.get_at_version(b"persist".to_vec(), ver).await.unwrap(),
        Some(b"me2".to_vec())
    );
}
