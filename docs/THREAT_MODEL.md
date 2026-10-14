# Threat Model & Failure Analysis

## Assumptions

* **Crash-recovery, not Byzantine.** Nodes crash (fail-stop) and recover;
  they do not lie. No protection against malicious nodes or clients.
* **Trusted network.** No TLS, no authentication on client or peer ports.
  Anyone who can TCP-connect can read/write data and join Raft traffic.
  (Production: add mTLS + auth; the framing layer is the place.)
* **Single Raft group.** All keys live in one replicated log. No sharding,
  no cross-shard transactions.
* **Durability = fsync.** Acknowledged writes survive host crashes *if* the
  disk honors fsync. Disk lies, filesystem bugs, and power loss during
  fsync are out of scope (as they are for every database).

## Failure handling matrix

| Failure | Behavior |
|---|---|
| Follower crash | Leader retries; on restart the follower recovers its log from WAL and catches up via AppendEntries or InstallSnapshot. |
| Leader crash **before** quorum commit | Write uncommitted; client gets no ack, retries with same req_id; new leader re-proposes (or the old entry is re-committed if the old leader returns — deduped by req_id). |
| Leader crash **after** quorum commit | Write is durable (quorum has it). New leader re-commits it (§8); client retry returns the recorded outcome. Covered by `leader_crash_after_quorum_commit_preserves_the_write`. |
| Network partition (minority isolated) | Minority cannot commit (no quorum) and cannot ack. Majority continues. Isolated old leader steps down when it sees a higher term. Covered by `minority_cannot_acknowledge_writes`. |
| Partition heals | Logs converge via standard Raft conflict repair; divergent uncommitted suffixes are truncated. |
| Stale leader serving reads | Documented: reads are served from the leader's applied state without a quorum check. A partitioned leader serves stale reads until it discovers the new term. Fix: read-index protocol (not implemented). |
| Duplicate / reordered / dropped messages | Deduplicated by req_id (client ops) and idempotent log merge (Raft). Covered by `duplicate_and_reordered_messages_are_safe` and `chaos_no_safety_violation`. |
| Torn WAL write (crash mid-append) | Unsynced tail is truncated on recovery; synced prefix is intact. Covered by storage tests. |
| Checksum mismatch **before** the durable boundary | Loud corruption error; node refuses to start (fail-stop) rather than serve corrupt data. |
| Disk full | Writes fail with `StorageFull`; the node halts new writes but keeps serving reads from committed state. Acknowledged writes are never lost (they were fsync'd before ack). |
| Client retry after failover | Same req_id → `completed` map returns the recorded outcome; never double-applies. Covered by `duplicate_request_id_is_deduplicated`. |
| Transaction conflict | Loser gets `Conflict` (first-committer-wins), deterministic on all replicas. Covered by `transaction_conflict_first_committer_wins`. |
| Snapshot installation | Follower replaces MVCC state atomically; `completed` map comes with the snapshot so dedup survives. Covered by `lagging_node_recovers_via_snapshot`. |

## What is NOT handled

* **Malicious actors.** No auth, no encryption, no Byzantine tolerance.
* **Clock skew.** The Raft core uses a monotonic clock only for timeouts;
  correctness never depends on wall time. (The sim injects virtual time.)
* **Disk corruption that the filesystem doesn't report.** Checksums catch
  it on read; silent corruption below the checksum layer is out of scope.
* **Split-brain with manual intervention.** A minority partition will not
  ack writes, but an operator who *forces* the minority to serve traffic
  can fork the database. Don't do that.
* **Unbounded `completed` map growth.** Request-ID history is kept forever
  (persisted in snapshots). At very large scale this needs client sessions
  with bounded history. See LIMITATIONS.md.
