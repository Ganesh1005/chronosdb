# Limitations & Unmet Requirements

Honest list of what ChronosDB does not do (yet).

## Correctness-adjacent

1. **Stale reads from a partitioned leader.** Reads don't do a quorum
   check. Fix: read-index protocol (leader confirms quorum before serving).
2. **No read-your-write guarantee across failover.** A client's write acked
   by L1 is visible after failover only once L2 re-commits and applies it;
   there is a brief window where a read may not see it.
3. **Snapshot isolation anomalies.** SI permits write skew (two txns each
   read x and y, then one writes x and the other writes y — both commit).
   Documented and tested as *allowed*; use is not serializable. If you need
   serializability, add predicate locking or SSI.
4. **Unbounded `completed` (req_id) map.** Grows forever; production needs
   client sessions with bounded dedup windows.
5. **Unbounded pending-client map** (capped at 50k with an overload error,
   but a slowloris of uncommittable writes could still pressure memory).
6. **No backpressure on peer channels.** Full peer queues drop Raft
   messages (safe: retries recover) and count `dropped_sends`, but a
   sustained overload degrades to livelock-ish retry storms.

## Performance

7. **Single-threaded driver.** All Raft + MVCC work happens on one thread;
   fsync latency is on the critical path of every commit. Measured:
   ~1400 puts/s single-node, ~500 puts/s 3-node (see README benchmarks).
8. **Full MVCC snapshot encodes the whole keyspace** (O(state) per
   snapshot). Fine for demos; production needs incremental/parallel
   snapshotting.
9. **No batching.** Each client op is one Raft entry. Group commit / batching
   would multiply throughput.
10. **TCP framing without compression or pipelining.**

## Operations

11. **Static membership.** No joint-consensus membership change; add/remove
    nodes by restarting with a new config (and manual data copy).
12. **No TLS / auth.** See THREAT_MODEL.md.
13. **No metrics / observability** beyond stderr logs.
14. **Single Raft group; no sharding.** Write throughput is capped by one
    log. Cross-shard transactions are out of scope (and not claimed).

## What was explicitly requested and is delivered

* Append-only WAL with framing/checksums/torn-write recovery — yes.
* Snapshots + compaction — yes (both storage-level and Raft/MVCC-level).
* MVCC snapshot isolation, first-committer-wins, historical reads,
  GC safe for active snapshots — yes, with a documented write-skew test.
* Raft from scratch (elections, log matching, quorum commit, §5.4.2,
  snapshots, deterministic fault-injection tests) — yes.
* TCP API with PUT/GET/DELETE/BEGIN/TX_GET/TX_PUT/COMMIT/ROLLBACK/
  GET_AT_VERSION, request IDs, dedup, leader routing, retry semantics — yes.
* 59 automated tests (unit, integration, deterministic chaos) — yes.
* Benchmarks with measured (not claimed) numbers — yes, in README.
* Docker Compose 3-node — yes.
