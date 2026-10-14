# ChronosDB

A distributed transactional key-value database in stable Rust, built from
scratch: append-only WAL storage, MVCC snapshot-isolated transactions, and
a from-scratch Raft implementation — no RocksDB, no sled, no Raft libraries.

## Layout

```
crates/
  chronos-storage/  checksummed WAL, atomic snapshots, compaction
  chronos-mvcc/     snapshot isolation, first-committer-wins, GC
  chronos-raft/     Raft consensus + deterministic fault-injection sim
  chronos-net/      TCP client API, server driver, CLI (`chronos`)
docs/
  ARCHITECTURE.md   layers, write/read/tx paths, recovery, diagrams
  THREAT_MODEL.md   failure matrix, assumptions
  LIMITATIONS.md    honest list of what it doesn't do
```

## Build & test

```bash
source "$HOME/.cargo/env"
cargo build --workspace
cargo test --workspace        # 59 tests, all green
```

## Run

Single node:

```bash
./target/debug/chronos serve --id 1 \
  --client-addr 127.0.0.1:7001 --peer-addr 127.0.0.1:7101 \
  --data-dir ./data1
```

Three nodes (separate terminals or hosts):

```bash
./target/debug/chronos serve --id 1 \
  --client-addr 127.0.0.1:7001 --peer-addr 127.0.0.1:7101 \
  --peer 2=127.0.0.1:7102 --peer 3=127.0.0.1:7103 --data-dir ./data1

./target/debug/chronos serve --id 2 \
  --client-addr 127.0.0.1:7002 --peer-addr 127.0.0.1:7102 \
  --peer 1=127.0.0.1:7101 --peer 3=127.0.0.1:7103 --data-dir ./data2

./target/debug/chronos serve --id 3 \
  --client-addr 127.0.0.1:7003 --peer-addr 127.0.0.1:7103 \
  --peer 1=127.0.0.1:7101 --peer 2=127.0.0.1:7102 --data-dir ./data3
```

Or with Docker:

```bash
docker compose up --build
```

## CLI

```bash
A=1=127.0.0.1:7001,2=127.0.0.1:7002,3=127.0.0.1:7003
./target/debug/chronos put --addrs $A hello world
./target/debug/chronos get --addrs $A hello
./target/debug/chronos get-at --addrs $A hello 2   # historical read
./target/debug/chronos delete --addrs $A hello
```

Transactions use the client library (`chronos_net::client::Client`):
`begin` → `tx_put`/`tx_get` → `commit` (or `rollback`). The `transaction()`
helper re-begins automatically on leader failover; write-write conflicts
return `Conflict` (first-committer-wins).

## Benchmarks (measured, not claimed)

`chronos bench --addrs $A --n 2000 --concurrency 4`, 64-byte values,
on this machine (debug build):

| Setup | Throughput | p50 | p95 | p99 |
|---|---|---|---|---|
| Single node | 1403 puts/s | 1.7 ms | 3.5 ms | 5.3 ms |
| 3 nodes | 508 puts/s | 5.5 ms | 16.4 ms | 26.5 ms |

* WAL growth: ~290 bytes/put single-node; ~180 bytes/put per node in a
  3-node cluster (2200 puts: 16K → 656K single; 388K/node clustered).
* Snapshots: MVCC snapshots taken every 1000 applied entries; Raft log
  compacted after each durable snapshot (verified: `snapshot-…1000.snap`,
  `snapshot-…2000.snap`, raft log 36K).
* Recovery: killing a node and restarting replays the WAL and rejoins in
  ~seconds (dominated by election timeout); committed data intact
  (integration test `restart_recovers_committed_data`).

Every commit fsyncs the Raft WAL and the applied-index file; the numbers
above are durability-first, debug-build numbers.

## Guarantees (and where they're tested)

* Never ack an uncommitted write (`chaos_no_safety_violation`,
  `minority_cannot_acknowledge_writes`, `figure_8_…`).
* Crash-safe: acked writes survive restarts (`restart_recovers_committed_data`,
  storage recovery tests).
* Exactly-once across retries/failover via request IDs
  (`duplicate_request_id_is_deduplicated`, `leader_failover_…`).
* Snapshot isolation with first-committer-wins; write skew documented as
  allowed (`documents_write_skew_is_permitted`).
* Deterministic fault injection: partitions, crashes, message
  drop/duplicate/reorder/delay, storage faults (`sim::tests::*`).

See `docs/` for the full architecture, threat model, and limitations.
