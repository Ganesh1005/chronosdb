# ChronosDB Architecture

A distributed transactional key-value database in stable Rust. One Raft
group replicates a log of MVCC write batches; every replica applies the log
to an identical MVCC state machine.

## Layer diagram

```
┌─────────────────────────────────────────────────────────────┐
│  chronos-net: TCP client API + server driver                │
│   - length-prefixed bincode protocol (client + peer ports)  │
│   - request-ID deduplication (exactly-once across retries)  │
│   - leader routing via NotLeader hints                      │
│   - driver thread: owns Raft Node + MVCC + durability       │
├─────────────────────────────────────────────────────────────┤
│  chronos-raft: Raft consensus from scratch                  │
│   - pure deterministic Node core (time/messages/effects)    │
│   - leader election, log replication, quorum commit         │
│   - §5.4.2: only current-term entries commit by counting    │
│   - snapshots + InstallSnapshot for lagging followers       │
├─────────────────────────────────────────────────────────────┤
│  chronos-mvcc: snapshot-isolated transactions               │
│   - multi-version chains, consistent snapshots, tombstones  │
│   - optimistic concurrency, first-committer-wins            │
│   - deterministic apply_decided() for the Raft state machine│
├─────────────────────────────────────────────────────────────┤
│  chronos-storage: crash-safe storage primitives             │
│   - checksummed WAL (framing, CRC32C, torn-write recovery)  │
│   - atomic snapshots, prefix compaction, suffix truncation  │
└─────────────────────────────────────────────────────────────┘
```

Only std + general-purpose crates (tokio, serde, bincode, crc32c, thiserror).
No RocksDB/SQLite/sled/redb, no Raft libraries, no DB engines.

## Write path (single key)

```
client                          leader                         followers
  │ PUT(k,v) req_id=7              │                                │
  ├───────────────────────────────>│                                │
  │                                │ Raft: append entry 12          │
  │                                │ persist (WAL fsync)            │
  │                                ├───────────────────────────────>│ AppendEntries
  │                                │<───────────────────────────────┤ success (match=12)
  │                                │ quorum(2/3) → commit 12        │
  │                                │ Apply(12): MVCC commit seq=12  │
  │                                │ persist applied.index (fsync)  │
  │<─ CommitVersion{12} ───────────┤                                │
```

MVCC commit seqs **are** Raft log indices: versions are stable across
crashes and replay is deterministic.

## Transaction path

```
client                          leader
  │ BEGIN ───────────────────────>│ mvcc.begin() → tx=41 (read_seq = last applied)
  │ TX_PUT(41,k,v) ──────────────>│ buffered locally (not durable yet)
  │ TX_GET(41,k) ────────────────>│ read-your-writes from buffer + snapshot
  │ COMMIT(41) ──────────────────>│ take_tx(41) → (read_seq, writes)
  │                                │ Raft: replicate TxCommit{read_seq, writes}
  │                                │ on commit, every replica runs
  │                                │   apply_decided(index, read_seq, writes):
  │                                │     abort if any key has version > read_seq
  │                                │     else install versions with seq=index
  │<─ CommitVersion{index} ────────┤  (or Conflict if aborted)
```

Validation happens **deterministically at apply time** on every replica, so
all replicas agree on commit vs abort. Single-key PUT/DELETE are degenerate
transactions (blind writes, no read set).

## Read path

Reads (`GET`, `GET_AT_VERSION`, `TX_GET`) are served by the leader from its
applied MVCC state — no Raft round trip. This is fast but has the standard
stale-leader-during-partition caveat (documented in LIMITATIONS.md); the fix
is a read-index/lease protocol.

## Crash recovery

```
startup:
  1. load latest MVCC snapshot (index S, includes completed req_id map)
  2. recover Raft log from WAL (base B; invariant S >= B)
  3. read applied.index → A
  4. replay Raft log entries (S, A] into MVCC (rebuilds versions + completed)
  5. serve clients; Raft re-commit drives Apply for anything newer
```

Safety argument: an entry is applied only after it commits; committed
entries are in the durable Raft log; on recovery they are re-committed and
re-applied in order; the `completed` req_id map makes re-application a
no-op and client retries return the recorded outcome. Exactly-once.

## Snapshots and log compaction

Every `snapshot_threshold` applied entries (default 1000):

1. Encode MVCC + completed map → atomic snapshot file at index C (fsync).
2. `node.compact_log(C)` → Raft log prefix discarded (WAL compaction).

Order matters: the log is compacted **only after** the covering snapshot is
durable, preserving `S >= B`. A follower too far behind gets `InstallSnapshot`
with the leader's MVCC snapshot bytes and jumps forward.

## Sequence diagram: leader failover

```
t0: leader=L1 commits entry 12 (PUT k=v, req_id=7), acks client
t1: L1 crashes (kill -9)
t2: followers time out, elect L2 (term+1)
t3: L2 commits no-op entry 13 (term 2); entry 12 re-committed by §8
t4: client retries PUT req_id=7 → L2 has outcome → returns CommitVersion{12}
    (no double apply)
t5: L1 restarts, rejoins as follower, receives missing entries, converges
```

## Five most dangerous correctness risks (and mitigations)

1. **Torn WAL write / ack before fsync.** Frame = magic+len+CRC32C+payload;
   fsync before ack; recovery truncates unsynced torn tails, and treats
   corruption *before* the durable boundary as loud corruption.
2. **Raft §5.4.2 (committing old-term entries by counting).** `try_advance_commit`
   only advances on entries from the *current* term; old-term entries commit
   only as a side effect. Covered by `figure_8_never_commits_old_term_entry_by_counting`.
3. **Non-atomic first-committer-wins.** Validation + install happen in one
   `apply_decided` critical section, deterministically at log-apply time on
   every replica — no TOCTOU between validate and commit.
4. **Stale leader acking without quorum.** Acks (`ClientResponse::Committed`)
   fire only from `advance_commit` on a quorum; a deposed leader steps down
   on any higher-term message before it can commit.
5. **MVCC GC / snapshot-install races.** GC never removes a version visible
   to an active snapshot; snapshot installation replaces the whole MVCC
   state atomically on the driver thread; `S >= B` is enforced before log
   compaction.

A sixth bug was caught by the fault-injection harness during development:
followers truncated their log on *every* AppendEntries (even without
conflict), letting a stale duplicate RPC delete entries and create phantom
quorums via a stale `match_index`. Fixed with conflict-only truncation and
exact-match responses (`prev_log_index + entries.len()`).
