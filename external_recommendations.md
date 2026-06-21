# Architecture Review: apps/bondy_oplog

Reviewer: external distributed-systems architect (Erlang, 20+ years)
Date: 2026-06-21
Scope: Core write-path, replication, and compaction layers

---

## 1. Executive Summary

`bondy_oplog` is a sophisticated, well-instrumented replicated event log built on Merkle Search Trees. It has clearly been hardened through extensive operational experience — telemetry is thorough, failure modes are handled explicitly, and design comments explain *why* decisions were made.

The codebase has evolved through a deliberate performance constraint: **a prior multi-process write path produced unacceptable latency, so the architecture was intentionally collapsed into a single per-instance gen_server.** Many seemingly complex features (fused writer, install coalescing, async compaction catch-up, AAE hibernation) are actually symptoms of retaining legacy multi-process components alongside this newer single-process model.

The key insight is that **the "fused" mode is not a niche optimization — it is the validated correct architecture for all instances.** The remaining applier process is a historical artifact that forces complex cross-process protocols (async catch-up, flow-control atomics, drain barriers) whose only purpose is to work around a process boundary the system has already proven should not exist on the write path.

---

## 2. Structural Observations

### 2.1 The Single-Process Constraint Is Correct

Evidence from the code itself:

| Path | Process hops | Source |
|---|---|---|
| `append_fast` (lock-free) | Caller → WAL (direct call) | `bondy_oplog_instance:do_append_fast/4` |
| `append` (slow fallback) | Caller → Instance → WAL | `bondy_oplog_instance:append/2` |
| Durable drain | WAL → Applier → Instance (cast) | `bondy_oplog_applier:drain_loop/1` + `install_local_batch` cast |
| Fused drain | Instance reads WAL + installs inline | `bondy_oplog_instance:run_fused_drain/1` |

The `fused` path eliminates the instance↔applier cast round-trip (the H1 bottleneck). The `install_coalesce_max` (A4), `install_in_flight` flow-control atomics, `drain_resume` casts, and `pending_compaction` async catch-up protocol all exist only because the applier and instance are separate processes. In the fused path, none of these exist.

**Recommendation: Eliminate the applier process entirely.** Merge the durable and fused drain paths into a single instance-owned loop. The applier module (`~3,600 lines`) should be removed from the supervision tree. The instance already has the fused infrastructure — it is the validated path, and making it universal removes the need to maintain two parallel implementations.

### 2.2 The Async Compaction Catch-Up Is a Self-Inflicted Problem

The `pending_compaction` record, `catch_up_done` cast protocol, and `compaction_catch_up_timeout` watchdog exist because:

1. The instance compacts the MST (truncates at a frontier).
2. Peer-merged events in the truncated range might not yet be folded into the projection.
3. The applier owns the projection fold, so the instance must cast pairs to it, wait for `catch_up_done`, then truncate.

If the applier is eliminated and the instance folds peer events **inline** during `integrate_peer_root` (as the fused path already does via `fused_replay_cell_events/1`), the projection is always current before truncation is considered. The entire async catch-up protocol disappears.

### 2.3 Functional Decomposition Without Process Boundaries

`bondy_oplog_instance.erl` is 5,486 lines with a `#state{}` record of ~35 fields. The size is a maintenance hazard, but the **process boundary is not the place to split it** — that was tried and rejected for performance reasons.

Instead, partition the code into **internal modules that operate on sub-records**:

| Module | Responsibility | State sub-record |
|---|---|---|
| `bondy_oplog_instance_writer` | HLC, Seq, validator, event building | `#writer_state{}` |
| `bondy_oplog_instance_store` | MST handle, overlay table, reads | `#store_state{}` |
| `bondy_oplog_instance_drain` | WAL reader loop, batching, inline install | `#drain_state{}` |
| `bondy_oplog_instance_compactor` | Frontier, snapshot, truncate, watermark | `#compactor_state{}` |
| `bondy_oplog_instance_aae` | Root hash, page serving, missing-set | `#aae_state{}` |

The top-level `bondy_oplog_instance` remains a thin `handle_call`/`handle_info` router. The 35-field `#state{}` becomes a nested record, and each sub-module exports functions like `drain_step(DrainState, WriterState, StoreState) -> {NewDrain, NewStore, Actions}`. This preserves single-process latency while making the code maintainable.

### 2.4 Fused Mode Duplication Is a Maintenance Fork

The `fused` path reimplements the applier's drain loop inside `bondy_oplog_instance`: `fused_collect_frames`, `fused_apply_batch`, `fused_commit_now`, `fused_replay_cell_events`, `fused_verify_batch`, etc. The comments note these are "state-free leaves reused verbatim from the applier," but the wiring is still duplicated.

With the applier removed, this duplication becomes unnecessary. Extract a **shared `bondy_oplog_drain` module** that implements the drain loop as pure functions parameterized by callbacks:

- `verify_and_install_local/2` — inline in the fused path, no-op in the legacy path (if any remains).
- `commit_and_advance/1` — always inline in the unified path.

The applier process is the only consumer of the current `bondy_oplog_applier` process model. Once it is gone, the duplication is gone.

---

## 3. Correctness & Safety

### 3.1 `await_apply` Barriers Are Latency Traps

Multiple public APIs (`root_hash/1`, `sync/2`, `bootstrap/2`, `truncate_prefix/2`) call `await_apply/1` (default 5s timeout) before proceeding. This blocks the caller until the applier drains the overlay.

Under sustained writes, the overlay is rarely empty. A caller doing `append/2` followed by `sync/2` will hit the 5s barrier. The comment in `bondy_oplog:compact/1` already acknowledges this problem explicitly:

> "The barrier was not just redundant but harmful: under sustained writes the overlay never reaches 0, so the 5s `await_apply` timed out every cycle and compaction effectively never ran."

This same reasoning applies to `sync/2` and `bootstrap/2`. A sync session operates on a **consistent snapshot** of the MST at the moment it starts. The overlay is a transient window; events arriving during the sync are handled by the next sync. Removing `await_apply` from these paths removes the latency trap.

**Recommendation:** Remove `await_apply` from `sync/2`, `bootstrap/2`, and `root_hash/1`. For `truncate_prefix/2`, the drain is necessary for correctness (the operator-supplied watermark may intersect overlay-pending events), so it should remain but with a documented timeout policy.

### 3.2 `db_of/1` Silently Returns `undefined` for Unknown DB Atoms

```erlang
db_of(InstanceId) ->
    [Db | _] = binary:split(InstanceId, <<"/">>),
    try binary_to_existing_atom(Db, utf8) catch error:badarg -> undefined end.
```

If a new DB namespace is introduced (e.g., a new table type), this function returns `undefined`, causing the topology fingerprint lookup to fail silently. Two nodes with different sharding topologies could then sync, causing **silent data corruption**.

**Recommendation:** This should be a **hard error**, not `undefined`. If the DB atom does not exist, the node has not loaded the configuration for that namespace and must not participate in sync. Alternatively, validate the DB segment at instance startup and cache the resolved atom in the registry.

### 3.3 `persistent_term` for Topology Fingerprints

`set_topology_fingerprint/2` and `topology_fingerprint/1` use `persistent_term`. The registry module explicitly avoids `persistent_term` because `put/2` triggers a **global GC scan of every process on the node**. Topology fingerprints are written "once at provision" but in long-lived systems, reconfiguration or rolling upgrades can trigger writes at runtime.

**Recommendation:** Move topology fingerprints to the **registry ETS table** or a dedicated `read_concurrency` ETS table. The lookup cost is negligible compared to the GC scan cost.

### 3.4 `one_for_all` Supervision Blast Radius

The per-instance supervisor (`bondy_oplog_instance_sup`) uses `one_for_all` for `[Instance, WAL, Applier, Scrubber]`. If the WAL crashes (disk full, `enomem`), the entire subtree restarts, losing:
- In-flight overlay rows (ETS dies with no heir).
- Applier verify state.
- Compaction async catch-up state.
- Scrubber progress.

Recovery is robust (WAL recovery + MST resume), but the restart storm under sustained load can cause cascading failures.

**Recommendation:** With the applier removed, the subtree is `[Instance, WAL, Scrubber]`. Evaluate whether the **WAL** can be restarted independently. The instance already has `ensure_wal_pid/1` to re-resolve the WAL pid from the registry. If the WAL is independently restartable, the instance could pause appends and resume rather than tearing down the entire MST + overlay + projection state. The `one_for_all` strategy is conservative; it trades availability for consistency, but consistency is already guaranteed by WAL recovery.

### 3.5 `append_many_fast` Memory Pressure

`append_many_fast` builds all events in the caller's process, stages them in the overlay, then calls `append_batch` on the WAL. On failure, it rolls back with `lists:foreach` over all events.

For a large batch, this is a large heap allocation and a synchronous rollback loop. The WAL already validates `max_batch_bytes`; the caller should pre-chunk to this limit.

**Recommendation:** Chunk large `append_many_fast` batches at the caller to `max_batch_bytes` boundaries. This bounds the caller's heap, the WAL frame size, and the rollback cost.

---

## 4. Performance

### 4.1 Shared-Memory WAL→Instance Communication

The current WAL writer is a gen_server. Even in the fused path, the instance polls the WAL via `bondy_oplog_wal_reader:next/1` (a gen_server call or fd read). For the absolute fastest write path, the WAL writer could publish new frames into a **shared-memory ring buffer** (an ETS table or a `persistent_term` array of frames) that the instance polls lock-free.

- **Common case:** WAL writes frame to disk, writes metadata to ring buffer, instance picks it up without messaging.
- **Backstop:** Instance calls the WAL gen_server only when the ring buffer is empty.

**Recommendation:** This is a significant optimization but requires careful design. Evaluate after the applier removal; with the process hop eliminated, the remaining WAL call may be the next bottleneck.

### 4.2 Group Commit vs. Batched Default

The WAL supports `group_commit` in `per_write` mode to coalesce multiple concurrent `append` calls into one `datasync`. However, `per_write` is fundamentally bounded by fsync rate (~5k/s). The `batched` mode with `await_durable/3` reaches ~200k/s.

**Recommendation:** The comments already recommend `batched` for high-throughput. Make `batched` the **global default** and require `per_write` to be explicitly opted-in for security-sensitive namespaces. This removes the need for group commit complexity in the common case.

### 4.3 AAE Hibernation as a Symptom

`maybe_hibernate_after` forces hibernation after AAE operations because they build large transient heaps on the long-lived instance process. This is a reactive fix.

**Recommendation:** If AAE is moved to a separate worker process (as suggested in the functional decomposition), the hibernation hack disappears entirely. A short-lived `bondy_oplog_aae_worker` spawned per sync request can build its heap, serve the pages, and die.

### 4.4 ETS Overlay Contention

The overlay table is `public` so callers can insert directly in `append_fast`. The code uses `atomics` mirrors for size/byte counts to avoid `ets:info/2`. However, if `decentralized_counters` is not set on the overlay table, concurrent `ets:insert` calls still contend on the table's lock bucket.

**Recommendation:** Explicitly add `decentralized_counters, true` (OTP 22+) to the overlay table creation. This further reduces contention on the fast path.

---

## 5. Prioritized Recommendations

| Priority | Recommendation | Expected Impact |
|---|---|---|
| **P1** | **Eliminate the applier process.** Merge durable and fused drain paths into a single instance-owned loop. Remove `bondy_oplog_applier` from supervision. | Removes ~3,600 lines, eliminates async catch-up protocol, install coalescing, flow-control atomics, and drain_resume. |
| **P1** | **Functional decomposition of `bondy_oplog_instance`** into internal modules (`writer`, `store`, `drain`, `compactor`, `aae`) operating on sub-records. | Reduces 5,486-line module to ~800-line router; preserves single-process latency. |
| **P2** | **Remove `await_apply` barriers** from `sync/2`, `bootstrap/2`, and `root_hash/1`. | Eliminates 5s latency traps under sustained writes. |
| **P2** | **Harden `db_of/1`** to fail hard on unknown DB atoms. | Prevents cross-topology sync corruption. |
| **P2** | **Move topology fingerprints** from `persistent_term` to registry ETS. | Avoids global GC scans. |
| **P3** | **Evaluate independent WAL restart** in supervision tree (after applier removal). | Improves availability under disk pressure. |
| **P3** | **Chunk `append_many_fast` batches** at `max_batch_bytes` boundaries. | Bounds caller heap and rollback cost. |
| **P3** | **Add `decentralized_counters`** to overlay ETS. | Reduces lock contention on fast path. |
| **P4** | **Shared-memory WAL→instance ring buffer** (post-applier removal). | Potential next bottleneck elimination. |
| **P4** | **Make `batched` fsync the default.** | Removes `per_write` group-commit complexity. |

---

## 6. Closing Note

The codebase is not over-engineered — it is **appropriately engineered for the constraints it has proven empirically**. The `fused` mode is not a hack; it is the architecture that the performance data validated. The remaining work is to **finish the job**: remove the legacy applier process, consolidate the drain loop, and decompose the large instance module into maintainable sub-modules without reintroducing the process boundaries that were already rejected. The result will be a smaller, faster, and more correct system.
