%% =============================================================================
%% SPDX-FileCopyrightText: 2023 - 2026 Leapsight
%% =============================================================================
%% Restart-recovery coverage for the per-instance projection content digest
%% (ISSUES.md AR-17, the MST-root-independent convergence oracle). Where
%% `bondy_oplog_content_digest_test` exercises the XOR maths and
%% `bondy_db_projection_leveled_test` the snapshot fold, this drives the FULL
%% durable stack (`bondy_db` facade → per-shard `bondy_oplog` instance → leveled
%% projection + pack-store MST) through a stop/restart and asserts the digest
%% survives by BOTH recovery paths:
%%
%%   - CLEAN restart — `terminate/2` persists the exact digest as a checkpoint
%%     seed, `init/1` restores it in O(1) and reports `ready` immediately (no
%%     fold).
%%   - CRASH restart (the seed wiped) — `init/1` finds no seed and recomputes
%%     the digest asynchronously from a boot-state projection snapshot; the
%%     instance reports `warming` until the fold lands the SAME digest the
%%     incremental apply path had maintained.
%%
%% The crash case is the load-bearing one: it proves the recomputed digest is
%% byte-identical to the incrementally-maintained one (the property the oracle
%% depends on).
%% =============================================================================
-module(bondy_oplog_content_digest_recovery_test).

-include_lib("eunit/include/eunit.hrl").

-define(FOLD, bondy_oplog_crdt_lww_register).
-define(SHARDS, 4).
-define(KEYS, 24).
-define(DB, content_digest_recovery_db).
-define(TOPOLOGY, bondy_db_topology_per_entity).

%% =============================================================================
%% Test generators
%% =============================================================================

clean_restart_restores_digest_test_() ->
    {timeout, 120, fun clean_restart_restores_digest/0}.

crash_restart_recomputes_digest_test_() ->
    {timeout, 120, fun crash_restart_recomputes_digest/0}.

%% =============================================================================
%% Tests
%% =============================================================================

clean_restart_restores_digest() ->
    {Sup0, Db0, LDir, PDir} = start_db(),
    Baseline =
        try
            {ok, T} = bondy_db:open_table(Db0, users, #{}),
            ok = write_keys(T, ?KEYS),
            ok = drain_all(),
            ok = wait_all_ready(),
            B = digest_map(),
            ?assert(map_size(B) >= 1),
            %% At least one shard must hold data (non-empty digest), else the
            %% test proves nothing about restore.
            ?assert(lists:any(fun(D) -> D =/= 0 end, maps:values(B))),
            ok = bondy_db:close_table(T),
            B
        after
            stop_db(Sup0, Db0)
        end,

    %% CLEAN restart: terminate wrote the seed; reopen restores it.
    {Sup1, Db1, _, _} = reopen_db(LDir, PDir),
    try
        {ok, _T1} = bondy_db:open_table(Db1, users, #{}),
        %% Every previously-known instance is authoritative IMMEDIATELY (the
        %% seed is installed synchronously in init — no recompute fold).
        maps:foreach(
            fun(InstanceId, Digest) ->
                ?assertEqual(
                    {ready, Digest},
                    bondy_oplog_instance:content_digest(InstanceId)
                )
            end,
            Baseline
        )
    after
        stop_db(Sup1, Db1),
        cleanup_dirs(LDir, PDir)
    end.

crash_restart_recomputes_digest() ->
    {Sup0, Db0, LDir, PDir} = start_db(),
    Baseline =
        try
            {ok, T} = bondy_db:open_table(Db0, users, #{}),
            ok = write_keys(T, ?KEYS),
            ok = drain_all(),
            ok = wait_all_ready(),
            B = digest_map(),
            ?assert(lists:any(fun(D) -> D =/= 0 end, maps:values(B))),
            ok = bondy_db:close_table(T),
            B
        after
            stop_db(Sup0, Db0)
        end,

    %% Simulate a CRASH: delete every checkpoint file so the clean-shutdown
    %% seed is gone. The durable leveled projection and the pack-store MST
    %% survive — exactly the post-crash on-disk state.
    Deleted = delete_checkpoints(PDir),
    ?assert(Deleted >= 1),

    {Sup1, Db1, _, _} = reopen_db(LDir, PDir),
    try
        {ok, _T1} = bondy_db:open_table(Db1, users, #{}),
        %% No seed ⇒ async recompute. Each instance is `warming` until its fold
        %% lands, then `ready` with the SAME digest the incremental path held.
        maps:foreach(
            fun(InstanceId, Digest) ->
                ?assertEqual(Digest, wait_ready_digest(InstanceId))
            end,
            Baseline
        )
    after
        stop_db(Sup1, Db1),
        cleanup_dirs(LDir, PDir)
    end.

%% =============================================================================
%% Helpers — lifecycle
%% =============================================================================

start_db() ->
    process_flag(trap_exit, true),
    {ok, _} = application:ensure_all_started(bondy_db),
    %% Quiesce background AAE + compaction so the test owns the checkpoint
    %% lifecycle deterministically.
    bondy_oplog_sync_scheduler:set_dispatch(undefined),
    bondy_oplog_gc_scheduler:set_trigger(undefined),
    LDir = make_tempdir("leveled"),
    PDir = make_tempdir("pack"),
    {Sup, Db} = open_db(LDir, PDir, #{seed => true}),
    {Sup, Db, LDir, PDir}.

reopen_db(LDir, PDir) ->
    %% Reopen over the SAME on-disk dirs; not a genesis peer this time, so no
    %% `seed` opt — the durable state is recovered, not re-seeded.
    {Sup, Db} = open_db(LDir, PDir, #{}),
    {Sup, Db, LDir, PDir}.

open_db(LDir, PDir, ExtraInstanceOpts) ->
    {ok, Sup} = bondy_db_leveled_sup:start_link(),
    {ok, Db} = bondy_db:open(?DB, #{
        topology => ?TOPOLOGY,
        topology_opts => #{sup => Sup, dir => LDir},
        shard_count => ?SHARDS,
        fold_module => ?FOLD,
        oplog_instance_opts => maps:merge(
            #{
                backend => bondy_mst_pack_store,
                storage_path => unicode:characters_to_binary(PDir)
            },
            ExtraInstanceOpts
        )
    }),
    {Sup, Db}.

stop_db(Sup, Db) ->
    _ = catch bondy_db:close(Db),
    %% Stop every instance so `terminate/2` runs (this is what writes the
    %% clean-shutdown digest seed).
    _ = [catch bondy_oplog:stop_instance(I) || I <- bondy_oplog:list_instances()],
    case is_process_alive(Sup) of
        true -> catch bondy_db_leveled_sup:stop(Sup);
        false -> ok
    end,
    ok.

cleanup_dirs(LDir, PDir) ->
    rmrf(LDir),
    rmrf(PDir),
    rmrf(wal_dir_for_this_db()),
    ok.

%% =============================================================================
%% Helpers — writes / digest
%% =============================================================================

write_keys(T, N) ->
    Realm = <<"r1">>,
    lists:foreach(
        fun(I) ->
            K = <<"k-", (integer_to_binary(I))/binary>>,
            H = bondy_db:tick(T),
            V = <<"v-", (integer_to_binary(I))/binary>>,
            ok = bondy_db:apply(T, Realm, K, {set, H, V})
        end,
        lists:seq(1, N)
    ).

%% Wait for every instance's applier to drain so the digest reflects all writes
%% (the hook fires on the applier's projection write, not on `apply/4`).
drain_all() ->
    lists:foreach(
        fun(I) -> _ = catch bondy_oplog_instance:await_apply(I) end,
        bondy_oplog:list_instances()
    ).

%% Map of InstanceId => Digest over every live instance (the table's shards).
digest_map() ->
    lists:foldl(
        fun(I, Acc) ->
            {_Status, Digest} = bondy_oplog_instance:content_digest(I),
            Acc#{I => Digest}
        end,
        #{},
        bondy_oplog:list_instances()
    ).

wait_all_ready() ->
    wait_all_ready(100).

wait_all_ready(0) ->
    error(content_digest_never_ready);
wait_all_ready(N) ->
    Ready = lists:all(
        fun(I) ->
            element(1, bondy_oplog_instance:content_digest(I)) =:= ready
        end,
        bondy_oplog:list_instances()
    ),
    case Ready of
        true -> ok;
        false ->
            timer:sleep(50),
            wait_all_ready(N - 1)
    end.

wait_ready_digest(InstanceId) ->
    wait_ready_digest(InstanceId, 200).

wait_ready_digest(InstanceId, 0) ->
    error({content_digest_never_ready, InstanceId});
wait_ready_digest(InstanceId, N) ->
    case bondy_oplog_instance:content_digest(InstanceId) of
        {ready, Digest} ->
            Digest;
        {warming, _Partial} ->
            timer:sleep(50),
            wait_ready_digest(InstanceId, N - 1)
    end.

%% Delete every `checkpoint.etf` under the pack dir, simulating the loss of the
%% clean-shutdown seed (a crash). Returns the count deleted.
delete_checkpoints(PDir) ->
    Files = filelib:wildcard(filename:join(PDir, "**/checkpoint.etf")),
    lists:foreach(fun(F) -> _ = file:delete(F) end, Files),
    length(Files).

%% =============================================================================
%% Helpers — dirs
%% =============================================================================

make_tempdir(Prefix) ->
    Base = filename:join([
        "/tmp",
        "bondy_oplog_content_digest_recovery",
        Prefix,
        integer_to_list(erlang:unique_integer([positive, monotonic]))
    ]),
    ok = filelib:ensure_dir(filename:join(Base, ".keep")),
    Base.

wal_dir_for_this_db() ->
    filename:join(["/tmp", "bondy_oplog_wal", os:getpid(), atom_to_list(?DB)]).

rmrf(Dir) ->
    case file:del_dir_r(Dir) of
        ok -> ok;
        {error, enoent} -> ok;
        {error, _} -> ok
    end.
