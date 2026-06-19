%% =============================================================================
%% SPDX-FileCopyrightText: 2016 - 2026 Leapsight
%% SPDX-License-Identifier: Apache-2.0
%% =============================================================================
%%
%% Regression: a durable (pack-store) instance with SEALED packs must survive a
%% restart. On restart the applier's cold-replay (`do_replay_cell_events/1`,
%% LastRoot = undefined ⇒ a full fold) rebuilds the cold projection by folding
%% the MST — and the MST's sealed packs are read through raw, process-bound fds
%% owned by the INSTANCE gen_server. The fold must therefore run in the instance
%% process (`bondy_oplog_instance:replay_pairs/2`), NOT the applier; otherwise
%% `prim_file:pread/3` on the instance's fd from the applier process fails with
%% `not_on_controlling_process` and the applier crash-loops on every restart of
%% a table large enough to have sealed a pack.
%% =============================================================================

-module(bondy_oplog_replay_sealed_pack_test).

-include_lib("eunit/include/eunit.hrl").

-define(B, <<>>).
-define(SEAL_EVERY, 30).
-define(BATCH, 120).

cold_replay_of_sealed_packs_runs_off_applier_test_() ->
    {setup, fun setup/0, fun cleanup/1, fun(Dir) ->
        {timeout, 60, fun() -> run(Dir) end}
    end}.

setup() ->
    {ok, _} = application:ensure_all_started(bondy_db),
    bondy_oplog_sync_scheduler:set_dispatch(undefined),
    bondy_oplog_gc_scheduler:set_trigger(undefined),
    Dir = filename:join(
        "/tmp",
        "creplay_" ++
            integer_to_list(erlang:unique_integer([positive, monotonic]))
    ),
    ok = filelib:ensure_dir(filename:join(Dir, "x")),
    Dir.

cleanup(Dir) ->
    [bondy_oplog:stop_instance(I) || I <- bondy_oplog:list_instances()],
    [
        bondy_oplog_core_registry:unregister(N, I, S)
     || E <- bondy_oplog_core_registry:list(),
        {N, I, S} <- [bondy_oplog_core_registry:entry_key(E)]
    ],
    _ = (catch del_tree(Dir)),
    ok.

run(Dir) ->
    InstId = mk_id(),
    NS = ns_of(InstId),

    %% Write enough to seal at least one pack: the MST now spans sealed packs on
    %% disk, read through raw fds owned by the instance gen_server.
    {C, P} = register_shard(NS),
    {ok, _} = open_pack_instance(InstId, NS, Dir),
    append_batch(InstId, 1, ?BATCH),
    _ = bondy_oplog_instance:await_apply(InstId),
    ?assert(length(sealed_packs(Dir)) >= 1),
    ?assertEqual(?BATCH, bondy_oplog:size(InstId)),

    InstP = bondy_oplog_registry:instance_pid(InstId),
    ApplierPid = bondy_oplog_registry:applier_pid(InstId),
    ?assert(is_pid(InstP)),
    ?assert(is_pid(ApplierPid)),

    %% The fix: the instance folds its own (sealed-pack) MST and returns the full
    %% set of pairs. This is the cold-replay fold the applier now DELEGATES here
    %% instead of running in its own process — reading the sealed packs from the
    %% fd-owning process, which is the whole point.
    {ok, {_Root, Pairs}} = bondy_oplog_instance:replay_pairs(InstP, undefined),
    ?assertEqual(?BATCH, length(Pairs)),

    %% Best-effort reproduction of the bug for documentation: folding the same
    %% MST from THIS (foreign) process reads a raw, instance-owned fd for any
    %% sealed page not in the page cache and crashes with
    %% `not_on_controlling_process` — the failure the fix avoids. Not asserted,
    %% because a warm page cache can serve every page from RAM.
    _ = (catch bondy_mst:to_list(bondy_oplog_registry:mst(InstId))),

    %% The applier's cold-replay path (which delegates to the instance) completes
    %% without crashing — the exact sequence that crash-looped on restart before.
    ?assertEqual(ok, bondy_oplog_applier:replay_cell_events_sync(ApplierPid)),
    ?assert(is_process_alive(ApplierPid)),

    ok = bondy_oplog:stop_instance(InstId),
    close_shard(C, P),
    ok = bondy_oplog_core_registry:unregister(NS, primary, 0),
    ok.

%% =============================================================================
%% Helpers (mirrored from bondy_oplog_compaction_durable_test)
%% =============================================================================

mk_id() ->
    list_to_binary(
        "creplay_" ++
            integer_to_list(erlang:unique_integer([positive, monotonic]))
    ).

ns_of(Id) when is_binary(Id) ->
    binary_to_atom(<<"ns_", Id/binary>>, utf8).

register_shard(NS) ->
    {ok, Cache} = bondy_oplog_cache_ets:init(NS, primary, 0, #{}),
    {ok, Proj} = bondy_oplog_projection_ets:open(NS, primary, 0, #{}),
    ok = bondy_oplog_core_registry:register(NS, primary, 0, #{
        shard_count => 1,
        cache_adapter => bondy_oplog_cache_ets,
        cache_handle => Cache,
        projection_adapter => bondy_oplog_projection_ets,
        projection_handle => Proj,
        fold_module => lww_register,
        overlay => disabled
    }),
    {Cache, Proj}.

close_shard(Cache, Proj) ->
    ok = bondy_oplog_projection_ets:close(Proj),
    ok = bondy_oplog_cache_ets:close(Cache).

open_pack_instance(InstanceId, NS, Dir) ->
    bondy_oplog:start_instance(InstanceId, #{
        origin => bondy_oplog_origin:new(),
        fold_module => lww_register,
        backend => bondy_mst_pack_store,
        storage_path => unicode:characters_to_binary(Dir),
        backend_options => #{auto_seal_records => ?SEAL_EVERY},
        seed => true,
        applier => #{cell_apply_target => {NS, primary, 0}}
    }).

append_batch(InstanceId, I, Batch) ->
    lists:foreach(
        fun(J) ->
            Key = key(I, J),
            Hlc = I * 1000 + J,
            _ = bondy_oplog:append(
                InstanceId, {cell_apply, ?B, Key, {set, Hlc, Key}}
            ),
            _ = bondy_oplog:projection(InstanceId)
        end,
        lists:seq(1, Batch)
    ).

key(I, J) ->
    <<"k_", (integer_to_binary(I))/binary, "_", (integer_to_binary(J))/binary>>.

sealed_packs(Dir) ->
    filelib:fold_files(
        Dir, "pack-.*\\.pack$", true, fun(F, Acc) -> [F | Acc] end, []
    ).

del_tree(Dir) ->
    case filelib:is_dir(Dir) of
        true ->
            {ok, Names} = file:list_dir(Dir),
            [del_tree(filename:join(Dir, N)) || N <- Names],
            file:del_dir(Dir);
        false ->
            file:delete(Dir)
    end.
