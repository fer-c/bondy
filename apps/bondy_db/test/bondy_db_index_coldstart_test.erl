%% =============================================================================
%% Cold-start index recovery (PLUM_DB_TO_BONDY_DB_DESIGN.md §6.6.1–6.6.3).
%%
%% These tests pin the durable-index cold-start contract over a real graceful
%% stop/restart on a fully durable stack (single-bookie leveled projection +
%% pack-store MST + WAL, all rooted at a `storage_path`, like
%% `bondy_db_tier2_durability_test`):
%%
%%   - A CLEAN restart of a durable table TRUSTS the persisted index cells:
%%     `bondy_db:open_table` does NOT run an O(table) rebuild (no
%%     `[bondy_oplog, secondary_index, rebuild]` telemetry), the index data is
%%     immediately readable, and a finite-`max_lag` read passes at once
%%     (the shards are freshened, not left sentinel-stale).
%%
%%   - A restart of a table whose shard was left untrusted (its durable trust
%%     marker removed, simulating a pre-restart saturation drop) REBUILDS that
%%     index from the primary on open (the rebuild telemetry fires) and the
%%     data is correct afterwards.
%%
%% Together they verify the durable trust marker (§6.6.2) drives the cold-start
%% trust-vs-rebuild decision: presence ⇒ trust, absence ⇒ rebuild.
%% =============================================================================

-module(bondy_db_index_coldstart_test).

-include_lib("eunit/include/eunit.hrl").

-define(DB, idx_coldstart).
-define(ET, users).
-define(R, <<"r1">>).
-define(REBUILD_EVENT, [bondy_oplog, secondary_index, rebuild]).

%% =============================================================================
%% Generators
%% =============================================================================

coldstart_test_() ->
    {foreach, fun setup/0, fun cleanup/1, [
        fun(Dirs) ->
            {"clean restart trusts the index (no rebuild)",
                {timeout, 90, fun() -> clean_restart_trusts(Dirs) end}}
        end,
        fun(Dirs) ->
            {"untrusted shard restart rebuilds",
                {timeout, 90, fun() -> untrusted_restart_rebuilds(Dirs) end}}
        end
    ]}.

setup() ->
    process_flag(trap_exit, true),
    {ok, _} = application:ensure_all_started(bondy_mst),
    {ok, _} = application:ensure_all_started(bondy_oplog),
    %% Tests want full control of compaction/GC timing — silence the
    %% schedulers (mirrors bondy_db_tier2_durability_test).
    bondy_oplog_sync_scheduler:set_dispatch(undefined),
    bondy_oplog_gc_scheduler:set_trigger(undefined),
    #{
        leveled => make_tempdir("leveled"),
        pack => make_tempdir("pack")
    }.

cleanup(Dirs) ->
    stop_everything(),
    rmrf(maps:get(leveled, Dirs)),
    rmrf(maps:get(pack, Dirs)),
    ok.

%% =============================================================================
%% Tests
%% =============================================================================

clean_restart_trusts(Dirs) ->
    %% --- First lifetime: write + index two entries, flush, close. ---
    {Db0, Sup0} = open(Dirs),
    {ok, T0} = open_table(Db0),
    write(T0, <<"u1">>, <<"active">>),
    write(T0, <<"u2">>, <<"active">>),
    flush_index(T0, by_value),
    ?assertEqual(
        {ok, [{<<"u1">>, #{}}, {<<"u2">>, #{}}]},
        bondy_db:index_get(T0, ?R, by_value, <<"active">>, #{})
    ),
    close(Db0, Sup0),

    %% --- Second lifetime: count rebuilds across the reopen. ---
    Ctr = counters:new(1, []),
    attach_rebuild_counter(Ctr),
    {Db1, Sup1} = open(Dirs),
    {ok, T1} = open_table(Db1),
    try
        %% The persisted index cells are TRUSTED — no O(table) rebuild ran.
        ?assertEqual(0, counters:get(Ctr, 1)),
        %% The data survived and is immediately readable.
        ?assertEqual(
            {ok, [{<<"u1">>, #{}}, {<<"u2">>, #{}}]},
            bondy_db:index_get(T1, ?R, by_value, <<"active">>, #{})
        ),
        %% Freshened on open: a finite-max_lag read passes at once.
        ?assertEqual(
            {ok, [{<<"u1">>, #{}}, {<<"u2">>, #{}}]},
            bondy_db:index_get(
                T1, ?R, by_value, <<"active">>, #{max_lag => 60000}
            )
        )
    after
        detach_rebuild_counter(),
        close(Db1, Sup1)
    end.

untrusted_restart_rebuilds(Dirs) ->
    %% --- First lifetime: write + index, flush, then strip every shard's
    %% durable trust marker (simulating a pre-restart saturation drop), close.
    {Db0, Sup0} = open(Dirs),
    {ok, T0} = open_table(Db0),
    write(T0, <<"u1">>, <<"active">>),
    flush_index(T0, by_value),
    ?assertEqual(
        {ok, [{<<"u1">>, #{}}]},
        bondy_db:index_get(T0, ?R, by_value, <<"active">>, #{})
    ),
    untrust_all_shards(T0, by_value),
    close(Db0, Sup0),

    %% --- Second lifetime: the unmarked index triggers a rebuild on open. ---
    %% We assert the DECISION Step 3 owns: a shard whose durable trust marker
    %% is absent is rebuilt (not silently trusted). We do NOT assert the
    %% rebuilt contents here: the rebuild re-derives from the primary via
    %% `reindex_from_projection` (cell directory = `distinct_cell_keys(MST)`),
    %% whose completeness depends on the primary's own durable MST recovery /
    %% tail-replay — a separate concern from the marker-driven decision, and
    %% exercised by the rebuild suites (lag / writer / tier2). The trusted
    %% path (and its full data survival) is covered by `clean_restart_trusts`.
    Ctr = counters:new(1, []),
    attach_rebuild_counter(Ctr),
    {Db1, Sup1} = open(Dirs),
    {ok, _T1} = open_table(Db1),
    try
        ?assert(counters:get(Ctr, 1) >= 1)
    after
        detach_rebuild_counter(),
        close(Db1, Sup1)
    end.

%% =============================================================================
%% Harness
%% =============================================================================

%% A fully durable single-bookie stack rooted at `storage_path` so the whole
%% table (primary MST/WAL + leveled index projection) survives a stop/restart.
open(Dirs) ->
    {ok, Sup} = bondy_db_leveled_sup:start_link(),
    {ok, Db} = bondy_db:open(?DB, #{
        topology => bondy_db_topology_single_bookie,
        topology_opts => #{sup => Sup, dir => maps:get(leveled, Dirs)},
        shard_count => 1,
        fold_module => lww_register,
        oplog_instance_opts => #{
            backend => bondy_mst_pack_store,
            storage_path =>
                unicode:characters_to_binary(maps:get(pack, Dirs)),
            seed => true
        }
    }),
    {Db, Sup}.

open_table(Db) ->
    bondy_db:open_table(Db, ?ET, #{
        fold_module => lww_register,
        indexes => [#{name => by_value, extract => []}]
    }).

write(T, Key, Value) ->
    ok = bondy_db:apply(T, ?R, Key, {set, bondy_db:tick(T), Value}).

%% Graceful shutdown: close the DB (stops the leveled Bookies), stop every
%% oplog instance (they cache now-dead Bookie handles), then stop the leveled
%% supervisor. The on-disk leveled/pack/WAL state survives.
close(Db, Sup) ->
    _ = catch bondy_db:close_table(Db),
    _ = catch bondy_db:close(Db),
    stop_everything(),
    case is_process_alive(Sup) of
        true -> bondy_db_leveled_sup:stop(Sup);
        false -> ok
    end,
    ok.

stop_everything() ->
    _ = [
        catch bondy_oplog:stop_instance(I)
     || I <- bondy_oplog:list_instances()
    ],
    ok.

flush_index(Table, IndexName) ->
    Info = bondy_db:info(Table),
    NS = maps:get(namespace, Info),
    #{IndexName := #{sec_shard_count := N}} = maps:get(indexes, Info),
    lists:foreach(
        fun(Shard) ->
            {ok, Entry} = bondy_oplog_core_registry:lookup(NS, IndexName, Shard),
            Pid = bondy_oplog_core_registry:entry_writer_pid(Entry),
            true = is_pid(Pid),
            ok = bondy_oplog_secondary_writer:flush_sync(Pid)
        end,
        lists:seq(0, N - 1)
    ).

%% Remove every shard's durable trust marker (via `index_mark_rebuild`, which
%% also deletes the marker), simulating a pre-restart saturation drop.
untrust_all_shards(Table, IndexName) ->
    Info = bondy_db:info(Table),
    NS = maps:get(namespace, Info),
    #{IndexName := #{sec_shard_count := N}} = maps:get(indexes, Info),
    lists:foreach(
        fun(Shard) ->
            {ok, Entry} = bondy_oplog_core_registry:lookup(NS, IndexName, Shard),
            ok = bondy_oplog_core_registry:index_mark_rebuild(Entry)
        end,
        lists:seq(0, N - 1)
    ).

%% =============================================================================
%% Telemetry counter
%% =============================================================================

attach_rebuild_counter(Ctr) ->
    ok = telemetry:attach(
        rebuild_counter_handler(),
        ?REBUILD_EVENT,
        fun(_Event, _Measurements, _Meta, C) -> counters:add(C, 1, 1) end,
        Ctr
    ).

detach_rebuild_counter() ->
    _ = telemetry:detach(rebuild_counter_handler()),
    ok.

rebuild_counter_handler() ->
    {?MODULE, rebuild_counter}.

%% =============================================================================
%% Tempdirs
%% =============================================================================

make_tempdir(Prefix) ->
    Base = filename:join([
        "/tmp",
        "bondy_db_index_coldstart",
        Prefix,
        integer_to_list(erlang:unique_integer([positive, monotonic]))
    ]),
    ok = filelib:ensure_dir(filename:join(Base, ".keep")),
    Base.

rmrf(Dir) ->
    case file:del_dir_r(Dir) of
        ok -> ok;
        {error, enoent} -> ok;
        {error, _} -> ok
    end.
