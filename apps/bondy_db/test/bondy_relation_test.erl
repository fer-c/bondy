%% =============================================================================
%% SPDX-FileCopyrightText: 2023 - 2026 Leapsight
%% SPDX-License-Identifier: Apache-2.0
%% =============================================================================

%% Keyset (cursor) pagination over `bondy_db` via `bondy_relation`, on the
%% `bondy_db_topology_shared_shards` topology — the production default for the
%% `core` DB, and the one that folds the realm into the cell key (G-1). The
%% relation's rows are therefore spread across all shards by
%% `phash2({Bucket, Key})`, so a correct page MUST scatter+merge across every
%% shard (`bondy_db:range_all/5`); a single-shard `range/5` would return a
%% partial page. These tests pin:
%%
%%   1. Multi-page asc/desc scans cover every row exactly once (no skip, no
%%      dup) across page boundaries, with `has_more`/`next` consistent.
%%   2. The decoder's `skip` (rejected rows interleaved with accepted ones —
%%      the user-table-with-aliases shape) is back-filled so every page holds
%%      `limit` accepted rows, and rejected rows never surface.
%%   3. `fold/4` streams every accepted row in key order, bounded memory.
%%   4. Cursor encode/decode round-trips and rejects stale/malformed cursors.
%% =============================================================================

-module(bondy_relation_test).

-include_lib("eunit/include/eunit.hrl").

-define(FOLD, bondy_oplog_crdt_lww_register).
-define(SHARDS, 4).
-define(DB, bondy_relation_test_db).
-define(R, <<"r1">>).

%% =============================================================================
%% Generators
%% =============================================================================

bondy_relation_test_() ->
    {foreach, fun setup/0, fun cleanup/1, [
        gen("asc_pages_cover_all_once", fun asc_pages_cover_all_once/1),
        gen("desc_pages_cover_all_once", fun desc_pages_cover_all_once/1),
        gen("rejected_rows_backfilled", fun rejected_rows_backfilled/1),
        gen("fold_streams_accepted_only", fun fold_streams_accepted_only/1),
        gen("lookup_hit_miss_rejected", fun lookup_hit_miss_rejected/1),
        gen("empty_relation", fun empty_relation/1),
        gen("cursor_wire_roundtrip", fun cursor_wire_roundtrip/1)
    ]}.

gen(Title, Fn) ->
    fun(Ctx) -> {Title, {timeout, 60, fun() -> Fn(Ctx) end}} end.

%% =============================================================================
%% Setup / teardown
%% =============================================================================

setup() ->
    process_flag(trap_exit, true),
    {ok, _} = application:ensure_all_started(bondy_db),
    bondy_oplog_sync_scheduler:set_dispatch(undefined),
    bondy_oplog_gc_scheduler:set_trigger(undefined),
    Dir = make_tempdir(),
    {ok, Sup} = bondy_db_leveled_sup:start_link(),
    {ok, Db} = bondy_db:open(?DB, #{
        topology => bondy_db_topology_shared_shards,
        topology_opts => #{sup => Sup, dir => Dir},
        shard_count => ?SHARDS,
        fold_module => ?FOLD
    }),
    {ok, T} = bondy_db:open_table(Db, users, #{fold_module => ?FOLD}),
    {Db, T, Sup, Dir}.

cleanup({Db, _T, Sup, Dir}) ->
    _ = catch bondy_db:close(Db),
    _ = [
        catch bondy_oplog:stop_instance(I)
     || I <- bondy_oplog:list_instances()
    ],
    case is_process_alive(Sup) of
        true -> bondy_db_leveled_sup:stop(Sup);
        false -> ok
    end,
    rmrf(Dir),
    rmrf(wal_dir_for_this_db()),
    ok.

%% =============================================================================
%% Tests
%% =============================================================================

%% 25 rows, page size 10 ⇒ 3 pages (10, 10, 5). The flattened pages MUST equal
%% the full sorted key set, each key once, in ascending order.
asc_pages_cover_all_once({_Db, T, _Sup, _Dir}) ->
    Keys = put_users(T, 25),
    Rel = relation(T),
    {Vals, Pages} = collect_all(Rel, ?R, 10, asc),
    ?assertEqual(Keys, [K || {K, _} <- Vals]),
    %% page shape: 10, 10, 5
    ?assertEqual([10, 10, 5], [length(P) || P <- Pages]).

desc_pages_cover_all_once({_Db, T, _Sup, _Dir}) ->
    Keys = put_users(T, 25),
    Rel = relation(T),
    {Vals, Pages} = collect_all(Rel, ?R, 10, desc),
    ?assertEqual(lists:reverse(Keys), [K || {K, _} <- Vals]),
    ?assertEqual([10, 10, 5], [length(P) || P <- Pages]).

%% Interleave 20 user rows with 20 alias rows the decoder rejects. Every page
%% must still hold `limit` USERS (back-fill across the rejected rows), the
%% aliases must never appear, and the scan must cover all users once.
rejected_rows_backfilled({_Db, T, _Sup, _Dir}) ->
    UserKeys = put_users(T, 20),
    _AliasKeys = put_aliases(T, 20),
    Rel = relation(T),
    {Vals, Pages} = collect_all(Rel, ?R, 7, asc),
    GotKeys = [K || {K, _} <- Vals],
    ?assertEqual(UserKeys, GotKeys),
    %% no alias leaked through
    ?assertEqual([], [
        K
     || {K, V} <- Vals, maps:get(type, V, undefined) =:= alias
    ]),
    %% full pages are exactly `limit` users despite interleaved rejects
    ?assertEqual([7, 7, 6], [length(P) || P <- Pages]).

fold_streams_accepted_only({_Db, T, _Sup, _Dir}) ->
    UserKeys = put_users(T, 30),
    _ = put_aliases(T, 30),
    Rel = relation(T),
    {ok, Acc} = bondy_relation:fold(
        Rel, ?R, fun({K, _V}, A) -> [K | A] end, []
    ),
    ?assertEqual(UserKeys, lists:reverse(Acc)).

lookup_hit_miss_rejected({_Db, T, _Sup, _Dir}) ->
    _ = put_users(T, 3),
    _ = put_aliases(T, 1),
    Rel = relation(T),
    ?assertMatch(
        {ok, {<<"u00001">>, _}}, bondy_relation:lookup(Rel, ?R, <<"u00001">>)
    ),
    ?assertEqual(
        {error, not_found}, bondy_relation:lookup(Rel, ?R, <<"missing">>)
    ),
    %% an alias cell exists but the decoder rejects it ⇒ not_found
    ?assertEqual(
        {error, not_found}, bondy_relation:lookup(Rel, ?R, <<"a00001">>)
    ).

empty_relation({_Db, T, _Sup, _Dir}) ->
    Rel = relation(T),
    ?assertEqual(
        {ok, #{values => [], next => undefined, has_more => false}},
        bondy_relation:list(Rel, ?R, #{limit => 10})
    ),
    ?assertEqual(
        {ok, []}, bondy_relation:fold(Rel, ?R, fun(X, A) -> [X | A] end, [])
    ).

cursor_wire_roundtrip({_Db, T, _Sup, _Dir}) ->
    _ = put_users(T, 15),
    Rel = relation(T),
    {ok, #{next := Cursor}} = bondy_relation:list(Rel, ?R, #{limit => 5}),
    ?assertNotEqual(undefined, Cursor),
    Wire = bondy_relation:encode_cursor(Cursor),
    ?assert(is_binary(Wire)),
    ?assertEqual({ok, Cursor}, bondy_relation:decode_cursor(Rel, Wire)),
    %% resuming from the decoded wire cursor yields the next page
    {ok, Decoded} = bondy_relation:decode_cursor(Rel, Wire),
    {ok, #{values := Vs}} =
        bondy_relation:list(Rel, ?R, #{limit => 5, cursor => Decoded}),
    ?assertEqual(
        [<<"u00006">>, <<"u00007">>, <<"u00008">>, <<"u00009">>, <<"u00010">>],
        [K || {K, _} <- Vs]
    ),
    %% malformed ⇒ malformed
    ?assertEqual(
        {error, malformed},
        bondy_relation:decode_cursor(Rel, <<"!!not-base64!!">>)
    ),
    %% a cursor minted for a different schema ⇒ stale
    Other = bondy_relation:new(users, #{
        table => T, decode => fun decode_row/1, schema => some_other_schema
    }),
    ?assertEqual({error, stale}, bondy_relation:decode_cursor(Other, Wire)).

%% =============================================================================
%% Fixture
%% =============================================================================

relation(T) ->
    bondy_relation:new(users, #{table => T, decode => fun decode_row/1}).

%% Accept user rows, reject alias rows.
decode_row({_Key, #{type := alias}, _Hlc}) ->
    skip;
decode_row({Key, Value, _Hlc}) when is_map(Value) ->
    {ok, {Key, Value}};
decode_row(_) ->
    skip.

%% Zero-padded keys so lexical byte order == numeric order.
put_users(T, N) ->
    Keys = [ukey(I) || I <- lists:seq(1, N)],
    [ok = put(T, K, #{type => user, n => K}) || K <- Keys],
    Keys.

put_aliases(T, N) ->
    Keys = [akey(I) || I <- lists:seq(1, N)],
    [ok = put(T, K, #{type => alias, points_to => K}) || K <- Keys],
    Keys.

put(T, K, V) ->
    bondy_db:apply(T, ?R, K, {set, bondy_db:tick(T), V}).

ukey(I) ->
    iolist_to_binary(io_lib:format("u~5..0b", [I])).

akey(I) ->
    iolist_to_binary(io_lib:format("a~5..0b", [I])).

%% Page through the whole relation, asserting `has_more`/`next` consistency,
%% and return {FlattenedValues, [PageValues]}.
collect_all(Rel, Realm, Limit, Dir) ->
    collect_all(Rel, Realm, Limit, Dir, undefined, [], []).

collect_all(Rel, Realm, Limit, Dir, Cursor, ValsAcc, PagesAcc) ->
    Opts0 = #{limit => Limit, direction => Dir},
    Opts =
        case Cursor of
            undefined -> Opts0;
            _ -> Opts0#{cursor => Cursor}
        end,
    {ok, #{values := Vs, next := Next, has_more := More}} =
        bondy_relation:list(Rel, Realm, Opts),
    ValsAcc1 = ValsAcc ++ Vs,
    PagesAcc1 = PagesAcc ++ [Vs],
    case More of
        true ->
            ?assertNotEqual(undefined, Next),
            collect_all(Rel, Realm, Limit, Dir, Next, ValsAcc1, PagesAcc1);
        false ->
            ?assertEqual(undefined, Next),
            {ValsAcc1, PagesAcc1}
    end.

%% =============================================================================
%% Misc helpers
%% =============================================================================

make_tempdir() ->
    Base = filename:join([
        "/tmp",
        "bondy_relation_test",
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
