%% =============================================================================
%% SPDX-FileCopyrightText: 2023 - 2026 Leapsight
%% =============================================================================
%% Integration coverage for the per-instance projection content digest
%% (ISSUES.md AR-17) maintained incrementally on the REAL apply path
%% (`bondy_oplog_cell_apply:apply_cell_batch/3`). Where the pure-module test
%% (`bondy_oplog_content_digest_test`) exercises the XOR maths, this drives
%% actual `bondy_oplog:append/2` writes through the applier and reads back the
%% live digest from the per-instance registry counter, asserting:
%%   - an empty instance's digest is 0;
%%   - applying cells moves it off 0;
%%   - the SAME cells applied in DIFFERENT orders converge to the SAME digest
%%     (the order-independence the oracle relies on);
%%   - genuinely different data yields different digests;
%%   - a divergent instance re-converges (equal digest) once it applies the
%%     matching update.
-module(bondy_oplog_content_digest_integration_test).

-include_lib("eunit/include/eunit.hrl").

-define(B, <<>>).

content_digest_integration_test_() ->
    {setup, fun setup/0, fun cleanup/1, fun(Dir) ->
        [
            {timeout, 60, fun() -> empty_instance_is_zero(Dir) end},
            {timeout, 60, fun() -> apply_moves_digest_off_zero(Dir) end},
            {timeout, 60, fun() -> order_independent_across_instances(Dir) end},
            {timeout, 60, fun() -> divergent_data_differs(Dir) end},
            {timeout, 60, fun() -> reconverges_after_matching_update(Dir) end},
            {timeout, 60, fun() -> digest_request_over_transport(Dir) end}
        ]
    end}.

setup() ->
    {ok, _} = application:ensure_all_started(bondy_db),
    bondy_oplog_sync_scheduler:set_dispatch(undefined),
    bondy_oplog_gc_scheduler:set_trigger(undefined),
    Dir = filename:join(
        "/tmp",
        "cdig_" ++
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

%% An instance with no cells applied has the empty digest.
empty_instance_is_zero(Dir) ->
    with_instance(Dir, fun(InstId) ->
        ?assertEqual(0, digest(InstId))
    end).

%% Applying cells moves the digest off zero (and a second batch moves it again).
apply_moves_digest_off_zero(Dir) ->
    with_instance(Dir, fun(InstId) ->
        put_cell(InstId, <<"k1">>, <<"v1">>, 1),
        sync(InstId),
        D1 = digest(InstId),
        ?assertNotEqual(0, D1),
        put_cell(InstId, <<"k2">>, <<"v2">>, 2),
        sync(InstId),
        D2 = digest(InstId),
        ?assertNotEqual(D1, D2),
        ?assertNotEqual(0, D2)
    end).

%% The same set of cells applied in opposite orders converges to the same
%% digest — order-independence through the real apply path.
order_independent_across_instances(Dir) ->
    Cells = [
        {<<"a">>, <<"1">>, 10},
        {<<"b">>, <<"2">>, 20},
        {<<"c">>, <<"3">>, 30},
        {<<"d">>, <<"4">>, 40}
    ],
    with_two_instances(Dir, fun(A, B) ->
        [put_cell(A, K, V, H) || {K, V, H} <- Cells],
        [put_cell(B, K, V, H) || {K, V, H} <- lists:reverse(Cells)],
        sync(A),
        sync(B),
        ?assertNotEqual(0, digest(A)),
        ?assertEqual(digest(A), digest(B))
    end).

%% Two instances holding different data for the same key diverge.
divergent_data_differs(Dir) ->
    with_two_instances(Dir, fun(A, B) ->
        put_cell(A, <<"k">>, <<"alice">>, 1),
        put_cell(B, <<"k">>, <<"bob">>, 1),
        sync(A),
        sync(B),
        ?assertNotEqual(digest(A), digest(B))
    end).

%% A divergent instance re-converges (equal digest) once it applies the same
%% winning update — the digest tracks the materialized state, not the path.
reconverges_after_matching_update(Dir) ->
    with_two_instances(Dir, fun(A, B) ->
        put_cell(A, <<"k">>, <<"v1">>, 1),
        put_cell(B, <<"k">>, <<"v1">>, 1),
        sync(A),
        sync(B),
        ?assertEqual(digest(A), digest(B)),
        %% B advances k to v2 (later HLC wins under lww) → diverges.
        put_cell(B, <<"k">>, <<"v2">>, 2),
        sync(B),
        ?assertNotEqual(digest(A), digest(B)),
        %% A applies the same winning update → re-converges.
        put_cell(A, <<"k">>, <<"v2">>, 2),
        sync(A),
        ?assertEqual(digest(A), digest(B))
    end).

%% The `get_content_digest` sync request (Stage 4) returns the same digest the
%% local read sees — over the inline transport (`{ok, {Status, Digest}}`) and via
%% the responder dispatch (`{ok, {Status, Digest}, Fingerprint}`).
digest_request_over_transport(Dir) ->
    with_instance(Dir, fun(InstId) ->
        put_cell(InstId, <<"k">>, <<"v">>, 1),
        sync(InstId),
        D = digest(InstId),
        ?assertNotEqual(0, D),
        %% Inline transport (peer_id is the local instance id), 2-tuple reply.
        ?assertEqual(
            {ok, {ready, D}},
            bondy_oplog_transport_inline:request(
                InstId, InstId, get_content_digest, #{}
            )
        ),
        %% Responder dispatch, 3-tuple reply (digest + topology fingerprint).
        ?assertMatch(
            {ok, {ready, D}, _Fingerprint},
            bondy_oplog_responder:dispatch(InstId, get_content_digest)
        )
    end).

%% =============================================================================
%% Helpers
%% =============================================================================

digest(InstId) ->
    Ref = bondy_oplog_registry:content_digest_ref(InstId),
    ?assert(Ref =/= undefined),
    bondy_oplog_content_digest:read_ref(Ref).

put_cell(InstId, Key, Val, Hlc) ->
    _ = bondy_oplog:append(InstId, {cell_apply, ?B, Key, {set, Hlc, Val}}),
    ok.

sync(InstId) ->
    _ = bondy_oplog:projection(InstId),
    _ = bondy_oplog_instance:await_apply(InstId),
    ok.

with_instance(Dir, Fun) ->
    InstId = mk_id(),
    NS = ns_of(InstId),
    {Cache, Proj} = register_shard(NS, primary, 0),
    {ok, _} = bondy_oplog:start_instance(InstId, start_opts(NS, Dir)),
    try
        Fun(InstId)
    after
        ok = bondy_oplog:stop_instance(InstId),
        ok = bondy_oplog_core_registry:unregister(NS, primary, 0),
        close_shard(Cache, Proj)
    end.

with_two_instances(Dir, Fun) ->
    A = mk_id(),
    B = mk_id(),
    NSA = ns_of(A),
    NSB = ns_of(B),
    {CA, PA} = register_shard(NSA, primary, 0),
    {CB, PB} = register_shard(NSB, primary, 0),
    {ok, _} = bondy_oplog:start_instance(A, start_opts(NSA, Dir)),
    {ok, _} = bondy_oplog:start_instance(B, start_opts(NSB, Dir)),
    try
        Fun(A, B)
    after
        ok = bondy_oplog:stop_instance(A),
        ok = bondy_oplog:stop_instance(B),
        ok = bondy_oplog_core_registry:unregister(NSA, primary, 0),
        ok = bondy_oplog_core_registry:unregister(NSB, primary, 0),
        close_shard(CA, PA),
        close_shard(CB, PB)
    end.

start_opts(NS, Dir) ->
    #{
        origin => bondy_oplog_origin:new(),
        fold_module => lww_register,
        backend => bondy_mst_pack_store,
        storage_path => unicode:characters_to_binary(Dir),
        seed => true,
        applier => #{cell_apply_target => {NS, primary, 0}}
    }.

mk_id() ->
    list_to_binary(
        "cdig_" ++ integer_to_list(erlang:unique_integer([positive, monotonic]))
    ).

ns_of(Id) when is_binary(Id) ->
    binary_to_atom(<<"ns_", Id/binary>>, utf8).

register_shard(NS, Index, Shard) ->
    {ok, Cache} = bondy_oplog_cache_ets:init(NS, Index, Shard, #{}),
    {ok, Proj} = bondy_oplog_projection_ets:open(NS, Index, Shard, #{}),
    ok = bondy_oplog_core_registry:register(NS, Index, Shard, #{
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
    ok = bondy_oplog_cache_ets:close(Cache),
    ok.

del_tree(Dir) ->
    case filelib:is_dir(Dir) of
        true ->
            {ok, Names} = file:list_dir(Dir),
            [del_tree(filename:join(Dir, N)) || N <- Names],
            file:del_dir(Dir);
        false ->
            file:delete(Dir)
    end.
