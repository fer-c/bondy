%% =============================================================================
%% SPDX-FileCopyrightText: 2016 - 2026 Leapsight
%% SPDX-License-Identifier: Apache-2.0
%% =============================================================================
%% Tests for the projection content digest (`bondy_oplog_content_digest`), the
%% MST-root-independent convergence oracle core (ISSUES.md AR-17). The load-
%% bearing properties are:
%%   - order-independence: the digest depends only on the SET of live
%%     `(Bucket, Key) -> StateBytes` cells, not the order they were applied —
%%     so two nodes that converge to the same content agree regardless of apply
%%     / merge order;
%%   - add/remove are XOR-inverses, so the digest is incrementally maintainable
%%     (`replace/5`) without a rescan;
%%   - distinct content yields a distinct digest (incl. unambiguous field
%%     boundaries via length-prefixing).
-module(bondy_oplog_content_digest_test).

-include_lib("eunit/include/eunit.hrl").

-define(D, bondy_oplog_content_digest).

%% --- shape ----------------------------------------------------------------

empty_is_zero_test() ->
    ?assertEqual(0, ?D:empty()),
    ?assert(?D:is_empty(?D:empty())).

cell_hash_is_deterministic_64bit_test() ->
    H1 = ?D:cell_hash(<<"b">>, <<"k">>, <<"state">>),
    H2 = ?D:cell_hash(<<"b">>, <<"k">>, <<"state">>),
    ?assertEqual(H1, H2),
    ?assert(is_integer(H1)),
    ?assert(H1 >= 0 andalso H1 =< (1 bsl 64) - 1).

to_hex_is_16_char_hex_test() ->
    ?assertEqual(<<"0000000000000000">>, ?D:to_hex(?D:empty())),
    H = ?D:to_hex(?D:cell_hash(<<"b">>, <<"k">>, <<"s">>)),
    ?assertEqual(16, byte_size(H)).

%% --- inverse / replace ----------------------------------------------------

add_then_remove_is_identity_test() ->
    D0 = ?D:empty(),
    D1 = ?D:add(D0, <<"b">>, <<"k">>, <<"s">>),
    ?assertNotEqual(D0, D1),
    ?assert(not ?D:is_empty(D1)),
    D2 = ?D:remove(D1, <<"b">>, <<"k">>, <<"s">>),
    ?assertEqual(D0, D2),
    ?assert(?D:is_empty(D2)).

replace_new_key_equals_add_test() ->
    D0 = ?D:add(?D:empty(), <<"b">>, <<"x">>, <<"sx">>),
    Via_add = ?D:add(D0, <<"b">>, <<"k">>, <<"s1">>),
    Via_replace = ?D:replace(D0, <<"b">>, <<"k">>, undefined, <<"s1">>),
    ?assertEqual(Via_add, Via_replace).

replace_delete_equals_remove_test() ->
    D0 = ?D:add(?D:empty(), <<"b">>, <<"k">>, <<"s1">>),
    Via_remove = ?D:remove(D0, <<"b">>, <<"k">>, <<"s1">>),
    Via_replace = ?D:replace(D0, <<"b">>, <<"k">>, <<"s1">>, undefined),
    ?assertEqual(Via_remove, Via_replace),
    ?assert(?D:is_empty(Via_replace)).

replace_update_equals_remove_then_add_test() ->
    D0 = ?D:add(?D:empty(), <<"b">>, <<"k">>, <<"old">>),
    Manual = ?D:add(?D:remove(D0, <<"b">>, <<"k">>, <<"old">>), <<"b">>, <<"k">>, <<"new">>),
    Via_replace = ?D:replace(D0, <<"b">>, <<"k">>, <<"old">>, <<"new">>),
    ?assertEqual(Manual, Via_replace).

replace_noop_when_both_undefined_test() ->
    D0 = ?D:add(?D:empty(), <<"b">>, <<"k">>, <<"s">>),
    ?assertEqual(D0, ?D:replace(D0, <<"b">>, <<"k2">>, undefined, undefined)).

%% --- order-independence (the convergence property) ------------------------

%% Same set of cells applied in any order yields the same digest.
order_independence_test() ->
    Cells = [
        {<<"b1">>, <<"k1">>, <<"s1">>},
        {<<"b1">>, <<"k2">>, <<"s2">>},
        {<<"b2">>, <<"k1">>, <<"s3">>},
        {<<"b2">>, <<"k9">>, <<"s4">>},
        {<<"b3">>, <<"k5">>, <<"s5">>}
    ],
    Forward = build(Cells),
    Reverse = build(lists:reverse(Cells)),
    Shuffled = build(rotate(Cells, 3)),
    ?assertEqual(Forward, Reverse),
    ?assertEqual(Forward, Shuffled).

%% Two "nodes" reaching the same content via different update paths (one updates
%% a cell in place, the other re-derives it) converge to the same digest —
%% mirrors a converged shard regardless of merge history.
convergence_via_different_paths_test() ->
    %% Node A: write k=v1, then update k=v2, then add j=w.
    A0 = ?D:replace(?D:empty(), <<"b">>, <<"k">>, undefined, <<"v1">>),
    A1 = ?D:replace(A0, <<"b">>, <<"k">>, <<"v1">>, <<"v2">>),
    A2 = ?D:replace(A1, <<"b">>, <<"j">>, undefined, <<"w">>),
    %% Node B: add j=w first, then write k straight to v2.
    B0 = ?D:replace(?D:empty(), <<"b">>, <<"j">>, undefined, <<"w">>),
    B1 = ?D:replace(B0, <<"b">>, <<"k">>, undefined, <<"v2">>),
    ?assertEqual(A2, B1).

%% combine folds disjoint partial digests (e.g. per-shard-range) into the whole.
combine_is_xor_with_empty_identity_test() ->
    Da = build([{<<"b">>, <<"k1">>, <<"s1">>}]),
    Db = build([{<<"b">>, <<"k2">>, <<"s2">>}]),
    Whole = build([{<<"b">>, <<"k1">>, <<"s1">>}, {<<"b">>, <<"k2">>, <<"s2">>}]),
    ?assertEqual(Whole, ?D:combine(Da, Db)),
    ?assertEqual(Da, ?D:combine(Da, ?D:empty())),
    ?assertEqual(Db, ?D:combine(?D:empty(), Db)).

%% --- divergence detection -------------------------------------------------

different_state_diverges_test() ->
    Same = build([{<<"b">>, <<"k">>, <<"s1">>}]),
    Diff = build([{<<"b">>, <<"k">>, <<"s2">>}]),
    ?assertNotEqual(Same, Diff).

different_keyset_diverges_test() ->
    A = build([{<<"b">>, <<"k1">>, <<"s">>}]),
    B = build([{<<"b">>, <<"k1">>, <<"s">>}, {<<"b">>, <<"k2">>, <<"s">>}]),
    ?assertNotEqual(A, B).

%% Length-prefixing keeps field boundaries unambiguous: shifting bytes between
%% bucket and key (same concatenation) must NOT collide.
length_prefix_prevents_boundary_collision_test() ->
    H1 = ?D:cell_hash(<<"a">>, <<"bc">>, <<"s">>),
    H2 = ?D:cell_hash(<<"ab">>, <<"c">>, <<"s">>),
    ?assertNotEqual(H1, H2).

%% The dangerous case AR-17 names: two shards both compacted to an empty MST
%% (both roots `undefined` → would read IN SYNC) but holding DIFFERENT data are
%% caught by the content digest.
both_empty_mst_but_divergent_data_detected_test() ->
    %% Roots are irrelevant here; the digest is over projection content only.
    NodeA = build([{<<"users">>, <<"alice">>, <<"v1">>}]),
    NodeB = build([{<<"users">>, <<"alice">>, <<"v2">>}]),
    ?assertNotEqual(NodeA, NodeB),
    %% ...and identical data converges.
    NodeA2 = build([{<<"users">>, <<"alice">>, <<"v1">>}]),
    ?assertEqual(NodeA, NodeA2).

%% =============================================================================
%% Helpers
%% =============================================================================

build(Cells) ->
    lists:foldl(
        fun({B, K, S}, Acc) -> ?D:add(Acc, B, K, S) end,
        ?D:empty(),
        Cells
    ).

rotate(L, 0) -> L;
rotate([H | T], N) -> rotate(T ++ [H], N - 1).
