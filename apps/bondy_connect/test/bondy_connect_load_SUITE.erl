%% =============================================================================
%% SPDX-FileCopyrightText: 2016 - 2026 Leapsight
%% SPDX-License-Identifier: Apache-2.0
%% =============================================================================

-module(bondy_connect_load_SUITE).

-moduledoc "Pure unit tests for the in-flight cap of `bondy_connect_load`.".

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

-compile([nowarn_export_all, export_all]).


all() ->
    [
        unlimited_by_default,
        cap_admits_up_to_max,
        cap_rejects_over_max,
        release_frees_a_slot,
        release_floors_at_zero
    ].


unlimited_by_default(_) ->
    L0 = bondy_connect_load:new(#{}),
    L = lists:foldl(
        fun(_, Acc) ->
            {ok, A} = bondy_connect_load:admit(Acc),
            A
        end,
        L0,
        lists:seq(1, 1000)
    ),
    ?assertEqual(1000, bondy_connect_load:in_flight(L)).


cap_admits_up_to_max(_) ->
    L0 = bondy_connect_load:new(#{max_concurrency => 2}),
    {ok, L1} = bondy_connect_load:admit(L0),
    {ok, L2} = bondy_connect_load:admit(L1),
    ?assertEqual(2, bondy_connect_load:in_flight(L2)).


cap_rejects_over_max(_) ->
    L0 = bondy_connect_load:new(#{max_concurrency => 1}),
    {ok, L1} = bondy_connect_load:admit(L0),
    ?assertEqual({error, overloaded}, bondy_connect_load:admit(L1)).


release_frees_a_slot(_) ->
    L0 = bondy_connect_load:new(#{max_concurrency => 1}),
    {ok, L1} = bondy_connect_load:admit(L0),
    ?assertEqual({error, overloaded}, bondy_connect_load:admit(L1)),
    L2 = bondy_connect_load:release(L1),
    ?assertEqual(0, bondy_connect_load:in_flight(L2)),
    ?assertMatch({ok, _}, bondy_connect_load:admit(L2)).


release_floors_at_zero(_) ->
    L0 = bondy_connect_load:new(#{}),
    L1 = bondy_connect_load:release(L0),
    ?assertEqual(0, bondy_connect_load:in_flight(L1)).
