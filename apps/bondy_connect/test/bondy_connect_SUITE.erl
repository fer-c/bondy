%% =============================================================================
%% SPDX-FileCopyrightText: 2016 - 2026 Leapsight
%% SPDX-License-Identifier: Apache-2.0
%% =============================================================================

-module(bondy_connect_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

-compile([nowarn_export_all, export_all]).


all() ->
    [
        app_starts_and_stops,
        sup_is_one_for_one
    ].


%% The skeleton must boot (with its library deps) and shut down cleanly,
%% without any dependency on the bondy router.
app_starts_and_stops(_) ->
    {ok, Started} = application:ensure_all_started(bondy_connect),
    ?assert(lists:member(bondy_connect, Started)),
    ?assert(is_pid(whereis(bondy_connect_sup))),

    %% No dependency on the router app should have been pulled in.
    ?assertNot(lists:member(bondy, Started)),

    ok = application:stop(bondy_connect),
    ?assertEqual(undefined, whereis(bondy_connect_sup)).


%% The top supervisor starts childless with a one_for_one strategy.
sup_is_one_for_one(_) ->
    {ok, _} = application:ensure_all_started(bondy_connect),
    Pid = whereis(bondy_connect_sup),
    ?assert(is_pid(Pid)),

    {ok, {SupFlags, ChildSpecs}} = bondy_connect_sup:init([]),
    ?assertEqual(one_for_one, maps:get(strategy, SupFlags)),
    ?assertEqual([], ChildSpecs),
    ?assertEqual([], supervisor:which_children(Pid)),

    ok = application:stop(bondy_connect).
