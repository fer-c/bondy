%% =============================================================================
%% SPDX-FileCopyrightText: 2016 - 2026 Leapsight
%% SPDX-License-Identifier: Apache-2.0
%% =============================================================================

-module(bondy_export_SUITE).
-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").
-compile([nowarn_export_all, export_all]).

-define(REALM, <<"com.example.export_test">>).

all() ->
    [export_import_roundtrip].

init_per_suite(Config) ->
    bondy_ct:start_bondy(),
    %% A real realm so per-realm table enumeration includes it. Durable core
    %% DB ⇒ may survive a prior run.
    _ =
        case bondy_realm:exists(?REALM) of
            true -> ok;
            false -> bondy_realm:create(?REALM)
        end,
    Config.

end_per_suite(Config) ->
    {save_config, Config}.

%% Round-trips a per-realm entry (security_users under the test realm's URI
%% band) and a global-band entry (bondy_bridge_relay under the <<>> band)
%% through export -> delete -> import, exercising both enumeration paths and
%% the new bondy_db export file format.
export_import_roundtrip(Config) ->
    Priv = ?config(priv_dir, Config),
    UsersTab = bondy_namespace_catalog:table(security_users),
    BridgeTab = bondy_namespace_catalog:table(bondy_bridge_relay),
    ?assertNotEqual(undefined, UsersTab),
    ?assertNotEqual(undefined, BridgeTab),

    UKey = <<"alice">>,
    UVal = #{username => UKey, marker => <<"export_test">>},
    BKey = <<"export_test_bridge">>,
    BVal = #{name => BKey, marker => <<"export_test">>},

    %% Seed: a per-realm entry and a global-band (<<>>) entry.
    ok = bondy_db:apply(UsersTab, ?REALM, UKey, {set, UVal}),
    ok = bondy_db:apply(BridgeTab, <<>>, BKey, {set, BVal}),
    ?assertMatch({ok, {UVal, _}}, bondy_db:read(UsersTab, ?REALM, UKey)),
    ?assertMatch({ok, {BVal, _}}, bondy_db:read(BridgeTab, <<>>, BKey)),

    %% Export the whole database.
    {ok, #{filename := File}} = bondy_export:export(#{path => Priv}),
    ok = wait_idle(100),

    %% The file carries the new bondy_db export header.
    {ok, Head} = bondy_export:status(#{filename => File}),
    ?assertMatch(
        #{format := bondy_db_export, vsn := <<"2.0.0">>, status := ok},
        Head
    ),

    %% Delete both entries.
    ok = bondy_db:apply(UsersTab, ?REALM, UKey, clear),
    ok = bondy_db:apply(BridgeTab, <<>>, BKey, clear),
    ?assertEqual({error, not_found}, bondy_db:read(UsersTab, ?REALM, UKey)),
    ?assertEqual({error, not_found}, bondy_db:read(BridgeTab, <<>>, BKey)),

    %% Import the export file.
    {ok, _} = bondy_export:import(#{filename => File}),
    ok = wait_idle(100),

    %% Both entries are restored byte-for-byte.
    ?assertMatch({ok, {UVal, _}}, bondy_db:read(UsersTab, ?REALM, UKey)),
    ?assertMatch({ok, {BVal, _}}, bondy_db:read(BridgeTab, <<>>, BKey)),
    ok.

%% @private
%% Polls the (async) export/import worker until it returns to idle.
wait_idle(0) ->
    {error, timeout};
wait_idle(N) ->
    case bondy_export:status(#{}) of
        {ok, undefined} ->
            ok;
        {ok, _InProgress} ->
            timer:sleep(100),
            wait_idle(N - 1)
    end.
