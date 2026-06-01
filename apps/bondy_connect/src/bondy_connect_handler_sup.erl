%% =============================================================================
%% SPDX-FileCopyrightText: 2016 - 2026 Leapsight
%% SPDX-License-Identifier: Apache-2.0
%% =============================================================================

-module(bondy_connect_handler_sup).

-moduledoc """
Per-connection supervisor for isolated handler workers (callee invocations and
subscriber events).

For the M1 walking skeleton (caller-only) it starts **childless**; in Phase 4
it becomes a `simple_one_for_one` supervisor of `bondy_connect_handler`
workers, started/monitored by the connection on each INVOCATION/EVENT.
""".

-behaviour(supervisor).

-export([start_link/0]).
-export([init/1]).



-spec start_link() -> supervisor:startlink_ret().
start_link() ->
    supervisor:start_link(?MODULE, []).


-spec init([]) ->
    {ok, {supervisor:sup_flags(), [supervisor:child_spec()]}}.
init([]) ->
    SupFlags = #{
        strategy => one_for_one,
        intensity => 10,
        period => 10
    },
    {ok, {SupFlags, []}}.
