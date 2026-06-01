%% =============================================================================
%% SPDX-FileCopyrightText: 2016 - 2026 Leapsight
%% SPDX-License-Identifier: Apache-2.0
%% =============================================================================

-module(bondy_connect).

-moduledoc """
Public API for the `bondy_connect` WAMP client.

```erlang
{ok, Conn} = bondy_connect:connect(#{
    transport => tcp,
    endpoint  => {"127.0.0.1", 18082},
    realm     => <<"com.example.realm">>,
    auth      => #{method => <<"anonymous">>}
}),
{ok, Result} = bondy_connect:call(Conn, <<"bondy.session.self">>, []),
ok = bondy_connect:disconnect(Conn).
```

`connect/1,2` blocks until the WAMP session is established. A connection handle
(`conn()`) is the connection pid; a named connection (`connect/2`) can also be
referenced by its name.

M1 (walking skeleton) implements the **caller** role over the raw TCP
transport. `register`/`subscribe`/`publish` arrive in Phase 4.
""".

-type conn()    ::  pid() | atom().

-export_type([conn/0]).

-define(CONNECT_TIMEOUT, 30000).

-export([connect/1]).
-export([connect/2]).
-export([disconnect/1]).
-export([status/1]).
-export([call/2]).
-export([call/3]).
-export([call/4]).
-export([call/5]).



%% =============================================================================
%% API
%% =============================================================================



-doc "Open an (unnamed) connection and wait for the session to establish.".
-spec connect(Spec :: map()) -> {ok, conn()} | {error, term()}.
connect(Spec) ->
    connect(undefined, Spec).


-doc "Open a named connection and wait for the session to establish.".
-spec connect(Name :: atom() | undefined, Spec :: map()) ->
    {ok, conn()} | {error, term()}.
connect(Name, Spec) ->
    case bondy_connect_manager:connect(Name, Spec) of
        {ok, Pid} ->
            case bondy_connect_connection:await_ready(Pid, ?CONNECT_TIMEOUT) of
                ok ->
                    {ok, Pid};
                {error, Reason} ->
                    _ = bondy_connect_manager:disconnect(Pid),
                    {error, Reason}
            end;
        {error, _} = Error ->
            Error
    end.


-doc "Close a connection.".
-spec disconnect(conn()) -> ok.
disconnect(Conn) ->
    bondy_connect_manager:disconnect(Conn).


-doc "The connection's status.".
-spec status(conn()) -> connecting | establishing | established | down.
status(Conn) ->
    case resolve(Conn) of
        undefined -> down;
        Pid -> bondy_connect_connection:status(Pid)
    end.


-doc "Call a procedure with no arguments.".
-spec call(conn(), binary()) -> {ok, map()} | {error, term()}.
call(Conn, Uri) ->
    call(Conn, Uri, [], #{}, #{}).


-doc "Call a procedure with positional arguments.".
-spec call(conn(), binary(), Args :: list()) -> {ok, map()} | {error, term()}.
call(Conn, Uri, Args) ->
    call(Conn, Uri, Args, #{}, #{}).


-doc "Call a procedure with positional + keyword arguments.".
-spec call(conn(), binary(), Args :: list(), KWArgs :: map()) ->
    {ok, map()} | {error, term()}.
call(Conn, Uri, Args, KWArgs) ->
    call(Conn, Uri, Args, KWArgs, #{}).


-doc """
Call a procedure. `Opts` may carry `timeout` (ms). Returns
`{ok, #{args := list(), kwargs := map()}}` or
`{error, #{uri := binary(), ...}}` / `{error, Reason}`.
""".
-spec call(conn(), binary(), Args :: list(), KWArgs :: map(), Opts :: map()) ->
    {ok, map()} | {error, term()}.
call(Conn, Uri, Args, KWArgs, Opts) ->
    case resolve(Conn) of
        undefined ->
            {error, not_connected};
        Pid ->
            bondy_connect_connection:call(Pid, Uri, Args, KWArgs, Opts)
    end.



%% =============================================================================
%% PRIVATE
%% =============================================================================



%% @private
resolve(Conn) when is_pid(Conn) ->
    Conn;
resolve(Name) when is_atom(Name) ->
    bondy_connect_manager:whereis_name(Name).
