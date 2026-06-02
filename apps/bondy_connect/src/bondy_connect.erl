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

All four client roles — **caller**, **callee**, **publisher**, **subscriber** —
are supported over the raw TCP transport (M2). Handlers (`handler()`) run in
isolated, load-regulated worker processes; a crashing handler never affects the
connection.
""".

-type conn()    ::  pid() | atom().
-type handler() ::  bondy_connect_handler_spec:handler().

-export_type([conn/0]).
-export_type([handler/0]).

-define(CONNECT_TIMEOUT, 30000).

-export([connect/1]).
-export([connect/2]).
-export([disconnect/1]).
-export([status/1]).
-export([call/2]).
-export([call/3]).
-export([call/4]).
-export([call/5]).
-export([call_async/3]).
-export([call_async/4]).
-export([call_async/5]).
-export([cancel/2]).
-export([cancel/3]).
-export([register/3]).
-export([register/4]).
-export([unregister/2]).
-export([subscribe/3]).
-export([subscribe/4]).
-export([unsubscribe/2]).
-export([publish/3]).
-export([publish/4]).
-export([publish/5]).



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
    with_conn(Conn, fun(Pid) ->
        bondy_connect_connection:call(Pid, Uri, Args, KWArgs, Opts)
    end).


-doc "Asynchronous call with positional arguments. See `call_async/5`.".
-spec call_async(conn(), binary(), list()) ->
    {ok, reference()} | {error, term()}.
call_async(Conn, Uri, Args) ->
    call_async(Conn, Uri, Args, #{}, #{}).


-doc "Asynchronous call with positional + keyword arguments. See `call_async/5`.".
-spec call_async(conn(), binary(), list(), map()) ->
    {ok, reference()} | {error, term()}.
call_async(Conn, Uri, Args, KWArgs) ->
    call_async(Conn, Uri, Args, KWArgs, #{}).


-doc """
Issue a call without blocking. Returns `{ok, Token}`; the reply is later sent to
the calling process as `{bondy_connect, Token, {ok, Result} | {error, Reason}}`.
""".
-spec call_async(conn(), binary(), list(), map(), map()) ->
    {ok, reference()} | {error, term()}.
call_async(Conn, Uri, Args, KWArgs, Opts) ->
    with_conn(Conn, fun(Pid) ->
        bondy_connect_connection:call_async(Pid, Uri, Args, KWArgs, Opts)
    end).


-doc "Cancel an in-flight async call (mode `killnowait`). See `cancel/3`.".
-spec cancel(conn(), reference()) -> ok | {error, term()}.
cancel(Conn, Token) ->
    cancel(Conn, Token, killnowait).


-doc """
Cancel an in-flight async call identified by the `Token` returned from
`call_async`. `Mode` is `skip` | `kill` | `killnowait`. The async caller still
receives a terminating `{bondy_connect, Token, {error, _}}` reply.
""".
-spec cancel(conn(), reference(), skip | kill | killnowait) ->
    ok | {error, term()}.
cancel(Conn, Token, Mode) ->
    with_conn(Conn, fun(Pid) ->
        bondy_connect_connection:cancel(Pid, Token, Mode)
    end).


-doc "Register a procedure. See `register/4`.".
-spec register(conn(), binary(), handler()) ->
    {ok, pos_integer()} | {error, term()}.
register(Conn, Uri, Handler) ->
    register(Conn, Uri, Handler, #{}).


-doc """
Register `Uri` as a procedure served by `Handler`. The handler runs in an
isolated worker on each invocation; see `m:bondy_connect_handler_spec` for the
contract. Returns `{ok, RegistrationId}`.
""".
-spec register(conn(), binary(), handler(), map()) ->
    {ok, pos_integer()} | {error, term()}.
register(Conn, Uri, Handler, Opts) ->
    with_conn(Conn, fun(Pid) ->
        bondy_connect_connection:register(Pid, Uri, Handler, Opts)
    end).


-doc "Unregister a procedure by its registration id or URI.".
-spec unregister(conn(), pos_integer() | binary()) -> ok | {error, term()}.
unregister(Conn, RegRef) ->
    with_conn(Conn, fun(Pid) ->
        bondy_connect_connection:unregister(Pid, RegRef)
    end).


-doc "Subscribe to a topic. See `subscribe/4`.".
-spec subscribe(conn(), binary(), handler()) ->
    {ok, pos_integer()} | {error, term()}.
subscribe(Conn, Topic, Handler) ->
    subscribe(Conn, Topic, Handler, #{}).


-doc """
Subscribe to `Topic`; `Handler` is invoked per event. Events are delivered
**FIFO per subscription** by default — pass `Opts` `#{ordered => false}` for
concurrent delivery. Returns `{ok, SubscriptionId}`.
""".
-spec subscribe(conn(), binary(), handler(), map()) ->
    {ok, pos_integer()} | {error, term()}.
subscribe(Conn, Topic, Handler, Opts) ->
    with_conn(Conn, fun(Pid) ->
        bondy_connect_connection:subscribe(Pid, Topic, Handler, Opts)
    end).


-doc "Unsubscribe from a topic by its subscription id or URI.".
-spec unsubscribe(conn(), pos_integer() | binary()) -> ok | {error, term()}.
unsubscribe(Conn, SubRef) ->
    with_conn(Conn, fun(Pid) ->
        bondy_connect_connection:unsubscribe(Pid, SubRef)
    end).


-doc "Publish to a topic with positional arguments. See `publish/5`.".
-spec publish(conn(), binary(), list()) -> ok | {ok, pos_integer()} | {error, term()}.
publish(Conn, Topic, Args) ->
    publish(Conn, Topic, Args, #{}, #{}).


-doc "Publish to a topic with positional + keyword arguments. See `publish/5`.".
-spec publish(conn(), binary(), list(), map()) ->
    ok | {ok, pos_integer()} | {error, term()}.
publish(Conn, Topic, Args, KWArgs) ->
    publish(Conn, Topic, Args, KWArgs, #{}).


-doc """
Publish to `Topic`. By default fire-and-forget (`ok`); with `Opts`
`#{acknowledge => true}` it waits for the router and returns
`{ok, PublicationId}`.
""".
-spec publish(conn(), binary(), list(), map(), map()) ->
    ok | {ok, pos_integer()} | {error, term()}.
publish(Conn, Topic, Args, KWArgs, Opts) ->
    with_conn(Conn, fun(Pid) ->
        bondy_connect_connection:publish(Pid, Topic, Args, KWArgs, Opts)
    end).



%% =============================================================================
%% PRIVATE
%% =============================================================================



%% @private
with_conn(Conn, Fun) ->
    case resolve(Conn) of
        undefined -> {error, not_connected};
        Pid -> Fun(Pid)
    end.



%% @private
resolve(Conn) when is_pid(Conn) ->
    Conn;
resolve(Name) when is_atom(Name) ->
    bondy_connect_manager:whereis_name(Name).
