%% =============================================================================
%% SPDX-FileCopyrightText: 2016 - 2026 Leapsight
%% SPDX-License-Identifier: Apache-2.0
%% =============================================================================

-module(bondy_connect_resilience_SUITE).

-moduledoc """
M4 (Phase 6 — resilience) tests.

- **Config** (pure): `reconnect`/`ping`/`network_timeout` defaults, user-merge
  and validation.
- **Keepalive** (live): an idle connection configured with a short ping interval
  survives well past its idle timeout — proving the router answers our pings and
  no false reconnect happens.
- **Reconnect + replay** (live): abruptly killing a connection's *server-side*
  ranch handler drops the socket without a GOODBYE; the client reconnects and
  replays its declared registration so the procedure is callable again.
- **Fail-fast** (live): an in-flight async call is terminated with
  `{error, disconnected}` when the link drops; and the initial connect to a dead
  endpoint fails fast by default (no blocking on retries).
""".

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").
-include_lib("bondy_wamp/include/bondy_wamp.hrl").
-include("bondy_connect.hrl").

-compile([nowarn_export_all, export_all]).

-define(REALM, <<"com.example.bondy_connect.m4.resilience">>).
-define(HOST, "127.0.0.1").
-define(PORT, 18082).
-define(DEAD_PORT, 18099).


all() ->
    [
        %% pure config
        config_defaults,
        config_merge_user_over_defaults,
        config_rejects_bad_option,

        %% live
        ping_keepalive_survives_idle,
        reconnect_replays_registration,
        in_flight_call_fails_on_drop,
        initial_connect_fails_fast,
        initial_connect_retries_when_enabled
    ].


init_per_suite(Config) ->
    bondy_ct:start_bondy(),
    {ok, _} = application:ensure_all_started(bondy_connect),
    ok = add_anon_realm(?REALM),
    Config.

end_per_suite(_) ->
    ok.



%% =============================================================================
%% CONFIG (pure)
%% =============================================================================



config_defaults(_) ->
    {ok, C} = bondy_connect_config:validate(#{realm => ?REALM}),
    R = maps:get(reconnect, C),
    ?assertEqual(true, maps:get(enabled, R)),
    ?assertEqual(false, maps:get(retry_initial_connect, R)),
    ?assertEqual(10, maps:get(max_retries, R)),
    ?assertEqual(3000, maps:get(interval, R)),
    ?assertEqual(true, maps:get(backoff_enabled, R)),
    P = maps:get(ping, C),
    ?assertEqual(true, maps:get(enabled, P)),
    ?assertEqual(30000, maps:get(idle_timeout, P)),
    ?assertEqual(10000, maps:get(timeout, P)),
    ?assertEqual(3, maps:get(max_attempts, P)),
    ?assertEqual(60000, maps:get(network_timeout, C)).


config_merge_user_over_defaults(_) ->
    {ok, C} = bondy_connect_config:validate(#{
        realm => ?REALM,
        reconnect => #{enabled => false, max_retries => 3}
    }),
    R = maps:get(reconnect, C),
    %% user values win, untouched defaults survive
    ?assertEqual(false, maps:get(enabled, R)),
    ?assertEqual(3, maps:get(max_retries, R)),
    ?assertEqual(3000, maps:get(interval, R)).


config_rejects_bad_option(_) ->
    ?assertMatch(
        {error, {unknown_option, reconnect, bogus}},
        bondy_connect_config:validate(#{realm => ?REALM, reconnect => #{bogus => 1}})
    ),
    ?assertMatch(
        {error, {invalid_option, ping, enabled, yes}},
        bondy_connect_config:validate(#{realm => ?REALM, ping => #{enabled => yes}})
    ),
    ?assertMatch(
        {error, {invalid_option, reconnect, max_retries, -1}},
        bondy_connect_config:validate(#{realm => ?REALM, reconnect => #{max_retries => -1}})
    ),
    ?assertMatch(
        {error, {invalid_network_timeout, -1}},
        bondy_connect_config:validate(#{realm => ?REALM, network_timeout => -1})
    ).



%% =============================================================================
%% LIVE
%% =============================================================================



%% A connection with a short ping idle interval must stay established across an
%% idle period far longer than that interval: the router answers each ping and
%% the client never falsely reconnects.
ping_keepalive_survives_idle(_) ->
    {Conn, _Server} = connect_and_server(#{
        ping => #{enabled => true, idle_timeout => 300, timeout => 1000, max_attempts => 3}
    }),
    ?assertEqual(established, bondy_connect:status(Conn)),
    %% Idle ~7x the ping interval — several ping/pong cycles must occur.
    timer:sleep(2000),
    ?assertEqual(established, bondy_connect:status(Conn)),
    %% And it still works.
    {ok, _} = bondy_connect:register(Conn, <<"com.example.res.ka">>, ok_handler()),
    {ok, R} = bondy_connect:call(Conn, <<"com.example.res.ka">>, [<<"hi">>]),
    ?assertEqual([<<"hi">>], maps:get(args, R)),
    ok = bondy_connect:disconnect(Conn).


%% Killing the callee's server-side handler drops its socket without a GOODBYE;
%% the client reconnects and replays its declared registration, so the procedure
%% is callable again on the fresh session.
reconnect_replays_registration(_) ->
    Proc = <<"com.example.res.echo">>,
    {Callee, CalleeServer} = connect_and_server(#{}),
    {ok, _} = bondy_connect:register(Callee, Proc, echo_handler()),

    Caller = connect(#{}),
    {ok, R0} = bondy_connect:call(Caller, Proc, [<<"a">>]),
    ?assertEqual([<<"a">>], maps:get(args, R0)),

    %% Abrupt server-side drop.
    true = is_process_alive(CalleeServer),
    _ = exit(CalleeServer, kill),

    %% The callee reconnects...
    ok = wait_until(fun() -> bondy_connect:status(Callee) =:= established end, 100, 100),

    %% ...and the replayed registration makes the procedure callable again.
    ok = wait_until(
        fun() ->
            case bondy_connect:call(Caller, Proc, [<<"b">>]) of
                {ok, #{args := [<<"b">>]}} -> true;
                _ -> false
            end
        end,
        100, 100
    ),

    ok = bondy_connect:disconnect(Caller),
    ok = bondy_connect:disconnect(Callee).


%% An in-flight async call is terminated with {error, disconnected} (fail-fast)
%% when the caller's link drops.
in_flight_call_fails_on_drop(_) ->
    Proc = <<"com.example.res.slow">>,
    Callee = connect(#{}),
    {ok, _} = bondy_connect:register(Callee, Proc, slow_handler()),

    {Caller, CallerServer} = connect_and_server(#{}),
    {ok, Token} = bondy_connect:call_async(Caller, Proc, []),
    %% Ensure the CALL is in flight.
    timer:sleep(300),

    _ = exit(CallerServer, kill),

    receive
        {bondy_connect, Token, Reply} ->
            ?assertEqual({error, disconnected}, Reply)
    after 5000 ->
        ct:fail(no_disconnected_reply)
    end,

    ok = bondy_connect:disconnect(Caller),
    ok = bondy_connect:disconnect(Callee).


%% By default the initial connect is fail-fast: a dead endpoint returns an error
%% promptly rather than blocking on the reconnect budget.
initial_connect_fails_fast(_) ->
    {Elapsed, Result} = timer:tc(fun() ->
        bondy_connect:connect(#{
            transport => tcp,
            endpoint => {?HOST, ?DEAD_PORT},
            realm => ?REALM,
            auth => #{method => ?WAMP_ANON_AUTH},
            serializers => [json]
        })
    end),
    ?assertMatch({error, _}, Result),
    %% Well under the 30s await_ready ceiling — proves it did not retry-loop.
    ?assert(Elapsed < 5000000).


%% With retry_initial_connect => true the initial connect retries the configured
%% budget and then returns an error (still bounded, no infinite block).
initial_connect_retries_when_enabled(_) ->
    Result = bondy_connect:connect(#{
        transport => tcp,
        endpoint => {?HOST, ?DEAD_PORT},
        realm => ?REALM,
        auth => #{method => ?WAMP_ANON_AUTH},
        serializers => [json],
        reconnect => #{
            enabled => true,
            retry_initial_connect => true,
            max_retries => 2,
            interval => 200,
            deadline => 0,
            backoff_enabled => false
        }
    }),
    ?assertMatch({error, _}, Result).



%% =============================================================================
%% HELPERS
%% =============================================================================



%% @private
ok_handler() ->
    fun(Args, _, _) -> {reply, Args} end.

echo_handler() ->
    fun(Args, _, _) -> {reply, Args} end.

slow_handler() ->
    fun(_, _, _) -> timer:sleep(3000), {reply, [<<"too_late">>]} end.


%% @private Establish a connection (with extra config merged in).
connect(Extra) ->
    Base = #{
        transport => tcp,
        endpoint => {?HOST, ?PORT},
        realm => ?REALM,
        auth => #{method => ?WAMP_ANON_AUTH},
        serializers => [json]
    },
    {ok, Conn} = bondy_connect:connect(maps:merge(Base, Extra)),
    Conn.


%% @private Establish a connection and return its *server-side* ranch handler pid
%% (identified as the new TCP connection that appears during connect).
connect_and_server(Extra) ->
    Before = bondy_wamp_tcp:tcp_connections(),
    Conn = connect(Extra),
    Server = new_server_conn(Before, 100),
    {Conn, Server}.


%% @private
new_server_conn(_Before, 0) ->
    error(no_new_server_conn);
new_server_conn(Before, N) ->
    case bondy_wamp_tcp:tcp_connections() -- Before of
        [Pid | _] -> Pid;
        [] -> timer:sleep(50), new_server_conn(Before, N - 1)
    end.


%% @private Poll `Fun` until it returns `true` (or fail after Tries x SleepMs).
wait_until(_Fun, 0, _Sleep) ->
    ct:fail(condition_not_met);
wait_until(Fun, Tries, Sleep) ->
    case Fun() of
        true -> ok;
        _ -> timer:sleep(Sleep), wait_until(Fun, Tries - 1, Sleep)
    end.


%% @private
add_anon_realm(RealmUri) ->
    Cfg = #{
        uri => RealmUri,
        authmethods => [?WAMP_ANON_AUTH],
        security_enabled => true,
        grants => [
            #{
                permissions => [
                    <<"wamp.register">>,
                    <<"wamp.unregister">>,
                    <<"wamp.call">>,
                    <<"wamp.subscribe">>,
                    <<"wamp.publish">>
                ],
                uri => <<"">>,
                match => <<"prefix">>,
                roles => [<<"anonymous">>]
            }
        ],
        sources => [
            #{
                usernames => [<<"anonymous">>],
                authmethod => ?WAMP_ANON_AUTH,
                cidr => <<"0.0.0.0/0">>
            }
        ]
    },
    _ = bondy_realm:create(Cfg),
    ok.
