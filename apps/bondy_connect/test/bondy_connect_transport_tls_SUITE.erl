%% =============================================================================
%% SPDX-FileCopyrightText: 2016 - 2026 Leapsight
%% SPDX-License-Identifier: Apache-2.0
%% =============================================================================

-module(bondy_connect_transport_tls_SUITE).

-moduledoc """
M5 — **raw WAMP socket over TLS** integration tests against a live Bondy
`wamp_tls` listener (port 18085, enabled in `bondy_ct`).

- **Round trip**: a full register→call and a publish→event over TLS
  (`verify_none`) prove the encrypted transport carries WAMP end to end — same
  4-octet handshake and frames as TCP, over `ssl`.
- **Secure by default is real**: `verify_peer` (the default) performs genuine
  certificate validation and **rejects** the server with a TLS alert rather than
  silently connecting. (The repo's test CA at `etc/ssl/server/cacert.pem` is
  currently expired, so even pinning it is rejected — which is the point: the
  client enforces certificate validity. A positive verify_peer path needs a
  freshly generated cert chain, tracked separately.)
""".

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").
-include_lib("bondy_wamp/include/bondy_wamp.hrl").
-include("bondy_connect.hrl").

-compile([nowarn_export_all, export_all]).

-define(REALM, <<"com.example.bondy_connect.m5.tls">>).
-define(HOST, "127.0.0.1").
-define(PORT, 18085).


all() ->
    [
        tls_call_round_trip,
        tls_pubsub_round_trip,
        verify_peer_rejects_server
    ].


init_per_suite(Config) ->
    bondy_ct:start_bondy(),
    {ok, _} = application:ensure_all_started(bondy_connect),
    ok = add_anon_realm(?REALM),
    Config.

end_per_suite(_) ->
    ok.



%% =============================================================================
%% TESTS
%% =============================================================================



%% A full register→call works over the TLS transport.
tls_call_round_trip(_) ->
    Conn = connect(#{verify => verify_none}),
    ?assertEqual(established, bondy_connect:status(Conn)),
    {ok, _} = bondy_connect:register(Conn, <<"com.example.res.tls">>, echo_handler()),
    {ok, R} = bondy_connect:call(Conn, <<"com.example.res.tls">>, [<<"hi">>]),
    ?assertEqual([<<"hi">>], maps:get(args, R)),
    ok = bondy_connect:disconnect(Conn).


%% A subscribe→publish→event round trip works over the TLS transport, proving the
%% EVENT path (not just request/response) survives the encrypted link.
tls_pubsub_round_trip(_) ->
    Topic = <<"com.example.res.tls.topic">>,
    Self = self(),
    Sub = connect(#{verify => verify_none}),
    {ok, _} = bondy_connect:subscribe(Sub, Topic, event_handler(Self)),

    Pub = connect(#{verify => verify_none}),
    ok = bondy_connect:publish(Pub, Topic, [<<"ping">>]),

    receive
        {event, [<<"ping">>]} -> ok
    after 5000 ->
        ct:fail(no_event)
    end,

    ok = bondy_connect:disconnect(Sub),
    ok = bondy_connect:disconnect(Pub).


%% Secure-by-default verification is real: with `verify_peer` (the default) the
%% TLS handshake validates the server's certificate chain and rejects it with a
%% TLS alert — a genuine verification failure, not an `econnrefused` or a silent
%% connect. (Today the repo's test CA is expired, so the alert is e.g.
%% `certificate_expired`/`unknown_ca`; either way verification is enforced.)
verify_peer_rejects_server(_) ->
    Result = bondy_connect:connect(#{
        transport => tls,
        endpoint => {?HOST, ?PORT},
        realm => ?REALM,
        auth => #{method => ?WAMP_ANON_AUTH},
        serializers => [json],
        tls => #{verify => verify_peer, server_name_indication => disable}
    }),
    %% The error is a `{connect_error, {tls_alert, _}}` reason, possibly wrapped
    %% in `{shutdown, _}` depending on whether the connect raced ahead of the
    %% `await_ready` reply — either way it must be a genuine TLS alert, never an
    %% `econnrefused` or a silent success.
    ?assertMatch({error, _}, Result),
    ?assert(has_tls_alert(Result)).



%% =============================================================================
%% HELPERS
%% =============================================================================



%% @private
echo_handler() ->
    fun(Args, _, _) -> {reply, Args} end.


%% @private An event handler that forwards each event's args to `Pid`.
event_handler(Pid) ->
    fun(Args, _, _) -> Pid ! {event, Args}, ok end.


%% @private Does the (possibly deeply wrapped) term contain a `{tls_alert, _}`?
has_tls_alert({tls_alert, _}) ->
    true;
has_tls_alert(T) when is_tuple(T) ->
    lists:any(fun has_tls_alert/1, tuple_to_list(T));
has_tls_alert(L) when is_list(L) ->
    lists:any(fun has_tls_alert/1, L);
has_tls_alert(_) ->
    false.


%% @private Connect over TLS with the given `tls` options merged in.
connect(TLS) ->
    {ok, Conn} = bondy_connect:connect(#{
        transport => tls,
        endpoint => {?HOST, ?PORT},
        realm => ?REALM,
        auth => #{method => ?WAMP_ANON_AUTH},
        serializers => [json],
        tls => TLS
    }),
    Conn.


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
