%% =============================================================================
%% SPDX-FileCopyrightText: 2016 - 2026 Leapsight
%% SPDX-License-Identifier: Apache-2.0
%% =============================================================================

-module(bondy_connect_transport_tls).

-moduledoc """
WAMP **raw socket over TLS** transport (`ssl`).

Identical on the wire to `bondy_connect_transport_tcp` — the same 4-octet
handshake and `bondy_connect_framing` frames — but over a TLS-secured socket.

## Secure by default (Decision 11)

The TLS session **verifies the server certificate by default** (`verify_peer`):

- CA trust: the user's `cacerts`/`cacertfile`, otherwise the OS trust store
  (`public_key:cacerts_get/0`).
- Hostname check: the connected host is used for SNI and matched against the
  certificate (`public_key:pkix_verify_hostname_match_fun(https)`).
- Protocol floor: TLS 1.2+ (`['tlsv1.3', 'tlsv1.2']`).
- Mutual TLS: supply `certfile`/`keyfile` (or `cert`/`key`).

Verification can be turned off explicitly with `tls => #{verify => verify_none}`,
which is **logged at warning level** — it disables all certificate checks and
must only be used for local testing against a self-signed router.
""".

-behaviour(bondy_connect_transport).

-include_lib("kernel/include/logger.hrl").

-record(state, {
    socket              ::  ssl:sslsocket(),
    codec               ::  bondy_connect_codec:t() | undefined,
    max_message_length  ::  pos_integer()
}).

-define(DEFAULT_MAX_MESSAGE_LENGTH, 16#1000000).    %% 16 MB
-define(DEFAULT_CONNECT_TIMEOUT, 5000).
-define(DEFAULT_HANDSHAKE_TIMEOUT, 5000).
-define(DEFAULT_VERSIONS, ['tlsv1.3', 'tlsv1.2']).
-define(DEFAULT_DEPTH, 10).

-export([connect/2]).
-export([handshake/2]).
-export([send/2]).
-export([ping/2]).
-export([pong/2]).
-export([recv/2]).
-export([handle_data/2]).
-export([handle_info/2]).
-export([setopts/2]).
-export([messages/0]).
-export([peername/1]).
-export([close/1]).



%% =============================================================================
%% bondy_connect_transport CALLBACKS
%% =============================================================================



-spec connect(bondy_connect_transport:endpoint(), map()) ->
    {ok, #state{}} | {error, term()}.

connect({Host, Port}, Opts) when is_integer(Port) ->
    Timeout = maps:get(connect_timeout, Opts, ?DEFAULT_CONNECT_TIMEOUT),
    Max = maps:get(max_message_length, Opts, ?DEFAULT_MAX_MESSAGE_LENGTH),
    SslOpts = ssl_opts(Host, Opts),
    case ssl:connect(Host, Port, SslOpts, Timeout) of
        {ok, Socket} ->
            {ok, #state{socket = Socket, max_message_length = Max}};
        {error, _} = Error ->
            Error
    end.


-spec handshake(bondy_connect_transport:subprotocol(), #state{}) ->
    {ok, bondy_connect_transport:subprotocol(), #state{}} | {error, term()}.

handshake({raw, binary, Enc}, #state{socket = Socket, max_message_length = Max} = St) ->
    Code = bondy_connect_framing:serializer_code(Enc),
    Exp = bondy_connect_framing:length_exponent(Max),
    Request = bondy_connect_framing:handshake_request(Exp, Code),
    case ssl:send(Socket, Request) of
        ok ->
            case ssl:recv(Socket, 4, ?DEFAULT_HANDSHAKE_TIMEOUT) of
                {ok, Reply} ->
                    negotiate(Reply, Enc, Exp, St);
                {error, _} = Error ->
                    Error
            end;
        {error, _} = Error ->
            Error
    end.


-spec send(bondy_wamp_message:t(), #state{}) -> ok | {error, term()}.

send(Msg, #state{socket = Socket, codec = Codec}) when Codec =/= undefined ->
    case bondy_connect_codec:encode(Msg, Codec) of
        {ok, Frame} ->
            ssl:send(Socket, Frame);
        {error, _} = Error ->
            Error
    end.


-spec ping(binary(), #state{}) -> ok | {error, term()}.

ping(Payload, #state{socket = Socket}) ->
    ssl:send(Socket, bondy_connect_framing:ping_frame(Payload)).


-spec pong(binary(), #state{}) -> ok | {error, term()}.

pong(Payload, #state{socket = Socket}) ->
    ssl:send(Socket, bondy_connect_framing:pong_frame(Payload)).


-spec recv(timeout(), #state{}) ->
    {ok, [bondy_connect_transport:inbound()], #state{}} | {error, term()}.

recv(Timeout, #state{socket = Socket} = St) ->
    case ssl:recv(Socket, 0, Timeout) of
        {ok, Data} ->
            case handle_data(Data, St) of
                {ok, Msgs, St1} ->
                    {ok, Msgs, St1};
                {error, Reason, _St1} ->
                    {error, Reason}
            end;
        {error, _} = Error ->
            Error
    end.


-spec handle_data(binary(), #state{}) ->
    {ok, [bondy_connect_transport:inbound()], #state{}}
    | {error, term(), #state{}}.

handle_data(Data, #state{codec = Codec} = St) when Codec =/= undefined ->
    case bondy_connect_codec:decode(Data, Codec) of
        {ok, Msgs, Codec1} ->
            {ok, Msgs, St#state{codec = Codec1}};
        {error, Reason, Codec1} ->
            {error, Reason, St#state{codec = Codec1}}
    end.


-spec handle_info(term(), #state{}) ->
    {ok, [bondy_connect_transport:inbound()], #state{}}
    | {error, term(), #state{}}
    | closed
    | ignore.

handle_info({ssl, Socket, Bin}, #state{socket = Socket} = St) ->
    case handle_data(Bin, St) of
        {ok, Msgs, St1} ->
            _ = ssl:setopts(Socket, [{active, once}]),
            {ok, Msgs, St1};
        {error, Reason, St1} ->
            {error, Reason, St1}
    end;

handle_info({ssl_closed, Socket}, #state{socket = Socket}) ->
    closed;

handle_info({ssl_error, Socket, Reason}, #state{socket = Socket} = St) ->
    {error, {connection_error, Reason}, St};

handle_info(_Info, _St) ->
    ignore.


-spec setopts(list() | map(), #state{}) -> ok | {error, term()}.

setopts(Opts, #state{socket = Socket}) when is_list(Opts) ->
    ssl:setopts(Socket, Opts);

setopts(_, _) ->
    {error, badarg}.


-spec messages() -> {ssl, ssl_closed, ssl_error}.
messages() ->
    {ssl, ssl_closed, ssl_error}.


-spec peername(#state{}) ->
    {ok, {inet:ip_address(), inet:port_number()}} | {error, term()}.
peername(#state{socket = Socket}) ->
    ssl:peername(Socket).


-spec close(#state{}) -> ok.
close(#state{socket = Socket}) ->
    ssl:close(Socket).



%% =============================================================================
%% PRIVATE
%% =============================================================================



%% @private Same handshake negotiation as the TCP transport.
negotiate(Reply, Enc, OurExp, St) ->
    case bondy_connect_framing:parse_handshake(Reply) of
        {ok, TheirExp, TheirCode} ->
            case bondy_connect_framing:code_to_encoding(TheirCode) of
                Enc ->
                    SendMax = bondy_connect_framing:exponent_to_bytes(TheirExp),
                    RecvMax = bondy_connect_framing:exponent_to_bytes(OurExp),
                    Codec = bondy_connect_codec:new(Enc, SendMax, RecvMax),
                    {ok, {raw, binary, Enc}, St#state{codec = Codec}};
                Other ->
                    {error, {serializer_mismatch, Other}}
            end;
        {error, Reason} ->
            {error, Reason}
    end.


%% @private Assemble the `ssl:connect/4` options, secure by default.
ssl_opts(Host, Opts) ->
    TLS = maps:get(tls, Opts, #{}),
    Verify = maps:get(verify, TLS, verify_peer),
    Versions = maps:get(versions, TLS, ?DEFAULT_VERSIONS),
    Base = [binary, {packet, 0}, {active, false}, {nodelay, true}],
    Base
        ++ [{versions, Versions}]
        ++ verify_opts(Verify, Host, TLS)
        ++ cert_opts(TLS).


%% @private
verify_opts(verify_none, _Host, _TLS) ->
    ?LOG_WARNING(#{
        description =>
            "TLS peer verification is disabled (verify_none); the server "
            "certificate will not be validated. Use only for local testing."
    }),
    [{verify, verify_none}];

verify_opts(verify_peer, Host, TLS) ->
    [{verify, verify_peer}, {depth, maps:get(depth, TLS, ?DEFAULT_DEPTH)}]
        ++ ca_opts(TLS)
        ++ hostname_opts(Host, TLS).


%% @private CA trust: user-supplied, otherwise the OS trust store.
ca_opts(#{cacerts := CAs}) ->
    [{cacerts, CAs}];
ca_opts(#{cacertfile := File}) ->
    [{cacertfile, File}];
ca_opts(_) ->
    [{cacerts, public_key:cacerts_get()}].


%% @private SNI + hostname verification. A string host is used for SNI and for
%% the HTTPS-style hostname match; `server_name_indication => disable` turns both
%% off (e.g. when connecting by IP to a cert without an IP SAN).
hostname_opts(Host, TLS) ->
    case maps:get(server_name_indication, TLS, default) of
        disable ->
            [{server_name_indication, disable}];
        default when is_list(Host) ->
            [{server_name_indication, Host} | hostname_check()];
        default ->
            hostname_check();
        Name ->
            [{server_name_indication, Name} | hostname_check()]
    end.


%% @private
hostname_check() ->
    [{customize_hostname_check, [
        {match_fun, public_key:pkix_verify_hostname_match_fun(https)}
    ]}].


%% @private Optional client certificate (mutual TLS) and related material.
cert_opts(TLS) ->
    lists:append([
        opt(certfile, TLS),
        opt(keyfile, TLS),
        opt(cert, TLS),
        opt(key, TLS),
        opt(password, TLS),
        opt(ciphers, TLS)
    ]).


%% @private
opt(Key, TLS) ->
    case maps:find(Key, TLS) of
        {ok, Value} -> [{Key, Value}];
        error -> []
    end.
