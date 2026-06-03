%% =============================================================================
%% SPDX-FileCopyrightText: 2016 - 2026 Leapsight
%% SPDX-License-Identifier: Apache-2.0
%% =============================================================================

-module(bondy_connect_transport_uds).

-moduledoc """
WAMP **raw socket over a Unix domain socket** transport (`gen_tcp` with the
`{local, Path}` address family).

Identical on the wire to `bondy_connect_transport_tcp` — the same 4-octet
handshake and `bondy_connect_framing` frames, and the same `{tcp, _, _}` active
message tags (a UDS stream socket is still a `gen_tcp` socket) — but it dials a
filesystem path instead of a host/port. The endpoint is `{local, Path}` where
`Path` is the socket file the router listens on.

There is no transport-level security (UDS is constrained by filesystem
permissions), so unlike `_tls` there are no certificate options.
""".

-behaviour(bondy_connect_transport).

-record(state, {
    socket              ::  gen_tcp:socket(),
    codec               ::  bondy_connect_codec:t() | undefined,
    max_message_length  ::  pos_integer()
}).

-define(DEFAULT_MAX_MESSAGE_LENGTH, 16#1000000).    %% 16 MB
-define(DEFAULT_CONNECT_TIMEOUT, 5000).
-define(DEFAULT_HANDSHAKE_TIMEOUT, 5000).

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

connect({local, Path}, Opts) ->
    Timeout = maps:get(connect_timeout, Opts, ?DEFAULT_CONNECT_TIMEOUT),
    Max = maps:get(max_message_length, Opts, ?DEFAULT_MAX_MESSAGE_LENGTH),
    SockOpts = [binary, {packet, 0}, {active, false}],
    case gen_tcp:connect({local, Path}, 0, SockOpts, Timeout) of
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
    case gen_tcp:send(Socket, Request) of
        ok ->
            case gen_tcp:recv(Socket, 4, ?DEFAULT_HANDSHAKE_TIMEOUT) of
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
            gen_tcp:send(Socket, Frame);
        {error, _} = Error ->
            Error
    end.


-spec ping(binary(), #state{}) -> ok | {error, term()}.

ping(Payload, #state{socket = Socket}) ->
    gen_tcp:send(Socket, bondy_connect_framing:ping_frame(Payload)).


-spec pong(binary(), #state{}) -> ok | {error, term()}.

pong(Payload, #state{socket = Socket}) ->
    gen_tcp:send(Socket, bondy_connect_framing:pong_frame(Payload)).


-spec recv(timeout(), #state{}) ->
    {ok, [bondy_connect_transport:inbound()], #state{}} | {error, term()}.

recv(Timeout, #state{socket = Socket} = St) ->
    case gen_tcp:recv(Socket, 0, Timeout) of
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

handle_info({tcp, Socket, Bin}, #state{socket = Socket} = St) ->
    case handle_data(Bin, St) of
        {ok, Msgs, St1} ->
            %% Re-arm the socket for the next message (the connection only arms
            %% the first `{active, once}` after the handshake).
            _ = inet:setopts(Socket, [{active, once}]),
            {ok, Msgs, St1};
        {error, Reason, St1} ->
            {error, Reason, St1}
    end;

handle_info({tcp_closed, Socket}, #state{socket = Socket}) ->
    closed;

handle_info({tcp_error, Socket, Reason}, #state{socket = Socket} = St) ->
    {error, {connection_error, Reason}, St};

handle_info(_Info, _St) ->
    ignore.


-spec setopts(list() | map(), #state{}) -> ok | {error, term()}.

setopts(Opts, #state{socket = Socket}) when is_list(Opts) ->
    inet:setopts(Socket, Opts);

setopts(_, _) ->
    {error, badarg}.


-spec messages() -> {tcp, tcp_closed, tcp_error}.
messages() ->
    {tcp, tcp_closed, tcp_error}.


-spec peername(#state{}) ->
    {ok, {inet:ip_address(), inet:port_number()}} | {error, term()}.
peername(#state{socket = Socket}) ->
    inet:peername(Socket).


-spec close(#state{}) -> ok.
close(#state{socket = Socket}) ->
    gen_tcp:close(Socket).



%% =============================================================================
%% PRIVATE
%% =============================================================================



%% @private
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
