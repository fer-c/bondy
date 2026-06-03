%% =============================================================================
%% SPDX-FileCopyrightText: 2016 - 2026 Leapsight
%% SPDX-License-Identifier: Apache-2.0
%% =============================================================================

-module(bondy_connect_transport).

-moduledoc """
Behaviour for `bondy_connect` transports — the **record-oriented** boundary
(Decision 3): a transport owns the socket, the codec and the framing, so the
connection process (`bondy_connect_connection`, Phase 3) and the protocol layer
deal only in WAMP **records**.

Implementations:

- `bondy_connect_transport_tcp` — WAMP raw socket over TCP (this phase).
- `_tls` / `_uds` — raw socket over TLS / Unix domain socket (Phase 7).
- `_ws` — WebSocket via gun (Phase 7).
- `_local` — in-VM peer (Phase 7).

Inbound bytes from an active socket arrive as `info` messages tagged per
`messages/0`; the connection feeds them to `handle_data/2` to obtain records.
For synchronous flows (the handshake, tests) `recv/2` reads and decodes in one
call. A decode failure is surfaced as `{error, {protocol_error, _}}`, never an
assertion crash.
""".

-type endpoint()    ::  {inet:hostname() | inet:ip_address(), inet:port_number()}
                        | {local, file:filename_all()}.
-type subprotocol() ::  {raw, binary, bondy_connect_framing:serializer()}.
-type opts()        ::  map().
-type state()       ::  term().
-type inbound()     ::  bondy_connect_codec:inbound().

-export_type([endpoint/0]).
-export_type([subprotocol/0]).
-export_type([opts/0]).
-export_type([state/0]).
-export_type([inbound/0]).



%% =============================================================================
%% CALLBACKS
%% =============================================================================



-doc "Establish the transport connection (no WAMP handshake yet).".
-callback connect(endpoint(), opts()) -> {ok, state()} | {error, term()}.

-doc "Perform the transport-level WAMP handshake and negotiate the subprotocol.".
-callback handshake(subprotocol(), state()) ->
    {ok, Negotiated :: subprotocol(), state()} | {error, term()}.

-doc "Encode, frame and send a WAMP record.".
-callback send(bondy_wamp_message:t(), state()) -> ok | {error, term()}.

-doc "Send a transport keepalive ping carrying `Payload`.".
-callback ping(Payload :: binary(), state()) -> ok | {error, term()}.

-doc "Send a transport keepalive pong (the reply to an inbound ping).".
-callback pong(Payload :: binary(), state()) -> ok | {error, term()}.

-doc "Read available bytes and decode them (synchronous/passive).".
-callback recv(timeout(), state()) ->
    {ok, [inbound()], state()} | {error, term()}.

-doc "Decode bytes delivered as an active-socket `info` message.".
-callback handle_data(binary(), state()) ->
    {ok, [inbound()], state()} | {error, term(), state()}.

-doc """
Interpret a process `info` message delivered by an active transport, so the
connection process never needs to know a transport's message-tag shapes (raw
socket `{tcp,_,_}`/`{ssl,_,_}`, gun `gun_*`, in-VM `{$bondy_request,…}`, …).

Returns:

- `{ok, Records, State}` — decoded inbound records; the transport has re-armed
  its own flow control (e.g. `{active, once}`).
- `closed` — the peer closed the link cleanly.
- `{error, Reason, State}` — a transport/decode failure; `Reason` is one of the
  connection's transport-failure reasons (e.g. `connection_closed`,
  `{connection_error, _}`, `{protocol_error, _}`).
- `ignore` — the message is not this transport's; the connection handles it
  elsewhere.
""".
-callback handle_info(Info :: term(), state()) ->
    {ok, [inbound()], state()}
    | {error, Reason :: term(), state()}
    | closed
    | ignore.

-doc "Set transport/socket options (e.g. toggle active mode).".
-callback setopts(Opts :: list() | map(), state()) -> ok | {error, term()}.

-doc "The `{OK, Closed, Error}` inbound message tags (à la `ranch_transport`).".
-callback messages() -> {atom(), atom(), atom()}.

-doc "The remote peer address.".
-callback peername(state()) ->
    {ok, {inet:ip_address(), inet:port_number()}} | {error, term()}.

-doc "Close the transport.".
-callback close(state()) -> ok.
