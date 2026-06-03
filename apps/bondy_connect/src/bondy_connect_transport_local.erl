%% =============================================================================
%% SPDX-FileCopyrightText: 2016 - 2026 Leapsight
%% SPDX-License-Identifier: Apache-2.0
%% =============================================================================

-module(bondy_connect_transport_local).

-moduledoc """
**In-VM (local) WAMP transport** — talks directly to the co-located
`bondy_router` instead of over a socket. There is **no socket, no encoding and
no framing**: WAMP message *records* are exchanged with the router in-process
(the faithful Bondy port of `awre_trans_local`).

## How it works

The `bondy_connect_connection` process **is** the in-VM peer. `connect/2` opens
a real Bondy session with `bondy_session_manager:open/3` *from the connection
process*, so the session's `bondy_ref` targets the connection pid. Everything
the router needs to deliver to the peer (RESULT/ERROR/EVENT/INVOCATION/…)
therefore arrives in the connection's own mailbox as `{$bondy_request, _, _, M}`
messages, which `handle_info/2` turns into inbound records — the very same
downstream path the socket transports feed.

Outbound records are handed to `bondy_router:forward/2` in `send/2`. The router
replies asynchronously (via the mailbox), so `send/2` is fire-and-forget for
CALL/REGISTER/SUBSCRIBE/PUBLISH just as on a socket.

## Handshake

There is no transport handshake (`handshake/2` is a no-op) and the session is
already open by the time the connection sends its `HELLO`. So when `send/2` sees
the `HELLO` it answers locally by synthesizing the `WELCOME` from the open
session and delivering it to the connection mailbox — making the in-VM peer look
exactly like a remote one to the connection's protocol layer.

## Authentication

An in-VM peer lives inside the same trusted BEAM as the router, so the WAMP
challenge/response auth methods (cryptosign/WAMP-CRA/ticket) do not apply: the
session is opened as **anonymous** (the realm must permit anonymous from the
loopback source). Realm-level authorization (the grants/sources) still applies.

## Keepalive / flow control

Network keepalive is meaningless in-VM (a dead router/peer is detected by the
session monitor, not a missed pong). `ping/2` answers itself immediately so the
connection tolerates ping being enabled; `setopts/2` is a no-op.
""".

-behaviour(bondy_connect_transport).

-include_lib("kernel/include/logger.hrl").
-include_lib("bondy_wamp/include/bondy_wamp.hrl").
-include("bondy_connect.hrl").

%% The router→peer delivery tag. Source of truth: apps/bondy/include/bondy.hrl
%% (`-define(BONDY_REQ, '$bondy_request').`). Defined locally to avoid pulling
%% the whole bondy.hrl into the bondy_connect app.
-define(BONDY_REQ, '$bondy_request').

%% A synthetic loopback peer for the session record (the router's IP-based
%% pipeline — logging, events, source-based authz — expects a `{IP, Port}`).
-define(LOCAL_PEER, {{127, 0, 0, 1}, 0}).

-record(state, {
    realm_uri           ::  binary(),
    session             ::  bondy_session:t() | undefined,
    context             ::  bondy_context:t() | undefined
}).

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



-spec connect(bondy_connect_transport:endpoint() | local | undefined, map()) ->
    {ok, #state{}} | {error, term()}.

connect(Endpoint, Opts) when Endpoint == local; Endpoint == undefined ->
    case maps:find(realm, Opts) of
        {ok, RealmUri} when is_binary(RealmUri) ->
            do_connect(RealmUri, Opts);
        _ ->
            {error, missing_realm}
    end;

connect({router, _}, Opts) ->
    connect(local, Opts);

connect(Endpoint, _Opts) ->
    {error, {unsupported_endpoint, Endpoint}}.


-spec handshake(bondy_connect_transport:subprotocol(), #state{}) ->
    {ok, bondy_connect_transport:subprotocol(), #state{}} | {error, term()}.

%% No transport handshake in-VM; the session is already open. We conform to the
%% behaviour's `subprotocol()' type with a value the connection discards.
handshake(_Sub, #state{} = St) ->
    {ok, {raw, binary, erl}, St}.


-spec send(bondy_wamp_message:t(), #state{}) -> ok | {error, term()}.

%% The connection's HELLO: the session is already open (connect/2), so we answer
%% locally by delivering a synthesized WELCOME to the connection mailbox.
send(#hello{}, #state{} = St) ->
    Welcome = welcome_msg(St),
    deliver(Welcome, St),
    ok;

%% A client GOODBYE: nothing to forward — the session is closed in `close/1'
%% (which also tears down the peer's registrations/subscriptions).
send(#goodbye{}, #state{}) ->
    ok;

send(#abort{}, #state{}) ->
    ok;

%% Any other WAMP record is forwarded straight to the router. Replies (RESULT,
%% ERROR, REGISTERED, SUBSCRIBED, EVENT, INVOCATION, …) come back asynchronously
%% as `{$bondy_request, _, _, M}' to the connection mailbox (see handle_info/2).
send(Msg, #state{context = Ctxt} = St) when Ctxt =/= undefined ->
    try bondy_router:forward(Msg, Ctxt) of
        {ok, _Ctxt1} ->
            ok;
        {reply, Reply, _Ctxt1} ->
            %% A synchronous reply (rare for these message types); deliver it on
            %% the same inbound path the async replies use.
            deliver(Reply, St),
            ok;
        {stop, Reply, _Ctxt1} ->
            deliver(Reply, St),
            ok
    catch
        Class:Reason ->
            {error, {forward_failed, Class, Reason}}
    end.


-spec ping(binary(), #state{}) -> ok | {error, term()}.

%% In-VM keepalive is meaningless, but answer ourselves so the connection
%% tolerates ping being enabled (a missing pong would otherwise drop the link).
ping(Payload, #state{}) ->
    self() ! {bondy_connect_local_pong, Payload},
    ok.


-spec pong(binary(), #state{}) -> ok | {error, term()}.

pong(_Payload, #state{}) ->
    ok.


-spec recv(timeout(), #state{}) ->
    {ok, [bondy_connect_transport:inbound()], #state{}} | {error, term()}.

%% Synchronous read: selectively pull the next router delivery from the mailbox.
%% (The active flow uses handle_info/2; this exists for completeness/tests.)
recv(Timeout, #state{} = St) ->
    receive
        {?BONDY_REQ, _Pid, _RealmUri, M} ->
            {ok, [M], St};
        {bondy_connect_local_pong, Payload} ->
            {ok, [{pong, Payload}], St}
    after Timeout ->
        {error, timeout}
    end.


-spec handle_data(binary(), #state{}) ->
    {ok, [bondy_connect_transport:inbound()], #state{}}
    | {error, term(), #state{}}.

%% There is no byte stream in-VM; nothing to decode.
handle_data(_Data, #state{} = St) ->
    {ok, [], St}.


-spec handle_info(term(), #state{}) ->
    {ok, [bondy_connect_transport:inbound()], #state{}}
    | {error, term(), #state{}}
    | closed
    | ignore.

%% A WAMP message delivered by the router (or the locally-synthesized WELCOME).
handle_info({?BONDY_REQ, _Pid, _RealmUri, M}, #state{} = St) ->
    {ok, [M], St};

%% Our own keepalive answer (see ping/2).
handle_info({bondy_connect_local_pong, Payload}, #state{} = St) ->
    {ok, [{pong, Payload}], St};

handle_info(_Info, _St) ->
    ignore.


-spec setopts(list() | map(), #state{}) -> ok | {error, term()}.

%% No socket to configure; the active `{active, once}' cycle does not apply.
setopts(_Opts, #state{}) ->
    ok.


-spec messages() -> {atom(), atom(), atom()}.
messages() ->
    {?BONDY_REQ, bondy_connect_local_closed, bondy_connect_local_error}.


-spec peername(#state{}) ->
    {ok, {inet:ip_address(), inet:port_number()}} | {error, term()}.
peername(#state{}) ->
    {ok, ?LOCAL_PEER}.


-spec close(#state{}) -> ok.
close(#state{session = undefined}) ->
    ok;
close(#state{session = Session}) ->
    _ = catch bondy_session_manager:close(Session),
    ok.



%% =============================================================================
%% PRIVATE
%% =============================================================================



%% @private Open the in-VM session and build the forwarding context.
do_connect(RealmUri, Opts) ->
    case bondy_realm:get(RealmUri) of
        {ok, _Realm} ->
            Roles = maps:get(roles, Opts, #{}),
            open_session(RealmUri, Roles);
        {error, not_found} ->
            {error, {no_such_realm, RealmUri}}
    end.


%% @private
open_session(RealmUri, Roles) ->
    SessionId = bondy_session_id:new(),
    Properties = #{
        roles => Roles,
        peer => ?LOCAL_PEER,
        authrealm => RealmUri,
        authid => bondy_utils:uuid(),
        authmethod => ?WAMP_ANON_AUTH,
        authrole => <<"anonymous">>,
        is_anonymous => true,
        type => client
    },
    try bondy_session_manager:open(SessionId, RealmUri, Properties) of
        {ok, Session} ->
            Ctxt = bondy_context:set_session(bondy_context:new(), Session),
            {ok, #state{
                realm_uri = RealmUri,
                session = Session,
                context = Ctxt
            }};
        {error, _} = Error ->
            Error
    catch
        Class:Reason ->
            {error, {session_open_failed, Class, Reason}}
    end.


%% @private Synthesize the WELCOME for the already-open session, mirroring
%% `bondy_wamp_protocol:open_session/2'.
welcome_msg(#state{session = Session, realm_uri = RealmUri}) ->
    SessionId = bondy_session:external_id(Session),
    Info = bondy_session:to_external(Session),
    bondy_wamp_message:welcome(SessionId, Info#{
        realm => RealmUri,
        agent => bondy_router:agent(),
        roles => bondy_router:roles()
    }).


%% @private Deliver an inbound WAMP record to the connection mailbox on the same
%% path the router uses, so handle_info/2 turns it into an inbound record.
deliver(Msg, #state{realm_uri = RealmUri}) ->
    self() ! {?BONDY_REQ, self(), RealmUri, Msg},
    ok.
