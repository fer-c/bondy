%% =============================================================================
%% SPDX-FileCopyrightText: 2016 - 2026 Leapsight
%% SPDX-License-Identifier: Apache-2.0
%% =============================================================================

-module(bondy_connect_connection).

-moduledoc """
The connection process: a `gen_statem` that owns the transport, drives the
session handshake through the pure `bondy_connect_protocol` layer, and
correlates requests/replies (Decision 3 — it speaks **records** to both the
transport and the protocol).

## Transport states (M1)

```
connecting --> handshaking --> establishing --> established
```

`connecting` opens the transport; `handshaking` runs the raw-socket transport
handshake (passive) then switches the socket to active and sends the HELLO the
protocol layer produces; `establishing` feeds inbound CHALLENGE/WELCOME/ABORT
records to the protocol layer until it reports `established` (or aborts);
`established` services CALLs and routes RESULT/ERROR back to callers.

M1 implements the **caller** role (CALL ⇒ RESULT/ERROR correlation) only;
register/subscribe/publish + handler workers arrive in Phase 4, reconnect/ping
in Phase 6. `process_flag(sensitive, true)` is set while authenticating and
cleared on `established` (Decision 12).
""".

-behaviour(gen_statem).

-include_lib("bondy_wamp/include/bondy_wamp.hrl").
-include("bondy_connect.hrl").

-record(data, {
    config          ::  map(),
    conn_sup        ::  pid(),
    transport_mod   ::  module(),
    transport       ::  term() | undefined,
    subprotocol     ::  {raw, binary, atom()},
    protocol        ::  bondy_connect_protocol:state(),
    session         ::  bondy_connect_session:t() | undefined,
    ready_waiters = []  ::  [gen_statem:from()],
    next_request_id = 1 ::  pos_integer(),
    pending = #{}   ::  #{pos_integer() => gen_statem:from()}
}).

-define(CONNECT_TIMEOUT, 5000).
-define(ESTABLISH_TIMEOUT, 10000).
-define(DEFAULT_CALL_TIMEOUT, 30000).

-export([start_link/2]).
-export([await_ready/2]).
-export([call/5]).
-export([status/1]).

-export([callback_mode/0]).
-export([init/1]).
-export([terminate/3]).
-export([format_status/1]).
%% state functions
-export([connecting/3]).
-export([handshaking/3]).
-export([establishing/3]).
-export([established/3]).



%% =============================================================================
%% API
%% =============================================================================



-spec start_link(Config :: map(), ConnSup :: pid()) ->
    {ok, pid()} | {error, term()}.
start_link(Config, ConnSup) ->
    gen_statem:start_link(?MODULE, {Config, ConnSup}, []).


-doc "Block until the session is established (or it fails).".
-spec await_ready(pid(), timeout()) -> ok | {error, term()}.
await_ready(Pid, Timeout) ->
    try
        gen_statem:call(Pid, await_ready, Timeout)
    catch
        exit:{timeout, _} -> {error, timeout};
        exit:{noproc, _} -> {error, not_connected};
        exit:{Reason, _} -> {error, Reason}
    end.


-doc "Issue a CALL and wait for the RESULT/ERROR.".
-spec call(pid(), uri(), list(), map(), map()) ->
    {ok, map()} | {error, term()}.
call(Pid, Uri, Args, KWArgs, Opts) ->
    Timeout = maps:get(timeout, Opts, ?DEFAULT_CALL_TIMEOUT),
    try
        gen_statem:call(Pid, {call, Uri, Args, KWArgs, Opts}, Timeout + 5000)
    catch
        exit:{timeout, _} -> {error, timeout};
        exit:{noproc, _} -> {error, not_connected};
        exit:{Reason, _} -> {error, Reason}
    end.


-doc "The public status of the connection.".
-spec status(pid()) -> connecting | establishing | established | down.
status(Pid) ->
    try
        gen_statem:call(Pid, status, 5000)
    catch
        _:_ -> down
    end.



%% =============================================================================
%% GEN_STATEM CALLBACKS
%% =============================================================================



callback_mode() ->
    [state_functions, state_enter].


init({Config, ConnSup}) ->
    process_flag(trap_exit, true),
    Mod = transport_mod(maps:get(transport, Config, tcp)),
    Sub = subprotocol(Config),
    {ok, Protocol} = bondy_connect_protocol:init(Config),
    Data = #data{
        config = Config,
        conn_sup = ConnSup,
        transport_mod = Mod,
        subprotocol = Sub,
        protocol = Protocol
    },
    {ok, connecting, Data}.


%% -----------------------------------------------------------------------------
%% connecting
%% -----------------------------------------------------------------------------

%% A 0-delay state_timeout kicks off the work (next_event is not allowed from a
%% state enter callback). transport:connect/2 carries its own connect timeout.
connecting(enter, _Old, Data) ->
    {keep_state, Data, [{state_timeout, 0, connect}]};

connecting(state_timeout, connect, Data) ->
    #data{transport_mod = Mod, config = Config} = Data,
    {Endpoint, Opts} = endpoint(Config),
    case Mod:connect(Endpoint, Opts) of
        {ok, T} ->
            {next_state, handshaking, Data#data{transport = T}};
        {error, Reason} ->
            fail({connect_error, Reason}, Data)
    end;

connecting(EventType, Event, Data) ->
    handle_common(EventType, Event, connecting, Data).


%% -----------------------------------------------------------------------------
%% handshaking (transport handshake + HELLO)
%% -----------------------------------------------------------------------------

handshaking(enter, _Old, Data) ->
    {keep_state, Data, [{state_timeout, 0, handshake}]};

handshaking(state_timeout, handshake, Data) ->
    #data{transport_mod = Mod, transport = T0, subprotocol = Sub} = Data,
    case Mod:handshake(Sub, T0) of
        {ok, _Negotiated, T1} ->
            ok = Mod:setopts([{active, once}], T1),
            {ok, Hello, P1} = bondy_connect_protocol:start(Data#data.protocol),
            Data1 = Data#data{transport = T1, protocol = P1},
            case Mod:send(Hello, T1) of
                ok ->
                    {next_state, establishing, Data1};
                {error, Reason} ->
                    fail({send_error, Reason}, Data1)
            end;
        {error, Reason} ->
            fail({handshake_error, Reason}, Data)
    end;

handshaking(EventType, Event, Data) ->
    handle_common(EventType, Event, handshaking, Data).


%% -----------------------------------------------------------------------------
%% establishing (WAMP session handshake)
%% -----------------------------------------------------------------------------

establishing(enter, _Old, Data) ->
    _ = process_flag(sensitive, true),
    {keep_state, Data, [{state_timeout, ?ESTABLISH_TIMEOUT, timeout}]};

establishing(state_timeout, timeout, Data) ->
    fail(establish_timeout, Data);

establishing(info, Info, Data) ->
    handle_socket(Info, establishing, Data);

establishing(EventType, Event, Data) ->
    handle_common(EventType, Event, establishing, Data).


%% -----------------------------------------------------------------------------
%% established
%% -----------------------------------------------------------------------------

established(enter, _Old, Data) ->
    _ = process_flag(sensitive, false),
    Data1 = reply_waiters(ok, Data),
    {keep_state, Data1};

established({call, From}, {call, Uri, Args, KWArgs, Opts}, Data) ->
    do_call(From, Uri, Args, KWArgs, Opts, Data);

established({call, From}, await_ready, Data) ->
    {keep_state, Data, [{reply, From, ok}]};

established(info, Info, Data) ->
    handle_socket(Info, established, Data);

established(EventType, Event, Data) ->
    handle_common(EventType, Event, established, Data).



%% =============================================================================
%% GEN_STATEM (terminate / status)
%% =============================================================================



terminate(_Reason, StateName, Data) ->
    _ = maybe_goodbye(StateName, Data),
    _ = close_transport(Data),
    _ = reply_pending({error, disconnected}, Data),
    _ = reply_waiters({error, disconnected}, Data),
    ok.


format_status(Status) ->
    maps:map(fun redact/2, Status).



%% =============================================================================
%% PRIVATE — common event handling
%% =============================================================================



%% @private
handle_common({call, From}, status, StateName, Data) ->
    {keep_state, Data, [{reply, From, public_status(StateName)}]};

handle_common({call, From}, await_ready, _StateName, Data) ->
    {keep_state, add_waiter(From, Data)};

handle_common({call, From}, {call, _, _, _, _}, _StateName, Data) ->
    %% A CALL before the session is established.
    {keep_state, Data, [{reply, From, {error, not_established}}]};

handle_common({call, From}, _Request, _StateName, Data) ->
    {keep_state, Data, [{reply, From, {error, badcall}}]};

handle_common(info, {tcp_closed, _}, _StateName, Data) ->
    fail(connection_closed, Data);

handle_common(info, {tcp_error, _, Reason}, _StateName, Data) ->
    fail({connection_error, Reason}, Data);

handle_common(_EventType, _Event, _StateName, Data) ->
    {keep_state, Data}.


%% @private Process inbound socket bytes into records and route them.
handle_socket({tcp, _Socket, Bin}, StateName, Data) ->
    #data{transport_mod = Mod, transport = T0} = Data,
    case Mod:handle_data(Bin, T0) of
        {ok, Records, T1} ->
            Data1 = Data#data{transport = T1},
            ok = Mod:setopts([{active, once}], T1),
            process_records(Records, StateName, Data1);
        {error, Reason, T1} ->
            fail({protocol_error, Reason}, Data#data{transport = T1})
    end;

handle_socket({tcp_closed, _}, _StateName, Data) ->
    fail(connection_closed, Data);

handle_socket({tcp_error, _, Reason}, _StateName, Data) ->
    fail({connection_error, Reason}, Data);

handle_socket(_Other, StateName, Data) ->
    {next_state, StateName, Data}.



%% =============================================================================
%% PRIVATE — record routing
%% =============================================================================



%% @private
process_records([], StateName, Data) ->
    {next_state, StateName, Data};

process_records([Record | Rest], StateName, Data) ->
    case route(Record, StateName, Data) of
        {continue, StateName1, Data1} ->
            process_records(Rest, StateName1, Data1);
        {stop, Reason, Data1} ->
            {stop, Reason, Data1}
    end.


%% @private
%% Control frames are keepalive concerns (Phase 6) — ignored in M1.
route({ping, _Payload}, StateName, Data) ->
    {continue, StateName, Data};
route({pong, _Payload}, StateName, Data) ->
    {continue, StateName, Data};
route(Record, established, Data) ->
    route_established(Record, Data);
route(Record, StateName, Data) ->
    route_handshake(Record, StateName, Data).


%% @private Drive the protocol layer during the handshake states.
route_handshake(Record, StateName, Data) ->
    case bondy_connect_protocol:handle_message(Record, Data#data.protocol) of
        {reply, OutMsgs, P1} ->
            Data1 = Data#data{protocol = P1},
            case send_all(OutMsgs, Data1) of
                ok -> {continue, StateName, Data1};
                {error, R} -> {stop, {shutdown, {send_error, R}}, Data1}
            end;
        {established, Session, P1} ->
            {continue, established, Data#data{protocol = P1, session = Session}};
        {stop, Reason, OutMsgs, P1} ->
            Data1 = Data#data{protocol = P1},
            _ = send_all(OutMsgs, Data1),
            {stop, {shutdown, Reason}, Data1};
        {passthrough, _Msg, P1} ->
            {continue, StateName, Data#data{protocol = P1}}
    end.


%% @private Route inbound records in the established state.
route_established(Record, Data) ->
    case bondy_connect_protocol:handle_message(Record, Data#data.protocol) of
        {passthrough, Msg, P1} ->
            route_app(Msg, Data#data{protocol = P1});
        {stop, Reason, OutMsgs, P1} ->
            Data1 = Data#data{protocol = P1},
            _ = send_all(OutMsgs, Data1),
            {stop, {shutdown, Reason}, Data1};
        {reply, OutMsgs, P1} ->
            Data1 = Data#data{protocol = P1},
            _ = send_all(OutMsgs, Data1),
            {continue, established, Data1}
    end.


%% @private Application-message routing (M1: CALL correlation only).
route_app(#result{request_id = ReqId} = R, Data) ->
    resolve_call(ReqId, {ok, result_payload(R)}, Data);

route_app(#error{request_type = ?CALL, request_id = ReqId} = E, Data) ->
    resolve_call(ReqId, {error, error_payload(E)}, Data);

route_app(_Other, Data) ->
    %% EVENT/INVOCATION/admin acks arrive in Phase 4.
    {continue, established, Data}.


%% @private
resolve_call(ReqId, Reply, #data{pending = Pending} = Data) ->
    case maps:take(ReqId, Pending) of
        {From, Pending1} ->
            gen_statem:reply(From, Reply),
            {continue, established, Data#data{pending = Pending1}};
        error ->
            {continue, established, Data}
    end.



%% =============================================================================
%% PRIVATE — CALL
%% =============================================================================



%% @private
do_call(From, Uri, Args, KWArgs, _Opts, Data) ->
    #data{next_request_id = ReqId, pending = Pending} = Data,
    Msg = bondy_wamp_message:call(ReqId, #{}, Uri, Args, KWArgs),
    case send_msg(Msg, Data) of
        ok ->
            Data1 = Data#data{
                next_request_id = next_id(ReqId),
                pending = maps:put(ReqId, From, Pending)
            },
            {keep_state, Data1};
        {error, Reason} ->
            {keep_state, Data, [{reply, From, {error, Reason}}]}
    end.


%% @private
result_payload(#result{args = Args, kwargs = KWArgs}) ->
    #{args => undefined_to(Args, []), kwargs => undefined_to(KWArgs, #{})}.


%% @private
error_payload(#error{error_uri = Uri, args = Args, kwargs = KWArgs}) ->
    #{uri => Uri, args => undefined_to(Args, []), kwargs => undefined_to(KWArgs, #{})}.



%% =============================================================================
%% PRIVATE — helpers
%% =============================================================================



%% @private
fail(Reason, Data) ->
    Data1 = reply_waiters({error, Reason}, Data),
    Data2 = reply_pending({error, Reason}, Data1),
    {stop, {shutdown, Reason}, Data2}.


%% @private
add_waiter(From, #data{ready_waiters = Ws} = Data) ->
    Data#data{ready_waiters = [From | Ws]}.


%% @private
reply_waiters(_Reply, #data{ready_waiters = []} = Data) ->
    Data;
reply_waiters(Reply, #data{ready_waiters = Ws} = Data) ->
    _ = [gen_statem:reply(From, Reply) || From <- Ws],
    Data#data{ready_waiters = []}.


%% @private
reply_pending(Reply, #data{pending = Pending} = Data) ->
    _ = [gen_statem:reply(From, Reply) || From <- maps:values(Pending)],
    Data#data{pending = #{}}.


%% @private
send_msg(Msg, #data{transport_mod = Mod, transport = T}) ->
    Mod:send(Msg, T).


%% @private
send_all([], _Data) ->
    ok;
send_all([Msg | Rest], Data) ->
    case send_msg(Msg, Data) of
        ok -> send_all(Rest, Data);
        {error, _} = Error -> Error
    end.


%% @private
maybe_goodbye(established, Data) ->
    Goodbye = bondy_wamp_message:goodbye(#{}, ?WAMP_CLOSE_NORMAL),
    send_msg(Goodbye, Data);
maybe_goodbye(_StateName, _Data) ->
    ok.


%% @private
close_transport(#data{transport = undefined}) ->
    ok;
close_transport(#data{transport_mod = Mod, transport = T}) ->
    catch Mod:close(T),
    ok.


%% @private
next_id(Id) when Id >= ?MAX_ID -> 1;
next_id(Id) -> Id + 1.


%% @private
undefined_to(undefined, Default) -> Default;
undefined_to(Value, _Default) -> Value.


%% @private
public_status(connecting) -> connecting;
public_status(handshaking) -> connecting;
public_status(establishing) -> establishing;
public_status(established) -> established;
public_status(_) -> down.


%% @private
transport_mod(tcp) -> bondy_connect_transport_tcp;
transport_mod(Other) -> error({unsupported_transport, Other}).


%% @private
subprotocol(Config) ->
    Serializers = maps:get(serializers, Config, [json]),
    Enc = hd(Serializers),
    {raw, binary, Enc}.


%% @private
endpoint(Config) ->
    Endpoint = maps:get(endpoint, Config),
    Opts = #{
        connect_timeout => ?CONNECT_TIMEOUT,
        max_message_length => maps:get(max_message_length, Config, 16#1000000)
    },
    {Endpoint, Opts}.


%% @private Scrub the protocol's auth material from status/crash dumps.
redact(data, #data{protocol = P} = Data) ->
    Data#data{protocol = bondy_connect_protocol:format_status(P)};
redact(_Key, Value) ->
    Value.
