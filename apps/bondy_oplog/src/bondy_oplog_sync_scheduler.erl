%% =============================================================================
%% SPDX-FileCopyrightText: 2023 - 2026 Leapsight
%% SPDX-License-Identifier: Apache-2.0
%% =============================================================================

-module(bondy_oplog_sync_scheduler).

-behaviour(gen_server).

-include_lib("kernel/include/logger.hrl").
-include("bondy_doc.hrl").
-include("bondy_oplog.hrl").

-moduledoc #{format => "text/markdown"}.
?MODULEDOC("""
Default sync scheduler.

Periodic `gen_server` that, on each tick, asks
`bondy_oplog:list_instances/0` for the running instances and
— for each — invokes the configured peer source and dispatches a sync
session to each peer.

## Configuration

Read from app env at boot:

| Key                   | Default | Meaning |
|---|---|---|
| `sync_scheduler`      | `true`  | Enable / disable the scheduler. |
| `sync_interval_ms`    | `500`   | Time between ticks. |
| `peer_source`         | `bondy_oplog_peer_source_static` | Default behaviour module. |
| `peer_source_opts`    | `#{}`   | Default opts passed to `peers_for/2`. |
| `sync_dispatch`       | `undefined` | Optional `fun((InstanceId, [PeerId]) -> any())`; defaults to the lifecycle-aware dispatch below. |
| `sync_session_opts`   | `#{}`   | Opts (`transport`, `transport_opts`) threaded into every session the default dispatch starts. `#{}` ⇒ `bondy_oplog_transport_inline`; a clustered node sets the Partisan transport + AE channel here. |
| `bootstrap_peer_strategy` | `first` | One of `first \| random \| round_robin`. Selects which peer a `pre_bootstrap` instance bootstraps from. |
| `max_inflight_bootstraps` | `4`     | Global cap on parallel bootstrap sessions. Dispatches above the cap are skipped (instance stays `pre_bootstrap` → retried next tick). |
| `bootstrap_retry_base_ms` | `500`   | Initial backoff after a failed bootstrap session. Doubles per consecutive failure up to `bootstrap_retry_max_ms`. |
| `bootstrap_retry_max_ms`  | `30000` | Upper bound on the exponential backoff window. |
| `bootstrap_retry_jitter`  | `true`  | Multiplies the computed wait by `uniform(0.5, 1.5)` to spread retries across instances. |
| `live_sync_adaptive`  | `true`  | Throttle live (post-bootstrap) syncs adaptively. `false` ⇒ every live instance syncs every peer every tick (historical). |
| `live_sync_base_ms`   | `sync_interval_ms` | Poll cadence for a live shard whose local root is actively moving. |
| `live_sync_max_ms`    | `5000` | Upper bound on the live-sync poll window once a shard goes quiescent. Instances that back the auth fence are exempt — they are never throttled. |

## Default dispatch — lifecycle-aware

For each tick, the default dispatch inspects the instance's bootstrap
lifecycle (`bondy_oplog_instance:lifecycle_state/1`) and routes
accordingly:

- **`pre_bootstrap`** — pick a single peer (via the configured
  `bootstrap_peer_strategy`, see below) and dispatch one bootstrap
  session via
  `bondy_oplog_sync_session:start_bootstrap_catalogue/3` (catalogue
  mode, `crdt_module = undefined`) or
  `bondy_oplog_sync_session:start_bootstrap/3` (single-CRDT mode).
  Single-peer to avoid duplicate snapshot transfers — bootstrap is
  expensive (full projection ship) and multi-peer would not improve
  correctness.
- **`live`** — fan out one async pull-direction sync session per peer
  via `bondy_oplog_sync_session:start/3`, gated by the adaptive
  live-sync throttle (see below).

## Bootstrap peer strategy

- **`first`** (default) — always pick `hd(Peers)`. Deterministic,
  cheap, brittle when that peer is overloaded or unreachable.
- **`random`** — uniform random pick. Best for thundering-herd
  avoidance when many `pre_bootstrap` instances start simultaneously.
- **`round_robin`** — per-instance index advanced on every dispatch
  decision (held in a small named ETS table). Even distribution
  across peers for a single instance that retries after failure.

The strategy is read per dispatch from app env; runtime changes via
`set_bootstrap_peer_strategy/1` take effect on the next tick.

## Bootstrap session cap

The number of bootstrap sessions in flight at any one time is capped
by `max_inflight_bootstraps` (default `4`). When the scheduler would
dispatch a session that crosses the cap, it skips silently — the
instance remains `pre_bootstrap` and the next tick retries naturally.
Operators get visibility via the
`[bondy_oplog, sync_scheduler, bootstrap_capped]` telemetry event.

The scheduler tracks in-flight sessions via a named ETS table keyed
by session Pid; entries are removed when the session process exits
(monitored via `erlang:monitor/2`). On a scheduler restart the table
is recreated empty — already-running session processes are then
untracked, which is correct because the scheduler did not spawn them
in its current incarnation.

## Bootstrap retry backoff

After a session exits non-normally (the session process raised
`{bootstrap_failed, _}` or `{bootstrap_catalogue_failed, _}` from
`bondy_oplog_sync_session`), the scheduler records a per-instance
failure count and a next-retry timestamp. Subsequent ticks skip
that instance until the timestamp has passed. The wait is
`base * 2^(count-1)` capped at `bootstrap_retry_max_ms`. With
jitter enabled (default), the final wait is multiplied by a
uniform random factor in `[0.5, 1.5]` to spread retries across
instances that failed in the same window.

A successful session exit (reason `normal`) clears the backoff
entry; the next disruption starts fresh from `base`.

`[bondy_oplog, sync_scheduler, bootstrap_backoff_deferred]`
telemetry fires on every cap-skip with `wait_ms` and `fail_count`
measurements — useful as an alert signal when an instance keeps
failing to bootstrap.

A consumer can override the routing entirely by setting
`sync_dispatch` to a custom fun. Exceptions raised by a custom
dispatch are caught and logged. Custom dispatchers bypass the cap
and the backoff; consumers wanting either with a custom strategy
should call back into
`bondy_oplog_sync_scheduler:default_dispatch/2` after their
selection.

## Live-sync throttle

A `live` instance only re-syncs to discover divergence; once its data
has converged it has nothing to pull, yet the historical dispatch still
spawned a session against every peer on every tick. Across many shards
this is a constant, pointless load — the dominant steady-state cost of
running AAE.

The throttle (default on; disable with `live_sync_adaptive = false`)
makes the live-sync cadence adaptive per instance, using the instance's
in-memory MST root as a free change detector:

- While the local root is moving — a local write, data arriving via
  normal replication, or a prior sync catching up — the instance
  dispatches every tick (cadence `live_sync_base_ms`, default the tick
  interval). This is exactly the historical behaviour during activity.
- Once the root goes quiescent, the poll window doubles each round up to
  `live_sync_max_ms` (default `5s`). The instance still polls at the
  capped cadence so divergence is discovered within at most one window.
- The first such poll that pulls anything moves the local root, which
  resets the window to the base interval, so recovery is fast once it
  starts.

`bondy_db` apply is pull-only (no eager push), so the capped cadence is
also the steady-state cross-node convergence latency for a quiescent
shard — keep `live_sync_max_ms` below the convergence SLA you need. The
default `5s` trades a 10× churn cut for at most `5s` of convergence lag
on idle shards.

**Fence exemption.** An instance that carries AE freshness targets backs
the read-side authentication fence: its successful sync round re-bumps
those targets (`bondy_oplog_sync_session:maybe_record/4`), even when the
shard is converged, and the fence refuses authentication once a target
goes unconfirmed past `auth_max_lag`. Such an instance is **never**
throttled — backing it off would starve the bump and trip the fence on
inactivity. Only instances with no AE targets (for which the bump is a
no-op) are throttled. This keeps the throttle a pure performance change
with no effect on auth availability.

The throttle keys solely on the local root, so it never trades away
freshness for data this node already has; it only stretches the
*detection* latency for data it is missing, bounded by the cap. The
`pre_bootstrap` path is untouched — it has its own peer strategy and
failure backoff. Telemetry: `[bondy_oplog, sync_scheduler,
live_sync_poll]` on each backed-off poll (with `window_ms`) and
`[bondy_oplog, sync_scheduler, live_sync_skipped]` on each tick a
converged instance is skipped.
""").

-record(state, {
    enabled :: boolean(),
    interval_ms :: non_neg_integer(),
    peer_source :: module(),
    peer_source_opts :: map(),
    dispatch :: undefined | fun((instance_id(), [peer_id()]) -> any()),
    tick_ref :: undefined | reference()
}).

%% Lifecycle
-export([start_link/0]).
-export([start_link/1]).
-export([child_spec/1]).

%% Control
-export([trigger/0]).
-export([set_dispatch/1]).
-export([set_peer_source/2]).
-export([set_interval_ms/1]).
-export([set_bootstrap_peer_strategy/1]).
-export([set_max_inflight_bootstraps/1]).
-export([set_bootstrap_retry_base_ms/1]).
-export([set_bootstrap_retry_max_ms/1]).
-export([set_bootstrap_retry_jitter/1]).
-export([set_live_sync_adaptive/1]).
-export([set_live_sync_base_ms/1]).
-export([set_live_sync_max_ms/1]).
-export([info/0]).
-export([default_dispatch/2]).

-define(RR_TAB, bondy_oplog_sync_scheduler_rr).
-define(INFLIGHT_TAB, bondy_oplog_sync_scheduler_inflight).
-define(BACKOFF_TAB, bondy_oplog_sync_scheduler_backoff).
-define(LIVE_BACKOFF_TAB, bondy_oplog_sync_scheduler_live_backoff).

%% gen_server callbacks
-export([init/1]).
-export([handle_call/3]).
-export([handle_cast/2]).
-export([handle_info/2]).
-export([terminate/2]).

-ifdef(TEST).
%% Exposed for deterministic unit testing of the live-sync backoff
%% state machine, decoupled from the clock and instance root reads.
-export([live_decide/5]).
-endif.

%% =============================================================================
%% LIFECYCLE
%% =============================================================================

-spec start_link() -> {ok, pid()} | {error, term()}.

start_link() ->
    start_link(#{}).

-spec start_link(map()) -> {ok, pid()} | {error, term()}.

start_link(Opts) when is_map(Opts) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, Opts, []).

-spec child_spec(map()) -> supervisor:child_spec().

child_spec(Opts) ->
    #{
        id => ?MODULE,
        start => {?MODULE, start_link, [Opts]},
        restart => permanent,
        shutdown => 5000,
        type => worker,
        modules => [?MODULE]
    }.

%% =============================================================================
%% CONTROL
%% =============================================================================

?DOC("""
Forces a tick now. Useful for tests and operational triggers.
""").
-spec trigger() -> ok.

trigger() ->
    gen_server:cast(?MODULE, tick).

?DOC("""
Replaces the dispatch callback. Pass `undefined` to disable dispatch
(ticks still run; nothing is invoked). Useful for runtime
reconfiguration and tests.
""").
-spec set_dispatch(undefined | fun((instance_id(), [peer_id()]) -> any())) ->
    ok.

set_dispatch(Fun) when is_function(Fun, 2); Fun =:= undefined ->
    gen_server:call(?MODULE, {set_dispatch, Fun}).

?DOC("""
Replaces the peer source module and options at runtime.
""").
-spec set_peer_source(module(), map()) -> ok.

set_peer_source(Mod, Opts) when is_atom(Mod), is_map(Opts) ->
    gen_server:call(?MODULE, {set_peer_source, Mod, Opts}).

?DOC("""
Sets the periodic-tick interval (in milliseconds) at runtime. `0`
disables periodic ticks entirely; explicit `trigger/0` still works.
The currently-scheduled timer is cancelled and a new one armed with
the new interval (if non-zero).

Useful for operator tuning and for tests that need to suppress
periodic firing while asserting on explicit triggers.
""").
-spec set_interval_ms(non_neg_integer()) -> ok.

set_interval_ms(Ms) when is_integer(Ms), Ms >= 0 ->
    gen_server:call(?MODULE, {set_interval_ms, Ms}).

?DOC("""
Sets the bootstrap-peer selection strategy. One of `first`, `random`,
or `round_robin`. Takes effect on the next tick; no scheduler restart
needed.

Writes through to app env (`bootstrap_peer_strategy`) so the choice
also survives a scheduler restart within the same VM lifetime.
""").
-spec set_bootstrap_peer_strategy(first | random | round_robin) -> ok.

set_bootstrap_peer_strategy(S) when
    S =:= first; S =:= random; S =:= round_robin
->
    application:set_env(bondy_oplog, bootstrap_peer_strategy, S),
    ok.

?DOC("""
Sets the global cap on parallel bootstrap sessions. `0` disables
dispatch (operator escape hatch). Takes effect on the next tick.

Writes through to app env (`max_inflight_bootstraps`).
""").
-spec set_max_inflight_bootstraps(non_neg_integer()) -> ok.

set_max_inflight_bootstraps(N) when is_integer(N), N >= 0 ->
    application:set_env(bondy_oplog, max_inflight_bootstraps, N),
    ok.

?DOC("""
Sets the initial bootstrap-retry backoff in milliseconds.
Doubled on each consecutive failure (clamped at
`bootstrap_retry_max_ms`).
""").
-spec set_bootstrap_retry_base_ms(non_neg_integer()) -> ok.

set_bootstrap_retry_base_ms(Ms) when is_integer(Ms), Ms >= 0 ->
    application:set_env(bondy_oplog, bootstrap_retry_base_ms, Ms),
    ok.

?DOC("""
Sets the upper bound on the exponential bootstrap-retry backoff.
""").
-spec set_bootstrap_retry_max_ms(non_neg_integer()) -> ok.

set_bootstrap_retry_max_ms(Ms) when is_integer(Ms), Ms >= 0 ->
    application:set_env(bondy_oplog, bootstrap_retry_max_ms, Ms),
    ok.

?DOC("""
Enables or disables jitter on the bootstrap-retry backoff. With
jitter the actual wait is `wait * uniform(0.5, 1.5)`.
""").
-spec set_bootstrap_retry_jitter(boolean()) -> ok.

set_bootstrap_retry_jitter(B) when is_boolean(B) ->
    application:set_env(bondy_oplog, bootstrap_retry_jitter, B),
    ok.

?DOC("""
Enables or disables the adaptive live-sync throttle. When `false`,
every live instance dispatches a sync against every peer on every tick
(the historical behaviour). When `true` (default), a converged shard
backs off (see `set_live_sync_max_ms/1`) until its local root moves.
""").
-spec set_live_sync_adaptive(boolean()) -> ok.

set_live_sync_adaptive(B) when is_boolean(B) ->
    application:set_env(bondy_oplog, live_sync_adaptive, B),
    ok.

?DOC("""
Sets the base live-sync poll interval in milliseconds — the cadence at
which a live shard re-syncs while its local root is actively moving.
Defaults to `sync_interval_ms` (the tick interval). The effective
cadence is rounded up to a whole number of ticks.
""").
-spec set_live_sync_base_ms(non_neg_integer()) -> ok.

set_live_sync_base_ms(Ms) when is_integer(Ms), Ms >= 0 ->
    application:set_env(bondy_oplog, live_sync_base_ms, Ms),
    ok.

?DOC("""
Sets the upper bound on the adaptive live-sync poll interval. Once a
shard's local root goes quiescent the poll window doubles each round up
to this cap; bounding how long a divergence can go undetected — and,
since `bondy_db` apply propagates pull-only, the steady-state cross-node
convergence latency for a quiescent shard. Defaults to `5000`.
Instances that back the auth freshness fence are never throttled
regardless of this value.
""").
-spec set_live_sync_max_ms(non_neg_integer()) -> ok.

set_live_sync_max_ms(Ms) when is_integer(Ms), Ms >= 0 ->
    application:set_env(bondy_oplog, live_sync_max_ms, Ms),
    ok.

?DOC("""
Returns the scheduler's current configuration. Cheap.
""").
-spec info() -> map().

info() ->
    gen_server:call(?MODULE, info).

%% =============================================================================
%% gen_server CALLBACKS
%% =============================================================================

init(Opts) ->
    process_flag(trap_exit, true),
    _ = ensure_rr_table(),
    _ = ensure_inflight_table(),
    _ = ensure_backoff_table(),
    _ = ensure_live_backoff_table(),
    Dispatch =
        case maps:find(dispatch, Opts) of
            {ok, V} ->
                V;
            error ->
                case application:get_env(bondy_oplog, sync_dispatch) of
                    {ok, EnvFun} -> EnvFun;
                    undefined -> fun default_dispatch/2
                end
        end,
    State = #state{
        enabled = maps:get(
            enabled,
            Opts,
            application:get_env(
                bondy_oplog,
                sync_scheduler,
                true
            )
        ),
        interval_ms = maps:get(
            interval_ms,
            Opts,
            application:get_env(
                bondy_oplog,
                sync_interval_ms,
                500
            )
        ),
        peer_source = maps:get(
            peer_source,
            Opts,
            application:get_env(
                bondy_oplog,
                peer_source,
                bondy_oplog_peer_source_static
            )
        ),
        peer_source_opts = maps:get(
            peer_source_opts,
            Opts,
            application:get_env(
                bondy_oplog, peer_source_opts, #{}
            )
        ),
        dispatch = Dispatch
    },
    {ok, schedule_tick(State)}.

handle_call(info, _From, State) ->
    Reply = #{
        enabled => State#state.enabled,
        interval_ms => State#state.interval_ms,
        peer_source => State#state.peer_source,
        peer_source_opts => State#state.peer_source_opts,
        dispatch_set => State#state.dispatch =/= undefined,
        bootstrap_peer_strategy =>
            application:get_env(bondy_oplog, bootstrap_peer_strategy, first),
        max_inflight_bootstraps =>
            application:get_env(bondy_oplog, max_inflight_bootstraps, 4),
        current_inflight_bootstraps => inflight_count(),
        bootstrap_retry_base_ms =>
            application:get_env(bondy_oplog, bootstrap_retry_base_ms, 500),
        bootstrap_retry_max_ms =>
            application:get_env(bondy_oplog, bootstrap_retry_max_ms, 30000),
        bootstrap_retry_jitter =>
            application:get_env(bondy_oplog, bootstrap_retry_jitter, true),
        live_sync_adaptive => live_adaptive_enabled(),
        live_sync_base_ms => live_sync_base_ms(),
        live_sync_max_ms => live_sync_max_ms()
    },
    {reply, Reply, State};
handle_call({set_dispatch, Fun}, _From, State) ->
    {reply, ok, State#state{dispatch = Fun}};
handle_call({set_peer_source, Mod, Opts}, _From, State) ->
    {reply, ok, State#state{peer_source = Mod, peer_source_opts = Opts}};
handle_call({set_interval_ms, Ms}, _From, State0) ->
    State1 = cancel_pending_tick(State0),
    State2 = schedule_tick(State1#state{interval_ms = Ms}),
    {reply, ok, State2};
handle_call(_Req, _From, State) ->
    {reply, {error, badcall}, State}.

handle_cast(tick, State) ->
    {noreply, run_tick(State)};
handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(tick, State) ->
    {noreply, schedule_tick(run_tick(State))};
handle_info({'DOWN', _MonRef, process, Pid, Reason}, State) ->
    case ets:lookup(?INFLIGHT_TAB, Pid) of
        [{Pid, InstanceId}] ->
            ets:delete(?INFLIGHT_TAB, Pid),
            update_backoff(InstanceId, Reason),
            telemetry:execute(
                [bondy_oplog, sync_scheduler, bootstrap_session, ended],
                #{remaining => inflight_count()},
                #{
                    instance_id => InstanceId,
                    pid => Pid,
                    reason => Reason
                }
            );
        [] ->
            %% DOWN from something we didn't track — ignore.
            ok
    end,
    {noreply, State};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

%% =============================================================================
%% PRIVATE
%% =============================================================================

%% @private
run_tick(#state{enabled = false} = State) ->
    State;
run_tick(#state{} = State) ->
    Instances = safe_list_instances(),
    lists:foreach(
        fun(InstanceId) -> dispatch_for(InstanceId, State) end,
        Instances
    ),
    telemetry:execute(
        [bondy_oplog, scheduler, sync, tick],
        #{instances => length(Instances)},
        #{}
    ),
    State.

%% @private
dispatch_for(InstanceId, #state{} = State) ->
    Peers = (State#state.peer_source):peers_for(
        InstanceId, State#state.peer_source_opts
    ),
    case State#state.dispatch of
        undefined ->
            ok;
        Fun when is_function(Fun, 2) ->
            try
                Fun(InstanceId, Peers)
            catch
                K:V:S ->
                    ?LOG_WARNING(#{
                        description => "sync dispatch raised",
                        instance => InstanceId,
                        class => K,
                        reason => V,
                        stacktrace => S
                    }),
                    ok
            end
    end.

%% @private
%% `list_instances/0` calls `info/1` on each running worker — if a
%% worker is mid-restart that call may briefly fail. Soft-fail to an
%% empty list rather than crash the scheduler.
safe_list_instances() ->
    try
        bondy_oplog:list_instances()
    catch
        _:_ -> []
    end.

%% @private
%% Cancels the in-flight `tick` timer (if any) and flushes any pending
%% `tick` message that may already be in the gen_server's mailbox.
%% Used by `set_interval_ms/1` so the new interval starts cleanly
%% without a leftover tick at the old cadence.
cancel_pending_tick(#state{tick_ref = undefined} = State) ->
    State;
cancel_pending_tick(#state{tick_ref = Ref} = State) ->
    _ = erlang:cancel_timer(Ref, [{async, false}, {info, false}]),
    receive
        tick -> ok
    after 0 -> ok
    end,
    State#state{tick_ref = undefined}.

%% @private
schedule_tick(#state{enabled = false} = State) ->
    State#state{tick_ref = undefined};
schedule_tick(#state{interval_ms = 0} = State) ->
    State#state{tick_ref = undefined};
schedule_tick(#state{interval_ms = Ms} = State) ->
    Ref = erlang:send_after(Ms, self(), tick),
    State#state{tick_ref = Ref}.

%% @private
%% Lifecycle-aware dispatch. Pre_bootstrap instances dispatch a single
%% bootstrap session against a peer chosen by the configured
%% `bootstrap_peer_strategy` (see module doc); live instances fan out
%% per-peer pull-direction sync sessions. Errors from the spawn are
%% absorbed by the session process and reported via peer_state / logs;
%% the scheduler does not wait for completion.
default_dispatch(InstanceId, []) ->
    %% Empty peer list this round. `maybe_bump_ae_isolated/1` certifies a
    %% genuine single-node deployment unconditionally (no peer to lag), and
    %% otherwise applies the `oplog.aae.fence.on_isolation` policy: `refuse`
    %% leaves freshness to decay (the fence refuses); `proceed`/`quorum` may
    %% certify so the node keeps authenticating.
    bondy_oplog_sync_session:maybe_bump_ae_isolated(InstanceId);
default_dispatch(InstanceId, Peers) ->
    case bondy_oplog_instance:lifecycle_state(InstanceId) of
        pre_bootstrap ->
            maybe_dispatch_bootstrap(InstanceId, Peers);
        live ->
            maybe_dispatch_live(InstanceId, Peers);
        undefined ->
            %% Instance is starting up or unknown — no-op for this
            %% tick; the next tick will see the lifecycle once
            %% `init/1` publishes the handle.
            ok
    end.

%% @private
%% Two gates before dispatch:
%%   1. Per-instance backoff (set on previous session failure).
%%   2. Global in-flight cap.
%% Backoff is checked first because an instance in backoff should
%% not count against the cap — other instances still get a slot.
maybe_dispatch_bootstrap(InstanceId, Peers) ->
    case backoff_remaining(InstanceId) of
        {Wait, FailCount} when Wait > 0 ->
            telemetry:execute(
                [bondy_oplog, sync_scheduler, bootstrap_backoff_deferred],
                #{wait_ms => Wait, fail_count => FailCount},
                #{instance_id => InstanceId}
            ),
            ok;
        {_, _} ->
            maybe_dispatch_bootstrap_cap_check(InstanceId, Peers)
    end.

%% @private
maybe_dispatch_bootstrap_cap_check(InstanceId, Peers) ->
    Cap = application:get_env(bondy_oplog, max_inflight_bootstraps, 4),
    Current = inflight_count(),
    case Current >= Cap of
        true ->
            telemetry:execute(
                [bondy_oplog, sync_scheduler, bootstrap_capped],
                #{current => Current, cap => Cap},
                #{instance_id => InstanceId}
            ),
            ok;
        false ->
            Strategy = application:get_env(
                bondy_oplog, bootstrap_peer_strategy, first
            ),
            Peer = pick_bootstrap_peer(Strategy, InstanceId, Peers),
            dispatch_bootstrap(InstanceId, Peer, Strategy)
    end.

%% @private
dispatch_bootstrap(InstanceId, Peer, Strategy) ->
    Mode =
        case bondy_oplog_instance:crdt_module(InstanceId) of
            undefined -> catalogue;
            _ -> single_crdt
        end,
    telemetry:execute(
        [bondy_oplog, sync_scheduler, dispatch_bootstrap],
        #{count => 1},
        #{
            instance_id => InstanceId,
            peer => Peer,
            mode => Mode,
            strategy => Strategy
        }
    ),
    SessionOpts = session_opts(),
    {ok, Pid} =
        case Mode of
            catalogue ->
                bondy_oplog_sync_session:start_bootstrap_catalogue(
                    InstanceId, Peer, SessionOpts
                );
            single_crdt ->
                bondy_oplog_sync_session:start_bootstrap(
                    InstanceId, Peer, SessionOpts
                )
        end,
    track_inflight(Pid, InstanceId),
    ok.

%% @private
%% Inserts the spawned session pid into the in-flight table and
%% monitors it so DOWN messages reach the scheduler gen_server's
%% mailbox. Safe to call from outside the gen_server (e.g. tests):
%% in that case the monitor is owned by the caller and the DOWN goes
%% to the caller's mailbox instead. Production calls happen inside
%% the gen_server's `run_tick/1` so the DOWN reaches the scheduler.
track_inflight(Pid, InstanceId) ->
    _ = ensure_inflight_table(),
    _ = erlang:monitor(process, Pid),
    ets:insert(?INFLIGHT_TAB, {Pid, InstanceId}),
    telemetry:execute(
        [bondy_oplog, sync_scheduler, bootstrap_session, started],
        #{current => inflight_count()},
        #{instance_id => InstanceId, pid => Pid}
    ),
    ok.

%% @private
%% Strategy-driven peer selection for pre_bootstrap dispatch. The
%% round-robin counter is held in a small named ETS table created in
%% `init/1`; on a cold call (e.g. unit-testing the function in
%% isolation) the table is created lazily.
pick_bootstrap_peer(first, _InstanceId, [P | _]) ->
    P;
pick_bootstrap_peer(random, _InstanceId, Peers) ->
    lists:nth(rand:uniform(length(Peers)), Peers);
pick_bootstrap_peer(round_robin, InstanceId, Peers) ->
    _ = ensure_rr_table(),
    N = length(Peers),
    %% update_counter creates the entry on first hit. Returns the new
    %% value, so first call yields 1 → nth(1, Peers).
    Idx = ets:update_counter(
        ?RR_TAB, InstanceId, {2, 1}, {InstanceId, 0}
    ),
    lists:nth(((Idx - 1) rem N) + 1, Peers);
pick_bootstrap_peer(_UnknownStrategy, _InstanceId, [P | _]) ->
    P.

%% @private
ensure_rr_table() ->
    case ets:info(?RR_TAB) of
        undefined ->
            try
                ets:new(?RR_TAB, [
                    named_table,
                    set,
                    public,
                    {read_concurrency, true},
                    {write_concurrency, true}
                ])
            catch
                error:badarg -> ?RR_TAB
            end;
        _ ->
            ?RR_TAB
    end.

%% @private
ensure_inflight_table() ->
    case ets:info(?INFLIGHT_TAB) of
        undefined ->
            try
                ets:new(?INFLIGHT_TAB, [
                    named_table,
                    set,
                    public,
                    {read_concurrency, true},
                    {write_concurrency, true}
                ])
            catch
                error:badarg -> ?INFLIGHT_TAB
            end;
        _ ->
            ?INFLIGHT_TAB
    end.

%% @private
inflight_count() ->
    case ets:info(?INFLIGHT_TAB, size) of
        undefined -> 0;
        N -> N
    end.

%% @private
ensure_backoff_table() ->
    case ets:info(?BACKOFF_TAB) of
        undefined ->
            try
                ets:new(?BACKOFF_TAB, [
                    named_table,
                    set,
                    public,
                    {read_concurrency, true},
                    {write_concurrency, true}
                ])
            catch
                error:badarg -> ?BACKOFF_TAB
            end;
        _ ->
            ?BACKOFF_TAB
    end.

%% @private
%% On a successful (normal) session exit, clear the entry — the next
%% disruption starts fresh from `base`. On any other exit reason,
%% bump the per-instance failure count and write a new next-retry
%% timestamp. Called from `handle_info({'DOWN', ...})`.
update_backoff(InstanceId, normal) ->
    _ = ensure_backoff_table(),
    ets:delete(?BACKOFF_TAB, InstanceId),
    ok;
update_backoff(InstanceId, _Reason) ->
    _ = ensure_backoff_table(),
    Count =
        case ets:lookup(?BACKOFF_TAB, InstanceId) of
            [{InstanceId, _NextMs, N}] -> N + 1;
            [] -> 1
        end,
    Wait = backoff_wait_ms(Count),
    NextMs = now_ms() + Wait,
    ets:insert(?BACKOFF_TAB, {InstanceId, NextMs, Count}),
    telemetry:execute(
        [bondy_oplog, sync_scheduler, bootstrap_retry_scheduled],
        #{wait_ms => Wait, fail_count => Count},
        #{instance_id => InstanceId}
    ),
    ok.

%% @private
%% Returns the wait in ms for failure-count N. Exponential with
%% optional uniform jitter in [0.5, 1.5].
backoff_wait_ms(N) when N >= 1 ->
    Base = application:get_env(bondy_oplog, bootstrap_retry_base_ms, 500),
    Max = application:get_env(bondy_oplog, bootstrap_retry_max_ms, 30000),
    %% 2^31 caps the exponent to avoid overflow on adversarial N.
    Exp = min(N - 1, 30),
    Raw = min(Base bsl Exp, Max),
    case application:get_env(bondy_oplog, bootstrap_retry_jitter, true) of
        true ->
            %% uniform float in [0.5, 1.5].
            Factor = 0.5 + rand:uniform(),
            trunc(Raw * Factor);
        false ->
            Raw
    end.

%% @private
now_ms() ->
    erlang:monotonic_time(millisecond).

%% @private
%% Returns 0 if the instance is not under backoff (or the timer has
%% already expired); otherwise the milliseconds until it can retry
%% plus the current fail count.
backoff_remaining(InstanceId) ->
    _ = ensure_backoff_table(),
    case ets:lookup(?BACKOFF_TAB, InstanceId) of
        [] ->
            {0, 0};
        [{InstanceId, NextMs, Count}] ->
            Remaining = NextMs - now_ms(),
            case Remaining > 0 of
                true -> {Remaining, Count};
                false -> {0, Count}
            end
    end.

%% @private
%% Adaptive live-sync throttle. A converged shard re-syncs only to
%% discover peer-side divergence; once its local root stops moving,
%% polling every peer every tick is pure churn. We dispatch on every
%% tick while the local root is changing (active local write, normal
%% replication, or catch-up pulling data in), and otherwise back the
%% poll cadence off geometrically up to `live_sync_max_ms`. Any local
%% root change — including data pulled in by a prior sync — resets the
%% window to the base interval, so missed replication heals within at
%% most one cap-length window and active divergence stays tick-fast.
%% Bootstrap is unaffected (different lifecycle, its own backoff).
maybe_dispatch_live(InstanceId, Peers) ->
    case live_adaptive_enabled() andalso not backs_fence(InstanceId) of
        false ->
            %% Either throttling is off, or this instance backs the auth
            %% freshness fence — its successful sync round re-bumps the
            %% fence's AE targets (`bondy_oplog_sync_session:maybe_record/4`),
            %% including for a converged shard, and the fence refuses
            %% authentication once a target goes unconfirmed past
            %% `auth_max_lag`. Such an instance MUST sync every tick;
            %% backing it off would trip the fence on inactivity. Dispatch
            %% unconditionally.
            dispatch_live_sync(InstanceId, Peers);
        true ->
            case live_should_dispatch(InstanceId) of
                true -> dispatch_live_sync(InstanceId, Peers);
                false -> ok
            end
    end.

%% @private
%% An instance "backs the fence" when it carries AE freshness targets
%% (set once at init via `bondy_oplog_registry:set_ae_targets/2`): a
%% successful AE round freshens those targets, and the read-side auth
%% fence depends on that bump landing within `auth_max_lag`. Throttling
%% such an instance would starve the bump and trip the fence, so it is
%% never throttled. An instance with no targets cannot affect the fence
%% (the bump is a strict no-op there), so throttling it is safe. On any
%% lookup error we fail safe — treat it as fence-backing (do not
%% throttle).
backs_fence(InstanceId) ->
    case catch bondy_oplog_registry:ae_targets(InstanceId) of
        L when is_list(L) -> L =/= [];
        _ -> true
    end.

%% @private
%% Decides whether this tick dispatches a live sync for the instance and
%% records the decision in `?LIVE_BACKOFF_TAB`:
%%     {InstanceId, LastRoot, NextDueMs, WindowMs}
%%   - First sight, or the local root changed since last sight → dispatch
%%     now and reset the window to the base interval (activity).
%%   - Root unchanged and the window has not elapsed → skip.
%%   - Root unchanged and the window has elapsed → dispatch a poll (to
%%     detect peer-side divergence) and grow the window (×2, capped).
live_should_dispatch(InstanceId) ->
    _ = ensure_live_backoff_table(),
    live_decide(
        InstanceId,
        current_root(InstanceId),
        now_ms(),
        live_sync_base_ms(),
        live_sync_max_ms()
    ).

%% @private
%% The live-sync backoff state machine, factored out of clock and
%% root-reading so it is deterministically unit-testable. Reads/writes
%% `?LIVE_BACKOFF_TAB` keyed by instance:
%%     {InstanceId, LastRoot, NextDueMs, WindowMs}
live_decide(InstanceId, Root, Now, Base, Max) ->
    case ets:lookup(?LIVE_BACKOFF_TAB, InstanceId) of
        [] ->
            ets:insert(
                ?LIVE_BACKOFF_TAB, {InstanceId, Root, Now + Base, Base}
            ),
            true;
        [{InstanceId, LastRoot, _Due, _Window}] when Root =/= LastRoot ->
            %% Activity → reset to the fast cadence.
            ets:insert(
                ?LIVE_BACKOFF_TAB, {InstanceId, Root, Now + Base, Base}
            ),
            true;
        [{InstanceId, _Root, Due, Window}] when Now >= Due ->
            %% Quiescent, poll window elapsed → poll + grow the window.
            NextWindow = min(Window * 2, max(Base, Max)),
            ets:insert(
                ?LIVE_BACKOFF_TAB,
                {InstanceId, Root, Now + NextWindow, NextWindow}
            ),
            telemetry:execute(
                [bondy_oplog, sync_scheduler, live_sync_poll],
                #{window_ms => NextWindow},
                #{instance_id => InstanceId}
            ),
            true;
        [{InstanceId, _Root, _Due, _Window}] ->
            %% Quiescent, within window → skip (the churn we are cutting).
            telemetry:execute(
                [bondy_oplog, sync_scheduler, live_sync_skipped],
                #{count => 1},
                #{instance_id => InstanceId}
            ),
            false
    end.

%% @private
%% The instance's in-memory MST root, used purely as a change detector
%% for the throttle. Soft-fails to `undefined` (treated as "no change"
%% against a prior `undefined`) if the instance is mid-restart.
current_root(InstanceId) ->
    try
        bondy_oplog_instance:root_hash(InstanceId)
    catch
        _:_ -> undefined
    end.

%% @private
live_adaptive_enabled() ->
    application:get_env(bondy_oplog, live_sync_adaptive, true).

%% @private
%% Base poll interval; defaults to the tick interval so an active shard
%% syncs every tick exactly as before.
live_sync_base_ms() ->
    application:get_env(
        bondy_oplog,
        live_sync_base_ms,
        application:get_env(bondy_oplog, sync_interval_ms, 500)
    ).

%% @private
live_sync_max_ms() ->
    application:get_env(bondy_oplog, live_sync_max_ms, 5000).

%% @private
ensure_live_backoff_table() ->
    case ets:info(?LIVE_BACKOFF_TAB) of
        undefined ->
            try
                ets:new(?LIVE_BACKOFF_TAB, [
                    named_table,
                    set,
                    public,
                    {read_concurrency, true},
                    {write_concurrency, true}
                ])
            catch
                error:badarg -> ?LIVE_BACKOFF_TAB
            end;
        _ ->
            ?LIVE_BACKOFF_TAB
    end.

%% @private
dispatch_live_sync(InstanceId, Peers) ->
    SessionOpts = session_opts(),
    lists:foreach(
        fun(Peer) ->
            _ = bondy_oplog_sync_session:start(
                InstanceId, Peer, SessionOpts
            )
        end,
        Peers
    ).

%% @private
%% Session opts threaded into every dispatched bootstrap / live-sync
%% session, read from app env each tick so a runtime change is picked up
%% on the next round. The default `#{}` keeps the historical behaviour:
%% the session falls back to `bondy_oplog_transport_inline`. A clustered
%% deployment sets `#{transport => bondy_oplog_transport_partisan,
%% transport_opts => #{channel => ...}}` here (see `bondy_app`).
session_opts() ->
    application:get_env(bondy_oplog, sync_session_opts, #{}).
