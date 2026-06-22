%% =============================================================================
%% SPDX-FileCopyrightText: 2016 - 2026 Leapsight
%% SPDX-License-Identifier: Apache-2.0
%% =============================================================================

-module(bondy_oplog_config).
-moduledoc """
The public configuration surface for the `bondy_oplog` layer.

This module is the single source of truth for `bondy_oplog`'s tunable
application-environment values: each key's **default lives here, once**, in its
accessor — so the same default can no longer drift between read sites (the way
`bootstrap_retry_base_ms` and friends previously carried a literal at every
call). The export list is, in effect, the layer's config schema.

## Live reads, not a cached snapshot

Each accessor reads `application:get_env(bondy_oplog, Key, Default)` directly, so
a value set at boot (by the release/cuttlefish schema) **or** changed at runtime
(`application:set_env/3`, as tests and operators do) takes effect on the next
read. This is deliberately NOT the `app_config`/`persistent_term` snapshot
pattern used by `bondy_mst_config`: that caches the env at init, which would
silently ignore a post-boot `set_env` — exactly the override the test harness
and operators rely on here.

## What is intentionally NOT here

- **Pluggable-implementation hooks** — `gc_trigger`, `sync_dispatch`,
  `peer_source`. These select a *module/fun* and fall back to a layer-local
  default implementation (`default_dispatch/2` etc.); they are not value
  tunables and their fallback belongs next to the implementation it names.
- **Per-instance keys** — `{validator_crypto, InstanceId}`. Instance-scoped
  state, not a global tunable.
""".

-define(APP, bondy_oplog).

%% SCHEDULERS
-export([sync_scheduler_enabled/0]).
-export([sync_interval_ms/0]).
-export([gc_scheduler_enabled/0]).
-export([gc_interval_ms/0]).
-export([gc_max_concurrency/0]).

%% LIVE-SYNC THROTTLE
-export([live_sync_adaptive/0]).
-export([live_sync_base_ms/0]).
-export([live_sync_max_ms/0]).

%% PEERS / BOOTSTRAP
-export([peer_timeout_ms/0]).
-export([bootstrap_peer_strategy/0]).
-export([max_inflight_bootstraps/0]).
-export([bootstrap_retry_base_ms/0]).
-export([bootstrap_retry_max_ms/0]).
-export([bootstrap_retry_jitter/0]).

%% AAE / SYNC SESSION
-export([aae_fence_on_isolation/0]).
-export([sync_session_opts/0]).

%% OBSERVABILITY / MISC
-export([catalogue_cursor_ttl_ms/0]).
-export([metrics_interval_ms/0]).
-export([oplog_latency_opts/0]).
-export([latency_probe/0]).

%% =============================================================================
%% API — SCHEDULERS
%% =============================================================================

-doc "Whether the periodic AAE sync scheduler ticks (default `true`).".
-spec sync_scheduler_enabled() -> boolean().

sync_scheduler_enabled() ->
    application:get_env(?APP, sync_scheduler, true).

-doc "Sync scheduler tick interval in milliseconds (default `500`).".
-spec sync_interval_ms() -> non_neg_integer().

sync_interval_ms() ->
    application:get_env(?APP, sync_interval_ms, 500).

-doc "Whether the periodic compaction (GC) scheduler ticks (default `true`).".
-spec gc_scheduler_enabled() -> boolean().

gc_scheduler_enabled() ->
    application:get_env(?APP, gc_scheduler, true).

-doc "GC scheduler tick interval in milliseconds (default `1000`).".
-spec gc_interval_ms() -> non_neg_integer().

gc_interval_ms() ->
    application:get_env(?APP, gc_interval_ms, 1000).

-doc "Maximum concurrent compaction cycles in flight (default `4`).".
-spec gc_max_concurrency() -> pos_integer().

gc_max_concurrency() ->
    application:get_env(?APP, gc_max_concurrency, 4).

%% =============================================================================
%% API — LIVE-SYNC THROTTLE
%% =============================================================================

-doc "Whether a converged instance backs its live-sync cadence off (default `true`).".
-spec live_sync_adaptive() -> boolean().

live_sync_adaptive() ->
    application:get_env(?APP, live_sync_adaptive, true).

-doc """
Base live-sync cadence in milliseconds: the cadence while the local root is
moving. Defaults to `sync_interval_ms/0` (the scheduler tick).
""".
-spec live_sync_base_ms() -> non_neg_integer().

live_sync_base_ms() ->
    application:get_env(?APP, live_sync_base_ms, sync_interval_ms()).

-doc "Maximum (backed-off) live-sync poll window in milliseconds (default `5000`).".
-spec live_sync_max_ms() -> non_neg_integer().

live_sync_max_ms() ->
    application:get_env(?APP, live_sync_max_ms, 5000).

%% =============================================================================
%% API — PEERS / BOOTSTRAP
%% =============================================================================

-doc "Peer liveness timeout in milliseconds (default `30000`).".
-spec peer_timeout_ms() -> non_neg_integer().

peer_timeout_ms() ->
    application:get_env(?APP, peer_timeout_ms, 30_000).

-doc "Bootstrap peer-selection strategy (default `first`).".
-spec bootstrap_peer_strategy() -> atom().

bootstrap_peer_strategy() ->
    application:get_env(?APP, bootstrap_peer_strategy, first).

-doc "Maximum concurrent bootstrap sessions in flight (default `4`).".
-spec max_inflight_bootstraps() -> pos_integer().

max_inflight_bootstraps() ->
    application:get_env(?APP, max_inflight_bootstraps, 4).

-doc "Bootstrap retry backoff base in milliseconds (default `500`).".
-spec bootstrap_retry_base_ms() -> non_neg_integer().

bootstrap_retry_base_ms() ->
    application:get_env(?APP, bootstrap_retry_base_ms, 500).

-doc "Bootstrap retry backoff ceiling in milliseconds (default `30000`).".
-spec bootstrap_retry_max_ms() -> non_neg_integer().

bootstrap_retry_max_ms() ->
    application:get_env(?APP, bootstrap_retry_max_ms, 30000).

-doc "Whether bootstrap retry backoff is jittered (default `true`).".
-spec bootstrap_retry_jitter() -> boolean().

bootstrap_retry_jitter() ->
    application:get_env(?APP, bootstrap_retry_jitter, true).

%% =============================================================================
%% API — AAE / SYNC SESSION
%% =============================================================================

-doc """
The AE-fence policy when this node is an isolated, non-solo minority
(`refuse | proceed | quorum`; default `refuse`).
""".
-spec aae_fence_on_isolation() -> atom().

aae_fence_on_isolation() ->
    application:get_env(?APP, aae_fence_on_isolation, refuse).

-doc "Extra options threaded into each sync session (default `#{}`).".
-spec sync_session_opts() -> map().

sync_session_opts() ->
    application:get_env(?APP, sync_session_opts, #{}).

%% =============================================================================
%% API — OBSERVABILITY / MISC
%% =============================================================================

-doc "Catalogue-snapshot cursor TTL in milliseconds (default `60000`).".
-spec catalogue_cursor_ttl_ms() -> non_neg_integer().

catalogue_cursor_ttl_ms() ->
    application:get_env(?APP, catalogue_cursor_ttl_ms, 60_000).

-doc """
Core-metrics reporting interval in milliseconds (default `1000`), read from the
`interval_ms` field of the `metrics` map env value when present.
""".
-spec metrics_interval_ms() -> non_neg_integer().

metrics_interval_ms() ->
    case application:get_env(?APP, metrics) of
        {ok, M} when is_map(M) -> maps:get(interval_ms, M, 1000);
        _ -> 1000
    end.

-doc "Write→readable latency-sampling options map (default `#{}`).".
-spec oplog_latency_opts() -> map().

oplog_latency_opts() ->
    case application:get_env(?APP, oplog_latency) of
        {ok, M} when is_map(M) -> M;
        _ -> #{}
    end.

-doc "The latency-probe config, or `undefined` when probing is disabled.".
-spec latency_probe() -> term().

latency_probe() ->
    application:get_env(?APP, latency_probe, undefined).
