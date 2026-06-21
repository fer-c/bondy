%% =============================================================================
%% SPDX-FileCopyrightText: 2016 - 2026 Leapsight
%% SPDX-License-Identifier: Apache-2.0
%% =============================================================================


-module(bondy_observer_cli_sync).
-moduledoc """
`observer_cli` plugin rendering bondy_db anti-entropy (AAE) sync status.

The authoritative convergence signal is the per-instance **projection content
digest** (`bondy_oplog_content_digest`, the AR-17 oracle), NOT the MST root.
The MST root is unreliable across nodes in different compaction states:
compaction empties the MST while the data persists in the projection, so two
converged nodes can advertise different roots (one compacted, one not) and —
worse — two nodes that have both compacted to an empty MST both advertise
`undefined`, so a root comparison reports IN SYNC without anything actually
verifying their projections match. The content digest is taken over the
projection content itself, so it is compaction-invariant: equal digests mean the
projections genuinely match.

Each row compares the local instance's LIVE digest against the peer's LIVE
digest, fetched on demand with a `get_content_digest` over the AAE channel (the
same transport the sync protocol uses). The verdict is gated three ways:

- **Lifecycle** — a `pre_bootstrap` instance (still pulling its initial
  snapshot) reads `bootstrap`, never IN SYNC; an unregistered one reads
  `starting`.
- **Readiness** — while either side's digest is still `warming` (a crash-restart
  recompute folding the projection), the row reads `warming` and is NOT judged —
  a partial digest must never be read as DIVERGED.
- **Topology** — digests are compared only when both nodes' keying-topology
  fingerprints match; a mismatch reads `topo≠` (the digests are incomparable).

For a **legacy peer** that predates the digest request (rolling upgrade), the
row falls back to the old `get_root` MST-root comparison and the peer cell reads
`legacy`.

Summary block: whether AAE is enabled, the scheduler tick interval, the instance
and peer counts, and an in-sync tally (bootstrapping, warming, and diverged
shards count toward the total but not in-sync, so it reads N/N only once every
shard is genuinely converged). Table: one row per `(instance, peer)` with both
digests (short hex) and a status — `IN SYNC`, `DIVERGED`, `warming`, `topo≠`,
`bootstrap`, `starting`, `no data` (peer unreachable / instance not running
there), or `solo` (no peers).

Register it via the `observer_cli` application env (see `sys.config`):

```erlang
{observer_cli, [
    {plugins, [
        #{module => bondy_observer_cli_sync, title => "Sync",
          interval => 2000, shortcut => "Y", sort_column => 5}
    ]}
]}
```

Reads are gathered defensively and run off the instance process: the digest is a
lock-free atomics read and the peer calls are `gen_server` calls, so none of them
touch the pack store's process-bound file descriptors.
""".

-behaviour(observer_cli_plugin).

%% observer_cli colour escapes (mirrors observer_cli.hrl).
-define(GREEN, <<"\e[32;1m">>).
-define(RED, <<"\e[31m">>).
-define(YELLOW, <<"\e[33m">>).

%% OBSERVER_CLI_PLUGIN CALLBACKS
-export([attributes/1]).
-export([sheet_header/0]).
-export([sheet_body/1]).

-ifdef(TEST).
%% Exposed for unit-testing the lifecycle-gated row classifier.
-export([status/3]).
-export([status_label/1]).
-endif.


%% =============================================================================
%% OBSERVER_CLI_PLUGIN CALLBACKS
%% =============================================================================


-doc "Top summary block: AAE state, interval, instance/peer counts, in-sync tally.".
-spec attributes(State :: term()) -> {[[map()]], NewState :: term()}.

attributes(State) ->
    Instances = instances(),
    Peers = peers(),
    {InSync, Compared} = tally(Instances, Peers),
    {AaeStr, AaeColour} = aae_label(),
    Rows = [
        [
            cell("AAE", 10),
            cell(AaeStr, 12, AaeColour),
            cell("Interval", 12),
            cell(integer_to_list(interval_ms()) ++ "ms", 12),
            cell("Instances", 12),
            cell(integer_to_list(length(Instances)), 8)
        ],
        [
            cell("Peers", 10),
            cell(integer_to_list(length(Peers)), 12),
            cell("In sync", 12),
            cell(
                io_lib:format("~p/~p", [InSync, Compared]),
                12,
                tally_colour(InSync, Compared)
            ),
            cell("", 12),
            cell("", 8)
        ]
    ],
    {Rows, State}.


-doc "Per `(instance, peer)` table columns.".
-spec sheet_header() -> [map()].

sheet_header() ->
    [
        #{title => "Instance", width => 16},
        #{title => "Peer", width => 28},
        #{title => "Local dig", width => 14},
        #{title => "Peer dig", width => 14},
        #{title => "Status", width => 12}
    ].


-doc "One row per `(instance, connected-peer)`; `solo` when there are no peers.".
-spec sheet_body(State :: term()) -> {[list()], NewState :: term()}.

sheet_body(State) ->
    Peers = peers(),
    Rows = lists:flatmap(
        fun(Id) ->
            Life = lifecycle(Id),
            Local = local_sig(Id),
            case Peers of
                [] ->
                    [[to_str(Id), "(solo)", local_cell(Local), "-", "solo"]];
                _ ->
                    [
                        begin
                            Peer = peer_sig(P, Id),
                            [
                                to_str(Id),
                                to_str(P),
                                local_cell(Local),
                                peer_cell(Peer),
                                status_label(status(Life, Local, Peer))
                            ]
                        end
                     || P <- Peers
                    ]
            end
        end,
        instances()
    ),
    {Rows, State}.


%% =============================================================================
%% PRIVATE
%% =============================================================================


%% @private
cell(Content, Width) ->
    #{content => Content, width => Width}.


%% @private
cell(Content, Width, Colour) ->
    #{content => Content, width => Width, color => Colour}.


%% @private
instances() ->
    case catch bondy_oplog:list_instances() of
        L when is_list(L) -> lists:sort(L);
        _ -> []
    end.


%% @private
peers() ->
    case catch partisan:nodes() of
        N when is_list(N) -> N;
        _ -> []
    end.


%% @private
root_hash(Id) ->
    %% In-memory MST root (kept fresh in the registry by the instance's
    %% publish/1); safe to read off the instance process. Used only for the
    %% legacy-peer fallback comparison.
    case catch bondy_oplog:root_hash(Id) of
        H when is_binary(H) -> H;
        _ -> undefined
    end.


%% @private
%% The local instance's content-digest signature for comparison:
%% `{ready | warming, Digest, Fingerprint, Root}`. The digest + readiness are a
%% lock-free read; the topology fingerprint gates cross-node comparison; the
%% root backs the legacy-peer fallback.
local_sig(Id) ->
    {Status, Digest} =
        case catch bondy_oplog_instance:content_digest(Id) of
            {S, D} when is_integer(D) -> {S, D};
            _ -> {warming, 0}
        end,
    {Status, Digest, local_fingerprint(Id), root_hash(Id)}.


%% @private
local_fingerprint(Id) ->
    case catch bondy_oplog:topology_fingerprint(bondy_oplog:db_of(Id)) of
        FP when is_binary(FP) -> FP;
        _ -> undefined
    end.


%% @private
%% The peer's LIVE digest signature, fetched fresh over the AAE channel:
%%   - `{digest, ready | warming, Digest, Fingerprint}` — the peer answered
%%     `get_content_digest`;
%%   - `{root, Root}` — a LEGACY peer that does not implement the digest request
%%     (rolling upgrade): fall back to its `get_root` MST root;
%%   - `not_found` — unreachable / slow, or the instance is not running there.
%% A fresh request (not the cached last-sync value) so it reflects what the peer
%% would serve right now.
peer_sig(Peer, Id) ->
    Opts = #{timeout => 2000, channel => aae_channel()},
    case
        catch bondy_oplog_transport_partisan:request(
            Peer, Id, get_content_digest, Opts
        )
    of
        {ok, {Status, Digest}, Fp} when is_integer(Digest) ->
            {digest, Status, Digest, Fp};
        {ok, {Status, Digest}} when is_integer(Digest) ->
            {digest, Status, Digest, undefined};
        _ ->
            %% No digest support (legacy peer) or an error: fall back to the
            %% MST root the old protocol always serves.
            peer_root(Peer, Id)
    end.


%% @private
%% Legacy fallback: the peer's LIVE advertised MST root via `get_root`.
%% `{root, Root}` on success, `not_found` otherwise.
peer_root(Peer, Id) ->
    Opts = #{timeout => 2000, channel => aae_channel()},
    case catch bondy_oplog_transport_partisan:request(Peer, Id, get_root, Opts) of
        {ok, Root} -> {root, Root};
        {ok, Root, _Fp} -> {root, Root};
        _ -> not_found
    end.


%% @private
aae_channel() ->
    case catch bondy_config:get(aae_channel) of
        Ch when is_atom(Ch) -> Ch;
        _ -> bondy_aae
    end.


%% @private
%% Classify an `(instance, peer)` pair from the local digest signature
%% `Local = {ready | warming, Digest, Fingerprint, Root}` and the peer signature
%% `Peer` (see `peer_sig/2`). Gated four ways:
%%
%%   - LIFECYCLE: a `pre_bootstrap` local instance is still pulling its initial
%%     snapshot ⇒ `bootstrap`, never IN SYNC; an unregistered one ⇒ `starting`.
%%   - REACHABILITY: no peer signature ⇒ `no_data`.
%%   - READINESS: either side's digest still `warming` (a crash-restart recompute
%%     folding the projection) ⇒ `warming`; a partial digest must NOT read as
%%     DIVERGED.
%%   - TOPOLOGY: digests are compared only when both fingerprints are present and
%%     EQUAL; a genuine mismatch ⇒ `topo` (the digests are incomparable). A
%%     missing fingerprint (either side) skips the check and compares anyway.
%%
%% Otherwise equal digests ⇒ `in_sync`, differing ⇒ `diverged`. A LEGACY peer
%% (`{root, _}`) falls back to MST-root equality against the local root.
status(pre_bootstrap, _Local, _Peer) ->
    bootstrap;
status(undefined, _Local, _Peer) ->
    starting;
status(live, _Local, not_found) ->
    no_data;
status(live, {warming, _, _, _}, _Peer) ->
    warming;
status(live, {ready, _, _, _}, {digest, warming, _, _}) ->
    warming;
status(live, {ready, LDigest, LFp, _LRoot}, {digest, ready, PDigest, PFp}) ->
    case fingerprints_differ(LFp, PFp) of
        true -> topo;
        false when LDigest =:= PDigest -> in_sync;
        false -> diverged
    end;
status(live, {ready, _LDigest, _LFp, LRoot}, {root, PRoot}) ->
    %% Legacy peer: MST-root fallback (the pre-AR-17 comparison).
    case LRoot =:= PRoot of
        true -> in_sync;
        false -> diverged
    end;
status(live, _Local, _Peer) ->
    unknown.


%% @private
%% Both fingerprints are present (not `undefined`) AND differ — only then are the
%% two nodes keying data differently, making their digests incomparable.
fingerprints_differ(LFp, PFp) ->
    LFp =/= undefined andalso PFp =/= undefined andalso LFp =/= PFp.


%% @private
status_label(in_sync) -> "IN SYNC";
status_label(diverged) -> "DIVERGED";
status_label(warming) -> "warming";
status_label(topo) -> "topo≠";
status_label(bootstrap) -> "bootstrap";
status_label(no_data) -> "no data";
status_label(starting) -> "starting";
status_label(unknown) -> "?".


%% @private
%% The instance's bootstrap lifecycle (`pre_bootstrap` until its initial
%% snapshot lands, then `live`). Any error / unknown maps to `undefined`
%% (rendered `starting`).
lifecycle(Id) ->
    case catch bondy_oplog_instance:lifecycle_state(Id) of
        live -> live;
        pre_bootstrap -> pre_bootstrap;
        _ -> undefined
    end.


%% @private
%% In-sync tally over the (instance, peer) pairs. A live pair with equal
%% digests counts as in-sync; live `diverged`/`warming`/`topo` pairs and a
%% `bootstrap` pair (local instance still pulling its snapshot) all count toward
%% the total but not in-sync — so the summary reads e.g. 20/32 mid-rebuild and
%% only reaches N/N once every shard is genuinely converged. Pairs with no peer
%% data, or a still-`starting` local instance, are simply uncompared.
tally(Instances, Peers) ->
    lists:foldl(
        fun(Id, Acc0) ->
            Life = lifecycle(Id),
            Local = local_sig(Id),
            lists:foldl(
                fun(P, {Ok, Total} = Acc) ->
                    case status(Life, Local, peer_sig(P, Id)) of
                        in_sync -> {Ok + 1, Total + 1};
                        diverged -> {Ok, Total + 1};
                        warming -> {Ok, Total + 1};
                        topo -> {Ok, Total + 1};
                        bootstrap -> {Ok, Total + 1};
                        _ -> Acc
                    end
                end,
                Acc0,
                Peers
            )
        end,
        {0, 0},
        Instances
    ).


%% @private
aae_label() ->
    case application:get_env(bondy_oplog, aae_enabled, false) of
        true -> {"on", ?GREEN};
        _ -> {"off", ?YELLOW}
    end.


%% @private
interval_ms() ->
    application:get_env(bondy_oplog, sync_interval_ms, 500).


%% @private
tally_colour(_, 0) -> ?YELLOW;
tally_colour(N, N) -> ?GREEN;
tally_colour(_, _) -> ?RED.


%% @private
%% Render the LOCAL digest cell from `local_sig/1`.
local_cell({warming, _Digest, _Fp, _Root}) -> "warming";
local_cell({ready, Digest, _Fp, _Root}) -> short_digest(Digest);
local_cell(_) -> "?".


%% @private
%% Render the PEER digest cell from `peer_sig/2`.
peer_cell({digest, warming, _Digest, _Fp}) -> "warming";
peer_cell({digest, ready, Digest, _Fp}) -> short_digest(Digest);
peer_cell({root, _Root}) -> "legacy";
peer_cell(not_found) -> "-";
peer_cell(_) -> "?".


%% @private
%% The leading 8 hex chars of a content digest; `0` (empty projection) renders
%% as `(empty)`.
short_digest(0) ->
    "(empty)";
short_digest(D) when is_integer(D) ->
    string:slice(binary_to_list(bondy_oplog_content_digest:to_hex(D)), 0, 8);
short_digest(_) ->
    "?".


%% @private
to_str(V) when is_atom(V) -> atom_to_list(V);
to_str(V) when is_binary(V) -> binary_to_list(V);
to_str(V) when is_list(V) -> V;
to_str(V) -> lists:flatten(io_lib:format("~p", [V])).
