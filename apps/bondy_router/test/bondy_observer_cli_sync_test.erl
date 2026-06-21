%% =============================================================================
%% SPDX-FileCopyrightText: 2016 - 2026 Leapsight
%% SPDX-License-Identifier: Apache-2.0
%% =============================================================================
%% Unit tests for the digest-based, lifecycle-gated sync-status classifier in
%% `bondy_observer_cli_sync`. The classifier decides the per-row Status in the
%% observer_cli "Sync" view from the local instance's content-digest signature
%% and the peer's. It guards two regressions:
%%   - a freshly-wiped node still pulling its initial snapshot must read
%%     `bootstrap`, never IN SYNC (lifecycle gate);
%%   - a crash-restarted node whose digest recompute is still folding must read
%%     `warming`, never DIVERGED (readiness gate — a partial digest is not a
%%     verdict).
%% It also covers the topology-fingerprint gate and the legacy-peer (MST-root)
%% fallback used during a rolling upgrade.
-module(bondy_observer_cli_sync_test).

-include_lib("eunit/include/eunit.hrl").

%% Digests (64-bit ints), topology fingerprints, MST roots.
-define(D1, 16#1111111111111111).
-define(D2, 16#2222222222222222).
-define(FP1, <<"fp-a">>).
-define(FP2, <<"fp-b">>).
-define(R1, <<1, 2, 3, 4>>).
-define(R2, <<9, 9, 9, 9>>).

%% Local signature constructor: {ready|warming, Digest, Fingerprint, Root}.
-define(LOCAL(St, D), {St, D, ?FP1, ?R1}).

%% A local instance still bootstrapping is never IN SYNC, regardless of digests.
pre_bootstrap_never_in_sync_test() ->
    ?assertEqual(
        bootstrap,
        st(pre_bootstrap, ?LOCAL(ready, ?D1), {digest, ready, ?D1, ?FP1})
    ),
    ?assertEqual(
        bootstrap,
        st(pre_bootstrap, ?LOCAL(warming, 0), not_found)
    ).

%% An instance not yet registered reports `starting`.
starting_when_lifecycle_unknown_test() ->
    ?assertEqual(
        starting,
        st(undefined, ?LOCAL(ready, ?D1), {digest, ready, ?D1, ?FP1})
    ).

%% An unreachable peer (no signature) reads `no data`, even mid-warmup.
live_unreachable_peer_no_data_test() ->
    ?assertEqual(no_data, st(live, ?LOCAL(ready, ?D1), not_found)),
    ?assertEqual(no_data, st(live, ?LOCAL(warming, 0), not_found)).

%% A still-warming digest (either side) is never judged — never DIVERGED.
warming_local_test() ->
    ?assertEqual(
        warming,
        st(live, ?LOCAL(warming, 0), {digest, ready, ?D1, ?FP1})
    ).

warming_peer_test() ->
    ?assertEqual(
        warming,
        st(live, ?LOCAL(ready, ?D1), {digest, warming, 0, ?FP1})
    ).

%% Equal digests with matching fingerprints converge; including two empty
%% projections (digest 0) — the AR-17 case the MST root could not verify.
live_equal_digests_in_sync_test() ->
    ?assertEqual(
        in_sync,
        st(live, ?LOCAL(ready, ?D1), {digest, ready, ?D1, ?FP1})
    ),
    ?assertEqual(
        in_sync,
        st(live, {ready, 0, ?FP1, undefined}, {digest, ready, 0, ?FP1})
    ).

%% Differing digests (same topology) diverge.
live_unequal_digests_diverged_test() ->
    ?assertEqual(
        diverged,
        st(live, ?LOCAL(ready, ?D1), {digest, ready, ?D2, ?FP1})
    ).

%% Differing fingerprints (both present) are incomparable: `topo`, never a false
%% IN SYNC/DIVERGED on data.
live_topology_mismatch_test() ->
    ?assertEqual(
        topo,
        st(live, ?LOCAL(ready, ?D1), {digest, ready, ?D1, ?FP2})
    ),
    %% Even equal digests across different keying topologies are not "in sync".
    ?assertEqual(
        topo,
        st(live, {ready, ?D1, ?FP1, ?R1}, {digest, ready, ?D1, ?FP2})
    ).

%% A missing fingerprint (either side) skips the topology gate and compares the
%% digests directly (best-effort — e.g. the inline transport carries no
%% fingerprint).
live_missing_fingerprint_compares_digests_test() ->
    ?assertEqual(
        in_sync,
        st(live, {ready, ?D1, undefined, ?R1}, {digest, ready, ?D1, undefined})
    ),
    ?assertEqual(
        diverged,
        st(live, {ready, ?D1, ?FP1, ?R1}, {digest, ready, ?D2, undefined})
    ).

%% Legacy peer (no digest support): fall back to MST-root equality.
legacy_peer_root_fallback_test() ->
    ?assertEqual(
        in_sync,
        st(live, {ready, ?D1, ?FP1, ?R1}, {root, ?R1})
    ),
    ?assertEqual(
        diverged,
        st(live, {ready, ?D1, ?FP1, ?R1}, {root, ?R2})
    ).

%% Every status atom has a human label.
labels_cover_all_statuses_test() ->
    [
        ?assert(is_list(bondy_observer_cli_sync:status_label(S)))
     || S <- [
            in_sync, diverged, warming, topo, bootstrap, no_data, starting,
            unknown
        ]
    ].

%% @private
st(Life, Local, Peer) ->
    bondy_observer_cli_sync:status(Life, Local, Peer).
