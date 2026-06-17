%% =============================================================================
%% SPDX-FileCopyrightText: 2016 - 2026 Leapsight
%% SPDX-License-Identifier: Apache-2.0
%% =============================================================================

-module(bondy_aae_cluster_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

-compile([nowarn_export_all, export_all]).

%% A 3-node Partisan cluster with bondy_db anti-entropy (`oplog.aae') enabled.
%% Each node runs the full bondy_router stack with all client listeners
%% disabled and an isolated data dir / Partisan port (see
%% `bondy_ct:start_cluster/2'). These tests write through `bondy_db' on one
%% node and assert the value converges on the others via the periodic sync
%% scheduler over the Partisan transport — i.e. the production AAE path, not a
%% hand-called `bondy_oplog:sync/3'.

-define(NODE_NAMES, [bondy1, bondy2, bondy3]).
%% A per-realm durable core table (band = realm URI, exercises G-1 realm
%% folding) and a global-band durable core table (band = <<>>).
-define(USERS_TABLE, security_users).
-define(BRIDGE_TABLE, bondy_bridge_relay).
-define(REALM_TABLE, bondy_realm).
-define(REALM, <<"com.bondy.aae_cluster">>).
%% How long to wait for a write to propagate across the cluster.
-define(CONVERGE_MS, 30000).

all() ->
    [
        per_realm_write_converges,
        global_band_write_converges,
        concurrent_writes_full_convergence,
        merge_event_fires_on_remote_write,
        realm_merge_event_fires_on_remote_write
    ].

suite() ->
    [{timetrap, {minutes, 10}}].

init_per_suite(Config) ->
    Nodes = bondy_ct:start_cluster(?NODE_NAMES, Config),
    %% The peer-side read/write helpers below run on the cluster nodes, so make
    %% this module loadable there.
    _ = [push_module(Node, ?MODULE) || {_, Node, _} <- Nodes],
    [{cluster, Nodes} | Config].

end_per_suite(Config) ->
    ok = bondy_ct:stop_cluster(?config(cluster, Config)),
    Config.

%% =============================================================================
%% TESTS
%% =============================================================================

%% Write a per-realm `security_users' entry on node 1; it must appear on
%% nodes 2 and 3 via background AAE.
per_realm_write_converges(Config) ->
    [N1, N2, N3] = nodes_of(Config),
    Key = <<"alice">>,
    Val = #{username => Key, marker => <<"per_realm">>},

    ok = apply_on(N1, ?USERS_TABLE, ?REALM, Key, Val),
    ?assertMatch({ok, {Val, _}}, read_on(N1, ?USERS_TABLE, ?REALM, Key)),

    ok = wait_converge(N2, ?USERS_TABLE, ?REALM, Key, Val),
    ok = wait_converge(N3, ?USERS_TABLE, ?REALM, Key, Val).

%% Write a global-band (<<>>) `bondy_bridge_relay' entry on node 2; it must
%% appear on nodes 1 and 3 — covers the const-band addressing path and a
%% different originating node.
global_band_write_converges(Config) ->
    [N1, N2, N3] = nodes_of(Config),
    Key = <<"bridge_a">>,
    Val = #{name => Key, marker => <<"global_band">>},

    ok = apply_on(N2, ?BRIDGE_TABLE, <<>>, Key, Val),
    ?assertMatch({ok, {Val, _}}, read_on(N2, ?BRIDGE_TABLE, <<>>, Key)),

    ok = wait_converge(N1, ?BRIDGE_TABLE, <<>>, Key, Val),
    ok = wait_converge(N3, ?BRIDGE_TABLE, <<>>, Key, Val).

%% Two distinct keys written on two different nodes must both be visible on
%% all three after AAE — bidirectional, full convergence.
concurrent_writes_full_convergence(Config) ->
    [N1, N2, N3] = nodes_of(Config),
    K1 = <<"conc_from_n1">>,
    V1 = #{username => K1, marker => <<"n1">>},
    K3 = <<"conc_from_n3">>,
    V3 = #{username => K3, marker => <<"n3">>},

    ok = apply_on(N1, ?USERS_TABLE, ?REALM, K1, V1),
    ok = apply_on(N3, ?USERS_TABLE, ?REALM, K3, V3),

    [
        begin
            ok = wait_converge(N, ?USERS_TABLE, ?REALM, K1, V1),
            ok = wait_converge(N, ?USERS_TABLE, ?REALM, K3, V3)
        end
     || N <- [N1, N2, N3]
    ],
    ok.

%% The merge-side reactor hook (bondy_oplog_core:publish_merge/4) must fire on
%% node 2 when anti-entropy merges a write authored on node 1, and must NOT fire
%% for node 2's own local writes. A collector process on node 2 subscribes to
%% the security_users namespace and records the events it receives.
merge_event_fires_on_remote_write(Config) ->
    [N1, N2, _N3] = nodes_of(Config),
    NS = erpc:call(N2, ?MODULE, do_namespace, [?USERS_TABLE]),
    ok = erpc:call(N2, ?MODULE, start_collector, [NS]),

    %% Remote write on node 1 → converges on node 2 AND delivers a merge event.
    RKey = <<"merge_hook_remote">>,
    RVal = #{username => RKey, marker => <<"merge_hook">>},
    ok = apply_on(N1, ?USERS_TABLE, ?REALM, RKey, RVal),
    ok = wait_converge(N2, ?USERS_TABLE, ?REALM, RKey, RVal),
    ok = wait_for_merge_event(N2, RKey, 15000),

    %% A purely local write on node 2 must NOT produce a merge event for its
    %% key (it fires a plain local event instead).
    LKey = <<"merge_hook_local_only">>,
    LVal = #{username => LKey, marker => <<"local">>},
    ok = apply_on(N2, ?USERS_TABLE, ?REALM, LKey, LVal),
    %% Give any (erroneous) merge event time to arrive before we assert absence.
    timer:sleep(1500),
    Events = erpc:call(N2, ?MODULE, collector_drain, []),
    Merges = [
        K
     || {bondy_oplog_core_merge_event, _, K, _, _} <- Events,
        binary:match(K, LKey) =/= nomatch
    ],
    ?assertEqual([], Merges),
    ok.

%% The merge hook must also fire for a global-band (<<>>) `publish => true' table
%% — here `bondy_realm', whose folded cell key is `<<0, Uri>>'. This is the path
%% the realm-delete reactor (`bondy_aae_reactor') consumes; the per-realm
%% security_users test above only covers the folded-band path.
realm_merge_event_fires_on_remote_write(Config) ->
    [N1, N2, _N3] = nodes_of(Config),
    NS = erpc:call(N2, ?MODULE, do_namespace, [?REALM_TABLE]),
    ok = erpc:call(N2, ?MODULE, start_collector, [NS]),

    Uri = <<"com.bondy.aae_realm_merge">>,
    Val = #{uri => Uri, marker => <<"realm_merge">>},
    ok = apply_on(N1, ?REALM_TABLE, <<>>, Uri, Val),
    ok = wait_converge(N2, ?REALM_TABLE, <<>>, Uri, Val),
    %% The collector records the merge event whose folded key `<<0, Uri>>'
    %% carries the realm URI as a substring.
    ok = wait_for_merge_event(N2, Uri, 15000),
    ok.

%% =============================================================================
%% CONTROLLER-SIDE HELPERS
%% =============================================================================

%% @private
nodes_of(Config) ->
    [Node || {_, Node, _} <- ?config(cluster, Config)].

%% @private
apply_on(Node, Table, Band, Key, Val) ->
    erpc:call(Node, ?MODULE, do_apply, [Table, Band, Key, Val]).

%% @private
read_on(Node, Table, Band, Key) ->
    erpc:call(Node, ?MODULE, do_read, [Table, Band, Key]).

%% @private
%% Polls `Node' until its local read of `Key' returns `Expected', forcing a
%% sync tick each round so we don't merely wait on the periodic timer.
wait_converge(Node, Table, Band, Key, Expected) ->
    Deadline = erlang:monotonic_time(millisecond) + ?CONVERGE_MS,
    wait_converge_loop(Node, Table, Band, Key, Expected, Deadline).

%% @private
wait_converge_loop(Node, Table, Band, Key, Expected, Deadline) ->
    %% Nudge the scheduler on the reading node to pull now.
    _ = catch erpc:call(Node, bondy_oplog_sync_scheduler, trigger, []),
    case read_on(Node, Table, Band, Key) of
        {ok, {Expected, _Hlc}} ->
            ok;
        Other ->
            case erlang:monotonic_time(millisecond) > Deadline of
                true ->
                    error({converge_timeout, Node, Table, Band, Key, Other});
                false ->
                    timer:sleep(250),
                    wait_converge_loop(
                        Node, Table, Band, Key, Expected, Deadline
                    )
            end
    end.

%% @private
%% Polls `Node`'s collector until it has recorded a merge event whose key
%% carries `Username` (the cell key is the G-1 realm-folded `<<Realm,0,User>>`,
%% so we match on substring rather than equality).
wait_for_merge_event(Node, Username, Timeout) ->
    Deadline = erlang:monotonic_time(millisecond) + Timeout,
    wait_for_merge_event_loop(Node, Username, Deadline).

%% @private
wait_for_merge_event_loop(Node, Username, Deadline) ->
    Events = erpc:call(Node, ?MODULE, collector_drain, []),
    Found = [
        K
     || {bondy_oplog_core_merge_event, _NS, K, _Hlc, _Op} <- Events,
        binary:match(K, Username) =/= nomatch
    ],
    case Found of
        [_ | _] ->
            ok;
        [] ->
            case erlang:monotonic_time(millisecond) > Deadline of
                true ->
                    error({no_merge_event, Node, Username, Events});
                false ->
                    timer:sleep(250),
                    wait_for_merge_event_loop(Node, Username, Deadline)
            end
    end.

%% @private
push_module(Node, Mod) ->
    {Mod, Bin, File} = code:get_object_code(Mod),
    {module, Mod} = erpc:call(Node, code, load_binary, [Mod, File, Bin]),
    ok.

%% =============================================================================
%% PEER-SIDE HELPERS (run on the cluster nodes via erpc)
%% =============================================================================

%% @private
do_apply(Table, Band, Key, Val) ->
    Tab = table_handle(Table),
    bondy_db:apply(Tab, Band, Key, {set, Val}).

%% @private
do_read(Table, Band, Key) ->
    Tab = table_handle(Table),
    bondy_db:read(Tab, Band, Key).

%% @private
table_handle(Table) ->
    case bondy_namespace_catalog:table(Table) of
        undefined -> error({table_not_provisioned, Table});
        Tab -> Tab
    end.

%% @private
do_namespace(Table) ->
    bondy_db:namespace(table_handle(Table)).

%% @private
%% Spawns a long-lived collector on this node subscribed to `NS`, registered as
%% `merge_collector`, recording every dispatcher event it receives. Returns once
%% the subscription is in place (so a subsequent remote write can't race it).
start_collector(NS) ->
    Parent = self(),
    Pid = spawn(fun() -> collector_init(NS, Parent) end),
    receive
        {Pid, ready} -> ok
    after 5000 ->
        error(collector_start_timeout)
    end,
    %% Re-register if a previous test left one behind.
    catch unregister(merge_collector),
    true = register(merge_collector, Pid),
    ok.

%% @private
collector_init(NS, Parent) ->
    {ok, _Ref} = bondy_oplog_core:subscribe(NS, all),
    Parent ! {self(), ready},
    collector_loop([]).

%% @private
collector_loop(Acc) ->
    receive
        {get, From} ->
            From ! {merge_collector_events, lists:reverse(Acc)},
            collector_loop(Acc);
        {bondy_oplog_core_merge_event, _, _, _, _} = E ->
            collector_loop([E | Acc]);
        {bondy_oplog_core_event, _, _, _, _} = E ->
            collector_loop([E | Acc]);
        _Other ->
            collector_loop(Acc)
    end.

%% @private
collector_drain() ->
    merge_collector ! {get, self()},
    receive
        {merge_collector_events, Events} -> Events
    after 5000 ->
        error(collector_drain_timeout)
    end.
