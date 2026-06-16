%% =============================================================================
%% Tests for `bondy_namespace_catalog` — the bondy_db DB/table declaration
%% point and owner of the durable `core` database.
%%
%% Pins: the twelve-table declaration (db split, shard_by mapping, fold class),
%% the core/registry DB specs, gated provisioning (core tables open + appear in
%% bondy_db:info, fold→CRDT wiring), the disabled no-op path, and teardown.
%% =============================================================================

-module(bondy_namespace_catalog_test).

-include_lib("eunit/include/eunit.hrl").

-define(CAT, bondy_namespace_catalog).


%% =============================================================================
%% Pure declaration tests (no setup)
%% =============================================================================

declarations_test_() ->
    Tables = ?CAT:tables(),
    ByName = maps:from_list([{maps:get(name, S), S} || S <- Tables]),
    Core = [S || S <- Tables, maps:get(db, S) =:= core],
    Registry = [S || S <- Tables, maps:get(db, S) =:= registry],
    [
        {"thirteen tables declared", ?_assertEqual(13, length(Tables))},
        {"eleven core, two registry", fun() ->
            ?assertEqual(11, length(Core)),
            ?assertEqual(2, length(Registry))
        end},
        {"group membership is a core aw fold (the §3 split table)", fun() ->
            Spec = maps:get(security_group_members, ByName),
            ?assertEqual(core, maps:get(db, Spec)),
            ?assertEqual(durable, maps:get(durability, Spec)),
            ?assertEqual(realm, maps:get(shard_by, Spec)),
            ?assertEqual(aw, maps:get(fold, Spec))
        end},
        {"ticket/oauth_token shard by key", fun() ->
            ?assertEqual(key, shard_by(ByName, bondy_ticket)),
            ?assertEqual(key, shard_by(ByName, bondy_oauth_token))
        end},
        {"all other tables shard by realm", fun() ->
            Others = [S || S <- Tables,
                not lists:member(maps:get(name, S),
                    [bondy_ticket, bondy_oauth_token])],
            ?assert(lists:all(
                fun(S) -> maps:get(shard_by, S) =:= realm end, Others
            ))
        end},
        {"grants + source are mv folds", fun() ->
            ?assertEqual(mv, fold(ByName, security_group_grants)),
            ?assertEqual(mv, fold(ByName, security_user_grants)),
            ?assertEqual(mv, fold(ByName, security_sources))
        end},
        {"registry tables are presence folds, ephemeral", fun() ->
            ?assert(lists:all(
                fun(S) ->
                    maps:get(fold, S) =:= presence andalso
                        maps:get(durability, S) =:= ephemeral
                end,
                Registry
            ))
        end},
        {"core_db_spec: shared_shards, durable, default shards", fun() ->
            Spec = ?CAT:core_db_spec(),
            ?assertMatch(
                #{
                    name := core,
                    topology := bondy_db_topology_shared_shards,
                    durability := durable,
                    shard_count := 16
                },
                Spec
            )
        end},
        {"registry_db_spec: memory, ephemeral, four ephemeral knobs", fun() ->
            #{
                topology := Topology,
                durability := Durability,
                table_opts := TOpts
            } = ?CAT:registry_db_spec(),
            ?assertEqual(bondy_db_topology_memory, Topology),
            ?assertEqual(ephemeral, Durability),
            ?assertMatch(
                #{
                    projection_backend := ets,
                    fused := true,
                    oplog_instance_opts := #{
                        backend := ets,
                        wal_backend := mem,
                        durability := ephemeral
                    }
                },
                TOpts
            )
        end}
    ].


%% =============================================================================
%% Lifecycle tests (need the substrate)
%% =============================================================================

lifecycle_test_() ->
    {setup,
        fun() -> {ok, _} = application:ensure_all_started(bondy_db), ok end,
        fun(_) -> ok end,
        [
            {timeout, 60,
                {"provision_all opens every core table", fun provision_all/0}},
            {timeout, 60,
                {"default provisions only migrated tables",
                    fun migrated_only/0}}
        ]}.


provision_all() ->
    Tmp = make_tmpdir(),
    set_env(true, 1, Tmp),
    {ok, Pid} = ?CAT:start_link(),
    try
        %% Core DB + all eleven core tables provisioned and published.
        ?assert(?CAT:is_open()),
        ?assertMatch(#{name := core}, ?CAT:core_db()),
        ?assertMatch(#{kind := db, name := core}, bondy_db:info(?CAT:core_db())),
        CoreNames = [maps:get(name, S)
            || S <- ?CAT:tables(), maps:get(db, S) =:= core],
        lists:foreach(
            fun(Name) ->
                ?assertMatch(
                    #{entity_type := Name, db_name := core},
                    ?CAT:table(Name)
                )
            end,
            CoreNames
        ),
        %% Registry tables are declared but NOT opened here.
        ?assertEqual(undefined, ?CAT:table(bondy_registration)),
        ?assertEqual(undefined, ?CAT:table(bondy_subscription)),
        %% Fold → CRDT wiring: mv tables carry the mv_register CRDT, the
        %% membership table the aw_map CRDT; lww tables resolve to lww_register.
        ?assertMatch(
            #{crdt_module := bondy_oplog_crdt_mv_register},
            bondy_db:info(?CAT:table(security_user_grants))
        ),
        ?assertMatch(
            #{crdt_module := bondy_oplog_crdt_aw_map},
            bondy_db:info(?CAT:table(security_group_members))
        ),
        ?assertMatch(
            #{fold_module := lww_register},
            bondy_db:info(?CAT:table(bondy_realm))
        ),
        %% info/0 summary.
        Info = ?CAT:info(),
        ?assertMatch(#{provision_all := true, core := #{kind := db}}, Info),
        ?assertEqual(11, map_size(maps:get(tables, Info)))
    after
        ok = stop_catalog(Pid),
        reset_env(),
        rmrf(Tmp)
    end,
    %% Teardown cleared the published handles.
    ?assert(await(fun() -> ?CAT:is_open() =:= false end, 100)),
    ?assertEqual(undefined, ?CAT:core_db()),
    ?assertEqual(undefined, ?CAT:table(bondy_realm)).


%% Default (flag off): only the migrated domain's table (api_gateway) is opened;
%% the core DB still comes up to host it, but not-yet-migrated tables stay shut.
migrated_only() ->
    Tmp = make_tmpdir(),
    set_env(false, 1, Tmp),
    {ok, Pid} = ?CAT:start_link(),
    try
        ?assert(?CAT:is_open()),
        ?assertMatch(#{entity_type := api_gateway, db_name := core},
            ?CAT:table(api_gateway)),
        %% Not-yet-migrated core tables are NOT opened.
        ?assertEqual(undefined, ?CAT:table(bondy_realm)),
        ?assertEqual(undefined, ?CAT:table(security_user_grants)),
        ?assertMatch(#{provision_all := false, core := #{kind := db}}, ?CAT:info())
    after
        ok = stop_catalog(Pid),
        reset_env(),
        rmrf(Tmp)
    end.


%% =============================================================================
%% Helpers
%% =============================================================================

shard_by(ByName, Name) ->
    maps:get(shard_by, maps:get(Name, ByName)).

fold(ByName, Name) ->
    maps:get(fold, maps:get(Name, ByName)).

set_env(Enabled, Shards, Dir) ->
    application:set_env(bondy_router, oplog_catalog_enabled, Enabled),
    application:set_env(bondy_router, oplog_core_shard_count, Shards),
    application:set_env(bondy_router, platform_data_dir, Dir).

reset_env() ->
    application:unset_env(bondy_router, oplog_catalog_enabled),
    application:unset_env(bondy_router, oplog_core_shard_count),
    application:unset_env(bondy_router, platform_data_dir).

stop_catalog(Pid) ->
    _ = catch gen_server:stop(Pid, normal, 30000),
    ok.

make_tmpdir() ->
    Base = filename:join(
        "/tmp",
        "bondy_catalog_test_" ++
            integer_to_list(erlang:unique_integer([positive, monotonic]))
    ),
    ok = filelib:ensure_path(Base),
    Base.

rmrf(Dir) ->
    _ = file:del_dir_r(Dir),
    ok.

%% Poll a predicate up to ~1s — teardown of leveled instances is async.
await(_Pred, 0) ->
    false;
await(Pred, N) ->
    case Pred() of
        true -> true;
        false -> timer:sleep(10), await(Pred, N - 1)
    end.
