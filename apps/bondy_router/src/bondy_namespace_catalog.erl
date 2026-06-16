%% =============================================================================
%% SPDX-FileCopyrightText: 2016 - 2026 Leapsight
%% SPDX-License-Identifier: Apache-2.0
%% =============================================================================


-module(bondy_namespace_catalog).
-behaviour(gen_server).
-moduledoc """
The single declaration point for Bondy's `bondy_db` databases and tables —
the `bondy_db` analogue of `bondy_plum_db.hrl`'s `?PLUM_DB_PREFIXES` — and the
owner process for the durable `core` database.

Two databases are declared:

- **`core`** — durable (`bondy_db_topology_shared_shards` over leveled),
  holding the eleven security / realm / gateway / token / bridge tables.
- **`registry`** — ephemeral (`bondy_db_topology_memory`, ETS), holding the
  two routing tables. Declared here but **not opened** until the registry
  domain cuts over (design §11.4); its ephemeral knobs are recorded so the
  open is a one-liner.

The table names mirror the `bondy_plum_db.hrl` prefixes (plus the net-new
`security_group_members` split table, design §3 — group membership lives in its
own `aw_map` table rather than inline in `security_groups`). Each table records
the `shard_by` strategy mapped from plum_db's `prefix|key` onto the new
`realm|key` model (design D-2: `prefix → realm`, `key → key`) and a fold class
(`lww | mv | aw | presence`, design §11.3). `shard_by` is **metadata**
at this stage — `bondy_db:open_table/3` does not yet honour realm-sharding
(that write-path lands with §11.4 realms); it is recorded so the cut-over can
consume it.

## Per-domain provisioning

The migration cuts over one domain at a time (design §11.4). A table spec
carries `migrated => true` once its domain reads/writes `bondy_db` instead of
`plum_db`; those tables are **always** provisioned at boot (they are required).
Not-yet-migrated tables stay on `plum_db` and are not opened — unless
`bondy_router.oplog_catalog_enabled` (`oplog.catalog`) is set, which provisions
**all** declared core tables too (for validating a future domain's provisioning
before its cut-over). So a default node opens exactly the migrated tables
(currently `api_gateway`) and serves every other read from `plum_db`.

## Lifecycle

This module is a `gen_server` (a child of `bondy_sup`). Because `bondy_db`
keeps leveled supervisors on-demand and **owned by the `open/2` caller**, the
catalogue process owns the `core` DB's `bondy_db_leveled_sup` for its lifetime:

- `init/1` — opens the durable `core` DB plus the tables to provision (migrated,
  plus all core tables when `oplog.catalog` is set), and publishes the DB /
  table handles via `persistent_term` for lock-free access. With nothing to
  open (no migrated tables and the flag off) it starts idle. On open failure it
  logs loudly and starts idle (it never bricks boot).
- `terminate/2` — closes each open table, the DB, and the leveled sup.

Accessors (`core_db/0`, `table/1`, `is_open/0`, `info/0`) read `persistent_term`
and never call the process. Declarations (`tables/0`, `core_db_spec/0`,
`registry_db_spec/0`) are pure.
""".

-include_lib("kernel/include/logger.hrl").

-include("bondy_plum_db.hrl").


-define(PT_DB(Name), {?MODULE, db, Name}).
-define(PT_TABLE(Name), {?MODULE, table, Name}).
-define(DEFAULT_CORE_SHARD_COUNT, 16).

%% The native CRDTs that have no short fold alias in `bondy_oplog_cell_kernel`
%% (`mv_register` for grants / sources, `aw_map` for group membership). They are
%% passed as an explicit `crdt_module`; the `fold_module` stays `lww_register`,
%% their byte-compatible carrier — see the mv_register / aw_map e2e tests.
-define(MV_CRDT, bondy_oplog_crdt_mv_register).
-define(AW_CRDT, bondy_oplog_crdt_aw_map).


-record(state, {
    db                      ::  bondy_db:db() | undefined,
    leveled_sup             ::  pid() | undefined,
    dir                     ::  file:filename_all() | undefined
}).


-type fold_class() :: lww | mv | aw | presence.
-type shard_strategy() :: realm | key.
-type db_name() :: core | registry.
-type table_spec() :: #{
    name := atom(),
    db := db_name(),
    durability := durable | ephemeral,
    shard_by := shard_strategy(),
    fold := fold_class(),
    %% `true` once the domain reads/writes bondy_db (always provisioned).
    migrated => boolean(),
    %% `true` to wire the table's appliers to publish change events.
    publish => boolean()
}.

-export_type([table_spec/0]).
-export_type([fold_class/0]).
-export_type([shard_strategy/0]).

%% API
-export([core_db/0]).
-export([core_db_spec/0]).
-export([fold_opts/1]).
-export([info/0]).
-export([is_open/0]).
-export([provision_all/0]).
-export([registry_db_spec/0]).
-export([start_link/0]).
-export([table/1]).
-export([tables/0]).

%% GEN_SERVER CALLBACKS
-export([init/1]).
-export([handle_call/3]).
-export([handle_cast/2]).
-export([handle_info/2]).
-export([terminate/2]).



%% =============================================================================
%% API
%% =============================================================================



-doc "Starts the catalogue process (a `bondy_sup` child).".
-spec start_link() -> {ok, pid()} | {error, term()}.

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).


-doc """
Returns the declarative specs for all twelve tables (both DBs), mirroring the
`bondy_plum_db.hrl` prefixes. The single source of truth for the catalogue.
""".
-spec tables() -> [table_spec()].

tables() ->
    [
        %% core — durable (leveled, shared_shards)
        #{name => ?PLUM_DB_REALM_TAB,        db => core, durability => durable, shard_by => realm, fold => lww},
        #{name => ?PLUM_DB_USER_TAB,         db => core, durability => durable, shard_by => realm, fold => lww},
        #{name => ?PLUM_DB_GROUP_TAB,        db => core, durability => durable, shard_by => realm, fold => lww},
        %% security_group_members — net-new split table (no plum_db prefix).
        %% Membership is its own observed-remove map so a concurrent add
        %% survives a remove that did not observe it (design §3, table 5b),
        %% rather than the inline LWW member list `security_groups` keeps today.
        #{name => security_group_members,    db => core, durability => durable, shard_by => realm, fold => aw},
        #{name => ?PLUM_DB_GROUP_GRANT_TAB,  db => core, durability => durable, shard_by => realm, fold => mv},
        #{name => ?PLUM_DB_USER_GRANT_TAB,   db => core, durability => durable, shard_by => realm, fold => mv},
        #{name => ?PLUM_DB_SOURCE_TAB,       db => core, durability => durable, shard_by => realm, fold => mv},
        %% api_gateway — first domain cut over to bondy_db (§11.4): always
        %% provisioned, and publishes change events so the cowboy-dispatch
        %% reactor rebuilds on local + AE-replicated spec writes.
        #{name => api_gateway,               db => core, durability => durable, shard_by => realm, fold => lww, migrated => true, publish => true},
        %% ticket / oauth_token shard by key — creation + point lookup are
        %% prioritised over listing / range (mirrors the plum_db rationale).
        #{name => ?PLUM_DB_TICKET_TAB,       db => core, durability => durable, shard_by => key,   fold => lww},
        #{name => ?PLUM_DB_OAUTH_TOKEN_TAB,  db => core, durability => durable, shard_by => key,   fold => lww},
        #{name => bondy_bridge_relay,        db => core, durability => durable, shard_by => realm, fold => lww},

        %% registry — ephemeral (ETS, memory topology); opened at §11.4. The
        %% fold is the presence-FSM, co-designed with the routing redesign (D-7).
        #{name => ?PLUM_DB_REGISTRATION_TAB, db => registry, durability => ephemeral, shard_by => realm, fold => presence},
        #{name => ?PLUM_DB_SUBSCRIPTION_TAB, db => registry, durability => ephemeral, shard_by => realm, fold => presence}
    ].


-doc """
The `core` DB declaration: durable shared-shards over leveled, with a
deployment-configurable shard count (design D-5, default 16).
""".
-spec core_db_spec() -> map().

core_db_spec() ->
    #{
        name => core,
        topology => bondy_db_topology_shared_shards,
        durability => durable,
        shard_count => core_shard_count()
    }.


-doc """
The `registry` DB declaration: ephemeral ETS, with the four explicit knobs
that pin the whole stack in-memory and avoid the disk-WAL footgun (design
D-6). Declared now; opened when the registry domain cuts over (§11.4).
""".
-spec registry_db_spec() -> map().

registry_db_spec() ->
    #{
        name => registry,
        topology => bondy_db_topology_memory,
        durability => ephemeral,
        %% The four ephemeral knobs, applied per-table at open time (§11.4).
        table_opts => #{
            projection_backend => ets,
            oplog_instance_opts => #{
                backend => ets,
                wal_backend => mem,
                durability => ephemeral
            },
            fused => true
        }
    }.


-doc """
The `bondy_db:open_table/3` options that wire a table's fold class to its
native CRDT — the per-table "WAMP fold module" selection (design §11.3).

- `lww` → `lww_register` (set / clear, highest-HLC wins): realm, user and
  group records, the API gateway spec, tickets, tokens, bridge relays.
- `mv`  → `lww_register` carrier + the `mv_register` CRDT: grants and sources.
  Concurrent writes to the same `(realm, principal, resource)` survive as
  siblings, so the auth layer can refuse / alert instead of silently
  accepting an LWW winner.
- `aw`  → `lww_register` carrier + the `aw_map` CRDT: group membership. A
  concurrent add survives a remove that did not observe it.

`mv_register` / `aw_map` have no short fold alias in `bondy_oplog_cell_kernel`,
so they are passed as an explicit `crdt_module` (the `fold_module` stays
`lww_register`, their byte-compatible carrier). `presence` (the registry
tables) is deferred with the registry domain (design §11.4 / D-7) and has no
mapping yet.
""".
-spec fold_opts(fold_class()) -> map().

fold_opts(lww) ->
    #{fold_module => lww_register};
fold_opts(mv) ->
    #{fold_module => lww_register, crdt_module => ?MV_CRDT};
fold_opts(aw) ->
    #{fold_module => lww_register, crdt_module => ?AW_CRDT};
fold_opts(presence) ->
    %% Registry presence-FSM fold — deferred with the registry domain
    %% (design §11.4 / D-7); registry tables are not opened yet.
    error({not_yet_supported, presence}).


-doc "The published `core` DB handle, or `undefined` when not open.".
-spec core_db() -> bondy_db:db() | undefined.

core_db() ->
    persistent_term:get(?PT_DB(core), undefined).


-doc "The published handle for table `Name`, or `undefined` when not open.".
-spec table(Name :: atom()) -> bondy_db:table() | undefined.

table(Name) when is_atom(Name) ->
    persistent_term:get(?PT_TABLE(Name), undefined).


-doc "Whether the `core` DB has been provisioned and published.".
-spec is_open() -> boolean().

is_open() ->
    core_db() =/= undefined.


-doc """
Whether the `oplog.catalog` flag is set, i.e. whether ALL declared core tables
are provisioned (not just the migrated ones). Off by default.
""".
-spec provision_all() -> boolean().

provision_all() ->
    application:get_env(bondy_router, oplog_catalog_enabled, false) =:= true.


-doc """
A summary of the catalogue: the `provision_all` flag, the `core` DB info and
each core table's `bondy_db:info/1` (or `not_open`).
""".
-spec info() -> map().

info() ->
    #{
        provision_all => provision_all(),
        core => case core_db() of
            undefined -> not_open;
            Db -> bondy_db:info(Db)
        end,
        tables => maps:from_list([
            {Name, table_info(Name)}
         || #{name := Name, db := core} <- tables()
        ])
    }.



%% =============================================================================
%% GEN_SERVER CALLBACKS
%% =============================================================================



init([]) ->
    %% Trap exits so terminate/2 runs on supervised shutdown (to close the DB)
    %% and so a leveled-sup crash surfaces as an EXIT message we can act on.
    process_flag(trap_exit, true),
    case specs_to_open() of
        [] ->
            %% No migrated domains and the `oplog.catalog` flag off — nothing
            %% to provision; every read still flows through plum_db.
            ?LOG_NOTICE(#{
                description =>
                    "bondy_db namespace catalogue idle; no core tables to "
                    "provision (reads continue via plum_db)"
            }),
            {ok, #state{}};
        Specs ->
            case do_open_core(Specs) of
                {ok, Db, Sup, Dir} ->
                    {ok, #state{db = Db, leveled_sup = Sup, dir = Dir}};
                {error, Reason} ->
                    %% Don't brick the node over a migration feature — log
                    %% loudly and start idle (is_open/0 stays false).
                    ?LOG_ERROR(#{
                        description =>
                            "Failed to provision bondy_db core tables; "
                            "catalogue starting idle",
                        reason => Reason
                    }),
                    {ok, #state{}}
            end
    end.


handle_call(_Request, _From, State) ->
    {reply, {error, badcall}, State}.


handle_cast(_Msg, State) ->
    {noreply, State}.


handle_info({'EXIT', Sup, Reason}, #state{leveled_sup = Sup} = State) ->
    %% Our leveled sup died — stop so bondy_sup restarts us and re-opens.
    ?LOG_ERROR(#{
        description => "bondy_db core leveled supervisor died",
        reason => Reason
    }),
    {stop, {leveled_sup_died, Reason}, State#state{leveled_sup = undefined}};

handle_info(_Info, State) ->
    {noreply, State}.


terminate(_Reason, #state{db = Db, leveled_sup = Sup}) ->
    _ = close_core(Db, Sup),
    ok.



%% =============================================================================
%% PRIVATE
%% =============================================================================



%% @private
%% The core table specs to provision at boot: the migrated ones always, plus
%% every core table when the `oplog.catalog` flag is set.
specs_to_open() ->
    Core = [S || #{db := core} = S <- tables()],
    case provision_all() of
        true -> Core;
        false -> [S || S <- Core, maps:get(migrated, S, false)]
    end.


%% @private
do_open_core(Specs) ->
    Spec = core_db_spec(),
    ShardCount = maps:get(shard_count, Spec),
    Dir = core_dir(),
    ok = filelib:ensure_path(Dir),
    case bondy_db_leveled_sup:start_link() of
        {ok, Sup} ->
            DbOpts = #{
                topology => maps:get(topology, Spec),
                topology_opts => #{sup => Sup, dir => Dir},
                shard_count => ShardCount,
                %% DB default fold; mv tables override via per-table crdt_module.
                fold_module => lww_register
            },
            case bondy_db:open(core, DbOpts) of
                {ok, Db} ->
                    ok = put_db(core, Db),
                    case open_tables(Db, Specs) of
                        ok ->
                            ?LOG_NOTICE(#{
                                description =>
                                    "bondy_db core tables provisioned",
                                count => length(Specs),
                                tables => [maps:get(name, S) || S <- Specs],
                                shard_count => ShardCount,
                                dir => Dir
                            }),
                            {ok, Db, Sup, Dir};
                        {error, _} = Err ->
                            _ = close_core(Db, Sup),
                            Err
                    end;
                {error, _} = Err ->
                    _ = stop_sup(Sup),
                    Err
            end;
        {error, _} = Err ->
            Err
    end.


%% @private
open_tables(_Db, []) ->
    ok;
open_tables(Db, [#{name := Name} = Spec | Rest]) ->
    case bondy_db:open_table(Db, Name, table_opts(Spec)) of
        {ok, Table} ->
            ok = put_table(Name, Table),
            open_tables(Db, Rest);
        {error, _} = Err ->
            Err
    end.


%% @private
%% Closes every open core table, the DB, and the leveled sup; clears the
%% published handles. Tolerant of partial state (any of Db / Sup undefined).
close_core(Db, Sup) ->
    _ = [
        begin
            _ = catch bondy_db:close_table(T),
            _ = persistent_term:erase(?PT_TABLE(Name))
        end
     || #{name := Name, db := core} <- tables(),
        (T = table(Name)) =/= undefined
    ],
    _ = case Db of
        undefined -> ok;
        _ ->
            _ = catch bondy_db:close(Db),
            persistent_term:erase(?PT_DB(core))
    end,
    _ = stop_sup(Sup),
    ok.


%% @private
stop_sup(undefined) ->
    ok;
stop_sup(Sup) when is_pid(Sup) ->
    catch bondy_db_leveled_sup:stop(Sup),
    ok.


%% @private
%% Maps a table spec to its `bondy_db:open_table/3` opts: the fold→CRDT wiring
%% (see `fold_opts/1`) plus `publish` for tables with a change reactor.
%% `shard_by` is NOT passed — `open_table` does not yet honour realm-sharding
%% (§11.4).
table_opts(#{fold := Class} = Spec) ->
    Opts = fold_opts(Class),
    case maps:get(publish, Spec, false) of
        true -> Opts#{publish => true};
        false -> Opts
    end.


%% @private
table_info(Name) ->
    case table(Name) of
        undefined -> not_open;
        Table -> bondy_db:info(Table)
    end.


%% @private
core_shard_count() ->
    application:get_env(
        bondy_router, oplog_core_shard_count, ?DEFAULT_CORE_SHARD_COUNT
    ).


%% @private
core_dir() ->
    DataDir = application:get_env(bondy_router, platform_data_dir, "data"),
    filename:join([DataDir, "bondy_db", "core"]).


%% @private
put_db(Name, Db) ->
    persistent_term:put(?PT_DB(Name), Db),
    ok.


%% @private
put_table(Name, Table) ->
    persistent_term:put(?PT_TABLE(Name), Table),
    ok.
