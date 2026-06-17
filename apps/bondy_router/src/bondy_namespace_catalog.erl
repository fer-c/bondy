%% =============================================================================
%% SPDX-FileCopyrightText: 2016 - 2026 Leapsight
%% SPDX-License-Identifier: Apache-2.0
%% =============================================================================

-module(bondy_namespace_catalog).
-behaviour(gen_server).
-moduledoc """
The single declaration point for Bondy's `bondy_db` databases and tables —
the `bondy_db` analogue of `bondy_db_tables.hrl`'s `?BONDY_DB_PREFIXES` — and the
owner process for the durable `core` database.

Two databases are declared:

- **`core`** — durable (`bondy_db_topology_shared_shards` over leveled),
  holding the twelve security / realm / gateway / token / bridge / retention
  tables.
- **`registry`** — ephemeral (`bondy_db_topology_memory`, ETS), holding the
  two routing tables (registrations / subscriptions).

The table names mirror the `bondy_db_tables.hrl` prefixes (plus the net-new
`security_group_members` split table, design §3 — group membership lives in its
own `aw_map` table rather than inline in `security_groups` — and
`retained_messages`, which had no `?BONDY_DB_*` macro). Each table records
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
(currently `bondy_realm`, `api_gateway`, `bondy_bridge_relay`, `bondy_ticket`,
`bondy_oauth_token`, `security_users`, `security_groups`,
`security_user_grants`, `security_group_grants`, `security_sources` and
`retained_messages` in `core`, plus `bondy_registration` / `bondy_subscription`
in `registry`) and serves every other read from `plum_db`.

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

-include("bondy_db_tables.hrl").

-define(PT_DB(Name), {?MODULE, db, Name}).
-define(PT_TABLE(Name), {?MODULE, table, Name}).
-define(DEFAULT_CORE_SHARD_COUNT, 16).
-define(DEFAULT_REGISTRY_SHARD_COUNT, 16).

%% The native CRDTs that have no short fold alias in `bondy_oplog_cell_kernel`
%% (`mv_register` for grants / sources, `aw_map` for group membership). They are
%% passed as an explicit `crdt_module`; the `fold_module` stays `lww_register`,
%% their byte-compatible carrier — see the mv_register / aw_map e2e tests.
-define(MV_CRDT, bondy_oplog_crdt_mv_register).
-define(AW_CRDT, bondy_oplog_crdt_aw_map).

-record(state, {
    db :: bondy_db:db() | undefined,
    leveled_sup :: pid() | undefined,
    dir :: file:filename_all() | undefined,
    %% The ephemeral `registry` DB (memory topology — no leveled sup / dir),
    %% provisioned alongside `core` when its tables are migrated (D-7).
    registry_db :: bondy_db:db() | undefined
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
    publish => boolean(),
    %% Declared secondary indexes (substrate-maintained reverse access
    %% paths), passed verbatim to `bondy_db:open_table/3`.
    indexes => [bondy_oplog_index_spec:spec()]
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
-export([registry_db/0]).
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
Returns the declarative specs for all fourteen tables (both DBs), mirroring the
`bondy_db_tables.hrl` prefixes. The single source of truth for the catalogue.
""".
-spec tables() -> [table_spec()].

tables() ->
    [
        %% core — durable (leveled, shared_shards)
        %% bondy_realm — ninth domain cut over to bondy_db (§11.4). Unlike the
        %% per-realm tables this is a GLOBAL registry: every realm shares one
        %% band (the empty binary) keyed by its Uri, so `shard_by => key`
        %% (NOT realm — a constant band under realm-sharding would put every
        %% realm on one shard) spreads realms across shards while a single
        %% `bondy_db:list/2` over the band scatter-scans them all. Local
        %% lifecycle is inline in bondy_realm; `publish => true` wires the remote
        %% on_merge seam so a peer's realm delete closes this node's sessions for
        %% that realm (the reactor is `bondy_aae_reactor`).
        #{
            name => ?BONDY_DB_REALM_TAB,
            db => core,
            durability => durable,
            shard_by => key,
            fold => lww,
            migrated => true,
            publish => true
        },
        %% security_users — fifth domain cut over to bondy_db (§11.4): always
        %% provisioned. Local lifecycle side-effects fire inline in
        %% bondy_rbac_user; `publish => true` wires the remote on_merge seam so
        %% a peer's user delete / credential change closes this node's sessions
        %% for that user (the reactor is `bondy_rbac_user`'s merge handler).
        #{
            name => ?BONDY_DB_USER_TAB,
            db => core,
            durability => durable,
            shard_by => realm,
            fold => lww,
            migrated => true,
            publish => true,
            indexes => user_indexes()
        },
        %% security_groups — sixth domain cut over to bondy_db (§11.4): always
        %% provisioned, storage-only (no `publish`). Local lifecycle events fire
        %% inline in bondy_rbac_group; on_merge was a no-op.
        #{
            name => ?BONDY_DB_GROUP_TAB,
            db => core,
            durability => durable,
            shard_by => realm,
            fold => lww,
            migrated => true
        },
        %% security_group_members — net-new split table (no plum_db prefix).
        %% The reverse membership READ path ("which users are in group G") is
        %% NOT this table: it is the substrate-maintained `by_group` secondary
        %% index on `security_users` (see `user_indexes/0`), which rides on the
        %% authoritative lww `user.groups`. This table stays DORMANT (not
        %% `migrated`) for the oplog.aae phase, where it becomes the *add-wins*
        %% forward membership relation (the `user.groups` → aw_map split, design
        %% §3 table 5b / D-R1) — its `aw` fold (observed-remove) only matters
        %% under concurrent multi-node member edits, which need AAE (off today).
        #{
            name => security_group_members,
            db => core,
            durability => durable,
            shard_by => realm,
            fold => aw
        },
        %% security_{group,user}_grants — seventh domain cut over to bondy_db
        %% (§11.4): always provisioned, storage-only (no `publish` — grants carry
        %% no lifecycle side-effects). Declared `mv` (sibling-preserving) but cut
        %% as `lww` per the CRDT-fork resolution: mv only differs from lww under
        %% concurrent multi-node grant edits, which need AAE (currently off), so
        %% honouring mv is deferred to the oplog.aae phase (same deferral as the
        %% dropped ticket resolver). The compound `{Rolename, Resource}` key is an
        %% order-preserving composite (`bondy_rbac:encode_key/1`) so the forward
        %% "grants for role" query is a bounded role-band range scan; the
        %% `by_resource` index (piece #2) provides the equality reverse lookup
        %% "grants on resource R" (see `grant_indexes/0`).
        #{
            name => ?BONDY_DB_GROUP_GRANT_TAB,
            db => core,
            durability => durable,
            shard_by => realm,
            fold => lww,
            migrated => true,
            indexes => grant_indexes()
        },
        #{
            name => ?BONDY_DB_USER_GRANT_TAB,
            db => core,
            durability => durable,
            shard_by => realm,
            fold => lww,
            migrated => true,
            indexes => grant_indexes()
        },
        %% security_sources — eighth domain cut over to bondy_db (§11.4):
        %% storage-only, same lww-defer as grants (declared mv → cut lww;
        %% honouring mv deferred to oplog.aae). The compound `{Username, AMask,
        %% Authmethod}` key is an order-preserving composite
        %% (`bondy_rbac_source:encode_key/1`) so the forward "sources for user"
        %% match (on the auth path) is a bounded username-band range scan. The
        %% reverse by-mask lookup is deferred (the stored `cidr` differs from the
        %% key's anchor-mask, and CIDR matching is containment, not equality).
        #{
            name => ?BONDY_DB_SOURCE_TAB,
            db => core,
            durability => durable,
            shard_by => realm,
            fold => lww,
            migrated => true
        },
        %% api_gateway — first domain cut over to bondy_db (§11.4): always
        %% provisioned, and publishes change events so the cowboy-dispatch
        %% reactor rebuilds on local + AE-replicated spec writes.
        #{
            name => api_gateway,
            db => core,
            durability => durable,
            shard_by => realm,
            fold => lww,
            migrated => true,
            publish => true
        },
        %% ticket / oauth_token shard by key — creation + point lookup are
        %% prioritised over listing / range (mirrors the plum_db rationale).
        %% Third/fourth domains cut over to bondy_db (§11.4): always provisioned,
        %% storage-only (no `publish` — revocation is inline, design D-3; nothing
        %% subscribes to ticket/token changes).
        #{
            name => ?BONDY_DB_TICKET_TAB,
            db => core,
            durability => durable,
            shard_by => key,
            fold => lww,
            migrated => true
        },
        #{
            name => ?BONDY_DB_OAUTH_TOKEN_TAB,
            db => core,
            durability => durable,
            shard_by => key,
            fold => lww,
            migrated => true
        },
        %% bridge_relay — second domain cut over to bondy_db (§11.4): always
        %% provisioned. Storage-only (no `publish`): bridge config has no
        %% change reactor — `bondy_bridge_relay_manager` reads it once at boot
        %% and runs only its OWN node's bridges (`nodestring` filter), so it
        %% needs no cluster-wide change notification.
        #{
            name => bondy_bridge_relay,
            db => core,
            durability => durable,
            shard_by => realm,
            fold => lww,
            migrated => true
        },
        %% retained_messages — WAMP retained-event store cut over to bondy_db
        %% (§11.4). A DURABLE core table regardless of the legacy
        %% `wamp.message_retention.storage_type` knob (now inert): the operator
        %% decision is that retained messages always survive a restart. Always
        %% provisioned — the feature is gated per-publish by the `retain`
        %% option, not at provisioning. Storage-only `lww`: the per-realm count
        %% / memory counters are maintained inline at the local write sites; the
        %% remote-replication counter sync (the retired plum_db `object_update`
        %% subscription) is deferred to the oplog.aae phase. Keyed by Topic and
        %% matched via key-ordered `range_all/5` prefix / wildcard scans (no
        %% secondary index).
        #{
            name => retained_messages,
            db => core,
            durability => durable,
            shard_by => realm,
            fold => lww,
            migrated => true
        },

        %% registry — tenth / last domain cut over to bondy_db (§11.4 / D-7):
        %% ephemeral (ETS projection, mem WAL, memory topology — NO durable or
        %% disk-backed storage, exactly like the plum_db `type => ram` tables it
        %% replaces), provisioned when migrated. Storage-only and cut as `lww`:
        %% the presence-FSM fold, SUSPEND/RESUME/EVICT and the remote change
        %% reactor (replacing the plum_db `on_merge`) are deferred to the
        %% oplog.aae phase — with AAE off there are no remote entries, so the
        %% merge machinery is inert. The durable key is the random realm-unique
        %% `entry_id`; the `by_session` index (registry_indexes/0) serves
        %% session-close cleanup (`remove_all`) as a bounded reverse lookup
        %% instead of a realm scan.
        #{
            name => ?BONDY_DB_REGISTRATION_TAB,
            db => registry,
            durability => ephemeral,
            shard_by => realm,
            fold => lww,
            migrated => true,
            indexes => registry_indexes()
        },
        #{
            name => ?BONDY_DB_SUBSCRIPTION_TAB,
            db => registry,
            durability => ephemeral,
            shard_by => realm,
            fold => lww,
            migrated => true,
            indexes => registry_indexes()
        }
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
        shard_count => registry_shard_count(),
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

-doc "The published ephemeral `registry` DB handle, or `undefined`.".
-spec registry_db() -> bondy_db:db() | undefined.

registry_db() ->
    persistent_term:get(?PT_DB(registry), undefined).

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
        core =>
            case core_db() of
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
    %% Trap exits so terminate/2 runs on supervised shutdown (to close the DBs)
    %% and so a leveled-sup crash surfaces as an EXIT message we can act on.
    process_flag(trap_exit, true),
    %% Provision the durable `core` and the ephemeral `registry` DBs
    %% independently — either may be idle (no migrated tables) without
    %% affecting the other.
    State0 = open_core_into(#state{}),
    State = open_registry_into(State0),
    {ok, State}.

%% @private
open_core_into(State) ->
    case specs_to_open() of
        [] ->
            %% No migrated core domains and the `oplog.catalog` flag off —
            %% nothing to provision; every such read still flows through plum_db.
            ?LOG_NOTICE(#{
                description =>
                    "bondy_db namespace catalogue idle; no core tables to "
                    "provision (reads continue via plum_db)"
            }),
            State;
        Specs ->
            case do_open_core(Specs) of
                {ok, Db, Sup, Dir} ->
                    State#state{db = Db, leveled_sup = Sup, dir = Dir};
                {error, Reason} ->
                    %% Don't brick the node over a migration feature — log
                    %% loudly and leave core idle (is_open/0 stays false).
                    ?LOG_ERROR(#{
                        description =>
                            "Failed to provision bondy_db core tables; "
                            "catalogue starting with core idle",
                        reason => Reason
                    }),
                    State
            end
    end.

%% @private
open_registry_into(State) ->
    case registry_specs_to_open() of
        [] ->
            State;
        Specs ->
            case do_open_registry(Specs) of
                {ok, Db} ->
                    State#state{registry_db = Db};
                {error, Reason} ->
                    ?LOG_ERROR(#{
                        description =>
                            "Failed to provision bondy_db registry tables; "
                            "catalogue starting with registry idle",
                        reason => Reason
                    }),
                    State
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

terminate(_Reason, #state{db = Db, leveled_sup = Sup, registry_db = RegistryDb}) ->
    _ = close_core(Db, Sup),
    _ = close_registry(RegistryDb),
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
%% The registry table specs to provision at boot: the migrated ones. The
%% `oplog.catalog` flag gates only the durable core tables, not the ephemeral
%% registry — it comes up exactly when its tables carry `migrated => true`.
registry_specs_to_open() ->
    [S || #{db := registry} = S <- tables(), maps:get(migrated, S, false)].

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
%% Provision the ephemeral `registry` DB (memory topology — no leveled sup or
%% on-disk dir) and its migrated tables. The per-table ephemeral knobs
%% (projection_backend / oplog_instance_opts / fused) ride in via `table_opts/1`
%% from `registry_db_spec/0`.
do_open_registry(Specs) ->
    Spec = registry_db_spec(),
    ShardCount = maps:get(shard_count, Spec),
    DbOpts = #{
        topology => maps:get(topology, Spec),
        shard_count => ShardCount,
        %% DB default fold (lww); the memory topology hosts the ETS projection.
        fold_module => lww_register,
        %% Pin the WAL in-memory at the DB level too; the per-table
        %% `oplog_instance_opts` (registry_db_spec/0) carry the full ephemeral
        %% knobs and replace this at open_table time.
        oplog_instance_opts => #{wal_backend => mem, durability => ephemeral}
    },
    case bondy_db:open(registry, DbOpts) of
        {ok, Db} ->
            ok = put_db(registry, Db),
            case open_tables(Db, Specs) of
                ok ->
                    ?LOG_NOTICE(#{
                        description => "bondy_db registry tables provisioned",
                        count => length(Specs),
                        tables => [maps:get(name, S) || S <- Specs],
                        shard_count => ShardCount
                    }),
                    {ok, Db};
                {error, _} = Err ->
                    _ = close_registry(Db),
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
    _ =
        case Db of
            undefined ->
                ok;
            _ ->
                _ = catch bondy_db:close(Db),
                persistent_term:erase(?PT_DB(core))
        end,
    _ = stop_sup(Sup),
    ok.

%% @private
%% Closes every open registry table and the registry DB; clears the published
%% handles. The memory topology owns no leveled sup / on-disk dir, so this is
%% simpler than close_core/2. Tolerant of `undefined` (registry idle).
close_registry(undefined) ->
    ok;
close_registry(Db) ->
    _ = [
        begin
            _ = catch bondy_db:close_table(T),
            _ = persistent_term:erase(?PT_TABLE(Name))
        end
     || #{name := Name, db := registry} <- tables(),
        (T = table(Name)) =/= undefined
    ],
    _ = catch bondy_db:close(Db),
    _ = persistent_term:erase(?PT_DB(registry)),
    ok.

%% @private
stop_sup(undefined) ->
    ok;
stop_sup(Sup) when is_pid(Sup) ->
    catch bondy_db_leveled_sup:stop(Sup),
    ok.

%% @private
%% Maps a table spec to its `bondy_db:open_table/3` opts: the fold→CRDT wiring
%% (see `fold_opts/1`), `publish` for tables with a change reactor, and any
%% declared secondary `indexes`. `shard_by` is NOT passed — `open_table` does
%% not yet honour realm-sharding (§11.4).
table_opts(#{fold := Class} = Spec) ->
    Opts0 = fold_opts(Class),
    Opts1 =
        case maps:get(publish, Spec, false) of
            true -> Opts0#{publish => true};
            false -> Opts0
        end,
    Opts2 =
        case maps:get(indexes, Spec, []) of
            [] -> Opts1;
            Indexes -> Opts1#{indexes => Indexes}
        end,
    maybe_ephemeral_opts(Spec, Opts2).

%% @private
%% Registry (ephemeral, memory-topology) tables carry the in-RAM projection /
%% WAL knobs at the DB-spec level (registry_db_spec/0); merge them under the
%% fold + index opts (the key sets are disjoint). Core tables pass through.
maybe_ephemeral_opts(#{db := registry}, Opts) ->
    maps:merge(maps:get(table_opts, registry_db_spec()), Opts);
maybe_ephemeral_opts(#{db := core, durability := durable}, Opts) ->
    %% Make each durable core table's per-shard WAL + MST pack durable, rooted
    %% under the data dir (collocated with the leveled projection) instead of
    %% the ephemeral `/tmp` fallback (which abandons fsynced frames on restart
    %% and keeps no MST pack on disk). Without this the DB-level
    %% `durability => durable` never reaches the oplog instances.
    %%
    %% The MST pack store (`storage_path`) and the WAL (`wal_dir`) live in their
    %% own sibling subtrees alongside the leveled `core' dir — see
    %% `bondy_db_dir/0`. An explicit `wal_dir` (rather than letting the WAL
    %% default to a `wal/' dir *under* the pack instance dir) keeps the WAL leaf
    %% at `wal/<InstanceId>' (`wal/core/<ET>/<Shard>') instead of the doubly
    %% nested `mst/.../<InstanceId>/wal/<InstanceId>'. The pack store keeps the
    %% default `sharded' path layout (`mst/<hash>/<hash>/<InstanceId>'); `flat'
    %% is unsafe here because the slash-bearing `InstanceId' makes the
    %% pack-store's shard-dir derivation double-nest the pack away from its
    %% manifest.
    %%
    %% `seed => true` starts each instance live as a genesis peer and writes a
    %% durable `lifecycle.live` flag that survives restart. A fresh persistent
    %% instance with the default `seed => false` would instead block in
    %% `pre_bootstrap` waiting for a live peer to bootstrap from — which a single
    %% node never has. Under multi-node AAE every node genesis-seeds and the lww
    %% merge reconciles their cells (proven by `bondy_aae_cluster_SUITE`).
    Opts#{
        oplog_instance_opts => #{
            backend => bondy_mst_pack_store,
            storage_path => core_mst_dir(),
            wal_dir => core_wal_dir(),
            seed => true
        }
    };
maybe_ephemeral_opts(_Spec, Opts) ->
    Opts.

%% @private
%% The `security_users` secondary indexes.
%%
%% `by_group` is the **reverse membership access path** — the `member`
%% relation's reverse direction (design §2.3 / D-R6). A multi-valued index
%% over the user record's `groups` list yields one entry per (group, user),
%% so "which users are in group G" is a bounded `bondy_db:index_get/5`
%% instead of the O(all-users) realm scan group deletion used to require.
%%
%% Terms are stored verbatim (`normalize => none`): `user.groups` is already
%% casefolded by `bondy_data_validators:groupnames/1` at write, and the query
%% side casefolds identically via `bondy_rbac_group:normalise_name/1`, so the
%% query term matches the stored term exactly. The substrate maintains the
%% index on every user write and removes every entry on delete; the forward
%% direction stays the authoritative lww `user.groups`.
user_indexes() ->
    [#{name => by_group, extract => [groups]}].

%% @private
%% The equality reverse index for grants (piece #2): "which roles have a grant
%% on resource R". The grant cell value is the fact map
%% `#{resource => Resource, permissions => [_]}` (reshaped from the bare
%% permissions list precisely so the resource column is reachable from the
%% value), and `normalize => canonical` maps the structured resource
%% (`any | {Uri, Strategy}`) to its deterministic binary so the lookup term
%% matches byte-for-byte. The reverse read (`bondy_rbac:grants_on_resource/2`)
%% decodes each hit's primary key to recover the role. Both grant tables share
%% the same `by_resource` name; co-located/per-table index scoping keeps them
%% distinct.
grant_indexes() ->
    [#{name => by_resource, extract => [resource], normalize => canonical}].

%% @private
%% The registry's reverse access path for session-close cleanup (D-7): "which
%% entries belong to session S". The registry cell value is the thin fact map
%% `#{session_id => SId, entry => Entry}` (the `#entry{}` record preserved
%% verbatim under `entry`, with `session_id` denormalised to the top level
%% precisely so this pointer-only index can extract it). `bondy_registry`'s
%% `remove_all/_` resolves a session's entries through `bondy_db:index_get/5`
%% (bounded) instead of a realm scan + filter. A session-less entry (callback /
%% internal registration, `session_id => undefined`) yields no index entry —
%% correct, since session-close never targets it.
registry_indexes() ->
    [#{name => by_session, extract => [session_id]}].

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
registry_shard_count() ->
    application:get_env(
        bondy_router, oplog_registry_shard_count, ?DEFAULT_REGISTRY_SHARD_COUNT
    ).

%% @private
%% Root of the on-disk layout for all bondy_db data, configurable via the
%% `platform_data_dir' schema knob. The durable `core' DB keeps its three
%% storage components in sibling subtrees under here:
%%
%%   <data>/bondy_db/core   leveled projection      (shards `core/0'..`core/N')
%%   <data>/bondy_db/mst    MST pack store          (`mst/<InstanceId>/...')
%%   <data>/bondy_db/wal    write-ahead log         (`wal/<InstanceId>/...')
%%
%% with `InstanceId = core/<EntityType>/<Shard>'. `mst' and `wal' are siblings
%% of `core' (not nested under it), and `path_layout => flat' keeps each leaf at
%% `<base>/core/<ET>/<Shard>' rather than under opaque hash dirs.
bondy_db_dir() ->
    DataDir = application:get_env(bondy_router, platform_data_dir, "data"),
    filename:join([DataDir, "bondy_db"]).

%% @private
core_dir() ->
    filename:join([bondy_db_dir(), "core"]).

%% @private
core_mst_dir() ->
    unicode:characters_to_binary(filename:join([bondy_db_dir(), "mst"])).

%% @private
core_wal_dir() ->
    unicode:characters_to_binary(filename:join([bondy_db_dir(), "wal"])).

%% @private
put_db(Name, Db) ->
    persistent_term:put(?PT_DB(Name), Db),
    ok.

%% @private
put_table(Name, Table) ->
    persistent_term:put(?PT_TABLE(Name), Table),
    ok.
