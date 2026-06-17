%% =============================================================================
%% SPDX-FileCopyrightText: 2016 - 2026 Leapsight
%% SPDX-License-Identifier: Apache-2.0
%% =============================================================================

-module(bondy_export).
-moduledoc """
A `gen_server` that exports and imports the Bondy database, running the work
asynchronously and writing to (or reading from) a `disk_log` file while
tracking progress and emitting lifecycle events.

This is a logical **export/import** of the durable `bondy_db` `core` tables
(security, realms, gateway specs, tokens, tickets, bridges, retained
messages) — not a byte-level backup. Each entry is dumped as a logical
`{entry, Table, Band, Key, Value}` tuple (the fold-decoded domain term) and
re-applied on import via `bondy_db:apply(Table, Band, Key, {set, Value})`.
Storage-level metadata (HLCs, CRDT lineage) is intentionally **not** preserved
— an import is a set of fresh writes, which is the correct semantics for moving
data between nodes / deployments.

Enumeration is domain-agnostic: every core table is listed over the band set
`[<<>> | RealmURIs]`. Per-realm tables (users, groups, grants, sources,
tickets, tokens, retained messages) hold their entries under each realm's URI
band; the global-band tables (realms, API gateway specs, bridges) hold theirs
under the constant `<<>>` band. The two never overlap, so the union covers
every table without per-table knowledge. The ephemeral `registry` (routing)
tables are not exported.

## Backwards compatibility

Import detects the file header. Files written by this module carry
`format => bondy_db_export`, `vsn => "2.0.0"`. The legacy `plum_db`-format
backups produced by the former `bondy_backup` module (`format => dvvset_log`,
`vsn =< "1.2.0"`) are recognised but importing them is **not yet supported**:
`import/1` rejects such a file with `{error, {legacy_format_unsupported, Vsn}}`
(`status/1` still reports its header). Translating them to `bondy_db` is parked
pending a real fixture to verify the per-domain key/value reshape — see the
§11.4 removal roadmap.

The administrative WAMP procedures are `bondy.export.create`,
`bondy.export.status` and `bondy.export.import`; the former `bondy.backup.*`
procedures are kept as deprecated aliases.
""".
-behaviour(gen_server).
-include_lib("kernel/include/logger.hrl").
-include("bondy.hrl").

%% The current (bondy_db) export file format + version.
-define(EXPORT_FORMAT, bondy_db_export).
-define(EXPORT_VSN, <<"2.0.0">>).
%% The legacy plum_db backup format written by the former bondy_backup module.
-define(LEGACY_FORMAT, dvvset_log).

-define(EXPORT_SPEC, #{
    <<"path">> => #{
        alias => path,
        key => path,
        required => true,
        allow_null => false,
        allow_undefined => false,
        validator => fun
            (X) when is_list(X) ->
                {ok, X};
            (X) when is_binary(X) ->
                {ok, unicode:characters_to_list(X)};
            (_) ->
                false
        end
    }
}).

-define(IMPORT_SPEC, #{
    <<"filename">> => #{
        alias => filename,
        key => filename,
        required => true,
        allow_null => false,
        allow_undefined => false,
        validator => fun
            (X) when is_list(X) ->
                {ok, X};
            (X) when is_binary(X) ->
                {ok, unicode:characters_to_list(X)};
            (_) ->
                false
        end
    }
}).

-define(STATUS_SPEC, #{
    <<"filename">> => #{
        alias => filename,
        key => filename,
        required => false,
        allow_null => false,
        allow_undefined => false,
        validator => fun
            (X) when is_list(X) ->
                {ok, X};
            (X) when is_binary(X) ->
                {ok, unicode:characters_to_list(X)};
            (_) ->
                false
        end
    }
}).

-record(state, {
    status :: status(),
    timestamp :: non_neg_integer(),
    pid :: pid() | undefined,
    filename :: file:filename() | undefined
}).

-type status() :: export_in_progress | import_in_progress | undefined.
-type info() :: #{
    filename => file:filename(),
    timestamp => non_neg_integer()
}.

%% API
-export([export/1]).
-export([import/1]).
-export([status/0]).
-export([status/1]).
-export([start_link/0]).

%% GEN_SERVER CALLBACKS
-export([init/1]).
-export([handle_info/2]).
-export([terminate/2]).
-export([code_change/3]).
-export([handle_call/3]).
-export([handle_cast/2]).

%% =============================================================================
%% API
%% =============================================================================

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

-doc """
Exports the database to a `disk_log` file in the directory indicated by `path`.
""".
-spec export(file:filename_all() | map()) ->
    {ok, info()} | {error, term()}.

export(Map0) when is_map(Map0) ->
    try maps_utils:validate(Map0, ?EXPORT_SPEC) of
        Map1 ->
            gen_server:call(?MODULE, {export, Map1})
    catch
        error:Reason ->
            {error, Reason}
    end;
export(Path) ->
    export(#{path => Path}).

status() ->
    status(#{}).

-spec status(file:filename_all() | map()) ->
    undefined | {status(), non_neg_integer()} | {error, unknown}.

status(Map0) when is_map(Map0) ->
    try maps_utils:validate(Map0, ?STATUS_SPEC) of
        Map1 ->
            gen_server:call(?MODULE, {status, Map1})
    catch
        error:Reason ->
            {error, Reason}
    end;
status(Filename) ->
    status(#{filename => Filename}).

-doc """
Imports an export file (or a legacy `bondy_backup` file).
""".
-spec import(file:filename_all() | map()) -> {ok, info()} | {error, term()}.

import(Map0) when is_map(Map0) ->
    try maps_utils:validate(Map0, ?IMPORT_SPEC) of
        Map1 ->
            gen_server:call(?MODULE, {import, Map1})
    catch
        error:Reason ->
            {error, Reason}
    end;
import(Filename) ->
    import(#{filename => Filename}).

%% =============================================================================
%% GEN_SERVER CALLBACKS
%% =============================================================================

init([]) ->
    {ok, #state{}}.

handle_call({export, Map}, _From, #state{status = undefined} = State0) ->
    {ok, State1} = async_export(Map, State0),
    Reply = #{
        filename => unicode:characters_to_binary(State1#state.filename),
        timestamp => State1#state.timestamp
    },
    {reply, {ok, Reply}, State1};
handle_call({export, _}, _From, State) ->
    {reply, {error, State#state.status}, State};
handle_call({import, Map}, _From, #state{status = undefined} = State0) ->
    {ok, State1} = async_import(Map, State0),
    Reply = #{
        filename => unicode:characters_to_binary(State1#state.filename),
        timestamp => State1#state.timestamp
    },
    {reply, {ok, Reply}, State1};
handle_call({import, _}, _From, State) ->
    {reply, {error, State#state.status}, State};
handle_call({status, Map}, _From, State) when map_size(Map) =:= 0 ->
    {reply, {ok, State#state.status}, State};
handle_call(
    {status, #{filename := Filename}},
    _From,
    #state{filename = Filename} = State
) ->
    Reply =
        case State#state.status of
            undefined ->
                read_head(Filename);
            Status ->
                Secs = erlang:system_time(second) - State#state.timestamp,
                {ok, #{status => Status, elapsed_time_secs => Secs}}
        end,
    {reply, Reply, State};
handle_call({status, #{filename := Filename}}, _From, State) ->
    {reply, read_head(Filename), State};
handle_call(_, _, State) ->
    {reply, ok, State}.

handle_cast(_Event, State) ->
    {noreply, State}.

handle_info({export_reply, ok, Pid}, #state{pid = Pid} = State) ->
    Secs = erlang:system_time(second) - State#state.timestamp,
    ok = notify_export_finished([State#state.filename, Secs]),
    {noreply, State#state{status = undefined, pid = undefined}};
handle_info({export_reply, {error, Reason}, Pid}, #state{pid = Pid} = State) ->
    Secs = erlang:system_time(second) - State#state.timestamp,
    ok = notify_export_error([Reason, State#state.filename, Secs]),
    {noreply, State#state{status = undefined, pid = undefined}};
handle_info({import_reply, {ok, Counters}, Pid}, #state{pid = Pid} = State) ->
    #{read_count := N, written_count := M} = Counters,
    Secs = erlang:system_time(second) - State#state.timestamp,
    ok = notify_import_finished([State#state.filename, Secs, N, M]),
    {noreply, State#state{status = undefined, pid = undefined}};
handle_info({import_reply, {error, Reason}, Pid}, #state{pid = Pid} = State) ->
    Secs = erlang:system_time(second) - State#state.timestamp,
    ok = notify_import_error([State#state.filename, Reason, Secs]),
    {noreply, State#state{status = undefined, pid = undefined}};
handle_info(Info, State) ->
    ?LOG_DEBUG(#{
        description => "Unexpected event received",
        event => Info
    }),
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% =============================================================================
%% PRIVATE: EXPORT
%% =============================================================================

%% @private
async_export(#{path := Path}, State0) ->
    Ts = erlang:system_time(second),
    Filename = "bondy_export." ++ integer_to_list(Ts) ++ ".bondy",
    File = filename:join([Path, Filename]),
    Me = self(),
    Pid = spawn_link(fun() ->
        case do_export(File, Ts) of
            ok ->
                Me ! {export_reply, ok, self()};
            {error, _} = Error ->
                Me ! {export_reply, Error, self()}
        end
    end),
    State1 = State0#state{
        filename = File,
        pid = Pid,
        timestamp = Ts,
        status = export_in_progress
    },
    {ok, State1}.

%% @private
do_export(File, Ts) ->
    Opts = [
        {name, log},
        {file, File},
        {type, halt},
        {size, infinity},
        {head, #{
            format => ?EXPORT_FORMAT,
            mod => ?MODULE,
            mod_vsn => mod_vsn(),
            node => erlang:node(),
            timestamp => Ts,
            vsn => ?EXPORT_VSN
        }}
    ],

    case disk_log:open(Opts) of
        {ok, Log} ->
            _ = notify_export_started(File),
            build_export(Log);
        {error, _} = Error ->
            Error
    end.

%% @private
mod_vsn() ->
    {vsn, Vsn} = lists:keyfind(vsn, 1, ?MODULE:module_info(attributes)),
    Vsn.

%% @private
build_export(Log) ->
    Bands = [<<>> | realm_uris()],
    Tables = core_table_names(),
    try
        Acc = lists:foldl(
            fun(Name, Acc0) -> export_table(Name, Bands, Log, Acc0) end,
            [],
            Tables
        ),
        %% Flush the remaining buffered entries.
        log(Acc, Log)
    catch
        throw:Reason ->
            {error, Reason}
    after
        disk_log:close(Log)
    end.

%% @private
%% Exports one core table over every band, buffering entries and flushing in
%% 500-entry batches (via `maybe_log/2`). Tables that are declared but not
%% provisioned on this node (handle `undefined`) are skipped.
export_table(Name, Bands, Log, Acc0) ->
    case bondy_namespace_catalog:table(Name) of
        undefined ->
            Acc0;
        Table ->
            lists:foldl(
                fun(Band, Acc1) ->
                    export_band(Name, Table, Band, Log, Acc1)
                end,
                Acc0,
                Bands
            )
    end.

%% @private
export_band(Name, Table, Band, Log, Acc0) ->
    case bondy_db:list(Table, Band) of
        {ok, Rows} ->
            lists:foldl(
                fun({Key, Value, _Hlc}, Acc) ->
                    maybe_log([{entry, Name, Band, Key, Value} | Acc], Log)
                end,
                Acc0,
                Rows
            );
        {error, Reason} ->
            throw(Reason)
    end.

%% @private
%% The durable `core` table names, in declaration order (realms first, so they
%% are imported before per-realm data).
core_table_names() ->
    [
        maps:get(name, S)
     || S <- bondy_namespace_catalog:tables(), maps:get(db, S) =:= core
    ].

%% @private
%% The URIs of all realms; drives per-realm table enumeration.
realm_uris() ->
    [
        Uri
     || R <- bondy_realm:list(), (Uri = bondy_realm:uri(R)) =/= undefined
    ].

%% @private
maybe_log(Acc, Log) when length(Acc) =:= 500 ->
    ok = log(Acc, Log),
    [];
maybe_log(Acc, _) ->
    Acc.

%% @private
log([], _) ->
    ok;
log(L, Log) ->
    ok = maybe_throw(disk_log:log_terms(Log, L)),
    maybe_throw(disk_log:sync(Log)).

%% @private
maybe_throw(ok) -> ok;
maybe_throw({error, Reason}) -> throw(Reason).

%% =============================================================================
%% PRIVATE: IMPORT
%% =============================================================================

%% @private
async_import(#{filename := Filename}, State0) ->
    Ts = erlang:system_time(second),
    Me = self(),
    Pid = spawn_link(fun() ->
        case do_import(Filename) of
            {ok, _Counters} = OK ->
                Me ! {import_reply, OK, self()};
            {error, _} = Error ->
                Me ! {import_reply, Error, self()}
        end
    end),
    State1 = State0#state{
        filename = Filename,
        pid = Pid,
        timestamp = Ts,
        status = import_in_progress
    },
    {ok, State1}.

%% @private
do_import(Filename) ->
    Opts = [
        {name, log},
        {mode, read_only},
        {file, Filename}
    ],
    case disk_log:open(Opts) of
        {ok, Log} ->
            ok = notify_import_started([Filename, 0, 0]),
            do_import_aux(Log);
        {repaired, Log, {recovered, Rec}, {badbytes, Bad}} ->
            ok = notify_import_started([Filename, Rec, Bad]),
            do_import_aux(Log);
        {error, _} = Error ->
            Error
    end.

%% @private
do_import_aux(Log) ->
    try
        Counters0 = #{read_count => 0, written_count => 0},
        import_chunk(
            {head, disk_log:chunk(Log, start)}, undefined, Log, Counters0
        )
    catch
        _:Reason ->
            {error, Reason}
    after
        _ = disk_log:close(Log)
    end.

%% @private
import_chunk(eof, _, Log, Counters) ->
    ok = disk_log:close(Log),
    {ok, Counters};
import_chunk({error, _} = Error, _, Log, _) ->
    _ = disk_log:close(Log),
    Error;
import_chunk({head, {Cont, [H | T]}}, undefined, Log, Counters) ->
    Mode = import_mode(H),
    import_chunk({Cont, T}, Mode, Log, Counters);
import_chunk({Cont, Terms}, Mode, Log, Counters0) ->
    try
        {ok, Counters} = import_terms(Terms, Mode, Counters0),
        import_chunk(disk_log:chunk(Log, Cont), Mode, Log, Counters)
    catch
        _:Reason ->
            {error, Reason}
    end.

%% @private
%% Determines the import mode from the file header: the current bondy_db export
%% format, or the legacy plum_db backup format (with its version, for the
%% < 1.2.0 prefix renames).
import_mode(#{format := ?EXPORT_FORMAT, vsn := Vsn}) when Vsn >= ?EXPORT_VSN ->
    new;
import_mode(#{format := ?LEGACY_FORMAT, vsn := Vsn}) ->
    %% Old plum_db-format backups are recognised but their translation to
    %% bondy_db is parked pending a fixture (see the moduledoc). Reject loudly
    %% rather than silently mis-placing data.
    throw({legacy_format_unsupported, Vsn});
import_mode(H) ->
    throw({invalid_header, H}).

%% @private
%% New (bondy_db) format: each entry is a logical `{entry, Table, Band, Key,
%% Value}`, re-applied as a fresh `{set, Value}`.
import_terms([], _Mode, Counters) ->
    {ok, Counters};
import_terms([{entry, Name, Band, Key, Value} | T], new, Counters) ->
    import_terms(T, new, apply_entry(Name, Band, Key, Value, Counters));
import_terms([_Other | T], new, #{read_count := N} = Counters) ->
    %% Unknown term (e.g. a stray header) — count as read, skip.
    import_terms(T, new, Counters#{read_count => N + 1}).

%% @private
%% Applies one logical entry to bondy_db. Tables declared but not provisioned on
%% this node are skipped (counted as read only).
apply_entry(Name, Band, Key, Value, #{read_count := N, written_count := M} = C) ->
    case bondy_namespace_catalog:table(Name) of
        undefined ->
            C#{read_count => N + 1};
        Table ->
            ok = bondy_db:apply(Table, Band, Key, {set, Value}),
            C#{read_count => N + 1, written_count => M + 1}
    end.

%% =============================================================================
%% PRIVATE: STATUS / HEADER
%% =============================================================================

%% @private
read_head(Filename) ->
    Opts = [
        {name, log},
        {mode, read_only},
        {file, Filename}
    ],
    Acc = #{filename => unicode:characters_to_binary(Filename)},
    case disk_log:open(Opts) of
        {ok, Log} ->
            do_read_head(Log, Acc);
        {repaired, Log, {recovered, Rec}, {badbytes, Bad}} ->
            do_read_head(Log, Acc#{recovered => Rec, bad_bytes => Bad});
        {error, no_such_log} ->
            {error, not_found};
        {error, _} = Error ->
            Error
    end.

%% @private
do_read_head(Log, Acc0) ->
    try
        case disk_log:chunk(Log, start) of
            {_Cont, [H | _]} ->
                ok = validate_head(H),
                {ok, maps:merge(Acc0#{status => ok, bad_bytes => 0}, H)};
            {_Cont, [H | _], BadBytes} ->
                ok = validate_head(H),
                {ok, maps:merge(Acc0#{status => ok, bad_bytes => BadBytes}, H)};
            eof ->
                {ok, Acc0#{status => invalid_format}};
            {error, {corrupt_log_file, _}} ->
                {ok, Acc0#{status => corrupt, bad_bytes => 0}};
            {error, {blocked_log, _}} ->
                {ok, Acc0#{status => blocked, bad_bytes => 0}};
            {error, _} = Error ->
                Error
        end
    catch
        _:Reason ->
            {error, Reason}
    after
        _ = disk_log:close(Log)
    end.

%% @private
validate_head(#{format := ?EXPORT_FORMAT}) ->
    ok;
validate_head(#{format := ?LEGACY_FORMAT}) ->
    ok;
validate_head(H) ->
    throw({invalid_header, H}).

%% =============================================================================
%% PRIVATE: EVENTS
%% =============================================================================

%% @private
notify_export_started(File) ->
    ?LOG_NOTICE(#{description => "Started export", filename => File}),
    bondy_event_manager:notify({[bondy, export, start], #{filename => File}}).

%% @private
notify_export_finished([Filename, Time]) ->
    ?LOG_NOTICE(#{
        description => "Finished creating export",
        filename => Filename,
        elapsed_time_secs => Time
    }),
    bondy_event_manager:notify(
        {[bondy, export, stop], #{
            filename => Filename, elapsed_time_secs => Time
        }}
    ).

%% @private
notify_export_error([Reason, Filename, Time]) ->
    ?LOG_ERROR(#{
        description => "Error creating export",
        filename => Filename,
        reason => Reason,
        elapsed_time_secs => Time
    }),
    bondy_event_manager:notify(
        {[bondy, export, exception], #{
            filename => Filename, reason => Reason, elapsed_time_secs => Time
        }}
    ).

%% @private
notify_import_started([Filename, Rec, Bad]) ->
    ?LOG_NOTICE(#{
        description => "Import started",
        filename => Filename,
        recovered => Rec,
        bad_bytes => Bad
    }),
    bondy_event_manager:notify(
        {[bondy, export, import, start], #{filename => Filename}}
    ).

%% @private
notify_import_finished([Filename, Time, Read, Written]) ->
    ?LOG_NOTICE(#{
        description => "Import finished",
        filename => Filename,
        elapsed_time_secs => Time,
        read_count => Read,
        written_count => Written
    }),
    bondy_event_manager:notify(
        {[bondy, export, import, stop], #{
            filename => Filename,
            elapsed_time_secs => Time,
            read_count => Read,
            written_count => Written
        }}
    ).

%% @private
notify_import_error([Filename, Reason, Time]) ->
    ?LOG_ERROR(#{
        description => "Import failed",
        filename => Filename,
        reason => Reason,
        elapsed_time_secs => Time
    }),
    bondy_event_manager:notify(
        {[bondy, export, import, exception], #{
            filename => Filename, reason => Reason, elapsed_time_secs => Time
        }}
    ).
