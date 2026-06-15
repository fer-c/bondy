%% =============================================================================
%% SPDX-FileCopyrightText: 2023 - 2026 Leapsight
%% SPDX-License-Identifier: Apache-2.0
%% =============================================================================

-module(bondy_db_projection_leveled).

-include("bondy_doc.hrl").
-include_lib("bondy_oplog/include/bondy_oplog.hrl").
-include_lib("leveled/include/leveled.hrl").

-moduledoc #{format => "text/markdown"}.
?MODULEDOC("""
`bondy_oplog_projection_adapter` implementation backed by a leveled
Bookie running in **`head_only` mode** with a SubKey split.

This adapter is a **pure mapper**: it owns no Bookie process, no
supervision, no path layout. It receives an already-opened Bookie pid
via `open/4`'s `Opts` and translates the substrate's seven callbacks
into the corresponding `leveled_bookie` calls.

Bookie lifecycle (start, stop, supervision, path layout, refcounting)
is the caller's concern — the consumer-facing `bondy_db` layer above
the substrate is where those decisions live. The Bookie **must** be
opened with `{head_only, with_lookup}` for this adapter to function;
see `bondy_db_topology_leveled_common:default_book_opts/1` for the
canonical opts.

## SubKey split

Each logical cell `(Bucket, Key)` is stored as **two** leveled HEAD
entries under the `?HEAD_TAG`, distinguished by SubKey:

| SubKey    | Payload (binary)                                              |
|-----------|---------------------------------------------------------------|
| `?SK_STATE` (`<<"s">>`) | `<<HlcLen:16, Hlc/binary, StateBytes/binary>>` |
| `?SK_VALUE` (`<<"v">>`) | `<<HlcLen:16, Hlc/binary, ValueBytes/binary>>` (HEAD wire format) |

Both subkeys carry the HLC so each can be decoded independently.
For cells whose CRDT/fold declares `value_equals_state/0 -> true`
(secondary index entries, G-Set) **only the `?SK_STATE` subkey is
written** — the value subkey is omitted and its absence on read is the
signal to reconstruct the frame with `ValueBytes = StateBytes` (see
`build_object_specs/2`, `get/3`). Because the state subkey is the only
one guaranteed present, **range scans enumerate `?SK_STATE`**, not
`?SK_VALUE`, so they see these cells too.

## Why head_only mode

Two material wins over the previous `?BONDY_FOLD_TAG` normal-mode
setup:

1. **Atomic batched writes via `book_mput/2`**. The cell-apply engine's
   `bondy_oplog_cell_apply:apply_cell_batch/3` collects all per-event
   writes into a single
   `put_batch/2` call; the adapter then translates that into one
   `book_mput` ObjectSpec list (two specs per cell — `?SK_STATE` +
   `?SK_VALUE`) and ships it to leveled atomically. Previous setup
   required one `book_put` gen_server roundtrip per cell write.
2. **Ledger-only reads**. In `head_only` mode the journal carries
   no body — the entire value is in the LSM HEAD entry. `book_get`
   becomes equivalent to `book_headonly` (no journal hop), so the
   apply path's read of OldState drops from ~1.7 ms (journal seek)
   to <100 µs (ledger lookup).

## Bucket is call-time

In line with the projection adapter behaviour, every data callback
takes `Bucket` as an argument and forwards it to `leveled_bookie`.
The handle is just the Bookie pid — one handle serves every Bucket
inside the shard.

## Handle shape

```erlang
#{bookie := pid()}
```

## Required `Opts` for `open/4`

| Key | Type | Meaning |
|---|---|---|
| `bookie` | `pid()` | The leveled Bookie this `(NS, Index, Shard)` writes to |

Anything else in `Opts` is ignored.

## Range bounds

Leveled's `book_keylist/5` range is **inclusive** on both ends; the
substrate contract is `[Low, High)` (half-open on the high side).
The fold function below excludes any composite key whose underlying
Key matches the High sentinel. Within each Key, only the
`?SK_STATE` SubKey is enumerated for the range (it is the one subkey
every cell is guaranteed to have — `value_equals_state` cells omit
`?SK_VALUE`); the full V2 frame is then reconstructed on demand via
`get/3`.

## What this adapter does NOT do

- Open, stop, or supervise the Bookie.
- Path management, journal/ledger directory creation, recovery.
- Routing or topology decisions.
- Reuse the legacy `?BONDY_FOLD_TAG` extractor at
  `bondy_db_leveled_tag` — head_only mode bypasses extractors
  entirely (HEAD bytes are written directly via `book_mput`).
""").

-behaviour(bondy_oplog_projection_adapter).

-export([
    open/4,
    close/1,
    get/3,
    head/3,
    put_batch/2,
    range/5,
    delete/3,
    clear/2,
    cell_keys/2,
    info/1
]).

-define(SK_STATE, <<"s">>).
-define(SK_VALUE, <<"v">>).

%% Lexicographically minimal/maximal SubKey sentinels for prefix scans
%% over a single Key (encompasses ?SK_STATE and ?SK_VALUE).
-define(SK_LOW, <<>>).
-define(SK_HIGH, <<255, 255, 255, 255>>).

-type handle() :: #{bookie := pid()}.

%% =============================================================================
%% API
%% =============================================================================

-spec open(
    Namespace :: atom(),
    Index :: atom(),
    Shard :: non_neg_integer(),
    Opts :: map()
) -> {ok, handle()} | {error, term()}.

open(_NS, _Index, _Shard, #{bookie := Pid} = _Opts) when is_pid(Pid) ->
    {ok, #{bookie => Pid}};
open(_NS, _Index, _Shard, Opts) when is_map(Opts) ->
    {error, {invalid_opts, Opts}}.

-spec close(handle()) -> ok.

close(#{bookie := _Pid}) ->
    ok.

-doc """
Full-cell read. Returns the V2 cell frame reconstructed from both
subkeys (state + value). Used by the applier's `apply_one_cell/11`
which needs both OldState and OldValueOpt.

Two `book_headonly/4` calls (ledger-only, no journal hop). On
not-found in either subkey returns `not_found` — the cell is treated
as absent (this is the same semantics as a key never having been
written; corruption that leaves only one subkey is logged elsewhere
and surfaces here as not_found).
""".
-spec get(handle(), Bucket :: binary(), Key :: binary()) ->
    {ok, Frame :: binary()} | not_found.

get(#{bookie := Pid}, Bucket, Key) when
    is_binary(Bucket), is_binary(Key)
->
    case read_state_subkey(Pid, Bucket, Key) of
        not_found ->
            not_found;
        {ok, Hlc, StateBytes} ->
            %% Value subkey is absent when the source frame had
            %% `HasValueColumn=0` (value_equals_state folds) — we
            %% deliberately don't write it on put_batch in that
            %% case. Reconstruct with the same flag the original
            %% encode/4 used.
            case read_value_subkey(Pid, Bucket, Key) of
                not_found ->
                    {ok,
                        bondy_oplog_cell_frame:encode(
                            Hlc, StateBytes, undefined, true
                        )};
                {ok, _Hlc, ValueBytes} ->
                    {ok,
                        bondy_oplog_cell_frame:encode(
                            Hlc, StateBytes, ValueBytes, false
                        )}
            end
    end.

-doc """
HEAD fast-path read. Returns the value subkey's payload as-is — it
**is** the HEAD wire format
(`<<HlcLen:16, HlcBin:HlcLen/binary, ValueBytes/binary>>`). One
`book_headonly/4` call.

This is the optional `head/3` callback on
`bondy_oplog_projection_adapter`; substrates that lack a native HEAD
mechanism can skip the export and let the caller fall back to
`get/3 + bondy_oplog_cell_frame:extract_head/1`.
""".
-spec head(handle(), Bucket :: binary(), Key :: binary()) ->
    {ok, HeadBytes :: binary()} | not_found.

head(#{bookie := Pid}, Bucket, Key) when
    is_binary(Bucket), is_binary(Key)
->
    case leveled_bookie:book_headonly(Pid, Bucket, Key, ?SK_VALUE) of
        {ok, HeadBytes} ->
            {ok, HeadBytes};
        not_found ->
            %% No value subkey → this cell was written by a
            %% value_equals_state fold (only state subkey exists).
            %% The state subkey payload IS the HEAD wire format
            %% (StateBytes doubles as ValueBytes for these folds).
            case leveled_bookie:book_headonly(Pid, Bucket, Key, ?SK_STATE) of
                {ok, HeadBytes} -> {ok, HeadBytes};
                not_found -> not_found
            end
    end.

-doc """
Batched cell write. Decodes each V2 frame into `{Hlc, State, Value}`,
builds two `book_mput` ObjectSpecs per entry (one for `?SK_STATE`,
one for `?SK_VALUE`), and ships them all to leveled in a single
atomic `book_mput/2` call.

The caller is expected to have already coalesced per-batch writes
(see `bondy_oplog_cell_apply:apply_cell_batch/3`) so this function
typically receives N entries and issues ONE gen_server roundtrip.
""".
-spec put_batch(
    handle(),
    [{Bucket :: binary(), Key :: binary(), Frame :: binary()}]
) -> ok | {error, term()}.

put_batch(_Handle, []) ->
    ok;
put_batch(#{bookie := Pid}, Entries) when is_list(Entries) ->
    ObjectSpecs = build_object_specs(Entries, []),
    case leveled_bookie:book_mput(Pid, ObjectSpecs) of
        ok -> ok;
        pause -> ok
    end.

-doc """
Range read over the value subkeys. Returns up to `Limit`
`{Key, Frame}` pairs in the requested direction. Each `Frame` is the
reconstructed V2 frame (one extra `book_headonly` per result to fetch
the matching state subkey).

The high bound is **exclusive** (substrate contract); the
keylist fold below filters out the matching key.

`High` may be the atom `infinity` for an open-ended scan (every value
subkey `>= Low` in the bucket) — the form the secondary-index
primary-scan fallback uses. It folds the whole bucket
(`book_keylist/4`) rather than a bounded `KeyRange`.
""".
-spec range(
    handle(),
    Bucket :: binary(),
    Low :: binary(),
    High :: binary() | infinity,
    Opts :: bondy_oplog_projection_adapter:range_opts()
) -> {ok, [{Key :: binary(), Frame :: binary()}]} | {error, term()}.

range(#{bookie := Pid}, Bucket, Low, High, Opts) when
    is_binary(Bucket),
    is_binary(Low),
    (is_binary(High) orelse High =:= infinity),
    is_map(Opts)
->
    Limit = maps:get(limit, Opts, 1000),
    Direction = maps:get(direction, Opts, asc),
    {async, Folder} =
        case High of
            infinity ->
                %% Whole-bucket fold from Low with no upper bound.
                FoldFun0 = make_state_keylist_fold_open(Limit, Low),
                leveled_bookie:book_keylist(
                    Pid, ?HEAD_TAG, Bucket, {FoldFun0, {0, []}}
                );
            _ ->
                %% Range over the {Key, SubKey} composite that brackets
                %% every **state** subkey between Low and High. We key off
                %% `?SK_STATE` (not `?SK_VALUE`) because the state subkey is
                %% the only one guaranteed present: a `value_equals_state`
                %% cell (e.g. a secondary index entry or a G-Set) omits the
                %% value subkey entirely (see `build_object_specs/2`), so a
                %% fold over `?SK_VALUE` would skip those cells. `get/3`
                %% reconstructs the full frame from whichever subkeys exist.
                KeyRange = {{Low, ?SK_STATE}, {High, ?SK_STATE}},
                FoldFun1 = make_state_keylist_fold(Limit, High),
                leveled_bookie:book_keylist(
                    Pid, ?HEAD_TAG, Bucket, KeyRange, {FoldFun1, {0, []}}
                )
        end,
    {_N, KeysRev} =
        try
            Folder()
        catch
            throw:{limit_reached, S} -> S
        end,
    KeysAsc = lists:reverse(KeysRev),
    %% Fetch the state subkey for each key found, reconstruct the V2
    %% frame. Returns [] if Reader fails on any key (treats as not_found).
    Pairs = [
        {K, F}
     || K <- KeysAsc,
        {ok, F} <- [get(#{bookie => Pid}, Bucket, K)]
    ],
    case Direction of
        asc -> {ok, Pairs};
        desc -> {ok, lists:reverse(Pairs)}
    end.

-doc """
Delete both subkeys for `(Bucket, Key)` atomically via `book_mput/2`
with `remove` ops.
""".
-spec delete(handle(), Bucket :: binary(), Key :: binary()) -> ok.

delete(#{bookie := Pid}, Bucket, Key) when
    is_binary(Bucket), is_binary(Key)
->
    ObjectSpecs = [
        {remove, Bucket, Key, ?SK_STATE, null},
        {remove, Bucket, Key, ?SK_VALUE, null}
    ],
    case leveled_bookie:book_mput(Pid, ObjectSpecs) of
        ok -> ok;
        pause -> ok
    end.

-doc """
Bucket-scoped wipe of one index's cells (the optional `clear/2` callback).
`Scope` is a `bondy_oplog_projection_adapter:clear_scope()` chosen by the
owner's topology:

- `{suffix, IndexName}` — wipe every bucket whose binary **ends with**
  `bondy_oplog_index_key:bucket_suffix(IndexName)` (`<<"/$idx/", IndexName>>`).
  Used on a `per_entity` Bookie, which holds a single logical table, so every
  index bucket present is this index's (across realms).

- `{entity, ET, IndexName}` — wipe only `ET`'s index buckets:
  `<<ET, "/$idx/", IndexName>>` (`shared_shards`) or
  `<<Realm, "/", ET, "/$idx/", IndexName>>` (`single_bookie`). Required on a
  Bookie that **co-locates several entity types**, so a sibling table that
  declared the same `IndexName` (a different `ET` prefix) is left untouched.

Two phases, ledger-only, for either scope:

1. `book_bucketlist/4` enumerates the Bookie's buckets and keeps the in-scope
   ones. This is cheap — there are few buckets — and skips the keys of every
   co-located table.
2. For each matching bucket, `book_keylist/4` folds its keys (keyed off the
   always-present `?SK_STATE` subkey so each cell is counted once) into
   `remove` specs for both subkeys, then one atomic `book_mput/2` per bucket.

Used by `bondy_oplog_index_rebuild` to drop orphaned index terms before
re-folding a secondary index from the primary.
""".
-spec clear(handle(), bondy_oplog_projection_adapter:clear_scope()) -> ok.

clear(#{bookie := Pid}, {suffix, IndexName}) when is_atom(IndexName) ->
    Suffix = bondy_oplog_index_key:bucket_suffix(IndexName),
    Buckets = matching_buckets(Pid, Suffix),
    lists:foreach(fun(Bucket) -> clear_bucket(Pid, Bucket) end, Buckets);
clear(#{bookie := Pid}, {entity, ET, IndexName}) when
    is_binary(ET), is_atom(IndexName)
->
    Buckets = entity_index_buckets(Pid, ET, IndexName),
    lists:foreach(fun(Bucket) -> clear_bucket(Pid, Bucket) end, Buckets).

-doc """
Enumerate the `{Bucket, Key}` of every PRIMARY cell of entity type `ET` in this
handle — the durable cell directory for a secondary-index rebuild.

The rebuild MUST read its cell directory from the projection, not the MST: the
MST is a truncatable recent-events structure (compaction drops events `<=` the
watermark, and a no-checkpoint crash loses its in-memory tail), whereas the
projection is the durable, complete materialised state. Reading the directory
from the MST would silently miss every already-compacted cell.

A primary bucket of `ET` is one that **equals `ET`** (`shared_shards`, where the
realm is folded into the key) or **ends with `/ET`** (`single_bookie`, where the
bucket is `<<Realm,"/",ET>>`), and that contains no `/$idx/` infix (excluding
this and other tables' index buckets). Other tables' primaries (`ET2`,
`<<Realm,"/",ET2>>`) and the reserved `$idx_*` buckets match neither test, so a
shared Bookie is correctly scoped to just `ET`'s cells. Each cell is counted
once off its always-present `?SK_STATE` subkey.

Note: `per_entity` (bucket = `<<Realm>>`, no `ET`) is not covered by this
`ET`-based scope and is not used in v1; it would need the dedicated-Bookie
"every non-index bucket" variant.
""".
-spec cell_keys(handle(), EntityType :: binary()) -> [{binary(), term()}].

cell_keys(#{bookie := Pid}, ET) when is_binary(ET) ->
    lists:flatmap(
        fun(Bucket) -> bucket_cell_keys(Pid, Bucket) end,
        primary_buckets(Pid, ET)
    ).

-spec info(handle()) -> #{atom() => term()}.

info(#{bookie := Pid}) ->
    #{
        backend => leveled,
        bookie => Pid,
        tag => ?HEAD_TAG,
        subkey_state => ?SK_STATE,
        subkey_value => ?SK_VALUE
    }.

%% =============================================================================
%% PRIVATE
%% =============================================================================

%% List every bucket in the Bookie's ?HEAD_TAG keyspace whose binary ends
%% with `Suffix`. Ledger-only fold over buckets (cheap — bounded by the
%% number of distinct buckets, not keys).
matching_buckets(Pid, Suffix) ->
    FoldFun =
        fun(Bucket, Acc) ->
            case is_bucket_suffix(Suffix, Bucket) of
                true -> [Bucket | Acc];
                false -> Acc
            end
        end,
    {async, Folder} =
        leveled_bookie:book_bucketlist(Pid, ?HEAD_TAG, {FoldFun, []}, all),
    Folder().

%% List every INDEX bucket of `IndexName` belonging to entity type `ET`:
%% `<<ET, "/$idx/", IndexName>>` (shared_shards) or
%% `<<Realm, "/", ET, "/$idx/", IndexName>>` (single_bookie). A sibling table's
%% same-named index (`<<ET2, "/$idx/", IndexName>>`) has a different `ET`
%% prefix and so is excluded — the entity-scoped fix. Mirrors
%% `is_primary_bucket/3`: exact-match the no-realm form, suffix-match the
%% realm-prefixed form. Ledger-only fold over buckets (cheap).
entity_index_buckets(Pid, ET, IndexName) ->
    Exact = bondy_oplog_index_key:bucket(ET, IndexName),
    Suffix = <<"/", Exact/binary>>,
    FoldFun =
        fun(Bucket, Acc) ->
            case Bucket =:= Exact orelse is_bucket_suffix(Suffix, Bucket) of
                true -> [Bucket | Acc];
                false -> Acc
            end
        end,
    {async, Folder} =
        leveled_bookie:book_bucketlist(Pid, ?HEAD_TAG, {FoldFun, []}, all),
    Folder().

%% List every PRIMARY bucket of entity type `ET`: equals `ET` (shared_shards)
%% or ends with `/ET` (single_bookie), and has no `/$idx/` infix (so index
%% buckets are excluded). Ledger-only fold over buckets (cheap).
primary_buckets(Pid, ET) ->
    Suffix = <<"/", ET/binary>>,
    FoldFun =
        fun(Bucket, Acc) ->
            case is_primary_bucket(ET, Suffix, Bucket) of
                true -> [Bucket | Acc];
                false -> Acc
            end
        end,
    {async, Folder} =
        leveled_bookie:book_bucketlist(Pid, ?HEAD_TAG, {FoldFun, []}, all),
    Folder().

is_primary_bucket(ET, Suffix, Bucket) when is_binary(Bucket) ->
    binary:match(Bucket, <<"/$idx/">>) =:= nomatch andalso
        (Bucket =:= ET orelse is_bucket_suffix(Suffix, Bucket));
is_primary_bucket(_ET, _Suffix, _Bucket) ->
    false.

%% Every `{Bucket, Key}` of one bucket, keyed off the always-present
%% `?SK_STATE` subkey so each cell is counted once.
bucket_cell_keys(Pid, Bucket) ->
    FoldFun =
        fun
            (B, {Key, ?SK_STATE}, Acc) -> [{B, Key} | Acc];
            (_B, {_Key, _SubKey}, Acc) -> Acc
        end,
    {async, Folder} =
        leveled_bookie:book_keylist(Pid, ?HEAD_TAG, Bucket, {FoldFun, []}),
    Folder().

%% True when binary `Bucket` ends with binary `Suffix`.
is_bucket_suffix(Suffix, Bucket) when is_binary(Bucket) ->
    SS = byte_size(Suffix),
    BS = byte_size(Bucket),
    BS >= SS andalso binary:part(Bucket, BS - SS, SS) =:= Suffix;
is_bucket_suffix(_Suffix, _Bucket) ->
    %% Non-binary bucket (not produced by this layer) — never a match.
    false.

%% Remove every cell of one bucket. Folds the bucket's keys keyed off the
%% always-present `?SK_STATE` subkey (so each cell is counted once) into
%% remove specs for both subkeys, then one atomic `book_mput/2`.
clear_bucket(Pid, Bucket) ->
    FoldFun =
        fun
            (B, {Key, ?SK_STATE}, Acc) ->
                [
                    {remove, B, Key, ?SK_STATE, null},
                    {remove, B, Key, ?SK_VALUE, null}
                    | Acc
                ];
            (_B, {_Key, _SubKey}, Acc) ->
                Acc
        end,
    {async, Folder} =
        leveled_bookie:book_keylist(Pid, ?HEAD_TAG, Bucket, {FoldFun, []}),
    case Folder() of
        [] ->
            ok;
        ObjectSpecs ->
            case leveled_bookie:book_mput(Pid, ObjectSpecs) of
                ok -> ok;
                pause -> ok
            end
    end.

%% Read the state subkey, returning {ok, Hlc, StateBytes} | not_found.
read_state_subkey(Pid, Bucket, Key) ->
    case leveled_bookie:book_headonly(Pid, Bucket, Key, ?SK_STATE) of
        {ok, <<HlcLen:16/big-unsigned, Hlc:HlcLen/binary, StateBytes/binary>>} ->
            HlcInt = binary:decode_unsigned(Hlc, big),
            {ok, HlcInt, StateBytes};
        not_found ->
            not_found
    end.

%% Read the value subkey, returning {ok, Hlc, ValueBytes} | not_found.
read_value_subkey(Pid, Bucket, Key) ->
    case leveled_bookie:book_headonly(Pid, Bucket, Key, ?SK_VALUE) of
        {ok, <<HlcLen:16/big-unsigned, Hlc:HlcLen/binary, ValueBytes/binary>>} ->
            HlcInt = binary:decode_unsigned(Hlc, big),
            {ok, HlcInt, ValueBytes};
        not_found ->
            not_found
    end.

%% Turn an [{Bucket, Key, Frame}] list into a flat [ObjectSpec] list
%% suitable for book_mput.
%%
%% For frames with `HasValueColumn=1` we emit BOTH subkeys (state +
%% value). For frames with `HasValueColumn=0` (value_equals_state
%% folds) we emit ONLY the state subkey — the value subkey absence is
%% the signal on read that the cell was written by a
%% value_equals_state fold. See `get/3` and `head/3` for the
%% read-side handling.
build_object_specs([], Acc) ->
    lists:reverse(Acc);
build_object_specs([{Bucket, Key, Frame} | Rest], Acc) ->
    {Hlc, StateBytes, ValueBytesOpt} =
        bondy_oplog_cell_frame:decode_full(Frame),
    HlcBin = <<Hlc:64/big-unsigned>>,
    HlcLen = byte_size(HlcBin),
    StatePayload = <<HlcLen:16/big-unsigned, HlcBin/binary, StateBytes/binary>>,
    Acc1 = [{add, Bucket, Key, ?SK_STATE, StatePayload} | Acc],
    Acc2 =
        case ValueBytesOpt of
            undefined ->
                Acc1;
            ValueBytes ->
                ValuePayload =
                    <<HlcLen:16/big-unsigned, HlcBin/binary,
                        ValueBytes/binary>>,
                [{add, Bucket, Key, ?SK_VALUE, ValuePayload} | Acc1]
        end,
    build_object_specs(Rest, Acc2).

%% Fold fun for keylist over `{Key, ?SK_STATE}` composite keys.
%% Accumulates Keys (deduped by the SubKey == ?SK_STATE filter) up to
%% Limit; excludes any Key matching the High sentinel (half-open range).
%% Keys off the state subkey because it is the only one every cell is
%% guaranteed to have (`value_equals_state` cells omit `?SK_VALUE`).
make_state_keylist_fold(Limit, High) ->
    fun(_B, {K, SubKey}, {N, Items}) ->
        case SubKey of
            ?SK_STATE when K =/= High ->
                N1 = N + 1,
                State = {N1, [K | Items]},
                case N1 >= Limit of
                    true -> throw({limit_reached, State});
                    false -> State
                end;
            _ ->
                {N, Items}
        end
    end.

%% Open-ended (`High =:= infinity`) variant: a whole-bucket fold keeps
%% only state subkeys whose key is `>= Low`, capped at `Limit`.
make_state_keylist_fold_open(Limit, Low) ->
    fun(_B, {K, SubKey}, {N, Items}) ->
        case SubKey of
            ?SK_STATE when K >= Low ->
                N1 = N + 1,
                State = {N1, [K | Items]},
                case N1 >= Limit of
                    true -> throw({limit_reached, State});
                    false -> State
                end;
            _ ->
                {N, Items}
        end
    end.
