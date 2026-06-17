%% =============================================================================
%% SPDX-FileCopyrightText: 2023 - 2026 Leapsight
%% SPDX-License-Identifier: Apache-2.0
%% =============================================================================

-module(bondy_relation).
-moduledoc """
A thin **relation / EDB layer** over `bondy_db`.

A relation is a `bondy_db` table viewed as an ordered set of tuples
(facts). This module adds the two things the raw cell facade lacks and
that every "list the things in a realm" call needs:

- **Keyset (cursor) pagination** — `list/3` returns a bounded
  `t:result_set/0` and an opaque `t:cursor/0` to resume from, instead of
  materialising the whole realm. This is the fix for the
  `bondy_db:list/2`-then-`lists:sublist/2` pattern that can OOM a node
  when an operator lists a large realm.
- **A bounded streaming `fold/4`** — for the internal "touch every row"
  operations (rename, bulk-delete) that must be complete but must not
  hold the whole table in memory.

## Why `range_all/5` and not `range/5`

A relation's keys are spread across all of a table's shards
(`phash2({Bucket, Key})`) until `shard_by => realm` is honoured, so a page
must scatter its `[Low, High)` window across every shard and k-way merge
the bounded per-shard results (`bondy_db:range_all/5`). A single-shard
`range/5` would return only the fraction of the window that hashes to one
shard. When realm-sharding lands the merge collapses to one non-empty
shard for free — same call, optimal cost.

## Cursor

The cursor is the order-preserving storage key of the last row returned,
plus a `schema_hash` identifying the `(tag, schema)` it was minted for.
Resumption is a `range_all/5` whose open bound is moved just past that key
(`<<Key, 0>>` ascending; the key itself as the exclusive upper bound
descending), so pages never skip or duplicate a row even when rejected
rows (see `t:decoder/0`) are interleaved between accepted ones.
`encode_cursor/1` / `decode_cursor/2` ship the cursor over the wire
(base64 of `term_to_binary/1`), rejecting a cursor minted for a different
relation with `{error, stale}`.

## Forward-map

The surface mirrors Bondy Language's `Relation.Adapter` behaviour
(`lookup`, paginated `select`, change-feed) so a future Datalog/CHR engine
binds to the same relations with no re-modelling: a relation here is the
adapter's store, a fact is a row, and `list/3`'s keyset pagination is the
adapter's `paginated_select`.
""".

-include_lib("kernel/include/logger.hrl").

%% A relation descriptor: the backing table, the row decoder, and the
%% cursor schema identity. Realm is NOT part of the descriptor — the table
%% spans every realm and the realm is a per-query argument, exactly as in
%% `bondy_db`.
-record(relation, {
    tag :: atom(),
    table :: bondy_db:table(),
    decode :: decoder(),
    schema_hash :: binary()
}).

%% An opaque resumption token: the storage key of the last emitted row,
%% scoped to the relation/schema that minted it.
-record(cursor, {
    key :: binary(),
    schema_hash :: binary()
}).

-opaque relation() :: #relation{}.
-opaque cursor() :: #cursor{}.

%% Maps a raw `bondy_db` row to the caller's tuple, or rejects it. Rejection
%% lets one physical table back more than one logical relation (e.g. the
%% user table interleaves user cells with alias-pointer cells): a rejected
%% row is skipped and the page is back-filled from the next row, so a page
%% always holds `limit` accepted rows when the band has that many.
-type decoder() :: fun((bondy_db:row()) -> {ok, term()} | skip).

-type page_opts() :: #{
    limit := pos_integer(),
    direction => asc | desc,
    cursor => cursor() | undefined
}.

-type result_set() :: #{
    values := [term()],
    next := cursor() | undefined,
    has_more := boolean()
}.

-export_type([relation/0]).
-export_type([cursor/0]).
-export_type([page_opts/0]).
-export_type([result_set/0]).

%% Over-fetch above the page size per scatter round so a band with
%% interleaved rejected rows still fills a page in few rounds.
-define(CHUNK_MIN, 64).
%% Schema-hash width. The hash only guards a client replaying a cursor
%% minted for a different relation/schema; 16 bytes makes an accidental
%% collision negligible.
-define(HASH_BYTES, 16).

%% API
-export([decode_cursor/2]).
-export([encode_cursor/1]).
-export([fold/4]).
-export([list/3]).
-export([lookup/3]).
-export([new/2]).

%% =============================================================================
%% API
%% =============================================================================

-doc """
Build a relation descriptor for `Tag`.

`Opts` MUST carry:

- `table` — the `bondy_db:table/0` handle backing the relation.
- `decode` — a `t:decoder/0` mapping a raw row to the caller's tuple (or
  `skip` to reject it).

`Opts` MAY carry `schema` (any term) which, with `Tag`, fixes the cursor
`schema_hash`; it defaults to `Tag`. Change it whenever a relation's key
encoding changes so old cursors are rejected as stale.
""".
-spec new(Tag :: atom(), Opts :: map()) -> relation().

new(Tag, #{table := Table, decode := Decode} = Opts) when
    is_atom(Tag), is_function(Decode, 1)
->
    Schema = maps:get(schema, Opts, Tag),
    #relation{
        tag = Tag,
        table = Table,
        decode = Decode,
        schema_hash = schema_hash(Tag, Schema)
    }.

-doc """
Point lookup of the tuple stored under `Key` in `Realm`.

Returns `{ok, Tuple}` (the decoder's output), `{error, not_found}` when no
live cell exists or the decoder rejects it, or a substrate `{error, _}`.
""".
-spec lookup(
    Relation :: relation(),
    Realm :: bondy_db:realm(),
    Key :: binary()
) ->
    {ok, term()} | {error, not_found} | {error, term()}.

lookup(#relation{table = Table, decode = Decode}, Realm, Key) when
    is_binary(Realm), is_binary(Key)
->
    case bondy_db:read(Table, Realm, Key) of
        {ok, {Value, Hlc}} ->
            case Decode({Key, Value, Hlc}) of
                {ok, Tuple} -> {ok, Tuple};
                skip -> {error, not_found}
            end;
        {error, _} = Err ->
            Err
    end.

-doc """
Keyset page over the tuples of `Relation` in `Realm`.

`Opts` is a `t:page_opts/0`:

- `limit` (required) — page size; the relation is scanned for at most
  `limit + 1` accepted rows so `has_more` needs no count.
- `direction` — `asc` (default) or `desc`, by storage-key order.
- `cursor` — a `t:cursor/0` from a previous page's `next`, or `undefined`
  (the first page). The cursor MUST have been minted by this relation.

Returns `{ok, ResultSet}` where `ResultSet` is a `t:result_set/0`
(`values`, `next`, `has_more`), or a substrate `{error, _}`. `next` is
`undefined` exactly when `has_more` is `false`.
""".
-spec list(
    Relation :: relation(),
    Realm :: bondy_db:realm(),
    Opts :: page_opts()
) ->
    {ok, result_set()} | {error, term()}.

list(#relation{} = Relation, Realm, #{limit := Limit} = Opts) when
    is_binary(Realm), is_integer(Limit), Limit > 0
->
    Dir = maps:get(direction, Opts, asc),
    After = maps:get(cursor, Opts, undefined),
    ok = assert_cursor(Relation, After),
    {Lo, Hi} = scan_bounds(Dir, After),
    case collect(Relation, Realm, Lo, Hi, Dir, Limit + 1, []) of
        {ok, AccRev} ->
            {ok, finalize_page(Relation, lists:reverse(AccRev), Limit)};
        {error, _} = Err ->
            Err
    end.

-doc """
Stream every tuple of `Relation` in `Realm` through `Fun` in ascending
storage-key order, accumulating into `Acc0`.

Unlike collecting `list/3` pages, this never materialises the whole
relation: it pages internally with a bounded window. Use it for the
"touch every row" admin operations (rename, bulk-delete) that must be
complete but must stay within bounded memory. Rejected rows (the
decoder's `skip`) are not passed to `Fun`.

Returns `{ok, Acc}` or a substrate `{error, _}`.
""".
-spec fold(
    Relation :: relation(),
    Realm :: bondy_db:realm(),
    Fun :: fun((Tuple :: term(), AccIn :: term()) -> AccOut :: term()),
    Acc0 :: term()
) ->
    {ok, term()} | {error, term()}.

fold(#relation{} = Relation, Realm, Fun, Acc0) when
    is_binary(Realm), is_function(Fun, 2)
->
    do_fold(Relation, Realm, <<>>, Fun, Acc0).

-doc """
Encode `Cursor` to an opaque, URL-safe-ish binary for transport on a list
endpoint. The inverse is `decode_cursor/2`.
""".
-spec encode_cursor(Cursor :: cursor()) -> binary().

encode_cursor(#cursor{} = Cursor) ->
    base64:encode(term_to_binary(Cursor)).

-doc """
Decode a wire cursor produced by `encode_cursor/1`, validating that it was
minted by `Relation`.

Returns `{ok, Cursor}`, `{error, stale}` if the cursor's `schema_hash`
does not match the relation (the relation was re-keyed, or the cursor
belongs to a different relation — the caller should restart from the first
page), or `{error, malformed}` if the binary is not a decodable cursor.
""".
-spec decode_cursor(Relation :: relation(), Bin :: binary()) ->
    {ok, cursor()} | {error, stale | malformed}.

decode_cursor(#relation{schema_hash = Hash}, Bin) when is_binary(Bin) ->
    try binary_to_term(base64:decode(Bin), [safe]) of
        #cursor{schema_hash = Hash} = Cursor ->
            {ok, Cursor};
        #cursor{} ->
            {error, stale};
        _ ->
            {error, malformed}
    catch
        _:_ ->
            {error, malformed}
    end.

%% =============================================================================
%% PRIVATE
%% =============================================================================

%% @private
%% Gather at least `Target` accepted rows (or exhaust the band), pulling
%% scatter chunks and advancing the open window past the last raw key of
%% each chunk. Returns the accumulator newest-first.
collect(#relation{table = Table} = Relation, Realm, Lo, Hi, Dir, Target, Acc) ->
    Chunk = erlang:max(Target, ?CHUNK_MIN),
    RangeOpts = #{limit => Chunk, direction => Dir},
    case bondy_db:range_all(Table, Realm, Lo, Hi, RangeOpts) of
        {ok, Rows} ->
            {Acc1, LastRawKey} = decode_rows(Relation, Rows, Acc),
            Enough = length(Acc1) >= Target,
            Exhausted = length(Rows) < Chunk,
            case Enough orelse Exhausted of
                true ->
                    {ok, Acc1};
                false ->
                    {Lo1, Hi1} = advance(Dir, Lo, Hi, LastRawKey),
                    collect(Relation, Realm, Lo1, Hi1, Dir, Target, Acc1)
            end;
        {error, _} = Err ->
            Err
    end.

%% @private
%% Decode a chunk, dropping rejected rows. Returns the accumulator extended
%% (newest first) with accepted `{Key, Tuple}` pairs, and the raw key of the
%% last row in the chunk (for window advancement; `undefined` for an empty
%% chunk, which only occurs at band exhaustion where it is unused).
decode_rows(#relation{decode = Decode}, Rows, Acc0) ->
    lists:foldl(
        fun({Key, _Value, _Hlc} = Row, {Acc, _Last}) ->
            case Decode(Row) of
                {ok, Tuple} -> {[{Key, Tuple} | Acc], Key};
                skip -> {Acc, Key}
            end
        end,
        {Acc0, undefined},
        Rows
    ).

%% @private
%% Slide the scan window past the last raw key of the previous chunk.
%% Ascending: raise the inclusive lower bound to the key's immediate
%% successor. Descending: lower the exclusive upper bound to the key.
advance(asc, _Lo, Hi, LastKey) ->
    {<<LastKey/binary, 0>>, Hi};
advance(desc, Lo, _Hi, LastKey) ->
    {Lo, LastKey}.

%% @private
%% The initial `[Lo, Hi)` window. The whole realm band is `[<<>>, infinity)`
%% (the facade folds the realm in). A cursor moves the open bound just past
%% the last emitted key.
scan_bounds(asc, undefined) ->
    {<<>>, infinity};
scan_bounds(asc, #cursor{key = Key}) ->
    {<<Key/binary, 0>>, infinity};
scan_bounds(desc, undefined) ->
    {<<>>, infinity};
scan_bounds(desc, #cursor{key = Key}) ->
    {<<>>, Key}.

%% @private
%% Build the result set from up to `Target = Limit + 1` accepted rows in
%% scan order. More than `Limit` ⇒ there is a next page; the cursor is the
%% last in-page key. The `Limit + 1` fetch makes `has_more` exact without a
%% count.
finalize_page(Relation, Accepted, Limit) ->
    case length(Accepted) > Limit of
        true ->
            Page = lists:sublist(Accepted, Limit),
            {LastKey, _} = lists:last(Page),
            #{
                values => values(Page),
                next => mk_cursor(Relation, LastKey),
                has_more => true
            };
        false ->
            #{
                values => values(Accepted),
                next => undefined,
                has_more => false
            }
    end.

%% @private
do_fold(
    #relation{table = Table, decode = Decode} = Relation, Realm, Lo, Fun, Acc
) ->
    RangeOpts = #{limit => ?CHUNK_MIN, direction => asc},
    case bondy_db:range_all(Table, Realm, Lo, infinity, RangeOpts) of
        {ok, []} ->
            {ok, Acc};
        {ok, Rows} ->
            Acc1 = lists:foldl(
                fun({_Key, _V, _H} = Row, A) ->
                    case Decode(Row) of
                        {ok, Tuple} -> Fun(Tuple, A);
                        skip -> A
                    end
                end,
                Acc,
                Rows
            ),
            case length(Rows) < ?CHUNK_MIN of
                true ->
                    {ok, Acc1};
                false ->
                    {LastKey, _, _} = lists:last(Rows),
                    do_fold(Relation, Realm, <<LastKey/binary, 0>>, Fun, Acc1)
            end;
        {error, _} = Err ->
            Err
    end.

%% @private
values(Pairs) ->
    [Tuple || {_Key, Tuple} <- Pairs].

%% @private
mk_cursor(#relation{schema_hash = Hash}, Key) ->
    #cursor{key = Key, schema_hash = Hash}.

%% @private
schema_hash(Tag, Schema) ->
    Full = crypto:hash(sha256, term_to_binary({Tag, Schema})),
    binary:part(Full, 0, ?HASH_BYTES).

%% @private
%% A supplied cursor MUST belong to this relation — a mismatch means the
%% caller threaded a foreign or pre-re-key cursor, a programming error.
assert_cursor(_Relation, undefined) ->
    ok;
assert_cursor(#relation{schema_hash = Hash}, #cursor{schema_hash = Hash}) ->
    ok;
assert_cursor(#relation{tag = Tag}, #cursor{}) ->
    error({stale_cursor, Tag}).
