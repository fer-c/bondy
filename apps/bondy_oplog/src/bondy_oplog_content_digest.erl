%% =============================================================================
%% SPDX-FileCopyrightText: 2016 - 2026 Leapsight
%% SPDX-License-Identifier: Apache-2.0
%% =============================================================================


-module(bondy_oplog_content_digest).
-moduledoc """
An order-independent, incrementally-maintainable digest of a `bondy_oplog`
instance's materialized **projection** content — the cross-node convergence
oracle (ISSUES.md AR-17).

## Why not the MST root?

Anti-entropy compares MST root hashes, but **compaction empties the MST while
the data persists in the projection** (compaction truncates the oplog/MST and
advances the checkpoint; it never mutates the materialized projection). So the
root is unreliable as a "do two nodes hold the same data?" oracle across nodes
in different compaction states: two converged nodes can show different roots
(one compacted, one not), and — worse — two nodes that have both compacted to an
empty MST both advertise `undefined`, so nothing actually *verifies* their
projections match.

This digest is taken over the **projection content** instead, so it is
compaction-invariant by construction: compaction does not change the projection,
so it does not change the digest.

## The construction

The digest is the XOR of a per-cell hash over every primary cell in the
projection:

```
digest = XOR over all cells of  H(Bucket, Key, StateBytes)
```

XOR is commutative and associative, so the digest does not depend on the order
cells were applied or merged — two nodes that converge to the same set of
`(Bucket, Key) -> StateBytes` cells compute the same digest regardless of how
they got there. XOR is also its own inverse, which makes the digest
*incrementally* maintainable: when a cell's state changes `Old -> New`, the
digest is updated by XOR-ing out the old contribution and XOR-ing in the new one
(`replace/5`) — an O(1) update on the apply path, no rescan.

`StateBytes` is the fold's encoded CRDT **state** (the same bytes the projection
and MST already store and that anti-entropy already requires to be deterministic
and identical across nodes for a converged cell). The user-facing value is a
pure function of the state, so the state captures convergence; hashing the state
(not the frame envelope) keeps the digest independent of projection-adapter
storage layout (ETS whole-frame vs. leveled split sub-keys).

## Width

A `t()` is a 64-bit unsigned integer. This is deliberately one machine word so
the live digest can be held in a single `atomics` counter and maintained
lock-free (XOR-CAS) exactly like `bondy_oplog_high_water`, with **atomic reads**
(a wider digest spread over several words could be read torn mid-update and
report spurious divergence). 64 bits is ample for the task: the oracle compares
two *specific* digests (node A vs node B), so a false "in sync" requires two
genuinely different projections to collide, with probability ~2^-64 per
comparison — not a birthday search. The empty projection's digest is `0`
(`empty/0`).
""".

-define(WIDTH_BITS, 64).
-define(MASK, ((1 bsl 64) - 1)).

-type t() :: non_neg_integer().
-type ref() :: atomics:atomics_ref().

-export_type([t/0]).
-export_type([ref/0]).

%% API
-export([empty/0]).
-export([is_empty/1]).
-export([cell_hash/3]).
-export([add/4]).
-export([remove/4]).
-export([replace/5]).
-export([combine/2]).
-export([to_hex/1]).

%% LIVE REF (per-instance store)
-export([new_ref/0]).
-export([read_ref/1]).
-export([apply_delta/2]).
-export([set_ref/2]).


%% =============================================================================
%% API
%% =============================================================================


-doc """
The digest of an empty projection: `0`. The identity element for `combine/2`,
`add/4`, and `remove/4`.
""".
-spec empty() -> t().

empty() ->
    0.


-doc "Whether the digest is the empty-projection digest (`0`).".
-spec is_empty(Digest :: t()) -> boolean().

is_empty(0) ->
    true;
is_empty(Digest) when is_integer(Digest), Digest >= 0, Digest =< ?MASK ->
    false.


-doc """
The per-cell contribution `H(Bucket, Key, StateBytes)`: a 64-bit value taken
from the leading 8 bytes of a SHA-256 over the length-prefixed concatenation of
the cell's bucket, key, and encoded CRDT state. Length-prefixing keeps the hash
unambiguous across differing field boundaries (so `{<<"a">>, <<"bc">>}` and
`{<<"ab">>, <<"c">>}` cannot collide).

`StateBytes` must be the fold's deterministic encoded state — identical across
nodes for a converged cell — so two nodes hashing the same converged cell
produce the same contribution.
""".
-spec cell_hash(
    Bucket :: binary(),
    Key :: binary(),
    StateBytes :: binary()
) -> t().

cell_hash(Bucket, Key, StateBytes) when
    is_binary(Bucket), is_binary(Key), is_binary(StateBytes)
->
    <<H:?WIDTH_BITS/big-unsigned, _/binary>> = crypto:hash(sha256, <<
        (byte_size(Bucket)):32/big-unsigned,
        Bucket/binary,
        (byte_size(Key)):32/big-unsigned,
        Key/binary,
        StateBytes/binary
    >>),
    H.


-doc """
Fold a cell's contribution into the digest (a new cell, or re-adding after a
`remove/4`). XOR-based, so applying `add/4` for the same cell twice cancels out —
callers maintain the invariant of one live contribution per `(Bucket, Key)` (use
`replace/5` for updates).
""".
-spec add(
    Digest :: t(),
    Bucket :: binary(),
    Key :: binary(),
    StateBytes :: binary()
) -> t().

add(Digest, Bucket, Key, StateBytes) ->
    Digest bxor cell_hash(Bucket, Key, StateBytes).


-doc """
Remove a cell's contribution from the digest (a delete/tombstone). XOR is its
own inverse, so this is identical to `add/4`; the distinct name documents intent
at the call site.
""".
-spec remove(
    Digest :: t(),
    Bucket :: binary(),
    Key :: binary(),
    StateBytes :: binary()
) -> t().

remove(Digest, Bucket, Key, StateBytes) ->
    Digest bxor cell_hash(Bucket, Key, StateBytes).


-doc """
Update the digest for a cell whose encoded state changed `Old -> New`: XOR out
the old contribution and XOR in the new one. This is the O(1) incremental update
applied on the apply path once per cell.

`OldStateBytes =:= undefined` for a brand-new cell (only the new contribution is
added); `NewStateBytes =:= undefined` for a delete/tombstone (only the old
contribution is removed). Both `undefined` is a no-op.
""".
-spec replace(
    Digest :: t(),
    Bucket :: binary(),
    Key :: binary(),
    OldStateBytes :: binary() | undefined,
    NewStateBytes :: binary() | undefined
) -> t().

replace(Digest, Bucket, Key, OldStateBytes, NewStateBytes) ->
    D1 =
        case OldStateBytes of
            undefined -> Digest;
            _ -> remove(Digest, Bucket, Key, OldStateBytes)
        end,
    case NewStateBytes of
        undefined -> D1;
        _ -> add(D1, Bucket, Key, NewStateBytes)
    end.


-doc """
Combine two digests (or digest deltas) by XOR. Commutative and associative with
`empty/0` as the identity — used to fold a batch's per-cell deltas into one delta
before applying it to the running digest, and to merge partial digests computed
over disjoint key ranges.
""".
-spec combine(A :: t(), B :: t()) -> t().

combine(A, B) ->
    A bxor B.


-doc "Zero-padded 16-char lowercase hex rendering of the digest, for logs and the operator view.".
-spec to_hex(Digest :: t()) -> binary().

to_hex(Digest) when is_integer(Digest), Digest >= 0, Digest =< ?MASK ->
    iolist_to_binary(io_lib:format("~16.16.0b", [Digest])).


%% =============================================================================
%% LIVE REF (per-instance store)
%% =============================================================================
%%
%% The live digest is held in a single `atomics` word, exactly like
%% `bondy_oplog_high_water` — one machine word so reads are atomic (a wider
%% digest spread over several words could be read torn mid-update and report
%% spurious divergence) and updates are lock-free. The ref is allocated once per
%% instance at init and shared between the applier (the writer, via
%% `apply_delta/2` after every committed batch) and read-only consumers (the
%% instance's `content_digest/1`, the responder, the observer).


-doc """
Allocate a fresh per-instance digest counter, initialised to `empty/0` (`0`).
Called once at instance init and published in `bondy_oplog_registry`.
""".
-spec new_ref() -> ref().

new_ref() ->
    atomics:new(1, [{signed, false}]).


-doc "Read the current live digest. A single atomic `get` — never torn.".
-spec read_ref(Ref :: ref()) -> t().

read_ref(Ref) ->
    atomics:get(Ref, 1).


-doc """
XOR a delta (the `combine/2` of a committed batch's per-cell `replace/5`
contributions) into the live digest, lock-free. Concurrent writers race on a
CAS loop, mirroring `bondy_oplog_high_water:advance/2`. A zero delta is a no-op.
""".
-spec apply_delta(Ref :: ref(), Delta :: t()) -> ok.

apply_delta(_Ref, 0) ->
    ok;
apply_delta(Ref, Delta) when is_integer(Delta), Delta > 0, Delta =< ?MASK ->
    xor_loop(Ref, Delta).


-doc """
Overwrite the live digest with `Digest`. Used to install a recomputed digest at
cold start or after a bootstrap snapshot — both points at which the applier is
not concurrently mutating this instance's digest (the lifecycle serialises
bootstrap-finalise before live draining), so a plain write is safe.
""".
-spec set_ref(Ref :: ref(), Digest :: t()) -> ok.

set_ref(Ref, Digest) when is_integer(Digest), Digest >= 0, Digest =< ?MASK ->
    atomics:put(Ref, 1, Digest),
    ok.


%% @private
xor_loop(Ref, Delta) ->
    Cur = atomics:get(Ref, 1),
    New = Cur bxor Delta,
    case atomics:compare_exchange(Ref, 1, Cur, New) of
        ok -> ok;
        _Other -> xor_loop(Ref, Delta)
    end.
