%% =============================================================================
%% SPDX-FileCopyrightText: 2016 - 2026 Leapsight
%% SPDX-License-Identifier: Apache-2.0
%% =============================================================================

-module(bondy_connect_registry).

-moduledoc """
Per-connection registry of the client's **registrations** (callee) and
**subscriptions** (subscriber).

It separates two views (Decision 12, §12):

- **declared** — the user's *desired* state (procedure/topic URI + handler +
  options). This is what `register/2`/`subscribe/2` record; it survives a
  reconnect and is what Phase 6 replays.
- **established** — the *server-confirmed* state, keyed by the id the router
  assigned (`registration_id`/`subscription_id`). This is what inbound
  `INVOCATION`/`EVENT` records are routed against.

A pure data structure — no process, no I/O. The connection process owns one
instance in its `gen_statem` data.
""".

-record(registry, {
    decl_regs = #{}     ::  #{uri() => entry()},
    decl_subs = #{}     ::  #{uri() => entry()},
    regs = #{}          ::  #{id() => established()},
    subs = #{}          ::  #{id() => established()},
    reg_uri = #{}       ::  #{uri() => id()},
    sub_uri = #{}       ::  #{uri() => id()}
}).

-type uri()         ::  binary().
-type id()          ::  integer().
-type handler()     ::  term().
-type entry()       ::  #{handler := handler(), options := map()}.
-type established() ::  #{uri := uri(), handler := handler(), options := map()}.
-type t()           ::  #registry{}.

-export_type([t/0]).

-export([new/0]).
-export([declare_registration/4]).
-export([declare_subscription/4]).
-export([confirm_registration/3]).
-export([confirm_subscription/3]).
-export([registration/2]).
-export([subscription/2]).
-export([registration_id/2]).
-export([subscription_id/2]).
-export([forget_registration/2]).
-export([forget_subscription/2]).
-export([declared_registrations/1]).
-export([declared_subscriptions/1]).
-export([clear_established/1]).



%% =============================================================================
%% API
%% =============================================================================



-doc "An empty registry.".
-spec new() -> t().
new() ->
    #registry{}.


-doc "Record the *desired* registration of `Uri` (before the router confirms).".
-spec declare_registration(uri(), handler(), map(), t()) -> t().
declare_registration(Uri, Handler, Opts, #registry{decl_regs = D} = R) ->
    R#registry{decl_regs = maps:put(Uri, entry(Handler, Opts), D)}.


-doc "Record the *desired* subscription of `Uri` (before the router confirms).".
-spec declare_subscription(uri(), handler(), map(), t()) -> t().
declare_subscription(Uri, Handler, Opts, #registry{decl_subs = D} = R) ->
    R#registry{decl_subs = maps:put(Uri, entry(Handler, Opts), D)}.


-doc """
Link the router-assigned `RegId` to the declared registration for `Uri`,
promoting it to *established*. A no-op if `Uri` was never declared.
""".
-spec confirm_registration(uri(), id(), t()) -> t().
confirm_registration(Uri, RegId, #registry{decl_regs = D} = R) ->
    case maps:find(Uri, D) of
        {ok, #{handler := H, options := O}} ->
            Est = #{uri => Uri, handler => H, options => O},
            R#registry{
                regs = maps:put(RegId, Est, R#registry.regs),
                reg_uri = maps:put(Uri, RegId, R#registry.reg_uri)
            };
        error ->
            R
    end.


-doc "As `confirm_registration/3`, for a subscription.".
-spec confirm_subscription(uri(), id(), t()) -> t().
confirm_subscription(Uri, SubId, #registry{decl_subs = D} = R) ->
    case maps:find(Uri, D) of
        {ok, #{handler := H, options := O}} ->
            Est = #{uri => Uri, handler => H, options => O},
            R#registry{
                subs = maps:put(SubId, Est, R#registry.subs),
                sub_uri = maps:put(Uri, SubId, R#registry.sub_uri)
            };
        error ->
            R
    end.


-doc "Look up an established registration by its id (for `INVOCATION` routing).".
-spec registration(id(), t()) -> {ok, established()} | error.
registration(RegId, #registry{regs = Regs}) ->
    maps:find(RegId, Regs).


-doc "Look up an established subscription by its id (for `EVENT` routing).".
-spec subscription(id(), t()) -> {ok, established()} | error.
subscription(SubId, #registry{subs = Subs}) ->
    maps:find(SubId, Subs).


-doc "Resolve a procedure URI to its established registration id.".
-spec registration_id(uri(), t()) -> {ok, id()} | error.
registration_id(Uri, #registry{reg_uri = Index}) ->
    maps:find(Uri, Index).


-doc "Resolve a topic URI to its established subscription id.".
-spec subscription_id(uri(), t()) -> {ok, id()} | error.
subscription_id(Uri, #registry{sub_uri = Index}) ->
    maps:find(Uri, Index).


-doc "Drop a registration (declared + established) by its established id.".
-spec forget_registration(id(), t()) -> t().
forget_registration(RegId, #registry{regs = Regs} = R) ->
    case maps:find(RegId, Regs) of
        {ok, #{uri := Uri}} ->
            R#registry{
                regs = maps:remove(RegId, Regs),
                reg_uri = maps:remove(Uri, R#registry.reg_uri),
                decl_regs = maps:remove(Uri, R#registry.decl_regs)
            };
        error ->
            R
    end.


-doc "Drop a subscription (declared + established) by its established id.".
-spec forget_subscription(id(), t()) -> t().
forget_subscription(SubId, #registry{subs = Subs} = R) ->
    case maps:find(SubId, Subs) of
        {ok, #{uri := Uri}} ->
            R#registry{
                subs = maps:remove(SubId, Subs),
                sub_uri = maps:remove(Uri, R#registry.sub_uri),
                decl_subs = maps:remove(Uri, R#registry.decl_subs)
            };
        error ->
            R
    end.


-doc "The declared registrations as `{Uri, Handler, Options}` (for replay).".
-spec declared_registrations(t()) -> [{uri(), handler(), map()}].
declared_registrations(#registry{decl_regs = D}) ->
    declared(D).


-doc "The declared subscriptions as `{Uri, Handler, Options}` (for replay).".
-spec declared_subscriptions(t()) -> [{uri(), handler(), map()}].
declared_subscriptions(#registry{decl_subs = D}) ->
    declared(D).


-doc """
Drop all *established* state (the router-assigned ids), keeping the *declared*
set intact. Called on a disconnect so a reconnect can replay the declared
registrations/subscriptions against fresh server-assigned ids.
""".
-spec clear_established(t()) -> t().
clear_established(#registry{} = R) ->
    R#registry{regs = #{}, subs = #{}, reg_uri = #{}, sub_uri = #{}}.



%% =============================================================================
%% PRIVATE
%% =============================================================================



%% @private
entry(Handler, Opts) ->
    #{handler => Handler, options => Opts}.


%% @private
declared(Map) ->
    [{Uri, H, O} || {Uri, #{handler := H, options := O}} <- maps:to_list(Map)].
