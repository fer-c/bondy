%% =============================================================================
%% SPDX-FileCopyrightText: 2016 - 2026 Leapsight
%% SPDX-License-Identifier: Apache-2.0
%% =============================================================================

-module(bondy_aae_reactor).
-moduledoc """
Node-local reactor for bondy_db **remote-merge** changes.

Subscribes to the change-notification namespaces of the bondy_db tables whose
remote (anti-entropy) changes require a node-local side-effect, and acts on the
`{bondy_oplog_core_merge_event, ...}` tag published by the merge-side hook
(`bondy_oplog_core:publish_merge/4`). Local writes are handled inline at their
own write/delete chokepoints, so the `{bondy_oplog_core_event, ...}` (local) tag
is ignored here.

## Reactions

| Table            | Remote change | Side-effect |
|------------------|---------------|-------------|
| `security_users` | delete        | close this node's sessions for the user (`bondy.user.deleted`) |
| `bondy_realm`    | delete        | close this node's sessions for the realm (`wamp.close.close_realm`) |

A peer's user *credential change* (a `set` rather than a `clear`) does not yet
close sessions here: the merge hook carries no old value, so this node cannot
tell a credential change from a metadata edit without re-reading every live
session. That refinement is deferred; tokens/tickets still converge to revoked
via the CRDT regardless. A realm `set` (create / update) likewise needs no
session-close.

## Subscription lifecycle

Each subscription is (re)established once the namespace catalogue has provisioned
the corresponding table (retried until then). Like the api_gateway reactor it
does not currently re-subscribe across a `bondy_oplog_core_dispatcher` restart —
the dispatcher is configured to effectively never restart.
""".

-behaviour(gen_server).

-include_lib("kernel/include/logger.hrl").
-include_lib("bondy_wamp/include/bondy_wamp.hrl").
-include("bondy_db_tables.hrl").
-include("bondy_uris.hrl").

-define(RESUBSCRIBE_AFTER, 500).

-record(state, {
    user_sub :: reference() | undefined,
    user_ns :: atom() | undefined,
    realm_sub :: reference() | undefined,
    realm_ns :: atom() | undefined
}).

%% API
-export([start_link/0]).

-ifdef(TEST).
%% Exposed for unit testing the reaction logic without a running cluster.
-export([react_user/2]).
-export([react_realm/2]).
-export([unfold_user_key/1]).
-export([unfold_realm_key/1]).
-endif.

%% GEN_SERVER CALLBACKS
-export([init/1]).
-export([handle_continue/2]).
-export([handle_call/3]).
-export([handle_cast/2]).
-export([handle_info/2]).
-export([terminate/2]).
-export([code_change/3]).

%% =============================================================================
%% API
%% =============================================================================

-spec start_link() -> {ok, pid()} | {error, term()}.

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%% =============================================================================
%% GEN_SERVER CALLBACKS
%% =============================================================================

init([]) ->
    {ok, #state{}, {continue, subscribe}}.

handle_continue(subscribe, State) ->
    {noreply, subscribe(State)}.

handle_call(_Request, _From, State) ->
    {reply, {error, unknown}, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(retry_subscribe, State) ->
    {noreply, subscribe(State)};
handle_info(
    {bondy_oplog_core_merge_event, NS, Key, _Hlc, Op},
    #state{user_ns = NS} = State
) ->
    %% A peer's security_users change arrived via anti-entropy.
    ok = react_user(Key, Op),
    {noreply, State};
handle_info(
    {bondy_oplog_core_merge_event, NS, Key, _Hlc, Op},
    #state{realm_ns = NS} = State
) ->
    %% A peer's bondy_realm change arrived via anti-entropy.
    ok = react_realm(Key, Op),
    {noreply, State};
handle_info({bondy_oplog_core_merge_event, _NS, _Key, _Hlc, _Op}, State) ->
    %% A subscribed namespace with no reaction (or one not yet bound in state).
    {noreply, State};
handle_info({bondy_oplog_core_event, _NS, _Key, _Hlc, _Op}, State) ->
    %% Local write — its side-effects fire inline at the write chokepoint.
    {noreply, State};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, #state{user_sub = U, realm_sub = R}) ->
    _ = is_reference(U) andalso bondy_oplog_core:unsubscribe(U),
    _ = is_reference(R) andalso bondy_oplog_core:unsubscribe(R),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% =============================================================================
%% PRIVATE
%% =============================================================================

%% @private
%% Subscribe to every reacted-on namespace once the catalogue has provisioned
%% its table; retry shortly while any remains pending.
subscribe(State0) ->
    State1 = subscribe_user(State0),
    State2 = subscribe_realm(State1),
    case pending(State2) of
        true ->
            _ = erlang:send_after(?RESUBSCRIBE_AFTER, self(), retry_subscribe),
            State2;
        false ->
            State2
    end.

%% @private
pending(#state{user_sub = U, realm_sub = R}) ->
    U =:= undefined orelse R =:= undefined.

%% @private
subscribe_user(#state{user_sub = Ref} = State) when is_reference(Ref) ->
    State;
subscribe_user(State) ->
    case subscribe_table(?BONDY_DB_USER_TAB, "security_users") of
        undefined ->
            State;
        {Ref, NS} ->
            State#state{user_sub = Ref, user_ns = NS}
    end.

%% @private
subscribe_realm(#state{realm_sub = Ref} = State) when is_reference(Ref) ->
    State;
subscribe_realm(State) ->
    case subscribe_table(?BONDY_DB_REALM_TAB, "bondy_realm") of
        undefined ->
            State;
        {Ref, NS} ->
            State#state{realm_sub = Ref, realm_ns = NS}
    end.

%% @private
%% Subscribe to a table's change namespace, returning `{Ref, NS}` once the
%% catalogue has provisioned it, or `undefined` while it is still pending.
subscribe_table(TableName, Label) ->
    case bondy_namespace_catalog:table(TableName) of
        undefined ->
            undefined;
        Table ->
            NS = bondy_db:namespace(Table),
            {ok, Ref} = bondy_oplog_core:subscribe(NS, all),
            ?LOG_INFO(#{
                description => "AAE merge reactor subscribed to remote changes",
                table => Label
            }),
            {Ref, NS}
    end.

%% @private
%% React to a remote security_users change. A `clear` (delete) closes this
%% node's sessions for the user; a `set` is a no-op here (see moduledoc).
react_user(Key, {clear, _Hlc}) ->
    {RealmUri, Username} = unfold_user_key(Key),
    ?LOG_INFO(#{
        description =>
            "Closing local sessions for a user deleted on a peer node",
        realm_uri => RealmUri,
        username => Username
    }),
    bondy_rbac_user:close_sessions(RealmUri, Username, ?BONDY_USER_DELETED);
react_user(_Key, _Op) ->
    ok.

%% @private
%% React to a remote bondy_realm change. A `clear` (delete) closes this node's
%% sessions for the realm; a `set` (create / update) is a no-op here.
react_realm(Key, {clear, _Hlc}) ->
    RealmUri = unfold_realm_key(Key),
    ?LOG_INFO(#{
        description =>
            "Closing local sessions for a realm deleted on a peer node",
        realm_uri => RealmUri
    }),
    bondy_realm:close(RealmUri, ?WAMP_CLOSE_REALM);
react_realm(_Key, _Op) ->
    ok.

%% @private
%% security_users is realm-sharded, so its cell key is the G-1 realm-folded
%% `<<Realm, 0, Username>>` (realm URIs are NUL-free). Split it back.
unfold_user_key(Key) ->
    case binary:split(Key, <<0>>) of
        [RealmUri, Username] ->
            {RealmUri, Username};
        _ ->
            error({malformed_user_cell_key, Key})
    end.

%% @private
%% bondy_realm is a global registry under the empty band `<<>>`, so on the
%% folding (`shared_shards`) core topology its cell key is `<<0, Uri>>` (the
%% empty band, a NUL separator, then the realm URI). Recover the URI.
unfold_realm_key(Key) ->
    case binary:split(Key, <<0>>) of
        [<<>>, RealmUri] ->
            RealmUri;
        _ ->
            error({malformed_realm_cell_key, Key})
    end.
