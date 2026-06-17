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

| Table                   | Remote change | Side-effect |
|-------------------------|---------------|-------------|
| `security_users`        | delete        | close this node's sessions for the user (`bondy.user.deleted`) |
| `bondy_realm`           | delete        | close this node's sessions for the realm (`wamp.close.close_realm`) |
| `security_user_grants`  | grant/revoke  | invalidate this node's cached RBAC contexts for the realm (§9.5) |
| `security_group_grants` | grant/revoke  | invalidate this node's cached RBAC contexts for the realm (§9.5) |

The split mirrors the authn-vs-authz distinction in the local write path: an
**authentication-level** change (a user or realm *delete*) tears the affected
sessions down, whereas an **authorization** change (a grant `set` or a revoke
`clear`) re-evaluates in place — the session survives and its next authorize
re-reads the subject's current grants. Grant invalidation is realm-wide because a
group-grant change affects every member; over-invalidating unaffected sessions
costs only a one-time context rebuild, so both grant tables share one reaction.

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

%% One reacted-on bondy_db table. `ns`/`ref` are filled once the namespace
%% catalogue has provisioned the table and the subscription is established;
%% until then they are `undefined` and the subscription is retried.
-record(sub, {
    table :: atom(),
    label :: string(),
    kind  :: user | realm | grant,
    ns    :: atom() | undefined,
    ref   :: reference() | undefined
}).

-record(state, {
    subs = [] :: [#sub{}]
}).

%% API
-export([start_link/0]).

-ifdef(TEST).
%% Exposed for unit testing the reaction logic without a running cluster.
-export([react_user/2]).
-export([react_realm/2]).
-export([react_grant/2]).
-export([unfold_user_key/1]).
-export([unfold_realm_key/1]).
-export([unfold_grant_key/1]).
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
    {ok, #state{subs = reacted_tables()}, {continue, subscribe}}.

handle_continue(subscribe, State) ->
    {noreply, subscribe(State)}.

handle_call(_Request, _From, State) ->
    {reply, {error, unknown}, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(retry_subscribe, State) ->
    {noreply, subscribe(State)};
handle_info(
    {bondy_oplog_core_merge_event, NS, Key, _Hlc, Op}, State
) ->
    %% A peer's change to a reacted-on table arrived via anti-entropy; route it
    %% to the matching reaction by namespace.
    ok = react(NS, Key, Op, State),
    {noreply, State};
handle_info({bondy_oplog_core_event, _NS, _Key, _Hlc, _Op}, State) ->
    %% Local write — its side-effects fire inline at the write chokepoint.
    {noreply, State};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, #state{subs = Subs}) ->
    _ = [
        bondy_oplog_core:unsubscribe(Ref)
        || #sub{ref = Ref} <- Subs, is_reference(Ref)
    ],
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% =============================================================================
%% PRIVATE
%% =============================================================================

%% @private
%% The static set of bondy_db tables this node reacts to on a remote merge, each
%% tagged with the reaction `kind` used to dispatch a delivered event. Both grant
%% tables share the `grant` kind (a realm-wide §9.5 invalidation).
reacted_tables() ->
    [
        #sub{
            table = ?BONDY_DB_USER_TAB,
            label = "security_users",
            kind = user
        },
        #sub{
            table = ?BONDY_DB_REALM_TAB,
            label = "bondy_realm",
            kind = realm
        },
        #sub{
            table = ?BONDY_DB_USER_GRANT_TAB,
            label = "security_user_grants",
            kind = grant
        },
        #sub{
            table = ?BONDY_DB_GROUP_GRANT_TAB,
            label = "security_group_grants",
            kind = grant
        }
    ].

%% @private
%% (Re)subscribe to every reacted-on namespace whose table the catalogue has
%% provisioned; retry shortly while any remains pending.
subscribe(#state{subs = Subs0} = State) ->
    Subs1 = [ensure_subscribed(S) || S <- Subs0],
    case lists:any(fun(#sub{ref = R}) -> R =:= undefined end, Subs1) of
        true ->
            _ = erlang:send_after(?RESUBSCRIBE_AFTER, self(), retry_subscribe),
            ok;
        false ->
            ok
    end,
    State#state{subs = Subs1}.

%% @private
%% Subscribe to one table's change namespace once the catalogue has provisioned
%% it; leave it pending (ns/ref `undefined`) until then.
ensure_subscribed(#sub{ref = Ref} = Sub) when is_reference(Ref) ->
    Sub;
ensure_subscribed(#sub{table = Table, label = Label} = Sub) ->
    case bondy_namespace_catalog:table(Table) of
        undefined ->
            Sub;
        Handle ->
            NS = bondy_db:namespace(Handle),
            {ok, Ref} = bondy_oplog_core:subscribe(NS, all),
            ?LOG_INFO(#{
                description => "AAE merge reactor subscribed to remote changes",
                table => Label
            }),
            Sub#sub{ns = NS, ref = Ref}
    end.

%% @private
%% Route a delivered merge event to the reaction for its namespace. An event for
%% a namespace not (yet) bound — or with no reaction — is ignored.
react(NS, Key, Op, #state{subs = Subs}) ->
    case lists:keyfind(NS, #sub.ns, Subs) of
        #sub{kind = user} ->
            react_user(Key, Op);
        #sub{kind = realm} ->
            react_realm(Key, Op);
        #sub{kind = grant} ->
            react_grant(Key, Op);
        false ->
            ok
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
%% React to a remote grant change (security_user_grants / security_group_grants).
%% A grant `set` and a revoke `clear` both change the authorization a cached RBAC
%% context would compute, so each invalidates this node's sessions for the realm
%% in place (§9.5): the next authorize re-reads the subject's current grants. No
%% teardown — an authorization change re-evaluates, it does not drop the session.
%% Realm-wide because a group-grant change affects every member.
react_grant(Key, _Op) ->
    RealmUri = unfold_grant_key(Key),
    ?LOG_INFO(#{
        description =>
            "Invalidating local RBAC contexts after a peer grant change",
        realm_uri => RealmUri
    }),
    bondy_session_manager:invalidate_rbac_all(RealmUri).

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

%% @private
%% Grant tables are realm-banded, so on the folding (`shared_shards`) core
%% topology a grant cell key is `<<RealmUri, 0, EncodedGrantKey/binary>>`. The
%% realm URI is NUL-free, so the first separator recovers it; the trailing
%% composite grant key (role + resource) is not needed, as invalidation is
%% realm-wide.
unfold_grant_key(Key) ->
    case binary:split(Key, <<0>>) of
        [RealmUri, _EncGrantKey] ->
            RealmUri;
        _ ->
            error({malformed_grant_cell_key, Key})
    end.
