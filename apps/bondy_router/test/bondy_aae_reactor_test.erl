%% =============================================================================
%% SPDX-FileCopyrightText: 2016 - 2026 Leapsight
%% SPDX-License-Identifier: Apache-2.0
%% =============================================================================

-module(bondy_aae_reactor_test).

-include_lib("eunit/include/eunit.hrl").
-include_lib("bondy_wamp/include/bondy_wamp.hrl").
-include("bondy_uris.hrl").

-define(REALM, <<"com.example.reactor">>).
-define(USER, <<"alice">>).
%% G-1 realm-folded security_users cell key: <<Realm, 0, Username>>.
-define(USER_KEY, <<?REALM/binary, 0, ?USER/binary>>).
%% bondy_realm global-band cell key on the folding core topology: <<0, Uri>>.
-define(REALM_KEY, <<0, ?REALM/binary>>).

%% A remote user delete (a `clear` op) must close this node's sessions for that
%% user with reason ?BONDY_USER_DELETED.
remote_delete_closes_user_sessions_test() ->
    ok = meck:new(bondy_rbac_user, [passthrough]),
    ok = meck:expect(
        bondy_rbac_user,
        close_sessions,
        fun(R, U, Reason) -> {closed, R, U, Reason} end
    ),
    try
        ?assertEqual(
            {closed, ?REALM, ?USER, ?BONDY_USER_DELETED},
            bondy_aae_reactor:react_user(?USER_KEY, {clear, 10})
        ),
        ?assert(
            meck:called(
                bondy_rbac_user,
                close_sessions,
                [?REALM, ?USER, ?BONDY_USER_DELETED]
            )
        )
    after
        meck:unload(bondy_rbac_user)
    end.

%% A remote user `set` (update / credential change) is a no-op here for now —
%% it must NOT close sessions (deferred; see the module docs).
remote_set_does_not_close_user_sessions_test() ->
    ok = meck:new(bondy_rbac_user, [passthrough]),
    ok = meck:expect(
        bondy_rbac_user, close_sessions, fun(_, _, _) -> ok end
    ),
    try
        ?assertEqual(
            ok, bondy_aae_reactor:react_user(?USER_KEY, {set, 10, #{}})
        ),
        ?assertNot(
            meck:called(bondy_rbac_user, close_sessions, ['_', '_', '_'])
        )
    after
        meck:unload(bondy_rbac_user)
    end.

%% A remote realm delete (a `clear` op) must close this node's sessions for that
%% realm with reason ?WAMP_CLOSE_REALM.
remote_delete_closes_realm_sessions_test() ->
    ok = meck:new(bondy_realm, [passthrough]),
    ok = meck:expect(
        bondy_realm, close, fun(R, Reason) -> {closed, R, Reason} end
    ),
    try
        ?assertEqual(
            {closed, ?REALM, ?WAMP_CLOSE_REALM},
            bondy_aae_reactor:react_realm(?REALM_KEY, {clear, 10})
        ),
        ?assert(
            meck:called(bondy_realm, close, [?REALM, ?WAMP_CLOSE_REALM])
        )
    after
        meck:unload(bondy_realm)
    end.

%% A remote realm `set` (create / update) is a no-op here — it must NOT close
%% sessions.
remote_set_does_not_close_realm_sessions_test() ->
    ok = meck:new(bondy_realm, [passthrough]),
    ok = meck:expect(bondy_realm, close, fun(_, _) -> ok end),
    try
        ?assertEqual(
            ok, bondy_aae_reactor:react_realm(?REALM_KEY, {set, 10, #{}})
        ),
        ?assertNot(meck:called(bondy_realm, close, ['_', '_']))
    after
        meck:unload(bondy_realm)
    end.

%% The realm-folded security_users cell key splits back into {RealmUri, Username}.
unfold_user_key_test() ->
    ?assertEqual(
        {?REALM, ?USER}, bondy_aae_reactor:unfold_user_key(?USER_KEY)
    ).

%% The global-band bondy_realm cell key splits back into the realm URI.
unfold_realm_key_test() ->
    ?assertEqual(?REALM, bondy_aae_reactor:unfold_realm_key(?REALM_KEY)).
