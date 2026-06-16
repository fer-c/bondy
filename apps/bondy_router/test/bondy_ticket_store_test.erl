%% =============================================================================
%% SPDX-FileCopyrightText: 2016 - 2026 Leapsight
%% SPDX-License-Identifier: Apache-2.0
%% =============================================================================

%% Focused coverage for `bondy_ticket`'s bulk revocation after its cut-over from
%% plum_db to bondy_db (design §11.4). The point read/write paths are exercised
%% end-to-end by `bondy_auth_ticket_SUITE`; what that suite does NOT cover is
%% `revoke_all/1,2`, which is real production code (realm / user deletion) and —
%% for `revoke_all/2` — the one place the cut changed the ALGORITHM: plum_db's
%% ordered key-prefix range became a realm `bondy_db:list/2` scan that decodes
%% each `term_to_binary/1` store key and filters by its `Authid` (the first
%% element of the composed `{Authid, A, B}` key). This pins that filter.
%%
%% These functions take `RealmUri` directly (no realm resolution), so the test
%% needs only a provisioned catalogue — not a full bondy boot.

-module(bondy_ticket_store_test).

-include_lib("eunit/include/eunit.hrl").

-define(REALM, <<"com.bondy.test.ticket_store">>).


revoke_all_test_() ->
    {setup, fun setup/0, fun cleanup/1, fun(_) ->
        [
            {"revoke_all/2 clears one user's tickets, leaves the others",
                fun revoke_all_user/0},
            {"revoke_all/1 clears the whole realm", fun revoke_all_realm/0}
        ]
    end}.


%% revoke_all/2 must clear exactly the target user's cells. Seed alice (two
%% distinct store keys) and bob (one); revoking alice leaves only bob.
revoke_all_user() ->
    T = table(),
    %% alice: an SSO ticket and a device-scoped SSO ticket (two store keys).
    seed(T, <<"alice">>, {<<"alice">>, <<>>, <<>>}),
    seed(T, <<"alice">>, {<<"alice">>, <<>>, <<"device-1">>}),
    %% bob: one SSO ticket.
    seed(T, <<"bob">>, {<<"bob">>, <<>>, <<>>}),
    ?assertEqual([<<"alice">>, <<"bob">>], live_authids(T)),

    ok = bondy_ticket:revoke_all(?REALM, <<"alice">>),
    ?assertEqual([<<"bob">>], live_authids(T)),

    %% Idempotent: revoking again is a no-op.
    ok = bondy_ticket:revoke_all(?REALM, <<"alice">>),
    ?assertEqual([<<"bob">>], live_authids(T)),

    %% Clean up bob for the next test.
    ok = bondy_ticket:revoke_all(?REALM, <<"bob">>),
    ?assertEqual([], live_authids(T)).

%% revoke_all/1 clears every cell in the realm regardless of user.
revoke_all_realm() ->
    T = table(),
    seed(T, <<"carol">>, {<<"carol">>, <<>>, <<>>}),
    seed(T, <<"dave">>, {<<"dave">>, <<"com.realm">>, <<>>}),
    ?assertEqual([<<"carol">>, <<"dave">>], live_authids(T)),

    ok = bondy_ticket:revoke_all(?REALM),
    ?assertEqual([], live_authids(T)).


%% =============================================================================
%% Helpers
%% =============================================================================

%% Seed a ticket cell the way `bondy_ticket:store_ticket/3` does — the composed
%% store key `term_to_binary`-encoded, a claims map value — so the module's own
%% encode/decode resolves it.
seed(Table, Authid, KeyTuple) ->
    Claims = #{
        authrealm => ?REALM,
        authid => Authid,
        scope => #{realm => all, client_id => all, device_id => all},
        expires_at => erlang:system_time(second) + 3600
    },
    ok = bondy_db:apply(Table, ?REALM, term_to_binary(KeyTuple), {set, Claims}).

%% The distinct Authids with a live (map-valued) cell in the realm, sorted.
live_authids(Table) ->
    {ok, Rows} = bondy_db:list(Table, ?REALM),
    lists:usort([
        element(1, binary_to_term(Key))
     || {Key, Value, _Hlc} <- Rows, is_map(Value)
    ]).

table() ->
    bondy_namespace_catalog:table(bondy_ticket).

setup() ->
    {ok, _} = application:ensure_all_started(bondy_db),
    Tmp = make_tmpdir(),
    application:set_env(bondy_router, oplog_catalog_enabled, false),
    application:set_env(bondy_router, oplog_core_shard_count, 1),
    application:set_env(bondy_router, platform_data_dir, Tmp),
    {ok, Pid} = bondy_namespace_catalog:start_link(),
    {Pid, Tmp}.

cleanup({Pid, Tmp}) ->
    _ = catch gen_server:stop(Pid, normal, 30000),
    application:unset_env(bondy_router, oplog_catalog_enabled),
    application:unset_env(bondy_router, oplog_core_shard_count),
    application:unset_env(bondy_router, platform_data_dir),
    _ = file:del_dir_r(Tmp),
    ok.

make_tmpdir() ->
    Base = filename:join(
        "/tmp",
        "bondy_ticket_store_test_" ++
            integer_to_list(erlang:unique_integer([positive, monotonic]))
    ),
    ok = filelib:ensure_path(Base),
    Base.
