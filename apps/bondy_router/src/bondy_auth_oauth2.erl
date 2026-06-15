%% =============================================================================
%% SPDX-FileCopyrightText: 2016 - 2026 Leapsight
%% SPDX-License-Identifier: Apache-2.0
%% =============================================================================

-module(bondy_auth_oauth2).
-moduledoc """
This module implements the `bondy_auth` behaviour for OAuth2 authentication,
verifying a JWT bearer token presented by the client against the realm.
""".
-behaviour(bondy_auth).

-include("bondy_security.hrl").

-type state() :: map().

%% BONDY_AUTH CALLBACKS
-export([init/1]).
-export([requirements/0]).
-export([challenge/3]).
-export([authenticate/4]).

%% =============================================================================
%% BONDY_AUTH CALLBACKS
%% =============================================================================

-spec init(bondy_auth:context()) ->
    {ok, State :: state()} | {error, Reason :: any()}.

init(Ctxt) ->
    try
        User = bondy_auth:user(Ctxt),
        User =/= undefined orelse throw(invalid_context),

        {ok, maps:new()}
    catch
        throw:Reason ->
            {error, Reason}
    end.

-spec requirements() -> map().

requirements() ->
    #{
        identification => true,
        password => {true, #{protocols => [cra, scram]}},
        authorized_keys => false
    }.

-spec challenge(
    Details :: map(), AuthCtxt :: bondy_auth:context(), State :: state()
) ->
    {false, NewState :: state()}
    | {true, Extra :: map(), NewState :: state()}
    | {error, Reason :: any(), NewState :: state()}.

challenge(_, _, State) ->
    %% The client will respond to the challenge by sending the Token
    {true, #{}, State}.

-spec authenticate(
    JWT :: binary(),
    DataIn :: map(),
    Ctxt :: bondy_auth:context(),
    CBState :: state()
) ->
    {ok, DataOut :: map(), CBState :: state()}
    | {error, Reason :: any(), CBState :: state()}.

authenticate(JWT, _, Ctxt, State) ->
    RealmUri = bondy_auth:realm_uri(Ctxt),
    UserId = bondy_auth:user_id(Ctxt),

    case bondy_oauth_jwt:verify(RealmUri, JWT) of
        {ok, #{<<"sub">> := UserId} = Claims} ->
            {ok, Claims, State};
        {ok, _} ->
            {error, oauth2_invalid_grant, State};
        {error, Reason} ->
            {error, Reason, State}
    end.
