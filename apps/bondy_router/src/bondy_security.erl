%% =============================================================================
%% SPDX-FileCopyrightText: 2016 - 2026 Leapsight
%% SPDX-License-Identifier: Apache-2.0
%% =============================================================================

-module(bondy_security).
-moduledoc """
Manages the per-realm security status (enabled or disabled), persisting it in
`plum_db`, and resolves the RBAC module used for a realm.
""".

-define(STATUS_PREFIX(RealmUri), {security_status, RealmUri}).

%% API
-export([disable/1]).
-export([enable/1]).
-export([is_enabled/1]).
-export([rbac_mod/1]).
-export([status/1]).

%% =============================================================================
%% API
%% =============================================================================

rbac_mod(_) ->
    bondy_rbac.

is_enabled(RealmUri) ->
    bondy_realm:exists(RealmUri) orelse error({no_such_realm, RealmUri}),
    case plum_db:get(?STATUS_PREFIX(RealmUri), enabled) of
        true -> true;
        _ -> false
    end.

enable(RealmUri) ->
    bondy_realm:exists(RealmUri) orelse error({no_such_realm, RealmUri}),
    plum_db:put(?STATUS_PREFIX(RealmUri), enabled, true).

disable(RealmUri) ->
    bondy_realm:exists(RealmUri) orelse error({no_such_realm, RealmUri}),
    plum_db:put(?STATUS_PREFIX(RealmUri), enabled, false).

status(RealmUri) ->
    case is_enabled(RealmUri) of
        true -> enabled;
        _ -> disabled
    end.
