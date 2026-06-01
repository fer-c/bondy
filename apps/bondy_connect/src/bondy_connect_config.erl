%% =============================================================================
%% SPDX-FileCopyrightText: 2016 - 2026 Leapsight
%% SPDX-License-Identifier: Apache-2.0
%% =============================================================================

-module(bondy_connect_config).

-moduledoc """
Validates and normalises a connection spec into the internal config map
consumed by `bondy_connect_protocol` (and, later, the transport/connection
layers).

This phase validates the **protocol-relevant** fields strictly — `realm`
(a valid WAMP URI), `roles`, `agent`, and `auth` — and supplies defaults for
the transport-related fields (`transport`, `serializers`, `reconnect`, `ping`,
`max_message_length`, `tls`) which are exercised in later phases. TLS defaults
are secure-by-default (`verify_peer`).
""".

-include("bondy_connect.hrl").

-define(DEFAULT_ROLES, #{
    caller => #{},
    callee => #{},
    publisher => #{},
    subscriber => #{}
}).

-define(DEFAULT_SERIALIZERS, [json]).
-define(DEFAULT_MAX_MESSAGE_LENGTH, 16777216). %% 16 MB

-export([validate/1]).



%% =============================================================================
%% API
%% =============================================================================



-doc """
Validate and normalise a connection `Spec`. Returns the normalised config map
on success.
""".
-spec validate(Spec :: map()) -> {ok, map()} | {error, term()}.

validate(Spec) when is_map(Spec) ->
    try
        Realm = validate_realm(Spec),
        Auth = validate_auth(Spec),
        Config = #{
            realm => Realm,
            roles => maps:get(roles, Spec, ?DEFAULT_ROLES),
            agent => validate_agent(Spec),
            auth => Auth,
            serializers => maps:get(serializers, Spec, ?DEFAULT_SERIALIZERS),
            transport => maps:get(transport, Spec, tcp),
            endpoint => maps:get(endpoint, Spec, undefined),
            max_message_length =>
                maps:get(max_message_length, Spec, ?DEFAULT_MAX_MESSAGE_LENGTH),
            reconnect => maps:get(reconnect, Spec, #{}),
            ping => maps:get(ping, Spec, #{}),
            tls => validate_tls(Spec)
        },
        {ok, Config}
    catch
        throw:Reason ->
            {error, Reason}
    end;

validate(_) ->
    {error, invalid_spec}.



%% =============================================================================
%% PRIVATE
%% =============================================================================



%% @private
validate_realm(#{realm := Realm}) when is_binary(Realm) ->
    try
        bondy_wamp_uri:validate(Realm)
    catch
        _:_ ->
            throw({invalid_realm, Realm})
    end;

validate_realm(_) ->
    throw(missing_realm).


%% @private
validate_agent(#{agent := Agent}) when is_binary(Agent) ->
    Agent;

validate_agent(#{agent := _}) ->
    throw({invalid_agent, not_a_binary});

validate_agent(_) ->
    ?BONDY_CONNECT_AGENT.


%% @private
%% Default to anonymous when no auth is configured.
validate_auth(#{auth := #{method := Method} = Auth}) when is_binary(Method) ->
    case lists:member(Method, methods()) of
        true ->
            Auth;
        false ->
            throw({unsupported_authmethod, Method})
    end;

validate_auth(#{auth := #{}}) ->
    throw(missing_authmethod);

validate_auth(#{auth := _}) ->
    throw(invalid_auth);

validate_auth(_) ->
    #{method => ?WAMP_ANON_AUTH}.


%% @private
%% Secure-by-default: when TLS options are not supplied, peer verification is
%% on. Only relevant for tls/wss transports (consumed in later phases).
validate_tls(#{tls := TLS}) when is_map(TLS) ->
    maps:merge(#{verify => verify_peer}, TLS);

validate_tls(#{tls := _}) ->
    throw(invalid_tls);

validate_tls(_) ->
    #{verify => verify_peer}.


%% @private
methods() ->
    [
        ?WAMP_ANON_AUTH,
        ?WAMP_CRA_AUTH,
        ?WAMP_CRYPTOSIGN_AUTH,
        ?WAMP_TICKET_AUTH
    ].
