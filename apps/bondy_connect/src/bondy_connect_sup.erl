%% =============================================================================
%% SPDX-FileCopyrightText: 2016 - 2026 Leapsight
%% SPDX-License-Identifier: Apache-2.0
%% =============================================================================

-module(bondy_connect_sup).

-moduledoc """
Top supervisor for the `bondy_connect` application.

`one_for_one`, permanent. The skeleton starts with no children. Per `DESIGN.md`
§5 the eventual tree is:

```
bondy_connect_sup            (one_for_one)
├── bondy_connect_manager            (gen_server)        name registry + connect/disconnect
└── bondy_connect_connections_sup    (simple_one_for_one) one child per connection
```

Those children are introduced in Phase 3 (walking skeleton); see
`IMPLEMENTATION.md`.
""".

-behaviour(supervisor).

-export([start_link/0]).

-export([init/1]).

-define(SERVER, ?MODULE).



%% =============================================================================
%% API
%% =============================================================================



-spec start_link() -> supervisor:startlink_ret().

start_link() ->
    supervisor:start_link({local, ?SERVER}, ?MODULE, []).



%% =============================================================================
%% SUPERVISOR CALLBACKS
%% =============================================================================



-spec init([]) ->
    {ok, {supervisor:sup_flags(), [supervisor:child_spec()]}}.

init([]) ->
    SupFlags = #{
        strategy => one_for_one,
        intensity => 5,
        period => 10
    },
    ChildSpecs = [],
    {ok, {SupFlags, ChildSpecs}}.
