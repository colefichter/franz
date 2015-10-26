-module(franz_sup).
-behaviour(supervisor).

%% API
-export([start_link/0]).

%% Supervisor callbacks
-export([init/1]).

%% Helper macro for declaring children of supervisor
-define(CHILD(I, Type), {I, {I, start_link, []}, permanent, 5000, Type, [I]}).

%% ===================================================================
%% API functions
%% ===================================================================

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%% ===================================================================
%% Supervisor callbacks
%% ===================================================================

init([]) ->
    MaxRestart = 5,
    MaxTime = 3000,
    FranzServer = ?CHILD(franz, worker),
    TopicSuperSup = ?CHILD(topic_supersup, supervisor),
    % one_for_all correct? Seems like it. They can't work without each other.
    {ok, {{one_for_all, MaxRestart, MaxTime}, [TopicSuperSup, FranzServer]}}.