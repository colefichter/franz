-module(topic_supersup).
-behaviour(supervisor).
-export([start_link/0, start_topic/2, stop_topic/1]).
-export([init/1]).

% Is {local, topic} right? This was ppool before...
start_link() -> supervisor:start_link({local, ?MODULE}, ?MODULE, []).

start_topic(Name, Partitions) ->
    ChildSpec = {Name,
                {topic_sup, start_link, [Name, Partitions]},
                permanent, 3000, supervisor, [topic_sup]},
    supervisor:start_child(?MODULE, ChildSpec).

stop_topic(Name) ->
    supervisor:terminate_child(?MODULE, Name),
    supervisor:delete_child(?MODULE, Name).

init([]) ->
    MaxRestart = 6,
    MaxTime = 3000,
    % one_for_one -> isolate topics from each other
    {ok, {{one_for_one, MaxRestart, MaxTime}, []}}.
