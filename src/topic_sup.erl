-module(topic_sup).
-behaviour(supervisor).

-export([start_link/2, init/1]).

%% Helper macro for declaring children of supervisor
-define(CHILD(I, Type, Args), {I, {I, start_link, [Args]}, permanent, 5000, Type, [I]}).
 
start_link(Name, Partitions) -> 
    {ok, TopicSup} = supervisor:start_link(?MODULE, {Name, Partitions}),
    Children = supervisor:which_children(TopicSup),
    [{topic_server, TopicServerPid, worker, _Mods}] = lists:filter(fun({Id, _, _, _}) -> Id == topic_server  end, Children),
    {ok, TopicServerPid}.
 
%init({Name, Limit, MFA}) ->
init({Name, Partitions}) ->
    MaxRestart = 5,
    MaxTime = 3000,
    Topic = {topic_server, {topic_server, start_link, [Name, Partitions, self()]}, permanent, 5000, worker, [topic_server]},
    % one_for_all correct? Seems like it. They can't work without each other.
    {ok, {{one_for_all, MaxRestart, MaxTime}, [Topic]}}.