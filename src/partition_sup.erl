-module(partition_sup).
-export([start_link/0, init/1]).
-behaviour(supervisor).
 
% start_link(MFA = {_,_,_}) ->
%     supervisor:start_link(?MODULE, MFA).
start_link() -> supervisor:start_link(?MODULE, []). % Must not be a registered process!
 
%init({M,F,A}) ->
init([]) ->
    MaxRestart = 5,
    MaxTime = 3000,
    Partition = {partition, {partition, start_link, []}, permanent, 5000, worker, [partition]},
    {ok, {{simple_one_for_one, MaxRestart, MaxTime}, [Partition]}}.