-module(time_transformer).

-behaviour(gen_stateless_transform).

-export([process/3, start_link/0]).

start_link() -> 
    time_producer:start_link(),
    timer:sleep(50),
    InTopic = "time_producer",
    OutTopic = "transformed_times",
    PollingInterval = 3000,
    gen_stateless_transform:start_link({local, ?MODULE}, ?MODULE, InTopic, OutTopic, PollingInterval).

%This is the core transformation callback
process(Offset, Key, Value = {Date, Time}) ->
    NewValue = {processed, Date, Time},
    io:format("~p transforming O:~p K:~p V:~p~n   -> ~p~n", [self(), Offset, Key, Value, NewValue]),
    % To publish a message to the output topic, return {Key, Value}.
    % To return without publishing a message, just return ok.
    {Key, NewValue}.