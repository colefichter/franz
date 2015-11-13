-module(time_transformer).

-behaviour(gen_stateless_transform).

-export([process/2, start_link/0]).

start_link() -> 
    time_producer:start_link(),
    timer:sleep(50),
    InTopic = "time_producer",
    OutTopic = "transformed_times",
    PollingInterval = 3000,
    gen_stateless_transform:start_link({local, ?MODULE}, ?MODULE, InTopic, OutTopic, PollingInterval).

%This is the core transformation callback
process(Key, Value = {Date, Time}) ->
    io:format("~p transforming ~p ~p~n", [self(), Key, Value]),
    NewValue = {processed, Date, Time},
    % To publish a message to the output topic, return {Key, Value}.
    % To return without publishing a message, just return ok.
    {Key, NewValue}.