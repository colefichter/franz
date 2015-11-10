-module(st).

-behaviour(gen_stateless_transform).

-export([process/2, start_link/0]).

start_link() -> 
    time_producer:start_link(),
    timer:sleep(50),
    gen_stateless_transform:start_link({local, ?MODULE}, ?MODULE, "transformed_times").

%This is the core transformation callback
process(Key, Value) ->
    io:format("~p transforming ~p ~p~n", [self(), Key, Value]),
    {Date, Time} = Value,
    NewValue = {processed, Date, Time},
    {Key, NewValue}.