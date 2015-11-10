-module(time_producer).

-behaviour(gen_timed_producer).

-export([start_link/0]).
-export([init/1, interval/1, do_one_cycle/1]).

-define(TOPIC, "time_producer").

% Client API
start_link() -> gen_timed_producer:start_link({local, ?MODULE}, ?MODULE, ?TOPIC, []).

% Callbacks
init([]) -> {ok, []}. %This simple producer doesn't need state.

% Having an interval callback allows us to adjust it dynamically after each cycle.
interval(_State) -> {ok, 1000}.

do_one_cycle(State) ->
    io:format("~p time_producer is doing one cycle.~n", [self()]),
    % To publish a message to the topic, return {next_message, Message, State}.
    % To return without publishing a message, just return ok.
    Message = calendar:local_time(),
    {next_message, Message, State}.