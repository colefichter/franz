-module(time_consumer).

-behaviour(gen_server).

-export([start_link/0, process/0, run/0]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-define(SERVER, ?MODULE).


% Committing offsets?
% Reprocessing? What to do in crash/restart?




process() -> gen_server:cast(?SERVER, {process}).


run() ->
    time_producer:start_link(),
    timer:sleep(50), % Allow the disk log to start...
    start_link().



start_link() ->
    time_producer:start_link(),
    gen_server:start_link({local, ?SERVER}, ?MODULE, {}, []).

init({}) ->
    process(),
    {ok, start}.

handle_call(_Request, _From, State) ->
    {reply, {error, unknown_call}, State}.

handle_cast({process}, State)   -> handle(State);
handle_cast(_Msg, State)        -> {noreply, State}.

handle_info({wake_up}, State)   -> handle(State);
handle_info(_Info, State)       -> {noreply, State}.

terminate(_Reason, _State) -> ok.

code_change(_OldVsn, State, _Extra) -> {ok, State}.



handle(State) ->
    %io:format("~p starting a new cycle...~n", [self()]),
    NewState = read_one_chunk(State),
    {noreply, NewState}.

read_one_chunk(Continuation) ->
    % TODO: wrap this in an API. What to do about partitions? What about number of records to pull?
    case disk_log:chunk("time_producer1", Continuation, 5) of
        eof ->
            await(), % Wait for more records
            Continuation;
        {NewContinuation, Records} ->
            print(Records),
            process(), % Trigger the next processing round immediately
            NewContinuation
    end.

print([]) -> ok;
print([H|T]) ->
    io:format("~p time_consumer processed message ~p~n", [self(), H]),
    print(T).

await() -> 
    %io:format(" *** SLEEPING ~p~n", [self()]),
    Interval = 750, % TODO will have to be configurable
    erlang:send_after(Interval, self(), {wake_up}).