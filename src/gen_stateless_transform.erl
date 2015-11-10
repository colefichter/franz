-module(gen_stateless_transform).


-behaviour(gen_server).

-export([behaviour_info/1]).

-export([start_link/3]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

% Callbacks that a producer module must support
behaviour_info(callbacks) -> [{process,2}];
behaviour_info(_Other) -> undefined.



% Committing offsets?
% Reprocessing? What to do in crash/restart?
% Configurable sleep interval?
% Some kind of API to make reads from disk easier



% CLIENT API:
start_link({local, Name}, CallbackModule, OutputTopic) when is_list(OutputTopic) ->
    gen_server:start_link({local, Name}, ?MODULE, {CallbackModule, OutputTopic}, []).




% gen_server callbacks
init({CallbackModule, OutputTopic}) -> %TODO: multiple partitions
    {ok, OT} = franz:new_topic(OutputTopic),
    io:format("~p got output topic ~p ~p~n", [self(), OutputTopic, OT]),
    await(),
    {ok, {{cont, start}, {output_topic, OT}, {module, CallbackModule}}}.

handle_cast({process}, State)   -> handle(State);
handle_cast(_Msg, State)        -> {noreply, State}.

handle_info({wake_up}, State)   -> handle(State);
handle_info(_Info, State)       -> {noreply, State}.

handle_call(_Request, _From, State) -> {reply, {error, unknown_call}, State}.
terminate(_Reason, _State) -> ok.
code_change(_OldVsn, State, _Extra) -> {ok, State}.



handle(State) ->
    NewState = read_one_chunk(State),
    {noreply, NewState}.

read_one_chunk({{cont, Continuation}, {output_topic, OT}, {module, Mod}}) ->
    % TODO: wrap this in an API. What to do about partitions? What about number of records to pull?
    NewCont = case disk_log:chunk("time_producer1", Continuation, 1) of
        eof ->
            await(), % Wait for more records
            Continuation;
        {NewContinuation, [Message]} ->
            {key, K, value, V} = Message,
            {OutK, OutV} = Mod:process(K,V),
            topic:put(OT, OutK, OutV),
            invoke_do_one_cycle(),
            NewContinuation
    end,
    {{cont, NewCont}, {output_topic, OT}, {module, Mod}}.


invoke_do_one_cycle() -> gen_server:cast(self(), {process}).
await() -> 
    Interval = 50, % TODO will have to be configurable
    erlang:send_after(Interval, self(), {wake_up}).