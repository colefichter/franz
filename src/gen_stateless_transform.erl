-module(gen_stateless_transform).


-behaviour(gen_server).

-export([behaviour_info/1]).

-export([start_link/4, start_link/5]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-define(DEFAULT_INTERVAL, 50).

% Callbacks that a producer module must support
behaviour_info(callbacks) -> [{process,2}];
behaviour_info(_Other) -> undefined.

% TODO:
%  Committing offsets?
%  Reprocessing? What to do in crash/restart?
%  Some kind of API to make reads from disk easier

% CLIENT API:

% Read from a single-partition input topic, write to a single-partition output topic: 
start_link({local, Name}, Mod, InTopic, OutTopic) when is_list(OutTopic) ->
    start_link({local, Name}, Mod, InTopic, OutTopic, ?DEFAULT_INTERVAL).

% Read from a single-partition input topic, write to a single-partition output topic, with specified poll interval.
start_link({local, Name}, Mod, InTopic, OutTopic, Interval) when is_list(OutTopic), is_integer(Interval), Interval > 0 ->
    % TODO: make an API for getting the disk partition:
    InTopic1 = InTopic ++ "1",
    gen_server:start_link({local, Name}, ?MODULE, {Mod, InTopic1, OutTopic, Interval}, []).

% gen_server callbacks
init({Mod, InTopic, OutTopic, Interval}) -> %TODO: multiple partitions
    {ok, OT} = franz:new_topic(OutTopic),
    io:format("~p got output topic ~p ~p~n", [self(), OutTopic, OT]),
    await(Interval),
    {ok, {{cont, start}, {input_topic, InTopic}, {output_topic, OT}, {module, Mod}, {interval, Interval}}}.

handle_cast({process}, State)       -> handle(State);
handle_cast(_Msg, State)            -> {noreply, State}.
handle_info({wake_up}, State)       -> handle(State);
handle_info(_Info, State)           -> {noreply, State}.
handle_call(_Request, _From, State) -> {reply, {error, unknown_call}, State}.
terminate(_Reason, _State)          -> ok.
code_change(_OldVsn, State, _Extra) -> {ok, State}.

% Internal helpers
invoke_do_one_cycle() -> gen_server:cast(self(), {process}).
await(Interval)       -> erlang:send_after(Interval, self(), {wake_up}).

handle(State) ->
    NewState = read_one_chunk(State),
    {noreply, NewState}.

read_one_chunk({{cont, Continuation}, {input_topic, InTopic}, {output_topic, OT}, {module, Mod}, {interval, I}}) ->
    % TODO: wrap this in an API. What to do about partitions? What about number of records to pull?
    NewCont = case disk_log:chunk(InTopic, Continuation, 1) of
        eof ->
            await(I), % Wait for more records
            Continuation;
        {NewContinuation, [Message]} ->
            {key, K, value, V} = Message,
            % {OutK, OutV} = Mod:process(K,V),
            case Mod:process(K, V) of
                ok -> ok;
                {OutK, OutV} -> 
                    topic:put(OT, OutK, OutV)
            end,
            invoke_do_one_cycle(),
            NewContinuation
    end,
    {{cont, NewCont}, {input_topic, InTopic}, {output_topic, OT}, {module, Mod}, {interval, I}}.