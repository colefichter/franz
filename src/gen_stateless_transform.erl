-module(gen_stateless_transform).

-behaviour(gen_server).

-export([behaviour_info/1]).

-export([start_link/4, start_link/5, load_last_offset/1]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-define(DEFAULT_INTERVAL, 50).

% Callbacks that a producer module must support
behaviour_info(callbacks) -> [{process,3}];
behaviour_info(_Other) -> undefined.

-record(state, {
        cont = {},
        name = "",
        input_topic = "", %InTopic remains a string
        output_topic = "", %OutTopic ends up being a topic server Pid.
        mod = null,
        interval = ?DEFAULT_INTERVAL
    }).

% TODO:
% Offsets are committed, but only at safe shutdown. Should the CSS have a timer to write to disk periodically?
%  Some kind of API to make reads from disk easier

% CLIENT API:

% Read from a single-partition input topic, write to a single-partition output topic: 
start_link({local, Name}, Mod, InTopic, OutTopic) when is_list(OutTopic) ->
    start_link({local, Name}, Mod, InTopic, OutTopic, ?DEFAULT_INTERVAL).

% Read from a single-partition input topic, write to a single-partition output topic, with specified poll interval.
start_link({local, Name}, Mod, InTopic, OutTopic, Interval) when is_list(OutTopic), is_integer(Interval), Interval > 0 ->
    % TODO: make an API for getting the disk partition:
    InTopic1 = InTopic ++ "1",
    gen_server:start_link({local, Name}, ?MODULE, {Name, Mod, InTopic1, OutTopic, Interval}, []).

% gen_server callbacks
init({Name, Mod, InTopic, OutTopic, Interval}) -> %TODO: multiple partitions
    {ok, OT} = franz:new_topic(OutTopic),
    io:format("~p got output topic ~p ~p~n", [self(), OutTopic, OT]),
    LastOffset = load_last_offset(Name),
    io:format("~p beginning at offset ~p~n", [self(), LastOffset]),
    Continuation = advance_to_offset(InTopic, LastOffset),
    await(Interval),
    FirstState = #state{name=Name, cont=Continuation, input_topic=InTopic, output_topic=OT, 
                        mod=Mod, interval=Interval},
    {ok, FirstState}.

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

read_one_chunk(State = #state{cont=Continuation, input_topic=InTopic, output_topic=OT, mod=Mod, interval=I}) ->
    % TODO: wrap this in an API. What to do about partitions? What about number of records to pull?
    NewCont = case disk_log:chunk(InTopic, Continuation, 1) of
        eof ->
            await(I), % Wait for more records
            Continuation;
        {NewContinuation, [Message]} ->
            {{offset, O}, {key, K}, {value, V}} = Message,
            process_and_put(Mod, OT, O, K, V),
            consumer_state_server:store(State#state.name, O),
            invoke_do_one_cycle(),
            NewContinuation
    end,
    State#state{cont=NewCont}.

load_last_offset(Name) ->
    case consumer_state_server:lookup(Name) of
        {value, not_found} -> 0;
        {value, Offset} -> Offset
    end.

advance_to_offset(_InTopic, 0) -> start;
advance_to_offset(InTopic, O) -> advance_to_offset(InTopic, O, start).
advance_to_offset(InTopic, O, Continuation) ->
    case disk_log:chunk(InTopic, Continuation, 1) of
        eof -> Continuation;
        {NewContinuation, [{{offset, X}, {key, _}, {value, _}}]} ->
            case X >= O of
                true -> NewContinuation; 
                false -> advance_to_offset(InTopic, O, NewContinuation)
            end
    end.

process_and_put(Mod, OutTopic, Offset, K, V) ->
    case Mod:process(Offset, K, V) of
        ok -> ok;
        {OutK, OutV} -> 
            topic:put(OutTopic, OutK, OutV)
    end.