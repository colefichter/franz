-module(iterator).
-behaviour(gen_server).

-export([start_link/2, start_link/3, start_link/4]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-define(SERVER, ?MODULE).
-define(INTERVAL, 2500). % TODO: make configurable!

start_link(TopicName, Callback) when is_list(TopicName), is_function(Callback) -> 
    start_link(TopicName, 1, Callback, 0).

start_link(TopicName, PartitionNumber, Callback) when is_list(TopicName), is_function(Callback) -> 
    start_link(TopicName, PartitionNumber, Callback, 0).

start_link(TopicName, PartitionNumber, Callback, LastOffset) when is_list(TopicName), is_function(Callback) ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, {TopicName, PartitionNumber, Callback, LastOffset}, []).

init({TopicName, PartitionNumber, Callback, LastOffset}) ->
    % TODO: make an API for getting the disk log name:
    LogName = TopicName ++ integer_to_list(PartitionNumber),
    Continuation = advance_to_offset(LogName, LastOffset),
    invoke_do_one_cycle(),
    {ok, {LogName, Continuation, Callback}}.

handle_call(_Request, _From, State) ->
    {reply, {error, unknown_call}, State}.

handle_cast({process}, State) -> handle(State);
handle_cast(_Msg, State) -> {noreply, State}.
handle_info({wake_up}, State) -> handle(State);
handle_info(_Info, State) -> {noreply, State}.
terminate(_Reason, _State) -> ok.
code_change(_OldVsn, State, _Extra) -> {ok, State}.

% Internal Helpers
await(Interval) -> erlang:send_after(Interval, self(), {wake_up}).
invoke_do_one_cycle() -> gen_server:cast(self(), {process}).

handle({LogName, Continuation, CB}) ->
    NewCont = case disk_log:chunk(LogName, Continuation, 1) of
        eof ->
            await(?INTERVAL), % Wait for more records
            Continuation;
        {NewContinuation, [Message]} ->
            CB(Message),
            invoke_do_one_cycle(),
            NewContinuation
    end,
    {noreply, {LogName, NewCont, CB}}.

advance_to_offset(_LogName, 0) -> start;
advance_to_offset(LogName, O) -> advance_to_offset(LogName, O, start).
advance_to_offset(LogName, O, Continuation) ->
    case disk_log:chunk(LogName, Continuation, 1) of
        eof -> Continuation;
        {NewContinuation, [{{offset, X}, {key, _}, {value, _}}]} ->
            case X >= O of
                true -> NewContinuation; 
                false -> advance_to_offset(LogName, O, NewContinuation)
            end
    end.