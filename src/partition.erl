-module(partition).

-behaviour(gen_server).

% Client API
-export([start_link/2, put/2, put/3]).

% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-export([log_name/1, log_name/2]).

-define(SERVER, ?MODULE).

-record(state, {topic = "",
                number = 0,
                log_name = ""
                }).

% Client API
start_link(Topic, PartitionNumber) -> gen_server:start_link(?MODULE, {Topic, PartitionNumber}, []).

%length(ServerPid) -> gen_server:call(ServerPid, {length}).

put(ServerPid, Key, Value) -> gen_server:cast(ServerPid, {put, Key, Value}).
% This turns out to be bad! Make K/V definition explicit!
%put(ServerPid, {Key, Value}) -> gen_server:cast(ServerPid, {put, Key, Value}).
put(ServerPid, Value) -> gen_server:cast(ServerPid, {put, null, Value}).

%get(ServerPid, Offset) -> gen_server:call(ServerPid, {get, Offset}).

% gen_server callbacks
init(Args = {Topic, PartitionNumber}) -> 
    LogName = log_name(Args),
    State = #state{topic=Topic, number=PartitionNumber, log_name=LogName},
    ok = open_disk_log(LogName),
    {ok, State}.

%handle_call({get, Offset}, _From, Items) when Offset > erlang:length(Items) -> %TODO cache length in state
%    {reply, offset_out_of_bounds, Items};
% handle_call({get, Offset}, _From, Items) ->
%     % The in-memory list is in the reverse order from the disk commit log. (This is because)
%     % Erlang lists are O(1) when appending to the head, where as a disk log is O(1) when
%     % appending to the tail. Thus, we must convert the offset into a list index.
%     Index = 1 + erlang:length(Items) - Offset,
%     {reply, lists:nth(Index, Items), Items};
%handle_call({length}, _From, Items) -> {reply, erlang:length(Items), Items};
handle_call(_Request, _From, State) -> {reply, {error, unknown_call}, State}.

handle_cast({put, Key, Value}, State) -> 
    % log() is synchronous. We've already replied to the caller, but this partition process will wait
    % until the disk write completes before moving to the next message.
    % This might be wrong! If a disk write fails, we're going to respond "ok" to the client!
    % But changing this from cast to call will force the client to block while the write happens...
    % which is worse?
    % See partition_tests:it_must_store_values_in_order_test() for an example of async issues.
    Message = {key, Key, value, Value},
    disk_log:log(State#state.log_name, Message),
    {noreply, State};
handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, State) ->
    disk_log:clost(State#state.log_name),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


% Internal Helpers
log_name({Topic, PartitionNumber}) -> log_name(Topic, PartitionNumber).
log_name(Topic, PartitionNumber) -> Topic ++ integer_to_list(PartitionNumber).

open_disk_log(LogName) ->
    Options = [{name, LogName}, {file, "data/" ++ LogName}],
    case disk_log:open(Options) of
        {ok, LogName} -> ok;
        {repaired, LogName, _, _} -> ok
    end.