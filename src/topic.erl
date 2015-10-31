-module(topic).
-behaiour(gen_server).

-export([partition_count/1, select_partition_number/3, put/2, put/3]).

-export([start_link/3, init/1, handle_call/3, handle_cast/2, handle_info/2, code_change/3, terminate/2]).

% A topic keeps track of its partitions (think of it as a partition server).

%% The friendly supervisor is started dynamically!
-define(SPEC,
    {partition_sup,
    {partition_sup, start_link, []},
    temporary,
    10000,
    supervisor,
    [partition_sup]}).

-record(state, {name = "",
                num_partitions = 1,
                partition_sup,
                partitions = [],
                last_partition_selected = 0,
                partitioner = fun select_partition_number/3
                }).

put(TopicServerPid, Key, Value) -> gen_server:call(TopicServerPid, {put, Key, Value}).
put(TopicServerPid, Value) -> put(TopicServerPid, null, Value).

% CLIENT API
partition_count(TopicServerPid) -> gen_server:call(TopicServerPid, {partition_count}).

% TopicSup is the Pid of the supervisor of this server. We'll ask him to start the PartitionSup.
start_link(Name, NumPartitions, TopicSup) ->
    gen_server:start_link(?MODULE, {Name, NumPartitions, TopicSup}, []).

% gen_server callbacks
init({Name, NumPartitions, TopicSup}) ->
    %% We need to find the Pid of the worker supervisor from here,
    %% but alas, this would be calling the supervisor while it waits for us!
    self() ! {start_partition_sup, NumPartitions, TopicSup}, % TODO: pass number of partitions?
    State = #state{name = Name, num_partitions = NumPartitions},
    {ok, State}. %This list will hold the partition Pids


handle_call({put, null, Value}, _From, State) ->
    NumPartitions = State#state.num_partitions,
    Partitions = State#state.partitions,
    Reply = case NumPartitions > 1 of
        true -> {error, must_have_key, {num_partitions, NumPartitions}};
        false -> write_to_partition(Partitions, 1, null, Value)
    end,
    {reply, Reply, State};
handle_call({put, Key, Value}, _From, State) ->
    NumPartitions = State#state.num_partitions,
    Partitions = State#state.partitions,
    {Reply, NewState} = case NumPartitions == 1 of
        true -> 
            Reply = write_to_partition(Partitions, 1, Key, Value), %Ignore partition selection
            {Reply, State};
        false -> 
            LastPartitionSelected = State#state.last_partition_selected,
            PartitionFunction = State#state.partitioner,
            SelectedPartitionNumber = PartitionFunction(Key, NumPartitions, LastPartitionSelected),
            Reply = write_to_partition(Partitions, SelectedPartitionNumber, Key, Value),
            NewState1 = State#state{last_partition_selected=SelectedPartitionNumber},
            {Reply, NewState1}
    end,
    {reply, Reply, NewState};


handle_call({partition_count}, _From, State = #state{num_partitions = NumPartitions}) ->
    {reply, NumPartitions, State};
handle_call(_Msg, _From, State) -> {noreply, State}.

handle_cast(_Msg, State) -> {noreply, State}.

handle_info({start_partition_sup, NumPartitions, TopicSup}, State) ->
    {ok, PartitionSup} = supervisor:start_child(TopicSup, ?SPEC),
    link(PartitionSup),
    PartitionsList = start_partitions(State#state.name, PartitionSup, NumPartitions),
    NewState = State#state{partition_sup = PartitionSup, partitions = PartitionsList},
    {noreply, NewState};
handle_info(Msg, State) ->
    io:format("Unknown msg: ~p~n", [Msg]),
    {noreply, State}.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

terminate(_Reason, _State) ->
    ok.

% Internal helpers
start_partitions(Topic, PartitionSup, NumPartitions) ->
    start_partitions(Topic, PartitionSup, NumPartitions, []).

start_partitions(_Topic, _PartitionSup, 0, PartitionsList) -> PartitionsList;
start_partitions(Topic, PartitionSup, NumPartitions, PartitionsList) ->
    {ok, PartitionPid} = supervisor:start_child(PartitionSup, [Topic, NumPartitions]),
    %TODO: do we actually need to monitor this guy?
    _Ref = erlang:monitor(process, PartitionPid),
    start_partitions(Topic, PartitionSup, NumPartitions - 1, [PartitionPid| PartitionsList]).

% The default partitioner:
select_partition_number(_Key, NumPartitions, LastPartitionSelected) ->
    (LastPartitionSelected + 1) rem NumPartitions.
    
write_to_partition(Partitions, SelectedPartitionNumber, Key, Value) ->
    P = lists:nth(SelectedPartitionNumber, Partitions),
    partition:put(P, Key, Value).
