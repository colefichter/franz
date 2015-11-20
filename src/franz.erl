-module(franz).
-behaviour(gen_server).

% This module acts as the API that clients will interact with. It also tracks all of the topics in the broker.

% Client API
-export([start_link/0, new_topic/1, new_topic/2, count/0, partition_count/1]).

% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-define(SERVER, ?MODULE).

start_link() -> gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

% Start a topic with a single partition
new_topic(Name) -> new_topic(Name, 1).

% TODO: simplify API by returning just Pid rather than {ok, Pid}?

% Start a topic with multiple partitions
new_topic(Name, Partitions) when Partitions > 0 -> 
    gen_server:call(?MODULE, {new_topic, Name, Partitions}).

count() -> gen_server:call(?MODULE, {count}).

partition_count(TopicPid) when is_pid(TopicPid) -> topic:partition_count(TopicPid);
partition_count(Name) -> gen_server:call(?MODULE, {partition_count, Name}).

% gen_server callbacks
init([]) -> 
    Topics = dict:new(),
    {ok, Topics}.

handle_call({partition_count, Name}, _From, Topics) ->
    Reply = case dict:find(Name, Topics) of
        error -> {error, unknown_topic};
        {ok, TopicPid} -> gen_server:call(TopicPid, {partition_count})
    end,
    {reply, Reply, Topics};
handle_call({new_topic, Name, Partitions}, _From, Topics) ->
     % TODO: refactor. Server must keep track of all topics.
    TopicPid = case topic_supersup:start_topic(Name, Partitions) of
        {ok, Pid} -> Pid;
        {error, {already_started, Pid}} -> Pid
    end,
    NewTopics = dict:store(Name, TopicPid, Topics),
    {reply, {ok, Pid}, NewTopics};
handle_call({count}, _From, Topics) -> {reply, dict:size(Topics), Topics};

handle_call(_Request, _From, State) -> {reply, {error, unknown_call}, State}.
handle_cast(_Msg, State)            -> {noreply, State}.
handle_info(_Info, State)           -> {noreply, State}.
terminate(_Reason, _State)          -> ok.
code_change(_OldVsn, State, _Extra) -> {ok, State}.
