-module(partition).

-behaviour(gen_server).

% Client API
-export([start_link/0, length/1, put/3]).

% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-define(SERVER, ?MODULE).

% Client API
start_link() -> gen_server:start_link(?MODULE, [], []).

length(ServerPid) -> gen_server:call(ServerPid, {length}).

put(ServerPid, Key, Value) -> gen_server:cast(ServerPid, {put, Key, Value}).


% gen_server callbacks
init([]) -> {ok, []}.

handle_call({length}, _From, Log) -> {reply, erlang:length(Log), Log};
handle_call(_Request, _From, State) -> {reply, {error, unknown_call}, State}.

handle_cast({put, Key, Value}, Log) -> {noreply, [{Key, Value}|Log]};
handle_cast(_Msg, State) ->
    {noreply, State}.

%% @private
handle_info(_Info, State) ->
    {noreply, State}.

%% @private
terminate(_Reason, _State) ->
    ok.

%% @private
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.