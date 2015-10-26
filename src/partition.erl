-module(partition).

-behaviour(gen_server).

% Client API
-export([start_link/0, length/1, put/2, put/3, get/2]).

% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-define(SERVER, ?MODULE).

% Client API
start_link() -> gen_server:start_link(?MODULE, [], []).

length(ServerPid) -> gen_server:call(ServerPid, {length}).

put(ServerPid, Key, Value) -> gen_server:cast(ServerPid, {put, Key, Value}).
put(ServerPid, {Key, Value}) -> gen_server:cast(ServerPid, {put, Key, Value}).

get(ServerPid, Offset) -> gen_server:call(ServerPid, {get, Offset}).

% gen_server callbacks
init([]) -> {ok, []}.

handle_call({get, Offset}, _From, Items) when Offset > erlang:length(Items) -> %TODO cache length in state
    {reply, offset_out_of_bounds, Items};
handle_call({get, Offset}, _From, Items) ->
    % The in-memory list is in the reverse order from the disk commit log. (This is because)
    % Erlang lists are O(1) when appending to the head, where as a disk log is O(1) when
    % appending to the tail. Thus, we must convert the offset into a list index.
    Index = 1 + erlang:length(Items) - Offset,
    {reply, lists:nth(Index, Items), Items};
handle_call({length}, _From, Items) -> {reply, erlang:length(Items), Items};
handle_call(_Request, _From, State) -> {reply, {error, unknown_call}, State}.

handle_cast({put, Key, Value}, Items) -> {noreply, [{Key, Value}|Items]};
handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.