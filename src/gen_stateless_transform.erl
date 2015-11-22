-module(gen_stateless_transform).

-behaviour(gen_server).

-export([behaviour_info/1]).

-export([start_link/4, start_link/5, load_last_offset/1, process/2]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-define(DEFAULT_INTERVAL, 50).

% Callbacks that a producer module must support
behaviour_info(callbacks) -> [{process,3}];
behaviour_info(_Other) -> undefined.

% TODO:
% Offsets are committed, but only at safe shutdown. Should the CSS have a timer to write to disk periodically?
%  Some kind of API to make reads from disk easier

% CLIENT API:

% Read from a single-partition input topic, write to a single-partition output topic: 
start_link({local, Name}, Mod, InTopic, OutTopic) when is_list(OutTopic) ->
    start_link({local, Name}, Mod, InTopic, OutTopic, ?DEFAULT_INTERVAL).

% Read from a single-partition input topic, write to a single-partition output topic, with specified poll interval.
start_link({local, Name}, Mod, InTopic, OutTopic, Interval) when is_list(OutTopic), is_integer(Interval), Interval > 0 ->
    gen_server:start_link({local, Name}, ?MODULE, {Name, Mod, InTopic, OutTopic, Interval}, []).

process(Pid, Message) -> gen_server:cast(Pid, {process, Message}).

% gen_server callbacks
init({Name, Mod, InTopic, OutTopic, _Interval}) -> %TODO: multiple partitions
    {ok, OT} = franz:new_topic(OutTopic),
    LastOffset = load_last_offset(Name),
    io:format("~p beginning at offset ~p~n", [self(), LastOffset]),
    S = self(),
    CB = fun(Message) -> process(S, Message) end,
    {ok, _Pid} = iterator:start_link(InTopic, 1, CB, LastOffset), % TODO: mointor or link?
    {ok, {Name, Mod, OT}}.

handle_cast({process, Message}, State = {Name, Mod, OT}) ->
    {{offset, O}, {key, K}, {value, V}} = Message,
    process_and_put(Mod, OT, O, K, V),
    consumer_state_server:store(Name, O),
    {noreply, State};
handle_cast(_Msg, State)            -> {noreply, State}.
handle_info(_Info, State)           -> {noreply, State}.
handle_call(_Request, _From, State) -> {reply, {error, unknown_call}, State}.
terminate(_Reason, _State)          -> ok.
code_change(_OldVsn, State, _Extra) -> {ok, State}.

% Internal helpers
load_last_offset(Name) ->
    case consumer_state_server:lookup(Name) of
        {value, not_found} -> 0;
        {value, Offset} -> Offset
    end.

process_and_put(Mod, OT, Offset, K, V) ->
    case Mod:process(Offset, K, V) of
        ok -> ok;
        {OutK, OutV} -> 
            topic:put(OT, OutK, OutV)
    end.