-module(gen_timed_producer).

-behaviour(gen_server).

-export([behaviour_info/1]).

-export([start_link/4]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

% Callbacks that a producer module must support
behaviour_info(callbacks) ->
    [{interval,1},
     {do_one_cycle,1}
    ];
behaviour_info(_Other) ->
    undefined.

% CLIENT API:
start_link({local, Name}, CallbackModule, Topic, InitArgs) when is_list(Topic) ->
    gen_server:start_link({local, Name}, ?MODULE, {CallbackModule, Topic, InitArgs}, []).

% gen_server callbacks
init({CallbackModule, Topic, InitArgs}) -> %TODO: multiple partitions
    {ok, TopicPid} = franz:new_topic(Topic),
    {ok, State} = CallbackModule:init(InitArgs),
    create_trigger(CallbackModule, State),
    {ok, {CallbackModule, TopicPid, State}}.

handle_info(trigger, {CallbackModule, TopicPid, InternalState}) ->
    NewInternalState = case CallbackModule:do_one_cycle(InternalState) of %TODO: do we pass any args into here? Maybe allow local state?
        ok -> InternalState;
        {next_message, Message, AdjustedInternalState} ->
            topic:put(TopicPid, Message),
            AdjustedInternalState
    end,
    create_trigger(CallbackModule, NewInternalState),
    {noreply, {CallbackModule, TopicPid, NewInternalState}};
handle_info(_Info, State) -> {noreply, State}.
handle_call(_Request, _From, State) -> {reply, {error, unknown_call}, State}.
handle_cast(_Msg, State) -> {noreply, State}.
terminate(_Reason, _State) -> ok.
code_change(_OldVsn, State, _Extra) -> {ok, State}.

% INTERNAL HELPERS
create_trigger(CallbackModule, InternalState) -> 
    {ok, Interval} = CallbackModule:interval(InternalState), %TODO: do we pass any args/state here to adjust the delay?
    erlang:send_after(Interval, self(), trigger).