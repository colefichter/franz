-module(time_producer).

-export([start_link/0]).

-behaviour(gen_server).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-define(SERVER, ?MODULE).
-define(INTERVAL, 1000).

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

init([]) ->
    {ok, Topic} = franz:new_topic("time_producer"),
    delay_trigger(),
    {ok, Topic}.

handle_call(_Request, _From, State) ->
    {reply, {error, unknown_call}, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(trigger, Topic) ->
    Message = calendar:local_time(),
    topic:put(Topic, Message),
    delay_trigger(),
    {noreply, Topic};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


% INTERNAL HELPERS
delay_trigger() -> erlang:send_after(?INTERVAL, self(), trigger).