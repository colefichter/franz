-module(franz_app).

-behaviour(application).

%% Application callbacks
-export([start/2, stop/1, bootstrap/0]).

bootstrap() -> 
    spawn(fun() -> 
        % Allow the application infrastructure to start...
        timer:sleep(250),
        % ...then launch our application.
        application:start(franz)
    end).

%% ===================================================================
%% Application callbacks
%% ===================================================================

start(_StartType, _StartArgs) ->
    {ok, Cwd} = file:get_cwd(),
    % Will this work on windows?
    DataDir = Cwd ++ "/data",
    io:format("FRANZ is starting in ~p with data directory ~p~n", [Cwd, DataDir]),
    file:make_dir(DataDir),
    franz_sup:start_link().

stop(_State) ->
    ok.


