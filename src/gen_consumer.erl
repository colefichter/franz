-module (gen_consumer).

-export([behaviour_info/1]).

behaviour_info(callbacks) ->
    [{process, 1}].