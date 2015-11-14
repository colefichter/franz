-module(gen_stateless_transform_tests).

% Unit tests
%-------------------------------------------------------------------------
-include_lib("eunit/include/eunit.hrl").

load_last_offset_test() ->
    Name = "load_last_offset_test",
    {ok, StateServer} = consumer_state_server:start_link(),
    ?assertEqual(0, gen_stateless_transform:load_last_offset(Name)),
    consumer_state_server:store(Name, 99),
    ?assertEqual(99, gen_stateless_transform:load_last_offset(Name)),
    consumer_state_server:stop().