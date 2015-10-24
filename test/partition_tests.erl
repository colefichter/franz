-module(partition_tests).

% Unit tests
%-------------------------------------------------------------------------
-include_lib("eunit/include/eunit.hrl").

it_should_start_empty_test() ->
    {ok, P} = partition:start_link(),
    ?assertEqual(0, partition:length(P)).

it_should_store_values_test() ->
    {ok, P} = partition:start_link(),
    ok = partition:put(P, "K1", "V1"),
    ?assertEqual(1, partition:length(P)).
