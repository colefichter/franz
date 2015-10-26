-module(partition_tests).

% Unit tests
%-------------------------------------------------------------------------
-include_lib("eunit/include/eunit.hrl").

it_should_start_empty_test() ->
    {ok, P} = partition:start_link(),
    ?assertEqual(0, partition:length(P)),
    ?assertEqual(offset_out_of_bounds, partition:get(P, 1)).

it_should_store_values_test() ->
    {ok, P} = partition:start_link(),
    ok = partition:put(P, "K1", "V1"),
    ?assertEqual(1, partition:length(P)),
    ?assertEqual({"K1", "V1"}, partition:get(P, 1)).

it_must_store_values_in_order_test() ->
    {ok, P} = partition:start_link(),
    A = {"A", "First"},
    B = {"B", <<"Second">>},
    C = {c, third},
    [partition:put(P, X) || X <- [A,B,C]],
    ?assertEqual(A, partition:get(P, 1)),
    ?assertEqual(B, partition:get(P, 2)),
    ?assertEqual(C, partition:get(P, 3)).

