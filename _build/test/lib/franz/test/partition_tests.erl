-module(partition_tests).

% Unit tests
%-------------------------------------------------------------------------
-include_lib("eunit/include/eunit.hrl").

% it_should_start_empty_test() ->
%     {ok, P} = partition:start_link("TEST", 1),
%     ?assertEqual(0, partition:length(P)),
%     ?assertEqual(offset_out_of_bounds, partition:get(P, 1)).

% it_should_store_values_test() ->
%     {ok, P} = partition:start_link("TEST", 1),
%     ok = partition:put(P, "K1", "V1"),
%     ?assertEqual(1, partition:length(P)),
%     ?assertEqual({"K1", "V1"}, partition:get(P, 1)).

it_must_store_values_in_order_test() ->
    file:delete("data/it_must_store_values_in_order_test1"),
    {ok, P} = partition:start_link("it_must_store_values_in_order_test", 1),
    A = {"A", "First"},
    B = {"B", <<"Second">>},
    C = {c, third},
    L = [A,B,C],
    [partition:put(P, X) || X <- L],
    % Wait for writes... should put() be a call rather than cast? If you remove this,
    % chances are the match will fail with EOF!
    timer:sleep(100),
    {_, ResultL} = disk_log:chunk("it_must_store_values_in_order_test1", start, 3),
    [RA, RB, RC] = ResultL,
    ?assertEqual({key, null, value, A}, RA),
    ?assertEqual({key, null, value, B}, RB),
    ?assertEqual({key, null, value, C}, RC).

it_must_create_a_disk_log_name_test() ->
    ?assertEqual("TEST0", partition:log_name("TEST", 0)),
    ?assertEqual("Sports1", partition:log_name("Sports", 1)),
    ?assertEqual("Movies999", partition:log_name("Movies", 999)).