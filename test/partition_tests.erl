-module(partition_tests).

% Unit tests
%-------------------------------------------------------------------------
-include_lib("eunit/include/eunit.hrl").

it_should_start_empty_test() ->
    file:delete("data/it_should_start_empty_test"),
    {ok, P} = partition:start_link("it_should_start_empty_test", 1),
    % Wait for writes... should put() be a call rather than cast? If you remove this,
    % chances are the match will fail with EOF!
    timer:sleep(100),
    ?assertEqual(eof, disk_log:chunk("it_should_start_empty_test1", start)).

it_must_store_values_in_order_test() ->
    file:delete("data/it_must_store_values_in_order_test1"),
    {ok, P} = partition:start_link("it_must_store_values_in_order_test", 1),
    L = [A,B,C] = [{"A", "First"}, {"B", <<"Second">>}, {c, third}],
    D = something_else, %This one is going to be saved with an explicit key
    [partition:put(P, X) || X <- L],
    partition:put(P, "D1", D),
    % Wait for writes... should put() be a call rather than cast? If you remove this,
    % chances are the match will fail with EOF!
    timer:sleep(100),
    {_, ResultL} = disk_log:chunk("it_must_store_values_in_order_test1", start, 4),
    [RA, RB, RC, RD] = ResultL,
    ?assertEqual({key, null, value, A}, RA),
    ?assertEqual({key, null, value, B}, RB),
    ?assertEqual({key, null, value, C}, RC),
    ?assertEqual({key, "D1", value, D}, RD).

it_must_create_a_disk_log_name_test() ->
    ?assertEqual("TEST0", partition:log_name("TEST", 0)),
    ?assertEqual("Sports1", partition:log_name("Sports", 1)),
    ?assertEqual("Movies999", partition:log_name("Movies", 999)).