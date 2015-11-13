-module(partition_tests).

% Unit tests
%-------------------------------------------------------------------------
-include_lib("eunit/include/eunit.hrl").

it_should_start_empty_test() ->
    file:delete("data/it_should_start_empty_test1"),
    {ok, _P} = partition:start_link(self(), "it_should_start_empty_test", 1),
    % Wait for writes... should put() be a call rather than cast? If you remove this,
    % chances are the match will fail with EOF!
    timer:sleep(100),
    ?assertEqual(eof, disk_log:chunk("it_should_start_empty_test1", start)).

it_must_store_values_in_order_test() ->
    file:delete("data/it_must_store_values_in_order_test1"),
    {ok, P} = partition:start_link(self(), "it_must_store_values_in_order_test", 1),
    L = [A,B,C] = [{"A", "First"}, {"B", <<"Second">>}, {c, third}],
    D = something_else, %This one is going to be saved with an explicit key
    [partition:put(P, X) || X <- L],
    partition:put(P, "D1", D),
    % Wait for writes... should put() be a call rather than cast? If you remove this,
    % chances are the match will fail with EOF!
    timer:sleep(100),
    {_, ResultL} = disk_log:chunk("it_must_store_values_in_order_test1", start, 4),
    [RA, RB, RC, RD] = ResultL,
    ?assertEqual({{offset, 1}, {key, null}, {value, A}}, RA),
    ?assertEqual({{offset, 2}, {key, null}, {value, B}}, RB),
    ?assertEqual({{offset, 3}, {key, null}, {value, C}}, RC),
    ?assertEqual({{offset, 4}, {key, "D1"}, {value, D}}, RD).

it_must_create_a_disk_log_name_test() ->
    ?assertEqual("TEST0", partition:log_name("TEST", 0)),
    ?assertEqual("Sports1", partition:log_name("Sports", 1)),
    ?assertEqual("Movies999", partition:log_name("Movies", 999)).

it_must_format_messages_test() ->
    M1 = {{offset, 1}, {key, k}, {value, v}},
    M2 = {{offset, 2}, {key, "Cole"}, {value, "Hi"}},
    M3 = {{offset, 3}, {key, <<123>>}, {value, <<"Hi">>}},
    ?assertEqual(M1, partition:format_message(1, k, v)),
    ?assertEqual(M2, partition:format_message(2, "Cole", "Hi")),
    ?assertEqual(M3, partition:format_message(3, <<123>>, <<"Hi">>)).