-module(topic_tests).
-compile([export_all]).

% Unit tests
%-------------------------------------------------------------------------
-include_lib("eunit/include/eunit.hrl").

it_must_do_round_robin_partitioning_test() ->
    ?assertEqual(1, topic:select_partition_number(k, 4, 0)),
    ?assertEqual(2, topic:select_partition_number(k, 4, 1)),
    ?assertEqual(3, topic:select_partition_number(k, 4, 2)),
    ?assertEqual(0, topic:select_partition_number(k, 4, 3)),
    ?assertEqual(0, topic:select_partition_number(k, 1, 0)),
    ?assertEqual(0, topic:select_partition_number(k, 1, 1)),
    ?assertEqual(0, topic:select_partition_number(k, 1, 2)),
    ?assertEqual(0, topic:select_partition_number(k, 1, 3)),
    ?assertEqual(0, topic:select_partition_number(k, 1, -1)). % Sanity check

it_must_have_the_default_partitioner_test() ->
    application:start(franz),
    {ok, T} = franz:new_topic("topic_tests"),
    DefaultPartitioner = topic:get_partitioner(T),
    ?assertEqual(fun topic:select_partition_number/3, DefaultPartitioner),
    application:stop(franz).

it_must_set_the_partitioner_test() ->
    application:start(franz),
    {ok, T} = franz:new_topic("topic_tests"),
    TestPartitioner = fun(_Key, _NumPartitions, _LastSelected) -> 99999999 end,
    ok = topic:set_partitioner(T, TestPartitioner),
    DefaultPartitioner = topic:get_partitioner(T),
    ?assertEqual(TestPartitioner, DefaultPartitioner),
    application:stop(franz).

it_must_validate_the_partioner_test() ->
    application:start(franz),
    {ok, T} = franz:new_topic("topic_tests"),
    TestPartitioner = fun() -> 99999999 end, % Invalid args!
    Reply = topic:set_partitioner(T, TestPartitioner),
    ?assertEqual({error, wrong_arity}, Reply),
    application:stop(franz).