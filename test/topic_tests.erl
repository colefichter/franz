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
    ?assertEqual(0, topic:select_partition_number(k, 1, -1)), % Sanity check
    ?assertEqual(1, topic:select_partition_number(null, 1, 0)),
    ?assertEqual(1, topic:select_partition_number(null, 1, 1)),
    ?assertEqual(1, topic:select_partition_number(null, 1, 2)),
    ?assertEqual(1, topic:select_partition_number(null, 2, 1)),
    ?assertEqual(1, topic:select_partition_number(null, 2, 2)).

it_must_have_the_default_partitioner_test() ->
    application:start(franz),
    T = create_topic("topic_tests"),
    DefaultPartitioner = topic:get_partitioner(T),
    ?assertEqual(fun topic:select_partition_number/3, DefaultPartitioner),
    application:stop(franz).

it_must_set_the_partitioner_test() ->
    application:start(franz),
    T = create_topic("topic_tests"),
    TestPartitioner = fun(_Key, _NumPartitions, _LastSelected) -> 99999999 end,
    ok = topic:set_partitioner(T, TestPartitioner),
    DefaultPartitioner = topic:get_partitioner(T),
    ?assertEqual(TestPartitioner, DefaultPartitioner),
    application:stop(franz).

it_must_validate_the_partioner_test() ->
    application:start(franz),
    T = create_topic("topic_tests"),
    TestPartitioner = fun() -> 99999999 end, % Invalid args!
    Reply = topic:set_partitioner(T, TestPartitioner),
    ?assertEqual({error, wrong_arity}, Reply),
    application:stop(franz).





it_must_handle_partition_failures_test() ->
    application:start(franz),
    file:delete("data/it_must_handle_partition_failures_test1"),
    T = create_topic("it_must_handle_partition_failures_test"),
    % Validate an initial write:
    topic:put(T, "TEST1"),
    {_, [R1]} = disk_log:chunk("it_must_handle_partition_failures_test1", start, 4),
    ?assertEqual({key, null, value, "TEST1"}, R1),
    % Kill the partition, so that it restarts:
    [P] = topic:get_partitions(T),
    exit(P, kill),
    timer:sleep(50), %wait for restart...
    % Validate a second write to ensure the topic can access the restarted process:
    topic:put(T, "TEST2"),
    {_, [R1, R2]} = disk_log:chunk("it_must_handle_partition_failures_test1", start, 4),
    ?assertEqual({key, null, value, "TEST2"}, R2),
    application:stop(franz).


% NOTE, Line 63: the sleep() call prevents us from losing a write. I think the topic needs to:
% 1) Monitor each partition
% 2) When a partion dies, remove it from the list
% 3) If a partion number does not exist when we try to write, pause briefly, then try the write again.
% If the retry fails, then reply with an error.


create_topic(Name) ->
    {ok, T} = franz:new_topic("it_must_handle_partition_failures_test"),
    timer:sleep(50), %wait for partitions to start and register themselves.
    T.