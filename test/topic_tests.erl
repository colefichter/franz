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