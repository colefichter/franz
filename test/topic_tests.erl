-module(topic_tests).
-compile([export_all]).

% Unit tests
%-------------------------------------------------------------------------
-include_lib("eunit/include/eunit.hrl").

setup() -> application:start(franz).
cleanup(_) -> application:stop(franz).

generator_test_() ->
    {foreach, fun setup/0, fun cleanup/1,
        [
        fun it_must_start_isolated_topics/0,
        fun it_must_not_start_multiple_topics/0,
        fun it_must_count_the_topics/0,
        fun it_must_start_topics_with_one_partition/0,
        fun it_must_start_topics_with_many_partitions/0
        ]
    }.

it_must_start_isolated_topics() ->
    {ok, T1} = topic:new_topic("Sports"),
    {ok, T2} = topic:new_topic("Hobbies"),
    ?assertNotEqual(T1, T2).

it_must_not_start_multiple_topics() ->
    {ok, T1} = topic:new_topic("Sports"),
    {ok, T1} = topic:new_topic("Sports").

it_must_count_the_topics() ->
    ?assertEqual(0, topic:count()),
    {ok, _} = topic:new_topic("Sports"),
    ?assertEqual(1, topic:count()),
    {ok, _} = topic:new_topic("Hobbies"),
    ?assertEqual(2, topic:count()).

it_must_start_topics_with_one_partition() ->
    {ok, T1} = topic:new_topic("Sports"),
    ?assertEqual(1, topic:partition_count("Sports")),
    ?assertEqual(1, topic:partition_count(T1)).

it_must_start_topics_with_many_partitions() ->
    {ok, T1} = topic:new_topic("Sports", 6),
    ?assertEqual(6, topic:partition_count("Sports")),
    ?assertEqual(6, topic:partition_count(T1)).

it_must_handle_unknown_topic_partition_count() ->
    ?assertEqual({error, unknown_topic}, topic:partition_count("NOT A TOPIC")).