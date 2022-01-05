-module(emojipoo_vbisect).

%% gb_tree as a blob

-export([
    from_orddict/1,
    from_gb_tree/1,
    to_gb_tree/1,
    first_key/1,
    find/2,
    find_geq/2,
    foldl/3,
    foldr/3,
    fold_until_stop/3,
    to_orddict/1,
    merge/3
]).

-define(MAGIC, "vbis").
-type key() :: binary().
-type value() :: binary().
-type bindict() :: binary().
-type gb_tree_node(K, V) ::
    'nil'
    | {K, V, gb_tree_node(K, V), gb_tree_node(K, V)}.
-type tree(Key, Value) :: {non_neg_integer(), gb_tree_node(Key, Value)}.

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-spec from_gb_tree(tree(key(), value())) -> bindict().
from_gb_tree({Count, Node}) when Count =< 16#ffffffff ->
    {_BinSize, IOList} = encode_gb_node(Node),
    erlang:iolist_to_binary([<<?MAGIC, Count:32/unsigned>> | IOList]);
from_gb_tree(_) ->
    throw({error, tree_too_large}).

-spec encode_gb_node(gb_tree_node(key(), value())) ->
    {non_neg_integer(), iolist()}.
encode_gb_node({Key, Value, Smaller, Bigger}) when
    is_binary(Key),
    is_binary(Value)
->
    {BinSizeSmaller, IOSmaller} = encode_gb_node(Smaller),
    {BinSizeBigger, IOBigger} = encode_gb_node(Bigger),

    KeySize = byte_size(Key),
    ValueSize = byte_size(Value),
    BinSize = 2 + KeySize + 4 + ValueSize + 4 + BinSizeSmaller + BinSizeBigger,
    IOList = [
        <<KeySize:16, Key/binary, BinSizeSmaller:32>>,
        IOSmaller,
        <<ValueSize:32, Value/binary>>
        | IOBigger
    ],
    {BinSize, IOList};
encode_gb_node(nil) ->
    {0, []}.

-spec to_gb_tree(binary()) -> tree(key(), value()).
to_gb_tree(<<?MAGIC, Count:32, Nodes/binary>>) ->
    {Count, to_gb_node(Nodes)}.

to_gb_node(<<>>) ->
    nil;
to_gb_node(
    <<KeySize:16, Key:KeySize/binary, BinSizeSmaller:32, Smaller:BinSizeSmaller/binary,
        ValueSize:32, Value:ValueSize/binary, Bigger/binary>>
) ->
    {Key, Value, to_gb_node(Smaller), to_gb_node(Bigger)}.

-spec find(Key :: key(), Dict :: bindict()) ->
    {ok, value()} | error.
find(Key, <<?MAGIC, _:32, Binary/binary>>) ->
    find_node(byte_size(Key), Key, Binary).

find_node(
    KeySize,
    Key,
    <<HereKeySize:16, HereKey:HereKeySize/binary, BinSizeSmaller:32, _:BinSizeSmaller/binary,
        ValueSize:32, Value:ValueSize/binary, _/binary>> = Bin
) ->
    if
        Key < HereKey ->
            Skip = 6 + HereKeySize,
            <<_:Skip/binary, Smaller:BinSizeSmaller/binary, _/binary>> = Bin,
            find_node(KeySize, Key, Smaller);
        HereKey < Key ->
            Skip = 10 + HereKeySize + BinSizeSmaller + ValueSize,
            <<_:Skip/binary, Bigger/binary>> = Bin,
            find_node(KeySize, Key, Bigger);
        true ->
            {ok, Value}
    end;
find_node(_, _, <<>>) ->
    error.

to_orddict(BinDict) ->
    Fold = fun(Key, Value, Acc) ->
        [{Key, Value} | Acc]
    end,
    foldr(Fold, [], BinDict).

-dialyzer([{nowarn_function, [merge/3]}, no_return]).
merge(Fun, BinDict1, BinDict2) ->
    OD1 = to_orddict(BinDict1),
    OD2 = to_orddict(BinDict2),
    OD3 = orddict:merge(Fun, OD1, OD2),
    from_orddict(OD3).

-spec first_key(bindict()) -> binary() | none.
first_key(BinDict) ->
    Stop = fun({K, _}, _) -> {stop, K} end,
    {_, Key} = fold_until_stop(Stop, none, BinDict),
    Key.

%% @doc Find largest {K,V} where K is smaller than or equal to key.
%% This is good for an inner node where key is the smallest key
%% in the child node.
-spec find_geq(Key :: binary(), Binary :: binary()) ->
    none | {ok, Key :: key(), Value :: value()}.
find_geq(Key, <<?MAGIC, _:32, Binary/binary>>) ->
    find_geq_node(byte_size(Key), Key, Binary, none).

find_geq_node(_, _, <<>>, Else) ->
    Else;
find_geq_node(
    KeySize,
    Key,
    <<HereKeySize:16, HereKey:HereKeySize/binary, BinSizeSmaller:32, _:BinSizeSmaller/binary,
        ValueSize:32, Value:ValueSize/binary, _/binary>> = Bin,
    Else
) ->
    if
        Key < HereKey ->
            Skip = 6 + HereKeySize,
            <<_:Skip/binary, Smaller:BinSizeSmaller/binary, _/binary>> = Bin,
            find_geq_node(KeySize, Key, Smaller, Else);
        HereKey < Key ->
            Skip = 10 + HereKeySize + BinSizeSmaller + ValueSize,
            <<_:Skip/binary, Bigger/binary>> = Bin,
            find_geq_node(KeySize, Key, Bigger, {ok, HereKey, Value});
        true ->
            {ok, HereKey, Value}
    end.

-spec foldl(fun((Key :: key(), Value :: value(), Acc :: term()) -> term()), term(), bindict()) ->
    term().
foldl(Fun, Acc, <<?MAGIC, _:32, Binary/binary>>) ->
    foldl_node(Fun, Acc, Binary).

foldl_node(_Fun, Acc, <<>>) ->
    Acc;
foldl_node(
    Fun,
    Acc,
    <<KeySize:16, Key:KeySize/binary, BinSizeSmaller:32, Smaller:BinSizeSmaller/binary,
        ValueSize:32, Value:ValueSize/binary, Bigger/binary>>
) ->
    Acc1 = foldl_node(Fun, Acc, Smaller),
    Acc2 = Fun(Key, Value, Acc1),
    foldl_node(Fun, Acc2, Bigger).

-spec fold_until_stop(function(), term(), bindict()) -> {stopped, term()} | {ok, term()}.
fold_until_stop(Fun, Acc, <<?MAGIC, _:32, Bin/binary>>) ->
    fold_until_stop2(Fun, {continue, Acc}, Bin).

fold_until_stop2(_Fun, {stop, Result}, _) ->
    {stopped, Result};
fold_until_stop2(_Fun, {continue, Acc}, <<>>) ->
    {ok, Acc};
fold_until_stop2(
    Fun,
    {continue, Acc},
    <<KeySize:16, Key:KeySize/binary, BinSizeSmaller:32, Smaller:BinSizeSmaller/binary,
        ValueSize:32, Value:ValueSize/binary, Bigger/binary>>
) ->
    case fold_until_stop2(Fun, {continue, Acc}, Smaller) of
        {stopped, _} = Result ->
            Result;
        {ok, Acc1} ->
            ContinueOrStopAcc = Fun({Key, Value}, Acc1),
            fold_until_stop2(Fun, ContinueOrStopAcc, Bigger)
    end.

-spec foldr(fun((Key :: key(), Value :: value(), Acc :: term()) -> term()), term(), bindict()) ->
    term().
foldr(Fun, Acc, <<?MAGIC, _:32, Binary/binary>>) ->
    foldr_node(Fun, Acc, Binary).

foldr_node(_Fun, Acc, <<>>) ->
    Acc;
foldr_node(
    Fun,
    Acc,
    <<KeySize:16, Key:KeySize/binary, BinSizeSmaller:32, Smaller:BinSizeSmaller/binary,
        ValueSize:32, Value:ValueSize/binary, Bigger/binary>>
) ->
    Acc1 = foldr_node(Fun, Acc, Bigger),
    Acc2 = Fun(Key, Value, Acc1),
    foldr_node(Fun, Acc2, Smaller).

-dialyzer([{nowarn_function, [from_orddict/1]}, no_opaque]).
from_orddict(OrdDict) ->
    from_gb_tree(gb_trees:from_orddict(OrdDict)).

-ifdef(TEST).

speed_test_() ->
    {timeout, 600, fun() ->
        Start = 100000000000000,
        N = 100000,
        Keys = lists:seq(Start, Start + N),
        KeyValuePairs = lists:map(
            fun(I) -> {<<I:64/integer>>, <<255:8/integer>>} end,
            Keys
        ),

        %% Will mostly be unique, if N is bigger than 10000
        ReadKeys = [<<(lists:nth(rand:uniform(N), Keys)):64/integer>> || _ <- lists:seq(1, 1000)],
        B = from_orddict(KeyValuePairs),
        time_reads(B, N, ReadKeys)
    end}.

geq_test() ->
    B = from_orddict([{<<2>>, <<2>>}, {<<4>>, <<4>>}, {<<6>>, <<6>>}, {<<122>>, <<122>>}]),
    none = find_geq(<<1>>, B),
    {ok, <<2>>, <<2>>} = find_geq(<<2>>, B),
    {ok, <<2>>, <<2>>} = find_geq(<<3>>, B),
    {ok, <<4>>, <<4>>} = find_geq(<<5>>, B),
    {ok, <<6>>, <<6>>} = find_geq(<<100>>, B),
    {ok, <<122>>, <<122>>} = find_geq(<<150>>, B),
    true.

time_reads(B, Size, ReadKeys) ->
    Parent = self(),
    spawn(
        fun() ->
            Runs = 20,
            Timings =
                lists:map(
                    fun(_) ->
                        StartTime = now(),
                        find_many(B, ReadKeys),
                        timer:now_diff(now(), StartTime)
                    end,
                    lists:seq(1, Runs)
                ),

            Rps = 1000000 / ((lists:sum(Timings) / length(Timings)) / 1000),
            error_logger:info_msg(
                "Average over ~p runs, ~p keys in dict~n"
                "Average fetch ~p keys: ~p us, max: ~p us~n"
                "Average fetch 1 key: ~p us~n"
                "Theoretical sequential RPS: ~w~n",
                [
                    Runs,
                    Size,
                    length(ReadKeys),
                    lists:sum(Timings) / length(Timings),
                    lists:max(Timings),
                    (lists:sum(Timings) / length(Timings)) / length(ReadKeys),
                    trunc(Rps)
                ]
            ),

            Parent ! done
        end
    ),
    receive
        done -> ok
    after 1000 -> ok
    end.

-spec find_many(bindict(), [key()]) -> non_neg_integer().
find_many(B, Keys) ->
    lists:foldl(
        fun(K, N) ->
            case find(K, B) of
                {ok, _} -> N + 1;
                error -> N
            end
        end,
        0,
        Keys
    ).

-endif.
