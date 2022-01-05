-module(emojipoo_util).

%% ====================================================================
%% API functions
%% ====================================================================
-export([
    compress/2,
    uncompress/1,
    estimate_node_size_increment/2,
    encode_index_node/2,
    decode_index_node/2,
    crc_encapsulate_kv_entry/2,
    decode_crc_data/3,
    file_exists/1,
    crc_encapsulate_batch/2,
    tstamp/0,
    expiry_time/1,
    has_expired/1,
    ensure_expiry_option/1,

    get_opt/2,
    get_opt/3,
    is_expired/1,
    get_value/1,
    umerge/1
]).

-include("emojipoo.hrl").

-define(BISECT_ENCODED, 126).
-define(CRC_ENCODED, 127).
-define(ERLANG_ENCODED, 131).

-define(NO_COMPRESSION, 0).
%% remove? add back later?
-define(SNAPPY_COMPRESSION, 1).
-define(GZIP_COMPRESSION, 2).
%% remove? add back later?
-define(LZ4_COMPRESSION, 3).

%% ====================================================================
%% Internal functions
%% ====================================================================

-compile({inline, [crc_encapsulate/1, crc_encapsulate_kv_entry/2]}).

-spec file_exists(string()) -> boolean().
file_exists(FileName) ->
    case file:read_file_info(FileName) of
        {ok, _} ->
            true;
        {error, enoent} ->
            false
    end.

estimate_node_size_increment(Key, {Value, _TStamp}) when
    is_integer(Value)
->
    byte_size(Key) + 5 + 4;
estimate_node_size_increment(Key, {Value, _TStamp}) when
    is_binary(Value)
->
    byte_size(Key) + 5 + 4 + byte_size(Value);
estimate_node_size_increment(Key, {Value, _TStamp}) when
    is_atom(Value)
->
    byte_size(Key) + 8 + 4;
estimate_node_size_increment(Key, {Value, _TStamp}) when
    is_tuple(Value)
->
    byte_size(Key) + 13 + 4;
estimate_node_size_increment(Key, Value) when
    is_integer(Value)
->
    byte_size(Key) + 5 + 4;
estimate_node_size_increment(Key, Value) when
    is_binary(Value)
->
    byte_size(Key) + 5 + 4 + byte_size(Value);
estimate_node_size_increment(Key, Value) when
    is_atom(Value)
->
    byte_size(Key) + 8 + 4;
estimate_node_size_increment(Key, Value) when
    is_tuple(Value)
->
    byte_size(Key) + 13 + 4.

compress(none, Bin) ->
    {?NO_COMPRESSION, Bin};
compress(snappy, Bin) ->
    {ok, CompressedBin} = snappy:compress(Bin),
    case bigger(erlang:iolist_size(Bin), erlang:iolist_size(CompressedBin)) of
        true -> {?SNAPPY_COMPRESSION, CompressedBin};
        false -> {?NO_COMPRESSION, Bin}
    end;
compress(lz4, Bin) ->
    {ok, CompressedBin} = lz4:compress(erlang:iolist_to_binary(Bin)),
    case bigger(erlang:iolist_size(Bin), erlang:iolist_size(CompressedBin)) of
        true -> {?LZ4_COMPRESSION, CompressedBin};
        false -> {?NO_COMPRESSION, Bin}
    end;
compress(gzip, Bin) ->
    CompressedBin = zlib:gzip(Bin),
    case bigger(erlang:iolist_size(Bin), erlang:iolist_size(CompressedBin)) of
        true -> {?GZIP_COMPRESSION, CompressedBin};
        false -> {?NO_COMPRESSION, Bin}
    end.

uncompress(<<?NO_COMPRESSION, Data/binary>>) ->
    Data;
uncompress(<<?SNAPPY_COMPRESSION, Data/binary>>) ->
    {ok, UncompressedData} = snappy:decompress(Data),
    UncompressedData;
uncompress(<<?LZ4_COMPRESSION, Data/binary>>) ->
    lz4:uncompress(Data);
uncompress(<<?GZIP_COMPRESSION, Data/binary>>) ->
    zlib:gunzip(Data).

bigger(UncompressedSize, CompressedSize) ->
    CompressedSize < UncompressedSize.

encode_index_node(KVList, Method) ->
    Binary = emojipoo_vbisect:from_orddict(lists:map(fun binary_encode_kv/1, KVList)),
    CRC = erlang:crc32(Binary),
    TermData = [?BISECT_ENCODED, <<CRC:32>>, Binary],
    {MethodFlag, OutData} = compress(Method, TermData),
    {ok, [MethodFlag | OutData]}.

decode_index_node(Level, Data) ->
    TermData = uncompress(Data),
    case decode_kv_list(TermData) of
        % erlang term as binary, unused...
        {ok, KVList} ->
            {ok, {node, Level, KVList}};
        {bisect, Binary} ->
            {ok, {node, Level, Binary}};
        {partial, KVList, _BrokenData} ->
            {ok, {node, Level, KVList}}
    end.

binary_encode_kv({Key, {Value, infinity}}) ->
    binary_encode_kv({Key, Value});
binary_encode_kv({Key, {?TOMBSTONE, TStamp}}) ->
    {Key, <<?TAG_DELETED2, TStamp:32>>};
binary_encode_kv({Key, ?TOMBSTONE}) ->
    {Key, <<?TAG_DELETED>>};
binary_encode_kv({Key, {Value, TStamp}}) when is_binary(Value) ->
    {Key, <<?TAG_KV_DATA2, TStamp:32, Value/binary>>};
binary_encode_kv({Key, Value}) when is_binary(Value) ->
    {Key, <<?TAG_KV_DATA, Value/binary>>};
binary_encode_kv({Key, {Pos, Len}}) when Len < 16#ffffffff ->
    {Key, <<?TAG_POSLEN32, Pos:64/unsigned, Len:32/unsigned>>}.

-spec crc_encapsulate_kv_entry(binary(), expvalue()) -> iolist().
crc_encapsulate_kv_entry(Key, {Value, infinity}) ->
    crc_encapsulate_kv_entry(Key, Value);
%
crc_encapsulate_kv_entry(Key, {?TOMBSTONE, TStamp}) ->
    crc_encapsulate([?TAG_DELETED2, <<TStamp:32>> | Key]);
crc_encapsulate_kv_entry(Key, ?TOMBSTONE) ->
    crc_encapsulate([?TAG_DELETED | Key]);
crc_encapsulate_kv_entry(Key, {Value, TStamp}) when is_binary(Value) ->
    crc_encapsulate([
        ?TAG_KV_DATA2,
        <<TStamp:32, (byte_size(Key)):32/unsigned>>,
        Key,
        Value
    ]);
crc_encapsulate_kv_entry(Key, Value) when is_binary(Value) ->
    crc_encapsulate([
        ?TAG_KV_DATA,
        <<(byte_size(Key)):32/unsigned>>,
        Key,
        Value
    ]);
crc_encapsulate_kv_entry(Key, {Pos, Len}) when Len < 16#ffffffff ->
    crc_encapsulate([
        ?TAG_POSLEN32,
        <<Pos:64/unsigned, Len:32/unsigned>>,
        Key
    ]).

-spec crc_encapsulate_batch([batchspec()], expiry()) -> iolist().
crc_encapsulate_batch(BatchSpec, Expiry) ->
    Enc = fun
        ({delete, Key}) ->
            crc_encapsulate_kv_entry(Key, {?TOMBSTONE, Expiry});
        ({put, Key, Value}) ->
            crc_encapsulate_kv_entry(Key, {Value, Expiry})
    end,
    crc_encapsulate([?TAG_BATCH | lists:map(Enc, BatchSpec)]).

-spec crc_encapsulate(iolist()) -> iolist().
crc_encapsulate(Blob) ->
    CRC = erlang:crc32(Blob),
    Size = erlang:iolist_size(Blob),
    [<<(Size):32/unsigned, CRC:32/unsigned>>, Blob, ?TAG_END].

-spec decode_kv_list(binary()) ->
    {ok, [kventry()]}
    | {partial, [kventry()], iolist()}
    | {bisect, binary()}.
decode_kv_list(<<?TAG_END, Custom/binary>>) ->
    decode_crc_data(Custom, [], []);
decode_kv_list(<<?ERLANG_ENCODED, _/binary>> = TermData) ->
    {ok, erlang:binary_to_term(TermData)};
decode_kv_list(<<?CRC_ENCODED, Custom/binary>>) ->
    decode_crc_data(Custom, [], []);
decode_kv_list(<<?BISECT_ENCODED, CRC:32/unsigned, Binary/binary>>) ->
    CRCTest = erlang:crc32(Binary),
    if
        CRC == CRCTest ->
            {bisect, Binary};
        true ->
            % corrupt so empty
            {bisect, emojipoo_vbisect:from_orddict([])}
    end.

-spec decode_crc_data(binary(), list(), list()) ->
    {ok, [kventry()]} | {partial, [kventry()], iolist()}.
decode_crc_data(<<>>, [], Acc) ->
    {ok, lists:reverse(Acc)};
decode_crc_data(<<>>, BrokenData, Acc) ->
    % TODO: find out when this happens
    {partial, lists:reverse(Acc), BrokenData};
decode_crc_data(
    <<BinSize:32/unsigned, CRC:32/unsigned, Bin:BinSize/binary, ?TAG_END, Rest/binary>>, Broken, Acc
) ->
    CRCTest = erlang:crc32(Bin),
    if
        CRC == CRCTest ->
            decode_crc_data(Rest, Broken, [decode_kv_data(Bin) | Acc]);
        true ->
            % TODO: chunk is broken, ignore it. Maybe we should tell someone?
            decode_crc_data(Rest, [Bin | Broken], Acc)
    end;
decode_crc_data(Bad, Broken, Acc) ->
    %% If a chunk is broken, try to find the next ?TAG_END and
    %% start decoding from there.
    {Skipped, MaybeGood} = skip_to_next_value(Bad),
    decode_crc_data(MaybeGood, [Skipped | Broken], Acc).

-spec skip_to_next_value(binary()) -> {binary(), binary()}.
skip_to_next_value(<<>>) ->
    {<<>>, <<>>};
skip_to_next_value(Bin) ->
    case binary:match(Bin, <<?TAG_END>>) of
        {Pos, _Len} ->
            <<SkipBin:Pos/binary, ?TAG_END, MaybeGood/binary>> = Bin,
            {SkipBin, MaybeGood};
        nomatch ->
            {Bin, <<>>}
    end.

-spec decode_kv_data(binary()) -> kventry().
decode_kv_data(<<?TAG_KV_DATA, KLen:32/unsigned, Key:KLen/binary, Value/binary>>) ->
    {Key, Value};
decode_kv_data(<<?TAG_DELETED, Key/binary>>) ->
    {Key, ?TOMBSTONE};
decode_kv_data(
    <<?TAG_KV_DATA2, TStamp:32/unsigned, KLen:32/unsigned, Key:KLen/binary, Value/binary>>
) ->
    {Key, {Value, TStamp}};
decode_kv_data(<<?TAG_DELETED2, TStamp:32/unsigned, Key/binary>>) ->
    {Key, {?TOMBSTONE, TStamp}};
decode_kv_data(<<?TAG_POSLEN32, Pos:64/unsigned, Len:32/unsigned, Key/binary>>) ->
    {Key, {Pos, Len}};
decode_kv_data(<<?TAG_BATCH, Rest/binary>>) ->
    {ok, TX} = decode_crc_data(Rest, [], []),
    TX.

%% @doc Return number of seconds since 1970
-spec tstamp() -> pos_integer().
tstamp() ->
    {Mega, Sec, _Micro} = ?NOW,
    (Mega * 1000000) + Sec.

%% @doc Return time when values expire (i.e. Now + ExpirySecs), or 0.
-spec expiry_time(pos_integer()) -> pos_integer().
expiry_time(ExpirySecs) when ExpirySecs > 0 ->
    tstamp() + ExpirySecs.

-spec has_expired(pos_integer()) -> boolean().
has_expired(Expiration) when Expiration > 0 ->
    Expiration < tstamp();
has_expired(infinity) ->
    false.

ensure_expiry_option(Opts) ->
    case get_opt(expiry_secs, Opts) of
        undefined ->
            exit(expiry_secs_not_set);
        N when N >= 0 ->
            ok
    end.

get_opt(Key, Opts) ->
    get_opt(Key, Opts, undefined).

get_opt(Key, Opts, Default) ->
    case proplists:get_value(Key, Opts) of
        undefined ->
            case application:get_env(?APP, Key) of
                {ok, Value} -> Value;
                undefined -> Default
            end;
        Value ->
            Value
    end.

is_expired(?TOMBSTONE) -> false;
is_expired({_Value, TStamp}) -> has_expired(TStamp);
is_expired(Bin) when is_binary(Bin) -> false.

get_value({Value, _TStamp}) -> Value;
get_value(Value) -> Value.

umerge(ListOfLists) ->
    Tab = ets:new(?MODULE, [ordered_set]),
    Fun = fun(L) ->
        ets:insert(Tab, L)
    end,
    ok = lists:foreach(Fun, lists:reverse(ListOfLists)),
    Ret = [KV || {_, V} = KV <- ets:tab2list(Tab), V =/= ?TOMBSTONE],
    ets:delete(Tab),
    Ret.
