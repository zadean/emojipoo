
-module(emojipoo_reader).

-include("emojipoo.hrl").

-include_lib("kernel/include/file.hrl").

%% API
-export([open/1, open/2,
         close/1,
         lookup/2,
         fold/3,
         range_fold/4, 
         destroy/1
        ]).

%% used by the merger, needs list value
-export([first_node/1,
         next_node/1]).

%% Internal
-export([serialize_state/1, 
         deserialize_state/1]).

-record(node, {level        :: non_neg_integer(),
               members = [] :: list(any()) | binary() }).

-record(reader, 
        {file        :: undefined | file:io_device(),
         root = none :: #node{} | none,
         name        :: string(),
         config = [] :: term()}).

-type read_file() :: #reader{}.
-export_type([read_file/0]).

-spec open(Name::string()) -> {ok, read_file()} | {error, any()}.
open(Name) ->
    open(Name, [random]).

-type config() :: [sequential | folding | random | {atom(), term()}].

-spec open(Name::string(), config()) -> {ok, read_file()}  | {error, any()}.
open(Name, Config) ->
   case proplists:get_bool(sequential, Config) of
      true ->
         ReadBufferSize = emojipoo_util:get_opt(read_buffer_size, 
                                                Config, 512 * 1024),
         case file:open(Name, [raw,read,{read_ahead, ReadBufferSize},binary]) of
            {ok, File} ->
               {ok, #reader{file = File, name = Name, config = Config}};
            {error, _} = Err ->
               Err
         end;
      false ->
         {ok, File} =
           case proplists:get_bool(folding, Config) of
              true ->
                 ReadBufferSize = emojipoo_util:get_opt(read_buffer_size, 
                                                        Config, 512 * 1024),
                 file:open(Name, [read, {read_ahead, ReadBufferSize}, binary]);
              false ->
                 file:open(Name, [read, binary])
           end,
         {ok, FileInfo} = file:read_file_info(Name),
         
         %% read and validate magic tag
         {ok, ?FILE_FORMAT} = file:pread(File, 0, byte_size(?FILE_FORMAT)),
         
         %% read root position
         {ok, <<RootPos:64/unsigned>>} = file:pread(File, FileInfo#file_info.size - 8, 8),
         
         %% read in the root node
         Root = case read_node(File, RootPos) of
                   {ok, Node} -> Node;
                   eof -> none
                end,
         {ok, #reader{file = File, 
                      root = Root, 
                      name = Name, 
                      config = Config}}
   end.

destroy(#reader{file = File, name = Name}) ->
   ok = file:close(File),
   file:delete(Name).

serialize_state(#reader{file = File} = Index) ->
   {ok, Position} = file:position(File, cur),
   ok = file:close(File),
   {seq_read_file, Index, Position}.

deserialize_state({seq_read_file, #reader{name = Name,
                                          config = Config}, Position}) ->
   {ok, #reader{file = File} = Index2} = open(Name, Config),
   {ok, Position} = file:position(File, {bof, Position}),
   Index2.



fold(Fun, Acc0, #reader{file = File}) ->
   {ok, Node} = read_node(File, ?FIRST_BLOCK_POS),
   fold0(File, fun({K, V}, Acc) -> Fun(K, V, Acc) end, Node, Acc0).

%% fold0(File, Fun, #node{level = 0, members = BinPage}, Acc0) 
%%   when is_binary(BinPage) ->
%%    F = fun(K, V, Acc2) -> Fun({K, decode_binary_value(V)}, Acc2) end,
%%    Acc1 = emojipoo_vbisect:foldl(F, Acc0, BinPage),
%%    fold1(File, Fun, Acc1);
fold0(File, Fun, #node{level = 0, members = List}, Acc0) when is_list(List) ->
   Acc1 = lists:foldl(Fun, Acc0, List),
   fold1(File, Fun, Acc1);
fold0(File, Fun, _InnerNode, Acc0) ->
   fold1(File, Fun, Acc0).

fold1(File, Fun, Acc0) ->
   case next_leaf_node(File) of
      eof -> Acc0;
      {ok, Node} ->
         fold0(File, Fun, Node, Acc0)
   end.

-spec range_fold(fun((binary(),binary(),any()) -> any()), any(), read_file(), keyrange()) ->
                        {limit, any(), binary()} | {done, any()}.
range_fold(Fun, Acc0, #reader{file = File, root = Root}, Range) ->
   case Range#key_range.from_key =< first_key(Root) of
      true ->
         {ok, _} = file:position(File, ?FIRST_BLOCK_POS),
         range_fold_from_here(Fun, Acc0, File, Range, Range#key_range.limit);
      false ->
         Find = find_leaf_node(File, Range#key_range.from_key, 
                               Root, ?FIRST_BLOCK_POS),
         case Find of
            {ok, {Pos,_}} ->
               {ok, _} = file:position(File, Pos),
               range_fold_from_here(Fun, Acc0, File, Range, Range#key_range.limit);
            {ok, Pos} ->
               {ok, _} = file:position(File, Pos),
               range_fold_from_here(Fun, Acc0, File, Range, Range#key_range.limit);
            none ->
               {done, Acc0}
         end
   end.

first_key(none) -> none;
first_key(#node{members = Dict}) ->
   {_, FirstKey} = fold_until_stop(fun({K,_},_) -> {stop, K} end, none, Dict),
   FirstKey.

fold_until_stop(Fun, Acc, List) when is_list(List) ->
   fold_until_stop2(Fun, {continue, Acc}, List);
fold_until_stop(Fun, Acc0, Bin) when is_binary(Bin) ->
   F = fun({Key,VBin},Acc1) ->
             Fun({Key, decode_binary_value(VBin)}, Acc1)
       end,
   emojipoo_vbisect:fold_until_stop(F, Acc0, Bin).

fold_until_stop2(_Fun, {stop, Result}, _) ->
   {stopped, Result};
fold_until_stop2(_Fun, {continue, Acc}, []) ->
   {ok, Acc};
fold_until_stop2(Fun, {continue, Acc}, [H|T]) ->
   fold_until_stop2(Fun, Fun(H, Acc), T).


range_fold_from_here_stop_fun(Fun, Range) ->
   fun({Key,_}, Acc) when not ?KEY_IN_TO_RANGE(Key,Range) ->
         {stop, {done, Acc}};
      ({Key,Value}, Acc) when ?KEY_IN_FROM_RANGE(Key, Range) ->
         case emojipoo_util:is_expired(Value) of
            true -> {continue, Acc};
            false ->
               {continue, Fun(Key, emojipoo_util:get_value(Value), Acc)}
         end;
      (_Huh, Acc) ->
         {continue, Acc}
   end.

range_fold_from_here_stop_fun2(Fun, Range) ->
   fun({Key,_}, {0,Acc}) ->
         {stop, {limit, Acc, Key}};
      ({Key,_}, {_,Acc}) when not ?KEY_IN_TO_RANGE(Key,Range)->
         {stop, {done, Acc}};
      ({Key,?TOMBSTONE}, {N1,Acc}) when ?KEY_IN_FROM_RANGE(Key,Range) ->
         {continue, {N1, Fun(Key, ?TOMBSTONE, Acc)}};
      ({Key,{?TOMBSTONE,TStamp}}, {N1,Acc}) when ?KEY_IN_FROM_RANGE(Key,Range) ->
         case emojipoo_util:has_expired(TStamp) of
            true -> {continue, {N1,Acc}};
            false ->
               {continue, {N1, Fun(Key, ?TOMBSTONE, Acc)}}
         end;
      ({Key,Value}, {N1,Acc}) when ?KEY_IN_FROM_RANGE(Key,Range) ->
         case emojipoo_util:is_expired(Value) of
            true -> {continue, {N1,Acc}};
            false ->
               {continue, {N1-1, Fun(Key, emojipoo_util:get_value(Value), Acc)}}
         end;
      (_, Acc) -> {continue, Acc}
   end.

range_fold_from_here(Fun, Acc0, File, Range, undefined) ->
   case next_leaf_node(File) of
      eof -> {done, Acc0};
      {ok, #node{members = Members}} ->
         Fx = range_fold_from_here_stop_fun(Fun, Range),
         case fold_until_stop(Fx, Acc0, Members) of
            {stopped, Result} -> Result;
            {ok, Acc1} ->
               range_fold_from_here(Fun, Acc1, File, Range, undefined)
         end
   end;
range_fold_from_here(Fun, Acc0, File, Range, N0) ->
   case next_leaf_node(File) of
      eof -> {done, Acc0};
      {ok, #node{members = Members}} ->
         Fx = range_fold_from_here_stop_fun2(Fun, Range),
         case fold_until_stop(Fx, {N0, Acc0}, Members) of
            {stopped, Result} -> Result;
            {ok, {N2, Acc1}} ->
               range_fold_from_here(Fun, Acc1, File, Range, N2)
         end
   end.

find_leaf_node(_File, _FromKey, #node{level = 0}, Pos) ->
   {ok, Pos};
find_leaf_node(File, FromKey, #node{members = Members, 
                                    level = N}, _) when is_list(Members) ->
   case find_start(FromKey, Members) of
      {ok, ChildPos} ->
         recursive_find(File, FromKey, N, ChildPos);
      not_found ->
         none
   end;
find_leaf_node(File, FromKey, #node{members = Members,
                                    level = N}, _) when is_binary(Members) ->
   case emojipoo_vbisect:find_geq(FromKey,Members) of
      {ok, _, <<?TAG_POSLEN32, Pos:64/unsigned, Len:32/unsigned>>} ->
         recursive_find(File, FromKey, N, {Pos,Len});
      none ->
         none
   end;
find_leaf_node(_, _, none, _) -> none.

recursive_find(_File, _FromKey, 1, ChildPos) -> {ok, ChildPos};
recursive_find(File, FromKey, N, ChildPos) when N > 1 ->
   case read_node(File, ChildPos) of
      {ok, ChildNode} ->
         find_leaf_node(File, FromKey, ChildNode, ChildPos);
      eof ->
         none
   end.

%% used by the merger, needs list value
first_node(#reader{file = File}) ->
   case read_node(File, ?FIRST_BLOCK_POS) of
      {ok, #node{level = 0, members = Members}} ->
         {kvlist, decode_member_list(Members)};
      eof->
         none
   end.

%% used by the merger, needs list value
next_node(#reader{file = File}) ->
   case next_leaf_node(File) of
      {ok, #node{level = 0, members = Members}} ->
         {kvlist, decode_member_list(Members)};
      eof ->
         end_of_data
   end.

decode_member_list(List) when is_list(List) -> List;
decode_member_list(BinDict) when is_binary(BinDict) ->
   F = fun(Key, Value, Acc) ->
             [{Key, decode_binary_value(Value) }|Acc]
       end,
   emojipoo_vbisect:foldr(F, [], BinDict).

close(#reader{file = undefined}) -> ok;
close(#reader{file = File}) ->
   file:close(File).


lookup(#reader{file = File, root = Node}, Key) ->
   case lookup_in_node(File, Node, Key) of
      not_found -> not_found;
      {ok, {Value, TStamp}} when Value =:= ?TOMBSTONE; 
                                 is_binary(Value) ->
         case emojipoo_util:has_expired(TStamp) of
            true -> not_found;
            false -> {ok, Value}
         end;
      {ok, Value} = Reply when Value =:= ?TOMBSTONE; 
                               is_binary(Value) ->
         Reply
   end.

lookup_in_node(_File, #node{level = 0, members = Members}, Key) ->
   find_in_leaf(Key, Members);
lookup_in_node(File, #node{members = Members}, Key) when is_binary(Members) ->
   case emojipoo_vbisect:find_geq(Key, Members) of
      {ok, _Key, <<?TAG_POSLEN32, Pos:64, Size:32>>} ->
         case read_node(File, {Pos, Size}) of
            {ok, Node} ->
               lookup_in_node(File, Node, Key);
            eof ->
               not_found
         end;
      none ->
         not_found
   end;

lookup_in_node(File, #node{members = Members}, Key) ->
   case find_1(Key, Members) of
      {ok, {Pos, Size}} ->
         %% do this in separate process, to avoid having to
         %% garbage collect all the inner node junk
         Parent = self(),
         Fun = fun() ->
                     case read_node(File, {Pos,Size}) of
                        {ok, Node} ->
                           Result = lookup_in_node2(File, Node, Key),
                           Parent ! {self(), Result};
                        eof ->
                           Parent ! {self(), {error, eof}}
                     end
               end,
         try
            Pid = erlang:spawn_link(Fun),
            receive
               {Pid, Result} ->
                  Result
            %% XXX maybe 'after' clause here
            end
         catch
            Class:Ex:Stack ->
               ?error("crashX: ~p:~p ~p~n", [Class,Ex,Stack]),
               not_found
         end;
      not_found ->
         not_found
   end.

lookup_in_node2(_File, #node{level = 0, members = Members}, Key) ->
   case lists:keyfind(Key, 1, Members) of
      false -> not_found;
      {_,Value} -> {ok, Value}
   end;
lookup_in_node2(File, #node{members = Members}, Key) ->
   case find_1(Key, Members) of
      {ok, {Pos, Size}} ->
         case read_node(File, {Pos, Size}) of
            {ok, Node} ->
               lookup_in_node2(File, Node, Key);
            eof ->
               {error, eof}
         end;
      not_found ->
         not_found
   end.

find_1(K, [{K1, V}, {K2, _}|_]) when K >= K1, K < K2 ->
   {ok, V};
find_1(K, [{K1, V}]) when K >= K1 ->
   {ok, V};
find_1(K, [_|T]) ->
   find_1(K, T);
find_1(_, _) ->
   not_found.

find_start(K, [{_,V},{K2,_}|_]) when K < K2 ->
   {ok, V};
find_start(_, [{_,{_,_} = V}]) ->
   {ok, V};
find_start(K, KVs) ->
   find_1(K, KVs).


-spec read_node(file:io_device(), non_neg_integer() | { non_neg_integer(), non_neg_integer() }) ->
                       {ok, #node{}} | eof.
read_node(File, {Pos, Size}) ->
   {ok, <<_:32/unsigned, Level:16/unsigned, Data/binary>>} = 
      file:pread(File, Pos, Size),
   emojipoo_util:decode_index_node(Level, Data);
read_node(File, Pos) ->
   {ok, Pos} = file:position(File, Pos),
   read_node(File).

read_node(File) ->
   {ok, <<Len:32/unsigned, Level:16/unsigned>>} = file:read(File, 6),
   case Len of
      0 -> eof;
      _ ->
         {ok, Data} = file:read(File, Len-2),
         emojipoo_util:decode_index_node(Level, Data)
   end.

next_leaf_node(File) ->
   case file:read(File, 6) of
      %% premature end-of-file
      eof -> eof;
      {ok, <<0:32/unsigned, _:16/unsigned>>} ->
         eof;
      {ok, <<Len:32/unsigned, 0:16/unsigned>>} ->
         {ok, Data} = file:read(File, Len - 2),
         emojipoo_util:decode_index_node(0, Data);
      {ok, <<Len:32/unsigned, _:16/unsigned>>} ->
         {ok, _} = file:position(File, {cur, Len - 2}),
         next_leaf_node(File)
   end.

find_in_leaf(Key, Bin) when is_binary(Bin) ->
   case emojipoo_vbisect:find(Key, Bin) of
      {ok, BinValue} ->
         {ok, decode_binary_value(BinValue)};
      error ->
         not_found
   end;
find_in_leaf(Key, List) when is_list(List) ->
   case lists:keyfind(Key, 1, List) of
      {_, Value} ->
         {ok, Value};
      false ->
         not_found
   end.

decode_binary_value(<<?TAG_KV_DATA, Value/binary>>) -> 
   Value;
decode_binary_value(<<?TAG_KV_DATA2, TStamp:32, Value/binary>>) ->
   {Value, TStamp};
decode_binary_value(<<?TAG_DELETED>>) -> 
   ?TOMBSTONE;
decode_binary_value(<<?TAG_DELETED2, TStamp:32>>) ->
   {?TOMBSTONE, TStamp};
decode_binary_value(<<?TAG_POSLEN32, Pos:64, Len:32>>) ->
   {Pos, Len}.
