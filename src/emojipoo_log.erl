
-module(emojipoo_log).

-include("emojipoo.hrl").

-include_lib("kernel/include/file.hrl").

-export([new/4,
         add/4, add/5,
         batch/3,
         range/4,
         lookup/2, 
         recover/5, 
         finish/2
        ]).

-export([set_max_depth/2, 
         destroy/1]).

%% do incremental merge every this many inserts
%% this value *must* be less than or equal to
%% 2^TOP_LEVEL == ?BTREE_SIZE(?TOP_LEVEL)
-define(INC_MERGE_STEP, ?BTREE_SIZE(MinLevel) div 2).

-spec new(string(), integer(), integer(), [_]) -> {ok, #log{}} | {error, term()}.
new(Directory, MinLevel, MaxLevel, Opts) ->
   emojipoo_util:ensure_expiry_option(Opts),
   
   {ok, File} = file:open(?LOGFILENAME(Directory),
                          [raw, exclusive, write, delayed_write, append]),
   {ok, #log{log_file = File, 
             dir = Directory, 
             cache = gb_trees:empty(),
             min_depth = MinLevel, 
             max_depth = MaxLevel,
             config = Opts}}.

recover(Directory, TopLevel, MinLevel, MaxLevel, Config)
   when MinLevel =< MaxLevel, 
        is_integer(MinLevel), 
        is_integer(MaxLevel) ->
   emojipoo_util:ensure_expiry_option(Config),
   case file:read_file_info(?LOGFILENAME(Directory)) of
      {ok, _} ->
         ok = do_recover(Directory, TopLevel, MinLevel, MaxLevel, Config),
         new(Directory, MinLevel, MaxLevel, Config);
      {error, enoent} ->
         new(Directory, MinLevel, MaxLevel, Config)
   end.

do_recover(Directory, TopLevel, MinLevel, MaxLevel, Config) ->
   %% repair the log file; storing it in log2
   LogFileName = ?LOGFILENAME(Directory),
   {ok, Log} = read_from_log(Directory, MinLevel, MaxLevel, Config),
   ok = finish(Log, TopLevel),
   %% assert log file is gone
   {error, enoent} = file:read_file_info(LogFileName),
   ok.

fill_cache({Key, Value}, Cache) when is_binary(Value); 
                                     Value =:= ?TOMBSTONE ->
   gb_trees:enter(Key, Value, Cache);
fill_cache({Key, {Value, _TStamp} = Entry}, Cache) when is_binary(Value); 
                                                        Value =:= ?TOMBSTONE ->
   gb_trees:enter(Key, Entry, Cache);
fill_cache([], Cache) ->
   Cache;
fill_cache(Transactions, Cache) when is_list(Transactions) ->
   lists:foldl(fun fill_cache/2, Cache, Transactions).

read_from_log(Directory, MinLevel, MaxLevel, Config) ->
   {ok, LogBinary} = file:read_file(?LOGFILENAME(Directory)),
   Cache = cache_from_binary(LogBinary, Directory),
   Log = #log{dir = Directory, 
              cache = Cache, 
              count = gb_trees:size(Cache), 
              min_depth = MinLevel, 
              max_depth = MaxLevel, 
              config = Config},
   {ok, Log}.

cache_from_binary(LogBinary, Directory) ->
   case emojipoo_util:decode_crc_data(LogBinary, [], []) of
      {ok, KVs} ->
         fill_cache(KVs, gb_trees:empty());
      {partial, KVs, _ErrorData} ->
         ?error("ignoring undecypherable bytes in ~p~n", 
                [?LOGFILENAME(Directory)]),
         fill_cache(KVs, gb_trees:empty())
   end.
   
%% @doc Add a Key/Value to the log
-spec do_add(#log{}, binary(), binary()|?TOMBSTONE, non_neg_integer() | infinity, pid()) -> {ok, #log{}} | {full, #log{}}.
do_add(Log, Key, Value, infinity, Top) ->
    do_add(Log, Key, Value, 0, Top);
do_add(#log{log_file = File, 
            cache = Cache, 
            total_size = TotalSize, 
            count = Count, 
            config = Config} = Log, Key, Value, KeyExpiryTime, _Top) ->
   DatabaseExpiryTime = emojipoo_util:get_opt(expiry_secs, Config),
   {Data, Cache2} =
     if (KeyExpiryTime + DatabaseExpiryTime) == 0 ->
           %% Both the database expiry and this key's expiry are unset or set to 0
           %% (aka infinity) so never automatically expire the value.
           {emojipoo_util:crc_encapsulate_kv_entry(Key, Value),
            gb_trees:enter(Key, Value, Cache)};
        true ->
           Expiry = get_expiration(DatabaseExpiryTime, KeyExpiryTime),
           {emojipoo_util:crc_encapsulate_kv_entry(Key, {Value, Expiry}),
            gb_trees:enter(Key, {Value, Expiry}, Cache)}
     end,
   
   ok = file:write(File, Data),
   Log1 = do_sync(File, Log),
   Log2 = Log1#log{cache = Cache2,
                   total_size = TotalSize + erlang:iolist_size(Data),
                   count = Count + 1},
   case has_room(Log2, 1) of
      true -> {ok, Log2};
      false -> {full, Log2}
   end.

get_expiration(DatabaseExpiryTime, KeyExpiryTime) ->
   if DatabaseExpiryTime == 0 ->
         %% It was the database's setting that was 0 so expire this
         %% value after KeyExpiryTime seconds elapse.
         emojipoo_util:expiry_time(KeyExpiryTime);
      true ->
         if KeyExpiryTime == 0 ->
               emojipoo_util:expiry_time(DatabaseExpiryTime);
            true ->
               emojipoo_util:expiry_time(min(KeyExpiryTime, DatabaseExpiryTime))
         end
   end.
   
do_sync(File, Log) ->
   % XXX persistent_term storage
   LastSync =
     case application:get_env(?APP, sync_strategy) of
        {ok, sync} ->
           file:datasync(File),
           ?NOW;
        {ok, {seconds, N}} ->
           MicrosSinceLastSync = timer:now_diff(?NOW, Log#log.last_sync),
           if (MicrosSinceLastSync div 1000000) >= N ->
                 file:datasync(File),
                 ?NOW;
              true ->
                 Log#log.last_sync
           end;
        _ ->
           Log#log.last_sync
     end,
   Log#log{last_sync = LastSync}.


lookup(Key, #log{cache = Cache}) ->
   case gb_trees:lookup(Key, Cache) of
      {value, {Value, TStamp}} ->
         case emojipoo_util:has_expired(TStamp) of
            true -> {value, ?TOMBSTONE};
            false -> {value, Value}
         end;
      Reply -> Reply
   end.

%% @doc
%% Finish this log (encode it to a btree, and delete the log file)
%% @end
-spec finish(Log::#log{}, TopLevel::pid()) -> ok.
finish(#log{dir = Dir, 
            cache = Cache, 
            log_file = LogFile, 
            count = Count, 
            config = Config, 
            min_depth = MinLevel}, TopLevel) ->
   
   emojipoo_util:ensure_expiry_option(Config),
   
   %% First, close the log file (if it is open)
   case LogFile of
      undefined -> ok;
      _ -> ok = file:close(LogFile)
   end,
   
   case Count of
      N when N > 0 ->
         %?log("Count ~p~n", [Count]),
         %% next, flush cache to a new BTree
         BTreeFileName = filename:join(Dir, "templog.data"),
         {ok, BT} = emojipoo_writer:open(BTreeFileName,
                                        %% XXX size 
                                        [{size, ?BTREE_SIZE(MinLevel)},
                                         {compress, none} | Config]),
         try
            F = fun(Key, Value, Acc) ->
                      ok = emojipoo_writer:add(BT, Key, Value),
                      Acc
                end,
            gb_tree_fold(F, 0, Cache)            
         catch
            A:B:C ->
               io:format("DID NOT WORK ~p~n", [{A,B,C}]),
               throw(A)
         after
             ok = emojipoo_writer:close(BT)
         end,
         
         %% Inject the B-Tree (blocking RPC)
         {ok, _X} = emojipoo_layer:inject(TopLevel, BTreeFileName),
         ok;
      _ ->
         ok
   end,
   %% then, delete the log file
   LogFileName = ?LOGFILENAME(Dir),
   file:delete(LogFileName),
   ok.

destroy(#log{dir = Dir, 
             log_file = LogFile}) ->
   %% first, close the log file
   if LogFile /= undefined ->
         ok = file:close(LogFile);
      true ->
         ok
   end,
   %% then delete it
   LogFileName = ?LOGFILENAME(Dir),
   file:delete(LogFileName),
   ok.

-spec add(key(), value(), #log{}, pid()) -> {ok, #log{}}.
add(Key, Value, Log, Top) ->
   add(Key, Value, infinity, Log, Top).

-spec add(key(), value(), expiry(), #log{}, pid()) -> {ok, #log{}}.
add(Key, Value, Expiry, Log, Top) ->
   case do_add(Log, Key, Value, Expiry, Top) of
      {ok, Log0} ->
         {ok, Log0};
      {full, Log0} ->
         flush(Log0, Top)
   end.

-spec range(#log{}, pid(), pid() | reference(), #key_range{}) -> ok.
range(#log{cache = Cache}, SendTo, Ref, Range) ->
   do_range_iter(Cache, SendTo, Ref, Range).

-spec flush(#log{}, pid()) -> {ok, #log{}}.
flush(#log{dir = Dir, 
           min_depth = MinLevel, 
           max_depth = MaxLevel, 
           config = Config} = Log, Top) ->
   ok = finish(Log, Top),
   {error, enoent} = file:read_file_info(?LOGFILENAME(Dir)),
   new(Dir, MinLevel,  MaxLevel, Config).

has_room(#log{count = Count, 
              min_depth = MinLevel}, N) ->
   (Count + N + 1) < ?BTREE_SIZE(MinLevel).

ensure_space(Log, NeededRoom, Top) ->
   case has_room(Log, NeededRoom) of
      true -> Log;
      false ->
         {ok, Log1} = flush(Log, Top),
         Log1
   end.

batch(Spec, Log, Top) ->
   batch1(Spec, ensure_space(Log, length(Spec), Top), Top).

batch1(Spec, #log{log_file = File, 
                  cache = Cache0, 
                  total_size = TotalSize, 
                  config = Config} = Log1, Top) ->
   Expiry =
     case emojipoo_util:get_opt(expiry_secs, Config) of
        0 -> infinity;
        DatabaseExpiryTime ->
           emojipoo_util:expiry_time(DatabaseExpiryTime)
     end,
   
   Data = emojipoo_util:crc_encapsulate_batch(Spec, Expiry),
   ok = file:write(File, Data),
   Log2 = do_sync(File, Log1),
   BatchFun = fun({put, Key, Value}, Cache) ->
                    case Expiry of
                       infinity ->
                          gb_trees:enter(Key, Value, Cache);
                       _ ->
                          gb_trees:enter(Key, {Value, Expiry}, Cache)
                    end;
                 ({delete, Key}, Cache) ->
                    case Expiry of
                       infinity ->
                          gb_trees:enter(Key, ?TOMBSTONE, Cache);
                       _ ->
                          gb_trees:enter(Key, {?TOMBSTONE, Expiry}, Cache)
                    end
              end,   
   Cache2 = lists:foldl(BatchFun, Cache0, Spec),

   Count = gb_trees:size(Cache2),
   %?log("Count ~p~n", [Count]),
   Log3 = Log2#log{cache = Cache2,
                   total_size = TotalSize + erlang:iolist_size(Data),
                   count = Count},
   flush(Log3, Top).

set_max_depth(Log = #log{}, MaxLevel) ->
   Log#log{max_depth = MaxLevel}.


gb_tree_fold(Fun, Acc, Tree) when is_function(Fun, 3) ->
   Iter = gb_trees:iterator(Tree),
   gb_tree_fold_1(Fun, Acc, gb_trees:next(Iter)).

gb_tree_fold_1(Fun, Acc0, {Key, Value, Iter}) ->
   Acc = Fun(Key, Value, Acc0),
   gb_tree_fold_1(Fun, Acc, gb_trees:next(Iter));
gb_tree_fold_1(_, Acc, none) ->
   Acc.


-spec do_range_iter(Tree      :: gb_trees:tree(),
                    SendTo    :: pid(),
                    SelfOrRef :: pid() | reference(),
                    Range     :: tuple() ) -> ok.
do_range_iter(Tree, SendTo, SelfOrRef, #key_range{from_key = FromKey,
                                                  from_inclusive = IncFrom,
                                                  to_key = ToKey,
                                                  to_inclusive = IncTo}) ->
   try 
      InitIter = gb_trees:iterator_from(FromKey, Tree),
      
      Fun = fun(Key, _, Acc) when Key >= ToKey,
                                  ToKey =/= undefined,
                                  not IncTo ->
                  Acc;
               (Key, _, Acc) when Key > ToKey,
                                  ToKey =/= undefined,
                                  IncTo ->
                  Acc;
               (Key, _, Acc) when Key == FromKey,
                                  not IncFrom ->
                  Acc;
               %% is in range
               (Key, Value, {0, KVs}) ->
                  send(SendTo, SelfOrRef, [{Key, Value}|KVs]),
                  {?BTREE_ASYNC_CHUNK_SIZE - 1, []};
               (Key, Value, {N, KVs}) ->
                  {N - 1,[{Key, Value}|KVs]}
            end,
      gb_tree_fold_1(Fun, {?BTREE_ASYNC_CHUNK_SIZE - 1, []}, gb_trees:next(InitIter)) 
   of
      {_, KVs} ->
         send(SendTo, SelfOrRef, KVs),
         SendTo ! {level_done, SelfOrRef}
   catch
      exit:worker_died -> ok
   end,
   ok.

send(_,_,[]) ->
    [];
send(SendTo,Ref,ReverseKVs) ->
   SendTo ! {level_results, Ref, lists:reverse(ReverseKVs)}.

