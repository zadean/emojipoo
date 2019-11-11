
-module(emojipoo_merge).

-include("emojipoo.hrl").

-export([start/6, 
         merge/6]).

-spec start(string(), string(), string(), integer(), boolean(), list()) -> pid().
start(A, B, X, Size, IsLastLevel, Options) ->
   Self = self(),
   Merge = 
      fun() ->
            Before = ?NOW,
            try
               {ok, OutCount} = ?MODULE:merge(A, B, X,
                                              Size,
                                              IsLastLevel,
                                              Options),
               gen_statem:cast(Self, {merge_done, OutCount, X})
            catch
               C:E:S ->
                  ?error("~p: merge failed ~p:~p ~p -> ~s~n",
                         [self(), C,E,S,X]),
                  erlang:raise(C,E,S)
            after
                Diff = timer:now_diff(?NOW, Before),
                ?log("Merge of ~p took: ~p~n", [Size, Diff div 1000])
            end
      end,
   erlang:spawn_link(Merge).

-spec merge(string(), string(), string(), integer(), boolean(), list()) -> {ok, integer()}.
merge(A, B, C, Size, IsLastLevel, Options) ->
   {ok, IXA} = emojipoo_reader:open(A, [sequential|Options]),
   {ok, IXB} = emojipoo_reader:open(B, [sequential|Options]),
   {ok, Out} = emojipoo_writer:init([C, [{size, Size} | Options]]),
   AKVs =
      case emojipoo_reader:first_node(IXA) of
         {kvlist, AKV} -> AKV;
         none -> []
      end,
   BKVs =
      case emojipoo_reader:first_node(IXB) of
         {kvlist, BKV} -> BKV;
         none -> []
      end,
   wait_loop(IXA, IXB, Out, IsLastLevel, AKVs, BKVs).

terminate(Out) ->
   {reply, {ok, Count}, Out1} = emojipoo_writer:handle_call(count, self(), Out),
   {stop, normal, ok, _} = emojipoo_writer:handle_call(close, self(), Out1),
   {ok, Count}.

wait_loop(IXA, IXB, Out, IsLastLevel, AKVs, BKVs) ->
   receive
      {merge, From} ->
         loop(IXA, IXB, Out, IsLastLevel, AKVs, BKVs, From)
   end.

loop(IXA, IXB, Out, IsLastLevel, [], BKVs, From) ->
   case emojipoo_reader:next_node(IXA) of
      {kvlist, AKVs} ->
         loop(IXA, IXB, Out, IsLastLevel, AKVs, BKVs, From);
      end_of_data ->
         emojipoo_reader:close(IXA),
         loop_one(IXB, Out, IsLastLevel, BKVs, From)
   end;
loop(IXA, IXB, Out, IsLastLevel, AKVs, [], From) ->
   case emojipoo_reader:next_node(IXB) of
      {kvlist, BKVs} ->
         loop(IXA, IXB, Out, IsLastLevel, AKVs, BKVs, From);
      end_of_data ->
         emojipoo_reader:close(IXB),
         loop_one(IXA, Out, IsLastLevel, AKVs, From)
   end;
loop(IXA, IXB, Out, IsLastLevel, [{Key1,Value1}|AT], [{Key2,_Value2}|_IX] = BKVs, From)
  when Key1 < Key2 ->
   % dirty stuff calling the handler...
   {noreply, Out3} = emojipoo_writer:handle_cast({add, Key1, Value1}, Out),
   loop(IXA, IXB, Out3, IsLastLevel, AT, BKVs, From);
loop(IXA, IXB, Out, IsLastLevel, [{Key1,_Value1}|_AT] = AKVs, [{Key2,Value2}|IX], From)
  when Key1 > Key2 ->
   {noreply, Out3} = emojipoo_writer:handle_cast({add, Key2, Value2}, Out),
   loop(IXA, IXB, Out3, IsLastLevel, AKVs, IX, From);
loop(IXA, IXB, Out, IsLastLevel, [{_Key1,_Value1}|AT], [{Key2,Value2}|IX], From) ->
   {noreply, Out3} = emojipoo_writer:handle_cast({add, Key2, Value2}, Out),
   loop(IXA, IXB, Out3, IsLastLevel, AT, IX, From).

loop_one(IX, Out, IsLastLevel, [], From) ->
   case emojipoo_reader:next_node(IX) of
      {kvlist, KVs} ->
         loop_one(IX, Out, IsLastLevel, KVs, From);
      end_of_data ->
%%          case From of
%%             none -> ok;
%%             {Pid, Ref} ->
%%                Pid ! {Ref, merge_done}
%%          end,
         emojipoo_reader:close(IX),
         terminate(Out)
   end;
loop_one(IX, Out, true, [{_,?TOMBSTONE}|Rest], From) ->
    loop_one(IX, Out, true, Rest, From);
loop_one(IX, Out, IsLastLevel, [{Key,Value}|Rest], From) ->
   {noreply, Out3} = emojipoo_writer:handle_cast({add, Key, Value}, Out),
   loop_one(IX, Out3, IsLastLevel, Rest, From).
