-module(emojipoo).

-behavior(gen_server).

-export([init/1, 
         handle_call/3, 
         handle_cast/2, 
         handle_info/2,
         terminate/2, 
         code_change/3]).

-export([start_link/1, start_link/2, start_link/3,
         batch/2, 
         close/1, 
         get/2, 
         delete/2, delete_async/2, 
         put/3, put/4,
         range/2, range/3,
         prefix/2, prefix/3,
         %fold/3, 
         %fold_range/4, 
         destroy/1]).

-include("emojipoo.hrl").

-record(state, {top       :: pid() | undefined,    % top of the pile, first in chain of layers
                log       :: #log{} | undefined,   % transaction log
                dir       :: string(),
                opt       :: term(),
                max_depth :: pos_integer()}).

%% 0 means never expire
-define(DEFAULT_EXPIRY_SECS, 0).

%% PUBLIC API

-type server() :: pid().
-type key_range() :: #key_range{}.
-type iter() :: fun(() -> maybe_improper_list({key(), value()} , iter()) | [] ).

-type config_option() :: {compress, none | gzip | snappy | lz4}
                       %| {page_size, pos_integer()}
                       | {read_buffer_size, pos_integer()}
                       | {write_buffer_size, pos_integer()}
                       %| {merge_strategy, fast | predictable }
                       | {sync_strategy, none | sync | {seconds, pos_integer()}}
                       | {expiry_secs, non_neg_integer()}
                       | {spawn_opt, list()}
                       | {default_depth, pos_integer()}
                       .

%% @doc
%% Create or open an emojipoo store as part of a supervision tree.
%% Argument `Dir' names a directory in which to keep the data files.
- spec start_link(Dir::string()) -> {ok, server()} | ignore | {error, term()}.
start_link(Dir) ->
    start_link(Dir, []).

%% @doc Create or open an emojipoo store as part of a supervision tree.
- spec start_link(Dir::string(), Opts::[config_option()]) -> {ok, server()} | ignore | {error, term()}.
start_link(Dir, Opts) ->
    SpawnOpt = emojipoo_util:get_opt(spawn_opt, Opts, []),
    gen_server:start_link(?MODULE, [Dir, Opts], [{spawn_opt,SpawnOpt}]).

%% @doc Create or open an emojipoo store as part of a supervision tree
%% with a registered name.
- spec start_link(Name::{local, Name::atom()} | {global, GlobalName::term()} | {via, ViaName::term()},
                 Dir::string(), Opts::[config_option()]) -> {ok, server()} | ignore | {error, term()}.
start_link(Name, Dir, Opts) ->
    SpawnOpt = emojipoo_util:get_opt(spawn_opt, Opts, []),
    gen_server:start_link(Name, ?MODULE, [Dir, Opts], [{spawn_opt,SpawnOpt}]).

%% @doc
%% Close an emojipoo data store.
- spec close(Ref::pid()) -> ok.
close(Ref) ->
    try
        gen_server:call(Ref, close, infinity)
    catch
        exit:{noproc,_} -> ok;
        exit:noproc -> ok;
        %% Handle the case where the monitor triggers
        exit:{normal, _} -> ok
    end.

-spec destroy(Ref::pid()) -> ok.
destroy(Ref) ->
    try
        gen_server:call(Ref, destroy, infinity)
    catch
        exit:{noproc,_} -> ok;
        exit:noproc -> ok;
        %% Handle the case where the monitor triggers
        exit:{normal, _} -> ok
    end.

get(Ref,Key) when is_binary(Key) ->
    gen_server:call(Ref, {get, Key}, infinity).

-spec delete(server(), binary()) ->
                    ok | {error, term()}.
delete(Ref,Key) when is_binary(Key) ->
    gen_server:call(Ref, {delete, Key}, infinity).

-spec delete_async(server(), binary()) ->
                    ok | {error, term()}.
delete_async(Ref, Key) when is_binary(Key) ->
    gen_server:cast(Ref, {delete, Key}).

-spec put(server(), binary(), binary()) ->
                 ok | {error, term()}.
put(Ref,Key,Value) when is_binary(Key), is_binary(Value) ->
    gen_server:call(Ref, {put, Key, Value, infinity}, infinity).

-spec put(server(), binary(), binary(), integer()) ->
                 ok | {error, term()}.
put(Ref,Key,Value,infinity) when is_binary(Key), is_binary(Value) ->
    gen_server:call(Ref, {put, Key, Value, infinity}, infinity);
put(Ref,Key,Value,Expiry) when is_binary(Key), is_binary(Value) ->
    gen_server:call(Ref, {put, Key, Value, Expiry}, infinity).

-type batch_spec() :: {put, binary(), binary()} | {delete, binary()}.
-spec batch(server(), [batch_spec()]) ->
                 ok | {error, term()}.
batch(Ref, BatchSpec) ->
    gen_server:call(Ref, {batch, BatchSpec}, infinity).

%% find range with given prefix, including the prefix
prefix(Server, <<>>) ->
   range(Server, #key_range{}, true);
prefix(Server, Prefix) ->
   Range = #key_range{from_key = Prefix,
                      to_key   = next_binary(Prefix)},
   range(Server, Range, true).

prefix(Server, Prefix, FilterMap) ->
   Range = #key_range{from_key = Prefix,
                      to_key   = next_binary(Prefix)},
   range(Server, Range, FilterMap).

-spec range(server(), key_range()) -> iter().
range(Server, #key_range{} = Range) ->
   range(Server, #key_range{} = Range, true).

range(Server, #key_range{} = Range, FilterMap) ->
   Ref = make_ref(),
   Target = #{ref => Ref, target => self()},
   C = fun() ->
             range_collector(Target)
       end,
   Collector = erlang:spawn_link(C),
   AllPids = gen_server:call(Server, {range, Collector, Range, FilterMap}, 60000),
   Collector ! {start, AllPids},
   make_iterator(Ref).

open_layers(Dir, Options) ->
   % delete any old hardlinked files
   {ok, HdLnkMatch} = re:compile("[ABC]F-[\\d]+-[\\d]+\.data"),
   DelF = fun(Name) ->
                case re:run(Name, HdLnkMatch) of
                   nomatch -> ok;
                  _ -> file:delete(Name)
                end
          end,
   {ok, Files0} = file:list_dir(Dir),
   _ = lists:foreach(DelF, Files0),
   {ok, Files} = file:list_dir(Dir),
   TopDepth0 = emojipoo_util:get_opt(default_depth, Options, ?DEFAULT_DEPTH),
   
   %% parse file names and find max depth if any
   Fold = fun(FileName, {LastMinDepth, LastMaxDepth}) ->
                case layer_from_filename(FileName) of
                   {ok, Depth} ->
                      {erlang:min(LastMinDepth, Depth),
                       erlang:max(LastMaxDepth, Depth)};
                   _ ->
                      {LastMinDepth, LastMaxDepth}
                end
          end,
   {MinDepth, MaxDepth} = lists:foldl(Fold, {TopDepth0, TopDepth0}, Files),
   
%%    %% remove old log data file
%%    LogFileName = ?LOGFILENAME(Dir),
%%    _ = file:delete(LogFileName),
   % delete temp file if it is there
   TempFileName = filename:join(Dir, "templog.data"),
   _ = file:delete(TempFileName),
   
   Start = 
      fun(Depth, NextDeeperPid) ->
            {ok, LayerPid} = 
               emojipoo_layer:start_link(Dir, Depth, NextDeeperPid, Options),
            LayerPid
      end,
   Depths = lists:seq(MaxDepth, MinDepth, -1),
   TopLayerPid = lists:foldl(Start, undefined, Depths),
   {ok, TopLayerPid, MinDepth, MaxDepth}.

layer_from_filename(FileName) ->
   case re:run(FileName, "^[^\\d]+-(\\d+)\\.data$", 
               [{capture,all_but_first,list}]) of
      {match,[StringVal]} ->
         {ok, list_to_integer(StringVal)};
      _ ->
         nomatch
   end.

init([Dir, Opts0]) ->
   %% ensure expory_secs option is set in config
   Opts =
      case emojipoo_util:get_opt(expiry_secs, Opts0) of
         undefined ->
            [{expiry_secs, ?DEFAULT_EXPIRY_SECS}|Opts0];
         N when is_integer(N), N >= 0 ->
            [{expiry_secs, N}|Opts0]
      end,
   emojipoo_util:ensure_expiry_option(Opts),
   {Top, Log, Max} =
      case filelib:is_dir(Dir) of
         true ->
            {ok, TopLevel, MinLevel, MaxLevel} = open_layers(Dir, Opts),
            {ok, Log0} = emojipoo_log:recover(Dir, TopLevel, MinLevel, MaxLevel, Opts),
            {TopLevel, Log0, MaxLevel};
         false ->
            filelib:ensure_dir(Dir ++ "/ignore"),
            MinLevel = emojipoo_util:get_opt(default_depth, Opts0, ?DEFAULT_DEPTH),
            {ok, TopLevel} = emojipoo_layer:start_link(Dir, MinLevel, undefined, Opts),
            io:format("~p~n", [{?LINE, TopLevel}]),
            MaxLevel = MinLevel,
            {ok, Nursery} = emojipoo_log:new(Dir, MinLevel, MaxLevel, Opts),
            {TopLevel, Nursery, MaxLevel}
      end,
   {ok, #state{top = Top, dir = Dir, log = Log, opt = Opts, max_depth = Max}}.


handle_info(Info,State) ->
   ?error("Unknown info ~p~n", [Info]),
   {stop, bad_msg, State}.


handle_cast({bottom_level, N}, #state{log = Log, top = Top} = State)
  when N > State#state.max_depth ->
   State2 = State#state{max_depth = N,
                        log = emojipoo_log:set_max_depth(Log, N)},
   _ = emojipoo_layer:set_max_depth(Top, N),
   {noreply, State2};
handle_cast({delete, Key}, State) when is_binary(Key) ->
   {ok, State2} = do_put(Key, ?TOMBSTONE, infinity, State),
   {noreply, State2};
handle_cast(Info,State) ->
   ?error("Unknown cast ~p~n", [Info]),
   {stop, bad_msg, State}.


handle_call({range, FoldWorkerPID, Range, FilterMap}, _From, 
            #state{top = TopLevel, log = Log} = State) ->
   Ref = make_ref(),
   ok = emojipoo_log:range(Log, FoldWorkerPID, Ref, Range, FilterMap),
   Result = emojipoo_layer:range(TopLevel, FoldWorkerPID, Range, [Ref], FilterMap),
   {reply, Result, State};
handle_call({put, Key, Value, Expiry}, _From, State) when is_binary(Key), 
                                                          is_binary(Value) ->
   {ok, State2} = do_put(Key, Value, Expiry, State),
   {reply, ok, State2};
handle_call({batch, BatchSpec}, _From, State) ->
   {ok, State2} = do_batch(BatchSpec, State),
   {reply, ok, State2};
handle_call({delete, Key}, _From, State) when is_binary(Key) ->
   {ok, State2} = do_put(Key, ?TOMBSTONE, infinity, State),
   {reply, ok, State2};
handle_call({get, Key}, From, #state{top = Top, 
                                     log = Log} = State) when is_binary(Key) ->
   case emojipoo_log:lookup(Key, Log) of
      ?TOMBSTONE ->
         {reply, not_found, State};
      Value when is_binary(Value) ->
         {reply, {ok, Value}, State};
      none ->
         Cb = fun(Reply) -> gen_server:reply(From, Reply) end,
         _ = emojipoo_layer:lookup(Top, Key, Cb),
         {noreply, State}
   end;
handle_call(close, _From, #state{log = undefined} = State) ->
   {stop, normal, State};
handle_call(close, _From, #state{log = Log, 
                                 top = Top, 
                                 dir = Dir, 
                                 max_depth = MaxDepth, 
                                 opt = Config} = State) ->
   try
      ok = emojipoo_log:finish(Log, Top),
      MinLevel = emojipoo_layer:depth(Top),
      {ok, Log2} = emojipoo_log:new(Dir, MinLevel, MaxDepth, Config),
      ok = emojipoo_layer:close(Top),
      {stop, normal, State#state{log = Log2}}
   catch
      E:R ->
         ?error("exception from close ~p:~p~n", [E,R]),
         {stop, normal, State}
   end;
handle_call(destroy, _From, #state{top = Top, log = Log} = State) ->
   TopLevelNumber = emojipoo_layer:depth(Top),
   ok = emojipoo_log:destroy(Log),
   ok = emojipoo_layer:destroy(Top),
   {stop, normal, ok, State#state{top = undefined, 
                                  log = undefined, 
                                  max_depth = TopLevelNumber}}.

%% premature delete -> cleanup
terminate(normal, _State) -> ok;
terminate(_Reason, _State) ->
   ?error("got terminate(~p, ~p)~n", [_Reason, _State]),
   ok.

code_change(_OldVsn, State, _Extra) ->
   {ok, State}.

-spec do_put(key(), value(), expiry(), #state{}) -> {ok, #state{}}.
do_put(Key, Value, Expiry, #state{log = Log, top = Top} = State) 
  when Log =/= undefined ->
   {ok, Log2} = emojipoo_log:add(Key, Value, Expiry, Log, Top),
   {ok, State#state{log = Log2}}.

do_batch([{put, Key, Value}], State) ->
   do_put(Key, Value, infinity, State);
do_batch([{delete, Key}], State) ->
   do_put(Key, ?TOMBSTONE, infinity, State);
do_batch([], State) ->
   {ok, State};
do_batch(BatchSpec, State=#state{log = Log, top = Top}) ->
   {ok, Log2} = emojipoo_log:batch(BatchSpec, Log, Top),
   {ok, State#state{log = Log2}}.


range_collector(Target) ->
   receive
      {start, AllPids} ->
         Tab = ets:new(?MODULE, [ordered_set]),
         RevPids = lists:reverse(AllPids),
         range_collector(Target, RevPids, RevPids, Tab)
   after
       100000 ->
       error(timeout)
   end.


range_collector(#{ref := Ref, 
                  target := SendTo}, _, [], Tab) ->
   ok = split_send(Tab, SendTo, Ref);

%% idea here is that the first pid is the oldest and largest
%% so empty it first, placing everything in an ets table 
%% the newer data overwrites the older 
range_collector(Target, [Pid|TPids] = Pids, AllPids, Tab) ->
   receive
      {level_result, Pid, KV} ->
         ets:insert(Tab, KV),
         range_collector(Target, Pids, AllPids, Tab);
      {level_results, Pid, KVs} ->
         ets:insert(Tab, KVs),
         range_collector(Target, Pids, AllPids, Tab);
      {level_limit, Pid, _LastKey} ->
         ok = drain_from_pid(Pid),
         NewAll = lists:delete(Pid, AllPids),
         range_collector(Target, TPids, NewAll, Tab);
      {level_done, Pid} when is_pid(Pid) ->
         ok = drain_from_pid(Pid),
         NewAll = lists:delete(Pid, AllPids),
         range_collector(Target, TPids, NewAll, Tab);
      {level_done, Pid} when is_reference(Pid) ->
         ok = drain_from_pid(Pid),
         NewAll = lists:delete(Pid, AllPids),
         range_collector(Target, TPids, NewAll, Tab)      
   after
       60000 ->
       error(timeout)
   end.

make_iterator(Ref) ->
   receive
      {Ref, eof} ->
         [];
      {Ref, Results} ->
         Results ++ fun() -> make_iterator(Ref) end
   end.

drain_from_pid(Pid) ->
   receive
      {level_results, Pid, _} -> drain_from_pid(Pid);
      {level_limit, Pid, _} -> ok;
      {level_done, Pid} -> ok
   after 0 ->
       ok
   end.


split_send(Tab, SendTo, Ref) ->
   Iter = ets:select(Tab, [{'$1',[],['$1']}], ?BTREE_ASYNC_CHUNK_SIZE),
   split_send_(Iter, SendTo, Ref),
   ets:delete(Tab),
   ok.

split_send_({KVs, Continuation}, SendTo, Ref) ->
   SendTo ! {Ref, KVs},
   split_send_(ets:select(Continuation), SendTo, Ref);
split_send_('$end_of_table', SendTo, Ref) ->
   SendTo ! {Ref, eof},
   ok.



%% for a prefix, return the value that all prefixed binaries are less than. 
next_binary(<<>>) -> undefined;
next_binary(Bin) ->
   Size = byte_size(Bin) - 1,
   case Bin of
      <<Pre:Size/binary, 255>> ->
         next_binary(Pre);
      <<Pre:Size/binary, N>> ->
         <<Pre:Size/binary, (N + 1)>>
   end.

      

