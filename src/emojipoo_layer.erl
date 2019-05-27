
-module(emojipoo_layer).

-behaviour(gen_statem).

-include("emojipoo.hrl").

%% gen_statem
-export([callback_mode/0,
         init/1,
         handle_event/4,
         terminate/3,
         code_change/4]).

%% API
-export([start_link/4,
         depth/1, 
         lookup/2, lookup/3, 
         inject/2, 
         close/1,
         destroy/1,
         set_max_depth/2,
         range/5
        ]).

-record(state, 
        {a, b, c,        % index records for each version
         next,           % next deeper state machine pid in the pile
         dir,            % directory where the files go
         depth,          % this layer`s depth from top of pile 
         inject_done_ref,% ref to wait for after injecting to next level 
         merge_pid,      % merging process id
         merge_ref,      % merge_pid monitor
         folding = [],
         opts = [], 
         max_depth = ?DEFAULT_DEPTH, % deepest known layer
         parent  % pid of process that started the layer 
        }).

%% API Functions
start_link(Directory, Depth, NextDeeperPid, Opts) when Depth > 0 ->
   ok = emojipoo_util:ensure_expiry_option(Opts),
   State = #state{dir = Directory, 
                  depth = Depth, 
                  next = NextDeeperPid, 
                  opts = Opts,
                  parent = self()},
   gen_statem:start_link(?MODULE, [State], []) .

%% Return the depth of this process.
depth(LayerPid) ->
   gen_statem:call(LayerPid, depth).

%% Lookup a single key from this layer
%% returns {ok, Result} | not_found
lookup(LayerPid, Key) ->
   gen_statem:call(LayerPid, {lookup, Key}).

%% Lookup a single key from this layer and apply ActionFun to it.
%% Single arg to fun is {ok, Result} | not_found
%% function always returns ok and is async
lookup(LayerPid, Key, ActionFun) ->
   gen_statem:cast(LayerPid, {lookup, Key, ActionFun}).

%% Let this statem know that there is a new file to merge into itself.
%% this call will block until any running merges are finished.
%% returns a monitor reference to merg process
inject(LayerPid, FileName) ->
   gen_statem:call(LayerPid, {inject, self(), FileName}).

%% close this layer
close(LayerPid) ->
   try
      gen_statem:call(LayerPid, close)
   catch
      exit:{noproc,_} -> ok;
      exit:noproc -> ok;
      exit:{normal, _} -> ok
   end.

%% destroy this layer. deletes files!
destroy(LayerPid) ->
   try
      gen_statem:call(LayerPid, destroy)
   catch
      exit:{noproc,_} -> ok;
      exit:noproc -> ok;
      exit:{normal, _} -> ok
   end.

%% sets the max_depth in the state. This is the deepest layer as integer.
set_max_depth(LayerPid, Depth) ->
   gen_statem:cast(LayerPid, {set_max_depth, Depth}).

range(LayerPid, SendTo, Range, List, FilterMap) ->
   {ok, Folders} = gen_statem:call(LayerPid, {init_range_fold, SendTo, Range, List, FilterMap}),
   Folders.

spawn_merge(State) ->
   AFileName = filename("A",State),
   BFileName = filename("B",State),
   XFileName = filename("X",State),

   ?log("starting merge~n", []),

   file:delete(XFileName),

   emojipoo_merge:start(AFileName, BFileName, XFileName,
                        ?BTREE_SIZE(State#state.depth + 1),
                        State#state.next =:= undefined,
                        State#state.opts).

%% Recover from post-merge crash. This is the case where
%% a merge completed, resulting in a file that needs to
%% stay at the *same* level because the resulting size
%% is smaller than or equal to this level's files.
recover_merge(MFileName, State) ->
   AFileName = filename("A",State),
   BFileName = filename("B",State),
   CFileName = filename("C",State),
   file:delete(AFileName),
   file:delete(BFileName),
   ok = file:rename(MFileName, AFileName),
   
   {ok, IXA} = emojipoo_reader:open(AFileName, [random|State#state.opts]),
   
   case exists_file(CFileName) of
      true ->
         file:rename(CFileName, BFileName),
         {ok, IXB} = emojipoo_reader:open(BFileName, [random|State#state.opts]),
         maybe_merge(State#state{a = IXA, b = IXB});
      false ->
         State#state{a = IXA, 
                     b = undefined}
   end.

trigger_merge(MergePid, From) when is_pid(MergePid) ->
   MergePid ! {merge, From}.

maybe_merge(#state{a = IXA, 
                   b = IXB,
                   merge_pid = undefined} = State) when IXA /= undefined,
                                                        IXB /= undefined ->
   MergePid = spawn_merge(State),
   MergeRef = monitor(process, MergePid),
   trigger_merge(MergePid, {self(), make_ref()}),
   State#state{merge_pid = MergePid,
               merge_ref = MergeRef};
maybe_merge(State) ->
   maybe_add_merge(State).

maybe_add_merge(#state{a = IXA, 
                       b = IXB} = State) when IXA /= undefined,
                                              IXB /= undefined ->
   MergePid = spawn_merge(State),
   trigger_merge(MergePid, {self(), make_ref()}),
   {merging, State#state{merge_pid = MergePid}};
maybe_add_merge(State) -> 
   {ready, State}.


callback_mode() -> handle_event_function.

init([State]) ->
   MFileName = filename("M",State),

   %% remove old merge file
   file:delete(filename("X",State)),

   %% remove old fold files (hard links to A/B/C used during fold)
   file:delete(filename("AF",State)),
   file:delete(filename("BF",State)),
   file:delete(filename("CF",State)),

   State1 = 
     case exists_file(MFileName) of
        true ->
           recover_merge(MFileName, State);
        false ->
           AFileName = filename("A",State),
           BFileName = filename("B",State),
           CFileName = filename("C",State),
           ROpts = [random|State#state.opts],
           case exists_file(BFileName) of
              true ->
                 {ok, IXA} = emojipoo_reader:open(AFileName, ROpts),
                 {ok, IXB} = emojipoo_reader:open(BFileName, ROpts),
                 {ok, IXC} = case exists_file(CFileName) of
                                true ->
                                   emojipoo_reader:open(CFileName, ROpts);
                                false ->
                                   {ok, undefined}
                             end,
                 maybe_merge(State#state{a = IXA, b = IXB, c = IXC});
              false ->
                 %% assert that there is no C file
                 false = exists_file(CFileName),
                 case exists_file(AFileName) of
                    true ->
                       {ok, IXA} = emojipoo_reader:open(AFileName, ROpts),
                       State#state{a = IXA};
                    false ->
                       State
                 end
           end
     end,
   {ok, ready, State1}.



%%% {call, From} | cast | info = EventType
%handle_event(EventType, EventContent, StateName, StateData) -> {next_state, StateName, StateData}.

terminate(Reason, _StateName, _StateData) when Reason == normal;
                                               Reason == shutdown ->
   %% XXX close all here
   ok;
terminate(_, _, _) -> ok.

code_change(_OldVsn, StateName, StateData, _Extra) ->
   {ok, StateName, StateData}.


%% ====================================================================
%% Internal functions
%% ====================================================================

%%% States are `ready` | `merging` | `wait_inject`

%%% {call, From} | cast | info = EventType
%handle_event(EventType, EventContent, StateName, State) -> {next_state, StateName, State}.

handle_event(cast, {set_max_depth, NewDepth}, _, #state{next = Next} = State) ->
   if Next =/= undefined ->
         ?MODULE:set_max_depth(Next, NewDepth);
      true -> ok
   end,
   {keep_state, State#state{max_depth = NewDepth}};
%% sets max_depth as with the server
handle_event(cast, {bottom_level, NewDepth}, _, State) ->
   {keep_state, State#state{max_depth = NewDepth}};
%% simply returns the depth
handle_event({call, From}, depth, _, State) ->
   {keep_state_and_data, {reply, From, State#state.depth}};

%% empty lookup
handle_event({call, From}, {lookup, _}, ready, #state{a = undefined,
                                                  b = undefined,
                                                  c = undefined,
                                                  next = undefined}) ->
   {keep_state_and_data, {reply, From, not_found}};
handle_event({call, From}, {lookup, Key}, ready, #state{next = Next} = State) ->
   % newer -> older lookup, take first
   case do_lookup(Key, [State#state.c, State#state.b, State#state.a]) of
      {found, ?TOMBSTONE} ->
         {keep_state_and_data, {reply, From, not_found}};
      not_found when Next == undefined ->
         {keep_state_and_data, {reply, From, not_found}};
      not_found ->
         Reply = gen_statem:call(Next, {lookup, Key}),
         {keep_state_and_data, {reply, From, Reply}};
      {found, Result} ->
         {keep_state_and_data, {reply, From, {ok, Result}}}
   end;
handle_event(cast, {lookup, Key, ActionFun}, ready, #state{next = Next} = State) ->
   % newer -> older lookup, take first, this passes down chain
   case do_lookup(Key, [State#state.c, State#state.b, State#state.a]) of
      {found, ?TOMBSTONE} ->
         ActionFun(not_found);
      not_found when Next == undefined ->
         ActionFun(not_found);
      not_found ->
         gen_statem:cast(Next, {lookup, Key, ActionFun});
      {found, Result} ->
         ActionFun({ok, Result})
   end,
   {keep_state_and_data, []};

%% Merging puts A nd B and puts it in X
% merging resulted in a file with less than level #entries, so we keep it here
handle_event(cast, {merge_done, 0, MergeFileName}, StateName, State) 
   when StateName == merging;
        StateName == ready -> % happens on startup 
   ok = file:delete(MergeFileName),
   {ok, State2} = close_and_delete_a_and_b(State),
   case State#state.c of
      undefined ->
         {next_state, ready, State2#state{merge_pid = undefined}};
      CFile ->
         ok = emojipoo_reader:close(CFile),
         ok = file:rename(filename("C", State2), filename("A", State2)),
         {ok, AFile} = emojipoo_reader:open(filename("A", State2), [random|State#state.opts]),
         State3 = State2#state{a = AFile, 
                               c = undefined, 
                               merge_pid = undefined},
         {next_state, ready, State3}
   end;
%% Merge fits here
handle_event(cast, {merge_done, Count, OutFileName}, StateName, State)
   when Count =< ?BTREE_SIZE(State#state.depth), StateName == merging;
        Count =< ?BTREE_SIZE(State#state.depth), StateName == ready -> 
   
   ROpts = [random|State#state.opts],
   ?log("merge_done, out: ~w~n", [Count]),

   % first, rename the tmp file to M, so recovery will pick it up
   MFileName = filename("M",State),
   ok = file:rename(OutFileName, MFileName),

   % then delete A and B (if we crash now, C will become the A file)
   {ok, State2} = close_and_delete_a_and_b(State),

   % then, rename M to A, and open it
   AFileName = filename("A",State2),
   ok = file:rename(MFileName, AFileName),
   {ok, AFile} = emojipoo_reader:open(AFileName, ROpts),

   % iff there is a C file, then move it to B position
   case State#state.c of
      undefined ->
         State3 = State2#state{a = AFile, 
                               b = undefined, 
                               merge_pid = undefined},
         {next_state, ready, State3};
      CFile ->
         ok = emojipoo_reader:close(CFile),
         ok = file:rename(filename("C", State2), filename("B", State2)),
         {ok, BFile} = emojipoo_reader:open(filename("B", State2), ROpts),
         State3 = State2#state{a = AFile, 
                               b = BFile, 
                               c = undefined,
                               merge_pid = undefined},
         {NextState, State4} = maybe_add_merge(State3),
         {next_state, NextState, State4}
   end;

%% merged file does not fit here, and there is nowhere to put it
%% link new layer to self and then send there
handle_event(cast, {merge_done, _, OutFileName}, StateName, State)
  when State#state.next =:= undefined, StateName == merging;
       State#state.next =:= undefined, StateName == ready -> 
   NewDepth = State#state.depth + 1,
   {ok, Pid} = ?MODULE:start_link(State#state.dir, NewDepth, 
                                  undefined, State#state.opts),
   gen_statem:cast(State#state.parent, {bottom_level, NewDepth}),
   State1 = State#state{next = Pid, max_depth = NewDepth},
   %% Inject will be blocking at the other layer
   {ok, _} = ?MODULE:inject(Pid, OutFileName),

   {ok, State3} = close_and_delete_a_and_b(State1),
   State2 = State3#state{inject_done_ref = undefined}, 
   case State2#state.c of
      undefined ->
         {next_state, ready, State2};
      CFile ->
         ROpts = [random|State#state.opts],
         ok = emojipoo_reader:close(CFile),
         ok = file:rename(filename("C", State2), filename("A", State2)),
         {ok, AFile} = emojipoo_reader:open(filename("A", State2), ROpts),
         {next_state, ready, State2#state{a = AFile,
                                          b = undefined,
                                          c = undefined}}
   end;
   
   %State2 = State1#state{inject_done_ref = MonitorRef, merge_pid = undefined},
   %{next_state, wait_inject, State2};

%% merged file does not fit here, push it down
handle_event(cast, {merge_done, _, OutFileName}, StateName, State)
  when StateName == merging;
       StateName == ready -> 
   %% no need to rename it since we don't accept new injects
   {ok, _} = ?MODULE:inject(State#state.next, OutFileName),
   
   {ok, State1} = close_and_delete_a_and_b(State),
   State2 = State1#state{inject_done_ref = undefined}, 
   case State2#state.c of
      undefined ->
         {next_state, ready, State2};
      CFile ->
         ROpts = [random|State#state.opts],
         ok = emojipoo_reader:close(CFile),
         ok = file:rename(filename("C", State2), filename("A", State2)),
         {ok, AFile} = emojipoo_reader:open(filename("A", State2), ROpts),
         {next_state, ready, State2#state{a = AFile,
                                          b = undefined,
                                          c = undefined}}
   end;
   %State1 = State#state{inject_done_ref = MonitorRef, merge_pid = undefined},
   %{next_state, wait_inject, State1};

%% waiting on inject to return something
handle_event(cast, {ok, MonRef}, wait_inject, State)
   when MonRef == State#state.inject_done_ref ->
   erlang:demonitor(MonRef, [flush]),
   {ok, State1} = close_and_delete_a_and_b(State),
   State2 = State1#state{inject_done_ref = undefined}, 
   case State2#state.c of
      undefined ->
         {next_state, ready, State2};
      CFile ->
         ROpts = [random|State#state.opts],
         ok = emojipoo_reader:close(CFile),
         ok = file:rename(filename("C", State2), filename("A", State2)),
         {ok, AFile} = emojipoo_reader:open(filename("A", State2), ROpts),
         {next_state, ready, State2#state{a = AFile,
                                          b = undefined,
                                          c = undefined}}
   end;
% should not happen, should be killed by link.
handle_event(info, {'DOWN', MonRef, _, _, Reason}, wait_inject, State)
   when MonRef == State#state.inject_done_ref ->
   exit(Reason);
handle_event(info, {'DOWN', _, _, _, _} = D, _, _) ->
   ?log("Got DOWN: ~p~n", [D]),
   {keep_state_and_data, []};
% block inject when merging, when not blocking needs to return a MonitorRef to the merging process
handle_event({call, _}, {inject, _, _}, merging, _) ->
   {keep_state_and_data, [postpone]};

handle_event({call, _}, {inject, _, _}, StateName, State) 
   when StateName == wait_inject, State#state.c =/= undefined;
        StateName == ready      , State#state.c =/= undefined ->
   % here do a merge and postpone the injection until c is not set

   {keep_state_and_data, [postpone]};

handle_event({call, From}, {inject, MonRef, FileName}, StateName, State) 
   when StateName == ready,       State#state.c == undefined;
        StateName == wait_inject, State#state.c == undefined ->
   %{merge, From}
   {ToFileName, SetPos} =
       case {State#state.a, State#state.b} of
           {undefined, undefined} ->
               {filename("A",State), #state.a};
           {_, undefined} ->
               {filename("B",State), #state.b};
           {_, _} ->
               {filename("C",State), #state.c}
       end,
   %?log("inject ~s~n", [ToFileName]),
   case file:rename(FileName, ToFileName) of
      ok -> ok;
      E  -> 
         ?error("rename failed ~p -> ~p :: ~p~n", [FileName, ToFileName, E]),
         error(E)
   end,
   
   %% give the ok to the caller
   %gen_statem:cast(element(1, From), {ok, MonRef}),

   case emojipoo_reader:open(ToFileName, [random|State#state.opts]) of
      {ok, BT} ->
         %?log("open worked ~p :: ~p~n", [ToFileName, BT]),
         State1 = if SetPos == #state.a ->
                        State#state{a = BT};
                     SetPos == #state.b ->
                        State#state{b = BT};
                     true ->
                        State#state{c = BT}
                  end,
         {NextState, State2} = maybe_add_merge(State1),
         {next_state, NextState, State2, {reply, From, {ok, MonRef}}};
      E2  -> 
         ?error("open failed ~p :: ~p~n", [ToFileName, E2]),
         error(E2)
   end;
handle_event({call, _}, close, _, #state{next = Next} = State) ->
   close_if_defined(State#state.a),
   close_if_defined(State#state.b),
   close_if_defined(State#state.c),
   [stop_if_defined(PID) || PID <- [State#state.merge_pid | State#state.folding]],
   
   %% this is synchronous all the way down, because our
   %% caller is monitoring *this* process, and thus the
   %% rpc would fail when we fall off the cliff
   if Next == undefined -> ok;
      true ->
           ?MODULE:close(Next)
   end,
   {stop_and_reply, normal, ok};
handle_event({call, _}, destroy, _, #state{next = Next} = State) ->
   destroy_if_defined(State#state.a),
   destroy_if_defined(State#state.b),
   destroy_if_defined(State#state.c),
   [stop_if_defined(PID) || PID <- [State#state.merge_pid | State#state.folding]],
   
   %% this is synchronous all the way down, because our
   %% caller is monitoring *this* proces, and thus the
   %% rpc would fail when we fall off the cliff
   if Next == undefined -> ok;
      true ->
           ?MODULE:destroy(Next)
   end,
   {stop_and_reply, normal, ok};

handle_event({call, From}, {init_range_fold, SendTo, Range, List, FilterMap}, ready, 
             #state{next = Next} = State) ->
   %?log("init_range_fold ~p -> ~p", [Range, SendTo]),
   
   Suffix = integer_to_list(erlang:unique_integer([positive])),
   {NextList, FoldingPIDs} =
      case {State#state.a, State#state.b, State#state.c} of
         {undefined, undefined, undefined} ->
            {List, []};
         {_, undefined, undefined} ->
            ok = make_hard_link("A", "AF", Suffix, State),
            Af = link_filename("AF", Suffix, State),
            {ok, PID0} = start_range_fold(Af, SendTo, Range, State, FilterMap),
            {[PID0|List], [PID0]};
         {_, _, undefined} ->
            ok = make_hard_link("A", "AF", Suffix, State),
            ok = make_hard_link("B", "BF", Suffix, State),
            Af = link_filename("AF", Suffix, State),
            Bf = link_filename("BF", Suffix, State),
            {ok, PIDA} = start_range_fold(Af, SendTo, Range, State, FilterMap),
            {ok, PIDB} = start_range_fold(Bf, SendTo, Range, State, FilterMap),
            {[PIDA,PIDB|List], [PIDB,PIDA]};
         {_, _, _} ->
            ok = make_hard_link("A", "AF", Suffix, State),
            ok = make_hard_link("B", "BF", Suffix, State),
            ok = make_hard_link("C", "CF", Suffix, State),
            Af = link_filename("AF", Suffix, State),
            Bf = link_filename("BF", Suffix, State),
            Cf = link_filename("CF", Suffix, State),
            {ok, PIDA} = start_range_fold(Af, SendTo, Range, State, FilterMap),
            {ok, PIDB} = start_range_fold(Bf, SendTo, Range, State, FilterMap),
            {ok, PIDC} = start_range_fold(Cf, SendTo, Range, State, FilterMap),
            {[PIDA,PIDB,PIDC|List], [PIDC,PIDB,PIDA]}
      end,
   case Next of
      undefined ->
         {keep_state_and_data, {reply, From, {ok, lists:reverse(NextList)}}};
      _ ->
         % send the full list of pids to the next layer down
         % `sh*t rolls down hill`
         Reply = gen_statem:call(Next, {init_range_fold, SendTo, Range, NextList, FilterMap}),
         {keep_state, 
          State#state{folding = FoldingPIDs},
          {reply, From, Reply}}
   end;
handle_event(cast, {range_fold_done, Pid}, _, State) ->
   NewFolding = lists:delete(Pid, State#state.folding),
   {keep_state, State#state{folding = NewFolding}};

handle_event(info, {'EXIT', Pid, Reason}, _, State)
  when Pid == State#state.merge_pid ->
   ?log("*** merge_died: ~p~n", [Reason]),
   %% XXX BAD
   State1 = restart_merge_then_loop(State#state{merge_pid = undefined}, Reason),
   {keep_state, State1};
handle_event(info, {'EXIT', Pid, _}, _, State) 
  when Pid == hd(State#state.folding);
       Pid == hd(tl(State#state.folding));
       Pid == hd(tl(tl(State#state.folding))) ->
   State1 = State#state{folding = lists:delete(Pid, State#state.folding)},
   {keep_state, State1}.


close_if_defined(undefined) -> ok;
close_if_defined(BT)        -> emojipoo_reader:close(BT).

destroy_if_defined(undefined) -> ok;
destroy_if_defined(BT)        -> emojipoo_reader:destroy(BT).

stop_if_defined(undefined) -> ok;
stop_if_defined(MergePid) when is_pid(MergePid) ->
   erlang:exit(MergePid, shutdown).

close_and_delete_a_and_b(State) ->
   AFileName = filename("A",State),
   BFileName = filename("B",State),

   ok = emojipoo_reader:close(State#state.a),
   ok = emojipoo_reader:close(State#state.b),

   ok = file:delete(AFileName),
   ok = file:delete(BFileName),

   {ok, State#state{a = undefined, b = undefined}}.

filename(PFX, #state{dir = Dir,
                     depth = Depth}) ->
   filename:join(Dir, PFX ++ "-" ++ integer_to_list(Depth) ++ ".data").

link_filename(Prefix, Suffix, #state{dir = Dir,
                                     depth = Depth}) ->
   filename:join(Dir, Prefix ++ "-" ++ integer_to_list(Depth) ++ 
                      "-" ++ Suffix ++ ".data").

start_range_fold(FileName, SendTo, Range, State, FilterMap) ->
   Self = self(),
   Fun = 
      fun() ->
            try
               %?log("start_range_fold ~p on ~p -> ~p", [self(), FileName, SendTo]),
               erlang:link(SendTo),
               {ok, File} = emojipoo_reader:open(FileName, [folding|State#state.opts]),
               do_range_iter(File, SendTo, self(), Range, FilterMap),
               emojipoo_reader:close(File),
               erlang:unlink(SendTo),
               ok
            catch
               Class:Ex:Stack ->
                  io:format(user, "BAD: ~p:~p ~p~n", [Class,Ex,Stack])
            after
               % delete the hard link to the file
               %?log("*** deleting: ~p~n", [FileName]),
               ok = file:delete(FileName),
               gen_statem:cast(Self, {range_fold_done, self()})
            end
      end,
   Pid = proc_lib:spawn( Fun ),
   {ok, Pid}.

-spec do_range_iter(BT        :: emojipoo_reader:read_file(),
                    SendTo    :: pid(),
                    SelfOrRef :: pid() | reference(),
                    Range     :: tuple(),
                    FilterMap :: fun() | true ) -> ok.
do_range_iter(BT, SendTo, SelfOrRef, Range, FilterMap) ->
   try 
      Fun = fun(Key, Value, {0, KVs}) ->
                  send(SendTo, SelfOrRef, [{Key, Value}|KVs], FilterMap),
                  {?BTREE_ASYNC_CHUNK_SIZE - 1, []};
               (Key, Value, {N, KVs}) ->
                  {N - 1,[{Key, Value}|KVs]}
            end,
      emojipoo_reader:range_fold(Fun, {?BTREE_ASYNC_CHUNK_SIZE-1, []},
                                 BT, Range) 
   of
      {limit, {_, KVs}, LastKey} ->
         send(SendTo, SelfOrRef, KVs, FilterMap),
         SendTo ! {level_limit, SelfOrRef, LastKey};
      {done, {_, KVs}} ->
         %% tell fold merge worker we're done
         send(SendTo, SelfOrRef, KVs, FilterMap),
         SendTo ! {level_done, SelfOrRef}
   catch
      exit:worker_died -> ok
   end,
   ok.

send(SendTo, Ref, ReverseKVs, true) ->
   send(SendTo, Ref, ReverseKVs);
send(SendTo, Ref, ReverseKVs, FilterMap) ->
   send(SendTo, Ref, lists:filtermap(FilterMap, ReverseKVs)).

send(_,_,[]) ->
    [];
send(SendTo,Ref,ReverseKVs) ->
   SendTo ! {level_results, Ref, lists:reverse(ReverseKVs)}.

exists_file(FileName) -> 
   filelib:is_file(FileName).

%% does chained lookup starting at the top and going down
do_lookup(_Key, []) -> 
   not_found;
do_lookup(Key, [undefined|Rest]) ->
   do_lookup(Key, Rest);
do_lookup(Key, [BT|Rest]) ->
   case emojipoo_reader:lookup(BT, Key) of
      {ok, ?TOMBSTONE} -> {found, ?TOMBSTONE};
      {ok, Result} -> {found, Result};
      not_found -> do_lookup(Key, Rest)
   end.

make_hard_link(Src, Tgt, Suffix, State) ->
   file:make_link(filename(Src, State), link_filename(Tgt, Suffix, State)).

restart_merge_then_loop(State, Reason) ->
   XFileName = filename("X",State),
   error_logger:warning_msg("Merger appears to have failed (reason: ~p). Removing outfile ~s\n", [Reason, XFileName]),
   file:delete(XFileName),
   maybe_add_merge(State).
