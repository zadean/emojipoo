-module(emojipoo_writer).

-behavior(gen_server).

-include("emojipoo.hrl").

%%
%% Streaming btree writer. Accepts only monotonically increasing keys for put.
%%

-define(NODE_SIZE, 8 * 1024).

%% gen_server
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3
]).

%% API
-export([
    open/1, open/2,
    add/3,
    add_cache/2,
    count/1,
    close/1
]).

%% Internal API
-export([
    serialize_state/1,
    deserialize_state/1
]).

-record(node, {
    level :: integer(),
    members = [] :: [{key(), expvalue()}],
    size = 0 :: integer()
}).

-record(state, {
    index_file :: file:io_device() | undefined,
    index_file_pos :: integer() | undefined,
    last_node_pos :: pos_integer() | undefined,
    last_node_size :: pos_integer() | undefined,
    nodes = [] :: list(#node{}),
    name :: string(),
    block_size = ?NODE_SIZE :: integer(),
    compress = none :: none | snappy | gzip | lz4,
    opts = [] :: list(any()),
    value_count = 0 :: integer(),
    tombstone_count = 0 :: integer()
}).

%%% PUBLIC API

open(Name) ->
    gen_server:start_link(?MODULE, [Name, [{expiry_secs, 0}]], []).

open(Name, Options) ->
    emojipoo_util:ensure_expiry_option(Options),
    gen_server:start_link(?MODULE, [Name, Options], []).

add(Server, Key, Value) ->
    gen_server:cast(Server, {add, Key, Value}).

add_cache(Server, Cache) ->
    gen_server:cast(Server, {add, Cache}).

%% @doc Return number of KVs added to this writer so far
count(Server) ->
    gen_server:call(Server, count, infinity).

%% @doc Close the btree index file
close(Server) ->
    gen_server:call(Server, close, infinity).

%%% INTERNAL API

%%% XXX serializers not currently used!!

%% Close file and serialize state
serialize_state(
    #state{
        index_file = File,
        index_file_pos = Position
    } = State
) ->
    case file:position(File, {eof, 0}) of
        {ok, Position} -> ok;
        {ok, WrongPosition} -> exit({bad_position, Position, WrongPosition})
    end,
    ok = file:close(File),
    State1 = State#state{index_file = undefined},
    erlang:term_to_binary(State1).

%% Revive state from binary with new file
deserialize_state(Binary) ->
    State = erlang:binary_to_term(Binary),
    {ok, IdxFile} = do_open(State#state.name, State#state.opts, []),
    State#state{index_file = IdxFile}.

%%%

init([Name, Options]) ->
    emojipoo_util:ensure_expiry_option(Options),
    case do_open(Name, Options, [exclusive]) of
        {ok, IdxFile} ->
            ok = file:write(IdxFile, ?FILE_FORMAT),
            BlockSize = emojipoo_util:get_opt(block_size, Options, ?NODE_SIZE),
            {ok, #state{
                name = Name,
                index_file_pos = ?FIRST_BLOCK_POS,
                index_file = IdxFile,
                block_size = BlockSize,
                compress = emojipoo_util:get_opt(compress, Options, none),
                opts = Options
            }};
        {error, _} = Error ->
            ?error("emojipoo_writer cannot open ~p: ~p~n", [Name, Error]),
            {stop, Error}
    end.

handle_cast({add, Key, {?TOMBSTONE, TStamp}}, State) when is_binary(Key) ->
    NewState =
        case emojipoo_util:has_expired(TStamp) of
            % don`t write if expired
            true ->
                State;
            false ->
                {ok, State2} = append_node(0, Key, {?TOMBSTONE, TStamp}, State),
                State2
        end,
    {noreply, NewState};
handle_cast({add, Key, ?TOMBSTONE}, State) when is_binary(Key) ->
    {ok, NewState} = append_node(0, Key, ?TOMBSTONE, State),
    {noreply, NewState};
handle_cast({add, Key, {Value, TStamp}}, State) when
    is_binary(Key),
    is_binary(Value)
->
    NewState =
        case emojipoo_util:has_expired(TStamp) of
            % don`t write if expired
            true ->
                State;
            false ->
                {ok, State2} = append_node(0, Key, {Value, TStamp}, State),
                State2
        end,
    {noreply, NewState};
handle_cast({add, Key, Value}, State) when is_binary(Key), is_binary(Value) ->
    {ok, State2} = append_node(0, Key, Value, State),
    {noreply, State2};
handle_cast({add, Cache}, State) ->
    Before = ?NOW,
    Fun = fun({Key, Value}, State0) ->
        {ok, State1} = append_node(0, Key, Value, State0),
        State1
    end,
    State2 = ets:foldl(Fun, State, Cache),
    Diff = timer:now_diff(?NOW, Before),
    ?log("Merge cache took: ~p~n", [Diff div 1000]),
    {noreply, State2}.

handle_call(
    count,
    _From,
    #state{
        value_count = VC,
        tombstone_count = TC
    } = State
) ->
    {reply, {ok, VC + TC}, State};
handle_call(close, _From, State) ->
    {ok, State2} = archive_nodes(State),
    {stop, normal, ok, State2}.

handle_info(Info, State) ->
    ?error("Unknown info ~p~n", [Info]),
    {stop, bad_msg, State}.

terminate(normal, _State) ->
    ok;
terminate(_Reason, State) ->
    %% premature delete -> cleanup
    _ = file:close(State#state.index_file),
    file:delete(State#state.name).

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% INTERNAL FUNCTIONS

do_open(Name, Options, OpenOpts) ->
    BufferSize = emojipoo_util:get_opt(write_buffer_size, Options, 512 * 1024),
    file:open(Name, [raw, append, {delayed_write, BufferSize, 2000} | OpenOpts]).

%% @doc flush pending nodes and write trailer
archive_nodes(
    #state{
        nodes = [],
        last_node_pos = LastNodePos,
        index_file = IdxFile
    } = State
) ->
    RootPos =
        case LastNodePos of
            undefined ->
                %% store contains no entries
                ok = file:write(IdxFile, <<0:32/unsigned, 0:16/unsigned>>),
                ?FIRST_BLOCK_POS;
            _ ->
                LastNodePos
        end,
    Trailer = [<<0:32/unsigned>>, <<RootPos:64/unsigned>>],

    ok = file:write(IdxFile, Trailer),
    ok = file:datasync(IdxFile),
    ok = file:close(IdxFile),
    {ok, State#state{index_file = undefined, index_file_pos = undefined}};
archive_nodes(
    #state{
        nodes = [
            #node{
                level = N,
                members = [{_, {Pos, _Len}}]
            }
        ],
        last_node_pos = Pos
    } = State
) when N > 0 ->
    %% Ignore this node, its stack consists of one node with one {pos,len} member
    archive_nodes(State#state{nodes = []});
archive_nodes(State) ->
    {ok, State2} = flush_node_buffer(State),
    archive_nodes(State2).

append_node(Level, Key, Value, #state{nodes = []} = State) ->
    append_node(Level, Key, Value, State#state{nodes = [#node{level = Level}]});
append_node(
    Level,
    Key,
    Value,
    #state{nodes = [#node{level = Level2} | _] = Stack} = State
) when
    Level < Level2
->
    NewNode = #node{level = (Level2 - 1)},
    append_node(Level, Key, Value, State#state{nodes = [NewNode | Stack]});
append_node(
    Level,
    Key,
    Value,
    #state{
        nodes = [
            #node{
                level = Level,
                members = MembersList,
                size = NodeSize
            } = CurrNode
            | RestNodes
        ],
        value_count = VC,
        tombstone_count = TC
    } = State
) ->
    %% The top-of-stack node is at the level we wish to insert at.
    %%    %% Assert that keys are increasing:
    %%    case MembersList of
    %%       [] -> ok;
    %%       [{PrevKey,_}|_] ->
    %%          if (Key >= PrevKey) -> ok;
    %%             true ->
    %%                ?error("keys not ascending ~p < ~p~n", [PrevKey, Key]),
    %%                exit({badarg, Key})
    %%          end
    %%    end,
    RecSize = emojipoo_util:estimate_node_size_increment(Key, Value),
    NewSize = NodeSize + RecSize,
    {TC1, VC1} =
        case Level of
            0 ->
                case Value of
                    ?TOMBSTONE -> {TC + 1, VC};
                    %% Matched when this Value can expire
                    {?TOMBSTONE, _} -> {TC + 1, VC};
                    _ -> {TC, VC + 1}
                end;
            _ ->
                {TC, VC + 1}
        end,

    NodeMembers = [{Key, Value} | MembersList],
    State2 = State#state{
        nodes = [
            CurrNode#node{
                members = NodeMembers,
                size = NewSize
            }
            | RestNodes
        ],
        value_count = VC1,
        tombstone_count = TC1
    },
    case NewSize >= State#state.block_size of
        true ->
            flush_node_buffer(State2);
        false ->
            {ok, State2}
    end.

flush_node_buffer(
    #state{
        nodes = [
            #node{
                level = Level,
                members = NodeMembers
            }
            | RestNodes
        ],
        compress = Compress,
        index_file_pos = NodePos
    } = State
) ->
    OrderedMembers = lists:reverse(NodeMembers),
    {ok, BlockData} = emojipoo_util:encode_index_node(OrderedMembers, Compress),
    BlockSize = erlang:iolist_size(BlockData),
    Data = [<<(BlockSize + 2):32/unsigned, Level:16/unsigned>> | BlockData],
    DataSize = BlockSize + 6,
    ok = file:write(State#state.index_file, Data),
    {FirstKey, _} = hd(OrderedMembers),
    append_node(
        Level + 1,
        FirstKey,
        {NodePos, DataSize},
        State#state{
            nodes = RestNodes,
            index_file_pos = NodePos + DataSize,
            last_node_pos = NodePos,
            last_node_size = DataSize
        }
    ).

%% gb_tree_fold(Fun, Acc, Tree) when is_function(Fun, 3) ->
%%    IFun = fun({Key, Value}, Acc0) ->
%%                 Fun(Key, Value, Acc0)
%%           end,
%%    ets:foldl(IFun, Acc, Tree).
