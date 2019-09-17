-include_lib("kernel/include/logger.hrl").

%-define(log(A,B), io:format("LOG: " ++ A, B)).
%-define(error(A,B), io:format("ERR: " ++ A, B)).
-define(log(A,B),?LOG_DEBUG(A, B)).
-define(error(A,B),?LOG_ERROR(A, B)).

-define(BTREE_ASYNC_CHUNK_SIZE, 100).

-define(NOW, os:timestamp()).
-define(APP, emojipoo).

-define(LOGFILENAME(Dir), filename:join(Dir, "transact.log")).

-record(key_range,
        {from_key = <<>>       :: binary(),
         from_inclusive = true :: boolean(),
         to_key                :: binary() | undefined,
         to_inclusive = false  :: boolean(),
         limit                 :: pos_integer() | undefined }).

-record(log,
        {log_file       :: undefined | file:fd(),
         dir            :: string(),
         cache          :: ets:tid(),
         total_size = 0 :: integer(),
         count = 0      :: integer(),
         last_sync = ?NOW :: {integer(), integer(), integer()},
         min_depth      :: integer(),
         max_depth      :: integer(),
         config = []    :: [{atom(), term()}],
         step = 0       :: integer(),
         merge_done = 0 :: integer()}).

%% smallest levels are 1024 entries
-define(DEFAULT_DEPTH, 10).
-define(BTREE_SIZE(Layer), (1 bsl (Layer))).
-define(FILE_FORMAT, <<"EMP1">>).
-define(FIRST_BLOCK_POS, byte_size(?FILE_FORMAT)).

-define(TOMBSTONE, 'deleted').

-define(KEY_IN_FROM_RANGE(Key,Range),
        ((Range#key_range.from_inclusive andalso
          (Range#key_range.from_key =< Key))
         orelse
           (Range#key_range.from_key < Key))).

-define(KEY_IN_TO_RANGE(Key,Range),
        ((Range#key_range.to_key == undefined)
         orelse
         ((Range#key_range.to_inclusive andalso
             (Key =< Range#key_range.to_key))
          orelse
             (Key <  Range#key_range.to_key)))).

-define(KEY_IN_RANGE(Key,Range),
        (?KEY_IN_FROM_RANGE(Key,Range) andalso ?KEY_IN_TO_RANGE(Key,Range))).

-type kventry() :: { key(), expvalue() } | [ kventry() ].
-type key() :: binary().
-type batchspec() :: { delete, key() } | { put, key(), value() }.
-type value() :: ?TOMBSTONE | binary().
-type expiry() :: infinity | integer().
-type filepos() :: { non_neg_integer(), non_neg_integer() }.
-type expvalue() :: { value(), expiry() }
                  | value()
                  | filepos().
-type keyrange() :: #key_range{}.

%% tags used in the on-disk representation
-define(TAG_KV_DATA,  16#80).
-define(TAG_DELETED,  16#81).
-define(TAG_POSLEN32, 16#82).
-define(TAG_BATCH,    16#83).
-define(TAG_KV_DATA2, 16#84).
-define(TAG_DELETED2, 16#85).
-define(TAG_END,      16#FF).


