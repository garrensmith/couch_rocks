% Licensed under the Apache License, Version 2.0 (the "License"); you may not
% use this file except in compliance with the License. You may obtain a copy of
% the License at
%
%   http://www.apache.org/licenses/LICENSE-2.0
%
% Unless required by applicable law or agreed to in writing, software
% distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
% WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
% License for the specific language governing permissions and limitations under
% the License.

-module(couch_rocks).
%-behavior(couch_db_engine).

% TODO:
% * Implement epochs for node
% * Compaction seq 
% * Disk size info
% * Implement attachments

-export([
    exists/1,

    delete/3,
    % delete_compaction_files/3,

    init/2,
    terminate/2,
    handle_call/2,
    handle_info/2,

    % incref/1,
    % decref/1,
    % monitored_by/1,

    get_compacted_seq/1,
    get_del_doc_count/1,
    get_disk_version/1,
    get_doc_count/1,
    get_epochs/1,
    get_last_purged/1,
    get_purge_seq/1,
    get_revs_limit/1,
    get_security/1,
    get_size_info/1,
    get_update_seq/1,
    get_uuid/1,

    set_revs_limit/2,
    set_security/2,

    open_docs/2,
    open_local_docs/2,
    read_doc_body/2,

    serialize_doc/2,
    write_doc_body/2,
    write_doc_infos/4,

    commit_data/1,

    open_write_stream/2,
    open_read_stream/2,
    is_active_stream/2,

    fold_docs/4,
    fold_local_docs/4,
    fold_changes/5,
    count_changes_since/2,

    % start_compaction/4,
    finish_compaction/4
]).


% These are used by the compactor
% -export([
%     init_state/6,
%     open_idx_data_files/3,
%     read_header/2,
%     write_header/3,
%     update_header/2,
%     write_doc_info/2
% ]).


% -export([
%     id_seq_tree_split/2,
%     id_seq_tree_join/3,

%     id_tree_reduce/2,
%     seq_tree_reduce/2,

%     local_tree_split/2,
%     local_tree_join/3
% ]).

% % Used by the compactor
% -export([
%     set_update_seq/2,
%     copy_security/2
% ]).

-include_lib("couch/include/couch_db.hrl").
-include("couch_rocks.hrl").
-include_lib("couch/include/couch_eunit.hrl").

-record(wiacc, {
    new_ids = [],
    rem_ids = [],
    new_seqs = [],
    rem_seqs = [],
    update_seq
}).

% Column family names
-define(SEQ, "seq").
-define(LOCAL, "local").
-define(ID, "default"). %to open an existing db file with column families it seems to need a cf called default
-define(META, "meta").
-define(DOC, "document").
-define(METABIN, <<"meta">>).

-record(meta, {
    disk_version= 1,
    deleted_doc_count = 0,
    update_seq = 0,
    purge_seq = 0,
    security_options,
    revs_limit = 1000,
    uuid,
    epochs,
    compacted_seq = 0,
    last_purged = []
}).


exists(DirPath) ->
    CPFile = DirPath ++ ".rocks",
    filelib:is_dir(CPFile).

init(DirPath, Options) ->
    CPFile = DirPath ++ ".rocks",
    Create = lists:member(create, Options),
    RocksDefaultOptions = [{create_if_missing, true}, {create_missing_column_families, true}],
    {ok, State} = case exists(DirPath) of 
        false when Create =:= false -> 
            throw({not_found, no_db_file});
        false ->
            open_db(CPFile, lists:append(Options, RocksDefaultOptions));
        true when Create =:= true -> 
            throw({error, eexist});
        _ ->
            open_db(CPFile, [])
    end,
    {ok, State}.

open_db(File, Options) ->
    case rocksdb:open_with_cf(File, Options, [{?ID, []}, {?SEQ, []}, {?LOCAL, []}, {?META, []}, {?DOC, []}]) of
        {ok, DBHandle, [IdHandle, SeqHandle, LocalHandle, MetaHandle, DocHandle]} ->
            Meta = case rocksdb:get(DBHandle, MetaHandle, ?METABIN, []) of
                {ok, MetaBin} ->
                    binary_to_term(MetaBin);
                not_found ->
                    create_default_meta(DBHandle, MetaHandle, Options);
                {error, Err} ->
                    throw(Err)
            end,
            State = #state{
                id_handle = IdHandle,
                seq_handle = SeqHandle,
                local_handle = LocalHandle,
                meta_handle = MetaHandle, 
                db_handle = DBHandle,
                doc_handle = DocHandle,
                meta = Meta,
                file_name = File
                },
            {ok, State};
        Err -> 
            throw(Err)
    end.

% setup_new_db(CPFile, Options) ->
%     {ok, State} = open_existing_db(CPFile, Options),
%     #state{
%         meta = Meta, 
%         db_handle = DBHandle, 
%         meta_handle = 
%         MetaHandle
%         } = State,
    
%     case rocksdb:put(DBHandle, MetaHandle, ?METABIN, term_to_binary(Meta), [{sync, true}]) of
%             ok -> ok;
%             Err -> throw(Err)
%         end,
%     State1 = State#state{
%         meta = Meta
%     },
%     {ok, State1}.


create_default_meta(DBHandle, MetaHandle, Options) ->
    SecurityOptions = couch_util:get_value(default_security_object, Options),
    Meta = #meta{
        security_options = SecurityOptions,
        uuid = couch_uuids:random(),
        epochs = [{node(), 0}]
    },
    case rocksdb:put(DBHandle, MetaHandle, ?METABIN, term_to_binary(Meta), [{sync, true}]) of
        ok -> Meta;
        Err -> throw(Err)
    end.





terminate(_Reason, #state{db_handle = DBHandle}) ->
    rocksdb:close(DBHandle).

delete(_RootDir, DirPath, _Options) ->
    File = DirPath ++ ".rocks",
    rocksdb:destroy(File, []).


% delete_compaction_files(RootDir, DirPath, _DelOpts) ->
%     nifile:lsdir(DirPath, fun(FName, _) ->
%         FNameLen = size(FName),
%         WithoutCompact = FNameLen - 8,
%         case FName of
%             <<_:WithoutCompact/binary, ".compact">> ->
%                 couch_ngen_file:delete(RootDir, FName);
%             _ ->
%                 ok
%         end
%     end, nil).


handle_call(Msg, St) ->
    {stop, {invalid_call, Msg}, {invalid_call, Msg}, St}.


handle_info({'DOWN', _, _, _, _}, St) ->
    {stop, normal, St}.


% incref(St) ->
%     Monitors = [
%         couch_ngen_file:monitor(St#st.idx_fd),
%         couch_ngen_file:monitor(St#st.data_fd)
%     ],
%     {ok, St#st{fd_monitors = Monitors}}.


% decref(St) ->
%     lists:foreach(fun(Ref) ->
%         true = erlang:demonitor(Ref, [flush])
%     end, St#st.fd_monitors),
%     ok.


% monitored_by(St) ->
%     lists:foldl(fun(Fd, Acc) ->
%         MB = couch_ngen_file:monitored_by(Fd),
%         lists:umerge(lists:sort(MB), Acc)
%     end, [], [St#st.cp_fd, St#st.idx_fd, St#st.data_fd]).


get_compacted_seq(State) ->
    #meta{compacted_seq = CompactSeq} = get_meta_info(State),
    CompactSeq.


% This could get really slow. We could optimise by keeping this count in meta
% and manually counting as updates/deletes happen
get_del_doc_count(#state{db_handle = DBHandle, id_handle = IDHandle}) ->
    rocksdb:fold(DBHandle, IDHandle, fun({_Key, BinBody}, Acc) ->
        #full_doc_info {
            deleted = Deleted
        } = binary_to_term(BinBody),
        case Deleted of
            true -> Acc + 1;
            _ -> Acc
        end
    end, 0, []).


get_disk_version(State) ->
    #meta{disk_version = DiskVersion} = get_meta_info(State),
    DiskVersion.


% This could get really slow. We could optimise by keeping this count in meta
% and manually counting as updates/deletes happen
get_doc_count(#state{db_handle = DBHandle, id_handle = IDHandle}) ->
    rocksdb:fold(DBHandle, IDHandle, fun({_Key, BinBody}, Acc) ->
        #full_doc_info {
            deleted = Deleted
        } = binary_to_term(BinBody),
        case Deleted of
            true -> Acc;
            _ -> Acc + 1
        end
    end, 0, []).



get_meta_info(#state{meta = Meta}) ->
    Meta.



get_epochs(State) ->
    #meta{epochs = Epochs} = get_meta_info(State),
    Epochs.


get_last_purged(State) ->
    #meta{last_purged = LastPurged} = get_meta_info(State),
    LastPurged.

get_purge_seq(State) ->
    #meta{purge_seq = PurgeSeq} = get_meta_info(State),
    PurgeSeq.



get_revs_limit(State) ->
    #meta{revs_limit = RevsLimit} = get_meta_info(State),
    RevsLimit.


get_security(State) ->
    #meta{security_options = SecurityOptions} = get_meta_info(State),
    SecurityOptions.


get_size_info(_State) ->
    [
        {active, 0},
        {external, 0},
        {file, 0}
    ].


get_update_seq(State) ->
    #meta{update_seq = UpdateSeq} = get_meta_info(State),
    UpdateSeq.


get_uuid(State) ->
    #meta{uuid = UUID} = get_meta_info(State),
    UUID.


save_meta(State, NewMeta) ->
    State1 = State#state{
        meta = NewMeta
    },
    {ok, State1}.
    % case rocksdb:put(DBHandle, MetaHandle, ?METABIN, term_to_binary(NewMeta), [{sync, true}]) of
    %     ok -> {ok, State};
    %     Err -> throw(Err)
    % end.



set_revs_limit(State, RevsLimit) ->
    Meta = get_meta_info(State),
    NewMeta = Meta#meta{
        revs_limit = RevsLimit
    },
    save_meta(State, NewMeta).



set_security(State, NewSecurity) ->
    Meta = get_meta_info(State),
    NewMeta = Meta#meta{
        security_options = NewSecurity
    },
    save_meta(State, NewMeta).



open_doc(DBHandle, CFHandle, Id, Options) when is_binary(Id) =:= false ->
    open_doc(DBHandle, CFHandle, term_to_binary(Id), Options);
open_doc(DBHandle, CFHandle, Id, Options)  ->
    case rocksdb:get(DBHandle, CFHandle, Id, Options) of 
        {ok, FDI} -> binary_to_term(FDI);
        not_found -> not_found;
        {err, Err} -> throw(Err)
    end.


open_docs(#state{db_handle = DBHandle, id_handle = IDHandle}, DocIds) ->
    lists:map(fun (Id) -> 
        open_doc(DBHandle, IDHandle, Id, []) 
    end, DocIds).

open_local_docs(#state{db_handle = DBHandle, local_handle = LocalHandle}, DocIds) ->
    lists:map(fun (Id) -> 
        open_doc(DBHandle, LocalHandle, Id, []) 
    end, DocIds).


% We won't do any compression here, and rather let Rocksdb do that for us
serialize_doc(_State, Doc) ->
    Doc.


write_doc_body(State, #doc{id = Id} = Doc) ->
    #state{
        doc_handle = DocHandle, 
        db_handle = DBHandle
    } = State,
    BodyBin = term_to_binary(Doc),
    case rocksdb:put(DBHandle, DocHandle, term_to_binary(Id), BodyBin, []) of 
        ok -> {
            ok,
            Doc,
            byte_size(BodyBin)
        };
        {err, Err} -> throw(Err)
    end.

read_doc_body(#state{db_handle = DBHandle, doc_handle = DocHandle}, #doc{id = Id}) ->
    case rocksdb:get(DBHandle, DocHandle, term_to_binary(Id), []) of
        {ok, BinDoc} -> binary_to_term(BinDoc);
        {error, Err} -> throw(Err)
    end.


write_doc_infos(#state{} = State, Pairs, LocalDocs, PurgedIdRevs) ->
    #state{
        id_handle = IDHandle,
        seq_handle = SeqHandle,
        local_handle = LocalHandle,
        db_handle = DBHandle
    } = State,
    #wiacc{
        new_ids = NewIds,
        rem_ids = RemIds,
        new_seqs = NewSeqs,
        rem_seqs = RemSeqs,
        update_seq = NewSeq
    } = get_write_info(State, Pairs),

    {ok, Batch} = rocksdb:batch(),

    {AddLDocs, RemLDocIds} = lists:foldl(fun(Doc, {AddAcc, RemAcc}) ->
        case Doc#doc.deleted of
            true ->
                {AddAcc, [Doc#doc.id | RemAcc]};
            false ->
                #doc{revs = {0, [RevInt | _]}} = Doc,
                RevBin = integer_to_binary(RevInt),
                Doc1 = Doc#doc{
                    revs = {0, [RevBin]}
                },
                {[{Doc1#doc.id, Doc1} | AddAcc], RemAcc}
        end
    end, {[], []}, LocalDocs),

    add_remove(Batch, IDHandle, NewIds, RemIds),
    add_remove(Batch, SeqHandle, NewSeqs, RemSeqs),
    add_remove(Batch, LocalHandle, AddLDocs, RemLDocIds),

    Meta = get_meta_info(State),

    #meta{
        deleted_doc_count = DelDocCount
    } = Meta,
    DeletedDocCount = DelDocCount + length(RemIds),

    NewMeta = case PurgedIdRevs of
        [] -> 
            Meta#meta{
                deleted_doc_count = DeletedDocCount,
                update_seq = NewSeq
            };
        _ ->
            #meta{
                purge_seq = OldPurgeSeq
                } = Meta,
            Meta#meta{
                deleted_doc_count = DeletedDocCount,
                update_seq = NewSeq + 1,
                purge_seq = OldPurgeSeq + 1,
                last_purged = PurgedIdRevs
            }
    end,

    State1 = save_meta(State, NewMeta),
    case rocksdb:write_batch(DBHandle, Batch, []) of
        ok -> State1;
        {error, Err} -> throw(Err)
    end.
    


key_to_binary(Key) when is_binary(Key) ->
    Key;
key_to_binary(Key) ->
    term_to_binary(Key).



add_remove(Batch, CFHandle, NewKeyValues, RemoveKeys) ->
    lists:foreach(fun(Key) ->
        ok = rocksdb:batch_delete(Batch, CFHandle, key_to_binary(Key))
    end, RemoveKeys),

    lists:foreach(fun({Key, Value}) ->
        ok = rocksdb:batch_put(Batch, CFHandle, key_to_binary(Key), term_to_binary(Value))
    end, NewKeyValues).


%Making a huge assumption for now. I'm hoping that a full sync of one item will cause all previous
%writes to also be flushed to the WAL. 
commit_data(#state{db_handle = DBHandle, meta_handle = MetaHandle, meta = Meta} = State) ->
    case rocksdb:put(DBHandle, MetaHandle, ?METABIN, term_to_binary(Meta), [{sync, true}]) of 
        ok -> {ok, State};
        {error, Err} -> throw(Err)
    end.

open_write_stream(_, _) ->
    throw(not_supported).
% open_write_stream(#st{} = St, Options) ->
%     couch_stream:open({couch_ngen_stream, {St#st.data_fd, []}}, Options).


open_read_stream(_, _) ->
    throw(not_supported).
% open_read_stream(#st{} = St, StreamSt) ->
%     {ok, {couch_ngen_stream, {St#st.data_fd, StreamSt}}}.


is_active_stream(_, _) ->
    throw(not_supported).
% is_active_stream(#st{} = St, {couch_ngen_stream, {Fd, _}}) ->
%     St#st.data_fd == Fd;
% is_active_stream(_, _) ->
%     false.


fold_docs(#state{db_handle = DBHandle, id_handle = IDHandle}, UserFun, UserAcc, Options) ->
    fold_docs_int(DBHandle, IDHandle, UserFun, UserAcc, Options).


fold_local_docs(#state{db_handle = DBHandle, local_handle = LocalHandle}, UserFun, UserAcc, Options) ->
    fold_docs_int(DBHandle, LocalHandle, UserFun, UserAcc, Options).


 fold_changes(#state{db_handle = DBHandle, seq_handle = SeqHandle}, SinceSeq, UserFun, UserAcc, Options) ->
    {Start, Direction} = case lists:member({dir, rev}, Options) of
        true -> {{seek_for_prev, term_to_binary(SinceSeq + 1)}, prev};
        _ -> {{seek, term_to_binary(SinceSeq + 1)}, next}
    end,
    case rocksdb:snapshot(DBHandle) of 
        {error, Err} -> 
            throw(Err);
        {ok, Snapshot} ->
            case rocksdb:iterator(DBHandle, SeqHandle, [{snapshot, Snapshot}]) of
                {ok, Iter} ->
                    try
                        fold_seq_loop(rocksdb:iterator_move(Iter, Start), Iter, Direction, SinceSeq, UserFun, UserAcc)
                    after
                        rocksdb:release_snapshot(Snapshot),
                        rocksdb:iterator_close(Iter)
                    end;

                {error, Err} ->
                    throw(Err)
            end
    end.


fold_seq_loop({error, iterator_closed}, _Iter, _Direction, _StartKey, _Fun, Acc0) ->
    throw({iterator_closed, Acc0});
fold_seq_loop({error, invalid_iterator}, _Iter, _Fun, _Direction, _StartKey, Acc0) ->
    {ok, Acc0};
fold_seq_loop({ok, _Key, Value}, Iter, Direction, StartKey, Fun, Acc0) ->
    {ok, OutAcc} = Fun(binary_to_term(Value), Acc0),
    fold_seq_loop(
        rocksdb:iterator_move(Iter, Direction), Iter, Direction, StartKey, Fun, OutAcc).



count_changes_since(State, SinceSeq) ->
    FoldFun = fun(_Doc, Acc) -> {ok, Acc + 1} end,
    {ok, ChangesSince} = fold_changes(State, SinceSeq, FoldFun, 0, []),
    ChangesSince.



% start_compaction(St, DbName, Options, Parent) ->
%     Args = [St, DbName, Options, Parent],
%     Pid = spawn_link(couch_ngen_compactor, start, Args),
%     {ok, St, Pid}.

finish_compaction(_S, _D, _O, _D) ->
    ok.
% finish_compaction(SrcSt, DbName, Options, DirPath) ->
%     {ok, TgtSt1} = ?MODULE:init(DirPath, [compactor | Options]),
%     SrcSeq = get_update_seq(SrcSt),
%     TgtSeq = get_update_seq(TgtSt1),
%     case SrcSeq == TgtSeq of
%         true ->
%             finish_compaction_int(SrcSt, TgtSt1);
%         false ->
%             couch_log:info("Compaction file still behind main file "
%                            "(update seq=~p. compact update seq=~p). Retrying.",
%                            [SrcSeq, TgtSeq]),
%             ok = decref(TgtSt1),
%             start_compaction(SrcSt, DbName, Options, self())
%     end.


% id_seq_tree_split({Key, Ptr}, _DataFd) ->
%     {Key, Ptr}.


% id_seq_tree_join(_Key, DiskPtr, DataFd) ->
%     {ok, DiskTerm} = couch_ngen_file:read_term(DataFd, DiskPtr),
%     {Id, Seq, Deleted, Sizes, DiskTree} = DiskTerm,
%     #full_doc_info{
%         id = Id,
%         update_seq = Seq,
%         deleted = ?i2b(Deleted),
%         sizes = couch_db_updater:upgrade_sizes(Sizes),
%         rev_tree = rev_tree(DiskTree)
%     }.


% id_tree_reduce(reduce, FullDocInfos) ->
%     FoldFun = fun(Info, {Count, DelCount, Sizes}) ->
%         Sizes2 = reduce_sizes(Sizes, Info#full_doc_info.sizes),
%         case Info#full_doc_info.deleted of
%             true -> {Count, DelCount + 1, Sizes2};
%             false -> {Count + 1, DelCount, Sizes2}
%         end
%     end,
%     lists:foldl(FoldFun, {0, 0, #size_info{}}, FullDocInfos);
% id_tree_reduce(rereduce, Reds) ->
%     FoldFun = fun({Count1, DelCount1, Sizes1}, {Count2, DelCount2, Sizes2}) ->
%             Sizes3 = reduce_sizes(Sizes1, Sizes2),
%             {Count1 + Count2, DelCount1 + DelCount2, Sizes3}
%     end,
%     lists:foldl(FoldFun, {0, 0, #size_info{}}, Reds).


% seq_tree_reduce(reduce, DocInfos) ->
%     % count the number of documents
%     length(DocInfos);
% seq_tree_reduce(rereduce, Reds) ->
%     lists:sum(Reds).




% increment_update_seq(#st{header = Header} = St) ->
%     UpdateSeq = couch_ngen_header:get(Header, update_seq),
%     St#st{
%         header = couch_ngen_header:set(Header, [
%             {update_seq, UpdateSeq + 1}
%         ])
%     }.



% delete_compaction_files(DirPath) ->
%     RootDir = config:get("couchdb", "database_dir", "."),
%     delete_compaction_files(RootDir, DirPath, []).


get_write_info(St, Pairs) ->
    Acc = #wiacc{update_seq = get_update_seq(St)},
    get_write_info(St, Pairs, Acc).


get_write_info(_St, [], Acc) ->
    Acc;

get_write_info(St, [{OldFDI, NewFDI} | Rest], Acc) ->
    NewAcc = case {OldFDI, NewFDI} of
        {not_found, #full_doc_info{}} ->
            #full_doc_info{
                id = Id,
                update_seq = Seq
            } = NewFDI,
            Acc#wiacc{
                new_ids = [{Id, NewFDI} | Acc#wiacc.new_ids],
                new_seqs = [{Seq, NewFDI} | Acc#wiacc.new_seqs],
                update_seq = erlang:max(Seq, Acc#wiacc.update_seq)
            };
        {#full_doc_info{id = Id}, #full_doc_info{id = Id}} ->
            #full_doc_info{
                update_seq = OldSeq
            } = OldFDI,
            #full_doc_info{
                update_seq = NewSeq
            } = NewFDI,
            Acc#wiacc{
                new_ids = [{Id, NewFDI} | Acc#wiacc.new_ids],
                new_seqs = [{NewSeq, NewFDI} | Acc#wiacc.new_seqs],
                rem_seqs = [OldSeq | Acc#wiacc.rem_seqs],
                update_seq = erlang:max(NewSeq, Acc#wiacc.update_seq)
            };
        {#full_doc_info{}, not_found} ->
            #full_doc_info{
                id = Id,
                update_seq = Seq
            } = OldFDI,
            Acc#wiacc{
                rem_ids = [Id | Acc#wiacc.rem_ids],
                rem_seqs = [Seq | Acc#wiacc.rem_seqs]
            }
    end,
    get_write_info(St, Rest, NewAcc).



% rev_tree(DiskTree) ->
%     couch_key_tree:map(fun
%         (_RevId, {Del, Ptr, Seq}) ->
%             #leaf{
%                 deleted = ?i2b(Del),
%                 ptr = Ptr,
%                 seq = Seq
%             };
%         (_RevId, {Del, Ptr, Seq, Size}) ->
%             #leaf{
%                 deleted = ?i2b(Del),
%                 ptr = Ptr,
%                 seq = Seq,
%                 sizes = couch_db_updater:upgrade_sizes(Size)
%             };
%         (_RevId, {Del, Ptr, Seq, Sizes, Atts}) ->
%             #leaf{
%                 deleted = ?i2b(Del),
%                 ptr = Ptr,
%                 seq = Seq,
%                 sizes = couch_db_updater:upgrade_sizes(Sizes),
%                 atts = Atts
%             };
%         (_RevId, ?REV_MISSING) ->
%             ?REV_MISSING
%     end, DiskTree).


% disk_tree(RevTree) ->
%     couch_key_tree:map(fun
%         (_RevId, ?REV_MISSING) ->
%             ?REV_MISSING;
%         (_RevId, #leaf{} = Leaf) ->
%             #leaf{
%                 deleted = Del,
%                 ptr = Ptr,
%                 seq = Seq,
%                 sizes = Sizes,
%                 atts = Atts
%             } = Leaf,
%             {?b2i(Del), Ptr, Seq, split_sizes(Sizes), Atts}
%     end, RevTree).


% split_sizes(#size_info{}=SI) ->
%     {SI#size_info.active, SI#size_info.external}.


% reduce_sizes(nil, _) ->
%     nil;
% reduce_sizes(_, nil) ->
%     nil;
% reduce_sizes(#size_info{}=S1, #size_info{}=S2) ->
%     #size_info{
%         active = S1#size_info.active + S2#size_info.active,
%         external = S1#size_info.external + S2#size_info.external
%     };
% reduce_sizes(S1, S2) ->
%     US1 = couch_db_updater:upgrade_sizes(S1),
%     US2 = couch_db_updater:upgrade_sizes(S2),
%     reduce_sizes(US1, US2).


% active_size(#st{} = St, Size) when is_integer(Size) ->
%     active_size(St, #size_info{active=Size});
% active_size(#st{} = St, #size_info{} = SI) ->
%     Trees = [
%         St#st.id_tree,
%         St#st.seq_tree,
%         St#st.local_tree
%     ],
%     lists:foldl(fun(T, Acc) ->
%         case couch_ngen_btree:size(T) of
%             _ when Acc == null ->
%                 null;
%             undefined ->
%                 null;
%             Size ->
%                 Acc + Size
%         end
%     end, SI#size_info.active, Trees).


fold_docs_int(DBHandle, CFHandle, UserFun, UserAcc, Options) ->
    %I'm not 100% certain around the reduce work, I need to follow up on that
    {Reduce, Acc0} = case lists:member(include_reductions, Options) of 
        true -> {true, {[],UserAcc}};
        _ -> {false, UserAcc}
    end,

    FoldFun = case lists:member(include_deleted, Options) of
        true -> UserFun;
        false when Reduce =:= true -> skip_deleted_with_reduce(UserFun);
        false when Reduce =:= false -> skip_deleted(UserFun)
    end,

    case rocksdb:iterator(DBHandle, CFHandle, []) of
        {ok, Iter} ->
            try
                {ok, StartKey, EndKey, Direction} = get_iter_range(Options),
                {ok, Acc} = fold_loop(rocksdb:iterator_move(Iter, StartKey), Iter, Direction, EndKey, Reduce, FoldFun, Acc0),
                case Reduce of
                    true -> 
                        {Red, FinalAcc} = Acc,

                        {ok, lists:sum(Red), FinalAcc};
                    _  -> 
                        {ok, Acc}
                end
            after
                rocksdb:iterator_move(Iter, next),
                rocksdb:iterator_close(Iter)
            end;

        {error, Err} ->
            throw(Err)
    end.



fold_loop({error, iterator_closed}, _Iter, _Direction, _EndKey, _Reduce, _Fun, Acc0) ->
    throw({iterator_closed, Acc0});
fold_loop({error, invalid_iterator}, _Iter, _Direction, _EndKey, _Reduce, _Fun, Acc0) ->
    {ok, Acc0};

%endkey checks
fold_loop({ok, Key, _Value}, _Iter, Direction, {EndKey, Inclusive}, _Reduce, _Fun, Acc0) 
    when Key >= EndKey, EndKey /= last, Direction =:= next, Inclusive =:= lte ->
    {ok, Acc0};
fold_loop({ok, Key, _Value}, _Iter, Direction, {EndKey, Inclusive}, _Reduce, _Fun, Acc0) 
    when Key =< EndKey, EndKey /= first, Direction =:= prev, Inclusive =:= lte ->
    {ok, Acc0};
fold_loop({ok, Key, _Value}, _Iter, Direction, {EndKey, Inclusive}, _Reduce, _Fun, Acc0) 
    when Key > EndKey, EndKey /= last, Direction =:= next, Inclusive =:= lt ->
    {ok, Acc0};
fold_loop({ok, Key, _Value}, _Iter, Direction, {EndKey, Inclusive}, _Reduce, _Fun, Acc0) 
    when Key < EndKey, EndKey /= first, Direction =:= prev, Inclusive =:= lt ->
    {ok, Acc0};

fold_loop({ok, _Key, Value}, Iter, Direction, EndKey, Reduce, Fun, Acc0) ->
    {Go, Acc2} = case Reduce of
        true ->
            {Red, Acc1} = Acc0,
            {Go0, AccOut} = Fun(binary_to_term(Value), Red, Acc1),
            {Go0, {Red, AccOut}};
        _ ->
            Fun(binary_to_term(Value), Acc0)
    end,
    case Go of
        stop -> 
            {ok, Acc2};
        _ -> 
            fold_loop(rocksdb:iterator_move(Iter, Direction), Iter, Direction, EndKey, Reduce, Fun, Acc2)
    end.

get_iter_range(Options) ->
    {BaseStart, BaseEnd, Direction} = case lists:member({dir, rev}, Options) of
        true -> {last, first, prev};
        _ -> {first, last, next}
    end,
    StartKey = get_iter_start_key(Options, BaseStart, Direction),
    EndKey = get_iter_end_key(Options, BaseEnd),
    {ok, StartKey, EndKey, Direction}.



get_iter_start_key(Options, BaseStart, Direction) ->
    case lists:keyfind(start_key, 1, Options) of
        {start_key, Key} -> 
            case Direction of
                next -> {seek, Key};
                _ -> {seek_for_prev, Key}
            end;
        _ -> 
            BaseStart
    end.



get_iter_end_key(Options, BaseEnd) ->
case lists:keyfind(end_key, 1, Options) of
    {end_key, Key} -> 
        {Key, lt};
    _ -> 
        case lists:keyfind(end_key_gt, 1, Options) of
            {end_key_gt, Key} ->
                {Key, lte};
            _ ->
                BaseEnd
        end
end.

skip_deleted_with_reduce(UserFun) ->
    fun(Doc, Red, Acc) ->
        Deleted = case Doc of
            #full_doc_info{deleted = Deleted0} -> Deleted0;
            #doc{deleted = Deleted0} -> Deleted0
        end,
        case Deleted of
            true -> {ok, Acc};
            _ -> UserFun(Doc, Red, Acc)
        end
    end.
    
skip_deleted(UserFun) ->
    fun(Doc, Acc) ->
        Deleted = case Doc of
            #full_doc_info{deleted = Deleted0} -> Deleted0;
            #doc{deleted = Deleted0} -> Deleted0
        end,

        case Deleted of
            true -> {ok, Acc};
            _ -> UserFun(Doc, Acc)
        end
    end.


% finish_compaction_int(#st{} = OldSt, #st{} = NewSt1) ->
%     #st{
%         dirpath = DirPath,
%         local_tree = OldLocal
%     } = OldSt,
%     #st{
%         dirpath = DirPath,
%         header = Header,
%         local_tree = NewLocal1
%     } = NewSt1,

%     % suck up all the local docs into memory and write them to the new db
%     LoadFun = fun(Value, _Offset, Acc) ->
%         {ok, [Value | Acc]}
%     end,
%     {ok, _, LocalDocs} = couch_ngen_btree:foldl(OldLocal, LoadFun, []),
%     {ok, NewLocal2} = couch_ngen_btree:add(NewLocal1, LocalDocs),

%     {ok, NewSt2} = commit_data(NewSt1#st{
%         header = couch_bt_engine_header:set(Header, [
%             {compacted_seq, get_update_seq(OldSt)},
%             {revs_limit, get_revs_limit(OldSt)}
%         ]),
%         local_tree = NewLocal2
%     }),

%     % Move our compaction files into place
%     ok = remove_compact_suffix(NewSt2#st.idx_fd),
%     ok = remove_compact_suffix(NewSt2#st.data_fd),
%     ok = delete_fd(NewSt2#st.idx_fd),

%     % Remove the old database files
%     ok = delete_fd(OldSt#st.data_fd),
%     ok = delete_fd(OldSt#st.idx_fd),
%     ok = delete_fd(OldSt#st.cp_fd),

%     % Final swap to finish compaction
%     ok = remove_compact_suffix(NewSt2#st.cp_fd),

%     decref(OldSt),
%     {ok, NewSt2, undefined}.


% remove_compact_suffix(Fd) ->
%     Path = couch_ngen_file:path(Fd),
%     PathWithoutCompact = size(Path) - size(<<".compact">>),
%     <<FileName:PathWithoutCompact/binary, ".compact">> = Path,
%     couch_ngen_file:rename(Fd, FileName).
