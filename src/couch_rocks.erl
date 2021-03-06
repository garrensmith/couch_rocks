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
-behavior(couch_db_engine).

% TODO:
% * Implement epochs for node
% * Compaction seq 
% * Disk size info
% * Implement attachments

-export([
    exists/1,

    delete/3,
    delete_compaction_files/3,

    init/2,
    terminate/2,
    handle_db_updater_call/2,
    handle_db_updater_info/2,

    incref/1,
    decref/1,
    monitored_by/1,

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

    start_compaction/4,
    finish_compaction/4
]).

%needed for the monitoring api calls
-export[mon_loop/0].


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
    filelib:is_dir(DirPath).

init(DirPath, Options) ->
    CPFile = DirPath,
    Create = lists:member(create, Options),
    RocksDefaultOptions = [{create_if_missing, true}, {create_missing_column_families, true}],
    {ok, State} = case exists(DirPath) of 
        false when Create =:= false -> 
            throw({not_found, no_db_file});
        false ->
            filelib:ensure_dir(CPFile),
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
                file_name = File,
                mon_pid = spawn_link(?MODULE, mon_loop, [])
                },
            {ok, State};
        Err -> 
            throw(Err)
    end.


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


delete_compaction_files(_RootDir, _DirPath, _DelOpts) ->
    ok.


handle_db_updater_call(Msg, St) ->
    {stop, {invalid_call, Msg}, {invalid_call, Msg}, St}.

handle_db_updater_info({'DOWN', Ref, _, _, _}, #state{db_monitor=Ref} = State) ->
    {stop, normal, State#state{mon_pid=undefined, db_monitor=closed}}.


incref(State) ->
    {ok, State#state{db_monitor = erlang:monitor(process, State#state.mon_pid)}}.


decref(State) ->
    true = erlang:demonitor(State#state.db_monitor, [flush]),
    ok.


monitored_by(#state{mon_pid = MonPid}) ->
    case erlang:process_info(MonPid, monitored_by) of
        {monitored_by, Pids} ->
            Pids;
        _ ->
            []
    end.

mon_loop() ->
    receive _ -> ok end,
    mon_loop().

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



% RocksDB handles compaction itself
start_compaction(State, _DbName, _Options, _Parent) ->
    {ok, State, self()}.

finish_compaction(_S, _D, _O, _D) ->
    ok.

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




fold_docs_int(DBHandle, CFHandle, UserFun, UserAcc, Options) ->
    Reduce = lists:member(include_reductions, Options),
    FoldFun = case lists:member(include_deleted, Options) of
        true -> UserFun;
        false when Reduce =:= true -> skip_deleted_with_reduce(UserFun);
        false when Reduce =:= false -> skip_deleted(UserFun)
    end,

    case rocksdb:snapshot(DBHandle) of
        {error, Err} -> 
                throw(Err);
        {ok, Snapshot} ->
            case rocksdb:iterator(DBHandle, CFHandle, [{snapshot, Snapshot}]) of
                {ok, Iter} ->
                    try
                        {ok, StartKey, EndKey, Direction} = get_iter_range(Options),
                        {ok, Acc} = fold_loop(rocksdb:iterator_move(Iter, StartKey), Iter, Direction, EndKey, Reduce, FoldFun, UserAcc),
                        case Reduce of
                            true -> 
                                {ok, 0, Acc};
                            _  -> 
                                {ok, Acc}
                        end
                    after
                        rocksdb:release_snapshot(Snapshot),
                        rocksdb:iterator_close(Iter)
                    end;

                {error, Err} ->
                    throw(Err)
        end
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
            Fun(binary_to_term(Value), {[],[]}, Acc0);
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
