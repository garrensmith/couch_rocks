couch_rocks
===

This is an alternative storage engine implementation for CouchDB. It relies on all of the 45918-pluggable-storage-engine branches to function correctly.

This storage engine has three major changes from the legacy engine. First, it uses nifile for performing all IO (rather than the builtin Erlang file module).
Secondly, it uses a system of three files (one for data, one for the three btree indexes, and one for commit data). This is to avoid requiring the `make_blocks` and related functions in `couch_file`. Thirdly it stores the same pointer to `#full_doc_info{}` records in both the `id_tree` and `seq_tree` to avoid duplicating data on disk.

Some initial micro benchmarks at the storage engine API layer suggest this is a faster implementation than the legacy engine.
