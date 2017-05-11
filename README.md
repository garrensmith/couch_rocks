# couch_rocks

This is an alternative storage engine implementation for CouchDB. It relies on all of the 45918-pluggable-storage-engine branches to function correctly.


# Testing
In CouchDB:
* git checkout pr/496
* Comment out tests that are not needed yet
* make eunit

In Couch_rocks
* export ERL_AFLAGS="-config /Users/garren/dev/couchdb/rel/files/eunit.config"
* BUILDDIR=/Users/garren/dev/couchdb rebar compile eunit 

[Erlang Rocks Docs](https://gitlab.com/barrel-db/erlang-rocksdb/blob/master/doc/rocksdb.md)

# Eunit
Use ?DebugFmt to log to console. Make sure `couch-unit` is included