# couch_rocks

This is a RocksDB storage engine for CouchDB. It relies on https://github.com/apache/couchdb/pull/496 to function correctly.

# Setup
In CouchDB's rebar.config.script add the following in the `DepDescs` section:

```
{rocksdb,  {url, "https://gitlab.com/barrel-db/erlang-rocksdb.git/"}, "master"},
{couch_rocks,  {url, "https://github.com/garrensmith/couch_rocks"}, "initial-work"},
```

Then in the local.ini add this section:
```
[couchdb_engines]
rocks = couch_rocks
```

When creating a db do the following to use rocksdb:
`curl -X PUT http://localhost:5984/my-db?engine=rocks`

Alternatively set `default_engine = rocks` in the `[couchdb]` section for all databases to use the RocksDB adapter


# Testing
In CouchDB:
* git checkout pr/496
* make eunit

In Couch_rocks
* make test

OR 

* export ERL_AFLAGS="-config /PATH/TO/couchdb/rel/files/eunit.config"
* BUILDDIR=/PATH/to/couchdb rebar compile eunit 

This uses [Erlang Rocks](https://gitlab.com/barrel-db/erlang-rocksdb/blob/master/doc/rocksdb.md) underneath.
