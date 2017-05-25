.PHONY: test all

all: test

test: 
	ERL_AFLAGS="-config /Users/garren/dev/couchdb/rel/files/eunit.config" BUILDDIR=/Users/garren/dev/couchdb rebar compile eunit