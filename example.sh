#!/bin/sh

SHA1=$(redis-cli SCRIPT LOAD "$(cat rexastore.lua)")

redis-cli EVALSHA $SHA1 2 mygraph put Alice friend Bob > /dev/null
redis-cli EVALSHA $SHA1 2 mygraph put Alice friend Carol > /dev/null
redis-cli EVALSHA $SHA1 2 mygraph put Bob friend Dan > /dev/null
redis-cli EVALSHA $SHA1 2 mygraph put Carol friend Alice > /dev/null

echo "Friends of Alice:"
redis-cli EVALSHA $SHA1 2 mygraph get Alice friend

echo "Friends of a friend of Carol"
redis-cli EVALSHA $SHA1 2 mygraph query "Carol:friend:>A" "<A:friend:>B" "<B"

redis-cli EVALSHA $SHA1 2 mygraph drop > /dev/null
