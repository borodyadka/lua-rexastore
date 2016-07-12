#!/bin/sh

SHA1=$(redis-cli SCRIPT LOAD "$(cat rexastore.lua)")
echo $SHA1
redis-cli EVALSHA $SHA1 2 test test 0
# redis-benchmark -n 10000 EVALSHA $SHA1 2 test test 0
