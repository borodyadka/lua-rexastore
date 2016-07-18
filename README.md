# rexastore.lua

Rexastore is redis-based graph queries engine, inspired by [hexastore](http://nodejsconfit.levelgraph.io/) and written in Lua.

I'm not a Lua guru, so it have some bad code inside.

## Usage

```
redis> EVALSHA SHA1 2 key command ...args
```

### Put new item

Put new item into graph: `put SUBJECT PREDICATE OBJECT`

```
redis> EVALSHA SHA1 2 graph put Alice friend Bob
redis> EVALSHA SHA1 2 graph put Alice friend Carol
redis> EVALSHA SHA1 2 graph put Bob friend Dan
redis> EVALSHA SHA1 2 graph put Carol friend Alice
```

### Get item

Get linked items: `get SUBJECT [PREDICATE [OBJECT]]`

```
redis> EVALSHA SHA1 2 graph get Alice friend
1) 1) "Alice"
   2) "friend"
   3) "Bob"
2) 1) "Alice"
   2) "friend"
   3) "Carol"
```

### Search items

Search is very simple: `query "Carol:friend:>A" "<A:friend:>B" "<B"`.

There is special syntax for links walking, eg. `>A` and `<A`, where `A` can be any string. `>A` means "save all values to A", `<A` means "iterate over all values from A and get them".

For example — get all friends of friends of Carol:

```
redis> EVALSHA SHA1 2 graph query "Carol:friend:>A" "<A:friend:>B" "<B"
1) "Bob"
2) "Carol"
```

### Delete item

It simlar to `put`: `delete SUBJECT PREDICATE OBJECT` or `del SUBJECT PREDICATE OBJECT`.

### Clear graph store

Just `drop` it:

```
redis> EVALSHA SHA1 2 graph drop
```
