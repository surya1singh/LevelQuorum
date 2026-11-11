# LevelQuorum

A key-value store that keeps its data on SSD, speaks the Redis protocol, and
lets each key prefix choose between cache behaviour (evictable, fastest) and
database behaviour (never evicted, never loses an acknowledged write).

It is built in Go on RocksDB. The goal is a cluster that holds terabytes at
SSD prices; today it is a single node.

- **Design:** [docs/DESIGN.md](docs/DESIGN.md)
- **Status:** phases 1 and 2 of 5 are done (a single node). Clustering,
  replication, authentication and TLS are not implemented yet.

## Build

Requires Go 1.24 or newer, a C/C++ compiler, and RocksDB 8.9 with its
headers. On Ubuntu 24.04, which ships RocksDB 8.9.1:

```sh
sudo apt-get install -y build-essential librocksdb-dev
go build -o levelquorum ./cmd/levelquorum
```

The RocksDB version matters: the Go binding in `go.mod`
(`grocksdb v1.8.12`) is built for RocksDB 8.9.1, and that is the only
combination tested. For another RocksDB release, change the binding to the
version that matches it.

## Run

```sh
./levelquorum                                      # defaults: 127.0.0.1:6380, ./data
./levelquorum -config levelquorum.example.yaml     # with namespaces
```

Then use any Redis client:

```
$ redis-cli -p 6380
127.0.0.1:6380> SET orders:1 paid
OK
127.0.0.1:6380> SET session:abc token EX 3600
OK
127.0.0.1:6380> NAMESPACE FOR orders:1
"orders"
127.0.0.1:6380> TTL session:abc
(integer) 3600
```

There is no authentication yet. Keep the server on an address that
untrusted clients cannot reach.

## Namespaces and profiles

A namespace owns every key that starts with its prefix, and its profile
decides how that data is treated:

| Profile | Evicted when full | Written to disk before OK | Use for |
|---|---|---|---|
| `cache` | yes, oldest first, at `max_size` | no | data you can recompute |
| `safe` | never | no | data that must not be evicted |
| `strict` | never | yes (fsync) | data where losing a write is not acceptable |

```yaml
namespaces:
  - name: sessions
    prefix: "session:"
    profile: cache
    max_size: 100GB
    default_ttl: 24h
  - name: orders
    prefix: "orders:"
    profile: strict
```

A cache namespace keeps what was written most recently, plus older keys
that are still being read; it evicts the rest in steps of 1/16 of
`max_size`. Leave about 15% of disk above `max_size` free for compaction
and the write-ahead log.

Keys that match no prefix go to the `default` namespace, a 1 GB cache unless
you define it. See [levelquorum.example.yaml](levelquorum.example.yaml) for
every setting, and the design document for what the profiles will mean once
there are replicas.

A connection can raise the durability of its own writes with
`DURABILITY strict`.

## Memory

Two settings decide how much RAM the server uses for data:

- `hot_cache_size` (default 128MB): recently used values, served without
  touching the database. `0` turns it off.
- `block_cache_size` (default 256MB): RocksDB's cache of what it reads from
  disk, including its indexes and bloom filters. For large data sets make
  this 2-5% of the data size.

On top of that, each namespace buffers up to about twice
`write_buffer_size` (default 64MB) of recent writes in RAM.

## Metrics

Set `metrics_listen: "127.0.0.1:9380"` to serve Prometheus metrics at
`/metrics`: command counts, errors and latency histograms, namespace sizes,
evictions, and hot-cache hit rates. `INFO` reports the same figures to
Redis clients. The endpoint has no authentication.

## Supported commands

`GET` `SET` `SETNX` `SETEX` `PSETEX` `GETSET` `GETDEL` `MGET` `MSET` `APPEND`
`STRLEN` `INCR` `DECR` `INCRBY` `DECRBY` `DEL` `UNLINK` `EXISTS` `TYPE`
`EXPIRE` `PEXPIRE` `EXPIREAT` `PEXPIREAT` `PERSIST` `TTL` `PTTL` `EXPIRETIME`
`PEXPIRETIME` `PING` `ECHO` `QUIT` `SELECT` `HELLO` `CLIENT` `COMMAND` `INFO`
`DBSIZE`, plus `NAMESPACE LIST`, `NAMESPACE FOR <key>` and `DURABILITY`.

Differences from Redis are listed in
[the design document](docs/DESIGN.md#52-differences-from-redis). The main
ones: strings only, no `KEYS`/`SCAN`/`FLUSHALL`, and `DBSIZE` is an estimate.

## Test

```sh
go test -race ./...
```

## Layout

```
cmd/levelquorum     the server binary
internal/storage    RocksDB engine: namespaces, expiry, eviction, durability
internal/hotcache   RAM tier for recently used values
internal/resp       Redis wire protocol
internal/server     connections, commands and metrics
internal/config     configuration file
docs/DESIGN.md      design, including the cluster plan
```
