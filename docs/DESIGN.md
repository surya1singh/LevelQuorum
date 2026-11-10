# LevelQuorum design

LevelQuorum is a distributed key-value store that keeps its data on SSD
instead of in RAM, speaks the Redis protocol, and lets each group of keys
choose between cache behaviour (fast, evictable, may lose the latest writes)
and database behaviour (never evicted, never loses an acknowledged write).

**Status:** phase 1 (a single node) is implemented and tested. Everything
about clustering in this document is design, not code. Each section says
which it is.

## Contents

1. [Goals](#1-goals)
2. [Architecture](#2-architecture)
3. [Data model: namespaces and profiles](#3-data-model-namespaces-and-profiles)
4. [Storage engine](#4-storage-engine) (implemented)
5. [Client protocol](#5-client-protocol) (implemented)
6. [RAM hot tier](#6-ram-hot-tier) (phase 2)
7. [Cluster layout](#7-cluster-layout) (phase 3)
8. [Replication and durability](#8-replication-and-durability) (phase 3)
9. [Moving data between nodes](#9-moving-data-between-nodes) (phase 4)
10. [Failure scenarios](#10-failure-scenarios)
11. [Sizing](#11-sizing)
12. [Security and operations](#12-security-and-operations)
13. [Risks and open questions](#13-risks-and-open-questions)
14. [Phases](#14-phases)
15. [Alternatives considered](#15-alternatives-considered)

## 1. Goals

- **Terabytes per cluster at SSD prices.** A node holds several TB on local
  NVMe and needs RAM for only a few percent of that.
- **Cache and database in one system.** The operator decides per key prefix
  whether data may be evicted and how safe a write must be before the client
  is told OK.
- **Works with existing Redis clients.** No client library of our own.
- **Scales by adding nodes**, and survives the loss of a node.

Non-goals:

- Matching Redis on latency. Redis answers from RAM; a read here may touch
  the SSD. The target is a small multiple of Redis latency at a fraction of
  the cost per TB.
- The full Redis command set. Strings first; other data types later if
  needed.
- Transactions across keys on different nodes.
- Replication across regions.

## 2. Architecture

```
            Redis clients (any Redis Cluster client)
                          |
                          |  Redis protocol (RESP)
        +-----------------+-----------------+
        v                 v                 v
   +---------+       +---------+       +---------+
   | Node A  |       | Node B  |       | Node C  |     one Go process per node
   |         |<----->|         |<----->|         |     replication between nodes
   | RocksDB |       | RocksDB |       | RocksDB |     local NVMe
   +----+----+       +----+----+       +----+----+
        |                 |                 |
        +-----------------+-----------------+
                          |
                 +--------+--------+
                 |  etcd (3 nodes) |    who is alive, who owns which keys,
                 +-----------------+    who is primary, namespace policy
```

Inside a node:

```
   connection --> protocol (RESP) --> command handlers
                                           |
                                    storage engine
                         +-----------------+------------------+
                         |  key -> namespace (by prefix)      |
                         |  per-key locks, expiry, durability |
                         +-----------------+------------------+
                                           |
                     RocksDB: one column family per namespace
                  +---------------+---------------+---------------+
                  | ns "default"  | ns "sessions" | ns "orders"   |
                  | cache, FIFO   | cache, FIFO   | strict, level |
                  +---------------+---------------+---------------+
```

The storage engine is [RocksDB](https://rocksdb.org), reached from Go through
the `grocksdb` binding. The project started from LevelDB; RocksDB is a fork
of it that adds what TB-scale needs (see
[section 15](#15-alternatives-considered)).

## 3. Data model: namespaces and profiles

A **namespace** is a set of keys that share a prefix, such as `session:` or
`orders:`. A key belongs to the namespace with the longest prefix that
matches it. Keys that match no prefix belong to the `default` namespace,
whose prefix is empty.

Each namespace has a **profile**, which fixes how its data is treated:

| Profile | The client gets OK when | Evicted when full | A crash of the primary loses |
|---|---|---|---|
| `cache` | the primary has applied the write | yes, oldest first | the last few ms to seconds of writes |
| `safe` | every in-sync replica has the write, and those are at least a majority | never | nothing, unless a majority of replicas fail together |
| `strict` | as `safe`, and each of those replicas has fsynced it | never | nothing, even if the whole cluster loses power |

On a single node there are no replicas, so the profiles reduce to this
(implemented):

| Profile | Evicted | fsync before OK | Survives process crash | Survives power loss |
|---|---|---|---|---|
| `cache` | yes | no | yes | recent writes may be lost |
| `safe` | no | no | yes | recent writes may be lost |
| `strict` | no | yes | yes | yes |

A namespace also has:

- `max_size` (cache only, required): the on-disk size at which eviction
  starts.
- `default_ttl` (optional): the expiry given to writes that do not set one.

A connection can raise the durability of its own writes with
`DURABILITY safe|strict`. Each write uses the higher of the connection's
level and its namespace's profile, so a connection can make writes safer
than the namespace default but never weaker. Eviction is not affected: it is
a property of the namespace.

**Why the policy is per namespace and not per request.** Important data must
be exempt from eviction as well as safely written, and eviction works on
whole files, so evictable and non-evictable data have to live in separate
files. A namespace is that separation. It also means application code cannot
forget to ask for durability.

**Rules for changing namespaces** (enforced at startup):

- A namespace's prefix cannot change. Its existing keys would become
  unreachable.
- A namespace cannot move between `cache` and a durable profile. That would
  change whether existing data can be evicted. `safe` to `strict` and back
  is allowed.
- A new namespace cannot be created if keys starting with its prefix already
  exist in the namespace that owned them until now. They would silently
  disappear from view. Define namespaces before writing keys under them.
- A namespace that is removed from the configuration keeps being served,
  with a warning. Its data is still on disk.

## 4. Storage engine

*Implemented in `internal/storage`.*

### 4.1 Layout on disk

```
<data_dir>/
  NAMESPACES.json     which namespaces exist (name, prefix, profile, limits)
  db/                 one RocksDB database
```

RocksDB column families map to namespaces: namespace `orders` is column
family `ns.orders`. RocksDB's mandatory `default` column family is left
empty. All column families share one write-ahead log, so a write that
touches several namespaces (`MSET`, `DEL`) is atomic.

`NAMESPACES.json` lives beside the database, not inside it, because RocksDB
needs each column family's options before it can open the database. The
file is replaced atomically (write a temporary file, fsync, rename, fsync
the directory). It is written before a new column family is created, so
after a crash between the two steps the next start finds the entry and
creates the missing column family.

### 4.2 Value format

Every stored value has a 10-byte header in front of the payload:

| Bytes | Content |
|---|---|
| 0 | format version, currently 1 |
| 1 | value type, 0 = string; other values are reserved for future types |
| 2-9 | expiry as unix milliseconds, big endian; 0 = never |

Keys are stored exactly as the client sent them, prefix included.

### 4.3 Expiry

- **On read:** a value whose expiry has passed is reported as missing. It is
  not deleted at that point, because a delete issued by a reader could race
  with a concurrent write of a fresh value.
- **Physical removal:** a RocksDB compaction filter drops expired values
  whenever compaction rewrites them (durable namespaces). In cache
  namespaces, expired values leave when their file is evicted.

The compaction filter is a Go function called from RocksDB's background
threads. That costs a C-to-Go call per key during compaction; see
[risks](#13-risks-and-open-questions).

### 4.4 Eviction

Cache namespaces use RocksDB's **FIFO compaction**: data is written to files
in arrival order, and when the namespace's files exceed `max_size`, RocksDB
deletes the oldest files whole. Eviction therefore rewrites nothing, which
is what makes it cheap enough for a cache of many TB.

Consequences:

- Eviction is by **write age**, not by last read. A key that is read
  constantly but was written long ago is evicted like any other. The hot
  tier in [section 6](#6-ram-hot-tier) is what softens this.
- The limit is enforced per file, so a namespace can exceed `max_size` by
  about one file plus the in-memory write buffer.
- Overwriting a key does not free the old copy until its file is evicted.

Durable namespaces use leveled compaction and are never evicted. They grow
until the disk is full; see [section 12](#12-security-and-operations).

### 4.5 Concurrency

- Reads take no lock. RocksDB reads are consistent on their own.
- Every write holds a lock for its key, chosen from 1024 lock stripes by a
  hash of the key. This is what makes read-modify-write commands (`INCR`,
  `SET NX`, `EXPIRE`, `APPEND`, `GETDEL`) atomic. Plain `SET` takes the lock
  too, or it could slip between another command's read and write.
- Multi-key writes lock all their stripes in a fixed order, so they cannot
  deadlock, and apply as one RocksDB write batch.

### 4.6 RocksDB settings in phase 1

| Setting | Value | Why |
|---|---|---|
| Block cache | one, shared by all namespaces (`block_cache_size`) | caps RAM used for data read from disk |
| Bloom filters | 10 bits per key | a read for a missing key rarely touches the disk |
| Compression | LZ4 | cheap on CPU |
| Write buffer | `write_buffer_size` per namespace | bigger means fewer, larger files |
| Background threads | one per CPU core | flush and compaction |

Tuning for TB scale (partitioned indexes and filters, separate storage for
large values, rate limits on compaction) is phase 2.

## 5. Client protocol

*Implemented in `internal/resp` and `internal/server`.*

The server speaks RESP2, the Redis wire protocol, so `redis-cli` and Redis
client libraries work unchanged. It also accepts inline commands (a plain
line such as `PING`), which is convenient for health checks with `nc`.

Each connection is served by one goroutine. Replies are buffered and sent
when the client has no more commands queued, so a pipelined batch is
answered with a single network write.

### 5.1 Commands

| Group | Commands |
|---|---|
| Strings | `GET` `SET` (with `EX` `PX` `EXAT` `PXAT` `NX` `XX` `KEEPTTL` `GET`) `SETNX` `SETEX` `PSETEX` `GETSET` `GETDEL` `MGET` `MSET` `APPEND` `STRLEN` `INCR` `DECR` `INCRBY` `DECRBY` |
| Keys | `DEL` `UNLINK` `EXISTS` `TYPE` `EXPIRE` `PEXPIRE` `EXPIREAT` `PEXPIREAT` `PERSIST` `TTL` `PTTL` `EXPIRETIME` `PEXPIRETIME` |
| Connection | `PING` `ECHO` `QUIT` `SELECT` `HELLO` `AUTH` `CLIENT` `COMMAND` |
| Server | `INFO` `DBSIZE` |
| Extensions | `NAMESPACE LIST`, `NAMESPACE FOR <key>`, `DURABILITY [cache\|safe\|strict]` |

### 5.2 Differences from Redis

- **No `KEYS`, `SCAN`, `FLUSHDB` or `FLUSHALL`.** Listing or wiping TB of
  keys is not something to do by accident. `SCAN` will come with the
  cluster work, where its cursor has to be designed anyway.
- **`DBSIZE` is an estimate.** It is RocksDB's estimate and can include
  keys that were overwritten, deleted or have expired but are not yet
  compacted away.
- **One database.** `SELECT 0` works; other numbers are refused. Namespaces
  replace numbered databases.
- **RESP2 only.** `HELLO 3` is refused with `NOPROTO`, which client
  libraries handle by staying on RESP2.
- **No authentication.** `AUTH` answers as a Redis server without a
  password does. See [section 12](#12-security-and-operations).
- **`EXPIRE` options `NX` `XX` `GT` `LT`** are not supported.
- **Strings only.** No lists, hashes, sets, sorted sets, pub/sub, scripts or
  transactions.
- **`MSET` in a namespace with a default TTL** gives the keys that TTL.

## 6. RAM hot tier

*Phase 2. Design only.*

FIFO eviction ignores reads, and a cold read costs an SSD access. A hot tier
addresses both:

1. **An in-memory LRU of recently read values** in front of RocksDB, sized
   in bytes. A hit never reaches RocksDB.
2. **Re-insertion of hot keys.** When a key in a cache namespace is read and
   its copy is in the oldest part of the namespace, it is written again, so
   it moves to the newest file and is not evicted while it stays in use.
   This approximates LRU at the cost of one extra write per hot key per
   eviction cycle. Meta's CacheLib uses the same idea for its flash cache.

Open point: re-insertion needs to know how old a key's copy is. The plan is
to keep the write time in the value header (the format version byte allows
this change).

## 7. Cluster layout

*Phase 3. Design only.*

### 7.1 Hash slots

Keys are spread over **16,384 hash slots** exactly as in Redis Cluster:
`slot = CRC16(key) mod 16384`, with `{hash tags}` to force related keys into
one slot. Nodes answer `CLUSTER SLOTS` / `CLUSTER SHARDS` and redirect with
`MOVED` and `ASK`.

This is the reason to copy Redis Cluster instead of using a custom hash
ring: every existing cluster-aware Redis client then routes requests to the
right node without a proxy and without a client library of our own.

### 7.2 Shards

Slots are grouped into **shards**. A shard is a contiguous range of slots
with one primary and N-1 replicas (N is the replication factor, 3 by
default), each on a different node. A node holds replicas of several shards.

Each shard replica is **its own RocksDB database** on the node:

- its own write-ahead log, which is the shard's replication log
  ([section 8](#8-replication-and-durability));
- it can be copied, checkpointed or deleted without touching other shards.

All shard databases on a node share one block cache and one pool of
background threads.

Namespaces stay column families inside each shard database. Slots and
namespaces are independent: the slot decides *which shard* holds a key, the
namespace decides *how* it is stored there.

### 7.3 Key format change

To move a slot between shards as a unit, a slot's keys must be contiguous on
disk. In cluster mode the stored key becomes `<2-byte slot><key>`. Phase 1
stores the bare key, so **phase-1 data directories will need a one-off
conversion** when cluster mode arrives. The manifest's version field exists
to detect this.

### 7.4 Cluster metadata in etcd

| Key | Content |
|---|---|
| `/nodes/<id>` | address; kept alive by a lease, so it disappears when the node dies |
| `/shards/<id>/config` | slot range, replica nodes, replication factor |
| `/shards/<id>/leader` | current primary and its **epoch**; held by a lease |
| `/shards/<id>/isr` | the in-sync replica set |
| `/namespaces/<name>` | prefix, profile, limits |

Nodes watch these keys and cache them. etcd is consulted for changes, never
on the path of a client request.

etcd is used instead of ZooKeeper (the original plan) because it is written
in Go, has a first-class Go client, and its leases and compare-and-swap
transactions are exactly the two primitives this design needs.

## 8. Replication and durability

*Phase 3. Design only.*

### 8.1 One log, three acknowledgement levels

There is one replication mechanism for all profiles. The profile only
decides how long the client waits.

1. The primary applies a write to its RocksDB. That appends it to the
   shard's write-ahead log and gives it a **sequence number**.
2. Replicas stream the log from the primary (`GetUpdatesSince(sequence)`)
   and apply the same write batches in the same order.
3. Each replica reports the sequence number it has applied, and separately
   the one it has fsynced.
4. The primary answers the client:
   - `cache`: immediately after step 1;
   - `safe`: when every in-sync replica has applied the write;
   - `strict`: when every in-sync replica has fsynced it.

Because the log is ordered, a `safe` write being acknowledged implies that
every earlier write, of any profile, has reached those replicas too.

### 8.2 In-sync replicas

The **in-sync replica set (ISR)** of a shard is the primary plus the
replicas that are caught up to within a lag limit. The primary maintains it
and stores every change in etcd with a compare-and-swap that includes its
epoch. A replica that falls behind is removed from the ISR; when it catches
up it is added again.

`min_insync` (default: a majority, 2 of 3) is the smallest ISR with which
durable writes are accepted. If the ISR is smaller:

- `safe` and `strict` writes are **refused with an error**. They are never
  silently downgraded.
- `cache` writes continue.

This is the model Apache Kafka uses (`acks=all` with
`min.insync.replicas`).

### 8.3 Failover

- The primary holds `/shards/<id>/leader` through an etcd lease that it
  renews every few seconds.
- If the lease expires, replicas compete for the key with an etcd
  transaction: *if the key is absent and I am in the ISR, write me as
  leader with epoch + 1*.
- **Only an ISR member can win.** Every ISR member has every acknowledged
  `safe` and `strict` write, so none is lost.
- **Fencing:** every replication message carries the sender's epoch, and
  replicas reject a lower one. A primary that cannot renew its lease stops
  acknowledging durable writes before the lease runs out (with a margin for
  clock drift), so two primaries can never both acknowledge a durable
  write.

If every ISR member is lost, the shard stops accepting durable writes and
waits. Promoting a replica that was not in sync loses acknowledged data, so
it is a manual operator action.

### 8.4 A returning old primary

An old primary may hold writes that were never replicated: `cache` writes it
acknowledged, and durable writes it never acknowledged. When it returns as a
replica these must go.

Each replica keeps the history of epochs it has seen with the sequence
number at which each began. On rejoining, it asks the new primary where its
last epoch ended. If the replica has writes beyond that point, it has
diverged.

RocksDB cannot undo applied writes, so in the first version a diverged
replica is **wiped and re-seeded** from a checkpoint of the new primary.
That is correct but slow for a large shard; see
[risks](#13-risks-and-open-questions).

### 8.5 Reads

| Read from | Guarantee | Default for |
|---|---|---|
| Primary | sees every acknowledged write | `safe`, `strict` |
| Replica | may be slightly stale | `cache` (opt-in per connection with `READONLY`, as in Redis Cluster) |

One subtlety: the primary applies a write locally before replicas have it,
so a read on the primary could see a durable write that is later lost in a
failover. To prevent that, a read in a durable namespace waits until the
latest write to its key is acknowledged. The per-key lock stripes of
[section 4.5](#45-concurrency) will record the sequence number of their
latest write for this purpose.

## 9. Moving data between nodes

*Phase 4. Design only.*

Slots move when nodes are added or removed, or to even out load. Copying
key by key is too slow at this scale, so a slot moves as files:

1. The source takes a RocksDB checkpoint of the shard (a set of hard
   links, near-instant).
2. It exports the slot's key range from the checkpoint as SST files
   (`SstFileWriter`) and streams them to the target.
3. The target ingests them (`IngestExternalFile`), which adds them to its
   database without rewriting them.
4. The target replays the writes the slot received since the checkpoint,
   from the source's log.
5. When the target is nearly caught up, the source briefly stops writes to
   the slot, sends the remainder, and ownership is switched in etcd.
   Clients are redirected with `ASK` during the move and `MOVED` after.
6. The source deletes the slot's range (`DeleteRange`).

At about 1 GB/s of network and disk throughput, 1 TB moves in roughly 17
minutes. This is an estimate, not a measurement.

Replicas are added or re-seeded the same way, with the whole shard instead
of one slot.

## 10. Failure scenarios

How the cluster design is meant to behave. None of this is implemented or
tested yet.

| Failure | `cache` namespaces | `safe` / `strict` namespaces |
|---|---|---|
| A replica dies | unaffected | unaffected while the ISR is at least `min_insync`; the replica is dropped from the ISR |
| The primary dies | a replica takes over within the lease time; the last unreplicated writes are lost | a replica from the ISR takes over; no acknowledged write is lost |
| The primary is cut off from etcd but still reachable by clients | keeps accepting writes until it notices; those are discarded when it rejoins | stops acknowledging before its lease expires; clients get errors and retry on the new primary |
| Fewer than `min_insync` replicas in sync | writes continue | writes are refused with an error; reads continue |
| All ISR members lost | a remaining replica can be promoted | unavailable until an ISR member returns or an operator forces a promotion and accepts the loss |
| etcd unavailable | serving continues with the cached cluster state | serving continues; no failover or ISR change is possible, so a second failure in that window stops durable writes |
| Whole cluster loses power | recent writes may be lost | `safe`: recent writes may be lost; `strict`: nothing acknowledged is lost |
| A disk fills up | eviction keeps the namespace at `max_size` | writes fail; see section 12 |

## 11. Sizing

These are estimates from RocksDB's known behaviour and public hardware
figures. **Nothing here has been measured on this system.**

**RAM.** With disk-backed storage, RAM is consumed by metadata, not data.
For 1 TB of 1 KB values (about a billion keys): bloom filters at 10 bits per
key take about 1.2 GB, and block indexes a few hundred MB more. Smaller
values mean more keys per TB and proportionally more RAM. Plan for RAM of
2-5% of the data size, plus the block cache and hot tier.

**Per node (production):**

| Resource | Recommendation |
|---|---|
| Disk | local NVMe, 2-8 TB. Network-attached disks add about a millisecond per cold read. |
| RAM | 2-5% of the data size; for example 64 GB for 2 TB |
| CPU | 8-16 cores |
| Network | 10-25 Gbit/s |

**Example:** 10 TB of data with 3 replicas is 30 TB of disk, or about 8-10
nodes with 3.75 TB of NVMe each.

**Latency expectation:** a hot read should be comparable to Redis (a network
round trip dominates). A cold read adds one NVMe read, roughly 0.1 ms. Tail
latency is where SSD-backed stores lose to RAM-backed ones, mainly from
compaction competing for the disk.

## 12. Security and operations

**Not implemented yet, and required before any real deployment:**

- **Authentication and TLS.** There is neither. The default listen address
  is `127.0.0.1` for that reason. Do not expose the port to a network you
  do not control.
- **Disk-full protection for durable namespaces.** They grow without limit.
  The plan is a per-namespace quota that refuses writes with a clear error
  before RocksDB runs out of space.
- **Metrics.** `INFO` reports per-namespace sizes today. Latency histograms
  and RocksDB internals (compaction backlog, stalls, cache hit rate) should
  be exported for Prometheus.
- **Connection limits and idle timeouts.**
- **Backups** of durable namespaces, using RocksDB checkpoints.

## 13. Risks and open questions

Ordered by how much they threaten the goal.

1. **Read cost of FIFO compaction at TB scale.** With FIFO, every file of a
   namespace can contain any key, so a read checks the bloom filter of each
   file, newest first, until it finds the key. At the default 64 MB write
   buffer, 1 TB is on the order of 16,000 files, and a read for a missing
   key probes all of them. My estimate is around a millisecond of CPU per
   miss at that size, which is too slow; it is not measured. Options, to be
   evaluated in phase 2:
   - much larger files (1 GB write buffers cut the file count sixteenfold);
   - letting RocksDB merge small files in the background
     (`allow_compaction`), at the cost of some rewriting;
   - replacing FIFO with **time buckets**: one leveled column family per
     time window, where eviction drops the oldest window whole. A read then
     checks a handful of windows instead of thousands of files.

   The profile and namespace model does not depend on which is chosen.

2. **Re-seeding a diverged replica is expensive** (section 8.4): about 17
   minutes per TB at best, during which the shard has one replica fewer.
   Since divergence is normal for `cache` writes after any failover, this
   needs a cheaper path. Candidate: keep enough of the old values in the log
   to undo the unreplicated tail.

3. **The expiry filter crosses from C++ into Go for every key compacted.**
   Fine at phase-1 scale; at TB scale it may limit compaction throughput.
   The fallback is to write the same ten-line filter in C++.

4. **Large values.** Compaction rewrites values along with keys. For values
   of tens of KB and up, RocksDB's BlobDB (values stored apart from keys)
   should be enabled. Needs the workload's value sizes to decide.

5. **Phase-1 data needs conversion for cluster mode** (section 7.3).

6. **Go garbage collection** adds a small, usually sub-millisecond, tail to
   latency. Values pass through Go memory on the way to the socket; if this
   shows up, the read path can hand RocksDB's buffer to the socket directly.

7. **Hot keys** concentrate load on one shard. Replica reads help for
   `cache`; for durable data there is no remedy yet.

8. **Should a cluster with only `cache` namespaces promote out-of-sync
   replicas automatically?** It has no durable data to lose, so probably
   yes. Undecided.

Workload questions that would sharpen the design: typical value size, read
to write ratio, target requests per second, and whether a cache must survive
a restart.

## 14. Phases

| Phase | Content | Status |
|---|---|---|
| 1 | Single node: RocksDB storage, Redis protocol, expiry, FIFO eviction, namespaces with profiles | **done** |
| 2 | RAM hot tier; RocksDB tuning for TB scale; resolve risk 1; metrics | next |
| 3 | Cluster: hash slots, etcd membership, replication log, the three acknowledgement levels, failover | |
| 4 | Moving slots and seeding replicas by streaming files | |
| 5 | Hardening: fault-injection tests of the durability guarantees, authentication, TLS, quotas | |

## 15. Alternatives considered

| Decision | Chosen | Instead of | Why |
|---|---|---|---|
| Storage engine | RocksDB | LevelDB | LevelDB has no FIFO eviction, no column families, single-threaded compaction, and no file ingestion. All four are needed here. |
| Language | Go | Python | Python cannot keep up with NVMe. Rust would avoid the C-to-Go call cost, but Go was preferred for development speed. |
| Coordination | etcd | ZooKeeper | Native Go client; leases and transactions fit the failover design directly. |
| Replication | one log with configurable acknowledgement | async replication for cache plus a Raft group per shard for durable data | One mechanism to build and test instead of two. Durability becomes a setting, which was the requirement. Raft would give automatic handling of divergence at the cost of running many Raft groups per node. |
| Sharding | Redis Cluster hash slots | a custom consistent-hash ring | Existing Redis clients already know how to route by slot. |
| Durability setting | per namespace, raised per connection | per request | Durable data must also be exempt from eviction, which needs separate files. |
| Eviction | by write age (FIFO) | LRU | Tracking reads on disk turns every read into a write. The hot tier approximates LRU where it matters. |

**Existing systems in this space:** Apache Kvrocks (Redis protocol on
RocksDB), Pika, Aerospike, and Meta's CacheLib. Kvrocks is the closest.
What this design adds is cache and durable data side by side with a
per-namespace choice of eviction and acknowledgement level.
