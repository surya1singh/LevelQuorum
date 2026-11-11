// Package storage is the single-node storage engine: RocksDB with per-key
// expiry, a RAM tier for hot values, never-evicted durable namespaces, and
// size-bounded cache namespaces that evict their oldest data.
package storage

import (
	"bytes"
	"errors"
	"fmt"
	"hash/maphash"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/linxGnu/grocksdb"

	"github.com/surya1singh/LevelQuorum/internal/hotcache"
)

const (
	// metaCF is RocksDB's mandatory "default" column family. No user data is
	// stored in it.
	metaCF = "default"
	// cfPrefix starts the name of every column family that holds a
	// namespace's data. See bucketCFName.
	cfPrefix = "ns."

	lockStripes = 1024
)

// Options configures Open.
type Options struct {
	// Dir holds the database and the namespace manifest.
	Dir string
	// Namespaces to serve. A "default" cache namespace is added when none is
	// given.
	Namespaces []NamespaceSpec
	// BlockCacheSize is the RAM, in bytes, for RocksDB's cache of data,
	// index and filter blocks, shared by all namespaces. Default 256 MiB.
	BlockCacheSize uint64
	// WriteBufferSize is the memtable size, in bytes, per namespace.
	// Default 64 MiB.
	WriteBufferSize uint64
	// HotCacheSize is the RAM, in bytes, for the cache of recently used
	// values that answers reads without touching RocksDB. Zero disables it.
	HotCacheSize uint64
	// MaintenanceInterval is how often cache namespaces are checked for
	// rotation and eviction. Default one second. A negative value disables
	// the periodic check; it exists for tests, which drive it by hand.
	MaintenanceInterval time.Duration
	// Now is the clock used for expiry. Default time.Now.
	Now func() time.Time
	// Logger receives warnings. Default slog.Default().
	Logger *slog.Logger
}

// bucket is one column family holding part of a namespace's data.
type bucket struct {
	// seq orders the buckets of a namespace; a higher seq is newer.
	seq uint64
	cf  *grocksdb.ColumnFamilyHandle

	// settled means the bucket is closed and fully compacted, so there is
	// nothing left to do to it. Used by the maintenance goroutine only.
	settled bool

	// The remaining fields coordinate background compaction with dropping
	// the bucket. Guarded by Engine.compactMu.
	queued  bool
	dropped bool
	done    chan struct{} // closed when the running compaction returns
}

type namespace struct {
	spec   NamespaceSpec
	prefix []byte
	opts   *grocksdb.Options

	// buckets is ordered newest first; buckets[0] receives all writes. A
	// durable namespace has exactly one. Guarded by Engine.topo.
	buckets []*bucket

	// promoteBelow marks the promotion zone of a cache namespace: a key
	// whose newest copy is in a bucket with a lower seq is rewritten into
	// the newest bucket when it is read. Zero means no zone.
	promoteBelow atomic.Uint64

	evictedBuckets atomic.Uint64
	promotions     atomic.Uint64

	// written counts bytes written to a cache namespace since maintenance
	// was last woken for it. See Engine.noteWrite.
	written atomic.Uint64
}

// Engine is safe for concurrent use.
type Engine struct {
	db         *grocksdb.DB
	metaHandle *grocksdb.ColumnFamilyHandle
	// namespaces is ordered longest prefix first; the default namespace
	// (empty prefix) is therefore last and matches every key. The slice is
	// fixed after Open.
	namespaces []*namespace

	// topo guards every namespace's bucket list and keeps the column family
	// handles in it alive. Every operation holds it for reading while it
	// uses a handle; only maintenance takes it for writing, briefly, to add
	// or remove a bucket. No goroutine acquires it twice.
	topo sync.RWMutex

	// Every write to a key holds that key's stripe exclusively, so
	// read-modify-write commands (INCR, SET NX, EXPIRE, ...) are atomic. A
	// read that misses the hot tier holds it shared while it reads RocksDB
	// and fills the hot tier, so it cannot insert a value that a concurrent
	// write has just replaced. Lock order: topo, then stripes.
	seed  maphash.Seed
	locks [lockStripes]sync.RWMutex

	// hot is nil when the hot tier is disabled.
	hot *hotcache.Cache

	blockCache *grocksdb.Cache
	options    []*grocksdb.Options
	ro         *grocksdb.ReadOptions
	wo         *grocksdb.WriteOptions
	woSync     *grocksdb.WriteOptions
	flushOpts  *grocksdb.FlushOptions

	writeBufferSize uint64
	nowFn           func() time.Time
	log             *slog.Logger

	// Background work; see buckets.go.
	maintMu       sync.Mutex
	kick          chan struct{}
	stop          chan struct{}
	bg            sync.WaitGroup
	bgJobs        atomic.Int64
	compactMu     sync.Mutex
	compactingNow *bucket
	compactQ      chan *bucket
	promoteQ      chan promoteReq
}

// bucketCFName is the column family of a namespace's bucket. A durable
// namespace has the single bucket 0, named "ns.<name>". A cache namespace's
// buckets are "ns.<name>#<seq>" with seq from 1; a bucket 0 can exist there
// too, left over from the version that kept a cache namespace in a single
// column family, and is treated as its oldest bucket.
func bucketCFName(ns string, seq uint64) string {
	if seq == 0 {
		return cfPrefix + ns
	}
	return cfPrefix + ns + "#" + strconv.FormatUint(seq, 10)
}

func parseBucketCFName(cf string) (ns string, seq uint64, ok bool) {
	rest, ok := strings.CutPrefix(cf, cfPrefix)
	if !ok {
		return "", 0, false
	}
	name, num, numbered := strings.Cut(rest, "#")
	if !numbered {
		return name, 0, true
	}
	seq, err := strconv.ParseUint(num, 10, 64)
	if err != nil || seq == 0 {
		return "", 0, false
	}
	return name, seq, true
}

// Open opens the database in o.Dir, creating it and any new namespaces.
func Open(o Options) (*Engine, error) {
	if o.Dir == "" {
		return nil, errors.New("storage: Dir is required")
	}
	if o.BlockCacheSize == 0 {
		o.BlockCacheSize = 256 << 20
	}
	if o.WriteBufferSize == 0 {
		o.WriteBufferSize = 64 << 20
	}
	if o.MaintenanceInterval == 0 {
		o.MaintenanceInterval = time.Second
	}
	if o.Now == nil {
		o.Now = time.Now
	}
	if o.Logger == nil {
		o.Logger = slog.Default()
	}
	if err := os.MkdirAll(o.Dir, 0o755); err != nil {
		return nil, err
	}

	stored, err := loadManifest(o.Dir)
	if err != nil {
		return nil, err
	}
	merged, created, err := reconcile(stored, o.Namespaces)
	if err != nil {
		return nil, err
	}
	configured := make(map[string]bool, len(o.Namespaces))
	for _, s := range o.Namespaces {
		configured[s.Name] = true
	}
	for _, s := range stored {
		if !configured[s.Name] && s.Name != DefaultNamespace {
			o.Logger.Warn("namespace exists on disk but is missing from the configuration; still serving it",
				"namespace", s.Name, "prefix", s.Prefix, "profile", s.Profile.String())
		}
	}

	e := &Engine{
		blockCache:      grocksdb.NewLRUCache(o.BlockCacheSize),
		ro:              grocksdb.NewDefaultReadOptions(),
		wo:              grocksdb.NewDefaultWriteOptions(),
		woSync:          grocksdb.NewDefaultWriteOptions(),
		flushOpts:       grocksdb.NewDefaultFlushOptions(),
		writeBufferSize: o.WriteBufferSize,
		nowFn:           o.Now,
		log:             o.Logger,
		seed:            maphash.MakeSeed(),
		kick:            make(chan struct{}, 1),
		compactQ:        make(chan *bucket, 256),
		promoteQ:        make(chan promoteReq, 1024),
	}
	e.woSync.SetSync(true)
	if o.HotCacheSize > 0 {
		e.hot = hotcache.New(o.HotCacheSize)
	}
	ok := false
	defer func() {
		if !ok {
			e.Close()
		}
	}()

	byName := make(map[string]*namespace, len(merged))
	for _, s := range merged {
		ns := &namespace{spec: s, prefix: []byte(s.Prefix), opts: e.cfOptions(s)}
		byName[s.Name] = ns
		e.namespaces = append(e.namespaces, ns)
	}
	sortByPrefixLen(e.namespaces, func(n *namespace) string { return n.spec.Prefix })

	dbPath := filepath.Join(o.Dir, "db")
	dbOpts := grocksdb.NewDefaultOptions()
	e.options = append(e.options, dbOpts)
	dbOpts.SetCreateIfMissing(true)
	dbOpts.IncreaseParallelism(runtime.NumCPU())
	// Write files to disk steadily instead of in bursts when they close.
	dbOpts.SetBytesPerSync(1 << 20)
	// A write-ahead log file can only be deleted once every column family
	// has flushed the data it holds there, so a namespace that is written
	// rarely would keep every later log file alive. Past this total size
	// RocksDB flushes whichever column families hold the oldest file. The
	// default limit grows with the number of column families, which is far
	// too generous with one per cache bucket.
	dbOpts.SetMaxTotalWalSize(4 * o.WriteBufferSize)

	// RocksDB requires every existing column family to be opened, each with
	// its own options.
	existing := []string{metaCF}
	if _, err := os.Stat(filepath.Join(dbPath, "CURRENT")); err == nil {
		existing, err = grocksdb.ListColumnFamilies(dbOpts, dbPath)
		if err != nil {
			return nil, fmt.Errorf("storage: list column families: %w", err)
		}
	}
	type owner struct {
		ns  *namespace
		seq uint64
	}
	owners := make([]owner, len(existing))
	cfOpts := make([]*grocksdb.Options, len(existing))
	for i, name := range existing {
		if name == metaCF {
			cfOpts[i] = dbOpts
			continue
		}
		nsName, seq, valid := parseBucketCFName(name)
		ns := byName[nsName]
		if !valid || ns == nil {
			return nil, fmt.Errorf("storage: column family %q exists in the database but is not recorded in %s", name, manifestFile)
		}
		if ns.spec.Profile.Durable() && seq != 0 {
			return nil, fmt.Errorf("storage: column family %q is a cache bucket, but namespace %q is %s", name, nsName, ns.spec.Profile)
		}
		owners[i] = owner{ns, seq}
		cfOpts[i] = ns.opts
	}

	db, handles, err := grocksdb.OpenDbColumnFamilies(dbOpts, dbPath, existing, cfOpts)
	if err != nil {
		return nil, fmt.Errorf("storage: open %s: %w", dbPath, err)
	}
	e.db = db
	for i, name := range existing {
		if name == metaCF {
			e.metaHandle = handles[i]
			continue
		}
		ns := owners[i].ns
		ns.buckets = append(ns.buckets, &bucket{seq: owners[i].seq, cf: handles[i]})
	}
	for _, ns := range e.namespaces {
		sort.Slice(ns.buckets, func(i, j int) bool { return ns.buckets[i].seq > ns.buckets[j].seq })
	}

	// A new namespace takes over every key that starts with its prefix. If
	// such keys already exist in the namespace that owned them until now,
	// they would silently disappear from view, so refuse.
	for _, name := range created {
		ns := byName[name]
		if len(ns.buckets) > 0 || ns.spec.Prefix == "" {
			continue
		}
		var prev *namespace
		for _, cand := range e.namespaces {
			if cand != ns && len(cand.buckets) > 0 && bytes.HasPrefix(ns.prefix, cand.prefix) {
				prev = cand
				break
			}
		}
		if prev == nil {
			continue
		}
		found, err := e.hasLiveKeyWithPrefix(prev, ns.prefix)
		if err != nil {
			return nil, err
		}
		if found {
			return nil, fmt.Errorf("storage: cannot create namespace %q: namespace %q already holds keys starting with %q, and they would become unreachable", ns.spec.Name, prev.spec.Name, ns.spec.Prefix)
		}
	}

	// Record intent before creating column families: after a crash in
	// between, the next Open finds the manifest entry and creates the
	// missing column family.
	if err := saveManifest(o.Dir, merged); err != nil {
		return nil, fmt.Errorf("storage: write %s: %w", manifestFile, err)
	}
	for _, ns := range e.namespaces {
		// A cache namespace that only has its old single column family gets
		// a first real bucket, so that the old one is closed and can be
		// evicted whole.
		if len(ns.buckets) > 0 && !(ns.spec.Profile == Cache && ns.buckets[0].seq == 0) {
			continue
		}
		var seq uint64
		if ns.spec.Profile == Cache {
			seq = 1
		}
		cf, err := db.CreateColumnFamily(ns.opts, bucketCFName(ns.spec.Name, seq))
		if err != nil {
			return nil, fmt.Errorf("storage: create namespace %q: %w", ns.spec.Name, err)
		}
		ns.buckets = append([]*bucket{{seq: seq, cf: cf}}, ns.buckets...)
	}

	e.stop = make(chan struct{})
	e.startBackground(o.MaintenanceInterval)
	ok = true
	return e, nil
}

// cfOptions builds the RocksDB options shared by a namespace's column
// families.
func (e *Engine) cfOptions(spec NamespaceSpec) *grocksdb.Options {
	opts := grocksdb.NewDefaultOptions()
	e.options = append(e.options, opts)

	opts.SetWriteBufferSize(e.writeBufferSize)
	opts.SetCompression(grocksdb.LZ4Compression)

	table := grocksdb.NewDefaultBlockBasedTableOptions()
	table.SetBlockCache(e.blockCache)
	table.SetFilterPolicy(grocksdb.NewBloomFilter(10))
	// Split each file's index and bloom filter into small blocks that live
	// in the block cache like data does. RAM used for them is then bounded
	// by the block cache instead of growing with the amount of data, which
	// is what lets a node hold far more data than it has RAM.
	table.SetIndexType(grocksdb.KTwoLevelIndexSearchIndexType)
	table.SetPartitionFilters(true)
	table.SetMetadataBlockSize(4096)
	table.SetCacheIndexAndFilterBlocks(true)
	table.SetCacheIndexAndFilterBlocksWithHighPriority(true)
	table.SetPinTopLevelIndexAndFilter(true)
	table.SetPinL0FilterAndIndexBlocksInCache(true)
	opts.SetBlockBasedTableFactory(table)

	if spec.Profile == Cache {
		// Each bucket is written for a while and then closed. Universal
		// compaction rewrites data less often than leveled compaction,
		// which suits data that will be dropped whole.
		//
		// There is deliberately no expiry filter: removing an expired value
		// from one bucket would let an older copy of the key in an older
		// bucket show through. Expired values leave with their bucket.
		opts.SetCompactionStyle(grocksdb.UniversalCompactionStyle)
	} else {
		opts.SetLevelCompactionDynamicLevelBytes(true)
		// Drops expired keys whenever a compaction rewrites them.
		opts.SetCompactionFilter(&ttlFilter{now: e.now})
	}
	return opts
}

// Close stops background work, flushes the write-ahead log to disk and
// releases the database. No other method may be in flight or called
// afterwards.
func (e *Engine) Close() error {
	var err error
	if e.stop != nil {
		close(e.stop)
		// Abort a compaction of a closed bucket if one is running; it
		// resumes after the next Open.
		e.db.DisableManualCompaction()
		e.bg.Wait()
		e.stop = nil
	}
	if e.db != nil {
		err = e.db.FlushWAL(true)
		for _, ns := range e.namespaces {
			for _, b := range ns.buckets {
				b.cf.Destroy()
			}
		}
		if e.metaHandle != nil {
			e.metaHandle.Destroy()
		}
		e.db.Close()
		e.db = nil
	}
	for _, o := range e.options {
		o.Destroy()
	}
	e.options = nil
	e.ro.Destroy()
	e.wo.Destroy()
	e.woSync.Destroy()
	e.flushOpts.Destroy()
	e.blockCache.Destroy()
	return err
}

// NowMillis is the engine's clock as unix milliseconds. Expiry times passed
// to the engine are compared against it.
func (e *Engine) NowMillis() int64 { return e.now() }

func (e *Engine) now() int64 { return e.nowFn().UnixMilli() }

// route returns the namespace that owns key: the one with the longest
// matching prefix.
func (e *Engine) route(key []byte) *namespace {
	for _, ns := range e.namespaces {
		if bytes.HasPrefix(key, ns.prefix) {
			return ns
		}
	}
	return nil
}

func (e *Engine) namespaceByName(name string) *namespace {
	for _, ns := range e.namespaces {
		if ns.spec.Name == name {
			return ns
		}
	}
	return nil
}

// NamespaceOf returns the spec of the namespace that owns key.
func (e *Engine) NamespaceOf(key []byte) NamespaceSpec {
	return e.route(key).spec
}

func (e *Engine) stripe(key []byte) *sync.RWMutex {
	return &e.locks[maphash.Bytes(e.seed, key)%lockStripes]
}

// lockKeys locks the stripes of all keys for writing, in a fixed order, and
// returns the function that unlocks them.
func (e *Engine) lockKeys(keys ...[]byte) (unlock func()) {
	if len(keys) == 1 {
		mu := e.stripe(keys[0])
		mu.Lock()
		return mu.Unlock
	}
	idx := make([]int, 0, len(keys))
	for _, k := range keys {
		idx = append(idx, int(maphash.Bytes(e.seed, k)%lockStripes))
	}
	sort.Ints(idx)
	uniq := idx[:0]
	for i, s := range idx {
		if i == 0 || s != idx[i-1] {
			uniq = append(uniq, s)
		}
	}
	for _, s := range uniq {
		e.locks[s].Lock()
	}
	return func() {
		for _, s := range uniq {
			e.locks[s].Unlock()
		}
	}
}

// writeOpts picks fsync-or-not for a write: the namespace's profile, raised
// to floor if the connection asked for more.
func (e *Engine) writeOpts(ns *namespace, floor Profile) *grocksdb.WriteOptions {
	if max(ns.spec.Profile, floor) == Strict {
		return e.woSync
	}
	return e.wo
}

// hasLiveKeyWithPrefix reports whether ns holds a live key starting with
// prefix. It is only used during Open, before any other goroutine exists.
func (e *Engine) hasLiveKeyWithPrefix(ns *namespace, prefix []byte) (bool, error) {
	now := e.now()
	for _, b := range ns.buckets {
		it := e.db.NewIteratorCF(e.ro, b.cf)
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			d, err := decodeValue(it.Value().Data())
			if err == nil && !d.live(now) {
				continue
			}
			// A live copy in this bucket may still be superseded by a
			// tombstone or an expired copy in a newer one.
			ent, _, err := e.lookup(ns, bytes.Clone(it.Key().Data()), false)
			if err != nil || ent != nil {
				it.Close()
				return err == nil, err
			}
		}
		err := it.Err()
		it.Close()
		if err != nil {
			return false, err
		}
	}
	return false, nil
}

// NamespaceStats describes one namespace and its current size.
type NamespaceStats struct {
	Spec NamespaceSpec
	// EstimatedKeys is RocksDB's estimate; it can count overwritten, deleted
	// and expired keys that have not been removed yet, and in a cache
	// namespace a key once per bucket that holds a copy of it.
	EstimatedKeys uint64
	// DiskBytes is the size of the namespace's data files.
	DiskBytes uint64
	// MemtableBytes is the size of data not yet flushed to files.
	MemtableBytes uint64
	// Buckets is the number of column families holding the namespace: one
	// for a durable namespace, several for a cache namespace.
	Buckets int
	// EvictedBuckets counts buckets dropped to stay within MaxSize since
	// the engine was opened.
	EvictedBuckets uint64
	// Promotions counts keys rewritten into the newest bucket because they
	// were read shortly before they would have been evicted.
	Promotions uint64
}

// Stats returns every namespace, ordered by name.
func (e *Engine) Stats() []NamespaceStats {
	e.topo.RLock()
	defer e.topo.RUnlock()
	out := make([]NamespaceStats, 0, len(e.namespaces))
	for _, ns := range e.namespaces {
		st := NamespaceStats{
			Spec:           ns.spec,
			Buckets:        len(ns.buckets),
			EvictedBuckets: ns.evictedBuckets.Load(),
			Promotions:     ns.promotions.Load(),
		}
		for _, b := range ns.buckets {
			st.EstimatedKeys += e.intProperty(b, "rocksdb.estimate-num-keys")
			st.DiskBytes += e.intProperty(b, "rocksdb.live-sst-files-size")
			st.MemtableBytes += e.intProperty(b, "rocksdb.cur-size-all-mem-tables")
		}
		out = append(out, st)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Spec.Name < out[j].Spec.Name })
	return out
}

func (e *Engine) intProperty(b *bucket, name string) uint64 {
	v, _ := e.db.GetIntPropertyCF(name, b.cf)
	return v
}

// HotStats returns the hot tier's counters. ok is false when the hot tier is
// disabled.
func (e *Engine) HotStats() (st hotcache.Stats, ok bool) {
	if e.hot == nil {
		return st, false
	}
	return e.hot.Stats(), true
}

// BlockCacheUsage returns how many bytes RocksDB's block cache currently
// holds. Its capacity is Options.BlockCacheSize.
func (e *Engine) BlockCacheUsage() uint64 { return e.blockCache.GetUsage() }

// Flush writes a namespace's in-memory data to files and waits for it.
func (e *Engine) Flush(name string) error {
	ns := e.namespaceByName(name)
	if ns == nil {
		return fmt.Errorf("storage: no namespace %q", name)
	}
	e.topo.RLock()
	defer e.topo.RUnlock()
	for _, b := range ns.buckets {
		if err := e.db.FlushCF(b.cf, e.flushOpts); err != nil {
			return err
		}
	}
	return nil
}

// ttlFilter removes expired keys from a durable namespace during
// compaction.
type ttlFilter struct{ now func() int64 }

func (f *ttlFilter) Filter(_ int, _, val []byte) (remove bool, newVal []byte) {
	d, err := decodeValue(val)
	if err != nil || d.tombstone {
		return false, nil
	}
	return d.expireAt != 0 && d.expireAt <= f.now(), nil
}

func (f *ttlFilter) Name() string            { return "levelquorum.ttl" }
func (f *ttlFilter) SetIgnoreSnapshots(bool) {}
func (f *ttlFilter) Destroy()                {}
