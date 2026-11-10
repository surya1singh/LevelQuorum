// Package storage is the single-node storage engine: RocksDB with one column
// family per namespace, per-key expiry, and size-bounded FIFO eviction for
// cache namespaces.
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
	"strings"
	"sync"
	"time"

	"github.com/linxGnu/grocksdb"
)

const (
	// metaCF is RocksDB's mandatory "default" column family. No user data is
	// stored in it; every namespace has its own column family.
	metaCF = "default"
	// cfPrefix turns a namespace name into its column family name.
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
	// BlockCacheSize is the RAM, in bytes, for RocksDB's cache of
	// uncompressed data blocks, shared by all namespaces. Default 256 MiB.
	BlockCacheSize uint64
	// WriteBufferSize is the memtable size, in bytes, per namespace.
	// Default 64 MiB.
	WriteBufferSize uint64
	// Now is the clock used for expiry. Default time.Now.
	Now func() time.Time
	// Logger receives warnings. Default slog.Default().
	Logger *slog.Logger
}

type namespace struct {
	spec   NamespaceSpec
	prefix []byte
	cf     *grocksdb.ColumnFamilyHandle
}

// Engine is safe for concurrent use.
type Engine struct {
	db         *grocksdb.DB
	metaHandle *grocksdb.ColumnFamilyHandle
	// namespaces is ordered longest prefix first; the default namespace
	// (empty prefix) is therefore last and matches every key.
	namespaces []*namespace

	blockCache *grocksdb.Cache
	options    []*grocksdb.Options
	ro         *grocksdb.ReadOptions
	wo         *grocksdb.WriteOptions
	woSync     *grocksdb.WriteOptions
	flushOpts  *grocksdb.FlushOptions

	writeBufferSize uint64
	nowFn           func() time.Time
	log             *slog.Logger

	// Every write to a key holds that key's stripe, so read-modify-write
	// commands (INCR, SET NX, EXPIRE, ...) are atomic. Reads take no lock.
	seed  maphash.Seed
	locks [lockStripes]sync.Mutex
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
	specs := make(map[string]NamespaceSpec, len(merged))
	for _, s := range merged {
		specs[s.Name] = s
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
	}
	e.woSync.SetSync(true)
	ok := false
	defer func() {
		if !ok {
			e.Close()
		}
	}()

	dbPath := filepath.Join(o.Dir, "db")
	dbOpts := grocksdb.NewDefaultOptions()
	e.options = append(e.options, dbOpts)
	dbOpts.SetCreateIfMissing(true)
	dbOpts.IncreaseParallelism(runtime.NumCPU())

	// RocksDB requires every existing column family to be opened, each with
	// its own options.
	existing := []string{metaCF}
	if _, err := os.Stat(filepath.Join(dbPath, "CURRENT")); err == nil {
		existing, err = grocksdb.ListColumnFamilies(dbOpts, dbPath)
		if err != nil {
			return nil, fmt.Errorf("storage: list column families: %w", err)
		}
	}
	cfOpts := make([]*grocksdb.Options, len(existing))
	for i, name := range existing {
		if name == metaCF {
			cfOpts[i] = dbOpts
			continue
		}
		nsName, isNS := strings.CutPrefix(name, cfPrefix)
		spec, known := specs[nsName]
		if !isNS || !known {
			return nil, fmt.Errorf("storage: column family %q exists in the database but is not recorded in %s", name, manifestFile)
		}
		cfOpts[i] = e.cfOptions(spec)
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
		e.addNamespace(specs[strings.TrimPrefix(name, cfPrefix)], handles[i])
	}

	// A new namespace takes over every key that starts with its prefix. If
	// such keys already exist in the namespace that owned them until now,
	// they would silently disappear from view, so refuse.
	isNew := make(map[string]bool, len(created))
	for _, name := range created {
		isNew[name] = true
	}
	var missing []NamespaceSpec
	for _, s := range merged {
		if e.namespaceByName(s.Name) == nil {
			missing = append(missing, s)
		}
	}
	for _, s := range missing {
		if !isNew[s.Name] || len(e.namespaces) == 0 || s.Prefix == "" {
			continue
		}
		owner := e.route([]byte(s.Prefix))
		if owner == nil {
			continue
		}
		found, err := e.hasLiveKeyWithPrefix(owner, []byte(s.Prefix))
		if err != nil {
			return nil, err
		}
		if found {
			return nil, fmt.Errorf("storage: cannot create namespace %q: namespace %q already holds keys starting with %q, and they would become unreachable", s.Name, owner.spec.Name, s.Prefix)
		}
	}

	// Record intent before creating column families: after a crash in
	// between, the next Open finds the manifest entry and creates the
	// missing column family.
	if err := saveManifest(o.Dir, merged); err != nil {
		return nil, fmt.Errorf("storage: write %s: %w", manifestFile, err)
	}
	for _, s := range missing {
		cf, err := db.CreateColumnFamily(e.cfOptions(s), cfPrefix+s.Name)
		if err != nil {
			return nil, fmt.Errorf("storage: create namespace %q: %w", s.Name, err)
		}
		e.addNamespace(s, cf)
	}

	ok = true
	return e, nil
}

func (e *Engine) addNamespace(spec NamespaceSpec, cf *grocksdb.ColumnFamilyHandle) {
	e.namespaces = append(e.namespaces, &namespace{spec: spec, prefix: []byte(spec.Prefix), cf: cf})
	sortByPrefixLen(e.namespaces, func(n *namespace) string { return n.spec.Prefix })
}

func (e *Engine) namespaceByName(name string) *namespace {
	for _, ns := range e.namespaces {
		if ns.spec.Name == name {
			return ns
		}
	}
	return nil
}

// cfOptions builds the RocksDB options for one namespace's column family.
func (e *Engine) cfOptions(spec NamespaceSpec) *grocksdb.Options {
	opts := grocksdb.NewDefaultOptions()
	e.options = append(e.options, opts)

	opts.SetWriteBufferSize(e.writeBufferSize)
	opts.SetCompression(grocksdb.LZ4Compression)

	table := grocksdb.NewDefaultBlockBasedTableOptions()
	table.SetBlockCache(e.blockCache)
	table.SetFilterPolicy(grocksdb.NewBloomFilter(10))
	opts.SetBlockBasedTableFactory(table)

	// Drops expired keys whenever a compaction rewrites them.
	opts.SetCompactionFilter(&ttlFilter{now: e.now})

	if spec.Profile == Cache {
		// FIFO: when the namespace exceeds MaxSize, RocksDB deletes its
		// oldest files whole. Eviction costs no rewriting of data.
		fifo := grocksdb.NewDefaultFIFOCompactionOptions()
		fifo.SetMaxTableFilesSize(spec.MaxSize)
		opts.SetCompactionStyle(grocksdb.FIFOCompactionStyle)
		opts.SetFIFOCompactionOptions(fifo)
	} else {
		opts.SetLevelCompactionDynamicLevelBytes(true)
	}
	return opts
}

// Close flushes the write-ahead log to disk and releases the database. No
// other method may be in flight or called afterwards.
func (e *Engine) Close() error {
	var err error
	if e.db != nil {
		err = e.db.FlushWAL(true)
		for _, ns := range e.namespaces {
			ns.cf.Destroy()
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

// NamespaceOf returns the spec of the namespace that owns key.
func (e *Engine) NamespaceOf(key []byte) NamespaceSpec {
	return e.route(key).spec
}

func (e *Engine) stripe(key []byte) int {
	return int(maphash.Bytes(e.seed, key) % lockStripes)
}

// lockKeys locks the stripes of all keys in a fixed order and returns the
// function that unlocks them.
func (e *Engine) lockKeys(keys ...[]byte) (unlock func()) {
	if len(keys) == 1 {
		mu := &e.locks[e.stripe(keys[0])]
		mu.Lock()
		return mu.Unlock
	}
	idx := make([]int, 0, len(keys))
	for _, k := range keys {
		idx = append(idx, e.stripe(k))
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

func (e *Engine) hasLiveKeyWithPrefix(ns *namespace, prefix []byte) (bool, error) {
	it := e.db.NewIteratorCF(e.ro, ns.cf)
	defer it.Close()
	now := e.now()
	for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
		_, exp, err := decodeValue(it.Value().Data())
		if err != nil || exp == 0 || exp > now {
			return true, nil
		}
	}
	return false, it.Err()
}

// NamespaceStats describes one namespace and its current size.
type NamespaceStats struct {
	Spec NamespaceSpec
	// EstimatedKeys is RocksDB's estimate; it can count overwritten, deleted
	// and expired keys that compaction has not removed yet.
	EstimatedKeys uint64
	// DiskBytes is the size of the namespace's data files.
	DiskBytes uint64
	// MemtableBytes is the size of data not yet flushed to files.
	MemtableBytes uint64
}

// Stats returns every namespace, ordered by name.
func (e *Engine) Stats() []NamespaceStats {
	out := make([]NamespaceStats, 0, len(e.namespaces))
	for _, ns := range e.namespaces {
		st := NamespaceStats{Spec: ns.spec}
		st.EstimatedKeys, _ = e.db.GetIntPropertyCF("rocksdb.estimate-num-keys", ns.cf)
		st.DiskBytes, _ = e.db.GetIntPropertyCF("rocksdb.total-sst-files-size", ns.cf)
		st.MemtableBytes, _ = e.db.GetIntPropertyCF("rocksdb.cur-size-all-mem-tables", ns.cf)
		out = append(out, st)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Spec.Name < out[j].Spec.Name })
	return out
}

// Flush writes a namespace's in-memory data to files and waits for it.
func (e *Engine) Flush(name string) error {
	ns := e.namespaceByName(name)
	if ns == nil {
		return fmt.Errorf("storage: no namespace %q", name)
	}
	return e.db.FlushCF(ns.cf, e.flushOpts)
}

// ttlFilter removes expired keys during compaction.
type ttlFilter struct{ now func() int64 }

func (f *ttlFilter) Filter(_ int, _, val []byte) (remove bool, newVal []byte) {
	_, exp, err := decodeValue(val)
	if err != nil {
		return false, nil
	}
	return exp != 0 && exp <= f.now(), nil
}

func (f *ttlFilter) Name() string            { return "levelquorum.ttl" }
func (f *ttlFilter) SetIgnoreSnapshots(bool) {}
func (f *ttlFilter) Destroy()                {}
