package storage

import (
	"crypto/rand"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/linxGnu/grocksdb"
)

func waitBackground(t *testing.T, e *Engine) {
	t.Helper()
	waitFor(t, "background compactions and promotions", func() bool { return e.bgJobs.Load() == 0 })
}

// settle runs maintenance until closed buckets are compacted and marked so.
func settle(t *testing.T, e *Engine) {
	t.Helper()
	for i := 0; i < 3; i++ {
		e.maintain()
		waitBackground(t, e)
	}
}

func mustRotate(t *testing.T, e *Engine, name string) {
	t.Helper()
	if err := e.rotate(e.namespaceByName(name)); err != nil {
		t.Fatal(err)
	}
}

// onDisk reads straight from RocksDB, past the hot tier, and returns the seq
// of the bucket holding the key's newest copy.
func onDisk(t *testing.T, e *Engine, key string) (value string, seq uint64, found bool) {
	t.Helper()
	e.topo.RLock()
	defer e.topo.RUnlock()
	ent, seq, err := e.lookup(e.route([]byte(key)), []byte(key), true)
	if err != nil {
		t.Fatal(err)
	}
	if ent == nil {
		return "", 0, false
	}
	return string(ent.Value), seq, true
}

func randomValue(t *testing.T, n int) []byte {
	t.Helper()
	v := make([]byte, n) // random, so compression cannot shrink it
	if _, err := rand.Read(v); err != nil {
		t.Fatal(err)
	}
	return v
}

func TestBucketCFNames(t *testing.T) {
	for _, tc := range []struct {
		ns  string
		seq uint64
		cf  string
	}{{"orders", 0, "ns.orders"}, {"sessions", 1, "ns.sessions#1"}, {"a-b_c", 1234, "ns.a-b_c#1234"}} {
		if got := bucketCFName(tc.ns, tc.seq); got != tc.cf {
			t.Errorf("bucketCFName(%q, %d) = %q; want %q", tc.ns, tc.seq, got, tc.cf)
		}
		ns, seq, ok := parseBucketCFName(tc.cf)
		if !ok || ns != tc.ns || seq != tc.seq {
			t.Errorf("parseBucketCFName(%q) = %q, %d, %v", tc.cf, ns, seq, ok)
		}
	}
	for _, bad := range []string{"default", "orders", "ns.a#", "ns.a#x", "ns.a#0", "ns.a#-1"} {
		if _, _, ok := parseBucketCFName(bad); ok {
			t.Errorf("parseBucketCFName(%q) succeeded", bad)
		}
	}
}

// A cache namespace stays within its size limit by dropping its oldest
// buckets; a durable namespace given the same writes keeps everything.
func TestCacheEviction(t *testing.T) {
	const (
		maxSize   = 1 << 20
		valueSize = 16 << 10
		count     = 512 // 8 MiB in total, eight times the limit
	)
	e, err := Open(Options{
		Dir:                 t.TempDir(),
		WriteBufferSize:     64 << 10,
		BlockCacheSize:      8 << 20,
		MaintenanceInterval: -1,
		Logger:              quietLogger(),
		Namespaces: []NamespaceSpec{
			{Name: "c", Prefix: "c:", Profile: Cache, MaxSize: maxSize},
			{Name: "d", Prefix: "d:", Profile: Safe},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()

	size := func() uint64 {
		st := namespaceStats(t, e, "c")
		return st.DiskBytes + st.MemtableBytes
	}
	var peak uint64
	for i := 0; i < count; i++ {
		value := randomValue(t, valueSize)
		for _, p := range []string{"c:", "d:"} {
			if _, err := e.Set([]byte(fmt.Sprintf("%s%04d", p, i)), value, SetOptions{}, Cache); err != nil {
				t.Fatal(err)
			}
		}
		e.maintain() // in production this runs once a second
		peak = max(peak, size())
	}
	settle(t, e)

	// Between maintenance passes the namespace can exceed the limit by what
	// was written in between: here, one value.
	if limit := uint64(maxSize + 2*valueSize); peak > limit {
		t.Errorf("the cache namespace peaked at %d bytes; want at most %d", peak, limit)
	}
	st := namespaceStats(t, e, "c")
	if st.EvictedBuckets == 0 || st.Buckets < 2 || st.Buckets > maxBuckets {
		t.Errorf("cache namespace has %d buckets and evicted %d; want eviction and a bounded bucket count", st.Buckets, st.EvictedBuckets)
	}

	wantMissing(t, e, "c:0000")
	if _, ok := get(t, e, fmt.Sprintf("c:%04d", count-1)); !ok {
		t.Fatal("the newest cache key was evicted")
	}
	survivors := 0
	for i := 0; i < count; i++ {
		if _, ok := get(t, e, fmt.Sprintf("c:%04d", i)); ok {
			survivors++
		} else if survivors > 0 {
			t.Fatalf("cache key %d is missing but older keys survive; eviction must be oldest-first", i)
		}
	}
	// A full namespace holds between max_size minus one bucket and max_size.
	if lo, hi := maxSize/valueSize*3/4, maxSize/valueSize; survivors < lo || survivors > hi {
		t.Errorf("%d cache keys survive; want between %d and %d", survivors, lo, hi)
	}

	for i := 0; i < count; i++ {
		if _, ok := get(t, e, fmt.Sprintf("d:%04d", i)); !ok {
			t.Fatalf("durable key %d is missing; durable namespaces must never evict", i)
		}
	}
	if d := namespaceStats(t, e, "d"); d.Buckets != 1 || d.DiskBytes+d.MemtableBytes < count*valueSize {
		t.Errorf("durable namespace: %d buckets, %d bytes; want 1 bucket holding at least %d bytes", d.Buckets, d.DiskBytes+d.MemtableBytes, count*valueSize)
	}
}

// The newest bucket that knows a key decides, whether it holds a newer
// value, a deletion or an expired value. Older copies must never resurface.
func TestNewerBucketsShadowOlderOnes(t *testing.T) { bothModes(t, testNewerBucketsShadowOlderOnes) }

func testNewerBucketsShadowOlderOnes(t *testing.T) {
	dir := t.TempDir()
	clock := newFakeClock()
	spec := NamespaceSpec{Name: "c", Prefix: "c:", Profile: Cache, MaxSize: 1 << 30}
	e := mustOpen(t, dir, clock, spec)

	for _, k := range []string{"c:deleted", "c:overwritten", "c:expiring", "c:kept", "c:reborn"} {
		mustSet(t, e, k, "old", SetOptions{})
	}
	mustRotate(t, e, "c")

	if n, err := e.Delete([][]byte{[]byte("c:deleted"), []byte("c:reborn")}, Cache); err != nil || n != 2 {
		t.Fatalf("Delete = %d, %v; want 2", n, err)
	}
	mustSet(t, e, "c:overwritten", "new", SetOptions{})
	mustSet(t, e, "c:expiring", "short-lived", SetOptions{ExpireAt: clock.Now().Add(time.Minute).UnixMilli()})
	mustRotate(t, e, "c")
	mustSet(t, e, "c:reborn", "again", SetOptions{})
	clock.Advance(time.Hour)

	check := func(when string) {
		t.Helper()
		for key, want := range map[string]string{"c:overwritten": "new", "c:kept": "old", "c:reborn": "again"} {
			if got, ok := get(t, e, key); !ok || got != want {
				t.Errorf("%s: Get(%q) = %q, exists=%v; want %q", when, key, got, ok, want)
			}
		}
		for _, key := range []string{"c:deleted", "c:expiring"} {
			if got, ok := get(t, e, key); ok {
				t.Errorf("%s: Get(%q) = %q; an older copy resurfaced", when, key, got)
			}
		}
		if n, _ := e.Delete([][]byte{[]byte("c:deleted")}, Cache); n != 0 {
			t.Errorf("%s: deleting an already deleted key reported %d", when, n)
		}
	}
	check("after the writes")

	// Compacting the closed buckets must not change any answer.
	settle(t, e)
	check("after compaction")

	// Nor must a restart, which also empties the hot tier.
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}
	e = mustOpen(t, dir, clock, spec)
	check("after a restart")
	if got := len(e.namespaceByName("c").buckets); got != 3 {
		t.Errorf("namespace has %d buckets after the restart; want the same 3", got)
	}
}

// A closed bucket ends up as a single sorted run, and is then left alone.
func TestClosedBucketsAreCompacted(t *testing.T) {
	e := mustOpen(t, t.TempDir(), nil, NamespaceSpec{Name: "c", Prefix: "c:", Profile: Cache, MaxSize: 1 << 30})
	ns := e.namespaceByName("c")

	// Three files: one fewer than the count at which RocksDB starts merging
	// files on its own, so they are still separate when the bucket closes.
	for round := 0; round < 3; round++ {
		for i := 0; i < 50; i++ {
			mustSet(t, e, fmt.Sprintf("c:%d", i), fmt.Sprint(round), SetOptions{})
		}
		if err := e.Flush("c"); err != nil {
			t.Fatal(err)
		}
	}
	mustRotate(t, e, "c")
	closed := ns.buckets[1]
	// Everything is flushed, so only the file layout can say that the
	// bucket needs work.
	if l0, _ := e.fileLayout(closed); l0 != 3 || !e.needsCompaction(closed) {
		t.Fatalf("closed bucket before maintenance: %d level-0 files, needsCompaction=%v; want 3 and true", l0, e.needsCompaction(closed))
	}
	// Data still in memory counts too.
	mustSet(t, e, "c:unflushed", "v", SetOptions{})
	mustRotate(t, e, "c")
	if e.needsCompaction(ns.buckets[0]) || !e.needsCompaction(ns.buckets[1]) {
		t.Fatal("want the empty newest bucket to need nothing and the one with unflushed data to need compaction")
	}

	settle(t, e)
	for _, b := range ns.buckets[1:] {
		l0, levels := e.fileLayout(b)
		if l0 != 0 || levels != 1 || e.needsCompaction(b) || !b.settled {
			t.Fatalf("closed bucket %d after maintenance: %d level-0 files, %d deeper levels, settled=%v; want one sorted run", b.seq, l0, levels, b.settled)
		}
	}
	if e.needsCompaction(ns.buckets[0]) {
		t.Fatal("the new, empty bucket was reported as needing compaction")
	}
	wantValue(t, e, "c:7", "2")
	wantValue(t, e, "c:unflushed", "v")

	// A settled bucket is not compacted again.
	e.maintain()
	if n := e.bgJobs.Load(); n != 0 {
		t.Fatalf("maintenance queued %d jobs for an already compacted bucket", n)
	}
}

// A key that is read while it sits in the oldest part of a nearly full cache
// namespace is rewritten into the newest bucket and so outlives the bucket
// it was written to. Keys that are not read are evicted with it.
func TestPromotion(t *testing.T) { bothModes(t, testPromotion) }

func testPromotion(t *testing.T) {
	const maxSize, valueSize = 1 << 20, 8 << 10
	e, err := Open(Options{
		Dir:                 t.TempDir(),
		WriteBufferSize:     64 << 10,
		BlockCacheSize:      8 << 20,
		HotCacheSize:        hotSize,
		MaintenanceInterval: -1,
		Logger:              quietLogger(),
		Namespaces:          []NamespaceSpec{{Name: "c", Prefix: "c:", Profile: Cache, MaxSize: maxSize}},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	ns := e.namespaceByName("c")

	written := 0
	write := func() {
		t.Helper()
		if _, err := e.Set([]byte(fmt.Sprintf("c:%05d", written)), randomValue(t, valueSize), SetOptions{}, Cache); err != nil {
			t.Fatal(err)
		}
		written++
		e.maintain()
	}

	// Fill the namespace until it has a promotion zone.
	for ns.promoteBelow.Load() == 0 {
		if written > 4*maxSize/valueSize {
			t.Fatal("the namespace never got a promotion zone")
		}
		write()
	}
	// The two oldest surviving keys share the oldest bucket.
	oldest := 0
	for ; ; oldest++ {
		if _, _, ok := onDisk(t, e, fmt.Sprintf("c:%05d", oldest)); ok {
			break
		}
	}
	used, unused := fmt.Sprintf("c:%05d", oldest), fmt.Sprintf("c:%05d", oldest+1)
	wantVal, origSeq, _ := onDisk(t, e, used)
	if _, seq, ok := onDisk(t, e, unused); !ok || seq != origSeq || origSeq >= ns.promoteBelow.Load() {
		t.Fatalf("test setup: keys %s and %s are not together in the promotion zone", used, unused)
	}

	// Reading the key promotes it.
	if got, ok := get(t, e, used); !ok || got != wantVal {
		t.Fatalf("Get(%s) did not return the stored value", used)
	}
	waitBackground(t, e)
	e.topo.RLock()
	newest := ns.buckets[0].seq
	e.topo.RUnlock()
	if _, seq, _ := onDisk(t, e, used); seq != newest {
		t.Fatalf("after a read, %s is in bucket %d; want the newest bucket %d", used, seq, newest)
	}
	if got := ns.promotions.Load(); got != 1 {
		t.Fatalf("promotions = %d; want 1", got)
	}
	// Reading it again changes nothing: it is no longer in the zone.
	get(t, e, used)
	waitBackground(t, e)
	if got := ns.promotions.Load(); got != 1 {
		t.Fatalf("promotions = %d after a second read; want still 1", got)
	}

	// Write until the bucket both keys started in has been evicted.
	for {
		e.topo.RLock()
		gone := ns.buckets[len(ns.buckets)-1].seq > origSeq
		e.topo.RUnlock()
		if gone {
			break
		}
		write()
	}
	if got, _, ok := onDisk(t, e, used); !ok || got != wantVal {
		t.Errorf("the key that was read did not survive the eviction of its original bucket")
	}
	if _, _, ok := onDisk(t, e, unused); ok {
		t.Errorf("the key that was never read survived the eviction of its bucket")
	}
}

func TestHotTier(t *testing.T) {
	hotSize = 8 << 20
	defer func() { hotSize = 0 }()
	dir := t.TempDir()
	clock := newFakeClock()
	spec := NamespaceSpec{Name: "s", Prefix: "s:", Profile: Safe, DefaultTTL: time.Hour}
	e := mustOpen(t, dir, clock, spec)
	stats := func() (hits, misses, entries uint64) {
		st, ok := e.HotStats()
		if !ok {
			t.Fatal("HotStats reports the hot tier as disabled")
		}
		return st.Hits, st.Misses, st.Entries
	}

	// A write fills the hot tier, so the read after it does not reach disk.
	buf := []byte("value-1")
	if _, err := e.Set([]byte("k"), buf, SetOptions{}, Cache); err != nil {
		t.Fatal(err)
	}
	copy(buf, "XXXXXXX") // the engine must have kept its own copy
	wantValue(t, e, "k", "value-1")
	if hits, misses, entries := stats(); hits != 1 || misses != 0 || entries != 1 {
		t.Fatalf("after Set+Get: hits=%d misses=%d entries=%d; want 1, 0, 1", hits, misses, entries)
	}

	// A value handed out is never changed by later writes.
	ent, _ := e.Get([]byte("k"))
	if _, err := e.Append([]byte("k"), []byte("+more"), Cache); err != nil {
		t.Fatal(err)
	}
	if string(ent.Value) != "value-1" {
		t.Fatalf("a value returned earlier changed to %q after Append", ent.Value)
	}
	wantValue(t, e, "k", "value-1+more")

	// Every kind of write keeps the hot tier current.
	e.MSet([][]byte{[]byte("m"), []byte("1")}, Cache)
	wantValue(t, e, "m", "1")
	e.IncrBy([]byte("m"), 41, Cache)
	wantValue(t, e, "m", "42")
	e.Delete([][]byte{[]byte("m")}, Cache)
	wantMissing(t, e, "m")
	e.GetDel([]byte("k"), Cache)
	wantMissing(t, e, "k")

	// Expiry is honoured for values served from RAM, and an expired entry
	// is dropped from RAM when it is next looked at.
	mustSet(t, e, "s:a", "v", SetOptions{})
	mustSet(t, e, "t", "v", SetOptions{ExpireAt: clock.Now().Add(time.Minute).UnixMilli()})
	e.Expire([]byte("t"), clock.Now().Add(2*time.Minute).UnixMilli(), Cache)
	clock.Advance(90 * time.Second)
	wantValue(t, e, "t", "v")
	if exp, ok, _ := e.Meta([]byte("s:a")); !ok || exp != clock.Now().Add(time.Hour-90*time.Second).UnixMilli() {
		t.Fatalf("Meta from the hot tier = %d, %v; want the namespace's default TTL", exp, ok)
	}
	clock.Advance(time.Minute)
	wantMissing(t, e, "t")
	_, _, before := stats()
	clock.Advance(time.Hour)
	wantMissing(t, e, "s:a")
	if _, _, after := stats(); after != before-1 {
		t.Fatalf("hot tier has %d entries after reading an expired key; want %d", after, before-1)
	}

	// After a restart the hot tier is empty and refills from disk.
	mustSet(t, e, "p", "persisted", SetOptions{})
	e.Close()
	e = mustOpen(t, dir, clock, spec)
	stats = func() (uint64, uint64, uint64) {
		st, _ := e.HotStats()
		return st.Hits, st.Misses, st.Entries
	}
	wantValue(t, e, "p", "persisted")
	wantValue(t, e, "p", "persisted")
	if hits, misses, entries := stats(); hits != 1 || misses != 1 || entries != 1 {
		t.Fatalf("after restart and two reads: hits=%d misses=%d entries=%d; want 1, 1, 1", hits, misses, entries)
	}

	// Disabled means disabled.
	hotSize = 0
	off := mustOpen(t, t.TempDir(), clock)
	if _, ok := off.HotStats(); ok {
		t.Fatal("HotStats reports a hot tier although HotCacheSize is 0")
	}
}

// Readers must never see a key go back to an older value, while writers,
// rotation, eviction, compaction and promotion all run at once. This is the
// test for the locking around the hot tier and the bucket list.
func TestConsistencyUnderLoad(t *testing.T) { bothModes(t, testConsistencyUnderLoad) }

func testConsistencyUnderLoad(t *testing.T) {
	e, err := Open(Options{
		Dir:                 t.TempDir(),
		WriteBufferSize:     64 << 10,
		BlockCacheSize:      8 << 20,
		HotCacheSize:        hotSize / 32, // a few entries per shard, so the hot tier evicts constantly
		MaintenanceInterval: -1,
		Logger:              quietLogger(),
		Namespaces: []NamespaceSpec{
			{Name: "c", Prefix: "c:", Profile: Cache, MaxSize: 256 << 10},
			{Name: "d", Prefix: "d:", Profile: Safe},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()

	const keysPerNS, writes = 4, 1500
	pad := string(randomValue(t, 1000))
	var stop atomic.Bool
	var bg, writers sync.WaitGroup
	fail := func(format string, a ...any) {
		t.Errorf(format, a...)
		stop.Store(true)
	}

	// Maintenance runs flat out instead of once a second.
	bg.Add(1)
	go func() {
		defer bg.Done()
		for !stop.Load() {
			e.maintain()
		}
	}()

	for _, prefix := range []string{"c:", "d:"} {
		for k := 0; k < keysPerNS; k++ {
			key := []byte(fmt.Sprintf("%s%d", prefix, k))
			durable := prefix == "d:"

			// One writer per key writes increasing numbers, mixing the
			// commands that take different paths through the engine.
			writers.Add(1)
			go func() {
				defer writers.Done()
				for i := 1; i <= writes && !stop.Load(); i++ {
					val := []byte(fmt.Sprintf("%08d%s", i, pad))
					var err error
					switch i % 4 {
					case 0:
						err = e.MSet([][]byte{key, val}, Cache)
					case 1:
						_, err = e.Set(key, val, SetOptions{KeepTTL: true}, Cache)
					default:
						_, err = e.Set(key, val, SetOptions{}, Cache)
					}
					if err != nil {
						fail("write %s: %v", key, err)
						return
					}
					// Churn in the cache namespace, to force rotation.
					if !durable {
						filler := []byte(fmt.Sprintf("c:filler-%s-%d", key, i))
						if _, err := e.Set(filler, val, SetOptions{}, Cache); err != nil {
							fail("write %s: %v", filler, err)
							return
						}
						if i%50 == 0 {
							e.Delete([][]byte{filler}, Cache)
						}
					}
				}
			}()

			for r := 0; r < 2; r++ {
				bg.Add(1)
				go func() {
					defer bg.Done()
					last, seen := 0, false
					for !stop.Load() {
						ent, err := e.Get(key)
						if err != nil {
							fail("read %s: %v", key, err)
							return
						}
						if ent == nil {
							// A cache key may be evicted; a durable key may
							// only be missing before its first write.
							if durable && seen {
								fail("durable key %s disappeared", key)
								return
							}
							continue
						}
						n, err := strconv.Atoi(string(ent.Value[:8]))
						if err != nil || len(ent.Value) != 8+len(pad) {
							fail("read %s: corrupt value %q", key, ent.Value[:min(len(ent.Value), 20)])
							return
						}
						if n < last {
							fail("key %s went back from %d to %d", key, last, n)
							return
						}
						last, seen = n, true
					}
				}()
			}
		}
	}

	writers.Wait()
	stop.Store(true)
	bg.Wait()
	if t.Failed() {
		return
	}
	// The hot-tier mode is only a test of the hot tier if reads both hit it
	// and fall through to disk, which is where a stale value could be put
	// back.
	if st, on := e.HotStats(); on && (st.Hits == 0 || st.Misses == 0 || st.Evictions == 0) {
		t.Errorf("hot tier stats %+v; want hits, misses and evictions, or the test exercises nothing", st)
	}

	// Every durable key holds its final value; the cache evicted.
	waitBackground(t, e)
	for k := 0; k < keysPerNS; k++ {
		got, ok := get(t, e, fmt.Sprintf("d:%d", k))
		if !ok || got[:8] != fmt.Sprintf("%08d", writes) {
			t.Errorf("durable key %d ends at %q; want %08d", k, got[:min(len(got), 8)], writes)
		}
	}
	if st := namespaceStats(t, e, "c"); st.EvictedBuckets == 0 {
		t.Error("the cache namespace never evicted; the test did not exercise eviction")
	}
}

// A data directory written by the version that kept a cache namespace in a
// single FIFO-compacted column family still opens. The old column family
// becomes the namespace's oldest bucket and is the first to be evicted.
func TestOpensSingleColumnFamilyCacheNamespace(t *testing.T) {
	dir := t.TempDir()
	const maxSize = 1 << 20
	spec := NamespaceSpec{Name: DefaultNamespace, Profile: Cache, MaxSize: maxSize}
	if err := saveManifest(dir, []NamespaceSpec{spec}); err != nil {
		t.Fatal(err)
	}

	// Build the old layout by hand.
	opts := grocksdb.NewDefaultOptions()
	opts.SetCreateIfMissing(true)
	opts.SetCreateIfMissingColumnFamilies(true)
	fifo := grocksdb.NewDefaultOptions()
	fifoOpts := grocksdb.NewDefaultFIFOCompactionOptions()
	fifoOpts.SetMaxTableFilesSize(maxSize)
	fifo.SetCompactionStyle(grocksdb.FIFOCompactionStyle)
	fifo.SetFIFOCompactionOptions(fifoOpts)
	db, handles, err := grocksdb.OpenDbColumnFamilies(opts, filepath.Join(dir, "db"),
		[]string{"default", "ns.default"}, []*grocksdb.Options{opts, fifo})
	if err != nil {
		t.Fatal(err)
	}
	wo, fo := grocksdb.NewDefaultWriteOptions(), grocksdb.NewDefaultFlushOptions()
	for round := 0; round < 3; round++ { // three files, as FIFO leaves them
		for _, k := range []string{"old-kept", "old-overwritten", "old-deleted", "old-removed-before-upgrade"} {
			if err := db.PutCF(wo, handles[1], []byte(k), encodeValue([]byte(fmt.Sprint("old", round)), 0)); err != nil {
				t.Fatal(err)
			}
		}
		db.DeleteCF(wo, handles[1], []byte("old-removed-before-upgrade"))
		if err := db.FlushCF(handles[1], fo); err != nil {
			t.Fatal(err)
		}
	}
	for _, h := range handles {
		h.Destroy()
	}
	db.Close()

	e := mustOpen(t, dir, nil, spec)
	ns := e.namespaceByName(DefaultNamespace)
	if len(ns.buckets) != 2 || ns.buckets[0].seq != 1 || ns.buckets[1].seq != 0 {
		t.Fatalf("buckets after opening the old layout: %d; want a new bucket 1 in front of the old bucket 0", len(ns.buckets))
	}
	wantValue(t, e, "old-kept", "old2")
	wantMissing(t, e, "old-removed-before-upgrade")
	mustSet(t, e, "old-overwritten", "new", SetOptions{})
	if n, _ := e.Delete([][]byte{[]byte("old-deleted")}, Cache); n != 1 {
		t.Fatal("could not delete a key stored in the old column family")
	}
	wantValue(t, e, "old-overwritten", "new")
	wantMissing(t, e, "old-deleted")
	if _, seq, _ := onDisk(t, e, "old-kept"); seq != 0 {
		t.Fatalf("old-kept is in bucket %d; want the old bucket 0", seq)
	}

	// The old column family is compacted like any closed bucket.
	if l0, _ := e.fileLayout(ns.buckets[1]); l0 != 3 {
		t.Fatalf("the old column family has %d level-0 files; the test built 3", l0)
	}
	settle(t, e)
	if l0, levels := e.fileLayout(ns.buckets[1]); l0 != 0 || levels != 1 {
		t.Fatalf("the old column family after maintenance: %d level-0 files, %d deeper levels; want one sorted run", l0, levels)
	}
	wantValue(t, e, "old-kept", "old2")

	// It survives a restart in the same position.
	e.Close()
	e = mustOpen(t, dir, nil, spec)
	ns = e.namespaceByName(DefaultNamespace)
	if len(ns.buckets) != 2 || ns.buckets[1].seq != 0 {
		t.Fatalf("after a restart the old bucket is not last of two: %d buckets", len(ns.buckets))
	}
	wantValue(t, e, "old-kept", "old2")
	wantValue(t, e, "old-overwritten", "new")
	wantMissing(t, e, "old-deleted")

	// And it is evicted first once the namespace fills up.
	for i := 0; i < 2*maxSize/(16<<10); i++ {
		if _, err := e.Set([]byte(fmt.Sprintf("fill-%d", i)), randomValue(t, 16<<10), SetOptions{}, Cache); err != nil {
			t.Fatal(err)
		}
		e.maintain()
	}
	for _, b := range ns.buckets {
		if b.seq == 0 {
			t.Fatal("the old column family is still there after the namespace was filled twice over")
		}
	}
	wantMissing(t, e, "old-kept")
	wantMissing(t, e, "old-deleted")
}

// A durable namespace must not have cache buckets.
func TestOpenRejectsBucketsInDurableNamespace(t *testing.T) {
	dir := t.TempDir()
	e := mustOpen(t, dir, nil, NamespaceSpec{Name: "c", Prefix: "c:", Profile: Cache, MaxSize: 1 << 20})
	e.Close()
	// Rewrite the manifest to claim the namespace is durable.
	if err := saveManifest(dir, []NamespaceSpec{
		{Name: "c", Prefix: "c:", Profile: Safe},
		{Name: DefaultNamespace, Profile: Cache, MaxSize: DefaultCacheSize},
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := openAt(t, dir, nil); err == nil {
		t.Fatal("Open accepted cache buckets in a durable namespace")
	}
}

// With the background goroutine running, as in production, a burst of writes
// far faster than the periodic check must still be split into buckets and
// held to the size limit. Only the writes themselves can trigger that.
func TestWritesWakeMaintenance(t *testing.T) {
	const maxSize, valueSize = 4 << 20, 8 << 10
	dir := t.TempDir()
	e, err := Open(Options{
		Dir:                 dir,
		WriteBufferSize:     64 << 10,
		BlockCacheSize:      8 << 20,
		MaintenanceInterval: time.Hour, // the periodic check never fires in this test
		Logger:              quietLogger(),
		Namespaces: []NamespaceSpec{
			{Name: "c", Prefix: "c:", Profile: Cache, MaxSize: maxSize},
			{Name: "d", Prefix: "d:", Profile: Safe},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()

	// One durable key written first and never flushed: it sits in the
	// oldest write-ahead log file for the whole test.
	mustSet(t, e, "d:pinned", "v", SetOptions{})

	const count = 4 * maxSize / valueSize // four times the limit
	for i := 0; i < count; i++ {
		if _, err := e.Set([]byte(fmt.Sprintf("c:%05d", i)), randomValue(t, valueSize), SetOptions{}, Cache); err != nil {
			t.Fatal(err)
		}
	}
	waitFor(t, "maintenance to catch up", func() bool {
		st := namespaceStats(t, e, "c")
		return st.DiskBytes+st.MemtableBytes <= maxSize && e.bgJobs.Load() == 0
	})

	st := namespaceStats(t, e, "c")
	if st.EvictedBuckets < cacheBuckets || st.Buckets < cacheBuckets/2 {
		t.Errorf("%d buckets, %d evicted; want the burst split into about %d buckets per max_size", st.Buckets, st.EvictedBuckets, cacheBuckets)
	}
	wantMissing(t, e, "c:00000")
	if _, ok := get(t, e, fmt.Sprintf("c:%05d", count-1)); !ok {
		t.Error("the newest key was evicted")
	}
	wantValue(t, e, "d:pinned", "v")

	// The write-ahead log must not have kept everything that was written.
	var walBytes int64
	logs, _ := filepath.Glob(filepath.Join(dir, "db", "*.log"))
	for _, f := range logs {
		if fi, err := os.Stat(f); err == nil {
			walBytes += fi.Size()
		}
	}
	if limit := int64(8 * (64 << 10)); walBytes > limit {
		t.Errorf("write-ahead log files total %d bytes after writing %d; want at most %d", walBytes, count*valueSize, limit)
	}
}
