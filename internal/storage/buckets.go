package storage

import (
	"bytes"
	"fmt"
	"strconv"
	"time"

	"github.com/linxGnu/grocksdb"
)

// How a cache namespace evicts.
//
// A cache namespace is a series of buckets, each its own column family.
// Writes go to the newest bucket. When that bucket reaches its share of the
// namespace's max_size, it is closed and a new one is started. When the
// namespace as a whole exceeds max_size, the oldest bucket is dropped, which
// deletes its files without reading or rewriting them.
//
// Compared with keeping the whole namespace in one FIFO-compacted column
// family, this bounds what a read has to check: a handful of buckets, each
// compacted into a single sorted run, instead of every file the namespace
// has ever written.
//
// A read walks the buckets newest first and stops at the first one that
// knows the key. That is why a delete is stored as a tombstone value and why
// expired values are not filtered out of a bucket: either would let an older
// copy in an older bucket show through.
const (
	// cacheBuckets is how many buckets a full cache namespace nominally
	// has: a bucket is closed at max_size/cacheBuckets. Eviction therefore
	// frees about 1/cacheBuckets of the namespace at a time.
	cacheBuckets = 16
	// maxBuckets caps the bucket count, and with it the cost of a read that
	// finds nothing. It is only reached when closed buckets shrink a lot
	// during compaction, as they do under heavy overwriting.
	maxBuckets = 2 * cacheBuckets
	// promoteMinBuckets is the fewest buckets a namespace needs before keys
	// in its oldest quarter are promoted when read.
	promoteMinBuckets = 8
)

type promoteReq struct {
	ns  *namespace
	key []byte
}

func (e *Engine) startBackground(interval time.Duration) {
	e.bg.Add(2)
	go e.compactionWorker()
	go e.promotionWorker()
	if interval > 0 {
		e.bg.Add(1)
		go func() {
			defer e.bg.Done()
			t := time.NewTicker(interval)
			defer t.Stop()
			for {
				select {
				case <-e.stop:
					return
				case <-t.C:
					e.maintain()
				case <-e.kick:
					e.maintain()
				}
			}
		}()
	}
}

// noteWrite records n bytes written to ns and wakes maintenance each time a
// quarter of a bucket's share has been written. Without it, rotation and
// eviction would only happen on the periodic check, and a fast writer could
// push a namespace well past its limit, or a whole burst into one bucket, in
// between.
func (e *Engine) noteWrite(ns *namespace, n int) {
	if ns.spec.Profile != Cache {
		return
	}
	if ns.written.Add(uint64(n)) < max(ns.spec.MaxSize/cacheBuckets/4, 1) {
		return
	}
	ns.written.Store(0)
	select {
	case e.kick <- struct{}{}:
	default: // maintenance is already due
	}
}

// maintain rotates, evicts and schedules compaction for every cache
// namespace. It runs periodically and when noteWrite asks for it; tests call
// it directly.
func (e *Engine) maintain() {
	e.maintMu.Lock()
	defer e.maintMu.Unlock()
	for _, ns := range e.namespaces {
		if ns.spec.Profile != Cache {
			continue
		}
		if err := e.maintainNamespace(ns); err != nil {
			e.log.Error("cache maintenance failed", "namespace", ns.spec.Name, "err", err)
		}
	}
}

// bucketSize is the bucket's data on disk plus what is still in memory.
func (e *Engine) bucketSize(b *bucket) uint64 {
	return e.intProperty(b, "rocksdb.live-sst-files-size") + e.intProperty(b, "rocksdb.cur-size-all-mem-tables")
}

// maintainNamespace is the only code that changes a bucket list after Open,
// and maintMu serialises it, so it may read ns.buckets without holding topo.
func (e *Engine) maintainNamespace(ns *namespace) error {
	maxSize := ns.spec.MaxSize
	target := max(maxSize/cacheBuckets, 1)

	// Close the newest bucket once it has its share.
	if e.bucketSize(ns.buckets[0]) >= target {
		if err := e.rotate(ns); err != nil {
			return err
		}
	}

	// Evict the oldest buckets while the namespace is over its limit.
	var total uint64
	sizes := make([]uint64, len(ns.buckets))
	for i, b := range ns.buckets {
		sizes[i] = e.bucketSize(b)
		total += sizes[i]
	}
	for n := len(ns.buckets); n > 1 && (total > maxSize || n > maxBuckets); n = len(ns.buckets) {
		oldest := ns.buckets[n-1]
		e.topo.Lock()
		ns.buckets = ns.buckets[: n-1 : n-1]
		e.topo.Unlock()
		total -= sizes[n-1]
		if err := e.dropBucket(oldest); err != nil {
			return fmt.Errorf("drop bucket %d: %w", oldest.seq, err)
		}
		ns.evictedBuckets.Add(1)
	}

	// Once the namespace is close to evicting, keys read from its oldest
	// quarter are rewritten into the newest bucket, so that data still in
	// use survives the eviction of the bucket it was written to.
	var below uint64
	if n := len(ns.buckets); n >= promoteMinBuckets && total+2*target > maxSize {
		below = ns.buckets[n-n/4].seq + 1
	}
	ns.promoteBelow.Store(below)

	// Closed buckets are compacted once into a single sorted run, so a read
	// checks one bloom filter per bucket.
	for _, b := range ns.buckets[1:] {
		if b.settled {
			continue
		}
		if e.needsCompaction(b) {
			e.enqueueCompaction(b)
		} else {
			b.settled = true
		}
	}
	return nil
}

// rotate closes the namespace's newest bucket by starting a new one in front
// of it. Writes that are in flight finish in the old bucket; that is safe
// because writes to one key are serialised and a later write can only land
// in the same or a newer bucket.
func (e *Engine) rotate(ns *namespace) error {
	seq := ns.buckets[0].seq + 1
	cf, err := e.db.CreateColumnFamily(ns.opts, bucketCFName(ns.spec.Name, seq))
	if err != nil {
		return fmt.Errorf("start bucket %d: %w", seq, err)
	}
	e.topo.Lock()
	ns.buckets = append([]*bucket{{seq: seq, cf: cf}}, ns.buckets...)
	e.topo.Unlock()
	return nil
}

// needsCompaction reports whether a closed bucket is anything other than
// empty or a single sorted run on disk.
func (e *Engine) needsCompaction(b *bucket) bool {
	if e.intProperty(b, "rocksdb.num-entries-active-mem-table")+e.intProperty(b, "rocksdb.num-entries-imm-mem-tables") > 0 {
		return true
	}
	l0, levels := e.fileLayout(b)
	return l0 > 0 || levels > 1
}

// fileLayout returns the number of files in level 0, where every file is its
// own sorted run, and the number of deeper levels that hold files, each of
// which is one sorted run.
func (e *Engine) fileLayout(b *bucket) (l0Files, deeperLevels int) {
	// These are string properties; RocksDB does not serve them through the
	// integer property call.
	l0Files, _ = strconv.Atoi(e.db.GetPropertyCF("rocksdb.num-files-at-level0", b.cf))
	for l := 1; l < 7; l++ {
		if n, _ := strconv.Atoi(e.db.GetPropertyCF("rocksdb.num-files-at-level"+strconv.Itoa(l), b.cf)); n > 0 {
			deeperLevels++
		}
	}
	return l0Files, deeperLevels
}

func (e *Engine) enqueueCompaction(b *bucket) {
	e.compactMu.Lock()
	defer e.compactMu.Unlock()
	if b.queued || b.dropped {
		return
	}
	select {
	case e.compactQ <- b:
		b.queued = true
		e.bgJobs.Add(1)
	default: // queue full; the next maintenance pass tries again
	}
}

func (e *Engine) compactionWorker() {
	defer e.bg.Done()
	for {
		select {
		case <-e.stop:
			return
		case b := <-e.compactQ:
			e.compactBucket(b)
			e.bgJobs.Add(-1)
		}
	}
}

// compactBucket flushes and fully compacts a closed bucket. The bucket's
// handle stays valid throughout: dropBucket waits for done before it
// destroys the handle.
func (e *Engine) compactBucket(b *bucket) {
	e.compactMu.Lock()
	if b.dropped {
		b.queued = false
		e.compactMu.Unlock()
		return
	}
	done := make(chan struct{})
	b.done = done
	e.compactingNow = b
	e.compactMu.Unlock()

	if err := e.db.FlushCF(b.cf, e.flushOpts); err != nil {
		e.log.Error("flushing a closed cache bucket failed", "err", err)
	} else {
		e.db.CompactRangeCF(b.cf, grocksdb.Range{})
	}

	e.compactMu.Lock()
	e.compactingNow = nil
	b.queued = false
	e.compactMu.Unlock()
	close(done)
}

// dropBucket deletes a bucket that has already been removed from its
// namespace's list. Taking topo for writing to remove it waited out every
// operation that could have been using the handle; what remains is a
// possible background compaction of this bucket, which is aborted.
func (e *Engine) dropBucket(b *bucket) error {
	e.compactMu.Lock()
	b.dropped = true
	var done chan struct{}
	if e.compactingNow == b {
		done = b.done
	}
	e.compactMu.Unlock()
	if done != nil {
		e.db.DisableManualCompaction()
		<-done
		e.db.EnableManualCompaction()
	}

	err := e.db.DropColumnFamily(b.cf)
	b.cf.Destroy()
	return err
}

// maybePromote queues key for promotion if its newest copy is in the
// promotion zone. It never blocks: when the queue is full the request is
// dropped, which bounds the write load that reads can cause.
func (e *Engine) maybePromote(ns *namespace, key []byte, seq uint64) {
	if seq >= ns.promoteBelow.Load() {
		return
	}
	select {
	case e.promoteQ <- promoteReq{ns: ns, key: bytes.Clone(key)}:
		e.bgJobs.Add(1)
	default:
	}
}

func (e *Engine) promotionWorker() {
	defer e.bg.Done()
	for {
		select {
		case <-e.stop:
			return
		case r := <-e.promoteQ:
			if err := e.promote(r); err != nil {
				e.log.Error("promoting a cache key failed", "namespace", r.ns.spec.Name, "err", err)
			}
			e.bgJobs.Add(-1)
		}
	}
}

// promote rewrites key into the newest bucket if its newest copy on disk is
// still in the promotion zone.
func (e *Engine) promote(r promoteReq) error {
	e.topo.RLock()
	defer e.topo.RUnlock()
	defer e.lockKeys(r.key)()

	ent, seq, err := e.lookup(r.ns, r.key, true)
	if err != nil {
		return err
	}
	if ent == nil {
		// The copy on disk is gone: deleted, expired, or evicted with its
		// bucket while the hot tier still held it. Let the hot tier follow.
		if e.hot != nil {
			e.hot.Delete(r.key)
		}
		return nil
	}
	if seq >= r.ns.promoteBelow.Load() {
		return nil // promoted, or rewritten by a client, in the meantime
	}
	if err := e.store(e.wo, r.ns, r.key, ent); err != nil {
		return err
	}
	r.ns.promotions.Add(1)
	return nil
}
