// Package hotcache is the RAM tier in front of the storage engine: a
// byte-bounded cache of recently used values, evicting the least recently
// used.
//
// The cache never decides on its own what is current. The storage engine
// keeps it consistent by updating it under the same per-key lock as the
// write to disk.
package hotcache

import (
	"hash/maphash"
	"sync"
	"sync/atomic"
)

const (
	numShards = 64
	// entryOverhead approximates the memory an entry uses beyond its key
	// and value: the node, the map slot and the string header.
	entryOverhead = 96
)

// Cache is safe for concurrent use.
type Cache struct {
	seed   maphash.Seed
	shards [numShards]shard

	hits, misses, evictions atomic.Uint64
}

type shard struct {
	mu       sync.Mutex
	items    map[string]*node
	head     *node // most recently used
	tail     *node // least recently used
	bytes    int64
	capacity int64
}

type node struct {
	key        string
	value      []byte
	expireAt   int64
	seq        uint64
	prev, next *node
}

// New returns a cache that holds about capacity bytes of keys and values.
func New(capacity uint64) *Cache {
	c := &Cache{seed: maphash.MakeSeed()}
	per := int64(capacity / numShards)
	for i := range c.shards {
		c.shards[i].items = make(map[string]*node)
		c.shards[i].capacity = per
	}
	return c
}

func (c *Cache) shard(key []byte) *shard {
	return &c.shards[maphash.Bytes(c.seed, key)%numShards]
}

func cost(key string, value []byte) int64 {
	return int64(len(key) + len(value) + entryOverhead)
}

// Get returns the cached value for key. The caller must not modify it.
// expireAt and seq are whatever Set stored with it; the cache does not
// interpret them.
func (c *Cache) Get(key []byte) (value []byte, expireAt int64, seq uint64, ok bool) {
	s := c.shard(key)
	s.mu.Lock()
	n := s.items[string(key)]
	if n == nil {
		s.mu.Unlock()
		c.misses.Add(1)
		return nil, 0, 0, false
	}
	s.moveToFront(n)
	value, expireAt, seq = n.value, n.expireAt, n.seq
	s.mu.Unlock()
	c.hits.Add(1)
	return value, expireAt, seq, true
}

// Set stores value for key, replacing any previous entry. The cache keeps
// value without copying it, so the caller must not modify it afterwards. A
// value too large to cache is not stored, and any previous entry for the key
// is removed so that it cannot be served stale.
func (c *Cache) Set(key, value []byte, expireAt int64, seq uint64) {
	s := c.shard(key)
	s.mu.Lock()
	defer s.mu.Unlock()

	if n := s.items[string(key)]; n != nil {
		if cost(n.key, value) > s.capacity/2 {
			s.remove(n)
			return
		}
		s.bytes += int64(len(value) - len(n.value))
		n.value, n.expireAt, n.seq = value, expireAt, seq
		s.moveToFront(n)
	} else {
		k := string(key)
		if cost(k, value) > s.capacity/2 {
			return
		}
		n := &node{key: k, value: value, expireAt: expireAt, seq: seq}
		s.items[k] = n
		s.pushFront(n)
		s.bytes += cost(k, value)
	}
	for s.bytes > s.capacity && s.tail != nil {
		s.remove(s.tail)
		c.evictions.Add(1)
	}
}

// Delete removes key from the cache.
func (c *Cache) Delete(key []byte) {
	s := c.shard(key)
	s.mu.Lock()
	if n := s.items[string(key)]; n != nil {
		s.remove(n)
	}
	s.mu.Unlock()
}

func (s *shard) pushFront(n *node) {
	n.prev, n.next = nil, s.head
	if s.head != nil {
		s.head.prev = n
	}
	s.head = n
	if s.tail == nil {
		s.tail = n
	}
}

func (s *shard) unlink(n *node) {
	if n.prev != nil {
		n.prev.next = n.next
	} else {
		s.head = n.next
	}
	if n.next != nil {
		n.next.prev = n.prev
	} else {
		s.tail = n.prev
	}
	n.prev, n.next = nil, nil
}

func (s *shard) moveToFront(n *node) {
	if s.head == n {
		return
	}
	s.unlink(n)
	s.pushFront(n)
}

func (s *shard) remove(n *node) {
	s.unlink(n)
	delete(s.items, n.key)
	s.bytes -= cost(n.key, n.value)
}

// Stats is a snapshot of the cache's counters.
type Stats struct {
	Hits, Misses uint64
	// Evictions counts entries removed to make room.
	Evictions uint64
	Entries   uint64
	Bytes     uint64
	Capacity  uint64
}

// Stats returns current counters and sizes.
func (c *Cache) Stats() Stats {
	st := Stats{Hits: c.hits.Load(), Misses: c.misses.Load(), Evictions: c.evictions.Load()}
	for i := range c.shards {
		s := &c.shards[i]
		s.mu.Lock()
		st.Entries += uint64(len(s.items))
		st.Bytes += uint64(s.bytes)
		st.Capacity += uint64(s.capacity)
		s.mu.Unlock()
	}
	return st
}
