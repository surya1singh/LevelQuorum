package hotcache

import (
	"bytes"
	"fmt"
	"sync"
	"testing"
)

func TestSetGetDelete(t *testing.T) {
	c := New(1 << 20)
	if _, _, _, ok := c.Get([]byte("k")); ok {
		t.Fatal("Get on an empty cache reported a hit")
	}
	c.Set([]byte("k"), []byte("v1"), 123, 7)
	v, exp, seq, ok := c.Get([]byte("k"))
	if !ok || string(v) != "v1" || exp != 123 || seq != 7 {
		t.Fatalf("Get = %q, %d, %d, %v", v, exp, seq, ok)
	}
	c.Set([]byte("k"), []byte("longer value"), 0, 9)
	v, exp, seq, _ = c.Get([]byte("k"))
	if string(v) != "longer value" || exp != 0 || seq != 9 {
		t.Fatalf("after replace, Get = %q, %d, %d", v, exp, seq)
	}
	if st := c.Stats(); st.Entries != 1 || st.Bytes != uint64(1+len("longer value")+entryOverhead) {
		t.Fatalf("after replace, stats = %+v; a replaced entry must be accounted once", st)
	}
	c.Delete([]byte("k"))
	c.Delete([]byte("k"))
	if _, _, _, ok := c.Get([]byte("k")); ok {
		t.Fatal("Get after Delete reported a hit")
	}
	if st := c.Stats(); st.Entries != 0 || st.Bytes != 0 || st.Hits != 2 || st.Misses != 2 {
		t.Fatalf("final stats = %+v", st)
	}
}

// The cache stays within its capacity and evicts the least recently used
// entries first.
func TestEvictsLeastRecentlyUsed(t *testing.T) {
	// Ordering is per shard, so use twelve keys that share one. Keys are
	// four bytes, and each shard fits ten entries of this size.
	c := New(numShards * 10 * (4 + 100 + entryOverhead))
	target := c.shard([]byte("seed"))
	var keys [][]byte
	for i := 0; len(keys) < 12; i++ {
		if k := []byte(fmt.Sprintf("%04d", i)); c.shard(k) == target {
			keys = append(keys, k)
		}
	}
	val := bytes.Repeat([]byte("x"), 100)

	for _, k := range keys[:10] {
		c.Set(k, val, 0, 0)
	}
	c.Get(keys[0]) // keys[0] is now the most recently used; keys[1] the least
	c.Set(keys[10], val, 0, 0)
	c.Set(keys[11], val, 0, 0)

	for i, k := range keys {
		_, _, _, ok := c.Get(k)
		wantGone := i == 1 || i == 2
		if ok == wantGone {
			t.Errorf("key %d present = %v; want the two least recently used (1 and 2) evicted and the rest kept", i, ok)
		}
	}
	if st := c.Stats(); st.Evictions != 2 {
		t.Errorf("evictions = %d; want 2", st.Evictions)
	}
}

func TestStaysWithinCapacity(t *testing.T) {
	const capacity = 1 << 20
	c := New(capacity)
	val := make([]byte, 1000)
	for i := 0; i < 20000; i++ {
		c.Set([]byte(fmt.Sprintf("key-%d", i)), val, 0, 0)
	}
	st := c.Stats()
	if st.Bytes > capacity || st.Entries == 0 || st.Evictions == 0 {
		t.Fatalf("stats after overfilling = %+v; want bytes within %d", st, capacity)
	}
	if _, _, _, ok := c.Get([]byte("key-19999")); !ok {
		t.Fatal("the most recently set key was evicted")
	}
}

// A value too big to cache must not leave an older value behind.
func TestOversizedValueRemovesOldEntry(t *testing.T) {
	c := New(numShards * 1000)
	c.Set([]byte("k"), []byte("small"), 0, 0)
	c.Set([]byte("k"), make([]byte, 5000), 0, 0)
	if v, _, _, ok := c.Get([]byte("k")); ok {
		t.Fatalf("Get returned a stale %d-byte value after an oversized Set", len(v))
	}
	c.Set([]byte("other"), make([]byte, 5000), 0, 0)
	if st := c.Stats(); st.Entries != 0 || st.Bytes != 0 {
		t.Fatalf("stats = %+v; oversized values must not be stored", st)
	}
}

func TestZeroCapacityStoresNothing(t *testing.T) {
	c := New(0)
	c.Set([]byte("k"), []byte("v"), 0, 0)
	if _, _, _, ok := c.Get([]byte("k")); ok {
		t.Fatal("a zero-capacity cache returned a hit")
	}
}

func TestConcurrentUse(t *testing.T) {
	c := New(256 << 10)
	var wg sync.WaitGroup
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < 5000; i++ {
				k := []byte(fmt.Sprintf("k%d", (i*7+w)%500))
				switch i % 3 {
				case 0:
					c.Set(k, k, 0, uint64(i))
				case 1:
					if v, _, _, ok := c.Get(k); ok && !bytes.Equal(v, k) {
						t.Errorf("Get(%s) = %s", k, v)
						return
					}
				default:
					c.Delete(k)
				}
			}
		}(w)
	}
	wg.Wait()
	if st := c.Stats(); st.Bytes > st.Capacity {
		t.Fatalf("stats = %+v; over capacity", st)
	}
}
