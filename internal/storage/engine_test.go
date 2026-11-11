package storage

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/linxGnu/grocksdb"
)

// fakeClock is a manually advanced clock.
type fakeClock struct{ ms atomic.Int64 }

func newFakeClock() *fakeClock {
	c := &fakeClock{}
	c.ms.Store(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC).UnixMilli())
	return c
}
func (c *fakeClock) Now() time.Time          { return time.UnixMilli(c.ms.Load()) }
func (c *fakeClock) Advance(d time.Duration) { c.ms.Add(d.Milliseconds()) }

func quietLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

// hotSize is the hot tier size the open helpers use. bothModes runs a test
// with the hot tier off and on, since every command must behave the same
// either way.
var hotSize uint64

func bothModes(t *testing.T, fn func(t *testing.T)) {
	t.Helper()
	for _, mode := range []struct {
		name string
		size uint64
	}{{"hot=off", 0}, {"hot=on", 8 << 20}} {
		t.Run(mode.name, func(t *testing.T) {
			hotSize = mode.size
			defer func() { hotSize = 0 }()
			fn(t)
		})
	}
}

// Tests drive cache maintenance by hand (MaintenanceInterval < 0) so that
// rotation and eviction happen at known points.
func openAt(t *testing.T, dir string, clock *fakeClock, specs ...NamespaceSpec) (*Engine, error) {
	t.Helper()
	o := Options{Dir: dir, Namespaces: specs, WriteBufferSize: 1 << 20, BlockCacheSize: 8 << 20, Logger: quietLogger(),
		HotCacheSize: hotSize, MaintenanceInterval: -1}
	if clock != nil {
		o.Now = clock.Now
	}
	return Open(o)
}

func mustOpen(t *testing.T, dir string, clock *fakeClock, specs ...NamespaceSpec) *Engine {
	t.Helper()
	e, err := openAt(t, dir, clock, specs...)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() {
		if e.db != nil {
			e.Close()
		}
	})
	return e
}

func mustSet(t *testing.T, e *Engine, key, val string, o SetOptions) SetResult {
	t.Helper()
	res, err := e.Set([]byte(key), []byte(val), o, Cache)
	if err != nil {
		t.Fatalf("Set(%q): %v", key, err)
	}
	return res
}

// get returns the value and whether the key exists.
func get(t *testing.T, e *Engine, key string) (string, bool) {
	t.Helper()
	ent, err := e.Get([]byte(key))
	if err != nil {
		t.Fatalf("Get(%q): %v", key, err)
	}
	if ent == nil {
		return "", false
	}
	return string(ent.Value), true
}

func wantValue(t *testing.T, e *Engine, key, want string) {
	t.Helper()
	if got, ok := get(t, e, key); !ok || got != want {
		t.Fatalf("Get(%q) = %q, exists=%v; want %q", key, got, ok, want)
	}
}

func wantMissing(t *testing.T, e *Engine, key string) {
	t.Helper()
	if got, ok := get(t, e, key); ok {
		t.Fatalf("Get(%q) = %q; want missing", key, got)
	}
}

func TestCodecRoundTrip(t *testing.T) {
	for _, payload := range [][]byte{nil, {}, []byte("hello"), bytes.Repeat([]byte{0xff}, 1000)} {
		for _, exp := range []int64{0, 1, 1767225600000, math.MaxInt64} {
			d, err := decodeValue(encodeValue(payload, exp))
			if err != nil || !bytes.Equal(d.payload, payload) || d.expireAt != exp || d.tombstone {
				t.Fatalf("round trip (%d bytes, exp %d) = %+v, %v", len(payload), exp, d, err)
			}
		}
	}
	if d, err := decodeValue(tombstone); err != nil || !d.tombstone || d.live(0) {
		t.Fatalf("decodeValue(tombstone) = %+v, %v", d, err)
	}
	if d, _ := decodeValue(encodeValue(nil, 100)); !d.live(99) || d.live(100) {
		t.Fatal("a value must be live strictly before its expiry and not at it")
	}
	for _, bad := range [][]byte{nil, []byte("short"), append([]byte{9, 0}, make([]byte, 8)...), append([]byte{1, 7}, make([]byte, 8)...)} {
		if _, err := decodeValue(bad); err == nil {
			t.Fatalf("decodeValue(%x) succeeded; want error", bad)
		}
	}
}

func TestSetGetDelete(t *testing.T) { bothModes(t, testSetGetDelete) }

func testSetGetDelete(t *testing.T) {
	e := mustOpen(t, t.TempDir(), nil)

	wantMissing(t, e, "k")
	mustSet(t, e, "k", "v1", SetOptions{})
	wantValue(t, e, "k", "v1")
	mustSet(t, e, "k", "v2", SetOptions{})
	wantValue(t, e, "k", "v2")

	mustSet(t, e, "empty", "", SetOptions{})
	if ent, _ := e.Get([]byte("empty")); ent == nil || ent.Value == nil || len(ent.Value) != 0 {
		t.Fatalf("empty value came back as %+v; want existing entry with empty value", ent)
	}

	n, err := e.Delete([][]byte{[]byte("k"), []byte("k"), []byte("nope"), []byte("empty")}, Cache)
	if err != nil || n != 2 {
		t.Fatalf("Delete = %d, %v; want 2 (duplicates and missing keys do not count)", n, err)
	}
	wantMissing(t, e, "k")
	wantMissing(t, e, "empty")
}

func TestSetConditions(t *testing.T) { bothModes(t, testSetConditions) }

func testSetConditions(t *testing.T) {
	clock := newFakeClock()
	e := mustOpen(t, t.TempDir(), clock)
	exp := clock.Now().Add(time.Minute).UnixMilli()

	if res := mustSet(t, e, "k", "a", SetOptions{XX: true}); res.Written {
		t.Fatal("XX wrote a missing key")
	}
	if res := mustSet(t, e, "k", "a", SetOptions{NX: true, ExpireAt: exp}); !res.Written {
		t.Fatal("NX did not write a missing key")
	}
	if res := mustSet(t, e, "k", "b", SetOptions{NX: true}); res.Written {
		t.Fatal("NX overwrote an existing key")
	}
	wantValue(t, e, "k", "a")

	res := mustSet(t, e, "k", "c", SetOptions{KeepTTL: true, ReturnOld: true})
	if !res.Written || res.Old == nil || string(res.Old.Value) != "a" {
		t.Fatalf("Set with ReturnOld = %+v; want old value a", res)
	}
	if got, ok, _ := e.Meta([]byte("k")); !ok || got != exp {
		t.Fatalf("KeepTTL: expiry = %d, exists=%v; want %d", got, ok, exp)
	}

	mustSet(t, e, "k", "d", SetOptions{})
	if got, _, _ := e.Meta([]byte("k")); got != 0 {
		t.Fatalf("plain Set kept expiry %d; want it cleared", got)
	}

	// Once the key has expired, NX sees it as absent.
	mustSet(t, e, "gone", "x", SetOptions{ExpireAt: exp})
	clock.Advance(2 * time.Minute)
	if res := mustSet(t, e, "gone", "y", SetOptions{NX: true}); !res.Written {
		t.Fatal("NX did not write over an expired key")
	}
	wantValue(t, e, "gone", "y")
}

func TestExpiry(t *testing.T) { bothModes(t, testExpiry) }

func testExpiry(t *testing.T) {
	clock := newFakeClock()
	e := mustOpen(t, t.TempDir(), clock)
	at := func(d time.Duration) int64 { return clock.Now().Add(d).UnixMilli() }

	mustSet(t, e, "k", "v", SetOptions{ExpireAt: at(10 * time.Second)})
	clock.Advance(9999 * time.Millisecond)
	wantValue(t, e, "k", "v")
	clock.Advance(time.Millisecond)
	wantMissing(t, e, "k")
	if _, ok, _ := e.Meta([]byte("k")); ok {
		t.Fatal("Meta reports an expired key as existing")
	}
	if n, _ := e.Delete([][]byte{[]byte("k")}, Cache); n != 0 {
		t.Fatalf("Delete of an expired key = %d; want 0", n)
	}

	// Expire and Persist on a live key.
	mustSet(t, e, "p", "v", SetOptions{})
	if ok, err := e.Expire([]byte("p"), at(time.Minute), Cache); err != nil || !ok {
		t.Fatalf("Expire = %v, %v; want true", ok, err)
	}
	if ok, err := e.Persist([]byte("p"), Cache); err != nil || !ok {
		t.Fatalf("Persist = %v, %v; want true", ok, err)
	}
	if ok, _ := e.Persist([]byte("p"), Cache); ok {
		t.Fatal("Persist on a key without expiry reported a change")
	}
	clock.Advance(time.Hour)
	wantValue(t, e, "p", "v")

	// Expire on a missing key, and with a time in the past.
	if ok, _ := e.Expire([]byte("missing"), at(time.Minute), Cache); ok {
		t.Fatal("Expire reported a missing key as existing")
	}
	if ok, _ := e.Expire([]byte("p"), at(-time.Second), Cache); !ok {
		t.Fatal("Expire in the past did not report the key")
	}
	wantMissing(t, e, "p")

	// A Set whose expiry is already past leaves no key behind.
	mustSet(t, e, "late", "old", SetOptions{})
	mustSet(t, e, "late", "new", SetOptions{ExpireAt: at(-time.Second)})
	wantMissing(t, e, "late")
}

func TestDefaultTTL(t *testing.T) { bothModes(t, testDefaultTTL) }

func testDefaultTTL(t *testing.T) {
	clock := newFakeClock()
	e := mustOpen(t, t.TempDir(), clock,
		NamespaceSpec{Name: "sessions", Prefix: "session:", Profile: Cache, MaxSize: 64 << 20, DefaultTTL: time.Hour})

	mustSet(t, e, "session:a", "v", SetOptions{})
	mustSet(t, e, "session:b", "v", SetOptions{ExpireAt: clock.Now().Add(time.Minute).UnixMilli()})
	mustSet(t, e, "other", "v", SetOptions{})
	if err := e.MSet([][]byte{[]byte("session:c"), []byte("v")}, Cache); err != nil {
		t.Fatal(err)
	}
	if _, err := e.IncrBy([]byte("session:n"), 1, Cache); err != nil {
		t.Fatal(err)
	}

	clock.Advance(2 * time.Minute)
	wantValue(t, e, "session:a", "v")
	wantMissing(t, e, "session:b") // its own, shorter expiry wins
	clock.Advance(time.Hour)
	wantMissing(t, e, "session:a")
	wantMissing(t, e, "session:c")
	wantMissing(t, e, "session:n")
	wantValue(t, e, "other", "v") // the default namespace has no default TTL
}

func TestIncrAppendGetDel(t *testing.T) { bothModes(t, testIncrAppendGetDel) }

func testIncrAppendGetDel(t *testing.T) {
	clock := newFakeClock()
	e := mustOpen(t, t.TempDir(), clock)
	k := []byte("n")

	if n, err := e.IncrBy(k, 5, Cache); err != nil || n != 5 {
		t.Fatalf("IncrBy on missing key = %d, %v; want 5", n, err)
	}
	exp := clock.Now().Add(time.Minute).UnixMilli()
	if _, err := e.Expire(k, exp, Cache); err != nil {
		t.Fatal(err)
	}
	if n, err := e.IncrBy(k, -7, Cache); err != nil || n != -2 {
		t.Fatalf("IncrBy = %d, %v; want -2", n, err)
	}
	if got, _, _ := e.Meta(k); got != exp {
		t.Fatalf("IncrBy changed the expiry to %d; want %d", got, exp)
	}

	mustSet(t, e, "max", fmt.Sprint(int64(math.MaxInt64)), SetOptions{})
	if _, err := e.IncrBy([]byte("max"), 1, Cache); !errors.Is(err, ErrOverflow) {
		t.Fatalf("IncrBy past MaxInt64: err = %v; want ErrOverflow", err)
	}
	mustSet(t, e, "min", fmt.Sprint(int64(math.MinInt64)), SetOptions{})
	if _, err := e.IncrBy([]byte("min"), -1, Cache); !errors.Is(err, ErrOverflow) {
		t.Fatalf("IncrBy below MinInt64: err = %v; want ErrOverflow", err)
	}
	mustSet(t, e, "text", "abc", SetOptions{})
	if _, err := e.IncrBy([]byte("text"), 1, Cache); !errors.Is(err, ErrNotInteger) {
		t.Fatalf("IncrBy on text: err = %v; want ErrNotInteger", err)
	}
	wantValue(t, e, "text", "abc")

	if n, err := e.Append([]byte("s"), []byte("foo"), Cache); err != nil || n != 3 {
		t.Fatalf("Append = %d, %v; want 3", n, err)
	}
	if n, err := e.Append([]byte("s"), []byte("bar"), Cache); err != nil || n != 6 {
		t.Fatalf("Append = %d, %v; want 6", n, err)
	}
	wantValue(t, e, "s", "foobar")

	old, err := e.GetDel([]byte("s"), Cache)
	if err != nil || old == nil || string(old.Value) != "foobar" {
		t.Fatalf("GetDel = %+v, %v; want foobar", old, err)
	}
	wantMissing(t, e, "s")
	if old, _ := e.GetDel([]byte("s"), Cache); old != nil {
		t.Fatalf("GetDel on a missing key = %+v; want nil", old)
	}
}

// Concurrent increments of the same keys must not lose updates.
func TestIncrByIsAtomic(t *testing.T) { bothModes(t, testIncrByIsAtomic) }

func testIncrByIsAtomic(t *testing.T) {
	e := mustOpen(t, t.TempDir(), nil)
	const workers, perWorker, keys = 8, 500, 4

	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < perWorker; i++ {
				if _, err := e.IncrBy([]byte(fmt.Sprintf("ctr%d", i%keys)), 1, Cache); err != nil {
					t.Error(err)
					return
				}
			}
		}()
	}
	wg.Wait()

	for i := 0; i < keys; i++ {
		wantValue(t, e, fmt.Sprintf("ctr%d", i), fmt.Sprint(workers*perWorker/keys))
	}
}

func TestMSetAndDeleteAcrossNamespaces(t *testing.T) { bothModes(t, testMSetAndDeleteAcrossNamespaces) }

func testMSetAndDeleteAcrossNamespaces(t *testing.T) {
	e := mustOpen(t, t.TempDir(), nil,
		NamespaceSpec{Name: "orders", Prefix: "orders:", Profile: Strict})

	pairs := [][]byte{[]byte("a"), []byte("1"), []byte("orders:1"), []byte("2"), []byte("a"), []byte("3")}
	if err := e.MSet(pairs, Cache); err != nil {
		t.Fatal(err)
	}
	wantValue(t, e, "a", "3") // the last write of a repeated key wins
	wantValue(t, e, "orders:1", "2")

	n, err := e.Delete([][]byte{[]byte("a"), []byte("orders:1"), []byte("orders:2")}, Cache)
	if err != nil || n != 2 {
		t.Fatalf("Delete = %d, %v; want 2", n, err)
	}
}

func TestNamespaceRouting(t *testing.T) {
	e := mustOpen(t, t.TempDir(), nil,
		NamespaceSpec{Name: "a", Prefix: "a:", Profile: Safe},
		NamespaceSpec{Name: "ab", Prefix: "a:b:", Profile: Strict},
	)
	for key, want := range map[string]string{
		"a:b:1": "ab",
		"a:b:":  "ab",
		"a:c":   "a",
		"a:":    "a",
		"a":     DefaultNamespace,
		"":      DefaultNamespace,
		"b:a:":  DefaultNamespace,
	} {
		if got := e.NamespaceOf([]byte(key)).Name; got != want {
			t.Errorf("NamespaceOf(%q) = %q; want %q", key, got, want)
		}
	}

	// Each namespace is a separate column family.
	mustSet(t, e, "a:b:1", "v", SetOptions{})
	for _, ns := range e.namespaces {
		s, err := e.db.GetCF(e.ro, ns.buckets[0].cf, []byte("a:b:1"))
		if err != nil {
			t.Fatal(err)
		}
		if found := s.Exists(); found != (ns.spec.Name == "ab") {
			t.Errorf("key a:b:1 present in namespace %q = %v", ns.spec.Name, found)
		}
		s.Free()
	}

	// Durability: the namespace's profile, raised by the connection's floor.
	for _, tc := range []struct {
		key   string
		floor Profile
		sync  bool
	}{
		{"x", Cache, false}, {"x", Safe, false}, {"x", Strict, true},
		{"a:x", Cache, false}, {"a:x", Strict, true},
		{"a:b:x", Cache, true}, {"a:b:x", Safe, true},
	} {
		got := e.writeOpts(e.route([]byte(tc.key)), tc.floor) == e.woSync
		if got != tc.sync {
			t.Errorf("fsync for key %q with floor %s = %v; want %v", tc.key, tc.floor, got, tc.sync)
		}
	}
}

func TestReopen(t *testing.T) { bothModes(t, testReopen) }

func testReopen(t *testing.T) {
	dir := t.TempDir()
	clock := newFakeClock()
	orders := NamespaceSpec{Name: "orders", Prefix: "orders:", Profile: Safe}
	sessions := NamespaceSpec{Name: "sessions", Prefix: "session:", Profile: Cache, MaxSize: 64 << 20}

	e := mustOpen(t, dir, clock, orders, sessions)
	mustSet(t, e, "orders:1", "durable", SetOptions{})
	mustSet(t, e, "session:1", "cached", SetOptions{ExpireAt: clock.Now().Add(time.Hour).UnixMilli()})
	mustSet(t, e, "plain", "default", SetOptions{})
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}

	// Reopen with a configuration that no longer mentions "sessions": the
	// namespace and its data must still be served.
	orders.Profile = Strict // safe -> strict is allowed
	e = mustOpen(t, dir, clock, orders)
	wantValue(t, e, "orders:1", "durable")
	wantValue(t, e, "session:1", "cached")
	wantValue(t, e, "plain", "default")
	if got := e.NamespaceOf([]byte("session:1")); got.Name != "sessions" || got.MaxSize != 64<<20 {
		t.Fatalf("namespace of session:1 after reopen = %+v; want the stored sessions namespace", got)
	}
	if got := e.NamespaceOf([]byte("orders:1")).Profile; got != Strict {
		t.Fatalf("orders profile after reopen = %s; want strict", got)
	}
	clock.Advance(2 * time.Hour)
	wantMissing(t, e, "session:1") // the expiry survived the restart
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}

	// Changes that would lose or endanger existing data are refused.
	for name, spec := range map[string]NamespaceSpec{
		"prefix cannot change":     {Name: "orders", Prefix: "order:", Profile: Strict},
		"profile cannot change":    {Name: "orders", Prefix: "orders:", Profile: Cache, MaxSize: 1 << 20},
		"both claim prefix":        {Name: "orders2", Prefix: "orders:", Profile: Safe},
		"only valid for the cache": {Name: "orders", Prefix: "orders:", Profile: Safe, MaxSize: 1 << 20},
	} {
		if _, err := openAt(t, dir, clock, spec); err == nil || !strings.Contains(err.Error(), name) {
			t.Errorf("Open with %+v: err = %v; want one containing %q", spec, err, name)
		}
	}

	// And a refused Open leaves the database usable.
	e = mustOpen(t, dir, clock)
	wantValue(t, e, "orders:1", "durable")
}

// A new namespace must not hide keys that already exist under its prefix.
func TestNewNamespaceMustNotShadowKeys(t *testing.T) {
	dir := t.TempDir()
	clock := newFakeClock()
	orders := NamespaceSpec{Name: "orders", Prefix: "orders:", Profile: Strict}

	e := mustOpen(t, dir, clock)
	mustSet(t, e, "orders:1", "v", SetOptions{})
	mustSet(t, e, "orders:2", "v", SetOptions{ExpireAt: clock.Now().Add(time.Minute).UnixMilli()})
	mustSet(t, e, "ordersX", "v", SetOptions{})
	e.Close()

	if _, err := openAt(t, dir, clock, orders); err == nil || !strings.Contains(err.Error(), "would become unreachable") {
		t.Fatalf("Open with a shadowing namespace: err = %v; want refusal", err)
	}

	// The refusal must not have recorded the namespace.
	e = mustOpen(t, dir, clock)
	if got := e.NamespaceOf([]byte("orders:1")).Name; got != DefaultNamespace {
		t.Fatalf("orders:1 is in namespace %q after a refused create; want default", got)
	}
	wantValue(t, e, "orders:1", "v")
	if _, err := e.Delete([][]byte{[]byte("orders:1")}, Cache); err != nil {
		t.Fatal(err)
	}
	e.Close()

	// With the live key deleted and the other one expired, nothing is hidden.
	clock.Advance(time.Hour)
	e = mustOpen(t, dir, clock, orders)
	wantMissing(t, e, "orders:1")
	wantValue(t, e, "ordersX", "v")
	mustSet(t, e, "orders:3", "v", SetOptions{})
	if got := e.NamespaceOf([]byte("orders:3")).Name; got != "orders" {
		t.Fatalf("orders:3 is in namespace %q; want orders", got)
	}
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func namespaceStats(t *testing.T, e *Engine, name string) NamespaceStats {
	t.Helper()
	for _, st := range e.Stats() {
		if st.Spec.Name == name {
			return st
		}
	}
	t.Fatalf("no stats for namespace %q", name)
	return NamespaceStats{}
}

// Compaction physically removes expired keys from a durable namespace.
func TestCompactionDropsExpiredKeys(t *testing.T) {
	clock := newFakeClock()
	e := mustOpen(t, t.TempDir(), clock, NamespaceSpec{Name: "d", Prefix: "d:", Profile: Safe})
	ns := e.namespaceByName("d")

	mustSet(t, e, "d:short", "v", SetOptions{ExpireAt: clock.Now().Add(time.Minute).UnixMilli()})
	mustSet(t, e, "d:long", "v", SetOptions{ExpireAt: clock.Now().Add(24 * time.Hour).UnixMilli()})
	mustSet(t, e, "d:forever", "v", SetOptions{})
	clock.Advance(time.Hour)

	stored := func(key string) bool {
		s, err := e.db.GetCF(e.ro, ns.buckets[0].cf, []byte(key))
		if err != nil {
			t.Fatal(err)
		}
		defer s.Free()
		return s.Exists()
	}
	if !stored("d:short") {
		t.Fatal("expired key was removed before any compaction; the test would prove nothing")
	}

	if err := e.Flush("d"); err != nil {
		t.Fatal(err)
	}
	e.db.CompactRangeCF(ns.buckets[0].cf, grocksdb.Range{})

	if stored("d:short") {
		t.Fatal("expired key is still stored after compaction")
	}
	if !stored("d:long") || !stored("d:forever") {
		t.Fatal("compaction removed a key that has not expired")
	}
}

func TestReconcile(t *testing.T) {
	cache := NamespaceSpec{Name: "c", Prefix: "c:", Profile: Cache, MaxSize: 1 << 20}
	def := NamespaceSpec{Name: DefaultNamespace, Profile: Safe}

	merged, created, err := reconcile(nil, []NamespaceSpec{cache})
	if err != nil || len(merged) != 2 || len(created) != 2 {
		t.Fatalf("fresh database: merged=%v created=%v err=%v; want the namespace plus an implicit default", merged, created, err)
	}
	if d := merged[1]; d.Name != DefaultNamespace || d.Profile != Cache || d.MaxSize != DefaultCacheSize {
		t.Fatalf("implicit default namespace = %+v", d)
	}

	merged, created, err = reconcile(nil, []NamespaceSpec{def})
	if err != nil || len(merged) != 1 || merged[0] != def || len(created) != 1 {
		t.Fatalf("explicit default: merged=%v created=%v err=%v", merged, created, err)
	}

	bigger := cache
	bigger.MaxSize = 2 << 20
	merged, created, err = reconcile([]NamespaceSpec{cache, def}, []NamespaceSpec{bigger})
	if err != nil || len(created) != 0 || merged[0].MaxSize != 2<<20 || merged[1] != def {
		t.Fatalf("resize: merged=%v created=%v err=%v", merged, created, err)
	}

	for name, specs := range map[string][]NamespaceSpec{
		"bad name":            {{Name: "Bad Name", Prefix: "x:", Profile: Safe}},
		"empty prefix":        {{Name: "x", Prefix: "", Profile: Safe}},
		"default with prefix": {{Name: DefaultNamespace, Prefix: "x:", Profile: Safe}},
		"cache without size":  {{Name: "x", Prefix: "x:", Profile: Cache}},
		"durable with size":   {{Name: "x", Prefix: "x:", Profile: Strict, MaxSize: 1}},
		"negative ttl":        {{Name: "x", Prefix: "x:", Profile: Safe, DefaultTTL: -1}},
		"duplicate name":      {cache, cache},
		"duplicate prefix":    {cache, {Name: "c2", Prefix: "c:", Profile: Safe}},
	} {
		if _, _, err := reconcile(nil, specs); err == nil {
			t.Errorf("%s: reconcile accepted %+v", name, specs)
		}
	}
}

func TestManifestRoundTrip(t *testing.T) {
	dir := t.TempDir()
	if got, err := loadManifest(dir); err != nil || got != nil {
		t.Fatalf("loadManifest on an empty directory = %v, %v; want nil, nil", got, err)
	}
	want := []NamespaceSpec{
		{Name: "a", Prefix: "a:", Profile: Cache, MaxSize: 5 << 30, DefaultTTL: 90 * time.Second},
		{Name: DefaultNamespace, Profile: Strict},
	}
	if err := saveManifest(dir, want); err != nil {
		t.Fatal(err)
	}
	got, err := loadManifest(dir)
	if err != nil || len(got) != 2 || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("loadManifest = %+v, %v; want %+v", got, err, want)
	}
}
