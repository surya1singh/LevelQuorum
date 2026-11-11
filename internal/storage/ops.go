package storage

import (
	"bytes"
	"errors"
	"fmt"
	"math"
	"strconv"

	"github.com/linxGnu/grocksdb"
)

// Errors returned by commands that interpret the value.
var (
	ErrNotInteger = errors.New("value is not an integer or out of range")
	ErrOverflow   = errors.New("increment or decrement would overflow")
)

// Entry is a stored value with its expiry.
type Entry struct {
	// Value may be shared with the hot tier. Callers must not modify it.
	Value []byte
	// ExpireAt is unix milliseconds; 0 means the key never expires.
	ExpireAt int64
}

// lookup reads key from RocksDB: the newest bucket that knows the key
// decides. It returns the live entry, or nil if the key is missing, deleted
// or expired, together with the seq of the bucket that held it. With
// copyValue false the entry's Value is nil. The caller holds topo.
func (e *Engine) lookup(ns *namespace, key []byte, copyValue bool) (*Entry, uint64, error) {
	now := e.now()
	for _, b := range ns.buckets {
		s, err := e.db.GetCF(e.ro, b.cf, key)
		if err != nil {
			return nil, 0, err
		}
		if !s.Exists() {
			s.Free()
			continue
		}
		d, err := decodeValue(s.Data())
		if err != nil {
			s.Free()
			return nil, 0, fmt.Errorf("%w (key %q)", err, key)
		}
		// An expired value is left in place on purpose: deleting it here
		// could race with a concurrent write of a fresh value.
		var ent *Entry
		if d.live(now) {
			ent = &Entry{ExpireAt: d.expireAt}
			if copyValue {
				ent.Value = append(make([]byte, 0, len(d.payload)), d.payload...)
			}
		}
		s.Free()
		return ent, b.seq, nil
	}
	return nil, 0, nil
}

// hotGet consults the hot tier. hit means a live entry was found; expired
// means an entry was found but its time has passed.
func (e *Engine) hotGet(key []byte) (ent *Entry, seq uint64, hit, expired bool) {
	if e.hot == nil {
		return nil, 0, false, false
	}
	v, exp, seq, ok := e.hot.Get(key)
	if !ok {
		return nil, 0, false, false
	}
	if exp != 0 && exp <= e.now() {
		return nil, 0, false, true
	}
	return &Entry{Value: v, ExpireAt: exp}, seq, true, false
}

// find returns the live entry for key from the hot tier or, failing that,
// from RocksDB, in which case the hot tier is filled. The caller holds topo
// and the key's stripe; holding the stripe is what makes filling the hot
// tier safe against a concurrent write.
func (e *Engine) find(ns *namespace, key []byte, wantValue bool) (*Entry, uint64, error) {
	ent, seq, hit, expired := e.hotGet(key)
	if hit {
		return ent, seq, nil
	}
	if expired {
		e.hot.Delete(key)
	}
	ent, seq, err := e.lookup(ns, key, wantValue)
	if err == nil && ent != nil && wantValue && e.hot != nil {
		e.hot.Set(key, ent.Value, ent.ExpireAt, seq)
	}
	return ent, seq, err
}

// read is the entry point for commands that only read: a lock-free look at
// the hot tier, then RocksDB under the key's stripe.
func (e *Engine) read(key []byte, wantValue bool) (*Entry, error) {
	ns := e.route(key)
	ent, seq, hit, expired := e.hotGet(key)
	if !hit {
		e.topo.RLock()
		mu := e.stripe(key)
		mu.RLock()
		if expired {
			e.hot.Delete(key)
		}
		var err error
		ent, seq, err = e.lookup(ns, key, wantValue)
		if err == nil && ent != nil && wantValue && e.hot != nil {
			e.hot.Set(key, ent.Value, ent.ExpireAt, seq)
		}
		mu.RUnlock()
		e.topo.RUnlock()
		if err != nil {
			return nil, err
		}
	}
	if ent != nil {
		e.maybePromote(ns, key, seq)
	}
	return ent, nil
}

// store writes ent under key into the namespace's newest bucket, or records
// a deletion if ent is nil or already expired, and brings the hot tier in
// line. ent.Value is kept by the hot tier, so it must not be modified
// afterwards. The caller holds topo and the key's stripe for writing.
func (e *Engine) store(wo *grocksdb.WriteOptions, ns *namespace, key []byte, ent *Entry) error {
	b := ns.buckets[0]
	live := ent != nil && (ent.ExpireAt == 0 || ent.ExpireAt > e.now())

	var err error
	switch {
	case live:
		err = e.db.PutCF(wo, b.cf, key, encodeValue(ent.Value, ent.ExpireAt))
		e.noteWrite(ns, len(key)+headerLen+len(ent.Value))
	case ns.spec.Profile == Cache:
		err = e.db.PutCF(wo, b.cf, key, tombstone)
		e.noteWrite(ns, len(key)+headerLen)
	default:
		err = e.db.DeleteCF(wo, b.cf, key)
	}
	if e.hot != nil {
		if err == nil && live {
			e.hot.Set(key, ent.Value, ent.ExpireAt, b.seq)
		} else {
			e.hot.Delete(key)
		}
	}
	return err
}

// update is the read-modify-write primitive. fn receives the live entry (nil
// if absent), which it must not modify, and returns the entry to store (nil
// to delete) and whether anything changed.
func (e *Engine) update(key []byte, floor Profile, fn func(cur *Entry) (next *Entry, changed bool, err error)) error {
	ns := e.route(key)
	e.topo.RLock()
	defer e.topo.RUnlock()
	defer e.lockKeys(key)()

	cur, _, err := e.find(ns, key, true)
	if err != nil {
		return err
	}
	next, changed, err := fn(cur)
	if err != nil || !changed {
		return err
	}
	return e.store(e.writeOpts(ns, floor), ns, key, next)
}

// Get returns the live entry for key, or nil.
func (e *Engine) Get(key []byte) (*Entry, error) {
	return e.read(key, true)
}

// Meta reports whether key exists and when it expires, without reading its
// value from disk.
func (e *Engine) Meta(key []byte) (expireAt int64, exists bool, err error) {
	ent, err := e.read(key, false)
	if err != nil || ent == nil {
		return 0, false, err
	}
	return ent.ExpireAt, true, nil
}

// SetOptions modifies Set.
type SetOptions struct {
	// ExpireAt is the absolute expiry in unix milliseconds; 0 means none, in
	// which case the namespace's default TTL applies.
	ExpireAt int64
	// KeepTTL keeps the expiry of the value being replaced.
	KeepTTL bool
	// NX writes only if the key is absent, XX only if it is present.
	NX, XX bool
	// ReturnOld asks for the previous entry in SetResult.Old.
	ReturnOld bool
}

// SetResult reports what Set did.
type SetResult struct {
	Written bool
	Old     *Entry
}

// Set stores value under key. floor raises the durability of this write
// above the namespace's profile; pass Cache for no change. Set copies value
// if it needs to keep it.
func (e *Engine) Set(key, value []byte, o SetOptions, floor Profile) (SetResult, error) {
	ns := e.route(key)
	e.topo.RLock()
	defer e.topo.RUnlock()
	defer e.lockKeys(key)()

	var res SetResult
	var cur *Entry
	if o.NX || o.XX || o.KeepTTL || o.ReturnOld {
		var err error
		if cur, _, err = e.find(ns, key, o.ReturnOld); err != nil {
			return res, err
		}
		if o.ReturnOld {
			res.Old = cur
		}
		if (o.NX && cur != nil) || (o.XX && cur == nil) {
			return res, nil
		}
	}

	ent := &Entry{Value: e.own(value), ExpireAt: o.ExpireAt}
	switch {
	case o.KeepTTL && cur != nil:
		ent.ExpireAt = cur.ExpireAt
	case ent.ExpireAt == 0 && ns.spec.DefaultTTL > 0:
		ent.ExpireAt = e.now() + ns.spec.DefaultTTL.Milliseconds()
	}
	if err := e.store(e.writeOpts(ns, floor), ns, key, ent); err != nil {
		return res, err
	}
	res.Written = true
	return res, nil
}

// own returns a value the hot tier may keep: a copy of the caller's slice,
// or the slice itself when there is no hot tier to keep it.
func (e *Engine) own(value []byte) []byte {
	if e.hot == nil {
		return value
	}
	return append(make([]byte, 0, len(value)), value...)
}

// MSet stores all pairs (key, value, key, value, ...) atomically.
func (e *Engine) MSet(pairs [][]byte, floor Profile) error {
	if len(pairs)%2 != 0 {
		return errors.New("storage: MSet needs key/value pairs")
	}
	keys := make([][]byte, 0, len(pairs)/2)
	for i := 0; i < len(pairs); i += 2 {
		keys = append(keys, pairs[i])
	}
	e.topo.RLock()
	defer e.topo.RUnlock()
	defer e.lockKeys(keys...)()

	batch := grocksdb.NewWriteBatch()
	defer batch.Destroy()
	wo := e.wo
	now := e.now()
	type written struct {
		exp int64
		seq uint64
	}
	done := make([]written, len(keys))
	for i, key := range keys {
		ns := e.route(key)
		b := ns.buckets[0]
		var exp int64
		if ns.spec.DefaultTTL > 0 {
			exp = now + ns.spec.DefaultTTL.Milliseconds()
		}
		batch.PutCF(b.cf, key, encodeValue(pairs[2*i+1], exp))
		e.noteWrite(ns, len(key)+headerLen+len(pairs[2*i+1]))
		done[i] = written{exp, b.seq}
		if e.writeOpts(ns, floor) == e.woSync {
			wo = e.woSync
		}
	}
	err := e.db.Write(wo, batch)
	if e.hot != nil {
		for i, key := range keys {
			if err == nil {
				e.hot.Set(key, e.own(pairs[2*i+1]), done[i].exp, done[i].seq)
			} else {
				e.hot.Delete(key)
			}
		}
	}
	return err
}

// Delete removes keys and returns how many of them existed.
func (e *Engine) Delete(keys [][]byte, floor Profile) (int, error) {
	e.topo.RLock()
	defer e.topo.RUnlock()
	defer e.lockKeys(keys...)()

	batch := grocksdb.NewWriteBatch()
	defer batch.Destroy()
	wo := e.wo
	seen := make(map[string]struct{}, len(keys))
	var deleted [][]byte
	for _, k := range keys {
		if _, dup := seen[string(k)]; dup {
			continue
		}
		seen[string(k)] = struct{}{}
		ns := e.route(k)
		ent, _, err := e.find(ns, k, false)
		if err != nil {
			return 0, err
		}
		if ent == nil {
			continue
		}
		if ns.spec.Profile == Cache {
			batch.PutCF(ns.buckets[0].cf, k, tombstone)
			e.noteWrite(ns, len(k)+headerLen)
		} else {
			batch.DeleteCF(ns.buckets[0].cf, k)
		}
		deleted = append(deleted, k)
		if e.writeOpts(ns, floor) == e.woSync {
			wo = e.woSync
		}
	}
	if len(deleted) == 0 {
		return 0, nil
	}
	err := e.db.Write(wo, batch)
	if e.hot != nil {
		for _, k := range deleted {
			e.hot.Delete(k)
		}
	}
	return len(deleted), err
}

// GetDel removes key and returns the entry it held, or nil.
func (e *Engine) GetDel(key []byte, floor Profile) (*Entry, error) {
	var old *Entry
	err := e.update(key, floor, func(cur *Entry) (*Entry, bool, error) {
		old = cur
		return nil, cur != nil, nil
	})
	return old, err
}

// Expire sets the absolute expiry of an existing key and reports whether the
// key existed. A time in the past deletes the key.
func (e *Engine) Expire(key []byte, expireAt int64, floor Profile) (bool, error) {
	found := false
	err := e.update(key, floor, func(cur *Entry) (*Entry, bool, error) {
		if cur == nil {
			return nil, false, nil
		}
		found = true
		if expireAt <= e.now() {
			return nil, true, nil
		}
		return &Entry{Value: cur.Value, ExpireAt: expireAt}, true, nil
	})
	return found, err
}

// Persist removes the expiry of key and reports whether there was one.
func (e *Engine) Persist(key []byte, floor Profile) (bool, error) {
	changed := false
	err := e.update(key, floor, func(cur *Entry) (*Entry, bool, error) {
		if cur == nil || cur.ExpireAt == 0 {
			return nil, false, nil
		}
		changed = true
		return &Entry{Value: cur.Value}, true, nil
	})
	return changed, err
}

// IncrBy adds delta to the integer stored at key (0 if absent), keeping its
// expiry, and returns the new value.
func (e *Engine) IncrBy(key []byte, delta int64, floor Profile) (int64, error) {
	var out int64
	ns := e.route(key)
	err := e.update(key, floor, func(cur *Entry) (*Entry, bool, error) {
		var n int64
		next := &Entry{}
		if cur != nil {
			var perr error
			if n, perr = strconv.ParseInt(string(cur.Value), 10, 64); perr != nil {
				return nil, false, ErrNotInteger
			}
			next.ExpireAt = cur.ExpireAt
		} else if ns.spec.DefaultTTL > 0 {
			next.ExpireAt = e.now() + ns.spec.DefaultTTL.Milliseconds()
		}
		if (delta > 0 && n > math.MaxInt64-delta) || (delta < 0 && n < math.MinInt64-delta) {
			return nil, false, ErrOverflow
		}
		out = n + delta
		next.Value = strconv.AppendInt(nil, out, 10)
		return next, true, nil
	})
	return out, err
}

// Append appends data to the value at key (creating it if absent), keeping
// its expiry, and returns the new length.
func (e *Engine) Append(key, data []byte, floor Profile) (int, error) {
	var n int
	ns := e.route(key)
	err := e.update(key, floor, func(cur *Entry) (*Entry, bool, error) {
		next := &Entry{}
		var old []byte
		if cur != nil {
			old, next.ExpireAt = cur.Value, cur.ExpireAt
		} else if ns.spec.DefaultTTL > 0 {
			next.ExpireAt = e.now() + ns.spec.DefaultTTL.Milliseconds()
		}
		// A new slice: cur.Value may be shared with the hot tier.
		next.Value = bytes.Join([][]byte{old, data}, nil)
		n = len(next.Value)
		return next, true, nil
	})
	return n, err
}
