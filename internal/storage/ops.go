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
	Value []byte
	// ExpireAt is unix milliseconds; 0 means the key never expires.
	ExpireAt int64
}

// read returns the live entry for key, or nil if it is missing or expired.
// With copyValue false the returned entry has a nil Value.
func (e *Engine) read(ns *namespace, key []byte, copyValue bool) (*Entry, error) {
	s, err := e.db.GetCF(e.ro, ns.cf, key)
	if err != nil {
		return nil, err
	}
	defer s.Free()
	if !s.Exists() {
		return nil, nil
	}
	payload, exp, err := decodeValue(s.Data())
	if err != nil {
		return nil, fmt.Errorf("%w (key %q)", err, key)
	}
	if exp != 0 && exp <= e.now() {
		// Left in place on purpose: deleting here could race with a
		// concurrent write of a fresh value. Compaction or eviction removes
		// it.
		return nil, nil
	}
	ent := &Entry{ExpireAt: exp}
	if copyValue {
		ent.Value = bytes.Clone(payload)
		if ent.Value == nil {
			ent.Value = []byte{}
		}
	}
	return ent, nil
}

// put stores ent under key, or deletes key if ent is nil or already expired.
func (e *Engine) put(wo *grocksdb.WriteOptions, ns *namespace, key []byte, ent *Entry) error {
	if ent == nil || (ent.ExpireAt != 0 && ent.ExpireAt <= e.now()) {
		return e.db.DeleteCF(wo, ns.cf, key)
	}
	return e.db.PutCF(wo, ns.cf, key, encodeValue(ent.Value, ent.ExpireAt))
}

// update is the read-modify-write primitive. fn receives the live entry (nil
// if absent) and returns the entry to store (nil to delete) and whether
// anything changed.
func (e *Engine) update(key []byte, floor Profile, fn func(cur *Entry) (next *Entry, changed bool, err error)) error {
	ns := e.route(key)
	defer e.lockKeys(key)()
	cur, err := e.read(ns, key, true)
	if err != nil {
		return err
	}
	next, changed, err := fn(cur)
	if err != nil || !changed {
		return err
	}
	return e.put(e.writeOpts(ns, floor), ns, key, next)
}

// Get returns the live entry for key, or nil.
func (e *Engine) Get(key []byte) (*Entry, error) {
	return e.read(e.route(key), key, true)
}

// Meta reports whether key exists and when it expires, without copying its
// value.
func (e *Engine) Meta(key []byte) (expireAt int64, exists bool, err error) {
	ent, err := e.read(e.route(key), key, false)
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
// above the namespace's profile; pass Cache for no change.
func (e *Engine) Set(key, value []byte, o SetOptions, floor Profile) (SetResult, error) {
	ns := e.route(key)
	defer e.lockKeys(key)()

	var res SetResult
	var cur *Entry
	if o.NX || o.XX || o.KeepTTL || o.ReturnOld {
		var err error
		if cur, err = e.read(ns, key, o.ReturnOld); err != nil {
			return res, err
		}
		if o.ReturnOld {
			res.Old = cur
		}
		if (o.NX && cur != nil) || (o.XX && cur == nil) {
			return res, nil
		}
	}

	ent := &Entry{Value: value, ExpireAt: o.ExpireAt}
	switch {
	case o.KeepTTL && cur != nil:
		ent.ExpireAt = cur.ExpireAt
	case ent.ExpireAt == 0 && ns.spec.DefaultTTL > 0:
		ent.ExpireAt = e.now() + ns.spec.DefaultTTL.Milliseconds()
	}
	if err := e.put(e.writeOpts(ns, floor), ns, key, ent); err != nil {
		return res, err
	}
	res.Written = true
	return res, nil
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
	defer e.lockKeys(keys...)()

	batch := grocksdb.NewWriteBatch()
	defer batch.Destroy()
	wo := e.wo
	now := e.now()
	for i := 0; i < len(pairs); i += 2 {
		ns := e.route(pairs[i])
		var exp int64
		if ns.spec.DefaultTTL > 0 {
			exp = now + ns.spec.DefaultTTL.Milliseconds()
		}
		batch.PutCF(ns.cf, pairs[i], encodeValue(pairs[i+1], exp))
		if e.writeOpts(ns, floor) == e.woSync {
			wo = e.woSync
		}
	}
	return e.db.Write(wo, batch)
}

// Delete removes keys and returns how many of them existed.
func (e *Engine) Delete(keys [][]byte, floor Profile) (int, error) {
	defer e.lockKeys(keys...)()

	batch := grocksdb.NewWriteBatch()
	defer batch.Destroy()
	wo := e.wo
	seen := make(map[string]struct{}, len(keys))
	for _, k := range keys {
		if _, dup := seen[string(k)]; dup {
			continue
		}
		seen[string(k)] = struct{}{}
		ns := e.route(k)
		ent, err := e.read(ns, k, false)
		if err != nil {
			return 0, err
		}
		if ent == nil {
			continue
		}
		batch.DeleteCF(ns.cf, k)
		if e.writeOpts(ns, floor) == e.woSync {
			wo = e.woSync
		}
	}
	n := batch.Count()
	if n == 0 {
		return 0, nil
	}
	return n, e.db.Write(wo, batch)
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
		cur.ExpireAt = expireAt
		return cur, true, nil
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
		cur.ExpireAt = 0
		return cur, true, nil
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
		if cur != nil {
			next.Value, next.ExpireAt = cur.Value, cur.ExpireAt
		} else if ns.spec.DefaultTTL > 0 {
			next.ExpireAt = e.now() + ns.spec.DefaultTTL.Milliseconds()
		}
		next.Value = append(next.Value, data...)
		n = len(next.Value)
		return next, true, nil
	})
	return n, err
}
