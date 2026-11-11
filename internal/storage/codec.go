package storage

import (
	"encoding/binary"
	"errors"
)

// Every value stored in RocksDB carries a fixed header in front of the
// payload:
//
//	byte 0     format version (currently 1)
//	byte 1     value type: 0 = string, 255 = tombstone; others are reserved
//	bytes 2-9  expiry as unix milliseconds, big endian; 0 means "never"
//
// The header is what lets expiry be checked on read and during compaction
// without a second lookup.
//
// A tombstone records that a key was deleted. Cache namespaces need it
// because their data is spread over several buckets (see buckets.go): a
// plain RocksDB delete in the newest bucket would let an older copy of the
// key in an older bucket show through.
const (
	formatV1      byte = 1
	typeString    byte = 0
	typeTombstone byte = 255
	headerLen          = 10
)

var errCorruptValue = errors.New("storage: corrupt value header")

// tombstone is the stored form of a deleted key. It is never modified.
var tombstone = []byte{formatV1, typeTombstone, 0, 0, 0, 0, 0, 0, 0, 0}

// encodeValue returns header+payload in a new slice.
func encodeValue(payload []byte, expireAt int64) []byte {
	buf := make([]byte, headerLen+len(payload))
	buf[0] = formatV1
	buf[1] = typeString
	binary.BigEndian.PutUint64(buf[2:headerLen], uint64(expireAt))
	copy(buf[headerLen:], payload)
	return buf
}

// decoded is a stored value split into its parts.
type decoded struct {
	// payload aliases the raw value.
	payload   []byte
	expireAt  int64
	tombstone bool
}

// live reports whether the value is a string that has not expired at now.
func (d decoded) live(now int64) bool {
	return !d.tombstone && (d.expireAt == 0 || d.expireAt > now)
}

func decodeValue(raw []byte) (decoded, error) {
	if len(raw) < headerLen || raw[0] != formatV1 {
		return decoded{}, errCorruptValue
	}
	switch raw[1] {
	case typeString:
	case typeTombstone:
		return decoded{tombstone: true}, nil
	default:
		return decoded{}, errCorruptValue
	}
	exp := int64(binary.BigEndian.Uint64(raw[2:headerLen]))
	if exp < 0 {
		return decoded{}, errCorruptValue
	}
	return decoded{payload: raw[headerLen:], expireAt: exp}, nil
}
