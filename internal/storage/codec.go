package storage

import (
	"encoding/binary"
	"errors"
)

// Every value stored in RocksDB carries a fixed header in front of the
// payload:
//
//	byte 0     format version (currently 1)
//	byte 1     value type (0 = string; other types are reserved)
//	bytes 2-9  expiry as unix milliseconds, big endian; 0 means "never"
//
// The header is what lets expiry be checked on read and during compaction
// without a second lookup.
const (
	formatV1   byte = 1
	typeString byte = 0
	headerLen       = 10
)

var errCorruptValue = errors.New("storage: corrupt value header")

// encodeValue returns header+payload in a new slice.
func encodeValue(payload []byte, expireAt int64) []byte {
	buf := make([]byte, headerLen+len(payload))
	buf[0] = formatV1
	buf[1] = typeString
	binary.BigEndian.PutUint64(buf[2:headerLen], uint64(expireAt))
	copy(buf[headerLen:], payload)
	return buf
}

// decodeValue splits a stored value into its payload and expiry. The payload
// aliases raw.
func decodeValue(raw []byte) (payload []byte, expireAt int64, err error) {
	if len(raw) < headerLen || raw[0] != formatV1 || raw[1] != typeString {
		return nil, 0, errCorruptValue
	}
	exp := int64(binary.BigEndian.Uint64(raw[2:headerLen]))
	if exp < 0 {
		return nil, 0, errCorruptValue
	}
	return raw[headerLen:], exp, nil
}
