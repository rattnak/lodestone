// Package doc defines how documents (vector + payload) are laid out inside
// the storage engine's key space, shared by the single-node server and the
// cluster state machine.
package doc

import (
	"encoding/binary"
	"errors"
	"math"
)

// Prefix namespaces document records; client KV keys may not start with 0x00.
var Prefix = []byte("\x00d/")

func Key(id uint64) []byte {
	k := make([]byte, len(Prefix)+8)
	copy(k, Prefix)
	binary.BigEndian.PutUint64(k[len(Prefix):], id)
	return k
}

// IsDocKey reports whether k is a document key, returning its id.
func IsDocKey(k []byte) (uint64, bool) {
	if len(k) != len(Prefix)+8 || string(k[:len(Prefix)]) != string(Prefix) {
		return 0, false
	}
	return binary.BigEndian.Uint64(k[len(Prefix):]), true
}

// Values: dim uvarint | vector [dim]f32 LE | payload (rest).
func Encode(vector []float32, payload []byte) []byte {
	buf := make([]byte, 0, binary.MaxVarintLen64+4*len(vector)+len(payload))
	buf = binary.AppendUvarint(buf, uint64(len(vector)))
	for _, x := range vector {
		buf = binary.LittleEndian.AppendUint32(buf, math.Float32bits(x))
	}
	return append(buf, payload...)
}

func Decode(value []byte) (vector []float32, payload []byte, err error) {
	dim, n := binary.Uvarint(value)
	if n <= 0 || uint64(len(value)-n) < 4*dim {
		return nil, nil, errors.New("corrupt document record")
	}
	vector = make([]float32, dim)
	off := n
	for i := range vector {
		vector[i] = math.Float32frombits(binary.LittleEndian.Uint32(value[off:]))
		off += 4
	}
	return vector, value[off:], nil
}
