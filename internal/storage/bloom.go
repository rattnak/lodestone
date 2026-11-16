package storage

// A bloom filter answers "might this table contain the key?" with no false
// negatives, letting reads skip SSTables that definitely lack a key — the
// standard fix for the LSM read-amplification problem: without it, a miss
// must search every table.
//
// Bits are set by double hashing: g_i = h1 + i*h2, which approximates k
// independent hash functions from two. Hashes must be stable across runs
// and machines because filters are persisted inside SSTables.

const bloomBitsPerKey = 10 // ~1% false-positive rate at k=7

type bloomFilter struct {
	bits []uint64
	k    uint32
}

func newBloomFilter(numKeys int) *bloomFilter {
	m := uint64(numKeys) * bloomBitsPerKey
	if m < 64 {
		m = 64
	}
	// k = ln2 * bits-per-key minimizes the false-positive rate.
	return &bloomFilter{bits: make([]uint64, (m+63)/64), k: 7}
}

func (f *bloomFilter) nbits() uint64 { return uint64(len(f.bits)) * 64 }

func (f *bloomFilter) add(h1, h2 uint64) {
	for i := uint64(0); i < uint64(f.k); i++ {
		bit := (h1 + i*h2) % f.nbits()
		f.bits[bit/64] |= 1 << (bit % 64)
	}
}

func (f *bloomFilter) mayContain(key []byte) bool {
	h1, h2 := bloomHash(key)
	for i := uint64(0); i < uint64(f.k); i++ {
		bit := (h1 + i*h2) % f.nbits()
		if f.bits[bit/64]&(1<<(bit%64)) == 0 {
			return false
		}
	}
	return true
}

// bloomHash returns the two base hashes for double hashing: FNV-1a, then a
// splitmix64 finalizer to derive an independent second hash. h2 is forced odd
// so successive probes cycle through distinct bits.
func bloomHash(key []byte) (h1, h2 uint64) {
	const (
		fnvOffset = 14695981039346656037
		fnvPrime  = 1099511628211
	)
	h1 = fnvOffset
	for _, b := range key {
		h1 ^= uint64(b)
		h1 *= fnvPrime
	}
	h2 = h1
	h2 ^= h2 >> 30
	h2 *= 0xbf58476d1ce4e5b9
	h2 ^= h2 >> 27
	h2 *= 0x94d049bb133111eb
	h2 ^= h2 >> 31
	h2 |= 1
	return h1, h2
}
