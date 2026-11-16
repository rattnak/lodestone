package index

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"math/rand"
	"os"
)

// Snapshot format, little-endian:
//
//	magic u32 | version u8 | dim uvarint | M uvarint | efCons uvarint |
//	entry varint | maxLevel uvarint | live uvarint | nodeCount uvarint
//	per node: id uvarint | deleted u8 | vec [dim]f32 |
//	          levelCount uvarint | per level: n uvarint, neighbors [n]varint
//
// The distance function cannot be serialized; Load takes Options and the
// caller must supply the same metric the index was built with.
const (
	snapMagic   = 0x484e5357 // "HNSW"
	snapVersion = 1
)

// Save writes a snapshot of the index. Concurrent reads continue; writes are
// blocked for the duration.
func (ix *Index) Save(w io.Writer) error {
	ix.mu.RLock()
	defer ix.mu.RUnlock()
	bw := bufio.NewWriter(w)
	var scratch [binary.MaxVarintLen64]byte
	u := func(v uint64) error {
		n := binary.PutUvarint(scratch[:], v)
		_, err := bw.Write(scratch[:n])
		return err
	}
	s := func(v int64) error {
		n := binary.PutVarint(scratch[:], v)
		_, err := bw.Write(scratch[:n])
		return err
	}

	var head [5]byte
	binary.LittleEndian.PutUint32(head[0:4], snapMagic)
	head[4] = snapVersion
	if _, err := bw.Write(head[:]); err != nil {
		return err
	}
	for _, v := range []uint64{uint64(ix.dim), uint64(ix.opts.M), uint64(ix.opts.EfConstruction)} {
		if err := u(v); err != nil {
			return err
		}
	}
	if err := s(int64(ix.entry)); err != nil {
		return err
	}
	for _, v := range []uint64{uint64(ix.maxLevel), uint64(ix.live), uint64(len(ix.nodes))} {
		if err := u(v); err != nil {
			return err
		}
	}

	var f32 [4]byte
	for i := range ix.nodes {
		n := &ix.nodes[i]
		if err := u(n.id); err != nil {
			return err
		}
		del := byte(0)
		if n.deleted {
			del = 1
		}
		if err := bw.WriteByte(del); err != nil {
			return err
		}
		for _, x := range n.vec {
			binary.LittleEndian.PutUint32(f32[:], math.Float32bits(x))
			if _, err := bw.Write(f32[:]); err != nil {
				return err
			}
		}
		if err := u(uint64(len(n.links))); err != nil {
			return err
		}
		for _, level := range n.links {
			if err := u(uint64(len(level))); err != nil {
				return err
			}
			for _, nb := range level {
				if err := s(int64(nb)); err != nil {
					return err
				}
			}
		}
	}
	return bw.Flush()
}

// Load reads a snapshot written by Save. opts.Distance must be the same
// metric the index was built with; other fields are taken from the snapshot.
func Load(r io.Reader, opts Options) (*Index, error) {
	br := bufio.NewReader(r)
	var head [5]byte
	if _, err := io.ReadFull(br, head[:]); err != nil {
		return nil, err
	}
	if binary.LittleEndian.Uint32(head[0:4]) != snapMagic {
		return nil, errors.New("index: bad snapshot magic")
	}
	if head[4] != snapVersion {
		return nil, fmt.Errorf("index: unsupported snapshot version %d", head[4])
	}
	u := func() (uint64, error) { return binary.ReadUvarint(br) }
	s := func() (int64, error) { return binary.ReadVarint(br) }

	dim, err := u()
	if err != nil {
		return nil, err
	}
	m, err := u()
	if err != nil {
		return nil, err
	}
	efc, err := u()
	if err != nil {
		return nil, err
	}
	opts.M, opts.EfConstruction = int(m), int(efc)
	ix := New(opts)
	ix.dim = int(dim)

	entry, err := s()
	if err != nil {
		return nil, err
	}
	maxLevel, err := u()
	if err != nil {
		return nil, err
	}
	live, err := u()
	if err != nil {
		return nil, err
	}
	count, err := u()
	if err != nil {
		return nil, err
	}
	if entry < -1 || entry >= int64(count) {
		return nil, errors.New("index: snapshot entry point out of range")
	}
	ix.entry = int32(entry)
	ix.maxLevel = int(maxLevel)
	ix.live = int(live)

	ix.nodes = make([]node, count)
	for i := range ix.nodes {
		n := &ix.nodes[i]
		if n.id, err = u(); err != nil {
			return nil, err
		}
		del, err := br.ReadByte()
		if err != nil {
			return nil, err
		}
		n.deleted = del == 1
		n.vec = make([]float32, dim)
		var f32 [4]byte
		for j := range n.vec {
			if _, err := io.ReadFull(br, f32[:]); err != nil {
				return nil, err
			}
			n.vec[j] = math.Float32frombits(binary.LittleEndian.Uint32(f32[:]))
		}
		levels, err := u()
		if err != nil {
			return nil, err
		}
		if levels > 64 {
			return nil, errors.New("index: snapshot level count out of range")
		}
		n.links = make([][]int32, levels)
		for l := range n.links {
			ln, err := u()
			if err != nil {
				return nil, err
			}
			if ln > count {
				return nil, errors.New("index: snapshot neighbor count out of range")
			}
			n.links[l] = make([]int32, ln)
			for j := range n.links[l] {
				nb, err := s()
				if err != nil {
					return nil, err
				}
				if nb < 0 || nb >= int64(count) {
					return nil, errors.New("index: snapshot neighbor out of range")
				}
				n.links[l][j] = int32(nb)
			}
		}
		if _, dup := ix.byID[n.id]; dup {
			return nil, fmt.Errorf("index: snapshot has duplicate id %d", n.id)
		}
		ix.byID[n.id] = int32(i)
	}
	ix.rng = rand.New(rand.NewSource(rand.Int63()))
	return ix, nil
}

// SaveFile snapshots to path atomically: temp file, fsync, rename.
func (ix *Index) SaveFile(path string) (err error) {
	tmp := path + ".tmp"
	f, err := os.Create(tmp)
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			f.Close()
			os.Remove(tmp)
		}
	}()
	if err = ix.Save(f); err != nil {
		return err
	}
	if err = f.Sync(); err != nil {
		return err
	}
	if err = f.Close(); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

// LoadFile reads a snapshot from path; see Load.
func LoadFile(path string, opts Options) (*Index, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	ix, err := Load(f, opts)
	if err != nil {
		return nil, fmt.Errorf("index snapshot %s: %w", path, err)
	}
	return ix, nil
}
