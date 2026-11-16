package storage

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"iter"
	"os"
	"sort"
)

// SSTable file format:
//
//	data section:   entries in ascending key order, each
//	                  flags   byte     // bit 0: tombstone
//	                  keyLen  uvarint
//	                  key     [keyLen]byte
//	                  valLen  uvarint  // 0 for tombstones
//	                  value   [valLen]byte
//	index section:  count uvarint, then per sparse entry
//	                  keyLen uvarint | key | offset uvarint
//	footer (20B):   indexOffset uint64 | entryCount uint64 | magic uint32
//
// The sparse index holds every sstIndexInterval-th key (always including the
// first), so a lookup binary-searches the in-memory index and scans at most
// one interval of the data section.
const (
	sstMagic         = 0x4c4f4445 // "LODE"
	sstIndexInterval = 16
	sstFooterSize    = 20
)

var ErrNotSorted = errors.New("sstable: entries not in strictly ascending key order")

type sstIndexEntry struct {
	key    []byte
	offset int64
}

// WriteSSTable writes entries (which must be in strictly ascending key order,
// as Memtable.All yields them) to an immutable table at path. The file is
// written to a temp name, synced, then renamed so a crash never leaves a
// partial table at path.
func WriteSSTable(path string, entries iter.Seq2[[]byte, Entry]) (err error) {
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

	w := bufio.NewWriter(f)
	var (
		offset  int64
		count   uint64
		index   []sstIndexEntry
		prevKey []byte
		scratch [binary.MaxVarintLen64]byte
	)
	writeUvarint := func(v uint64) error {
		n := binary.PutUvarint(scratch[:], v)
		_, werr := w.Write(scratch[:n])
		offset += int64(n)
		return werr
	}

	for key, ent := range entries {
		if count > 0 && bytes.Compare(prevKey, key) >= 0 {
			return fmt.Errorf("%w: %q then %q", ErrNotSorted, prevKey, key)
		}
		if count%sstIndexInterval == 0 {
			index = append(index, sstIndexEntry{key: append([]byte(nil), key...), offset: offset})
		}
		var flags byte
		value := ent.Value
		if ent.Tombstone {
			flags, value = 1, nil
		}
		if werr := w.WriteByte(flags); werr != nil {
			return werr
		}
		offset++
		if werr := writeUvarint(uint64(len(key))); werr != nil {
			return werr
		}
		if _, werr := w.Write(key); werr != nil {
			return werr
		}
		offset += int64(len(key))
		if werr := writeUvarint(uint64(len(value))); werr != nil {
			return werr
		}
		if _, werr := w.Write(value); werr != nil {
			return werr
		}
		offset += int64(len(value))
		prevKey = append(prevKey[:0], key...)
		count++
	}
	if count == 0 {
		return errors.New("sstable: refusing to write empty table")
	}

	indexOffset := offset
	if err = writeUvarint(uint64(len(index))); err != nil {
		return err
	}
	for _, ie := range index {
		if err = writeUvarint(uint64(len(ie.key))); err != nil {
			return err
		}
		if _, err = w.Write(ie.key); err != nil {
			return err
		}
		if err = writeUvarint(uint64(ie.offset)); err != nil {
			return err
		}
	}
	var footer [sstFooterSize]byte
	binary.LittleEndian.PutUint64(footer[0:8], uint64(indexOffset))
	binary.LittleEndian.PutUint64(footer[8:16], count)
	binary.LittleEndian.PutUint32(footer[16:20], sstMagic)
	if _, err = w.Write(footer[:]); err != nil {
		return err
	}
	if err = w.Flush(); err != nil {
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

// SSTable reads an immutable table. Lookups are safe for concurrent use:
// each reads through its own SectionReader rather than seeking the shared
// file handle.
type SSTable struct {
	f           *os.File
	index       []sstIndexEntry
	indexOffset int64
	count       uint64
}

func OpenSSTable(path string) (*SSTable, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	st, err := loadSSTable(f)
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("sstable %s: %w", path, err)
	}
	return st, nil
}

func loadSSTable(f *os.File) (*SSTable, error) {
	fi, err := f.Stat()
	if err != nil {
		return nil, err
	}
	if fi.Size() < sstFooterSize {
		return nil, errors.New("file too small for footer")
	}
	var footer [sstFooterSize]byte
	if _, err := f.ReadAt(footer[:], fi.Size()-sstFooterSize); err != nil {
		return nil, err
	}
	if binary.LittleEndian.Uint32(footer[16:20]) != sstMagic {
		return nil, errors.New("bad magic")
	}
	st := &SSTable{
		f:           f,
		indexOffset: int64(binary.LittleEndian.Uint64(footer[0:8])),
		count:       binary.LittleEndian.Uint64(footer[8:16]),
	}
	if st.indexOffset < 0 || st.indexOffset > fi.Size()-sstFooterSize {
		return nil, errors.New("index offset out of range")
	}
	r := bufio.NewReader(io.NewSectionReader(f, st.indexOffset, fi.Size()-sstFooterSize-st.indexOffset))
	n, err := binary.ReadUvarint(r)
	if err != nil {
		return nil, err
	}
	st.index = make([]sstIndexEntry, 0, n)
	for i := uint64(0); i < n; i++ {
		klen, err := binary.ReadUvarint(r)
		if err != nil {
			return nil, err
		}
		key := make([]byte, klen)
		if _, err := io.ReadFull(r, key); err != nil {
			return nil, err
		}
		off, err := binary.ReadUvarint(r)
		if err != nil {
			return nil, err
		}
		st.index = append(st.index, sstIndexEntry{key: key, offset: int64(off)})
	}
	return st, nil
}

// Get looks up key. ok is false when the table holds nothing for the key;
// a returned tombstone means the key was deleted as of this table.
func (s *SSTable) Get(key []byte) (ent Entry, ok bool, err error) {
	// Last sparse entry with key <= target starts the scan interval.
	i := sort.Search(len(s.index), func(i int) bool {
		return bytes.Compare(s.index[i].key, key) > 0
	})
	if i == 0 {
		return Entry{}, false, nil // key sorts before the table's first key
	}
	start := s.index[i-1].offset
	r := bufio.NewReader(io.NewSectionReader(s.f, start, s.indexOffset-start))
	for {
		k, e, rerr := readSSTEntry(r)
		if errors.Is(rerr, io.EOF) {
			return Entry{}, false, nil
		}
		if rerr != nil {
			return Entry{}, false, rerr
		}
		switch c := bytes.Compare(k, key); {
		case c == 0:
			return e, true, nil
		case c > 0:
			return Entry{}, false, nil
		}
	}
}

// All yields every entry, including tombstones, in ascending key order.
// Compaction merges tables through this. A read error surfaces via the
// returned *error after iteration completes.
func (s *SSTable) All() (iter.Seq2[[]byte, Entry], *error) {
	var iterErr error
	seq := func(yield func([]byte, Entry) bool) {
		r := bufio.NewReader(io.NewSectionReader(s.f, 0, s.indexOffset))
		for {
			k, e, err := readSSTEntry(r)
			if errors.Is(err, io.EOF) {
				return
			}
			if err != nil {
				iterErr = err
				return
			}
			if !yield(k, e) {
				return
			}
		}
	}
	return seq, &iterErr
}

// Count returns the number of entries, counting tombstones.
func (s *SSTable) Count() uint64 { return s.count }

func (s *SSTable) Close() error { return s.f.Close() }

func readSSTEntry(r *bufio.Reader) ([]byte, Entry, error) {
	flags, err := r.ReadByte()
	if err != nil {
		return nil, Entry{}, err // io.EOF: clean end of section
	}
	klen, err := binary.ReadUvarint(r)
	if err != nil {
		return nil, Entry{}, noEOF(err)
	}
	key := make([]byte, klen)
	if _, err := io.ReadFull(r, key); err != nil {
		return nil, Entry{}, noEOF(err)
	}
	vlen, err := binary.ReadUvarint(r)
	if err != nil {
		return nil, Entry{}, noEOF(err)
	}
	value := make([]byte, vlen)
	if _, err := io.ReadFull(r, value); err != nil {
		return nil, Entry{}, noEOF(err)
	}
	return key, Entry{Value: value, Tombstone: flags&1 != 0}, nil
}

// noEOF converts an EOF inside a record into a real error: only an EOF before
// a record's first byte is a clean end of data.
func noEOF(err error) error {
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return errors.New("sstable: truncated entry")
	}
	return err
}
