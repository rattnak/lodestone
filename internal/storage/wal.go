// Package storage implements lodestone's persistent storage engine: a
// log-structured merge tree built from a write-ahead log, an in-memory
// memtable, and sorted string tables on disk.
package storage

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"sync"
)

// Op identifies the kind of mutation a WAL record carries.
type Op byte

const (
	OpPut Op = iota + 1
	OpDelete
)

// Record is a single logged mutation. For OpDelete, Value is ignored.
type Record struct {
	Op    Op
	Key   []byte
	Value []byte
}

// WAL record wire format, little-endian:
//
//	crc32  uint32   // IEEE CRC of the payload
//	length uint32   // payload length in bytes
//	payload:
//	  op     byte
//	  keyLen uint32
//	  key    [keyLen]byte
//	  value  [length - 5 - keyLen]byte
//
// A record is durable only after Sync returns. On reopen, a torn record at
// the tail (from a crash mid-write) is detected by its CRC or short length
// and truncated away; everything before it is intact.
const walHeaderSize = 8

const maxRecordSize = 64 << 20 // sanity bound; a longer length field means corruption

var ErrClosed = errors.New("wal: closed")

// WAL is an append-only log. It is safe for concurrent use, though the
// storage engine drives it from a single writer.
type WAL struct {
	mu   sync.Mutex
	f    *os.File
	buf  *bufio.Writer
	size int64 // offset of the end of the last valid record
}

// OpenWAL opens or creates the log at path. If the file ends in a torn or
// corrupt record, the tail is truncated so subsequent appends start from the
// last valid record boundary.
func OpenWAL(path string) (*WAL, error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, err
	}
	valid, err := scanValidPrefix(f)
	if err != nil {
		f.Close()
		return nil, err
	}
	if err := f.Truncate(valid); err != nil {
		f.Close()
		return nil, fmt.Errorf("wal: truncating torn tail: %w", err)
	}
	if _, err := f.Seek(valid, io.SeekStart); err != nil {
		f.Close()
		return nil, err
	}
	return &WAL{f: f, buf: bufio.NewWriter(f), size: valid}, nil
}

// scanValidPrefix reads the file from the start and returns the offset just
// past the last fully intact record.
func scanValidPrefix(f *os.File) (int64, error) {
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return 0, err
	}
	r := bufio.NewReader(f)
	var offset int64
	for {
		n, _, err := readRecord(r)
		if err != nil {
			if errors.Is(err, io.EOF) || isTornRecord(err) {
				return offset, nil
			}
			return 0, err
		}
		offset += n
	}
}

// errTorn marks a record that is incomplete or fails its checksum — the
// expected shape of a crash mid-write at the tail of the log.
var errTorn = errors.New("wal: torn record")

func isTornRecord(err error) bool { return errors.Is(err, errTorn) }

// readRecord reads one record, returning the total bytes it occupies on disk.
func readRecord(r io.Reader) (int64, Record, error) {
	var header [walHeaderSize]byte
	if _, err := io.ReadFull(r, header[:]); err != nil {
		if errors.Is(err, io.ErrUnexpectedEOF) {
			return 0, Record{}, errTorn
		}
		return 0, Record{}, err // io.EOF: clean end of log
	}
	crc := binary.LittleEndian.Uint32(header[0:4])
	length := binary.LittleEndian.Uint32(header[4:8])
	if length < 5 || length > maxRecordSize {
		return 0, Record{}, errTorn
	}
	payload := make([]byte, length)
	if _, err := io.ReadFull(r, payload); err != nil {
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			return 0, Record{}, errTorn
		}
		return 0, Record{}, err
	}
	if crc32.ChecksumIEEE(payload) != crc {
		return 0, Record{}, errTorn
	}
	keyLen := binary.LittleEndian.Uint32(payload[1:5])
	if 5+int64(keyLen) > int64(length) {
		return 0, Record{}, errTorn
	}
	rec := Record{
		Op:    Op(payload[0]),
		Key:   payload[5 : 5+keyLen],
		Value: payload[5+keyLen:],
	}
	return walHeaderSize + int64(length), rec, nil
}

// Append writes rec to the log buffer. The record is not durable until Sync.
func (w *WAL) Append(rec Record) error {
	payload := make([]byte, 5+len(rec.Key)+len(rec.Value))
	payload[0] = byte(rec.Op)
	binary.LittleEndian.PutUint32(payload[1:5], uint32(len(rec.Key)))
	copy(payload[5:], rec.Key)
	copy(payload[5+len(rec.Key):], rec.Value)

	var header [walHeaderSize]byte
	binary.LittleEndian.PutUint32(header[0:4], crc32.ChecksumIEEE(payload))
	binary.LittleEndian.PutUint32(header[4:8], uint32(len(payload)))

	w.mu.Lock()
	defer w.mu.Unlock()
	if w.f == nil {
		return ErrClosed
	}
	if _, err := w.buf.Write(header[:]); err != nil {
		return err
	}
	if _, err := w.buf.Write(payload); err != nil {
		return err
	}
	w.size += walHeaderSize + int64(len(payload))
	return nil
}

// Sync flushes buffered records and forces them to stable storage. A write
// may be acknowledged to a client only after Sync returns.
func (w *WAL) Sync() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.f == nil {
		return ErrClosed
	}
	if err := w.buf.Flush(); err != nil {
		return err
	}
	return w.f.Sync()
}

// Replay invokes fn for every intact record in order. It is used on startup
// to rebuild the memtable. fn's record slices alias a per-record buffer and
// must be copied if retained.
func (w *WAL) Replay(fn func(Record) error) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.f == nil {
		return ErrClosed
	}
	if err := w.buf.Flush(); err != nil {
		return err
	}
	if _, err := w.f.Seek(0, io.SeekStart); err != nil {
		return err
	}
	defer w.f.Seek(w.size, io.SeekStart)

	r := bufio.NewReader(io.LimitReader(w.f, w.size))
	for {
		_, rec, err := readRecord(r)
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
		if err := fn(rec); err != nil {
			return err
		}
	}
}

// Size returns the offset of the end of the last appended record.
func (w *WAL) Size() int64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.size
}

// Close flushes, syncs, and closes the log file.
func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.f == nil {
		return ErrClosed
	}
	flushErr := w.buf.Flush()
	syncErr := w.f.Sync()
	closeErr := w.f.Close()
	w.f = nil
	return errors.Join(flushErr, syncErr, closeErr)
}
