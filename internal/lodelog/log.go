// Package lodelog is a minimal append-only log broker in the shape of Kafka:
// a topic split into partitions, each an ordered sequence of records
// addressed by a monotonic offset. It is the ingestion backbone — connectors
// produce events into it, and the embedder worker consumes them at its own
// pace, replaying from any offset. The durability design mirrors the storage
// engine's WAL: length-and-CRC framed records, fsync before acknowledgement,
// torn-tail truncation on reopen.
package lodelog

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sync"
)

// Record wire format per partition segment, little-endian:
//
//	crc32   uint32   // IEEE CRC of (offset || payload)
//	length  uint32   // payload length
//	offset  uint64   // this record's offset (redundant with position, but
//	                  // lets a reader trust what it read)
//	payload [length]byte
//
// Offsets are dense and start at 0. A torn record at the tail (crash during
// Append before the next Sync) is detected by CRC or short read and dropped.
const recHeader = 8 + 8 // crc+len, then offset

const maxRecord = 16 << 20

var (
	ErrClosed       = errors.New("lodelog: closed")
	ErrBadPartition = errors.New("lodelog: partition out of range")
	errTorn         = errors.New("lodelog: torn record")
)

// Log is a partitioned append-only topic rooted at a directory.
type Log struct {
	dir        string
	partitions []*partition
}

type partition struct {
	mu   sync.Mutex
	id   int
	f    *os.File
	buf  *bufio.Writer
	size int64   // end of the last valid record
	ends []int64 // ends[o] = byte offset just past record o; len == next offset
	next uint64  // next offset to assign
}

// Open opens or creates a log with the given partition count under dir. The
// count is fixed for the life of the data: changing it would reshuffle which
// partition a key lands in.
func Open(dir string, partitions int) (*Log, error) {
	if partitions <= 0 {
		return nil, fmt.Errorf("lodelog: partitions must be positive")
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	l := &Log{dir: dir}
	for i := 0; i < partitions; i++ {
		p, err := openPartition(dir, i)
		if err != nil {
			l.Close()
			return nil, err
		}
		l.partitions = append(l.partitions, p)
	}
	return l, nil
}

func openPartition(dir string, id int) (*partition, error) {
	path := filepath.Join(dir, fmt.Sprintf("part-%04d.log", id))
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, err
	}
	p := &partition{id: id, f: f}
	if err := p.rebuildIndex(); err != nil {
		f.Close()
		return nil, err
	}
	p.buf = bufio.NewWriter(f)
	return p, nil
}

// rebuildIndex scans the segment, recording each record's end offset and the
// next assignable offset, and truncates a torn tail.
func (p *partition) rebuildIndex() error {
	if _, err := p.f.Seek(0, io.SeekStart); err != nil {
		return err
	}
	r := bufio.NewReader(p.f)
	var pos int64
	for {
		n, off, _, err := readRecord(r)
		if err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, errTorn) {
				break
			}
			return err
		}
		if off != p.next {
			// Offsets must be dense and in order; anything else is
			// corruption past the last good record.
			break
		}
		pos += n
		p.ends = append(p.ends, pos)
		p.next++
	}
	if err := p.f.Truncate(pos); err != nil {
		return err
	}
	p.size = pos
	_, err := p.f.Seek(pos, io.SeekStart)
	return err
}

func readRecord(r io.Reader) (n int64, offset uint64, payload []byte, err error) {
	var h [recHeader]byte
	if _, err = io.ReadFull(r, h[:]); err != nil {
		if errors.Is(err, io.ErrUnexpectedEOF) {
			return 0, 0, nil, errTorn
		}
		return 0, 0, nil, err // clean EOF
	}
	crc := binary.LittleEndian.Uint32(h[0:4])
	length := binary.LittleEndian.Uint32(h[4:8])
	offset = binary.LittleEndian.Uint64(h[8:16])
	if length > maxRecord {
		return 0, 0, nil, errTorn
	}
	payload = make([]byte, length)
	if _, err = io.ReadFull(r, payload); err != nil {
		return 0, 0, nil, errTorn
	}
	var ob [8]byte
	binary.LittleEndian.PutUint64(ob[:], offset)
	if crc32.ChecksumIEEE(append(ob[:], payload...)) != crc {
		return 0, 0, nil, errTorn
	}
	return recHeader + int64(length), offset, payload, nil
}

// Append writes payload to a partition and returns its offset. The record is
// durable only after Sync.
func (l *Log) Append(partition int, payload []byte) (uint64, error) {
	if partition < 0 || partition >= len(l.partitions) {
		return 0, ErrBadPartition
	}
	return l.partitions[partition].append(payload)
}

func (p *partition) append(payload []byte) (uint64, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.f == nil {
		return 0, ErrClosed
	}
	offset := p.next
	var h [recHeader]byte
	binary.LittleEndian.PutUint32(h[4:8], uint32(len(payload)))
	binary.LittleEndian.PutUint64(h[8:16], offset)
	var ob [8]byte
	binary.LittleEndian.PutUint64(ob[:], offset)
	crc := crc32.ChecksumIEEE(append(ob[:], payload...))
	binary.LittleEndian.PutUint32(h[0:4], crc)

	if _, err := p.buf.Write(h[:]); err != nil {
		return 0, err
	}
	if _, err := p.buf.Write(payload); err != nil {
		return 0, err
	}
	p.size += recHeader + int64(len(payload))
	p.ends = append(p.ends, p.size)
	p.next = offset + 1
	return offset, nil
}

// Sync flushes and fsyncs every partition; records are durable after it
// returns. Producers that must not lose data call Sync (or AppendSync).
func (l *Log) Sync() error {
	var errs []error
	for _, p := range l.partitions {
		errs = append(errs, p.sync())
	}
	return errors.Join(errs...)
}

func (p *partition) sync() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.f == nil {
		return ErrClosed
	}
	if err := p.buf.Flush(); err != nil {
		return err
	}
	return p.f.Sync()
}

// AppendSync appends and makes the record durable before returning.
func (l *Log) AppendSync(partition int, payload []byte) (uint64, error) {
	off, err := l.Append(partition, payload)
	if err != nil {
		return 0, err
	}
	return off, l.partitions[partition].sync()
}

// Message is a record returned to a consumer.
type Message struct {
	Partition int
	Offset    uint64
	Value     []byte
}

// Read returns up to max records from a partition starting at fromOffset.
// Reading at or past the end returns no records and no error (the consumer
// polls again later). Reads are safe for concurrent use with appends.
func (l *Log) Read(partition int, fromOffset uint64, max int) ([]Message, error) {
	if partition < 0 || partition >= len(l.partitions) {
		return nil, ErrBadPartition
	}
	return l.partitions[partition].read(fromOffset, max)
}

func (p *partition) read(from uint64, max int) ([]Message, error) {
	p.mu.Lock()
	if p.f == nil {
		p.mu.Unlock()
		return nil, ErrClosed
	}
	if err := p.buf.Flush(); err != nil {
		p.mu.Unlock()
		return nil, err
	}
	if from >= p.next {
		p.mu.Unlock()
		return nil, nil
	}
	start := int64(0)
	if from > 0 {
		start = p.ends[from-1]
	}
	limit := p.size
	f := p.f
	p.mu.Unlock()

	r := bufio.NewReader(io.NewSectionReader(f, start, limit-start))
	var out []Message
	off := from
	for len(out) < max {
		_, recOff, payload, err := readRecord(r)
		if errors.Is(err, io.EOF) || errors.Is(err, errTorn) {
			break
		}
		if err != nil {
			return out, err
		}
		if recOff != off {
			return out, fmt.Errorf("lodelog: offset gap at %d (read %d)", off, recOff)
		}
		out = append(out, Message{Partition: p.id, Offset: recOff, Value: payload})
		off++
	}
	return out, nil
}

// EndOffset returns the next offset that will be assigned to a partition —
// i.e. one past the last record (the log's high-water mark).
func (l *Log) EndOffset(partition int) (uint64, error) {
	if partition < 0 || partition >= len(l.partitions) {
		return 0, ErrBadPartition
	}
	p := l.partitions[partition]
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.next, nil
}

// Partitions returns the fixed partition count.
func (l *Log) Partitions() int { return len(l.partitions) }

// Close syncs and closes every partition.
func (l *Log) Close() error {
	var errs []error
	for _, p := range l.partitions {
		if p == nil {
			continue
		}
		p.mu.Lock()
		if p.f != nil {
			errs = append(errs, p.buf.Flush(), p.f.Sync(), p.f.Close())
			p.f = nil
		}
		p.mu.Unlock()
	}
	return errors.Join(errs...)
}
