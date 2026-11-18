package lodelog

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

// Consumer reads a log for a named group, tracking per-partition offsets that
// survive restarts. Delivery is at-least-once: the offset is committed only
// after the caller has handled a message, so a crash between handling and
// commit replays the last message rather than dropping it. Downstream writes
// must therefore be idempotent (the embedder dedupes by event id).
type Consumer struct {
	mu        sync.Mutex
	log       *Log
	group     string
	offsets   []uint64 // committed next-offset per partition
	path      string
	cursors   []uint64 // in-flight read position per partition (>= committed)
}

// NewConsumer opens (or creates) the offset store for group and loads any
// committed offsets. A fresh group starts at offset 0 on every partition.
func NewConsumer(log *Log, group string) (*Consumer, error) {
	c := &Consumer{
		log:     log,
		group:   group,
		offsets: make([]uint64, log.Partitions()),
		cursors: make([]uint64, log.Partitions()),
		path:    filepath.Join(log.dir, "offsets-"+sanitize(group)),
	}
	if err := c.load(); err != nil {
		return nil, err
	}
	copy(c.cursors, c.offsets)
	return c, nil
}

func sanitize(s string) string {
	b := []byte(s)
	for i, c := range b {
		if !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '-' || c == '_') {
			b[i] = '_'
		}
	}
	return string(b)
}

// Poll returns up to max messages from a partition at the group's current
// cursor, advancing the cursor so repeated Polls stream forward. The returned
// messages are not yet committed — call Commit after handling them.
func (c *Consumer) Poll(partition, max int) ([]Message, error) {
	if partition < 0 || partition >= len(c.offsets) {
		return nil, ErrBadPartition
	}
	c.mu.Lock()
	from := c.cursors[partition]
	c.mu.Unlock()

	msgs, err := c.log.Read(partition, from, max)
	if err != nil {
		return nil, err
	}
	if len(msgs) > 0 {
		c.mu.Lock()
		c.cursors[partition] = msgs[len(msgs)-1].Offset + 1
		c.mu.Unlock()
	}
	return msgs, nil
}

// Commit durably records that the group has processed everything before
// nextOffset on a partition. Committing only ever moves forward.
func (c *Consumer) Commit(partition int, nextOffset uint64) error {
	if partition < 0 || partition >= len(c.offsets) {
		return ErrBadPartition
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if nextOffset <= c.offsets[partition] {
		return nil
	}
	c.offsets[partition] = nextOffset
	return c.persist()
}

// Committed returns the group's committed next-offset for a partition.
func (c *Consumer) Committed(partition int) uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.offsets[partition]
}

// Seek resets both the committed offset and the live cursor for a partition —
// used by the batch re-index job to replay a partition from the start.
func (c *Consumer) Seek(partition int, offset uint64) error {
	if partition < 0 || partition >= len(c.offsets) {
		return ErrBadPartition
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.offsets[partition] = offset
	c.cursors[partition] = offset
	return c.persist()
}

// Offset store format: magic (4) | version (1) | per partition uint64.
const (
	offMagic   = 0x4c4f4746 // "LOGF"
	offVersion = 1
)

func (c *Consumer) persist() error {
	buf := make([]byte, 5+8*len(c.offsets))
	binary.LittleEndian.PutUint32(buf[0:4], offMagic)
	buf[4] = offVersion
	for i, o := range c.offsets {
		binary.LittleEndian.PutUint64(buf[5+8*i:], o)
	}
	tmp := c.path + ".tmp"
	if err := os.WriteFile(tmp, buf, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, c.path)
}

func (c *Consumer) load() error {
	data, err := os.ReadFile(c.path)
	if errors.Is(err, os.ErrNotExist) {
		return nil // fresh group
	}
	if err != nil {
		return err
	}
	if len(data) < 5 || binary.LittleEndian.Uint32(data[0:4]) != offMagic {
		return fmt.Errorf("lodelog: corrupt offset store %s", c.path)
	}
	n := (len(data) - 5) / 8
	for i := 0; i < n && i < len(c.offsets); i++ {
		c.offsets[i] = binary.LittleEndian.Uint64(data[5+8*i:])
	}
	return nil
}
