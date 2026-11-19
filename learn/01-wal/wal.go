// Package wal is your rebuild of the storage engine's write-ahead log.
//
// Read SPEC.md first. Implement every method below so that wal_test.go passes.
// You own the struct fields and the on-disk byte format — the tests check
// behavior, not layout.
//
// Do not look at internal/storage/wal.go until the tests are green.
package wal

import "errors"

// Op identifies the kind of mutation a record carries.
type Op byte

const (
	OpPut Op = iota + 1
	OpDelete
)

// Record is a single logged mutation. Value is ignored for OpDelete.
type Record struct {
	Op    Op
	Key   []byte
	Value []byte
}

// ErrClosed is returned by operations on a closed WAL.
var ErrClosed = errors.New("wal: closed")

// WAL is an append-only log.
//
// Design your own fields here — you'll likely want the file handle, a
// buffered writer, and the current end-of-log byte offset.
type WAL struct {
	// TODO: your fields
}

// OpenWAL opens or creates the log at path. If the file ends in a torn or
// corrupt record (from a crash mid-append), the tail is truncated so later
// appends start from the last valid record boundary.
func OpenWAL(path string) (*WAL, error) {
	panic("implement OpenWAL — see SPEC.md")
}

// Append buffers rec. The record is not durable until Sync returns.
func (w *WAL) Append(rec Record) error {
	panic("implement Append")
}

// Sync flushes buffered records and fsyncs them to stable storage. A write
// may be acknowledged to a caller only after Sync returns.
func (w *WAL) Sync() error {
	panic("implement Sync")
}

// Replay invokes fn for every intact record, in append order. Used on startup
// to rebuild in-memory state.
func (w *WAL) Replay(fn func(Record) error) error {
	panic("implement Replay")
}

// Size returns the byte offset of the end of the last valid record.
func (w *WAL) Size() int64 {
	panic("implement Size")
}

// Close flushes, syncs, and closes the log file.
func (w *WAL) Close() error {
	panic("implement Close")
}
