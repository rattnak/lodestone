// Package cluster assembles lodestone into a distributed system: data is
// split into fixed partitions by key hash, each partition is a Raft group
// replicated across nodes chosen by rendezvous hashing, and any node routes
// requests to the right group's leader.
package cluster

import (
	"bytes"
	"encoding/gob"
)

// CommandOp enumerates state-machine commands.
type CommandOp byte

const (
	OpPut CommandOp = iota + 1
	OpDelete
	OpUpsertDoc
	OpDeleteDoc
)

// Command is one replicated state-machine operation; it is what goes into
// Raft log entries. Applying the same command twice must be harmless (set
// semantics), because replays happen after restarts.
type Command struct {
	Op      CommandOp
	Key     []byte
	Value   []byte
	DocID   uint64
	Vector  []float32
	Payload []byte
}

func EncodeCommand(c Command) ([]byte, error) {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(c); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func DecodeCommand(data []byte) (Command, error) {
	var c Command
	err := gob.NewDecoder(bytes.NewReader(data)).Decode(&c)
	return c, err
}

// kvPair is the snapshot stream unit: full state-machine contents as engine
// key/value pairs (documents included — their vectors live in the values).
type kvPair struct {
	Key   []byte
	Value []byte
}
