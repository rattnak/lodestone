package cluster

import (
	"bytes"
	"context"
	"encoding/gob"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rattnak/lodestone/internal/doc"
	"github.com/rattnak/lodestone/internal/index"
	"github.com/rattnak/lodestone/internal/raft"
	"github.com/rattnak/lodestone/internal/storage"
)

// ErrNotLeader is returned with the leader's id (an address) as a hint.
type ErrNotLeader struct{ LeaderID string }

func (e ErrNotLeader) Error() string {
	return fmt.Sprintf("not leader (leader hint: %q)", e.LeaderID)
}

var ErrLostLeadership = errors.New("cluster: leadership lost before commit")

// GroupTransport routes a Raft message to the named node for a group.
type GroupTransport interface {
	SendGroup(gid uint32, m raft.Message)
}

type groupTrans struct {
	gt  GroupTransport
	gid uint32
}

func (t groupTrans) Send(m raft.Message) { t.gt.SendGroup(t.gid, m) }

// Group is one partition: a Raft-replicated state machine of a storage
// engine plus a vector index. The Raft log is the write-ahead authority, so
// the engine runs unsynced — crash recovery replays the Raft log, not the
// engine WAL.
type Group struct {
	ID  uint32
	dir string

	rn *raft.Node
	db *storage.DB
	ix *index.Index

	mu      sync.Mutex
	waiters map[uint64]*waiter
	// results holds recent apply outcomes for proposals whose waiter
	// registered after the entry already applied (Propose cannot hold the
	// lock across rn.Propose, so that window is real).
	results map[uint64]applyResult

	appliedIdx atomic.Uint64
	applied    chan struct{} // closed-and-replaced on every apply, for read waits
}

type waiter struct {
	term uint64
	ch   chan error
}

type applyResult struct {
	term uint64
	err  error
}

const resultHistory = 4096

// OpenGroup opens or creates the partition's state under dir. peers excludes
// self. The returned group is inert until Tick/Step are driven.
func OpenGroup(gid uint32, dir, selfID string, peers []string, gt GroupTransport) (*Group, error) {
	g := &Group{
		ID:      gid,
		dir:     dir,
		waiters: map[uint64]*waiter{},
		results: map[uint64]applyResult{},
		applied: make(chan struct{}),
	}
	if err := g.openStateMachine(); err != nil {
		return nil, err
	}
	rs, err := raft.OpenWALStorage(filepath.Join(dir, "raft"))
	if err != nil {
		g.db.Close()
		return nil, err
	}
	g.rn, err = raft.NewNode(raft.Config{
		ID:      selfID,
		Peers:   peers,
		Store:   rs,
		Trans:   groupTrans{gt, gid},
		Apply:   g.applyEntry,
		Restore: g.restoreSnapshot,
	})
	if err != nil {
		g.db.Close()
		rs.Close()
		return nil, err
	}
	// The engine may be ahead of lastApplied=snapIndex from a previous
	// run's unsynced writes, or behind it; both are fine because commands
	// re-apply idempotently and the engine is rebuilt on restore. What we
	// must do is reconcile the index with the engine.
	return g, g.rebuildIndex()
}

func (g *Group) openStateMachine() error {
	db, err := storage.Open(filepath.Join(g.dir, "state"), &storage.Options{SyncWrites: false})
	if err != nil {
		return err
	}
	g.db = db
	g.ix = index.New(index.Options{})
	return nil
}

func (g *Group) rebuildIndex() error {
	seq, errCheck := g.db.All()
	for k, v := range seq {
		id, isDoc := doc.IsDocKey(k)
		if !isDoc {
			continue
		}
		vec, _, err := doc.Decode(v)
		if err != nil {
			return err
		}
		if err := g.ix.Add(id, vec); err != nil {
			return err
		}
	}
	return errCheck()
}

// Tick advances the group's Raft clock.
func (g *Group) Tick() { g.rn.Tick() }

// Step delivers a Raft message.
func (g *Group) Step(m raft.Message) { g.rn.Step(m) }

// Raft exposes the underlying node for membership management.
func (g *Group) Raft() *raft.Node { return g.rn }

// applyEntry is the Raft apply callback (serialized, outside raft's lock).
func (g *Group) applyEntry(e raft.Entry) {
	var applyErr error
	if e.Type == raft.EntryNormal && len(e.Data) > 0 {
		cmd, err := DecodeCommand(e.Data)
		if err != nil {
			applyErr = err
		} else {
			applyErr = g.applyCommand(cmd)
		}
	}
	g.mu.Lock()
	if w, ok := g.waiters[e.Index]; ok {
		delete(g.waiters, e.Index)
		if w.term != e.Term {
			w.ch <- ErrLostLeadership
		} else {
			w.ch <- applyErr
		}
	} else {
		g.results[e.Index] = applyResult{term: e.Term, err: applyErr}
		delete(g.results, e.Index-resultHistory)
	}
	g.appliedIdx.Store(e.Index)
	close(g.applied)
	g.applied = make(chan struct{})
	g.mu.Unlock()
}

func (g *Group) applyCommand(c Command) error {
	switch c.Op {
	case OpPut:
		return g.db.Put(c.Key, c.Value)
	case OpDelete:
		return g.db.Delete(c.Key)
	case OpUpsertDoc:
		if err := g.db.Put(doc.Key(c.DocID), doc.Encode(c.Vector, c.Payload)); err != nil {
			return err
		}
		if err := g.ix.Delete(c.DocID); err != nil && !errors.Is(err, index.ErrNotFound) {
			return err
		}
		return g.ix.Add(c.DocID, c.Vector)
	case OpDeleteDoc:
		if err := g.db.Delete(doc.Key(c.DocID)); err != nil {
			return err
		}
		if err := g.ix.Delete(c.DocID); err != nil && !errors.Is(err, index.ErrNotFound) {
			return err
		}
		return nil
	}
	return fmt.Errorf("cluster: unknown command op %d", c.Op)
}

// Propose replicates cmd and waits until it commits and applies locally.
func (g *Group) Propose(ctx context.Context, cmd Command) error {
	data, err := EncodeCommand(cmd)
	if err != nil {
		return err
	}
	// rn.Propose may apply the entry inline (single-member group) before
	// we can register a waiter, which is why applyEntry parks results.
	idx, term, ok := g.rn.Propose(data)
	if !ok {
		return ErrNotLeader{LeaderID: g.rn.Status().LeaderID}
	}
	ch := make(chan error, 1)
	g.mu.Lock()
	if r, done := g.results[idx]; done {
		delete(g.results, idx)
		g.mu.Unlock()
		if r.term != term {
			return ErrLostLeadership
		}
		return r.err
	}
	g.waiters[idx] = &waiter{term: term, ch: ch}
	g.mu.Unlock()

	select {
	case err := <-ch:
		return err
	case <-ctx.Done():
		g.mu.Lock()
		delete(g.waiters, idx)
		g.mu.Unlock()
		return ctx.Err()
	}
}

// LinearizableRead waits until this node may serve a read with no staleness:
// it must be the leader holding a lease, and the state machine must have
// applied everything committed as of the lease check. Note the wait is on
// the group's own applied counter, not Raft's — Raft counts an entry applied
// when queued, but the read needs the engine write to have happened.
func (g *Group) LinearizableRead(ctx context.Context) error {
	readIndex, ok := g.rn.ReadIndex()
	if !ok {
		return ErrNotLeader{LeaderID: g.rn.Status().LeaderID}
	}
	for {
		if g.appliedIdx.Load() >= readIndex {
			return nil
		}
		g.mu.Lock()
		ch := g.applied
		g.mu.Unlock()
		select {
		case <-ch:
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(50 * time.Millisecond):
		}
	}
}

// Get serves a linearizable read of key.
func (g *Group) Get(ctx context.Context, key []byte) ([]byte, bool, error) {
	if err := g.LinearizableRead(ctx); err != nil {
		return nil, false, err
	}
	return g.db.Get(key)
}

// Search serves a linearizable vector search.
func (g *Group) Search(ctx context.Context, vector []float32, k, ef int) ([]index.Result, error) {
	if err := g.LinearizableRead(ctx); err != nil {
		return nil, err
	}
	return g.ix.Search(vector, k, ef)
}

// Payload fetches a document payload (for filling search hits).
func (g *Group) Payload(id uint64) ([]byte, bool) {
	v, ok, err := g.db.Get(doc.Key(id))
	if err != nil || !ok {
		return nil, false
	}
	_, payload, err := doc.Decode(v)
	if err != nil {
		return nil, false
	}
	return payload, true
}

// --- snapshots: how partition state moves between nodes ---

// snapshotState serializes the full engine contents.
func (g *Group) snapshotState() ([]byte, error) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	seq, errCheck := g.db.All()
	for k, v := range seq {
		if err := enc.Encode(kvPair{Key: k, Value: v}); err != nil {
			return nil, err
		}
	}
	if err := errCheck(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// restoreSnapshot replaces the state machine wholesale (Raft restore
// callback; also runs at OpenGroup when booting from a snapshot).
func (g *Group) restoreSnapshot(data []byte) {
	g.db.Close()
	if err := os.RemoveAll(filepath.Join(g.dir, "state")); err != nil {
		panic(fmt.Sprintf("cluster g%d: clearing state for restore: %v", g.ID, err))
	}
	if err := g.openStateMachine(); err != nil {
		panic(fmt.Sprintf("cluster g%d: reopening state for restore: %v", g.ID, err))
	}
	dec := gob.NewDecoder(bytes.NewReader(data))
	for {
		var p kvPair
		if err := dec.Decode(&p); err != nil {
			break // io.EOF: done
		}
		if err := g.db.Put(p.Key, p.Value); err != nil {
			panic(fmt.Sprintf("cluster g%d: restoring state: %v", g.ID, err))
		}
	}
	if err := g.rebuildIndex(); err != nil {
		panic(fmt.Sprintf("cluster g%d: rebuilding index after restore: %v", g.ID, err))
	}
	// appliedIdx is deliberately not advanced here (this callback also
	// runs during NewNode, before g.rn exists). Reads require leadership,
	// and winning an election mints a no-op entry whose apply advances
	// appliedIdx past the snapshot — so reads unstick without heuristics.
}

// MaybeCompact snapshots the state machine into Raft when the log has grown
// past threshold entries beyond the last snapshot.
func (g *Group) MaybeCompact(threshold uint64) error {
	st := g.rn.Status()
	if st.LastApplied < st.SnapIndex+threshold {
		return nil
	}
	data, err := g.snapshotState()
	if err != nil {
		return err
	}
	return g.rn.CompactTo(st.LastApplied, data)
}

// Status surfaces the group's Raft view.
func (g *Group) Status() raft.Status { return g.rn.Status() }

// DB and Index expose state for telemetry.
func (g *Group) DB() *storage.DB      { return g.db }
func (g *Group) Index() *index.Index  { return g.ix }

func (g *Group) Close() error {
	return g.db.Close()
}

// Destroy closes the group and deletes all its on-disk state — used when
// this node is no longer a member of the partition's group.
func (g *Group) Destroy() error {
	g.db.Close()
	return os.RemoveAll(g.dir)
}
