// Package raft implements the Raft consensus algorithm (Ongaro & Ousterhout,
// "In Search of an Understandable Consensus Algorithm") from the paper:
// leader election, log replication, the commitment safety rules, and
// snapshot-based log compaction.
//
// The node is tick-driven: it owns no goroutines and no clocks. The caller
// advances time with Tick and delivers messages with Step, which is what
// makes whole-cluster behavior reproducible under the deterministic simulator
// in sim.go — a failing seed replays exactly.
package raft

import (
	"fmt"
	"math/rand"
	"sync"
)

type StateType int

const (
	Follower StateType = iota
	Candidate
	Leader
)

func (s StateType) String() string {
	return [...]string{"follower", "candidate", "leader"}[s]
}

// Entry is one log record. Index and Term identify it uniquely across the
// cluster; equal (Index, Term) implies equal Data and equal preceding logs —
// the Log Matching property.
type Entry struct {
	Index uint64
	Term  uint64
	Data  []byte
}

type MsgType int

const (
	MsgVote MsgType = iota
	MsgVoteReply
	MsgApp
	MsgAppReply
	MsgSnap
)

// Message is every RPC in one flat struct; unused fields are zero. Replies
// are messages too — the transport is fire-and-forget, which models real
// networks (and the simulator) honestly.
type Message struct {
	Type MsgType
	From string
	To   string
	Term uint64

	// MsgVote: candidate's log position. MsgApp: previous entry check.
	LastLogIndex uint64
	LastLogTerm  uint64

	// MsgVoteReply
	Granted bool

	// MsgApp
	PrevLogIndex uint64
	PrevLogTerm  uint64
	Entries      []Entry
	Commit       uint64

	// MsgAppReply
	Success    bool
	MatchIndex uint64
	// On rejection, the follower's hint for where to retry (fast backup).
	ConflictIndex uint64

	// MsgSnap
	SnapData  []byte
	SnapIndex uint64
	SnapTerm  uint64
}

// Transport delivers messages asynchronously; losing, delaying, or
// reordering them must never break safety, only liveness.
type Transport interface {
	Send(msg Message)
}

// Config assembles a node. Tick granularity: election timeouts are drawn
// uniformly from [ElectionTicks, 2*ElectionTicks); heartbeats fire every
// HeartbeatTicks. Typical: 10 and 1 with a 10-50ms tick.
type Config struct {
	ID    string
	Peers []string // other voting members, excluding self
	Store Storage
	Trans Transport
	// Apply is invoked for each committed entry, in index order, exactly
	// once per node lifetime (replays happen after restarts — the state
	// machine must be rebuilt or idempotent). Entries with empty Data are
	// leader no-ops: they consume an index but carry no command. Called
	// with internal lock held: it must not call back into the node.
	Apply func(Entry)
	// Restore replaces state-machine contents from an installed snapshot.
	Restore func(data []byte)

	ElectionTicks  int
	HeartbeatTicks int
	// Rand seeds election-timeout jitter; nil seeds from global rand.
	Rand *rand.Rand
}

// Node is a single Raft participant. Tick and Step must be called from one
// goroutine; Propose, ReadIndex, and Status may be called from any.
type Node struct {
	mu sync.Mutex

	id    string
	peers []string
	store Storage
	trans Transport
	apply func(Entry)
	restore func(data []byte)

	state    StateType
	term     uint64
	votedFor string
	leaderID string

	// log[0] corresponds to index snapIndex+1.
	log       []Entry
	snapIndex uint64
	snapTerm  uint64
	snapData  []byte

	commitIndex uint64
	lastApplied uint64

	nextIndex  map[string]uint64
	matchIndex map[string]uint64

	// Election clock, in ticks.
	electionElapsed  int
	electionTimeout  int
	heartbeatElapsed int
	electionTicks    int
	heartbeatTicks   int
	rng              *rand.Rand

	// votes received this candidacy.
	votes map[string]bool

	// tickCount is a monotonic clock; lastAck[peer] is the tick of the
	// peer's last successful append reply, used for lease reads.
	tickCount uint64
	lastAck   map[string]uint64
}

func NewNode(cfg Config) (*Node, error) {
	if cfg.ElectionTicks <= 0 {
		cfg.ElectionTicks = 10
	}
	if cfg.HeartbeatTicks <= 0 {
		cfg.HeartbeatTicks = 1
	}
	if cfg.Rand == nil {
		cfg.Rand = rand.New(rand.NewSource(rand.Int63()))
	}
	n := &Node{
		id:             cfg.ID,
		peers:          cfg.Peers,
		store:          cfg.Store,
		trans:          cfg.Trans,
		apply:          cfg.Apply,
		restore:        cfg.Restore,
		electionTicks:  cfg.ElectionTicks,
		heartbeatTicks: cfg.HeartbeatTicks,
		rng:            cfg.Rand,
		nextIndex:      map[string]uint64{},
		matchIndex:     map[string]uint64{},
		lastAck:        map[string]uint64{},
	}
	st, err := cfg.Store.Load()
	if err != nil {
		return nil, err
	}
	n.term, n.votedFor, n.log = st.Term, st.VotedFor, st.Log
	n.snapData, n.snapIndex, n.snapTerm = st.SnapData, st.SnapIndex, st.SnapTerm
	if n.snapIndex > 0 {
		n.commitIndex, n.lastApplied = n.snapIndex, n.snapIndex
		if n.restore != nil && len(n.snapData) > 0 {
			n.restore(n.snapData)
		}
	}
	n.resetElectionTimeout()
	return n, nil
}

// --- log position helpers (all with mu held) ---

func (n *Node) lastIndex() uint64 {
	if len(n.log) == 0 {
		return n.snapIndex
	}
	return n.log[len(n.log)-1].Index
}

func (n *Node) termAt(index uint64) (uint64, bool) {
	if index == n.snapIndex {
		return n.snapTerm, true
	}
	if index < n.snapIndex || index > n.lastIndex() {
		return 0, false
	}
	return n.log[index-n.snapIndex-1].Term, true
}

func (n *Node) entriesFrom(index uint64) []Entry {
	if index > n.lastIndex() {
		return nil
	}
	return n.log[index-n.snapIndex-1:]
}

func (n *Node) lastTerm() uint64 {
	t, _ := n.termAt(n.lastIndex())
	return t
}

func (n *Node) resetElectionTimeout() {
	n.electionElapsed = 0
	n.electionTimeout = n.electionTicks + n.rng.Intn(n.electionTicks)
}

func (n *Node) quorum() int { return (len(n.peers)+1)/2 + 1 }

// mustPersist writes hard state; Raft's correctness depends on term, vote,
// and log surviving crashes, so a storage failure is fatal to the node.
func (n *Node) mustPersistState() {
	if err := n.store.SetState(n.term, n.votedFor); err != nil {
		panic(fmt.Sprintf("raft %s: persisting state: %v", n.id, err))
	}
}

func (n *Node) mustAppend(entries []Entry) {
	if err := n.store.Append(entries); err != nil {
		panic(fmt.Sprintf("raft %s: persisting log: %v", n.id, err))
	}
}

// --- public API ---

// Tick advances the node's clock by one unit.
func (n *Node) Tick() {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.tickCount++
	switch n.state {
	case Leader:
		n.heartbeatElapsed++
		if n.heartbeatElapsed >= n.heartbeatTicks {
			n.heartbeatElapsed = 0
			n.broadcastAppend()
		}
	default:
		n.electionElapsed++
		if n.electionElapsed >= n.electionTimeout {
			n.startElection()
		}
	}
}

// Step delivers one message from the transport.
func (n *Node) Step(m Message) {
	n.mu.Lock()
	defer n.mu.Unlock()

	if m.Term > n.term {
		n.becomeFollower(m.Term, "")
	}
	switch m.Type {
	case MsgVote:
		n.handleVote(m)
	case MsgVoteReply:
		n.handleVoteReply(m)
	case MsgApp:
		n.handleAppend(m)
	case MsgAppReply:
		n.handleAppendReply(m)
	case MsgSnap:
		n.handleSnapshot(m)
	}
}

// Propose appends data to the replicated log if this node is leader.
// Returns the entry's index and term; ok=false means "not leader, retry
// elsewhere" — the entry was not accepted.
func (n *Node) Propose(data []byte) (index, term uint64, ok bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.state != Leader {
		return 0, 0, false
	}
	e := Entry{Index: n.lastIndex() + 1, Term: n.term, Data: data}
	n.log = append(n.log, e)
	n.mustAppend([]Entry{e})
	n.broadcastAppend()
	n.maybeCommit()
	return e.Index, e.Term, true
}

// Status reports the node's current term, role, and known leader.
type Status struct {
	ID          string
	Term        uint64
	State       StateType
	LeaderID    string
	CommitIndex uint64
	LastApplied uint64
	LastIndex   uint64
	SnapIndex   uint64
}

func (n *Node) Status() Status {
	n.mu.Lock()
	defer n.mu.Unlock()
	return Status{
		ID: n.id, Term: n.term, State: n.state, LeaderID: n.leaderID,
		CommitIndex: n.commitIndex, LastApplied: n.lastApplied,
		LastIndex: n.lastIndex(), SnapIndex: n.snapIndex,
	}
}

// --- elections ---

func (n *Node) becomeFollower(term uint64, leader string) {
	n.state = Follower
	n.term = term
	n.votedFor = ""
	n.leaderID = leader
	n.mustPersistState()
	n.resetElectionTimeout()
}

func (n *Node) startElection() {
	n.state = Candidate
	n.term++
	n.votedFor = n.id
	n.leaderID = ""
	n.mustPersistState()
	n.resetElectionTimeout()
	n.votes = map[string]bool{n.id: true}
	if len(n.peers) == 0 {
		n.becomeLeader()
		return
	}
	for _, p := range n.peers {
		n.trans.Send(Message{
			Type: MsgVote, From: n.id, To: p, Term: n.term,
			LastLogIndex: n.lastIndex(), LastLogTerm: n.lastTerm(),
		})
	}
}

func (n *Node) handleVote(m Message) {
	if m.Term < n.term {
		n.trans.Send(Message{Type: MsgVoteReply, From: n.id, To: m.From, Term: n.term, Granted: false})
		return
	}
	// Election restriction (§5.4.1): only vote for candidates whose log is
	// at least as up-to-date — this is what keeps committed entries safe
	// across leader changes.
	upToDate := m.LastLogTerm > n.lastTerm() ||
		(m.LastLogTerm == n.lastTerm() && m.LastLogIndex >= n.lastIndex())
	grant := (n.votedFor == "" || n.votedFor == m.From) && upToDate
	if grant {
		n.votedFor = m.From
		n.mustPersistState()
		n.resetElectionTimeout()
	}
	n.trans.Send(Message{Type: MsgVoteReply, From: n.id, To: m.From, Term: n.term, Granted: grant})
}

func (n *Node) handleVoteReply(m Message) {
	if n.state != Candidate || m.Term != n.term || !m.Granted {
		return
	}
	n.votes[m.From] = true
	if len(n.votes) >= n.quorum() {
		n.becomeLeader()
	}
}

func (n *Node) becomeLeader() {
	n.state = Leader
	n.leaderID = n.id
	n.heartbeatElapsed = 0
	for _, p := range n.peers {
		n.nextIndex[p] = n.lastIndex() + 1
		n.matchIndex[p] = 0
	}
	// Append a no-op entry in the new term (§8). Without it the leader can
	// never commit older-term entries — maybeCommit only counts replicas
	// of current-term entries — so a cluster that elects after a quiet
	// period would stall with fully replicated but uncommittable data.
	noop := Entry{Index: n.lastIndex() + 1, Term: n.term}
	n.log = append(n.log, noop)
	n.mustAppend([]Entry{noop})
	n.broadcastAppend()
}

// --- replication ---

func (n *Node) broadcastAppend() {
	for _, p := range n.peers {
		n.sendAppend(p)
	}
}

func (n *Node) sendAppend(to string) {
	next := n.nextIndex[to]
	if next <= n.snapIndex {
		// The entries this follower needs are compacted away: ship the
		// snapshot instead.
		n.trans.Send(Message{
			Type: MsgSnap, From: n.id, To: to, Term: n.term,
			SnapData: n.snapData, SnapIndex: n.snapIndex, SnapTerm: n.snapTerm,
		})
		return
	}
	prevTerm, _ := n.termAt(next - 1)
	m := Message{
		Type: MsgApp, From: n.id, To: to, Term: n.term,
		PrevLogIndex: next - 1, PrevLogTerm: prevTerm,
		Commit: n.commitIndex,
	}
	if es := n.entriesFrom(next); len(es) > 0 {
		m.Entries = append([]Entry(nil), es...)
	}
	n.trans.Send(m)
}

func (n *Node) handleAppend(m Message) {
	reply := Message{Type: MsgAppReply, From: n.id, To: m.From, Term: n.term}
	if m.Term < n.term {
		n.trans.Send(reply)
		return
	}
	// Valid leader for this term: stay (or become) follower and reset the
	// election clock.
	n.state = Follower
	n.leaderID = m.From
	n.resetElectionTimeout()

	if m.PrevLogIndex < n.snapIndex {
		// The leader is replaying ground our snapshot already covers
		// (it can't know our compaction point). Everything at or below
		// snapIndex is committed, so acknowledge it and keep only the
		// portion of the append that extends past the snapshot.
		if m.PrevLogIndex+uint64(len(m.Entries)) <= n.snapIndex {
			reply.Success = true
			reply.MatchIndex = n.snapIndex
			n.trans.Send(reply)
			return
		}
		m.Entries = m.Entries[n.snapIndex-m.PrevLogIndex:]
		m.PrevLogIndex = n.snapIndex
		m.PrevLogTerm = n.snapTerm
	}
	prevTerm, have := n.termAt(m.PrevLogIndex)
	if !have || prevTerm != m.PrevLogTerm {
		// Consistency check failed. Hint where to back up: the start of
		// our log, or the first index of the conflicting term.
		reply.Success = false
		reply.ConflictIndex = n.conflictHint(m.PrevLogIndex)
		n.trans.Send(reply)
		return
	}
	// Append, truncating any conflicting suffix (§5.3). Entries we already
	// hold with matching terms are skipped, so stale retransmits never
	// truncate newer data.
	for i, e := range m.Entries {
		t, ok := n.termAt(e.Index)
		if ok && t == e.Term {
			continue
		}
		if e.Index <= n.lastIndex() {
			n.log = n.log[:e.Index-n.snapIndex-1]
		}
		n.log = append(n.log, m.Entries[i:]...)
		n.mustAppend(m.Entries[i:])
		break
	}
	lastNew := m.PrevLogIndex + uint64(len(m.Entries))
	if m.Commit > n.commitIndex {
		n.commitIndex = min(m.Commit, lastNew)
		n.applyCommitted()
	}
	reply.Success = true
	reply.MatchIndex = lastNew
	n.trans.Send(reply)
}

func (n *Node) conflictHint(prevIndex uint64) uint64 {
	last := n.lastIndex()
	if prevIndex > last {
		return last + 1
	}
	t, ok := n.termAt(prevIndex)
	if !ok {
		return n.snapIndex + 1
	}
	i := prevIndex
	for i > n.snapIndex+1 {
		pt, ok := n.termAt(i - 1)
		if !ok || pt != t {
			break
		}
		i--
	}
	return i
}

func (n *Node) handleAppendReply(m Message) {
	if n.state != Leader || m.Term != n.term {
		return
	}
	if m.Success {
		n.lastAck[m.From] = n.tickCount
		if m.MatchIndex > n.matchIndex[m.From] {
			n.matchIndex[m.From] = m.MatchIndex
			n.maybeCommit()
		}
		// Raise nextIndex even when matchIndex didn't move: a stale
		// rejection may have dragged it below what the follower already
		// acknowledged, and leaving it there loops useless resends.
		if m.MatchIndex+1 > n.nextIndex[m.From] {
			n.nextIndex[m.From] = m.MatchIndex + 1
		}
		return
	}
	// Back up (or forward: a compacted follower can hint above our guess)
	// and retry. Clamped to [matchIndex+1, lastIndex+1] — stale or wrong
	// hints cost retries, never correctness, since the consistency check
	// gates every actual append.
	ni := n.nextIndex[m.From]
	if m.ConflictIndex > 0 {
		ni = m.ConflictIndex
	} else if ni > 1 {
		ni--
	}
	ni = max(ni, n.matchIndex[m.From]+1)
	ni = min(ni, n.lastIndex()+1)
	n.nextIndex[m.From] = ni
	n.sendAppend(m.From)
}

// maybeCommit advances commitIndex to the highest N replicated on a quorum —
// but only if log[N].term is the current term (§5.4.2, Figure 8): counting
// replicas of old-term entries can commit data that a future leader is
// allowed to erase.
func (n *Node) maybeCommit() {
	for N := n.lastIndex(); N > n.commitIndex; N-- {
		t, ok := n.termAt(N)
		if !ok || t != n.term {
			// Older-term entries become committed implicitly when a
			// current-term entry above them commits — never directly.
			break
		}
		count := 1 // self
		for _, p := range n.peers {
			if n.matchIndex[p] >= N {
				count++
			}
		}
		if count >= n.quorum() {
			n.commitIndex = N
			n.applyCommitted()
			return
		}
	}
}

func (n *Node) applyCommitted() {
	for n.lastApplied < n.commitIndex {
		n.lastApplied++
		e := n.log[n.lastApplied-n.snapIndex-1]
		if n.apply != nil {
			n.apply(e)
		}
	}
}

// --- snapshots ---

// CompactTo installs a state-machine snapshot covering everything up to
// index (which must be applied) and discards the log through it. The caller
// provides the serialized state machine.
func (n *Node) CompactTo(index uint64, data []byte) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	if index <= n.snapIndex {
		return nil // already compacted past there
	}
	if index > n.lastApplied {
		return fmt.Errorf("raft: cannot snapshot unapplied index %d (applied %d)", index, n.lastApplied)
	}
	term, _ := n.termAt(index)
	n.log = append([]Entry(nil), n.entriesFrom(index+1)...)
	n.snapIndex, n.snapTerm, n.snapData = index, term, data
	return n.store.SetSnapshot(data, index, term)
}

func (n *Node) handleSnapshot(m Message) {
	reply := Message{Type: MsgAppReply, From: n.id, To: m.From, Term: n.term}
	if m.Term < n.term {
		n.trans.Send(reply)
		return
	}
	n.state = Follower
	n.leaderID = m.From
	n.resetElectionTimeout()
	if m.SnapIndex <= n.commitIndex {
		// We already have this prefix; just ack our position.
		reply.Success = true
		reply.MatchIndex = n.commitIndex
		n.trans.Send(reply)
		return
	}
	n.snapData, n.snapIndex, n.snapTerm = m.SnapData, m.SnapIndex, m.SnapTerm
	n.log = nil
	n.commitIndex, n.lastApplied = m.SnapIndex, m.SnapIndex
	if err := n.store.SetSnapshot(m.SnapData, m.SnapIndex, m.SnapTerm); err != nil {
		panic(fmt.Sprintf("raft %s: persisting snapshot: %v", n.id, err))
	}
	if n.restore != nil {
		n.restore(m.SnapData)
	}
	reply.Success = true
	reply.MatchIndex = m.SnapIndex
	n.trans.Send(reply)
}

// --- lease reads ---

// ReadIndex returns the commit index a linearizable read may be served at,
// and whether this node currently holds a read lease: it is leader and has
// heard successful acks from a quorum within one minimum election timeout.
// Within that window no other node can have won an election, so no other
// leader can have committed writes this node hasn't seen. (This leans on
// bounded tick-rate skew between nodes, the standard lease assumption.)
func (n *Node) ReadIndex() (uint64, bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.state != Leader {
		return 0, false
	}
	if len(n.peers) == 0 {
		return n.commitIndex, true
	}
	count := 1
	horizon := uint64(0)
	if n.tickCount > uint64(n.electionTicks) {
		horizon = n.tickCount - uint64(n.electionTicks)
	}
	for _, p := range n.peers {
		if n.lastAck[p] >= horizon && n.lastAck[p] > 0 {
			count++
		}
	}
	return n.commitIndex, count >= n.quorum()
}
