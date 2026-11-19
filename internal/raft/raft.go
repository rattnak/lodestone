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
	"bytes"
	"encoding/gob"
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

// EntryType distinguishes client commands from cluster-management entries.
type EntryType byte

const (
	// EntryNormal carries a client command (empty Data = leader no-op).
	EntryNormal EntryType = iota
	// EntryConfChange carries an encoded ConfChange; applying it changes
	// the voting membership.
	EntryConfChange
)

// Entry is one log record. Index and Term identify it uniquely across the
// cluster; equal (Index, Term) implies equal Data and equal preceding logs —
// the Log Matching property.
type Entry struct {
	Index uint64
	Term  uint64
	Type  EntryType
	Data  []byte
}

// ConfChange carries the complete voting membership after the change, not a
// delta. Encoding the full set means any node — including one joining years
// later with none of the earlier history — learns the exact configuration
// from the single entry that changes it, so every node converges on the same
// membership. Callers still change the set by one member at a time (§4.1): a
// single addition or removal keeps the old and new quorums overlapping, which
// is what makes the transition safe without joint consensus.
type ConfChange struct {
	Members []string // complete voting set after applying this change
}

func contains(ss []string, s string) bool {
	for _, x := range ss {
		if x == s {
			return true
		}
	}
	return false
}

// membersFrom reconstructs a full member list from a persisted peer set plus
// whether self is a voter.
func membersFrom(peers []string, voter bool, self string) []string {
	members := append([]string(nil), peers...)
	if voter {
		members = append(members, self)
	}
	return members
}

type MsgType int

const (
	MsgVote MsgType = iota
	MsgVoteReply
	MsgApp
	MsgAppReply
	MsgSnap
	// Pre-vote (§9.6): a hypothetical election one term ahead that changes
	// no term or vote. A node starts a real election only after winning a
	// pre-vote, so a node that cannot win — because its log is behind or it
	// is partitioned — never disrupts a healthy leader by repeatedly
	// bumping the term. This is what keeps many co-located groups from
	// livelocking during membership churn.
	MsgPreVote
	MsgPreVoteReply
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
	SnapData    []byte
	SnapIndex   uint64
	SnapTerm    uint64
	SnapMembers []string // configuration as of the snapshot floor
}

// Transport delivers messages asynchronously; losing, delaying, or
// reordering them must never break safety, only liveness.
type Transport interface {
	Send(msg Message)
}

func EncodeConfChange(cc ConfChange) ([]byte, error) {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(cc); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func DecodeConfChange(data []byte) (ConfChange, error) {
	var cc ConfChange
	err := gob.NewDecoder(bytes.NewReader(data)).Decode(&cc)
	return cc, err
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
	// leader no-ops: they consume an index but carry no command. Calls
	// are serialized and happen outside the node's internal lock, so the
	// callback may be slow and may read node state, but a Propose from
	// inside it would deadlock the apply pipeline — don't.
	Apply func(Entry)
	// Restore replaces state-machine contents from an installed snapshot.
	Restore func(data []byte)

	ElectionTicks  int
	HeartbeatTicks int
	// Learner starts the node as a non-voter with an empty configuration.
	// A node joining an existing group uses this: it accepts the leader's
	// entries and becomes a voter only when it applies a ConfChange that
	// includes it. Founders (the initial members, who all know each other)
	// leave this false and pass the full founder set as Peers.
	Learner bool

	// Rand seeds election-timeout jitter; nil seeds from global rand.
	Rand *rand.Rand
}

// Node is a single Raft participant. Tick and Step must be called from one
// goroutine; Propose, ReadIndex, and Status may be called from any.
type Node struct {
	mu sync.Mutex

	id      string
	store   Storage
	trans   Transport
	apply   func(Entry)
	restore func(data []byte)

	state    StateType
	term     uint64
	votedFor string
	leaderID string
	// The effective configuration is always the newest ConfChange in the
	// log, committed or not (§4): a server uses the latest configuration it
	// has stored, so an appended change takes effect immediately and a
	// truncated one reverts. baseMembers is the configuration as of the
	// snapshot (the log's floor); peers/voter are derived by scanning the
	// log over it via refreshConfig after every log mutation.
	baseMembers []string // config at the snapshot floor (self + others)
	peers       []string // derived: other voting members in effect now
	voter       bool     // derived: whether self is in the effective config
	// configured distinguishes a removed member (configured && !voter) from
	// a learner that has never been in any configuration.
	configured bool

	// log[0] corresponds to index snapIndex+1.
	log       []Entry
	snapIndex uint64
	snapTerm  uint64
	snapData  []byte

	commitIndex uint64
	lastApplied uint64

	nextIndex  map[string]uint64
	matchIndex map[string]uint64
	// demoted holds nodes removed from the configuration that the leader
	// must keep replicating to until they have received the entry removing
	// them (mapped to that entry's index). Without this, a removed node
	// never learns of its removal and campaigns forever, disrupting the
	// group. Demoted nodes receive AppendEntries but are not counted toward
	// quorum.
	demoted map[string]uint64

	// Election clock, in ticks.
	electionElapsed  int
	electionTimeout  int
	heartbeatElapsed int
	electionTicks    int
	heartbeatTicks   int
	rng              *rand.Rand

	// votes received this real candidacy (nil until the real election
	// starts); preVotes received during the pre-vote round.
	votes    map[string]bool
	preVotes map[string]bool

	// tickCount is a monotonic clock; lastAck[peer] is the tick of the
	// peer's last successful append reply, used for lease reads.
	tickCount uint64
	lastAck   map[string]uint64

	// Committed entries and snapshot restores are queued under mu and
	// delivered to the Apply/Restore callbacks after it is released,
	// serialized FIFO by applyMu — callbacks may be slow or take their
	// own locks without deadlocking the consensus core.
	applyMu sync.Mutex
	pending []applyItem
}

type applyItem struct {
	entry     Entry
	restore   []byte
	isRestore bool
}

// flushApply delivers queued applies/restores in order. Any goroutine
// leaving a public entry point calls it; applyMu keeps delivery serial and
// the FIFO queue keeps it ordered regardless of which goroutine drains.
func (n *Node) flushApply() {
	n.applyMu.Lock()
	defer n.applyMu.Unlock()
	for {
		n.mu.Lock()
		batch := n.pending
		n.pending = nil
		n.mu.Unlock()
		if len(batch) == 0 {
			return
		}
		for _, it := range batch {
			if it.isRestore {
				if n.restore != nil {
					n.restore(it.restore)
				}
			} else if n.apply != nil {
				n.apply(it.entry)
			}
		}
	}
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
		demoted:        map[string]uint64{},
	}
	st, err := cfg.Store.Load()
	if err != nil {
		return nil, err
	}
	n.term, n.votedFor, n.log = st.Term, st.VotedFor, st.Log
	n.snapData, n.snapIndex, n.snapTerm = st.SnapData, st.SnapIndex, st.SnapTerm

	// Establish the base configuration (the config at the snapshot floor).
	// A persisted config from a prior run wins; otherwise founders start in
	// their bootstrap set and learners start empty.
	switch {
	case st.Configured:
		n.baseMembers = membersFrom(st.Peers, st.Voter, cfg.ID)
		n.configured = true
	case cfg.Learner:
		n.baseMembers = nil
	default:
		n.baseMembers = membersFrom(cfg.Peers, true, cfg.ID)
		n.configured = true
	}
	n.refreshConfig()
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
	defer n.flushApply()
	defer n.mu.Unlock()
	n.tickCount++
	switch n.state {
	case Leader:
		n.heartbeatElapsed++
		if n.heartbeatElapsed >= n.heartbeatTicks {
			n.heartbeatElapsed = 0
			n.broadcastAppend()
		}
		// CheckQuorum: a leader that has not heard from a quorum within an
		// election timeout steps down. This is the safety backstop for
		// leader reads — a partitioned leader relinquishes leadership
		// before any other node could win an election, so it cannot keep
		// serving reads once it may have been superseded.
		n.electionElapsed++
		if n.electionElapsed >= n.electionTimeout {
			if n.quorumActive() {
				n.electionElapsed = 0
			} else {
				n.becomeFollower(n.term, "")
			}
		}
	default:
		n.electionElapsed++
		if n.electionElapsed >= n.electionTimeout && n.voter {
			n.startElection()
		}
	}
}

// Step delivers one message from the transport.
func (n *Node) Step(m Message) {
	n.mu.Lock()
	defer n.flushApply()
	defer n.mu.Unlock()

	// Pre-vote requests are hypothetical (term+1) and must not change our
	// term — handling them before the generic step-down is what makes
	// pre-vote non-disruptive. A pre-vote *reply* still carries the
	// responder's real term, so a higher one means we are behind.
	if m.Type == MsgPreVote {
		n.handlePreVote(m)
		return
	}
	if m.Term > n.term {
		n.becomeFollower(m.Term, "")
	}
	switch m.Type {
	case MsgPreVoteReply:
		n.handlePreVoteReply(m)
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
	defer n.flushApply()
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
	// Removed is set once the node applies its own removal from the
	// membership; the hosting layer should shut it down.
	Removed bool
}

func (n *Node) Status() Status {
	n.mu.Lock()
	defer n.mu.Unlock()
	return Status{
		ID: n.id, Term: n.term, State: n.state, LeaderID: n.leaderID,
		CommitIndex: n.commitIndex, LastApplied: n.lastApplied,
		LastIndex: n.lastIndex(), SnapIndex: n.snapIndex,
		// A node that has a configuration but is not in it has been
		// removed; a learner that was never configured is not "removed".
		Removed: n.configured && !n.voter,
	}
}

// Progress returns, for a leader, each peer's acknowledged match index.
// Membership orchestration uses it to gate removals: never remove a voter
// while a newly added one is still catching up, or two lagging members
// could form an empty-logged quorum.
func (n *Node) Progress() map[string]uint64 {
	n.mu.Lock()
	defer n.mu.Unlock()
	out := make(map[string]uint64, len(n.matchIndex))
	for id, m := range n.matchIndex {
		out[id] = m
	}
	return out
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

// startElection runs the pre-vote round: a hypothetical election at term+1
// that leaves this node's term and vote untouched. Only on winning does it
// start a real election, so a node that cannot win never disrupts the leader.
func (n *Node) startElection() {
	n.state = Candidate
	n.leaderID = ""
	n.resetElectionTimeout()
	n.preVotes = map[string]bool{n.id: true}
	n.votes = nil
	if len(n.peers) == 0 {
		n.startRealElection()
		return
	}
	for _, p := range n.peers {
		n.trans.Send(Message{
			Type: MsgPreVote, From: n.id, To: p, Term: n.term + 1,
			LastLogIndex: n.lastIndex(), LastLogTerm: n.lastTerm(),
		})
	}
}

// startRealElection increments the term and campaigns for real.
func (n *Node) startRealElection() {
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

// logUpToDate reports whether a candidate's (lastTerm, lastIndex) is at least
// as current as ours — the §5.4.1 election restriction.
func (n *Node) logUpToDate(lastLogTerm, lastLogIndex uint64) bool {
	return lastLogTerm > n.lastTerm() ||
		(lastLogTerm == n.lastTerm() && lastLogIndex >= n.lastIndex())
}

// handlePreVote grants a pre-vote when the candidate's log is up-to-date and
// this node's own election timeout is near (meaning it is not currently
// hearing from a healthy leader). It changes no term or vote, so an unlucky
// pre-candidate costs nothing.
func (n *Node) handlePreVote(m Message) {
	grant := m.Term >= n.term &&
		n.logUpToDate(m.LastLogTerm, m.LastLogIndex) &&
		n.electionElapsed >= n.electionTicks
	n.trans.Send(Message{Type: MsgPreVoteReply, From: n.id, To: m.From, Term: n.term, Granted: grant})
}

func (n *Node) handlePreVoteReply(m Message) {
	// Only meaningful while still soliciting pre-votes (not yet in a real
	// election, where votes != nil).
	if n.state != Candidate || n.votes != nil || !m.Granted {
		return
	}
	if n.preVotes == nil {
		n.preVotes = map[string]bool{n.id: true}
	}
	n.preVotes[m.From] = true
	if len(n.preVotes) >= n.quorum() {
		n.startRealElection()
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
	grant := (n.votedFor == "" || n.votedFor == m.From) && n.logUpToDate(m.LastLogTerm, m.LastLogIndex)
	if grant {
		n.votedFor = m.From
		n.mustPersistState()
		n.resetElectionTimeout()
	}
	n.trans.Send(Message{Type: MsgVoteReply, From: n.id, To: m.From, Term: n.term, Granted: grant})
}

func (n *Node) handleVoteReply(m Message) {
	if n.state != Candidate || n.votes == nil || m.Term != n.term || !m.Granted {
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
	n.nextIndex = map[string]uint64{}
	n.matchIndex = map[string]uint64{}
	n.demoted = map[string]uint64{}
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
	// Keep feeding demoted nodes until they learn of their own removal.
	for d := range n.demoted {
		n.sendAppend(d)
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
			SnapMembers: n.baseMembers,
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
	changed := false
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
		changed = true
		break
	}
	if changed {
		n.refreshConfig() // membership follows the log (§4)
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
		// A demoted node that has received its removal entry now knows it
		// is out; stop tracking it.
		if idx, ok := n.demoted[m.From]; ok && m.MatchIndex >= idx {
			delete(n.demoted, m.From)
			delete(n.nextIndex, m.From)
			delete(n.matchIndex, m.From)
			delete(n.lastAck, m.From)
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
	// Membership already took effect when each entry was appended
	// (refreshConfig), so commit only delivers entries to the state machine
	// for index accounting and client commands.
	for n.lastApplied < n.commitIndex {
		n.lastApplied++
		e := n.log[n.lastApplied-n.snapIndex-1]
		n.pending = append(n.pending, applyItem{entry: e})
	}
}

// refreshConfig recomputes the effective membership — the newest ConfChange
// in the log, or baseMembers if the log holds none — after any log mutation
// (append, truncate, or snapshot install). Taking effect on append rather
// than commit is what the single-server membership change requires for
// safety (§4): otherwise a removed leader can leave while followers still
// hold the superseded config, deadlocking the group.
func (n *Node) refreshConfig() {
	members := n.baseMembers
	for i := len(n.log) - 1; i >= 0; i-- {
		if n.log[i].Type == EntryConfChange {
			cc, err := DecodeConfChange(n.log[i].Data)
			if err != nil {
				panic(fmt.Sprintf("raft %s: corrupt conf change at %d: %v", n.id, n.log[i].Index, err))
			}
			members = cc.Members
			break
		}
	}
	wasVoter := n.voter
	n.voter = contains(members, n.id)
	if len(members) > 0 {
		n.configured = true
	}
	peers := make([]string, 0, len(members))
	for _, m := range members {
		if m != n.id {
			peers = append(peers, m)
		}
	}
	n.peers = peers

	if n.state == Leader {
		// Track progress for exactly the current peer set: keep known
		// peers' indices, start new ones at the log tail. A peer that was
		// a voter and no longer is becomes demoted — still replicated to
		// (keeping its indices) until it acknowledges its own removal — so
		// it learns it is out and stops campaigning.
		newPeers := map[string]bool{}
		for _, p := range peers {
			newPeers[p] = true
		}
		for old := range n.matchIndex {
			if !newPeers[old] {
				n.demoted[old] = n.lastIndex()
			}
		}
		next := map[string]uint64{}
		match := map[string]uint64{}
		for _, p := range peers {
			delete(n.demoted, p) // re-added before catching up: a normal peer again
			if cur, ok := n.nextIndex[p]; ok {
				next[p], match[p] = cur, n.matchIndex[p]
			} else {
				next[p], match[p] = n.lastIndex()+1, 0
			}
		}
		// Preserve demoted nodes' replication indices.
		for d := range n.demoted {
			if cur, ok := n.nextIndex[d]; ok {
				next[d], match[d] = cur, n.matchIndex[d]
			} else {
				next[d], match[d] = n.lastIndex()+1, 0
			}
		}
		n.nextIndex, n.matchIndex = next, match
	}
	if wasVoter && !n.voter {
		// No longer in the configuration: step down and stop campaigning
		// so a removed member cannot disturb the rest (§4.2.3). The host
		// observes Removed and shuts us down once the change commits.
		n.state = Follower
	}
}

// ProposeConfChange appends a membership change if this node is leader and no
// other change is still uncommitted (one at a time keeps quorums
// overlapping). cc.Members is the complete voting set after the change and
// must differ from the current set by exactly one member.
func (n *Node) ProposeConfChange(cc ConfChange) (index uint64, ok bool) {
	n.mu.Lock()
	defer n.flushApply()
	defer n.mu.Unlock()
	if n.state != Leader {
		return 0, false
	}
	for _, e := range n.entriesFrom(n.commitIndex + 1) {
		if e.Type == EntryConfChange {
			return 0, false // one in flight already
		}
	}
	data, err := EncodeConfChange(cc)
	if err != nil {
		return 0, false
	}
	e := Entry{Index: n.lastIndex() + 1, Term: n.term, Type: EntryConfChange, Data: data}
	n.log = append(n.log, e)
	n.mustAppend([]Entry{e})
	n.refreshConfig() // new membership takes effect now, before it commits
	n.broadcastAppend()
	n.maybeCommit()
	return e.Index, true
}

// Members returns the current voting set (peers plus self if a voter).
func (n *Node) Members() []string {
	n.mu.Lock()
	defer n.mu.Unlock()
	out := append([]string(nil), n.peers...)
	if n.voter {
		out = append(out, n.id)
	}
	return out
}

// Peers returns the node's current view of the other voting members.
func (n *Node) Peers() []string {
	n.mu.Lock()
	defer n.mu.Unlock()
	return append([]string(nil), n.peers...)
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
	// The snapshot floor's configuration is the newest ConfChange at or
	// before index; it becomes the new base once earlier entries are gone.
	n.baseMembers = n.membersAsOf(index)
	n.log = append([]Entry(nil), n.entriesFrom(index+1)...)
	n.snapIndex, n.snapTerm, n.snapData = index, term, data
	if err := n.store.SetSnapshot(data, index, term); err != nil {
		return err
	}
	if err := n.persistBaseConfig(); err != nil {
		return err
	}
	n.refreshConfig()
	return nil
}

// membersAsOf returns the configuration in effect at log index: the newest
// ConfChange with Index <= index, or baseMembers if none.
func (n *Node) membersAsOf(index uint64) []string {
	for i := len(n.log) - 1; i >= 0; i-- {
		e := n.log[i]
		if e.Index > index {
			continue
		}
		if e.Type == EntryConfChange {
			cc, err := DecodeConfChange(e.Data)
			if err != nil {
				panic(fmt.Sprintf("raft %s: corrupt conf change at %d: %v", n.id, e.Index, err))
			}
			return cc.Members
		}
	}
	return n.baseMembers
}

func (n *Node) persistBaseConfig() error {
	peers := make([]string, 0, len(n.baseMembers))
	voter := false
	for _, m := range n.baseMembers {
		if m == n.id {
			voter = true
		} else {
			peers = append(peers, m)
		}
	}
	return n.store.SetConfig(peers, voter)
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
	n.baseMembers = m.SnapMembers
	n.commitIndex, n.lastApplied = m.SnapIndex, m.SnapIndex
	if err := n.store.SetSnapshot(m.SnapData, m.SnapIndex, m.SnapTerm); err != nil {
		panic(fmt.Sprintf("raft %s: persisting snapshot: %v", n.id, err))
	}
	if err := n.persistBaseConfig(); err != nil {
		panic(fmt.Sprintf("raft %s: persisting snapshot config: %v", n.id, err))
	}
	n.refreshConfig()
	n.pending = append(n.pending, applyItem{restore: m.SnapData, isRestore: true})
	reply.Success = true
	reply.MatchIndex = m.SnapIndex
	n.trans.Send(reply)
}

// --- linearizable reads ---

// quorumActive reports whether a quorum (including self) has acknowledged
// this leader within the last election timeout — the CheckQuorum condition.
func (n *Node) quorumActive() bool {
	if len(n.peers) == 0 {
		return true
	}
	horizon := uint64(0)
	if n.tickCount > uint64(n.electionTimeout) {
		horizon = n.tickCount - uint64(n.electionTimeout)
	}
	count := 1 // self
	for _, p := range n.peers {
		if n.lastAck[p] > 0 && n.lastAck[p] >= horizon {
			count++
		}
	}
	return count >= n.quorum()
}

// Linearizable reads use the ReadIndex protocol with *active* confirmation,
// not a passive lease. A lease based on the logical clock is unsafe under a
// process pause: a paused leader's clock freezes, so on resume it can still
// believe its lease is valid and serve a stale read before learning a new
// leader exists. Instead, a read:
//
//  1. takes a barrier (the current tick) and the read index (commit index,
//     which must include a committed current-term entry so it reflects every
//     earlier write), then
//  2. waits until a quorum has acknowledged this leader *after* the barrier —
//     fresh contact proving it is still the leader right now.
//
// A paused-then-resumed node that is no longer leader gets higher-term
// rejections when it broadcasts and steps down, so confirmation fails and the
// read is refused rather than served stale.

// ReadBarrier begins a linearizable read. It returns the tick barrier and the
// read index, or ok=false if this node cannot start a read (not leader, or no
// committed current-term entry yet).
func (n *Node) ReadBarrier() (barrier, readIndex uint64, ok bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.state != Leader {
		return 0, 0, false
	}
	if t, ok := n.termAt(n.commitIndex); !ok || t != n.term {
		return 0, 0, false
	}
	// Prompt a heartbeat so confirmation arrives quickly rather than waiting
	// for the next scheduled beat.
	n.broadcastAppend()
	return n.tickCount, n.commitIndex, true
}

// ReadConfirmed reports whether this node is still the leader and a quorum has
// acknowledged it strictly after barrier — fresh enough to prove no other
// leader has superseded it.
func (n *Node) ReadConfirmed(barrier uint64) bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.state != Leader {
		return false
	}
	if len(n.peers) == 0 {
		return true
	}
	count := 1 // self
	for _, p := range n.peers {
		if n.lastAck[p] > barrier {
			count++
		}
	}
	return count >= n.quorum()
}
