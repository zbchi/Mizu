package raft

import (
	"math/rand"
	"testing"

	"github.com/zbchi/linkv/proto/raftpb"
)

// ============================================================
// Test Helper Functions
// ============================================================

// newMemoryStorageWithEnts returns a new MemoryStorage with only ents filled
func newMemoryStorageWithEnts(ents []*raftpb.Entry) *MemoryStorage {
	return &MemoryStorage{
		ents:     ents,
		snapshot: &raftpb.Snapshot{},
	}
}

type stateMachine interface {
	Step(m *raftpb.Message) error
	readMessages() []*raftpb.Message
}

func (r *Raft) readMessages() []*raftpb.Message {
	msgs := r.msgs
	r.msgs = nil
	return msgs
}

// ============================================================
// Network for Testing
// ============================================================

type network struct {
	peers   map[uint64]stateMachine
	storage map[uint64]*MemoryStorage
	dropm   map[connem]float64
	ignorem map[raftpb.Type]bool
}

// newNetwork initializes a network from peers.
// A nil node will be replaced with a new *Raft.
func newNetwork(peers ...stateMachine) *network {
	return newNetworkWithConfig(nil, peers...)
}

// newNetworkWithConfig is like newNetwork but calls the given func to
// modify the configuration of any Raft instances it creates.
func newNetworkWithConfig(configFunc func(*Config), peers ...stateMachine) *network {
	size := len(peers)
	peerAddrs := idsBySize(size)

	npeers := make(map[uint64]stateMachine, size)
	nstorage := make(map[uint64]*MemoryStorage, size)

	for j, p := range peers {
		id := peerAddrs[j]
		switch v := p.(type) {
		case nil:
			nstorage[id] = NewMemoryStorage()
			cfg := newTestConfig(id, peerAddrs, 10, 1, nstorage[id])
			if configFunc != nil {
				configFunc(cfg)
			}
			sm := NewRaft(*cfg)
			npeers[id] = sm
		case *Raft:
			npeers[id] = v
		case *blackHole:
			npeers[id] = v
		default:
			panic("unexpected state machine type")
		}
	}
	return &network{
		peers:   npeers,
		storage: nstorage,
		dropm:   make(map[connem]float64),
		ignorem: make(map[raftpb.Type]bool),
	}
}

func (nw *network) send(msgs ...*raftpb.Message) {
	for len(msgs) > 0 {
		m := msgs[0]
		p := nw.peers[m.To]
		p.Step(m)
		msgs = append(msgs[1:], nw.filter(p.readMessages())...)
	}
}

func (nw *network) drop(from, to uint64, perc float64) {
	nw.dropm[connem{from, to}] = perc
}

func (nw *network) cut(one, other uint64) {
	nw.drop(one, other, 2.0) // always drop
	nw.drop(other, one, 2.0) // always drop
}

func (nw *network) isolate(id uint64) {
	for i := 0; i < len(nw.peers); i++ {
		nid := uint64(i) + 1
		if nid != id {
			nw.drop(id, nid, 1.0) // always drop
			nw.drop(nid, id, 1.0) // always drop
		}
	}
}

func (nw *network) ignore(t raftpb.Type) {
	nw.ignorem[t] = true
}

func (nw *network) recover() {
	nw.dropm = make(map[connem]float64)
	nw.ignorem = make(map[raftpb.Type]bool)
}

func (nw *network) filter(msgs []*raftpb.Message) []*raftpb.Message {
	mm := []*raftpb.Message{}
	for _, m := range msgs {
		if nw.ignorem[m.Type] {
			continue
		}
		switch m.Type {
		case raftpb.Type_MsgHup:
			// hups never go over the network, so don't drop them but panic
			panic("unexpected MsgHup")
		default:
			perc := nw.dropm[connem{m.From, m.To}]
			if n := rand.Float64(); n < perc {
				continue
			}
		}
		mm = append(mm, m)
	}
	return mm
}

type connem struct {
	from, to uint64
}

type blackHole struct{}

func (blackHole) Step(*raftpb.Message) error      { return nil }
func (blackHole) readMessages() []*raftpb.Message { return nil }

var nopStepper = &blackHole{}

func idsBySize(size int) []uint64 {
	ids := make([]uint64, size)
	for i := 0; i < size; i++ {
		ids[i] = 1 + uint64(i)
	}
	return ids
}

func newTestConfig(id uint64, peers []uint64, election, heartbeat int, storage *MemoryStorage) *Config {
	return &Config{
		ID:               id,
		Peers:            peers,
		ElectionTimeout:  election,
		HeartbeatTimeout: heartbeat,
	}
}

// ============================================================
// Leader Election Tests
// ============================================================

func TestLeaderElection2AA(t *testing.T) {
	tests := []struct {
		*network
		state   StateType
		expTerm uint64
	}{
		{newNetwork(nil, nil, nil), StateLeader, 1},
		{newNetwork(nil, nil, nopStepper), StateLeader, 1},
		{newNetwork(nil, nopStepper, nopStepper), StateCandidate, 1},
		{newNetwork(nil, nopStepper, nopStepper, nil), StateCandidate, 1},
		{newNetwork(nil, nopStepper, nopStepper, nil, nil), StateLeader, 1},
	}

	for i, tt := range tests {
		tt.send(&raftpb.Message{From: 1, To: 1, Type: raftpb.Type_MsgHup})
		sm := tt.peers[1].(*Raft)
		if sm.State() != tt.state {
			t.Errorf("#%d: state = %s, want %s", i, sm.State(), tt.state)
		}
		if sm.Term() != tt.expTerm {
			t.Errorf("#%d: term = %d, want %d", i, sm.Term(), tt.expTerm)
		}
	}
}

func TestLeaderCycle2AA(t *testing.T) {
	n := newNetwork(nil, nil, nil)
	for campaignerID := uint64(1); campaignerID <= 3; campaignerID++ {
		n.send(&raftpb.Message{From: campaignerID, To: campaignerID, Type: raftpb.Type_MsgHup})

		for _, peer := range n.peers {
			sm := peer.(*Raft)
			if sm.id == campaignerID && sm.State() != StateLeader {
				t.Errorf("campaigning node %d state = %v, want StateLeader",
					sm.id, sm.State())
			} else if sm.id != campaignerID && sm.State() != StateFollower {
				t.Errorf("after campaign of node %d, "+
					"node %d had state = %v, want StateFollower",
					campaignerID, sm.id, sm.State())
			}
		}
	}
}

func TestSingleNodeCandidate2AA(t *testing.T) {
	tt := newNetwork(nil)
	tt.send(&raftpb.Message{From: 1, To: 1, Type: raftpb.Type_MsgHup})

	sm := tt.peers[1].(*Raft)
	if sm.State() != StateLeader {
		t.Errorf("state = %d, want %d", sm.State(), StateLeader)
	}
}

// ============================================================
// Vote Tests
// ============================================================

func TestVoteFromAnyState2AA(t *testing.T) {
	for st := StateType(0); st <= StateLeader; st++ {
		r := NewRaft(Config{
			ID:               1,
			Peers:            []uint64{1, 2, 3},
			ElectionTimeout:  10,
			HeartbeatTimeout: 1,
		})
		r.hardState.Term = 1

		switch st {
		case StateFollower:
			r.becomeFollower(r.Term(), 3)
		case StateCandidate:
			r.becomeCandidate()
		case StateLeader:
			r.becomeCandidate()
			r.becomeLeader()
		}
		r.readMessages() // clear message

		newTerm := r.Term() + 1

		msg := &raftpb.Message{
			From:     2,
			To:       1,
			Type:     raftpb.Type_MsgVote,
			Term:     newTerm,
			LogTerm:  newTerm,
			LogIndex: 42,
		}
		if err := r.Step(msg); err != nil {
			t.Errorf("%s: Step failed: %s", st, err)
		}
		if len(r.msgs) != 1 {
			t.Errorf("%s: %d response messages, want 1: %+v", st, len(r.msgs), r.msgs)
		} else {
			resp := r.msgs[0]
			if resp.Type != raftpb.Type_MsgVoteResp {
				t.Errorf("%s: response message is %s, want %s",
					st, resp.Type, raftpb.Type_MsgVoteResp)
			}
			if resp.Reject {
				t.Errorf("%s: unexpected rejection", st)
			}
		}

		if r.State() != StateFollower {
			t.Errorf("%s: state %s, want %s", st, r.State(), StateFollower)
		}
		if r.Term() != newTerm {
			t.Errorf("%s: term %d, want %d", st, r.Term(), newTerm)
		}
		if r.Vote() != 2 {
			t.Errorf("%s: vote %d, want 2", st, r.Vote())
		}
	}
}

func TestAllServerStepdown2AB(t *testing.T) {
	tests := []struct {
		state  StateType
		wstate StateType
		wterm  uint64
	}{
		{StateFollower, StateFollower, 3},
		{StateCandidate, StateFollower, 3},
		{StateLeader, StateFollower, 3},
	}

	tterm := uint64(3)

	for i, tt := range tests {
		sm := NewRaft(Config{
			ID:               1,
			Peers:            []uint64{1, 2, 3},
			ElectionTimeout:  10,
			HeartbeatTimeout: 1,
		})
		switch tt.state {
		case StateFollower:
			sm.becomeFollower(1, 0)
		case StateCandidate:
			sm.becomeCandidate()
		case StateLeader:
			sm.becomeCandidate()
			sm.becomeLeader()
		}

		sm.Step(&raftpb.Message{From: 2, Type: raftpb.Type_MsgVote, Term: tterm, LogTerm: tterm})

		if sm.State() != tt.wstate {
			t.Errorf("#%d: state = %v , want %v", i, sm.State(), tt.wstate)
		}
		if sm.Term() != tt.wterm {
			t.Errorf("#%d: term = %v , want %v", i, sm.Term(), tt.wterm)
		}
		wlead := uint64(2)
		if sm.Lead() != wlead {
			t.Errorf("#%d, sm.Lead = %d, want %d", i, sm.Lead(), wlead)
		}
	}
}

// ============================================================
// Snapshot Tests
// ============================================================

func TestRestoreSnapshot2C(t *testing.T) {
	s := &raftpb.Snapshot{
		Index: 11, // magic number
		Term:  11, // magic number
		Data:  nil,
	}

	sm := NewRaft(Config{
		ID:               1,
		Peers:            []uint64{1, 2},
		ElectionTimeout:  10,
		HeartbeatTimeout: 1,
	})
	sm.handleSnapshot(&raftpb.Message{Snapshot: s})

	if sm.raftLog.LastIndex() != s.Index {
		t.Errorf("log.lastIndex = %d, want %d", sm.raftLog.LastIndex(), s.Index)
	}
	term := sm.raftLog.Term(s.Index)
	if term != s.Term {
		t.Errorf("log.lastTerm = %d, want %d", term, s.Term)
	}
	sg := nodes(sm)
	if len(sg) != 2 || sg[0] != 1 || sg[1] != 2 {
		t.Errorf("sm.Nodes = %+v, want [1, 2]", sg)
	}
}

func TestRestoreIgnoreSnapshot2C(t *testing.T) {
	previousEnts := []*raftpb.Entry{{Term: 1, Index: 1}, {Term: 1, Index: 2}, {Term: 1, Index: 3}}
	sm := NewRaft(Config{
		ID:               1,
		Peers:            []uint64{1, 2},
		ElectionTimeout:  10,
		HeartbeatTimeout: 1,
	})
	for _, e := range previousEnts {
		sm.raftLog.Append(e)
	}
	sm.hardState.CommitIndex = 3

	wcommit := uint64(3)
	s := &raftpb.Snapshot{
		Index: 1,
		Term:  1,
	}

	// ignore snapshot
	sm.handleSnapshot(&raftpb.Message{Snapshot: s})
	if sm.hardState.CommitIndex != wcommit {
		t.Errorf("commit = %d, want %d", sm.hardState.CommitIndex, wcommit)
	}
}

// ============================================================
// Node Management Tests
// ============================================================

func TestCampaignWhileLeader2AA(t *testing.T) {
	cfg := Config{
		ID:               1,
		Peers:            []uint64{1},
		ElectionTimeout:  5,
		HeartbeatTimeout: 1,
	}
	r := NewRaft(cfg)
	if r.State() != StateFollower {
		t.Errorf("expected new node to be follower but got %s", r.State())
	}
	r.Step(&raftpb.Message{From: 1, To: 1, Type: raftpb.Type_MsgHup})
	if r.State() != StateLeader {
		t.Errorf("expected single-node election to become leader but got %s", r.State())
	}
	term := r.Term()
	r.Step(&raftpb.Message{From: 1, To: 1, Type: raftpb.Type_MsgHup})
	if r.State() != StateLeader {
		t.Errorf("expected to remain leader but got %s", r.State())
	}
	if r.Term() != term {
		t.Errorf("expected to remain in term %v but got %v", term, r.Term())
	}
}

// ============================================================
// Helper Functions
// ============================================================

func nodeSlicesEqual(a, b []uint64) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
