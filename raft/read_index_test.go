package raft

import (
	"testing"

	"github.com/zbchi/mizu/proto/raftpb"
)

func TestReadIndexWaitsForCurrentTermCommit(t *testing.T) {
	r := newReadIndexLeader([]uint64{1, 2, 3})
	if r.LastIndex() != 1 || len(r.raftLog.Entry(1).Data) != 0 {
		t.Fatalf("leader did not append a no-op entry: last index %d", r.LastIndex())
	}
	r.Advance()

	const readID = 10
	if !r.ReadIndex(readID) {
		t.Fatal("leader rejected ReadIndex")
	}
	if rd := r.Ready(); len(rd.Messages) != 0 || len(rd.ReadStates) != 0 {
		t.Fatalf("read started before current-term entry committed: %+v", rd)
	}

	ackAppend(r, 2, 0, false)
	rd := r.Ready()
	if r.CommitIndex() != 1 {
		t.Fatalf("commit index %d, want 1", r.CommitIndex())
	}
	if len(rd.ReadStates) != 0 {
		t.Fatalf("read completed without a fresh quorum: %+v", rd.ReadStates)
	}
	assertReadProbe(t, rd.Messages, readID, 2)
	r.Advance()

	// Log rejection still proves that this voter heard from the leader in the
	// current term, so it is a valid ReadIndex acknowledgement.
	ackAppend(r, 2, readID, true)
	assertReadState(t, r.Ready().ReadStates, readID, 1)
	r.Advance()
	if states := r.Ready().ReadStates; len(states) != 0 {
		t.Fatalf("Advance did not clear read states: %+v", states)
	}
}

func TestReadIndexCountsDistinctCurrentTermVoters(t *testing.T) {
	r := newReadIndexLeader([]uint64{1, 2, 3, 4, 5})
	commitLeaderNoop(t, r, 2, 3)
	r.Advance()

	const readID = 11
	if !r.ReadIndex(readID) {
		t.Fatal("leader rejected ReadIndex")
	}
	r.Advance()

	ackAppend(r, 2, readID, true)
	if got := len(r.Ready().ReadStates); got != 0 {
		t.Fatalf("read completed with only two votes: %d states", got)
	}
	ackAppend(r, 2, readID, true)
	if got := len(r.Ready().ReadStates); got != 0 {
		t.Fatalf("duplicate response was counted: %d states", got)
	}

	r.Step(&raftpb.Message{
		Type:   raftpb.Type_MsgAppResp,
		From:   6,
		To:     1,
		Term:   r.Term(),
		ReadId: readID,
	})
	r.Step(&raftpb.Message{
		Type:   raftpb.Type_MsgAppResp,
		From:   3,
		To:     1,
		Term:   r.Term() - 1,
		ReadId: readID,
	})
	r.Step(&raftpb.Message{
		Type:   raftpb.Type_MsgAppResp,
		From:   3,
		To:     1,
		Term:   r.Term(),
		ReadId: readID + 1,
	})
	if got := len(r.Ready().ReadStates); got != 0 {
		t.Fatalf("invalid response completed read: %d states", got)
	}

	ackAppend(r, 3, readID, true)
	assertReadState(t, r.Ready().ReadStates, readID, 1)
}

func TestReadIndexIsCanceledByHigherTerm(t *testing.T) {
	r := newReadIndexLeader([]uint64{1, 2, 3})
	commitLeaderNoop(t, r, 2)
	r.Advance()

	const readID = 12
	if !r.ReadIndex(readID) {
		t.Fatal("leader rejected ReadIndex")
	}
	r.Advance()

	r.Step(&raftpb.Message{
		Type:   raftpb.Type_MsgAppResp,
		From:   2,
		To:     1,
		Term:   r.Term() + 1,
		ReadId: readID,
	})
	if r.State() != StateFollower {
		t.Fatalf("state %s, want follower", r.State())
	}
	if len(r.readRounds) != 0 {
		t.Fatalf("read rounds were not cleared: %d", len(r.readRounds))
	}
	if got := len(r.Ready().ReadStates); got != 0 {
		t.Fatalf("higher-term response completed read: %d states", got)
	}
}

func TestReadIndexReturnsCapturedCommitIndex(t *testing.T) {
	r := newReadIndexLeader([]uint64{1, 2, 3})
	commitLeaderNoop(t, r, 2)
	r.Advance()

	const readID = 13
	if !r.ReadIndex(readID) {
		t.Fatal("leader rejected ReadIndex")
	}
	if !r.Propose([]byte("later write")) {
		t.Fatal("leader rejected proposal")
	}
	r.Advance()

	ackAppend(r, 2, readID, true)
	assertReadState(t, r.Ready().ReadStates, readID, 1)
}

func TestReadIndexPropagatesAcrossSnapshotMessages(t *testing.T) {
	r := newReadIndexLeader([]uint64{1, 2, 3})
	commitLeaderNoop(t, r, 2)
	r.Snapshot(1, []byte("snapshot"))
	r.prs[2].Next = 1
	r.Advance()

	const readID = 14
	if !r.ReadIndex(readID) {
		t.Fatal("leader rejected ReadIndex")
	}

	found := false
	for _, msg := range r.Ready().Messages {
		if msg.To == 2 && msg.Type == raftpb.Type_MsgSnap && msg.ReadId == readID {
			found = true
		}
	}
	if !found {
		t.Fatal("snapshot message did not carry read ID")
	}
	r.Advance()

	r.Step(&raftpb.Message{
		Type:   raftpb.Type_MsgSnapResp,
		From:   2,
		To:     1,
		Term:   r.Term(),
		Reject: true,
		ReadId: readID,
	})
	assertReadState(t, r.Ready().ReadStates, readID, 1)
}

func TestAppendResponseEchoesReadID(t *testing.T) {
	r := NewRaft(Config{
		ID:               2,
		Peers:            []uint64{1, 2, 3},
		ElectionTimeout:  10,
		HeartbeatTimeout: 1,
	})
	r.Step(&raftpb.Message{
		Type:     raftpb.Type_MsgApp,
		From:     1,
		To:       2,
		Term:     1,
		LogIndex: 0,
		LogTerm:  0,
		ReadId:   21,
	})

	messages := r.Ready().Messages
	if len(messages) != 1 {
		t.Fatalf("responses %d, want 1", len(messages))
	}
	if messages[0].Type != raftpb.Type_MsgAppResp || messages[0].ReadId != 21 {
		t.Fatalf("response did not echo read ID: %+v", messages[0])
	}
}

func newReadIndexLeader(peers []uint64) *Raft {
	r := NewRaft(Config{
		ID:               1,
		Peers:            peers,
		ElectionTimeout:  10,
		HeartbeatTimeout: 1,
	})
	r.becomeCandidate()
	r.becomeLeader()
	return r
}

func commitLeaderNoop(t *testing.T, r *Raft, voters ...uint64) {
	t.Helper()
	for _, voter := range voters {
		ackAppend(r, voter, 0, false)
	}
	if r.CommitIndex() != 1 {
		t.Fatalf("no-op commit index %d, want 1", r.CommitIndex())
	}
}

func ackAppend(r *Raft, from, readID uint64, reject bool) {
	r.Step(&raftpb.Message{
		Type:     raftpb.Type_MsgAppResp,
		From:     from,
		To:       r.ID(),
		Term:     r.Term(),
		LogIndex: r.LastIndex(),
		Reject:   reject,
		ReadId:   readID,
	})
}

func assertReadProbe(t *testing.T, messages []*raftpb.Message, readID uint64, want int) {
	t.Helper()
	got := 0
	for _, msg := range messages {
		if msg.ReadId == readID {
			got++
		}
	}
	if got != want {
		t.Fatalf("read probes %d, want %d", got, want)
	}
}

func assertReadState(t *testing.T, states []ReadState, readID, index uint64) {
	t.Helper()
	if len(states) != 1 {
		t.Fatalf("read states %d, want 1: %+v", len(states), states)
	}
	if states[0].ReadID != readID || states[0].Index != index {
		t.Fatalf("read state %+v, want id=%d index=%d", states[0], readID, index)
	}
}
