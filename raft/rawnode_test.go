package raft

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/zbchi/linkv/proto/raftpb"
)

// newTestRaft creates a new Raft instance for testing
func newTestRaft(id uint64, peers []uint64, election, heartbeat int, storage *MemoryStorage) *Raft {
	return NewRaft(Config{
		ID:               id,
		Peers:            peers,
		ElectionTimeout:  election,
		HeartbeatTimeout: heartbeat,
		Storage:          storage,
	})
}

// drainReady reads and processes a single Ready if available
func drainReady(node *RawNode) bool {
	select {
	case <-node.Ready():
		node.Advance()
		return true
	case <-time.After(10 * time.Millisecond):
		return false
	}
}

// waitReady waits for a Ready with timeout
func waitReady(node *RawNode, timeout time.Duration) (Ready, bool) {
	select {
	case rd := <-node.Ready():
		return rd, true
	case <-time.After(timeout):
		return Ready{}, false
	}
}

// TestRawNodeProposeAndConfChange ensures that RawNode.Propose and RawNode.ProposeConfChange
// send the given proposal and ConfChange to the underlying raft.
func TestRawNodeProposeAndConfChange3A(t *testing.T) {
	r := newTestRaft(1, []uint64{1}, 10, 1, NewMemoryStorage())
	r.state = StateFollower
	node := NewRawNodeWithRaft(r)
	defer node.Stop()

	// Drain initial ready
	drainReady(node)

	// Campaign to become leader
	node.Campaign()
	rd, ok := waitReady(node, 1*time.Second)
	if !ok {
		t.Fatal("expected ready after campaign")
	}
	if r.State() != StateLeader {
		t.Fatalf("expected to become leader, got %v", r.State())
	}
	node.Advance()

	// No immediate ready after advance
	select {
	case <-node.Ready():
		node.Advance()
	case <-time.After(10 * time.Millisecond):
	}

	// Propose a command
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	if err := node.Propose(ctx, []byte("somedata")); err != nil {
		t.Fatalf("Propose failed: %v", err)
	}

	// Should get ready with proposed entry
	rd, ok = waitReady(node, 500*time.Millisecond)
	if !ok {
		t.Fatal("expected ready with proposed entry")
	}
	if len(rd.Entries) != 1 {
		t.Errorf("expected 1 entry, got %d", len(rd.Entries))
	}
	if !bytes.Equal(rd.Entries[0].Data, []byte("somedata")) {
		t.Errorf("expected entry data 'somedata', got %v", rd.Entries[0].Data)
	}
	node.Advance()
}

// TestRawNodeStart ensures that a node can be started correctly, and can accept and commit
// proposals.
func TestRawNodeStart2AC(t *testing.T) {
	r := newTestRaft(1, []uint64{1}, 10, 1, NewMemoryStorage())
	r.state = StateFollower
	node := NewRawNodeWithRaft(r)
	defer node.Stop()

	// Drain initial ready
	drainReady(node)

	// Campaign to become leader
	node.Campaign()
	rd, ok := waitReady(node, 1*time.Second)
	if !ok {
		t.Fatal("expected ready after campaign")
	}
	if r.State() != StateLeader {
		t.Fatalf("expected to become leader, got %v", r.State())
	}
	node.Advance()

	// Propose some data
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	if err := node.Propose(ctx, []byte("foo")); err != nil {
		t.Fatalf("Propose failed: %v", err)
	}

	// Should get ready with entry and committed entry
	rd, ok = waitReady(node, 500*time.Millisecond)
	if !ok {
		t.Fatal("expected ready with proposed entry")
	}
	if el := len(rd.Entries); el != len(rd.CommittedEntries) || el != 1 {
		t.Errorf("got len(Entries): %d, len(CommittedEntries): %d, want 1",
			len(rd.Entries), len(rd.CommittedEntries))
	}
	if !bytes.Equal(rd.Entries[0].Data, rd.CommittedEntries[0].Data) ||
		!bytes.Equal(rd.Entries[0].Data, []byte("foo")) {
		t.Errorf("got %+v %+v, want %+v",
			rd.Entries[0].Data, rd.CommittedEntries[0].Data, []byte("foo"))
	}
	node.Advance()

	// No more immediate ready
	select {
	case rd := <-node.Ready():
		if !rd.IsEmpty() {
			t.Errorf("unexpected Ready: %+v", rd)
		}
		node.Advance()
	case <-time.After(50 * time.Millisecond):
	}
}

// TestRawNodeRestart tests that a node can be restarted with previous state.
func TestRawNodeRestart2AC(t *testing.T) {
	entries := []*raftpb.Entry{
		{Term: 1, Index: 1},
		{Term: 1, Index: 2, Data: []byte("foo")},
	}
	st := HardState{Term: 1, CommitIndex: 1}

	// Create node with restored state
	r := newTestRaft(1, []uint64{1}, 10, 1, NewMemoryStorage())
	r.RestoreState(st, entries)

	node := NewRawNodeWithRaft(r)
	defer node.Stop()

	// Should get ready with committed entries
	rd, ok := waitReady(node, 1*time.Second)
	if !ok {
		t.Fatal("expected ready on restart")
	}

	if len(rd.Entries) != 0 {
		t.Errorf("expected no new entries, got %d", len(rd.Entries))
	}
	if len(rd.CommittedEntries) != 1 {
		t.Errorf("expected 1 committed entry, got %d", len(rd.CommittedEntries))
	}
	// Entry at index 1 has no data (it's empty)
	if len(rd.CommittedEntries) > 0 && len(rd.CommittedEntries[0].Data) != 0 {
		t.Errorf("expected empty committed entry data, got %v", rd.CommittedEntries[0].Data)
	}
	node.Advance()

	// No more immediate ready
	select {
	case rd := <-node.Ready():
		if !rd.IsEmpty() {
			t.Errorf("unexpected Ready: %+v", rd)
		}
		node.Advance()
	case <-time.After(50 * time.Millisecond):
	}
}

// TestRawNodeRestartFromSnapshot tests restart from a snapshot.
func TestRawNodeRestartFromSnapshot2C(t *testing.T) {
	snap := &raftpb.Snapshot{
		Term:  1,
		Index: 2,
		Data:  []byte("snapshot data"),
	}
	entries := []*raftpb.Entry{
		{Term: 1, Index: 3, Data: []byte("foo")},
	}
	st := HardState{Term: 1, CommitIndex: 3}

	// Create node with restored snapshot and state
	r := newTestRaft(1, []uint64{1}, 10, 1, NewMemoryStorage())
	r.RestoreSnapshot(snap)
	r.RestoreState(st, entries)

	node := NewRawNodeWithRaft(r)
	defer node.Stop()

	// Should get ready with committed entries (post-snapshot)
	rd, ok := waitReady(node, 1*time.Second)
	if !ok {
		t.Fatal("expected ready on restart")
	}

	if len(rd.Entries) != 0 {
		t.Errorf("expected no new entries, got %d", len(rd.Entries))
	}
	if len(rd.CommittedEntries) != 1 {
		t.Errorf("expected 1 committed entry, got %d", len(rd.CommittedEntries))
	}
	if len(rd.CommittedEntries) > 0 && !bytes.Equal(rd.CommittedEntries[0].Data, []byte("foo")) {
		t.Errorf("expected committed entry data 'foo', got %v", rd.CommittedEntries[0].Data)
	}
	node.Advance()

	// No more immediate ready
	select {
	case rd := <-node.Ready():
		if !rd.IsEmpty() {
			t.Errorf("unexpected Ready: %+v", rd)
		}
		node.Advance()
	case <-time.After(50 * time.Millisecond):
	}
}

// TestRawNodeTick tests that Tick advances the logical clock.
func TestRawNodeTick(t *testing.T) {
	t.Run("election timeout", func(t *testing.T) {
		r := newTestRaft(1, []uint64{1}, 10, 1, NewMemoryStorage())
		r.state = StateFollower
		node := NewRawNodeWithRaft(r)
		defer node.Stop()

		drainReady(node)

		initialTerm := r.Term()

		// Tick enough times to trigger election
		// Need to tick enough to pass electionTimeout + random jitter
		for i := 0; i < r.electionTimeout*2; i++ {
			node.Tick()
			time.Sleep(1 * time.Millisecond)
		}

		// Wait for campaign to complete
		_, ok := waitReady(node, 500*time.Millisecond)
		if ok {
			node.Advance()
		}

		if r.State() != StateLeader {
			t.Errorf("expected to become leader, got %v", r.State())
		}
		if r.Term() <= initialTerm {
			t.Errorf("expected term to increase, got %d (was %d)", r.Term(), initialTerm)
		}
	})
}

// TestRawNodeStep tests that Step properly processes incoming messages.
func TestRawNodeStep(t *testing.T) {
	t.Run("vote request", func(t *testing.T) {
		r := newTestRaft(1, []uint64{1, 2}, 10, 1, NewMemoryStorage())
		r.state = StateFollower
		node := NewRawNodeWithRaft(r)
		defer node.Stop()

		drainReady(node)

		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()

		// Send a vote request from node 2
		msg := &raftpb.Message{
			Type: raftpb.Type_MsgVote,
			From: 2,
			To:   1,
			Term: 2,
		}

		if err := node.Step(ctx, msg); err != nil {
			t.Fatalf("Step failed: %v", err)
		}

		// Wait for the vote to be processed
		rd, ok := waitReady(node, 200*time.Millisecond)
		if ok && rd.HardState != nil {
			node.Advance()
		} else {
			// May not generate a ready if vote was rejected
			drainReady(node)
		}

		// Check that we voted for node 2 (higher term)
		if r.Term() != 2 {
			t.Errorf("expected term 2, got %d", r.Term())
		}
		if r.Vote() != 2 {
			t.Errorf("expected vote for 2, got %d", r.Vote())
		}
	})

	t.Run("append entries", func(t *testing.T) {
		r := newTestRaft(1, []uint64{1, 2}, 10, 1, NewMemoryStorage())
		r.state = StateFollower
		node := NewRawNodeWithRaft(r)
		defer node.Stop()

		drainReady(node)

		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()

		// Send append entries from leader (node 2)
		msg := &raftpb.Message{
			Type: raftpb.Type_MsgApp,
			From: 2,
			To:   1,
			Term: 1,
			Entries: []*raftpb.Entry{
				{Term: 1, Index: 1, Data: []byte("test data")},
			},
		}

		if err := node.Step(ctx, msg); err != nil {
			t.Fatalf("Step failed: %v", err)
		}

		time.Sleep(50 * time.Millisecond)

		// Check that entry was appended
		if r.raftLog.LastIndex() != 1 {
			t.Errorf("expected last index 1, got %d", r.raftLog.LastIndex())
		}
	})
}

// TestRawNodeStop tests graceful shutdown.
func TestRawNodeStop(t *testing.T) {
	r := newTestRaft(1, []uint64{1}, 10, 1, NewMemoryStorage())
	r.state = StateFollower
	node := NewRawNodeWithRaft(r)

	// Stop should complete within reasonable time
	done := make(chan struct{})
	go func() {
		node.Stop()
		close(done)
	}()

	select {
	case <-done:
		// Success
	case <-time.After(2 * time.Second):
		t.Fatal("Stop took too long")
	}

	// Operations after stop should return error
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	err := node.Propose(ctx, []byte("test"))
	if err != ErrStopped {
		t.Errorf("expected ErrStopped after Stop, got %v", err)
	}
}

// TestRawNodeContextCancel tests that operations respect context cancellation.
func TestRawNodeContextCancel(t *testing.T) {
	r := newTestRaft(1, []uint64{1}, 10, 1, NewMemoryStorage())
	r.state = StateLeader
	node := NewRawNodeWithRaft(r)
	defer node.Stop()

	ctx, cancel := context.WithCancel(context.Background())

	// Start a proposal
	proposeErr := make(chan error, 1)
	go func() {
		proposeErr <- node.Propose(ctx, []byte("test"))
	}()

	// Cancel immediately
	cancel()

	// Should get context canceled error
	select {
	case err := <-proposeErr:
		if err != context.Canceled {
			t.Errorf("expected context.Canceled, got %v", err)
		}
	case <-time.After(1 * time.Second):
		t.Fatal("expected Propose to return after cancel")
	}
}
