package raft

import (
	"context"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v3"
	"github.com/zbchi/linkv/proto/raftpb"
)

// TestNodeBasicLifecycle tests basic node start and stop
func TestNodeBasicLifecycle(t *testing.T) {
	// Create a simple Raft instance for testing
	db, err := badger.Open(badger.DefaultOptions("").WithInMemory(true))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	storage := &BadgerRaftStorage{db: db}

	r := &Raft{
		id:               1,
		state:            StateFollower,
		hardState:        HardState{Term: 0, Vote: 0, CommitIndex: 0},
		raftLog:          &RaftLog{entries: []raftpb.Entry{{Term: 0, Index: 0}}, offset: 0, appliedIndex: 0},
		prs:              &ProgressTracker{prs: map[uint64]*Progress{1: {Match: 0, Next: 1}}},
		msgs:             []raftpb.Message{},
		lead:             0,
		electionTimeout:  10,
		heartbeatTimeout: 1,
		votes:            map[uint64]bool{},
		storage:          storage,
		readyEntries:     nil,
		committedEntries: nil,
		readySnapshot:    nil,
	}

	// Start node
	node := StartNode(r)

	// Give it a moment to start
	time.Sleep(10 * time.Millisecond)

	// Stop node
	node.Stop()
}

// TestNodePropose tests that Propose works
func TestNodePropose(t *testing.T) {
	db, err := badger.Open(badger.DefaultOptions("").WithInMemory(true))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	storage := &BadgerRaftStorage{db: db}

	r := &Raft{
		id:               1,
		state:            StateLeader, // Start as leader to accept proposals
		hardState:        HardState{Term: 1, Vote: 0, CommitIndex: 0},
		raftLog:          &RaftLog{entries: []raftpb.Entry{{Term: 0, Index: 0}}, offset: 0, appliedIndex: 0},
		prs:              &ProgressTracker{prs: map[uint64]*Progress{1: {Match: 0, Next: 1}}},
		msgs:             []raftpb.Message{},
		lead:             1,
		electionTimeout:  10,
		heartbeatTimeout: 1,
		votes:            map[uint64]bool{},
		storage:          storage,
		readyEntries:     nil,
		committedEntries: nil,
		readySnapshot:    nil,
	}

	node := StartNode(r)
	defer node.Stop()

	// Propose some data
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	err = node.Propose(ctx, []byte("test data"))
	if err != nil {
		t.Fatalf("Propose failed: %v", err)
	}

	// Give it time to process
	time.Sleep(50 * time.Millisecond)
}

// TestNodeReadyChannel tests that Ready channel works
func TestNodeReadyChannel(t *testing.T) {
	db, err := badger.Open(badger.DefaultOptions("").WithInMemory(true))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	storage := &BadgerRaftStorage{db: db}

	r := &Raft{
		id:               1,
		state:            StateLeader,
		hardState:        HardState{Term: 1, Vote: 0, CommitIndex: 0},
		raftLog:          &RaftLog{entries: []raftpb.Entry{{Term: 0, Index: 0}}, offset: 0, appliedIndex: 0},
		prs:              &ProgressTracker{prs: map[uint64]*Progress{1: {Match: 0, Next: 1}}},
		msgs:             []raftpb.Message{},
		lead:             1,
		electionTimeout:  10,
		heartbeatTimeout: 1,
		votes:            map[uint64]bool{},
		storage:          storage,
		readyEntries:     nil,
		committedEntries: nil,
		readySnapshot:    nil,
	}

	node := StartNode(r)
	defer node.Stop()

	// Propose to generate a Ready
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	err = node.Propose(ctx, []byte("test data"))
	if err != nil {
		t.Fatalf("Propose failed: %v", err)
	}

	// Read from Ready channel with timeout
	select {
	case rd := <-node.Ready():
		// Should receive a Ready with entries
		if len(rd.Entries) == 0 && len(rd.Messages) == 0 {
			t.Log("Received empty Ready, this is OK for initial state")
		}
		// Advance to acknowledge
		node.Advance()

	case <-time.After(2 * time.Second):
		t.Fatal("Timeout waiting for Ready")
	}
}

// TestNodeTick tests that Tick works
func TestNodeTick(t *testing.T) {
	db, err := badger.Open(badger.DefaultOptions("").WithInMemory(true))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	storage := &BadgerRaftStorage{db: db}

	r := &Raft{
		id:               1,
		state:            StateFollower,
		hardState:        HardState{Term: 0, Vote: 0, CommitIndex: 0},
		raftLog:          &RaftLog{entries: []raftpb.Entry{{Term: 0, Index: 0}}, offset: 0, appliedIndex: 0},
		prs:              &ProgressTracker{prs: map[uint64]*Progress{1: {Match: 0, Next: 1}}},
		msgs:             []raftpb.Message{},
		lead:             0,
		electionTimeout:  10,
		heartbeatTimeout: 1,
		votes:            map[uint64]bool{},
		storage:          storage,
		readyEntries:     nil,
		committedEntries: nil,
		readySnapshot:    nil,
	}

	node := StartNode(r)
	defer node.Stop()

	// Call Tick a few times
	for i := 0; i < 5; i++ {
		node.Tick()
		time.Sleep(10 * time.Millisecond)
	}

	// Should not crash
}

// TestNodeStop tests graceful shutdown
func TestNodeStop(t *testing.T) {
	db, err := badger.Open(badger.DefaultOptions("").WithInMemory(true))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	storage := &BadgerRaftStorage{db: db}

	r := &Raft{
		id:               1,
		state:            StateFollower,
		hardState:        HardState{Term: 0, Vote: 0, CommitIndex: 0},
		raftLog:          &RaftLog{entries: []raftpb.Entry{{Term: 0, Index: 0}}, offset: 0, appliedIndex: 0},
		prs:              &ProgressTracker{prs: map[uint64]*Progress{1: {Match: 0, Next: 1}}},
		msgs:             []raftpb.Message{},
		lead:             0,
		electionTimeout:  10,
		heartbeatTimeout: 1,
		votes:            map[uint64]bool{},
		storage:          storage,
		readyEntries:     nil,
		committedEntries: nil,
		readySnapshot:    nil,
	}

	node := StartNode(r)

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
}
