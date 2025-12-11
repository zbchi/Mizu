package raft

import (
	"context"
	"testing"
	"time"

	"github.com/zbchi/linkv/proto/raftpb"
)

// 创建测试用的 Raft 实例
func newTestRaft(id uint64, peers []uint64, state StateType) *Raft {
	r := NewRaft(Config{
		ID:               id,
		Peers:            peers,
		ElectionTimeout:  10,
		HeartbeatTimeout: 1,
	})
	r.state = state
	if state == StateLeader {
		r.lead = id
		r.hardState.Term = 1
	}
	return r
}

// TestNodeBasicLifecycle tests basic node start and stop
func TestNodeBasicLifecycle(t *testing.T) {
	r := newTestRaft(1, []uint64{1}, StateFollower)
	node := NewRawNodeWithRaft(r)

	// Give it a moment to start
	time.Sleep(10 * time.Millisecond)

	// Stop node
	node.Stop()
}

// TestNodePropose tests that Propose works
func TestNodePropose(t *testing.T) {
	r := newTestRaft(1, []uint64{1}, StateLeader)
	node := NewRawNodeWithRaft(r)
	defer node.Stop()

	// Propose some data
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	err := node.Propose(ctx, []byte("test data"))
	if err != nil {
		t.Fatalf("Propose failed: %v", err)
	}

	// Give it time to process
	time.Sleep(50 * time.Millisecond)
}

// TestNodeReadyChannel tests that Ready channel works
func TestNodeReadyChannel(t *testing.T) {
	r := newTestRaft(1, []uint64{1}, StateLeader)
	node := NewRawNodeWithRaft(r)
	defer node.Stop()

	// Propose to generate a Ready
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	err := node.Propose(ctx, []byte("test data"))
	if err != nil {
		t.Fatalf("Propose failed: %v", err)
	}

	// Read from Ready channel with timeout
	select {
	case rd := <-node.Ready():
		// Should receive a Ready with entries
		if len(rd.Entries) == 0 && len(rd.Messages) == 0 && rd.HardState == nil {
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
	r := newTestRaft(1, []uint64{1}, StateFollower)
	node := NewRawNodeWithRaft(r)
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
	r := newTestRaft(1, []uint64{1}, StateFollower)
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
}

// TestNodeStep tests that Step works
func TestNodeStep(t *testing.T) {
	r := newTestRaft(1, []uint64{1, 2}, StateFollower)
	node := NewRawNodeWithRaft(r)
	defer node.Stop()

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	// Send a vote request message
	msg := &raftpb.Message{
		Type: raftpb.Type_MsgVote,
		From: 2,
		To:   1,
		Term: 2,
	}

	err := node.Step(ctx, msg)
	if err != nil {
		t.Fatalf("Step failed: %v", err)
	}

	// Give it time to process
	time.Sleep(50 * time.Millisecond)
}

// TestNodeSnapshot tests that Snapshot works
func TestNodeSnapshot(t *testing.T) {
	r := newTestRaft(1, []uint64{1}, StateLeader)
	// 添加一些日志
	r.raftLog.Append(&raftpb.Entry{Term: 1, Index: 1, Data: []byte("data1")})
	r.raftLog.Append(&raftpb.Entry{Term: 1, Index: 2, Data: []byte("data2")})
	r.hardState.CommitIndex = 2
	r.raftLog.SetAppliedIndex(2)

	node := NewRawNodeWithRaft(r)
	defer node.Stop()

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	sn, err := node.Snapshot(ctx, 2, []byte("snapshot data"))
	if err != nil {
		t.Fatalf("Snapshot failed: %v", err)
	}

	if sn == nil {
		t.Fatal("Expected snapshot, got nil")
	}

	if sn.Index != 2 {
		t.Fatalf("Expected snapshot index 2, got %d", sn.Index)
	}
}
