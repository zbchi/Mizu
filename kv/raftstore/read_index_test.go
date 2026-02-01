package raftstore

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/zbchi/mizu/kv/region"
	"github.com/zbchi/mizu/proto/raftpb"
	"github.com/zbchi/mizu/raft"
)

func TestPeerReadIndexWaitsForApply(t *testing.T) {
	peer := newReadIndexPeer(t, []uint64{1})
	if err := peer.Step(&raftpb.Message{
		Type: raftpb.Type_MsgHup,
		From: 1,
		To:   1,
	}); err != nil {
		t.Fatal(err)
	}
	peer.Advance()

	req := newReadRequest(context.Background().Done())
	peer.startLinearizableRead(req)
	peer.handleReadStates(peer.Ready().ReadStates)

	select {
	case err := <-req.done:
		t.Fatalf("read completed before applied index caught up: %v", err)
	default:
	}

	peer.SetAppliedIndex(1)
	peer.notifyAppliedReads()
	select {
	case err := <-req.done:
		if err != nil {
			t.Fatalf("read failed after applied index caught up: %v", err)
		}
	default:
		t.Fatal("read did not complete after applied index caught up")
	}
}

func TestPeerReadIndexRejectsFollower(t *testing.T) {
	peer := newReadIndexPeer(t, []uint64{1, 2, 3})
	req := newReadRequest(nil)
	peer.startLinearizableRead(req)

	err := <-req.done
	if !errors.Is(err, ErrNotLeader) {
		t.Fatalf("error %v, want ErrNotLeader", err)
	}
}

func TestPeerPendingReadFailsWhenLeaderStepsDown(t *testing.T) {
	peer := newReadIndexPeer(t, []uint64{1, 2, 3})
	if err := peer.Step(&raftpb.Message{
		Type: raftpb.Type_MsgHup,
		From: 1,
		To:   1,
	}); err != nil {
		t.Fatal(err)
	}
	if err := peer.Step(&raftpb.Message{
		Type:   raftpb.Type_MsgVoteResp,
		From:   2,
		To:     1,
		Term:   peer.Term(),
		Reject: false,
	}); err != nil {
		t.Fatal(err)
	}

	req := newReadRequest(nil)
	peer.startLinearizableRead(req)
	if err := peer.Step(&raftpb.Message{
		Type: raftpb.Type_MsgAppResp,
		From: 2,
		To:   1,
		Term: peer.Term() + 1,
	}); err != nil {
		t.Fatal(err)
	}

	err := <-req.done
	if !errors.Is(err, ErrNotLeader) {
		t.Fatalf("error %v, want ErrNotLeader", err)
	}
	if len(peer.reads.quorum) != 0 {
		t.Fatalf("read requests were not cleared: %d", len(peer.reads.quorum))
	}
}

func TestReadTrackerPrunesCanceledReads(t *testing.T) {
	var appliedIndex atomic.Uint64
	tracker := newReadTracker(&appliedIndex)
	canceled := make(chan struct{})
	req := newReadRequest(canceled)
	readID := tracker.start(req)
	tracker.handleStates([]raft.ReadState{{ReadID: readID, Index: 2}})
	close(canceled)
	tracker.pruneCanceled()
	if len(tracker.apply) != 0 {
		t.Fatalf("canceled read remains queued: %d", len(tracker.apply))
	}
	if err := <-req.done; !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled read error %v, want context.Canceled", err)
	}
}

func TestRestoredPeerDoesNotTreatCommitAsApplied(t *testing.T) {
	storage := raft.NewMemoryStorage()
	if err := storage.SaveEntries([]*raftpb.Entry{{
		Term:  1,
		Index: 1,
		Data:  []byte("committed"),
	}}); err != nil {
		t.Fatal(err)
	}
	if err := storage.SaveHardState(raft.HardState{Term: 1, CommitIndex: 1}); err != nil {
		t.Fatal(err)
	}

	peer := NewPeer(&region.Region{
		ID:    1,
		Peers: []region.PeerInfo{{NodeID: 1}},
	}, 1, storage)
	if got := peer.GetAppliedIndex(); got != 0 {
		t.Fatalf("applied index %d, want 0 before replay", got)
	}
	if got := len(peer.Ready().CommittedEntries); got != 1 {
		t.Fatalf("restored committed entries %d, want 1", got)
	}
}

func newReadIndexPeer(t *testing.T, nodeIDs []uint64) *Peer {
	t.Helper()
	peers := make([]region.PeerInfo, 0, len(nodeIDs))
	for _, nodeID := range nodeIDs {
		peers = append(peers, region.PeerInfo{NodeID: nodeID})
	}
	return NewPeer(&region.Region{
		ID:    1,
		Peers: peers,
	}, 1, raft.NewMemoryStorage())
}
