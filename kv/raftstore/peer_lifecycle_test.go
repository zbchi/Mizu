package raftstore

import (
	"testing"

	"github.com/zbchi/mizu/kv/region"
	"github.com/zbchi/mizu/proto/kvpb"
	"github.com/zbchi/mizu/proto/raftpb"
	"github.com/zbchi/mizu/raft"
)

func TestPeerOwnsProposalLifecycle(t *testing.T) {
	peer := newLifecyclePeer()
	if err := peer.Step(&raftpb.Message{Type: raftpb.Type_MsgHup, From: 1, To: 1}); err != nil {
		t.Fatal(err)
	}

	pending := &proposal{request: &kvpb.RaftCmdRequest{}, future: newProposalFuture()}
	if !peer.propose([]byte("command"), pending) {
		t.Fatal("leader rejected proposal")
	}
	index := peer.raft.LastIndex()
	if got := peer.takeProposal(index); got != pending {
		t.Fatal("peer did not return its pending proposal")
	}
	if got := peer.takeProposal(index); got != nil {
		t.Fatal("completed proposal remained registered")
	}
}

func TestPeerAllowsOnePendingSnapshot(t *testing.T) {
	peer := newLifecyclePeer()
	peer.SetAppliedIndex(SnapshotThreshold)

	index, ok := peer.beginSnapshot(SnapshotThreshold)
	if !ok || index != SnapshotThreshold {
		t.Fatalf("BeginSnapshot = (%d, %t), want (%d, true)", index, ok, SnapshotThreshold)
	}
	if _, ok := peer.beginSnapshot(SnapshotThreshold); ok {
		t.Fatal("second snapshot was allowed while first was pending")
	}

	peer.cancelSnapshot(index)
	if _, ok := peer.beginSnapshot(SnapshotThreshold); !ok {
		t.Fatal("snapshot was not retried after cancellation")
	}
}

func newLifecyclePeer() *Peer {
	return NewPeer(&region.Region{
		ID:    1,
		Peers: []region.PeerInfo{{NodeID: 1}},
	}, 1, raft.NewMemoryStorage())
}
