package kvnode

import (
	"testing"

	"github.com/zbchi/linkv/kv/kvnode/message"
	"github.com/zbchi/linkv/kv/region"
)

// newTestPeer creates a test peer
func newTestPeer(id uint64) *Peer {
	return &Peer{
		region:        &region.Region{ID: id, StartKey: []byte{}, EndKey: []byte{}},
		appliedIndex: 10,
		readWaitQueue: &ReadWaitQueue{},
	}
}

func TestPeerRouter(t *testing.T) {
	router := NewPeerRouter()

	peer := newTestPeer(1)

	// Register peer for region 1
	router.Register(peer)

	// Get should return the peer
	ps := router.Get(1)
	if ps == nil {
		t.Fatal("expected peer to be registered")
	}
	if ps.peer != peer {
		t.Fatal("expected same peer")
	}

	// Get for non-existent region should return nil
	ps = router.Get(999)
	if ps != nil {
		t.Fatal("expected nil for non-existent region")
	}

	// Close region 1
	router.Close(1)

	// Get should return nil after close
	ps = router.Get(1)
	if ps != nil {
		t.Fatal("expected nil after close")
	}
}

func TestPeerRouterSend(t *testing.T) {
	router := NewPeerRouter()

	peer := newTestPeer(1)
	router.Register(peer)

	// Send to existing peer should succeed
	msg := message.Msg{Type: message.MsgTypeTick, RegionID: 1}
	err := router.Send(1, msg)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	// Send to non-existent peer should return error
	err = router.Send(999, msg)
	if err == nil {
		t.Fatal("expected error for non-existent peer")
	}
	if err != ErrPeerNotFound {
		t.Fatalf("expected ErrPeerNotFound, got %v", err)
	}
}

func TestPeerRouterMultiplePeers(t *testing.T) {
	router := NewPeerRouter()

	// Register multiple peers
	for i := uint64(1); i <= 3; i++ {
		router.Register(newTestPeer(i))
	}

	// All peers should be registered
	for i := uint64(1); i <= 3; i++ {
		ps := router.Get(i)
		if ps == nil {
			t.Fatalf("expected peer %d to be registered", i)
		}
	}

	// Close all
	router.CloseAll()

	// All peers should be closed
	for i := uint64(1); i <= 3; i++ {
		ps := router.Get(i)
		if ps != nil {
			t.Fatalf("expected peer %d to be removed after close", i)
		}
	}
}
