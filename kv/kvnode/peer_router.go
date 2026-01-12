package kvnode

import (
	"sync"
	"sync/atomic"

	"github.com/zbchi/linkv/kv/kvnode/message"
)

var (
	// ErrPeerNotFound is returned when target peer is not found
	ErrPeerNotFound = NewError(ErrCodePeerNotFound, "peer not found")
)

// PeerState contains the peer state
type PeerState struct {
	peer   *Peer
	closed uint32 // atomic flag
}

// PeerRouter manages multiple peers and routes messages to them
type PeerRouter struct {
	peers     sync.Map // regionID -> *PeerState
	msgSender chan message.Msg
	closeCh   chan struct{}
}

// NewPeerRouter creates a new PeerRouter
func NewPeerRouter() *PeerRouter {
	return &PeerRouter{
		peers:     sync.Map{},
		msgSender: make(chan message.Msg, 4096),
		closeCh:   make(chan struct{}),
	}
}

// Get returns the peer state for the given region ID
func (pr *PeerRouter) Get(regionID uint64) *PeerState {
	v, ok := pr.peers.Load(regionID)
	if !ok {
		return nil
	}
	return v.(*PeerState)
}

// Register registers a peer to the router
func (pr *PeerRouter) Register(peer *Peer) {
	state := &PeerState{
		peer:   peer,
		closed: 0,
	}
	pr.peers.Store(peer.RegionID(), state)
}

// Close marks the peer as closed and removes it from router
func (pr *PeerRouter) Close(regionID uint64) {
	v, ok := pr.peers.Load(regionID)
	if ok {
		ps := v.(*PeerState)
		atomic.StoreUint32(&ps.closed, 1)
		pr.peers.Delete(regionID)
	}
}

// Send sends a message to the peer with the given region ID
func (pr *PeerRouter) Send(regionID uint64, msg message.Msg) error {
	ps := pr.Get(regionID)
	if ps == nil || atomic.LoadUint32(&ps.closed) == 1 {
		return ErrPeerNotFound
	}
	pr.msgSender <- msg
	return nil
}

// MsgChan returns the message sender channel
func (pr *PeerRouter) MsgChan() <-chan message.Msg {
	return pr.msgSender
}

// CloseAll closes all peers and stops the router
func (pr *PeerRouter) CloseAll() {
	close(pr.closeCh)
	// Delete all peers
	pr.peers.Range(func(key, value interface{}) bool {
		atomic.StoreUint32(&value.(*PeerState).closed, 1)
		pr.peers.Delete(key)
		return true
	})
}
