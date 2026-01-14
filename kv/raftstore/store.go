package raftstore

import (
	"log/slog"
	"time"

	"github.com/zbchi/linkv/kv/region"
	"github.com/zbchi/linkv/proto/raftkvpb"
	"github.com/zbchi/linkv/proto/raftpb"
	"github.com/zbchi/linkv/raft"
)

// MsgType represents the type of internal message
type MsgType int

const (
	// MsgTypeRaftMessage is for raft messages from network
	MsgTypeRaftMessage MsgType = iota
	// MsgTypeRaftCmd is for client commands
	MsgTypeRaftCmd
	// MsgTypeTick is for raft ticker
	MsgTypeTick
)

// Msg is an internal message for routing to peers
type Msg struct {
	Type     MsgType
	RegionID uint64
	Data     interface{}
}

// CommandApplier is the interface for applying raft commands to state machine
type CommandApplier interface {
	ApplyCommand(req *raftkvpb.RaftCmdRequest) error
}

// Store manages all raft peers, processes raft messages, and drives raft state machines.
// It encapsulates the raft layer implementation details from the KVNode.
type Store struct {
	peerRouter  *PeerRouter
	callbackMgr *CallbackManager
	raftWorker  *raftWorker
	applyWorker *applyWorker
	regionMap   *region.RegionMap
	transport   Transport
	closeCh     chan struct{}
	nodeID      uint64
	storage     RaftStorageProvider
	applier     CommandApplier
}

// RaftStorageProvider provides raft storage for peers
type RaftStorageProvider interface {
	RaftStorage() raft.RaftStorage
}

// NewStore creates a new Store for managing raft peers
func NewStore(nodeID uint64, storage RaftStorageProvider, closeCh chan struct{}) *Store {
	s := &Store{
		nodeID:     nodeID,
		storage:    storage,
		regionMap:  region.NewRegionMap(),
		closeCh:    closeCh,
		peerRouter: NewPeerRouter(),
	}
	return s
}

// Init initializes store with callback manager and command applier
func (s *Store) Init(node PeerGetter, applier CommandApplier) {
	s.callbackMgr = NewCallbackManager(node)
	s.applier = applier
	s.raftWorker = newRaftWorker(s.peerRouter, s)
	s.applyWorker = newApplyWorker(s)
}

// Start starts the raft worker, apply worker, and ticker
func (s *Store) Start() {
	s.raftWorker.start(s.closeCh)
	s.applyWorker.start()
	go s.runTicker()
}

// Stop stops the raft worker, apply worker, and closes all peers
func (s *Store) Stop() {
	if s.raftWorker != nil {
		s.raftWorker.stop()
	}
	if s.applyWorker != nil {
		s.applyWorker.stop()
	}
	s.peerRouter.CloseAll()
}

// SetTransport sets the transport for network communication
func (s *Store) SetTransport(t Transport) {
	s.transport = t
	if s.raftWorker != nil {
		s.raftWorker.transport = t
	}
	go s.receiveLoop()
}

// receiveLoop receives messages from transport and forwards to peerRouter
func (s *Store) receiveLoop() {
	recvCh := s.transport.Receive()
	for msg := range recvCh {
		raftMsg := Msg{
			Type:     MsgTypeRaftMessage,
			RegionID: msg.RegionId,
			Data:     msg,
		}
		s.peerRouter.Send(msg.RegionId, raftMsg)
	}
}

// AddPeer adds a new peer for a region
func (s *Store) AddPeer(reg *region.Region, peer *Peer) {
	if err := s.regionMap.AddRegion(reg); err != nil {
		slog.Error("Failed to add region", "region", reg.ID, "error", err)
		return
	}
	s.peerRouter.Register(peer)
}

// GetPeer returns peer by regionID
func (s *Store) GetPeer(regionID uint64) *Peer {
	ps := s.peerRouter.Get(regionID)
	if ps == nil {
		return nil
	}
	return ps.peer
}

// NextIndex returns the index that will be assigned to the next proposed entry for a region
func (s *Store) NextIndex(regionID uint64) uint64 {
	peer := s.GetPeer(regionID)
	if peer == nil {
		return 0
	}
	return peer.NextIndex()
}

// SendTick sends a tick message to a peer
func (s *Store) SendTick(regionID uint64) error {
	tickMsg := Msg{
		Type:     MsgTypeTick,
		RegionID: regionID,
		Data:     nil,
	}
	return s.peerRouter.Send(regionID, tickMsg)
}

// SendCmd sends a raft command to a peer
func (s *Store) SendCmd(regionID uint64, cmd *RaftCmd) error {
	cmdMsg := Msg{
		Type:     MsgTypeRaftCmd,
		RegionID: regionID,
		Data:     cmd,
	}
	return s.peerRouter.Send(regionID, cmdMsg)
}

// ProcessIncomingMessage processes an incoming raft message from network
func (s *Store) ProcessIncomingMessage(msg *raftpb.Message) error {
	raftMsg := Msg{
		Type:     MsgTypeRaftMessage,
		RegionID: msg.RegionId,
		Data:     msg,
	}
	return s.peerRouter.Send(msg.RegionId, raftMsg)
}

// runTicker runs ticker for all peers
func (s *Store) runTicker() {
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	// Default to region 1 for now
	regionID := uint64(1)
	for {
		select {
		case <-ticker.C:
			s.SendTick(regionID)
		case <-s.closeCh:
			return
		}
	}
}

// RegisterCallback registers a callback for a raft command
func (s *Store) RegisterCallback(cmd *RaftCmd, regionID uint64) {
	s.callbackMgr.Register(cmd, regionID)
}

// UnregisterCallback unregisters a callback for a raft command
func (s *Store) UnregisterCallback(cmd *RaftCmd, regionID uint64) {
	s.callbackMgr.Unregister(cmd, regionID)
}

// TriggerCallback triggers a callback for a specific region
func (s *Store) TriggerCallback(regionID, index, term uint64, err error) {
	s.callbackMgr.TriggerForRegion(regionID, index, term, err)
}

// CallbackMgr returns the callback manager
func (s *Store) CallbackMgr() *CallbackManager {
	return s.callbackMgr
}

// NodeID returns the current node ID
func (s *Store) NodeID() uint64 {
	return s.nodeID
}

// ApplyCommand applies a command using the registered applier
func (s *Store) ApplyCommand(req *raftkvpb.RaftCmdRequest) error {
	if s.applier == nil {
		return nil
	}
	return s.applier.ApplyCommand(req)
}
