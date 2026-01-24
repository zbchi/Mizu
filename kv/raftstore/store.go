package raftstore

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/zbchi/mizu/kv/region"
	"github.com/zbchi/mizu/proto/raftkvpb"
	"github.com/zbchi/mizu/raft"
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
	// MsgTypeSnapshotTrigger is for triggering snapshot creation
	MsgTypeSnapshotTrigger
	// MsgTypeReadIndex is for serializing linearizable read checks through the raft worker
	MsgTypeReadIndex
)

// Msg is an internal message for routing to peers
type Msg struct {
	Type     MsgType
	RegionID uint64
	Data     interface{}
}

// SnapshotTrigger represents a trigger for snapshot creation
type SnapshotTrigger struct {
	Index uint64
	Data  []byte
}

// ReadIndexRequest asks the raft worker to evaluate a linearizable read for a region.
type ReadIndexRequest struct {
	Resp chan ReadIndexResult
}

// ReadIndexResult contains the outcome of a serialized read-index check.
type ReadIndexResult struct {
	Ready <-chan struct{}
	Err   error
}

// CommandApplier is the interface for applying raft commands to state machine
type CommandApplier interface {
	ApplyCommand(regionID uint64, req *raftkvpb.RaftCmdRequest) error
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
	RaftStorage(regionID uint64) raft.RaftStorage
}

// NewStore creates a fully initialized Store for managing raft peers.
func NewStore(nodeID uint64, storage RaftStorageProvider, closeCh chan struct{}, applier CommandApplier) *Store {
	s := &Store{
		nodeID:     nodeID,
		storage:    storage,
		regionMap:  region.NewRegionMap(),
		closeCh:    closeCh,
		peerRouter: NewPeerRouter(),
		applier:    applier,
	}
	s.callbackMgr = NewCallbackManager(s)
	s.raftWorker = newRaftWorker(s.peerRouter, s)
	s.applyWorker = newApplyWorker(s)
	return s
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
func (s *Store) AddPeer(reg *region.Region, peer *Peer) error {
	if err := s.regionMap.AddRegion(reg); err != nil {
		slog.Error("Failed to add region", "region", reg.ID, "error", err)
		return err
	}
	if peer == nil {
		slog.Error("Failed to add peer", "region", reg.ID, "reason", "peer is nil")
		return errors.New("peer is nil")
	}
	s.peerRouter.Register(peer)
	return nil
}

func (s *Store) peer(regionID uint64) *Peer {
	ps := s.peerRouter.Get(regionID)
	if ps == nil {
		return nil
	}
	return ps.peer
}

// nextIndex returns the index that will be assigned to the next proposed entry for a region.
func (s *Store) nextIndex(regionID uint64) uint64 {
	peer := s.peer(regionID)
	if peer == nil {
		return 0
	}
	return peer.NextIndex()
}

func (s *Store) sendTick(regionID uint64) error {
	tickMsg := Msg{
		Type:     MsgTypeTick,
		RegionID: regionID,
		Data:     nil,
	}
	return s.peerRouter.Send(regionID, tickMsg)
}

func (s *Store) sendCmd(regionID uint64, cmd *RaftCmd) error {
	cmdMsg := Msg{
		Type:     MsgTypeRaftCmd,
		RegionID: regionID,
		Data:     cmd,
	}
	return s.peerRouter.Send(regionID, cmdMsg)
}

func (s *Store) requestReadIndex(ctx context.Context, regionID uint64) (ReadIndexResult, error) {
	req := &ReadIndexRequest{Resp: make(chan ReadIndexResult, 1)}
	msg := Msg{
		Type:     MsgTypeReadIndex,
		RegionID: regionID,
		Data:     req,
	}
	if err := s.peerRouter.Send(regionID, msg); err != nil {
		return ReadIndexResult{}, err
	}

	select {
	case result := <-req.Resp:
		return result, nil
	case <-ctx.Done():
		return ReadIndexResult{}, ctx.Err()
	case <-s.closeCh:
		return ReadIndexResult{}, context.Canceled
	}
}

// Propose submits one raft command to one peer and waits for its callback response.
func (s *Store) Propose(regionID uint64, req *raftkvpb.RaftCmdRequest) (*raftkvpb.RaftCmdResponse, error) {
	cb := &Callback{Done: make(chan struct{})}
	cmd := &RaftCmd{
		Request: req,
		Cb:      cb,
	}

	if err := s.sendCmd(regionID, cmd); err != nil {
		return nil, err
	}

	resp, err := cb.Wait()
	if err != nil && resp != nil {
		return resp, nil
	}
	return resp, err
}

// WaitLinearizableRead waits until the region can be read locally under ReadIndex semantics.
func (s *Store) WaitLinearizableRead(ctx context.Context, regionID uint64) error {
	readState, err := s.requestReadIndex(ctx, regionID)
	if err != nil {
		return err
	}
	if readState.Err != nil || readState.Ready == nil {
		return readState.Err
	}

	select {
	case <-readState.Ready:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-s.closeCh:
		return context.Canceled
	}
}

// runTicker runs ticker for all peers
func (s *Store) runTicker() {
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			for _, regionID := range s.peerRouter.ListRegionIDs() {
				if err := s.sendTick(regionID); err != nil {
					slog.Debug("Failed to send tick", "region", regionID, "error", err)
				}
			}
		case <-s.closeCh:
			return
		}
	}
}

func (s *Store) registerCallback(cmd *RaftCmd, regionID uint64) {
	s.callbackMgr.Register(cmd, regionID)
}

func (s *Store) unregisterCallback(cmd *RaftCmd, regionID uint64) {
	s.callbackMgr.Unregister(cmd, regionID)
}

func (s *Store) triggerCallback(regionID, index uint64, err error) {
	s.callbackMgr.TriggerForRegion(regionID, index, err)
}

// applyCommand applies a command using the registered applier.
func (s *Store) applyCommand(regionID uint64, req *raftkvpb.RaftCmdRequest) error {
	if s.applier == nil {
		return nil
	}
	return s.applier.ApplyCommand(regionID, req)
}

// BuildResponse builds a response with the store's current routing metadata.
func (s *Store) BuildResponse(req *raftkvpb.RaftCmdRequest, regionID uint64, responses []*raftkvpb.Response, err error) *raftkvpb.RaftCmdResponse {
	header := &raftkvpb.ResponseHeader{
		ClusterId: requestClusterID(req),
		NodeId:    s.nodeID,
		Success:   err == nil,
	}

	reg := s.regionMap.GetRegionByID(regionID)
	if reg != nil {
		header.RegionId = reg.ID
		header.RegionStartKey = cloneBytes(reg.StartKey)
		header.RegionEndKey = cloneBytes(reg.EndKey)
	}

	if peer := s.peer(regionID); peer != nil {
		header.LeaderNodeId = peer.LeaderNodeID()
	}

	header.Error = responseError(err, header)
	return &raftkvpb.RaftCmdResponse{Header: header, Responses: responses}
}
