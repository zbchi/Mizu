package raftstore

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/zbchi/mizu/proto/kvpb"
)

var (
	ErrPeerNotFound   = errors.New("peer not found")
	ErrRegionNotFound = errors.New("region not found")
	ErrKeyNotInRegion = errors.New("key not in region")
	ErrNotLeader      = errors.New("not leader")
)

// Store manages all raft peers, processes raft messages, and drives raft state machines.
// It encapsulates the raft layer implementation details from the KVNode.
type Store struct {
	peers        *peerRegistry
	callbackMgr  *CallbackManager
	raftWorker   *raftWorker
	applyWorker  *applyWorker
	transport    Transport
	stopCh       chan struct{}
	stopOnce     sync.Once
	nodeID       uint64
	stateMachine StateMachine
}

// NewStore creates a fully initialized Store for managing raft peers.
func NewStore(nodeID uint64, stateMachine StateMachine) *Store {
	s := &Store{
		nodeID:       nodeID,
		stopCh:       make(chan struct{}),
		peers:        newPeerRegistry(),
		stateMachine: stateMachine,
	}
	s.callbackMgr = NewCallbackManager(s)
	s.raftWorker = newRaftWorker(s.peers, s)
	s.applyWorker = newApplyWorker(s)
	return s
}

// Start starts the raft worker, apply worker, and ticker
func (s *Store) Start() {
	s.raftWorker.start(s.stopCh)
	s.applyWorker.start(s.stopCh)
	go s.runTicker()
}

// Stop stops the raft worker, apply worker, and closes all peers
func (s *Store) Stop() {
	s.stopOnce.Do(func() { close(s.stopCh) })
	s.raftWorker.wait()
	s.applyWorker.wait()
	s.peers.clear()
}

// SetTransport sets the transport for network communication
func (s *Store) SetTransport(t Transport) {
	s.transport = t
	s.raftWorker.transport = t
	go s.receiveLoop()
}

// receiveLoop forwards network messages to the Raft worker.
func (s *Store) receiveLoop() {
	recvCh := s.transport.Receive()
	for msg := range recvCh {
		raftMsg := Msg{
			Type:     MsgTypeRaftMessage,
			RegionID: msg.RegionId,
			Data:     msg,
		}
		if err := s.raftWorker.submit(raftMsg); err != nil {
			slog.Debug("Failed to route raft message", "region", msg.RegionId, "error", err)
		}
	}
}

// AddPeer adds one region peer to the store.
func (s *Store) AddPeer(peer *Peer) error {
	return s.peers.register(peer)
}

func (s *Store) peer(regionID uint64) *Peer {
	return s.peers.get(regionID)
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
	return s.raftWorker.submit(tickMsg)
}

func (s *Store) sendCmd(regionID uint64, cmd *RaftCmd) error {
	cmdMsg := Msg{
		Type:     MsgTypeRaftCmd,
		RegionID: regionID,
		Data:     cmd,
	}
	return s.raftWorker.submit(cmdMsg)
}

func (s *Store) requestReadIndex(ctx context.Context, regionID uint64) (ReadIndexResult, error) {
	req := &ReadIndexRequest{Resp: make(chan ReadIndexResult, 1)}
	msg := Msg{
		Type:     MsgTypeReadIndex,
		RegionID: regionID,
		Data:     req,
	}
	if err := s.raftWorker.submit(msg); err != nil {
		return ReadIndexResult{}, err
	}

	select {
	case result := <-req.Resp:
		return result, nil
	case <-ctx.Done():
		return ReadIndexResult{}, ctx.Err()
	case <-s.stopCh:
		return ReadIndexResult{}, context.Canceled
	}
}

// Propose submits one raft command to one peer and waits for its callback response.
func (s *Store) Propose(regionID uint64, req *kvpb.RaftCmdRequest) (*kvpb.RaftCmdResponse, error) {
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
	case <-s.stopCh:
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
			for _, regionID := range s.peers.listRegionIDs() {
				if err := s.sendTick(regionID); err != nil {
					slog.Debug("Failed to send tick", "region", regionID, "error", err)
				}
			}
		case <-s.stopCh:
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
func (s *Store) applyCommand(regionID uint64, req *kvpb.RaftCmdRequest) error {
	return s.stateMachine.ApplyCommand(regionID, req)
}

func (s *Store) createSnapshot(regionID uint64) ([]byte, error) {
	return s.stateMachine.CreateSnapshot(regionID)
}

func (s *Store) applySnapshot(regionID uint64, data []byte) error {
	return s.stateMachine.ApplySnapshot(regionID, data)
}

// BuildResponse builds a response with the store's current routing metadata.
func (s *Store) BuildResponse(req *kvpb.RaftCmdRequest, regionID uint64, responses []*kvpb.Response, err error) *kvpb.RaftCmdResponse {
	header := &kvpb.ResponseHeader{
		ClusterId: requestClusterID(req),
		NodeId:    s.nodeID,
		Success:   err == nil,
	}

	peer := s.peer(regionID)
	if peer != nil {
		reg := peer.Region()
		header.RegionId = reg.ID
		header.RegionStartKey = cloneBytes(reg.StartKey)
		header.RegionEndKey = cloneBytes(reg.EndKey)
		header.LeaderNodeId = peer.LeaderNodeID()
	}

	header.Error = responseError(err, header)
	return &kvpb.RaftCmdResponse{Header: header, Responses: responses}
}
