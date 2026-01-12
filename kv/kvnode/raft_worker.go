package kvnode

import (
	"log/slog"
	"sync"

	"github.com/zbchi/linkv/kv/kvnode/message"
	"github.com/zbchi/linkv/proto/raftkvpb"
	"github.com/zbchi/linkv/proto/raftpb"
	"github.com/zbchi/linkv/raft"
	protov2 "google.golang.org/protobuf/proto"
)

// raftWorker is responsible for processing raft messages in batches
type raftWorker struct {
	router    *PeerRouter
	node      *KVNode
	transport Transport
	msgCh     <-chan message.Msg
	closeCh   chan struct{}
	stopCh    chan struct{}
	wg        *sync.WaitGroup
}

// newRaftWorker creates a new raftWorker
func newRaftWorker(router *PeerRouter, node *KVNode) *raftWorker {
	return &raftWorker{
		router:  router,
		node:    node,
		msgCh:   router.MsgChan(),
		closeCh: make(chan struct{}),
		stopCh:  make(chan struct{}),
		wg:      &sync.WaitGroup{},
	}
}

// start starts the raftWorker
func (rw *raftWorker) start(closeCh <-chan struct{}) {
	rw.wg.Add(1)
	go func() {
		defer rw.wg.Done()
		rw.run(closeCh)
	}()
}

// stop stops the raftWorker gracefully
func (rw *raftWorker) stop() {
	close(rw.stopCh)
	rw.wg.Wait()
}

// run runs the raft worker loop
func (rw *raftWorker) run(closeCh <-chan struct{}) {
	var msgs []message.Msg
	for {
		msgs = msgs[:0]

		// Wait for first message or close signal
		select {
		case <-closeCh:
			return
		case <-rw.stopCh:
			return
		case msg := <-rw.msgCh:
			msgs = append(msgs, msg)
		}

		// Batch read: drain all pending messages
		pending := len(rw.msgCh)
		for i := 0; i < pending; i++ {
			msgs = append(msgs, <-rw.msgCh)
		}

		// Group messages by RegionID
		peerMap := make(map[uint64][]message.Msg)
		for _, msg := range msgs {
			peerMap[msg.RegionID] = append(peerMap[msg.RegionID], msg)
		}

		// Process each peer's messages
		for regionID, msgs := range peerMap {
			peerState := rw.router.Get(regionID)
			if peerState == nil {
				slog.Debug("Peer not found", "region", regionID)
				continue
			}

			// Process all messages for this peer
			for _, msg := range msgs {
				rw.handleMsg(peerState.peer, msg)
				// Check and process ready after each message
				rw.handleReady(peerState.peer)
			}
		}
	}
}

// handleMsg handles a single message for a peer
func (rw *raftWorker) handleMsg(peer *Peer, msg message.Msg) {
	switch msg.Type {
	case message.MsgTypeRaftMessage:
		raftMsg, ok := msg.Data.(*raftpb.Message)
		if !ok {
			slog.Warn("Invalid raft message type", "region", peer.RegionID())
			return
		}
		if err := peer.Step(raftMsg); err != nil {
			slog.Warn("Failed to step raft", "region", peer.RegionID(), "error", err)
		}

	case message.MsgTypeRaftCmd:
		cmd, ok := msg.Data.(*RaftCmd)
		if !ok {
			slog.Warn("Invalid raft cmd type", "region", peer.RegionID())
			return
		}
		rw.handleRaftCmd(peer, cmd)

	case message.MsgTypeTick:
		peer.Tick()

	default:
		slog.Warn("Unknown message type", "type", msg.Type, "region", peer.RegionID())
	}
}

// handleRaftCmd handles a raft command (client proposal)
func (rw *raftWorker) handleRaftCmd(peer *Peer, cmd *RaftCmd) {
	// Marshal the request
	data, err := protov2.Marshal(cmd.Request)
	if err != nil {
		rw.node.callbackMgr.TriggerForRegion(peer.RegionID(), 0, 0, err)
		return
	}

	// Register callback BEFORE proposing
	rw.node.callbackMgr.Register(cmd, peer.RegionID())

	// Propose to Raft
	if !peer.Propose(data) {
		rw.node.callbackMgr.Unregister(cmd, peer.RegionID())
		rw.node.callbackMgr.TriggerForRegion(peer.RegionID(), 0, 0, ErrNotLeader)
	}
}

// handleReady processes the ready state for a peer
func (rw *raftWorker) handleReady(peer *Peer) {
	rd := peer.Ready()
	if rd.IsEmpty() {
		return
	}

	// Process ready
	if err := rw.processReady(peer, rd); err != nil {
		slog.Error("Failed to process ready", "region", peer.RegionID(), "error", err)
		return
	}

	peer.Advance()
}

// processReady processes the raft ready state for a peer
func (rw *raftWorker) processReady(peer *Peer, rd raft.Ready) error {
	// Save HardState
	if rd.HardState != nil && !rd.HardState.IsEmpty() {
		if err := peer.raftStorage.SaveHardState(*rd.HardState); err != nil {
			return err
		}
	}

	// Save Entries
	if len(rd.Entries) > 0 {
		if err := peer.raftStorage.SaveEntries(rd.Entries); err != nil {
			return err
		}
	}

	// Save/Apply Snapshot
	if rd.Snapshot != nil {
		if err := peer.raftStorage.SaveSnapshot(rd.Snapshot); err != nil {
			return err
		}
		if err := peer.raftStorage.ApplySnapshotData(rd.Snapshot.Data); err != nil {
			return err
		}
	}

	// Send messages
	for _, msg := range rd.Messages {
		msg.RegionId = peer.RegionID()
		if rw.transport != nil {
			rw.transport.Send(msg)
		}
	}

	// Apply committed entries
	if len(rd.CommittedEntries) > 0 {
		rw.applyEntries(peer, rd.CommittedEntries)
	}

	return nil
}

// applyEntries applies committed entries for a peer
func (rw *raftWorker) applyEntries(peer *Peer, entries []*raftpb.Entry) {
	for _, entry := range entries {
		rw.applyEntry(peer, entry)
		peer.appliedIndex = entry.Index
	}
	// Notify waiting read requests for this peer
	peer.notifyReadWaitQueue()
}

// applyEntry applies a single entry
func (rw *raftWorker) applyEntry(peer *Peer, entry *raftpb.Entry) {
	if len(entry.Data) == 0 {
		return
	}

	var req raftkvpb.RaftCmdRequest
	if err := protov2.Unmarshal(entry.Data, &req); err != nil {
		rw.node.callbackMgr.TriggerForRegion(peer.RegionID(), entry.Index, entry.Term, err)
		return
	}

	if err := rw.node.applyCommand(&req); err != nil {
		rw.node.callbackMgr.TriggerForRegion(peer.RegionID(), entry.Index, entry.Term, err)
		return
	}

	rw.node.callbackMgr.TriggerForRegion(peer.RegionID(), entry.Index, entry.Term, nil)
}
