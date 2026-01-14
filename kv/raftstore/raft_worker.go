package raftstore

import (
	"errors"
	"log/slog"
	"sync"

	"github.com/zbchi/linkv/proto/raftpb"
	"github.com/zbchi/linkv/raft"
	protov2 "google.golang.org/protobuf/proto"
)

var (
	// ErrNotLeader is returned when a command is proposed by a follower
	ErrNotLeader = errors.New("not leader")
)

// raftWorker is responsible for processing raft messages in batches
type raftWorker struct {
	router    *PeerRouter
	store     *Store
	transport Transport
	msgCh     <-chan Msg
	closeCh   chan struct{}
	stopCh    chan struct{}
	wg        *sync.WaitGroup
}

// newRaftWorker creates a new raftWorker
func newRaftWorker(router *PeerRouter, store *Store) *raftWorker {
	return &raftWorker{
		router:  router,
		store:   store,
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
	var msgs []Msg
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
		peerMap := make(map[uint64][]Msg)
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
func (rw *raftWorker) handleMsg(peer *Peer, msg Msg) {
	switch msg.Type {
	case MsgTypeRaftMessage:
		raftMsg, ok := msg.Data.(*raftpb.Message)
		if !ok {
			slog.Warn("Invalid raft message type", "region", peer.RegionID())
			return
		}
		if err := peer.Step(raftMsg); err != nil {
			slog.Warn("Failed to step raft", "region", peer.RegionID(), "error", err)
		}

	case MsgTypeRaftCmd:
		cmd, ok := msg.Data.(*RaftCmd)
		if !ok {
			slog.Warn("Invalid raft cmd type", "region", peer.RegionID())
			return
		}
		rw.handleRaftCmd(peer, cmd)

	case MsgTypeTick:
		peer.Tick()

	default:
		slog.Warn("Unknown message type", "type", msg.Type, "region", peer.RegionID())
	}
}

// handleRaftCmd handles a raft command (client proposal)
func (rw *raftWorker) handleRaftCmd(peer *Peer, cmd *RaftCmd) {
	slog.Info("handleRaftCmd: processing command", "region", peer.RegionID())

	// Marshal the request
	data, err := protov2.Marshal(cmd.Request)
	if err != nil {
		slog.Error("handleRaftCmd: failed to marshal request", "error", err)
		rw.store.CallbackMgr().TriggerForRegion(peer.RegionID(), 0, 0, err)
		return
	}

	// Register callback BEFORE proposing
	rw.store.RegisterCallback(cmd, peer.RegionID())

	// Propose to Raft
	if !peer.Propose(data) {
		slog.Error("handleRaftCmd: propose failed", "region", peer.RegionID())
		rw.store.UnregisterCallback(cmd, peer.RegionID())
		rw.store.CallbackMgr().TriggerForRegion(peer.RegionID(), 0, 0, ErrNotLeader)
		return
	}

	slog.Info("handleRaftCmd: propose succeeded", "region", peer.RegionID())
}

// handleReady processes the ready state for a peer
func (rw *raftWorker) handleReady(peer *Peer) {
	rd := peer.Ready()
	if rd.IsEmpty() {
		return
	}

	slog.Debug("handleReady: processing ready state", "region", peer.RegionID(),
		"entries", len(rd.Entries), "committed", len(rd.CommittedEntries), "messages", len(rd.Messages))

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

	// Persist Snapshot (只保存，不apply)
	if !raft.IsEmptySnap(rd.Snapshot) {
		if err := peer.raftStorage.SaveSnapshot(rd.Snapshot); err != nil {
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

	// 提交 snapshot + committed entries 给 apply_worker
	if !raft.IsEmptySnap(rd.Snapshot) || len(rd.CommittedEntries) > 0 {
		slog.Info("processReady: submitting task to applyWorker", "region", peer.RegionID(),
			"hasSnapshot", !raft.IsEmptySnap(rd.Snapshot), "entries", len(rd.CommittedEntries))
		rw.store.applyWorker.Submit(ApplyTask{
			RegionID: peer.RegionID(),
			Peer:     peer,
			Snapshot: rd.Snapshot,
			Entries:  rd.CommittedEntries,
		})
	}

	return nil
}

// Transport defines the interface for sending and receiving Raft messages over network
type Transport interface {
	Send(msg *raftpb.Message) error
	Start() error
	Close() error
	Receive() <-chan *raftpb.Message
}
