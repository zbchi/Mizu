package raftstore

import (
	"log/slog"
	"sync"

	"github.com/zbchi/mizu/proto/raftpb"
	"github.com/zbchi/mizu/raft"
	protov2 "google.golang.org/protobuf/proto"
)

// raftWorker serializes events for all local Raft peers.
type raftWorker struct {
	peers     *peerRegistry
	store     *Store
	transport Transport
	msgCh     chan peerEvent
	wg        sync.WaitGroup
}

// newRaftWorker creates a new raftWorker
func newRaftWorker(peers *peerRegistry, store *Store) *raftWorker {
	return &raftWorker{
		peers: peers,
		store: store,
		msgCh: make(chan peerEvent, 4096),
	}
}

func (rw *raftWorker) submit(event peerEvent) error {
	if rw.peers.get(event.regionID) == nil {
		return ErrPeerNotFound
	}
	rw.msgCh <- event
	return nil
}

// start starts the raftWorker
func (rw *raftWorker) start(stopCh <-chan struct{}) {
	rw.wg.Add(1)
	go func() {
		defer rw.wg.Done()
		rw.run(stopCh)
	}()
}

func (rw *raftWorker) wait() {
	rw.wg.Wait()
}

// run runs the raft worker loop
func (rw *raftWorker) run(stopCh <-chan struct{}) {
	var events []peerEvent
	for {
		events = events[:0]

		// Wait for the first event or close signal.
		select {
		case <-stopCh:
			return
		case event := <-rw.msgCh:
			events = append(events, event)
		}

		// Batch read: drain all pending events.
		pending := len(rw.msgCh)
		for i := 0; i < pending; i++ {
			events = append(events, <-rw.msgCh)
		}

		// Group events by RegionID.
		peerMap := make(map[uint64][]peerEvent)
		for _, event := range events {
			peerMap[event.regionID] = append(peerMap[event.regionID], event)
		}

		// Process each peer's events.
		for regionID, events := range peerMap {
			peer := rw.peers.get(regionID)
			if peer == nil {
				slog.Debug("Peer not found", "region", regionID)
				continue
			}

			// Process all events for this peer.
			for _, event := range events {
				rw.handleEvent(peer, event.event)
				// Check and process ready after each message
				rw.handleReady(peer)
			}
		}
	}
}

// handleEvent dispatches one peer-local event.
func (rw *raftWorker) handleEvent(peer *Peer, event raftEvent) {
	switch event := event.(type) {
	case raftMessageEvent:
		if err := peer.Step(event.message); err != nil {
			slog.Warn("Failed to step raft", "region", peer.RegionID(), "error", err)
		}

	case raftCommandEvent:
		rw.handleProposal(peer, event.proposal)

	case tickEvent:
		peer.Tick()

	case snapshotTask:
		rw.handleSnapshotTask(peer, event)

	case readIndexEvent:
		peer.startLinearizableRead(event.request)

	default:
		slog.Warn("Unknown raft event", "region", peer.RegionID())
	}
}

// handleProposal serializes and appends one client write.
func (rw *raftWorker) handleProposal(peer *Peer, proposal *proposal) {
	// Marshal the request
	data, err := protov2.Marshal(proposal.request)
	if err != nil {
		proposal.future.finish(rw.store.BuildResponse(proposal.request, peer.RegionID(), nil, err), err)
		return
	}

	if !peer.propose(data, proposal) {
		err := ErrNotLeader
		proposal.future.finish(rw.store.BuildResponse(proposal.request, peer.RegionID(), nil, err), err)
		return
	}
}

// handleSnapshotTask persists and compacts one locally-created snapshot.
func (rw *raftWorker) handleSnapshotTask(peer *Peer, task snapshotTask) {
	slog.Info("raft_worker: trigger snapshot", "region", peer.RegionID(), "index", task.index)

	if err := peer.saveSnapshotAndCompact(task); err != nil {
		slog.Error("snapshot compaction failed", "region", peer.RegionID(), "error", err)
		return
	}

	slog.Info("raft_worker: log compacted", "region", peer.RegionID(), "index", task.index)
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
	if err := rw.persistReady(peer, rd); err != nil {
		return err
	}
	rw.sendMessages(peer, rd.Messages)
	if err := rw.submitApply(peer, rd); err != nil {
		return err
	}
	peer.handleReadStates(rd.ReadStates)
	return nil
}

func (rw *raftWorker) persistReady(peer *Peer, rd raft.Ready) error {
	return peer.persistReady(rd)
}

func (rw *raftWorker) sendMessages(peer *Peer, messages []*raftpb.Message) {
	for _, msg := range messages {
		msg.RegionId = peer.RegionID()
		if rw.transport != nil {
			_ = rw.transport.Send(msg)
		}
	}
}

func (rw *raftWorker) submitApply(peer *Peer, rd raft.Ready) error {
	if !raft.IsEmptySnap(rd.Snapshot) || len(rd.CommittedEntries) > 0 {
		return rw.store.applyWorker.Submit(ApplyTask{
			RegionID: peer.RegionID(),
			Peer:     peer,
			Snapshot: rd.Snapshot,
			Entries:  rd.CommittedEntries,
		})
	}
	return nil
}
