package raftstore

import (
	"log/slog"
	"sync"
	"sync/atomic"

	"github.com/zbchi/mizu/kv/region"
	"github.com/zbchi/mizu/proto/raftpb"
	"github.com/zbchi/mizu/raft"
)

// Peer represents a region replica on this node
type Peer struct {
	region            *region.Region
	raft              *raft.Raft
	raftStorage       raft.RaftStorage
	leaderNodeID      atomic.Uint64
	appliedIndex      atomic.Uint64
	lastSnapshotIndex atomic.Uint64

	readWaitQueue *ReadWaitQueue
}

// NewPeer creates a new peer for a region
func NewPeer(reg *region.Region, nodeID uint64, raftStorage raft.RaftStorage) *Peer {
	peerIDs := make([]uint64, len(reg.Peers))
	for i, p := range reg.Peers {
		peerIDs[i] = p.NodeID
	}

	raftInst := raft.NewRaft(raft.Config{ID: nodeID, Peers: peerIDs})

	// 1. Load snapshot
	snapshot, err := raftStorage.LoadSnapshot()
	if err != nil {
		slog.Warn("Failed to load snapshot", "error", err)
		snapshot = nil
	}

	// 2. Load hard state
	hardState, err := raftStorage.LoadHardState()
	if err != nil {
		slog.Warn("Failed to load hard state", "error", err)
	}

	// 3. Load entries from storage
	// entries after snapshot: [snapshotIndex+1, commitIndex)
	var entries []*raftpb.Entry
	if !hardState.IsEmpty() {
		start := uint64(0)
		if snapshot != nil && snapshot.Index > 0 {
			start = snapshot.Index + 1
		}
		entries, err = raftStorage.LoadEntries(start, hardState.CommitIndex+1)
		if err != nil {
			slog.Error("Failed to load entries", "error", err)
		}
	}

	// 4. Restore snapshot to Raft
	if snapshot != nil && snapshot.Index > 0 {
		raftInst.RestoreSnapshot(snapshot)
		slog.Info("Restored raft state from snapshot", "snapshotIndex", snapshot.Index, "snapshotTerm", snapshot.Term)
	}

	// 5. Restore hard state and entries to Raft
	if !hardState.IsEmpty() {
		raftInst.RestoreState(hardState, entries)
		slog.Info("Restored raft state", "commitIndex", hardState.CommitIndex, "term", hardState.Term, "entries", len(entries))
	}

	// 6. Set appliedIndex with raft state machine
	appliedIndex := raftInst.AppliedIndex()
	peer := &Peer{
		region:        reg,
		raft:          raftInst,
		raftStorage:   raftStorage,
		readWaitQueue: &ReadWaitQueue{},
	}
	peer.syncLeaderNodeID()
	peer.appliedIndex.Store(appliedIndex)
	peer.lastSnapshotIndex.Store(appliedIndex)
	return peer
}

// RegionID returns region ID
func (p *Peer) RegionID() uint64 {
	return p.region.ID
}

// Tick advances raft ticker
func (p *Peer) Tick() {
	p.raft.Tick()
	p.syncLeaderNodeID()
}

// Step processes a raft message
func (p *Peer) Step(m *raftpb.Message) error {
	err := p.raft.Step(m)
	p.syncLeaderNodeID()
	return err
}

// Propose proposes a command to raft
func (p *Peer) Propose(data []byte) bool {
	ok := p.raft.Propose(data)
	p.syncLeaderNodeID()
	return ok
}

// NextIndex returns the index that will be assigned to the next proposed entry
func (p *Peer) NextIndex() uint64 {
	return p.raft.LastIndex() + 1
}

// LeaderNodeID returns the latest leader hint published by the raft worker.
func (p *Peer) LeaderNodeID() uint64 {
	return p.leaderNodeID.Load()
}

// Ready returns ready state for raft
func (p *Peer) Ready() raft.Ready {
	return p.raft.Ready()
}

// HasReady checks if there are pending ready states
func (p *Peer) HasReady() bool {
	return !p.raft.Ready().IsEmpty()
}

// Advance advances raft state machine after processing ready
func (p *Peer) Advance() {
	p.raft.Advance()
	p.syncLeaderNodeID()
}

// Term returns the current raft term.
func (p *Peer) Term() uint64 {
	return p.raft.Term()
}

// Snapshot compacts the raft log and publishes a snapshot to the raft state machine.
func (p *Peer) Snapshot(index uint64, data []byte) {
	p.raft.Snapshot(index, data)
	p.syncLeaderNodeID()
}

// Stop stops the peer
func (p *Peer) Stop() {
	// Raft has no explicit stop, just let it go out of scope
}

// GetAppliedIndex returns current applied index
func (p *Peer) GetAppliedIndex() uint64 {
	return p.appliedIndex.Load()
}

// SetAppliedIndex updates the peer's applied index after a committed entry or snapshot is applied.
func (p *Peer) SetAppliedIndex(index uint64) {
	p.appliedIndex.Store(index)
}

// LastSnapshotIndex returns the index of the latest snapshot taken for this peer.
func (p *Peer) LastSnapshotIndex() uint64 {
	return p.lastSnapshotIndex.Load()
}

// SetLastSnapshotIndex records the latest snapshot index for this peer.
func (p *Peer) SetLastSnapshotIndex(index uint64) {
	p.lastSnapshotIndex.Store(index)
}

// PrepareLinearizableRead is called by the raft worker to serialize ReadIndex checks
// with the peer's raft state machine.
func (p *Peer) PrepareLinearizableRead() ReadIndexResult {
	leaderNodeID := p.raft.Lead()
	p.leaderNodeID.Store(leaderNodeID)

	readIndex := p.raft.ReadIndex()
	result := ReadIndexResult{}
	if readIndex == 0 {
		result.Err = ErrNotLeader
		return result
	}

	req := &ReadRequest{
		ReadIndex: readIndex,
		Done:      make(chan struct{}),
	}
	if _, queued := p.readWaitQueue.AddIfPending(req, p.GetAppliedIndex); queued {
		result.Ready = req.Done
	}
	return result
}

// notifyReadWaitQueue notifies read wait queue when appliedIndex advances
func (p *Peer) notifyReadWaitQueue() {
	p.readWaitQueue.Notify(p.GetAppliedIndex())
}

func (p *Peer) syncLeaderNodeID() {
	p.leaderNodeID.Store(p.raft.Lead())
}

// ReadWaitQueue manages read requests waiting for appliedIndex >= readIndex
type ReadWaitQueue struct {
	mu    sync.Mutex
	queue []*ReadRequest
}

// Notify checks and wakes up read requests when appliedIndex advances
func (q *ReadWaitQueue) Notify(appliedIndex uint64) {
	q.mu.Lock()
	defer q.mu.Unlock()

	if len(q.queue) == 0 {
		return
	}

	newQueue := make([]*ReadRequest, 0, len(q.queue))
	for _, req := range q.queue {
		if appliedIndex >= req.ReadIndex {
			close(req.Done)
		} else {
			newQueue = append(newQueue, req)
		}
	}
	q.queue = newQueue
}

// AddIfPending appends a read request only if appliedIndex is still behind the target.
func (q *ReadWaitQueue) AddIfPending(req *ReadRequest, currentAppliedIndex func() uint64) (int, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if currentAppliedIndex() >= req.ReadIndex {
		return len(q.queue), false
	}
	q.queue = append(q.queue, req)
	return len(q.queue), true
}

// ReadRequest represents a pending read request waiting for apply
type ReadRequest struct {
	ReadIndex uint64
	Done      chan struct{}
}
