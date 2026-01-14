package raftstore

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/zbchi/linkv/kv/region"
	"github.com/zbchi/linkv/proto/raftpb"
	"github.com/zbchi/linkv/raft"
)

// Peer represents a region replica on this node
type Peer struct {
	region            *region.Region
	raft              *raft.Raft
	raftStorage       raft.RaftStorage
	appliedIndex      uint64
	lastSnapshotIndex uint64

	closeCh       chan struct{}
	readWaitQueue *ReadWaitQueue
}

// NewPeer creates a new peer for a region
func NewPeer(reg *region.Region, nodeID uint64, closeCh chan struct{}, raftStorage raft.RaftStorage) *Peer {
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

	return &Peer{
		region:        reg,
		raft:          raftInst,
		raftStorage:   raftStorage,
		appliedIndex:  appliedIndex,
		closeCh:       closeCh,
		readWaitQueue: &ReadWaitQueue{},
	}
}

// RegionID returns region ID
func (p *Peer) RegionID() uint64 {
	return p.region.ID
}

// Tick advances raft ticker
func (p *Peer) Tick() {
	p.raft.Tick()
}

// Step processes a raft message
func (p *Peer) Step(m *raftpb.Message) error {
	return p.raft.Step(m)
}

// Propose proposes a command to raft
func (p *Peer) Propose(data []byte) bool {
	return p.raft.Propose(data)
}

// NextIndex returns the index that will be assigned to the next proposed entry
func (p *Peer) NextIndex() uint64 {
	return p.raft.LastIndex() + 1
}

// ReadIndex gets a read index for linearizable read
func (p *Peer) ReadIndex() uint64 {
	return p.raft.ReadIndex()
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
}

// Stop stops the peer
func (p *Peer) Stop() {
	// Raft has no explicit stop, just let it go out of scope
}

// GetAppliedIndex returns current applied index
func (p *Peer) GetAppliedIndex() uint64 {
	return p.appliedIndex
}

// notifyReadWaitQueue notifies read wait queue when appliedIndex advances
func (p *Peer) notifyReadWaitQueue() {
	p.readWaitQueue.Notify(p.appliedIndex)
}

// WaitForReadIndex waits until appliedIndex reaches readIndex
func (p *Peer) WaitForReadIndex(ctx context.Context, readIndex uint64) error {
	slog.Info("WaitForReadIndex called", "readIndex", readIndex, "currentAppliedIndex", p.appliedIndex)

	if p.appliedIndex >= readIndex {
		slog.Info("WaitForReadIndex condition already satisfied", "readIndex", readIndex, "appliedIndex", p.appliedIndex)
		return nil
	}

	req := &ReadRequest{
		ReadIndex: readIndex,
		Done:      make(chan struct{}),
		AddedAt:   time.Now(),
	}
	p.readWaitQueue.Add(req)

	slog.Info("WaitForReadIndex added to queue", "readIndex", readIndex, "queueSize", len(p.readWaitQueue.queue))

	select {
	case <-req.Done:
		slog.Info("WaitForReadIndex completed", "readIndex", readIndex)
		return nil
	case <-ctx.Done():
		slog.Warn("WaitForReadIndex canceled by client", "readIndex", readIndex, "error", ctx.Err())
		return ctx.Err()
	case <-p.closeCh:
		return nil
	}
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
		slog.Info("read notify queue == 0")
		return
	}

	newQueue := make([]*ReadRequest, 0, len(q.queue))
	for _, req := range q.queue {
		if appliedIndex >= req.ReadIndex {
			slog.Info("read request ready", "appliedIndex", appliedIndex, "readIndex", req.ReadIndex)
			close(req.Done)

			waitTime := time.Since(req.AddedAt)
			if waitTime > 100*time.Millisecond {
				// slog.Debug("read wait duration", "duration", waitTime, "key_len", len(req.Key), "cf", req.Cf)
			}
		} else {
			newQueue = append(newQueue, req)
		}
	}
	q.queue = newQueue
}

// Add adds a read request to the queue
func (q *ReadWaitQueue) Add(req *ReadRequest) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.queue = append(q.queue, req)
}

// ReadRequest represents a pending read request waiting for apply
type ReadRequest struct {
	ReadIndex uint64
	Cf        string
	Key       []byte
	Done      chan struct{}
	AddedAt   time.Time
}
