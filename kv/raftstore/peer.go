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
	pendingSnapshot   atomic.Uint64
	proposalMu        sync.Mutex
	pendingProposals  map[uint64]*proposal

	reads *readTracker
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
		start := uint64(1)
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

	// The actual state machine may have crashed behind commitIndex. Start its
	// apply watermark conservatively; the restored Ready will replay the
	// snapshot and committed entries before linearizable reads are released.
	lastSnapshotIndex := uint64(0)
	if snapshot != nil {
		lastSnapshotIndex = snapshot.Index
	}
	peer := &Peer{
		region:           reg,
		raft:             raftInst,
		raftStorage:      raftStorage,
		pendingProposals: make(map[uint64]*proposal),
	}
	peer.reads = newReadTracker(&peer.appliedIndex)
	peer.syncLeaderNodeID()
	peer.lastSnapshotIndex.Store(lastSnapshotIndex)
	return peer
}

// takeProposal removes and returns the proposal applied at index.
func (p *Peer) takeProposal(index uint64) *proposal {
	p.proposalMu.Lock()
	cmd := p.pendingProposals[index]
	delete(p.pendingProposals, index)
	p.proposalMu.Unlock()
	return cmd
}

// RegionID returns region ID
func (p *Peer) RegionID() uint64 {
	return p.region.ID
}

// Region returns the metadata owned by this peer.
func (p *Peer) Region() *region.Region {
	return p.region
}

// Tick advances raft ticker
func (p *Peer) Tick() {
	p.pruneCanceledReadRequests()
	p.raft.Tick()
	p.syncLeaderNodeID()
	p.finishReadRequestsIfNotLeader()
}

// Step processes a raft message
func (p *Peer) Step(m *raftpb.Message) error {
	err := p.raft.Step(m)
	p.syncLeaderNodeID()
	p.finishReadRequestsIfNotLeader()
	return err
}

// Propose appends data and records its completion under the index Raft assigned.
// Both actions run on raftWorker, before Ready can reach apply.
func (p *Peer) propose(data []byte, proposal *proposal) bool {
	if !p.raft.Propose(data) {
		p.syncLeaderNodeID()
		return false
	}

	p.proposalMu.Lock()
	p.pendingProposals[p.raft.LastIndex()] = proposal
	p.proposalMu.Unlock()
	p.syncLeaderNodeID()
	return true
}

// LeaderNodeID returns the latest leader hint published by the raft worker.
func (p *Peer) LeaderNodeID() uint64 {
	return p.leaderNodeID.Load()
}

// Ready returns ready state for raft
func (p *Peer) Ready() raft.Ready {
	return p.raft.Ready()
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

// persistReady writes every durable field in a Ready before its messages are
// sent or committed entries are handed to the apply worker.
func (p *Peer) persistReady(rd raft.Ready) error {
	if rd.HardState != nil && !rd.HardState.IsEmpty() {
		if err := p.raftStorage.SaveHardState(*rd.HardState); err != nil {
			return err
		}
	}
	if len(rd.Entries) > 0 {
		if err := p.raftStorage.SaveEntries(rd.Entries); err != nil {
			return err
		}
	}
	if !raft.IsEmptySnap(rd.Snapshot) {
		if err := p.raftStorage.SaveSnapshot(rd.Snapshot); err != nil {
			return err
		}
	}
	return nil
}

// saveSnapshotAndCompact persists a locally-created snapshot, then publishes
// the matching compaction to Raft and its durable log.
func (p *Peer) saveSnapshotAndCompact(task snapshotTask) error {
	defer p.pendingSnapshot.CompareAndSwap(task.index, 0)

	snapshot := &raftpb.Snapshot{
		Term:  p.Term(),
		Index: task.index,
		Data:  task.data,
	}
	if err := p.raftStorage.SaveSnapshot(snapshot); err != nil {
		return err
	}
	p.Snapshot(task.index, task.data)
	if err := p.raftStorage.Compact(task.index); err != nil {
		return err
	}
	p.lastSnapshotIndex.Store(task.index)
	return nil
}

// GetAppliedIndex returns the highest index applied to the KV state machine.
func (p *Peer) GetAppliedIndex() uint64 {
	return p.appliedIndex.Load()
}

// SetAppliedIndex records a committed entry or snapshot after the KV state machine applies it.
func (p *Peer) SetAppliedIndex(index uint64) {
	p.appliedIndex.Store(index)
}

// beginSnapshot reserves the next locally-created snapshot. Only one snapshot
// may be queued while raftWorker persists and compacts the previous one.
func (p *Peer) beginSnapshot(threshold uint64) (uint64, bool) {
	appliedIndex := p.GetAppliedIndex()
	lastSnapshotIndex := p.lastSnapshotIndex.Load()
	if appliedIndex < lastSnapshotIndex || appliedIndex-lastSnapshotIndex < threshold {
		return 0, false
	}
	if !p.pendingSnapshot.CompareAndSwap(0, appliedIndex) {
		return 0, false
	}
	return appliedIndex, true
}

func (p *Peer) cancelSnapshot(index uint64) {
	p.pendingSnapshot.CompareAndSwap(index, 0)
}

func (p *Peer) syncLeaderNodeID() {
	p.leaderNodeID.Store(p.raft.Lead())
}
