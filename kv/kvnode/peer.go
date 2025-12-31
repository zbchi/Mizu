package kvnode

import (
	"context"
	"log/slog"

	"github.com/zbchi/linkv/kv/region"
	"github.com/zbchi/linkv/proto/raftkvpb"
	"github.com/zbchi/linkv/proto/raftpb"
	"github.com/zbchi/linkv/raft"
	"google.golang.org/protobuf/proto"
)

// Peer represents a region replica on this node
type Peer struct {
	region       *region.Region
	raftNode     *raft.RawNode
	raftStorage  raft.RaftStorage
	appliedIndex uint64

	node          *KVNode
	readWaitQueue *ReadWaitQueue
}

// NewPeer creates a new peer for a region
func NewPeer(reg *region.Region, nodeID uint64, node *KVNode, raftStorage raft.RaftStorage) *Peer {
	peerIDs := make([]uint64, len(reg.Peers))
	for i, p := range reg.Peers {
		peerIDs[i] = p.NodeID
	}

	raftCfg := raft.Config{
		ID:    nodeID,
		Peers: peerIDs,
	}

	raftNode := raft.NewRawNode(raftCfg)

	return &Peer{
		region:        reg,
		raftNode:      raftNode,
		raftStorage:   raftStorage,
		appliedIndex:  0,
		node:          node,
		readWaitQueue: &ReadWaitQueue{},
	}
}

// RegionID returns the region ID
func (p *Peer) RegionID() uint64 {
	return p.region.ID
}

// Tick advances the raft ticker
func (p *Peer) Tick() {
	p.raftNode.Tick()
}

// Step processes a raft message
func (p *Peer) Step(ctx context.Context, msg *raftpb.Message) error {
	return p.raftNode.Step(ctx, msg)
}

// Propose proposes a command to raft
func (p *Peer) Propose(ctx context.Context, data []byte) error {
	return p.raftNode.Propose(ctx, data)
}

// ReadIndex gets a read index for linearizable read
func (p *Peer) ReadIndex(ctx context.Context) (uint64, error) {
	return p.raftNode.ReadIndex(ctx)
}

// Ready returns the ready channel for raft
func (p *Peer) Ready() <-chan raft.Ready {
	return p.raftNode.Ready()
}

// HasReady checks if there are pending ready states
func (p *Peer) HasReady() bool {
	return p.raftNode.HasReady()
}

// Advance advances the raft state machine after processing ready
func (p *Peer) Advance() {
	p.raftNode.Advance()
}

// Stop stops the raft node
func (p *Peer) Stop() {
	p.raftNode.Stop()
}

// ProcessReady processes the raft ready state
func (p *Peer) ProcessReady(rd raft.Ready) error {
	if rd.HardState != nil && !rd.HardState.IsEmpty() {
		if err := p.raftStorage.SaveHardState(*rd.HardState); err != nil {
			slog.Error("Failed to save hard state", "region", p.region.ID, "error", err)
			return err
		}
	}

	if len(rd.Entries) > 0 {
		if err := p.raftStorage.SaveEntries(rd.Entries); err != nil {
			slog.Error("Failed to save entries", "region", p.region.ID, "error", err)
			return err
		}
	}

	if rd.Snapshot != nil {
		if err := p.raftStorage.SaveSnapshot(rd.Snapshot); err != nil {
			slog.Error("Failed to save snapshot", "region", p.region.ID, "error", err)
			return err
		}
		if err := p.raftStorage.ApplySnapshotData(rd.Snapshot.Data); err != nil {
			slog.Error("Failed to apply snapshot data", "region", p.region.ID, "error", err)
			return err
		}
	}

	for _, msg := range rd.Messages {
		p.sendMessage(msg)
	}

	if len(rd.CommittedEntries) > 0 {
		p.applyEntries(rd.CommittedEntries)
	}

	return nil
}

// sendMessage sends a raft message with region ID
func (p *Peer) sendMessage(msg *raftpb.Message) {
	msg.RegionId = p.region.ID
	p.node.router.Send(*msg)
}

// applyEntries applies committed entries to state machine
func (p *Peer) applyEntries(entries []*raftpb.Entry) {
	for _, entry := range entries {
		p.applyEntry(entry)
		p.appliedIndex = entry.Index
	}
}

// applyEntry applies a single entry
func (p *Peer) applyEntry(entry *raftpb.Entry) {
	if len(entry.Data) == 0 {
		return
	}
	p.processCommittedEntry(entry)
}

// processCommittedEntry processes a committed entry and notifies waiting client
func (p *Peer) processCommittedEntry(entry *raftpb.Entry) {
	var req raftkvpb.RaftCmdRequest
	if err := proto.Unmarshal(entry.Data, &req); err != nil {
		p.node.router.triggerCallbackForRegion(p.region.ID, entry.Index, entry.Term, err)
		return
	}

	if err := p.applyCommand(&req); err != nil {
		p.node.router.triggerCallbackForRegion(p.region.ID, entry.Index, entry.Term, err)
		return
	}

	p.node.router.triggerCallbackForRegion(p.region.ID, entry.Index, entry.Term, nil)
}

// applyCommand applies a command to storage
func (p *Peer) applyCommand(req *raftkvpb.RaftCmdRequest) error {
	if len(req.Requests) == 0 {
		return nil
	}

	// TODO: implement actual storage write
	for range req.Requests {
	}

	return nil
}

// GetAppliedIndex returns the current applied index
func (p *Peer) GetAppliedIndex() uint64 {
	return p.appliedIndex
}

// notifyReadWaitQueue notifies the read wait queue when appliedIndex advances
func (p *Peer) notifyReadWaitQueue() {
	p.readWaitQueue.Notify(p.appliedIndex)
}

// waitForReadIndex waits until appliedIndex reaches readIndex
func (p *Peer) waitForReadIndex(ctx context.Context, readIndex uint64) error {
	req := &ReadRequest{
		readIndex: readIndex,
		done:      make(chan struct{}),
	}
	p.readWaitQueue.Add(req)

	select {
	case <-req.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
