package raftstore

import (
	"context"
	"log/slog"
	"sync"

	"github.com/zbchi/mizu/proto/kvpb"
	"github.com/zbchi/mizu/proto/raftpb"
	"github.com/zbchi/mizu/raft"
	protov2 "google.golang.org/protobuf/proto"
)

const (
	// SnapshotThreshold is the number of applied entries after which a snapshot is triggered
	SnapshotThreshold = 5
)

// ApplyTask represents a task to apply committed entries for a region
type ApplyTask struct {
	RegionID uint64
	Peer     *Peer
	Snapshot *raftpb.Snapshot
	Entries  []*raftpb.Entry
}

// applyWorker applies committed entries to the business state machine.
type applyWorker struct {
	store  *Store
	taskCh chan ApplyTask
	wg     sync.WaitGroup
}

// newApplyWorker creates a new applyWorker
func newApplyWorker(store *Store) *applyWorker {
	return &applyWorker{
		store:  store,
		taskCh: make(chan ApplyTask, 100),
	}
}

// start starts the applyWorker
func (aw *applyWorker) start(stopCh <-chan struct{}) {
	aw.wg.Add(1)
	go func() {
		defer aw.wg.Done()
		aw.run(stopCh)
	}()
}

func (aw *applyWorker) wait() {
	aw.wg.Wait()
}

// run runs the apply worker loop
func (aw *applyWorker) run(stopCh <-chan struct{}) {
	for {
		select {
		case <-stopCh:
			return
		case task := <-aw.taskCh:
			aw.apply(task)
		}
	}
}

// Submit hands a task to the worker without dropping committed entries.
func (aw *applyWorker) Submit(task ApplyTask) error {
	select {
	case aw.taskCh <- task:
		return nil
	case <-aw.store.stopCh:
		return context.Canceled
	}
}

// apply applies committed entries for a region
func (aw *applyWorker) apply(task ApplyTask) {
	// 1. apply snapshot first
	if task.Snapshot != nil && !raft.IsEmptySnap(task.Snapshot) {
		aw.applySnapshot(task.Peer, task.Snapshot)
	}

	// 2. apply logs
	for _, entry := range task.Entries {
		if len(entry.Data) == 0 {
			task.Peer.SetAppliedIndex(entry.Index)
			continue
		}

		var req kvpb.RaftCmdRequest
		if err := protov2.Unmarshal(entry.Data, &req); err != nil {
			slog.Error("applyWorker: failed to unmarshal request", "error", err, "index", entry.Index)
			aw.store.completeProposal(task.Peer, entry.Index, err)
			task.Peer.SetAppliedIndex(entry.Index)
			continue
		}

		if err := aw.store.applyCommand(task.RegionID, &req); err != nil {
			slog.Error("applyWorker: failed to apply command", "error", err, "index", entry.Index)
			aw.store.completeProposal(task.Peer, entry.Index, err)
			task.Peer.SetAppliedIndex(entry.Index)
			continue
		}

		aw.store.completeProposal(task.Peer, entry.Index, nil)
		task.Peer.SetAppliedIndex(entry.Index)

		// snapshot trigger
		if appliedIndex, due := task.Peer.beginSnapshot(SnapshotThreshold); due {
			data, err := aw.store.createSnapshot(task.RegionID)
			if err != nil {
				slog.Error("applyWorker: failed to create snapshot", "region", task.RegionID, "error", err)
				task.Peer.cancelSnapshot(appliedIndex)
				continue
			}

			if err := aw.store.raftWorker.submit(peerEvent{
				regionID: task.RegionID,
				event:    snapshotTask{index: appliedIndex, data: data},
			}); err != nil {
				slog.Error("applyWorker: failed to schedule snapshot", "region", task.RegionID, "error", err)
				task.Peer.cancelSnapshot(appliedIndex)
				continue
			}
		}
	}

	// Notify waiting read requests for this peer
	task.Peer.notifyAppliedReads()
}

// applySnapshot applies a snapshot to the peer
func (aw *applyWorker) applySnapshot(peer *Peer, sn *raftpb.Snapshot) {
	slog.Info("applyWorker: applying snapshot", "region", peer.RegionID(), "index", sn.Index, "term", sn.Term)

	if err := aw.store.applySnapshot(peer.RegionID(), sn.Data); err != nil {
		slog.Error("applyWorker: failed to apply snapshot data", "error", err, "index", sn.Index)
		return
	}

	// Update appliedIndex to snapshot index
	peer.SetAppliedIndex(sn.Index)
}
