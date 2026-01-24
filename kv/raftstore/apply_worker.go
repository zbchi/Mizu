package raftstore

import (
	"log/slog"
	"sync"

	"github.com/zbchi/mizu/proto/raftkvpb"
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

// applyWorker is responsible for applying committed entries asynchronously
type applyWorker struct {
	store  *Store
	taskCh chan ApplyTask
	stopCh chan struct{}
	wg     sync.WaitGroup
}

// newApplyWorker creates a new applyWorker
func newApplyWorker(store *Store) *applyWorker {
	return &applyWorker{
		store:  store,
		taskCh: make(chan ApplyTask, 100),
		stopCh: make(chan struct{}),
	}
}

// start starts the applyWorker
func (aw *applyWorker) start() {
	aw.wg.Add(1)
	go func() {
		defer aw.wg.Done()
		aw.run()
	}()
}

// stop stops the applyWorker gracefully
func (aw *applyWorker) stop() {
	close(aw.stopCh)
	aw.wg.Wait()
}

// run runs the apply worker loop
func (aw *applyWorker) run() {
	for {
		select {
		case <-aw.stopCh:
			return
		case task := <-aw.taskCh:
			aw.apply(task)
		}
	}
}

// Submit submits an apply task to the worker
func (aw *applyWorker) Submit(task ApplyTask) {
	select {
	case aw.taskCh <- task:
	default:
		slog.Warn("applyWorker task channel full, dropping apply task", "region", task.RegionID, "entries", len(task.Entries))
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

		var req raftkvpb.RaftCmdRequest
		if err := protov2.Unmarshal(entry.Data, &req); err != nil {
			slog.Error("applyWorker: failed to unmarshal request", "error", err, "index", entry.Index)
			aw.store.triggerCallback(task.RegionID, entry.Index, err)
			task.Peer.SetAppliedIndex(entry.Index)
			continue
		}

		if err := aw.store.applyCommand(task.RegionID, &req); err != nil {
			slog.Error("applyWorker: failed to apply command", "error", err, "index", entry.Index)
			aw.store.triggerCallback(task.RegionID, entry.Index, err)
			task.Peer.SetAppliedIndex(entry.Index)
			continue
		}

		aw.store.triggerCallback(task.RegionID, entry.Index, nil)
		task.Peer.SetAppliedIndex(entry.Index)

		// snapshot trigger
		appliedIndex := task.Peer.GetAppliedIndex()
		if appliedIndex-task.Peer.LastSnapshotIndex() >= SnapshotThreshold {
			// Get snapshot data from raftStorage
			data := task.Peer.raftStorage.MakeSnapshotData()

			aw.store.peerRouter.Send(task.RegionID, Msg{
				Type:     MsgTypeSnapshotTrigger,
				RegionID: task.RegionID,
				Data: &SnapshotTrigger{
					Index: appliedIndex,
					Data:  data,
				},
			})

			task.Peer.SetLastSnapshotIndex(appliedIndex)
		}
	}

	// Notify waiting read requests for this peer
	task.Peer.notifyReadWaitQueue()
}

// applySnapshot applies a snapshot to the peer
func (aw *applyWorker) applySnapshot(peer *Peer, sn *raftpb.Snapshot) {
	slog.Info("applyWorker: applying snapshot", "region", peer.RegionID(), "index", sn.Index, "term", sn.Term)

	if err := peer.raftStorage.ApplySnapshotData(sn.Data); err != nil {
		slog.Error("applyWorker: failed to apply snapshot data", "error", err, "index", sn.Index)
		return
	}

	// Update appliedIndex to snapshot index
	peer.SetAppliedIndex(sn.Index)
}
