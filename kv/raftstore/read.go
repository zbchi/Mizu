package raftstore

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/zbchi/mizu/raft"
)

// readRequest is the client-facing future for one linearizable read. Its
// lifecycle is waiting for quorum, waiting for apply, then done.
type readRequest struct {
	done     chan error
	canceled <-chan struct{}

	finishOnce sync.Once
}

func newReadRequest(canceled <-chan struct{}) *readRequest {
	return &readRequest{
		done:     make(chan error, 1),
		canceled: canceled,
	}
}

func (r *readRequest) finish(err error) {
	r.finishOnce.Do(func() {
		r.done <- err
	})
}

// readTracker owns the complete lifecycle of reads for one Peer. Raft worker
// methods move requests between quorum and apply stages; the apply worker only
// calls NotifyApplied.
type readTracker struct {
	mu sync.Mutex

	nextID       uint64
	quorum       map[uint64]*readRequest
	apply        []*pendingRead
	appliedIndex *atomic.Uint64
}

type pendingRead struct {
	index   uint64
	request *readRequest
}

func newReadTracker(appliedIndex *atomic.Uint64) *readTracker {
	return &readTracker{
		quorum:       make(map[uint64]*readRequest),
		appliedIndex: appliedIndex,
	}
}

func (t *readTracker) start(req *readRequest) uint64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.nextID++
	if t.nextID == 0 {
		t.nextID++
	}
	t.quorum[t.nextID] = req
	return t.nextID
}

func (t *readTracker) reject(readID uint64, err error) {
	t.mu.Lock()
	req := t.quorum[readID]
	delete(t.quorum, readID)
	t.mu.Unlock()
	if req != nil {
		req.finish(err)
	}
}

func (t *readTracker) handleStates(states []raft.ReadState) {
	t.mu.Lock()
	defer t.mu.Unlock()
	appliedIndex := t.appliedIndex.Load()

	for _, state := range states {
		req := t.quorum[state.ReadID]
		if req == nil {
			continue
		}
		delete(t.quorum, state.ReadID)
		if isCanceled(req.canceled) {
			req.finish(context.Canceled)
			continue
		}

		if state.Index <= appliedIndex {
			req.finish(nil)
			continue
		}
		t.apply = append(t.apply, &pendingRead{index: state.Index, request: req})
	}
}

func (t *readTracker) notifyApplied() {
	t.mu.Lock()
	defer t.mu.Unlock()
	appliedIndex := t.appliedIndex.Load()

	pending := t.apply[:0]
	for _, read := range t.apply {
		req := read.request
		if isCanceled(req.canceled) {
			req.finish(context.Canceled)
			continue
		}
		if read.index <= appliedIndex {
			req.finish(nil)
			continue
		}
		pending = append(pending, read)
	}
	t.apply = pending
}

func (t *readTracker) failIfNotLeader() []uint64 {
	t.mu.Lock()
	defer t.mu.Unlock()

	ids := make([]uint64, 0, len(t.quorum))
	for id, req := range t.quorum {
		delete(t.quorum, id)
		ids = append(ids, id)
		if isCanceled(req.canceled) {
			req.finish(context.Canceled)
		} else {
			req.finish(ErrNotLeader)
		}
	}
	return ids
}

func (t *readTracker) pruneCanceled() []uint64 {
	t.mu.Lock()
	defer t.mu.Unlock()

	ids := make([]uint64, 0)
	for id, req := range t.quorum {
		if !isCanceled(req.canceled) {
			continue
		}
		delete(t.quorum, id)
		ids = append(ids, id)
		req.finish(context.Canceled)
	}

	pending := t.apply[:0]
	for _, read := range t.apply {
		req := read.request
		if isCanceled(req.canceled) {
			req.finish(context.Canceled)
			continue
		}
		pending = append(pending, read)
	}
	t.apply = pending
	return ids
}

func isCanceled(done <-chan struct{}) bool {
	if done == nil {
		return false
	}
	select {
	case <-done:
		return true
	default:
		return false
	}
}

// startLinearizableRead starts a quorum-confirmed ReadIndex round.
func (p *Peer) startLinearizableRead(req *readRequest) {
	if isCanceled(req.canceled) {
		req.finish(context.Canceled)
		return
	}
	p.leaderNodeID.Store(p.raft.Lead())

	readID := p.reads.start(req)

	if !p.raft.ReadIndex(readID) {
		p.reads.reject(readID, ErrNotLeader)
		return
	}
}

// handleReadStates moves quorum-confirmed requests into the apply wait queue.
func (p *Peer) handleReadStates(states []raft.ReadState) {
	p.reads.handleStates(states)
}

func (p *Peer) notifyAppliedReads() {
	p.reads.notifyApplied()
}

func (p *Peer) finishReadRequestsIfNotLeader() {
	if p.raft.State() == raft.StateLeader {
		return
	}
	for _, readID := range p.reads.failIfNotLeader() {
		p.raft.CancelReadIndex(readID)
	}
}

func (p *Peer) pruneCanceledReadRequests() {
	for _, readID := range p.reads.pruneCanceled() {
		p.raft.CancelReadIndex(readID)
	}
}
