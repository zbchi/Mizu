package raftstore

import "sync"

// ReadWaitQueue tracks reads waiting for the local state machine to catch up.
type ReadWaitQueue struct {
	mu    sync.Mutex
	queue []*ReadRequest
}

func (q *ReadWaitQueue) Notify(appliedIndex uint64) {
	q.mu.Lock()
	defer q.mu.Unlock()

	if len(q.queue) == 0 {
		return
	}

	pending := make([]*ReadRequest, 0, len(q.queue))
	for _, req := range q.queue {
		if appliedIndex >= req.ReadIndex {
			close(req.Done)
		} else {
			pending = append(pending, req)
		}
	}
	q.queue = pending
}

func (q *ReadWaitQueue) AddIfPending(req *ReadRequest, currentAppliedIndex func() uint64) (int, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if currentAppliedIndex() >= req.ReadIndex {
		return len(q.queue), false
	}
	q.queue = append(q.queue, req)
	return len(q.queue), true
}

type ReadRequest struct {
	ReadIndex uint64
	Done      chan struct{}
}
