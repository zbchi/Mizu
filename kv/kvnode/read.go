package kvnode

import (
	"log/slog"
	"sync"
	"time"
)

// ReadRequest represents a pending read request waiting for apply
type ReadRequest struct {
	readIndex uint64
	cf        string
	key       []byte
	done      chan struct{}
	addedAt   time.Time
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

	newQueue := make([]*ReadRequest, 0, len(q.queue))
	for _, req := range q.queue {
		if appliedIndex >= req.readIndex {
			close(req.done)

			waitTime := time.Since(req.addedAt)
			if waitTime > 100*time.Millisecond {
				slog.Debug("read wait duration", "duration", waitTime, "key_len", len(req.key), "cf", req.cf)
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
