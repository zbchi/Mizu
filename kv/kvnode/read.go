package kvnode

import (
	"context"
	"log"
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
				log.Printf("Read wait took %v for key_len=%d, cf=%s",
					waitTime, len(req.key), req.cf)
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

// ReadIndexBatcher batches multiple read requests to share a single ReadIndex call
type ReadIndexBatcher struct {
	mu       sync.Mutex
	requests []*ReadRequest
	trigger  chan struct{}
	kvnode   *KVNode
}

// NewReadIndexBatcher creates a new ReadIndex batcher
func NewReadIndexBatcher(kvnode *KVNode) *ReadIndexBatcher {
	return &ReadIndexBatcher{
		requests: make([]*ReadRequest, 0, 16),
		trigger:  make(chan struct{}, 1),
		kvnode:   kvnode,
	}
}

// enqueue adds a read request to the batch and triggers processing
func (b *ReadIndexBatcher) enqueue(cf string, key []byte) *ReadRequest {
	req := &ReadRequest{
		cf:      cf,
		key:     key,
		done:    make(chan struct{}),
		addedAt: time.Now(),
	}

	b.mu.Lock()
	b.requests = append(b.requests, req)
	batchSize := len(b.requests)
	b.mu.Unlock()

	select {
	case b.trigger <- struct{}{}:
	default:
	}

	if batchSize > 1 {
		log.Printf("Read batch size: %d", batchSize)
	}

	return req
}

// run processes read batches
func (b *ReadIndexBatcher) run(stopCh <-chan struct{}) {
	for {
		select {
		case <-b.trigger:
			b.processBatch()
		case <-stopCh:
			return
		}
	}
}

// processBatch processes the current batch of read requests
func (b *ReadIndexBatcher) processBatch() {
	b.mu.Lock()

	if len(b.requests) == 0 {
		b.mu.Unlock()
		return
	}

	requests := b.requests
	b.requests = make([]*ReadRequest, 0, 16)
	b.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	readIndex, err := b.kvnode.raftNode.ReadIndex(ctx)

	// Handle failure (error or not leader)
	if err != nil || readIndex == 0 {
		if err != nil {
			log.Printf("ReadIndex failed: %v", err)
		} else {
			log.Printf("Not leader, cannot serve linearizable read")
		}
		b.failRequests(requests, 0)
		return
	}

	// Success: assign readIndex and add to wait queue
	for _, req := range requests {
		req.readIndex = readIndex
		b.kvnode.readWaitQueue.Add(req)
	}
	b.kvnode.notifyReadWaitQueue()
}

// failRequests marks all requests as failed
func (b *ReadIndexBatcher) failRequests(requests []*ReadRequest, readIndex uint64) {
	for _, req := range requests {
		req.readIndex = readIndex
		close(req.done)
	}
}

