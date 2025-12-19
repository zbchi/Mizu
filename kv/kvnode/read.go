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
	addedAt   time.Time // for monitoring
}

// ReadWaitQueue manages read requests waiting for appliedIndex >= readIndex
type ReadWaitQueue struct {
	mu    sync.Mutex
	queue []*ReadRequest
}

// NewReadWaitQueue creates a new read wait queue
func NewReadWaitQueue() *ReadWaitQueue {
	return &ReadWaitQueue{
		queue: make([]*ReadRequest, 0, 128),
	}
}

// Add adds a read request to the queue
func (q *ReadWaitQueue) Add(req *ReadRequest) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.queue = append(q.queue, req)
}

// Notify checks and wakes up read requests when appliedIndex advances
func (q *ReadWaitQueue) Notify(appliedIndex uint64) {
	q.mu.Lock()
	defer q.mu.Unlock()

	// Filter out completed requests
	newQueue := make([]*ReadRequest, 0, len(q.queue))
	for _, req := range q.queue {
		if appliedIndex >= req.readIndex {
			// Wake up the waiting goroutine
			close(req.done)

			// Log wait time for monitoring (avoid printing binary keys)
			waitTime := time.Since(req.addedAt)
			if waitTime > 100*time.Millisecond {
				log.Printf("Read wait took %v for key_len=%d, cf=%s",
					waitTime, len(req.key), req.cf)
			}
		} else {
			// Keep waiting
			newQueue = append(newQueue, req)
		}
	}
	q.queue = newQueue
}

// readBatch represents a batch of read requests sharing the same ReadIndex
type readBatch struct {
	requests []*ReadRequest
	started  bool // whether ReadIndex has been triggered
}

// newReadBatch creates a new read batch
func newReadBatch() *readBatch {
	return &readBatch{
		requests: make([]*ReadRequest, 0, 16),
		started:  false,
	}
}

// ReadIndexBatcher batches multiple read requests to share a single ReadIndex call
type ReadIndexBatcher struct {
	mu      sync.Mutex
	batch   *readBatch
	trigger chan struct{}
	kvnode  *KVNode
}

// NewReadIndexBatcher creates a new ReadIndex batcher
func NewReadIndexBatcher(kvnode *KVNode) *ReadIndexBatcher {
	return &ReadIndexBatcher{
		batch:   newReadBatch(),
		trigger: make(chan struct{}, 1),
		kvnode:  kvnode,
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
	b.batch.requests = append(b.batch.requests, req)
	batchSize := len(b.batch.requests)
	b.mu.Unlock()

	// Trigger batch processing (non-blocking)
	select {
	case b.trigger <- struct{}{}:
	default:
	}

	// Log batch size for monitoring
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

	// Check if already processing
	if b.batch.started || len(b.batch.requests) == 0 {
		b.mu.Unlock()
		return
	}

	// Mark as started
	b.batch.started = true

	// Take the current batch
	batch := b.batch
	b.batch = newReadBatch()
	b.mu.Unlock()

	// Trigger ReadIndex
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	readIndex, err := b.kvnode.raftNode.ReadIndex(ctx)
	if err != nil {
		// ReadIndex failed, mark all requests as failed
		log.Printf("ReadIndex failed: %v", err)
		b.failBatch(batch, 0)
		return
	}

	// Check if we are the leader (readIndex = 0 means not leader)
	if readIndex == 0 {
		log.Printf("Not leader, cannot serve linearizable read")
		b.failBatch(batch, 0)
		return
	}

	// Assign readIndex to all requests and add to wait queue
	for _, req := range batch.requests {
		req.readIndex = readIndex
		b.kvnode.readWaitQueue.Add(req)
	}

	// Check if already satisfied (apply might have advanced)
	b.kvnode.notifyReadWaitQueue()
}

// failBatch marks all requests in a batch as failed
func (b *ReadIndexBatcher) failBatch(batch *readBatch, readIndex uint64) {
	for _, req := range batch.requests {
		req.readIndex = readIndex // 0 indicates failure
		close(req.done)
	}
}
