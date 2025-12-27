package kvnode

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/zbchi/linkv/proto/raftkvpb"
)

func TestReadWaitQueue(t *testing.T) {
	queue := &ReadWaitQueue{}

	req1 := &ReadRequest{
		readIndex: 10,
		cf:        "default",
		key:       []byte("key1"),
		done:      make(chan struct{}),
		addedAt:   time.Now(),
	}
	req2 := &ReadRequest{
		readIndex: 15,
		cf:        "default",
		key:       []byte("key2"),
		done:      make(chan struct{}),
		addedAt:   time.Now(),
	}

	queue.Add(req1)
	queue.Add(req2)

	// Notify with appliedIndex=10, should wake req1 only (10 >= 10, but 10 < 15)
	queue.Notify(10)

	select {
	case <-req1.done:
		// Expected
	case <-time.After(100 * time.Millisecond):
		t.Fatal("req1 should be woken")
	}

	select {
	case <-req2.done:
		t.Fatal("req2 should not be woken yet")
	case <-time.After(50 * time.Millisecond):
		// Expected
	}

	// Notify with appliedIndex=15 to wake req2
	queue.Notify(15)

	select {
	case <-req2.done:
		// Expected now
	case <-time.After(100 * time.Millisecond):
		t.Fatal("req2 should be woken now")
	}
}

func TestReadWaitQueueMultipleNotify(t *testing.T) {
	queue := &ReadWaitQueue{}

	// Add multiple requests with different readIndex values
	requests := []*ReadRequest{
		{readIndex: 5, cf: "cf", key: []byte("k1"), done: make(chan struct{}), addedAt: time.Now()},
		{readIndex: 10, cf: "cf", key: []byte("k2"), done: make(chan struct{}), addedAt: time.Now()},
		{readIndex: 15, cf: "cf", key: []byte("k3"), done: make(chan struct{}), addedAt: time.Now()},
	}

	for _, req := range requests {
		queue.Add(req)
	}

	// First notify: appliedIndex=10, should wake first 2
	queue.Notify(10)

	if !notifiedBefore(requests[0].done, 50*time.Millisecond) {
		t.Fatal("request with readIndex=5 should be woken")
	}
	if !notifiedBefore(requests[1].done, 50*time.Millisecond) {
		t.Fatal("request with readIndex=10 should be woken")
	}
	if notifiedBefore(requests[2].done, 50*time.Millisecond) {
		t.Fatal("request with readIndex=15 should NOT be woken yet")
	}

	// Second notify: appliedIndex=20, should wake all
	queue.Notify(20)

	if !notifiedBefore(requests[2].done, 50*time.Millisecond) {
		t.Fatal("request with readIndex=15 should be woken now")
	}
}

func notifiedBefore(ch chan struct{}, timeout time.Duration) bool {
	select {
	case <-ch:
		return true
	case <-time.After(timeout):
		return false
	}
}

func TestRouterCallbacks(t *testing.T) {
	node := &KVNode{
		cfg: &Config{
			NodeID: 1,
		},
		appliedIndex: 5,
	}
	router := NewRouter(node)

	cb := &Callback{Done: make(chan struct{})}
	cmd := &RaftCmd{
		Request: &raftkvpb.RaftCmdRequest{
			Header: &raftkvpb.RequestHeader{
				ClusterId: 1,
			},
		},
		cb: cb,
	}

	// Register callback
	router.registerCallback(cmd)
	if cmd.index != 6 {
		t.Fatalf("expected index 6, got %d", cmd.index)
	}

	// Trigger callback with success
	router.triggerCallback(6, 1, nil)

	select {
	case <-cb.Done:
		// Expected - callback was finished
	case <-time.After(100 * time.Millisecond):
		t.Fatal("callback should be finished")
	}

	resp, err := cb.Wait()
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if resp == nil {
		t.Fatal("expected response")
	}
	if !resp.Header.Success {
		t.Fatal("expected success")
	}

	// Trigger callback with error - no callback registered for index 7, so nothing happens
	router.triggerCallback(7, 1, errors.New("test error"))
}

func TestRouterCallbackError(t *testing.T) {
	node := &KVNode{
		cfg: &Config{
			NodeID: 1,
		},
		appliedIndex: 10,
	}
	router := NewRouter(node)

	testErr := errors.New("storage error")
	cb := &Callback{Done: make(chan struct{})}
	cmd := &RaftCmd{
		Request: &raftkvpb.RaftCmdRequest{
			Header: &raftkvpb.RequestHeader{
				ClusterId: 1,
			},
		},
		cb: cb,
	}

	router.registerCallback(cmd)
	router.triggerCallback(11, 1, testErr)

	select {
	case <-cb.Done:
		// Expected
	case <-time.After(100 * time.Millisecond):
		t.Fatal("callback should be finished")
	}

	resp, err := cb.Wait()
	if err != testErr {
		t.Fatalf("expected error %v, got %v", testErr, err)
	}
	if resp.Header.Success {
		t.Fatal("expected failure in response")
	}
	if resp.Header.Error != testErr.Error() {
		t.Fatalf("expected error message %q, got %q", testErr.Error(), resp.Header.Error)
	}
}

func TestRouterUnregisterCallback(t *testing.T) {
	node := &KVNode{
		cfg: &Config{
			NodeID: 1,
		},
		appliedIndex: 5,
	}
	router := NewRouter(node)

	cb := &Callback{Done: make(chan struct{})}
	cmd := &RaftCmd{
		Request: &raftkvpb.RaftCmdRequest{
			Header: &raftkvpb.RequestHeader{
				ClusterId: 1,
			},
		},
		cb: cb,
	}

	router.registerCallback(cmd)
	router.unregisterCallback(cmd)

	// Trigger callback - should not call Finish since it was unregistered
	router.triggerCallback(6, 1, nil)

	select {
	case <-cb.Done:
		t.Fatal("callback should NOT be finished after unregister")
	case <-time.After(50 * time.Millisecond):
		// Expected - callback was not called
	}
}

func TestReadIndexBatcherFailRequests(t *testing.T) {
	node := &KVNode{
		readWaitQueue: &ReadWaitQueue{},
	}
	batcher := NewReadIndexBatcher(node)

	requests := []*ReadRequest{
		{cf: "cf1", key: []byte("k1"), done: make(chan struct{}), addedAt: time.Now()},
		{cf: "cf2", key: []byte("k2"), done: make(chan struct{}), addedAt: time.Now()},
	}

	// Simulate ReadIndex failure with readIndex=0
	batcher.failRequests(requests, 0)

	for _, req := range requests {
		select {
		case <-req.done:
			// Expected
		case <-time.After(100 * time.Millisecond):
			t.Fatal("request should be marked as failed")
		}

		if req.readIndex != 0 {
			t.Fatalf("expected readIndex 0, got %d", req.readIndex)
		}
	}
}

func TestCallbackWaitTimeout(t *testing.T) {
	cb := &Callback{Done: make(chan struct{})}

	// Wait should block until Done is closed
	doneCh := make(chan struct{})
	go func() {
		cb.Wait()
		close(doneCh)
	}()

	select {
	case <-doneCh:
		t.Fatal("Wait should not return before Finish is called")
	case <-time.After(50 * time.Millisecond):
		// Expected - Wait is still blocking
	}

	// Now call Finish
	cb.Finish(&raftkvpb.RaftCmdResponse{}, nil)

	select {
	case <-doneCh:
		// Expected - Wait returned
	case <-time.After(100 * time.Millisecond):
		t.Fatal("Wait should return after Finish is called")
	}
}

func TestCallbackFinishWithError(t *testing.T) {
	cb := &Callback{Done: make(chan struct{})}
	doneCh := make(chan struct{})

	go func() {
		cb.Wait()
		close(doneCh)
	}()

	// Wait should block
	select {
	case <-cb.Done:
		t.Fatal("callback should not be finished yet")
	case <-doneCh:
		t.Fatal("Wait should not return yet")
	case <-time.After(50 * time.Millisecond):
		// Expected - Wait is still blocking
	}

	// Finish the callback with an error
	cb.Finish(nil, context.Canceled)

	select {
	case <-doneCh:
		// Expected - Wait returned
	case <-time.After(100 * time.Millisecond):
		t.Fatal("Wait should return after Finish")
	}
}
