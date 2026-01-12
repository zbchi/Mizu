package kvnode

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/zbchi/linkv/kv/region"
	"github.com/zbchi/linkv/proto/raftkvpb"
)

// newTestRegion creates a test region with given ID
func newTestRegion(id uint64) *region.Region {
	return &region.Region{
		ID:       id,
		StartKey: []byte{},
		EndKey:   []byte{},
	}
}

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

func TestCallbackManager(t *testing.T) {
	// Create a minimal mock KVNode
	node := &KVNode{
		cfg: &Config{
			NodeID: 1,
		},
		peerRouter: NewPeerRouter(),
	}

	// Create a mock peer with appliedIndex
	peer := &Peer{
		region:        newTestRegion(1),
		appliedIndex:  5,
		readWaitQueue: &ReadWaitQueue{},
	}
	// Register peer to router
	node.peerRouter.Register(peer)

	callbackMgr := NewCallbackManager(node)

	cb := &Callback{Done: make(chan struct{})}
	cmd := &RaftCmd{
		Request: &raftkvpb.RaftCmdRequest{
			Header: &raftkvpb.RequestHeader{
				ClusterId: 1,
			},
		},
		cb: cb,
	}

	// Register callback for region 1
	callbackMgr.Register(cmd, 1)
	if cmd.index != 6 {
		t.Fatalf("expected index 6, got %d", cmd.index)
	}

	// Trigger callback with success
	callbackMgr.TriggerForRegion(1, 6, 1, nil)

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
	callbackMgr.TriggerForRegion(1, 7, 1, errors.New("test error"))
}

func TestCallbackManagerError(t *testing.T) {
	node := &KVNode{
		cfg: &Config{
			NodeID: 1,
		},
		peerRouter: NewPeerRouter(),
	}

	peer := &Peer{
		region:        newTestRegion(1),
		appliedIndex:  10,
		readWaitQueue: &ReadWaitQueue{},
	}
	node.peerRouter.Register(peer)

	callbackMgr := NewCallbackManager(node)

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

	callbackMgr.Register(cmd, 1)
	callbackMgr.TriggerForRegion(1, 11, 1, testErr)

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

func TestCallbackManagerUnregister(t *testing.T) {
	node := &KVNode{
		cfg: &Config{
			NodeID: 1,
		},
		peerRouter: NewPeerRouter(),
	}

	peer := &Peer{
		region:        newTestRegion(1),
		appliedIndex:  5,
		readWaitQueue: &ReadWaitQueue{},
	}
	node.peerRouter.Register(peer)

	callbackMgr := NewCallbackManager(node)

	cb := &Callback{Done: make(chan struct{})}
	cmd := &RaftCmd{
		Request: &raftkvpb.RaftCmdRequest{
			Header: &raftkvpb.RequestHeader{
				ClusterId: 1,
			},
		},
		cb: cb,
	}

	callbackMgr.Register(cmd, 1)
	callbackMgr.Unregister(cmd, 1)

	// Trigger callback - should not call Finish since it was unregistered
	callbackMgr.TriggerForRegion(1, 6, 1, nil)

	select {
	case <-cb.Done:
		t.Fatal("callback should NOT be finished after unregister")
	case <-time.After(50 * time.Millisecond):
		// Expected - callback was not called
	}
}

func TestPeerWaitForReadIndex(t *testing.T) {
	node := &KVNode{closeCh: make(chan struct{})}
	peer := &Peer{
		region:        newTestRegion(1),
		appliedIndex:  10,
		readWaitQueue: &ReadWaitQueue{},
		node:          node,
	}

	// Start a goroutine to wait for readIndex=15
	// It should block since appliedIndex never reaches 15
	doneCh := make(chan error, 1)
	go func() {
		doneCh <- peer.waitForReadIndex(15)
	}()

	// Close closeCh after delay to unblock the wait
	go func() {
		time.Sleep(50 * time.Millisecond)
		close(node.closeCh)
	}()

	select {
	case <-doneCh:
		// Expected - wait returned when closeCh closed
	case <-time.After(100 * time.Millisecond):
		t.Fatal("wait should return when closeCh closes")
	}
}

func TestPeerWaitForReadIndexImmediate(t *testing.T) {
	peer := &Peer{
		region:        newTestRegion(1),
		appliedIndex:  10,
		readWaitQueue: &ReadWaitQueue{},
		node:          &KVNode{closeCh: make(chan struct{})},
	}

	// Start a goroutine to advance appliedIndex after a short delay
	go func() {
		time.Sleep(50 * time.Millisecond)
		peer.appliedIndex = 15
		peer.notifyReadWaitQueue()
	}()

	// Request to wait for readIndex=15
	err := peer.waitForReadIndex(15)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	if peer.appliedIndex != 15 {
		t.Fatalf("expected appliedIndex 15, got %d", peer.appliedIndex)
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

	// Finish callback with an error
	cb.Finish(&raftkvpb.RaftCmdResponse{}, context.Canceled)

	select {
	case <-doneCh:
		// Expected - Wait returned
	case <-time.After(100 * time.Millisecond):
		t.Fatal("Wait should return after Finish")
	}
}
