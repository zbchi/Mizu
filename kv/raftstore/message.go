package raftstore

import (
	"sync"

	"github.com/zbchi/mizu/proto/raftkvpb"
)

// RaftCmd represents a Raft command with callback
type RaftCmd struct {
	Request *raftkvpb.RaftCmdRequest
	Cb      *Callback
	Index   uint64
}

// Callback is used to notify when a command is committed and applied
type Callback struct {
	Done chan struct{}
	resp *raftkvpb.RaftCmdResponse
	err  error
}

// Finish notifies the callback with response
func (cb *Callback) Finish(resp *raftkvpb.RaftCmdResponse, err error) {
	cb.resp = resp
	cb.err = err
	close(cb.Done)
}

// Wait waits for the callback to complete
func (cb *Callback) Wait() (*raftkvpb.RaftCmdResponse, error) {
	<-cb.Done
	return cb.resp, cb.err
}

// callbackKey uniquely identifies a pending callback
type callbackKey struct {
	regionID uint64
	index    uint64
}

// CallbackManager manages pending callbacks for client requests
type CallbackManager struct {
	store            *Store
	pendingCallbacks map[callbackKey]*RaftCmd
	mu               sync.RWMutex
}

// NewCallbackManager creates a new CallbackManager
func NewCallbackManager(store *Store) *CallbackManager {
	return &CallbackManager{
		store:            store,
		pendingCallbacks: make(map[callbackKey]*RaftCmd),
	}
}

// Register stores a callback waiting for entry to be committed and applied
func (cm *CallbackManager) Register(cmd *RaftCmd, regionID uint64) {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	if cm.store != nil {
		cmd.Index = cm.store.nextIndex(regionID)
	}

	key := callbackKey{regionID: regionID, index: cmd.Index}
	cm.pendingCallbacks[key] = cmd
}

// Unregister removes a callback (used when propose fails)
func (cm *CallbackManager) Unregister(cmd *RaftCmd, regionID uint64) {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	key := callbackKey{regionID: regionID, index: cmd.Index}
	delete(cm.pendingCallbacks, key)
}

// TriggerForRegion notifies a waiting callback for a specific region.
func (cm *CallbackManager) TriggerForRegion(regionID uint64, index uint64, err error) {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	key := callbackKey{regionID: regionID, index: index}
	cmd, ok := cm.pendingCallbacks[key]
	if !ok {
		return
	}

	delete(cm.pendingCallbacks, key)

	var responses []*raftkvpb.Response
	if err == nil {
		// Mirror the original write command shape so callers receive one response entry per
		// proposed sub-request after the log entry is applied.
		responses = buildWriteResponses(cmd.Request)
	}

	resp := cm.store.BuildResponse(cmd.Request, regionID, responses, err)
	cmd.Cb.Finish(resp, err)
}
