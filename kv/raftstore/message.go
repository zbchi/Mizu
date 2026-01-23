package raftstore

import (
	"log/slog"
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

// CallbackResponseProvider provides callback bookkeeping and response metadata.
type CallbackResponseProvider interface {
	NextIndex(regionID uint64) uint64
	BuildResponse(req *raftkvpb.RaftCmdRequest, regionID uint64, responses []*raftkvpb.Response, err error) *raftkvpb.RaftCmdResponse
}

// CallbackManager manages pending callbacks for client requests
type CallbackManager struct {
	provider         CallbackResponseProvider
	pendingCallbacks map[callbackKey]*RaftCmd
	mu               sync.RWMutex
}

// NewCallbackManager creates a new CallbackManager
func NewCallbackManager(provider CallbackResponseProvider) *CallbackManager {
	return &CallbackManager{
		provider:         provider,
		pendingCallbacks: make(map[callbackKey]*RaftCmd),
	}
}

// Register stores a callback waiting for entry to be committed and applied
func (cm *CallbackManager) Register(cmd *RaftCmd, regionID uint64) {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	if cm.provider != nil {
		cmd.Index = cm.provider.NextIndex(regionID)
	}

	key := callbackKey{regionID: regionID, index: cmd.Index}
	cm.pendingCallbacks[key] = cmd
	slog.Info("Callback registered", "region", regionID, "index", cmd.Index)
}

// Unregister removes a callback (used when propose fails)
func (cm *CallbackManager) Unregister(cmd *RaftCmd, regionID uint64) {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	key := callbackKey{regionID: regionID, index: cmd.Index}
	delete(cm.pendingCallbacks, key)
}

// Trigger notifies a waiting callback when its entry is committed and applied
// Legacy method for compatibility, assumes region 1
func (cm *CallbackManager) Trigger(index uint64, term uint64, err error) {
	cm.TriggerForRegion(1, index, term, err)
}

// TriggerForRegion notifies a waiting callback for a specific region
func (cm *CallbackManager) TriggerForRegion(regionID uint64, index uint64, term uint64, err error) {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	key := callbackKey{regionID: regionID, index: index}
	cmd, ok := cm.pendingCallbacks[key]
	if !ok {
		slog.Debug("Callback not found", "region", regionID, "index", index)
		return
	}

	delete(cm.pendingCallbacks, key)

	var responses []*raftkvpb.Response
	if err == nil {
		// Mirror the original write command shape so callers receive one response entry per
		// proposed sub-request after the log entry is applied.
		responses = BuildWriteResponses(cmd.Request)
	}

	resp := cm.provider.BuildResponse(cmd.Request, regionID, responses, err)

	slog.Info("Callback triggered", "region", regionID, "index", index, "success", err == nil)
	if err != nil {
		slog.Error("Callback triggered with error", "error", err)
	}
	cmd.Cb.Finish(resp, err)
}
