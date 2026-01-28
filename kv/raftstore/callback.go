package raftstore

import (
	"sync"

	"github.com/zbchi/mizu/proto/kvpb"
)

// Callback is used to notify when a command is committed and applied.
type Callback struct {
	Done chan struct{}
	resp *kvpb.RaftCmdResponse
	err  error
}

func (cb *Callback) Finish(resp *kvpb.RaftCmdResponse, err error) {
	cb.resp = resp
	cb.err = err
	close(cb.Done)
}

func (cb *Callback) Wait() (*kvpb.RaftCmdResponse, error) {
	<-cb.Done
	return cb.resp, cb.err
}

type callbackKey struct {
	regionID uint64
	index    uint64
}

// CallbackManager tracks proposals until their entries are applied.
type CallbackManager struct {
	store            *Store
	pendingCallbacks map[callbackKey]*RaftCmd
	mu               sync.RWMutex
}

func NewCallbackManager(store *Store) *CallbackManager {
	return &CallbackManager{
		store:            store,
		pendingCallbacks: make(map[callbackKey]*RaftCmd),
	}
}

func (cm *CallbackManager) Register(cmd *RaftCmd, regionID uint64) {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	cmd.Index = cm.store.nextIndex(regionID)
	key := callbackKey{regionID: regionID, index: cmd.Index}
	cm.pendingCallbacks[key] = cmd
}

func (cm *CallbackManager) Unregister(cmd *RaftCmd, regionID uint64) {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	key := callbackKey{regionID: regionID, index: cmd.Index}
	delete(cm.pendingCallbacks, key)
}

func (cm *CallbackManager) TriggerForRegion(regionID uint64, index uint64, err error) {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	key := callbackKey{regionID: regionID, index: index}
	cmd, ok := cm.pendingCallbacks[key]
	if !ok {
		return
	}
	delete(cm.pendingCallbacks, key)

	var responses []*kvpb.Response
	if err == nil {
		responses = buildWriteResponses(cmd.Request)
	}

	resp := cm.store.BuildResponse(cmd.Request, regionID, responses, err)
	cmd.Cb.Finish(resp, err)
}
