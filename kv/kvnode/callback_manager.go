package kvnode

import (
	"sync"

	"github.com/zbchi/linkv/proto/raftkvpb"
)

// callbackKey uniquely identifies a pending callback
type callbackKey struct {
	regionID uint64
	index    uint64
}

// CallbackManager manages pending callbacks for client requests
type CallbackManager struct {
	node             *KVNode
	pendingCallbacks map[callbackKey]*RaftCmd
	mu               sync.RWMutex
}

// NewCallbackManager creates a new CallbackManager
func NewCallbackManager(node *KVNode) *CallbackManager {
	return &CallbackManager{
		node:             node,
		pendingCallbacks: make(map[callbackKey]*RaftCmd),
	}
}

// Register stores a callback waiting for entry to be committed and applied
func (cm *CallbackManager) Register(cmd *RaftCmd, regionID uint64) {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	peer := cm.node.getPeer(regionID)
	if peer == nil {
		return
	}

	// Predict the index this entry will get
	index := peer.appliedIndex + 1
	key := callbackKey{regionID: regionID, index: index}
	cm.pendingCallbacks[key] = cmd
	cmd.index = index
}

// Unregister removes a callback (used when propose fails)
func (cm *CallbackManager) Unregister(cmd *RaftCmd, regionID uint64) {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	key := callbackKey{regionID: regionID, index: cmd.index}
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
		return
	}

	delete(cm.pendingCallbacks, key)

	resp := &raftkvpb.RaftCmdResponse{
		Header: &raftkvpb.ResponseHeader{
			ClusterId: cmd.Request.Header.ClusterId,
			NodeId:    cm.node.NodeID(),
			Success:   err == nil,
		},
	}

	if err != nil {
		resp.Header.Error = err.Error()
	}

	cmd.cb.Finish(resp, err)
}
