package kvnode

import (
	"log/slog"
	"sync"

	"github.com/zbchi/linkv/proto/raftpb"
	"github.com/zbchi/linkv/proto/raftkvpb"
)

// Router handles message routing for KVNode
type Router struct {
	node            *KVNode
	pendingCallbacks map[uint64]*RaftCmd  // index → waiting command with callback
	mu              sync.RWMutex
	transport       Transport
}

// Transport defines the interface for sending and receiving Raft messages
type Transport interface {
	Send(msg *raftpb.Message) error
	Start() error
	Close() error
	Receive() <-chan *raftpb.Message
}

// NewRouter creates a new Router
func NewRouter(node *KVNode) *Router {
	return &Router{
		node:            node,
		pendingCallbacks: make(map[uint64]*RaftCmd),
	}
}

// Send sends a Raft message through transport
func (r *Router) Send(msg raftpb.Message) error {
	if r.transport == nil {
		slog.Warn("No transport configured, dropping message", "to", msg.To)
		return nil
	}
	return r.transport.Send(&msg)
}

// registerCallback stores a callback waiting for entry to be committed and applied
func (r *Router) registerCallback(cmd *RaftCmd) {
	r.mu.Lock()
	defer r.mu.Unlock()

	// Predict the index this entry will get (assuming sequential)
	index := r.node.appliedIndex + 1
	r.pendingCallbacks[index] = cmd
	cmd.index = index
}

// unregisterCallback removes a callback (used when propose fails)
func (r *Router) unregisterCallback(cmd *RaftCmd) {
	r.mu.Lock()
	defer r.mu.Unlock()

	delete(r.pendingCallbacks, cmd.index)
}

// triggerCallback notifies a waiting callback when its entry is committed and applied
func (r *Router) triggerCallback(index uint64, term uint64, err error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	cmd, ok := r.pendingCallbacks[index]
	if !ok {
		return
	}

	delete(r.pendingCallbacks, index)

	// TODO: Build response from applied entry
	resp := &raftkvpb.RaftCmdResponse{
		Header: &raftkvpb.ResponseHeader{
			ClusterId: cmd.Request.Header.ClusterId,
			NodeId:    r.node.NodeID(),
			Success:   err == nil,
		},
	}

	if err != nil {
		resp.Header.Error = err.Error()
	}

	cmd.cb.Finish(resp, err)
}

// SetTransport sets the transport and starts receiving messages
func (r *Router) SetTransport(t Transport) {
	r.transport = t

	// Start receiving messages from transport
	go r.receiveLoop()
}

// receiveLoop receives messages from transport and forwards to KVNode
func (r *Router) receiveLoop() {
	recvCh := r.transport.Receive()
	for msg := range recvCh {
		select {
		case r.node.raftCh <- *msg:
		default:
			slog.Warn("Raft channel full, dropping message")
		}
	}
}
