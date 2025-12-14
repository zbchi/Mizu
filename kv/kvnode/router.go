package kvnode

import (
	"log"
	"sync"

	"github.com/zbchi/linkv/proto/raftpb"
	"github.com/zbchi/linkv/proto/raftkvpb"
)

// Router handles message routing for KVNode
type Router struct {
	node       *KVNode
	proposals   map[uint64]*RaftCmd
	mu          sync.RWMutex
	transport   Transport
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
		node:       node,
		proposals:  make(map[uint64]*RaftCmd),
	}
}

// Send sends a Raft message through transport
func (r *Router) Send(msg raftpb.Message) error {
	if r.transport == nil {
		log.Printf("No transport configured, dropping message to %d", msg.To)
		return nil
	}
	return r.transport.Send(&msg)
}

// SendPtr sends a Raft message pointer through transport (avoid copying lock)
func (r *Router) SendPtr(msg *raftpb.Message) error {
	if r.transport == nil {
		log.Printf("No transport configured, dropping message to %d", msg.To)
		return nil
	}
	return r.transport.Send(msg)
}

// RegisterProposal registers a proposal waiting to be committed
func (r *Router) RegisterProposal(cmd *RaftCmd) uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()

	// The index will be assigned when the entry is proposed
	// For now, we use a simple counter
	index := r.node.appliedIndex + 1
	r.proposals[index] = cmd
	return index
}

// NotifyProposal notifies a proposal when it's committed and applied
func (r *Router) NotifyProposal(index uint64, term uint64, err error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	cmd, ok := r.proposals[index]
	if !ok {
		return
	}

	delete(r.proposals, index)

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
			log.Printf("Raft channel full, dropping message")
		}
	}
}
