package kvnode

import (
	"context"

	"github.com/zbchi/linkv/proto/raftkvpb"
)

// Server implements the RaftKV gRPC service
type Server struct {
	raftkvpb.UnimplementedRaftKVServer
	node *KVNode
}

// NewServer creates a new RaftKV server
func NewServer(node *KVNode) *Server {
	return &Server{
		node: node,
	}
}

// Propose implements the RaftKV.Propose RPC
func (s *Server) Propose(ctx context.Context, req *raftkvpb.RaftCmdRequest) (*raftkvpb.RaftCmdResponse, error) {
	// Propose the command through Raft consensus
	return s.node.Propose(req)
}
