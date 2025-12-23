package raft_server

import (
	"context"
	"errors"

	"github.com/zbchi/linkv/kv/kvnode"
	"github.com/zbchi/linkv/proto/raftkvpb"
)

// Server implements raftkvpb.RaftKVServer
type Server struct {
	raftkvpb.UnimplementedRaftKVServer
	node *kvnode.KVNode
}

// NewServer creates a new RaftKV server
func NewServer(node *kvnode.KVNode) *Server {
	return &Server{node: node}
}

func (s *Server) Propose(ctx context.Context, req *raftkvpb.RaftCmdRequest) (*raftkvpb.RaftCmdResponse, error) {
	if len(req.Requests) == 0 {
		return nil, errors.New("empty requests")
	}

	switch req.Requests[0].CmdType {
	case raftkvpb.CmdType_Get:
		return s.node.Get(ctx, req)
	case raftkvpb.CmdType_Put, raftkvpb.CmdType_Delete, raftkvpb.CmdType_Scan:
		return s.node.Put(req)
	default:
		return nil, errors.New("unknown command type")
	}
}
