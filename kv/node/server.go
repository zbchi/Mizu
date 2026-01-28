package node

import (
	"context"
	"errors"

	"github.com/zbchi/mizu/proto/kvpb"
)

// Server implements the RaftKV gRPC service
type Server struct {
	kvpb.UnimplementedRaftKVServer
	node *Node
}

// NewServer creates a new RaftKV server
func NewServer(node *Node) *Server {
	return &Server{
		node: node,
	}
}

func (s *Server) Propose(ctx context.Context, req *kvpb.RaftCmdRequest) (*kvpb.RaftCmdResponse, error) {
	if len(req.Requests) == 0 {
		return nil, errors.New("empty requests")
	}

	switch req.Requests[0].CmdType {
	case kvpb.CmdType_Get:
		return s.node.Get(ctx, req)
	case kvpb.CmdType_Scan:
		return s.node.Scan(ctx, req)
	case kvpb.CmdType_Put, kvpb.CmdType_Delete:
		return s.node.Write(req)
	default:
		return nil, errors.New("unknown command type")
	}
}
