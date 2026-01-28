package raftstore

import (
	"github.com/zbchi/mizu/proto/kvpb"
	"github.com/zbchi/mizu/proto/raftpb"
)

// StateMachine owns the replicated business state for each region.
type StateMachine interface {
	ApplyCommand(regionID uint64, req *kvpb.RaftCmdRequest) error
	CreateSnapshot(regionID uint64) ([]byte, error)
	ApplySnapshot(regionID uint64, data []byte) error
}

type Transport interface {
	Send(msg *raftpb.Message) error
	Receive() <-chan *raftpb.Message
}
