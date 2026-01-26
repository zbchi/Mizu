package raftstore

import "github.com/zbchi/mizu/proto/raftkvpb"

// StateMachine owns the replicated business state for each region.
type StateMachine interface {
	ApplyCommand(regionID uint64, req *raftkvpb.RaftCmdRequest) error
	CreateSnapshot(regionID uint64) ([]byte, error)
	ApplySnapshot(regionID uint64, data []byte) error
}
