package raftstore

import "github.com/zbchi/mizu/proto/kvpb"

type MsgType int

const (
	MsgTypeRaftMessage MsgType = iota
	MsgTypeRaftCmd
	MsgTypeTick
	MsgTypeSnapshotTrigger
	MsgTypeReadIndex
)

// Msg is an internal event consumed by raftWorker.
type Msg struct {
	Type     MsgType
	RegionID uint64
	Data     interface{}
}

// RaftCmd is a replicated business command with its completion callback.
type RaftCmd struct {
	Request *kvpb.RaftCmdRequest
	Cb      *Callback
	Index   uint64
}

type SnapshotTrigger struct {
	Index uint64
	Data  []byte
}

type ReadIndexRequest struct {
	Resp chan ReadIndexResult
}

type ReadIndexResult struct {
	Ready <-chan struct{}
	Err   error
}
