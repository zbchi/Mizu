package kvnode

import (
	"github.com/zbchi/linkv/proto/raftkvpb"
	"github.com/zbchi/linkv/proto/raftpb"
)

// MsgType represents the type of message
type MsgType int

const (
	MsgTypeRaftMessage MsgType = iota
	MsgTypeRaftCmd
	MsgTypeTick
)

// Message represents a message in the kvnode
type Message struct {
	Type MsgType
	Data interface{}
}

// RaftMessage wraps raft protobuf message
type RaftMessage struct {
	From uint64
	To   uint64
	Msg  raftpb.Message
}

// RaftCmd represents a Raft command with callback
type RaftCmd struct {
	Request *raftkvpb.RaftCmdRequest
	cb      *Callback
}

// Callback is used to notify when a command is committed and applied
type Callback struct {
	Done  chan struct{}
	resp  *raftkvpb.RaftCmdResponse
	err   error
}

// NewCallback creates a new callback
func NewCallback() *Callback {
	return &Callback{
		Done: make(chan struct{}),
	}
}

// Finish notifies the callback with response
func (cb *Callback) Finish(resp *raftkvpb.RaftCmdResponse, err error) {
	cb.resp = resp
	cb.err = err
	close(cb.Done)
}

// Wait waits for the callback to complete
func (cb *Callback) Wait() (*raftkvpb.RaftCmdResponse, error) {
	<-cb.Done
	return cb.resp, cb.err
}
