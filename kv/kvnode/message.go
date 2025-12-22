package kvnode

import (
	"github.com/zbchi/linkv/proto/raftkvpb"
)

// RaftCmd represents a Raft command with callback
type RaftCmd struct {
	Request *raftkvpb.RaftCmdRequest
	cb      *Callback
	index   uint64
}

// Callback is used to notify when a command is committed and applied
type Callback struct {
	Done chan struct{}
	resp *raftkvpb.RaftCmdResponse
	err  error
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
