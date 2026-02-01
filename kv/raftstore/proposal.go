package raftstore

import "github.com/zbchi/mizu/proto/kvpb"

// proposal is one client write waiting for its replicated entry to apply.
type proposal struct {
	request *kvpb.RaftCmdRequest
	future  *proposalFuture
}

// proposalFuture is completed exactly once when a proposal is rejected or
// reaches the region state machine.
type proposalFuture struct {
	done chan struct{}
	resp *kvpb.RaftCmdResponse
	err  error
}

func newProposalFuture() *proposalFuture {
	return &proposalFuture{done: make(chan struct{})}
}

func (f *proposalFuture) finish(resp *kvpb.RaftCmdResponse, err error) {
	f.resp = resp
	f.err = err
	close(f.done)
}

func (f *proposalFuture) wait() (*kvpb.RaftCmdResponse, error) {
	<-f.done
	return f.resp, f.err
}
