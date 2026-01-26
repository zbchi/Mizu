package raftstore

import "github.com/zbchi/mizu/proto/raftpb"

// Transport exchanges Raft protocol messages between nodes.
type Transport interface {
	Send(msg *raftpb.Message) error
	Receive() <-chan *raftpb.Message
}
