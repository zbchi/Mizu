package kvnode

import "github.com/zbchi/linkv/proto/raftpb"

// Transport defines the interface for sending and receiving Raft messages over network
type Transport interface {
	Send(msg *raftpb.Message) error
	Start() error
	Close() error
	Receive() <-chan *raftpb.Message
}
