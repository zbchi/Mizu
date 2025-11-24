package raft

import (
	"sync"
	"time"

	"github.com/zbchi/linkv/proto/raftpb"
)

type Raft struct {
	mu    sync.Mutex
	peers []uint64
	//persister
	me uint64
	//dead

	term      uint64
	state     uint64
	votedFor  uint64
	voteCount uint64

	lastHeartbeat time.Time

	logs        []raftpb.Entry
	commitIndex uint64

	nextIndex  map[uint64]uint64
	matchIndex map[uint64]uint64
}
