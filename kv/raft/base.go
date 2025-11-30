package raft

import "github.com/zbchi/linkv/proto/raftpb"

type StateType int

const (
	StateFollower StateType = iota
	StateCandidate
	StateLeader
)

type HardState struct {
	Term   uint64
	Vote   uint64
	Commit uint64
}

type RaftLog struct {
	entries   []raftpb.Entry
	committed uint64
	applied   uint64
}

type Progress struct {
	Match uint64
	Next  uint64
}

type ProgressTracker struct {
	prs map[uint64]*Progress
}
