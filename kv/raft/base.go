package raft

import (
	"sort"

	"github.com/zbchi/linkv/proto/raftpb"
)

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
	entries      []raftpb.Entry
	commitIndex  uint64
	appliedIndex uint64
}

type Progress struct {
	Match uint64
	Next  uint64
}

type ProgressTracker struct {
	prs map[uint64]*Progress
}

func (l *RaftLog) LastIndex() uint64 {
	return uint64(len(l.entries)) - 1
}

func (l *RaftLog) LastTerm() uint64 {
	return uint64(l.entries[len(l.entries)-1].Term)
}

func (r *Raft) preLogIndex(id uint64) uint64 {
	return r.prs.prs[id].Next - 1
}

func (r *Raft) preLogTerm(id uint64) uint64 {
	return r.raftLog.entries[r.preLogIndex(id)].Term
}

func (r *Raft) NewRaftLog() *RaftLog {
	return &RaftLog{
		entries:      []raftpb.Entry{{Term: 0, Index: 0}},
		commitIndex:  0,
		appliedIndex: 0,
	}
}

func (p *ProgressTracker) Committed() uint64 {
	n := len(p.prs)
	if n == 0 {
		return 0
	}
	mathes := make([]uint64, 0, n)
	for _, pr := range p.prs {
		mathes = append(mathes, pr.Match)
	}
	sort.Slice(mathes, func(i, j int) bool {
		return mathes[i] > mathes[j]
	})

	quorum := n/2 + 1
	return mathes[quorum-1]
}

func (l *RaftLog) commitTo(to uint64) {
	if to > l.commitIndex {
		l.commitIndex = to
	}
}

// match commit log's term with leader's term
func (l *RaftLog) matchCommitTerm(index uint64, term uint64) bool {
	if l.entries[index].Term == term {
		return true
	}
	return false
}
