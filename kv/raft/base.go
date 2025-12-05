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
	Term        uint64
	Vote        uint64
	CommitIndex uint64
}

type RaftLog struct {
	entries      []raftpb.Entry
	offset       uint64
	appliedIndex uint64
}

type Progress struct {
	Match uint64
	Next  uint64
}

type ProgressTracker struct {
	prs map[uint64]*Progress
}

func (l *RaftLog) FirstIndex() uint64 {
	return l.offset
}

func (l *RaftLog) LastIndex() uint64 {
	return l.offset + uint64(len(l.entries)) - 1
}

func (l *RaftLog) LastTerm() uint64 {
	return l.entries[len(l.entries)-1].Term
}

func (l *RaftLog) Term(i uint64) uint64 {
	if i < l.offset || i > l.LastIndex() {
		return 0
	}
	return l.entries[i-l.offset].Term
}

func (l *RaftLog) Entry(i uint64) raftpb.Entry {
	return l.entries[i-l.offset]
}

func (l *RaftLog) Slice(lo uint64, hi uint64) []raftpb.Entry {
	return l.entries[lo-l.offset : hi-l.offset]
}

func (r *Raft) preLogIndex(id uint64) uint64 {
	return r.prs.prs[id].Next - 1
}

func (r *Raft) preLogTerm(id uint64) uint64 {
	return r.raftLog.Term(r.preLogIndex(id))
}

func (r *Raft) NewRaftLog() *RaftLog {
	return &RaftLog{
		entries:      []raftpb.Entry{{Term: 0, Index: 0}},
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

func (r *Raft) commitTo(to uint64) {
	if to > r.hardState.CommitIndex {
		r.hardState.CommitIndex = to
		r.storage.SaveHardState(r.hardState)
	}
}

func (r *Raft) compactTo(to uint64, term uint64) {
	if to <= r.raftLog.offset {
		return
	}

	last := r.raftLog.LastIndex()
	if to > last {
		to = last
	}

	sentinel := raftpb.Entry{
		Index: to,
		Term:  term,
	}
	rest := r.raftLog.Slice(to+1, last+1)

	newEntries := []raftpb.Entry{}
	newEntries = append(newEntries, sentinel)
	newEntries = append(newEntries, rest...)

	r.raftLog.entries = newEntries
	r.raftLog.offset = to
}

func (r *Raft) restore(snap raftpb.Snapshot) {
	r.raftLog.offset = snap.Index
	r.raftLog.entries = []raftpb.Entry{{Term: snap.Term, Index: snap.Index}}
}

// matchCommitTerm 检查指定索引的日志条目的任期是否匹配
func (l *RaftLog) matchCommitTerm(index uint64, term uint64) bool {
	if index < uint64(len(l.entries)) && l.entries[index].Term == term {
		return true
	}
	return false
}
