package raft

import "github.com/zbchi/linkv/proto/raftpb"

// RaftLog 管理 Raft 日志
type RaftLog struct {
	// entries[0] 是哨兵条目，entries[i] 对应索引 offset+i
	entries []raftpb.Entry

	// offset 是 entries[0] 的索引（快照后会增加）
	offset uint64

	// appliedIndex 是已应用到状态机的最高索引
	appliedIndex uint64
}

// NewRaftLog 创建一个新的 RaftLog
func NewRaftLog() *RaftLog {
	return &RaftLog{
		entries:      []raftpb.Entry{{Term: 0, Index: 0}}, // 哨兵
		offset:       0,
		appliedIndex: 0,
	}
}

// NewRaftLogFromSnapshot 从快照创建 RaftLog
func NewRaftLogFromSnapshot(snapIndex, snapTerm uint64) *RaftLog {
	return &RaftLog{
		entries:      []raftpb.Entry{{Term: snapTerm, Index: snapIndex}},
		offset:       snapIndex,
		appliedIndex: snapIndex,
	}
}

// FirstIndex 返回第一个日志索引（哨兵索引）
func (l *RaftLog) FirstIndex() uint64 {
	return l.offset
}

// LastIndex 返回最后一个日志索引
func (l *RaftLog) LastIndex() uint64 {
	return l.offset + uint64(len(l.entries)) - 1
}

// LastTerm 返回最后一个日志的任期
func (l *RaftLog) LastTerm() uint64 {
	if len(l.entries) == 0 {
		return 0
	}
	return l.entries[len(l.entries)-1].Term
}

// Term 返回指定索引的任期
func (l *RaftLog) Term(i uint64) uint64 {
	if i < l.offset || i > l.LastIndex() {
		return 0
	}
	return l.entries[i-l.offset].Term
}

// Entry 返回指定索引的日志条目
func (l *RaftLog) Entry(i uint64) raftpb.Entry {
	return l.entries[i-l.offset]
}

// Slice 返回 [lo, hi) 范围的日志条目
func (l *RaftLog) Slice(lo, hi uint64) []raftpb.Entry {
	if lo < l.offset {
		lo = l.offset
	}
	if hi > l.LastIndex()+1 {
		hi = l.LastIndex() + 1
	}
	if lo >= hi {
		return nil
	}
	return l.entries[lo-l.offset : hi-l.offset]
}

// Append 追加日志条目
func (l *RaftLog) Append(entries ...raftpb.Entry) {
	l.entries = append(l.entries, entries...)
}

// TruncateAndAppend 截断并追加日志（处理冲突）
// prevIndex: 新条目的前一个索引
func (l *RaftLog) TruncateAndAppend(prevIndex uint64, entries []raftpb.Entry) {
	pos := prevIndex - l.offset + 1
	l.entries = append(l.entries[:pos], entries...)
}

// Truncate 截断指定索引之后的所有日志
func (l *RaftLog) Truncate(index uint64) {
	if index < l.offset {
		return
	}
	cut := index - l.offset
	l.entries = l.entries[:cut]
}

// MatchTerm 检查指定索引的任期是否匹配
func (l *RaftLog) MatchTerm(index, term uint64) bool {
	if index < l.offset || index > l.LastIndex() {
		return false
	}
	return l.Term(index) == term
}

// FindLastIndexOfTerm 查找指定任期的最后一个索引
func (l *RaftLog) FindLastIndexOfTerm(term uint64) (uint64, bool) {
	for i := l.LastIndex(); i >= l.FirstIndex(); i-- {
		if l.Term(i) == term {
			return i, true
		}
		if i == l.FirstIndex() {
			break
		}
	}
	return 0, false
}

// CompactTo 压缩日志到指定索引
func (l *RaftLog) CompactTo(index, term uint64) {
	if index <= l.offset {
		return
	}

	last := l.LastIndex()
	if index > last {
		index = last
	}

	// 创建新的哨兵
	sentinel := raftpb.Entry{
		Index: index,
		Term:  term,
	}

	// 保留 index 之后的日志
	var rest []raftpb.Entry
	if index < last {
		rest = l.Slice(index+1, last+1)
	}

	l.entries = append([]raftpb.Entry{sentinel}, rest...)
	l.offset = index
}

// Restore 从快照恢复日志状态
func (l *RaftLog) Restore(snapIndex, snapTerm uint64) {
	l.offset = snapIndex
	l.entries = []raftpb.Entry{{Term: snapTerm, Index: snapIndex}}
	l.appliedIndex = snapIndex
}

// AppliedIndex 返回已应用索引
func (l *RaftLog) AppliedIndex() uint64 {
	return l.appliedIndex
}

// SetAppliedIndex 设置已应用索引
func (l *RaftLog) SetAppliedIndex(index uint64) {
	l.appliedIndex = index
}

