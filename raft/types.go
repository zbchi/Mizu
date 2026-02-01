package raft

import "github.com/zbchi/mizu/proto/raftpb"

type StateType int

const (
	StateFollower StateType = iota
	StateCandidate
	StateLeader
)

func (s StateType) String() string {
	switch s {
	case StateFollower:
		return "Follower"
	case StateCandidate:
		return "Candidate"
	case StateLeader:
		return "Leader"
	default:
		return "Unknown"
	}
}

// 需要持久化的 Raft 状态
type HardState struct {
	Term uint64
	Vote uint64
	// CommitIndex is the highest index committed by a Raft quorum.
	CommitIndex uint64
}

// 检查 HardState 是否为空
func (hs HardState) IsEmpty() bool {
	return hs.Term == 0 && hs.Vote == 0 && hs.CommitIndex == 0
}

// Progress表示一个 peer 的日志复制进度
type Progress struct {
	Match uint64
	Next  uint64
}

// Ready is the boundary between Raft and the application. The application
// persists its durable fields, sends Messages, submits committed work to the
// apply worker, publishes ReadStates, and only then calls Advance.
type Ready struct {
	// HardState 需要持久化的状态（term/vote/commit）
	// 如果为空则无需持久化
	HardState *HardState

	//需要持久化到稳定存储的日志条目
	Entries []*raftpb.Entry

	//需要持久化的快照
	Snapshot *raftpb.Snapshot

	//需要应用到状态机的已提交条目
	CommittedEntries []*raftpb.Entry

	//需要发送给其他节点的消息
	Messages []*raftpb.Message

	// ReadStates contains ReadIndex results confirmed by a quorum.
	// They are volatile and do not need to be persisted.
	ReadStates []ReadState
}

// ReadState is a quorum-confirmed index for one linearizable read.
type ReadState struct {
	ReadID uint64
	Index  uint64
}

func (rd Ready) IsEmpty() bool {
	return rd.HardState == nil &&
		len(rd.Entries) == 0 &&
		rd.Snapshot == nil &&
		len(rd.CommittedEntries) == 0 &&
		len(rd.Messages) == 0 &&
		len(rd.ReadStates) == 0
}
