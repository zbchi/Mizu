package raft

import "github.com/zbchi/linkv/proto/raftpb"

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
	Term        uint64
	Vote        uint64
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

// Ready - Raft 与应用层的接口
// Ready 封装了需要处理的 Raft 状态更新
// 应用层必须按顺序处理：
//  1. 持久化 HardState
//  2. 持久化 Entries
//  3. 持久化/应用 Snapshot
//  4. 发送 Messages
//  6. 调用 Advance()
type Ready struct {
	//  5. 应用 CommittedEntries 到状态机
	// HardState 需要持久化的状态（term/vote/commit）
	// 如果为空则无需持久化
	HardState *HardState

	//需要持久化到稳定存储的日志条目
	Entries []raftpb.Entry

	//需要持久化的快照
	Snapshot *raftpb.Snapshot

	//需要应用到状态机的已提交条目
	CommittedEntries []raftpb.Entry

	//需要发送给其他节点的消息
	Messages []raftpb.Message
}

func (rd Ready) IsEmpty() bool {
	return rd.HardState == nil &&
		len(rd.Entries) == 0 &&
		rd.Snapshot == nil &&
		len(rd.CommittedEntries) == 0 &&
		len(rd.Messages) == 0
}

// 快照请求（用于 Node 层通信）
type SnapshotRequest struct {
	Index   uint64
	Data    []byte
	ResultC chan *raftpb.Snapshot
}
