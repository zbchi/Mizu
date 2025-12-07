package raft

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/zbchi/linkv/proto/raftpb"
)

// ============================================================
// 测试辅助函数
// ============================================================

// newRaft 创建测试用的 Raft 实例（与 node_test.go 的 newTestRaft 区分）
func newRaft(id uint64, peers []uint64) *Raft {
	return NewRaft(Config{
		ID:               id,
		Peers:            peers,
		ElectionTimeout:  10,
		HeartbeatTimeout: 1,
	})
}

// readMessages 读取并清空消息队列
func (r *Raft) readMessages() []raftpb.Message {
	msgs := r.msgs
	r.msgs = nil
	return msgs
}

// ============================================================
// 选举测试
// ============================================================

// TestLeaderElection 测试领导者选举
func TestLeaderElection(t *testing.T) {
	tests := []struct {
		name  string
		peers []uint64
	}{
		{"single node", []uint64{1}},
		{"three nodes", []uint64{1, 2, 3}},
		{"five nodes", []uint64{1, 2, 3, 4, 5}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := newRaft(1, tt.peers)

			// 发起选举
			r.campaign()

			if len(tt.peers) == 1 {
				// 单节点集群应该直接成为 Leader
				assert.Equal(t, StateLeader, r.State())
				assert.Equal(t, uint64(1), r.Lead())
			} else {
				// 多节点集群应该成为 Candidate 并发送投票请求
				assert.Equal(t, StateCandidate, r.State())
				msgs := r.readMessages()
				voteMsgs := filterMessageType(msgs, raftpb.Type_MsgVote)
				assert.Equal(t, len(tt.peers)-1, len(voteMsgs))
			}
		})
	}
}

// TestLeaderElectionWithVotes 测试通过投票赢得选举
func TestLeaderElectionWithVotes(t *testing.T) {
	r := newRaft(1, []uint64{1, 2, 3})

	r.campaign()
	require.Equal(t, StateCandidate, r.State())

	// 收到足够的投票（quorum = 3/2 + 1 = 2）
	r.Step(raftpb.Message{
		Type:   raftpb.Type_MsgVoteResp,
		From:   2,
		To:     1,
		Term:   1,
		Reject: false,
	})

	// 有两票（包括自己），达到 quorum，应该成为 Leader
	assert.Equal(t, StateLeader, r.State())
	assert.Equal(t, uint64(1), r.Lead())
}

// TestVoteRequest 测试投票请求处理
func TestVoteRequest(t *testing.T) {
	tests := []struct {
		name        string
		existingLog []raftpb.Entry
		vote        uint64
		msg         raftpb.Message
		expectGrant bool
		expectVote  uint64
	}{
		{
			name: "grant vote to candidate with up-to-date log",
			msg: raftpb.Message{
				Type:     raftpb.Type_MsgVote,
				From:     2,
				To:       1,
				Term:     2,
				LogIndex: 1,
				LogTerm:  1,
			},
			expectGrant: true,
			expectVote:  2,
		},
		{
			name: "grant vote if candidate has higher term",
			existingLog: []raftpb.Entry{
				{Term: 1, Index: 1},
			},
			vote: 2, // 已经投票给了节点 2（任期 1）
			msg: raftpb.Message{
				Type:     raftpb.Type_MsgVote,
				From:     3,
				To:       1,
				Term:     2, // 更高的任期
				LogIndex: 1,
				LogTerm:  1,
			},
			expectGrant: true, // 更高任期应该授予投票
			expectVote:  3,
		},
		{
			name: "reject vote if already voted for different candidate in same term",
			existingLog: []raftpb.Entry{
				{Term: 1, Index: 1},
			},
			vote: 2, // 已经投票给了节点 2
			msg: raftpb.Message{
				Type:     raftpb.Type_MsgVote,
				From:     3,
				To:       1,
				Term:     1, // 相同的任期
				LogIndex: 1,
				LogTerm:  1,
			},
			expectGrant: false, // 相同任期已投票，应拒绝
			expectVote:  2,
		},
		{
			name: "reject vote if candidate log is stale",
			existingLog: []raftpb.Entry{
				{Term: 1, Index: 1},
			},
			msg: raftpb.Message{
				Type:     raftpb.Type_MsgVote,
				From:     2,
				To:       1,
				Term:     2,
				LogIndex: 0,
				LogTerm:  0,
			},
			expectGrant: false,
			expectVote:  0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := newRaft(1, []uint64{1, 2, 3})
			r.hardState.Term = 1
			r.hardState.Vote = tt.vote

			for _, e := range tt.existingLog {
				r.raftLog.Append(e)
			}

			err := r.Step(tt.msg)
			require.NoError(t, err)

			msgs := r.readMessages()
			require.Len(t, msgs, 1)

			resp := msgs[0]
			assert.Equal(t, raftpb.Type_MsgVoteResp, resp.Type)
			assert.Equal(t, tt.expectGrant, !resp.Reject)

			if tt.expectGrant {
				assert.Equal(t, tt.expectVote, r.hardState.Vote)
			}
		})
	}
}

// TestLeaderStepDown 测试 Leader 遇到更高任期时下台
func TestLeaderStepDown(t *testing.T) {
	tests := []struct {
		name      string
		state     StateType
		msgType   raftpb.Type
		expectNew bool
	}{
		{"follower becomes follower on higher term", StateFollower, raftpb.Type_MsgApp, false},
		{"candidate becomes follower on higher term", StateCandidate, raftpb.Type_MsgVote, false},
		{"leader steps down on higher term", StateLeader, raftpb.Type_MsgApp, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := newRaft(1, []uint64{1, 2, 3})
			r.state = tt.state
			r.hardState.Term = 2
			if tt.state == StateLeader {
				r.lead = 1
			}

			msg := raftpb.Message{
				Type: tt.msgType,
				From: 2,
				To:   1,
				Term: 3, // 更高的任期
			}

			err := r.Step(msg)
			require.NoError(t, err)

			assert.Equal(t, StateFollower, r.State())
			assert.Equal(t, uint64(3), r.Term())

			// MsgApp 会设置 leader 为消息发送者
			// MsgVote 不会设置 leader
			if tt.msgType == raftpb.Type_MsgApp {
				assert.Equal(t, uint64(2), r.Lead())
			} else {
				assert.Equal(t, uint64(0), r.Lead())
			}
		})
	}
}

// ============================================================
// 日志复制测试
// ============================================================

// TestLeaderPropose 测试 Leader 提议日志
func TestLeaderPropose(t *testing.T) {
	r := newRaft(1, []uint64{1, 2, 3})
	r.state = StateLeader
	r.lead = 1
	r.hardState.Term = 1

	data := []byte("test data")
	ok := r.Propose(data)
	require.True(t, ok)

	// 验证日志被追加
	assert.Equal(t, uint64(1), r.raftLog.LastIndex())

	// 验证有待持久化的日志
	rd := r.Ready()
	assert.Len(t, rd.Entries, 1)
	assert.Equal(t, data, rd.Entries[0].Data)

	// 验证发送了 AppendEntries 消息
	appMsgs := filterMessageType(rd.Messages, raftpb.Type_MsgApp)
	assert.Len(t, appMsgs, 2) // 发给节点 2 和 3
}

// TestFollowerAppendEntries 测试 Follower 接收日志
func TestFollowerAppendEntries(t *testing.T) {
	tests := []struct {
		name         string
		existingLog  []raftpb.Entry
		msg          raftpb.Message
		expectReject bool
		expectIndex  uint64
		expectCommit uint64
	}{
		{
			name: "successful append",
			msg: raftpb.Message{
				Type:     raftpb.Type_MsgApp,
				From:     2,
				To:       1,
				Term:     1,
				LogIndex: 0,
				LogTerm:  0,
				Entries: []*raftpb.Entry{
					{Term: 1, Index: 1, Data: []byte("entry1")},
					{Term: 1, Index: 2, Data: []byte("entry2")},
				},
				Commit: 1,
			},
			expectReject: false,
			expectIndex:  2,
			expectCommit: 1,
		},
		{
			name: "prev term mismatch",
			existingLog: []raftpb.Entry{
				{Term: 1, Index: 1},
			},
			msg: raftpb.Message{
				Type:     raftpb.Type_MsgApp,
				From:     2,
				To:       1,
				Term:     1,
				LogIndex: 1,
				LogTerm:  2, // 任期不匹配
				Entries:  []*raftpb.Entry{{Term: 2, Index: 2}},
			},
			expectReject: true,
			expectIndex:  1,
		},
		{
			name: "prev index beyond log",
			msg: raftpb.Message{
				Type:     raftpb.Type_MsgApp,
				From:     2,
				To:       1,
				Term:     1,
				LogIndex: 10, // 超出日志范围
				LogTerm:  1,
			},
			expectReject: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := newRaft(1, []uint64{1, 2})
			r.hardState.Term = 1

			for _, e := range tt.existingLog {
				r.raftLog.Append(e)
			}

			err := r.Step(tt.msg)
			require.NoError(t, err)

			msgs := r.readMessages()
			require.Len(t, msgs, 1)

			resp := msgs[0]
			assert.Equal(t, raftpb.Type_MsgAppResp, resp.Type)
			assert.Equal(t, tt.expectReject, resp.Reject)

			if !tt.expectReject {
				assert.Equal(t, tt.expectIndex, r.raftLog.LastIndex())
				assert.Equal(t, tt.expectCommit, r.CommitIndex())
			}
		})
	}
}

// TestCommit 测试日志提交
func TestCommit(t *testing.T) {
	tests := []struct {
		name    string
		peers   []uint64
		matches []uint64
		logTerm uint64
		term    uint64
		expect  uint64
	}{
		{
			name:    "single node commits its own entry",
			peers:   []uint64{1},
			matches: []uint64{1},
			logTerm: 1,
			term:    1,
			expect:  1,
		},
		{
			name:    "majority commits entry",
			peers:   []uint64{1, 2, 3},
			matches: []uint64{1, 1, 0},
			logTerm: 1,
			term:    1,
			expect:  1,
		},
		{
			name:    "cannot commit old term entry",
			peers:   []uint64{1, 2, 3},
			matches: []uint64{1, 1, 0},
			logTerm: 1,
			term:    2,
			expect:  0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := newRaft(1, tt.peers)
			r.state = StateLeader
			r.hardState.Term = tt.term
			r.lead = 1

			// 添加日志
			r.raftLog.Append(raftpb.Entry{Term: tt.logTerm, Index: 1})

			// 设置每个节点的 Match
			for i, match := range tt.matches {
				r.prs[tt.peers[i]].Match = match
			}

			r.maybeCommit()

			assert.Equal(t, tt.expect, r.CommitIndex())
		})
	}
}

// ============================================================
// 快照测试
// ============================================================

// TestSnapshot 测试创建快照
func TestSnapshot(t *testing.T) {
	r := newRaft(1, []uint64{1})
	r.state = StateLeader
	r.hardState.Term = 1

	// 添加日志
	for i := uint64(1); i <= 5; i++ {
		r.raftLog.Append(raftpb.Entry{Term: 1, Index: i, Data: []byte{byte(i)}})
	}
	r.hardState.CommitIndex = 5
	r.raftLog.SetAppliedIndex(5)

	snapData := []byte("snapshot data")
	sn := r.Snapshot(3, snapData)

	require.NotNil(t, sn)
	assert.Equal(t, uint64(3), sn.Index)
	assert.Equal(t, uint64(1), sn.Term)
	assert.Equal(t, snapData, sn.Data)

	// 验证日志被压缩
	assert.Equal(t, uint64(3), r.raftLog.FirstIndex())

	// 验证快照被存储在 currentSnapshot
	assert.NotNil(t, r.currentSnapshot)
	assert.Equal(t, sn, r.currentSnapshot)
}

// TestSnapshotCannotCompactBeforeFirst 测试不能压缩到第一个索引之前
func TestSnapshotCannotCompactBeforeFirst(t *testing.T) {
	r := newRaft(1, []uint64{1})
	r.raftLog.Append(raftpb.Entry{Term: 1, Index: 1, Data: []byte("data1")})
	r.raftLog.SetAppliedIndex(1)

	// 尝试压缩到索引 0，应该返回 nil
	sn := r.Snapshot(0, []byte("data"))
	assert.Nil(t, sn)

	// 压缩到索引 1 应该成功
	sn = r.Snapshot(1, []byte("data"))
	assert.NotNil(t, sn)
}

// TestHandleSnapshot 测试接收快照
func TestHandleSnapshot(t *testing.T) {
	tests := []struct {
		name         string
		firstIndex   uint64
		snapIndex    uint64
		expectReject bool
	}{
		{
			name:         "accept snapshot",
			firstIndex:   1,
			snapIndex:    10,
			expectReject: false,
		},
		{
			name:         "reject snapshot if already have newer data",
			firstIndex:   20,
			snapIndex:    10,
			expectReject: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := newRaft(1, []uint64{1, 2})
			r.hardState.Term = 1
			r.raftLog.offset = tt.firstIndex

			snap := &raftpb.Snapshot{
				Index: tt.snapIndex,
				Term:  2,
				Data:  []byte("snapshot data"),
			}

			msg := raftpb.Message{
				Type:     raftpb.Type_MsgSnap,
				From:     2,
				To:       1,
				Term:     2,
				Snapshot: snap,
			}

			err := r.Step(msg)
			require.NoError(t, err)

			msgs := r.readMessages()
			require.Len(t, msgs, 1)

			resp := msgs[0]
			assert.Equal(t, raftpb.Type_MsgSnapResp, resp.Type)
			assert.Equal(t, tt.expectReject, resp.Reject)

			if !tt.expectReject {
				rd := r.Ready()
				assert.NotNil(t, rd.Snapshot)
				assert.Equal(t, tt.snapIndex, rd.Snapshot.Index)
			}
		})
	}
}

// ============================================================
// Ready 和 Advance 测试
// ============================================================

// TestReadyIsEmpty 测试 Ready 是否为空
func TestReadyIsEmpty(t *testing.T) {
	r := newRaft(1, []uint64{1, 2, 3})

	rd := r.Ready()
	assert.True(t, rd.IsEmpty(), "initial Ready should be empty")
}

// TestReadyContainsEntries 测试 Ready 包含日志
func TestReadyContainsEntries(t *testing.T) {
	r := newRaft(1, []uint64{1, 2, 3})
	r.state = StateLeader
	r.lead = 1
	r.hardState.Term = 1

	r.Propose([]byte("test data"))

	rd := r.Ready()
	assert.False(t, rd.IsEmpty())
	assert.Len(t, rd.Entries, 1)
	// HardState 只有在状态改变时才非空
	// rd.HardState 可能是 nil
	assert.Len(t, rd.Messages, 2) // 发给节点 2 和 3
}

// TestAdvanceClearsReady 测试 Advance 清空 Ready
func TestAdvanceClearsReady(t *testing.T) {
	r := newRaft(1, []uint64{1, 2, 3})
	r.state = StateLeader
	r.lead = 1
	r.hardState.Term = 1

	r.Propose([]byte("test data"))

	// 第一次 Ready 应该有数据
	rd1 := r.Ready()
	assert.False(t, rd1.IsEmpty())

	r.Advance()

	// 第二次 Ready 应该为空
	rd2 := r.Ready()
	assert.True(t, rd2.IsEmpty())
}

// ============================================================
// 边界情况测试
// ============================================================

// TestProposeWhenNotLeader 测试非 Leader 不能提议
func TestProposeWhenNotLeader(t *testing.T) {
	tests := []struct {
		name  string
		state StateType
	}{
		{"follower cannot propose", StateFollower},
		{"candidate cannot propose", StateCandidate},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := newRaft(1, []uint64{1, 2, 3})
			r.state = tt.state

			ok := r.Propose([]byte("data"))
			assert.False(t, ok, "non-leader should not be able to propose")
		})
	}
}

// TestSingleNodeBecomesLeader 测试单节点直接成为 Leader
func TestSingleNodeBecomesLeader(t *testing.T) {
	r := newRaft(1, []uint64{1})

	r.campaign()

	assert.Equal(t, StateLeader, r.State())
	assert.Equal(t, uint64(1), r.Lead())
	assert.Equal(t, uint64(1), r.Term())
}

// TestIgnoreOldTermMessages 测试忽略旧任期消息
func TestIgnoreOldTermMessages(t *testing.T) {
	r := newRaft(1, []uint64{1, 2})
	r.hardState.Term = 3

	msg := raftpb.Message{
		Type: raftpb.Type_MsgApp,
		From: 2,
		To:   1,
		Term: 2, // 旧任期
	}

	err := r.Step(msg)
	require.NoError(t, err)

	// 任期不应该改变
	assert.Equal(t, uint64(3), r.Term())

	// 应该拒绝
	msgs := r.readMessages()
	require.Len(t, msgs, 1)
	assert.True(t, msgs[0].Reject)
}

// TestRestoreState 测试恢复状态
func TestRestoreState(t *testing.T) {
	r := newRaft(1, []uint64{1, 2})

	hs := HardState{
		Term:        5,
		Vote:        2,
		CommitIndex: 10,
	}

	entries := []raftpb.Entry{
		{Term: 5, Index: 1, Data: []byte("entry1")},
		{Term: 5, Index: 2, Data: []byte("entry2")},
	}

	r.RestoreState(hs, entries)

	assert.Equal(t, uint64(5), r.Term())
	assert.Equal(t, uint64(2), r.hardState.Vote)
	assert.Equal(t, uint64(10), r.CommitIndex())
	assert.Equal(t, uint64(2), r.raftLog.LastIndex())
}

// TestRestoreSnapshot 测试恢复快照
func TestRestoreSnapshot(t *testing.T) {
	r := newRaft(1, []uint64{1, 2})

	snap := raftpb.Snapshot{
		Index: 100,
		Term:  5,
		Data:  []byte("snapshot"),
	}

	r.RestoreSnapshot(snap)

	assert.Equal(t, uint64(100), r.raftLog.FirstIndex())
	assert.Equal(t, uint64(100), r.raftLog.LastIndex())
	assert.Equal(t, uint64(100), r.CommitIndex())
	assert.Equal(t, uint64(5), r.Term())
	assert.NotNil(t, r.currentSnapshot)
}

// TestEmptySnapshot 测试空快照
func TestEmptySnapshot(t *testing.T) {
	r := newRaft(1, []uint64{1, 2})

	snap := raftpb.Snapshot{
		Index: 0, // 空快照
		Term:  0,
	}

	r.RestoreSnapshot(snap)

	// 状态不应该改变
	assert.Equal(t, uint64(0), r.raftLog.FirstIndex())
	assert.Equal(t, uint64(0), r.CommitIndex())
}

// ============================================================
// 辅助函数
// ============================================================

// filterMessageType 按类型过滤消息
func filterMessageType(msgs []raftpb.Message, msgType raftpb.Type) []raftpb.Message {
	var filtered []raftpb.Message
	for _, m := range msgs {
		if m.Type == msgType {
			filtered = append(filtered, m)
		}
	}
	return filtered
}
