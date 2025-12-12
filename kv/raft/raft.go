package raft

import (
	"log/slog"
	"math/rand"
	"sort"

	"github.com/zbchi/linkv/proto/raftpb"
)

type Raft struct {
	id    uint64
	state StateType
	lead  uint64

	hardState HardState
	raftLog   *RaftLog

	prs   map[uint64]*Progress
	votes map[uint64]bool

	msgs []*raftpb.Message

	electionElapsed  int
	heartbeatElapsed int
	electionTimeout  int
	heartbeatTimeout int

	// 当前快照（用于发送给落后的 Follower）
	currentSnapshot *raftpb.Snapshot

	// Ready 相关
	pendingHardState *HardState
	pendingEntries   []*raftpb.Entry
	pendingSnapshot  *raftpb.Snapshot
	committedEntries []*raftpb.Entry
}

type Config struct {
	ID               uint64
	Peers            []uint64
	ElectionTimeout  int
	HeartbeatTimeout int
}

func NewRaft(cfg Config) *Raft {
	if cfg.ElectionTimeout == 0 {
		cfg.ElectionTimeout = 10
	}
	if cfg.HeartbeatTimeout == 0 {
		cfg.HeartbeatTimeout = 3
	}

	prs := make(map[uint64]*Progress)
	for _, peer := range cfg.Peers {
		prs[peer] = &Progress{Match: 0, Next: 1}
	}

	return &Raft{
		id:               cfg.ID,
		state:            StateFollower,
		prs:              prs,
		votes:            make(map[uint64]bool),
		raftLog:          NewRaftLog(),
		electionTimeout:  cfg.ElectionTimeout,
		heartbeatTimeout: cfg.HeartbeatTimeout,
	}
}

func (r *Raft) RestoreState(hs HardState, entries []*raftpb.Entry) {
	r.hardState = hs
	for _, e := range entries {
		r.raftLog.Append(e)
	}
	// Apply committed entries to generate them in the next Ready
	r.applyCommitted()
}

func (r *Raft) RestoreSnapshot(snap *raftpb.Snapshot) {
	if snap.Index == 0 {
		return
	}
	r.raftLog.Restore(snap.Index, snap.Term)
	r.hardState.CommitIndex = max(snap.Index, r.hardState.CommitIndex)
	if snap.Term > r.hardState.Term {
		r.hardState.Term = snap.Term
	}
	// 保存快照供 sendSnapshot 使用
	r.currentSnapshot = snap
}

func (r *Raft) Tick() {
	switch r.state {
	case StateLeader:
		r.heartbeatElapsed++
		if r.heartbeatElapsed >= r.heartbeatTimeout {
			r.heartbeatElapsed = 0
			r.bcastAppend()
		}
	default:
		r.electionElapsed++
		if r.electionElapsed >= r.electionTimeout+rand.Intn(r.electionTimeout) {
			r.electionElapsed = 0
			r.campaign()
		}
	}
}

func (r *Raft) Step(m *raftpb.Message) error {
	if m.Term > r.hardState.Term {
		r.becomeFollower(m.Term, 0)
	}

	switch r.state {
	case StateFollower:
		return r.stepFollower(m)
	case StateCandidate:
		return r.stepCandidate(m)
	case StateLeader:
		return r.stepLeader(m)
	}
	return nil
}

func (r *Raft) Propose(data []byte) bool {
	if r.state != StateLeader {
		return false
	}

	index := r.raftLog.LastIndex() + 1
	entry := &raftpb.Entry{
		Term:  r.hardState.Term,
		Index: index,
		Data:  data,
	}

	r.raftLog.Append(entry)
	r.pendingEntries = append(r.pendingEntries, entry)

	r.prs[r.id].Match = index
	r.prs[r.id].Next = index + 1

	r.bcastAppend()
	r.maybeCommit()
	return true
}

func (r *Raft) Ready() Ready {
	rd := Ready{
		HardState:        r.pendingHardState,
		Entries:          r.pendingEntries,
		Snapshot:         r.pendingSnapshot,
		CommittedEntries: r.committedEntries,
		Messages:         r.msgs,
	}
	return rd
}

func (r *Raft) Advance() {
	r.pendingHardState = nil
	r.pendingEntries = nil
	r.pendingSnapshot = nil
	r.committedEntries = nil
	r.msgs = nil
}

func (r *Raft) Snapshot(index uint64, data []byte) *raftpb.Snapshot {
	if index <= r.raftLog.FirstIndex() {
		return nil
	}

	term := r.raftLog.Term(index)
	r.raftLog.CompactTo(index, term)
	r.hardState.CommitIndex = max(index, r.hardState.CommitIndex)
	r.raftLog.SetAppliedIndex(max(index, r.raftLog.AppliedIndex()))
	r.markHardStateChanged()

	// 更新 Leader 的 progress
	if r.state == StateLeader {
		for _, pr := range r.prs {
			if pr.Next <= r.raftLog.FirstIndex() {
				pr.Next = r.raftLog.FirstIndex() + 1
			}
			if pr.Match < r.raftLog.FirstIndex() {
				pr.Match = r.raftLog.FirstIndex()
			}
		}
	}

	sn := &raftpb.Snapshot{
		Term:  term,
		Index: index,
		Data:  data,
	}

	//保存快照供 sendSnapshot 使用
	r.currentSnapshot = sn
	return sn
}

func (r *Raft) State() StateType    { return r.state }
func (r *Raft) Term() uint64        { return r.hardState.Term }
func (r *Raft) Vote() uint64        { return r.hardState.Vote }
func (r *Raft) Lead() uint64        { return r.lead }
func (r *Raft) ID() uint64          { return r.id }
func (r *Raft) CommitIndex() uint64 { return r.hardState.CommitIndex }

func (r *Raft) becomeFollower(term, lead uint64) {
	r.state = StateFollower
	r.lead = lead

	if term > r.hardState.Term {
		r.hardState.Term = term
		r.hardState.Vote = 0
		r.markHardStateChanged()
	}

	r.votes = make(map[uint64]bool)
	r.electionElapsed = 0
}

func (r *Raft) becomeCandidate() {
	r.state = StateCandidate
	r.lead = 0
	r.hardState.Term++
	r.hardState.Vote = r.id
	r.markHardStateChanged()
	r.votes = map[uint64]bool{r.id: true}
	r.electionElapsed = 0
}

func (r *Raft) becomeLeader() {
	slog.Info("become leader",
		"term", r.hardState.Term,
		"commit", r.hardState.CommitIndex,
		"id", r.id)

	r.state = StateLeader
	r.lead = r.id
	r.heartbeatElapsed = 0

	for _, pr := range r.prs {
		pr.Match = 0
		pr.Next = r.raftLog.LastIndex() + 1
	}

	r.bcastAppend()
}

func (r *Raft) stepFollower(m *raftpb.Message) error {
	switch m.Type {
	case raftpb.Type_MsgVote:
		r.handleVote(m)
	case raftpb.Type_MsgApp:
		r.handleAppend(m)
	case raftpb.Type_MsgSnap:
		r.handleSnapshot(m)
	}
	return nil
}

func (r *Raft) stepCandidate(m *raftpb.Message) error {
	switch m.Type {
	case raftpb.Type_MsgVoteResp:
		r.handleVoteResp(m)
	case raftpb.Type_MsgVote:
		r.handleVote(m)
	case raftpb.Type_MsgApp:
		r.handleAppend(m)
	case raftpb.Type_MsgSnap:
		r.handleSnapshot(m)
	}
	return nil
}

func (r *Raft) stepLeader(m *raftpb.Message) error {
	switch m.Type {
	case raftpb.Type_MsgAppResp:
		r.handleAppendResp(m)
	case raftpb.Type_MsgSnapResp:
		r.handleSnapshotResp(m)
	}
	return nil
}

func (r *Raft) campaign() {
	r.becomeCandidate()
	slog.Info("start campaign", "term", r.hardState.Term, "id", r.id)

	// 单节点直接成为 Leader
	if len(r.prs) == 1 {
		r.becomeLeader()
		return
	}

	for id := range r.prs {
		if id == r.id {
			continue
		}
		r.send(&raftpb.Message{
			Type:     raftpb.Type_MsgVote,
			From:     r.id,
			To:       id,
			Term:     r.hardState.Term,
			LogIndex: r.raftLog.LastIndex(),
			LogTerm:  r.raftLog.LastTerm(),
		})
	}
}

func (r *Raft) handleVote(m *raftpb.Message) {
	granted := r.canVote(m)

	if granted {
		r.hardState.Vote = m.From
		r.markHardStateChanged()
		r.electionElapsed = 0
	}

	r.send(&raftpb.Message{
		Type:   raftpb.Type_MsgVoteResp,
		From:   r.id,
		To:     m.From,
		Term:   r.hardState.Term,
		Reject: !granted,
	})
}

func (r *Raft) canVote(m *raftpb.Message) bool {
	if r.hardState.Term > m.Term {
		return false
	}
	if r.hardState.Vote != 0 && r.hardState.Vote != m.From {
		return false
	}

	// 检查日志是否足够新
	lastTerm := r.raftLog.LastTerm()
	lastIndex := r.raftLog.LastIndex()
	if lastTerm > m.LogTerm {
		return false
	}
	if lastTerm == m.LogTerm && lastIndex > m.LogIndex {
		return false
	}

	return true
}

func (r *Raft) handleVoteResp(m *raftpb.Message) {
	r.votes[m.From] = !m.Reject
	slog.Info("got vote", "from", m.From, "granted", !m.Reject, "id", r.id)

	granted, rejected := 0, 0
	for _, v := range r.votes {
		if v {
			granted++
		} else {
			rejected++
		}
	}

	quorum := len(r.prs)/2 + 1
	if granted >= quorum {
		r.becomeLeader()
	} else if rejected >= quorum {
		r.becomeFollower(r.hardState.Term, 0)
	}
}

func (r *Raft) bcastAppend() {
	for id := range r.prs {
		if id == r.id {
			continue
		}
		r.sendAppend(id)
	}
}

func (r *Raft) sendAppend(to uint64) {
	pr := r.prs[to]

	// 需要发送快照
	if pr.Next <= r.raftLog.FirstIndex() {
		r.sendSnapshot(to)
		return
	}

	prevIndex := pr.Next - 1
	prevTerm := r.raftLog.Term(prevIndex)
	entries := r.raftLog.Slice(pr.Next, r.raftLog.LastIndex()+1)

	r.send(&raftpb.Message{
		Type:     raftpb.Type_MsgApp,
		From:     r.id,
		To:       to,
		Term:     r.hardState.Term,
		LogIndex: prevIndex,
		LogTerm:  prevTerm,
		Entries:  entries,
		Commit:   r.hardState.CommitIndex,
	})
}

func (r *Raft) sendSnapshot(to uint64) {
	if r.currentSnapshot == nil || r.currentSnapshot.Index == 0 {
		slog.Warn("no snapshot available to send", "to", to, "id", r.id)
		return
	}

	r.send(&raftpb.Message{
		Type:     raftpb.Type_MsgSnap,
		From:     r.id,
		To:       to,
		Term:     r.hardState.Term,
		Snapshot: r.currentSnapshot,
	})
}

func (r *Raft) handleAppend(m *raftpb.Message) {
	reply := &raftpb.Message{
		Type: raftpb.Type_MsgAppResp,
		From: r.id,
		To:   m.From,
		Term: r.hardState.Term,
	}

	if r.hardState.Term > m.Term {
		reply.Reject = true
		r.send(reply)
		return
	}

	r.becomeFollower(m.Term, m.From)

	// prevIndex 超出日志范围
	if m.LogIndex > r.raftLog.LastIndex() {
		reply.Reject = true
		reply.LogIndex = r.raftLog.LastIndex() + 1
		r.send(reply)
		return
	}

	// prevTerm 不匹配
	if m.LogTerm != r.raftLog.Term(m.LogIndex) {
		reply.Reject = true
		reply.LogTerm = r.raftLog.Term(m.LogIndex)

		// 找到冲突任期的第一个索引
		first := m.LogIndex
		for first > r.raftLog.FirstIndex() && r.raftLog.Term(first-1) == reply.LogTerm {
			first--
		}
		reply.LogIndex = first

		// 截断冲突日志
		r.raftLog.Truncate(first)

		r.send(reply)
		return
	}

	// 追加日志
	if len(m.Entries) > 0 {
		r.raftLog.TruncateAndAppend(m.LogIndex, m.Entries)
		r.pendingEntries = append(r.pendingEntries, m.Entries...)
	}

	// 更新 commit
	if m.Commit > r.hardState.CommitIndex {
		r.commitTo(min(m.Commit, r.raftLog.LastIndex()))
	}

	reply.Reject = false
	reply.LogIndex = r.raftLog.LastIndex()
	r.send(reply)
}

func (r *Raft) handleAppendResp(m *raftpb.Message) {
	pr := r.prs[m.From]
	if pr == nil {
		return
	}

	if !m.Reject {
		pr.Match = m.LogIndex
		pr.Next = min(m.LogIndex+1, r.raftLog.LastIndex()+1)
	} else {
		if m.LogTerm == 0 {
			pr.Next = max(m.LogIndex, 1)
		} else {
			if found, ok := r.raftLog.FindLastIndexOfTerm(m.LogTerm); ok {
				pr.Next = found + 1
			} else {
				pr.Next = m.LogIndex
			}
		}
	}

	if pr.Match >= pr.Next {
		pr.Match = pr.Next - 1
	}

	r.maybeCommit()
}

func (r *Raft) handleSnapshot(m *raftpb.Message) {
	reply := &raftpb.Message{
		Type: raftpb.Type_MsgSnapResp,
		From: r.id,
		To:   m.From,
		Term: r.hardState.Term,
	}

	if r.hardState.Term > m.Term || m.Snapshot == nil {
		reply.Reject = true
		r.send(reply)
		return
	}

	if r.raftLog.FirstIndex() >= m.Snapshot.Index {
		reply.Reject = true
		r.send(reply)
		return
	}

	r.becomeFollower(m.Term, m.From)

	// 设置待处理的快照
	r.pendingSnapshot = m.Snapshot
	r.RestoreSnapshot(m.Snapshot)

	reply.Reject = false
	r.send(reply)
}

func (r *Raft) handleSnapshotResp(m *raftpb.Message) {
	if m.Reject {
		return
	}
	pr := r.prs[m.From]
	if pr != nil {
		pr.Next = r.raftLog.FirstIndex() + 1
		pr.Match = r.raftLog.FirstIndex()
	}
}

func (r *Raft) send(m *raftpb.Message) {
	r.msgs = append(r.msgs, m)
}

func (r *Raft) markHardStateChanged() {
	hs := r.hardState
	r.pendingHardState = &hs
}

func (r *Raft) commitTo(index uint64) {
	if index > r.hardState.CommitIndex {
		r.hardState.CommitIndex = index
		r.markHardStateChanged()
		r.applyCommitted()
	}
}

func (r *Raft) applyCommitted() {
	for r.raftLog.AppliedIndex() < r.hardState.CommitIndex {
		idx := r.raftLog.AppliedIndex() + 1
		r.raftLog.SetAppliedIndex(idx)

		if idx < r.raftLog.FirstIndex() || idx > r.raftLog.LastIndex() {
			continue
		}
		entry := r.raftLog.Entry(idx)
		r.committedEntries = append(r.committedEntries, entry)
	}
}

func (r *Raft) maybeCommit() {
	matches := make([]uint64, 0, len(r.prs))
	for _, pr := range r.prs {
		matches = append(matches, pr.Match)
	}
	sort.Slice(matches, func(i, j int) bool {
		return matches[i] > matches[j]
	})

	quorum := len(r.prs)/2 + 1
	mci := matches[quorum-1]

	// 只提交当前任期的日志
	if r.raftLog.MatchTerm(mci, r.hardState.Term) {
		r.commitTo(mci)
	}
}
