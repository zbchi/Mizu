package raft

import (
	"log/slog"
	"math/rand"

	"github.com/zbchi/linkv/proto/raftpb"
)

type Raft struct {
	id    uint64
	state StateType

	hardState HardState
	raftLog   *RaftLog

	prs  *ProgressTracker
	msgs []raftpb.Message

	lead uint64

	electionElapsed  int
	heartbeatElapsed int
	electionTimeout  int
	heartbeatTimeout int

	votes map[uint64]bool

	storage RaftStroage
}

func (r *Raft) Tick() {
	switch r.state {
	case StateLeader:
		r.heartbeatElapsed++
		if r.heartbeatElapsed >= r.heartbeatTimeout {
			r.heartbeatElapsed = 0
			r.bcastHeartBeat()
		}
	default:
		r.electionElapsed++
		if r.electionElapsed >= r.electionTimeout+rand.Intn(r.electionTimeout) {
			r.electionElapsed = 0
			r.Step(raftpb.Message{Type: raftpb.Type_MsgHup, From: r.id, To: r.id})
		}
	}
}

func (r *Raft) Step(m raftpb.Message) error {
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

func (r *Raft) stepFollower(m raftpb.Message) error {
	switch m.Type {
	case raftpb.Type_MsgHup:
		return r.campaign()
	case raftpb.Type_MsgVote:
		return r.vote(m)
	case raftpb.Type_MsgApp:
		return r.handleAppend(m)
	}
	return nil
}

func (r *Raft) stepCandidate(m raftpb.Message) error {
	switch m.Type {
	case raftpb.Type_MsgHup:
		return r.campaign()
	case raftpb.Type_MsgVoteResp:
		return r.handleVoteResp(m)
	case raftpb.Type_MsgVote:
		return r.vote(m)
	case raftpb.Type_MsgApp:
		return r.handleAppend(m)
	}
	return nil
}

func (r *Raft) stepLeader(m raftpb.Message) error {
	switch m.Type {
	case raftpb.Type_MsgAppResp:
		return r.handleAppResp(m)
	case raftpb.Type_MsgSnapResp:
		return r.handleSnapshotResp(m)
	}
	return nil
}

func (r *Raft) handleVoteResp(m raftpb.Message) error {
	r.votes[m.From] = !m.Reject
	slog.Info("got vote", slog.Bool("isVote", !m.Reject),
		slog.Int("from", int(m.From)),
		slog.Int("fromTerm", int(m.Term)), slog.Int("id", int(r.id)))
	won, lost := r.checkVotes()
	if won {
		r.becomeLeader()
	}
	if lost {
		r.becomeFollower(r.hardState.Term, 0)
	}
	return nil
}

func (r *Raft) handleAppResp(m raftpb.Message) error {
	if !m.Reject { //不冲突
		r.prs.prs[m.From].Match = m.LogIndex
		r.prs.prs[m.From].Next = min(m.LogIndex+1, r.raftLog.LastIndex()+1) //slog.Info("updateNext", "next", m.LogIndex+1, "sub", m.From)
	} else { //冲突
		if m.LogTerm == 0 { //无冲突任期
			r.prs.prs[m.From].Next = max(m.LogIndex, 0)
		} else { //有冲突任期
			if found, ok := r.raftLog.findLastIndexOfTerm(m.LogTerm); ok {
				r.prs.prs[m.From].Next = found + 1
			} else {
				r.prs.prs[m.From].Next = m.LogIndex
			}
		}
	}
	if r.prs.prs[m.From].Match >= r.prs.prs[m.From].Next {
		r.prs.prs[m.From].Match = r.prs.prs[m.From].Next - 1
	}
	r.advanceCommitIndex()

	return nil
}

func (l *RaftLog) findLastIndexOfTerm(term uint64) (uint64, bool) {
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

func (r *Raft) handleAppend(m raftpb.Message) error {
	reply := raftpb.Message{
		Type: raftpb.Type_MsgAppResp,
		From: r.id,
		To:   m.From,
		Term: r.hardState.Term,
	}

	if r.hardState.Term > m.Term {
		reply.Reject = true
		r.send(reply)
		return nil
	}

	reply.LogTerm = 0 //default 0
	lastIndex := r.raftLog.LastIndex()

	//preLogIndex > len entries
	if m.LogIndex > lastIndex {
		reply.Reject = true
		reply.LogIndex = lastIndex + 1
		r.send(reply)
		return nil
	}

	r.becomeFollower(m.Term, m.From)

	//not match preLogTerm
	if m.LogTerm != r.raftLog.Term(m.LogIndex) {
		reply.Reject = true
		reply.LogTerm = r.raftLog.Term(m.LogIndex) //conflictTerm

		first := m.LogIndex
		for first > r.raftLog.FirstIndex() && r.raftLog.Term(first-1) == reply.LogTerm {
			first--
		}
		reply.LogIndex = first //conflictIndex

		cut := reply.LogIndex - r.raftLog.offset
		r.raftLog.entries = r.raftLog.entries[:cut]
		r.storage.TruncateFrom(reply.LogIndex)
		if r.hardState.CommitIndex > r.raftLog.LastIndex() {
			r.commitTo(r.raftLog.LastIndex())
		}

		r.send(reply)
		return nil
	}

	if len(m.Entries) > 0 {
		pos := m.LogIndex - r.raftLog.offset
		newEntries := derefEntries(m.Entries)
		r.raftLog.entries = append(r.raftLog.entries[:pos+1], newEntries...)
		r.storage.SaveEntries(newEntries)
	}
	if m.Commit > r.hardState.CommitIndex {
		r.commitTo(min(m.Commit, r.raftLog.LastIndex()))
		r.applyLog()
	}

	if len(m.Entries) > 0 {
		slog.Info("recieve", "commitIndex", r.hardState.CommitIndex, "me", int(r.id))
	}

	reply.Reject = false
	reply.LogIndex = r.raftLog.LastIndex() //matchIndex
	r.send(reply)
	return nil
}

func (r *Raft) handleSnapshot(m raftpb.Message) error {
	reply := raftpb.Message{
		Type: raftpb.Type_MsgSnapResp,
		From: r.id,
		To:   m.From,
		Term: r.hardState.Term,
	}

	if r.hardState.Term > m.Term {
		reply.Reject = true
		r.send(reply)
		return nil
	}

	if m.Snapshot == nil {
		reply.Reject = true
		r.send(reply)
		return nil
	}

	if r.raftLog.offset >= m.Snapshot.Index {
		reply.Reject = true
		r.send(reply)
		return nil
	}

	r.becomeFollower(m.Term, m.From)

	if err := r.applySnapshot(*m.Snapshot); err != nil {
		reply.Reject = true
		r.send(reply)
		return err
	}

	reply.Reject = false
	r.send(reply)
	return nil
}

func (r *Raft) handleSnapshotResp(m raftpb.Message) error {
	if m.Reject == true {
		return nil
	}

	r.prs.prs[m.From].Next = r.raftLog.offset + 1
	r.prs.prs[m.From].Match = r.raftLog.offset
	return nil
}

func (r *Raft) send(m raftpb.Message) {
	if m.Type == raftpb.Type_MsgAppResp && m.Reject == true {
		slog.Info("rejectAppend", "conflictIndex", m.LogIndex, "me", r.id)
	}
	r.msgs = append(r.msgs, m)
}

func (r *Raft) campaign() error {
	r.becomeCandidate()
	slog.Info("start campaign", slog.Int("msgsize", len(r.msgs)), slog.Int("term", int(r.hardState.Term)), slog.Int("me", int(r.id)))
	for id := range r.prs.prs {
		if id == r.id {
			continue
		}
		r.msgs = append(r.msgs, raftpb.Message{
			Type:     raftpb.Type_MsgVote,
			From:     r.id,
			To:       id,
			Term:     r.hardState.Term,
			LogIndex: r.raftLog.LastIndex(),
			LogTerm:  r.raftLog.LastTerm(),
		})
	}
	return nil
}

func (r *Raft) grantVote(m raftpb.Message) bool {
	if r.hardState.Term > m.Term {
		return false
	}
	if r.hardState.Vote != 0 && r.hardState.Vote != m.From {
		return false
	}
	lastTerm := r.raftLog.LastTerm()
	lastIndex := r.raftLog.LastIndex()
	if lastTerm > m.LogTerm {
		return false
	}
	if lastTerm == m.LogTerm && lastIndex > m.LogIndex {
		return false
	}
	r.electionElapsed = 0
	r.hardState.Vote = m.From
	r.storage.SaveHardState(r.hardState)
	return true
}

func (r *Raft) vote(m raftpb.Message) error {
	granted := r.grantVote(m)
	r.msgs = append(r.msgs, raftpb.Message{
		Type:   raftpb.Type_MsgVoteResp,
		From:   r.id,
		To:     m.From,
		Term:   r.hardState.Term,
		Reject: !granted,
	})
	if granted {
		r.becomeFollower(m.Term, 0)
	}
	return nil
}

func (r *Raft) becomeFollower(term, lead uint64) {
	r.state = StateFollower
	r.lead = lead
	if term > r.hardState.Term && r.hardState.Vote != 0 {
		r.hardState.Vote = 0 //leader任期大于自己才清空投票
		r.storage.SaveHardState(r.hardState)
	}
	if term > r.hardState.Term {
		r.hardState.Term = term
		r.storage.SaveHardState(r.hardState)
	}
	r.votes = map[uint64]bool{}
	r.electionElapsed = 0
}

func (r *Raft) becomeCandidate() {
	r.state = StateCandidate
	r.lead = 0
	r.hardState.Term++
	r.hardState.Vote = r.id
	r.storage.SaveHardState(r.hardState)
	r.votes = map[uint64]bool{r.id: true}
	r.electionElapsed = 0
}

func (r *Raft) becomeLeader() {
	slog.Info("become leader", "commitIndex", r.hardState.CommitIndex,
		"logsize", len(r.raftLog.entries), "term", r.hardState.Term,
		"me", int(r.id))
	r.state = StateLeader
	r.lead = r.id
	r.heartbeatElapsed = 0
	slog.Info("init next", "value", r.raftLog.LastIndex()+1)
	for _, prs := range r.prs.prs {
		prs.Match = 0
		prs.Next = r.raftLog.LastIndex() + 1
	}
	r.bcastHeartBeat()
}

func (r *Raft) bcastHeartBeat() {
	for id := range r.prs.prs {
		if r.state != StateLeader {
			return
		}
		if id == r.id {
			continue
		}

		if r.prs.prs[id].Next <= r.raftLog.offset {
			r.sendSnapshot(id)
			continue
		}

		//slog.Info("send", "next", r.prs.prs[id].Next, "to", id, "me", r.id)
		m := raftpb.Message{
			Type: raftpb.Type_MsgApp,
			From: r.id,
			To:   id,
			Term: r.hardState.Term,

			Entries:  refEntries(r.raftLog.Slice(r.prs.prs[id].Next, r.raftLog.LastIndex()+1)),
			Commit:   r.hardState.CommitIndex,
			LogIndex: r.preLogIndex(id),
			LogTerm:  r.preLogTerm(id),
		}

		if len(m.Entries) > 0 {
			//slog.Info("send", slog.Int("next", int(r.prs.prs[id].Next)))
		}
		r.send(m)
	}
}

func (r *Raft) sendSnapshot(id uint64) {
	if r.state != StateLeader {
		return
	}

	snapshot, _ := r.storage.LoadSnapshot()
	if snapshot.Index == 0 {
		return
	}
	m := raftpb.Message{
		Type: raftpb.Type_MsgSnap,
		From: r.id,
		To:   id,
		Term: r.hardState.Term,

		Snapshot: &snapshot,
	}
	r.send(m)
}

func (r *Raft) checkVotes() (won, lost bool) {
	ag, rj := 0, 0
	for _, v := range r.votes {
		if v {
			ag++
		} else {
			rj++
		}
	}
	quorum := len(r.prs.prs)/2 + 1
	if ag >= quorum {
		return true, false
	}
	if rj >= quorum {
		return false, true
	}
	return false, false
}

func (r *Raft) advanceCommitIndex() {
	mci := r.prs.Committed()
	if r.raftLog.matchCommitTerm(mci, r.hardState.Term) {
		r.commitTo(mci)
		r.applyLog()
	}
}

// Snapshot creates a snapshot at the given index and compacts the log.
// This should be called by the application layer after applying entries.
func (r *Raft) Snapshot(index uint64) {
	if index <= r.raftLog.offset {
		return
	}
	r.compactTo(index, r.raftLog.Term(index))
	r.hardState.CommitIndex = max(index, r.hardState.CommitIndex)
	r.raftLog.appliedIndex = max(index, r.raftLog.appliedIndex)
	r.storage.SaveHardState(r.hardState)

	if r.state == StateLeader {
		for _, prs := range r.prs.prs {
			if prs.Next <= r.raftLog.offset {
				prs.Next = r.raftLog.offset + 1
			}
			if prs.Match < r.raftLog.offset {
				prs.Match = r.raftLog.offset
			}
		}
	}
	sn := raftpb.Snapshot{
		Term:  r.raftLog.Term(index),
		Index: index,
		Data:  r.storage.MakeSnapshotData(),
	}
	r.storage.SaveSnapshot(sn)
}

func (r *Raft) applyLog() {
	/*for r.raftLog.appliedIndex < r.hardState.CommitIndex {
		r.raftLog.appliedIndex++
		idx := r.raftLog.appliedIndex
		entry := r.raftLog.entries[idx]

		msg := raftapi.ApplyMsg{
			CommandValid: true,
			Command:      entry.Data,
			CommandIndex: int(entry.Index),
		}
		//slog.Info("apply", slog.Int("index", msg.CommandIndex), slog.Int("me", int(r.id)))
		r.applyCh <- msg
	}*/
}

func (r *Raft) applySnapshot(sn raftpb.Snapshot) error {
	if sn.Index == 0 {
		return nil
	}

	// persist snapshot metadata and state machine data first
	if err := r.storage.SaveSnapshot(sn); err != nil {
		return err
	}
	if err := r.storage.ApplySnapshotData(sn.Data); err != nil {
		return err
	}

	// restore in-memory raft state from snapshot
	r.restore(sn)
	return r.storage.SaveHardState(r.hardState)
}
