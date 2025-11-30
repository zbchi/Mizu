package raft

import (
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
		granted := r.grantVote(m)
		r.msgs = append(r.msgs, raftpb.Message{
			Type:   raftpb.Type_MsgVoteResp,
			From:   r.id,
			To:     m.From,
			Term:   r.hardState.Term,
			Reject: !granted,
		})
		return nil
	case raftpb.Type_MsgApp:
		ok := r.handleAppend(m)
		r.msgs = append(r.msgs, raftpb.Message{
			Type:   raftpb.Type_MsgAppResp,
			From:   r.id,
			To:     m.From,
			Term:   r.hardState.Term,
			Reject: !ok,
		})
		if ok {
			r.lead = m.From
		}
		return nil
	}
	return nil
}

func (r *Raft) stepCandidate(m raftpb.Message) error {
	switch m.Type {
	case raftpb.Type_MsgHup:
		return r.campaign()
	case raftpb.Type_MsgVoteResp:
		r.votes[m.From] = !m.Reject
		won, lost := r.checkVotes()
		if won {
			r.becomeLeader()
		}
		if lost {
			r.becomeFollower(r.hardState.Term, 0)
		}
		return nil
	case raftpb.Type_MsgVote:
		granted := r.grantVote(m)
		r.msgs = append(r.msgs, raftpb.Message{
			Type:   raftpb.Type_MsgVoteResp,
			From:   r.id,
			To:     m.From,
			Term:   r.hardState.Term,
			Reject: !granted,
		})
		return nil
	case raftpb.Type_MsgApp:
		ok := r.handleAppend(m)
		r.msgs = append(r.msgs, raftpb.Message{
			Type:   raftpb.Type_MsgAppResp,
			From:   r.id,
			To:     m.From,
			Term:   r.hardState.Term,
			Reject: !ok,
		})
		return nil
	}
	return nil
}

func (r *Raft) stepLeader(m raftpb.Message) error {
	return nil
}

func (r *Raft) handleAppend(m raftpb.Message) bool {
	if r.hardState.Term > m.Term {
		return false
	}
	r.becomeFollower(m.Term, m.From)
	return true
}

func (r *Raft) campaign() error {
	r.becomeCandidate()
	for id := range r.prs.prs {
		if id == r.id {
			continue
		}
		r.msgs = append(r.msgs, raftpb.Message{
			Type: raftpb.Type_MsgVote,
			From: r.id,
			To:   id,
			Term: r.hardState.Term,
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
	r.electionElapsed = 0
	r.hardState.Vote = m.From
	return true
}

func (r *Raft) becomeFollower(term, lead uint64) {
	r.state = StateFollower
	r.lead = lead
	if term > r.hardState.Term {
		r.hardState.Vote = 0 //leader任期大于自己才清空投票
	}
	r.hardState.Term = term
	r.votes = map[uint64]bool{}
	r.electionElapsed = 0
}

func (r *Raft) becomeCandidate() {
	r.state = StateCandidate
	r.lead = 0
	r.hardState.Term++
	r.hardState.Vote = r.id
	r.votes = map[uint64]bool{r.id: true}
	r.electionElapsed = 0
}

func (r *Raft) becomeLeader() {
	r.state = StateLeader
	r.lead = r.id
	r.heartbeatElapsed = 0
	r.bcastHeartBeat()
}

func (r *Raft) bcastHeartBeat() {
	for id := range r.prs.prs {
		if id == r.id {
			continue
		}
		m := raftpb.Message{
			Type: raftpb.Type_MsgApp,
			From: r.id,
			To:   id,
			Term: r.hardState.Term,
		}
		r.msgs = append(r.msgs, m)
	}
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
