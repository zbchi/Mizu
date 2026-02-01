package raft

// readRound tracks the Raft-side quorum confirmation for one ReadIndex request.
// It is intentionally separate from raftstore's client-facing readRequest.
type readRound struct {
	index   uint64
	started bool
	acks    map[uint64]struct{}
}

// ReadIndex starts a quorum-confirmed read round for readID. A completed round
// is delivered as a ReadState in Ready.
func (r *Raft) ReadIndex(readID uint64) bool {
	if r.state != StateLeader || readID == 0 {
		return false
	}
	if _, exists := r.readRounds[readID]; exists {
		return false
	}

	r.readRounds[readID] = &readRound{}
	r.startReadRound(readID)
	return true
}

// CancelReadIndex discards a read round that the caller no longer needs.
func (r *Raft) CancelReadIndex(readID uint64) {
	delete(r.readRounds, readID)
}

func (r *Raft) committedEntryInCurrentTerm() bool {
	return r.hardState.CommitIndex > 0 &&
		r.raftLog.Term(r.hardState.CommitIndex) == r.hardState.Term
}

func (r *Raft) startPendingReadRounds() {
	if r.state != StateLeader || !r.committedEntryInCurrentTerm() {
		return
	}
	for readID := range r.readRounds {
		r.startReadRound(readID)
	}
}

func (r *Raft) retryReadRounds() {
	for readID, round := range r.readRounds {
		if round.started {
			r.bcastAppend(readID)
		}
	}
}

func (r *Raft) startReadRound(readID uint64) {
	round := r.readRounds[readID]
	if round == nil || round.started || !r.committedEntryInCurrentTerm() {
		return
	}

	round.index = r.hardState.CommitIndex
	round.started = true
	round.acks = map[uint64]struct{}{r.id: {}}
	if len(round.acks) >= r.quorum() {
		r.completeReadRound(readID)
		return
	}
	r.bcastAppend(readID)
}

func (r *Raft) ackReadRound(readID, from uint64) {
	if readID == 0 || r.state != StateLeader {
		return
	}
	if _, voter := r.prs[from]; !voter {
		return
	}
	round := r.readRounds[readID]
	if round == nil || !round.started {
		return
	}
	round.acks[from] = struct{}{}
	if len(round.acks) >= r.quorum() {
		r.completeReadRound(readID)
	}
}

func (r *Raft) completeReadRound(readID uint64) {
	round := r.readRounds[readID]
	if round == nil {
		return
	}
	r.readStates = append(r.readStates, ReadState{ReadID: readID, Index: round.index})
	delete(r.readRounds, readID)
}
