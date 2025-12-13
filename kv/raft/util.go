package raft

import (
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"os/exec"
	"sort"
	"strings"

	"github.com/zbchi/linkv/proto/raftpb"
)

func min(a, b uint64) uint64 {
	if a > b {
		return b
	}
	return a
}

func max(a, b uint64) uint64 {
	if a > b {
		return a
	}
	return b
}

// IsEmptyHardState returns true if the given HardState is empty.
func IsEmptyHardState(st HardState) bool {
	return isHardStateEqual(st, HardState{})
}

// IsEmptySnap returns true if the given Snapshot is empty.
func IsEmptySnap(sp *raftpb.Snapshot) bool {
	if sp == nil {
		return true
	}
	return sp.Index == 0
}

func mustTerm(term uint64, err error) uint64 {
	if err != nil {
		panic(err)
	}
	return term
}

func nodes(r *Raft) []uint64 {
	nodes := make([]uint64, 0, len(r.prs))
	for id := range r.prs {
		nodes = append(nodes, id)
	}
	sort.Sort(uint64Slice(nodes))
	return nodes
}

func diffu(a, b string) string {
	if a == b {
		return ""
	}
	aname, bname := mustTemp("base", a), mustTemp("other", b)
	defer os.Remove(aname)
	defer os.Remove(bname)
	cmd := exec.Command("diff", "-u", aname, bname)
	buf, err := cmd.CombinedOutput()
	if err != nil {
		if _, ok := err.(*exec.ExitError); ok {
			// do nothing
			return string(buf)
		}
		panic(err)
	}
	return string(buf)
}

func mustTemp(pre, body string) string {
	f, err := ioutil.TempFile("", pre)
	if err != nil {
		panic(err)
	}
	_, err = io.Copy(f, strings.NewReader(body))
	if err != nil {
		panic(err)
	}
	f.Close()
	return f.Name()
}

func ltoa(l *RaftLog) string {
	s := fmt.Sprintf("applied:  %d\n", l.appliedIndex)
	for i, e := range l.entries {
		s += fmt.Sprintf("#%d: %+v\n", i, e)
	}
	return s
}

type uint64Slice []uint64

func (p uint64Slice) Len() int           { return len(p) }
func (p uint64Slice) Less(i, j int) bool { return p[i] < p[j] }
func (p uint64Slice) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }

func IsLocalMsg(msgt raftpb.Type) bool {
	return msgt == raftpb.Type_MsgHup || msgt == raftpb.Type_MsgBeat
}

func IsResponseMsg(msgt raftpb.Type) bool {
	return msgt == raftpb.Type_MsgAppResp || msgt == raftpb.Type_MsgVoteResp || msgt == raftpb.Type_MsgSnapResp
}

func isHardStateEqual(a, b HardState) bool {
	return a.Term == b.Term && a.Vote == b.Vote && a.CommitIndex == b.CommitIndex
}
