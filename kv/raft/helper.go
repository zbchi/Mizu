package raft

import "github.com/zbchi/linkv/proto/raftpb"

func derefEntries(src []*raftpb.Entry) []raftpb.Entry {
	res := make([]raftpb.Entry, len(src))
	for i, e := range src {
		if e != nil {
			res[i] = *e
		}
	}
	return res
}

func refEntries(src []raftpb.Entry) []*raftpb.Entry {
	res := make([]*raftpb.Entry, len(src))
	for i := range src {
		res[i] = &src[i] // ← 正确：直接取切片内部元素的地址
	}
	return res
}
