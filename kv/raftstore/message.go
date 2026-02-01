package raftstore

import "github.com/zbchi/mizu/proto/raftpb"

// peerEvent routes one typed event to a local Peer.
type peerEvent struct {
	regionID uint64
	event    raftEvent
}

// raftEvent is a closed set of events consumed by raftWorker. Concrete event
// types keep the worker dispatch type-safe without introducing a framework.
type raftEvent interface{ isRaftEvent() }

type raftMessageEvent struct{ message *raftpb.Message }

func (raftMessageEvent) isRaftEvent() {}

type raftCommandEvent struct{ proposal *proposal }

func (raftCommandEvent) isRaftEvent() {}

type tickEvent struct{}

func (tickEvent) isRaftEvent() {}

type snapshotTask struct {
	index uint64
	data  []byte
}

func (snapshotTask) isRaftEvent() {}

type readIndexEvent struct{ request *readRequest }

func (readIndexEvent) isRaftEvent() {}
