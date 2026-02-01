package raft

import (
	"github.com/zbchi/mizu/proto/raftpb"
)

// RaftStorage defines the interface for persisting Raft state
type RaftStorage interface {
	SaveHardState(st HardState) error
	LoadHardState() (HardState, error)

	SaveEntries(entries []*raftpb.Entry) error
	LoadEntries(lo uint64, hi uint64) ([]*raftpb.Entry, error)
	TruncateFrom(index uint64) error
	Compact(index uint64) error

	SaveSnapshot(sn *raftpb.Snapshot) error
	LoadSnapshot() (*raftpb.Snapshot, error)
}

// MemoryStorage is a simple in-memory storage for testing
type MemoryStorage struct {
	ents      []*raftpb.Entry
	snapshot  *raftpb.Snapshot
	hardState HardState
}

var _ RaftStorage = (*MemoryStorage)(nil)

// NewMemoryStorage creates a new MemoryStorage
func NewMemoryStorage() *MemoryStorage {
	return &MemoryStorage{
		ents:     make([]*raftpb.Entry, 0),
		snapshot: &raftpb.Snapshot{},
	}
}

// SaveHardState saves the hard state
func (ms *MemoryStorage) SaveHardState(st HardState) error {
	ms.hardState = st
	return nil
}

// LoadHardState loads the hard state
func (ms *MemoryStorage) LoadHardState() (HardState, error) {
	return ms.hardState, nil
}

// SaveSnapshot saves the snapshot
func (ms *MemoryStorage) SaveSnapshot(sn *raftpb.Snapshot) error {
	ms.snapshot = sn
	return nil
}

// LoadSnapshot loads the snapshot
func (ms *MemoryStorage) LoadSnapshot() (*raftpb.Snapshot, error) {
	if ms.snapshot.Index == 0 {
		return nil, nil
	}
	return ms.snapshot, nil
}

// SaveEntries saves entries
func (ms *MemoryStorage) SaveEntries(entries []*raftpb.Entry) error {
	ms.ents = append(ms.ents, entries...)
	return nil
}

// Compact compacts the log up to index i
func (ms *MemoryStorage) Compact(compactIndex uint64) error {
	if compactIndex <= ms.snapshot.Index {
		return nil
	}
	ms.ents = ms.ents[compactIndex-1:]
	if ms.snapshot.Index == 0 {
		ms.snapshot = &raftpb.Snapshot{
			Index: compactIndex,
			Term:  ms.ents[0].Term,
		}
	}
	return nil
}

// LoadEntries loads entries in the range [lo, hi)
func (ms *MemoryStorage) LoadEntries(lo, hi uint64) ([]*raftpb.Entry, error) {
	last := uint64(len(ms.ents)) + 1
	if lo == 0 || lo >= last {
		return nil, nil
	}
	if hi > last {
		hi = last
	}
	if lo >= hi {
		return nil, nil
	}
	return ms.ents[lo-1 : hi-1], nil
}

// TruncateFrom truncates entries from index
func (ms *MemoryStorage) TruncateFrom(index uint64) error {
	if index > uint64(len(ms.ents)) {
		return nil
	}
	ms.ents = ms.ents[:index-1]
	return nil
}
