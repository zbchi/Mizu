package raft

import (
	"github.com/zbchi/linkv/proto/raftpb"
)

const MaxLogsCount = 2000

// RaftStorage defines the interface for persisting Raft state
type RaftStorage interface {
	SaveHardState(st HardState) error
	LoadHardState() (HardState, error)

	SaveEntries(entries []*raftpb.Entry) error
	LoadEntries(lo uint64, hi uint64) ([]*raftpb.Entry, error)
	TruncateFrom(index uint64) error

	SaveSnapshot(sn *raftpb.Snapshot) error
	LoadSnapshot() (*raftpb.Snapshot, error)

	MakeSnapshotData() []byte
	ApplySnapshotData(data []byte) error
}

// MemoryStorage is a simple in-memory storage for testing
type MemoryStorage struct {
	ents        []*raftpb.Entry
	snapshot    *raftpb.Snapshot
	hardState   HardState
	commitIndex uint64
}

// NewMemoryStorage creates a new MemoryStorage
func NewMemoryStorage() *MemoryStorage {
	return &MemoryStorage{
		ents:     make([]*raftpb.Entry, 0),
		snapshot: &raftpb.Snapshot{},
	}
}

// InitialState returns the initial hard state and snapshot
func (ms *MemoryStorage) InitialState() (HardState, *raftpb.Snapshot, error) {
	return ms.hardState, ms.snapshot, nil
}

// SetHardState sets the hard state
func (ms *MemoryStorage) SetHardState(st HardState) error {
	ms.hardState = st
	return nil
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

// Entries returns entries between lo and hi
func (ms *MemoryStorage) Entries(lo, hi uint64) ([]*raftpb.Entry, error) {
	if lo > uint64(len(ms.ents)) {
		return nil, nil
	}
	if hi > uint64(len(ms.ents)) {
		hi = uint64(len(ms.ents))
	}
	return ms.ents[lo-1 : hi-1], nil
}

// Term returns the term of entry at index i
func (ms *MemoryStorage) Term(i uint64) (uint64, error) {
	if i > uint64(len(ms.ents)) {
		return 0, nil
	}
	if i < 1 {
		return 0, nil
	}
	return ms.ents[i-1].Term, nil
}

// LastIndex returns the last index
func (ms *MemoryStorage) LastIndex() uint64 {
	return uint64(len(ms.ents))
}

// FirstIndex returns the first index
func (ms *MemoryStorage) FirstIndex() uint64 {
	return 1
}

// Snapshot returns the snapshot
func (ms *MemoryStorage) Snapshot() (*raftpb.Snapshot, error) {
	return ms.snapshot, nil
}

// ApplySnapshot applies the snapshot
func (ms *MemoryStorage) ApplySnapshot(snap *raftpb.Snapshot) error {
	ms.snapshot = snap
	ms.ents = make([]*raftpb.Entry, 0)
	ms.commitIndex = snap.Index
	return nil
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

// CreateSnapshot creates a snapshot at index i
func (ms *MemoryStorage) CreateSnapshot(i uint64, data []byte) (*raftpb.Snapshot, error) {
	ms.snapshot = &raftpb.Snapshot{
		Index: i,
		Term:  ms.ents[i-1].Term,
		Data:  data,
	}
	return ms.snapshot, nil
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

// Append appends entries
func (ms *MemoryStorage) Append(entries []*raftpb.Entry) error {
	if len(entries) == 0 {
		return nil
	}
	ms.ents = append(ms.ents, entries...)
	return nil
}

// LoadEntries loads entries in the range [lo, hi)
func (ms *MemoryStorage) LoadEntries(lo, hi uint64) ([]*raftpb.Entry, error) {
	if lo > uint64(len(ms.ents)) {
		return nil, nil
	}
	if hi > uint64(len(ms.ents)) {
		hi = uint64(len(ms.ents))
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

// ApplySnapshotData applies snapshot data (for testing)
func (ms *MemoryStorage) ApplySnapshotData(data []byte) error {
	return nil
}

// MakeSnapshotData creates snapshot data (for testing)
func (ms *MemoryStorage) MakeSnapshotData() []byte {
	return nil
}
