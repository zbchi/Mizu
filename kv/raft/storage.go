package raft

import (
	"bytes"
	"encoding/binary"

	"github.com/dgraph-io/badger/v3"
	"github.com/zbchi/linkv/proto/raftpb"
	"google.golang.org/protobuf/proto"
)

const MaxLogsCount = 2000

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

const (
	keyHardState = "raft/hard_state"
	keySnapshot  = "raft/snapshot"
	keyEntry     = "raft/entry"
	KeyRaft      = "raft/"
)

type BadgerRaftStorage struct {
	db *badger.DB
}

func NewBadgerRaftStorage(db *badger.DB) RaftStorage {
	return &BadgerRaftStorage{db: db}
}

func entryKey(index uint64) []byte {
	b := make([]byte, len(keyEntry)+8)
	copy(b, []byte(keyEntry))
	binary.BigEndian.PutUint64(b[len(keyEntry):], index)
	return b
}

func (s *BadgerRaftStorage) SaveHardState(st HardState) error {
	data := encodeHardState(st)
	return s.db.Update(func(txn *badger.Txn) error {
		return txn.Set([]byte(keyHardState), data)
	})
}

func (s *BadgerRaftStorage) LoadHardState() (HardState, error) {
	var st HardState
	err := s.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(keyHardState))
		if err == badger.ErrKeyNotFound {
			return nil
		}
		if err != nil {
			return err
		}
		v, _ := item.ValueCopy(nil)
		st = decodeHardState(v)
		return nil
	})
	return st, err
}

func (s *BadgerRaftStorage) SaveEntries(entries []*raftpb.Entry) error {
	return s.db.Update(func(txn *badger.Txn) error {
		for _, e := range entries {
			key := entryKey(e.Index)
			b, _ := proto.Marshal(e)
			if err := txn.Set(key, b); err != nil {
				return err
			}
		}
		return nil
	})
}

func (s *BadgerRaftStorage) TruncateFrom(index uint64) error {
	return s.db.Update(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()

		start := entryKey(index)
		it.Seek(start)

		for ; it.Valid(); it.Next() {
			k := it.Item().KeyCopy(nil)
			if err := txn.Delete(k); err != nil {
				return err
			}
		}
		return nil
	})
}

func (s *BadgerRaftStorage) LoadEntries(lo uint64, hi uint64) ([]*raftpb.Entry, error) {
	entries := make([]*raftpb.Entry, 0)

	err := s.db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()

		start := entryKey(lo)
		it.Seek(start)

		for ; it.Valid(); it.Next() {
			item := it.Item()
			key := item.Key()

			// 检查 key 长度是否有效
			if len(key) < len(keyEntry)+8 {
				break
			}

			// 检查是否是 raft entry key
			if !bytes.HasPrefix(key, []byte(keyEntry)) {
				break
			}

			idx := binary.BigEndian.Uint64(key[len(keyEntry):])
			if idx >= hi {
				break
			}

			v, _ := item.ValueCopy(nil)

			var e raftpb.Entry
			proto.Unmarshal(v, &e)
			entries = append(entries, &e)
		}
		return nil
	})
	return entries, err
}

func (s *BadgerRaftStorage) SaveSnapshot(sn *raftpb.Snapshot) error {
	data, _ := proto.Marshal(sn)
	return s.db.Update(func(txn *badger.Txn) error {
		return txn.Set([]byte(keySnapshot), data)
	})
}

func (s *BadgerRaftStorage) LoadSnapshot() (*raftpb.Snapshot, error) {
	var sn raftpb.Snapshot
	err := s.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(keySnapshot))
		if err == badger.ErrKeyNotFound {
			return nil
		}
		if err != nil {
			return err
		}
		v, _ := item.ValueCopy(nil)
		proto.Unmarshal(v, &sn)
		return nil
	})
	if err != nil || sn.Index == 0 {
		return nil, err
	}
	return &sn, err
}

func (s *BadgerRaftStorage) MakeSnapshotData() []byte {
	sn := &raftpb.SnapshotData{}
	s.db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()

		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			key := item.KeyCopy(nil)

			if bytes.HasPrefix(key, []byte(KeyRaft)) {
				continue
			}
			val, _ := item.ValueCopy(nil)
			sn.Kvs = append(sn.Kvs, &raftpb.KvPair{
				Key:   key,
				Value: val,
			})
		}
		return nil
	})
	data, _ := proto.Marshal(sn)
	return data
}

func (s *BadgerRaftStorage) ApplySnapshotData(data []byte) error {
	if len(data) == 0 {
		return nil
	}

	var sn raftpb.SnapshotData
	if err := proto.Unmarshal(data, &sn); err != nil {
		return err
	}

	return s.db.Update(func(txn *badger.Txn) error {
		// wipe existing user data before applying snapshot content
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()

		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			key := item.KeyCopy(nil)
			if bytes.HasPrefix(key, []byte(KeyRaft)) {
				continue
			}
			if err := txn.Delete(key); err != nil {
				return err
			}
		}

		for _, kv := range sn.Kvs {
			if err := txn.Set(kv.Key, kv.Value); err != nil {
				return err
			}
		}
		return nil
	})
}

func encodeHardState(st HardState) []byte {
	b := make([]byte, 24)
	binary.BigEndian.PutUint64(b[0:], st.Term)
	binary.BigEndian.PutUint64(b[8:], st.Vote)
	binary.BigEndian.PutUint64(b[16:], st.CommitIndex)
	return b
}

func decodeHardState(b []byte) HardState {
	if len(b) < 24 {
		return HardState{}
	}
	return HardState{
		Term:        binary.BigEndian.Uint64(b[0:8]),
		Vote:        binary.BigEndian.Uint64(b[8:16]),
		CommitIndex: binary.BigEndian.Uint64(b[16:24]),
	}
}

// MemoryStorage is a simple in-memory storage for testing
type MemoryStorage struct {
	ents            []*raftpb.Entry
	snapshot        *raftpb.Snapshot
	hardState       HardState
	commitIndex     uint64
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
	return ms.ents[lo-1:hi-1], nil
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
