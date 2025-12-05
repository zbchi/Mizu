package raft

import (
	"bytes"
	"encoding/binary"

	"github.com/dgraph-io/badger/v3"
	"github.com/zbchi/linkv/proto/raftpb"
	"google.golang.org/protobuf/proto"
)

const MaxLogsCount = 2000

type RaftStroage interface {
	SaveHardState(st HardState) error
	LoadHardState() (HardState, error)

	SaveEntries(entries []raftpb.Entry) error
	LoadEntries(lo uint64, hi uint64) ([]raftpb.Entry, error)
	TruncateFrom(index uint64) error

	SaveSnapshot(sn raftpb.Snapshot) error
	LoadSnapshot() (raftpb.Snapshot, error)

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

func (s *BadgerRaftStorage) SaveEntries(entries []raftpb.Entry) error {
	return s.db.Update(func(txn *badger.Txn) error {
		for _, e := range entries {
			key := entryKey(e.Index)
			b, _ := proto.Marshal(&e)
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

func (s *BadgerRaftStorage) LoadEntries(lo uint64, hi uint64) ([]raftpb.Entry, error) {
	entries := make([]raftpb.Entry, 0)

	err := s.db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()

		start := entryKey(lo)
		it.Seek(start)

		for ; it.Valid(); it.Next() {
			item := it.Item()
			key := item.Key()

			idx := binary.BigEndian.Uint64(key[len(keyEntry):])
			if idx >= hi {
				break
			}

			v, _ := item.ValueCopy(nil)

			var e raftpb.Entry
			proto.Unmarshal(v, &e)
			entries = append(entries, e)
		}
		return nil
	})
	return entries, err
}

func (s *BadgerRaftStorage) SaveSnapshot(sn raftpb.Snapshot) error {
	data, _ := proto.Marshal(&sn)
	return s.db.Update(func(txn *badger.Txn) error {
		return txn.Set([]byte(keySnapshot), data)
	})
}

func (s *BadgerRaftStorage) LoadSnapshot() (raftpb.Snapshot, error) {
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
	return sn, err
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
