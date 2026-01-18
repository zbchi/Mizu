package raftstorage

import (
	"bytes"
	"encoding/binary"
	"strconv"

	"github.com/dgraph-io/badger/v3"
	"github.com/zbchi/mizu/kv/storage"
	"github.com/zbchi/mizu/proto/raftpb"
	"github.com/zbchi/mizu/raft"
	"google.golang.org/protobuf/proto"
)

const (
	KeyRaft = "raft/"
)

// Storage implements persistent Raft storage using BadgerDB
type Storage struct {
	db       *badger.DB
	regionID uint64
}

// NewStorage creates a new Badger-based Raft storage
func NewStorage(db *badger.DB, regionID uint64) raft.RaftStorage {
	return &Storage{db: db, regionID: regionID}
}

func (s *Storage) regionPrefix() []byte {
	return []byte(KeyRaft + encodeUint64(s.regionID) + "/")
}

func (s *Storage) hardStateKey() []byte {
	return append(s.regionPrefix(), []byte("hard_state")...)
}

func (s *Storage) snapshotKey() []byte {
	return append(s.regionPrefix(), []byte("snapshot")...)
}

func (s *Storage) entryPrefix() []byte {
	return append(s.regionPrefix(), []byte("entry/")...)
}

func (s *Storage) entryKey(index uint64) []byte {
	prefix := s.entryPrefix()
	b := make([]byte, len(prefix)+8)
	copy(b, prefix)
	binary.BigEndian.PutUint64(b[len(prefix):], index)
	return b
}

func (s *Storage) SaveHardState(st raft.HardState) error {
	data := encodeHardState(st)
	return s.db.Update(func(txn *badger.Txn) error {
		return txn.Set(s.hardStateKey(), data)
	})
}

func (s *Storage) LoadHardState() (raft.HardState, error) {
	var st raft.HardState
	err := s.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(s.hardStateKey())
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

func (s *Storage) SaveEntries(entries []*raftpb.Entry) error {
	return s.db.Update(func(txn *badger.Txn) error {
		for _, e := range entries {
			key := s.entryKey(e.Index)
			b, _ := proto.Marshal(e)
			if err := txn.Set(key, b); err != nil {
				return err
			}
		}
		return nil
	})
}

// Compact deletes all logs before the given index, keeping index and all logs after it
func (s *Storage) Compact(index uint64) error {
	if index == 0 {
		return nil
	}

	return s.db.Update(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()

		entryPrefix := s.entryPrefix()
		start := s.entryKey(0)
		it.Seek(start)

		for ; it.Valid(); it.Next() {
			item := it.Item()
			key := item.Key()

			if !bytes.HasPrefix(key, entryPrefix) {
				break
			}

			if len(key) < len(entryPrefix)+8 {
				break
			}

			idx := binary.BigEndian.Uint64(key[len(entryPrefix):])

			if idx >= index {
				break
			}

			k := item.KeyCopy(nil)
			if err := txn.Delete(k); err != nil {
				return err
			}
		}
		return nil
	})
}

func (s *Storage) TruncateFrom(index uint64) error {
	return s.db.Update(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()

		entryPrefix := s.entryPrefix()
		start := s.entryKey(index)
		it.Seek(start)

		for ; it.Valid(); it.Next() {
			key := it.Item().Key()
			if !bytes.HasPrefix(key, entryPrefix) {
				break
			}
			k := it.Item().KeyCopy(nil)
			if err := txn.Delete(k); err != nil {
				return err
			}
		}
		return nil
	})
}

func (s *Storage) LoadEntries(lo uint64, hi uint64) ([]*raftpb.Entry, error) {
	entries := make([]*raftpb.Entry, 0)

	err := s.db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()

		entryPrefix := s.entryPrefix()
		start := s.entryKey(lo)
		it.Seek(start)

		for ; it.Valid(); it.Next() {
			item := it.Item()
			key := item.Key()

			// 检查 key 长度是否有效
			if len(key) < len(entryPrefix)+8 {
				break
			}

			// 检查是否是 raft entry key
			if !bytes.HasPrefix(key, entryPrefix) {
				break
			}

			idx := binary.BigEndian.Uint64(key[len(entryPrefix):])
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

func (s *Storage) SaveSnapshot(sn *raftpb.Snapshot) error {
	data, _ := proto.Marshal(sn)
	return s.db.Update(func(txn *badger.Txn) error {
		return txn.Set(s.snapshotKey(), data)
	})
}

func (s *Storage) LoadSnapshot() (*raftpb.Snapshot, error) {
	var sn raftpb.Snapshot
	err := s.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(s.snapshotKey())
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

func (s *Storage) MakeSnapshotData() []byte {
	sn := &raftpb.SnapshotData{}
	s.db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()

		dataPrefix := storage.RegionDataPrefix(s.regionID)
		it.Seek(dataPrefix)

		for ; it.Valid(); it.Next() {
			item := it.Item()
			key := item.KeyCopy(nil)

			if !bytes.HasPrefix(key, dataPrefix) {
				break
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

func (s *Storage) ApplySnapshotData(data []byte) error {
	if len(data) == 0 {
		return nil
	}

	var sn raftpb.SnapshotData
	if err := proto.Unmarshal(data, &sn); err != nil {
		return err
	}

	return s.db.Update(func(txn *badger.Txn) error {
		dataPrefix := storage.RegionDataPrefix(s.regionID)

		// wipe existing user data for this region before applying snapshot content
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()

		it.Seek(dataPrefix)
		for ; it.Valid(); it.Next() {
			item := it.Item()
			key := item.KeyCopy(nil)
			if !bytes.HasPrefix(key, dataPrefix) {
				break
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

func encodeHardState(st raft.HardState) []byte {
	b := make([]byte, 24)
	binary.BigEndian.PutUint64(b[0:], st.Term)
	binary.BigEndian.PutUint64(b[8:], st.Vote)
	binary.BigEndian.PutUint64(b[16:], st.CommitIndex)
	return b
}

func encodeUint64(v uint64) string {
	return strconv.FormatUint(v, 10)
}

func decodeHardState(b []byte) raft.HardState {
	if len(b) < 24 {
		return raft.HardState{}
	}
	return raft.HardState{
		Term:        binary.BigEndian.Uint64(b[0:8]),
		Vote:        binary.BigEndian.Uint64(b[8:16]),
		CommitIndex: binary.BigEndian.Uint64(b[16:24]),
	}
}
