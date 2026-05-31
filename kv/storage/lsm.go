package storage

import (
	"bytes"
	"encoding/binary"
	"strconv"

	"github.com/zbchi/mizu/internal/lsmtree"
	"github.com/zbchi/mizu/proto/raftpb"
	"github.com/zbchi/mizu/raft"
	"google.golang.org/protobuf/proto"
)

// LSMStorage stores region data and Raft state in one lsmtree database. The
// two namespaces are separated by their key prefixes.
type LSMStorage struct {
	db *lsmtree.DB
}

var _ Storage = (*LSMStorage)(nil)
var _ RegionStorage = (*lsmRegionStorage)(nil)
var _ StorageReader = (*lsmRegionReader)(nil)
var _ Iterator = (*lsmIterator)(nil)
var _ raft.RaftStorage = (*lsmRaftStorage)(nil)

func NewLSMStorage(dbPath string) *LSMStorage {
	db, err := lsmtree.Open(dbPath)
	if err != nil {
		panic(err)
	}
	return &LSMStorage{db: db}
}

func (s *LSMStorage) Start() error { return nil }

func (s *LSMStorage) Stop() error {
	s.db.Close()
	return nil
}

func (s *LSMStorage) RegionStorage(regionID uint64) RegionStorage {
	return &lsmRegionStorage{db: s.db, regionID: regionID}
}

func (s *LSMStorage) RaftStorage(regionID uint64) raft.RaftStorage {
	return &lsmRaftStorage{db: s.db, regionID: regionID}
}

type lsmRegionStorage struct {
	db       *lsmtree.DB
	regionID uint64
}

func (s *lsmRegionStorage) Reader() (StorageReader, error) {
	reader, err := s.db.Reader()
	if err != nil {
		return nil, err
	}
	return &lsmRegionReader{reader: reader, regionID: s.regionID}, nil
}

func (s *lsmRegionStorage) Write(modifications []Modify) error {
	batch, err := s.db.NewBatch()
	if err != nil {
		return err
	}
	defer batch.Close()

	for _, modification := range modifications {
		switch data := modification.Data.(type) {
		case Put:
			if err := batch.Put(EncodeKey(s.regionID, data.Key, data.Cf), data.Value); err != nil {
				return err
			}
		case Delete:
			if err := batch.Erase(EncodeKey(s.regionID, data.Key, data.Cf)); err != nil {
				return err
			}
		}
	}
	return s.db.Write(batch, false)
}

func (s *lsmRegionStorage) CreateSnapshot() ([]byte, error) {
	reader, err := s.db.Reader()
	if err != nil {
		return nil, err
	}
	defer reader.Close()

	iterator, err := reader.Iterator()
	if err != nil {
		return nil, err
	}
	defer iterator.Close()
	iterator.Seek(RegionDataPrefix(s.regionID))

	snapshot := &raftpb.SnapshotData{}
	for iterator.Valid() {
		item, err := iterator.Item()
		if err != nil {
			return nil, err
		}
		if !bytes.HasPrefix(item.Key, RegionDataPrefix(s.regionID)) {
			break
		}
		snapshot.Kvs = append(snapshot.Kvs, &raftpb.KvPair{
			Key:   item.Key,
			Value: item.Value,
		})
		iterator.Next()
	}
	if err := iterator.Status(); err != nil {
		return nil, err
	}
	return proto.Marshal(snapshot)
}

func (s *lsmRegionStorage) ApplySnapshot(data []byte) error {
	if len(data) == 0 {
		return nil
	}
	var snapshot raftpb.SnapshotData
	if err := proto.Unmarshal(data, &snapshot); err != nil {
		return err
	}

	reader, err := s.db.Reader()
	if err != nil {
		return err
	}
	iterator, err := reader.Iterator()
	if err != nil {
		reader.Close()
		return err
	}
	iterator.Seek(RegionDataPrefix(s.regionID))
	keys := make([][]byte, 0, len(snapshot.Kvs))
	for iterator.Valid() {
		item, itemErr := iterator.Item()
		if itemErr != nil {
			iterator.Close()
			reader.Close()
			return itemErr
		}
		if !bytes.HasPrefix(item.Key, RegionDataPrefix(s.regionID)) {
			break
		}
		keys = append(keys, item.Key)
		iterator.Next()
	}
	if err := iterator.Status(); err != nil {
		iterator.Close()
		reader.Close()
		return err
	}
	iterator.Close()
	reader.Close()

	batch, err := s.db.NewBatch()
	if err != nil {
		return err
	}
	defer batch.Close()
	for _, key := range keys {
		if err := batch.Erase(key); err != nil {
			return err
		}
	}
	for _, kv := range snapshot.Kvs {
		if kv == nil {
			continue
		}
		if err := batch.Put(kv.Key, kv.Value); err != nil {
			return err
		}
	}
	return s.db.Write(batch, true)
}

type lsmRegionReader struct {
	reader   *lsmtree.Reader
	regionID uint64
}

func (r *lsmRegionReader) GetCF(cf string, key []byte) ([]byte, error) {
	value, found, err := r.reader.Get(EncodeKey(r.regionID, key, cf))
	if err != nil || !found {
		return value, err
	}
	return value, nil
}

func (r *lsmRegionReader) IterCF(cf string) Iterator {
	iterator, err := r.reader.Iterator()
	if err != nil {
		return &lsmIterator{err: err}
	}
	return &lsmIterator{
		iterator: iterator,
		prefix:   EncodeCFPrefix(r.regionID, cf),
	}
}

func (r *lsmRegionReader) Close() { r.reader.Close() }

type lsmIterator struct {
	iterator *lsmtree.Iterator
	prefix   []byte
	err      error
}

func (it *lsmIterator) Seek(key []byte) {
	if it.err != nil {
		return
	}
	encoded := make([]byte, len(it.prefix)+len(key))
	copy(encoded, it.prefix)
	copy(encoded[len(it.prefix):], key)
	it.iterator.Seek(encoded)
}

func (it *lsmIterator) Valid() bool {
	if it.err != nil || it.iterator == nil || !it.iterator.Valid() {
		return false
	}
	item, err := it.iterator.Item()
	if err != nil {
		it.err = err
		return false
	}
	return bytes.HasPrefix(item.Key, it.prefix)
}

func (it *lsmIterator) Next() {
	if it.err == nil && it.iterator != nil {
		it.iterator.Next()
	}
}

func (it *lsmIterator) Item() (Item, error) {
	if it.err != nil {
		return Item{}, it.err
	}
	item, err := it.iterator.Item()
	if err != nil {
		return Item{}, err
	}
	return Item{Key: item.Key, Value: item.Value}, nil
}

func (it *lsmIterator) Close() {
	if it.iterator != nil {
		it.iterator.Close()
	}
}

func (it *lsmIterator) Status() error {
	if it.err != nil {
		return it.err
	}
	if it.iterator == nil {
		return nil
	}
	return it.iterator.Status()
}

const raftKeyPrefix = "raft/"

type lsmRaftStorage struct {
	db       *lsmtree.DB
	regionID uint64
}

func (s *lsmRaftStorage) regionPrefix() []byte {
	return []byte(raftKeyPrefix + strconv.FormatUint(s.regionID, 10) + "/")
}

func (s *lsmRaftStorage) hardStateKey() []byte {
	return append(s.regionPrefix(), []byte("hard_state")...)
}

func (s *lsmRaftStorage) snapshotKey() []byte {
	return append(s.regionPrefix(), []byte("snapshot")...)
}

func (s *lsmRaftStorage) entryPrefix() []byte {
	return append(s.regionPrefix(), []byte("entry/")...)
}

func (s *lsmRaftStorage) entryKey(index uint64) []byte {
	key := append([]byte(nil), s.entryPrefix()...)
	encoded := make([]byte, len(key)+8)
	copy(encoded, key)
	binary.BigEndian.PutUint64(encoded[len(key):], index)
	return encoded
}

func (s *lsmRaftStorage) SaveHardState(state raft.HardState) error {
	batch, err := s.db.NewBatch()
	if err != nil {
		return err
	}
	defer batch.Close()
	data := make([]byte, 24)
	binary.BigEndian.PutUint64(data[0:], state.Term)
	binary.BigEndian.PutUint64(data[8:], state.Vote)
	binary.BigEndian.PutUint64(data[16:], state.CommitIndex)
	if err := batch.Put(s.hardStateKey(), data); err != nil {
		return err
	}
	return s.db.Write(batch, true)
}

func (s *lsmRaftStorage) LoadHardState() (raft.HardState, error) {
	reader, err := s.db.Reader()
	if err != nil {
		return raft.HardState{}, err
	}
	defer reader.Close()
	data, found, err := reader.Get(s.hardStateKey())
	if err != nil || !found {
		return raft.HardState{}, err
	}
	if len(data) < 24 {
		return raft.HardState{}, nil
	}
	return raft.HardState{
		Term:        binary.BigEndian.Uint64(data[0:8]),
		Vote:        binary.BigEndian.Uint64(data[8:16]),
		CommitIndex: binary.BigEndian.Uint64(data[16:24]),
	}, nil
}

func (s *lsmRaftStorage) SaveEntries(entries []*raftpb.Entry) error {
	batch, err := s.db.NewBatch()
	if err != nil {
		return err
	}
	defer batch.Close()
	for _, entry := range entries {
		if entry == nil {
			continue
		}
		data, err := proto.Marshal(entry)
		if err != nil {
			return err
		}
		if err := batch.Put(s.entryKey(entry.Index), data); err != nil {
			return err
		}
	}
	return s.db.Write(batch, true)
}

func (s *lsmRaftStorage) Compact(index uint64) error {
	if index == 0 {
		return nil
	}
	return s.deleteEntries(func(entryIndex uint64) bool { return entryIndex < index }, s.entryKey(0))
}

func (s *lsmRaftStorage) TruncateFrom(index uint64) error {
	return s.deleteEntries(func(entryIndex uint64) bool { return entryIndex >= index }, s.entryKey(index))
}

func (s *lsmRaftStorage) deleteEntries(shouldDelete func(uint64) bool, start []byte) error {
	reader, err := s.db.Reader()
	if err != nil {
		return err
	}
	iterator, err := reader.Iterator()
	if err != nil {
		reader.Close()
		return err
	}
	iterator.Seek(start)
	keys := make([][]byte, 0)
	for iterator.Valid() {
		item, itemErr := iterator.Item()
		if itemErr != nil {
			iterator.Close()
			reader.Close()
			return itemErr
		}
		if !bytes.HasPrefix(item.Key, s.entryPrefix()) {
			break
		}
		if len(item.Key) < len(s.entryPrefix())+8 {
			break
		}
		entryIndex := binary.BigEndian.Uint64(item.Key[len(s.entryPrefix()):])
		if !shouldDelete(entryIndex) {
			break
		}
		keys = append(keys, item.Key)
		iterator.Next()
	}
	if err := iterator.Status(); err != nil {
		iterator.Close()
		reader.Close()
		return err
	}
	iterator.Close()
	reader.Close()

	batch, err := s.db.NewBatch()
	if err != nil {
		return err
	}
	defer batch.Close()
	for _, key := range keys {
		if err := batch.Erase(key); err != nil {
			return err
		}
	}
	return s.db.Write(batch, true)
}

func (s *lsmRaftStorage) LoadEntries(lo, hi uint64) ([]*raftpb.Entry, error) {
	reader, err := s.db.Reader()
	if err != nil {
		return nil, err
	}
	defer reader.Close()
	iterator, err := reader.Iterator()
	if err != nil {
		return nil, err
	}
	defer iterator.Close()
	iterator.Seek(s.entryKey(lo))
	entries := make([]*raftpb.Entry, 0)
	for iterator.Valid() {
		item, itemErr := iterator.Item()
		if itemErr != nil {
			return nil, itemErr
		}
		if !bytes.HasPrefix(item.Key, s.entryPrefix()) || len(item.Key) < len(s.entryPrefix())+8 {
			break
		}
		index := binary.BigEndian.Uint64(item.Key[len(s.entryPrefix()):])
		if index >= hi {
			break
		}
		entry := new(raftpb.Entry)
		if err := proto.Unmarshal(item.Value, entry); err != nil {
			return nil, err
		}
		entries = append(entries, entry)
		iterator.Next()
	}
	if err := iterator.Status(); err != nil {
		return nil, err
	}
	return entries, nil
}

func (s *lsmRaftStorage) SaveSnapshot(snapshot *raftpb.Snapshot) error {
	data, err := proto.Marshal(snapshot)
	if err != nil {
		return err
	}
	batch, err := s.db.NewBatch()
	if err != nil {
		return err
	}
	defer batch.Close()
	if err := batch.Put(s.snapshotKey(), data); err != nil {
		return err
	}
	return s.db.Write(batch, true)
}

func (s *lsmRaftStorage) LoadSnapshot() (*raftpb.Snapshot, error) {
	reader, err := s.db.Reader()
	if err != nil {
		return nil, err
	}
	defer reader.Close()
	data, found, err := reader.Get(s.snapshotKey())
	if err != nil || !found {
		return nil, err
	}
	snapshot := new(raftpb.Snapshot)
	if err := proto.Unmarshal(data, snapshot); err != nil {
		return nil, err
	}
	if snapshot.Index == 0 {
		return nil, nil
	}
	return snapshot, nil
}
