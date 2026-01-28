package storage

import (
	"bytes"

	badgerdb "github.com/dgraph-io/badger/v3"
	"github.com/zbchi/mizu/kv/storage/raftstorage"
	"github.com/zbchi/mizu/proto/raftpb"
	"github.com/zbchi/mizu/raft"
	"google.golang.org/protobuf/proto"
)

// BadgerStorage provides both region data and Raft persistence on one local DB.
type BadgerStorage struct {
	db *badgerdb.DB
}

func NewBadgerStorage(dbPath string) *BadgerStorage {
	db, err := badgerdb.Open(badgerdb.DefaultOptions(dbPath))
	if err != nil {
		panic(err)
	}
	return &BadgerStorage{db: db}
}

func (s *BadgerStorage) Start() error {
	return nil
}

func (s *BadgerStorage) Stop() error {
	return s.db.Close()
}

func (s *BadgerStorage) RaftStorage(regionID uint64) raft.RaftStorage {
	return raftstorage.NewStorage(s.db, regionID)
}

func (s *BadgerStorage) RegionStorage(regionID uint64) RegionStorage {
	return &badgerRegionStorage{db: s.db, regionID: regionID}
}

type badgerRegionStorage struct {
	db       *badgerdb.DB
	regionID uint64
}

func (s *badgerRegionStorage) Reader() (StorageReader, error) {
	txn := s.db.NewTransaction(false)
	return &badgerReader{txn: txn, regionID: s.regionID}, nil
}

func (s *badgerRegionStorage) Write(batch []Modify) error {
	return s.db.Update(func(txn *badgerdb.Txn) error {
		for _, modification := range batch {
			switch data := modification.Data.(type) {
			case Put:
				if err := txn.Set(EncodeKey(s.regionID, data.Key, data.Cf), data.Value); err != nil {
					return err
				}
			case Delete:
				if err := txn.Delete(EncodeKey(s.regionID, data.Key, data.Cf)); err != nil {
					return err
				}
			}
		}
		return nil
	})
}

func (s *badgerRegionStorage) CreateSnapshot() ([]byte, error) {
	snapshot := &raftpb.SnapshotData{}
	err := s.db.View(func(txn *badgerdb.Txn) error {
		it := txn.NewIterator(badgerdb.DefaultIteratorOptions)
		defer it.Close()

		prefix := RegionDataPrefix(s.regionID)
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()
			value, err := item.ValueCopy(nil)
			if err != nil {
				return err
			}
			snapshot.Kvs = append(snapshot.Kvs, &raftpb.KvPair{
				Key:   item.KeyCopy(nil),
				Value: value,
			})
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return proto.Marshal(snapshot)
}

func (s *badgerRegionStorage) ApplySnapshot(data []byte) error {
	if len(data) == 0 {
		return nil
	}

	var snapshot raftpb.SnapshotData
	if err := proto.Unmarshal(data, &snapshot); err != nil {
		return err
	}

	return s.db.Update(func(txn *badgerdb.Txn) error {
		prefix := RegionDataPrefix(s.regionID)
		it := txn.NewIterator(badgerdb.DefaultIteratorOptions)
		defer it.Close()

		for it.Seek(prefix); it.Valid(); it.Next() {
			key := it.Item().KeyCopy(nil)
			if !bytes.HasPrefix(key, prefix) {
				break
			}
			if err := txn.Delete(key); err != nil {
				return err
			}
		}

		for _, kv := range snapshot.Kvs {
			if err := txn.Set(kv.Key, kv.Value); err != nil {
				return err
			}
		}
		return nil
	})
}

type badgerReader struct {
	txn      *badgerdb.Txn
	regionID uint64
}

func (r *badgerReader) GetCF(cf string, key []byte) ([]byte, error) {
	item, err := r.txn.Get(EncodeKey(r.regionID, key, cf))
	if err == badgerdb.ErrKeyNotFound {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return item.ValueCopy(nil)
}

func (r *badgerReader) IterCF(cf string) Iterator {
	opts := badgerdb.DefaultIteratorOptions
	opts.PrefetchValues = true
	return newBadgerIterator(r.txn.NewIterator(opts), cf, r.regionID)
}

func (r *badgerReader) Close() {
	r.txn.Discard()
}

type badgerIterator struct {
	it       *badgerdb.Iterator
	cf       string
	regionID uint64
	prefix   []byte
}

func newBadgerIterator(it *badgerdb.Iterator, cf string, regionID uint64) *badgerIterator {
	return &badgerIterator{
		it:       it,
		cf:       cf,
		regionID: regionID,
		prefix:   EncodeCFPrefix(regionID, cf),
	}
}

func (it *badgerIterator) Seek(key []byte) {
	it.it.Seek(EncodeKey(it.regionID, key, it.cf))
}

func (it *badgerIterator) Valid() bool {
	return it.it.ValidForPrefix(it.prefix)
}

func (it *badgerIterator) Next() {
	it.it.Next()
}

func (it *badgerIterator) Item() *badgerdb.Item {
	return it.it.Item()
}

func (it *badgerIterator) Close() {
	it.it.Close()
}
