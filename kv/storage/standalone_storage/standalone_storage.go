package standalonestorage

import (
	"github.com/dgraph-io/badger/v3"
	"github.com/zbchi/mizu/kv/config"
	"github.com/zbchi/mizu/kv/storage"
	"github.com/zbchi/mizu/kv/storage/raftstorage"
	raftbadger "github.com/zbchi/mizu/raft"
)

type StandaloneStorage struct {
	db *badger.DB
}

func NewStandaloneStorage(conf *config.Config) *StandaloneStorage {
	opts := badger.DefaultOptions(conf.DBPath)
	db, err := badger.Open(opts)
	if err != nil {
		panic(err)
	}
	return &StandaloneStorage{
		db: db,
	}
}

func (s *StandaloneStorage) Start() error {
	return nil
}

func (s *StandaloneStorage) Stop() error {
	return s.db.Close()
}

func (s *StandaloneStorage) RaftStorage(regionID uint64) raftbadger.RaftStorage {
	return raftstorage.NewStorage(s.db, regionID)
}

func (s *StandaloneStorage) RegionStorage(regionID uint64) storage.RegionStorage {
	return &regionStorage{
		db:       s.db,
		regionID: regionID,
	}
}

type regionStorage struct {
	db       *badger.DB
	regionID uint64
}

func (s *regionStorage) Reader() (storage.StorageReader, error) {
	txn := s.db.NewTransaction(false)
	return &StandaloneReader{txn: txn, regionID: s.regionID}, nil
}

func (s *regionStorage) Write(batch []storage.Modify) error {
	return s.db.Update(func(txn *badger.Txn) error {
		for _, m := range batch {
			switch data := m.Data.(type) {
			case storage.Put:
				if err := txn.Set(storage.EncodeKey(s.regionID, data.Key, data.Cf), data.Value); err != nil {
					return err
				}
			case storage.Delete:
				if err := txn.Delete(storage.EncodeKey(s.regionID, data.Key, data.Cf)); err != nil {
					return err
				}
			default:
				//ignore
				continue
			}
		}
		return nil
	})

}

type StandaloneReader struct {
	txn      *badger.Txn
	regionID uint64
}

func (r *StandaloneReader) GetCF(cf string, key []byte) ([]byte, error) {
	item, err := r.txn.Get(storage.EncodeKey(r.regionID, key, cf))
	if err == badger.ErrKeyNotFound {
		return nil, nil
	} else if err != nil {
		return nil, err
	}
	return item.ValueCopy(nil)
}

func (r *StandaloneReader) IterCF(cf string) storage.Iterator {
	opts := badger.DefaultIteratorOptions
	opts.PrefetchValues = true
	it := r.txn.NewIterator(opts)
	return NewBadgerIterator(it, cf, r.regionID)
}

func (r *StandaloneReader) Close() {
	r.txn.Discard()
}

type BadgerIterator struct {
	it       *badger.Iterator
	cf       string
	regionID uint64
	prefix   []byte
}

func NewBadgerIterator(it *badger.Iterator, cf string, regionID uint64) *BadgerIterator {
	return &BadgerIterator{
		it:       it,
		cf:       cf,
		regionID: regionID,
		prefix:   storage.EncodeCFPrefix(regionID, cf),
	}
}

func (it *BadgerIterator) Seek(key []byte) {
	it.it.Seek(storage.EncodeKey(it.regionID, key, it.cf))
}

func (it *BadgerIterator) Valid() bool {
	return it.it.ValidForPrefix(it.prefix)
}

func (it *BadgerIterator) Next() {
	it.it.Next()
}

func (it *BadgerIterator) Item() *badger.Item {
	return it.it.Item()
}

func (it *BadgerIterator) Close() {
	it.it.Close()
}
