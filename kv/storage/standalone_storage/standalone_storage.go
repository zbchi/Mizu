package standalonestorage

import (
	"github.com/dgraph-io/badger/v3"
	"github.com/zbchi/linkv/kv/config"
	"github.com/zbchi/linkv/kv/storage"
	"github.com/zbchi/linkv/proto/linkvpb"
	"github.com/zbchi/linkv/raft"
)

type StandaloneStorage struct {
	db           *badger.DB
	raftStorage  raft.RaftStorage
}

func NewStandaloneStorage(conf *config.Config) *StandaloneStorage {
	opts := badger.DefaultOptions(conf.DBPath)
	db, err := badger.Open(opts)
	if err != nil {
		panic(err)
	}
	return &StandaloneStorage{
		db:           db,
		raftStorage:  raft.NewBadgerRaftStorage(db),
	}
}

func (s *StandaloneStorage) Start() error {
	return nil
}

func (s *StandaloneStorage) Stop() error {
	return s.db.Close()
}

func (s *StandaloneStorage) RaftStorage() raft.RaftStorage {
	return s.raftStorage
}

func (s *StandaloneStorage) Reader(ctx *linkvpb.Context) (storage.StorageReader, error) {
	txn := s.db.NewTransaction(false)
	return &StandaloneReader{txn: txn}, nil
}

func (s *StandaloneStorage) Write(ctx *linkvpb.Context, batch []storage.Modify) error {

	return s.db.Update(func(txn *badger.Txn) error {
		for _, m := range batch {
			switch data := m.Data.(type) {
			case storage.Put:
				if err := txn.Set(storage.EncodeKey(data.Key, data.Cf), data.Value); err != nil {
					return err
				}
			case storage.Delete:
				if err := txn.Delete(storage.EncodeKey(data.Key, data.Cf)); err != nil {
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
	txn *badger.Txn
}

func (r *StandaloneReader) GetCF(cf string, key []byte) ([]byte, error) {
	item, err := r.txn.Get(storage.EncodeKey(key, cf))
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
	return NewBadgerIterator(it, cf)
}

func (r *StandaloneReader) Close() {
	r.txn.Discard()
}

type BadgerIterator struct {
	it *badger.Iterator
	cf string
}

func NewBadgerIterator(it *badger.Iterator, cf string) *BadgerIterator {
	return &BadgerIterator{
		it: it,
		cf: cf,
	}
}

func (it *BadgerIterator) Seek(key []byte) {
	it.it.Seek(storage.EncodeKey(key, it.cf))
}

func (it *BadgerIterator) Valid() bool {
	return it.it.Valid()
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
