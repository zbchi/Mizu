package storage

import (
	"github.com/dgraph-io/badger/v3"
	"github.com/zbchi/linkv/proto/linkvpb"
	"github.com/zbchi/linkv/raft"
)

type Storage interface {
	Start() error
	Stop() error
	Reader(ctx *linkvpb.Context) (StorageReader, error)
	Write(ctx *linkvpb.Context, batch []Modify) error

	// RaftStorage returns the Raft state storage
	RaftStorage() raft.RaftStorage
}

type StorageReader interface {
	GetCF(cf string, key []byte) ([]byte, error)
	IterCF(cf string) Iterator
	Close()
}

type Modify struct {
	Data interface{}
}

type Put struct {
	Key   []byte
	Value []byte
	Cf    string
}

type Delete struct {
	Key []byte
	Cf  string
}

type Iterator interface {
	Seek(key []byte)
	Valid() bool
	Next()
	Item() *badger.Item
	Close()
}

func EncodeKey(key []byte, cf string) []byte {
	return []byte(cf + "_" + string(key))
}
