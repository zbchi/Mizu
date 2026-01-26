package storage

import (
	"bytes"
	"strconv"

	"github.com/dgraph-io/badger/v3"
	"github.com/zbchi/mizu/raft"
)

const KeyData = "data/"

type Storage interface {
	Start() error
	Stop() error
	RegionStorage(regionID uint64) RegionStorage

	// RaftStorage returns the Raft state storage for a region.
	RaftStorage(regionID uint64) raft.RaftStorage
}

type RegionStorage interface {
	Reader() (StorageReader, error)
	Write(batch []Modify) error
	CreateSnapshot() ([]byte, error)
	ApplySnapshot(data []byte) error
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

func RegionDataPrefix(regionID uint64) []byte {
	return []byte(KeyData + strconv.FormatUint(regionID, 10) + "/")
}

func EncodeCFPrefix(regionID uint64, cf string) []byte {
	base := RegionDataPrefix(regionID)
	prefix := make([]byte, len(base)+len(cf)+1)
	copy(prefix, base)
	copy(prefix[len(base):], cf)
	prefix[len(base)+len(cf)] = '/'
	return prefix
}

func EncodeKey(regionID uint64, key []byte, cf string) []byte {
	prefix := EncodeCFPrefix(regionID, cf)
	encoded := make([]byte, len(prefix)+len(key))
	copy(encoded, prefix)
	copy(encoded[len(prefix):], key)
	return encoded
}

func DecodeUserKey(regionID uint64, cf string, encodedKey []byte) ([]byte, bool) {
	prefix := EncodeCFPrefix(regionID, cf)
	if !bytes.HasPrefix(encodedKey, prefix) {
		return nil, false
	}

	userKey := make([]byte, len(encodedKey)-len(prefix))
	copy(userKey, encodedKey[len(prefix):])
	return userKey, true
}
