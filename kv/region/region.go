package region

import (
	"bytes"

	"github.com/zbchi/mizu/proto"
)

// Region represents a data partition.
type Region struct {
	ID       uint64
	StartKey []byte
	EndKey   []byte
	Peers    []proto.PeerInfo
	Leader   proto.PeerInfo
}

// Contains reports whether key belongs to the half-open region range.
func (r *Region) Contains(key []byte) bool {
	return bytes.Compare(key, r.StartKey) >= 0 &&
		(len(r.EndKey) == 0 || bytes.Compare(key, r.EndKey) < 0)
}
