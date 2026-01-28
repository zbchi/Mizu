package region

import (
	"bytes"
)

// PeerInfo identifies one node participating in a region's Raft group.
type PeerInfo struct {
	NodeID uint64
}

// Region represents a data partition.
type Region struct {
	ID       uint64
	StartKey []byte
	EndKey   []byte
	Peers    []PeerInfo
	Leader   PeerInfo
}

// Contains reports whether key belongs to the half-open region range.
func (r *Region) Contains(key []byte) bool {
	return bytes.Compare(key, r.StartKey) >= 0 &&
		(len(r.EndKey) == 0 || bytes.Compare(key, r.EndKey) < 0)
}
