package raftstore

import "errors"

var (
	ErrPeerNotFound   = errors.New("peer not found")
	ErrRegionNotFound = errors.New("region not found")
	ErrKeyNotInRegion = errors.New("key not in region")
	ErrNotLeader      = errors.New("not leader")
)
