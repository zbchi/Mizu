package kvnode

import "errors"

// Read operation errors
var (
	// ErrNotLeader is returned when a read request is served by a follower
	ErrNotLeader = errors.New("not leader, cannot serve linearizable read")

	// ErrReadTimeout is returned when a read request times out
	ErrReadTimeout = errors.New("read timeout")

	// ErrNodeStopped is returned when the node is stopped
	ErrNodeStopped = errors.New("node stopped")
)
