package raftstore

import (
	"fmt"
	"sort"
	"sync"
)

// peerRegistry owns the regionID to Peer mapping.
// Message delivery belongs to raftWorker, not to this registry.
type peerRegistry struct {
	mu    sync.RWMutex
	peers map[uint64]*Peer
}

func newPeerRegistry() *peerRegistry {
	return &peerRegistry{peers: make(map[uint64]*Peer)}
}

func (r *peerRegistry) register(peer *Peer) error {
	if peer == nil {
		return fmt.Errorf("peer is nil")
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	regionID := peer.RegionID()
	if _, exists := r.peers[regionID]; exists {
		return fmt.Errorf("peer for region %d already exists", regionID)
	}
	r.peers[regionID] = peer
	return nil
}

func (r *peerRegistry) get(regionID uint64) *Peer {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.peers[regionID]
}

func (r *peerRegistry) listRegionIDs() []uint64 {
	r.mu.RLock()
	defer r.mu.RUnlock()

	regionIDs := make([]uint64, 0, len(r.peers))
	for regionID := range r.peers {
		regionIDs = append(regionIDs, regionID)
	}
	sort.Slice(regionIDs, func(i, j int) bool {
		return regionIDs[i] < regionIDs[j]
	})
	return regionIDs
}

func (r *peerRegistry) clear() {
	r.mu.Lock()
	defer r.mu.Unlock()
	clear(r.peers)
}
