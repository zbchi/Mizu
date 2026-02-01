package raftstore

import (
	"fmt"
	"sort"
	"sync"
)

// peerRegistry indexes local peers by region ID. Key-to-region routing belongs
// to kv/node.Router instead.
type peerRegistry struct {
	mu    sync.RWMutex
	peers map[uint64]*Peer
}

func newPeerRegistry() *peerRegistry {
	return &peerRegistry{peers: make(map[uint64]*Peer)}
}

func (r *peerRegistry) register(peer *Peer) error {
	if peer == nil {
		return fmt.Errorf("register peer: peer is nil")
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.peers[peer.RegionID()]; exists {
		return fmt.Errorf("register peer: region %d already exists", peer.RegionID())
	}
	r.peers[peer.RegionID()] = peer
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
	sort.Slice(regionIDs, func(i, j int) bool { return regionIDs[i] < regionIDs[j] })
	return regionIDs
}

func (r *peerRegistry) clear() {
	r.mu.Lock()
	defer r.mu.Unlock()
	clear(r.peers)
}
