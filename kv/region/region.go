package region

import (
	"bytes"
	"fmt"
	"sort"
	"sync"

	"github.com/zbchi/linkv/proto"
)

// Region represents a data partition
type Region struct {
	ID       uint64           // region id
	StartKey []byte           // start key (inclusive)
	EndKey   []byte           // end key (exclusive)
	Peers    []proto.PeerInfo // peer replicas
	Leader   proto.PeerInfo   // leader peer
}

// Contains checks if key is in this region's range
func (r *Region) Contains(key []byte) bool {
	return bytes.Compare(key, r.StartKey) >= 0 &&
		(len(r.EndKey) == 0 || bytes.Compare(key, r.EndKey) < 0)
}

// RegionMap manages multiple regions
type RegionMap struct {
	mu      sync.RWMutex
	regions []*Region // sorted by StartKey for binary search
	byID    map[uint64]*Region
}

// NewRegionMap creates a new RegionMap
func NewRegionMap() *RegionMap {
	return &RegionMap{
		regions: make([]*Region, 0),
		byID:    make(map[uint64]*Region),
	}
}

// FindRegion returns the region containing the key, or nil if not found
func (rm *RegionMap) FindRegion(key []byte) *Region {
	rm.mu.RLock()
	defer rm.mu.RUnlock()

	// binary search
	i := sort.Search(len(rm.regions), func(i int) bool {
		return bytes.Compare(key, rm.regions[i].StartKey) < 0
	})

	// check previous region
	if i > 0 {
		region := rm.regions[i-1]
		if region.Contains(key) {
			return region
		}
	}

	// check current position
	if i < len(rm.regions) {
		region := rm.regions[i]
		if region.Contains(key) {
			return region
		}
	}

	return nil
}

// AddRegion adds a new region
func (rm *RegionMap) AddRegion(region *Region) error {
	rm.mu.Lock()
	defer rm.mu.Unlock()

	// check duplicate id
	if _, exists := rm.byID[region.ID]; exists {
		return fmt.Errorf("region %d already exists", region.ID)
	}

	rm.byID[region.ID] = region

	// insert into sorted slice
	i := sort.Search(len(rm.regions), func(i int) bool {
		return bytes.Compare(region.StartKey, rm.regions[i].StartKey) < 0
	})

	rm.regions = append(rm.regions, nil)
	copy(rm.regions[i+1:], rm.regions[i:])
	rm.regions[i] = region

	return nil
}

// UpdateRegion updates an existing region's metadata
func (rm *RegionMap) UpdateRegion(region *Region) error {
	rm.mu.Lock()
	defer rm.mu.Unlock()

	if _, exists := rm.byID[region.ID]; !exists {
		return fmt.Errorf("region %d not found", region.ID)
	}

	rm.byID[region.ID] = region

	for i, r := range rm.regions {
		if r.ID == region.ID {
			rm.regions[i] = region
			break
		}
	}

	return nil
}

// GetRegionByID returns region by id
func (rm *RegionMap) GetRegionByID(id uint64) *Region {
	rm.mu.RLock()
	defer rm.mu.RUnlock()
	return rm.byID[id]
}

// RemoveRegion removes a region
func (rm *RegionMap) RemoveRegion(id uint64) error {
	rm.mu.Lock()
	defer rm.mu.Unlock()

	if _, exists := rm.byID[id]; !exists {
		return fmt.Errorf("region %d not found", id)
	}

	delete(rm.byID, id)

	for i, r := range rm.regions {
		if r.ID == id {
			rm.regions = append(rm.regions[:i], rm.regions[i+1:]...)
			break
		}
	}

	return nil
}

// ListRegions returns a snapshot of all regions
func (rm *RegionMap) ListRegions() []*Region {
	rm.mu.RLock()
	defer rm.mu.RUnlock()

	regions := make([]*Region, len(rm.regions))
	copy(regions, rm.regions)
	return regions
}
