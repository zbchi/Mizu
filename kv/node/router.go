package node

import (
	"sync"

	"github.com/zbchi/linkv/kv/region"
)

// Router manages key → regionID routing for Node.
// It uses linear scan for simplicity. Future enhancements may include:
// - Region Split support
// - Region Merge support
// - BTree/RangeTree for O(log n) lookups
type Router struct {
	mu      sync.RWMutex
	regions []*region.Region
}

// NewRouter creates a new Router instance.
func NewRouter() *Router {
	return &Router{
		regions: make([]*region.Region, 0),
	}
}

// AddRegion adds a region to the router.
// The region is appended to the list for linear scan routing.
func (r *Router) AddRegion(region *region.Region) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.regions = append(r.regions, region)
}

// RemoveRegion removes a region by ID from the router.
func (r *Router) RemoveRegion(regionID uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()

	for i, reg := range r.regions {
		if reg.ID == regionID {
			r.regions = append(r.regions[:i], r.regions[i+1:]...)
			break
		}
	}
}

// Route returns the regionID that contains the given key.
// It uses linear scan to find the first region where StartKey <= key < EndKey.
// Returns 0 if no region contains the key.
func (r *Router) Route(key []byte) uint64 {
	r.mu.RLock()
	defer r.mu.RUnlock()

	for _, reg := range r.regions {
		if reg.Contains(key) {
			return reg.ID
		}
	}

	return 0
}
