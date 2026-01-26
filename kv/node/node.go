package node

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/zbchi/mizu/kv/raftstore"
	"github.com/zbchi/mizu/kv/region"
	"github.com/zbchi/mizu/kv/storage"
	"github.com/zbchi/mizu/proto/raftkvpb"
)

// Config represents KVNode configuration
type Config struct {
	NodeID        uint64
	ClusterID     uint64
	RaftAddr      string
	StoragePath   string
	ElectionTick  int
	HeartbeatTick int
	Regions       []*region.Region
}

// Node represents Multi-Raft KV store node
type Node struct {
	cfg     *Config
	storage storage.Storage
	store   *raftstore.Store
	router  *Router
}

// New creates a new Node
func New(cfg *Config, storage storage.Storage) (*Node, error) {
	kn := &Node{
		cfg:     cfg,
		storage: storage,
		router:  NewRouter(),
	}

	// Create store
	kn.store = raftstore.NewStore(cfg.NodeID, kn)

	// Initialize peers
	if err := kn.initPeers(); err != nil {
		return nil, err
	}

	return kn, nil
}

// initPeers initializes all configured region peers
func (kn *Node) initPeers() error {
	if len(kn.cfg.Regions) == 0 {
		return errors.New("node config requires at least one region")
	}

	for _, reg := range kn.cfg.Regions {
		if reg == nil {
			return errors.New("node config contains nil region")
		}
		if reg.ID == 0 {
			slog.Error("Invalid region config", "reason", "region id must be non-zero")
			return errors.New("region id must be non-zero")
		}
		if len(reg.Peers) == 0 {
			slog.Error("Invalid region config", "region", reg.ID, "reason", "region must have at least one peer")
			return errors.New("region must have at least one peer")
		}

		raftStorage := kn.storage.RaftStorage(reg.ID)
		peer := raftstore.NewPeer(reg, kn.cfg.NodeID, raftStorage)

		if err := kn.store.AddPeer(peer); err != nil {
			return err
		}

		kn.router.AddRegion(reg)
	}
	return nil
}

// Start starts KVNode
func (kn *Node) Start() error {
	slog.Info("Starting KVNode", "node", kn.cfg.NodeID)
	kn.store.Start()
	return nil
}

// Stop stops KVNode
func (kn *Node) Stop() error {
	slog.Info("Stopping KVNode", "node", kn.cfg.NodeID)
	kn.store.Stop()
	return nil
}

func (kn *Node) regionForKey(key []byte) (*region.Region, error) {
	reg := kn.router.FindRegion(key)
	if reg != nil {
		return reg, nil
	}
	if kn.router.RegionCount() == 0 {
		return nil, raftstore.ErrRegionNotFound
	}
	return nil, raftstore.ErrKeyNotInRegion
}

// Write proposes a write batch through the Raft log.
func (kn *Node) Write(req *raftkvpb.RaftCmdRequest) (*raftkvpb.RaftCmdResponse, error) {
	reg, resp := kn.routeWriteBatch(req)
	if resp != nil {
		return resp, nil
	}

	resp, err := kn.store.Propose(reg.ID, req)
	if err != nil {
		if errors.Is(err, raftstore.ErrPeerNotFound) {
			return kn.errorResponse(req, reg, raftstore.ErrRegionNotFound), nil
		}
		return kn.errorResponse(req, reg, err), nil
	}
	return resp, nil
}

// SetTransport sets the transport for network communication
func (kn *Node) SetTransport(t raftstore.Transport) {
	kn.store.SetTransport(t)
}

// ApplyCommand applies a committed command to the region state machine.
func (kn *Node) ApplyCommand(regionID uint64, req *raftkvpb.RaftCmdRequest) error {
	if len(req.Requests) == 0 {
		return nil
	}

	var mods []storage.Modify

	for _, r := range req.Requests {
		switch r.CmdType {
		case raftkvpb.CmdType_Put:
			mods = append(mods, storage.Modify{
				Data: storage.Put{Key: r.Put.Key, Value: r.Put.Value, Cf: r.Put.Cf},
			})
		case raftkvpb.CmdType_Delete:
			mods = append(mods, storage.Modify{
				Data: storage.Delete{Key: r.Delete.Key, Cf: r.Delete.Cf},
			})
		}
	}

	if len(mods) > 0 {
		return kn.storage.RegionStorage(regionID).Write(mods)
	}
	return nil
}

// CreateSnapshot captures the business state of one region.
func (kn *Node) CreateSnapshot(regionID uint64) ([]byte, error) {
	return kn.storage.RegionStorage(regionID).CreateSnapshot()
}

// ApplySnapshot replaces the business state of one region.
func (kn *Node) ApplySnapshot(regionID uint64, data []byte) error {
	return kn.storage.RegionStorage(regionID).ApplySnapshot(data)
}

// Get performs a linearizable read using ReadIndex
func (kn *Node) Get(ctx context.Context, req *raftkvpb.RaftCmdRequest) (*raftkvpb.RaftCmdResponse, error) {
	readReq, resp := kn.readRequest(req, raftkvpb.CmdType_Get)
	if resp != nil {
		return resp, nil
	}

	reg, resp := kn.linearizableRead(ctx, req, readReq.Get.Key)
	if resp != nil {
		return resp, nil
	}

	reader, err := kn.storage.RegionStorage(reg.ID).Reader()
	if err != nil {
		return kn.errorResponse(req, reg, err), nil
	}
	defer reader.Close()

	value, err := reader.GetCF(readReq.Get.Cf, readReq.Get.Key)
	if err != nil {
		return kn.errorResponse(req, reg, err), nil
	}

	return kn.successResponse(req, reg, []*raftkvpb.Response{
		{
			CmdType: raftkvpb.CmdType_Get,
			Get:     &raftkvpb.GetResponse{Value: value},
		},
	}), nil
}

// Scan performs a linearizable range scan within a single region.
func (kn *Node) Scan(ctx context.Context, req *raftkvpb.RaftCmdRequest) (*raftkvpb.RaftCmdResponse, error) {
	readReq, resp := kn.readRequest(req, raftkvpb.CmdType_Scan)
	if resp != nil {
		return resp, nil
	}

	reg, resp := kn.linearizableRead(ctx, req, readReq.Scan.StartKey)
	if resp != nil {
		return resp, nil
	}

	reader, err := kn.storage.RegionStorage(reg.ID).Reader()
	if err != nil {
		return kn.errorResponse(req, reg, err), nil
	}
	defer reader.Close()

	iter := reader.IterCF(readReq.Scan.Cf)
	defer iter.Close()
	iter.Seek(readReq.Scan.StartKey)

	limit := int(readReq.Scan.Limit)
	if limit < 0 {
		limit = 0
	}

	pairs := make([]*raftkvpb.KvPair, 0, limit)
	for ; iter.Valid(); iter.Next() {
		item := iter.Item()
		encodedKey := item.KeyCopy(nil)
		// RegionStorage keys are prefixed by region/CF metadata; decode also filters out
		// entries that belong to another logical keyspace in the same Badger instance.
		userKey, ok := storage.DecodeUserKey(reg.ID, readReq.Scan.Cf, encodedKey)
		if !ok {
			continue
		}

		value, err := item.ValueCopy(nil)
		if err != nil {
			return kn.errorResponse(req, reg, err), nil
		}

		pairs = append(pairs, &raftkvpb.KvPair{
			Key:   userKey,
			Value: value,
		})

		if limit > 0 && len(pairs) >= limit {
			break
		}
	}

	return kn.successResponse(req, reg, []*raftkvpb.Response{
		{
			CmdType: raftkvpb.CmdType_Scan,
			Scan:    &raftkvpb.ScanResponse{Pairs: pairs},
		},
	}), nil
}

func (kn *Node) linearizableRead(ctx context.Context, req *raftkvpb.RaftCmdRequest, key []byte) (*region.Region, *raftkvpb.RaftCmdResponse) {
	reg, err := kn.regionForKey(key)
	if err != nil {
		return nil, kn.errorResponse(req, nil, err)
	}

	if err := kn.store.WaitLinearizableRead(ctx, reg.ID); err != nil {
		if errors.Is(err, raftstore.ErrPeerNotFound) {
			return reg, kn.errorResponse(req, reg, raftstore.ErrRegionNotFound)
		}
		return reg, kn.errorResponse(req, reg, err)
	}
	return reg, nil
}

func (kn *Node) routeWriteBatch(req *raftkvpb.RaftCmdRequest) (*region.Region, *raftkvpb.RaftCmdResponse) {
	if len(req.Requests) == 0 {
		return nil, kn.errorResponse(req, nil, errors.New("empty requests"))
	}

	var target *region.Region
	for idx, r := range req.Requests {
		key, err := writeKey(r)
		if err != nil {
			return nil, kn.errorResponse(req, nil, err)
		}

		reg, err := kn.regionForKey(key)
		if err != nil {
			return nil, kn.errorResponse(req, nil, err)
		}

		if target == nil {
			target = reg
			continue
		}

		// The current proposal path submits one Raft command to one peer, so a write batch
		// must stay within a single region until cross-region coordination exists.
		if target.ID != reg.ID {
			err := fmt.Errorf("request spans multiple regions: request %d key %q belongs to region %d [%q,%q), expected region %d [%q,%q)", idx, string(key), reg.ID, string(reg.StartKey), string(reg.EndKey), target.ID, string(target.StartKey), string(target.EndKey))
			return nil, kn.errorResponse(req, reg, err)
		}
	}

	return target, nil
}

func (kn *Node) readRequest(req *raftkvpb.RaftCmdRequest, expected raftkvpb.CmdType) (*raftkvpb.Request, *raftkvpb.RaftCmdResponse) {
	if len(req.Requests) != 1 {
		return nil, kn.errorResponse(req, nil, fmt.Errorf("%s requests must contain exactly one command", expected.String()))
	}
	if req.Requests[0].CmdType != expected {
		return nil, kn.errorResponse(req, nil, fmt.Errorf("expected %s request, got %s", expected.String(), req.Requests[0].CmdType.String()))
	}
	return req.Requests[0], nil
}

func (kn *Node) successResponse(req *raftkvpb.RaftCmdRequest, reg *region.Region, responses []*raftkvpb.Response) *raftkvpb.RaftCmdResponse {
	return kn.store.BuildResponse(req, regionID(reg), responses, nil)
}

func (kn *Node) errorResponse(req *raftkvpb.RaftCmdRequest, reg *region.Region, err error) *raftkvpb.RaftCmdResponse {
	return kn.store.BuildResponse(req, regionID(reg), nil, err)
}

func writeKey(r *raftkvpb.Request) ([]byte, error) {
	switch r.CmdType {
	case raftkvpb.CmdType_Put:
		if r.Put == nil {
			return nil, errors.New("put request payload is missing")
		}
		return r.Put.Key, nil
	case raftkvpb.CmdType_Delete:
		if r.Delete == nil {
			return nil, errors.New("delete request payload is missing")
		}
		return r.Delete.Key, nil
	default:
		return nil, fmt.Errorf("unsupported write command type %s", r.CmdType.String())
	}
}

func regionID(reg *region.Region) uint64 {
	if reg == nil {
		return 0
	}
	return reg.ID
}
