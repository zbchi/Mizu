package node

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"

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
	cfg       *Config
	storage   storage.Storage
	store     *raftstore.Store
	router    *Router
	transport raftstore.Transport
	closeCh   chan struct{}
	closeOnce sync.Once

	sync.RWMutex
}

// New creates a new Node
func New(cfg *Config, storage storage.Storage) (*Node, error) {
	kn := &Node{
		cfg:     cfg,
		storage: storage,
		router:  NewRouter(),
		closeCh: make(chan struct{}),
	}

	// Create store
	kn.store = raftstore.NewStore(cfg.NodeID, storage, kn.closeCh)

	// Initialize store with callback manager and command applier
	kn.store.Init(kn)

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
		peer := raftstore.NewPeer(reg, kn.cfg.NodeID, kn.closeCh, raftStorage)

		if err := kn.store.AddPeer(reg, peer); err != nil {
			return err
		}

		kn.router.AddRegion(reg)
	}
	return nil
}

// Start starts KVNode
func (kn *Node) Start() error {
	slog.Info("Starting KVNode", "node", kn.cfg.NodeID)

	// Start storage
	if err := kn.storage.Start(); err != nil {
		return err
	}

	// Start store (includes raft worker and ticker)
	kn.store.Start()

	return nil
}

// Stop stops KVNode
func (kn *Node) Stop() error {
	slog.Info("Stopping KVNode", "node", kn.cfg.NodeID)

	kn.closeOnce.Do(func() {
		close(kn.closeCh)
	})

	// Stop store (includes raft worker and peers)
	kn.store.Stop()

	// Stop storage
	if kn.storage != nil {
		return kn.storage.Stop()
	}

	return nil
}

// getPeer returns peer by regionID
func (kn *Node) getPeer(regionID uint64) *raftstore.Peer {
	return kn.store.GetPeer(regionID)
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

	cb := &raftstore.Callback{Done: make(chan struct{})}
	cmd := &raftstore.RaftCmd{
		Request: req,
		Cb:      cb,
	}

	if err := kn.store.SendCmd(reg.ID, cmd); err != nil {
		if errors.Is(err, raftstore.ErrPeerNotFound) {
			return kn.errorResponse(req, reg, raftstore.ErrRegionNotFound), nil
		}
		return kn.errorResponse(req, reg, err), nil
	}

	resp, err := cb.Wait()
	if err != nil && resp != nil {
		return resp, nil
	}
	return resp, err
}

// NodeID returns current node ID
func (kn *Node) NodeID() uint64 {
	return kn.cfg.NodeID
}

// SetTransport sets the transport for network communication
func (kn *Node) SetTransport(t raftstore.Transport) {
	kn.transport = t
	kn.store.SetTransport(t)
}

// CallbackMgr returns callback manager
func (kn *Node) CallbackMgr() *raftstore.CallbackManager {
	return kn.store.CallbackMgr()
}

// ApplyCommand applies a raft command to storage (implements CommandApplier interface)
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

// Get performs a linearizable read using ReadIndex
func (kn *Node) Get(ctx context.Context, req *raftkvpb.RaftCmdRequest) (*raftkvpb.RaftCmdResponse, error) {
	readReq, resp := kn.readRequest(req, raftkvpb.CmdType_Get)
	if resp != nil {
		return resp, nil
	}

	reg, _, resp := kn.linearizableRead(ctx, req, readReq.Get.Key)
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

	reg, _, resp := kn.linearizableRead(ctx, req, readReq.Scan.StartKey)
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

func (kn *Node) linearizableRead(ctx context.Context, req *raftkvpb.RaftCmdRequest, key []byte) (*region.Region, *raftstore.Peer, *raftkvpb.RaftCmdResponse) {
	reg, err := kn.regionForKey(key)
	if err != nil {
		return nil, nil, kn.errorResponse(req, nil, err)
	}

	peer := kn.getPeer(reg.ID)
	if peer == nil {
		return reg, nil, kn.errorResponse(req, reg, raftstore.ErrRegionNotFound)
	}

	// ReadIndex only succeeds on the current leader. Waiting for appliedIndex to catch up
	// lets the final storage read observe at least the leader's committed state.
	readIndex := peer.ReadIndex()
	if readIndex == 0 {
		return reg, peer, kn.errorResponse(req, reg, raftstore.ErrNotLeader)
	}

	if err := peer.WaitForReadIndex(ctx, readIndex); err != nil {
		return reg, peer, kn.errorResponse(req, reg, err)
	}

	return reg, peer, nil
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
	return raftstore.BuildResponse(req, kn.responseMeta(req, reg), responses, nil)
}

func (kn *Node) errorResponse(req *raftkvpb.RaftCmdRequest, reg *region.Region, err error) *raftkvpb.RaftCmdResponse {
	return raftstore.BuildResponse(req, kn.responseMeta(req, reg), nil, err)
}

func (kn *Node) responseMeta(req *raftkvpb.RaftCmdRequest, reg *region.Region) raftstore.ResponseMeta {
	clusterID := uint64(0)
	if req != nil && req.Header != nil {
		clusterID = req.Header.ClusterId
	}
	if reg == nil {
		return raftstore.ResponseMeta{
			ClusterID: clusterID,
			NodeID:    kn.NodeID(),
		}
	}

	meta := kn.store.ResponseMeta(clusterID, reg.ID)
	if meta.RegionID == 0 {
		// Fall back to static router metadata when the store cannot surface live peer state yet.
		return raftstore.MetaFromRegion(clusterID, kn.NodeID(), reg, 0)
	}
	return meta
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
