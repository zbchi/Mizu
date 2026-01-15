package node

import (
	"context"
	"log/slog"
	"sync"

	"github.com/zbchi/linkv/kv/raftstore"
	"github.com/zbchi/linkv/kv/region"
	"github.com/zbchi/linkv/kv/storage"
	"github.com/zbchi/linkv/proto"
	"github.com/zbchi/linkv/proto/linkvpb"
	"github.com/zbchi/linkv/proto/raftkvpb"
)

// Config represents KVNode configuration
type Config struct {
	NodeID        uint64
	ClusterID     uint64
	RaftAddr      string
	StoragePath   string
	ElectionTick  int
	HeartbeatTick int
	Peers         []proto.PeerInfo
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
	kn.store.Init(kn.store, kn)

	// Initialize peers
	if err := kn.initPeers(); err != nil {
		return nil, err
	}

	return kn, nil
}

// initPeers initializes default region peer
func (kn *Node) initPeers() error {
	// Create default region (region 1) with full key range
	defaultRegion := &region.Region{
		ID:       1,
		StartKey: []byte{},
		EndKey:   []byte{},
		Peers:    kn.cfg.Peers,
		Leader:   kn.cfg.Peers[0],
	}

	// Add region to router
	kn.router.AddRegion(defaultRegion)

	// Create peer for region 1
	raftStorage := kn.storage.RaftStorage(defaultRegion.ID)
	peer := raftstore.NewPeer(defaultRegion, kn.cfg.NodeID, kn.closeCh, raftStorage)

	// Add peer to store
	kn.store.AddPeer(defaultRegion, peer)

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

// Put writes a command through Raft log
func (kn *Node) Put(req *raftkvpb.RaftCmdRequest) (*raftkvpb.RaftCmdResponse, error) {
	key := req.Requests[0].Put.Key
	slog.Info("Put request received", "node", kn.cfg.NodeID, "key", string(key))

	cb := &raftstore.Callback{Done: make(chan struct{})}
	cmd := &raftstore.RaftCmd{
		Request: req,
		Cb:      cb,
	}

	// Route key to region
	regionID := kn.router.Route(key)
	if regionID == 0 {
		slog.Error("No region found for key", "key", string(key))
		return nil, raftstore.ErrPeerNotFound
	}

	// Send command to the target region
	if err := kn.store.SendCmd(regionID, cmd); err != nil {
		slog.Error("Failed to send cmd", "error", err)
		return nil, err
	}

	slog.Info("Waiting for callback...")
	return cb.Wait()
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
func (kn *Node) ApplyCommand(req *raftkvpb.RaftCmdRequest) error {
	if len(req.Requests) == 0 {
		return nil
	}

	ctx := &linkvpb.Context{}
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
		return kn.storage.Write(ctx, mods)
	}
	return nil
}

// Get performs a linearizable read using ReadIndex
func (kn *Node) Get(ctx context.Context, req *raftkvpb.RaftCmdRequest) (*raftkvpb.RaftCmdResponse, error) {
	getReq := req.Requests[0].Get

	// Route key to region
	regionID := kn.router.Route(getReq.Key)
	if regionID == 0 {
		slog.Error("No region found for key", "key", string(getReq.Key))
		return nil, raftstore.ErrPeerNotFound
	}

	// Route to peer
	peer := kn.getPeer(regionID)
	if peer == nil {
		slog.Error("Peer not found for region", "regionID", regionID)
		return nil, raftstore.ErrNotLeader
	}

	// Get read index directly from peer
	readIndex := peer.ReadIndex()
	if readIndex == 0 {
		slog.Error("ReadIndex failed, node is not leader", "regionID", regionID)
		return nil, raftstore.ErrNotLeader
	}

	// Wait for appliedIndex to reach readIndex
	if err := peer.WaitForReadIndex(ctx, readIndex); err != nil {
		slog.Error("WaitForReadIndex failed", "error", err, "readIndex", readIndex)
		return nil, err
	}

	// Read from storage
	storageCtx := &linkvpb.Context{}
	reader, err := kn.storage.Reader(storageCtx)
	if err != nil {
		slog.Error("Failed to get storage reader", "error", err)
		return nil, err
	}
	defer reader.Close()

	value, err := reader.GetCF(getReq.Cf, getReq.Key)
	if err != nil {
		slog.Error("Failed to read from storage", "error", err, "key", string(getReq.Key))
		return nil, err
	}

	return &raftkvpb.RaftCmdResponse{
		Header: &raftkvpb.ResponseHeader{
			ClusterId: req.Header.ClusterId,
			NodeId:    kn.NodeID(),
			Success:   true,
		},
		Responses: []*raftkvpb.Response{
			{
				CmdType: raftkvpb.CmdType_Get,
				Get:     &raftkvpb.GetResponse{Value: value},
			},
		},
	}, nil
}
