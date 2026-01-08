package kvnode

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/zbchi/linkv/kv/kvnode/message"
	"github.com/zbchi/linkv/kv/region"
	"github.com/zbchi/linkv/kv/storage"
	"github.com/zbchi/linkv/proto"
	"github.com/zbchi/linkv/proto/linkvpb"
	"github.com/zbchi/linkv/proto/raftkvpb"
)

// Config represents the KVNode configuration
type Config struct {
	NodeID        uint64
	ClusterID     uint64
	RaftAddr      string
	StoragePath   string
	ElectionTick  int
	HeartbeatTick int
	Peers         []proto.PeerInfo
}

// KVNode represents the Multi-Raft KV store node
type KVNode struct {
	cfg         *Config
	storage     storage.Storage
	callbackMgr *CallbackManager
	peerRouter  *PeerRouter
	raftWorker  *raftWorker
	regionMap   *region.RegionMap
	transport   Transport
	closeCh     chan struct{}
	closeOnce   sync.Once

	sync.RWMutex
}

// NewKVNode creates a new KVNode
func NewKVNode(cfg *Config, store storage.Storage) (*KVNode, error) {
	kn := &KVNode{
		cfg:         cfg,
		storage:     store,
		regionMap:   region.NewRegionMap(),
		closeCh:     make(chan struct{}),
	}

	// Create callback manager
	kn.callbackMgr = NewCallbackManager(kn)

	// Create peer router
	kn.peerRouter = NewPeerRouter()

	// Initialize peers
	if err := kn.initPeers(); err != nil {
		return nil, err
	}

	return kn, nil
}

// initPeers initializes the default region peer
func (kn *KVNode) initPeers() error {
	// Create default region (region 1) with full key range
	defaultRegion := &region.Region{
		ID:       1,
		StartKey: []byte{},
		EndKey:   []byte{},
		Peers:    kn.cfg.Peers,
		Leader:   kn.cfg.Peers[0],
	}

	// Add to region map
	if err := kn.regionMap.AddRegion(defaultRegion); err != nil {
		return err
	}

	// Create peer for region 1
	raftStorage := kn.storage.RaftStorage()
	peer := NewPeer(defaultRegion, kn.cfg.NodeID, kn, raftStorage)

	// Register peer to router
	kn.peerRouter.Register(peer)

	return nil
}

// Start starts the KVNode
func (kn *KVNode) Start() error {
	slog.Info("Starting KVNode", "node", kn.cfg.NodeID)

	// Start storage
	if err := kn.storage.Start(); err != nil {
		return err
	}

	// Create worker context
	ctx := &WorkerContext{
		node:      kn,
		transport: kn.transport,
	}

	// Create and start raft worker
	kn.raftWorker = newRaftWorker(kn.peerRouter, ctx)
	kn.raftWorker.start(kn.closeCh)

	// Start ticker
	go kn.runTicker()

	return nil
}

// Stop stops the KVNode
func (kn *KVNode) Stop() error {
	slog.Info("Stopping KVNode", "node", kn.cfg.NodeID)

	kn.closeOnce.Do(func() {
		close(kn.closeCh)
	})

	// Stop raft worker
	if kn.raftWorker != nil {
		kn.raftWorker.stop()
	}

	// Close all peers
	kn.peerRouter.CloseAll()

	// Stop storage
	if kn.storage != nil {
		return kn.storage.Stop()
	}

	return nil
}

// getPeer returns peer by regionID
func (kn *KVNode) getPeer(regionID uint64) *Peer {
	ps := kn.peerRouter.Get(regionID)
	if ps == nil {
		return nil
	}
	return ps.peer
}

// runTicker runs the ticker for all peers
func (kn *KVNode) runTicker() {
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	// Send tick to region 1 (default region)
	for {
		select {
		case <-ticker.C:
			tickMsg := message.Msg{
				Type:     message.MsgTypeTick,
				RegionID: 1,
				Data:     nil,
			}
			kn.peerRouter.Send(1, tickMsg)
		case <-kn.closeCh:
			return
		}
	}
}

// Put writes a command through Raft log
func (kn *KVNode) Put(req *raftkvpb.RaftCmdRequest) (*raftkvpb.RaftCmdResponse, error) {
	cb := &Callback{Done: make(chan struct{})}
	cmd := &RaftCmd{
		Request: req,
		cb:      cb,
	}

	// Send command to region 1 (TODO: route by key)
	cmdMsg := message.Msg{
		Type:     message.MsgTypeRaftCmd,
		RegionID: 1,
		Data:     cmd,
	}

	if err := kn.peerRouter.Send(1, cmdMsg); err != nil {
		return nil, err
	}

	return cb.Wait()
}

// NodeID returns the current node ID
func (kn *KVNode) NodeID() uint64 {
	return kn.cfg.NodeID
}

// SetTransport sets the transport for network communication
func (kn *KVNode) SetTransport(t Transport) {
	kn.transport = t

	// Update worker context
	if kn.raftWorker != nil {
		kn.raftWorker.ctx.transport = t
	}

	// Start receiving messages from transport
	go kn.receiveLoop()
}

// receiveLoop receives messages from transport and forwards to peerRouter
func (kn *KVNode) receiveLoop() {
	recvCh := kn.transport.Receive()
	for msg := range recvCh {
		raftMsg := message.Msg{
			Type:     message.MsgTypeRaftMessage,
			RegionID: msg.RegionId,
			Data:     msg,
		}
		kn.peerRouter.Send(msg.RegionId, raftMsg)
	}
}

// CallbackMgr returns the callback manager
func (kn *KVNode) CallbackMgr() *CallbackManager {
	return kn.callbackMgr
}

// applyCommand applies a single command to storage
func (kn *KVNode) applyCommand(req *raftkvpb.RaftCmdRequest) error {
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
func (kn *KVNode) Get(ctx context.Context, req *raftkvpb.RaftCmdRequest) (*raftkvpb.RaftCmdResponse, error) {
	getReq := req.Requests[0].Get

	// Route to peer (currently only region 1)
	peer := kn.getPeer(1)
	if peer == nil {
		return nil, ErrNotLeader
	}

	// Get read index directly from peer
	readIndex, err := peer.ReadIndex(ctx)
	if err != nil || readIndex == 0 {
		return nil, ErrNotLeader
	}

	// Wait for appliedIndex to reach readIndex
	if err := peer.waitForReadIndex(ctx, readIndex); err != nil {
		return nil, err
	}

	// Read from storage
	storageCtx := &linkvpb.Context{}
	reader, err := kn.storage.Reader(storageCtx)
	if err != nil {
		return nil, err
	}
	defer reader.Close()

	value, err := reader.GetCF(getReq.Cf, getReq.Key)
	if err != nil {
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
