package kvnode

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/zbchi/linkv/kv/region"
	"github.com/zbchi/linkv/kv/storage"
	"github.com/zbchi/linkv/proto"
	"github.com/zbchi/linkv/proto/linkvpb"
	"github.com/zbchi/linkv/proto/raftkvpb"
	"github.com/zbchi/linkv/proto/raftpb"
	"github.com/zbchi/linkv/raft"
	protov2 "google.golang.org/protobuf/proto"
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
	cfg       *Config
	storage   storage.Storage
	router    *Router
	peers     map[uint64]*Peer // regionID -> Peer
	regionMap *region.RegionMap

	// message channels
	raftCh  chan raftpb.Message
	cmdCh   chan *RaftCmd
	tickCh  chan struct{}
	closeCh chan struct{}

	sync.RWMutex

	// Read optimization: ReadIndex batching and wait queue
	readIndexBatcher *ReadIndexBatcher
	readWaitQueue    *ReadWaitQueue
}

// NewKVNode creates a new KVNode
func NewKVNode(cfg *Config, store storage.Storage) (*KVNode, error) {
	kn := &KVNode{
		cfg:       cfg,
		storage:   store,
		raftCh:    make(chan raftpb.Message, 1024),
		cmdCh:     make(chan *RaftCmd, 128),
		tickCh:    make(chan struct{}, 1),
		closeCh:   make(chan struct{}),
		peers:     make(map[uint64]*Peer),
		regionMap: region.NewRegionMap(),
	}

	// Create router
	kn.router = NewRouter(kn)

	// Initialize peers
	if err := kn.initPeers(); err != nil {
		return nil, err
	}

	// Initialize read optimization
	kn.readWaitQueue = &ReadWaitQueue{}
	kn.readIndexBatcher = NewReadIndexBatcher(kn)

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

	kn.peers[defaultRegion.ID] = peer

	return nil
}

// Start starts the KVNode
func (kn *KVNode) Start() error {
	slog.Info("Starting KVNode", "node", kn.cfg.NodeID)

	// Start storage
	if err := kn.storage.Start(); err != nil {
		return err
	}

	// Start worker goroutines
	go kn.runRaftLoop()
	go kn.runTicker()
	go kn.readIndexBatcher.run(kn.closeCh)

	return nil
}

// Stop stops the KVNode
func (kn *KVNode) Stop() error {
	slog.Info("Stopping KVNode", "node", kn.cfg.NodeID)

	close(kn.closeCh)

	// Stop all peers
	kn.RLock()
	for _, peer := range kn.peers {
		peer.Stop()
	}
	kn.RUnlock()

	// Stop storage
	if kn.storage != nil {
		return kn.storage.Stop()
	}

	return nil
}

// getPeer returns peer by regionID
func (kn *KVNode) getPeer(regionID uint64) *Peer {
	kn.RLock()
	defer kn.RUnlock()
	return kn.peers[regionID]
}

// runTicker runs the ticker for all peers
func (kn *KVNode) runTicker() {
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			select {
			case kn.tickCh <- struct{}{}:
			default:
			}
		case <-kn.closeCh:
			return
		}
	}
}

// runRaftLoop runs the main Raft loop for all peers (TiKV style)
func (kn *KVNode) runRaftLoop() {
	ctx := context.Background()
	for {
		select {
		case <-kn.tickCh:
			// Tick all peers
			kn.RLock()
			for _, peer := range kn.peers {
				peer.Tick()
			}
			kn.RUnlock()

		case msg := <-kn.raftCh:
			// Route message to peer by regionID
			peer := kn.getPeer(msg.RegionId)
			if peer != nil {
				peer.Step(ctx, &msg)
			}

		case cmd := <-kn.cmdCh:
			kn.proposeCommand(cmd)

		case <-kn.closeCh:
			return

		default:
			// No pending events, check and process ready for all peers
			kn.RLock()
			for _, peer := range kn.peers {
				if !peer.HasReady() {
					continue
				}
				rd := <-peer.Ready()
				if err := kn.handleReady(peer, rd); err != nil {
					slog.Error("Failed to handle ready", "region", peer.RegionID(), "error", err)
					continue
				}
				peer.Advance()
			}
			kn.RUnlock()
		}
	}
}

// handleReady processes raft ready state for a peer
func (kn *KVNode) handleReady(peer *Peer, rd raft.Ready) error {
	// Save HardState
	if rd.HardState != nil && !rd.HardState.IsEmpty() {
		if err := peer.raftStorage.SaveHardState(*rd.HardState); err != nil {
			return err
		}
	}

	// Save Entries
	if len(rd.Entries) > 0 {
		if err := peer.raftStorage.SaveEntries(rd.Entries); err != nil {
			return err
		}
	}

	// Save/Apply Snapshot
	if rd.Snapshot != nil {
		if err := peer.raftStorage.SaveSnapshot(rd.Snapshot); err != nil {
			return err
		}
		if err := peer.raftStorage.ApplySnapshotData(rd.Snapshot.Data); err != nil {
			return err
		}
	}

	// Send messages
	for _, msg := range rd.Messages {
		msg.RegionId = peer.RegionID()
		kn.router.Send(*msg)
	}

	// Apply committed entries
	if len(rd.CommittedEntries) > 0 {
		kn.applyPeerEntries(peer, rd.CommittedEntries)
	}

	return nil
}

// proposeCommand proposes a command to the correct region peer
func (kn *KVNode) proposeCommand(cmd *RaftCmd) {
	ctx := context.Background()

	// Route to region peer (currently all keys go to region 1)
	peer := kn.getPeer(1) // TODO: route by key
	if peer == nil {
		cmd.cb.Finish(nil, ErrNotLeader)
		return
	}

	// Marshal the request
	data, err := protov2.Marshal(cmd.Request)
	if err != nil {
		cmd.cb.Finish(nil, err)
		return
	}

	// Register callback BEFORE proposing
	kn.router.registerCallback(cmd, peer.RegionID())

	// Propose to Raft
	if err := peer.Propose(ctx, data); err != nil {
		kn.router.unregisterCallback(cmd, peer.RegionID())
		cmd.cb.Finish(nil, err)
		return
	}
}

// applyPeerEntries applies committed entries for a specific peer
func (kn *KVNode) applyPeerEntries(peer *Peer, entries []*raftpb.Entry) {
	for _, entry := range entries {
		kn.applyPeerEntry(peer, entry)
		peer.appliedIndex = entry.Index
	}
	// Notify waiting read requests
	kn.notifyReadWaitQueueForPeer(peer.RegionID())
}

// applyPeerEntry applies a single entry for a specific peer
func (kn *KVNode) applyPeerEntry(peer *Peer, entry *raftpb.Entry) {
	if len(entry.Data) == 0 {
		return
	}

	var req raftkvpb.RaftCmdRequest
	if err := protov2.Unmarshal(entry.Data, &req); err != nil {
		kn.router.triggerCallbackForRegion(peer.RegionID(), entry.Index, entry.Term, err)
		return
	}

	if err := kn.applyCommand(&req); err != nil {
		kn.router.triggerCallbackForRegion(peer.RegionID(), entry.Index, entry.Term, err)
		return
	}

	kn.router.triggerCallbackForRegion(peer.RegionID(), entry.Index, entry.Term, nil)
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

// Put writes a command through Raft log
func (kn *KVNode) Put(req *raftkvpb.RaftCmdRequest) (*raftkvpb.RaftCmdResponse, error) {
	cb := &Callback{Done: make(chan struct{})}
	cmd := &RaftCmd{
		Request: req,
		cb:      cb,
	}

	select {
	case kn.cmdCh <- cmd:
	case <-kn.closeCh:
		return nil, context.Canceled
	}

	return cb.Wait()
}

// NodeID returns the current node ID
func (kn *KVNode) NodeID() uint64 {
	return kn.cfg.NodeID
}

// Router returns the router for setting transport
func (kn *KVNode) Router() *Router {
	return kn.router
}

// Get performs a linearizable read using ReadIndex
func (kn *KVNode) Get(ctx context.Context, req *raftkvpb.RaftCmdRequest) (*raftkvpb.RaftCmdResponse, error) {
	getReq := req.Requests[0].Get

	// Enqueue and wait for ReadIndex
	readReq := kn.readIndexBatcher.enqueue(getReq.Cf, getReq.Key)

	select {
	case <-readReq.done:
		if readReq.readIndex == 0 {
			return nil, ErrNotLeader
		}
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-kn.closeCh:
		return nil, ErrNodeStopped
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

// notifyReadWaitQueueForPeer notifies the read wait queue when a peer's appliedIndex advances
func (kn *KVNode) notifyReadWaitQueueForPeer(regionID uint64) {
	peer := kn.getPeer(regionID)
	if peer != nil {
		kn.readWaitQueue.Notify(peer.appliedIndex)
	}
}
