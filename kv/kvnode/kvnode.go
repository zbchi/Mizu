package kvnode

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/zbchi/linkv/kv/storage"
	"github.com/zbchi/linkv/proto/linkvpb"
	"github.com/zbchi/linkv/proto/raftkvpb"
	"github.com/zbchi/linkv/proto/raftpb"
	"github.com/zbchi/linkv/raft"
	"google.golang.org/protobuf/proto"
)

// Config represents the KVNode configuration
type Config struct {
	NodeID        uint64
	ClusterID     uint64
	RaftAddr      string
	StoragePath   string
	ElectionTick  int
	HeartbeatTick int
	Peers         []PeerInfo
}

// PeerInfo represents peer information
type PeerInfo struct {
	NodeID uint64
	Addr   string
}

// KVNode represents the Raft-based KV store node
type KVNode struct {
	cfg          *Config
	raftNode     *raft.RawNode
	storage      storage.Storage
	raftStorage  raft.RaftStorage  // Raft state storage (persistent)
	router       *Router

	// message channels
	raftCh    chan raftpb.Message
	cmdCh     chan *RaftCmd
	tickCh    chan struct{}
	closeCh   chan struct{}

	// apply state
	appliedIndex uint64
	sync.RWMutex

	// Read optimization: ReadIndex batching and wait queue
	readIndexBatcher *ReadIndexBatcher
	readWaitQueue   *ReadWaitQueue
}

// NewKVNode creates a new KVNode
func NewKVNode(cfg *Config, store storage.Storage) (*KVNode, error) {
	kn := &KVNode{
		cfg:          cfg,
		storage:      store,
		raftCh:       make(chan raftpb.Message, 1024),
		cmdCh:        make(chan *RaftCmd, 128),
		tickCh:       make(chan struct{}, 1),
		closeCh:      make(chan struct{}),
		appliedIndex: 0,
	}

	// Create router
	kn.router = NewRouter(kn)

	// Initialize read optimization
	kn.readWaitQueue = NewReadWaitQueue()
	kn.readIndexBatcher = NewReadIndexBatcher(kn)

	// Initialize Raft node
	if err := kn.initRaftNode(); err != nil {
		return nil, err
	}

	return kn, nil
}

// initRaftNode initializes the Raft node
func (kn *KVNode) initRaftNode() error {
	// Collect peer IDs
	peerIDs := make([]uint64, len(kn.cfg.Peers))
	for i, peer := range kn.cfg.Peers {
		peerIDs[i] = peer.NodeID
	}

	// Get persistent Raft storage from KV storage
	kn.raftStorage = kn.storage.RaftStorage()

	// Create Raft config
	raftCfg := raft.Config{
		ID:               kn.cfg.NodeID,
		Peers:            peerIDs,
		ElectionTimeout:  kn.cfg.ElectionTick,
		HeartbeatTimeout: kn.cfg.HeartbeatTick,
	}

	// Create RawNode
	kn.raftNode = raft.NewRawNode(raftCfg)

	return nil
}

// Start starts the KVNode
func (kn *KVNode) Start() error {
	log.Printf("Starting KVNode node %d", kn.cfg.NodeID)

	// Start storage
	if err := kn.storage.Start(); err != nil {
		return err
	}

	// Start worker goroutines
	go kn.runRaftLoop()
	go kn.runApplyLoop()
	go kn.runTicker()
	go kn.readIndexBatcher.run(kn.closeCh)

	return nil
}

// Stop stops the KVNode
func (kn *KVNode) Stop() error {
	log.Printf("Stopping KVNode node %d", kn.cfg.NodeID)

	close(kn.closeCh)

	// Stop Raft node
	if kn.raftNode != nil {
		kn.raftNode.Stop()
	}

	// Stop storage
	if kn.storage != nil {
		return kn.storage.Stop()
	}

	return nil
}

// runTicker runs the ticker for Raft
func (kn *KVNode) runTicker() {
	ticker := time.NewTicker(100 * time.Millisecond)
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

// runRaftLoop runs the main Raft loop
func (kn *KVNode) runRaftLoop() {
	ctx := context.Background()
	for {
		select {
		case msg := <-kn.raftCh:
			kn.raftNode.Step(ctx, &msg)
		case cmd := <-kn.cmdCh:
			kn.proposeCommand(cmd)
		case <-kn.tickCh:
			kn.raftNode.Tick()
		case <-kn.closeCh:
			return
		}
	}
}

// proposeCommand proposes a command to Raft
func (kn *KVNode) proposeCommand(cmd *RaftCmd) {
	ctx := context.Background()

	// Marshal the request
	data, err := proto.Marshal(cmd.Request)
	if err != nil {
		cmd.cb.Finish(nil, err)
		return
	}

	// Register callback BEFORE proposing to ensure we don't miss the Ready
	kn.router.registerCallback(cmd)

	// Propose to Raft
	if err := kn.raftNode.Propose(ctx, data); err != nil {
		// Propose failed, remove the registered callback
		kn.router.unregisterCallback(cmd)
		cmd.cb.Finish(nil, err)
		return
	}
}

// runApplyLoop applies committed entries
func (kn *KVNode) runApplyLoop() {
	readyc := kn.raftNode.Ready()
	for {
		select {
		case rd := <-readyc:
			// Step 1: Save HardState (term, vote, commit)
			if rd.HardState != nil && !rd.HardState.IsEmpty() {
				if err := kn.raftStorage.SaveHardState(*rd.HardState); err != nil {
					log.Printf("Failed to save hard state: %v", err)
					// Cannot advance without persisting hard state
					continue
				}
			}

			// Step 2: Save Entries to storage
			if len(rd.Entries) > 0 {
				if err := kn.raftStorage.SaveEntries(rd.Entries); err != nil {
					log.Printf("Failed to save entries: %v", err)
					// Cannot advance without persisting entries
					continue
				}
			}

			// Step 3: Save/Apply Snapshot
			if rd.Snapshot != nil {
				if err := kn.raftStorage.SaveSnapshot(rd.Snapshot); err != nil {
					log.Printf("Failed to save snapshot: %v", err)
					continue
				}
				if err := kn.raftStorage.ApplySnapshotData(rd.Snapshot.Data); err != nil {
					log.Printf("Failed to apply snapshot data: %v", err)
					continue
				}
			}

			// Step 4: Send messages to other nodes
			for _, msg := range rd.Messages {
				kn.sendMessage(msg)
			}

			// Step 5: Apply committed entries to state machine
			if len(rd.CommittedEntries) > 0 {
				kn.applyEntries(rd.CommittedEntries)
			}

			// Step 6: Advance Raft (must be done after all persistence)
			kn.raftNode.Advance()

		case <-kn.closeCh:
			return
		}
	}
}

// applyEntries applies committed entries to storage
func (kn *KVNode) applyEntries(entries []*raftpb.Entry) {
	for _, entry := range entries {
		kn.applyEntry(entry)
		kn.appliedIndex = entry.Index
	}
	// Notify waiting read requests that appliedIndex has advanced
	kn.notifyReadWaitQueue()
}

// applyEntry applies a single entry
func (kn *KVNode) applyEntry(entry *raftpb.Entry) {
	// In this simplified implementation, all entries are normal KV operations
	if len(entry.Data) == 0 {
		return
	}
	kn.processCommittedEntry(entry)
}

// processCommittedEntry processes a committed entry and notifies waiting client
func (kn *KVNode) processCommittedEntry(entry *raftpb.Entry) {
	// Trigger the callback waiting for this entry
	kn.router.triggerCallback(entry.Index, entry.Term, nil)
}

// sendMessage sends a Raft message
func (kn *KVNode) sendMessage(msg *raftpb.Message) {
	kn.router.Send(*msg)
}

// Propose proposes a command through Raft
func (kn *KVNode) Propose(req *raftkvpb.RaftCmdRequest) (*raftkvpb.RaftCmdResponse, error) {
	cb := NewCallback()
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

// Get performs a linearizable read using ReadIndex
// It doesn't write to Raft log, only confirms leadership and waits for apply
// ctx can be used to cancel the read or set a timeout
func (kn *KVNode) Get(ctx context.Context, cf string, key []byte) ([]byte, error) {
	// Check node state first
	select {
	case <-kn.closeCh:
		return nil, ErrNodeStopped
	default:
	}

	// Check context
	if ctx.Err() != nil {
		return nil, fmt.Errorf("context canceled before read: %w", ctx.Err())
	}

	// Step 1: Enqueue read request to batch
	req := kn.readIndexBatcher.enqueue(cf, key)

	// Step 2: Wait for readIndex to be assigned and appliedIndex to advance
	select {
	case <-req.done:
		// Check if this was a follower read (readIndex = 0 indicates failure)
		if req.readIndex == 0 {
			return nil, ErrNotLeader
		}
	case <-ctx.Done():
		return nil, fmt.Errorf("%w: %v", ErrReadTimeout, ctx.Err())
	case <-kn.closeCh:
		return nil, ErrNodeStopped
	}

	// Step 3: Read from state machine
	storageCtx := &linkvpb.Context{}
	reader, err := kn.storage.Reader(storageCtx)
	if err != nil {
		return nil, fmt.Errorf("create reader failed: %w", err)
	}
	defer reader.Close()

	value, err := reader.GetCF(cf, key)
	if err != nil {
		return nil, fmt.Errorf("storage get failed: %w", err)
	}

	return value, nil
}

// notifyReadWaitQueue notifies the read wait queue when appliedIndex advances
// This should be called after applying entries
func (kn *KVNode) notifyReadWaitQueue() {
	kn.RLock()
	applied := kn.appliedIndex
	kn.RUnlock()

	kn.readWaitQueue.Notify(applied)
}
