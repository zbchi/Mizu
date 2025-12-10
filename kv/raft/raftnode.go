package raft

import (
	"context"
	"time"

	"github.com/zbchi/linkv/proto/raftpb"
)

type RaftNode struct {
	node      Node
	storage   RaftStorage
	transport Transport

	//输出channel
	commitC chan<- *raftpb.Entry
	errorC  chan<- error

	ctx    context.Context
	cancel context.CancelFunc
}

type Transport interface {
	Send(msg *raftpb.Message) error
	Receive() <-chan *raftpb.Message
	Close() error
}

type RaftNodeConfig struct {
	ID        uint64
	Peers     []uint64
	Storage   RaftStorage
	Transport Transport
	CommitC   chan<- *raftpb.Entry
	ErrorC    chan<- error
}

func NewRaftNode(cfg RaftNodeConfig) (*RaftNode, error) {
	ctx, cancel := context.WithCancel(context.Background())

	raft := NewRaft(Config{
		ID:    cfg.ID,
		Peers: cfg.Peers,
	})

	if err := restoreFromStorage(raft, cfg.Storage); err != nil {
		cancel()
		return nil, err
	}

	node := StartNodeWithRaft(raft)

	return &RaftNode{
		node:      node,
		storage:   cfg.Storage,
		transport: cfg.Transport,
		commitC:   cfg.CommitC,
		errorC:    cfg.ErrorC,
		ctx:       ctx,
		cancel:    cancel,
	}, nil
}

func restoreFromStorage(r *Raft, storage RaftStorage) error {
	// 恢复快照
	snap, err := storage.LoadSnapshot()
	if err != nil {
		return err
	}
	if snap != nil && snap.Index > 0 {
		r.RestoreSnapshot(snap)
	}

	// 恢复 HardState
	hs, err := storage.LoadHardState()
	if err != nil {
		return err
	}

	// 恢复日志
	startIndex := r.raftLog.LastIndex() + 1
	entries, err := storage.LoadEntries(startIndex, ^uint64(0))
	if err != nil {
		return err
	}

	r.RestoreState(hs, entries)
	return nil
}

func (rn *RaftNode) Start() {
	go rn.runTicker()
	go rn.runReceiver()
	go rn.runReady()
}

func (rn *RaftNode) Stop() {
	rn.cancel()
	rn.node.Stop()
	if rn.transport != nil {
		rn.transport.Close()
	}
}

func (rn *RaftNode) Propose(data []byte) error {
	ctx, cancel := context.WithTimeout(rn.ctx, 3*time.Second)
	defer cancel()
	return rn.node.Propose(ctx, data)
}

func (rn *RaftNode) ProposeWithContext(ctx context.Context, data []byte) error {
	return rn.node.Propose(ctx, data)
}

func (rn *RaftNode) Snapshot(index uint64, data []byte) error {
	ctx, cancel := context.WithTimeout(rn.ctx, 3*time.Second)
	defer cancel()

	sn, err := rn.node.Snapshot(ctx, index, data)
	if err != nil {
		return err
	}
	if sn == nil {
		return nil
	}

	// 保存快照到 storage
	return rn.storage.SaveSnapshot(sn)
}

// 定时 tick
func (rn *RaftNode) runTicker() {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			rn.node.Tick()
		case <-rn.ctx.Done():
			return
		}
	}
}

// 接收网络消息
func (rn *RaftNode) runReceiver() {
	recvC := rn.transport.Receive()
	for {
		select {
		case msg := <-recvC:
			rn.node.Step(rn.ctx, msg)
		case <-rn.ctx.Done():
			return
		}
	}
}

// 处理 Ready
func (rn *RaftNode) runReady() {
	for {
		select {
		case rd := <-rn.node.Ready():
			rn.handleReady(rd)
		case <-rn.ctx.Done():
			return
		}
	}
}

// handleReady 处理 Ready 中的所有内容
func (rn *RaftNode) handleReady(rd Ready) {
	//持久化 HardState
	if rd.HardState != nil {
		if err := rn.storage.SaveHardState(*rd.HardState); err != nil {
			rn.reportError(err)
		}
	}

	//持久化 Entries
	if len(rd.Entries) > 0 {
		if err := rn.storage.SaveEntries(rd.Entries); err != nil {
			rn.reportError(err)
		}
	}

	//持久化并应用 Snapshot
	if rd.Snapshot != nil {
		if err := rn.storage.SaveSnapshot(rd.Snapshot); err != nil {
			rn.reportError(err)
		}
		if err := rn.storage.ApplySnapshotData(rd.Snapshot.Data); err != nil {
			rn.reportError(err)
		}
	}

	//发送消息
	for _, msg := range rd.Messages {
		rn.transport.Send(msg)
	}

	//应用已提交条目
	for _, entry := range rd.CommittedEntries {
		select {
		case rn.commitC <- entry:
		case <-rn.ctx.Done():
			return
		}
	}

		// 确认完成
		rn.node.Advance()
}

func (rn *RaftNode) reportError(err error) {
	if rn.errorC != nil {
		select {
		case rn.errorC <- err:
		default:
		}
	}
}
