package raft

import (
	"context"
	"errors"

	"github.com/zbchi/linkv/proto/raftpb"
)

var (
	ErrStopped = errors.New("raft: stopped")
)

// RawNode 是 Raft 的线程安全接口
// 它封装了 Raft 状态机，通过 channel 提供异步交互
type RawNode struct {
	propc    chan []byte
	recvc    chan *raftpb.Message
	tickc    chan struct{}
	snapc    chan SnapshotRequest
	campaignc chan struct{}
	readyc   chan Ready
	advancec chan struct{}
	stopc    chan struct{}
	done     chan struct{}

	raft *Raft
}

// NewRawNode 创建一个新的 RawNode
func NewRawNode(cfg Config) *RawNode {
	r := NewRaft(cfg)
	return startRawNode(r)
}

// NewRawNodeWithRaft 从已有 Raft 实例创建 RawNode
func NewRawNodeWithRaft(r *Raft) *RawNode {
	return startRawNode(r)
}

func startRawNode(r *Raft) *RawNode {
	n := &RawNode{
		propc:     make(chan []byte),
		recvc:     make(chan *raftpb.Message),
		tickc:     make(chan struct{}, 1),
		snapc:     make(chan SnapshotRequest),
		campaignc: make(chan struct{}, 1),
		readyc:    make(chan Ready),
		advancec:  make(chan struct{}),
		stopc:     make(chan struct{}),
		done:      make(chan struct{}),
		raft:      r,
	}

	go n.run()
	return n
}

// Tick 推进逻辑时钟
func (n *RawNode) Tick() {
	select {
	case n.tickc <- struct{}{}:
	default:
	}
}

// Propose 提议新数据
func (n *RawNode) Propose(ctx context.Context, data []byte) error {
	select {
	case n.propc <- data:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-n.stopc:
		return ErrStopped
	}
}

// Step 处理收到的消息
func (n *RawNode) Step(ctx context.Context, m *raftpb.Message) error {
	select {
	case n.recvc <- m:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-n.stopc:
		return ErrStopped
	}
}

// Ready 返回待处理状态的 channel
func (n *RawNode) Ready() <-chan Ready {
	return n.readyc
}

// Advance 确认已处理完 Ready
func (n *RawNode) Advance() {
	select {
	case n.advancec <- struct{}{}:
	case <-n.stopc:
	}
}

// Campaign 触发选举
func (n *RawNode) Campaign() {
	select {
	case n.campaignc <- struct{}{}:
	default:
	}
}

// Snapshot 创建快照
func (n *RawNode) Snapshot(ctx context.Context, index uint64, data []byte) (*raftpb.Snapshot, error) {
	req := SnapshotRequest{
		Index:   index,
		Data:    data,
		ResultC: make(chan *raftpb.Snapshot, 1),
	}

	select {
	case n.snapc <- req:
		select {
		case sn := <-req.ResultC:
			return sn, nil
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-n.stopc:
			return nil, ErrStopped
		}
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-n.stopc:
		return nil, ErrStopped
	}
}

// Stop 停止节点
func (n *RawNode) Stop() {
	select {
	case n.stopc <- struct{}{}:
	case <-n.done:
		return
	}
	<-n.done
	// Close stopc so that pending operations detect the stop
	close(n.stopc)
}

// run 主循环
func (n *RawNode) run() {
	defer close(n.done)

	var readyc chan Ready
	var rd Ready

	for {
		if rd.IsEmpty() {
			rd = n.raft.Ready()
		}

		if rd.IsEmpty() {
			readyc = nil
		} else {
			readyc = n.readyc
		}

		select {
		case <-n.tickc:
			n.raft.Tick()

		case m := <-n.recvc:
			n.raft.Step(m)

		case data := <-n.propc:
			n.raft.Propose(data)

		case req := <-n.snapc:
			sn := n.raft.Snapshot(req.Index, req.Data)
			req.ResultC <- sn

		case <-n.campaignc:
			n.raft.campaign()

		case readyc <- rd:
			<-n.advancec
			n.raft.Advance()
			rd = Ready{}

		case <-n.stopc:
			return
		}
	}
}
