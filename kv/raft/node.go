package raft

import (
	"context"
	"errors"

	"github.com/zbchi/linkv/proto/raftpb"
)

var (
	ErrStopped = errors.New("raft: stopped")
)

// Node是Raft的线程安全接口
// 它封装了Raft状态机，通过channel提供异步交互
type Node interface {
	// Tick 推进逻辑时钟
	Tick()

	//Propose 提议新数据
	Propose(ctx context.Context, data []byte) error

	//Step 处理收到的消息
	Step(ctx context.Context, m *raftpb.Message) error

	//Ready 返回待处理状态的 channel
	Ready() <-chan Ready

	//Advance 确认已处理完 Ready
	Advance()

	//Snapshot 创建快照
	Snapshot(ctx context.Context, index uint64, data []byte) (*raftpb.Snapshot, error)

	//Stop停止节点
	Stop()
}

type node struct {
	propc    chan []byte
	recvc    chan *raftpb.Message
	tickc    chan struct{}
	snapc    chan SnapshotRequest
	readyc   chan Ready
	advancec chan struct{}
	stopc    chan struct{}
	done     chan struct{}

	raft *Raft
}

func StartNode(cfg Config) Node {
	r := NewRaft(cfg)
	return startNode(r)
}

func StartNodeWithRaft(r *Raft) Node {
	return startNode(r)
}

func startNode(r *Raft) Node {
	n := &node{
		propc:    make(chan []byte),
		recvc:    make(chan *raftpb.Message),
		tickc:    make(chan struct{}, 1),
		snapc:    make(chan SnapshotRequest),
		readyc:   make(chan Ready),
		advancec: make(chan struct{}),
		stopc:    make(chan struct{}),
		done:     make(chan struct{}),
		raft:     r,
	}

	go n.run()
	return n
}

// 主循环
func (n *node) run() {
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

		case readyc <- rd:
			<-n.advancec
			n.raft.Advance()
			rd = Ready{}

		case <-n.stopc:
			return
			<-n.advancec
			n.raft.Advance()
			rd = Ready{}

		case <-n.stopc:
			return
		}
	}
}

func (n *node) Tick() {
	select {
	case n.tickc <- struct{}{}:
	default:
	}
}

func (n *node) Propose(ctx context.Context, data []byte) error {
	select {
	case n.propc <- data:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-n.stopc:
		return ErrStopped
	}
}

func (n *node) Step(ctx context.Context, m *raftpb.Message) error {
	select {
	case n.recvc <- m:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-n.stopc:
		return ErrStopped
	}
}

func (n *node) Ready() <-chan Ready {
	return n.readyc
}

func (n *node) Advance() {
	select {
	case n.advancec <- struct{}{}:
	case <-n.stopc:
	}
}

func (n *node) Snapshot(ctx context.Context, index uint64, data []byte) (*raftpb.Snapshot, error) {
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

func (n *node) Stop() {
	select {
	case n.stopc <- struct{}{}:
	case <-n.done:
		return
	}
	<-n.done
}
