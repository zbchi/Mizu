package raft

import (
	"context"
	"errors"
	"time"

	"github.com/zbchi/linkv/proto/raftpb"
)

var (
	ErrStopped = errors.New("raft: stopped")
)

// Node represents a node in a raft cluster.
// It encapsulates the raft state machine and provides a thread-safe
// asynchronous interface for interacting with raft.
type Node interface {
	// Tick increments the internal logical clock by a single tick.
	// Election timeouts and heartbeat timeouts are in units of ticks.
	Tick()

	// Propose proposes data to be appended to the raft log.
	Propose(ctx context.Context, data []byte) error

	// Step advances the state machine using the given message.
	Step(ctx context.Context, m raftpb.Message) error

	// Ready returns a channel that yields a Ready struct when there is
	// state ready to be processed by the application.
	Ready() <-chan Ready

	// Advance notifies the Node that the application has saved progress
	// up to the last Ready. It prepares the node to return the next
	// available Ready.
	Advance()

	// Stop performs any necessary cleanup and stops the node.
	Stop()
}

// node implements the Node interface.
type node struct {
	// Input channels
	propc chan []byte         // for Propose
	recvc chan raftpb.Message // for Step
	tickc chan struct{}       // for Tick

	// Output channel
	readyc chan Ready // for Ready

	// Control channels
	advancec chan struct{} // for Advance
	stopc    chan struct{} // for Stop
	done     chan struct{} // signals run() has exited

	// The raft state machine
	rn *Raft
}

// StartNode creates and starts a new Node with the given configuration.
// The Node will run in a background goroutine.
func StartNode(r *Raft) Node {
	n := &node{
		propc:    make(chan []byte),
		recvc:    make(chan raftpb.Message),
		tickc:    make(chan struct{}),
		readyc:   make(chan Ready),
		advancec: make(chan struct{}),
		stopc:    make(chan struct{}),
		done:     make(chan struct{}),
		rn:       r,
	}

	go n.run()
	return n
}

// run is the main loop that runs in a background goroutine.
// It processes all inputs and generates Ready outputs.
func (n *node) run() {
	defer close(n.done)

	var readyc chan Ready
	var rd Ready

	for {
		// Only enable readyc if we have a non-empty Ready to send
		if rd.IsEmpty() {
			readyc = nil
		} else {
			readyc = n.readyc
		}

		select {
		case <-n.tickc:
			n.rn.Tick()

		case m := <-n.recvc:
			n.rn.Step(m)

		case data := <-n.propc:
			n.rn.Propose(data)

		case readyc <- rd:
			<-n.advancec
			n.rn.Advance()
			rd = Ready{} // clear

		case <-n.stopc:
			return

		default:
			// Check if there's a new Ready
			if rd.IsEmpty() {
				rd = n.rn.Ready()
			}

			// Small sleep to avoid busy loop when nothing is ready
			if rd.IsEmpty() {
				time.Sleep(time.Millisecond)
			}
		}
	}
}

func (n *node) Tick() {
	select {
	case n.tickc <- struct{}{}:
	case <-n.stopc:
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

func (n *node) Step(ctx context.Context, m raftpb.Message) error {
	if m.Type == raftpb.Type_MsgHup {
		return nil
	}

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

func (n *node) Stop() {
	select {
	case n.stopc <- struct{}{}:
	case <-n.done:
		return
	}
	<-n.done
}
