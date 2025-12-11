package transport

import (
	"context"
	"errors"
	"net"
	"sync"
	"time"

	"github.com/zbchi/linkv/proto/raftpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var (
	// ErrUnknownPeer 表示目标 peer 不在已知列表中
	ErrUnknownPeer = errors.New("unknown peer")

	// ErrTransportClosed 表示 transport 已关闭
	ErrTransportClosed = errors.New("transport closed")
)

// Transport 负责 Raft 节点之间的网络通信
// 实现了 raft.Transport 接口
type Transport struct {
	raftpb.UnimplementedRaftServer

	id    uint64            // 本节点 ID
	addr  string            // 本节点监听地址
	peers map[uint64]string // peer ID -> 地址映射

	recvC chan *raftpb.Message // 接收消息的 channel

	mu      sync.RWMutex
	clients map[uint64]raftpb.RaftClient // 到各 peer 的 gRPC 客户端
	conns   map[uint64]*grpc.ClientConn  // 连接池

	server   *grpc.Server
	listener net.Listener

	ctx    context.Context
	cancel context.CancelFunc
}

// Config 是 Transport 的配置
type Config struct {
	ID    uint64            // 本节点 ID
	Addr  string            // 本节点监听地址 (如 ":9000")
	Peers map[uint64]string // peer ID -> 地址映射 (如 {1: "localhost:9001", 2: "localhost:9002"})
}

// New 创建一个新的 Transport
func New(cfg Config) *Transport {
	ctx, cancel := context.WithCancel(context.Background())
	return &Transport{
		id:      cfg.ID,
		addr:    cfg.Addr,
		peers:   cfg.Peers,
		recvC:   make(chan *raftpb.Message, 1024),
		clients: make(map[uint64]raftpb.RaftClient),
		conns:   make(map[uint64]*grpc.ClientConn),
		ctx:     ctx,
		cancel:  cancel,
	}
}

// Start 启动 Transport，开始监听和接收消息
func (t *Transport) Start() error {
	// 启动 gRPC 服务端
	listener, err := net.Listen("tcp", t.addr)
	if err != nil {
		return err
	}
	t.listener = listener

	t.server = grpc.NewServer()
	raftpb.RegisterRaftServer(t.server, t)

	go func() {
		if err := t.server.Serve(listener); err != nil {
			// 服务器关闭时会返回错误，这里可以忽略
		}
	}()

	return nil
}

// Send 发送消息到目标节点
func (t *Transport) Send(msg *raftpb.Message) error {
	// 不发送给自己
	if msg.To == t.id {
		return nil
	}

	client, err := t.getClient(msg.To)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(t.ctx, 100*time.Millisecond)
	defer cancel()

	_, err = client.Step(ctx, msg)
	return err
}

// Receive 返回接收消息的 channel
func (t *Transport) Receive() <-chan *raftpb.Message {
	return t.recvC
}

// Close 关闭 Transport
func (t *Transport) Close() error {
	t.cancel()

	// 关闭所有客户端连接
	t.mu.Lock()
	for _, conn := range t.conns {
		conn.Close()
	}
	t.clients = make(map[uint64]raftpb.RaftClient)
	t.conns = make(map[uint64]*grpc.ClientConn)
	t.mu.Unlock()

	// 关闭服务端
	if t.server != nil {
		t.server.GracefulStop()
	}

	close(t.recvC)
	return nil
}

// Step 实现 raftpb.RaftServer 接口，接收来自其他节点的消息
func (t *Transport) Step(ctx context.Context, msg *raftpb.Message) (*raftpb.Message, error) {
	select {
	case t.recvC <- msg:
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-t.ctx.Done():
		return nil, t.ctx.Err()
	}
	return &raftpb.Message{}, nil
}

// getClient 获取到目标节点的 gRPC 客户端，如果不存在则创建
func (t *Transport) getClient(id uint64) (raftpb.RaftClient, error) {
	t.mu.RLock()
	client, ok := t.clients[id]
	t.mu.RUnlock()
	if ok {
		return client, nil
	}

	// 需要创建新连接
	t.mu.Lock()
	defer t.mu.Unlock()

	// 双重检查
	if client, ok := t.clients[id]; ok {
		return client, nil
	}

	addr, ok := t.peers[id]
	if !ok {
		return nil, ErrUnknownPeer
	}

	conn, err := grpc.NewClient(addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return nil, err
	}

	client = raftpb.NewRaftClient(conn)
	t.conns[id] = conn
	t.clients[id] = client

	return client, nil
}

// AddPeer 动态添加一个 peer
func (t *Transport) AddPeer(id uint64, addr string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.peers[id] = addr
}

// RemovePeer 动态移除一个 peer
func (t *Transport) RemovePeer(id uint64) {
	t.mu.Lock()
	defer t.mu.Unlock()

	delete(t.peers, id)
	if conn, ok := t.conns[id]; ok {
		conn.Close()
		delete(t.conns, id)
		delete(t.clients, id)
	}
}
