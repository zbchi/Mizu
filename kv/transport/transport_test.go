package transport

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/zbchi/linkv/proto/raftpb"
)

// getFreePort 获取一个可用的端口
func getFreePort(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer l.Close()
	addr := l.Addr().(*net.TCPAddr)
	return addr.String()
}

// newTestTransport 创建一个用于测试的 Transport
func newTestTransport(t *testing.T, id uint64, addr string, peers map[uint64]string) *Transport {
	t.Helper()
	cfg := Config{
		ID:    id,
		Addr:  addr,
		Peers: peers,
	}
	return New(cfg)
}

// TestNewTransport 测试创建 Transport
func TestNewTransport(t *testing.T) {
	addr := getFreePort(t)
	peers := map[uint64]string{
		2: "127.0.0.1:9002",
		3: "127.0.0.1:9003",
	}

	trans := newTestTransport(t, 1, addr, peers)

	assert.Equal(t, uint64(1), trans.id)
	assert.Equal(t, addr, trans.addr)
	assert.Equal(t, peers, trans.peers)
	assert.NotNil(t, trans.recvC)
	assert.NotNil(t, trans.clients)
	assert.NotNil(t, trans.conns)
	assert.NotNil(t, trans.ctx)
	assert.NotNil(t, trans.cancel)
}

// TestNewTransportWithEmptyPeers 测试创建没有 peers 的 Transport
func TestNewTransportWithEmptyPeers(t *testing.T) {
	addr := getFreePort(t)
	trans := newTestTransport(t, 1, addr, nil)

	assert.Empty(t, trans.peers)
	assert.Empty(t, trans.clients)
	assert.Empty(t, trans.conns)
}

// TestStartAndClose 测试启动和关闭 Transport
func TestStartAndClose(t *testing.T) {
	addr := getFreePort(t)
	trans := newTestTransport(t, 1, addr, nil)

	// 启动
	err := trans.Start()
	require.NoError(t, err)
	assert.NotNil(t, trans.server)
	assert.NotNil(t, trans.listener)

	// 关闭
	err = trans.Close()
	require.NoError(t, err)

	// 验证 channel 已关闭
	_, ok := <-trans.recvC
	assert.False(t, ok, "receive channel should be closed")
}

// TestStartWithInvalidAddress 测试使用无效地址启动
func TestStartWithInvalidAddress(t *testing.T) {
	trans := newTestTransport(t, 1, "invalid-address", nil)

	err := trans.Start()
	assert.Error(t, err)
}

// TestCloseMultipleTimes 测试多次关闭
func TestCloseMultipleTimes(t *testing.T) {
	addr := getFreePort(t)
	trans := newTestTransport(t, 1, addr, nil)

	err := trans.Start()
	require.NoError(t, err)

	// 第一次关闭
	err = trans.Close()
	require.NoError(t, err)

	// 第二次关闭应该不会 panic
	err = trans.Close()
	require.NoError(t, err)
}

// TestSendToSelf 测试发送消息给自己
func TestSendToSelf(t *testing.T) {
	addr := getFreePort(t)
	trans := newTestTransport(t, 1, addr, nil)

	msg := &raftpb.Message{
		Type: raftpb.Type_MsgApp,
		From: 1,
		To:   1, // 发送给自己
		Term: 1,
	}

	err := trans.Send(msg)
	assert.NoError(t, err, "sending to self should succeed")
}

// TestSendToUnknownPeer 测试发送消息到未知 peer
func TestSendToUnknownPeer(t *testing.T) {
	addr := getFreePort(t)
	trans := newTestTransport(t, 1, addr, nil) // 没有 peers

	msg := &raftpb.Message{
		Type: raftpb.Type_MsgApp,
		From: 1,
		To:   2, // 未知 peer
		Term: 1,
	}

	err := trans.Send(msg)
	assert.True(t, errors.Is(err, ErrUnknownPeer))
}

// TestSendWithTwoTransports 测试两个 Transport 之间发送消息
func TestSendWithTwoTransports(t *testing.T) {
	addr1 := getFreePort(t)
	addr2 := getFreePort(t)

	// 创建两个 transport
	trans1 := newTestTransport(t, 1, addr1, map[uint64]string{2: addr2})
	trans2 := newTestTransport(t, 2, addr2, map[uint64]string{1: addr1})

	// 启动
	require.NoError(t, trans1.Start())
	require.NoError(t, trans2.Start())
	defer trans1.Close()
	defer trans2.Close()

	// 发送消息
	msg := &raftpb.Message{
		Type: raftpb.Type_MsgApp,
		From: 1,
		To:   2,
		Term: 1,
	}

	err := trans1.Send(msg)
	require.NoError(t, err)

	// 接收消息
	select {
	case received := <-trans2.Receive():
		assert.Equal(t, msg.Type, received.Type)
		assert.Equal(t, msg.From, received.From)
		assert.Equal(t, msg.To, received.To)
		assert.Equal(t, msg.Term, received.Term)
	case <-time.After(1 * time.Second):
		t.Fatal("timeout waiting for message")
	}
}

// TestSendMultipleMessages 测试发送多条消息
func TestSendMultipleMessages(t *testing.T) {
	addr1 := getFreePort(t)
	addr2 := getFreePort(t)

	trans1 := newTestTransport(t, 1, addr1, map[uint64]string{2: addr2})
	trans2 := newTestTransport(t, 2, addr2, map[uint64]string{1: addr1})

	require.NoError(t, trans1.Start())
	require.NoError(t, trans2.Start())
	defer trans1.Close()
	defer trans2.Close()

	// 发送多条消息
	for i := 0; i < 5; i++ {
		msg := &raftpb.Message{
			Type: raftpb.Type_MsgApp,
			From: 1,
			To:   2,
			Term: uint64(i + 1),
		}
		err := trans1.Send(msg)
		require.NoError(t, err)
	}

	// 接收所有消息
	receivedCount := 0
	timeout := time.After(2 * time.Second)
	for receivedCount < 5 {
		select {
		case <-trans2.Receive():
			receivedCount++
		case <-timeout:
			t.Fatalf("timeout after receiving %d messages", receivedCount)
		}
	}

	assert.Equal(t, 5, receivedCount)
}

// TestSendAfterClose 测试关闭后发送消息
func TestSendAfterClose(t *testing.T) {
	addr := getFreePort(t)
	trans := newTestTransport(t, 1, addr, map[uint64]string{2: "127.0.0.1:9999"})

	require.NoError(t, trans.Start())
	trans.Close()

	msg := &raftpb.Message{
		Type: raftpb.Type_MsgApp,
		From: 1,
		To:   2,
		Term: 1,
	}

	// 关闭后发送应该失败（超时或上下文取消）
	err := trans.Send(msg)
	assert.Error(t, err)
}

// TestReceiveChannel 测试 Receive 返回的 channel
func TestReceiveChannel(t *testing.T) {
	addr := getFreePort(t)
	trans := newTestTransport(t, 1, addr, nil)

	ch := trans.Receive()
	assert.NotNil(t, ch)
	// 验证是同一个底层 channel - 发送消息后能接收到
	go func() {
		trans.recvC <- &raftpb.Message{Type: raftpb.Type_MsgApp}
	}()
	msg, ok := <-ch
	assert.True(t, ok)
	assert.NotNil(t, msg)
}

// TestStep 测试 Step 方法（gRPC 接口）
func TestStep(t *testing.T) {
	addr := getFreePort(t)
	trans := newTestTransport(t, 1, addr, nil)

	msg := &raftpb.Message{
		Type: raftpb.Type_MsgApp,
		From: 2,
		To:   1,
		Term: 1,
	}

	// 调用 Step
	resp, err := trans.Step(context.Background(), msg)
	require.NoError(t, err)
	assert.NotNil(t, resp)

	// 验证消息被放到接收 channel
	select {
	case received := <-trans.Receive():
		assert.Equal(t, msg, received)
	case <-time.After(100 * time.Millisecond):
		t.Fatal("message not received")
	}
}

// TestStepWithCanceledContext 测试使用取消的上下文调用 Step
func TestStepWithCanceledContext(t *testing.T) {
	addr := getFreePort(t)
	// 创建一个 buffer 很小的 Transport，使其更容易填满
	trans := newTestTransport(t, 1, addr, nil)

	// 填满 channel 以确保 select 会检查 context
	for i := 0; i < cap(trans.recvC); i++ {
		trans.recvC <- &raftpb.Message{}
	}

	msg := &raftpb.Message{
		Type: raftpb.Type_MsgApp,
		From: 2,
		To:   1,
		Term: 1,
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // 立即取消

	_, err := trans.Step(ctx, msg)
	assert.Error(t, err)
}

// TestGetClientUnknownPeer 测试获取未知 peer 的客户端
func TestGetClientUnknownPeer(t *testing.T) {
	addr := getFreePort(t)
	trans := newTestTransport(t, 1, addr, nil) // 没有 peers

	_, err := trans.getClient(999)
	assert.True(t, errors.Is(err, ErrUnknownPeer))
}

// TestGetClientDoubleCheck 测试双重检查锁定
func TestGetClientDoubleCheck(t *testing.T) {
	addr := getFreePort(t)
	peerAddr := getFreePort(t)

	trans := newTestTransport(t, 1, addr, map[uint64]string{2: peerAddr})

	// 第一次获取 - 创建新连接
	client1, err := trans.getClient(2)
	require.NoError(t, err)
	assert.NotNil(t, client1)

	// 第二次获取 - 应该返回缓存的客户端
	client2, err := trans.getClient(2)
	require.NoError(t, err)
	assert.Same(t, client1, client2)

	// 验证连接被缓存
	trans.mu.RLock()
	_, ok := trans.conns[2]
	trans.mu.RUnlock()
	assert.True(t, ok, "connection should be cached")
}

// TestGetClientConcurrent 测试并发获取客户端
func TestGetClientConcurrent(t *testing.T) {
	addr := getFreePort(t)
	peerAddr := getFreePort(t)

	trans := newTestTransport(t, 1, addr, map[uint64]string{
		2: peerAddr,
		3: "127.0.0.1:9999",
	})

	// 并发获取
	done := make(chan struct{})
	go func() {
		for i := 0; i < 10; i++ {
			trans.getClient(2)
		}
		done <- struct{}{}
	}()

	go func() {
		for i := 0; i < 10; i++ {
			trans.getClient(3)
		}
		done <- struct{}{}
	}()

	<-done
	<-done

	// 验证只有两个连接
	trans.mu.RLock()
	conns := len(trans.conns)
	clients := len(trans.clients)
	trans.mu.RUnlock()

	assert.Equal(t, 2, conns)
	assert.Equal(t, 2, clients)
}

// TestAddPeer 测试添加 peer
func TestAddPeer(t *testing.T) {
	addr := getFreePort(t)
	trans := newTestTransport(t, 1, addr, nil)

	trans.AddPeer(2, "127.0.0.1:9002")

	trans.mu.RLock()
	peerAddr, ok := trans.peers[2]
	trans.mu.RUnlock()

	assert.True(t, ok)
	assert.Equal(t, "127.0.0.1:9002", peerAddr)
}

// TestAddPeerExisting 测试添加已存在的 peer
func TestAddPeerExisting(t *testing.T) {
	addr := getFreePort(t)
	trans := newTestTransport(t, 1, addr, map[uint64]string{2: "old-address"})

	// 更新 peer 地址
	trans.AddPeer(2, "new-address")

	trans.mu.RLock()
	peerAddr := trans.peers[2]
	trans.mu.RUnlock()

	assert.Equal(t, "new-address", peerAddr)
}

// TestRemovePeer 测试移除 peer
func TestRemovePeer(t *testing.T) {
	addr := getFreePort(t)
	trans := newTestTransport(t, 1, addr, map[uint64]string{2: "127.0.0.1:9002"})

	trans.RemovePeer(2)

	trans.mu.RLock()
	_, ok := trans.peers[2]
	trans.mu.RUnlock()

	assert.False(t, ok, "peer should be removed")
}

// TestRemovePeerWithConnection 测试移除有连接的 peer
func TestRemovePeerWithConnection(t *testing.T) {
	addr1 := getFreePort(t)
	addr2 := getFreePort(t)

	trans1 := newTestTransport(t, 1, addr1, map[uint64]string{2: addr2})
	trans2 := newTestTransport(t, 2, addr2, map[uint64]string{1: addr1})

	require.NoError(t, trans1.Start())
	require.NoError(t, trans2.Start())

	// 发送一条消息以建立连接
	msg := &raftpb.Message{
		Type: raftpb.Type_MsgApp,
		From: 1,
		To:   2,
		Term: 1,
	}
	err := trans1.Send(msg)
	require.NoError(t, err)

	// 验证连接已建立
	trans1.mu.RLock()
	_, hasConn := trans1.conns[2]
	_, hasClient := trans1.clients[2]
	trans1.mu.RUnlock()
	assert.True(t, hasConn)
	assert.True(t, hasClient)

	// 移除 peer
	trans1.RemovePeer(2)

	// 验证连接被关闭
	trans1.mu.RLock()
	_, hasConn = trans1.conns[2]
	_, hasClient = trans1.clients[2]
	_, hasPeer := trans1.peers[2]
	trans1.mu.RUnlock()

	assert.False(t, hasConn, "connection should be removed")
	assert.False(t, hasClient, "client should be removed")
	assert.False(t, hasPeer, "peer should be removed")
}

// TestRemovePeerNonExistent 测试移除不存在的 peer
func TestRemovePeerNonExistent(t *testing.T) {
	addr := getFreePort(t)
	trans := newTestTransport(t, 1, addr, nil)

	// 不应该 panic
	trans.RemovePeer(999)
}

// TestThreeNodeCluster 测试三节点集群通信
func TestThreeNodeCluster(t *testing.T) {
	addr1 := getFreePort(t)
	addr2 := getFreePort(t)
	addr3 := getFreePort(t)

	trans1 := newTestTransport(t, 1, addr1, map[uint64]string{
		2: addr2,
		3: addr3,
	})
	trans2 := newTestTransport(t, 2, addr2, map[uint64]string{
		1: addr1,
		3: addr3,
	})
	trans3 := newTestTransport(t, 3, addr3, map[uint64]string{
		1: addr1,
		2: addr2,
	})

	require.NoError(t, trans1.Start())
	require.NoError(t, trans2.Start())
	require.NoError(t, trans3.Start())
	defer trans1.Close()
	defer trans2.Close()
	defer trans3.Close()

	// 节点 1 向节点 2 和 3 发送消息
	msg2 := &raftpb.Message{
		Type: raftpb.Type_MsgVote,
		From: 1,
		To:   2,
		Term: 1,
	}
	msg3 := &raftpb.Message{
		Type: raftpb.Type_MsgVote,
		From: 1,
		To:   3,
		Term: 1,
	}

	require.NoError(t, trans1.Send(msg2))
	require.NoError(t, trans1.Send(msg3))

	// 验证节点 2 收到消息
	select {
	case <-trans2.Receive():
	case <-time.After(1 * time.Second):
		t.Error("node 2 did not receive message")
	}

	// 验证节点 3 收到消息
	select {
	case <-trans3.Receive():
	case <-time.After(1 * time.Second):
		t.Error("node 3 did not receive message")
	}
}

// TestSendToOfflinePeer 测试发送到离线 peer
func TestSendToOfflinePeer(t *testing.T) {
	addr1 := getFreePort(t)
	addr2 := getFreePort(t)

	trans1 := newTestTransport(t, 1, addr1, map[uint64]string{2: addr2})

	require.NoError(t, trans1.Start())
	defer trans1.Close()

	// 不启动 trans2，直接发送
	msg := &raftpb.Message{
		Type: raftpb.Type_MsgApp,
		From: 1,
		To:   2,
		Term: 1,
	}

	// 应该失败或超时
	err := trans1.Send(msg)
	assert.Error(t, err)
}
