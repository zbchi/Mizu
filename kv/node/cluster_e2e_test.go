package node

import (
	"context"
	"fmt"
	"net"
	"path/filepath"
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/zbchi/mizu/clientlib"
	"github.com/zbchi/mizu/kv/config"
	"github.com/zbchi/mizu/kv/region"
	standalonestorage "github.com/zbchi/mizu/kv/storage/standalone_storage"
	"github.com/zbchi/mizu/kv/transport"
	"github.com/zbchi/mizu/proto"
	"github.com/zbchi/mizu/proto/raftkvpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type clusterNode struct {
	store     *standalonestorage.StandaloneStorage
	node      *Node
	transport *transport.Transport
	server    *grpc.Server
	listener  net.Listener
}

type testCluster struct {
	t         *testing.T
	clusterID uint64
	nodeIDs   []uint64
	nodes     map[uint64]*clusterNode
	kvAddrs   map[uint64]string
	raftAddrs map[uint64]string
	client    *clientlib.Client
	baseDir   string
}

func TestClusterRoutesKeysDeleteAndScan(t *testing.T) {
	cluster := newTestCluster(t)

	putResp := cluster.mustPropose(putRequest("apple", "v-apple"))
	require.True(t, putResp.Header.Success)
	require.Equal(t, uint64(1), putResp.Header.RegionId)

	putResp = cluster.mustPropose(putRequest("beta", "v-beta"))
	require.True(t, putResp.Header.Success)
	require.Equal(t, uint64(1), putResp.Header.RegionId)

	putResp = cluster.mustPropose(putRequest("moon", "v-moon"))
	require.True(t, putResp.Header.Success)
	require.Equal(t, uint64(2), putResp.Header.RegionId)

	putResp = cluster.mustPropose(putRequest("zebra", "v-zebra"))
	require.True(t, putResp.Header.Success)
	require.Equal(t, uint64(3), putResp.Header.RegionId)

	getResp := cluster.mustPropose(getRequest("moon"))
	require.True(t, getResp.Header.Success)
	require.Equal(t, uint64(2), getResp.Header.RegionId)
	require.Len(t, getResp.Responses, 1)
	require.Equal(t, "v-moon", string(getResp.Responses[0].Get.Value))

	scanResp := cluster.mustPropose(scanRequest("apple", 10))
	require.True(t, scanResp.Header.Success)
	require.Equal(t, uint64(1), scanResp.Header.RegionId)
	require.Len(t, scanResp.Responses, 1)
	require.Len(t, scanResp.Responses[0].Scan.Pairs, 2)
	require.Equal(t, "apple", string(scanResp.Responses[0].Scan.Pairs[0].Key))
	require.Equal(t, "beta", string(scanResp.Responses[0].Scan.Pairs[1].Key))

	deleteResp := cluster.mustPropose(deleteRequest("beta"))
	require.True(t, deleteResp.Header.Success)
	require.Equal(t, uint64(1), deleteResp.Header.RegionId)

	getResp = cluster.mustPropose(getRequest("beta"))
	require.True(t, getResp.Header.Success)
	require.Equal(t, uint64(1), getResp.Header.RegionId)
	require.Nil(t, getResp.Responses[0].Get.Value)
}

func TestClusterClientRetriesAfterLeaderSwitch(t *testing.T) {
	cluster := newTestCluster(t)

	resp := cluster.mustPropose(putRequest("apple", "before-switch"))
	require.True(t, resp.Header.Success)

	regionID, oldLeader := cluster.waitForLeader(t, []byte("apple"), 0)
	require.Equal(t, resp.Header.RegionId, regionID)
	require.Equal(t, oldLeader, cluster.client.KnownLeader(regionID))

	cluster.stopNode(oldLeader)

	_, newLeader := cluster.waitForLeader(t, []byte("apple"), oldLeader)
	require.NotZero(t, newLeader)
	require.NotEqual(t, oldLeader, newLeader)

	resp = cluster.mustPropose(putRequest("apricot", "after-switch"))
	require.True(t, resp.Header.Success)
	require.Equal(t, regionID, resp.Header.RegionId)
	require.Equal(t, newLeader, cluster.client.KnownLeader(regionID))

	resp = cluster.mustPropose(getRequest("apricot"))
	require.True(t, resp.Header.Success)
	require.Equal(t, "after-switch", string(resp.Responses[0].Get.Value))
}

func TestClusterRestartsAndReadsAfterSnapshot(t *testing.T) {
	cluster := newTestCluster(t)

	for i := 0; i < 6; i++ {
		cluster.mustPropose(putRequest(fmt.Sprintf("t-key-%d", i), fmt.Sprintf("value-%d", i)))
	}

	snapshotIndex := cluster.waitForSnapshot(t, 3, 3)
	require.GreaterOrEqual(t, snapshotIndex, uint64(5))

	cluster.stopNode(3)
	cluster.restartNode(t, 3)

	cluster.waitForLeader(t, []byte("t-key-0"), 0)

	for i := 0; i < 6; i++ {
		resp := cluster.mustPropose(getRequest(fmt.Sprintf("t-key-%d", i)))
		require.True(t, resp.Header.Success)
		require.Equal(t, fmt.Sprintf("value-%d", i), string(resp.Responses[0].Get.Value))
	}

	cluster.waitForLocalValue(t, 3, 3, []byte("t-key-0"), []byte("value-0"))
}

func newTestCluster(t *testing.T) *testCluster {
	t.Helper()

	cluster := &testCluster{
		t:         t,
		clusterID: 1,
		nodeIDs:   []uint64{1, 2, 3},
		nodes:     make(map[uint64]*clusterNode),
		kvAddrs:   make(map[uint64]string),
		raftAddrs: make(map[uint64]string),
		baseDir:   t.TempDir(),
	}

	raftListeners := make(map[uint64]net.Listener, len(cluster.nodeIDs))
	kvListeners := make(map[uint64]net.Listener, len(cluster.nodeIDs))
	for _, id := range cluster.nodeIDs {
		raftListeners[id] = listenLocal(t)
		kvListeners[id] = listenLocal(t)
		cluster.raftAddrs[id] = raftListeners[id].Addr().String()
		cluster.kvAddrs[id] = kvListeners[id].Addr().String()
	}

	for _, id := range cluster.nodeIDs {
		cluster.startNode(t, id, raftListeners[id], kvListeners[id])
	}

	cluster.client = clientlib.New(cluster.clusterID, cluster.kvAddrs)
	t.Cleanup(cluster.close)

	cluster.waitForLeader(t, []byte("apple"), 0)
	cluster.waitForLeader(t, []byte("moon"), 0)
	cluster.waitForLeader(t, []byte("zebra"), 0)
	return cluster
}

func (c *testCluster) close() {
	if c.client != nil {
		_ = c.client.Close()
		c.client = nil
	}

	ids := append([]uint64(nil), c.nodeIDs...)
	sort.Slice(ids, func(i, j int) bool { return ids[i] > ids[j] })
	for _, id := range ids {
		c.stopNode(id)
	}
}

func (c *testCluster) startNode(t *testing.T, id uint64, raftListener, kvListener net.Listener) {
	t.Helper()

	regions := buildStaticRegionsForTest(raftPeerInfos(c.raftAddrs))
	store := standalonestorage.NewStandaloneStorage(&config.Config{
		DBPath: filepath.Join(c.baseDir, fmt.Sprintf("node-%d", id)),
	})

	node, err := New(&Config{
		NodeID:    id,
		ClusterID: c.clusterID,
		RaftAddr:  c.raftAddrs[id],
		Regions:   regions,
	}, store)
	require.NoError(t, err)

	trans := transport.New(transport.Config{
		ID:       id,
		Addr:     c.raftAddrs[id],
		Peers:    cloneAddrMap(c.raftAddrs),
		Listener: raftListener,
	})
	require.NoError(t, trans.Start())

	node.SetTransport(trans)
	require.NoError(t, node.Start())

	srv := grpc.NewServer()
	raftkvpb.RegisterRaftKVServer(srv, NewServer(node))
	go func() {
		_ = srv.Serve(kvListener)
	}()

	c.nodes[id] = &clusterNode{
		store:     store,
		node:      node,
		transport: trans,
		server:    srv,
		listener:  kvListener,
	}
}

func (c *testCluster) stopNode(id uint64) {
	n, ok := c.nodes[id]
	if !ok || n == nil {
		return
	}

	if n.server != nil {
		n.server.Stop()
		n.server = nil
	}
	if n.listener != nil {
		_ = n.listener.Close()
		n.listener = nil
	}
	if n.node != nil {
		_ = n.node.Stop()
		n.node = nil
	}
	if n.transport != nil {
		_ = n.transport.Close()
		n.transport = nil
	}
	if n.store != nil {
		n.store = nil
	}
}

func (c *testCluster) restartNode(t *testing.T, id uint64) {
	t.Helper()
	c.stopNode(id)
	c.startNode(t, id, listenAddr(t, c.raftAddrs[id]), listenAddr(t, c.kvAddrs[id]))
}

func (c *testCluster) mustPropose(req *raftkvpb.RaftCmdRequest) *raftkvpb.RaftCmdResponse {
	c.t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	resp, err := c.client.Propose(ctx, req)
	require.NoError(c.t, err)
	require.NotNil(c.t, resp)
	require.NotNil(c.t, resp.Header)
	return resp
}

func (c *testCluster) waitForLeader(t *testing.T, key []byte, exclude uint64) (uint64, uint64) {
	t.Helper()

	var regionID uint64
	var leaderID uint64
	require.Eventually(t, func() bool {
		regionID, leaderID = c.probeLeader(key)
		return regionID != 0 && leaderID != 0 && leaderID != exclude
	}, 10*time.Second, 50*time.Millisecond)
	return regionID, leaderID
}

func (c *testCluster) probeLeader(key []byte) (uint64, uint64) {
	req := getRequest(string(key))
	for _, id := range c.nodeIDs {
		addr := c.kvAddrs[id]
		resp, err := singleNodePropose(addr, req)
		if err != nil || resp == nil || resp.Header == nil {
			continue
		}
		if resp.Header.RegionId != 0 && resp.Header.LeaderNodeId != 0 {
			return resp.Header.RegionId, resp.Header.LeaderNodeId
		}
	}
	return 0, 0
}

func (c *testCluster) waitForSnapshot(t *testing.T, nodeID, regionID uint64) uint64 {
	t.Helper()

	var index uint64
	require.Eventually(t, func() bool {
		n := c.nodes[nodeID]
		if n == nil || n.node == nil {
			return false
		}
		sn, err := n.node.storage.RaftStorage(regionID).LoadSnapshot()
		if err != nil || sn == nil || sn.Index == 0 {
			return false
		}
		index = sn.Index
		return true
	}, 10*time.Second, 50*time.Millisecond)
	return index
}

func (c *testCluster) waitForLocalValue(t *testing.T, nodeID, regionID uint64, key, expected []byte) {
	t.Helper()

	require.Eventually(t, func() bool {
		n := c.nodes[nodeID]
		if n == nil || n.node == nil {
			return false
		}

		reader, err := n.node.storage.RegionStorage(regionID).Reader()
		if err != nil {
			return false
		}
		defer reader.Close()

		value, err := reader.GetCF("default", key)
		if err != nil {
			return false
		}
		return string(value) == string(expected)
	}, 10*time.Second, 50*time.Millisecond)
}

func putRequest(key, value string) *raftkvpb.RaftCmdRequest {
	return &raftkvpb.RaftCmdRequest{
		Requests: []*raftkvpb.Request{
			{
				CmdType: raftkvpb.CmdType_Put,
				Put:     &raftkvpb.PutRequest{Cf: "default", Key: []byte(key), Value: []byte(value)},
			},
		},
	}
}

func getRequest(key string) *raftkvpb.RaftCmdRequest {
	return &raftkvpb.RaftCmdRequest{
		Requests: []*raftkvpb.Request{
			{
				CmdType: raftkvpb.CmdType_Get,
				Get:     &raftkvpb.GetRequest{Cf: "default", Key: []byte(key)},
			},
		},
	}
}

func deleteRequest(key string) *raftkvpb.RaftCmdRequest {
	return &raftkvpb.RaftCmdRequest{
		Requests: []*raftkvpb.Request{
			{
				CmdType: raftkvpb.CmdType_Delete,
				Delete:  &raftkvpb.DeleteRequest{Cf: "default", Key: []byte(key)},
			},
		},
	}
}

func scanRequest(startKey string, limit uint32) *raftkvpb.RaftCmdRequest {
	return &raftkvpb.RaftCmdRequest{
		Requests: []*raftkvpb.Request{
			{
				CmdType: raftkvpb.CmdType_Scan,
				Scan:    &raftkvpb.ScanRequest{Cf: "default", StartKey: []byte(startKey), Limit: limit},
			},
		},
	}
}

func singleNodePropose(addr string, req *raftkvpb.RaftCmdRequest) (*raftkvpb.RaftCmdResponse, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	client := raftkvpb.NewRaftKVClient(conn)
	return client.Propose(ctx, req)
}

func listenLocal(t *testing.T) net.Listener {
	return listenAddr(t, "127.0.0.1:0")
}

func listenAddr(t *testing.T, addr string) net.Listener {
	t.Helper()

	lis, err := net.Listen("tcp", addr)
	require.NoError(t, err)
	return lis
}

func raftPeerInfos(addrs map[uint64]string) []proto.PeerInfo {
	ids := make([]uint64, 0, len(addrs))
	for id := range addrs {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })

	peers := make([]proto.PeerInfo, 0, len(ids))
	for _, id := range ids {
		peers = append(peers, proto.PeerInfo{NodeID: id, Addr: addrs[id]})
	}
	return peers
}

func buildStaticRegionsForTest(peers []proto.PeerInfo) []*region.Region {
	regions := []*region.Region{
		{ID: 1, StartKey: []byte{}, EndKey: []byte("m"), Peers: clonePeers(peers)},
		{ID: 2, StartKey: []byte("m"), EndKey: []byte("t"), Peers: clonePeers(peers)},
		{ID: 3, StartKey: []byte("t"), EndKey: []byte{}, Peers: clonePeers(peers)},
	}
	if len(peers) > 0 {
		for _, reg := range regions {
			reg.Leader = peers[0]
		}
	}
	return regions
}

func clonePeers(peers []proto.PeerInfo) []proto.PeerInfo {
	out := make([]proto.PeerInfo, len(peers))
	copy(out, peers)
	return out
}

func cloneAddrMap(in map[uint64]string) map[uint64]string {
	out := make(map[uint64]string, len(in))
	for id, addr := range in {
		out[id] = addr
	}
	return out
}
