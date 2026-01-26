package main

import (
	"errors"
	"flag"
	"log/slog"
	"net"
	"os"
	"strconv"

	"github.com/zbchi/mizu/kv/config"
	"github.com/zbchi/mizu/kv/node"
	"github.com/zbchi/mizu/kv/region"
	"github.com/zbchi/mizu/kv/storage"
	"github.com/zbchi/mizu/kv/transport"
	"github.com/zbchi/mizu/proto"
	"github.com/zbchi/mizu/proto/raftkvpb"

	"google.golang.org/grpc"
)

func main() {
	id := flag.Uint64("id", 1, "Node ID")
	clusterID := flag.Uint64("cluster", 1, "Cluster ID")
	raftAddr := flag.String("raft-addr", ":3001", "Raft communication address")
	addr := flag.String("addr", ":2008", "KV service address")
	dbPath := flag.String("db", "/tmp/mizu-raft", "Database path")
	peers := flag.String("peers", "", "Peers in format: id1@addr1,id2@addr2...")
	flag.Parse()

	if *peers == "" {
		slog.Error("--peers is required", "format", "id1@addr1,id2@addr2...")
		os.Exit(1)
	}

	peerInfos, raftPeers, err := parsePeers(*peers)
	if err != nil {
		slog.Error("Failed to parse peers", "error", err)
		os.Exit(1)
	}
	regions, err := buildStaticRegions(peerInfos)
	if err != nil {
		slog.Error("Failed to build regions", "error", err)
		os.Exit(1)
	}

	storageConf := &config.Config{DBPath: dbPathForNode(*dbPath, *id)}
	store := storage.NewBadgerStorage(storageConf)
	if err := store.Start(); err != nil {
		slog.Error("Failed to start storage", "error", err)
		os.Exit(1)
	}
	defer store.Stop()

	kvCfg := &node.Config{
		NodeID:        *id,
		ClusterID:     *clusterID,
		RaftAddr:      *raftAddr,
		StoragePath:   storageConf.DBPath,
		ElectionTick:  10,
		HeartbeatTick: 1,
		Regions:       regions,
	}

	kvNode, err := node.New(kvCfg, store)
	if err != nil {
		slog.Error("Failed to create KVNode", "error", err)
		os.Exit(1)
	}

	transCfg := transport.Config{
		ID:    *id,
		Addr:  *raftAddr,
		Peers: raftPeers,
	}
	trans := transport.New(transCfg)
	if err := trans.Start(); err != nil {
		slog.Error("Failed to start transport", "error", err)
		os.Exit(1)
	}
	defer trans.Close()

	kvNode.SetTransport(trans)

	if err := kvNode.Start(); err != nil {
		slog.Error("Failed to start KVNode", "error", err)
		os.Exit(1)
	}
	defer kvNode.Stop()

	srv := grpc.NewServer()
	raftKVServer := node.NewServer(kvNode)
	raftkvpb.RegisterRaftKVServer(srv, raftKVServer)

	lis, err := net.Listen("tcp", *addr)
	if err != nil {
		slog.Error("Failed to listen", "error", err)
		os.Exit(1)
	}

	slog.Info("RaftKV node starting", "id", *id, "addr", *addr, "raft", *raftAddr)
	if err := srv.Serve(lis); err != nil {
		slog.Error("Failed to serve", "error", err)
		os.Exit(1)
	}
}

func parsePeers(peersStr string) ([]proto.PeerInfo, map[uint64]string, error) {
	var peerInfos []proto.PeerInfo
	raftPeers := make(map[uint64]string)

	for _, p := range splitPeers(peersStr) {
		id, addr, err := parsePeer(p)
		if err != nil {
			return nil, nil, err
		}
		peerInfos = append(peerInfos, proto.PeerInfo{NodeID: id, Addr: addr})
		raftPeers[id] = addr
	}

	return peerInfos, raftPeers, nil
}

func splitPeers(s string) []string {
	var peers []string
	current := ""
	for _, c := range s {
		if c == ',' {
			if current != "" {
				peers = append(peers, current)
				current = ""
			}
		} else {
			current += string(c)
		}
	}
	if current != "" {
		peers = append(peers, current)
	}
	return peers
}

func parsePeer(p string) (uint64, string, error) {
	for i, c := range p {
		if c == '@' {
			id, err := strconv.ParseUint(p[:i], 10, 64)
			if err != nil {
				return 0, "", err
			}
			return id, p[i+1:], nil
		}
	}
	return 0, "", strconv.ErrSyntax
}

func dbPathForNode(base string, id uint64) string {
	return base + "-" + strconv.FormatUint(id, 10)
}

func buildStaticRegions(peers []proto.PeerInfo) ([]*region.Region, error) {
	if len(peers) == 0 {
		return nil, errors.New("at least one peer is required")
	}

	leader := peers[0]
	return []*region.Region{
		{
			ID:       1,
			StartKey: []byte{},
			EndKey:   []byte("m"),
			Peers:    peers,
			Leader:   leader,
		},
		{
			ID:       2,
			StartKey: []byte("m"),
			EndKey:   []byte("t"),
			Peers:    peers,
			Leader:   leader,
		},
		{
			ID:       3,
			StartKey: []byte("t"),
			EndKey:   []byte{},
			Peers:    peers,
			Leader:   leader,
		},
	}, nil
}
