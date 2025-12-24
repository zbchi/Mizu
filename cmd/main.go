package main

import (
	"flag"
	"log/slog"
	"net"
	"os"
	"strconv"

	"github.com/zbchi/linkv/kv/config"
	"github.com/zbchi/linkv/kv/kvnode"
	raft_server "github.com/zbchi/linkv/kv/server"
	standalonestorage "github.com/zbchi/linkv/kv/storage/standalone_storage"
	"github.com/zbchi/linkv/kv/transport"
	"github.com/zbchi/linkv/proto/raftkvpb"

	"google.golang.org/grpc"
)

func main() {
	id := flag.Uint64("id", 1, "Node ID")
	clusterID := flag.Uint64("cluster", 1, "Cluster ID")
	raftAddr := flag.String("raft-addr", ":3001", "Raft communication address")
	addr := flag.String("addr", ":2008", "KV service address")
	dbPath := flag.String("db", "/tmp/linkv-raft", "Database path")
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

	storageConf := &config.Config{DBPath: dbPathForNode(*dbPath, *id)}
	store := standalonestorage.NewStandaloneStorage(storageConf)
	if err := store.Start(); err != nil {
		slog.Error("Failed to start storage", "error", err)
		os.Exit(1)
	}
	defer store.Stop()

	kvCfg := &kvnode.Config{
		NodeID:        *id,
		ClusterID:     *clusterID,
		RaftAddr:      *raftAddr,
		StoragePath:   storageConf.DBPath,
		ElectionTick:  10,
		HeartbeatTick: 1,
		Peers:         peerInfos,
	}

	node, err := kvnode.NewKVNode(kvCfg, store)
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

	node.Router().SetTransport(trans)

	if err := node.Start(); err != nil {
		slog.Error("Failed to start KVNode", "error", err)
		os.Exit(1)
	}
	defer node.Stop()

	srv := grpc.NewServer()
	raftKVServer := raft_server.NewServer(node)
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

func parsePeers(peersStr string) ([]kvnode.PeerInfo, map[uint64]string, error) {
	var peerInfos []kvnode.PeerInfo
	raftPeers := make(map[uint64]string)

	for _, p := range splitPeers(peersStr) {
		id, addr, err := parsePeer(p)
		if err != nil {
			return nil, nil, err
		}
		peerInfos = append(peerInfos, kvnode.PeerInfo{NodeID: id, Addr: addr})
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
