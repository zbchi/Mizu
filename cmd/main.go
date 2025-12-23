package main

import (
	"flag"
	"log"
	"net"
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
		log.Fatal("--peers is required (format: id1@addr1,id2@addr2...)")
	}

	peerInfos, raftPeers, err := parsePeers(*peers)
	if err != nil {
		log.Fatalf("Failed to parse peers: %v", err)
	}

	storageConf := &config.Config{DBPath: dbPathForNode(*dbPath, *id)}
	store := standalonestorage.NewStandaloneStorage(storageConf)
	if err := store.Start(); err != nil {
		log.Fatalf("Failed to start storage: %v", err)
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
		log.Fatalf("Failed to create KVNode: %v", err)
	}

	transCfg := transport.Config{
		ID:    *id,
		Addr:  *raftAddr,
		Peers: raftPeers,
	}
	trans := transport.New(transCfg)
	if err := trans.Start(); err != nil {
		log.Fatalf("Failed to start transport: %v", err)
	}
	defer trans.Close()

	node.Router().SetTransport(trans)

	if err := node.Start(); err != nil {
		log.Fatalf("Failed to start KVNode: %v", err)
	}
	defer node.Stop()

	srv := grpc.NewServer()
	raftKVServer := raft_server.NewServer(node)
	raftkvpb.RegisterRaftKVServer(srv, raftKVServer)

	lis, err := net.Listen("tcp", *addr)
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}

	log.Printf("RaftKV node %d starting on %s (raft: %s)", *id, *addr, *raftAddr)
	if err := srv.Serve(lis); err != nil {
		log.Fatalf("Failed to serve: %v", err)
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
