package main

import (
	"context"
	"flag"
	"log/slog"
	"os"
	"time"

	"github.com/zbchi/mizu/clientlib"
	"github.com/zbchi/mizu/proto/raftkvpb"
)

func main() {
	clusterID := flag.Uint64("cluster", 1, "Cluster ID")
	servers := flag.String("servers", "1@127.0.0.1:2008,2@127.0.0.1:2009,3@127.0.0.1:2010", "KV servers in format: id@addr,id@addr,...")
	timeout := flag.Duration("timeout", 3*time.Second, "Per-request timeout")
	flag.Parse()

	nodeAddrs, err := clientlib.ParseNodeAddrs(*servers)
	if err != nil {
		slog.Error("invalid --servers", "error", err)
		os.Exit(1)
	}

	client := clientlib.New(*clusterID, nodeAddrs)
	defer client.Close()

	run := func(name string, req *raftkvpb.RaftCmdRequest) *raftkvpb.RaftCmdResponse {
		ctx, cancel := context.WithTimeout(context.Background(), *timeout)
		defer cancel()

		resp, err := client.Propose(ctx, req)
		if err != nil {
			slog.Error(name+" failed", "error", err, "header", respHeader(resp))
			os.Exit(1)
		}
		slog.Info(name+" succeeded", "header", respHeader(resp))
		return resp
	}

	run("Put alpha", writeReq(raftkvpb.CmdType_Put, &raftkvpb.Request{
		CmdType: raftkvpb.CmdType_Put,
		Put:     &raftkvpb.PutRequest{Cf: "default", Key: []byte("alpha"), Value: []byte("v-alpha")},
	}))
	run("Put beta", writeReq(raftkvpb.CmdType_Put, &raftkvpb.Request{
		CmdType: raftkvpb.CmdType_Put,
		Put:     &raftkvpb.PutRequest{Cf: "default", Key: []byte("beta"), Value: []byte("v-beta")},
	}))
	run("Put moon", writeReq(raftkvpb.CmdType_Put, &raftkvpb.Request{
		CmdType: raftkvpb.CmdType_Put,
		Put:     &raftkvpb.PutRequest{Cf: "default", Key: []byte("moon"), Value: []byte("v-moon")},
	}))
	run("Put zebra", writeReq(raftkvpb.CmdType_Put, &raftkvpb.Request{
		CmdType: raftkvpb.CmdType_Put,
		Put:     &raftkvpb.PutRequest{Cf: "default", Key: []byte("zebra"), Value: []byte("v-zebra")},
	}))

	resp := run("Get moon", &raftkvpb.RaftCmdRequest{
		Requests: []*raftkvpb.Request{
			{
				CmdType: raftkvpb.CmdType_Get,
				Get:     &raftkvpb.GetRequest{Cf: "default", Key: []byte("moon")},
			},
		},
	})
	if len(resp.Responses) > 0 && resp.Responses[0].Get != nil {
		slog.Info("Get moon value", "value", string(resp.Responses[0].Get.Value))
	}

	resp = run("Scan alpha", &raftkvpb.RaftCmdRequest{
		Requests: []*raftkvpb.Request{
			{
				CmdType: raftkvpb.CmdType_Scan,
				Scan:    &raftkvpb.ScanRequest{Cf: "default", StartKey: []byte("alpha"), Limit: 10},
			},
		},
	})
	if len(resp.Responses) > 0 && resp.Responses[0].Scan != nil {
		for _, pair := range resp.Responses[0].Scan.Pairs {
			slog.Info("Scan pair", "key", string(pair.Key), "value", string(pair.Value))
		}
	}

	run("Delete beta", writeReq(raftkvpb.CmdType_Delete, &raftkvpb.Request{
		CmdType: raftkvpb.CmdType_Delete,
		Delete:  &raftkvpb.DeleteRequest{Cf: "default", Key: []byte("beta")},
	}))

	resp = run("Get beta", &raftkvpb.RaftCmdRequest{
		Requests: []*raftkvpb.Request{
			{
				CmdType: raftkvpb.CmdType_Get,
				Get:     &raftkvpb.GetRequest{Cf: "default", Key: []byte("beta")},
			},
		},
	})
	if len(resp.Responses) > 0 && resp.Responses[0].Get != nil {
		slog.Info("Get beta after delete", "value", string(resp.Responses[0].Get.Value))
	}
}

func writeReq(cmdType raftkvpb.CmdType, req *raftkvpb.Request) *raftkvpb.RaftCmdRequest {
	return &raftkvpb.RaftCmdRequest{
		Requests: []*raftkvpb.Request{req},
	}
}

func respHeader(resp *raftkvpb.RaftCmdResponse) any {
	if resp == nil {
		return nil
	}
	return map[string]any{
		"cluster": resp.Header.GetClusterId(),
		"node":    resp.Header.GetNodeId(),
		"region":  resp.Header.GetRegionId(),
		"leader":  resp.Header.GetLeaderNodeId(),
		"success": resp.Header.GetSuccess(),
		"error":   resp.Header.GetError(),
	}
}
