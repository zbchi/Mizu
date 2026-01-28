package main

import (
	"context"
	"flag"
	"log/slog"
	"os"
	"time"

	"github.com/zbchi/mizu/client"
	"github.com/zbchi/mizu/proto/kvpb"
)

func main() {
	clusterID := flag.Uint64("cluster", 1, "Cluster ID")
	servers := flag.String("servers", "1@127.0.0.1:2008,2@127.0.0.1:2009,3@127.0.0.1:2010", "KV servers in format: id@addr,id@addr,...")
	timeout := flag.Duration("timeout", 3*time.Second, "Per-request timeout")
	flag.Parse()

	nodeAddrs, err := client.ParseNodeAddrs(*servers)
	if err != nil {
		slog.Error("invalid --servers", "error", err)
		os.Exit(1)
	}

	kvClient := client.New(*clusterID, nodeAddrs)
	defer kvClient.Close()

	run := func(name string, req *kvpb.RaftCmdRequest) *kvpb.RaftCmdResponse {
		ctx, cancel := context.WithTimeout(context.Background(), *timeout)
		defer cancel()

		resp, err := kvClient.Propose(ctx, req)
		if err != nil {
			slog.Error(name+" failed", "error", err, "header", respHeader(resp))
			os.Exit(1)
		}
		slog.Info(name+" succeeded", "header", respHeader(resp))
		return resp
	}

	run("Put alpha", writeReq(kvpb.CmdType_Put, &kvpb.Request{
		CmdType: kvpb.CmdType_Put,
		Put:     &kvpb.PutRequest{Cf: "default", Key: []byte("alpha"), Value: []byte("v-alpha")},
	}))
	run("Put beta", writeReq(kvpb.CmdType_Put, &kvpb.Request{
		CmdType: kvpb.CmdType_Put,
		Put:     &kvpb.PutRequest{Cf: "default", Key: []byte("beta"), Value: []byte("v-beta")},
	}))
	run("Put moon", writeReq(kvpb.CmdType_Put, &kvpb.Request{
		CmdType: kvpb.CmdType_Put,
		Put:     &kvpb.PutRequest{Cf: "default", Key: []byte("moon"), Value: []byte("v-moon")},
	}))
	run("Put zebra", writeReq(kvpb.CmdType_Put, &kvpb.Request{
		CmdType: kvpb.CmdType_Put,
		Put:     &kvpb.PutRequest{Cf: "default", Key: []byte("zebra"), Value: []byte("v-zebra")},
	}))

	resp := run("Get moon", &kvpb.RaftCmdRequest{
		Requests: []*kvpb.Request{
			{
				CmdType: kvpb.CmdType_Get,
				Get:     &kvpb.GetRequest{Cf: "default", Key: []byte("moon")},
			},
		},
	})
	if len(resp.Responses) > 0 && resp.Responses[0].Get != nil {
		slog.Info("Get moon value", "value", string(resp.Responses[0].Get.Value))
	}

	resp = run("Scan alpha", &kvpb.RaftCmdRequest{
		Requests: []*kvpb.Request{
			{
				CmdType: kvpb.CmdType_Scan,
				Scan:    &kvpb.ScanRequest{Cf: "default", StartKey: []byte("alpha"), Limit: 10},
			},
		},
	})
	if len(resp.Responses) > 0 && resp.Responses[0].Scan != nil {
		for _, pair := range resp.Responses[0].Scan.Pairs {
			slog.Info("Scan pair", "key", string(pair.Key), "value", string(pair.Value))
		}
	}

	run("Delete beta", writeReq(kvpb.CmdType_Delete, &kvpb.Request{
		CmdType: kvpb.CmdType_Delete,
		Delete:  &kvpb.DeleteRequest{Cf: "default", Key: []byte("beta")},
	}))

	resp = run("Get beta", &kvpb.RaftCmdRequest{
		Requests: []*kvpb.Request{
			{
				CmdType: kvpb.CmdType_Get,
				Get:     &kvpb.GetRequest{Cf: "default", Key: []byte("beta")},
			},
		},
	})
	if len(resp.Responses) > 0 && resp.Responses[0].Get != nil {
		slog.Info("Get beta after delete", "value", string(resp.Responses[0].Get.Value))
	}
}

func writeReq(cmdType kvpb.CmdType, req *kvpb.Request) *kvpb.RaftCmdRequest {
	return &kvpb.RaftCmdRequest{
		Requests: []*kvpb.Request{req},
	}
}

func respHeader(resp *kvpb.RaftCmdResponse) any {
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
