package main

import (
	"context"
	"log/slog"
	"os"
	"time"

	"github.com/zbchi/mizu/proto/raftkvpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	conn, err := grpc.NewClient("127.0.0.1:2008", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		slog.Error("dial error", "error", err)
		os.Exit(1)
	}
	defer conn.Close()

	client := raftkvpb.NewRaftKVClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	// Test Put
	_, err = client.Propose(ctx, &raftkvpb.RaftCmdRequest{
		Header: &raftkvpb.RequestHeader{
			ClusterId: 1,
			NodeId:    1,
		},
		Requests: []*raftkvpb.Request{
			{
				CmdType: raftkvpb.CmdType_Put,
				Put: &raftkvpb.PutRequest{
					Cf:    "default",
					Key:   []byte("key"),
					Value: []byte("value"),
				},
			},
		},
	})
	if err != nil {
		slog.Error("Propose (Put) error", "error", err)
		os.Exit(1)
	}
	slog.Info("Put success")

	// Test Get
	resp, err := client.Propose(ctx, &raftkvpb.RaftCmdRequest{
		Header: &raftkvpb.RequestHeader{
			ClusterId: 1,
			NodeId:    1,
		},
		Requests: []*raftkvpb.Request{
			{
				CmdType: raftkvpb.CmdType_Get,
				Get: &raftkvpb.GetRequest{
					Cf:  "default",
					Key: []byte("key"),
				},
			},
		},
	})
	if err != nil {
		slog.Error("Propose (Get) error", "error", err)
		os.Exit(1)
	}

	if len(resp.Responses) > 0 && resp.Responses[0].Get != nil {
		slog.Info("Get success", "value", string(resp.Responses[0].Get.Value))
	}
}
