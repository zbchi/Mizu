package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/zbchi/linkv/proto/raftkvpb"
	"google.golang.org/grpc"
)

func main() {
	conn, err := grpc.Dial("127.0.0.1:2008", grpc.WithInsecure())
	if err != nil {
		log.Fatal("dial error:", err)
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
		log.Fatal("Propose (Put) error:", err)
	}
	fmt.Println("Put success")

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
		log.Fatal("Propose (Get) error:", err)
	}

	if len(resp.Responses) > 0 && resp.Responses[0].Get != nil {
		fmt.Printf("Get success: key -> %s\n", resp.Responses[0].Get.Value)
	}
}
