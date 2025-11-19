package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/zbchi/linkv/proto/linkvpb"
	"google.golang.org/grpc"
)

func main() {
	conn, err := grpc.Dial("127.0.0.1:2007", grpc.WithInsecure())
	if err != nil {
		log.Fatal("dial error:", err)
	}
	defer conn.Close()

	client := linkvpb.NewLinkvClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	_, err = client.RawPut(ctx, &linkvpb.RawPutRequest{
		Cf:    "default",
		Key:   []byte("key"),
		Value: []byte("value"),
	})
	if err != nil {
		log.Fatal("RawPut error:", err)
	}

	resp, err := client.RawGet(ctx, &linkvpb.RawGetRequest{
		Cf:  "default",
		Key: []byte("key"),
	})
	if err != nil {
		log.Fatal("RawGet error:", err)
	}
	fmt.Printf("key -> %s\n", resp.Value)
}
