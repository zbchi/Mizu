package main

import (
	"log"
	"net"

	"github.com/zbchi/linkv/kv/config"
	"github.com/zbchi/linkv/kv/server"
	standalonestorage "github.com/zbchi/linkv/kv/storage/standalone_storage"
	"github.com/zbchi/linkv/proto/linkvpb"

	"google.golang.org/grpc"
)

func main() {
	conf := config.Config{DBPath: "/db/linkv"}
	storage := standalonestorage.NewStandaloneStorage(&conf)
	if err := storage.Start(); err != nil {
		log.Fatalf("failed to start storage: %v", err)
	}
	defer storage.Stop()

	srv := grpc.NewServer()
	rawServer := server.NewServer(storage)
	linkvpb.RegisterLinkvServer(srv, rawServer)

	lis, err := net.Listen("tcp", ":2007")
	if err != nil {
		log.Fatal("failed to listen: %v", err)
	}
	if err := srv.Serve(lis); err != nil {
		log.Fatal("failed to serve: %v", err)
	}
}
