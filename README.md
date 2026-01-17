# Mizu

Mizu is a distributed key-value store built on Raft.

It is an open source project under active development, with a focus on replicated writes, linearizable reads, snapshotting, and a clean modular architecture.

> Status: experimental and not production-ready yet.

## Features

- Custom Raft implementation
- gRPC-based peer communication
- Badger-backed local storage
- Replicated writes
- Linearizable reads via `ReadIndex`
- Snapshotting and log compaction

## Quick Start

### Build

```bash
go build -o bin/mizu ./cmd
go build -o bin/mizu-client ./client
```

### Run a Single Node

```bash
go run ./cmd \
  --id 1 \
  --cluster 1 \
  --addr :2008 \
  --raft-addr :3001 \
  --db /tmp/mizu \
  --peers 1@127.0.0.1:3001
```

Run the example client in another terminal:

```bash
go run ./client
```

### Run a 3-Node Cluster

```bash
# terminal 1
go run ./cmd \
  --id 1 \
  --cluster 1 \
  --addr :2008 \
  --raft-addr :3001 \
  --db /tmp/mizu \
  --peers 1@127.0.0.1:3001,2@127.0.0.1:3002,3@127.0.0.1:3003

# terminal 2
go run ./cmd \
  --id 2 \
  --cluster 1 \
  --addr :2009 \
  --raft-addr :3002 \
  --db /tmp/mizu \
  --peers 1@127.0.0.1:3001,2@127.0.0.1:3002,3@127.0.0.1:3003

# terminal 3
go run ./cmd \
  --id 3 \
  --cluster 1 \
  --addr :2010 \
  --raft-addr :3003 \
  --db /tmp/mizu \
  --peers 1@127.0.0.1:3001,2@127.0.0.1:3002,3@127.0.0.1:3003
```

## Project Layout

```text
cmd/           server entrypoint
client/        example client
raft/          custom Raft implementation
kv/            node, raftstore, storage, transport
proto/         protobuf definitions
```

## Development

```bash
go test ./...
make proto
```

## Roadmap

- Improve multi-region support
- Improve recovery and consistency testing
- Improve error handling and observability
