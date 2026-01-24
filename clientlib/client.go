package clientlib

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/zbchi/mizu/kv/raftstore"
	"github.com/zbchi/mizu/proto/raftkvpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type regionRoute struct {
	id           uint64
	startKey     []byte
	endKey       []byte
	leaderNodeID uint64
}

// Client is a minimal RaftKV client that retries across nodes and follows leader hints.
type Client struct {
	clusterID uint64

	mu           sync.Mutex
	nodeAddrs    map[uint64]string
	nodeIDs      []uint64
	conns        map[uint64]*grpc.ClientConn
	clients      map[uint64]raftkvpb.RaftKVClient
	regionRoutes map[uint64]regionRoute
}

// New creates a retrying client from a nodeID -> KV address map.
func New(clusterID uint64, nodeAddrs map[uint64]string) *Client {
	ids := make([]uint64, 0, len(nodeAddrs))
	cloned := make(map[uint64]string, len(nodeAddrs))
	for id, addr := range nodeAddrs {
		ids = append(ids, id)
		cloned[id] = addr
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })

	return &Client{
		clusterID:    clusterID,
		nodeAddrs:    cloned,
		nodeIDs:      ids,
		conns:        make(map[uint64]*grpc.ClientConn),
		clients:      make(map[uint64]raftkvpb.RaftKVClient),
		regionRoutes: make(map[uint64]regionRoute),
	}
}

// ParseNodeAddrs parses an "id@addr,id@addr" list used by the demo client and tests.
func ParseNodeAddrs(spec string) (map[uint64]string, error) {
	nodes := make(map[uint64]string)
	if spec == "" {
		return nodes, errors.New("empty node address list")
	}

	for _, part := range strings.Split(spec, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}

		pieces := strings.SplitN(part, "@", 2)
		if len(pieces) != 2 || pieces[0] == "" || pieces[1] == "" {
			return nil, fmt.Errorf("invalid node address %q, want id@addr", part)
		}

		var id uint64
		if _, err := fmt.Sscanf(pieces[0], "%d", &id); err != nil {
			return nil, fmt.Errorf("invalid node id in %q: %w", part, err)
		}
		nodes[id] = pieces[1]
	}

	if len(nodes) == 0 {
		return nil, errors.New("no valid node addresses found")
	}
	return nodes, nil
}

// Close closes all underlying gRPC connections.
func (c *Client) Close() error {
	c.mu.Lock()
	conns := make([]*grpc.ClientConn, 0, len(c.conns))
	for _, conn := range c.conns {
		conns = append(conns, conn)
	}
	c.conns = make(map[uint64]*grpc.ClientConn)
	c.clients = make(map[uint64]raftkvpb.RaftKVClient)
	c.mu.Unlock()

	var firstErr error
	for _, conn := range conns {
		if err := conn.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// KnownLeader returns the cached leader node ID for a region, if any.
func (c *Client) KnownLeader(regionID uint64) uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()

	route := c.regionRoutes[regionID]
	return route.leaderNodeID
}

// Propose sends a request and retries on network errors and not-leader responses.
func (c *Client) Propose(ctx context.Context, req *raftkvpb.RaftCmdRequest) (*raftkvpb.RaftCmdResponse, error) {
	if req == nil {
		return nil, errors.New("nil request")
	}
	if len(req.Requests) == 0 {
		return nil, errors.New("empty requests")
	}

	// Retries update headers and may inspect payload keys repeatedly; work on a deep copy so
	// the caller never observes client-side mutations across attempts.
	req = cloneRequest(req)
	if req.Header == nil {
		req.Header = &raftkvpb.RequestHeader{}
	}
	if req.Header.ClusterId == 0 {
		req.Header.ClusterId = c.clusterID
	}

	targetRegionID := c.cachedRegionForRequest(req)
	candidates := c.candidateNodeIDs(targetRegionID)
	if len(candidates) == 0 {
		return nil, errors.New("client has no known nodes")
	}

	tried := make(map[uint64]struct{}, len(candidates))
	var lastResp *raftkvpb.RaftCmdResponse
	var lastErr error

	for {
		if err := ctx.Err(); err != nil {
			if lastErr != nil {
				return lastResp, lastErr
			}
			return nil, err
		}

		nodeID, ok := nextCandidate(candidates, tried)
		if !ok {
			tried = make(map[uint64]struct{}, len(c.nodeIDs))
			candidates = c.candidateNodeIDs(targetRegionID)
			nodeID, ok = nextCandidate(candidates, tried)
			if !ok {
				if lastErr != nil {
					return lastResp, lastErr
				}
				return nil, errors.New("client has no known nodes")
			}

			// Give elections and reconnects a brief chance to settle before starting another round.
			timer := time.NewTimer(10 * time.Millisecond)
			select {
			case <-timer.C:
			case <-ctx.Done():
				timer.Stop()
				if lastErr != nil {
					return lastResp, lastErr
				}
				return nil, ctx.Err()
			}
		}

		tried[nodeID] = struct{}{}

		resp, err := c.proposeToNode(ctx, nodeID, req)
		if err != nil {
			lastErr = err
			continue
		}

		lastResp = resp
		if resp.Header != nil {
			// Cache whatever region range/leader metadata the server knows, even on failures.
			targetRegionID = c.rememberHeader(resp.Header)
		}

		if resp.Header == nil || resp.Header.Success {
			return resp, nil
		}

		lastErr = errors.New(resp.Header.Error)

		if resp.Header.LeaderNodeId != 0 {
			targetRegionID = resp.Header.RegionId
			// Retry the hinted leader immediately even if it already failed earlier in this round.
			delete(tried, resp.Header.LeaderNodeId)
			candidates = prioritizeNode(candidates, resp.Header.LeaderNodeId)
		}

		if !retryableHeader(resp.Header) {
			return resp, lastErr
		}
	}
}

func (c *Client) proposeToNode(ctx context.Context, nodeID uint64, req *raftkvpb.RaftCmdRequest) (*raftkvpb.RaftCmdResponse, error) {
	client, err := c.clientForNode(nodeID)
	if err != nil {
		return nil, err
	}
	return client.Propose(ctx, req)
}

func (c *Client) clientForNode(nodeID uint64) (raftkvpb.RaftKVClient, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if client, ok := c.clients[nodeID]; ok {
		return client, nil
	}

	addr, ok := c.nodeAddrs[nodeID]
	if !ok {
		return nil, fmt.Errorf("unknown node %d", nodeID)
	}

	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}

	client := raftkvpb.NewRaftKVClient(conn)
	c.conns[nodeID] = conn
	c.clients[nodeID] = client
	return client, nil
}

func (c *Client) cachedRegionForRequest(req *raftkvpb.RaftCmdRequest) uint64 {
	key, err := requestKey(req)
	if err != nil {
		return 0
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	for _, route := range c.regionRoutes {
		if contains(route, key) {
			return route.id
		}
	}
	return 0
}

func (c *Client) rememberHeader(header *raftkvpb.ResponseHeader) uint64 {
	if header == nil || header.RegionId == 0 {
		return 0
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	route := c.regionRoutes[header.RegionId]
	route.id = header.RegionId
	route.startKey = cloneBytes(header.RegionStartKey)
	route.endKey = cloneBytes(header.RegionEndKey)
	if header.LeaderNodeId != 0 {
		route.leaderNodeID = header.LeaderNodeId
	}
	c.regionRoutes[header.RegionId] = route
	return route.id
}

func (c *Client) candidateNodeIDs(regionID uint64) []uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()

	candidates := make([]uint64, 0, len(c.nodeIDs))
	if regionID != 0 {
		if route, ok := c.regionRoutes[regionID]; ok && route.leaderNodeID != 0 {
			// Put the cached leader first so steady-state requests stay single hop.
			candidates = append(candidates, route.leaderNodeID)
		}
	}

	for _, id := range c.nodeIDs {
		if len(candidates) > 0 && candidates[0] == id {
			continue
		}
		candidates = append(candidates, id)
	}
	return candidates
}

func retryableHeader(header *raftkvpb.ResponseHeader) bool {
	if header == nil || header.Success {
		return false
	}
	if header.LeaderNodeId != 0 {
		return true
	}
	return strings.Contains(strings.ToLower(header.Error), raftstore.ErrNotLeader.Error())
}

func requestKey(req *raftkvpb.RaftCmdRequest) ([]byte, error) {
	switch req.Requests[0].CmdType {
	case raftkvpb.CmdType_Get:
		if req.Requests[0].Get == nil {
			return nil, errors.New("get request payload is missing")
		}
		return req.Requests[0].Get.Key, nil
	case raftkvpb.CmdType_Put:
		if req.Requests[0].Put == nil {
			return nil, errors.New("put request payload is missing")
		}
		return req.Requests[0].Put.Key, nil
	case raftkvpb.CmdType_Delete:
		if req.Requests[0].Delete == nil {
			return nil, errors.New("delete request payload is missing")
		}
		return req.Requests[0].Delete.Key, nil
	case raftkvpb.CmdType_Scan:
		if req.Requests[0].Scan == nil {
			return nil, errors.New("scan request payload is missing")
		}
		return req.Requests[0].Scan.StartKey, nil
	default:
		return nil, fmt.Errorf("unsupported command type %s", req.Requests[0].CmdType.String())
	}
}

func nextCandidate(candidates []uint64, tried map[uint64]struct{}) (uint64, bool) {
	for _, id := range candidates {
		if _, seen := tried[id]; !seen {
			return id, true
		}
	}
	return 0, false
}

func prioritizeNode(candidates []uint64, nodeID uint64) []uint64 {
	out := make([]uint64, 0, len(candidates)+1)
	out = append(out, nodeID)
	for _, id := range candidates {
		if id != nodeID {
			out = append(out, id)
		}
	}
	return out
}

func contains(route regionRoute, key []byte) bool {
	if bytes.Compare(key, route.startKey) < 0 {
		return false
	}
	if len(route.endKey) == 0 {
		return true
	}
	return bytes.Compare(key, route.endKey) < 0
}

func cloneRequest(req *raftkvpb.RaftCmdRequest) *raftkvpb.RaftCmdRequest {
	out := &raftkvpb.RaftCmdRequest{}
	if req.Header != nil {
		out.Header = &raftkvpb.RequestHeader{
			ClusterId: req.Header.GetClusterId(),
			NodeId:    req.Header.GetNodeId(),
		}
	}

	out.Requests = make([]*raftkvpb.Request, len(req.Requests))
	for i, r := range req.Requests {
		if r == nil {
			continue
		}

		cloned := &raftkvpb.Request{CmdType: r.CmdType}
		if r.Get != nil {
			cloned.Get = &raftkvpb.GetRequest{Cf: r.Get.Cf, Key: cloneBytes(r.Get.Key)}
		}
		if r.Put != nil {
			cloned.Put = &raftkvpb.PutRequest{Cf: r.Put.Cf, Key: cloneBytes(r.Put.Key), Value: cloneBytes(r.Put.Value)}
		}
		if r.Delete != nil {
			cloned.Delete = &raftkvpb.DeleteRequest{Cf: r.Delete.Cf, Key: cloneBytes(r.Delete.Key)}
		}
		if r.Scan != nil {
			cloned.Scan = &raftkvpb.ScanRequest{Cf: r.Scan.Cf, StartKey: cloneBytes(r.Scan.StartKey), Limit: r.Scan.Limit}
		}
		out.Requests[i] = cloned
	}
	return out
}

func cloneBytes(src []byte) []byte {
	if len(src) == 0 {
		return nil
	}
	dst := make([]byte, len(src))
	copy(dst, src)
	return dst
}
