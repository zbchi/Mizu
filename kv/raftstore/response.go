package raftstore

import (
	"errors"
	"fmt"

	"github.com/zbchi/mizu/kv/region"
	"github.com/zbchi/mizu/proto/raftkvpb"
)

// ResponseMeta carries routing metadata surfaced back to clients.
type ResponseMeta struct {
	ClusterID    uint64
	NodeID       uint64
	RegionID     uint64
	RegionStart  []byte
	RegionEnd    []byte
	LeaderNodeID uint64
}

func MetaFromRegion(clusterID, nodeID uint64, reg *region.Region, leaderNodeID uint64) ResponseMeta {
	meta := ResponseMeta{
		ClusterID:    clusterID,
		NodeID:       nodeID,
		LeaderNodeID: leaderNodeID,
	}
	if reg == nil {
		return meta
	}

	meta.RegionID = reg.ID
	meta.RegionStart = cloneBytes(reg.StartKey)
	meta.RegionEnd = cloneBytes(reg.EndKey)
	return meta
}

// BuildResponse constructs a Raft command response with routing hints.
func BuildResponse(req *raftkvpb.RaftCmdRequest, meta ResponseMeta, responses []*raftkvpb.Response, err error) *raftkvpb.RaftCmdResponse {
	clusterID := meta.ClusterID
	if clusterID == 0 && req != nil && req.Header != nil {
		clusterID = req.Header.ClusterId
	}

	return &raftkvpb.RaftCmdResponse{
		Header: &raftkvpb.ResponseHeader{
			ClusterId:      clusterID,
			NodeId:         meta.NodeID,
			Success:        err == nil,
			Error:          humanError(err, meta),
			RegionId:       meta.RegionID,
			RegionStartKey: cloneBytes(meta.RegionStart),
			RegionEndKey:   cloneBytes(meta.RegionEnd),
			LeaderNodeId:   meta.LeaderNodeID,
		},
		Responses: responses,
	}
}

// BuildWriteResponses mirrors write requests back as empty write responses.
func BuildWriteResponses(req *raftkvpb.RaftCmdRequest) []*raftkvpb.Response {
	if req == nil {
		return nil
	}

	responses := make([]*raftkvpb.Response, 0, len(req.Requests))
	for _, r := range req.Requests {
		switch r.CmdType {
		case raftkvpb.CmdType_Put:
			responses = append(responses, &raftkvpb.Response{
				CmdType: raftkvpb.CmdType_Put,
				Put:     &raftkvpb.PutResponse{},
			})
		case raftkvpb.CmdType_Delete:
			responses = append(responses, &raftkvpb.Response{
				CmdType: raftkvpb.CmdType_Delete,
				Delete:  &raftkvpb.DeleteResponse{},
			})
		}
	}
	return responses
}

func humanError(err error, meta ResponseMeta) string {
	if err == nil {
		return ""
	}

	// Keep the public error strings descriptive because the client uses the header metadata
	// and these messages together to decide whether to retry, reroute, or surface the error.
	switch {
	case errors.Is(err, ErrNotLeader):
		if meta.RegionID == 0 {
			return "not leader"
		}
		if meta.LeaderNodeID != 0 {
			return fmt.Sprintf("not leader for region %d %s; current leader is node %d", meta.RegionID, formatRegionBounds(meta.RegionStart, meta.RegionEnd), meta.LeaderNodeID)
		}
		return fmt.Sprintf("not leader for region %d %s; leader is currently unknown", meta.RegionID, formatRegionBounds(meta.RegionStart, meta.RegionEnd))
	case errors.Is(err, ErrRegionNotFound):
		if meta.RegionID == 0 {
			return fmt.Sprintf("region not found on node %d", meta.NodeID)
		}
		return fmt.Sprintf("region %d %s is not available on node %d", meta.RegionID, formatRegionBounds(meta.RegionStart, meta.RegionEnd), meta.NodeID)
	case errors.Is(err, ErrKeyNotInRegion):
		if meta.RegionID == 0 {
			return "key does not belong to any configured region"
		}
		return fmt.Sprintf("key belongs to region %d %s", meta.RegionID, formatRegionBounds(meta.RegionStart, meta.RegionEnd))
	default:
		return err.Error()
	}
}

func formatRegionBounds(start, end []byte) string {
	return fmt.Sprintf("[%q,%q)", string(start), string(end))
}

func cloneBytes(src []byte) []byte {
	if len(src) == 0 {
		return nil
	}
	dst := make([]byte, len(src))
	copy(dst, src)
	return dst
}
