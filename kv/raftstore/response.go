package raftstore

import (
	"errors"
	"fmt"

	"github.com/zbchi/mizu/proto/raftkvpb"
)

// buildWriteResponses mirrors write requests back as empty write responses.
func buildWriteResponses(req *raftkvpb.RaftCmdRequest) []*raftkvpb.Response {
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

func responseError(err error, header *raftkvpb.ResponseHeader) string {
	if err == nil {
		return ""
	}

	// Keep the public error strings descriptive because the client uses the header metadata
	// and these messages together to decide whether to retry, reroute, or surface the error.
	switch {
	case errors.Is(err, ErrNotLeader):
		if header.RegionId == 0 {
			return "not leader"
		}
		if header.LeaderNodeId != 0 {
			return fmt.Sprintf("not leader for region %d %s; current leader is node %d", header.RegionId, formatRegionBounds(header.RegionStartKey, header.RegionEndKey), header.LeaderNodeId)
		}
		return fmt.Sprintf("not leader for region %d %s; leader is currently unknown", header.RegionId, formatRegionBounds(header.RegionStartKey, header.RegionEndKey))
	case errors.Is(err, ErrRegionNotFound):
		if header.RegionId == 0 {
			return fmt.Sprintf("region not found on node %d", header.NodeId)
		}
		return fmt.Sprintf("region %d %s is not available on node %d", header.RegionId, formatRegionBounds(header.RegionStartKey, header.RegionEndKey), header.NodeId)
	case errors.Is(err, ErrKeyNotInRegion):
		if header.RegionId == 0 {
			return "key does not belong to any configured region"
		}
		return fmt.Sprintf("key belongs to region %d %s", header.RegionId, formatRegionBounds(header.RegionStartKey, header.RegionEndKey))
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

func requestClusterID(req *raftkvpb.RaftCmdRequest) uint64 {
	if req != nil && req.Header != nil {
		return req.Header.ClusterId
	}
	return 0
}
