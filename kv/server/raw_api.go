package server

import (
	"context"

	"github.com/zbchi/linkv/kv/storage"
	linkvpb "github.com/zbchi/linkv/proto/pkg"
)

type Server struct {
	storage storage.Storage
}

func (s *Server) RawGet(ctx context.Context, req *linkvpb.RawGetRequest) (*linkvpb.RawGetResponse, error) {
	reader, err := s.storage.Reader(req.Context)
	if err != nil {
		return nil, err
	}
	defer reader.Close()

	val, err := reader.GetCF(req.Cf, req.Key)
	if err != nil {
		return nil, err
	}

	resp := &linkvpb.RawGetResponse{Value: val}
	return resp, nil
}

func (s *Server) RawPut(ctx context.Context, req *linkvpb.RawPutRequest) (*linkvpb.RawPutResponse, error) {
	put := storage.Put{Key: req.Key, Value: req.Value, Cf: req.Cf}
	mods := []storage.Modify{{Data: put}}
	err := s.storage.Write(req.Context, mods)
	return &linkvpb.RawPutResponse{}, err
}

func (s *Server) RawDelete(ctx *context.Context, req *linkvpb.RawDeleteRequest) (*linkvpb.RawDeleteResponse, error) {
	del := storage.Delete{Key: req.Key, Cf: req.Cf}
	mods := []storage.Modify{{Data: del}}
	err := s.storage.Write(req.Context, mods)
	return &linkvpb.RawDeleteResponse{}, err
}

func (s *Server) RawScan(ctx *context.Context, req *linkvpb.RawScanRequest) (*linkvpb.RawScanResponse, error) {
	reader, err := s.storage.Reader(req.Context)
	if err != nil {
		return nil, err
	}
	defer reader.Close()

	iter := reader.IterCF(req.Cf)
	defer iter.Close()

	iter.Seek(req.StartKey)
	kvPairs := make([]*linkvpb.KvPair, 0)

	for ; iter.Valid() && len(kvPairs) < int(req.Limit); iter.Next() {
		item := iter.Item()
		key := item.Key()
		val, err := item.ValueCopy(nil)
		if err != nil {
			continue
		}
		kvPairs = append(kvPairs, &linkvpb.KvPair{Key: key, Value: val})
	}
	return &linkvpb.RawScanResponse{Pairs: kvPairs}, nil
}
