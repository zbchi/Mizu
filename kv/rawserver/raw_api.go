package server

import (
	"context"

	"github.com/zbchi/mizu/kv/storage"
	"github.com/zbchi/mizu/proto/mizupb"
)

type Server struct {
	storage storage.Storage
	mizupb.UnimplementedMizuServer
}

func NewServer(st storage.Storage) *Server {
	return &Server{storage: st}
}

func (s *Server) RawGet(ctx context.Context, req *mizupb.RawGetRequest) (*mizupb.RawGetResponse, error) {
	reader, err := s.storage.Reader(req.Context)
	if err != nil {
		return nil, err
	}
	defer reader.Close()

	val, err := reader.GetCF(req.Cf, req.Key)
	if err != nil {
		return nil, err
	}

	resp := &mizupb.RawGetResponse{Value: val}
	return resp, nil
}

func (s *Server) RawPut(ctx context.Context, req *mizupb.RawPutRequest) (*mizupb.RawPutResponse, error) {
	put := storage.Put{Key: req.Key, Value: req.Value, Cf: req.Cf}
	mods := []storage.Modify{{Data: put}}
	err := s.storage.Write(req.Context, mods)
	return &mizupb.RawPutResponse{}, err
}

func (s *Server) RawDelete(ctx context.Context, req *mizupb.RawDeleteRequest) (*mizupb.RawDeleteResponse, error) {
	del := storage.Delete{Key: req.Key, Cf: req.Cf}
	mods := []storage.Modify{{Data: del}}
	err := s.storage.Write(req.Context, mods)
	return &mizupb.RawDeleteResponse{}, err
}

func (s *Server) RawScan(ctx context.Context, req *mizupb.RawScanRequest) (*mizupb.RawScanResponse, error) {
	reader, err := s.storage.Reader(req.Context)
	if err != nil {
		return nil, err
	}
	defer reader.Close()

	iter := reader.IterCF(req.Cf)
	defer iter.Close()

	iter.Seek(req.StartKey)
	kvPairs := make([]*mizupb.KvPair, 0)

	for ; iter.Valid() && len(kvPairs) < int(req.Limit); iter.Next() {
		item := iter.Item()
		key := item.Key()
		val, err := item.ValueCopy(nil)
		if err != nil {
			continue
		}
		kvPairs = append(kvPairs, &mizupb.KvPair{Key: key, Value: val})
	}
	return &mizupb.RawScanResponse{Pairs: kvPairs}, nil
}
