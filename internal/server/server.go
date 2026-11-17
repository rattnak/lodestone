// Package server exposes the storage engine and vector index over gRPC.
package server

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/rattnak/lodestone/internal/doc"
	"github.com/rattnak/lodestone/internal/index"
	"github.com/rattnak/lodestone/internal/server/pb"
	"github.com/rattnak/lodestone/internal/storage"
)

// Server implements the Lodestone gRPC service over one storage engine and
// one HNSW index.
//
// Durability model: a document upsert writes vector+payload to the engine
// (WAL-backed) and then updates the in-memory index. On startup the index is
// rebuilt from the engine's document keys, so the engine is the single
// source of truth and the index is derived state — DDIA's log-and-views
// pattern in miniature. Snapshot-accelerated rebuilds can come later without
// changing this contract.
type Server struct {
	pb.UnimplementedLodestoneServer
	db *storage.DB
	ix *index.Index
	gs *grpc.Server

	puts     atomic.Uint64
	gets     atomic.Uint64
	searches atomic.Uint64
}

// New opens the engine in dataDir and rebuilds the vector index from it.
func New(dataDir string, opts *storage.Options) (*Server, error) {
	db, err := storage.Open(dataDir, opts)
	if err != nil {
		return nil, err
	}
	s := &Server{db: db, ix: index.New(index.Options{})}

	seq, errCheck := db.All()
	for k, v := range seq {
		id, isDoc := doc.IsDocKey(k)
		if !isDoc {
			continue
		}
		vec, _, err := doc.Decode(v)
		if err != nil {
			db.Close()
			return nil, fmt.Errorf("rebuilding index from doc %d: %w", id, err)
		}
		if err := s.ix.Add(id, vec); err != nil {
			db.Close()
			return nil, fmt.Errorf("rebuilding index from doc %d: %w", id, err)
		}
	}
	if err := errCheck(); err != nil {
		db.Close()
		return nil, err
	}
	return s, nil
}

// Serve starts the gRPC server on lis and blocks until Stop.
func (s *Server) Serve(lis net.Listener) error {
	s.gs = grpc.NewServer()
	pb.RegisterLodestoneServer(s.gs, s)
	return s.gs.Serve(lis)
}

// Stop drains in-flight RPCs, stops the gRPC server, and closes the engine.
func (s *Server) Stop() error {
	if s.gs != nil {
		s.gs.GracefulStop()
	}
	return s.db.Close()
}

func (s *Server) Put(ctx context.Context, req *pb.PutRequest) (*pb.PutResponse, error) {
	if len(req.Key) == 0 {
		return nil, status.Error(codes.InvalidArgument, "empty key")
	}
	if len(req.Key) > 0 && req.Key[0] == 0 {
		return nil, status.Error(codes.InvalidArgument, "keys beginning with 0x00 are reserved")
	}
	if err := s.db.Put(req.Key, req.Value); err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	s.puts.Add(1)
	return &pb.PutResponse{}, nil
}

func (s *Server) Get(ctx context.Context, req *pb.GetRequest) (*pb.GetResponse, error) {
	v, ok, err := s.db.Get(req.Key)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	s.gets.Add(1)
	return &pb.GetResponse{Value: v, Found: ok}, nil
}

func (s *Server) Delete(ctx context.Context, req *pb.DeleteRequest) (*pb.DeleteResponse, error) {
	if err := s.db.Delete(req.Key); err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &pb.DeleteResponse{}, nil
}

func (s *Server) UpsertDocument(ctx context.Context, req *pb.UpsertDocumentRequest) (*pb.UpsertDocumentResponse, error) {
	if len(req.Vector) == 0 {
		return nil, status.Error(codes.InvalidArgument, "empty vector")
	}
	// Engine first: if the index update then fails, a restart rebuilds the
	// index from the engine and heals the inconsistency.
	if err := s.db.Put(doc.Key(req.Id), doc.Encode(req.Vector, req.Payload)); err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	if err := s.ix.Delete(req.Id); err != nil && !errors.Is(err, index.ErrNotFound) {
		return nil, status.Error(codes.Internal, err.Error())
	}
	if err := s.ix.Add(req.Id, req.Vector); err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	s.puts.Add(1)
	return &pb.UpsertDocumentResponse{}, nil
}

func (s *Server) DeleteDocument(ctx context.Context, req *pb.DeleteDocumentRequest) (*pb.DeleteDocumentResponse, error) {
	if err := s.db.Delete(doc.Key(req.Id)); err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	if err := s.ix.Delete(req.Id); err != nil && !errors.Is(err, index.ErrNotFound) {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &pb.DeleteDocumentResponse{}, nil
}

func (s *Server) SearchVectors(ctx context.Context, req *pb.SearchVectorsRequest) (*pb.SearchVectorsResponse, error) {
	if req.K == 0 {
		return nil, status.Error(codes.InvalidArgument, "k must be positive")
	}
	res, err := s.ix.Search(req.Vector, int(req.K), int(req.Ef))
	if err != nil {
		if errors.Is(err, index.ErrDimension) {
			return nil, status.Error(codes.InvalidArgument, err.Error())
		}
		return nil, status.Error(codes.Internal, err.Error())
	}
	s.searches.Add(1)
	resp := &pb.SearchVectorsResponse{Hits: make([]*pb.SearchHit, 0, len(res))}
	for _, r := range res {
		hit := &pb.SearchHit{Id: r.ID, Score: r.Score}
		if req.IncludePayloads {
			v, ok, err := s.db.Get(doc.Key(r.ID))
			if err != nil {
				return nil, status.Error(codes.Internal, err.Error())
			}
			if ok {
				if _, payload, err := doc.Decode(v); err == nil {
					hit.Payload = payload
				}
			}
		}
		resp.Hits = append(resp.Hits, hit)
	}
	return resp, nil
}

func (s *Server) WatchTelemetry(req *pb.WatchTelemetryRequest, stream grpc.ServerStreamingServer[pb.TelemetrySnapshot]) error {
	interval := time.Duration(req.IntervalMs) * time.Millisecond
	if interval <= 0 {
		interval = time.Second
	}
	tick := time.NewTicker(interval)
	defer tick.Stop()
	for {
		snap := &pb.TelemetrySnapshot{
			UnixMs:         time.Now().UnixMilli(),
			MemtableBytes:  uint64(s.db.MemtableSize()),
			SstableCount:   uint32(s.db.TableCount()),
			IndexedVectors: uint64(s.ix.Len()),
			PutsTotal:      s.puts.Load(),
			GetsTotal:      s.gets.Load(),
			SearchesTotal:  s.searches.Load(),
		}
		if err := stream.Send(snap); err != nil {
			return err
		}
		select {
		case <-stream.Context().Done():
			return stream.Context().Err()
		case <-tick.C:
		}
	}
}
