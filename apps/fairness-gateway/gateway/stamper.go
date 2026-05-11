// Package gateway implements the fairness-gateway hot path.
//
// `Stamper` fetches monotonic (seq, ts) stamps from the time-service in
// batches (one RPC per ~10k orders), so the per-order amortised cost is
// well under a microsecond.
package gateway

import (
	"context"
	"sync"
	"time"

	pb "github.com/KartikMehra22/IronBook/pkg/proto/ironbook/v1"
)

// Stamper amortises calls to time-service.NextStamps over a configurable batch.
//
// Hot path:
//
//	seq, ts, err := stamper.Next(ctx)
//
// Cold path (refill): hits time-service.NextStamps and updates the local window.
type Stamper struct {
	cli       pb.TimeServiceClient
	batchSize uint32

	mu       sync.Mutex
	nextSeq  uint64
	endSeq   uint64
	originTs uint64
	originAt time.Time
}

// NewStamper builds a stamper that will refill via `cli` whenever its local
// window runs out, asking for `batchSize` stamps each refill.
func NewStamper(cli pb.TimeServiceClient, batchSize uint32) *Stamper {
	if batchSize == 0 {
		batchSize = 10_000
	}
	return &Stamper{cli: cli, batchSize: batchSize}
}

// Next reserves one stamp. Returns (platform_seq, platform_ts_ns).
// The timestamp is the server's first_ts_ns plus locally-measured elapsed
// time since the window was issued, so it stays monotonic between refills.
func (s *Stamper) Next(ctx context.Context) (uint64, uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.nextSeq >= s.endSeq {
		resp, err := s.cli.NextStamps(ctx, &pb.NextStampsRequest{BatchSize: s.batchSize})
		if err != nil {
			return 0, 0, err
		}
		s.nextSeq = resp.GetFirstSeq()
		s.endSeq = resp.GetFirstSeq() + uint64(resp.GetBatchSize())
		s.originTs = resp.GetFirstTsNs()
		s.originAt = time.Now()
	}
	seq := s.nextSeq
	s.nextSeq++
	tsNs := s.originTs + uint64(time.Since(s.originAt).Nanoseconds()) //nolint:gosec
	return seq, tsNs, nil
}
