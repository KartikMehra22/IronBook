package gateway

import (
	"context"
	"errors"
	"sync"

	pb "github.com/KartikMehra22/IronBook/pkg/proto/ironbook/v1"
)

// Sink receives a copy of every order + reply pair for downstream consumers
// (divergence detector, telemetry-ingester). Implementations must be
// non-blocking on the hot path — drop on full, counter, etc.
type Sink interface {
	OnIn(o *pb.NormalizedOrder)
	OnReply(o *pb.NormalizedOrder, source string, r *pb.Reply)
}

// Fork sends each NormalizedOrder to both the submission and the reference
// oracle in parallel. The bot sees the submission's reply; the oracle's
// reply is teed to the sink so the divergence-detector can join them.
//
// Both fan-out calls must succeed for the order to be considered "delivered".
// If either fails, the per-order error is returned and the run is marked
// invalid downstream (spec §2.3 invariants 2 + 5).
type Fork struct {
	Submission pb.OrderIntakeClient
	Oracle     pb.OrderIntakeClient
	Sink       Sink
}

// Process fans `o` out to both clients in parallel and returns the
// submission's reply. The order is also passed to the sink (once on input,
// once per successful reply).
func (f *Fork) Process(ctx context.Context, o *pb.NormalizedOrder) (*pb.Reply, error) {
	if f.Sink != nil {
		f.Sink.OnIn(o)
	}

	var (
		subR, orR *pb.Reply
		subE, orE error
		wg        sync.WaitGroup
	)
	wg.Add(2)
	go func() {
		defer wg.Done()
		subR, subE = f.Submission.Submit(ctx, o)
		if subE == nil && f.Sink != nil {
			f.Sink.OnReply(o, "submission", subR)
		}
	}()
	go func() {
		defer wg.Done()
		orR, orE = f.Oracle.Submit(ctx, o)
		if orE == nil && f.Sink != nil {
			f.Sink.OnReply(o, "oracle", orR)
		}
	}()
	wg.Wait()

	if subE != nil || orE != nil {
		return nil, errors.Join(subE, orE)
	}
	_ = orR // intentional: oracle's reply is consumed by the sink only
	return subR, nil
}
