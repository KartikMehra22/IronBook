package gateway

import (
	"context"
	"sync/atomic"
	"testing"

	"google.golang.org/grpc"

	pb "github.com/KartikMehra22/IronBook/pkg/proto/ironbook/v1"
)

// fakeTimeClient hands out monotonic batches and counts how many RPCs it served.
type fakeTimeClient struct {
	pb.TimeServiceClient
	nextSeq  uint64
	originTs uint64
	rpcs     atomic.Int32
}

func (f *fakeTimeClient) NextStamps(_ context.Context, req *pb.NextStampsRequest, _ ...grpc.CallOption) (*pb.NextStampsResponse, error) {
	f.rpcs.Add(1)
	n := req.GetBatchSize()
	if n == 0 {
		n = 1
	}
	first := f.nextSeq
	f.nextSeq += uint64(n)
	return &pb.NextStampsResponse{
		FirstSeq:  first,
		FirstTsNs: f.originTs + first*100,
		StepNs:    100,
		BatchSize: n,
	}, nil
}

// The grpc client type embeds *grpc.ClientConn et al; we satisfy only NextStamps
// for the stamper's purposes via interface assignment.
type stubClient struct{ pb.TimeServiceClient }

func TestStamper_OneRPCAmortisesBatch(t *testing.T) {
	fake := &fakeTimeClient{originTs: 1_000_000}
	// We pass the fake directly; the stamper only calls NextStamps.
	var cli pb.TimeServiceClient = fake
	s := NewStamper(cli, 100)

	got := make([]uint64, 100)
	for i := 0; i < 100; i++ {
		seq, _, err := s.Next(context.Background())
		if err != nil {
			t.Fatalf("Next %d: %v", i, err)
		}
		got[i] = seq
	}
	if fake.rpcs.Load() != 1 {
		t.Fatalf("expected 1 RPC across 100 Next() calls, got %d", fake.rpcs.Load())
	}
	for i, seq := range got {
		if seq != uint64(i) {
			t.Fatalf("seq %d wrong: %d", i, seq)
		}
	}

	// 101st call triggers a refill.
	if _, _, err := s.Next(context.Background()); err != nil {
		t.Fatal(err)
	}
	if fake.rpcs.Load() != 2 {
		t.Fatalf("expected 2 RPCs after window exhaustion, got %d", fake.rpcs.Load())
	}
	_ = stubClient{}
}
