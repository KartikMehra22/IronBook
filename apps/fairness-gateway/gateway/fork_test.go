package gateway

import (
	"context"
	"net"
	"sync"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	pb "github.com/KartikMehra22/IronBook/pkg/proto/ironbook/v1"
)

// recordingServer implements OrderIntakeServer and stores every order it sees.
type recordingServer struct {
	pb.UnimplementedOrderIntakeServer
	mu   sync.Mutex
	seen []*pb.NormalizedOrder
	name string
}

func (r *recordingServer) Submit(_ context.Context, o *pb.NormalizedOrder) (*pb.Reply, error) {
	r.mu.Lock()
	r.seen = append(r.seen, o)
	r.mu.Unlock()
	return &pb.Reply{
		Ack: &pb.Ack{
			PlatformSeq: o.PlatformSeq,
			AckTsNs:     42,
			Status:      0,
			Message:     r.name,
		},
	}, nil
}

func (r *recordingServer) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.seen)
}

// bufconnClient creates an OrderIntakeClient backed by an in-process bufconn.
// Returns the recording server so tests can inspect what it saw.
func bufconnClient(t *testing.T) (pb.OrderIntakeClient, *recordingServer) {
	t.Helper()
	lis := bufconn.Listen(1 << 16)
	srv := grpc.NewServer()
	rec := &recordingServer{name: "fixture"}
	pb.RegisterOrderIntakeServer(srv, rec)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)

	dialer := func(_ context.Context, _ string) (net.Conn, error) { return lis.Dial() }
	conn, err := grpc.NewClient(
		"passthrough://bufnet",
		grpc.WithContextDialer(dialer),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("dial bufconn: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return pb.NewOrderIntakeClient(conn), rec
}

// stubSink captures Sink events for assertion.
type stubSink struct {
	mu      sync.Mutex
	ins     int
	replies map[string]int
}

func newStubSink() *stubSink { return &stubSink{replies: map[string]int{}} }

func (s *stubSink) OnIn(_ *pb.NormalizedOrder) { s.mu.Lock(); s.ins++; s.mu.Unlock() }
func (s *stubSink) OnReply(_ *pb.NormalizedOrder, source string, _ *pb.Reply) {
	s.mu.Lock()
	s.replies[source]++
	s.mu.Unlock()
}

func TestFork_FansOutToBothEndpoints(t *testing.T) {
	subCli, subRec := bufconnClient(t)
	subRec.name = "submission"
	orCli, orRec := bufconnClient(t)
	orRec.name = "oracle"
	sink := newStubSink()

	f := &Fork{Submission: subCli, Oracle: orCli, Sink: sink}
	order := &pb.NormalizedOrder{PlatformSeq: 7, PlatformTsNs: 7, Qty: 1, Price: 100}

	rep, err := f.Process(context.Background(), order)
	if err != nil {
		t.Fatalf("Process: %v", err)
	}
	if rep == nil || rep.GetAck().GetPlatformSeq() != 7 {
		t.Fatalf("unexpected reply: %+v", rep)
	}
	if subRec.count() != 1 {
		t.Fatalf("submission did not see the order; count=%d", subRec.count())
	}
	if orRec.count() != 1 {
		t.Fatalf("oracle did not see the order; count=%d", orRec.count())
	}
	if rep.GetAck().GetMessage() != "submission" {
		t.Fatalf("Process should return submission's reply, got %q", rep.GetAck().GetMessage())
	}
	if sink.ins != 1 {
		t.Fatalf("sink should have seen 1 input, got %d", sink.ins)
	}
	if sink.replies["submission"] != 1 || sink.replies["oracle"] != 1 {
		t.Fatalf("sink replies wrong: %+v", sink.replies)
	}
}

func TestPackClientOrderID_DecodesBackToBotIDAndLocalSeq(t *testing.T) {
	packed := PackClientOrderID(0xCAFEBABE, 0xDEADBEEF)
	want := []byte{0, 0, 0, 0, 0xCA, 0xFE, 0xBA, 0xBE, 0, 0, 0, 0, 0xDE, 0xAD, 0xBE, 0xEF}
	for i, b := range want {
		if packed[i] != b {
			t.Fatalf("byte %d: got %02x want %02x", i, packed[i], b)
		}
	}
}

func TestSessionTokenFor_IsStableAcrossCalls(t *testing.T) {
	secret := []byte("ironbook-run-secret-fixed-bytes")
	a := SessionTokenFor(42, secret)
	b := SessionTokenFor(42, secret)
	if a != b {
		t.Fatalf("session token not deterministic for same input")
	}
	c := SessionTokenFor(43, secret)
	if a == c {
		t.Fatalf("session token must differ across bot_ids")
	}
}
