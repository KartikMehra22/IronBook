package gateway

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sync"

	pb "github.com/KartikMehra22/IronBook/pkg/proto/ironbook/v1"
)

// FileSink writes one JSONL line per event to a local file. Phase-2 stand-in
// for Phase-3's Redpanda producer (which lands in Phase 3 Task 14.3).
type FileSink struct {
	mu sync.Mutex
	f  *os.File
}

// NewFileSink opens (or creates) the JSONL file at `path`, creating parent
// directories as needed.
func NewFileSink(path string) (*FileSink, error) {
	if dir := filepath.Dir(path); dir != "" {
		_ = os.MkdirAll(dir, 0o755)
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	return &FileSink{f: f}, nil
}

type eventRow struct {
	Kind   string              `json:"kind"`
	Source string              `json:"source,omitempty"`
	Order  *pb.NormalizedOrder `json:"order,omitempty"`
	Reply  *pb.Reply           `json:"reply,omitempty"`
}

// OnIn records the gateway-stamped input on the way to fan-out.
func (s *FileSink) OnIn(o *pb.NormalizedOrder) {
	s.write(eventRow{Kind: "in", Order: o})
}

// OnReply records a per-source reply (one from submission, one from oracle).
func (s *FileSink) OnReply(o *pb.NormalizedOrder, source string, r *pb.Reply) {
	s.write(eventRow{Kind: "reply", Source: source, Order: o, Reply: r})
}

// Close flushes and closes the file.
func (s *FileSink) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.f.Close()
}

func (s *FileSink) write(row eventRow) {
	s.mu.Lock()
	defer s.mu.Unlock()
	_ = json.NewEncoder(s.f).Encode(row)
}
