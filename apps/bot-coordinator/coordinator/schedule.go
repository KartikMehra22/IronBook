// Package coordinator compiles a Scenario into a deterministic schedule of
// orders for the bot-coordinator to dispatch.
//
// Phase 2 keeps the compiler minimal: yamlSpec is ignored, seed drives a
// PRNG, durationSec sets the number of 1ms slots. Phase 3 introduces a
// real DSL parser. The ScheduleHash output is what divergence-detector
// will compare across runs to confirm replay determinism.
package coordinator

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"math/rand"
)

type Event struct {
	OffsetNs uint64
	BotID    uint64
	Order    OrderSpec
}

type OrderSpec struct {
	Side      string
	OrderType string
	TIF       string
	Qty       uint64
	Price     int64
}

// Compile produces one limit order per millisecond for durationSec seconds.
// The same (seed, durationSec) pair always yields the same event slice.
func Compile(_yamlSpec string, seed int64, durationSec int) []Event {
	r := rand.New(rand.NewSource(seed))
	total := durationSec * 1000
	if total < 0 {
		total = 0
	}
	out := make([]Event, 0, total)
	sides := [2]string{"bid", "ask"}
	for i := 0; i < total; i++ {
		out = append(out, Event{
			OffsetNs: uint64(i) * 1_000_000,
			BotID:    uint64(r.Intn(8) + 1),
			Order: OrderSpec{
				Side:      sides[r.Intn(2)],
				OrderType: "limit",
				TIF:       "gtc",
				Qty:       uint64(1 + r.Intn(20)),
				Price:     90 + int64(r.Intn(20)),
			},
		})
	}
	return out
}

// ScheduleHash is sha256 over the canonical encoding of the event slice.
// Stable across runs given the same (seed, durationSec).
func ScheduleHash(events []Event) string {
	h := sha256.New()
	var b [8]byte
	for _, e := range events {
		binary.LittleEndian.PutUint64(b[:], e.OffsetNs)
		h.Write(b[:])
		binary.LittleEndian.PutUint64(b[:], e.BotID)
		h.Write(b[:])
		binary.LittleEndian.PutUint64(b[:], e.Order.Qty)
		h.Write(b[:])
		binary.LittleEndian.PutUint64(b[:], uint64(e.Order.Price))
		h.Write(b[:])
		h.Write([]byte(e.Order.Side))
		h.Write([]byte(e.Order.OrderType))
		h.Write([]byte(e.Order.TIF))
	}
	return hex.EncodeToString(h.Sum(nil))
}
