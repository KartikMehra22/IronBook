// Command fairness-gateway is the IronBook proxy that stamps every order with
// platform-issued (seq, ts) before forwarding to a contestant's submission.
// Phase 1 ships a stub that just acks orders for end-to-end smoke testing.
// Phase 2 replaces this with the real stamping + identity-stripping + fork-
// to-oracle gateway (see spec §3.4).
package main

import (
	"encoding/json"
	"log"
	"net/http"
	"os"
	"sync/atomic"
	"time"
)

// Order is the wire shape the stub accepts.
type Order struct {
	ClientOrderID string `json:"client_order_id"`
}

// Ack is the wire shape returned to bots/clients.
type Ack struct {
	ClientOrderID string `json:"client_order_id"`
	PlatformSeq   uint64 `json:"platform_seq"`
	AckTsNs       int64  `json:"ack_ts_ns"`
	GatewayPhase  string `json:"gateway_phase"` // "stub" until Phase 2
}

var seq atomic.Uint64

func main() {
	addr := envOr("IRONBOOK_ADDR", ":8080")
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/order", handleOrder)
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("ok"))
	})
	log.Printf("stub fairness-gateway listening on %s", addr)
	if err := http.ListenAndServe(addr, mux); err != nil {
		log.Fatalf("serve: %v", err)
	}
}

func handleOrder(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var o Order
	if err := json.NewDecoder(r.Body).Decode(&o); err != nil {
		http.Error(w, "bad json: "+err.Error(), http.StatusBadRequest)
		return
	}
	ack := Ack{
		ClientOrderID: o.ClientOrderID,
		PlatformSeq:   seq.Add(1),
		AckTsNs:       time.Now().UnixNano(),
		GatewayPhase:  "stub",
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(ack)
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
