package coordinator

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

func TestDispatcher_PostsEventsToGateway(t *testing.T) {
	var count atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/order" {
			t.Errorf("unexpected path: %s", r.URL.Path)
		}
		if r.Header.Get("Content-Type") != "application/json" {
			t.Errorf("missing content-type")
		}
		var body map[string]any
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Errorf("decode: %v", err)
		}
		io.Copy(io.Discard, r.Body)
		count.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	events := []Event{
		{OffsetNs: 0, BotID: 1, Order: OrderSpec{Side: "bid", OrderType: "limit", TIF: "gtc", Qty: 5, Price: 100}},
		{OffsetNs: 1_000_000, BotID: 2, Order: OrderSpec{Side: "ask", OrderType: "limit", TIF: "gtc", Qty: 7, Price: 101}},
	}
	d := &Dispatcher{GatewayURL: srv.URL, HTTP: srv.Client()}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := d.Run(ctx, events); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if count.Load() != 2 {
		t.Fatalf("expected 2 events posted; got %d", count.Load())
	}
}

func TestDispatcher_RequiresGatewayURL(t *testing.T) {
	d := &Dispatcher{}
	if err := d.Run(context.Background(), nil); err == nil {
		t.Fatal("expected error for empty GatewayURL")
	}
}
