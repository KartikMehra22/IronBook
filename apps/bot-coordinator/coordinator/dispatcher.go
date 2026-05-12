package coordinator

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

// Dispatcher fires the compiled events at the fairness-gateway over REST.
// Phase 2 ignores per-event errors — the gateway already logs them; Phase 3
// pipes them into telemetry.
type Dispatcher struct {
	GatewayURL string
	HTTP       *http.Client
}

// Run walks the events in order and posts each to /v1/order at its absolute
// offset from the dispatcher start. Returns when ctx is cancelled or all
// events have been sent.
func (d *Dispatcher) Run(ctx context.Context, events []Event) error {
	if d.HTTP == nil {
		d.HTTP = &http.Client{Timeout: 5 * time.Second}
	}
	if d.GatewayURL == "" {
		return fmt.Errorf("dispatcher: GatewayURL is required")
	}
	start := time.Now()
	for _, e := range events {
		dt := time.Until(start.Add(time.Duration(e.OffsetNs)))
		if dt > 0 {
			select {
			case <-time.After(dt):
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		if err := d.post(ctx, e); err != nil && ctx.Err() != nil {
			return ctx.Err()
		}
	}
	return nil
}

func (d *Dispatcher) post(ctx context.Context, e Event) error {
	body, _ := json.Marshal(map[string]any{
		"bot_id":     e.BotID,
		"local_seq":  e.OffsetNs,
		"side":       e.Order.Side,
		"qty":        e.Order.Qty,
		"price":      e.Order.Price,
		"order_type": e.Order.OrderType,
		"tif":        e.Order.TIF,
	})
	req, err := http.NewRequestWithContext(ctx, "POST", d.GatewayURL+"/v1/order", bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := d.HTTP.Do(req)
	if err != nil {
		return err
	}
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	return nil
}
