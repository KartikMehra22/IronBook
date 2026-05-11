package gateway

import (
	"context"
	"encoding/json"
	"net/http"

	pb "github.com/KartikMehra22/IronBook/pkg/proto/ironbook/v1"
)

// RESTOrder is the JSON shape the bot fleet POSTs to /v1/order. It's a thin
// envelope that the server normalises into a NormalizedOrder before fan-out.
type RESTOrder struct {
	BotID     uint64 `json:"bot_id"`
	LocalSeq  uint64 `json:"local_seq"`
	Side      string `json:"side"` // "bid" | "ask"
	Qty       uint64 `json:"qty"`
	Price     int64  `json:"price"`
	OrderType string `json:"order_type"` // "limit" | "market"
	TIF       string `json:"tif"`        // "gtc" | "ioc" | "fok"
}

// RESTReply is what the bot sees back. Mirrors the gRPC `Reply` but in
// JSON-friendly shape.
type RESTReply struct {
	PlatformSeq uint64     `json:"platform_seq"`
	AckTsNs     uint64     `json:"ack_ts_ns"`
	Status      uint32     `json:"status"`
	Message     string     `json:"message,omitempty"`
	Fills       []RESTFill `json:"fills,omitempty"`
}

// RESTFill is the JSON shape of a Fill returned to the bot.
type RESTFill struct {
	TradeID          uint64 `json:"trade_id"`
	PlatformSeqMaker uint64 `json:"platform_seq_maker"`
	Price            int64  `json:"price"`
	Qty              uint64 `json:"qty"`
	TsNs             uint64 `json:"ts_ns"`
}

// Server stitches a Stamper + Fork into HTTP handlers.
type Server struct {
	Stamper   *Stamper
	Fork      *Fork
	RunSecret []byte
}

// HandleOrder is the canonical REST entry point. POST /v1/order with a
// RESTOrder body.
func (s *Server) HandleOrder(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var ro RESTOrder
	if err := json.NewDecoder(r.Body).Decode(&ro); err != nil {
		http.Error(w, "bad json: "+err.Error(), http.StatusBadRequest)
		return
	}
	rep, status, err := s.Handle(r.Context(), &ro)
	if err != nil {
		http.Error(w, err.Error(), status)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(rep)
}

// Handle normalises, stamps, and fans-out a single order. Exposed so the
// WebSocket transport can reuse the same code path.
func (s *Server) Handle(ctx context.Context, ro *RESTOrder) (*RESTReply, int, error) {
	seq, ts, err := s.Stamper.Next(ctx)
	if err != nil {
		return nil, http.StatusBadGateway, err
	}
	coid := PackClientOrderID(ro.BotID, ro.LocalSeq)
	token := SessionTokenFor(ro.BotID, s.RunSecret)

	n := &pb.NormalizedOrder{
		PlatformSeq:   seq,
		PlatformTsNs:  ts,
		ClientOrderId: coid[:],
		SessionToken:  token[:],
		Side:          sideFromString(ro.Side),
		Qty:           ro.Qty,
		Price:         ro.Price,
		OrderType:     orderTypeFromString(ro.OrderType),
		Tif:           tifFromString(ro.TIF),
	}
	rep, err := s.Fork.Process(ctx, n)
	if err != nil {
		return nil, http.StatusBadGateway, err
	}
	return toRESTReply(rep), http.StatusOK, nil
}

func toRESTReply(r *pb.Reply) *RESTReply {
	out := &RESTReply{}
	if a := r.GetAck(); a != nil {
		out.PlatformSeq = a.GetPlatformSeq()
		out.AckTsNs = a.GetAckTsNs()
		out.Status = a.GetStatus()
		out.Message = a.GetMessage()
	}
	for _, f := range r.GetFills() {
		out.Fills = append(out.Fills, RESTFill{
			TradeID:          f.GetTradeId(),
			PlatformSeqMaker: f.GetPlatformSeqMaker(),
			Price:            f.GetPrice(),
			Qty:              f.GetQty(),
			TsNs:             f.GetTsNs(),
		})
	}
	return out
}

func sideFromString(s string) pb.Side {
	if s == "ask" {
		return pb.Side_SIDE_ASK
	}
	return pb.Side_SIDE_BID
}

func orderTypeFromString(s string) pb.OrderType {
	if s == "market" {
		return pb.OrderType_ORDER_TYPE_MARKET
	}
	return pb.OrderType_ORDER_TYPE_LIMIT
}

func tifFromString(s string) pb.TimeInForce {
	switch s {
	case "ioc":
		return pb.TimeInForce_TIME_IN_FORCE_IOC
	case "fok":
		return pb.TimeInForce_TIME_IN_FORCE_FOK
	default:
		return pb.TimeInForce_TIME_IN_FORCE_GTC
	}
}
