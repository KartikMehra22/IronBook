// Command fairness-gateway is the IronBook hot-path proxy.
//
// For every order from a bot:
//  1. Reserve a (platform_seq, platform_ts_ns) stamp from the time-service.
//  2. Strip the bot's identity (replace bot_id with sha256(bot_id || run_secret)).
//  3. Fan out the normalised order to BOTH the submission and the reference
//     oracle in parallel.
//  4. Return the submission's reply to the bot; tee both replies to the sink
//     so the divergence-detector can join them downstream.
//
// REST on POST /v1/order. WebSocket on /v1/order/ws. Health on /healthz.
package main

import (
	"encoding/hex"
	"log"
	"net/http"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/KartikMehra22/IronBook/apps/fairness-gateway/config"
	"github.com/KartikMehra22/IronBook/apps/fairness-gateway/gateway"
	"github.com/KartikMehra22/IronBook/apps/fairness-gateway/transport"
	pb "github.com/KartikMehra22/IronBook/pkg/proto/ironbook/v1"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("config: %v", err)
	}

	timeConn := mustDial(cfg.TimeService)
	subConn := mustDial(cfg.SubmissionEnd)
	orConn := mustDial(cfg.OracleEnd)
	defer timeConn.Close()
	defer subConn.Close()
	defer orConn.Close()

	stamper := gateway.NewStamper(pb.NewTimeServiceClient(timeConn), cfg.StampBatchSize)

	sink, err := gateway.NewFileSink(cfg.EventLogPath)
	if err != nil {
		log.Fatalf("file sink: %v", err)
	}
	defer sink.Close()

	fork := &gateway.Fork{
		Submission: pb.NewOrderIntakeClient(subConn),
		Oracle:     pb.NewOrderIntakeClient(orConn),
		Sink:       sink,
	}

	secret, err := hex.DecodeString(cfg.RunSecret)
	if err != nil {
		log.Fatalf("decode run secret: %v", err)
	}
	srv := &gateway.Server{Stamper: stamper, Fork: fork, RunSecret: secret}

	mux := http.NewServeMux()
	mux.HandleFunc("/v1/order", srv.HandleOrder)
	mux.HandleFunc("/v1/order/ws", transport.WSHandler(srv))
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("ok"))
	})

	log.Printf("fairness-gateway listening on %s (time=%s sub=%s oracle=%s)",
		cfg.HTTPAddr, cfg.TimeService, cfg.SubmissionEnd, cfg.OracleEnd)
	if err := http.ListenAndServe(cfg.HTTPAddr, mux); err != nil { //nolint:gosec
		log.Fatalf("serve: %v", err)
	}
}

func mustDial(addr string) *grpc.ClientConn {
	c, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("dial %s: %v", addr, err)
	}
	return c
}
