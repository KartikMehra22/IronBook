// Command bot-coordinator compiles a Scenario into a deterministic event
// schedule and dispatches it to the run's fairness-gateway. Reads env vars
// stamped by the BenchmarkRun reconciler (see
// apps/benchmark-operator/internal/controller/benchmarkrun_controller.go).
package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"strconv"
	"syscall"

	"github.com/KartikMehra22/IronBook/apps/bot-coordinator/coordinator"
)

func main() {
	yaml := os.Getenv("IRONBOOK_SCENARIO_YAML")
	seed, err := strconv.ParseInt(os.Getenv("IRONBOOK_SCENARIO_SEED"), 10, 64)
	if err != nil {
		log.Fatalf("IRONBOOK_SCENARIO_SEED: %v", err)
	}
	dur, err := strconv.Atoi(os.Getenv("IRONBOOK_DURATION_SECONDS"))
	if err != nil {
		log.Fatalf("IRONBOOK_DURATION_SECONDS: %v", err)
	}
	gw := os.Getenv("IRONBOOK_GATEWAY_URL")
	if gw == "" {
		log.Fatal("IRONBOOK_GATEWAY_URL is required")
	}

	events := coordinator.Compile(yaml, seed, dur)
	hash := coordinator.ScheduleHash(events)
	log.Printf("bot-coordinator: compiled %d events, schedule_hash=%s gateway=%s", len(events), hash[:16], gw)

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	d := &coordinator.Dispatcher{GatewayURL: gw}
	if err := d.Run(ctx, events); err != nil && ctx.Err() == nil {
		log.Fatalf("dispatcher: %v", err)
	}
	log.Printf("bot-coordinator: complete")
}
