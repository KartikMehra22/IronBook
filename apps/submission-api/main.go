// Command submission-api is the contestant upload + lifecycle gRPC + REST service.
package main

import (
	"context"
	"log"
	"net/http"

	"github.com/KartikMehra22/IronBook/apps/submission-api/config"
	"github.com/KartikMehra22/IronBook/apps/submission-api/repo"
	"github.com/KartikMehra22/IronBook/apps/submission-api/server"
	"github.com/KartikMehra22/IronBook/apps/submission-api/service"
	"github.com/KartikMehra22/IronBook/pkg/miniclient"
	"github.com/KartikMehra22/IronBook/pkg/postgresclient"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("config: %v", err)
	}
	ctx := context.Background()

	pg, err := postgresclient.New(ctx, cfg.PostgresDSN)
	if err != nil {
		log.Fatalf("pg: %v", err)
	}
	defer pg.Close()

	mc, err := miniclient.New(cfg.MinIOEndpoint, cfg.MinIOAccessKey, cfg.MinIOSecretKey, cfg.MinIOBucket, cfg.MinIOUseSSL)
	if err != nil {
		log.Fatalf("minio: %v", err)
	}
	if err := mc.EnsureBucket(ctx); err != nil {
		log.Fatalf("ensure bucket: %v", err)
	}

	svc := &service.Service{
		PG:    &repo.Postgres{Pool: pg.Pool},
		MinIO: &repo.MinIO{C: mc},
	}
	srv := &server.Server{Svc: svc}

	mux := http.NewServeMux()
	mux.HandleFunc("/v1/upload", srv.HandleUpload)
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("ok"))
	})

	log.Printf("submission-api listening on %s", cfg.HTTPAddr)
	if err := http.ListenAndServe(cfg.HTTPAddr, mux); err != nil {
		log.Fatalf("serve: %v", err)
	}
}
