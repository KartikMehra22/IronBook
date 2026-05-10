//go:build integration

package integration

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	tcminio "github.com/testcontainers/testcontainers-go/modules/minio"
	tcpg "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/KartikMehra22/IronBook/apps/submission-api/repo"
	"github.com/KartikMehra22/IronBook/apps/submission-api/server"
	"github.com/KartikMehra22/IronBook/apps/submission-api/service"
	"github.com/KartikMehra22/IronBook/pkg/miniclient"
	"github.com/KartikMehra22/IronBook/pkg/postgresclient"
)

func TestUpload_HappyPath(t *testing.T) {
	ctx := context.Background()

	pgC, err := tcpg.RunContainer(ctx,
		testcontainers.WithImage("postgres:16-alpine"),
		tcpg.WithDatabase("ironbook"),
		tcpg.WithUsername("u"),
		tcpg.WithPassword("p"),
		tcpg.WithInitScripts(
			"../../../deploy/manifests/base/postgres/migrations/001_init.sql",
			"../../../deploy/manifests/base/postgres/migrations/002_submissions.sql",
		),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(60*time.Second),
		),
	)
	if err != nil {
		t.Fatalf("postgres container: %v", err)
	}
	t.Cleanup(func() { _ = pgC.Terminate(ctx) })
	dsn, err := pgC.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}
	pg, err := postgresclient.New(ctx, dsn)
	if err != nil {
		t.Fatalf("pg client: %v", err)
	}
	t.Cleanup(pg.Close)

	mC, err := tcminio.RunContainer(ctx,
		testcontainers.WithImage("minio/minio:RELEASE.2024-06-13T22-53-53Z"),
		tcminio.WithUsername("ironuser"),
		tcminio.WithPassword("PASSWORD123"),
	)
	if err != nil {
		t.Fatalf("minio container: %v", err)
	}
	t.Cleanup(func() { _ = mC.Terminate(ctx) })
	mEnd, err := mC.ConnectionString(ctx)
	if err != nil {
		t.Fatal(err)
	}
	mc, err := miniclient.New(mEnd, "ironuser", "PASSWORD123", "submissions", false)
	if err != nil {
		t.Fatal(err)
	}
	if err := mc.EnsureBucket(ctx); err != nil {
		t.Fatalf("ensure bucket: %v", err)
	}

	svc := &service.Service{PG: &repo.Postgres{Pool: pg.Pool}, MinIO: &repo.MinIO{C: mc}}
	srv := &server.Server{Svc: svc}
	ts := httptest.NewServer(http.HandlerFunc(srv.HandleUpload))
	t.Cleanup(ts.Close)

	body := []byte("hello-world rust source archive")
	wantHash := sha256.Sum256(body)
	wantHex := hex.EncodeToString(wantHash[:])

	resp, err := http.Post(ts.URL+"?language=rust", "application/zstd", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("status=%d body=%s", resp.StatusCode, b)
	}
	var out map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatal(err)
	}
	gotSha, _ := out["sha256"].(string)
	if gotSha != wantHex {
		t.Fatalf("sha mismatch: got %q want %q", gotSha, wantHex)
	}

	// Re-uploading the same body must dedupe (return the same id, status code 200).
	resp2, err := http.Post(ts.URL+"?language=rust", "application/zstd", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	defer resp2.Body.Close()
	if resp2.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp2.Body)
		t.Fatalf("dedup status=%d body=%s", resp2.StatusCode, b)
	}
	var out2 map[string]any
	if err := json.NewDecoder(resp2.Body).Decode(&out2); err != nil {
		t.Fatal(err)
	}
	if out2["id"] != out["id"] {
		t.Fatalf("dedup failed: ids differ first=%v second=%v", out["id"], out2["id"])
	}

	// Bad language should 400.
	resp3, _ := http.Post(ts.URL+"?language=bogus", "application/zstd", bytes.NewReader(body))
	defer resp3.Body.Close()
	if resp3.StatusCode != http.StatusBadRequest {
		t.Fatalf("expected 400 for bogus language; got %d", resp3.StatusCode)
	}

	if ctx.Err() != nil {
		t.Fatal(ctx.Err())
	}
	_ = fmt.Sprintf
}
