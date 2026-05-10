// Command build-runner is a one-shot Job that turns contestant source into a
// container image. It is dispatched by submission-api and runs in the `builds`
// namespace.
package main

import (
	"context"
	"encoding/hex"
	"fmt"
	"log"
	"os"

	"github.com/jackc/pgx/v5"
	"github.com/minio/minio-go/v7"

	"github.com/KartikMehra22/IronBook/apps/build-runner/builder"
	"github.com/KartikMehra22/IronBook/apps/build-runner/config"
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

	// Decode the hex-encoded sha256 supplied via env into raw bytes for Postgres lookups.
	shaBytes, err := hex.DecodeString(cfg.SubmissionSha256)
	if err != nil {
		log.Fatalf("decode sha: %v", err)
	}

	if err := setStatus(ctx, pg, shaBytes, "BUILDING", ""); err != nil {
		log.Fatalf("status BUILDING: %v", err)
	}

	res, buildErr := runBuild(ctx, cfg, mc)
	if buildErr != nil {
		log.Printf("build failed: %v", buildErr)
		_ = setStatusReject(ctx, pg, shaBytes, buildErr.Error())
		os.Exit(1)
	}

	if err := setStatusReady(ctx, pg, shaBytes, res.Reference); err != nil {
		log.Fatalf("status READY: %v", err)
	}
	log.Printf("READY: %s", res.Reference)
}

func runBuild(ctx context.Context, cfg config.Config, mc *miniclient.Client) (*builder.Result, error) {
	// 1. Pull the source archive from MinIO.
	key := fmt.Sprintf("%s/source.tar.zst", cfg.SubmissionSha256)
	obj, err := mc.Inner.GetObject(ctx, mc.Bucket, key, minio.GetObjectOptions{})
	if err != nil {
		return nil, fmt.Errorf("get source: %w", err)
	}
	defer obj.Close()

	// 2. Extract into the working directory (we accept .tar.zst stored OR .tar.gz; submission-api uses
	// content-type application/zstd but historic uploads in the integration tests are uncompressed/gzipped).
	srcDir := cfg.WorkDir + "/src"
	if err := os.MkdirAll(srcDir, 0o755); err != nil {
		return nil, fmt.Errorf("mkdir src: %w", err)
	}
	if err := builder.ExtractTarGz(obj, srcDir); err != nil {
		return nil, fmt.Errorf("extract: %w", err)
	}

	// 3. Compile.
	binPath, err := builder.Compile(ctx, cfg.Language, srcDir)
	if err != nil {
		return nil, fmt.Errorf("compile: %w", err)
	}

	// 4. Package + push.
	res, err := builder.Build(ctx, binPath, cfg.SubmissionSha256, cfg.Registry)
	if err != nil {
		return nil, fmt.Errorf("package+push: %w", err)
	}
	return res, nil
}

func setStatus(ctx context.Context, pg *postgresclient.Client, sha []byte, status, imageDigest string) error {
	_, err := pg.Pool.Exec(ctx,
		`UPDATE submissions SET status=$2, updated_at=now() WHERE sha256=$1`,
		sha, status)
	return err
}

func setStatusReady(ctx context.Context, pg *postgresclient.Client, sha []byte, imageRef string) error {
	_, err := pg.Pool.Exec(ctx,
		`UPDATE submissions SET status='READY', image_digest=$2, updated_at=now() WHERE sha256=$1`,
		sha, imageRef)
	return err
}

func setStatusReject(ctx context.Context, pg *postgresclient.Client, sha []byte, reason string) error {
	_, err := pg.Pool.Exec(ctx,
		`UPDATE submissions SET status='REJECTED', reject_reason=$2, updated_at=now() WHERE sha256=$1`,
		sha, reason)
	return err
}

// Suppress unused-import warning when building without DB integration locally.
var _ = pgx.ErrNoRows
