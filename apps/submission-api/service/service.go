// Package service implements the submission-api business logic.
package service

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/google/uuid"

	"github.com/KartikMehra22/IronBook/apps/submission-api/repo"
)

// Service orchestrates the upload flow: store source in MinIO content-addressed,
// then write submission metadata to Postgres. Build is dispatched in a later phase.
type Service struct {
	PG    *repo.Postgres
	MinIO *repo.MinIO
}

// UploadResult is what Upload returns to the caller.
type UploadResult struct {
	ID     uuid.UUID
	Sha256 [32]byte
	Size   int64
}

// Upload streams r to MinIO, hashes it, and inserts (or dedupes) the submission row.
func (s *Service) Upload(ctx context.Context, language string, r io.Reader) (*UploadResult, error) {
	sha, n, err := s.MinIO.PutContentAddressed(ctx, r)
	if err != nil {
		return nil, fmt.Errorf("minio put: %w", err)
	}
	id := uuid.New()
	sub := &repo.Submission{ID: id, Sha256: sha[:], Language: language, Status: "PENDING"}
	if err := s.PG.Insert(ctx, sub); err != nil {
		if errors.Is(err, repo.ErrSubmissionExists) {
			existing, e2 := s.PG.BySha256(ctx, sha[:])
			if e2 != nil {
				return nil, fmt.Errorf("dedupe lookup: %w", e2)
			}
			return &UploadResult{ID: existing.ID, Sha256: sha, Size: n}, nil
		}
		return nil, fmt.Errorf("postgres insert: %w", err)
	}
	return &UploadResult{ID: id, Sha256: sha, Size: n}, nil
}
