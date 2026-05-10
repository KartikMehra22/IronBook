// Package service implements the submission-api business logic.
package service

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"io"

	"github.com/google/uuid"

	"github.com/KartikMehra22/IronBook/apps/submission-api/dispatcher"
	"github.com/KartikMehra22/IronBook/apps/submission-api/repo"
)

// Service orchestrates the upload flow: store source in MinIO content-addressed,
// write submission metadata to Postgres, dispatch a build Job.
type Service struct {
	PG    *repo.Postgres
	MinIO *repo.MinIO
	Disp  *dispatcher.Dispatcher // optional; if nil the build dispatch is skipped (used by integration tests)
}

// UploadResult is what Upload returns to the caller.
type UploadResult struct {
	ID     uuid.UUID
	Sha256 [32]byte
	Size   int64
}

// Upload streams r to MinIO, hashes it, inserts (or dedupes) the submission row,
// then dispatches a build Job. If the submission deduped (already existed), the
// build is not re-dispatched (the original Job's TTL/result is authoritative).
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

	if s.Disp != nil {
		if dErr := s.Disp.Dispatch(ctx, dispatcher.Inputs{
			SubmissionID: id.String(),
			Sha256Hex:    hex.EncodeToString(sha[:]),
			Language:     language,
		}); dErr != nil {
			return nil, fmt.Errorf("dispatch build: %w", dErr)
		}
	}

	return &UploadResult{ID: id, Sha256: sha, Size: n}, nil
}
