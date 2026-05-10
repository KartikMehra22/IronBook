// Package repo implements persistence for submission-api.
package repo

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Submission mirrors a row in the submissions table.
type Submission struct {
	ID        uuid.UUID
	Sha256    []byte
	Language  string
	Status    string
	CreatedAt time.Time
	UpdatedAt time.Time
}

// Postgres is a thin layer over a pgxpool for the submissions table.
type Postgres struct {
	Pool *pgxpool.Pool
}

// ErrSubmissionExists is returned by Insert when a unique-violation collides
// on the sha256 column.
var ErrSubmissionExists = errors.New("submission already exists")

// Insert writes a new submission. Returns ErrSubmissionExists on sha256 dedup.
func (p *Postgres) Insert(ctx context.Context, s *Submission) error {
	_, err := p.Pool.Exec(ctx,
		`INSERT INTO submissions (id, sha256, language, status) VALUES ($1, $2, $3, $4)`,
		s.ID, s.Sha256, s.Language, s.Status)
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23505" {
			return ErrSubmissionExists
		}
		return err
	}
	return nil
}

// BySha256 fetches the submission with the given content hash.
func (p *Postgres) BySha256(ctx context.Context, sha []byte) (*Submission, error) {
	row := p.Pool.QueryRow(ctx,
		`SELECT id, sha256, language, status, created_at, updated_at
		 FROM submissions WHERE sha256 = $1`, sha)
	var s Submission
	if err := row.Scan(&s.ID, &s.Sha256, &s.Language, &s.Status, &s.CreatedAt, &s.UpdatedAt); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, err
		}
		return nil, err
	}
	return &s, nil
}
