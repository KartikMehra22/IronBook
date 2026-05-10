// Package postgresclient is a thin pgxpool wrapper used by every IronBook
// service that talks to Postgres.
package postgresclient

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Client owns a pgxpool.Pool. Construct with [New] and close with [Client.Close].
type Client struct {
	Pool *pgxpool.Pool
}

// New parses dsn, opens a pool capped at 16 conns, and pings it.
func New(ctx context.Context, dsn string) (*Client, error) {
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("parse dsn: %w", err)
	}
	cfg.MaxConns = 16
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("connect: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping: %w", err)
	}
	return &Client{Pool: pool}, nil
}

// Close releases the underlying pool.
func (c *Client) Close() {
	if c.Pool != nil {
		c.Pool.Close()
	}
}
