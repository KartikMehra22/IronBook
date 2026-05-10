// Package miniclient is a thin wrapper around the MinIO Go SDK used by every
// IronBook service that talks to MinIO (or any S3-compatible store).
package miniclient

import (
	"context"
	"fmt"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

// Client wraps a *minio.Client with a default bucket.
type Client struct {
	Inner  *minio.Client
	Bucket string
}

// New constructs a Client. endpoint must NOT include a scheme; useSSL controls TLS.
func New(endpoint, accessKey, secretKey, bucket string, useSSL bool) (*Client, error) {
	c, err := minio.New(endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(accessKey, secretKey, ""),
		Secure: useSSL,
	})
	if err != nil {
		return nil, fmt.Errorf("minio new: %w", err)
	}
	return &Client{Inner: c, Bucket: bucket}, nil
}

// EnsureBucket creates Client.Bucket if it does not already exist.
func (c *Client) EnsureBucket(ctx context.Context) error {
	exists, err := c.Inner.BucketExists(ctx, c.Bucket)
	if err != nil {
		return fmt.Errorf("bucket exists: %w", err)
	}
	if !exists {
		if err := c.Inner.MakeBucket(ctx, c.Bucket, minio.MakeBucketOptions{}); err != nil {
			return fmt.Errorf("make bucket: %w", err)
		}
	}
	return nil
}

// PutOpts returns the canonical PutObjectOptions for IronBook source uploads.
func PutOpts() minio.PutObjectOptions {
	return minio.PutObjectOptions{ContentType: "application/zstd"}
}
