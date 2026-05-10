package repo

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"io"

	"github.com/KartikMehra22/IronBook/pkg/miniclient"
)

// MinIO writes submission source archives to a bucket, content-addressed by
// sha256 of the bytes streamed in.
type MinIO struct {
	C *miniclient.Client
}

// PutContentAddressed buffers r in memory, computes sha256, and writes to
// <bucket>/<sha-hex>/source.tar.zst. Returns the digest, byte count, and any error.
func (m *MinIO) PutContentAddressed(ctx context.Context, r io.Reader) (sha [32]byte, n int64, err error) {
	var buf bytes.Buffer
	h := sha256.New()
	tr := io.TeeReader(r, h)
	n, err = io.Copy(&buf, tr)
	if err != nil {
		return
	}
	copy(sha[:], h.Sum(nil))
	key := fmt.Sprintf("%x/source.tar.zst", sha)
	_, err = m.C.Inner.PutObject(ctx, m.C.Bucket, key, &buf, n, miniclient.PutOpts())
	return
}
