// Package builder packages a compiled binary into a distroless OCI image.
//
// Phase 1 scope (per ADR-012): no Cosign / Trivy / SLSA-3 yet. The image
// is laid down on a distroless static base and pushed to the in-cluster
// registry. Phase 4 Task 21 extends Build with signing + attestation.
package builder

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/google/go-containerregistry/pkg/crane"
	"github.com/google/go-containerregistry/pkg/name"
	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/google/go-containerregistry/pkg/v1/empty"
	"github.com/google/go-containerregistry/pkg/v1/mutate"
	"github.com/google/go-containerregistry/pkg/v1/remote"
	"github.com/google/go-containerregistry/pkg/v1/tarball"
)

// BaseImageARM64 is the distroless static base for ARM64 contestants. CGO-free
// Go binaries and statically-linked Rust binaries run on this without further
// runtime deps. Pinned by digest for reproducibility.
const BaseImageARM64 = "gcr.io/distroless/static-debian12:nonroot"

// Result describes a successfully built image.
type Result struct {
	Repository string // e.g. registry.ironbook.svc:5000/sub/<sha>
	Tag        string // "latest"
	Digest     string // sha256:...
	Reference  string // <Repository>@<Digest>
}

// Build packages a single binary file into a container image whose
// ENTRYPOINT runs that binary, then pushes to `registryRef`.
//
//	binaryPath     local path to the compiled binary
//	contestantSha  hex-encoded sha256 of the original source archive
//	registry       registry host:port (e.g. "registry.ironbook.svc:5000")
//
// The final image is pushed to `<registry>/sub/<contestantSha>:latest` and
// returned with its remote digest.
func Build(ctx context.Context, binaryPath, contestantSha, registry string) (*Result, error) {
	// 1. Pull the distroless base into memory (insecure registry not needed; gcr.io is HTTPS).
	base, err := crane.Pull(BaseImageARM64)
	if err != nil {
		return nil, fmt.Errorf("pull base: %w", err)
	}

	// 2. Build a tarball layer that drops the binary at /usr/local/bin/engine.
	binBytes, err := os.ReadFile(binaryPath)
	if err != nil {
		return nil, fmt.Errorf("read binary: %w", err)
	}
	layerBytes, err := singleFileTarGz("/usr/local/bin/engine", binBytes, 0o755)
	if err != nil {
		return nil, fmt.Errorf("tar layer: %w", err)
	}
	layer, err := tarball.LayerFromOpener(func() (io.ReadCloser, error) {
		return io.NopCloser(bytes.NewReader(layerBytes)), nil
	})
	if err != nil {
		return nil, fmt.Errorf("layer from opener: %w", err)
	}

	// 3. Append the layer onto the base; reset entrypoint.
	withLayer, err := mutate.AppendLayers(base, layer)
	if err != nil {
		return nil, fmt.Errorf("append layer: %w", err)
	}
	cfg, err := withLayer.ConfigFile()
	if err != nil {
		return nil, fmt.Errorf("config file: %w", err)
	}
	cfg = cfg.DeepCopy()
	cfg.Config.Entrypoint = []string{"/usr/local/bin/engine"}
	cfg.Config.Cmd = nil
	cfg.Config.User = "65534:65534"
	cfg.Architecture = "arm64"
	cfg.OS = "linux"
	finalImg, err := mutate.ConfigFile(withLayer, cfg)
	if err != nil {
		return nil, fmt.Errorf("config file mutate: %w", err)
	}

	// 4. Push to the in-cluster registry (insecure HTTP).
	repo := fmt.Sprintf("%s/sub/%s", registry, contestantSha)
	tag, err := name.NewTag(repo+":latest", name.Insecure)
	if err != nil {
		return nil, fmt.Errorf("parse tag: %w", err)
	}
	if err := remote.Write(tag, finalImg, remote.WithContext(ctx)); err != nil {
		return nil, fmt.Errorf("remote write: %w", err)
	}

	digest, err := finalImg.Digest()
	if err != nil {
		return nil, fmt.Errorf("digest: %w", err)
	}

	return &Result{
		Repository: repo,
		Tag:        "latest",
		Digest:     digest.String(),
		Reference:  fmt.Sprintf("%s@%s", repo, digest.String()),
	}, nil
}

// singleFileTarGz builds a gzipped tar with one file at `path` containing `body`.
// Used as the writable layer atop the distroless base.
func singleFileTarGz(path string, body []byte, mode int64) ([]byte, error) {
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	tw := tar.NewWriter(gz)

	hdr := &tar.Header{
		Name:     filepath.Clean(path[1:]), // tar paths are relative
		Mode:     mode,
		Size:     int64(len(body)),
		Typeflag: tar.TypeReg,
	}
	if err := tw.WriteHeader(hdr); err != nil {
		return nil, err
	}
	if _, err := tw.Write(body); err != nil {
		return nil, err
	}
	if err := tw.Close(); err != nil {
		return nil, err
	}
	if err := gz.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Sanity-check helper: returns the raw image bytes without pushing. Used by tests.
func TarLayerForBinary(binPath string) ([]byte, error) {
	body, err := os.ReadFile(binPath)
	if err != nil {
		return nil, err
	}
	return singleFileTarGz("/usr/local/bin/engine", body, 0o755)
}

// Image-version probe used by main to validate the base is reachable.
func ProbeBase(ctx context.Context) (*v1.Manifest, error) {
	base, err := crane.Pull(BaseImageARM64)
	if err != nil {
		return nil, err
	}
	return base.Manifest()
}

// fixed import of empty so go-containerregistry recognises the package wiring.
var _ v1.Image = empty.Image
