package builder

import (
	"archive/tar"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
)

// ExtractTarGz unpacks a .tar.gz stream into destDir.
func ExtractTarGz(src io.Reader, destDir string) error {
	if err := os.MkdirAll(destDir, 0o755); err != nil {
		return err
	}
	gz, err := gzip.NewReader(src)
	if err != nil {
		return fmt.Errorf("gzip reader: %w", err)
	}
	defer gz.Close()
	tr := tar.NewReader(gz)
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("tar next: %w", err)
		}
		// Reject path traversal.
		clean := filepath.Clean(hdr.Name)
		if clean == ".." || (len(clean) >= 3 && clean[:3] == "../") {
			return fmt.Errorf("tar entry escapes root: %s", hdr.Name)
		}
		target := filepath.Join(destDir, clean)
		switch hdr.Typeflag {
		case tar.TypeDir:
			if err := os.MkdirAll(target, os.FileMode(hdr.Mode)); err != nil {
				return err
			}
		case tar.TypeReg:
			if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
				return err
			}
			f, err := os.OpenFile(target, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, os.FileMode(hdr.Mode))
			if err != nil {
				return err
			}
			if _, err := io.Copy(f, tr); err != nil {
				f.Close()
				return err
			}
			f.Close()
		}
	}
	return nil
}

// Compile builds the contestant source in srcDir for `language` and returns the
// path to the produced binary. Phase 1: minimal, in-process invocation of the
// language toolchain. Phase 4: hermetic Buildkit + dependency mirror.
func Compile(ctx context.Context, language, srcDir string) (string, error) {
	switch language {
	case "rust":
		return compileRust(ctx, srcDir)
	case "go":
		return compileGo(ctx, srcDir)
	case "cpp":
		return compileCpp(ctx, srcDir)
	default:
		return "", fmt.Errorf("unsupported language %q", language)
	}
}

func compileRust(ctx context.Context, srcDir string) (string, error) {
	cmd := exec.CommandContext(ctx, "cargo", "build", "--release")
	cmd.Dir = srcDir
	cmd.Env = append(os.Environ(), "CARGO_HOME=/work/.cargo", "CARGO_TARGET_DIR=/work/target")
	cmd.Stdout, cmd.Stderr = os.Stdout, os.Stderr
	if err := cmd.Run(); err != nil {
		return "", fmt.Errorf("cargo build: %w", err)
	}
	// Cargo workspaces and single-bin crates both end up under target/release/<name>.
	// Pick the first executable file in target/release that isn't a build script.
	entries, err := os.ReadDir("/work/target/release")
	if err != nil {
		return "", err
	}
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		if e.Name() == "deps" || filepath.Ext(e.Name()) != "" {
			continue
		}
		path := filepath.Join("/work/target/release", e.Name())
		fi, err := os.Stat(path)
		if err == nil && fi.Mode()&0o111 != 0 {
			return path, nil
		}
	}
	return "", fmt.Errorf("no release binary found under /work/target/release")
}

func compileGo(ctx context.Context, srcDir string) (string, error) {
	out := "/work/engine"
	cmd := exec.CommandContext(ctx, "go", "build", "-trimpath", "-ldflags=-s -w", "-o", out, "./...")
	cmd.Dir = srcDir
	cmd.Env = append(os.Environ(), "CGO_ENABLED=0", "GOOS=linux", "GOARCH=arm64")
	cmd.Stdout, cmd.Stderr = os.Stdout, os.Stderr
	if err := cmd.Run(); err != nil {
		return "", fmt.Errorf("go build: %w", err)
	}
	return out, nil
}

func compileCpp(ctx context.Context, srcDir string) (string, error) {
	// Phase 1: assume CMakeLists.txt produces an `engine` binary.
	if _, err := os.Stat(filepath.Join(srcDir, "CMakeLists.txt")); err != nil {
		return "", fmt.Errorf("cpp build expects CMakeLists.txt at srcDir: %w", err)
	}
	build := filepath.Join(srcDir, "build")
	if err := exec.CommandContext(ctx, "cmake", "-B", build, "-S", srcDir, "-DCMAKE_BUILD_TYPE=Release").Run(); err != nil {
		return "", fmt.Errorf("cmake: %w", err)
	}
	if err := exec.CommandContext(ctx, "cmake", "--build", build, "--parallel").Run(); err != nil {
		return "", fmt.Errorf("cmake build: %w", err)
	}
	out := filepath.Join(build, "engine")
	if _, err := os.Stat(out); err != nil {
		return "", fmt.Errorf("expected binary at %s: %w", out, err)
	}
	return out, nil
}
